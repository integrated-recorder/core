// Package pluginregistry reads the Runtime Host's approved static plugin
// registry and prepares verified adapter sources for the existing immutable
// adapter catalog. It does not build or execute installed adapters directly.
package pluginregistry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/storageproto"
)

const (
	SchemaVersion        = 1
	SchemaVersionV2      = 2
	SchemaVersionV3      = 3
	OfficialCatalogV3URL = "https://integrated-recorder.github.io/plugin-registry/catalog-v3.json"
	TypeSource           = "source"
	TypeStorage          = "storage"
	StorageBinaryPrefix  = "integrated-recorder-storage-"
	MaxCatalogBytes      = 2 << 20
	MaxPlugins           = 256
	MaxReleasesPerPlugin = 128
	MaxArtifactsRelease  = 16
	MaxArtifactBytes     = adaptercatalog.MaxArtifactBytes
	maxRegistryURLBytes  = 2048
	maxNameBytes         = 128
	maxIdentityBytes     = 128
	maxRepositoryBytes   = 512
	maxSourceCommitBytes = 128
	minSourceCommitBytes = 7
	minFilenameBytes     = 29
	maxFilenameBytes     = 92
	maxChannelBytes      = 32
)

var (
	ErrInvalidConfig       = errors.New("plugin registry configuration is invalid")
	ErrUnavailable         = errors.New("plugin registry unavailable")
	ErrPluginNotFound      = errors.New("plugin was not found")
	ErrAlreadyInstalled    = errors.New("plugin is already installed")
	ErrNotInstalled        = errors.New("plugin is not installed")
	ErrPlatformUnsupported = errors.New("plugin is unavailable for this platform")
	ErrDownloadFailed      = errors.New("plugin artifact download failed")
	ErrVerificationFailed  = errors.New("plugin artifact verification failed")
	ErrIdentityMismatch    = errors.New("plugin descriptor identity does not match registry")
	ErrInstallFailed       = errors.New("plugin installation could not be prepared")
	ErrNoUpdateAvailable   = errors.New("plugin has no stable update available")
	ErrOperationConflict   = errors.New("plugin operation conflicts with current desired state")
	ErrUnsafeStore         = errors.New("plugin registry store is unsafe")
	versionPattern         = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
	pluginIDPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	sourceCommitPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{6,127}$`)
	shaPattern             = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type registryDocument struct {
	SchemaVersion int              `json:"schema_version"`
	Plugins       []registryPlugin `json:"plugins"`
}

type registryPlugin struct {
	ID         string            `json:"id"`
	Type       string            `json:"type,omitempty"`
	Publisher  *PublisherInfo    `json:"publisher,omitempty"`
	Name       string            `json:"name"`
	Repository string            `json:"repository"`
	Channels   map[string]string `json:"channels"`
	Releases   []registryRelease `json:"releases"`
}

// PublisherInfo is a Registry v3 affiliation assertion. The effective trust
// attestation still depends on the configured Registry authority, not only on
// this document field.
type PublisherInfo struct {
	Kind string `json:"kind"`
}

type registryRelease struct {
	Version         string             `json:"version"`
	ProtocolVersion int                `json:"protocol_version,omitempty"`
	Protocol        *Protocol          `json:"protocol,omitempty"`
	SourceCommit    string             `json:"source_commit"`
	Artifacts       []registryArtifact `json:"artifacts"`
}

// Protocol describes the typed protocol required by schema v2 releases.
// Schema v1 continues to use registryRelease.ProtocolVersion.
type Protocol struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type registryArtifact struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	URL      string `json:"url"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}

// Registry, Plugin, Release, and Artifact are the stable v1 static-catalog
// wire shapes. They intentionally contain approval metadata only.
type Registry = registryDocument
type Plugin = registryPlugin
type Release = registryRelease
type Artifact = registryArtifact

type View struct {
	Configured  bool         `json:"configured"`
	Available   bool         `json:"available"`
	FailureCode string       `json:"failure_code,omitempty"`
	Plugins     []PluginView `json:"plugins"`
	// Installed is for in-process Host orchestration only. In particular, its
	// content digest and binary filename must never become a public projection.
	Installed []DesiredPlugin `json:"-"`
}

type PluginView struct {
	ID               string                  `json:"id"`
	Type             string                  `json:"type"`
	Name             string                  `json:"name"`
	AvailableVersion string                  `json:"available_version,omitempty"`
	InstalledVersion string                  `json:"installed_version,omitempty"`
	Installed        bool                    `json:"installed"`
	UpdateAvailable  bool                    `json:"update_available"`
	Trust            plugintrust.Attestation `json:"trust"`
}

type selectedRelease struct {
	plugin  registryPlugin
	release registryRelease
}

func decodeRegistry(data []byte) (registryDocument, error) {
	if len(data) == 0 || len(data) > MaxCatalogBytes || !utf8.Valid(data) {
		return registryDocument{}, ErrUnavailable
	}
	if validateRegistryWireShape(data) != nil {
		return registryDocument{}, ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document registryDocument
	if err := decoder.Decode(&document); err != nil {
		return registryDocument{}, ErrUnavailable
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return registryDocument{}, ErrUnavailable
	}
	if err := validateRegistry(document); err != nil {
		return registryDocument{}, ErrUnavailable
	}
	return document, nil
}

// validateRegistryWireShape enforces exact field spelling and presence for
// each supported schema version.
// encoding/json intentionally matches struct keys case-insensitively and
// accepts null for several Go zero values, both of which are too permissive
// for a signed-off distribution catalog contract. It also rejects duplicate
// object keys so the same bytes cannot have parser-dependent meanings.
func validateRegistryWireShape(data []byte) error {
	root, err := strictJSONObject(data, "schema_version", "plugins")
	if err != nil || isJSONNull(root["schema_version"]) {
		return ErrUnavailable
	}
	var schemaVersion int
	if err := json.Unmarshal(root["schema_version"], &schemaVersion); err != nil {
		return ErrUnavailable
	}
	if schemaVersion != SchemaVersion && schemaVersion != SchemaVersionV2 && schemaVersion != SchemaVersionV3 {
		return ErrUnavailable
	}
	plugins, err := strictJSONArray(root["plugins"], MaxPlugins)
	if err != nil {
		return ErrUnavailable
	}
	for _, rawPlugin := range plugins {
		pluginFields := []string{"id", "name", "repository", "channels", "releases"}
		if schemaVersion == SchemaVersionV2 || schemaVersion == SchemaVersionV3 {
			pluginFields = []string{"id", "type", "name", "repository", "channels", "releases"}
		}
		if schemaVersion == SchemaVersionV3 {
			pluginFields = []string{"id", "type", "publisher", "name", "repository", "channels", "releases"}
		}
		plugin, err := strictJSONObject(rawPlugin, pluginFields...)
		if err != nil {
			return ErrUnavailable
		}
		for _, key := range []string{"id", "name", "repository"} {
			if !validJSONStringShape(plugin[key]) {
				return ErrUnavailable
			}
		}
		if (schemaVersion == SchemaVersionV2 || schemaVersion == SchemaVersionV3) && !validJSONStringShape(plugin["type"]) {
			return ErrUnavailable
		}
		if schemaVersion == SchemaVersionV3 {
			publisher, err := strictJSONObject(plugin["publisher"], "kind")
			if err != nil || !validJSONStringShape(publisher["kind"]) {
				return ErrUnavailable
			}
		}
		channels, err := strictJSONObjectAny(plugin["channels"], 3)
		if err != nil {
			return ErrUnavailable
		}
		for _, value := range channels {
			if !validJSONStringShape(value) {
				return ErrUnavailable
			}
		}
		releases, err := strictJSONArray(plugin["releases"], MaxReleasesPerPlugin)
		if err != nil {
			return ErrUnavailable
		}
		for _, rawRelease := range releases {
			releaseFields := []string{"version", "protocol_version", "source_commit", "artifacts"}
			if schemaVersion == SchemaVersionV2 || schemaVersion == SchemaVersionV3 {
				releaseFields = []string{"version", "protocol", "source_commit", "artifacts"}
			}
			release, err := strictJSONObject(rawRelease, releaseFields...)
			if err != nil {
				return ErrUnavailable
			}
			for _, key := range []string{"version", "source_commit"} {
				if !validJSONStringShape(release[key]) {
					return ErrUnavailable
				}
			}
			if schemaVersion == SchemaVersion {
				if isJSONNull(release["protocol_version"]) {
					return ErrUnavailable
				}
			} else {
				protocol, err := strictJSONObject(release["protocol"], "name", "version")
				if err != nil || !validJSONStringShape(protocol["name"]) || isJSONNull(protocol["version"]) {
					return ErrUnavailable
				}
			}
			artifacts, err := strictJSONArray(release["artifacts"], MaxArtifactsRelease)
			if err != nil {
				return ErrUnavailable
			}
			for _, rawArtifact := range artifacts {
				artifact, err := strictJSONObject(rawArtifact, "os", "arch", "url", "filename", "size", "sha256")
				if err != nil {
					return ErrUnavailable
				}
				for _, key := range []string{"os", "arch", "url", "filename", "sha256"} {
					if !validJSONStringShape(artifact[key]) {
						return ErrUnavailable
					}
				}
				if isJSONNull(artifact["size"]) {
					return ErrUnavailable
				}
			}
		}
	}
	return nil
}

func strictJSONObject(data []byte, required ...string) (map[string]json.RawMessage, error) {
	fields, err := strictJSONObjectAny(data, len(required))
	if err != nil || len(fields) != len(required) {
		return nil, ErrUnavailable
	}
	for _, key := range required {
		if _, ok := fields[key]; !ok {
			return nil, ErrUnavailable
		}
	}
	return fields, nil
}

func strictJSONObjectAny(data []byte, maxProperties int) (map[string]json.RawMessage, error) {
	if maxProperties < 0 {
		return nil, ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	delim, ok := token.(json.Delim)
	if err != nil || !ok || delim != '{' {
		return nil, ErrUnavailable
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, ErrUnavailable
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, ErrUnavailable
		}
		if len(fields) >= maxProperties {
			return nil, ErrUnavailable
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil || len(value) == 0 || isJSONNull(value) {
			return nil, ErrUnavailable
		}
		fields[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, ErrUnavailable
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, ErrUnavailable
	}
	return fields, nil
}

func strictJSONArray(data []byte, maxItems int) ([]json.RawMessage, error) {
	if maxItems < 0 {
		return nil, ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	delim, ok := token.(json.Delim)
	if err != nil || !ok || delim != '[' {
		return nil, ErrUnavailable
	}
	values := make([]json.RawMessage, 0)
	for decoder.More() {
		if len(values) >= maxItems {
			return nil, ErrUnavailable
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil || len(value) == 0 || isJSONNull(value) {
			return nil, ErrUnavailable
		}
		values = append(values, value)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, ErrUnavailable
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, ErrUnavailable
	}
	return values, nil
}

func validJSONStringShape(data []byte) bool {
	if isJSONNull(data) {
		return false
	}
	var value string
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(&value) != nil {
		return false
	}
	return requireJSONEOF(decoder) == nil
}

func isJSONNull(data []byte) bool { return bytes.Equal(bytes.TrimSpace(data), []byte("null")) }

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrUnavailable
	}
	return nil
}

func validateRegistry(document registryDocument) error {
	if (document.SchemaVersion != SchemaVersion && document.SchemaVersion != SchemaVersionV2 && document.SchemaVersion != SchemaVersionV3) || len(document.Plugins) > MaxPlugins {
		return ErrUnavailable
	}
	plugins := make(map[string]bool, len(document.Plugins))
	for _, plugin := range document.Plugins {
		pluginType := plugin.Type
		if document.SchemaVersion == SchemaVersion {
			if plugin.Type != "" {
				return ErrUnavailable
			}
			// A v1 plugin has no type on the wire and is interpreted as a source.
			pluginType = TypeSource
		} else if pluginType != TypeSource && pluginType != TypeStorage {
			return ErrUnavailable
		}
		if document.SchemaVersion == SchemaVersionV3 {
			if plugin.Publisher == nil || (plugin.Publisher.Kind != string(plugintrust.FirstParty) && plugin.Publisher.Kind != string(plugintrust.ThirdParty)) {
				return ErrUnavailable
			}
		} else if plugin.Publisher != nil {
			return ErrUnavailable
		}
		if !validPluginID(plugin.ID) || plugins[plugin.ID] || !validText(plugin.Name, maxNameBytes) || !validRepositoryURL(plugin.Repository) || len(plugin.Releases) == 0 || len(plugin.Releases) > MaxReleasesPerPlugin || len(plugin.Channels) == 0 || len(plugin.Channels) > 3 {
			return ErrUnavailable
		}
		// "local" is the Runtime Host's built-in primary-storage selector in
		// the Storage API. A remote provider with this ID would be impossible
		// to activate or address unambiguously.
		if pluginType == TypeStorage && plugin.ID == "local" {
			return ErrUnavailable
		}
		if pluginType == TypeSource && plugin.ID == "hls" {
			return ErrUnavailable
		}
		plugins[plugin.ID] = true
		releases := make(map[string]registryRelease, len(plugin.Releases))
		for _, release := range plugin.Releases {
			protocolVersion := release.ProtocolVersion
			if document.SchemaVersion == SchemaVersion {
				if release.Protocol != nil {
					return ErrUnavailable
				}
			} else {
				if release.Protocol == nil || release.Protocol.Name != pluginType || release.Protocol.Version != protocolVersionForType(pluginType) || release.ProtocolVersion != 0 {
					return ErrUnavailable
				}
				protocolVersion = release.Protocol.Version
			}
			if !validIdentity(release.Version) || releases[release.Version].Version != "" || protocolVersion != protocolVersionForType(pluginType) || !validSourceCommit(release.SourceCommit) || len(release.Artifacts) == 0 || len(release.Artifacts) > MaxArtifactsRelease {
				return ErrUnavailable
			}
			platforms := map[string]bool{}
			for _, artifact := range release.Artifacts {
				platform := artifact.OS + "/" + artifact.Arch
				prefix := adaptercatalog.BinaryPrefix
				if pluginType == TypeStorage {
					prefix = StorageBinaryPrefix
				}
				if !validTarget(artifact.OS, artifact.Arch) || platforms[platform] || !validArtifactURL(artifact.URL) || artifact.Filename != prefix+plugin.ID || len(artifact.Filename) < minFilenameBytes || !validText(artifact.Filename, maxFilenameBytes) || artifact.Size <= 0 || artifact.Size > MaxArtifactBytes || !shaPattern.MatchString(artifact.SHA256) {
					return ErrUnavailable
				}
				platforms[platform] = true
			}
			releases[release.Version] = release
		}
		for channel, version := range plugin.Channels {
			if channel != "stable" && channel != "beta" && channel != "development" || !validIdentity(channel) || !validIdentity(version) {
				return ErrUnavailable
			}
			if _, ok := releases[version]; !ok {
				return ErrUnavailable
			}
		}
	}
	return nil
}

func protocolVersionForType(pluginType string) int {
	switch pluginType {
	case TypeSource:
		return adapterproto.Version
	case TypeStorage:
		return storageproto.Version
	default:
		return 0
	}
}

func validIdentity(value string) bool {
	return len(value) <= maxIdentityBytes && versionPattern.MatchString(value) && value != "." && value != ".."
}

func validPluginID(value string) bool { return len(value) <= 64 && pluginIDPattern.MatchString(value) }

func validSourceCommit(value string) bool {
	return len(value) >= minSourceCommitBytes && len(value) <= maxSourceCommitBytes && sourceCommitPattern.MatchString(value)
}

func validRepositoryURL(raw string) bool {
	if len(raw) > maxRepositoryBytes || !validHTTPSURL(raw, false) {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && u.EscapedPath() != "" && u.EscapedPath() != "/"
}

func validArtifactURL(raw string) bool {
	if !validHTTPSURL(raw, true) {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && u.EscapedPath() != ""
}

func validText(value string, max int) bool {
	if len(value) == 0 || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return false
	}
	for _, r := range value {
		if r == 0 || r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validTarget(goos, goarch string) bool {
	if goarch != "amd64" && goarch != "arm64" {
		return false
	}
	// Linux is the production container target. Native Darwin builds use the
	// same Go executable and Unix-domain-socket protocol and are supported for
	// local installations and development.
	return goos == "linux" || goos == "darwin"
}

func validHTTPSURL(raw string, allowQuery bool) bool {
	if len(raw) == 0 || len(raw) > maxRegistryURLBytes || !utf8.ValidString(raw) {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || (!allowQuery && (u.RawQuery != "" || u.ForceQuery)) {
		return false
	}
	return validHostPort(u)
}

func validHostPort(u *url.URL) bool {
	if strings.ContainsAny(u.Host, "\r\n\\") {
		return false
	}
	port := u.Port()
	if port == "" {
		return !strings.HasSuffix(u.Host, ":")
	}
	for _, r := range port {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func stableRelease(plugin registryPlugin) (registryRelease, bool) {
	version := plugin.Channels["stable"]
	if version == "" {
		return registryRelease{}, false
	}
	for _, release := range plugin.Releases {
		if release.Version == version {
			return release, true
		}
	}
	return registryRelease{}, false
}

func isOfficialCatalogResponse(configuredURL, finalURL string) bool {
	return configuredURL == OfficialCatalogV3URL && finalURL == OfficialCatalogV3URL
}

func registryAttestation(schemaVersion int, authority plugintrust.Authority, publisher *PublisherInfo) plugintrust.Attestation {
	if schemaVersion == SchemaVersionV3 && authority == plugintrust.Official && publisher != nil {
		return plugintrust.NewOfficialRegistry(plugintrust.Publisher(publisher.Kind))
	}
	// Older schemas have no publisher affiliation. Custom registries may
	// include v3 publisher claims, but those claims do not establish official
	// authority or first-party status.
	return plugintrust.NewCustomRegistry()
}

func desiredAttestation(plugin DesiredPlugin) plugintrust.Attestation {
	if plugin.Attestation == nil {
		return plugintrust.Legacy()
	}
	return *plugin.Attestation
}

func projectedDesiredAttestation(plugin DesiredPlugin) plugintrust.Attestation {
	attestation := desiredAttestation(plugin)
	if attestation.Provenance == plugintrust.LegacyUnclassified {
		return plugintrust.NewOperator()
	}
	return attestation
}

func attestationPointer(value plugintrust.Attestation) *plugintrust.Attestation {
	copy := value
	return &copy
}

func selectArtifact(release registryRelease, goos, goarch string) (registryArtifact, bool) {
	for _, artifact := range release.Artifacts {
		if artifact.OS == goos && artifact.Arch == goarch {
			return artifact, true
		}
	}
	return registryArtifact{}, false
}

func redactFailure(err error) error {
	if errors.Is(err, ErrPlatformUnsupported) {
		return ErrPlatformUnsupported
	}
	if errors.Is(err, ErrIdentityMismatch) {
		return ErrIdentityMismatch
	}
	if errors.Is(err, ErrVerificationFailed) {
		return ErrVerificationFailed
	}
	if errors.Is(err, ErrDownloadFailed) {
		return ErrDownloadFailed
	}
	return fmt.Errorf("%w", ErrInstallFailed)
}
