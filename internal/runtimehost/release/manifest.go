// Package release defines and verifies immutable application release manifests.
// It deliberately performs no network access or archive extraction.
package release

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	ManifestSchemaVersion = 1
	// MaxManifestBytes bounds parsing and signature verification inputs.
	MaxManifestBytes = 1 << 20
	// MaxArtifactBytes bounds the size of any one downloaded release artifact.
	MaxArtifactBytes = 512 << 20
)

var (
	ErrInvalidManifest   = errors.New("invalid release manifest")
	ErrInvalidSignature  = errors.New("invalid release signature")
	ErrUnknownSigningKey = errors.New("unknown release signing key")
	ErrIncompatible      = errors.New("release is incompatible with runtime host")
	ErrInvalidArtifact   = errors.New("invalid release artifact")
	ErrArtifactFile      = errors.New("release artifact file could not be verified")

	semverPattern   = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)
	commitPattern   = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)
	namePattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	keyIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	platformPattern = regexp.MustCompile(`^[a-z][a-z0-9]{0,31}$`)
	goPlatforms     = map[string]struct{}{
		"aix": {}, "android": {}, "darwin": {}, "dragonfly": {}, "freebsd": {}, "illumos": {},
		"ios": {}, "js": {}, "linux": {}, "netbsd": {}, "openbsd": {}, "plan9": {},
		"solaris": {}, "wasip1": {}, "windows": {},
	}
	goArchitectures = map[string]struct{}{
		"386": {}, "amd64": {}, "amd64p32": {}, "arm": {}, "arm64": {}, "loong64": {},
		"mips": {}, "mipsle": {}, "mips64": {}, "mips64le": {}, "ppc64": {}, "ppc64le": {},
		"riscv64": {}, "s390x": {}, "wasm": {},
	}
)

const (
	RoleRuntimeHost    = "runtime-host"
	RoleControlPlane   = "control-plane"
	RoleRecorderEngine = "recorder-engine"
	RoleAdapterRuntime = "adapter-runtime"
)

var requiredRoles = [...]string{RoleRuntimeHost, RoleControlPlane, RoleRecorderEngine, RoleAdapterRuntime}

// Manifest is the signed, versioned description of one immutable application
// release. Field names and compatibility ranges are part of the release
// contract; compatibility is never inferred from the release version.
type Manifest struct {
	ManifestSchemaVersion   int        `json:"manifest_schema_version"`
	ReleaseVersion          string     `json:"release_version"`
	Commit                  string     `json:"commit"`
	BuildTime               string     `json:"build_time"`
	Channel                 string     `json:"channel"`
	KeyID                   string     `json:"key_id"`
	MinimumHostProtocol     int        `json:"minimum_host_protocol"`
	MaximumHostProtocol     int        `json:"maximum_host_protocol"`
	ControlProtocolVersion  int        `json:"control_protocol_version"`
	EngineProtocolVersion   int        `json:"engine_protocol_version"`
	AdapterProtocolMinimum  int        `json:"adapter_protocol_minimum"`
	AdapterProtocolMaximum  int        `json:"adapter_protocol_maximum"`
	ArchiveReadMinimum      int        `json:"archive_read_minimum"`
	ArchiveReadMaximum      int        `json:"archive_read_maximum"`
	ArchiveWriteFormat      int        `json:"archive_write_format"`
	ManagementSchemaMinimum int        `json:"management_schema_minimum"`
	ManagementSchemaMaximum int        `json:"management_schema_maximum"`
	Platform                string     `json:"platform"`
	Architecture            string     `json:"architecture"`
	Artifacts               []Artifact `json:"artifacts"`
}

type Artifact struct {
	Role     string `json:"role"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}

// ProtocolRange is an inclusive, explicitly declared compatibility range.
type ProtocolRange struct {
	Minimum int
	Maximum int
}

// HostCompatibility describes installed host capabilities. Values must come
// from explicit runtime contracts rather than release-version comparisons.
type HostCompatibility struct {
	Platform                string
	Architecture            string
	RuntimeProtocolVersion  int
	ControlProtocolRange    ProtocolRange
	EngineProtocolRange     ProtocolRange
	AdapterProtocolRange    ProtocolRange
	ArchiveReadRange        ProtocolRange
	ArchiveWriteFormat      int
	ManagementSchemaVersion int
}

// Validate checks every manifest field and rejects ambiguous artifact tables.
func (m Manifest) Validate() error {
	if m.ManifestSchemaVersion != ManifestSchemaVersion {
		return invalid("unsupported manifest schema version")
	}
	if !semverPattern.MatchString(m.ReleaseVersion) {
		return invalid("release version is invalid")
	}
	if !commitPattern.MatchString(m.Commit) {
		return invalid("release commit is invalid")
	}
	if _, err := time.Parse(time.RFC3339, m.BuildTime); err != nil {
		return invalid("build time is invalid")
	}
	if m.Channel != "stable" && m.Channel != "prerelease" {
		return invalid("release channel is invalid")
	}
	if !keyIDPattern.MatchString(m.KeyID) {
		return invalid("signing key ID is invalid")
	}
	if !validRange(m.MinimumHostProtocol, m.MaximumHostProtocol) ||
		!validRange(m.AdapterProtocolMinimum, m.AdapterProtocolMaximum) ||
		!validRange(m.ArchiveReadMinimum, m.ArchiveReadMaximum) ||
		!validRange(m.ManagementSchemaMinimum, m.ManagementSchemaMaximum) ||
		m.ControlProtocolVersion < 1 || m.EngineProtocolVersion < 1 || m.ArchiveWriteFormat < 1 ||
		m.ArchiveWriteFormat < m.ArchiveReadMinimum || m.ArchiveWriteFormat > m.ArchiveReadMaximum {
		return invalid("protocol or schema compatibility range is invalid")
	}
	if !validTarget(m.Platform, m.Architecture) {
		return invalid("platform or architecture is invalid")
	}
	if len(m.Artifacts) != len(requiredRoles) {
		return invalid("release artifact roles are incomplete")
	}
	roles := make(map[string]struct{}, len(m.Artifacts))
	filenames := make(map[string]struct{}, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		if _, required := requiredRole(artifact.Role); !required {
			return invalid("release artifact role is unsupported")
		}
		if _, duplicate := roles[artifact.Role]; duplicate {
			return invalid("release artifact role is duplicated")
		}
		roles[artifact.Role] = struct{}{}
		if !safeFilename(artifact.Filename) {
			return invalid("release artifact filename is unsafe")
		}
		filenameKey := strings.ToLower(artifact.Filename)
		if _, duplicate := filenames[filenameKey]; duplicate {
			return invalid("release artifact filename is duplicated")
		}
		filenames[filenameKey] = struct{}{}
		if artifact.Size < 1 || artifact.Size > MaxArtifactBytes {
			return invalid("release artifact size is outside the allowed bound")
		}
		if len(artifact.SHA256) != sha256.Size*2 || artifact.SHA256 != strings.ToLower(artifact.SHA256) {
			return invalid("release artifact SHA-256 is invalid")
		}
		if _, err := hex.DecodeString(artifact.SHA256); err != nil {
			return invalid("release artifact SHA-256 is invalid")
		}
	}
	for _, role := range requiredRoles {
		if _, ok := roles[role]; !ok {
			return invalid("release artifact roles are incomplete")
		}
	}
	return nil
}

// DecodeAndVerifyManifest strictly decodes, validates, then verifies the
// detached Ed25519 signature over the exact original JSON bytes. Signature
// trust is entirely supplied by the caller; this package contains no trust root.
func DecodeAndVerifyManifest(manifestJSON []byte, signatureBase64 string, trustedKeys map[string]ed25519.PublicKey) (Manifest, error) {
	var manifest Manifest
	if len(manifestJSON) == 0 || len(manifestJSON) > MaxManifestBytes {
		return Manifest{}, invalid("manifest size is outside the allowed bound")
	}
	if err := rejectDuplicateJSONKeys(manifestJSON); err != nil {
		return Manifest{}, invalid("manifest JSON is malformed or ambiguous")
	}
	decoder := json.NewDecoder(bytes.NewReader(manifestJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, invalid("manifest JSON is malformed")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Manifest{}, invalid("manifest JSON has trailing data")
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	publicKey, ok := trustedKeys[manifest.KeyID]
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return Manifest{}, ErrUnknownSigningKey
	}
	encodedSignature := strings.TrimSpace(signatureBase64)
	if len(encodedSignature) != base64.StdEncoding.EncodedLen(ed25519.SignatureSize) {
		return Manifest{}, ErrInvalidSignature
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(encodedSignature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, manifestJSON, signature) {
		return Manifest{}, ErrInvalidSignature
	}
	return manifest, nil
}

// CheckCompatibility compares an already validated candidate against explicit
// host protocol/schema capabilities. It makes no SemVer compatibility guesses.
func CheckCompatibility(manifest Manifest, host HostCompatibility) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if !validTarget(host.Platform, host.Architecture) ||
		host.Platform != manifest.Platform || host.Architecture != manifest.Architecture {
		return incompatible("platform or architecture does not match")
	}
	if host.RuntimeProtocolVersion < manifest.MinimumHostProtocol || host.RuntimeProtocolVersion > manifest.MaximumHostProtocol {
		return incompatible("runtime host protocol is outside the release range")
	}
	if !host.ControlProtocolRange.contains(manifest.ControlProtocolVersion) || !host.EngineProtocolRange.contains(manifest.EngineProtocolVersion) {
		return incompatible("control or engine protocol is unsupported")
	}
	if !rangesOverlap(host.AdapterProtocolRange, ProtocolRange{Minimum: manifest.AdapterProtocolMinimum, Maximum: manifest.AdapterProtocolMaximum}) {
		return incompatible("adapter protocol ranges do not overlap")
	}
	if !rangeContains(manifest.ArchiveReadMinimum, manifest.ArchiveReadMaximum, host.ArchiveWriteFormat) ||
		!host.ArchiveReadRange.contains(manifest.ArchiveWriteFormat) {
		return incompatible("archive write format is not mutually readable")
	}
	if host.ManagementSchemaVersion < manifest.ManagementSchemaMinimum || host.ManagementSchemaVersion > manifest.ManagementSchemaMaximum {
		return incompatible("management schema is outside the release range")
	}
	return nil
}

// VerifyArtifactFile verifies a staged artifact without following symlinks.
// Callers must choose the private staging path; this helper never extracts or
// executes files and never includes the path in returned error text.
func VerifyArtifactFile(path string, artifact Artifact) error {
	if _, ok := requiredRole(artifact.Role); !ok || !safeFilename(artifact.Filename) || artifact.Size < 1 || artifact.Size > MaxArtifactBytes || len(artifact.SHA256) != sha256.Size*2 {
		return ErrInvalidArtifact
	}
	expectedHash, err := hex.DecodeString(artifact.SHA256)
	if err != nil || artifact.SHA256 != strings.ToLower(artifact.SHA256) {
		return ErrInvalidArtifact
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() != artifact.Size {
		return ErrArtifactFile
	}
	file, err := openRegularNoFollow(path)
	if err != nil {
		return ErrArtifactFile
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != artifact.Size {
		return ErrArtifactFile
	}
	hasher := sha256.New()
	count, err := io.CopyN(hasher, file, MaxArtifactBytes+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return ErrArtifactFile
	}
	if count != artifact.Size || count > MaxArtifactBytes || !bytes.Equal(hasher.Sum(nil), expectedHash) {
		return ErrArtifactFile
	}
	return nil
}

func invalid(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidManifest, reason)
}

func incompatible(reason string) error {
	return fmt.Errorf("%w: %s", ErrIncompatible, reason)
}

func validRange(minimum, maximum int) bool { return minimum >= 1 && maximum >= minimum }

func validTarget(platform, architecture string) bool {
	if !platformPattern.MatchString(platform) || !platformPattern.MatchString(architecture) {
		return false
	}
	_, platformOK := goPlatforms[platform]
	_, architectureOK := goArchitectures[architecture]
	return platformOK && architectureOK
}

func requiredRole(role string) (struct{}, bool) {
	for _, candidate := range requiredRoles {
		if role == candidate {
			return struct{}{}, true
		}
	}
	return struct{}{}, false
}

func safeFilename(name string) bool {
	if !namePattern.MatchString(name) || name == "." || name == ".." || strings.Contains(name, "..") || strings.ContainsAny(name, `/\\:`) || strings.HasSuffix(name, ".") {
		return false
	}
	device := strings.ToUpper(strings.SplitN(name, ".", 2)[0])
	if device == "CON" || device == "PRN" || device == "AUX" || device == "NUL" ||
		(len(device) == 4 && (strings.HasPrefix(device, "COM") || strings.HasPrefix(device, "LPT")) && device[3] >= '1' && device[3] <= '9') {
		return false
	}
	return true
}

func (r ProtocolRange) contains(version int) bool {
	return r.Minimum >= 1 && r.Maximum >= r.Minimum && version >= r.Minimum && version <= r.Maximum
}

func rangeContains(minimum, maximum, version int) bool {
	return version >= minimum && version <= maximum
}

func rangesOverlap(a, b ProtocolRange) bool {
	return a.Minimum >= 1 && a.Maximum >= a.Minimum && b.Minimum >= 1 && b.Maximum >= b.Minimum && a.Minimum <= b.Maximum && b.Minimum <= a.Maximum
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// rejectDuplicateJSONKeys makes signed JSON unambiguous before typed decoding.
func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	var trailing json.Token
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		trailing = token
		_ = trailing
		return errors.New("trailing JSON value")
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return errors.New("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("unterminated object")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("unterminated array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}
