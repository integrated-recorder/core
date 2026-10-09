// Package source provides bounded release discovery and download sources for
// the runtime installer. GitHub metadata only locates signed assets; trust is
// established later by install.Installer using the release manifest signature.
package source

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

const (
	officialOwner      = "integrated-recorder"
	officialRepository = "core"
	officialAPIBase    = "https://api.github.com"
	requestTimeout     = 30 * time.Second
	maxAPIResponse     = 2 << 20
	maxReleaseAssets   = 256
	maxManifestFiles   = 16
	maxRedirects       = 5
)

type Channel string

const (
	Stable     Channel = "stable"
	Prerelease Channel = "prerelease"
)

var (
	ErrInvalidConfig     = errors.New("invalid GitHub release source configuration")
	ErrInvalidChannel    = errors.New("unsupported release channel")
	ErrSourceUnavailable = errors.New("release source unavailable")
	ErrReleaseNotFound   = errors.New("release not found")
	ErrInvalidRelease    = errors.New("invalid release source metadata")
	ErrUnsafeRedirect    = errors.New("release source redirect rejected")
	ErrArtifactNotListed = errors.New("release artifact is not listed")
	ErrResponseTooLarge  = errors.New("release source response exceeds size limit")
)

type GitHubReleaseSource struct {
	channel      Channel
	platform     string
	architecture string
	apiBase      *url.URL
	client       *http.Client
	allowedHost  map[string]struct{}
	allowedHTTP  bool

	mu       sync.Mutex
	snapshot *releaseSnapshot
}

type releaseSnapshot struct {
	manifest  []byte
	signature string
	notes     string
	assets    map[string]assetRef
}

type assetRef struct {
	id   int64
	size int64
}

// NewGitHubReleaseSource creates a source for official Integrated Recorder
// releases targeting the current Go runtime platform. The source discovers
// assets through GitHub's API, but does not treat API metadata as a trust root.
// The installer must verify the selected platform manifest's signature.
func NewGitHubReleaseSource(channel Channel) (*GitHubReleaseSource, error) {
	return NewGitHubReleaseSourceForTarget(channel, runtime.GOOS, runtime.GOARCH)
}

// NewGitHubReleaseSourceForTarget creates a source for an explicit target.
// Release metadata remains untrusted until the installer verifies its signature.
func NewGitHubReleaseSourceForTarget(channel Channel, platform, architecture string) (*GitHubReleaseSource, error) {
	if channel != Stable && channel != Prerelease {
		return nil, ErrInvalidChannel
	}
	if !validReleaseTarget(platform, architecture) {
		return nil, ErrInvalidConfig
	}
	return newGitHubReleaseSource(channel, platform, architecture)
}

func newGitHubReleaseSource(channel Channel, platform, architecture string) (*GitHubReleaseSource, error) {
	base, _ := url.Parse(officialAPIBase)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
	}
	client := &http.Client{Transport: transport, Timeout: requestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	allowed := map[string]struct{}{
		"api.github.com":                        {},
		"github.com":                            {},
		"release-assets.githubusercontent.com":  {},
		"objects.githubusercontent.com":         {},
		"github-releases.githubusercontent.com": {},
	}
	return &GitHubReleaseSource{channel: channel, platform: platform, architecture: architecture, apiBase: base, client: client, allowedHost: allowed}, nil
}

// newTestSource is intentionally package-private: non-test callers cannot
// replace the official API endpoint or host allow-list.
func newTestSource(channel Channel, client *http.Client, base string) (*GitHubReleaseSource, error) {
	return newTestSourceForTarget(channel, client, base, "linux", "amd64")
}

func newTestSourceForTarget(channel Channel, client *http.Client, base, platform, architecture string) (*GitHubReleaseSource, error) {
	if channel != Stable && channel != Prerelease || client == nil {
		return nil, ErrInvalidConfig
	}
	if !validReleaseTarget(platform, architecture) {
		return nil, ErrInvalidConfig
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, ErrInvalidConfig
	}
	clone := *client
	if clone.Timeout <= 0 || clone.Timeout > requestTimeout {
		clone.Timeout = requestTimeout
	}
	clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &GitHubReleaseSource{
		channel:      channel,
		platform:     platform,
		architecture: architecture,
		apiBase:      parsed,
		client:       &clone,
		allowedHost:  map[string]struct{}{strings.ToLower(parsed.Host): {}},
		allowedHTTP:  parsed.Scheme == "http",
	}, nil
}

// Manifest implements install.Source. The result is copied from a per-source
// immutable cache so repeated calls do not refetch release metadata.
func (s *GitHubReleaseSource) Manifest(ctx context.Context) ([]byte, string, error) {
	if s == nil || ctx == nil {
		return nil, "", ErrInvalidConfig
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if s.snapshot == nil {
		snapshot, err := s.loadSnapshot(ctx)
		if err != nil {
			return nil, "", err
		}
		s.snapshot = snapshot
	}
	return append([]byte(nil), s.snapshot.manifest...), s.snapshot.signature, nil
}

// ReleaseNotesSummary returns a bounded, one-line plain-text excerpt from the
// untrusted GitHub release body. Callers must render it as text, never HTML.
func (s *GitHubReleaseSource) ReleaseNotesSummary() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		return ""
	}
	return s.snapshot.notes
}

// OpenArtifact implements install.Source. Only safe names present in the
// signed manifest's bounded artifact table can be mapped to GitHub asset IDs.
func (s *GitHubReleaseSource) OpenArtifact(ctx context.Context, filename string) (io.ReadCloser, error) {
	if s == nil || ctx == nil || !safeFilename(filename) {
		return nil, ErrArtifactNotListed
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.snapshot == nil {
		s.mu.Unlock()
		return nil, ErrArtifactNotListed
	}
	ref, ok := s.snapshot.assets[filename]
	s.mu.Unlock()
	if !ok {
		return nil, ErrArtifactNotListed
	}
	requestURL := s.assetURL(ref.id)
	response, err := s.get(ctx, requestURL, ref.size, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	if response.ContentLength >= 0 && response.ContentLength != ref.size {
		_ = response.Body.Close()
		return nil, ErrInvalidRelease
	}
	return &boundedBody{body: response.Body, max: ref.size}, nil
}

func (s *GitHubReleaseSource) loadSnapshot(ctx context.Context) (*releaseSnapshot, error) {
	releaseInfo, err := s.discover(ctx)
	if err != nil {
		return nil, err
	}
	assets := make(map[string]assetRef, len(releaseInfo.Assets))
	for _, asset := range releaseInfo.Assets {
		if asset.ID < 1 || asset.Name == "" || asset.State != "uploaded" || asset.Size < 1 {
			return nil, ErrInvalidRelease
		}
		key := strings.ToLower(asset.Name)
		if _, exists := assets[key]; exists {
			return nil, ErrInvalidRelease
		}
		assets[key] = assetRef{id: asset.ID, size: asset.Size}
	}
	manifestName := "release-" + s.platform + "-" + s.architecture + ".json"
	signatureName := manifestName + ".sig"
	manifestAsset, ok := assets[strings.ToLower(manifestName)]
	if !ok {
		return nil, ErrReleaseNotFound
	}
	if manifestAsset.size > release.MaxManifestBytes {
		return nil, ErrResponseTooLarge
	}
	signatureAsset, ok := assets[strings.ToLower(signatureName)]
	if !ok {
		return nil, ErrReleaseNotFound
	}
	if signatureAsset.size > 1024 {
		return nil, ErrResponseTooLarge
	}
	manifest, err := s.fetchAsset(ctx, manifestAsset, release.MaxManifestBytes)
	if err != nil {
		return nil, err
	}
	signature, err := s.fetchAsset(ctx, signatureAsset, 1024)
	if err != nil {
		return nil, err
	}
	if len(manifest) == 0 || len(signature) == 0 {
		return nil, ErrInvalidRelease
	}
	entries, channel, version, platform, architecture, err := manifestArtifactTable(manifest)
	if err != nil || channel != string(s.channel) || !sameVersion(version, releaseInfo.TagName) || platform != s.platform || architecture != s.architecture {
		return nil, ErrInvalidRelease
	}
	artifactRefs := make(map[string]assetRef, len(entries))
	for _, entry := range entries {
		ref, exists := assets[strings.ToLower(entry.Filename)]
		if !exists || ref.size != entry.Size || ref.size > release.MaxArtifactBytes {
			return nil, ErrInvalidRelease
		}
		artifactRefs[entry.Filename] = ref
	}
	return &releaseSnapshot{manifest: manifest, signature: string(signature), notes: boundedNotesSummary(releaseInfo.Body), assets: artifactRefs}, nil
}

const maxReleaseNotesSummaryBytes = 512

func boundedNotesSummary(body string) string {
	if !utf8.ValidString(body) {
		return ""
	}
	var clean strings.Builder
	clean.Grow(min(len(body), maxReleaseNotesSummaryBytes))
	spacePending := false
	for _, r := range body {
		if unicode.IsControl(r) {
			if unicode.IsSpace(r) {
				spacePending = clean.Len() > 0
			}
			continue
		}
		if unicode.IsSpace(r) {
			spacePending = clean.Len() > 0
			continue
		}
		if spacePending {
			if clean.Len()+1 > maxReleaseNotesSummaryBytes {
				break
			}
			clean.WriteByte(' ')
			spacePending = false
		}
		if clean.Len()+utf8.RuneLen(r) > maxReleaseNotesSummaryBytes {
			break
		}
		clean.WriteRune(r)
	}
	return clean.String()
}

func (s *GitHubReleaseSource) discover(ctx context.Context) (githubRelease, error) {
	endpoint := "releases/latest"
	requestURL := *s.apiBase
	requestURL.Path = path.Join(s.apiBase.Path, "repos", officialOwner, officialRepository, endpoint)
	// path.Join would interpret the query suffix as path text; set it separately.
	if s.channel == Prerelease {
		requestURL.Path = path.Join(s.apiBase.Path, "repos", officialOwner, officialRepository, "releases")
		requestURL.RawQuery = "per_page=100"
	}
	response, err := s.get(ctx, &requestURL, maxAPIResponse, "application/vnd.github+json")
	if err != nil {
		return githubRelease{}, err
	}
	defer response.Body.Close()
	data, err := readBounded(response.Body, maxAPIResponse)
	if err != nil {
		return githubRelease{}, err
	}
	var selected githubRelease
	if s.channel == Stable {
		if err := json.Unmarshal(data, &selected); err != nil || selected.Prerelease || selected.Draft {
			return githubRelease{}, ErrReleaseNotFound
		}
	} else {
		var releases []githubRelease
		if err := json.Unmarshal(data, &releases); err != nil || len(releases) > maxReleaseAssets {
			return githubRelease{}, ErrInvalidRelease
		}
		found := false
		for _, candidate := range releases {
			if candidate.Prerelease && !candidate.Draft {
				selected = candidate
				found = true
				break
			}
		}
		if !found {
			return githubRelease{}, ErrReleaseNotFound
		}
	}
	if selected.Draft || (selected.Prerelease != (s.channel == Prerelease)) || selected.TagName == "" || len(selected.Assets) > maxReleaseAssets {
		return githubRelease{}, ErrInvalidRelease
	}
	return selected, nil
}

func (s *GitHubReleaseSource) fetchAsset(ctx context.Context, asset assetRef, limit int64) ([]byte, error) {
	if asset.size < 1 || asset.size > limit {
		return nil, ErrResponseTooLarge
	}
	response, err := s.get(ctx, s.assetURL(asset.id), asset.size, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.ContentLength >= 0 && response.ContentLength != asset.size {
		return nil, ErrInvalidRelease
	}
	data, err := readBounded(response.Body, asset.size)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != asset.size {
		return nil, ErrInvalidRelease
	}
	return data, nil
}

func (s *GitHubReleaseSource) assetURL(id int64) *url.URL {
	u := *s.apiBase
	u.Path = path.Join(s.apiBase.Path, "repos", officialOwner, officialRepository, "releases", "assets", strconv.FormatInt(id, 10))
	u.RawQuery = ""
	return &u
}

func (s *GitHubReleaseSource) get(ctx context.Context, initial *url.URL, maxBytes int64, accept string) (*http.Response, error) {
	current := initial
	for redirects := 0; ; redirects++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := s.validateURL(current); err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, current.String(), nil)
		if err != nil {
			return nil, ErrSourceUnavailable
		}
		request.Header.Set("Accept", accept)
		request.Header.Set("User-Agent", "Integrated-Recorder-Runtime-Host")
		if accept == "application/vnd.github+json" {
			request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		}
		response, err := s.client.Do(request)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrSourceUnavailable
		}
		if response.StatusCode >= 300 && response.StatusCode < 400 {
			location := response.Header.Get("Location")
			_ = response.Body.Close()
			if redirects >= maxRedirects || location == "" {
				return nil, ErrUnsafeRedirect
			}
			next, parseErr := current.Parse(location)
			if parseErr != nil || s.validateURL(next) != nil {
				return nil, ErrUnsafeRedirect
			}
			current = next
			continue
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			return nil, ErrSourceUnavailable
		}
		if response.ContentLength > maxBytes {
			_ = response.Body.Close()
			return nil, ErrResponseTooLarge
		}
		return response, nil
	}
}

func (s *GitHubReleaseSource) validateURL(u *url.URL) error {
	if u == nil || u.User != nil || u.Fragment != "" {
		return ErrUnsafeRedirect
	}
	if u.Scheme != "https" && !(s.allowedHTTP && u.Scheme == "http") {
		return ErrUnsafeRedirect
	}
	host := strings.ToLower(u.Host)
	if _, ok := s.allowedHost[host]; !ok {
		// Production allow-list entries are hostnames while URL.Host may include
		// an explicit default port. Only 443 is accepted for HTTPS production.
		if port := u.Port(); port != "443" || !strings.EqualFold(u.Hostname(), strings.TrimSuffix(host, ":443")) {
			return ErrUnsafeRedirect
		}
		if _, ok := s.allowedHost[strings.ToLower(u.Hostname())]; !ok {
			return ErrUnsafeRedirect
		}
	}
	if u.Port() != "" && !(u.Scheme == "https" && u.Port() == "443") && !(s.allowedHTTP && u.Host == strings.TrimPrefix(strings.TrimPrefix(s.apiBase.Host, "http://"), "https://")) {
		// Test servers use an ephemeral port. It is allowed only on the exact
		// injected authority and cannot be used by production instances.
		return ErrUnsafeRedirect
	}
	return nil
}

type githubRelease struct {
	TagName    string        `json:"tag_name"`
	Body       string        `json:"body"`
	Prerelease bool          `json:"prerelease"`
	Draft      bool          `json:"draft"`
	Assets     []githubAsset `json:"assets"`
}

type githubAsset struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	State string `json:"state"`
}

type manifestHeader struct {
	ReleaseVersion string `json:"release_version"`
	Channel        string `json:"channel"`
	Platform       string `json:"platform"`
	Architecture   string `json:"architecture"`
	Artifacts      []struct {
		Filename string `json:"filename"`
		Size     int64  `json:"size"`
	} `json:"artifacts"`
}

type manifestFile struct {
	Filename string
	Size     int64
}

func manifestArtifactTable(data []byte) ([]manifestFile, string, string, string, string, error) {
	if len(data) == 0 || len(data) > release.MaxManifestBytes {
		return nil, "", "", "", "", ErrInvalidRelease
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var header manifestHeader
	if err := decoder.Decode(&header); err != nil {
		return nil, "", "", "", "", ErrInvalidRelease
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, "", "", "", "", ErrInvalidRelease
	}
	if header.ReleaseVersion == "" || (header.Channel != string(Stable) && header.Channel != string(Prerelease)) || !validReleaseTarget(header.Platform, header.Architecture) || len(header.Artifacts) == 0 || len(header.Artifacts) > maxManifestFiles {
		return nil, "", "", "", "", ErrInvalidRelease
	}
	seen := make(map[string]struct{}, len(header.Artifacts))
	files := make([]manifestFile, 0, len(header.Artifacts))
	for _, artifact := range header.Artifacts {
		if !safeFilename(artifact.Filename) || artifact.Size < 1 || artifact.Size > release.MaxArtifactBytes {
			return nil, "", "", "", "", ErrInvalidRelease
		}
		key := strings.ToLower(artifact.Filename)
		if _, exists := seen[key]; exists {
			return nil, "", "", "", "", ErrInvalidRelease
		}
		seen[key] = struct{}{}
		files = append(files, manifestFile{Filename: artifact.Filename, Size: artifact.Size})
	}
	return files, header.Channel, header.ReleaseVersion, header.Platform, header.Architecture, nil
}

// validReleaseTarget delegates identifier validation to the manifest contract
// without duplicating its supported GOOS/GOARCH allow-list in this package.
func validReleaseTarget(platform, architecture string) bool {
	manifest := release.Manifest{
		ManifestSchemaVersion: release.ManifestSchemaVersion,
		ReleaseVersion:        "1.0.0", Commit: strings.Repeat("0", 40),
		BuildTime: "2000-01-01T00:00:00Z", Channel: string(Stable), KeyID: "target-check",
		MinimumHostProtocol: 1, MaximumHostProtocol: 1, ControlProtocolVersion: 1, EngineProtocolVersion: 1,
		AdapterProtocolMinimum: 1, AdapterProtocolMaximum: 1,
		ArchiveReadMinimum: 2, ArchiveReadMaximum: 2, ArchiveWriteFormat: 2,
		ManagementSchemaMinimum: 1, ManagementSchemaMaximum: 1,
		Platform: platform, Architecture: architecture,
		Artifacts: []release.Artifact{
			{Role: release.RoleRuntimeHost, Filename: "runtime-host", Size: 1, SHA256: strings.Repeat("0", 64)},
			{Role: release.RoleControlPlane, Filename: "control-plane", Size: 1, SHA256: strings.Repeat("0", 64)},
			{Role: release.RoleRecorderEngine, Filename: "recorder-engine", Size: 1, SHA256: strings.Repeat("0", 64)},
			{Role: release.RoleAdapterRuntime, Filename: "adapter-runtime", Size: 1, SHA256: strings.Repeat("0", 64)},
		},
	}
	return manifest.Validate() == nil
}

func sameVersion(version, tag string) bool {
	return strings.TrimPrefix(version, "v") == strings.TrimPrefix(tag, "v")
}

func safeFilename(name string) bool {
	if name == "" || len(name) > 128 || name == "." || name == ".." || strings.Contains(name, "..") || strings.ContainsAny(name, `/\\:`) {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, ErrSourceUnavailable
	}
	if int64(len(data)) > limit {
		return nil, ErrResponseTooLarge
	}
	return data, nil
}

type boundedBody struct {
	body io.ReadCloser
	max  int64
	read int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.read >= b.max {
		var extra [1]byte
		n, err := b.body.Read(extra[:])
		if n > 0 {
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > b.max-b.read {
		p = p[:b.max-b.read]
	}
	n, err := b.body.Read(p)
	b.read += int64(n)
	return n, err
}

func (b *boundedBody) Close() error { return b.body.Close() }
