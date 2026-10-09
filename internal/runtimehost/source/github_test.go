package source

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

func TestGitHubSourceDiscoversStableAndPrerelease(t *testing.T) {
	for _, channel := range []Channel{Stable, Prerelease} {
		t.Run(string(channel), func(t *testing.T) {
			fixture := newReleaseFixture(t, channel)
			var discoveryPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/releases/latest") || strings.HasSuffix(r.URL.Path, "/releases") {
					discoveryPath = r.URL.RequestURI()
					fixture.writeDiscovery(w, channel)
					return
				}
				fixture.serveAsset(w, r)
			}))
			defer server.Close()
			source := fixture.source(t, channel, server)
			if _, _, err := source.Manifest(context.Background()); err != nil {
				t.Fatalf("Manifest(): %v", err)
			}
			if got := source.ReleaseNotesSummary(); got != "Release notes for 1.2.3" {
				t.Fatalf("release notes summary = %q", got)
			}
			if channel == Stable && !strings.HasSuffix(discoveryPath, "/releases/latest") {
				t.Fatalf("stable discovery request = %q", discoveryPath)
			}
			if channel == Prerelease && (!strings.HasSuffix(discoveryPath, "/releases?per_page=100") || !strings.Contains(discoveryPath, "per_page=100")) {
				t.Fatalf("prerelease discovery request = %q", discoveryPath)
			}
		})
	}
}

func TestBoundedNotesSummaryIsPlainTextAndBounded(t *testing.T) {
	got := boundedNotesSummary("  first\n\tsecond\x00 <b>third</b>  ")
	if got != "first second <b>third</b>" {
		t.Fatalf("summary = %q", got)
	}
	long := strings.Repeat("한", maxReleaseNotesSummaryBytes)
	got = boundedNotesSummary(long)
	if len(got) > maxReleaseNotesSummaryBytes || !utf8.ValidString(got) {
		t.Fatalf("summary is not bounded valid UTF-8: bytes=%d", len(got))
	}
}

func TestGitHubSourceSelectsMatchingArchitectureManifestAndAssets(t *testing.T) {
	amd64 := newReleaseFixtureForTarget(t, Stable, "amd64")
	arm64 := newReleaseFixtureForTarget(t, Stable, "arm64")
	assets := append([]githubAsset(nil), amd64.metadata...)
	for _, asset := range arm64.metadata {
		remapped := asset
		remapped.ID += 100
		assets = append(assets, remapped)
		arm64.assets[remapped.ID] = arm64.assets[asset.ID]
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			writeRelease(w, githubRelease{TagName: "v1.2.3", Assets: assets})
			return
		}
		fixture := amd64
		id, err := strconvParseInt(path.Base(r.URL.Path))
		if err == nil && id >= 100 {
			fixture = arm64
		}
		fixture.serveAsset(w, r)
	}))
	defer server.Close()

	for _, architecture := range []string{"amd64", "arm64"} {
		t.Run(architecture, func(t *testing.T) {
			source, err := newTestSourceForTarget(Stable, server.Client(), server.URL, "linux", architecture)
			if err != nil {
				t.Fatal(err)
			}
			manifestBytes, _, err := source.Manifest(context.Background())
			if err != nil {
				t.Fatalf("Manifest(): %v", err)
			}
			var manifest release.Manifest
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.Architecture != architecture {
				t.Fatalf("selected manifest architecture = %q, want %q", manifest.Architecture, architecture)
			}
			body, err := source.OpenArtifact(context.Background(), manifest.Artifacts[0].Filename)
			if err != nil {
				t.Fatalf("OpenArtifact(): %v", err)
			}
			data, err := io.ReadAll(body)
			_ = body.Close()
			if err != nil {
				t.Fatalf("Read artifact: %v", err)
			}
			fixture := amd64
			if architecture == "arm64" {
				fixture = arm64
			}
			if want := fixture.assets[3]; !bytes.Equal(data, want) {
				t.Fatalf("artifact came from wrong target: got %q, want %q", data, want)
			}
		})
	}
}

func TestGitHubSourceRejectsMissingTargetManifest(t *testing.T) {
	fixture := newReleaseFixtureForTarget(t, Stable, "amd64")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			fixture.writeDiscovery(w, Stable)
			return
		}
		fixture.serveAsset(w, r)
	}))
	defer server.Close()
	source, err := newTestSourceForTarget(Stable, server.Client(), server.URL, "linux", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Manifest(context.Background()); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("Manifest() error = %v, want ErrReleaseNotFound", err)
	}
}

func TestGitHubSourceExplicitTargetValidation(t *testing.T) {
	if _, err := NewGitHubReleaseSourceForTarget(Stable, "linux", "x86_64"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported architecture error = %v, want ErrInvalidConfig", err)
	}
	if _, err := NewGitHubReleaseSourceForTarget(Channel("nightly"), "linux", "amd64"); !errors.Is(err, ErrInvalidChannel) {
		t.Fatalf("unsupported channel error = %v, want ErrInvalidChannel", err)
	}
}

func TestGitHubSourceRejectsMissingAndDuplicateReleaseAssets(t *testing.T) {
	tests := []struct {
		name   string
		assets []githubAsset
		want   error
	}{
		{name: "missing manifest", assets: []githubAsset{{ID: 2, Name: "release-linux-amd64.json.sig", Size: 1, State: "uploaded"}}, want: ErrReleaseNotFound},
		{name: "missing signature", assets: []githubAsset{{ID: 1, Name: "release-linux-amd64.json", Size: 1, State: "uploaded"}}, want: ErrReleaseNotFound},
		{name: "duplicate manifest", assets: []githubAsset{{ID: 1, Name: "release-linux-amd64.json", Size: 1, State: "uploaded"}, {ID: 9, Name: "RELEASE-LINUX-AMD64.JSON", Size: 1, State: "uploaded"}}, want: ErrInvalidRelease},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeRelease(w, githubRelease{TagName: "v1.2.3", Assets: test.assets})
			}))
			defer server.Close()
			source, err := newTestSource(Stable, server.Client(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := source.Manifest(context.Background()); !errors.Is(err, test.want) {
				t.Fatalf("Manifest() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestGitHubSourceRejectsTraversalAndMalformedArtifactList(t *testing.T) {
	tests := []struct {
		name     string
		manifest []byte
	}{
		{name: "traversal", manifest: []byte(`{"release_version":"1.2.3","channel":"stable","artifacts":[{"filename":"../escape","size":1}]}`)},
		{name: "malformed artifact table", manifest: []byte(`{"release_version":"1.2.3","channel":"stable","artifacts":{}}`)},
		{name: "duplicate artifact filenames", manifest: []byte(`{"release_version":"1.2.3","channel":"stable","artifacts":[{"filename":"a.bin","size":1},{"filename":"A.BIN","size":1}]}`)},
		{name: "trailing JSON", manifest: []byte(`{"release_version":"1.2.3","channel":"stable","artifacts":[{"filename":"a.bin","size":1}]} {}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assets := []githubAsset{{ID: 1, Name: "release-linux-amd64.json", Size: int64(len(test.manifest)), State: "uploaded"}, {ID: 2, Name: "release-linux-amd64.json.sig", Size: 1, State: "uploaded"}, {ID: 3, Name: "a.bin", Size: 1, State: "uploaded"}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/releases/latest"):
					writeRelease(w, githubRelease{TagName: "v1.2.3", Assets: assets})
				case strings.HasSuffix(r.URL.Path, "/assets/1"):
					w.Write(test.manifest)
				case strings.HasSuffix(r.URL.Path, "/assets/2"):
					w.Write([]byte("x"))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			source, err := newTestSource(Stable, server.Client(), server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := source.Manifest(context.Background()); !errors.Is(err, ErrInvalidRelease) {
				t.Fatalf("Manifest() error = %v, want ErrInvalidRelease", err)
			}
		})
	}
}

func TestGitHubSourceBoundsResponsesAndArtifactMetadata(t *testing.T) {
	t.Run("oversized API body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprint(maxAPIResponse+1))
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		source, err := newTestSource(Stable, server.Client(), server.URL)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := source.Manifest(context.Background()); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("Manifest() error = %v, want ErrResponseTooLarge", err)
		}
	})

	t.Run("oversized asset metadata", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeRelease(w, githubRelease{TagName: "v1.2.3", Assets: []githubAsset{{ID: 1, Name: "release-linux-amd64.json", Size: release.MaxManifestBytes + 1, State: "uploaded"}}})
		}))
		defer server.Close()
		source, err := newTestSource(Stable, server.Client(), server.URL)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := source.Manifest(context.Background()); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("Manifest() error = %v, want ErrResponseTooLarge", err)
		}
	})

	t.Run("artifact body exceeds signed size", func(t *testing.T) {
		fixture := newReleaseFixture(t, Stable)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/releases/latest") {
				fixture.writeDiscovery(w, Stable)
				return
			}
			id, err := strconvParseInt(path.Base(r.URL.Path))
			if err != nil {
				http.NotFound(w, r)
				return
			}
			data := fixture.assets[id]
			if id == 3 {
				data = append(append([]byte(nil), data...), 0)
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			w.Write(data)
		}))
		defer server.Close()
		source := fixture.source(t, Stable, server)
		if _, _, err := source.Manifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		body, err := source.OpenArtifact(context.Background(), fixture.manifest.Artifacts[0].Filename)
		if err != nil {
			t.Fatal(err)
		}
		defer body.Close()
		if _, err := io.Copy(io.Discard, body); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("artifact read error = %v, want ErrResponseTooLarge", err)
		}
	})
}

func TestGitHubSourceRejectsUnsafeRedirectHost(t *testing.T) {
	var externalHits int
	external := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { externalHits++; w.Write([]byte("unexpected")) }))
	defer external.Close()
	fixture := newReleaseFixture(t, Stable)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/assets/1") {
			http.Redirect(w, r, external.URL+"/asset", http.StatusFound)
			return
		}
		fixture.writeDiscovery(w, Stable)
	}))
	defer server.Close()
	source := fixture.source(t, Stable, server)
	if _, _, err := source.Manifest(context.Background()); !errors.Is(err, ErrUnsafeRedirect) {
		t.Fatalf("Manifest() error = %v, want ErrUnsafeRedirect", err)
	}
	if externalHits != 0 {
		t.Fatalf("redirect host received %d requests", externalHits)
	}
}

func TestGitHubSourceHonorsCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	source, err := newTestSource(Stable, server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := source.Manifest(ctx); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Manifest() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Manifest() did not return after cancellation")
	}
}

func TestGitHubSourceFeedsSignedLocalFixtureToInstaller(t *testing.T) {
	fixture := newReleaseFixture(t, Stable)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			fixture.writeDiscovery(w, Stable)
			return
		}
		if strings.Contains(r.URL.Path, "/releases/assets/") {
			http.Redirect(w, r, "/fixture-content/"+path.Base(r.URL.Path), http.StatusFound)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/fixture-content/") {
			fixture.serveAsset(w, r)
			return
		}
		fixture.serveAsset(w, r)
	}))
	defer server.Close()
	source := fixture.source(t, Stable, server)
	compatibility := release.HostCompatibility{
		Platform: "linux", Architecture: "amd64", RuntimeProtocolVersion: 1,
		ControlProtocolRange: release.ProtocolRange{Minimum: 1, Maximum: 2},
		EngineProtocolRange:  release.ProtocolRange{Minimum: 1, Maximum: 2},
		AdapterProtocolRange: release.ProtocolRange{Minimum: 1, Maximum: 2},
		ArchiveReadRange:     release.ProtocolRange{Minimum: 2, Maximum: 2},
		ArchiveWriteFormat:   2, ManagementSchemaVersion: 1,
	}
	runtimeRoot := filepath.Join(t.TempDir(), "runtime")
	t.Cleanup(func() { cleanupRuntimeTree(runtimeRoot) })
	installer, err := install.NewInstaller(runtimeRoot, map[string]ed25519.PublicKey{"fixture-key": fixture.public}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := installer.Stage(context.Background(), source)
	if err != nil {
		t.Fatalf("Stage(signed GitHub fixture): %v", err)
	}
	if !staged.Verified || staged.Version != "1.2.3" || staged.ArtifactCount != 4 {
		t.Fatalf("unexpected staged release: %+v", staged)
	}
}

func TestProductionSourceRedirectAllowlistRejectsPrivateAndLookalikeHosts(t *testing.T) {
	source, err := NewGitHubReleaseSource(Stable)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"https://api.github.com/repos/integrated-recorder/core/releases/latest",
		"https://release-assets.githubusercontent.com/path?token=opaque",
		"https://github.com/path",
	} {
		target, err := url.Parse(raw)
		if err != nil || source.validateURL(target) != nil {
			t.Fatalf("official URL %q rejected: %v", raw, source.validateURL(target))
		}
	}
	for _, raw := range []string{
		"http://api.github.com/path",
		"https://release-assets.githubusercontent.com.evil.test/path",
		"https://127.0.0.1/path",
		"https://github.com:444/path",
	} {
		target, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := source.validateURL(target); !errors.Is(err, ErrUnsafeRedirect) {
			t.Fatalf("unsafe URL %q accepted: %v", raw, err)
		}
	}
}

func cleanupRuntimeTree(root string) {
	var directories []string
	_ = filepath.Walk(root, func(current string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() {
			directories = append(directories, current)
		} else {
			_ = os.Chmod(current, 0600)
		}
		return nil
	})
	for _, directory := range directories {
		_ = os.Chmod(directory, 0700)
	}
	_ = os.RemoveAll(root)
}

func TestGitHubSourceArtifactLookupRequiresCachedManifestAndSafeName(t *testing.T) {
	source, err := newTestSource(Stable, http.DefaultClient, "https://api.github.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.OpenArtifact(context.Background(), "../release"); !errors.Is(err, ErrArtifactNotListed) {
		t.Fatalf("traversal lookup error = %v", err)
	}
	if _, err := source.OpenArtifact(context.Background(), "runtime-host"); !errors.Is(err, ErrArtifactNotListed) {
		t.Fatalf("uncached artifact lookup error = %v", err)
	}
}

type releaseFixture struct {
	manifest  release.Manifest
	manifestB []byte
	signature []byte
	public    ed25519.PublicKey
	private   ed25519.PrivateKey
	assets    map[int64][]byte
	metadata  []githubAsset
	mu        sync.Mutex
}

func newReleaseFixture(t *testing.T, channel Channel) *releaseFixture {
	return newReleaseFixtureForTarget(t, channel, "amd64")
}

func newReleaseFixtureForTarget(t *testing.T, channel Channel, architecture string) *releaseFixture {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f := &releaseFixture{public: public, private: private, assets: make(map[int64][]byte)}
	f.manifest = release.Manifest{
		ManifestSchemaVersion: 1, ReleaseVersion: "1.2.3", Commit: strings.Repeat("a", 40),
		BuildTime: "2026-09-29T12:00:00Z", Channel: string(channel), KeyID: "fixture-key",
		MinimumHostProtocol: 1, MaximumHostProtocol: 2, ControlProtocolVersion: 1, EngineProtocolVersion: 1,
		AdapterProtocolMinimum: 1, AdapterProtocolMaximum: 2, ArchiveReadMinimum: 2, ArchiveReadMaximum: 2,
		ArchiveWriteFormat: 2, ManagementSchemaMinimum: 1, ManagementSchemaMaximum: 2,
		Platform: "linux", Architecture: architecture,
	}
	for i, role := range []string{release.RoleRuntimeHost, release.RoleControlPlane, release.RoleRecorderEngine, release.RoleAdapterRuntime} {
		payload := []byte(fmt.Sprintf("fixture-%s-%s-artifact-%d", architecture, role, i))
		hash := sha256.Sum256(payload)
		name := role + "-linux-" + architecture
		f.manifest.Artifacts = append(f.manifest.Artifacts, release.Artifact{Role: role, Filename: name, Size: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])})
		f.assets[int64(i+3)] = payload
		f.metadata = append(f.metadata, githubAsset{ID: int64(i + 3), Name: name, Size: int64(len(payload)), State: "uploaded"})
	}
	f.manifestB, err = json.Marshal(f.manifest)
	if err != nil {
		t.Fatal(err)
	}
	f.signature = []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(private, f.manifestB)))
	f.assets[1] = f.manifestB
	f.assets[2] = f.signature
	manifestName := "release-linux-" + architecture + ".json"
	f.metadata = append([]githubAsset{{ID: 1, Name: manifestName, Size: int64(len(f.manifestB)), State: "uploaded"}, {ID: 2, Name: manifestName + ".sig", Size: int64(len(f.signature)), State: "uploaded"}}, f.metadata...)
	return f
}

func (f *releaseFixture) source(t *testing.T, channel Channel, server *httptest.Server) *GitHubReleaseSource {
	t.Helper()
	source, err := newTestSource(channel, server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func (f *releaseFixture) writeDiscovery(w http.ResponseWriter, channel Channel) {
	assets := append([]githubAsset(nil), f.metadata...)
	info := githubRelease{TagName: "v" + f.manifest.ReleaseVersion, Body: "Release notes for " + f.manifest.ReleaseVersion, Prerelease: channel == Prerelease, Assets: assets}
	if channel == Prerelease {
		_ = json.NewEncoder(w).Encode([]githubRelease{{TagName: "v0.9.0", Assets: assets}, info})
		return
	}
	writeRelease(w, info)
}

func (f *releaseFixture) serveAsset(w http.ResponseWriter, r *http.Request) {
	base := path.Base(r.URL.Path)
	id, err := strconvParseInt(base)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	f.mu.Lock()
	data, ok := f.assets[id]
	f.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeRelease(w http.ResponseWriter, info githubRelease) {
	w.Header().Set("Content-Type", "application/vnd.github+json")
	_ = json.NewEncoder(w).Encode(info)
}

func strconvParseInt(value string) (int64, error) { return strconv.ParseInt(value, 10, 64) }
