package install

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

func TestStageLocalSignedBundlePublishesPrivateImmutableRelease(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)

	staged, err := installer.Stage(context.Background(), LocalDirectorySource{Directory: fixture.directory})
	if err != nil {
		t.Fatalf("Stage(): %v", err)
	}
	if !staged.Verified || staged.Version != fixture.manifest.ReleaseVersion || staged.Commit != strings.ToLower(fixture.manifest.Commit) || staged.ArtifactCount != 4 {
		t.Fatalf("unexpected staged result: %+v", staged)
	}
	if strings.Contains(staged.ID, string(filepath.Separator)) || strings.Contains(staged.ID, root) {
		t.Fatalf("unsafe or path-bearing release ID: %+v", staged)
	}
	installed := filepath.Join(root, "releases", staged.ID)
	assertMode(t, installed, 0500)
	assertMode(t, filepath.Join(installed, "release.json"), 0400)
	assertMode(t, filepath.Join(installed, "release.json.sig"), 0400)
	for _, artifact := range fixture.manifest.Artifacts {
		assertMode(t, filepath.Join(installed, artifact.Filename), 0500)
		if err := release.VerifyArtifactFile(filepath.Join(installed, artifact.Filename), artifact); err != nil {
			t.Fatalf("installed artifact invalid: %v", err)
		}
	}
	if entries, err := os.ReadDir(filepath.Join(root, "staging")); err != nil || len(entries) != 0 {
		t.Fatalf("staging was not atomically emptied: entries=%v err=%v", entries, err)
	}
	if _, err := installer.Stage(context.Background(), LocalDirectorySource{Directory: fixture.directory}); !errors.Is(err, ErrReleaseExists) {
		t.Fatalf("same release overwrite error = %v, want ErrReleaseExists", err)
	}
}

func TestStageFailsClosedBeforePublication(t *testing.T) {
	tests := []struct {
		name         string
		mutateBundle func(*testing.T, *bundleFixture)
		mutateHost   func(*release.HostCompatibility)
		want         error
		withoutKey   bool
	}{
		{name: "bad signature", mutateBundle: func(t *testing.T, f *bundleFixture) {
			f.signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
		}, want: release.ErrInvalidSignature},
		{name: "hash mismatch", mutateBundle: func(t *testing.T, f *bundleFixture) {
			f.manifest.Artifacts[0].SHA256 = strings.Repeat("0", 64)
			f.resign(t)
		}, want: release.ErrArtifactFile},
		{name: "declared size mismatch", mutateBundle: func(t *testing.T, f *bundleFixture) { f.manifest.Artifacts[0].Size++; f.resign(t) }, want: ErrArtifactWrite},
		{name: "compatibility", mutateHost: func(h *release.HostCompatibility) { h.Platform = "darwin" }, want: release.ErrIncompatible},
		{name: "empty trust root", withoutKey: true, want: release.ErrUnknownSigningKey},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundle(t)
			if test.mutateBundle != nil {
				test.mutateBundle(t, fixture)
			}
			host := fixture.compatibility
			if test.mutateHost != nil {
				test.mutateHost(&host)
			}
			keys := map[string]ed25519.PublicKey{"fixture-key": fixture.publicKey}
			if test.withoutKey {
				keys = map[string]ed25519.PublicKey{}
			}
			root := filepath.Join(t.TempDir(), "runtime")
			installer, err := NewInstaller(root, keys, host)
			if err != nil {
				t.Fatal(err)
			}
			_, err = installer.Stage(context.Background(), fixture)
			if !errors.Is(err, test.want) {
				t.Fatalf("Stage() error = %v, want %v", err, test.want)
			}
			assertNoPublishedReleases(t, root)
		})
	}
}

func TestCompatibilityIsCheckedBeforeArtifactDownload(t *testing.T) {
	fixture := newBundle(t)
	host := fixture.compatibility
	host.Platform = "darwin"
	installer := newInstaller(t, filepath.Join(t.TempDir(), "runtime"), fixture.publicKey, host)
	counted := &countingSource{bundleFixture: fixture}
	if _, err := installer.Stage(context.Background(), counted); !errors.Is(err, release.ErrIncompatible) {
		t.Fatalf("Stage() error = %v, want ErrIncompatible", err)
	}
	if counted.opens != 0 {
		t.Fatalf("compatibility rejected after %d artifact reads, want no downloads", counted.opens)
	}
}

func TestStageRejectsExtraBytesAndCancellationWithoutPublication(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	manifest := fixture.manifest
	data := append([]byte(nil), fixture.payloads[manifest.Artifacts[0].Filename]...)
	fixture.payloads[manifest.Artifacts[0].Filename] = append(data, 1)
	_, err := installer.Stage(context.Background(), fixture)
	if !errors.Is(err, ErrArtifactWrite) {
		t.Fatalf("extra artifact bytes error = %v, want ErrArtifactWrite", err)
	}
	assertNoPublishedReleases(t, root)

	cancelFixture := newBundle(t)
	cancelRoot := filepath.Join(t.TempDir(), "cancel-runtime")
	activeState := filepath.Join(cancelRoot, "state", "active.json")
	if err := os.MkdirAll(filepath.Dir(activeState), 0700); err != nil {
		t.Fatal(err)
	}
	activeBefore := []byte(`{"active":"generation-a"}`)
	if err := os.WriteFile(activeState, activeBefore, 0600); err != nil {
		t.Fatal(err)
	}
	cancelInstaller := newInstaller(t, cancelRoot, cancelFixture.publicKey, cancelFixture.compatibility)
	ctx, cancel := context.WithCancel(context.Background())
	_, err = cancelInstaller.Stage(ctx, cancelDuringCopySource{bundleFixture: cancelFixture, cancel: cancel})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Stage() error = %v, want context.Canceled", err)
	}
	assertNoPublishedReleases(t, cancelInstaller.runtimeRoot)
	staging, readErr := os.ReadDir(filepath.Join(cancelInstaller.runtimeRoot, "staging"))
	if readErr != nil || len(staging) != 0 {
		t.Fatalf("cancelled staging residue: entries=%v err=%v", staging, readErr)
	}
	activeAfter, readErr := os.ReadFile(activeState)
	if readErr != nil || string(activeAfter) != string(activeBefore) {
		t.Fatalf("cancelled staging changed active generation state: bytes=%q err=%v", activeAfter, readErr)
	}
}

func TestLocalSourceRejectsSymlinkedRootAndArtifacts(t *testing.T) {
	fixture := newBundle(t)
	alias := filepath.Join(t.TempDir(), "bundle-link")
	if err := os.Symlink(fixture.directory, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := (LocalDirectorySource{Directory: alias}).Manifest(context.Background()); !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("symlink root error = %v, want ErrInvalidSource", err)
	}
	artifact := fixture.manifest.Artifacts[0].Filename
	path := filepath.Join(fixture.directory, artifact)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, fixture.payloads[artifact], 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := (LocalDirectorySource{Directory: fixture.directory}).OpenArtifact(context.Background(), artifact); !errors.Is(err, ErrSourceRead) {
		t.Fatalf("symlink artifact error = %v, want ErrSourceRead", err)
	}
}

func TestStageRejectsUnsafeRuntimeRootAndExistingTargetCollision(t *testing.T) {
	fixture := newBundle(t)
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real")
	if err := os.Mkdir(realRoot, 0700); err != nil {
		t.Fatal(err)
	}
	rootLink := filepath.Join(parent, "runtime-link")
	if err := os.Symlink(realRoot, rootLink); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	installer := newInstaller(t, rootLink, fixture.publicKey, fixture.compatibility)
	if _, err := installer.Stage(context.Background(), LocalDirectorySource{Directory: fixture.directory}); !errors.Is(err, ErrUnsafeDirectory) {
		t.Fatalf("symlink runtime root error = %v, want ErrUnsafeDirectory", err)
	}
	if entries, err := os.ReadDir(realRoot); err != nil || len(entries) != 0 {
		t.Fatalf("symlink target was modified: entries=%v err=%v", entries, err)
	}
}

func TestStageRejectsSymlinkAtReleaseTarget(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	releaseRoot := filepath.Join(root, "releases")
	if err := os.Mkdir(releaseRoot, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	target := filepath.Join(releaseRoot, releaseID(fixture.manifest.ReleaseVersion, fixture.manifest.Commit))
	if err := os.Symlink(outside, target); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	if _, err := installer.Stage(context.Background(), fixture); !errors.Is(err, ErrReleaseExists) {
		t.Fatalf("target collision error = %v, want ErrReleaseExists", err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("target symlink destination was modified: entries=%v err=%v", entries, err)
	}
	if info, err := os.Lstat(target); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("existing release symlink changed: info=%v err=%v", info, err)
	}
}

func TestStageRejectsPathTraversalFromSignedManifest(t *testing.T) {
	fixture := newBundle(t)
	fixture.manifest.Artifacts[0].Filename = "../runtime-host"
	fixture.resign(t)
	root := filepath.Join(t.TempDir(), "runtime")
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	if _, err := installer.Stage(context.Background(), fixture); !errors.Is(err, release.ErrInvalidManifest) {
		t.Fatalf("traversal manifest error = %v, want ErrInvalidManifest", err)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime directory created before manifest validation: err=%v", err)
	}
}

func TestStageDoesNotExecuteArtifacts(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	if _, err := installer.Stage(context.Background(), fixture); err != nil {
		t.Fatal(err)
	}
	// Artifact bytes remain the exact fixture payload. The installer has no
	// execution phase; an executable-looking shell payload stays inert data.
	installedID := releaseID(fixture.manifest.ReleaseVersion, fixture.manifest.Commit)
	got, err := os.ReadFile(filepath.Join(root, "releases", installedID, fixture.manifest.Artifacts[0].Filename))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(fixture.payloads[fixture.manifest.Artifacts[0].Filename]) {
		t.Fatalf("artifact changed or was unexpectedly executed")
	}
}

type bundleFixture struct {
	directory     string
	manifest      release.Manifest
	manifestBytes []byte
	signature     string
	publicKey     ed25519.PublicKey
	privateKey    ed25519.PrivateKey
	compatibility release.HostCompatibility
	payloads      map[string][]byte
}

func newBundle(t *testing.T) *bundleFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &bundleFixture{
		directory: t.TempDir(), publicKey: publicKey, privateKey: privateKey,
		compatibility: release.HostCompatibility{
			Platform: "linux", Architecture: "amd64", RuntimeProtocolVersion: 1,
			ControlProtocolRange: release.ProtocolRange{Minimum: 1, Maximum: 1},
			EngineProtocolRange:  release.ProtocolRange{Minimum: 1, Maximum: 1},
			AdapterProtocolRange: release.ProtocolRange{Minimum: 1, Maximum: 2},
			ArchiveReadRange:     release.ProtocolRange{Minimum: 2, Maximum: 2}, ArchiveWriteFormat: 2,
			ManagementSchemaVersion: 1,
		},
		payloads: make(map[string][]byte),
	}
	fixture.manifest = release.Manifest{
		ManifestSchemaVersion: 1, ReleaseVersion: "1.2.3", Commit: strings.Repeat("a", 40),
		BuildTime: time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Channel:   "stable", KeyID: "fixture-key", MinimumHostProtocol: 1, MaximumHostProtocol: 1,
		ControlProtocolVersion: 1, EngineProtocolVersion: 1, AdapterProtocolMinimum: 1, AdapterProtocolMaximum: 2,
		ArchiveReadMinimum: 2, ArchiveReadMaximum: 2, ArchiveWriteFormat: 2,
		ManagementSchemaMinimum: 1, ManagementSchemaMaximum: 1, Platform: "linux", Architecture: "amd64",
	}
	roles := []struct{ role, filename string }{
		{release.RoleRuntimeHost, "runtime-host"}, {release.RoleControlPlane, "control-plane"},
		{release.RoleRecorderEngine, "recorder-engine"}, {release.RoleAdapterRuntime, "adapter-runtime"},
	}
	for _, item := range roles {
		data := []byte("#!/not-executed\n" + item.role)
		hash := sha256.Sum256(data)
		fixture.manifest.Artifacts = append(fixture.manifest.Artifacts, release.Artifact{Role: item.role, Filename: item.filename, Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])})
		fixture.payloads[item.filename] = data
		if err := os.WriteFile(filepath.Join(fixture.directory, item.filename), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	fixture.resign(t)
	return fixture
}

func (f *bundleFixture) resign(t *testing.T) {
	t.Helper()
	data, err := json.Marshal(f.manifest)
	if err != nil {
		t.Fatal(err)
	}
	f.manifestBytes = data
	f.signature = base64.StdEncoding.EncodeToString(ed25519.Sign(f.privateKey, data))
	if err := os.WriteFile(filepath.Join(f.directory, "release.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.directory, "release.json.sig"), []byte(f.signature), 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *bundleFixture) Manifest(ctx context.Context) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	return append([]byte(nil), f.manifestBytes...), f.signature, nil
}

func (f *bundleFixture) OpenArtifact(ctx context.Context, filename string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, ok := f.payloads[filename]
	if !ok {
		return nil, ErrSourceRead
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

type cancelDuringCopySource struct {
	*bundleFixture
	cancel context.CancelFunc
}

type countingSource struct {
	*bundleFixture
	opens int
}

func (s *countingSource) OpenArtifact(ctx context.Context, filename string) (io.ReadCloser, error) {
	s.opens++
	return s.bundleFixture.OpenArtifact(ctx, filename)
}

func (s cancelDuringCopySource) OpenArtifact(ctx context.Context, filename string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, ok := s.payloads[filename]
	if !ok {
		return nil, ErrSourceRead
	}
	return &cancelAfterChunk{reader: strings.NewReader(string(data)), cancel: s.cancel}, nil
}

type cancelAfterChunk struct {
	reader   *strings.Reader
	cancel   context.CancelFunc
	canceled bool
}

func (r *cancelAfterChunk) Read(p []byte) (int, error) {
	if len(p) > 2 {
		p = p[:2]
	}
	n, err := r.reader.Read(p)
	if n > 0 && !r.canceled {
		r.canceled = true
		r.cancel()
	}
	return n, err
}

func (r *cancelAfterChunk) Close() error { return nil }

func newInstaller(t *testing.T, root string, publicKey ed25519.PublicKey, compatibility release.HostCompatibility) *Installer {
	t.Helper()
	installer, err := NewInstaller(root, map[string]ed25519.PublicKey{"fixture-key": publicKey}, compatibility)
	if err != nil {
		t.Fatal(err)
	}
	return installer
}

func assertNoPublishedReleases(t *testing.T, root string) {
	t.Helper()
	releases := filepath.Join(root, "releases")
	entries, err := os.ReadDir(releases)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatalf("read release directory: %v", err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".lock") {
			t.Fatalf("release unexpectedly published: %q", entry.Name())
		}
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat staged path: %v", err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("permissions for %s = %#o, want %#o", filepath.Base(path), got, want)
	}
}

func cleanupReadonlyTree(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if entry.IsDir() {
				_ = os.Chmod(path, 0700)
			} else {
				_ = os.Chmod(path, 0600)
			}
			return nil
		})
	})
}
