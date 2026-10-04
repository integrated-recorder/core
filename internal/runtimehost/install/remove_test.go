package install

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

func TestRemoveInstalledReleaseVerifiesAndRemovesImmutableDirectory(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	staged, err := installer.Stage(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}

	if err := RemoveInstalledRelease(root, staged.ID, map[string]ed25519.PublicKey{"fixture-key": fixture.publicKey}); err != nil {
		t.Fatalf("RemoveInstalledRelease(): %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "releases", staged.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("release still exists after removal: %v", err)
	}
}

func TestRemoveInstalledReleaseRetriesPartialCleanup(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	staged, err := installer.Stage(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "releases", staged.ID)
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, fixture.manifest.Artifacts[0].Filename)); err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"fixture-key": fixture.publicKey}
	if err := RemoveInstalledRelease(root, staged.ID, keys); err != nil {
		t.Fatalf("RemoveInstalledRelease() after partial artifact cleanup: %v", err)
	}
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partially cleaned release still exists: %v", err)
	}
}

func TestRemoveInstalledReleaseRejectsSymlinkWithoutTouchingTarget(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	staged, err := installer.Stage(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "releases", staged.ID)
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside")
	contents := []byte("keep me")
	if err := os.WriteFile(target, contents, 0600); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(directory, fixture.manifest.Artifacts[0].Filename)
	if err := os.Remove(artifact); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, artifact); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := RemoveInstalledRelease(root, staged.ID, map[string]ed25519.PublicKey{"fixture-key": fixture.publicKey}); !errors.Is(err, ErrUnsafeDirectory) {
		t.Fatalf("RemoveInstalledRelease() error = %v, want ErrUnsafeDirectory", err)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != string(contents) {
		t.Fatalf("symlink target changed: bytes=%q err=%v", got, err)
	}
}

func TestRemoveInstalledReleaseRequiresTrustForIntactDirectory(t *testing.T) {
	fixture := newBundle(t)
	root := filepath.Join(t.TempDir(), "runtime")
	cleanupReadonlyTree(t, root)
	installer := newInstaller(t, root, fixture.publicKey, fixture.compatibility)
	staged, err := installer.Stage(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := RemoveInstalledRelease(root, staged.ID, nil); !errors.Is(err, release.ErrUnknownSigningKey) {
		t.Fatalf("RemoveInstalledRelease() error = %v, want unknown signing key", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "releases", staged.ID)); err != nil {
		t.Fatalf("untrusted release was removed: %v", err)
	}
}
