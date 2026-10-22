package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/storageproto"
)

func TestProbeStorageProviderSetReportsPhysicalArchiveProbeFailure(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { makeStorageDiagnosticTreeRemovable(root) })
	catalogRoot := filepath.Join(root, "runtime", "storage-providers")
	catalog, err := storagecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}

	packageDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "storage.local")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelBuild()
	command := exec.CommandContext(buildCtx, "go", "build", "-trimpath", "-o", binary, "../../../cmd/storage-local")
	command.Dir = packageDir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build storage.local fixture: %v\n%s", err, output)
	}
	artifact, err := catalog.ImportBundled(context.Background(), binary, "local")
	if err != nil {
		t.Fatalf("import bundled storage.local fixture: %v", err)
	}
	if err := catalog.InstallWithAttestation(artifact, plugintrust.NewBundled()); err != nil {
		t.Fatal(err)
	}
	archiveRoot := filepath.Join(root, "recordings")
	if err := os.Mkdir(archiveRoot, 0700); err != nil {
		t.Fatal(err)
	}
	rootJSON, err := json.Marshal(archiveRoot)
	if err != nil {
		t.Fatal(err)
	}
	set, err := catalog.CreateInstalledSet("local", storagecatalog.SetConfig{Values: map[string]json.RawMessage{"root": rootJSON}})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate an existing private marker directory that the current process
	// can traverse but cannot extend. This leaves the archive root itself
	// healthy, so the failure occurs in the physical object-store write probe.
	privateDir := filepath.Join(archiveRoot, "\x1f.ir-storage-local")
	otherDir := filepath.Join(privateDir, "other")
	if err := os.MkdirAll(otherDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(privateDir, 0500); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(otherDir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(otherDir, 0700)
		_ = os.Chmod(privateDir, 0700)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = probeStorageProviderSet(ctx, catalogRoot, set.ID)
	var diagnostic *storageproto.DiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic.Stage() != "physical_storage_probe" || diagnostic.Code() != "write" {
		t.Fatalf("probe error = %v, diagnostic=%#v; want physical_storage_probe/write", err, diagnostic)
	}
	if !strings.Contains(err.Error(), "stage=physical_storage_probe") || strings.Contains(err.Error(), archiveRoot) {
		t.Fatalf("physical probe error is not safely diagnostic: %v", err)
	}
}

func TestEnsureBundledLocalStoragePreservesImportStage(t *testing.T) {
	root := t.TempDir()
	catalog, err := storagecatalog.Open(filepath.Join(root, "runtime", "storage-providers"))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "storage.local")
	if err := os.WriteFile(binary, []byte("not executable"), 0600); err != nil {
		t.Fatal(err)
	}

	_, err = ensureBundledLocalStorage(context.Background(), catalog, binary, filepath.Join(root, "recordings"))
	var diagnostic *storageproto.DiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic.Stage() != "bundled_executable_probe" || diagnostic.Code() != "executable_invalid" {
		t.Fatalf("bundled storage error=%v diagnostic=%#v, want bundled_executable_probe/executable_invalid", err, diagnostic)
	}
	if !errors.Is(err, storagecatalog.ErrInvalidArtifact) {
		t.Fatalf("bundled storage diagnostic lost ErrInvalidArtifact compatibility: %v", err)
	}
	if strings.Contains(err.Error(), binary) {
		t.Fatalf("bundled storage diagnostic exposed executable path: %v", err)
	}
}

func makeStorageDiagnosticTreeRemovable(root string) {
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		mode := os.FileMode(0600)
		if entry.IsDir() {
			mode = 0700
		}
		_ = os.Chmod(path, mode)
		return nil
	})
}
