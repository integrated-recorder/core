//go:build linux || darwin

package replication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRestorePublicationDoesNotReplaceExistingEmptyDirectory(t *testing.T) {
	root := t.TempDir()
	staged := filepath.Join(root, "staged")
	destination := filepath.Join(root, "destination")
	if err := os.Mkdir(staged, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, "complete"), []byte("verified"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		t.Fatal(err)
	}

	err := replicationRenameNoReplace(staged, destination)
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("publication over an existing empty directory error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "complete")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("existing destination was replaced: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staged, "complete")); err != nil {
		t.Fatalf("failed publication removed the verified staging tree: %v", err)
	}
}
