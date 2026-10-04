package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestReadOnlyLoadDoesNotRecoverActiveArchive(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", PendingSequences: []uint64{4}}}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "recordings", id, "recording.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadRecordingReadOnly(id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != domain.StateRecording || len(loaded.Tracks["main"].PendingSequences) != 1 {
		t.Fatalf("read-only snapshot unexpectedly recovered archive: %#v", loaded)
	}
	rows, err := store.LoadAllReadOnly()
	if err != nil || len(rows) != 1 {
		t.Fatalf("read-only list returned %d rows, err=%v", len(rows), err)
	}
	if _, err := store.LoadAllReadOnlyLimit(0); !errors.Is(err, ErrReadOnlyListLimit) {
		t.Fatalf("zero-sized read-only listing error = %v, want limit error", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("read-only loading changed canonical recording.json")
	}
}

func TestReadOnlyLoadRejectsUnsafeIdentityAndSymlink(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadRecordingReadOnly("../secret"); err == nil {
		t.Fatal("path traversal recording ID was accepted")
	}
	const id = "1123456789abcdef0123456789abcdef"
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "recordings", id)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadRecordingReadOnly(id); err == nil {
		t.Fatal("symlinked recording directory was accepted")
	}
}
