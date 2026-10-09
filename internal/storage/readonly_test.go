package storage

import (
	"bytes"
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

func TestReadRecordingRootSnapshotAcceptsAtomicReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.json")
	oldData := []byte(`{"format_version":2,"title":"old"}`)
	newData := []byte(`{"format_version":2,"title":"new"}`)
	if err := os.WriteFile(path, oldData, 0600); err != nil {
		t.Fatal(err)
	}
	observed, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(dir, "recording.next")
	if err := os.WriteFile(temporary, newData, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
	got, err := readRecordingRootSnapshot(path, observed)
	if err != nil {
		t.Fatalf("atomic replacement caused read failure: %v", err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatalf("snapshot bytes=%q, want %q", got, newData)
	}
}

func TestReadRecordingRootSnapshotStillRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.json")
	if err := os.WriteFile(path, []byte(`{"format_version":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	observed, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte(`{"format_version":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := readRecordingRootSnapshot(path, observed); err == nil {
		t.Fatal("snapshot accepted a recording metadata symlink")
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
