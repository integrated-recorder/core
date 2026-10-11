package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/domain"
)

const futureFormatRecordingID = "a123456789abcdef0123456789abcdef"

func TestRecordingFormatVersionSnapshotAcceptsAtomicRootReplacement(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.NewRecordingDir(futureFormatRecordingID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "recordings", futureFormatRecordingID, "recording.json")
	oldRoot := []byte(`{"format_version":2,"id":"` + futureFormatRecordingID + `","title":"old"}`)
	newRoot := []byte(`{"format_version":2,"id":"` + futureFormatRecordingID + `","title":"new"}`)
	if err := os.WriteFile(path, oldRoot, 0600); err != nil {
		t.Fatal(err)
	}
	observed, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(dir, "recording.next")
	if err := os.WriteFile(temporary, newRoot, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}

	version, err := store.RecordingFormatVersion(context.Background(), futureFormatRecordingID)
	if err != nil || version != ShardedArchiveFormatVersion {
		t.Fatalf("format version after atomic replacement = %d, %v; want V2", version, err)
	}
	// Reuse stale Lstat result to deterministically model publication in the
	// interval between RecordingFormatVersion's Lstat and Open calls.
	version, err = readRecordingFormatVersionSnapshot(context.Background(), path, observed)
	if err != nil || version != ShardedArchiveFormatVersion {
		t.Fatalf("format snapshot after atomic replacement = %d, %v; want V2", version, err)
	}
}

func TestRecordingFormatVersionSnapshotRejectsRootSymlinkSwap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.json")
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(path, []byte(`{"format_version":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	observed, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(`{"format_version":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := readRecordingFormatVersionSnapshot(context.Background(), path, observed); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("root symlink swap error = %v, want ErrShardedArchiveInvalid", err)
	}
}

func TestRecordingFormatVersionSnapshotRejectsOversizedPublishedRoot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recording.json")
	if err := os.WriteFile(path, []byte(`{"format_version":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	observed, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(dir, "recording.next")
	file, err := os.Create(temporary)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxRecordingJSONBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
	if _, err := readRecordingFormatVersionSnapshot(context.Background(), path, observed); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("oversized published root error = %v, want ErrShardedArchiveInvalid", err)
	}
}

func TestLocalUnknownRecordingFormatIsPreservedAndRejected(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.NewRecordingDir(futureFormatRecordingID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "recordings", futureFormatRecordingID, "recording.json")
	original := futureFormatRecordingRoot(t, futureFormatRecordingID)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := store.LoadRecordingReadOnly(futureFormatRecordingID); !errors.Is(err, ErrUnsupportedRecordingFormat) {
		t.Fatalf("LoadRecordingReadOnly error = %v, want ErrUnsupportedRecordingFormat", err)
	}
	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("LoadAll returned unsupported root: %#v", loaded)
	}
	if !hasIssue(store.RecoveryIssues(), futureFormatRecordingID, "metadata_format_unsupported") {
		t.Fatalf("unsupported format issue missing: %#v", store.RecoveryIssues())
	}
	if err := store.SaveRecording(&domain.Recording{FormatVersion: 3, ID: futureFormatRecordingID, State: domain.StateStopped}); !errors.Is(err, ErrUnsupportedRecordingFormat) {
		t.Fatalf("SaveRecording error = %v, want ErrUnsupportedRecordingFormat", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(original) {
		t.Fatal("unsupported recording root changed during read, recovery, or save")
	}
}

func TestObjectStoreUnknownRecordingFormatIsPreservedAndRejected(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	key := recordingObjectKey(futureFormatRecordingID, "recording.json")
	original := futureFormatRecordingRoot(t, futureFormatRecordingID)
	objects.replace(key, original)

	if _, err := store.LoadRecordingReadOnly(futureFormatRecordingID); !errors.Is(err, ErrUnsupportedRecordingFormat) {
		t.Fatalf("LoadRecordingReadOnly error = %v, want ErrUnsupportedRecordingFormat", err)
	}
	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("LoadAll returned unsupported root: %#v", loaded)
	}
	if !hasIssue(store.RecoveryIssues(), futureFormatRecordingID, "metadata_format_unsupported") {
		t.Fatalf("unsupported format issue missing: %#v", store.RecoveryIssues())
	}
	if err := store.SaveRecording(&domain.Recording{FormatVersion: 3, ID: futureFormatRecordingID, State: domain.StateStopped}); !errors.Is(err, ErrUnsupportedRecordingFormat) {
		t.Fatalf("SaveRecording error = %v, want ErrUnsupportedRecordingFormat", err)
	}
	if got := objects.get(key); string(got) != string(original) {
		t.Fatal("unsupported recording object changed during read, recovery, or save")
	}
	objects.mu.Lock()
	putCount := len(objects.putKeys)
	objects.mu.Unlock()
	if putCount != 0 {
		t.Fatalf("unsupported recording recovery published %d objects", putCount)
	}
}

func TestLocalLegacyAndCurrentRecordingFormatsRemainReadable(t *testing.T) {
	for _, version := range []int{legacyRecordingFormatVersion, currentRecordingFormatVersion} {
		t.Run(formatVersionName(version), func(t *testing.T) {
			root := t.TempDir()
			store, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			id := "b123456789abcdef0123456789abcdef"
			if err := store.NewRecordingDir(id); err != nil {
				t.Fatal(err)
			}
			recording := stoppedRecording(id)
			recording.FormatVersion = version
			if err := store.SaveRecording(recording); err != nil {
				t.Fatal(err)
			}
			loaded, err := store.LoadRecordingReadOnly(id)
			if err != nil || loaded.FormatVersion != version {
				t.Fatalf("loaded format=%d err=%v, want %d", formatVersionOf(loaded), err, version)
			}
			if err := store.SaveRecording(loaded); err != nil {
				t.Fatalf("supported format save: %v", err)
			}
			recovered, err := store.LoadAll()
			if err != nil || len(recovered) != 1 || recovered[0].FormatVersion != version {
				t.Fatalf("LoadAll recovered=%#v err=%v, want one format %d recording", recovered, err, version)
			}
		})
	}
}

func TestObjectStoreLegacyAndCurrentRecordingFormatsRemainReadable(t *testing.T) {
	for _, version := range []int{legacyRecordingFormatVersion, currentRecordingFormatVersion} {
		t.Run(formatVersionName(version), func(t *testing.T) {
			objects := newMemoryPhysicalObjects()
			store, err := NewWithObjectStore(t.TempDir(), objects)
			if err != nil {
				t.Fatal(err)
			}
			id := "c123456789abcdef0123456789abcdef"
			recording := stoppedRecording(id)
			recording.FormatVersion = version
			data, err := json.Marshal(recording)
			if err != nil {
				t.Fatal(err)
			}
			key := recordingObjectKey(id, "recording.json")
			objects.replace(key, data)
			loaded, err := store.LoadRecordingReadOnly(id)
			if err != nil || loaded.FormatVersion != version {
				t.Fatalf("loaded format=%d err=%v, want %d", formatVersionOf(loaded), err, version)
			}
			if err := store.SaveRecording(loaded); err != nil {
				t.Fatalf("supported format save: %v", err)
			}
			recovered, err := store.LoadAll()
			if err != nil || len(recovered) != 1 || recovered[0].FormatVersion != version {
				t.Fatalf("LoadAll recovered=%#v err=%v, want one format %d recording", recovered, err, version)
			}
		})
	}
}

func futureFormatRecordingRoot(t *testing.T, id string) []byte {
	t.Helper()
	return []byte(`{"format_version":3,"id":"` + id + `","state":"recording","tracks":{"main":{"id":"main","pending_sequences":[7],"segments":[]}},"future_extension":{"preserve":"opaque-value"}}`)
}

func formatVersionName(version int) string {
	if version == legacyRecordingFormatVersion {
		return "legacy-zero"
	}
	return "current-one"
}

func formatVersionOf(recording *domain.Recording) int {
	if recording == nil {
		return -1
	}
	return recording.FormatVersion
}
