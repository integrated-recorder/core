package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/domain"
)

const futureFormatRecordingID = "a123456789abcdef0123456789abcdef"

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
