package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const sidecarTestRecordingID = "0123456789abcdef0123456789abcdef"

type sidecarTestDocument struct {
	Value string `json:"value"`
}

func TestLoadSidecarLocalFilesystem(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	testLoadSidecarCases(t, store, func(t *testing.T, relative string, data []byte) {
		path := filepath.Join(store.root, "recordings", sidecarTestRecordingID, filepath.FromSlash(relative+".json"))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLoadSidecarObjectStore(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	testLoadSidecarCases(t, store, func(t *testing.T, relative string, data []byte) {
		objects.replace(recordingObjectKey(sidecarTestRecordingID, relative)+".json", data)
	})
}

func testLoadSidecarCases(t *testing.T, store *Store, putRaw func(*testing.T, string, []byte)) {
	t.Helper()
	const relative = "tracks/main/item.m4s"

	t.Run("round trip", func(t *testing.T) {
		want := sidecarTestDocument{Value: "metadata"}
		if err := store.SaveSidecar(sidecarTestRecordingID, relative, want); err != nil {
			t.Fatal(err)
		}
		var got sidecarTestDocument
		if err := store.LoadSidecar(sidecarTestRecordingID, relative, 1024, &got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("sidecar = %#v, want %#v", got, want)
		}
	})

	t.Run("missing", func(t *testing.T) {
		var got sidecarTestDocument
		if err := store.LoadSidecar("1123456789abcdef0123456789abcdef", relative, 1024, &got); !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("missing sidecar error = %v", err)
		}
	})

	t.Run("oversized", func(t *testing.T) {
		putRaw(t, "tracks/main/oversized.m4s", []byte(`{"value":"this is too large"}`))
		var got sidecarTestDocument
		if err := store.LoadSidecar(sidecarTestRecordingID, "tracks/main/oversized.m4s", 8, &got); err == nil {
			t.Fatal("oversized sidecar was accepted")
		}
	})

	t.Run("malformed", func(t *testing.T) {
		putRaw(t, "tracks/main/malformed.m4s", []byte(`{"value":`))
		var got sidecarTestDocument
		if err := store.LoadSidecar(sidecarTestRecordingID, "tracks/main/malformed.m4s", 1024, &got); err == nil {
			t.Fatal("malformed sidecar was accepted")
		}
	})

	t.Run("trailing JSON", func(t *testing.T) {
		putRaw(t, "tracks/main/trailing.m4s", []byte(`{"value":"one"} {"value":"two"}`))
		var got sidecarTestDocument
		if err := store.LoadSidecar(sidecarTestRecordingID, "tracks/main/trailing.m4s", 1024, &got); err == nil {
			t.Fatal("sidecar with trailing JSON was accepted")
		}
	})
}

func TestLoadSidecarRejectsInvalidArgumentsAndOptionalCapability(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, id, path string
		limit          int64
	}{
		{name: "recording id", id: "../recording", path: "tracks/main/item", limit: 1024},
		{name: "path traversal", id: sidecarTestRecordingID, path: "tracks/../item", limit: 1024},
		{name: "zero limit", id: sidecarTestRecordingID, path: "tracks/main/item", limit: 0},
		{name: "limit above maximum", id: sidecarTestRecordingID, path: "tracks/main/item", limit: maxSidecarReadBytes + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output sidecarTestDocument
			if err := store.LoadSidecar(test.id, test.path, test.limit, &output); err == nil {
				t.Fatal("invalid sidecar arguments were accepted")
			}
		})
	}

	withoutOptionalReader := &Store{StorageBackend: sidecarBackendWithoutOptionalReader{StorageBackend: store.StorageBackend}}
	var output sidecarTestDocument
	if err := withoutOptionalReader.LoadSidecar(sidecarTestRecordingID, "tracks/main/item", 1024, &output); !errors.Is(err, ErrSidecarReadUnsupported) {
		t.Fatalf("missing optional capability error = %v", err)
	}
	if err := withoutOptionalReader.CreateRecordingWithSidecar(stoppedRecording(sidecarTestRecordingID), "archive-index/acquisition-context", sidecarTestDocument{Value: "private"}); !errors.Is(err, ErrAtomicRecordingCreationUnsupported) {
		t.Fatalf("missing atomic create capability error = %v", err)
	}
}

// Embedding the mandatory interface must not accidentally promote optional
// backend methods that are intentionally outside StorageBackend.
type sidecarBackendWithoutOptionalReader struct {
	StorageBackend
}
