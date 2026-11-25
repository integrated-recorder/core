package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestArchiveSnapshotIdentityReturnsOnlySealedIdentifiers(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(filepath.Join(root, "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	recording := writeProductRecording(t, store, recordingID, domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())

	catalog, err := archivecatalog.Open(filepath.Join(root, "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SealArchiveContext(context.Background(), recordingID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.PersistArchiveSnapshot(context.Background(), catalog, recordingID)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewWithOptions(manager, nil, nil, Options{ArchiveCatalog: catalog})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/archive/snapshot", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("snapshot identity status=%d body=%s", response.Code, response.Body.String())
	}
	var view archiveSnapshotIdentityView
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.RecordingID != recordingID || view.ArchiveID != snapshot.Content.ArchiveID || view.SnapshotID != snapshot.SnapshotID || view.Sequence != 1 || view.ObjectCount == 0 {
		t.Fatalf("snapshot identity projection=%+v", view)
	}
	for _, forbidden := range []string{"private.invalid", "recordings/local/path", "plaintext_sha256", "provenance", "content_hash"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("snapshot identity response exposed %q: %s", forbidden, response.Body.String())
		}
	}
}

func TestArchiveSnapshotIdentityDistinguishesPendingCatalog(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(filepath.Join(root, "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	writeProductRecording(t, store, recordingID, domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	catalog, err := archivecatalog.Open(filepath.Join(root, "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{ArchiveCatalog: catalog})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recordingID+"/archive/snapshot", nil))
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"error_code":"archive_snapshot_pending"`) {
		t.Fatalf("pending snapshot status=%d body=%s", response.Code, response.Body.String())
	}
}
