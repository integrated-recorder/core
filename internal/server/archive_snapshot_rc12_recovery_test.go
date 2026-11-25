package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/storage"
)

const rc12SealedFixtureID = "4d6f3c8208754a8cbff5064a171c2490"

// archiveSnapshotPublishingManager models the production Engine boundary:
// sealing is idempotent, and a retry also resumes catalog snapshot publication.
type archiveSnapshotPublishingManager struct {
	*acquire.Manager
	catalog *archivecatalog.Store
}

func (m *archiveSnapshotPublishingManager) SealArchiveContext(ctx context.Context, id string) error {
	if err := m.Manager.SealArchiveContext(ctx, id); err != nil {
		return err
	}
	_, err := m.Manager.PersistArchiveSnapshot(ctx, m.catalog, id)
	return err
}

func TestRC12SealedArchiveCanRetrySnapshotPublicationThroughAPI(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	fixture := filepath.Join("..", "acquire", "testdata", "rc12-sharded-archive")
	recordingDir := filepath.Join(root, "recordings", rc12SealedFixtureID)
	if err := copyRC12Fixture(fixture, recordingDir); err != nil {
		t.Fatalf("copy exact RC12 fixture: %v", err)
	}
	before, err := serverFixtureDigests(recordingDir)
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(ctx)
	catalog, err := archivecatalog.Open(filepath.Join(root, "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(&archiveSnapshotPublishingManager{Manager: manager, catalog: catalog}, nil, nil, Options{ArchiveCatalog: catalog})

	getIdentity := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+rc12SealedFixtureID+"/archive/snapshot", nil))
		return response
	}
	if pending := getIdentity(); pending.Code != http.StatusConflict {
		t.Fatalf("legacy sealed fixture without catalog snapshot status=%d body=%s", pending.Code, pending.Body.String())
	}

	for attempt := 1; attempt <= 2; attempt++ {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/recordings/"+rc12SealedFixtureID+"/seal", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("idempotent seal/snapshot retry %d status=%d body=%s", attempt, response.Code, response.Body.String())
		}
	}

	resolved := getIdentity()
	if resolved.Code != http.StatusOK {
		t.Fatalf("snapshot identity after retry status=%d body=%s", resolved.Code, resolved.Body.String())
	}
	var identity archiveSnapshotIdentityView
	if err := json.Unmarshal(resolved.Body.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}
	if identity.RecordingID != rc12SealedFixtureID || identity.ArchiveID == "" || identity.SnapshotID == "" || identity.Sequence != 1 {
		t.Fatalf("unexpected materialized identity: %+v", identity)
	}
	after, err := serverFixtureDigests(recordingDir)
	if err != nil {
		t.Fatal(err)
	}
	if !sameFixtureDigests(before, after) {
		t.Fatalf("snapshot publication changed canonical RC12 files: before=%v after=%v", before, after)
	}
}

func copyRC12Fixture(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return os.WriteFile(target, body, info.Mode().Perm())
	})
}

func serverFixtureDigests(root string) (map[string]string, error) {
	digests := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(body)
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		digests[filepath.ToSlash(rel)] = hex.EncodeToString(hash[:])
		return nil
	})
	return digests, err
}

func sameFixtureDigests(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for path, digest := range left {
		if right[path] != digest {
			return false
		}
	}
	return true
}
