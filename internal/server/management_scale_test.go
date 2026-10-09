package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recordquery"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestRecordingsManagementPageScalesAcrossLightweightV2Summaries(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &readModelStorageBackend{StorageBackend: store.StorageBackend}
	store.StorageBackend = backend

	const recordingCount = 1_000
	const pageSize = 20
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	recordings := make([]*domain.Recording, 0, recordingCount)
	for index := 0; index < recordingCount; index++ {
		created := base.Add(time.Duration(index) * time.Second)
		mediaCount := uint64(index + 1)
		if index == recordingCount-1 {
			// A large archive remains a bounded root summary in this list path.
			mediaCount = 100_000
		}
		recordings = append(recordings, v2ManagementFixture(
			fmt.Sprintf("%032x", index+1), created, mediaCount,
		))
	}
	manager := &fixedRecordingManager{recordings: recordings}
	handler := NewWithOptions(manager, nil, nil, Options{Storage: store})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet,
		"/api/v2/recordings?sort=-created_at&limit=20",
		nil,
	))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}

	var page recordquery.Result
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != recordingCount {
		t.Fatalf("total=%d, want %d", page.Total, recordingCount)
	}
	if len(page.Items) != pageSize {
		t.Fatalf("page rows=%d, want %d", len(page.Items), pageSize)
	}
	for offset, item := range page.Items {
		index := recordingCount - 1 - offset
		wantID := fmt.Sprintf("%032x", index+1)
		if item.ID != wantID {
			t.Fatalf("page[%d].id=%q, want newest-first id %q", offset, item.ID, wantID)
		}
		if !item.CreatedAt.Equal(base.Add(time.Duration(index) * time.Second)) {
			t.Fatalf("page[%d].created_at=%s, want index %d", offset, item.CreatedAt, index)
		}
		if index == recordingCount-1 && item.SegmentCount != 100_000 {
			t.Fatalf("long recording segment summary=%d, want 100000", item.SegmentCount)
		}
	}
	if manager.getCalls != 0 {
		t.Fatalf("management list materialized full recordings through Manager.Get %d times", manager.getCalls)
	}
	if calls := backend.directoryBytesCalls.Load(); calls != 0 {
		t.Fatalf("management list enumerated physical archive bytes %d times", calls)
	}
}
