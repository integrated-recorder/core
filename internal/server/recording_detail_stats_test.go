package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestRecordingDetailIncludesDerivedArchiveStatistics(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("d", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Statistics recordingStatistics `json:"statistics"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	stats := result.Statistics
	if stats.ArchiveSizeBytes == nil || *stats.ArchiveSizeBytes <= 0 || stats.Status != "complete" || stats.UnavailableFields == nil || len(stats.UnavailableFields) != 0 || stats.MediaPayloadSizeBytes != int64(len("original-media-payload")) ||
		stats.InitPayloadSizeBytes != int64(len("original-init-payload")) || stats.ManifestSizeBytes != int64(len("#EXTM3U\n")) ||
		stats.SegmentCount != 1 || stats.InitSegmentCount != 1 || stats.ManifestSnapshotCount != 1 ||
		stats.GapCount != 1 || stats.GapSegmentCount != 2 || stats.GapDurationSeconds != nil {
		t.Fatalf("detail statistics are incomplete or inaccurate: %+v", stats)
	}
}
