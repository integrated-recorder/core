package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestTimedEventAPIUsesCursorPagesAndPreservingExport(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "aee2f247ab144c6a850fd5c07fb1e6a1"
	now := time.Now().UTC()
	recording := &domain.Recording{FormatVersion: storage.ShardedArchiveFormatVersion, ShardedArchive: &domain.ShardedArchiveSummary{}, ID: id, AdapterID: "fixture", State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}}}
	if err := store.CreateShardedRecording(recording); err != nil {
		t.Fatal(err)
	}
	events := make([]adapterproto.TimedEvent, 0, 3)
	for index := 0; index < 3; index++ {
		events = append(events, adapterproto.TimedEvent{EventType: "chat.message", SourceEventID: string(rune('a' + index)), Payload: json.RawMessage(`{"text":"message"}`), Extension: json.RawMessage(`{"platform_field":"kept"}`)})
	}
	if _, err := store.AppendShardedTimedEventBatch(context.Background(), id, adapterproto.TimedEventBatch{StreamID: "stream", BatchID: "batch", Events: events}); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/timed-events?limit=2", nil))
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"has_more":true`) || strings.Contains(first.Body.String(), "identity_hash") || strings.Contains(first.Body.String(), "resume_state") {
		t.Fatalf("first timed event page status=%d body=%s", first.Code, first.Body.String())
	}
	var page struct {
		Items      []map[string]any `json:"items"`
		NextCursor string           `json:"next_cursor"`
		HasMore    bool             `json:"has_more"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("first page=%#v err=%v", page, err)
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/timed-events?limit=2&cursor="+page.NextCursor, nil))
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"event_archive_ordinal":3`) || strings.Contains(second.Body.String(), `"event_archive_ordinal":2`) {
		t.Fatalf("second timed event page status=%d body=%s", second.Code, second.Body.String())
	}
	export := httptest.NewRecorder()
	handler.ServeHTTP(export, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/timed-events/export", nil))
	if export.Code != http.StatusOK || !strings.Contains(export.Header().Get("Content-Type"), "application/x-ndjson") || len(strings.Split(strings.TrimSpace(export.Body.String()), "\n")) != 3 || !strings.Contains(export.Body.String(), "platform_field") || strings.Contains(export.Body.String(), "identity_hash") || strings.Contains(export.Body.String(), "resume_state") {
		t.Fatalf("timed event export status=%d headers=%v body=%s", export.Code, export.Header(), export.Body.String())
	}
	managementEvents := httptest.NewRecorder()
	handler.ServeHTTP(managementEvents, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/events", nil))
	if managementEvents.Code != http.StatusOK || strings.Contains(managementEvents.Body.String(), "chat.message") {
		t.Fatalf("management events were conflated: %d %s", managementEvents.Code, managementEvents.Body.String())
	}
	filteredTail := httptest.NewRecorder()
	handler.ServeHTTP(filteredTail, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/timed-events?tail=1&type=chat.message", nil))
	if filteredTail.Code != http.StatusBadRequest {
		t.Fatalf("filtered tail status=%d body=%s", filteredTail.Code, filteredTail.Body.String())
	}
}
