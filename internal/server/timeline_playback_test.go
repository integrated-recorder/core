package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestVODProjectsLatePrefixAndRepairedGapByTimelineOrdinal(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdefabcdefabcdefabcdefabcdefab"
	now := time.Now().UTC()
	segments := []domain.Segment{
		{ID: "tail", TrackID: "main", Sequence: 103, SourceEpoch: 1, ArchiveOrdinal: 2, TimelineOrdinal: 4, Duration: 4, StoragePath: "tracks/main/tail.ts"},
		{ID: "middle", TrackID: "main", Sequence: 101, SourceEpoch: 1, ArchiveOrdinal: 1, TimelineOrdinal: 2, Duration: 2, StoragePath: "tracks/main/middle.ts"},
		{ID: "prefix", TrackID: "main", Sequence: 100, SourceEpoch: 1, ArchiveOrdinal: 3, TimelineOrdinal: 1, Duration: 1, StoragePath: "tracks/main/prefix.ts"},
		{ID: "repair", TrackID: "main", Sequence: 102, SourceEpoch: 1, ArchiveOrdinal: 4, TimelineOrdinal: 3, Duration: 3, StoragePath: "tracks/main/repair.ts"},
	}
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: segments}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	for i := range segments {
		result, saveErr := store.SavePayload(id, segments[i].StoragePath, strings.NewReader(segments[i].ID), 1024)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		segments[i].PayloadSize = result.Size
		segments[i].SHA256 = result.SHA256
	}
	recording.Tracks["main"].Segments = segments
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	handler := New(manager, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/tracks/main/playlist.m3u8", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("playlist status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	wantOrder := []string{"/segments/prefix\n", "/segments/middle\n", "/segments/repair\n", "/segments/tail\n"}
	previous := -1
	for _, uri := range wantOrder {
		position := strings.Index(body, uri)
		if position <= previous {
			t.Fatalf("playlist order does not follow the current late-prefix/gap-repair projection; missing or out of order %q:\n%s", uri, body)
		}
		previous = position
	}
	if !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:0\n") {
		t.Fatalf("media sequence should be derived from the current timeline position:\n%s", body)
	}
}
