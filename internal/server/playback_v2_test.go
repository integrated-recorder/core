package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestShardedVODPlaylistUsesOrderedTimelineWithoutRecordingReconstruction(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 270)
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})

	master := httptest.NewRecorder()
	handler.ServeHTTP(master, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/master.m3u8", nil))
	if master.Code != http.StatusOK || !strings.Contains(master.Body.String(), "BANDWIDTH=2400000") {
		t.Fatalf("master status=%d body=%s", master.Code, master.Body.String())
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/tracks/main/playlist.m3u8", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("playlist status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if !strings.Contains(body, "#EXT-X-PLAYLIST-TYPE:VOD\n") || !strings.Contains(body, "#EXT-X-TARGETDURATION:2\n") || !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:0\n") || !strings.HasSuffix(body, "#EXT-X-ENDLIST\n") {
		t.Fatalf("playlist omitted VOD headers or end marker:\n%s", body[:min(len(body), 1200)])
	}
	if got := strings.Count(body, "/play/segments/seg-"); got != 270 {
		t.Fatalf("playlist media references=%d, want 270", got)
	}
	if got := strings.Count(body, "#EXT-X-MAP:URI="); got != 2 {
		t.Fatalf("playlist map transitions=%d, want 2:\n%s", got, body[:min(len(body), 1200)])
	}
	if got := strings.Count(body, "#EXT-X-DISCONTINUITY\n"); got != 3 {
		t.Fatalf("playlist discontinuities=%d, want missing source slot, source flag, and epoch boundary:\n%s", got, body[:min(len(body), 1200)])
	}
	if strings.Contains(body, "source-secret") || strings.Contains(body, "example.invalid") {
		t.Fatalf("playlist leaked source URI: %s", body[:min(len(body), 1200)])
	}

	wantFirst := []string{
		"/play/segments/seg-00000000000000000001\n",
		"/play/segments/seg-00000000000000000003\n",
		"/play/segments/seg-00000000000000000002\n",
		"/play/segments/seg-00000000000000000004\n",
	}
	previous := -1
	for _, uri := range wantFirst {
		position := strings.Index(body, uri)
		if position <= previous {
			t.Fatalf("timeline order missing or changed at %q:\n%s", uri, body[:min(len(body), 1200)])
		}
		previous = position
	}
	if !strings.Contains(body, `#EXT-X-MAP:URI="/api/recordings/`+id+`/play/segments/init-vod-a"`) || !strings.Contains(body, `#EXT-X-MAP:URI="/api/recordings/`+id+`/play/segments/init-vod-b"`) {
		t.Fatalf("playlist omitted expected init map identities:\n%s", body[:min(len(body), 1200)])
	}

	header, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(header.Tracks["main"].Segments); got != 0 {
		t.Fatalf("v2 header reconstructed %d media records", got)
	}
	if header.Tracks["main"].MediaCount != 270 || header.SegmentCount() != 270 {
		t.Fatalf("bounded v2 media summary track=%d root=%d", header.Tracks["main"].MediaCount, header.SegmentCount())
	}
}

func TestShardedGapStreamSkipsOtherTracks(t *testing.T) {
	stream := &shardedGapStream{
		items: make(chan shardedGapMessage, 3),
		track: "main",
	}
	stream.items <- shardedGapMessage{gap: &domain.Gap{TrackID: "aux", SourceEpoch: 1, FromSequence: 2, ToSequence: 3}}
	stream.items <- shardedGapMessage{gap: &domain.Gap{TrackID: "main", SourceEpoch: 1, FromSequence: 2, ToSequence: 3}}
	stream.items <- shardedGapMessage{done: true}

	hasGap, err := stream.hasGapBetween("main", domain.Segment{SourceEpoch: 1, Sequence: 1}, domain.Segment{SourceEpoch: 1, Sequence: 3})
	if err != nil || !hasGap {
		t.Fatalf("target-track gap was lost after another track: hasGap=%t err=%v", hasGap, err)
	}
	if err := stream.finish(); err != nil {
		t.Fatalf("finish gap stream: %v", err)
	}
}

func TestShardedSegmentRangeUsesDirectMediaLookup(t *testing.T) {
	store, id, payloads := newShardedVODFixture(t, 3)
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})

	request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/segments/seg-00000000000000000001", nil)
	request.Header.Set("Range", "bytes=2-5")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	want := payloads[1][2:6]
	if response.Code != http.StatusPartialContent || !bytes.Equal(response.Body.Bytes(), want) {
		t.Fatalf("range status=%d body=%q, want 206 %q", response.Code, response.Body.Bytes(), want)
	}
	if got, want := response.Header().Get("Content-Range"), fmt.Sprintf("bytes 2-5/%d", len(payloads[1])); got != want {
		t.Fatalf("Content-Range=%q, want %q", got, want)
	}
}

func TestShardedSegmentFullGETRemains200(t *testing.T) {
	store, id, payloads := newShardedVODFixture(t, 1)
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})
	request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/segments/seg-00000000000000000001", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), payloads[1]) {
		t.Fatalf("full GET status=%d body=%q, want 200 %q", response.Code, response.Body.Bytes(), payloads[1])
	}
}

func TestShardedIntegrityRouteUsesBoundedHeaderWithoutManager(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 270)
	service, err := integrity.Open(t.TempDir(), store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store, Integrity: service})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/integrity", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"unknown"`) {
		t.Fatalf("integrity route status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestShardedExportListUsesBoundedHeaderWithoutManager(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 270)
	root := t.TempDir()
	ffmpegPath := filepath.Join(root, "fake-ffmpeg")
	if err := os.WriteFile(ffmpegPath, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ffmpegPath, 0700); err != nil {
		t.Fatal(err)
	}
	service, err := derivative.Open(root, store, ffmpegPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store, Derivatives: service})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/exports", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"items":[]`) || !strings.Contains(response.Body.String(), `"available":true`) {
		t.Fatalf("export list status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestShardedArchiveIndexUsesBoundedHeaderAndIterators(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 3)
	at := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	snapshot, err := store.SaveSnapshot(id, "main", "https://example.invalid/private-manifest", []byte("#EXTM3U\n"), at)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendShardedManifest(context.Background(), id, snapshot); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/archive/index", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("archive index status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		RecordingID string                 `json:"recording_id"`
		Entries     []storage.ArchiveEntry `json:"entries"`
		NextCursor  string                 `json:"next_cursor"`
		HasMore     bool                   `json:"has_more"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.RecordingID != id || len(result.Entries) != 8 || result.HasMore || result.NextCursor != "" {
		t.Fatalf("archive index recording=%q entries=%d: %+v", result.RecordingID, len(result.Entries), result.Entries)
	}
	var terminalPayload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &terminalPayload); err != nil {
		t.Fatal(err)
	}
	if _, exists := terminalPayload["next_cursor"]; exists {
		t.Fatalf("terminal archive index page must omit next_cursor: %s", response.Body.String())
	}
	wantPaths := []string{
		"recording.json", snapshot.StoragePath, snapshot.StoragePath + ".json",
		"tracks/main/init-vod-a.mp4", "tracks/main/init-vod-b.mp4",
		"tracks/main/segment-00000000000000000001.ts", "tracks/main/segment-00000000000000000002.ts", "tracks/main/segment-00000000000000000003.ts",
	}
	for i, entry := range result.Entries {
		if entry.Path != wantPaths[i] {
			t.Fatalf("archive index order[%d]=%q want=%q: %+v", i, entry.Path, wantPaths[i], result.Entries)
		}
		if strings.HasPrefix(entry.Path, "tracks/") && strings.HasSuffix(entry.Path, ".json") {
			t.Fatalf("v2 media sidecar unexpectedly visible: %+v", entry)
		}
	}
	if !strings.Contains(response.Body.String(), `"kind":"manifest_sidecar"`) || strings.Contains(response.Body.String(), "private-manifest") {
		t.Fatalf("archive index omitted visible manifest sidecar or leaked source URI: %s", response.Body.String())
	}

	var pagedPaths []string
	cursor := ""
	for {
		request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/archive/index?limit=3&cursor="+cursor, nil)
		if cursor == "" {
			request = httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/archive/index?limit=3", nil)
		}
		pageResponse := httptest.NewRecorder()
		handler.ServeHTTP(pageResponse, request)
		if pageResponse.Code != http.StatusOK {
			t.Fatalf("archive index page status=%d body=%s", pageResponse.Code, pageResponse.Body.String())
		}
		var page struct {
			Entries    []storage.ArchiveEntry `json:"entries"`
			NextCursor string                 `json:"next_cursor"`
			HasMore    bool                   `json:"has_more"`
		}
		if err := json.Unmarshal(pageResponse.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		for _, entry := range page.Entries {
			pagedPaths = append(pagedPaths, entry.Path)
		}
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Fatalf("terminal archive index page cursor=%q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" {
			t.Fatal("non-terminal archive index page omitted cursor")
		}
		cursor = page.NextCursor
	}
	if len(pagedPaths) != len(wantPaths) {
		t.Fatalf("paged archive index paths=%v want=%v", pagedPaths, wantPaths)
	}
	for index, want := range wantPaths {
		if pagedPaths[index] != want {
			t.Fatalf("paged archive index[%d]=%q want=%q", index, pagedPaths[index], want)
		}
	}
	invalidLimit := httptest.NewRecorder()
	handler.ServeHTTP(invalidLimit, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/archive/index?limit=101", nil))
	if invalidLimit.Code != http.StatusBadRequest || !strings.Contains(invalidLimit.Body.String(), `"error_code":"archive_index_invalid_pagination"`) {
		t.Fatalf("invalid archive index limit status=%d body=%s", invalidLimit.Code, invalidLimit.Body.String())
	}
	firstPage := httptest.NewRecorder()
	handler.ServeHTTP(firstPage, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/archive/index?limit=1", nil))
	var firstPageResult struct {
		NextCursor string `json:"next_cursor"`
	}
	if firstPage.Code != http.StatusOK || json.Unmarshal(firstPage.Body.Bytes(), &firstPageResult) != nil || firstPageResult.NextCursor == "" {
		t.Fatalf("archive index first page status=%d body=%s", firstPage.Code, firstPage.Body.String())
	}
	changedTitle := "revision changed after page one"
	if err := store.AppendShardedMetadata(context.Background(), id, domain.MetadataRevision{ObservedAt: at.Add(time.Minute), Title: &changedTitle}); err != nil {
		t.Fatal(err)
	}
	stalePage := httptest.NewRecorder()
	staleRequest := httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/archive/index?limit=1&cursor="+firstPageResult.NextCursor, nil)
	handler.ServeHTTP(stalePage, staleRequest)
	if stalePage.Code != http.StatusConflict || !strings.Contains(stalePage.Body.String(), `"error_code":"archive_index_stale_cursor"`) {
		t.Fatalf("stale archive index cursor status=%d body=%s", stalePage.Code, stalePage.Body.String())
	}
}

func TestShardedMetadataRouteKeepsBoundedTail(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 3)
	at := time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC)
	for index := 0; index < metadataTimelineAPILimit+4; index++ {
		title := fmt.Sprintf("title-%03d", index)
		revision := domain.MetadataRevision{ObservedAt: at.Add(time.Duration(index) * time.Second), Title: &title}
		if err := store.AppendShardedMetadata(context.Background(), id, revision); err != nil {
			t.Fatalf("append metadata revision %d: %v", index, err)
		}
	}
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/metadata", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("metadata status=%d body=%s", response.Code, response.Body.String())
	}
	var result recordingMetadataResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != metadataTimelineAPILimit || !result.Truncated || result.Current == nil || result.Current.Title == nil || *result.Current.Title != "title-259" {
		t.Fatalf("metadata projection count=%d truncated=%t current=%+v", len(result.Items), result.Truncated, result.Current)
	}
	if result.Items[0].Title == nil || *result.Items[0].Title != "title-004" || result.Items[len(result.Items)-1].Title == nil || *result.Items[len(result.Items)-1].Title != "title-259" {
		t.Fatalf("metadata tail was not ordered: first=%+v last=%+v", result.Items[0], result.Items[len(result.Items)-1])
	}
}

func TestShardedRecordingEventsUseManifestAndGapIterators(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 3)
	at := time.Date(2026, 10, 9, 3, 0, 0, 0, time.UTC)
	for index := 0; index < 2; index++ {
		fetchedAt := at.Add(time.Duration(index) * time.Minute)
		body := []byte(fmt.Sprintf("#EXTM3U\n# manifest %d\n", index))
		snapshot, err := store.SaveSnapshot(id, "main", "https://example.invalid/source.m3u8", body, fetchedAt)
		if err != nil {
			t.Fatalf("save manifest %d: %v", index, err)
		}
		if err := store.AppendShardedManifest(context.Background(), id, snapshot); err != nil {
			t.Fatalf("append manifest %d: %v", index, err)
		}
	}
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/events", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("events status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Items []struct {
			Type  string    `json:"type"`
			At    time.Time `json:"at"`
			Count int       `json:"count"`
		} `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	byType := make(map[string]struct {
		At    time.Time
		Count int
	})
	for _, event := range result.Items {
		byType[event.Type] = struct {
			At    time.Time
			Count int
		}{At: event.At, Count: event.Count}
	}
	if got := byType["manifest_observed"]; got.Count != 2 || !got.At.Equal(at) {
		t.Fatalf("manifest event=%+v", got)
	}
	if got := byType["gap_detected"]; got.Count != 2 || !got.At.Equal(time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("gap event=%+v", got)
	}
}

func TestShardedVODPlaylistStreams100KLocalFilesystemTimelineRecords(t *testing.T) {
	if os.Getenv("IR_RUN_SCALE_TESTS") != "1" {
		t.Skip("set IR_RUN_SCALE_TESTS=1 to run large local filesystem timeline acceptance")
	}
	const count = 100_000
	store, id := newLocalV2TimelineStore(t, count)
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/tracks/main/playlist.m3u8", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("100k playlist status=%d body=%s", response.Code, response.Body.String()[:min(response.Body.Len(), 500)])
	}
	body := response.Body.String()
	if got := strings.Count(body, "#EXTINF:"); got != count {
		t.Fatalf("playlist entry count=%d, want %d", got, count)
	}
	if !strings.HasSuffix(body, "#EXT-X-ENDLIST\n") || !strings.Contains(body, "/play/segments/seg-00000000000000000001\n") || !strings.Contains(body, "/play/segments/seg-00000000000000100000\n") {
		t.Fatalf("100k playlist is incomplete: prefix=%q suffix=%q", body[:min(len(body), 200)], body[max(0, len(body)-200):])
	}
	first := strings.Index(body, "/play/segments/seg-00000000000000000001\n")
	last := strings.Index(body, "/play/segments/seg-00000000000000100000\n")
	if first < 0 || last <= first {
		t.Fatalf("100k timeline order is invalid: first=%d last=%d", first, last)
	}
	header, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(header.Tracks["main"].Segments) != 0 || header.Tracks["main"].MediaCount != count {
		t.Fatalf("100k v2 header materialized history or lost count: segments=%d count=%d", len(header.Tracks["main"].Segments), header.Tracks["main"].MediaCount)
	}
}

type v2MediaPageFixture struct {
	Version int                     `json:"version"`
	TrackID string                  `json:"track_id"`
	Number  uint64                  `json:"number"`
	Entries []storage.V2MediaRecord `json:"entries"`
}

type v2TimelineEntryFixture struct {
	Coordinate              archiveindex.Coordinate `json:"coordinate"`
	ArchiveOrdinal          uint64                  `json:"archive_ordinal,omitempty"`
	InitOrdinal             uint64                  `json:"init_ordinal,omitempty"`
	ID                      string                  `json:"id"`
	Duration                float64                 `json:"duration"`
	ProgramDateTime         *time.Time              `json:"program_date_time,omitempty"`
	InitSegmentID           string                  `json:"init_segment_id,omitempty"`
	Discontinuity           bool                    `json:"discontinuity,omitempty"`
	ByteRange               *domain.ByteRange       `json:"byte_range,omitempty"`
	LivePresentationOrdinal uint64                  `json:"live_presentation_ordinal,omitempty"`
	LiveDiscontinuity       bool                    `json:"live_discontinuity,omitempty"`
	LiveDiscontinuitySeq    uint64                  `json:"live_discontinuity_sequence,omitempty"`
}

type v2TimelinePageFixture struct {
	Version               int                      `json:"version"`
	TrackID               string                   `json:"track_id"`
	SourceEpoch           uint64                   `json:"source_epoch"`
	DiscontinuitySequence uint64                   `json:"discontinuity_sequence"`
	SequenceBucket        uint64                   `json:"sequence_bucket"`
	Kind                  archiveindex.ObjectKind  `json:"kind"`
	Entries               []v2TimelineEntryFixture `json:"entries"`
}

// newLocalV2TimelineStore writes actual bounded shards under the local Store
// root. Direct fixture writes avoid 100k fsyncs through append APIs; the HTTP
// route and Store iterator still read the canonical on-disk sidecars.
func newLocalV2TimelineStore(t *testing.T, count int) (*storage.Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "d123456789abcdef0123456789abcdef"
	const trackID = "main"
	sessionID := "session-" + strings.Repeat("b", 64)
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion, ID: id, SourceSessionID: sessionID,
		State: domain.StateCompleted, CreatedAt: now, StartedAt: now,
		ShardedArchive: &domain.ShardedArchiveSummary{MediaCount: uint64(count), PayloadBytes: uint64(count), DurationSeconds: float64(count)},
		Tracks: map[string]*domain.Track{trackID: {
			ID: trackID, NextArchiveOrdinal: uint64(count) + 1, MediaCount: uint64(count),
			MediaHighWater: uint64(count), PayloadBytes: uint64(count), DurationSeconds: float64(count),
		}},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	recordingRoot := filepath.Join(root, "recordings", id)
	trackKeyBytes := sha256.Sum256([]byte(trackID))
	trackKey := hex.EncodeToString(trackKeyBytes[:16])
	pageRoot := filepath.Join(recordingRoot, "archive", "v2", "media", trackKey)
	if err := os.MkdirAll(pageRoot, 0700); err != nil {
		t.Fatal(err)
	}
	writeSidecar := func(relative string, value any) {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		filename := filepath.Join(recordingRoot, filepath.FromSlash(relative+".json"))
		if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
			t.Fatalf("create local v2 sidecar directory %s: %v", relative, err)
		}
		if err := os.WriteFile(filename, data, 0600); err != nil {
			t.Fatalf("write local v2 sidecar %s: %v", relative, err)
		}
	}
	pageEntries := make([]storage.V2MediaRecord, 0, storage.MediaShardMaxEntries)
	timelinePages := make(map[string]*v2TimelinePageFixture)
	payloadHash := strings.Repeat("a", 64)
	for ordinal := uint64(1); ordinal <= uint64(count); ordinal++ {
		pageNo := (ordinal - 1) / storage.MediaShardMaxEntries
		epoch := (ordinal-1)/storage.ArchiveShardMaxEntries + 1
		coordinate := archiveindex.Coordinate{
			SessionID: sessionID, TrackID: trackID, SourceEpoch: epoch, Sequence: ordinal,
			Kind: archiveindex.ObjectMedia,
		}
		record := storage.V2MediaRecord{
			Coordinate:   coordinate,
			IndexOrdinal: ordinal,
			Segment: domain.Segment{
				ID: fmt.Sprintf("seg-%020d", ordinal), TrackID: trackID, Sequence: ordinal,
				SourceEpoch: epoch, ArchiveOrdinal: ordinal, Duration: 1,
				StoragePath: "tracks/main/shared.ts", PayloadSize: 1, SHA256: payloadHash,
			},
		}
		pageEntries = append(pageEntries, record)
		timelineRelative := fmt.Sprintf("archive/v2/timeline/%s/media/%020d/%020d/%020d", trackKey, coordinate.SourceEpoch, coordinate.DiscontinuitySequence, coordinate.Sequence/storage.MediaShardMaxEntries)
		timelinePage := timelinePages[timelineRelative]
		if timelinePage == nil {
			timelinePage = &v2TimelinePageFixture{
				Version: 1, TrackID: trackID, SourceEpoch: coordinate.SourceEpoch,
				DiscontinuitySequence: coordinate.DiscontinuitySequence,
				SequenceBucket:        coordinate.Sequence / storage.MediaShardMaxEntries,
				Kind:                  archiveindex.ObjectMedia,
			}
			timelinePages[timelineRelative] = timelinePage
		}
		timelinePage.Entries = append(timelinePage.Entries, v2TimelineEntryFixture{
			Coordinate: coordinate, ArchiveOrdinal: ordinal, ID: record.Segment.ID,
			Duration: record.Segment.Duration, InitSegmentID: record.Segment.InitSegmentID,
			Discontinuity: record.Segment.Discontinuity, ByteRange: record.Segment.ByteRange,
		})
		if len(pageEntries) == storage.MediaShardMaxEntries || ordinal == uint64(count) {
			page := v2MediaPageFixture{Version: 1, TrackID: trackID, Number: pageNo, Entries: append([]storage.V2MediaRecord(nil), pageEntries...)}
			pagePath := fmt.Sprintf("archive/v2/media/%s/%020d", trackKey, pageNo)
			writeSidecar(pagePath, page)
			pageEntries = pageEntries[:0]
		}
	}
	for relative, page := range timelinePages {
		writeSidecar(relative, page)
	}
	return store, id
}

func newShardedVODFixture(t *testing.T, count int) (*storage.Store, string, map[uint64][]byte) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "c123456789abcdef0123456789abcdef"
	sessionID := "session-" + strings.Repeat("a", 64)
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion:   storage.ShardedArchiveFormatVersion,
		ID:              id,
		SourceSessionID: sessionID,
		State:           domain.StateCompleted,
		CreatedAt:       now,
		StartedAt:       now,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Bandwidth: 2_400_000, NextArchiveOrdinal: 1},
		},
		ShardedArchive: &domain.ShardedArchiveSummary{},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatalf("create sharded fixture: %v", err)
	}
	for _, init := range []struct {
		id       string
		sequence uint64
		payload  []byte
	}{{"init-vod-a", 1000, []byte("init-a-payload")}, {"init-vod-b", 1003, []byte("init-b-payload")}} {
		path := "tracks/main/" + init.id + ".mp4"
		result, err := store.SavePayload(id, path, bytes.NewReader(init.payload), int64(len(init.payload)))
		if err != nil {
			t.Fatalf("save init payload %s: %v", init.id, err)
		}
		record := storage.V2MediaRecord{
			Coordinate: archiveindex.Coordinate{SessionID: sessionID, TrackID: "main", SourceEpoch: 1, Sequence: init.sequence, Kind: archiveindex.ObjectInit},
			Segment:    domain.Segment{ID: init.id, TrackID: "main", SourceEpoch: 1, Sequence: init.sequence, SourceURI: "https://example.invalid/" + init.id, IsInit: true, StoragePath: path, PayloadSize: result.Size, SHA256: result.SHA256},
		}
		if err := store.AppendShardedMedia(context.Background(), id, record); err != nil {
			t.Fatalf("append init record %s: %v", init.id, err)
		}
	}
	if err := store.AppendShardedGap(context.Background(), id, domain.Gap{
		TrackID: "main", SourceEpoch: 1, FromSequence: 1000, ToSequence: 1001,
		DetectedAt: now, Reason: "repaired source slot",
	}); err != nil {
		t.Fatalf("append repaired gap: %v", err)
	}
	if err := store.AppendShardedGap(context.Background(), id, domain.Gap{
		TrackID: "main", SourceEpoch: 1, FromSequence: 1020, ToSequence: 1021,
		DetectedAt: now.Add(time.Second), Reason: "source manifest gap",
	}); err != nil {
		t.Fatalf("append missing-slot gap: %v", err)
	}

	payloads := make(map[uint64][]byte, count)
	for ordinal := 1; ordinal <= count; ordinal++ {
		sequence := uint64(999 + ordinal)
		if ordinal > 21 {
			sequence++
		}
		switch ordinal {
		case 2:
			sequence = 1002
		case 3:
			sequence = 1001
		case 21:
			sequence = 1021
		}
		epoch := uint64(1)
		if ordinal == count && count > 4 {
			epoch = 2
			sequence = 1
		}
		payload := []byte(fmt.Sprintf("v2-payload-%03d-range", ordinal))
		payloads[uint64(ordinal)] = payload
		path := fmt.Sprintf("tracks/main/segment-%020d.ts", ordinal)
		result, err := store.SavePayload(id, path, bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatalf("save media payload %d: %v", ordinal, err)
		}
		initID := "init-vod-a"
		if sequence >= 1003 || epoch == 2 {
			initID = "init-vod-b"
		}
		segment := domain.Segment{
			ID: fmt.Sprintf("seg-%020d", ordinal), TrackID: "main", Sequence: sequence,
			SourceEpoch: epoch, ArchiveOrdinal: uint64(ordinal), SourceURI: "https://example.invalid/source-secret.ts",
			Duration: 1.25, InitSegmentID: initID, StoragePath: path,
			PayloadSize: result.Size, SHA256: result.SHA256, Discontinuity: ordinal == 200,
		}
		coordinate := archiveindex.Coordinate{
			SessionID: sessionID, TrackID: "main", SourceEpoch: epoch,
			Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}
		if err := store.AppendShardedMedia(context.Background(), id, storage.V2MediaRecord{Coordinate: coordinate, Segment: segment}); err != nil {
			t.Fatalf("append media record %d: %v", ordinal, err)
		}
	}
	return store, id, payloads
}
