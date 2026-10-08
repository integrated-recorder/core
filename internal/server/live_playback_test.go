package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestActiveLivePlaylistsAreBoundedSafeArchiveViews(t *testing.T) {
	const count = 14
	handler, manager, recordingID := newLivePlaybackFixture(t, count)
	defer closeLivePlaybackFixture(t, manager)

	master := requestLivePlaylist(handler, "/api/recordings/"+recordingID+"/play/live/master.m3u8")
	if master.Code != http.StatusOK {
		t.Fatalf("live master status=%d body=%s", master.Code, master.Body.String())
	}
	if !strings.Contains(master.Body.String(), "/play/live/tracks/main/playlist.m3u8") {
		t.Fatalf("live master did not point at same-origin archive route: %s", master.Body.String())
	}

	response := requestLivePlaylist(handler, "/api/recordings/"+recordingID+"/play/live/tracks/main/playlist.m3u8")
	if response.Code != http.StatusOK {
		t.Fatalf("live playlist status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if strings.Contains(body, "#EXT-X-ENDLIST") || strings.Contains(body, "#EXT-X-PLAYLIST-TYPE:VOD") {
		t.Fatalf("live playlist has terminal VOD markers:\n%s", body)
	}
	if !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:2") {
		t.Fatalf("live playlist media sequence must start at archive ordinal 3:\n%s", body)
	}
	if !strings.Contains(body, "#EXT-X-DISCONTINUITY-SEQUENCE:1") {
		t.Fatalf("live playlist must account for the removed discontinuity:\n%s", body)
	}
	if !strings.Contains(body, "#EXT-X-DISCONTINUITY\n") {
		t.Fatalf("live playlist omitted an in-window discontinuity:\n%s", body)
	}
	if got := strings.Count(body, "#EXT-X-DISCONTINUITY\n"); got != 1 {
		t.Fatalf("one source discontinuity was emitted %d times:\n%s", got, body)
	}
	if !strings.Contains(body, `#EXT-X-MAP:URI="/api/recordings/`+recordingID+`/play/segments/`) {
		t.Fatalf("live playlist omitted the first selected init map:\n%s", body)
	}
	if got := strings.Count(body, "#EXT-X-MAP:URI="); got != 3 {
		t.Fatalf("live playlist map transitions=%d, want the initial map plus discontinuity/map changes:\n%s", got, body)
	}
	if strings.Contains(body, "access_token") || strings.Contains(body, "source-secret") || strings.Contains(body, "source.invalid") || strings.Contains(body, "127.0.0.1") || strings.Contains(body, "localhost") {
		t.Fatalf("live playlist leaked source URI data:\n%s", body)
	}

	var mediaURIs []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "/api/recordings/") && strings.Contains(line, "/play/segments/") {
			mediaURIs = append(mediaURIs, line)
		}
	}
	if len(mediaURIs) != livePlaylistSegmentLimit {
		t.Fatalf("live playlist has %d local payload references, want %d:\n%s", len(mediaURIs), livePlaylistSegmentLimit, body)
	}
	for index, uri := range mediaURIs {
		want := fmt.Sprintf("/play/segments/seg-%020d", index+3)
		if !strings.HasSuffix(uri, want) {
			t.Errorf("media URI[%d]=%q, want archive order suffix %q", index, uri, want)
		}
	}
	firstMapping := livePlaylistURISequences(t, body)
	if len(firstMapping) != livePlaylistSegmentLimit {
		t.Fatalf("inferred sequence mapping has %d items, want %d", len(firstMapping), livePlaylistSegmentLimit)
	}
	for index, uri := range mediaURIs {
		if firstMapping[uri] != uint64(index+2) {
			t.Errorf("URI %s has inferred sequence %d, want %d", uri, firstMapping[uri], index+2)
		}
	}
	reloaded := requestLivePlaylist(handler, "/api/recordings/"+recordingID+"/play/live/tracks/main/playlist.m3u8")
	if reloaded.Code != http.StatusOK {
		t.Fatalf("reloaded live playlist status=%d body=%s", reloaded.Code, reloaded.Body.String())
	}
	if got := livePlaylistURISequences(t, reloaded.Body.String()); !sameSequenceMapping(firstMapping, got) {
		t.Fatalf("overlap URI sequence mapping changed across reload: before=%v after=%v", firstMapping, got)
	}
	if response.Header().Get("Content-Type") != "application/vnd.apple.mpegurl; charset=utf-8" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("live playlist headers content-type=%q cache=%q", response.Header().Get("Content-Type"), response.Header().Get("Cache-Control"))
	}

	terminal, err := manager.Stop(recordingID)
	if err != nil {
		t.Fatalf("stop active fixture: %v", err)
	}
	if terminal.State != domain.StateStopped {
		t.Fatalf("recording state after stop=%q", terminal.State)
	}
	vod := requestLivePlaylist(handler, "/api/recordings/"+recordingID+"/play/tracks/main/playlist.m3u8")
	if vod.Code != http.StatusOK || !strings.Contains(vod.Body.String(), "#EXT-X-ENDLIST") || !strings.Contains(vod.Body.String(), "#EXT-X-PLAYLIST-TYPE:VOD") {
		t.Fatalf("terminal VOD contract changed: status=%d body=%s", vod.Code, vod.Body.String())
	}
	if strings.Count(vod.Body.String(), "/play/segments/seg-") != count {
		t.Fatalf("terminal VOD playlist did not retain all %d segments:\n%s", count, vod.Body.String())
	}
	for _, path := range []string{
		"/api/recordings/" + recordingID + "/play/live/master.m3u8",
		"/api/recordings/" + recordingID + "/play/live/tracks/main/playlist.m3u8",
	} {
		response := requestLivePlaylist(handler, path)
		if response.Code != http.StatusConflict {
			t.Errorf("live route %s after stop status=%d, want 409", path, response.Code)
		}
	}
}

func TestLivePlaylistProtocolIdentitySurvivesAutomaticHistoricalPrefixAndWindowSlide(t *testing.T) {
	initialLive := numberedLiveManifestWithGap(100, 113, 108)
	historical := numberedHistoricalManifest(90, 113)
	transport := &liveRecoveryFixtureTransport{
		live: initialLive, historical: historical,
		historySeen: make(chan struct{}), releaseHistory: make(chan struct{}),
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fence := livePlaybackTestFence{}
	manager, err := acquire.NewManagerWithFencedRecovery(store, &http.Client{Transport: transport}, nil,
		func(context.Context, string) error { return nil }, fence, fence)
	if err != nil {
		t.Fatal(err)
	}
	defer closeLivePlaybackFixture(t, manager)
	media := adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://source.invalid/live.m3u8",
		HistoricalAvailability: &adapterproto.HistoricalAvailability{
			Mode: adapterproto.HistoricalModeManifest, HistoricalManifestURL: "https://source.invalid/history.m3u8",
		},
	}
	owner := acquire.OwnershipToken{RecordingID: "f04f9fef9d4241c8b7307621e9f95a01", EngineGeneration: "generation-a", WorkerInstance: "worker-a", Epoch: 1}
	if err := manager.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (acquire.OwnershipToken, error) { return owner, nil }); err != nil {
		t.Fatal(err)
	}
	started, err := manager.StartResolvedWithIDOwned(context.Background(), owner, owner.RecordingID, "fixture", media, nil, "Live repair fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.historySeen:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic historical scheduler did not request the declared manifest")
	}
	waitLiveFixture(t, manager, started.ID, 13)
	handler := New(manager, nil, nil)
	first := requestLivePlaylist(handler, "/api/recordings/"+started.ID+"/play/live/tracks/main/playlist.m3u8")
	if first.Code != http.StatusOK {
		t.Fatalf("initial live playlist status=%d body=%s", first.Code, first.Body.String())
	}
	firstMapping := livePlaylistURISequences(t, first.Body.String())
	close(transport.releaseHistory)
	waitLiveFixture(t, manager, started.ID, 24)

	second := requestLivePlaylist(handler, "/api/recordings/"+started.ID+"/play/live/tracks/main/playlist.m3u8")
	if second.Code != http.StatusOK {
		t.Fatalf("post-repair live playlist status=%d body=%s", second.Code, second.Body.String())
	}
	postRepairMapping := livePlaylistURISequences(t, second.Body.String())
	for uri, sequence := range firstMapping {
		if got, stillPresent := postRepairMapping[uri]; stillPresent && got != sequence {
			t.Fatalf("historical prefix/gap repair renumbered overlap URI %s: %d -> %d", uri, sequence, got)
		}
	}
	root, err := manager.Get(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if root.TimelineRevision < 2 || root.Tracks["main"].Segments[0].TimelineOrdinal == 0 {
		t.Fatalf("historical prefix did not update canonical VOD timeline: revision=%d", root.TimelineRevision)
	}
	var repairedGap domain.Segment
	for _, segment := range root.Tracks["main"].Segments {
		if segment.Sequence == 108 {
			repairedGap = segment
			break
		}
	}
	if repairedGap.ID == "" || postRepairMapping["/api/recordings/"+started.ID+"/play/segments/"+repairedGap.ID] != 8 {
		t.Fatalf("historically repaired live slot did not retain HLS sequence 8: segment=%#v mapping=%v", repairedGap, postRepairMapping)
	}

	transport.setLive(numberedLiveManifest(102, 116))
	waitLiveFixture(t, manager, started.ID, 27)
	third := requestLivePlaylist(handler, "/api/recordings/"+started.ID+"/play/live/tracks/main/playlist.m3u8")
	if third.Code != http.StatusOK {
		t.Fatalf("slid live playlist status=%d body=%s", third.Code, third.Body.String())
	}
	slidMapping := livePlaylistURISequences(t, third.Body.String())
	for uri, sequence := range postRepairMapping {
		if got, stillPresent := slidMapping[uri]; stillPresent && got != sequence {
			t.Errorf("sliding window changed overlap identity for %s: %d -> %d", uri, sequence, got)
		}
	}
	if len(slidMapping) != acquire.LivePlaybackWindowSize {
		t.Fatalf("slid playlist contains %d media objects, want %d", len(slidMapping), acquire.LivePlaybackWindowSize)
	}
	for uri, sequence := range slidMapping {
		if strings.HasSuffix(uri, "/seg-116.ts") && sequence != 16 {
			t.Errorf("new live segment sequence=%d, want 16", sequence)
		}
	}
}

type liveRecoveryFixtureTransport struct {
	mu             sync.Mutex
	live           string
	historical     string
	historySeen    chan struct{}
	releaseHistory chan struct{}
	historyOnce    sync.Once
}

type livePlaybackTestFence struct{}

func (livePlaybackTestFence) WithCommit(_ acquire.OwnershipToken, commit func() error) error {
	return commit()
}

func (livePlaybackTestFence) WithUnownedCommit(_ string, commit func() error) error {
	return commit()
}

func (livePlaybackTestFence) WithFencedRecovery(recover func() error) error {
	return recover()
}

func (transport *liveRecoveryFixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, "fixture media bytes"
	switch {
	case request.URL.Path == "/live.m3u8":
		transport.mu.Lock()
		body = transport.live
		transport.mu.Unlock()
	case request.URL.Path == "/history.m3u8":
		transport.historyOnce.Do(func() { close(transport.historySeen) })
		select {
		case <-request.Context().Done():
			return nil, request.Context().Err()
		case <-transport.releaseHistory:
		}
		body = transport.historical
	case strings.HasPrefix(request.URL.Path, "/seg-"):
	default:
		status, body = http.StatusNotFound, "not found"
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request, ContentLength: int64(len(body))}, nil
}

func (transport *liveRecoveryFixtureTransport) setLive(manifest string) {
	transport.mu.Lock()
	transport.live = manifest
	transport.mu.Unlock()
}

func numberedLiveManifest(first, last uint64) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", first)
	for sequence := first; sequence <= last; sequence++ {
		fmt.Fprintf(&builder, "#EXTINF:2.0,\nseg-%d.ts\n", sequence)
	}
	return builder.String()
}

func numberedLiveManifestWithGap(first, last, gap uint64) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", first)
	for sequence := first; sequence <= last; sequence++ {
		fmt.Fprintf(&builder, "#EXTINF:2.0,\n")
		if sequence == gap {
			builder.WriteString("#EXT-X-GAP\n")
		}
		fmt.Fprintf(&builder, "seg-%d.ts\n", sequence)
	}
	return builder.String()
}

func numberedHistoricalManifest(first, last uint64) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n", first)
	for sequence := first; sequence <= last; sequence++ {
		fmt.Fprintf(&builder, "#EXTINF:2.0,\nseg-%d.ts\n", sequence)
	}
	builder.WriteString("#EXT-X-ENDLIST\n")
	return builder.String()
}

func waitLiveFixture(t *testing.T, manager *acquire.Manager, recordingID string, count int) {
	t.Helper()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		recording, err := manager.Get(recordingID)
		if err != nil {
			t.Fatal(err)
		}
		if len(recording.Tracks["main"].Segments) >= count {
			return
		}
		if recording.State != domain.StateRecording {
			t.Fatalf("fixture recording became terminal: state=%s error=%s", recording.State, recording.LastError)
		}
		select {
		case <-deadline.C:
			t.Fatalf("recording has %d segments, want at least %d", len(recording.Tracks["main"].Segments), count)
		case <-ticker.C:
		}
	}
}

func livePlaylistURISequences(t *testing.T, body string) map[string]uint64 {
	t.Helper()
	mediaSequence := uint64(0)
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:") {
			if _, err := fmt.Sscanf(line, "#EXT-X-MEDIA-SEQUENCE:%d", &mediaSequence); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:") {
		t.Fatal("live playlist has no media sequence")
	}
	sequences := make(map[string]uint64)
	sequence := mediaSequence
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "/api/recordings/") && (strings.Contains(line, "/play/segments/") || strings.Contains(line, "/play/live/gaps/")) {
			if strings.Contains(line, "/play/segments/") {
				sequences[line] = sequence
			}
			sequence++
		}
	}
	return sequences
}

func sameSequenceMapping(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for uri, sequence := range left {
		if right[uri] != sequence {
			return false
		}
	}
	return true
}

func TestLivePlaylistBeforeFirstSegmentReturnsConflict(t *testing.T) {
	handler, manager, recordingID := newLivePlaybackFixture(t, 0)
	defer closeLivePlaybackFixture(t, manager)
	for _, path := range []string{
		"/api/recordings/" + recordingID + "/play/live/master.m3u8",
		"/api/recordings/" + recordingID + "/play/live/tracks/main/playlist.m3u8",
	} {
		response := requestLivePlaylist(handler, path)
		if response.Code != http.StatusConflict {
			t.Errorf("empty live route %s status=%d body=%s, want 409", path, response.Code, response.Body.String())
		}
	}
}

func TestPendingLiveAcquisitionIsNotPublishedAsKnownGap(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "e0100101010101010101010101010101"
	result, err := store.SavePayload(recordingID, "payloads/live.bin", strings.NewReader("first-segment"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	first := domain.Segment{
		ID: "seg-00000000000000000001", TrackID: "main", StoragePath: "payloads/live.bin",
		Duration: 1, PayloadSize: result.Size, SHA256: result.SHA256, LivePresentationOrdinal: 100,
	}
	view := acquire.LivePlaybackView{
		RecordingID: recordingID, State: domain.StateRecording, TrackID: "main", TrackFound: true,
		Bandwidth: 1_000_000, Segments: []domain.Segment{first},
		Slots: []acquire.LivePlaybackSlot{
			{Ordinal: 100, Segment: &first, Duration: 1},
			{Ordinal: 101, Pending: true, Duration: 1},
		},
	}
	handler := NewWithOptions(liveSnapshotFixtureManager{view: view}, nil, nil, Options{Storage: store})
	response := requestLivePlaylist(handler, "/api/recordings/"+recordingID+"/play/live/tracks/main/playlist.m3u8")
	if response.Code != http.StatusOK {
		t.Fatalf("live playlist status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if strings.Contains(body, "#EXT-X-GAP") || strings.Contains(body, "/play/segments/seg-00000000000000000002") {
		t.Fatalf("pending acquisition was published as a known gap or media URI:\n%s", body)
	}
	if !strings.Contains(body, "/play/segments/seg-00000000000000000001") {
		t.Fatalf("playlist omitted the committed prefix before pending media:\n%s", body)
	}
}

func newLivePlaybackFixture(t *testing.T, segmentCount int) (http.Handler, *acquire.Manager, string) {
	t.Helper()
	manifest := strings.Builder{}
	manifest.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:100\n")
	if segmentCount > 0 {
		manifest.WriteString("#EXT-X-MAP:URI=\"init-one.mp4\"\n")
	}
	for index := 0; index < segmentCount; index++ {
		if index == 1 || index == 5 {
			manifest.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if index == 6 {
			manifest.WriteString("#EXT-X-MAP:URI=\"init-two.mp4\"\n")
		}
		fmt.Fprintf(&manifest, "#EXTINF:1.0,\nseg-%03d.ts\n", index+1)
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, &http.Client{Transport: liveFixtureTransport{manifest: manifest.String()}}, nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	started, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{
		Type:        "hls",
		ManifestURL: "https://source.invalid/live.m3u8?access_token=source-secret",
	}, nil, "Live fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		recording, getErr := manager.Get(started.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if len(recording.Tracks["main"].Segments) >= segmentCount {
			break
		}
		if recording.State != domain.StateRecording {
			t.Fatalf("fixture recording became terminal before all segments were committed: state=%s err=%s", recording.State, recording.LastError)
		}
		time.Sleep(10 * time.Millisecond)
	}
	recording, err := manager.Get(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(recording.Tracks["main"].Segments) != segmentCount {
		t.Fatalf("committed segments=%d, want %d; state=%s last_error=%s", len(recording.Tracks["main"].Segments), segmentCount, recording.State, recording.LastError)
	}
	return New(manager, nil, nil), manager, started.ID
}

type liveFixtureTransport struct{ manifest string }

type liveSnapshotFixtureManager struct {
	recordingManager
	view acquire.LivePlaybackView
}

func (manager liveSnapshotFixtureManager) LivePlaybackSnapshot(context.Context, string) (acquire.LivePlaybackView, error) {
	return manager.view, nil
}

func (transport liveFixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, "fixture payload"
	switch {
	case request.URL.Path == "/live.m3u8":
		body = transport.manifest
	case strings.HasPrefix(request.URL.Path, "/seg-") || strings.HasPrefix(request.URL.Path, "/init-"):
	default:
		status, body = http.StatusNotFound, "not found"
	}
	return &http.Response{
		StatusCode:    status,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		Request:       request,
		ContentLength: int64(len(body)),
	}, nil
}

func requestLivePlaylist(handler http.Handler, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	return response
}

func closeLivePlaybackFixture(t *testing.T, manager *acquire.Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Errorf("close live playback fixture: %v", err)
	}
}
