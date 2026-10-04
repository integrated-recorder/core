package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if !strings.Contains(body, `#EXT-X-MAP:URI="/api/recordings/`+recordingID+`/play/segments/`) {
		t.Fatalf("live playlist omitted the first selected init map:\n%s", body)
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
		if index == 2 {
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
