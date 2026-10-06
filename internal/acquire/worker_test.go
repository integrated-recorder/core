package acquire

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestObservePlaylistRecordsWindowAndManifestGaps(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	recording := &domain.Recording{ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", HasLastObservedSequence: true, LastObservedSequence: 9, Segments: []domain.Segment{{ID: "old", TrackID: "main", Sequence: 9}}}}}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording}
	playlist := hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 11}, {Sequence: 13}}}
	if err = manager.observePlaylist(e, playlist); err != nil {
		t.Fatal(err)
	}
	if len(e.recording.Gaps) != 2 || e.recording.Gaps[0].FromSequence != 10 || e.recording.Gaps[0].ToSequence != 10 || e.recording.Gaps[1].FromSequence != 12 {
		t.Fatalf("gaps = %#v", e.recording.Gaps)
	}

	zeroID := "1234567890abcdef1234567890abcdef"
	if err = store.NewRecordingDir(zeroID); err != nil {
		t.Fatal(err)
	}
	zeroRecording := &domain.Recording{ID: zeroID, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", HasLastObservedSequence: true, LastObservedSequence: 0, Segments: []domain.Segment{{ID: "zero", TrackID: "main", Sequence: 0}}}}}
	if err = store.SaveRecording(zeroRecording); err != nil {
		t.Fatal(err)
	}
	zeroEntry := &entry{recording: zeroRecording}
	if err = manager.observePlaylist(zeroEntry, hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 2}}}); err != nil {
		t.Fatal(err)
	}
	if len(zeroEntry.recording.Gaps) != 1 || zeroEntry.recording.Gaps[0].FromSequence != 1 || zeroEntry.recording.Gaps[0].ToSequence != 1 {
		t.Fatalf("sequence zero gap = %#v", zeroEntry.recording.Gaps)
	}
}

func TestProcessRecordsExplicitManifestGapWithoutFetchingIt(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "0987654321abcdef0987654321abcdef"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	recording := &domain.Recording{ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording}
	done, err := manager.process(e, context.Background(), hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 7, URI: "http://example.invalid/missing.ts", Duration: 1, Gap: true}}})
	if err != nil || done {
		t.Fatalf("process() = done %v, err %v", done, err)
	}
	if len(e.recording.Tracks["main"].Segments) != 0 || len(e.recording.Gaps) != 1 || e.recording.Gaps[0].FromSequence != 7 || e.recording.Gaps[0].ToSequence != 7 {
		t.Fatalf("manifest gap was not recorded without capture: %#v", e.recording)
	}
}

func TestWorkerTransformsManifestVariantAndRelativeMediaAgainstFetchedURLs(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	segmentBytes := []byte("transform-fixture-exact-segment")
	transport := testRoundTripper(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		requests = append(requests, request.URL.String())
		mu.Unlock()
		var body []byte
		switch request.URL.Path {
		case "/archive/live.m3u8":
			body = []byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=500000\nvariant.ts\n")
		case "/archive/variant.m3u8":
			body = []byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:41\n#EXTINF:1,\nsegment.ts\n#EXT-X-ENDLIST\n")
		case "/archive/segment.m4v":
			body = segmentBytes
		default:
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil)), Request: request}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
	})
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{Transport: transport}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		_ = manager.Close(closeCtx)
	})
	policy := &adapterproto.URLTransformPolicy{Rules: []adapterproto.URLTransformRule{
		{Scopes: []adapterproto.ResourceRequestScope{adapterproto.RequestScopeManifest}, PathSuffix: &adapterproto.PathSuffixRewrite{From: ".ts", To: ".m3u8"}, QueryParameters: []adapterproto.QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}}},
		{Scopes: []adapterproto.ResourceRequestScope{adapterproto.RequestScopeVariant}, PathSuffix: &adapterproto.PathSuffixRewrite{From: ".ts", To: ".m3u8"}, QueryParameters: []adapterproto.QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}}},
		{Scopes: []adapterproto.ResourceRequestScope{adapterproto.RequestScopeMedia}, PathSuffix: &adapterproto.PathSuffixRewrite{From: ".ts", To: ".m4v"}, QueryParameters: []adapterproto.QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}}},
	}}
	started, err := manager.StartResolved(ctx, "fixture", adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://media.example/archive/live.ts?hdnts=signed-value",
		RequestPolicy: &adapterproto.RequestPolicy{URLTransform: policy},
	}, nil, "transformed HLS", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForRecordingState(t, manager, started.ID, domain.StateCompleted)
	recording, err := manager.Get(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recording.SegmentCount() != 1 {
		t.Fatalf("captured segment count=%d, want 1", recording.SegmentCount())
	}
	const wantPlaylistURL = "https://media.example/archive/variant.m3u8?__bgda__=signed-value"
	if got := recording.Tracks["main"].SourcePlaylistURL; got != wantPlaylistURL {
		t.Fatalf("source playlist URL=%q, want actual transformed absolute URL %q", got, wantPlaylistURL)
	}
	segment := recording.Tracks["main"].Segments[0]
	if segment.SourceURI != "https://media.example/archive/segment.ts" {
		t.Fatalf("relative media URI resolved against wrong base: %q", segment.SourceURI)
	}
	assertStoredPayload(t, store, recording.ID, segment.StoragePath, segmentBytes)
	if digest := sha256.Sum256(segmentBytes); segment.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("stored segment digest=%s, want exact source bytes", segment.SHA256)
	}

	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	wantPaths := map[string]string{
		"/archive/live.m3u8":    "signed-value",
		"/archive/variant.m3u8": "signed-value",
		"/archive/segment.m4v":  "signed-value",
	}
	seen := make(map[string]bool, len(wantPaths))
	for _, raw := range gotRequests {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		wantQuery, ok := wantPaths[parsed.Path]
		if !ok {
			t.Errorf("unexpected transformed request URL %q", raw)
			continue
		}
		if parsed.Query().Get("__bgda__") != wantQuery {
			t.Errorf("request %q lacks propagated query value", raw)
		}
		seen[parsed.Path] = true
	}
	for path := range wantPaths {
		if !seen[path] {
			t.Errorf("transformed request path %q was not fetched; requests=%v", path, gotRequests)
		}
	}
}

func TestBlockedHeadDoesNotBlockNewerSegmentsAndKeepsArchiveOrder(t *testing.T) {
	var polls atomic.Int32
	var active, maximum atomic.Int32
	releaseHead := make(chan struct{})
	var releaseHeadOnce sync.Once
	headStarted := make(chan struct{})
	headRetried := make(chan struct{})
	secondPoll := make(chan struct{})
	var pollOnce sync.Once
	fastStarted := make(map[uint64]chan struct{}, 5)
	fastStartedOnce := make(map[uint64]*sync.Once, 5)
	for sequence := uint64(101); sequence <= 105; sequence++ {
		fastStarted[sequence] = make(chan struct{})
		fastStartedOnce[sequence] = &sync.Once{}
	}
	counts := make(map[uint64]*atomic.Int32, 6)
	for sequence := uint64(100); sequence <= 105; sequence++ {
		counts[sequence] = &atomic.Int32{}
	}
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			if polls.Add(1) == 2 {
				pollOnce.Do(func() { close(secondPoll) })
			}
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:100")
			for sequence := uint64(100); sequence <= 105; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,segment-%d\n%d.ts\n", sequence, sequence)
			}
			return
		}
		var sequence uint64
		if _, err := fmt.Sscanf(r.URL.Path, "/%d.ts", &sequence); err != nil || counts[sequence] == nil {
			http.NotFound(w, r)
			return
		}
		requestNumber := counts[sequence].Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		if sequence == 100 {
			if requestNumber == 1 {
				close(headStarted)
				<-releaseHead
				http.Error(w, "transient", http.StatusServiceUnavailable)
				return
			}
			close(headRetried)
		} else {
			fastStartedOnce[sequence].Do(func() { close(fastStarted[sequence]) })
		}
		_, _ = fmt.Fprintf(w, "original-payload-%d", sequence)
	}))
	defer func() {
		releaseHeadOnce.Do(func() { close(releaseHead) })
		server.Close()
	}()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}
	recording, err := manager.StartResolved(context.Background(), "fixture", media, nil, "blocked head", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, headStarted, "blocked sequence 100 request")
	for sequence := uint64(101); sequence <= 105; sequence++ {
		waitSignal(t, fastStarted[sequence], fmt.Sprintf("sequence %d before head release", sequence))
	}
	waitSignal(t, secondPoll, "manifest repoll while sequence 100 is blocked")
	waitForSegmentCount(t, manager, recording.ID, 5)
	if got := counts[100].Load(); got != 1 {
		t.Fatalf("sequence 100 duplicated during in-flight/retry rediscovery: requests=%d", got)
	}
	releaseHeadOnce.Do(func() { close(releaseHead) })
	waitSignal(t, headRetried, "sequence 100 retry")
	waitForSegmentCount(t, manager, recording.ID, 6)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got < 2 || got > maxSegmentWorkers {
		t.Fatalf("maximum simultaneous segment requests=%d, want 2..%d", got, maxSegmentWorkers)
	}
	segments := stopped.Tracks["main"].Segments
	if len(segments) != 6 {
		t.Fatalf("captured segments=%d want=6: %#v", len(segments), segments)
	}
	for index, sequence := range []uint64{100, 101, 102, 103, 104, 105} {
		segment := segments[index]
		if segment.Sequence != sequence || segment.ArchiveOrdinal != uint64(index+1) {
			t.Fatalf("segment[%d]=sequence %d ordinal %d, want sequence %d ordinal %d", index, segment.Sequence, segment.ArchiveOrdinal, sequence, index+1)
		}
		want := []byte(fmt.Sprintf("original-payload-%d", sequence))
		stored, readErr := os.ReadFile(filepath.Join(store.Root(), "recordings", recording.ID, filepath.FromSlash(segment.StoragePath)))
		if readErr != nil || !bytes.Equal(stored, want) {
			t.Fatalf("sequence %d payload=%q err=%v", sequence, stored, readErr)
		}
		digest := sha256.Sum256(want)
		if segment.SHA256 != hex.EncodeToString(digest[:]) || segment.SourceURI != fmt.Sprintf("%s/%d.ts", server.URL, sequence) {
			t.Fatalf("sequence %d metadata mismatch: %#v", sequence, segment)
		}
		wantRequests := int32(1)
		if sequence == 100 {
			wantRequests = 2
		}
		if got := counts[sequence].Load(); got != wantRequests {
			t.Fatalf("sequence %d HTTP requests=%d want=%d", sequence, got, wantRequests)
		}
	}
}

func waitForSegmentCount(t *testing.T, manager *Manager, id string, count int) {
	t.Helper()
	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		recording, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if recording.SegmentCount() >= count {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d captured segments; got %d", count, recording.SegmentCount())
		case <-ticker.C:
		}
	}
}

func TestInitialCatchUpUsesBoundedConcurrentWorkers(t *testing.T) {
	var active, maximum atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	fourActive := make(chan struct{})
	var fourOnce sync.Once
	var captured atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:100")
			for sequence := uint64(100); sequence < 108; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\n%d.ts\n", sequence)
			}
			return
		}
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		if current == maxSegmentWorkers {
			fourOnce.Do(func() { close(fourActive) })
		}
		<-release
		captured.Add(1)
		_, _ = io.WriteString(w, "catch-up")
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		server.Close()
	}()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "catch-up", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, fourActive, "four concurrent catch-up fetches")
	if got := maximum.Load(); got > maxSegmentWorkers {
		t.Fatalf("active segment requests=%d exceeds bound %d", got, maxSegmentWorkers)
	}
	releaseOnce.Do(func() { close(release) })
	waitForSegmentCount(t, manager, recording.ID, 8)
	if _, err = manager.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got < 2 || got > maxSegmentWorkers || captured.Load() != 8 {
		t.Fatalf("max concurrency=%d captured=%d", got, captured.Load())
	}
}

func TestSteadyStateUsesOneSegmentFetchAtATime(t *testing.T) {
	var polls, active, maximum atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			poll := polls.Add(1)
			if poll > 3 {
				poll = 3
			}
			_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n")
			for sequence := 1; sequence <= int(poll); sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\n%d.ts\n", sequence)
			}
			return
		}
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		_, _ = io.WriteString(w, "steady")
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "steady", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSegmentCount(t, manager, recording.ID, 3)
	if got := maximum.Load(); got != 1 {
		t.Fatalf("steady-state concurrent segment fetches=%d, want 1", got)
	}
	if _, err = manager.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
}

func TestSharedInitMapAcquiredOnceForConcurrentSegments(t *testing.T) {
	var initRequests atomic.Int32
	releaseInit := make(chan struct{})
	var releaseOnce sync.Once
	initStarted := make(chan struct{})
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-MAP:URI=\"init.mp4\"")
			for sequence := 1; sequence <= 3; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\n%d.m4s\n", sequence)
			}
		case "/init.mp4":
			if initRequests.Add(1) == 1 {
				close(initStarted)
			}
			<-releaseInit
			_, _ = io.WriteString(w, "init-bytes")
		default:
			_, _ = io.WriteString(w, "media-bytes")
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(releaseInit) })
		server.Close()
	}()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "init singleflight", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, initStarted, "initialization object request")
	waitForSchedulerBusy(t, manager, recording.ID, 3)
	if got := initRequests.Load(); got != 1 {
		t.Fatalf("init requests while all three segment workers are active=%d, want 1", got)
	}
	releaseOnce.Do(func() { close(releaseInit) })
	waitForSegmentCount(t, manager, recording.ID, 3)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stopped.Tracks["main"].InitSegments) != 1 || initRequests.Load() != 1 {
		t.Fatalf("init segment metadata=%d requests=%d", len(stopped.Tracks["main"].InitSegments), initRequests.Load())
	}
	for _, segment := range stopped.Tracks["main"].Segments {
		if segment.InitSegmentID != stopped.Tracks["main"].InitSegments[0].ID {
			t.Fatalf("media segment has wrong init reference: %#v", segment)
		}
	}
}

func waitForSchedulerBusy(t *testing.T, manager *Manager, id string, count int) {
	t.Helper()
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording entry disappeared")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		scheduler := activeScheduler(e)
		if scheduler == nil {
			t.Fatal("segment scheduler is not active")
		}
		scheduler.mu.Lock()
		busy, changed := scheduler.busy, scheduler.changed
		scheduler.mu.Unlock()
		if busy >= count {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("scheduler busy workers=%d, want at least %d", busy, count)
		case <-changed:
		}
	}
}

func TestConcurrentStatusFailuresRefreshOneMediaGenerationOnce(t *testing.T) {
	const segments = 4
	var oldRequests, newRequests atomic.Int32
	var newRequestMu sync.Mutex
	newRequestPaths := make(map[string]int)
	allOld := make(chan struct{})
	var oldOnce sync.Once
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old.m3u8":
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:20")
			for sequence := 20; sequence < 20+segments; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\nold-%d.ts\n", sequence)
			}
		case "/new.m3u8":
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:20")
			for sequence := 20; sequence < 20+segments; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\nnew-%d.ts\n", sequence)
			}
		default:
			if strings.HasPrefix(r.URL.Path, "/old-") {
				if oldRequests.Add(1) == segments {
					oldOnce.Do(func() { close(allOld) })
				}
				http.Error(w, "expired", http.StatusForbidden)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/new-") {
				newRequests.Add(1)
				newRequestMu.Lock()
				newRequestPaths[r.URL.Path]++
				newRequestMu.Unlock()
				_, _ = io.WriteString(w, "fresh-segment")
				return
			}
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	refreshStarted := make(chan struct{})
	var refreshOnce sync.Once
	resolver := &refreshingResolver{
		initial: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/old.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{OnHTTPStatus: []int{http.StatusForbidden}}},
		next:    adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"},
	}
	resolver.refresh = func(ctx context.Context, _ adapterproto.MediaSource) (adapterproto.MediaSource, error) {
		refreshOnce.Do(func() { close(refreshStarted) })
		select {
		case <-ctx.Done():
			return adapterproto.MediaSource{}, ctx.Err()
		case <-allOld:
			return resolver.next, nil
		}
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", resolver.initial, nil, "refresh singleflight", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, refreshStarted, "status-trigger refresh")
	waitSignal(t, allOld, "all old-source requests")
	waitForSegmentCount(t, manager, recording.ID, segments)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls.Load() != 1 || oldRequests.Load() != segments || newRequests.Load() != segments || len(stopped.Tracks["main"].Segments) != segments {
		newRequestMu.Lock()
		gotPaths := fmt.Sprintf("%v", newRequestPaths)
		newRequestMu.Unlock()
		t.Fatalf("refresh=%d old=%d new=%d segments=%d paths=%s", resolver.calls.Load(), oldRequests.Load(), newRequests.Load(), len(stopped.Tracks["main"].Segments), gotPaths)
	}
	for _, segment := range stopped.Tracks["main"].Segments {
		if !strings.Contains(segment.SourceURI, "/new-") {
			t.Fatalf("segment retained stale source URI: %q", segment.SourceURI)
		}
	}
}

func TestEndListDrainsDiscoveredSegmentsBeforeCompletion(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	segmentStarted := make(chan struct{}, 2)
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1")
			_, _ = fmt.Fprintln(w, "#EXTINF:1,\n1.ts\n#EXTINF:1,\n2.ts\n#EXT-X-ENDLIST")
			return
		}
		segmentStarted <- struct{}{}
		if r.URL.Path == "/1.ts" {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.WriteString(w, "end-list-payload")
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		server.Close()
	}()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "endlist drain", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, segmentStarted, "first ENDLIST segment")
	waitSignal(t, segmentStarted, "second ENDLIST segment")
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != domain.StateRecording {
		t.Fatalf("recording finalized before queued work drained: %s", current.State)
	}
	releaseOnce.Do(func() { close(release) })
	waitForRecordingState(t, manager, recording.ID, domain.StateCompleted)
	completed, err := manager.Get(recording.ID)
	if err != nil || completed.SegmentCount() != 2 {
		t.Fatalf("completed=%#v err=%v", completed, err)
	}
}

func TestEndListWinsRefreshAwaitManifestTransition(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "feedfacefeedfacefeedfacefeedface"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, PendingSegments: []domain.PendingSequence{{SourceEpoch: 1, Sequence: 9}}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording}
	key := segmentTaskKey{epoch: 1, sequence: 9}
	task := &segmentTask{key: key, state: segmentTaskInFlight, available: true}
	scheduler := &segmentScheduler{
		manager: manager,
		e:       e,
		changed: make(chan struct{}),
		tasks:   map[segmentTaskKey]*segmentTask{key: task},
		ending:  true,
	}

	// This is the interleaving where ENDLIST arrives after refresh wakes the
	// poller but before the failed worker transitions to AwaitManifest.
	scheduler.awaitManifest(task)
	if len(scheduler.tasks) != 0 {
		t.Fatalf("ENDLIST left await-manifest task active: %#v", scheduler.tasks)
	}
	if len(e.recording.Gaps) != 1 || e.recording.Gaps[0].SourceEpoch != 1 || e.recording.Gaps[0].FromSequence != 9 || e.recording.Gaps[0].ToSequence != 9 {
		t.Fatalf("ENDLIST did not persist the terminal segment gap: %#v", e.recording.Gaps)
	}
}

func TestStopCancelsRetryWaitAndJoinsScheduler(t *testing.T) {
	var requests atomic.Int32
	failed := make(chan struct{})
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:8\n#EXTINF:1,\n8.ts")
			return
		}
		requests.Add(1)
		http.Error(w, "temporary", http.StatusServiceUnavailable)
		select {
		case <-failed:
		default:
			close(failed)
		}
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "cancel retry", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, failed, "transient segment failure")
	waitForRetryWait(t, manager, recording.ID)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != domain.StateStopped {
		t.Fatalf("stop state=%s", stopped.State)
	}
	e, _ := manager.entry(recording.ID)
	e.mu.Lock()
	active := e.scheduler
	e.mu.Unlock()
	if active != nil {
		t.Fatal("scheduler remained attached after Stop returned")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("retry escaped cancellation: requests=%d", got)
	}
}

func TestQueueBackpressureCancellationPreservesUndiscoveredManifestTail(t *testing.T) {
	const totalSegments = maxSegmentTasks + 1
	started := make(chan struct{}, maxSegmentWorkers)
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0")
			for sequence := 0; sequence < totalSegments; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\n%d.ts\n", sequence)
			}
			return
		}
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "bounded queue", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxSegmentWorkers; i++ {
		waitSignal(t, started, "bounded scheduler worker")
	}
	waitForSchedulerTaskCount(t, manager, recording.ID, maxSegmentTasks)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != domain.StateStopped {
		t.Fatalf("recording state after cancellation = %s", stopped.State)
	}
	foundTailGap := false
	for _, gap := range stopped.Gaps {
		if gap.TrackID == "main" && gap.SourceEpoch == 0 && uint64(totalSegments-1) >= gap.FromSequence && uint64(totalSegments-1) <= gap.ToSequence {
			foundTailGap = true
			break
		}
	}
	if !foundTailGap {
		t.Fatalf("undiscovered manifest tail was not preserved as a gap: %#v", stopped.Gaps)
	}
}

func TestRefreshingTaskQueuesOnlyAfterPostRefreshManifest(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "baadf00dbaadf00dbaadf00dbaadf00d"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording, mediaGeneration: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := segmentTaskKey{epoch: 0, sequence: 42}
	task := &segmentTask{key: key, state: segmentTaskInFlight, available: true, requiredGeneration: 1}
	scheduler := &segmentScheduler{
		manager:                  manager,
		e:                        e,
		ctx:                      ctx,
		cancel:                   cancel,
		changed:                  make(chan struct{}),
		tasks:                    map[segmentTaskKey]*segmentTask{key: task},
		ready:                    []*segmentTask{},
		workers:                  minSegmentWorkers,
		init:                     make(map[string]*initFlight),
		manifestWake:             make(chan struct{}, 1),
		latestManifestGeneration: 1,
		manifestGenerationSet:    true,
	}

	if !scheduler.markRefreshing(task) {
		t.Fatal("in-flight task did not enter Refreshing state")
	}
	stale := hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 42, URI: "https://old.example/old.ts", Duration: 1}}}
	if accepted, discoverErr := scheduler.discoverAtGeneration(ctx, 0, stale, 0); discoverErr != nil || accepted {
		t.Fatalf("stale discovery accepted=%v err=%v", accepted, discoverErr)
	}
	scheduler.mu.Lock()
	stateWhileRefreshing, readyWhileRefreshing := task.state, len(scheduler.ready)
	scheduler.mu.Unlock()
	if stateWhileRefreshing != segmentTaskRefreshing || readyWhileRefreshing != 0 {
		t.Fatalf("refreshing rediscovery made task runnable: state=%d ready=%d", stateWhileRefreshing, readyWhileRefreshing)
	}
	select {
	case <-scheduler.manifestWake:
		t.Fatal("refreshing task woke the poller before AwaitManifest transition")
	default:
	}

	scheduler.awaitManifest(task)
	scheduler.mu.Lock()
	stateAwaiting, readyAwaiting := task.state, len(scheduler.ready)
	scheduler.mu.Unlock()
	if stateAwaiting != segmentTaskAwaitManifest || readyAwaiting != 0 {
		t.Fatalf("task did not await refreshed manifest: state=%d ready=%d", stateAwaiting, readyAwaiting)
	}
	select {
	case <-scheduler.manifestWake:
	default:
		t.Fatal("manifest poll was not woken after AwaitManifest transition")
	}

	fresh := hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 42, URI: "https://new.example/fresh.ts", Duration: 1}}}
	if accepted, discoverErr := scheduler.discoverAtGeneration(ctx, 0, fresh, 1); discoverErr != nil || !accepted {
		t.Fatalf("fresh discovery accepted=%v err=%v", accepted, discoverErr)
	}
	if accepted, discoverErr := scheduler.discoverAtGeneration(ctx, 0, fresh, 1); discoverErr != nil || !accepted {
		t.Fatalf("duplicate fresh discovery accepted=%v err=%v", accepted, discoverErr)
	}
	scheduler.mu.Lock()
	stateQueued, readyCount, sourceURI := task.state, len(scheduler.ready), task.source.URI
	scheduler.mu.Unlock()
	if stateQueued != segmentTaskQueued || readyCount != 1 || sourceURI != "https://new.example/fresh.ts" {
		t.Fatalf("fresh manifest queue state=%d ready=%d source=%q", stateQueued, readyCount, sourceURI)
	}
}

func TestRetryWaitRediscoveryKeepsSingleTaskAndTimer(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "a11ce000a11ce000a11ce000a11ce000"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording}
	key := segmentTaskKey{epoch: 0, sequence: 7}
	timerCancel := func() {}
	task := &segmentTask{key: key, source: hls.MediaSegment{Sequence: 7, URI: "https://old.example/7.ts"}, available: true, state: segmentTaskRetryWait, timerToken: 9, retryCancel: timerCancel}
	scheduler := &segmentScheduler{manager: manager, e: e, changed: make(chan struct{}), tasks: map[segmentTaskKey]*segmentTask{key: task}, ready: []*segmentTask{}}
	playlist := hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 7, URI: "https://fresh.example/7.ts", Duration: 1}}}
	if err = scheduler.discover(context.Background(), 0, playlist); err != nil {
		t.Fatal(err)
	}
	scheduler.mu.Lock()
	gotTask := scheduler.tasks[key]
	state, token, cancelNil, ready, taskCount := task.state, task.timerToken, task.retryCancel == nil, len(scheduler.ready), len(scheduler.tasks)
	sourceURI := task.source.URI
	scheduler.mu.Unlock()
	if taskCount != 1 || gotTask != task || state != segmentTaskRetryWait || token != 9 || cancelNil || ready != 0 || sourceURI != "https://fresh.example/7.ts" {
		t.Fatalf("retry rediscovery changed task identity/state: count=%d same=%v state=%d token=%d retryCancelNil=%v ready=%d uri=%q", taskCount, gotTask == task, state, token, cancelNil, ready, sourceURI)
	}
}

func TestAcquisitionAdmissionRejectsQueuedOldGenerationBeforeFetch(t *testing.T) {
	var requests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, "must-not-be-requested")
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "baddcafe00baddcafe00baddcafe00ba"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording, media: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"}, mediaGeneration: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := segmentTaskKey{epoch: 0, sequence: 17}
	task := &segmentTask{
		key: key, source: hls.MediaSegment{Sequence: 17, URI: server.URL + "/old.ts", Duration: 1},
		ordinal: 1, available: true, observed: true, observedGeneration: 0, state: segmentTaskInFlight,
	}
	scheduler := &segmentScheduler{
		manager: manager, e: e, ctx: ctx, cancel: cancel, changed: make(chan struct{}),
		tasks: map[segmentTaskKey]*segmentTask{key: task}, ready: []*segmentTask{}, manifestWake: make(chan struct{}, 1),
	}

	if err = scheduler.acquire(task); !errors.Is(err, errAwaitManifest) {
		t.Fatalf("stale acquisition error = %v, want errAwaitManifest", err)
	}
	scheduler.awaitManifest(task)
	scheduler.mu.Lock()
	state, required, available, observed, observedGeneration := task.state, task.requiredGeneration, task.available, task.observed, task.observedGeneration
	scheduler.mu.Unlock()
	if state != segmentTaskAwaitManifest || required != 1 || available || observed || observedGeneration != 0 {
		t.Fatalf("stale task state=%d required=%d available=%v observed=%v observedGeneration=%d", state, required, available, observed, observedGeneration)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("stale source URL was requested %d times", got)
	}
	select {
	case <-scheduler.manifestWake:
	default:
		t.Fatal("stale task transition did not wake manifest polling")
	}
}

func TestRefreshingTaskInFreshEndListIsCaptured(t *testing.T) {
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fresh.ts" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, "fresh-endlist-segment")
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "c0ffee00c0ffee00c0ffee00c0ffee00"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1, Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &entry{recording: recording, media: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/manifest.m3u8"}, mediaGeneration: 1}
	scheduler, err := newSegmentScheduler(ctx, manager, e)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.close()
	key := segmentTaskKey{epoch: 0, sequence: 42}
	task := &segmentTask{key: key, ordinal: 1, state: segmentTaskRefreshing, requiredGeneration: 1}
	scheduler.mu.Lock()
	scheduler.tasks[key] = task
	scheduler.mu.Unlock()

	playlist := hls.MediaPlaylist{TargetDuration: 1, EndList: true, Segments: []hls.MediaSegment{{Sequence: 42, URI: server.URL + "/fresh.ts", Duration: 1}}}
	result := make(chan error, 1)
	go func() {
		_, processErr := manager.processWithSchedulerGeneration(e, ctx, playlist, false, 1)
		result <- processErr
	}()
	waitForObservedEndList(t, scheduler, task, 1)
	// The original fetch worker completes refresh handling only after this fresh
	// manifest has already been observed. Its AwaitManifest transition must
	// reuse that observation even though ENDLIST has closed admission.
	scheduler.awaitManifest(task)
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	current := clone(e.recording)
	e.mu.Unlock()
	if current.State != domain.StateCompleted || current.SegmentCount() != 1 {
		t.Fatalf("fresh ENDLIST did not drain the segment: state=%s segments=%d gaps=%#v", current.State, current.SegmentCount(), current.Gaps)
	}
	segment := current.Tracks["main"].Segments[0]
	if segment.Sequence != 42 || segment.SourceURI != server.URL+"/fresh.ts" {
		t.Fatalf("captured segment metadata = %#v", segment)
	}
	for _, gap := range current.Gaps {
		if gap.SourceEpoch == 0 && gap.FromSequence <= 42 && gap.ToSequence >= 42 {
			t.Fatalf("captured segment was also marked as a gap: %#v", gap)
		}
	}
}

func TestStaleEndListCannotMutateObservationOrBlockFreshGeneration(t *testing.T) {
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fresh.ts" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, "fresh-generation-segment")
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "deadbeefdeadbeefdeadbeefdeadbeef"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", HasLastObservedSequence: true, LastObservedSequence: 40, NextArchiveOrdinal: 1, Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &entry{recording: recording, media: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/old.m3u8"}}
	scheduler, err := newSegmentScheduler(ctx, manager, e)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.close()
	key := segmentTaskKey{epoch: 0, sequence: 41}
	task := &segmentTask{key: key, ordinal: 1, state: segmentTaskRefreshing, requiredGeneration: 1}
	scheduler.mu.Lock()
	scheduler.tasks[key] = task
	scheduler.mu.Unlock()

	oldEndListResult := make(chan error, 1)
	go func() {
		_, processErr := manager.processWithSchedulerGeneration(e, ctx, hls.MediaPlaylist{TargetDuration: 1, EndList: true}, false, 0)
		oldEndListResult <- processErr
	}()
	waitForEndListGeneration(t, scheduler, 0)
	e.mu.Lock()
	e.mediaGeneration = 1
	e.media.ManifestURL = server.URL + "/fresh.m3u8"
	e.mu.Unlock()
	scheduler.noteMediaGeneration(1)
	if err = <-oldEndListResult; err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	current := clone(e.recording)
	e.mu.Unlock()
	if current.Tracks["main"].LastObservedSequence != 40 || len(current.Gaps) != 0 {
		t.Fatalf("stale manifest mutated observation state: last=%d gaps=%#v", current.Tracks["main"].LastObservedSequence, current.Gaps)
	}
	scheduler.mu.Lock()
	ending := scheduler.ending
	scheduler.mu.Unlock()
	if ending {
		t.Fatal("stale ENDLIST closed the current source generation")
	}
	select {
	case <-scheduler.manifestWake:
	default:
		t.Fatal("generation advance did not wake the manifest poller")
	}

	stale := hls.MediaPlaylist{TargetDuration: 1, EndList: true, Segments: []hls.MediaSegment{{Sequence: 100, URI: server.URL + "/stale.ts", Duration: 1}}}
	if done, processErr := manager.processWithSchedulerGeneration(e, ctx, stale, false, 0); processErr != nil || done {
		t.Fatalf("late stale ENDLIST result done=%v err=%v", done, processErr)
	}
	e.mu.Lock()
	current = clone(e.recording)
	e.mu.Unlock()
	if current.Tracks["main"].LastObservedSequence != 40 || len(current.Gaps) != 0 {
		t.Fatalf("late stale manifest mutated observation state: last=%d gaps=%#v", current.Tracks["main"].LastObservedSequence, current.Gaps)
	}

	fresh := hls.MediaPlaylist{TargetDuration: 1, EndList: true, Segments: []hls.MediaSegment{{Sequence: 41, URI: server.URL + "/fresh.ts", Duration: 1}}}
	result := make(chan error, 1)
	go func() {
		_, processErr := manager.processWithSchedulerGeneration(e, ctx, fresh, false, 1)
		result <- processErr
	}()
	waitForObservedEndList(t, scheduler, task, 1)
	scheduler.awaitManifest(task)
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	current = clone(e.recording)
	e.mu.Unlock()
	if current.State != domain.StateCompleted || current.SegmentCount() != 1 || len(current.Gaps) != 0 {
		t.Fatalf("fresh generation did not capture cleanly: state=%s segments=%d gaps=%#v", current.State, current.SegmentCount(), current.Gaps)
	}
}

func waitForObservedEndList(t *testing.T, scheduler *segmentScheduler, task *segmentTask, generation uint64) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		scheduler.mu.Lock()
		observed := scheduler.ending && scheduler.endingGeneration == generation && task.observed && task.observedGeneration >= generation
		changed := scheduler.changed
		scheduler.mu.Unlock()
		if observed {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("scheduler did not observe the expected ENDLIST generation")
		case <-changed:
		}
	}
}

func waitForEndListGeneration(t *testing.T, scheduler *segmentScheduler, generation uint64) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		scheduler.mu.Lock()
		observed := scheduler.ending && scheduler.endingGeneration == generation
		changed := scheduler.changed
		scheduler.mu.Unlock()
		if observed {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("scheduler did not accept ENDLIST for generation %d", generation)
		case <-changed:
		}
	}
}

func waitForSchedulerTaskCount(t *testing.T, manager *Manager, id string, want int) {
	t.Helper()
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording entry disappeared")
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		scheduler := activeScheduler(e)
		if scheduler == nil {
			t.Fatal("segment scheduler is not active")
		}
		scheduler.mu.Lock()
		got := len(scheduler.tasks)
		changed := scheduler.changed
		scheduler.mu.Unlock()
		if got == want {
			return
		}
		if got > want {
			t.Fatalf("scheduler task count %d exceeds bound %d", got, want)
		}
		select {
		case <-deadline.C:
			t.Fatalf("scheduler task count remained %d, want %d", got, want)
		case <-changed:
		}
	}
}

func waitForRetryWait(t *testing.T, manager *Manager, id string) {
	t.Helper()
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording entry disappeared")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		scheduler := activeScheduler(e)
		if scheduler == nil {
			t.Fatal("segment scheduler is not active")
		}
		scheduler.mu.Lock()
		waiting := false
		for _, task := range scheduler.tasks {
			waiting = waiting || task.state == segmentTaskRetryWait
		}
		changed := scheduler.changed
		scheduler.mu.Unlock()
		if waiting {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("segment did not enter retry-wait")
		case <-changed:
		}
	}
}

func waitForRecordingState(t *testing.T, manager *Manager, id string, state domain.RecordingState) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		recording, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if recording.State == state {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("recording state remained %s, want %s", recording.State, state)
		case <-ticker.C:
		}
	}
}

func TestProcessHelperReturnsAfterOneFailedFetchWithoutAnotherPoll(t *testing.T) {
	var requests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	if err = store.CreateRecording(&domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}, media: adapterproto.MediaSource{ManifestURL: server.URL + "/manifest"}}
	done, err := manager.process(e, context.Background(), hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 1, URI: server.URL + "/1.ts", Duration: 1}}})
	if err != nil || done {
		t.Fatalf("process() = done %v, err %v", done, err)
	}
	if requests.Load() != 1 || len(e.recording.Tracks["main"].PendingSegments) != 1 {
		t.Fatalf("single poll requests=%d pending=%#v", requests.Load(), e.recording.Tracks["main"].PendingSegments)
	}
}

func TestEpochDiscontinuityMarkerFollowsEarliestCapturedOrdinalAndSidecars(t *testing.T) {
	releaseZero := make(chan struct{})
	var releaseOnce sync.Once
	zeroStarted := make(chan struct{})
	oneStarted := make(chan struct{})
	var polls atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			if polls.Add(1) == 1 {
				_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:100\n#EXTINF:1,\nold.ts")
				return
			}
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1,\n0.ts\n#EXTINF:1,\n1.ts")
		case "/0.ts":
			close(zeroStarted)
			select {
			case <-releaseZero:
			case <-r.Context().Done():
				return
			}
			_, _ = io.WriteString(w, "zero")
		case "/1.ts":
			close(oneStarted)
			_, _ = io.WriteString(w, "one")
		default:
			_, _ = io.WriteString(w, "old")
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(releaseZero) })
		server.Close()
	}()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "epoch marker", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSegmentCount(t, manager, recording.ID, 1)
	waitSignal(t, zeroStarted, "earlier sequence in reset epoch")
	waitSignal(t, oneStarted, "later sequence in reset epoch")
	waitForSegmentCount(t, manager, recording.ID, 2)
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Tracks["main"].Segments) != 2 || !current.Tracks["main"].Segments[1].Discontinuity {
		t.Fatalf("first completed epoch-1 segment did not receive marker: %#v", current.Tracks["main"].Segments)
	}
	releaseOnce.Do(func() { close(releaseZero) })
	waitForSegmentCount(t, manager, recording.ID, 3)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	segments := stopped.Tracks["main"].Segments
	if len(segments) != 3 || segments[1].SourceEpoch != 1 || segments[1].Sequence != 0 || !segments[1].Discontinuity || segments[2].Sequence != 1 || segments[2].Discontinuity {
		t.Fatalf("ordinal-based epoch marker=%#v", segments)
	}
	for _, segment := range segments[1:] {
		data, readErr := os.ReadFile(filepath.Join(store.Root(), "recordings", recording.ID, filepath.FromSlash(segment.StoragePath)+".json"))
		if readErr != nil {
			t.Fatal(readErr)
		}
		var sidecar domain.Segment
		if readErr = json.Unmarshal(data, &sidecar); readErr != nil {
			t.Fatal(readErr)
		}
		if sidecar.Discontinuity != segment.Discontinuity {
			t.Fatalf("sidecar marker disagrees with root for ordinal %d: sidecar=%v root=%v", segment.ArchiveOrdinal, sidecar.Discontinuity, segment.Discontinuity)
		}
	}
}

func TestOldEpochGapDoesNotSuppressReusedSequence(t *testing.T) {
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "new-epoch")
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "ffffffffffffffffffffffffffffffff"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Gaps: []domain.Gap{{TrackID: "main", SourceEpoch: 0, FromSequence: 0, ToSequence: 0, Reason: "old epoch gap"}}, Tracks: map[string]*domain.Track{"main": {ID: "main", SourceEpoch: 1, NextArchiveOrdinal: 1, HasLastObservedSequence: true, LastObservedSequence: 0, Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording, media: adapterproto.MediaSource{ManifestURL: server.URL + "/live.m3u8"}}
	done, err := manager.process(e, context.Background(), hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 0, URI: server.URL + "/0.ts", Duration: 1}}})
	if err != nil || done {
		t.Fatalf("process() = done %v err %v", done, err)
	}
	if len(e.recording.Tracks["main"].Segments) != 1 || e.recording.Tracks["main"].Segments[0].SourceEpoch != 1 || e.recording.Tracks["main"].Segments[0].Sequence != 0 {
		t.Fatalf("new epoch sequence was suppressed by old gap: %#v", e.recording.Tracks["main"].Segments)
	}
}

func TestCatchUpSequenceResetCreatesStableEpochAndReloadsInArchiveOrder(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "new-source-segment")
	}))
	defer source.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "cccccccccccccccccccccccccccccccc"
	old := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {
		ID: "main", SourceEpoch: 0, NextArchiveOrdinal: 3, HasLastObservedSequence: true, LastObservedSequence: 1,
		Segments: []domain.Segment{},
	}}}
	if err = store.CreateRecording(old); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		id, path, uri string
		sequence      uint64
		ordinal       uint64
	}{
		{id: "old-0", path: "tracks/main/legacy-0.ts", uri: source.URL + "/old-0.ts", sequence: 0, ordinal: 1},
		{id: "old-1", path: "tracks/main/legacy-1.ts", uri: source.URL + "/old-1.ts", sequence: 1, ordinal: 2},
	} {
		payload := []byte("legacy-source-" + fixture.id)
		result, saveErr := store.SavePayloadExact(id, fixture.path, bytes.NewReader(payload), 1<<20, int64(len(payload)))
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		old.Tracks["main"].Segments = append(old.Tracks["main"].Segments, domain.Segment{
			ID: fixture.id, TrackID: "main", Sequence: fixture.sequence, SourceEpoch: 0,
			DiscontinuitySequence: 0, ArchiveOrdinal: fixture.ordinal, SourceURI: fixture.uri,
			Duration: 1, StoragePath: fixture.path, PayloadSize: result.Size, SHA256: result.SHA256,
		})
	}
	if err = store.SaveRecording(old); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	playlist, err := hls.ParseMedia([]byte(`#EXTM3U
#EXT-X-TARGETDURATION:1
#EXT-X-DISCONTINUITY-SEQUENCE:1
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:1,
new-0.ts
#EXTINF:1,
new-1.ts
#EXTINF:1,
new-2.ts
`), source.URL+"/list.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: old, media: adapterproto.MediaSource{ManifestURL: source.URL + "/list.m3u8"}}
	if err = manager.observePlaylist(e, playlist); err != nil {
		t.Fatal(err)
	}
	if got := e.recording.Tracks["main"].SourceEpoch; got != 1 {
		t.Fatalf("catch-up reset epoch = %d, want 1", got)
	}
	if err = manager.observePlaylist(e, playlist); err != nil {
		t.Fatal(err)
	}
	if got := e.recording.Tracks["main"].SourceEpoch; got != 1 {
		t.Fatalf("repeated playlist changed epoch to %d", got)
	}
	if done, processErr := manager.process(e, context.Background(), playlist); processErr != nil || done {
		t.Fatalf("process reset playlist = done %v, err %v", done, processErr)
	}
	segments := e.recording.Tracks["main"].Segments
	if len(segments) != 5 {
		t.Fatalf("captured segments = %#v", segments)
	}
	for index, want := range []struct{ epoch, sequence, ordinal uint64 }{{0, 0, 1}, {0, 1, 2}, {1, 0, 3}, {1, 1, 4}, {1, 2, 5}} {
		segment := segments[index]
		if segment.SourceEpoch != want.epoch || segment.Sequence != want.sequence || segment.ArchiveOrdinal != want.ordinal {
			t.Fatalf("segment[%d] = %#v, want epoch=%d sequence=%d ordinal=%d", index, segment, want.epoch, want.sequence, want.ordinal)
		}
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("reload = %d recordings, %v", len(loaded), err)
	}
	segments = loaded[0].Tracks["main"].Segments
	if len(segments) != 5 || segments[2].SourceEpoch != 1 || segments[2].Sequence != 0 || segments[4].Sequence != 2 {
		t.Fatalf("reloaded archive order = %#v", segments)
	}
}

func TestObservePlaylistDetectsSameSequenceIdentityChangesButNotSlidingOverlap(t *testing.T) {
	baseTime := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		incoming  hls.MediaSegment
		wantEpoch uint64
	}{
		{
			name:      "source URI changed",
			incoming:  hls.MediaSegment{Sequence: 1, DiscontinuitySequence: 0, URI: "https://edge.example/new.ts", Duration: 1},
			wantEpoch: 1,
		},
		{
			name:      "signed query rotated for same segment",
			incoming:  hls.MediaSegment{Sequence: 1, DiscontinuitySequence: 0, URI: "https://edge.example/old-1.ts?token=rotated", Duration: 1},
			wantEpoch: 0,
		},
		{
			name:      "program date time changed",
			incoming:  hls.MediaSegment{Sequence: 1, DiscontinuitySequence: 0, URI: "https://edge.example/old-1.ts", ProgramTime: timePtr(baseTime.Add(5 * time.Second)), Duration: 1},
			wantEpoch: 1,
		},
		{
			name:      "normal sliding overlap",
			incoming:  hls.MediaSegment{Sequence: 1, DiscontinuitySequence: 0, URI: "https://edge.example/old-1.ts", ProgramTime: timePtr(baseTime), Duration: 1},
			wantEpoch: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := storage.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			const id = "dddddddddddddddddddddddddddddddd"
			old := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {
				ID: "main", SourceEpoch: 0, HasLastObservedSequence: true, LastObservedSequence: 2,
				Segments: []domain.Segment{
					{ID: "old-1", TrackID: "main", Sequence: 1, SourceEpoch: 0, DiscontinuitySequence: 0, SourceURI: "https://edge.example/old-1.ts", ProgramDateTime: timePtr(baseTime)},
					{ID: "old-2", TrackID: "main", Sequence: 2, SourceEpoch: 0, DiscontinuitySequence: 0, SourceURI: "https://edge.example/old-2.ts", ProgramDateTime: timePtr(baseTime.Add(time.Second))},
				},
			}}}
			if err = store.CreateRecording(old); err != nil {
				t.Fatal(err)
			}
			manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			incoming := tc.incoming
			if tc.name == "normal sliding overlap" {
				incoming.Sequence = 1
			}
			incomingSegments := []hls.MediaSegment{incoming, {Sequence: 2, DiscontinuitySequence: 0, URI: "https://edge.example/old-2.ts", ProgramTime: timePtr(baseTime.Add(time.Second)), Duration: 1}, {Sequence: 3, DiscontinuitySequence: 0, URI: "https://edge.example/new-3.ts", ProgramTime: timePtr(baseTime.Add(2 * time.Second)), Duration: 1}}
			e := &entry{recording: old}
			if err = manager.observePlaylist(e, hls.MediaPlaylist{TargetDuration: 1, Segments: incomingSegments}); err != nil {
				t.Fatal(err)
			}
			if got := e.recording.Tracks["main"].SourceEpoch; got != tc.wantEpoch {
				t.Fatalf("source epoch=%d want=%d", got, tc.wantEpoch)
			}
		})
	}
}

func TestDownloadObjectRetriesTruncatedResponseBody(t *testing.T) {
	var requests atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Content-Length", "32")
			_, _ = io.WriteString(w, "truncated")
			return
		}
		_, _ = io.WriteString(w, "complete-original-payload")
	}))
	defer source.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("complete-original-payload")
	result, err := manager.downloadObject(context.Background(), source.URL+"/segment.ts", nil, id, "tracks/main/segment.ts", adapterproto.MediaSource{ManifestURL: source.URL + "/manifest"})
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() < 2 {
		t.Fatalf("truncated source body was not retried: requests=%d", requests.Load())
	}
	file, err := store.OpenPayload(id, "tracks/main/segment.ts")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(file)
	file.Close()
	if err != nil || !bytes.Equal(got, want) || result.Size != int64(len(want)) {
		t.Fatalf("saved payload = %q (%d), err=%v", got, result.Size, err)
	}
}

type emptyResolver struct{}

func (emptyResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, nil
}

func TestMediaHeadersStayWithinSourceOrigin(t *testing.T) {
	const headerName = "X-Generic-Session"
	const headerValue = "opaque-test-value"
	var sourceHeader string
	var cdnHeader string
	var mu sync.Mutex
	cdn := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cdnHeader = r.Header.Get(headerName)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer cdn.Close()
	source := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sourceHeader = r.Header.Get(headerName)
		mu.Unlock()
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, cdn.URL+"/segment", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer source.Close()
	client := &http.Client{}
	response, err := doMediaRequest(client, mustRequest(t, source.URL+"/manifest"), map[string]string{headerName: headerValue}, source.URL+"/manifest", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	response, err = doMediaRequest(client, mustRequest(t, source.URL+"/redirect"), map[string]string{headerName: headerValue}, source.URL+"/redirect", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if sourceHeader != headerValue {
		t.Fatalf("same-origin header = %q", sourceHeader)
	}
	if cdnHeader != "" {
		t.Fatal("adapter-provided header was forwarded to a different origin")
	}
}

func TestCompressedHTTPContentEncodingIsRejectedWithoutStoringPayload(t *testing.T) {
	acceptEncoding := make(chan string, 2)
	source := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acceptEncoding <- r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Encoding", "gzip")
		writer := gzip.NewWriter(w)
		_, _ = writer.Write([]byte("compressed representation of media bytes"))
		_ = writer.Close()
	}))
	defer source.Close()

	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "abcdef0123456789abcdef0123456789"
	if err = store.NewRecordingDir(recordingID); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	manifestURL := source.URL + "/manifest.m3u8"
	if _, err = fetchManifest(context.Background(), manager.client, manifestURL, nil, manifestURL, nil); err == nil {
		t.Fatal("compressed manifest was accepted")
	}
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: manifestURL}
	if _, err = manager.downloadObject(context.Background(), source.URL+"/segment.ts", nil, recordingID, "tracks/main/segment.ts", media); err == nil {
		t.Fatal("compressed media segment was accepted")
	}
	segmentPath := filepath.Join(store.Root(), "recordings", recordingID, "tracks", "main", "segment.ts")
	if _, err = os.Stat(segmentPath); !os.IsNotExist(err) {
		t.Fatalf("rejected encoded response was stored: stat error=%v", err)
	}
	close(acceptEncoding)
	for value := range acceptEncoding {
		if value != "identity" {
			t.Errorf("Accept-Encoding = %q, want explicit identity", value)
		}
	}
}

func mustRequest(t *testing.T, raw string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func newIPv4Server(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}

func TestFetchBoundaryRejectsStaleMediaGeneration(t *testing.T) {
	var oldRequests, freshRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old.ts":
			oldRequests.Add(1)
			_, _ = io.WriteString(w, "stale")
		case "/fresh.ts":
			freshRequests.Add(1)
			_, _ = io.WriteString(w, "fresh-media")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	fixture := newGenerationBoundaryFixture(t, server, hls.MediaSegment{Sequence: 17, URI: server.URL + "/old.ts", Duration: 1})
	fixture.blockAtBoundary(t, server.URL+"/old.ts", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"})
	fixture.advanceAndAwait(t)
	fixture.rediscoverAndCapture(t, hls.MediaSegment{Sequence: 17, URI: server.URL + "/fresh.ts", Duration: 1})

	if got := oldRequests.Load(); got != 0 {
		t.Fatalf("stale media URL was requested %d times", got)
	}
	if got := freshRequests.Load(); got != 1 {
		t.Fatalf("fresh media URL request count = %d, want 1", got)
	}
	fixture.assertCaptured(t, "fresh-media", "")
}

func TestFetchBoundaryRejectsStaleInitGeneration(t *testing.T) {
	var oldInitRequests, oldMediaRequests, freshInitRequests, freshMediaRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old-init.mp4":
			oldInitRequests.Add(1)
			_, _ = io.WriteString(w, "stale-init")
		case "/old.ts":
			oldMediaRequests.Add(1)
			_, _ = io.WriteString(w, "stale-media")
		case "/new-init.mp4":
			freshInitRequests.Add(1)
			_, _ = io.WriteString(w, "fresh-init")
		case "/fresh.ts":
			freshMediaRequests.Add(1)
			_, _ = io.WriteString(w, "fresh-media")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	initial := hls.MediaSegment{Sequence: 18, URI: server.URL + "/old.ts", Duration: 1, Init: &hls.Map{URI: server.URL + "/old-init.mp4"}}
	fixture := newGenerationBoundaryFixture(t, server, initial)
	fixture.blockAtBoundary(t, server.URL+"/old-init.mp4", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"})
	fixture.advanceAndAwait(t)
	fresh := hls.MediaSegment{Sequence: 18, URI: server.URL + "/fresh.ts", Duration: 1, Init: &hls.Map{URI: server.URL + "/new-init.mp4"}}
	fixture.rediscoverAndCapture(t, fresh)

	if got := oldInitRequests.Load(); got != 0 {
		t.Fatalf("stale init URL was requested %d times", got)
	}
	if got := oldMediaRequests.Load(); got != 0 {
		t.Fatalf("stale media URL was requested %d times", got)
	}
	if got := freshInitRequests.Load(); got != 1 {
		t.Fatalf("fresh init URL request count = %d, want 1", got)
	}
	if got := freshMediaRequests.Load(); got != 1 {
		t.Fatalf("fresh media URL request count = %d, want 1", got)
	}
	fixture.assertCaptured(t, "fresh-media", server.URL+"/new-init.mp4")
}

func TestFetchBoundaryRejectsStaleMediaAfterInit(t *testing.T) {
	var initRequests, oldMediaRequests, freshMediaRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shared-init.mp4":
			initRequests.Add(1)
			_, _ = io.WriteString(w, "shared-init")
		case "/old.ts":
			oldMediaRequests.Add(1)
			_, _ = io.WriteString(w, "stale-media")
		case "/fresh.ts":
			freshMediaRequests.Add(1)
			_, _ = io.WriteString(w, "fresh-media")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sharedInit := &hls.Map{URI: server.URL + "/shared-init.mp4"}
	initial := hls.MediaSegment{Sequence: 19, URI: server.URL + "/old.ts", Duration: 1, Init: sharedInit}
	fixture := newGenerationBoundaryFixture(t, server, initial)
	fixture.blockAtBoundary(t, server.URL+"/old.ts", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"})
	if got := initRequests.Load(); got != 1 {
		t.Fatalf("init request count before media boundary = %d, want 1", got)
	}
	fixture.advanceAndAwait(t)
	fresh := hls.MediaSegment{Sequence: 19, URI: server.URL + "/fresh.ts", Duration: 1, Init: &hls.Map{URI: server.URL + "/shared-init.mp4"}}
	fixture.rediscoverAndCapture(t, fresh)

	if got := oldMediaRequests.Load(); got != 0 {
		t.Fatalf("stale media URL was requested %d times", got)
	}
	if got := freshMediaRequests.Load(); got != 1 {
		t.Fatalf("fresh media URL request count = %d, want 1", got)
	}
	if got := initRequests.Load(); got != 1 {
		t.Fatalf("shared init request count = %d, want existing object to be reused", got)
	}
	fixture.assertCaptured(t, "fresh-media", server.URL+"/shared-init.mp4")
}

type generationBoundaryFixture struct {
	manager   *Manager
	e         *entry
	scheduler *segmentScheduler
	task      *segmentTask
	ctx       context.Context
	started   chan struct{}
	release   chan struct{}
	refreshed adapterproto.MediaSource
}

func newGenerationBoundaryFixture(t *testing.T, server *httptest.Server, initial hls.MediaSegment) *generationBoundaryFixture {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "feec0badfeec0badfeec0badfeec0bad"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 2, Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording, media: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/old.m3u8"}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	key := segmentTaskKey{epoch: 0, sequence: initial.Sequence}
	task := &segmentTask{key: key, source: initial, ordinal: 1, available: true, observed: true, state: segmentTaskQueued}
	scheduler := &segmentScheduler{
		manager: manager, e: e, ctx: ctx, cancel: cancel, changed: make(chan struct{}),
		tasks: map[segmentTaskKey]*segmentTask{key: task}, ready: []*segmentTask{task}, init: make(map[string]*initFlight),
		workers: minSegmentWorkers,
		next:    2, epochMarkers: make(map[uint64]epochMarker), manifestWake: make(chan struct{}, 1),
		latestManifestGeneration: 0, manifestGenerationSet: true,
	}
	scheduler.wg.Add(1)
	return &generationBoundaryFixture{manager: manager, e: e, scheduler: scheduler, task: task, ctx: ctx}
}

func (f *generationBoundaryFixture) blockAtBoundary(t *testing.T, uri string, refreshed adapterproto.MediaSource) {
	t.Helper()
	f.started = make(chan struct{})
	f.release = make(chan struct{})
	var once sync.Once
	f.manager.fetchBoundaryHook = func(requestURI string) {
		if requestURI == uri {
			once.Do(func() {
				close(f.started)
				<-f.release
			})
		}
	}
	t.Cleanup(func() { _ = f.scheduler.close() })
	go f.scheduler.worker(false)
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		close(f.release)
		t.Fatal("acquisition did not reach the generation-guarded fetch boundary")
	}
	f.refreshed = refreshed
}

func (f *generationBoundaryFixture) advanceAndAwait(t *testing.T) {
	t.Helper()
	f.e.mu.Lock()
	f.e.media = cloneMediaSource(f.refreshed)
	f.e.mediaGeneration = 1
	f.e.mu.Unlock()
	f.scheduler.noteMediaGeneration(1)
	close(f.release)
	waitForTaskState(t, f.scheduler, f.task, segmentTaskAwaitManifest)
	f.scheduler.mu.Lock()
	state, attempt, refreshCycles := f.task.state, f.task.attempt, f.task.refreshCycles
	f.scheduler.mu.Unlock()
	if attempt != 0 || refreshCycles != 0 {
		t.Fatalf("stale generation consumed retry/refresh budget: attempt=%d refreshCycles=%d", attempt, refreshCycles)
	}
	if f.scheduler.failure() != nil {
		t.Fatalf("stale generation made the scheduler fatal: %v", f.scheduler.failure())
	}
	f.e.mu.Lock()
	gapCount := len(f.e.recording.Gaps)
	f.e.mu.Unlock()
	if gapCount != 0 {
		t.Fatalf("stale generation persisted %d gap(s)", gapCount)
	}
	f.scheduler.mu.Lock()
	state, required, available, observed := f.task.state, f.task.requiredGeneration, f.task.available, f.task.observed
	f.scheduler.mu.Unlock()
	if state != segmentTaskAwaitManifest || required != 1 || available || observed {
		t.Fatalf("task did not await generation 1 manifest: state=%d required=%d available=%v observed=%v", state, required, available, observed)
	}
}

func (f *generationBoundaryFixture) rediscoverAndCapture(t *testing.T, source hls.MediaSegment) {
	t.Helper()
	accepted, err := f.scheduler.discoverAtGeneration(f.ctx, 0, hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{source}}, 1)
	if err != nil || !accepted {
		t.Fatalf("fresh-generation discovery accepted=%v err=%v", accepted, err)
	}
	waitForFixtureSegmentCount(t, f.e, 1)
}

func waitForFixtureSegmentCount(t *testing.T, e *entry, count int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		e.mu.Lock()
		current := e.recording.SegmentCount()
		e.mu.Unlock()
		if current >= count {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("recording segment count did not reach %d; got %d", count, current)
		case <-ticker.C:
		}
	}
}

func waitForTaskState(t *testing.T, scheduler *segmentScheduler, task *segmentTask, want segmentTaskState) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		scheduler.mu.Lock()
		state := task.state
		changed := scheduler.changed
		scheduler.mu.Unlock()
		if state == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("task state did not become %d; got %d", want, state)
		case <-changed:
		}
	}
}

func (f *generationBoundaryFixture) assertCaptured(t *testing.T, payload, initURI string) {
	t.Helper()
	f.e.mu.Lock()
	recording := clone(f.e.recording)
	f.e.mu.Unlock()
	track := recording.Tracks["main"]
	if len(track.Segments) != 1 {
		t.Fatalf("captured segment count = %d, want 1", len(track.Segments))
	}
	segment := track.Segments[0]
	digest := sha256.Sum256([]byte(payload))
	if segment.Sequence != f.task.key.sequence || segment.SourceURI != f.task.source.URI || segment.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("captured segment metadata/payload hash = %#v", segment)
	}
	if len(recording.Gaps) != 0 {
		t.Fatalf("captured segment has gap metadata: %#v", recording.Gaps)
	}
	if initURI == "" {
		if segment.InitSegmentID != "" || len(track.InitSegments) != 0 {
			t.Fatalf("unexpected init metadata: segment=%#v init=%#v", segment, track.InitSegments)
		}
		return
	}
	if len(track.InitSegments) != 1 || segment.InitSegmentID == "" || segment.InitSegmentID != track.InitSegments[0].ID || track.InitSegments[0].SourceURI != initURI {
		t.Fatalf("captured init metadata/reference is invalid: segment=%#v init=%#v", segment, track.InitSegments)
	}
}
