package acquire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/storage"
)

type orderedHistoricalTransport struct {
	manifest      []byte
	firstStarted  chan struct{}
	secondStarted chan struct{}
	releaseFirst  chan struct{}
	firstOnce     sync.Once
	secondOnce    sync.Once
	mu            sync.Mutex
	active        int
	maximum       int
	calls         []string
	firstCanceled bool
}

func (t *orderedHistoricalTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if path == "/archive/index.m3u8" {
		return historicalTestResponse(request, t.manifest), nil
	}

	t.mu.Lock()
	t.calls = append(t.calls, path)
	t.active++
	if t.active > t.maximum {
		t.maximum = t.active
	}
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		t.active--
		t.mu.Unlock()
	}()

	switch path {
	case "/archive/segment-1.ts":
		t.firstOnce.Do(func() { close(t.firstStarted) })
		select {
		case <-t.releaseFirst:
		case <-request.Context().Done():
			t.mu.Lock()
			t.firstCanceled = true
			t.mu.Unlock()
			return nil, request.Context().Err()
		}
		return historicalTestResponse(request, []byte("segment-one")), nil
	case "/archive/segment-2.ts":
		t.secondOnce.Do(func() { close(t.secondStarted) })
		select {
		case <-t.firstStarted:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		return historicalTestResponse(request, []byte("segment-two")), nil
	case "/archive/segment-3.ts":
		return historicalTestResponse(request, []byte("segment-three")), nil
	default:
		return historicalTestResponseStatus(request, nil, http.StatusNotFound), nil
	}
}

func historicalTestResponse(request *http.Request, body []byte) *http.Response {
	return historicalTestResponseStatus(request, body, http.StatusOK)
}

func historicalTestResponseStatus(request *http.Request, body []byte, status int) *http.Response {
	return &http.Response{
		StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))),
		ContentLength: int64(len(body)), Request: request,
	}
}

func newManifestWindowTestManager(t *testing.T, transport http.RoundTripper, id string) (*Manager, *entry, OwnershipToken) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://media.example/archive/index.m3u8", SessionRef: "manifest-window-test",
		HistoricalAvailability: &adapterproto.HistoricalAvailability{
			Mode: adapterproto.HistoricalModeManifest, HistoricalManifestURL: "https://media.example/archive/index.m3u8",
		},
	}
	root := newRecoveryFairnessRoot(t, store, id, media)
	fence := &repairOwnerFence{}
	validate := func(_ context.Context, raw string) error {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("invalid fixture URL")
		}
		return nil
	}
	manager, err := NewManagerWithMode(store, &http.Client{Transport: transport}, nil, validate, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(fence); err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureTerminalOwnerRelease(func(_ context.Context, owner OwnershipToken) error { return fence.Release(owner) }); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: loaded, done: closedChannel(), adapterID: loaded.AdapterID, media: media}
	manager.mu.Lock()
	manager.entries[id] = e
	manager.mu.Unlock()
	manager.freshGeneration = false
	owner := ownerForRepair(id, 1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := manager.Close(ctx); closeErr != nil {
			t.Errorf("close historical window manager: %v", closeErr)
		}
	})
	return manager, e, owner
}

func TestHistoricalWindowFetchesConcurrentlyAndPublishesInManifestOrder(t *testing.T) {
	manifest := "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXTINF:2,\nsegment-2.ts\n#EXTINF:2,\nsegment-3.ts\n#EXT-X-ENDLIST\n"
	transport := &orderedHistoricalTransport{
		manifest: []byte(manifest), firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), releaseFirst: make(chan struct{}),
	}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(transport.releaseFirst) })
	manager, _, owner := newManifestWindowTestManager(t, transport, "cccccccccccccccccccccccccccccccc")
	passDone := make(chan error, 1)
	go func() {
		passDone <- manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID)
	}()
	select {
	case <-transport.firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first historical fetch did not start")
	}
	select {
	case <-transport.secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second historical fetch did not run concurrently")
	}
	select {
	case err := <-passDone:
		t.Fatalf("recovery completed while first fetch remained blocked: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	transport.mu.Lock()
	activeWhileBlocked, maximumWhileBlocked := transport.active, transport.maximum
	calls, firstCanceled := append([]string(nil), transport.calls...), transport.firstCanceled
	transport.mu.Unlock()
	if activeWhileBlocked != 1 || maximumWhileBlocked != 2 {
		t.Fatalf("fetch window active/max before releasing first response=%d/%d, want 1/2; calls=%v firstCanceled=%v", activeWhileBlocked, maximumWhileBlocked, calls, firstCanceled)
	}
	current, err := manager.Get(owner.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Tracks["main"].Segments) != 0 {
		t.Fatalf("canonical segments published before ordered window completed: %d", len(current.Tracks["main"].Segments))
	}
	releaseOnce.Do(func() { close(transport.releaseFirst) })
	select {
	case err := <-passDone:
		if err != nil {
			t.Fatalf("repair manifest window: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("repair manifest window did not finish")
	}
	current, err = manager.Get(owner.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Tracks["main"].Segments) != 3 {
		t.Fatalf("canonical segment count=%d, want 3", len(current.Tracks["main"].Segments))
	}
	for index, sequence := range []uint64{1, 2, 3} {
		segment := current.Tracks["main"].Segments[index]
		if segment.Sequence != sequence || segment.ArchiveOrdinal != uint64(index+1) || segment.TimelineOrdinal != uint64(index+1) {
			t.Fatalf("publication order changed: index=%d segment=%#v", index, segment)
		}
	}
	if current.TimelineRevision <= 1 || current.Duration() != 6 {
		t.Fatalf("timeline projection revision/duration=%d/%g", current.TimelineRevision, current.Duration())
	}
	transport.mu.Lock()
	maximum := transport.maximum
	transport.mu.Unlock()
	if maximum > initialHistoricalFetchConcurrency {
		t.Fatalf("historical request concurrency=%d, initial cap=%d", maximum, initialHistoricalFetchConcurrency)
	}
	if maximum != initialHistoricalFetchConcurrency {
		t.Fatalf("historical request concurrency=%d, want initial cap %d", maximum, initialHistoricalFetchConcurrency)
	}
}

func TestHistoricalWindowCancellationWaitsForFetchesAndReleasesSpoolSlots(t *testing.T) {
	manifest := "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXTINF:2,\nsegment-2.ts\n#EXT-X-ENDLIST\n"
	transport := &orderedHistoricalTransport{
		manifest: []byte(manifest), firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), releaseFirst: make(chan struct{}),
	}
	manager, _, owner := newManifestWindowTestManager(t, transport, "dddddddddddddddddddddddddddddddd")
	ctx, cancel := context.WithCancel(context.Background())
	passDone := make(chan error, 1)
	go func() { passDone <- manager.RepairDeclaredHistory(ctx, owner, owner.RecordingID) }()
	select {
	case <-transport.firstStarted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("first historical fetch did not start")
	}
	select {
	case <-transport.secondStarted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("second historical fetch did not start")
	}
	cancel()
	select {
	case err := <-passDone:
		if err == nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
			t.Fatalf("canceled recovery error=%v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery did not wait for and join fetch workers")
	}
	manager.historicalSpools.mu.Lock()
	activeSpools := manager.historicalSpools.active
	manager.historicalSpools.mu.Unlock()
	if activeSpools != 0 {
		t.Fatalf("historical spool slots after cancellation=%d, want zero", activeSpools)
	}
}
