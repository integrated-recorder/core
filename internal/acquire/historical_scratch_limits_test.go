package acquire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/storage"
)

type boundedHistoryTransport struct {
	manifest []byte
	release  <-chan struct{}
	started  chan string
	mu       sync.Mutex
	active   int
	maximum  int
	calls    int
}

func (t *boundedHistoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/archive/index.m3u8" {
		return historicalTestResponse(request, t.manifest), nil
	}
	t.mu.Lock()
	t.active++
	t.calls++
	if t.active > t.maximum {
		t.maximum = t.active
	}
	t.mu.Unlock()
	select {
	case t.started <- request.URL.Path:
	case <-request.Context().Done():
		t.finish()
		return nil, request.Context().Err()
	}
	select {
	case <-t.release:
	case <-request.Context().Done():
		t.finish()
		return nil, request.Context().Err()
	}
	t.finish()
	return historicalTestResponse(request, []byte(request.URL.Path)), nil
}

func (t *boundedHistoryTransport) finish() {
	t.mu.Lock()
	t.active--
	t.mu.Unlock()
}

func TestDefaultHistoricalScratchWindowReachesGovernorMaximum(t *testing.T) {
	var manifest strings.Builder
	manifest.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n")
	for sequence := 1; sequence <= maxHistoricalScratchObjects+1; sequence++ {
		fmt.Fprintf(&manifest, "#EXTINF:2,\nsegment-%d.ts\n", sequence)
	}
	manifest.WriteString("#EXT-X-ENDLIST\n")
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	transport := &boundedHistoryTransport{
		manifest: []byte(manifest.String()), release: release,
		started: make(chan string, maxHistoricalScratchObjects+2),
	}
	manager, _, owner := newManifestWindowTestManager(t, transport, "12121212121212121212121212121212")
	if got := manager.historicalSpools.capacity(); got != maxHistoricalFetchConcurrency {
		t.Fatalf("default scratch object cap=%d, want %d", got, maxHistoricalFetchConcurrency)
	}
	if got := maxConcurrentHistoricalSpools(storage.DefaultIngestOptions()); got != maxHistoricalFetchConcurrency {
		t.Fatalf("default spool cap=%d, governor cap=%d", got, maxHistoricalFetchConcurrency)
	}

	base := time.Unix(1_800_000_000, 0)
	governor := manager.historicalFetch
	governor.observe(storage.IngestSnapshot{}, base)
	governor.observe(storage.IngestSnapshot{}, base.Add(historicalHealthyInterval))
	governor.observe(storage.IngestSnapshot{}, base.Add(2*historicalHealthyInterval))
	governor.mu.Lock()
	governor.clock = func() time.Time { return base.Add(2 * historicalHealthyInterval) }
	governor.mu.Unlock()
	if got := governor.currentLimit(); got != maxHistoricalFetchConcurrency {
		t.Fatalf("expanded governor limit=%d, want %d", got, maxHistoricalFetchConcurrency)
	}

	passDone := make(chan error, 1)
	go func() { passDone <- manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID) }()
	for index := 0; index < maxHistoricalFetchConcurrency; index++ {
		select {
		case <-transport.started:
		case <-time.After(5 * time.Second):
			releaseAll()
			t.Fatalf("historical fetch %d did not enter expanded window", index+1)
		}
	}
	manager.historicalSpools.mu.Lock()
	activeSpools := manager.historicalSpools.active
	manager.historicalSpools.mu.Unlock()
	if activeSpools != maxHistoricalScratchObjects {
		t.Fatalf("active scratch objects=%d, want bounded maximum %d", activeSpools, maxHistoricalScratchObjects)
	}
	transport.mu.Lock()
	active, maximum := transport.active, transport.maximum
	transport.mu.Unlock()
	if active != maxHistoricalFetchConcurrency || maximum != maxHistoricalFetchConcurrency {
		t.Fatalf("actual historical fetch window active/max=%d/%d, want %d/%d", active, maximum, maxHistoricalFetchConcurrency, maxHistoricalFetchConcurrency)
	}
	select {
	case <-transport.started:
		releaseAll()
		t.Fatal("fetch beyond bounded window started before first window completed")
	case <-time.After(25 * time.Millisecond):
	}
	releaseAll()
	select {
	case err := <-passDone:
		if err != nil {
			t.Fatalf("repair historical window: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("historical pass did not finish after releasing scratch requests")
	}
	transport.mu.Lock()
	maximum = transport.maximum
	calls := transport.calls
	transport.mu.Unlock()
	if maximum != maxHistoricalFetchConcurrency || calls != maxHistoricalFetchConcurrency+1 {
		t.Fatalf("fetch max/calls=%d/%d, want %d/%d", maximum, calls, maxHistoricalFetchConcurrency, maxHistoricalFetchConcurrency+1)
	}
	manager.historicalSpools.mu.Lock()
	activeSpools = manager.historicalSpools.active
	manager.historicalSpools.mu.Unlock()
	if activeSpools != 0 {
		t.Fatalf("scratch slots after completed pass=%d, want zero", activeSpools)
	}
}

func TestHistoricalSpoolPoolBoundsConcurrentWindows(t *testing.T) {
	pool := newHistoricalSpoolPool(maxHistoricalScratchObjects)
	maxPayloadBytes := storage.DefaultIngestOptions().MaxPayloadBytes
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := pool.acquire(context.Background(), 2, 2*maxPayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.acquire(context.Background(), 2, 2*maxPayloadBytes)
	if err != nil {
		first()
		t.Fatal(err)
	}
	third := make(chan func(), 1)
	thirdErr := make(chan error, 1)
	go func() {
		release, acquireErr := pool.acquire(ctx, 1, maxPayloadBytes)
		if acquireErr != nil {
			thirdErr <- acquireErr
			return
		}
		third <- release
	}()
	select {
	case <-third:
		first()
		second()
		t.Fatal("concurrent windows exceeded shared scratch object bound")
	case err := <-thirdErr:
		first()
		second()
		t.Fatal(err)
	case <-time.After(25 * time.Millisecond):
	}
	pool.mu.Lock()
	active := pool.active
	pool.mu.Unlock()
	if active != maxHistoricalScratchObjects {
		first()
		second()
		t.Fatalf("reserved scratch objects=%d, want %d", active, maxHistoricalScratchObjects)
	}
	first()
	var thirdRelease func()
	select {
	case thirdRelease = <-third:
	case err := <-thirdErr:
		second()
		t.Fatal(err)
	case <-time.After(time.Second):
		second()
		t.Fatal("scratch reservation did not proceed after a window released slots")
	}
	second()
	thirdRelease()
	pool.mu.Lock()
	active = pool.active
	pool.mu.Unlock()
	if active != 0 {
		t.Fatalf("scratch pool leaked %d object reservations", active)
	}
}

func TestManagersShareProcessHistoricalSpoolPool(t *testing.T) {
	firstManager, _, _ := newManifestWindowTestManager(t, nil, "15151515151515151515151515151515")
	secondManager, _, _ := newManifestWindowTestManager(t, nil, "16161616161616161616161616161616")
	if firstManager.historicalSpools != secondManager.historicalSpools {
		t.Fatal("Managers do not share process historical scratch pool")
	}

	maxPayloadBytes := storage.DefaultIngestOptions().MaxPayloadBytes
	firstRelease, err := firstManager.historicalSpools.acquire(context.Background(), maxHistoricalScratchObjects, maxHistoricalScratchBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	secondResult := make(chan func(), 1)
	secondErr := make(chan error, 1)
	go func() {
		release, acquireErr := secondManager.historicalSpools.acquire(ctx, 1, maxPayloadBytes)
		if acquireErr != nil {
			secondErr <- acquireErr
			return
		}
		secondResult <- release
	}()
	select {
	case <-secondResult:
		firstRelease()
		t.Fatal("second Manager exceeded process scratch quota")
	case err := <-secondErr:
		firstRelease()
		t.Fatal(err)
	case <-time.After(25 * time.Millisecond):
	}
	firstRelease()
	select {
	case release := <-secondResult:
		release()
	case err := <-secondErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("second Manager did not acquire scratch after first release")
	}

	firstManager.historicalSpools.mu.Lock()
	activeObjects, activeBytes := firstManager.historicalSpools.active, firstManager.historicalSpools.activeBytes
	firstManager.historicalSpools.mu.Unlock()
	if activeObjects != 0 || activeBytes != 0 {
		t.Fatalf("process scratch reservations leaked: objects=%d bytes=%d", activeObjects, activeBytes)
	}
}

func TestHistoricalSpoolPoolBoundsBytesAndReleaseIsIdempotent(t *testing.T) {
	pool := newHistoricalSpoolPool(maxHistoricalScratchObjects, 1<<30)
	if got := pool.capacityFor(2 << 30); got != 0 {
		t.Fatalf("payload larger than scratch byte budget has capacity %d, want zero", got)
	}
	first, err := pool.acquire(context.Background(), 2, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	first()
	first()
	pool.mu.Lock()
	activeObjects, activeBytes := pool.active, pool.activeBytes
	pool.mu.Unlock()
	if activeObjects != 0 || activeBytes != 0 {
		t.Fatalf("idempotent release left reservations: objects=%d bytes=%d", activeObjects, activeBytes)
	}

	first, err = pool.acquire(context.Background(), 2, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	secondResult := make(chan func(), 1)
	secondErr := make(chan error, 1)
	go func() {
		release, acquireErr := pool.acquire(ctx, 1, 512<<20)
		if acquireErr != nil {
			secondErr <- acquireErr
			return
		}
		secondResult <- release
	}()
	select {
	case <-secondResult:
		first()
		t.Fatal("scratch reservation exceeded byte quota")
	case err := <-secondErr:
		first()
		t.Fatal(err)
	case <-time.After(25 * time.Millisecond):
	}
	first()
	select {
	case second := <-secondResult:
		second()
	case err := <-secondErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("byte-limited scratch reservation did not proceed after release")
	}
}

func TestHistoricalWindowCapacityFitsConfiguredPayloadLimit(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultIngestOptions()
	options.GlobalBytes = 2 << 30
	options.PerRecordingBytes = 1536 << 20
	options.MaxPayloadBytes = 1 << 30
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, nil, nil, nil, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close configured scratch manager: %v", err)
		}
	})
	if got := manager.historicalWindowCapacity(); got != 2 {
		t.Fatalf("1 GiB payload window capacity=%d, want 2", got)
	}
}

type trackedPriorityBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *trackedPriorityBody) Close() error {
	b.closed.Add(1)
	return nil
}

func TestLivePriorityPreservesActiveHistoryAndResumesAfterBodyRelease(t *testing.T) {
	for _, releaseLiveBy := range []string{"eof", "close"} {
		t.Run(releaseLiveBy, func(t *testing.T) {
			arrivals := make(chan string, maxHistoricalFetchConcurrency+2)
			bodies := make(chan *trackedPriorityBody, maxHistoricalFetchConcurrency+2)
			contexts := make([]context.Context, 0, maxHistoricalFetchConcurrency+2)
			var contextsMu sync.Mutex
			base := testRoundTripper(func(request *http.Request) (*http.Response, error) {
				contextsMu.Lock()
				contexts = append(contexts, request.Context())
				contextsMu.Unlock()
				arrivals <- request.URL.Path
				body := &trackedPriorityBody{Reader: strings.NewReader("body")}
				bodies <- body
				return &http.Response{
					StatusCode: http.StatusOK, Header: make(http.Header), Body: body, Request: request,
				}, nil
			})
			governor := newHistoricalFetchGovernor(nil)
			governor.mu.Lock()
			governor.setLimitLocked(maxHistoricalFetchConcurrency)
			governor.mu.Unlock()
			transport := historicalPriorityRoundTripper{base: base, governor: governor}
			newRequest := func(path string, historical bool) *http.Request {
				ctx := context.Background()
				if historical {
					ctx = withHistoricalAcquisitionPriority(ctx)
				}
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://media.example"+path, nil)
				if err != nil {
					t.Fatal(err)
				}
				return request
			}

			historyBodies := make([]*governedResponseBody, 0, maxHistoricalFetchConcurrency)
			underlying := make([]*trackedPriorityBody, 0, maxHistoricalFetchConcurrency+2)
			for index := 0; index < maxHistoricalFetchConcurrency; index++ {
				path := fmt.Sprintf("/recording-a/history-%d", index)
				response, err := transport.RoundTrip(newRequest(path, true))
				if err != nil {
					t.Fatal(err)
				}
				if got := <-arrivals; got != path {
					t.Fatalf("history arrival=%q, want %q", got, path)
				}
				historyBodies = append(historyBodies, response.Body.(*governedResponseBody))
				underlying = append(underlying, (<-bodies))
			}

			live, err := transport.RoundTrip(newRequest("/recording-b/live", false))
			if err != nil {
				t.Fatal(err)
			}
			if got := <-arrivals; got != "/recording-b/live" {
				t.Fatalf("live request waited behind history: arrival=%q", got)
			}
			liveBody := live.Body.(*governedResponseBody)
			underlying = append(underlying, <-bodies)

			historyWaiter := make(chan *http.Response, 1)
			historyErr := make(chan error, 1)
			go func() {
				response, roundTripErr := transport.RoundTrip(newRequest("/recording-b/history-next", true))
				if roundTripErr != nil {
					historyErr <- roundTripErr
					return
				}
				historyWaiter <- response
			}()
			select {
			case got := <-arrivals:
				t.Fatalf("new history entered with live body active: %q", got)
			case err := <-historyErr:
				t.Fatal(err)
			case <-time.After(30 * time.Millisecond):
			}
			for index, body := range underlying[:maxHistoricalFetchConcurrency] {
				if body.closed.Load() != 0 {
					t.Fatalf("active historical body %d was closed during live preemption", index)
				}
			}
			contextsMu.Lock()
			for index, requestContext := range contexts[:maxHistoricalFetchConcurrency] {
				if requestContext.Err() != nil {
					contextsMu.Unlock()
					t.Fatalf("active historical request %d was canceled during live preemption: %v", index, requestContext.Err())
				}
			}
			contextsMu.Unlock()
			governor.mu.Lock()
			activeHistory, activeLive := governor.active, governor.liveActive
			governor.mu.Unlock()
			if activeHistory != maxHistoricalFetchConcurrency || activeLive != 1 {
				t.Fatalf("active history/live=%d/%d, want %d/1", activeHistory, activeLive, maxHistoricalFetchConcurrency)
			}

			if releaseLiveBy == "eof" {
				if _, err := io.Copy(io.Discard, liveBody); err != nil {
					t.Fatal(err)
				}
			} else if err := liveBody.Close(); err != nil {
				t.Fatal(err)
			}
			// A waiting admission observes the live-body release asynchronously.
			// Drive one healthy observation here so the state assertion stays
			// deterministic and does not depend on the polling interval.
			governor.observe(storage.IngestSnapshot{}, time.Now())
			governor.mu.Lock()
			activeLive, limit := governor.liveActive, governor.limit
			governor.mu.Unlock()
			if activeLive != 0 || limit != minHistoricalFetchConcurrency {
				t.Fatalf("live release state active/limit=%d/%d, want 0/%d", activeLive, limit, minHistoricalFetchConcurrency)
			}
			select {
			case got := <-arrivals:
				t.Fatalf("history bypassed still-active history capacity after live release: %q", got)
			case <-time.After(30 * time.Millisecond):
			}
			for _, responseBody := range historyBodies {
				if err := responseBody.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case response := <-historyWaiter:
				if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
			case err := <-historyErr:
				t.Fatal(err)
			case <-time.After(time.Second):
				t.Fatal("historical admission did not resume after active bodies released")
			}
			if got := <-arrivals; got != "/recording-b/history-next" {
				t.Fatalf("resumed history arrival=%q", got)
			}
			if err := liveBody.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHistoricalFetchFailureCleansScratchObjectsAndSlots(t *testing.T) {
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponseStatus(request, nil, http.StatusNotFound), nil
	}), "13131313131313131313131313131313")
	scratchRoot, err := os.MkdirTemp("", "integrated-recorder-history-test-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(scratchRoot, 0o700); err != nil {
		_ = os.Remove(scratchRoot)
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", scratchRoot)
	t.Cleanup(func() {
		if err := os.Remove(scratchRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("remove scratch test root: %v", err)
		}
	})
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); err != nil {
		t.Fatalf("repair after failed source request: %v", err)
	}
	manager.historicalSpools.mu.Lock()
	activeSpools := manager.historicalSpools.active
	manager.historicalSpools.mu.Unlock()
	if activeSpools != 0 {
		t.Fatalf("scratch slots after failed fetch=%d, want zero", activeSpools)
	}
	entries, err := os.ReadDir(scratchRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed historical fetch retained %d scratch entries", len(entries))
	}
}
