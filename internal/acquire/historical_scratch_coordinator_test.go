package acquire

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/storage"
)

type historicalScratchLeaseCall struct {
	leaseID string
	objects int
	bytes   int64
}

type historicalScratchReleaseContext struct {
	hasDeadline bool
	wasCanceled bool
}

type historicalScratchCoordinatorSpy struct {
	mu             sync.Mutex
	acquires       []historicalScratchLeaseCall
	releases       []string
	releaseContext []historicalScratchReleaseContext
	acquireErr     error
	releaseErrors  []error
}

type contextWaitingReadCloser struct{ ctx context.Context }

func (r contextWaitingReadCloser) Read([]byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (contextWaitingReadCloser) Close() error { return nil }

func (s *historicalScratchCoordinatorSpy) AcquireHistoricalScratch(_ context.Context, leaseID string, objects int, bytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquires = append(s.acquires, historicalScratchLeaseCall{leaseID: leaseID, objects: objects, bytes: bytes})
	return s.acquireErr
}

func (s *historicalScratchCoordinatorSpy) ReleaseHistoricalScratch(ctx context.Context, leaseID string) error {
	_, hasDeadline := ctx.Deadline()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases = append(s.releases, leaseID)
	s.releaseContext = append(s.releaseContext, historicalScratchReleaseContext{
		hasDeadline: hasDeadline,
		wasCanceled: ctx.Err() != nil,
	})
	index := len(s.releases) - 1
	if index < len(s.releaseErrors) {
		return s.releaseErrors[index]
	}
	return nil
}

func TestHistoricalScratchCoordinatorReservesAndReleasesWindow(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXTINF:2,\nsegment-2.ts\n#EXT-X-ENDLIST\n")
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponse(request, []byte(request.URL.Path)), nil
	}), "17171717171717171717171717171717")
	coordinator := &historicalScratchCoordinatorSpy{}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); err != nil {
		t.Fatalf("repair historical window: %v", err)
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if len(coordinator.acquires) != 1 || len(coordinator.releases) != 1 {
		t.Fatalf("Host scratch acquire/release calls=%d/%d, want 1/1", len(coordinator.acquires), len(coordinator.releases))
	}
	acquired := coordinator.acquires[0]
	if acquired.leaseID == "" || acquired.objects != 2 || acquired.bytes != 2*storage.DefaultIngestOptions().MaxPayloadBytes {
		t.Fatalf("Host scratch reservation=%+v, want 2 objects and conservative payload bytes", acquired)
	}
	if coordinator.releases[0] != acquired.leaseID {
		t.Fatalf("released lease=%q, acquired lease=%q", coordinator.releases[0], acquired.leaseID)
	}
	if len(coordinator.releaseContext) != 1 || !coordinator.releaseContext[0].hasDeadline || coordinator.releaseContext[0].wasCanceled {
		t.Fatalf("Host release context=%+v, want fresh bounded context", coordinator.releaseContext)
	}
}

func TestHistoricalScratchCoordinatorReleaseSurvivesCancellation(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
	started := make(chan struct{})
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		close(started)
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header), Body: contextWaitingReadCloser{ctx: request.Context()},
			ContentLength: -1, Request: request,
		}, nil
	}), "18181818181818181818181818181818")
	coordinator := &historicalScratchCoordinatorSpy{}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	passDone := make(chan error, 1)
	go func() { passDone <- manager.RepairDeclaredHistory(ctx, owner, owner.RecordingID) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("historical request did not start")
	}
	cancel()
	select {
	case err := <-passDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled repair error=%v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled repair did not finish")
	}
	assertHistoricalScratchLeaseReleased(t, manager, coordinator)
}

func TestHistoricalScratchCoordinatorReleaseAfterCommitFailure(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
	options := storage.DefaultIngestOptions()
	options.PersistAttempts = 1
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponse(request, []byte("historical segment")), nil
	}), "19191919191919191919191919191919", options)
	coordinator := &historicalScratchCoordinatorSpy{}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("injected historical commit failure")
	backend := &canonicalCommitFaultBackend{
		StorageBackend: manager.store.StorageBackend,
		target:         "payload",
		cause:          cause,
	}
	manager.store.StorageBackend = backend
	err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID)
	if !backend.fired.Load() {
		t.Fatal("historical payload fault was not reached")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("repair error=%v, want injected commit failure", err)
	}
	assertHistoricalScratchLeaseReleased(t, manager, coordinator)
}

func TestHistoricalScratchCoordinatorAdmissionFailureReleasesLocalReservation(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponse(request, []byte("historical segment")), nil
	}), "20202020202020202020202020202020")
	coordinator := &historicalScratchCoordinatorSpy{acquireErr: errors.New("Host reservation failed")}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); err == nil {
		t.Fatal("repair succeeded after Host scratch admission failed")
	}
	assertHistoricalScratchLeaseReleased(t, manager, coordinator)
}

func TestHistoricalScratchReleaseFailureRetriesBeforeNextAcquisition(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
	transport := testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponse(request, []byte(request.URL.Path)), nil
	})
	manager, _, owner := newManifestWindowTestManager(t, transport, "21212121212121212121212121212121")
	transientErr := errors.New("temporary Host release failure")
	coordinator := &historicalScratchCoordinatorSpy{releaseErrors: []error{transientErr, transientErr, transientErr}}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); !errors.Is(err, transientErr) {
		t.Fatalf("first repair error=%v, want final Host release error", err)
	}
	manager.historicalSpools.mu.Lock()
	activeObjects, activeBytes := manager.historicalSpools.active, manager.historicalSpools.activeBytes
	manager.historicalSpools.mu.Unlock()
	if activeObjects != 0 || activeBytes != 0 {
		t.Fatalf("local scratch reservations leaked after Host release failure: objects=%d bytes=%d", activeObjects, activeBytes)
	}

	manifest = []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXTINF:2,\nsegment-2.ts\n#EXT-X-ENDLIST\n")
	owner = ownerForRepair(owner.RecordingID, owner.Epoch+1)
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); err != nil {
		t.Fatalf("second repair after release retry: %v", err)
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if len(coordinator.acquires) != 2 || len(coordinator.releases) != 5 {
		t.Fatalf("Host scratch calls after retry: acquires=%d releases=%d, want 2/5", len(coordinator.acquires), len(coordinator.releases))
	}
	firstLease, secondLease := coordinator.acquires[0].leaseID, coordinator.acquires[1].leaseID
	for index := 0; index < 4; index++ {
		if coordinator.releases[index] != firstLease {
			t.Fatalf("release %d used lease %q, want retained lease %q", index, coordinator.releases[index], firstLease)
		}
	}
	if coordinator.releases[4] != secondLease || firstLease == secondLease {
		t.Fatalf("new window release=%q, leases=%q/%q", coordinator.releases[4], firstLease, secondLease)
	}
}

func TestManagerCloseRetriesPendingHistoricalScratchRelease(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponse(request, []byte(request.URL.Path)), nil
	}), "22222222222222222222222222222222")
	transientErr := errors.New("temporary Host release failure")
	coordinator := &historicalScratchCoordinatorSpy{releaseErrors: []error{transientErr, transientErr, transientErr}}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); !errors.Is(err, transientErr) {
		t.Fatalf("repair error=%v, want final Host release error", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Fatalf("manager close did not clear pending Host scratch lease: %v", err)
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if len(coordinator.acquires) != 1 || len(coordinator.releases) != 4 {
		t.Fatalf("Host scratch calls after Close: acquires=%d releases=%d, want 1/4", len(coordinator.acquires), len(coordinator.releases))
	}
	for index, leaseID := range coordinator.releases {
		if leaseID != coordinator.acquires[0].leaseID {
			t.Fatalf("release %d used lease %q, want retained lease %q", index, leaseID, coordinator.acquires[0].leaseID)
		}
	}
}

func TestManagerCloseBoundsPendingScratchReleaseByCallerDeadline(t *testing.T) {
	manifest := []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n" +
		"#EXTINF:2,\nsegment-1.ts\n#EXT-X-ENDLIST\n")
	manager, _, owner := newManifestWindowTestManager(t, testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/archive/index.m3u8" {
			return historicalTestResponse(request, manifest), nil
		}
		return historicalTestResponse(request, []byte(request.URL.Path)), nil
	}), "23232323232323232323232323232323")
	transientErr := errors.New("temporary Host release failure")
	coordinator := &historicalScratchCoordinatorSpy{releaseErrors: []error{transientErr, transientErr, transientErr, transientErr}}
	if err := manager.ConfigureHistoricalScratchCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	if err := manager.RepairDeclaredHistory(context.Background(), owner, owner.RecordingID); !errors.Is(err, transientErr) {
		t.Fatalf("repair error=%v, want final Host release error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := manager.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Manager.Close error=%v, want caller deadline", err)
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if len(coordinator.releases) != 4 {
		t.Fatalf("Host release calls after bounded Close=%d, want initial 3 plus one deadline-bounded attempt", len(coordinator.releases))
	}
}

func assertHistoricalScratchLeaseReleased(t *testing.T, manager *Manager, coordinator *historicalScratchCoordinatorSpy) {
	t.Helper()
	coordinator.mu.Lock()
	if len(coordinator.acquires) != 1 || len(coordinator.releases) != 1 {
		coordinator.mu.Unlock()
		t.Fatalf("Host scratch acquire/release calls=%d/%d, want 1/1", len(coordinator.acquires), len(coordinator.releases))
	}
	acquiredLease := coordinator.acquires[0].leaseID
	releasedLease := coordinator.releases[0]
	contexts := append([]historicalScratchReleaseContext(nil), coordinator.releaseContext...)
	coordinator.mu.Unlock()
	if acquiredLease == "" || releasedLease != acquiredLease {
		t.Fatalf("Host scratch lease acquire/release=%q/%q", acquiredLease, releasedLease)
	}
	if len(contexts) != 1 || !contexts[0].hasDeadline || contexts[0].wasCanceled {
		t.Fatalf("Host release context=%+v, want fresh bounded context", contexts)
	}
	manager.historicalSpools.mu.Lock()
	activeObjects, activeBytes := manager.historicalSpools.active, manager.historicalSpools.activeBytes
	manager.historicalSpools.mu.Unlock()
	if activeObjects != 0 || activeBytes != 0 {
		t.Fatalf("local scratch reservations leaked: objects=%d bytes=%d", activeObjects, activeBytes)
	}
}
