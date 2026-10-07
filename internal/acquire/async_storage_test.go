package acquire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestBlockedStorageWriterDoesNotBlockSegmentFetchWorkers(t *testing.T) {
	const segmentCount = 8
	var fetched atomic.Int32
	var activeRequests atomic.Int32
	var maxConcurrent atomic.Int32
	var requestCounts [segmentCount]atomic.Int32
	firstFourReady := make(chan struct{})
	releaseResponses := make(chan struct{})
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1")
			for i := 1; i <= segmentCount; i++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\n%d.ts\n", i)
			}
			return
		}
		sequence, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), ".ts"))
		if err != nil || sequence < 1 || sequence > segmentCount {
			http.NotFound(w, r)
			return
		}
		requestCounts[sequence-1].Add(1)
		fetched.Add(1)
		active := activeRequests.Add(1)
		for old := maxConcurrent.Load(); active > old && !maxConcurrent.CompareAndSwap(old, active); old = maxConcurrent.Load() {
		}
		if active == 4 {
			close(firstFourReady)
		}
		if sequence <= 4 {
			<-releaseResponses
		}
		body := []byte(fmt.Sprintf("canonical-source-%d", sequence))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
		activeRequests.Add(-1)
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
	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{})
	var hookCalls atomic.Int32
	manager.storageWriteHook = func() {
		if hookCalls.Add(1) == 1 {
			close(started)
			<-release
		}
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "slow storage", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, firstFourReady, "four concurrent segment requests")
	if maxConcurrent.Load() < 4 {
		t.Fatalf("maximum concurrent HTTP bodies=%d want at least 4", maxConcurrent.Load())
	}
	close(releaseResponses)
	waitSignal(t, started, "blocked storage writer")
	waitAtomicCount(t, &fetched, segmentCount, "segment fetches while storage is blocked")
	waitUntil(t, 2*time.Second, func() bool {
		return manager.ingest.Snapshot().BufferUsedBytes >= int64(segmentCount*len("canonical-source-1"))
	}, "all source payloads buffered")
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := current.SegmentCount(); got != 0 {
		t.Fatalf("buffered payloads became canonical before storage commit: segment count=%d", got)
	}
	first := domain.Segment{TrackID: "main", Sequence: 1, SourceURI: server.URL + "/1.ts"}
	logicalID, identityErr := archiveindex.SegmentIdentity(coordinateForSegment(current.SourceSessionID, first))
	if identityErr != nil {
		t.Fatal(identityErr)
	}
	firstPath := immutableObjectPath(logicalID, sha256Hex([]byte("canonical-source-1")), first.SourceURI)
	if _, err = store.StatPayload(recording.ID, firstPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical payload was visible or stat failed unexpectedly before commit: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	waitForSegmentCount(t, manager, recording.ID, segmentCount)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(stopped.Tracks["main"].Segments); got != segmentCount {
		t.Fatalf("committed segment count=%d want=%d", got, segmentCount)
	}
	for i, segment := range stopped.Tracks["main"].Segments {
		if segment.ArchiveOrdinal != uint64(i+1) || segment.Sequence != uint64(i+1) {
			t.Fatalf("segment[%d] ordering = ordinal %d sequence %d", i, segment.ArchiveOrdinal, segment.Sequence)
		}
		if requestCounts[i].Load() != 1 {
			t.Fatalf("sequence %d was fetched %d times", i+1, requestCounts[i].Load())
		}
	}
}

func TestStorageRetryReusesFetchedBytesWithoutNetworkRedownload(t *testing.T) {
	var requests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:7\n#EXTINF:1,\n7.ts\n")
			return
		}
		requests.Add(1)
		_, _ = io.WriteString(w, "same fetched bytes")
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultIngestOptions()
	options.PersistAttempts = 3
	options.RetryBase = 10 * time.Millisecond
	options.RetryMaxBackoff = 10 * time.Millisecond
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	manager.storageWriteFailureHook = func() error {
		if attempts.Add(1) < 3 {
			return errors.New("temporary storage failure")
		}
		return nil
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "retry storage", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSegmentCount(t, manager, recording.ID, 1)
	if got := requests.Load(); got != 1 {
		t.Fatalf("network segment fetch count=%d after storage retry, want 1", got)
	}
	if attempts.Load() != int32(options.PersistAttempts) {
		t.Fatalf("storage hook attempts=%d, want configured attempt count %d", attempts.Load(), options.PersistAttempts)
	}
	if _, err = manager.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
}

type transientSegmentRootBackend struct {
	storage.StorageBackend
	failures atomic.Int32
}

type permanentSegmentRootBackend struct {
	storage.StorageBackend
	segments int
	cause    error
	failed   atomic.Bool
}

func (b *permanentSegmentRootBackend) SaveRecording(recording *domain.Recording) error {
	if track := recording.Tracks["main"]; track != nil && len(track.Segments) == b.segments && b.failed.CompareAndSwap(false, true) {
		return b.cause
	}
	return b.StorageBackend.SaveRecording(recording)
}

func (b *permanentSegmentRootBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	reader, ok := b.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	})
	if !ok {
		return storage.ErrSidecarReadUnsupported
	}
	return reader.LoadSidecar(id, relativePath, maxBytes, output)
}

func (b *permanentSegmentRootBackend) CreateRecordingWithSidecar(recording *domain.Recording, relativePath string, value any) error {
	creator, ok := b.StorageBackend.(interface {
		CreateRecordingWithSidecar(*domain.Recording, string, any) error
	})
	if !ok {
		return storage.ErrAtomicRecordingCreationUnsupported
	}
	return creator.CreateRecordingWithSidecar(recording, relativePath, value)
}

func (b *transientSegmentRootBackend) SaveRecording(recording *domain.Recording) error {
	if track := recording.Tracks["main"]; track != nil && len(track.Segments) > 0 && b.failures.CompareAndSwap(0, 1) {
		return errors.New("temporary root metadata write failure")
	}
	return b.StorageBackend.SaveRecording(recording)
}

func (b *transientSegmentRootBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	reader, ok := b.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	})
	if !ok {
		return storage.ErrSidecarReadUnsupported
	}
	return reader.LoadSidecar(id, relativePath, maxBytes, output)
}

func (b *transientSegmentRootBackend) CreateRecordingWithSidecar(recording *domain.Recording, relativePath string, value any) error {
	creator, ok := b.StorageBackend.(interface {
		CreateRecordingWithSidecar(*domain.Recording, string, any) error
	})
	if !ok {
		return storage.ErrAtomicRecordingCreationUnsupported
	}
	return creator.CreateRecordingWithSidecar(recording, relativePath, value)
}

func TestTransientRootMetadataFailureDoesNotPoisonSuccessfulStorageRetry(t *testing.T) {
	var requests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:11\n#EXTINF:1,\n11.ts\n")
			return
		}
		requests.Add(1)
		_, _ = io.WriteString(w, "canonical source bytes")
	}))
	defer server.Close()
	localStore, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &transientSegmentRootBackend{StorageBackend: localStore.StorageBackend}
	store := &storage.Store{StorageBackend: backend}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "root metadata retry", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSegmentCount(t, manager, recording.ID, 1)
	if backend.failures.Load() != 1 || requests.Load() != 1 {
		t.Fatalf("root write failures=%d network requests=%d, want one each", backend.failures.Load(), requests.Load())
	}
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatalf("successful storage retry left a terminal error: %v", err)
	}
	if len(stopped.Tracks["main"].Segments) != 1 {
		t.Fatalf("successful retry selected segments=%#v", stopped.Tracks["main"].Segments)
	}
	path := stopped.Tracks["main"].Segments[0].StoragePath
	if got, statErr := localStore.StatPayload(recording.ID, path); statErr != nil || !got.Regular || got.Size == 0 {
		t.Fatalf("canonical segment after retry at %q = %#v, %v", path, got, statErr)
	}
}

func TestPermanentRootCommitFailureRetainsInternalCauseAndSanitizesPublicError(t *testing.T) {
	const segmentCount = 5
	var requests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprintln(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:21")
			for sequence := 21; sequence < 21+segmentCount; sequence++ {
				_, _ = fmt.Fprintf(w, "#EXTINF:1,\n%d.ts\n", sequence)
			}
			return
		}
		requests.Add(1)
		_, _ = fmt.Fprintf(w, "canonical-source-%s", strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), ".ts"))
	}))
	defer server.Close()

	localStore, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultIngestOptions()
	options.PersistAttempts = 1
	options.Writers = 1
	backendFailure := errors.New("synthetic root backend failure")
	backend := &permanentSegmentRootBackend{
		StorageBackend: localStore.StorageBackend,
		segments:       segmentCount,
		cause:          backendFailure,
	}
	store := &storage.Store{StorageBackend: backend}
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "root failure", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current, getErr := manager.Get(recording.ID)
		if backend.failed.Load() || getErr == nil && current.State == domain.StateInterrupted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !backend.failed.Load() {
		current, getErr := manager.Get(recording.ID)
		if getErr != nil {
			t.Fatalf("backend root failure was not reached; recording read failed: %v", getErr)
		}
		t.Fatalf("backend root failure was not reached; state=%s segment_count=%d requests=%d", current.State, current.SegmentCount(), requests.Load())
	}
	waitUntil(t, 5*time.Second, func() bool {
		current, getErr := manager.Get(recording.ID)
		return getErr == nil && current.State == domain.StateInterrupted
	}, "permanent root commit failure terminal state")

	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LastError != "recording storage commit failed" {
		t.Fatalf("public LastError=%q, want sanitized storage error", current.LastError)
	}
	if got := current.SegmentCount(); got != segmentCount-1 {
		t.Fatalf("canonical segment count=%d, want=%d", got, segmentCount-1)
	}
	if len(current.Gaps) != 0 {
		t.Fatalf("storage failure created source gaps: %#v", current.Gaps)
	}
	track := current.Tracks["main"]
	if len(track.PendingSegments) != 0 || len(track.PendingSequences) != 0 {
		t.Fatalf("storage failure left pending source observations: %#v", track)
	}

	e, ok := manager.entry(recording.ID)
	if !ok {
		t.Fatal("recording entry disappeared")
	}
	e.mu.Lock()
	diagnostic := e.storageFailureDiagnostic
	e.mu.Unlock()
	if diagnostic == nil {
		t.Fatal("internal storage failure diagnostic is missing")
	}
	if !errors.Is(diagnostic, errStorageCommit) || !errors.Is(diagnostic, backendFailure) {
		t.Fatalf("diagnostic lost classification or backend cause: %v", diagnostic)
	}
	if !strings.Contains(diagnostic.Error(), "media payload commit") || !strings.Contains(diagnostic.Error(), "recording root commit") {
		t.Fatalf("diagnostic lost commit stage: %v", diagnostic)
	}
	var retryFailure interface{ Attempts() int }
	if !errors.As(diagnostic, &retryFailure) || retryFailure.Attempts() != 1 {
		t.Fatalf("diagnostic retry attempts missing or incorrect: %v", diagnostic)
	}
	if got := safeFailureDescription(diagnostic); got != "recording storage commit failed" {
		t.Fatalf("public sanitizer exposed internal diagnostic: %q", got)
	}
	if !backend.failed.Load() || requests.Load() == 0 {
		t.Fatalf("root failure or source fetch did not occur: failed=%t requests=%d", backend.failed.Load(), requests.Load())
	}
	if _, err := manager.Stop(recording.ID); err == nil || err.Error() != "recording storage commit failed" {
		t.Fatalf("Stop error=%v, want sanitized storage error", err)
	}
}

func TestSchedulerStorageFailureRetainsStageAndCause(t *testing.T) {
	cause := errors.New("synthetic scheduler commit cause")
	scheduler := &segmentScheduler{e: &entry{}, changed: make(chan struct{}), cancel: func() {}}
	scheduler.failStorage("manifest snapshot commit", cause)

	failure := scheduler.failure()
	if !errors.Is(failure, errStorageCommit) || !errors.Is(failure, cause) {
		t.Fatalf("scheduler failure lost classification or cause: %v", failure)
	}
	var staged interface{ Stage() string }
	if !errors.As(failure, &staged) || staged.Stage() != "manifest snapshot commit" {
		t.Fatalf("scheduler failure stage=%v, want manifest snapshot commit", failure)
	}
	if got := safeFailureDescription(failure); got != "recording storage commit failed" {
		t.Fatalf("public sanitizer exposed scheduler cause: %q", got)
	}
}

func TestPermanentStorageFailureIsNotRecordedAsSourceGap(t *testing.T) {
	var requests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:9\n#EXTINF:1,\n9.ts\n")
			return
		}
		requests.Add(1)
		_, _ = io.WriteString(w, "source bytes already fetched")
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
	manager.storageWriteFailureHook = func() error { return errors.New("permanent storage failure") }
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "failed storage", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		current, getErr := manager.Get(recording.ID)
		return getErr == nil && current.State == domain.StateInterrupted
	}, "storage failure terminal state")
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LastError != "recording storage commit failed" || len(current.Gaps) != 0 || current.SegmentCount() != 0 {
		t.Fatalf("storage failure was misclassified: %#v", current)
	}
	if track := current.Tracks["main"]; len(track.PendingSegments) != 0 || len(track.PendingSequences) != 0 {
		t.Fatalf("storage failure left pending source gaps: %#v", track)
	}
	if requests.Load() != 1 {
		t.Fatalf("source was downloaded %d times after storage failure", requests.Load())
	}
}

func TestBlockedSnapshotWriterDoesNotBlockUnchangedManifestPollOrSegmentFetch(t *testing.T) {
	var manifestPolls atomic.Int32
	var segmentRequests atomic.Int32
	manifest := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:12\n#EXTINF:1,\n12.ts\n"
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			manifestPolls.Add(1)
			_, _ = io.WriteString(w, manifest)
		case "/12.ts":
			segmentRequests.Add(1)
			_, _ = io.WriteString(w, "stored source segment")
		default:
			http.NotFound(w, r)
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
	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{})
	var hookCalls atomic.Int32
	manager.storageSnapshotWriteHook = func() {
		if hookCalls.Add(1) == 1 {
			close(started)
			<-release
		}
	}
	cleanup := func() {
		releaseOnce.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	}
	t.Cleanup(cleanup)

	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "blocked manifest storage", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "blocked manifest snapshot writer")
	waitAtomicCount(t, &manifestPolls, 2, "unchanged manifest repoll while snapshot persistence is blocked")
	waitAtomicCount(t, &segmentRequests, 1, "media fetch while snapshot persistence is blocked")
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.SegmentCount() != 0 || len(current.Snapshots) != 0 {
		t.Fatalf("buffered manifest or segment became canonical before writer release: segments=%d snapshots=%d", current.SegmentCount(), len(current.Snapshots))
	}

	// Stop must wait for every accepted snapshot and media commit even though
	// the unchanged playlist has no new sequence to make ENDLIST task-drain
	// accounting cover the snapshot jobs.
	stopDone := make(chan struct{})
	go func() {
		_, _ = manager.Stop(recording.ID)
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("stop returned while accepted manifest snapshot writes were blocked")
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not finish after releasing the storage writer")
	}
	stopped, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.SegmentCount() != 1 || len(stopped.Snapshots) < 1 {
		t.Fatalf("accepted async writes were not drained: segments=%d snapshots=%d", stopped.SegmentCount(), len(stopped.Snapshots))
	}
}

func TestBlockedRootMetadataWriterDoesNotBlockSegmentDiscoveryOrFetch(t *testing.T) {
	var manifestPolls atomic.Int32
	var segmentRequests atomic.Int32
	manifest := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:13\n#EXTINF:1,\n13.ts\n"
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			manifestPolls.Add(1)
			_, _ = io.WriteString(w, manifest)
		case "/13.ts":
			segmentRequests.Add(1)
			_, _ = io.WriteString(w, "source while root write is blocked")
		default:
			http.NotFound(w, r)
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
	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{})
	manager.storageMetadataWriteHook = func() {
		select {
		case <-started:
			return
		default:
			close(started)
			<-release
		}
	}
	cleanup := func() {
		releaseOnce.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	}
	t.Cleanup(cleanup)

	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "blocked root metadata", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "blocked manifest observation root write")
	waitAtomicCount(t, &manifestPolls, 2, "manifest repoll while root metadata persistence is blocked")
	waitAtomicCount(t, &segmentRequests, 1, "segment fetch while root metadata persistence is blocked")
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.SegmentCount() != 0 {
		t.Fatalf("buffered segment became canonical before writer release: %d", current.SegmentCount())
	}
	releaseOnce.Do(func() { close(release) })
	waitForSegmentCount(t, manager, recording.ID, 1)
	if _, err = manager.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
}

func TestNoSegmentEndListWaitsForAcceptedManifestSnapshot(t *testing.T) {
	var manifestPolls atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		manifestPolls.Add(1)
		_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-ENDLIST\n")
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
	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{})
	manager.storageSnapshotWriteHook = func() {
		select {
		case <-started:
		default:
			close(started)
			<-release
		}
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})

	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "empty endlist", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "blocked no-segment ENDLIST snapshot")
	waitAtomicCount(t, &manifestPolls, 1, "no-segment ENDLIST manifest")
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != domain.StateRecording || len(current.Snapshots) != 0 {
		t.Fatalf("ENDLIST completed before its accepted snapshot: state=%s snapshots=%d", current.State, len(current.Snapshots))
	}
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, 5*time.Second, func() bool {
		current, getErr := manager.Get(recording.ID)
		return getErr == nil && current.State == domain.StateCompleted && len(current.Snapshots) == 1
	}, "ENDLIST snapshot durable before terminal state")
}

func TestStaleAsyncManifestSnapshotCannotUpdateNewGeneration(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", SourcePlaylistURL: "https://media.example/old.m3u8", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, http.DefaultClient, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording was not loaded")
	}
	e.mu.Lock()
	e.mediaGeneration = 1
	e.mu.Unlock()
	scheduler, err := newSegmentScheduler(context.Background(), manager, e)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	started := make(chan struct{})
	manager.storageSnapshotWriteHook = func() {
		close(started)
		<-release
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = scheduler.close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})
	payload, err := manager.ingest.ReadPayload(context.Background(), id, strings.NewReader("#EXTM3U\n"), 4<<20, int64(len("#EXTM3U\n")), int64(len("#EXTM3U\n")))
	if err != nil {
		t.Fatal(err)
	}
	if err = scheduler.queueSnapshot(1, "main", "https://media.example/master.m3u8", payload, func(r *domain.Recording) error {
		r.Tracks["main"].SourcePlaylistURL = "https://media.example/new-variant.m3u8"
		r.Tracks["main"].Bandwidth = 99
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "stale generation snapshot writer")
	e.mu.Lock()
	e.mediaGeneration = 2
	e.mu.Unlock()
	releaseOnce.Do(func() { close(release) })
	waitUntil(t, 3*time.Second, func() bool {
		scheduler.mu.Lock()
		pending := scheduler.pendingSnapshots
		scheduler.mu.Unlock()
		return pending == 0
	}, "stale snapshot callback completion")
	current, err := manager.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Snapshots) != 0 || current.Tracks["main"].SourcePlaylistURL != "https://media.example/old.m3u8" || current.Tracks["main"].Bandwidth != 0 {
		t.Fatalf("stale snapshot mutated current generation metadata: snapshots=%d track=%#v", len(current.Snapshots), current.Tracks["main"])
	}
}

type blockingSaveRecordingBackend struct {
	storage.StorageBackend
	blockNext atomic.Bool
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (b *blockingSaveRecordingBackend) SaveRecording(recording *domain.Recording) error {
	if b.blockNext.CompareAndSwap(true, false) {
		b.startOnce.Do(func() { close(b.started) })
		<-b.release
	}
	return b.StorageBackend.SaveRecording(recording)
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestBlockedRootWriteDoesNotHoldEntryLockAcrossFetchAdmission(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "aabbccddeeff00112233445566778899"
	recording := &domain.Recording{FormatVersion: 1, ID: id, Title: "before", State: domain.StateCompleted, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	backend := &blockingSaveRecordingBackend{StorageBackend: store.StorageBackend, started: make(chan struct{}), release: make(chan struct{})}
	store.StorageBackend = backend
	manager, err := NewManager(store, http.DefaultClient, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording was not loaded")
	}
	e.mu.Lock()
	e.mediaGeneration = 9
	e.media = adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}
	e.mu.Unlock()
	backend.blockNext.Store(true)
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- manager.update(e, func(next *domain.Recording) error {
			next.Title = "after"
			return nil
		})
	}()
	waitSignal(t, backend.started, "blocked root metadata write")
	releaseOnce := sync.Once{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(backend.release) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})

	getDone := make(chan *domain.Recording, 1)
	go func() {
		current, _ := manager.Get(id)
		getDone <- current
	}()
	select {
	case current := <-getDone:
		if current == nil || current.Title != "before" {
			t.Fatalf("uncommitted metadata became visible during blocked write: %#v", current)
		}
	case <-time.After(time.Second):
		t.Fatal("Get waited for root metadata I/O instead of returning the old state")
	}

	generationDone := make(chan uint64, 1)
	go func() {
		_, generation := currentMediaVersion(e)
		generationDone <- generation
	}()
	select {
	case generation := <-generationDone:
		if generation != 9 {
			t.Fatalf("media generation = %d, want 9", generation)
		}
	case <-time.After(time.Second):
		t.Fatal("generation read waited for root metadata I/O")
	}

	admitted := make(chan struct{}, 1)
	client := &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
		admitted <- struct{}{}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("manifest")), Request: request}, nil
	})}
	request, err := http.NewRequest(http.MethodGet, "https://media.example/live.m3u8", nil)
	if err != nil {
		t.Fatal(err)
	}
	fetchDone := make(chan error, 1)
	go func() {
		response, fetchErr := doMediaRequestAtGeneration(client, request, nil, "https://media.example/live.m3u8", nil, e, 9, nil)
		if response != nil {
			_ = response.Body.Close()
		}
		fetchDone <- fetchErr
	}()
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("HTTP fetch was not admitted while root metadata write was blocked")
	}
	select {
	case err = <-fetchDone:
		if err != nil {
			t.Fatalf("generation-guarded HTTP request: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("generation-guarded HTTP request did not finish")
	}

	releaseOnce.Do(func() { close(backend.release) })
	select {
	case err = <-updateDone:
		if err != nil {
			t.Fatalf("metadata update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metadata update did not finish after releasing the backend")
	}
	current, err := manager.Get(id)
	if err != nil || current.Title != "after" {
		t.Fatalf("durable metadata was not published: recording=%#v err=%v", current, err)
	}
}

func TestDeleteSerializesAfterBlockedRootWriteAndPreventsRecreation(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "11223344556677889900aabbccddeeff"
	recording := &domain.Recording{FormatVersion: 1, ID: id, Title: "before", State: domain.StateCompleted, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	backend := &blockingSaveRecordingBackend{StorageBackend: store.StorageBackend, started: make(chan struct{}), release: make(chan struct{})}
	store.StorageBackend = backend
	manager, err := NewManager(store, http.DefaultClient, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording was not loaded")
	}
	backend.blockNext.Store(true)
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- manager.update(e, func(next *domain.Recording) error {
			next.Title = "updated before delete"
			return nil
		})
	}()
	waitSignal(t, backend.started, "blocked root metadata write")
	releaseOnce := sync.Once{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(backend.release) })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = manager.Close(ctx)
	})

	deleteStarted := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		close(deleteStarted)
		deleteDone <- manager.Delete(id)
	}()
	waitSignal(t, deleteStarted, "concurrent recording deletion")
	select {
	case deleteErr := <-deleteDone:
		t.Fatalf("Delete completed ahead of the in-flight metadata write: %v", deleteErr)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(backend.release) })
	select {
	case err = <-updateDone:
		if err != nil {
			t.Fatalf("metadata update: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metadata update did not finish after releasing the backend")
	}
	select {
	case err = <-deleteDone:
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delete did not run after metadata write completed")
	}
	if _, err = manager.Get(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get after delete error = %v", err)
	}
	if err = manager.update(e, func(next *domain.Recording) error { next.Title = "must not recreate"; return nil }); err == nil {
		t.Fatal("metadata update after delete unexpectedly succeeded")
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 0 {
		t.Fatalf("delete was undone by a later SaveRecording: loaded=%d err=%v", len(loaded), err)
	}
}

func waitAtomicCount(t *testing.T, counter *atomic.Int32, count int32, description string) {
	t.Helper()
	waitUntil(t, 2*time.Second, func() bool { return counter.Load() >= count }, description)
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestCanceledAcquisitionPersistsCompletedPayloadAfterQueueUnblocks(t *testing.T) {
	var segmentRequests atomic.Int32
	client := &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/segment.ts" {
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		segmentRequests.Add(1)
		body := "fully fetched source bytes"
		header := make(http.Header)
		header.Set("Content-Length", strconv.Itoa(len(body)))
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
	})}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "c001cafe1234567890abcdef12345678"
	root := &domain.Recording{
		FormatVersion: 1, ID: id, State: domain.StateRecording,
		Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}},
	}
	if err = store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, client, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording was not loaded")
	}
	e.mu.Lock()
	e.media = adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}
	e.mu.Unlock()
	blockStarted, releaseWriter := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-releaseWriter:
		default:
			close(releaseWriter)
		}
	}()
	if err = manager.ingest.SubmitCommit(context.Background(), "blocker", func() error {
		close(blockStarted)
		<-releaseWriter
		return nil
	}, func(error) {}); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, blockStarted, "blocked ingest writer")
	for i := 0; i < storage.MaxIngestObjects; i++ {
		if err = manager.ingest.SubmitCommit(context.Background(), "queued", func() error { return nil }, func(error) {}); err != nil {
			t.Fatalf("fill ingest queue at %d: %v", i, err)
		}
	}

	acquisitionCtx, cancelAcquisition := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	e.mu.Lock()
	e.cancel = cancelAcquisition
	e.done = workerDone
	e.mu.Unlock()
	scheduler, err := newSegmentScheduler(acquisitionCtx, manager, e)
	if err != nil {
		cancelAcquisition()
		t.Fatal(err)
	}
	task := &segmentTask{
		key:     segmentTaskKey{epoch: 0, sequence: 1},
		source:  hls.MediaSegment{Sequence: 1, URI: "https://media.example/segment.ts", Duration: 1},
		ordinal: 1, observed: true, available: true, state: segmentTaskInFlight,
	}
	scheduler.mu.Lock()
	scheduler.tasks[task.key] = task
	scheduler.mu.Unlock()
	acquireDone := make(chan error, 1)
	go func() { acquireDone <- scheduler.acquire(task) }()
	waitUntil(t, time.Second, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return task.state == segmentTaskBuffered && manager.ingest.Snapshot().BufferUsedBytes > 0
	}, "complete segment body buffered while queue is full")
	if segmentRequests.Load() != 1 {
		t.Fatalf("segment request count=%d want=1", segmentRequests.Load())
	}
	select {
	case err := <-acquireDone:
		t.Fatalf("payload unexpectedly passed the full queue: %v", err)
	default:
	}
	cancelAcquisition()
	select {
	case err := <-acquireDone:
		t.Fatalf("acquisition cancellation discarded its completed payload: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	closeErr := manager.Close(closeCtx)
	closeCancel()
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("Manager.Close while the completed payload waited returned %v, want deadline", closeErr)
	}
	close(releaseWriter)
	select {
	case err := <-acquireDone:
		if !errors.Is(err, errPersistQueued) {
			t.Fatalf("acquire result after queue drain = %v, want queued persist", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("completed segment was not admitted after writer capacity returned")
	}
	waitForSegmentCount(t, manager, id, 1)
	if err = scheduler.close(); err != nil {
		t.Fatal(err)
	}
	current, err := manager.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if current.SegmentCount() != 1 || len(current.Gaps) != 0 {
		t.Fatalf("completed source body did not commit exactly once without a gap: segments=%d gaps=%#v", current.SegmentCount(), current.Gaps)
	}
	if segmentRequests.Load() != 1 {
		t.Fatalf("source was fetched %d times, want once", segmentRequests.Load())
	}
	close(workerDone)
	select {
	case <-manager.ingest.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Manager.Close did not let its background ingest coordinator join after the writer recovered")
	}
}

func TestManagerCloseDeadlineStartsBackgroundIngestDrain(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "d001cafe1234567890abcdef12345678"
	if err = store.CreateRecording(&domain.Recording{
		FormatVersion: 1, ID: id, State: domain.StateRecording,
		Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}},
	}); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, http.DefaultClient, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording was not loaded")
	}
	workerDone := make(chan struct{})
	workerCanceled := make(chan struct{})
	e.mu.Lock()
	e.recording.State = domain.StateRecording
	e.done = workerDone
	e.cancel = func() { close(workerCanceled) }
	e.mu.Unlock()
	writerStarted, releaseWriter := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-releaseWriter:
		default:
			close(releaseWriter)
		}
	}()
	payload, err := manager.ingest.ReadPayload(context.Background(), id, strings.NewReader("complete"), 8, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.ingest.Submit(context.Background(), payload, func(data []byte) (storage.PayloadResult, error) {
		close(writerStarted)
		<-releaseWriter
		return storage.PayloadResult{Size: int64(len(data))}, nil
	}, func(storage.PayloadResult, error) {}); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, writerStarted, "blocked storage writer")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err = manager.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Manager.Close error=%v, want caller deadline", err)
	}
	select {
	case <-workerCanceled:
	case <-time.After(time.Second):
		t.Fatal("Manager.Close did not cancel active recording")
	}
	if err := manager.ingest.SubmitCommit(context.Background(), "after-close", func() error { return nil }, func(error) {}); !errors.Is(err, storage.ErrIngestClosed) {
		t.Fatalf("ingest admission after Manager.Close deadline = %v, want closed", err)
	}
	close(releaseWriter)
	select {
	case <-manager.ingest.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("background close coordinator did not join writer, queue, and sampler")
	}
	close(workerDone)
}
