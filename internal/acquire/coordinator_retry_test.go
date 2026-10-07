package acquire

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

func coordinatorRetryFixture() (*segmentScheduler, context.CancelFunc, *segmentTask) {
	ctx, cancel := context.WithCancel(context.Background())
	key := segmentTaskKey{epoch: 1, sequence: 42}
	task := &segmentTask{key: key, available: true, observed: true, state: segmentTaskInFlight}
	scheduler := &segmentScheduler{
		ctx: ctx, cancel: cancel, e: &entry{}, changed: make(chan struct{}),
		tasks: map[segmentTaskKey]*segmentTask{key: task}, workers: maxSegmentWorkers,
	}
	return scheduler, cancel, task
}

func TestLiveMediaCommitPropagatesInitCoordinatorFailureWithoutCanonicalClass(t *testing.T) {
	completed := make(chan struct{})
	close(completed)
	flight := &initFlight{done: completed, err: storage.ErrIngestCoordinatorUnavailable}
	scheduler := &segmentScheduler{}
	_, err := scheduler.persistSegmentFrom(domain.Segment{}, flight, nil, archiveindex.ClaimLiveOrigin)
	if !errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
		t.Fatalf("init coordinator error=%v, want coordinator classification", err)
	}
	if errors.Is(err, errStorageCommit) || errors.Is(err, storage.ErrCanonicalCommitFailed) {
		t.Fatalf("init coordinator error became canonical failure: %v", err)
	}
}

func TestCoordinatorRetrySurvivesSourceSlidingOutOfLiveWindow(t *testing.T) {
	scheduler, cancel, task := coordinatorRetryFixture()
	defer func() {
		cancel()
		scheduler.wg.Wait()
	}()
	scheduler.retryCoordinatorTask(task)

	// A newer playlist has already moved past the task coordinate. This is
	// not evidence of a media gap while coordinator retry is pending.
	if err := scheduler.observeAtGeneration(2, hls.MediaPlaylist{Segments: []hls.MediaSegment{{Sequence: 43}}}, 0); err != nil {
		t.Fatalf("observe slid live window: %v", err)
	}
	scheduler.mu.Lock()
	state, available, pending := task.state, task.available, task.coordinatorRetryPending
	queued := len(scheduler.ready)
	scheduler.mu.Unlock()
	if state != segmentTaskRetryWait || available || !pending || queued != 0 {
		t.Fatalf("coordinator-pending task state=%d available=%v pending=%v ready=%d; want retry wait, unavailable, pending, not queued as a gap", state, available, pending, queued)
	}
	if err := scheduler.failure(); err != nil {
		t.Fatalf("sliding source window made coordinator retry fatal: %v", err)
	}
}

func TestCoordinatorUnavailableRequeuesLiveTaskWithoutCanonicalFailure(t *testing.T) {
	scheduler, cancel, task := coordinatorRetryFixture()
	defer func() {
		cancel()
		scheduler.wg.Wait()
	}()
	scheduler.retryCoordinatorTask(task)
	if err := scheduler.failure(); err != nil {
		t.Fatalf("transient coordinator failure became fatal: %v", err)
	}
	scheduler.mu.Lock()
	state, retries := task.state, task.coordinatorRetries
	scheduler.mu.Unlock()
	if state != segmentTaskRetryWait || retries != 1 {
		t.Fatalf("task retry state=%d retries=%d, want retry-wait/1", state, retries)
	}
	// Source may slide while the coordinator is down. A fetched payload failed
	// before canonical mutation, so coordinator retry must not turn that event
	// into a false source gap.
	scheduler.mu.Lock()
	task.available = false
	scheduler.mu.Unlock()
	waitUntil(t, time.Second, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		return task.state == segmentTaskQueued && len(scheduler.ready) == 1
	}, "coordinator retry admission")
	if err := scheduler.observeAtGeneration(2, hls.MediaPlaylist{Segments: []hls.MediaSegment{{Sequence: 43}}}, 0); err != nil {
		t.Fatalf("observe slid live window after retry became due: %v", err)
	}
	scheduler.mu.Lock()
	if len(scheduler.tasks) != 1 || scheduler.ready[0] != task || task.available || !task.coordinatorRetryPending {
		scheduler.mu.Unlock()
		t.Fatal("coordinator retry lost task or changed source availability")
	}
	scheduler.mu.Unlock()
	if err := scheduler.failure(); err != nil {
		t.Fatalf("coordinator retry created fatal error: %v", err)
	}
}

func TestCoordinatorRetryExhaustionIsNotCanonicalStorageFailure(t *testing.T) {
	scheduler, cancel, task := coordinatorRetryFixture()
	defer cancel()
	task.coordinatorRetries = maxCoordinatorTaskRetries
	scheduler.retryCoordinatorTask(task)
	err := scheduler.failure()
	if !errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
		t.Fatalf("exhaustion error=%v, want coordinator classification", err)
	}
	if errors.Is(err, errStorageCommit) || errors.Is(err, storage.ErrCanonicalCommitFailed) {
		t.Fatalf("coordinator exhaustion was misclassified as canonical failure: %v", err)
	}
	if task.state != segmentTaskInFlight {
		t.Fatalf("exhaustion changed task state to %d", task.state)
	}
	cancel()
}

type unavailableWriterCoordinator struct {
	writerCalls atomic.Int32
}

func (*unavailableWriterCoordinator) SetReservation(context.Context, string, string, int64) error {
	return nil
}
func (*unavailableWriterCoordinator) ReleaseReservation(context.Context, string, string) error {
	return nil
}
func (*unavailableWriterCoordinator) AcquireQueue(context.Context, string) error { return nil }
func (*unavailableWriterCoordinator) ReleaseQueue(context.Context, string) error { return nil }
func (c *unavailableWriterCoordinator) AcquireWriter(context.Context, string) error {
	c.writerCalls.Add(1)
	return errors.New("temporary coordinator outage")
}
func (*unavailableWriterCoordinator) ReleaseWriter(context.Context, string) error { return nil }

func TestHistoricalInitCoordinatorFailureDoesNotPublishAcquisitionFailure(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	media.HistoricalAvailability = &adapterproto.HistoricalAvailability{
		Mode:           adapterproto.HistoricalModeSequenceRanges,
		SequenceRanges: []adapterproto.HistoricalSequenceRange{{Start: 10, End: 10}},
	}
	root := newRecoveryFairnessRoot(t, store, "f330a19f5c144a9c8d53902032f58c11", media)
	coordinator := &unavailableWriterCoordinator{}
	if err := store.ConfigureRuntimeIngestCoordinator(coordinator); err != nil {
		t.Fatal(err)
	}
	fixture := &repairFixtureTransport{
		manifest: []byte("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-DISCONTINUITY-SEQUENCE:7\n#EXT-X-MAP:URI=\"init.ts\"\n#EXTINF:4,\nprefix.ts\n#EXT-X-ENDLIST\n"),
		objects:  map[string][]byte{"/archive/init.m4v": []byte("init"), "/archive/prefix.m4v": []byte("media")},
	}
	manager, _, closeManager := newRepairTestManager(t, store, fixture, root, &repairOwnerFence{}, nil)
	defer func() {
		if err := closeManager(); err != nil {
			t.Errorf("close repair manager: %v", err)
		}
	}()
	owner := ownerForRepair(root.ID, 1)
	progress, err := manager.repairDeclaredHistoryPass(context.Background(), owner, root.ID)
	var retryable *retryableHistoricalError
	if !errors.As(err, &retryable) || !errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
		t.Fatalf("historical coordinator failure=%v, want retryable coordinator error", err)
	}
	if progress.failed != 0 || progress.acquired != 0 {
		t.Fatalf("coordinator outage changed historical progress: %+v", progress)
	}
	inventory, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{
		SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 0,
		DiscontinuitySequence: 7, Sequence: 10, Kind: archiveindex.ObjectMedia,
	}
	if got := archiveindex.CoverageAt(inventory, coordinate); got != archiveindex.CoverageUnknown {
		t.Fatalf("coverage after writer lease outage=%q, want unknown", got)
	}
	if coordinator.writerCalls.Load() == 0 {
		t.Fatal("historical init commit did not reach coordinator writer acquisition")
	}
}
