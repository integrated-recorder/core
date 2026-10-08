package acquire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	minSegmentWorkers         = 1
	maxSegmentWorkers         = 4
	maxSegmentTasks           = 128
	maxCoordinatorTaskRetries = 8
)

type segmentTaskKey struct {
	epoch                 uint64
	discontinuitySequence uint64
	sequence              uint64
}

type segmentTaskState uint8

const (
	segmentTaskQueued segmentTaskState = iota
	segmentTaskInFlight
	segmentTaskBuffered
	segmentTaskPersisting
	segmentTaskRefreshing
	segmentTaskRetryWait
	segmentTaskAwaitManifest
	segmentTaskGapCommitting
)

type segmentTask struct {
	key                     segmentTaskKey
	source                  hls.MediaSegment
	ordinal                 uint64
	available               bool
	observed                bool
	observedGeneration      uint64
	requiredGeneration      uint64
	awaitingManifest        bool
	attempt                 int
	coordinatorRetries      int
	coordinatorRetryPending bool
	refreshCycles           int
	state                   segmentTaskState
	timerToken              uint64
	retryCancel             context.CancelFunc
	preloaded               bool
	stagedInitPayload       *storage.IngestPayload
	stagedMediaPayload      *storage.IngestPayload
}

type initFlight struct {
	queued   chan struct{}
	done     chan struct{}
	id       string
	queueErr error
	err      error
}

var (
	errPersistQueued = errors.New("canonical payload persistence queued")
	errStorageCommit = errors.New("canonical storage commit failed")
)

// storageCommitFailure keeps storage classification and the internal failure
// stage while preserving the underlying commit error for diagnostics.
type storageCommitFailure struct {
	stage string
	cause error
}

// storageCoordinatorFailure records a terminal coordinator failure without
// classifying it as a canonical archive commit failure.
type storageCoordinatorFailure struct {
	stage string
	cause error
}

func (e storageCoordinatorFailure) Error() string {
	return fmt.Sprintf("storage coordinator unavailable during %s: %v", e.stage, e.cause)
}

func (e storageCoordinatorFailure) Unwrap() error { return e.cause }

func (e storageCoordinatorFailure) Stage() string { return e.stage }

// storageStageError adds internal operation context without changing existing
// failure classification for callers that handle canonical commits.
type storageStageError struct {
	stage string
	cause error
}

func newStorageStageError(stage string, cause error) error {
	if cause == nil {
		cause = errors.New("storage commit cause unavailable")
	}
	return &storageStageError{stage: stage, cause: cause}
}

func newStorageCommitFailure(stage string, cause error) error {
	if cause == nil {
		cause = errors.New("storage commit cause unavailable")
	}
	return &storageCommitFailure{stage: stage, cause: cause}
}

func (e *storageCommitFailure) Error() string {
	if e == nil {
		return errStorageCommit.Error()
	}
	if e.cause == nil {
		return fmt.Sprintf("%s: %s", errStorageCommit, e.stage)
	}
	return fmt.Sprintf("%s: %s: %v", errStorageCommit, e.stage, e.cause)
}

func (e *storageCommitFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *storageCommitFailure) Is(target error) bool {
	return target == errStorageCommit
}

func (e *storageCommitFailure) Stage() string {
	if e == nil {
		return ""
	}
	return e.stage
}

func (e *storageStageError) Error() string {
	if e == nil {
		return "storage commit stage unavailable"
	}
	if e.cause == nil {
		return e.stage
	}
	return fmt.Sprintf("%s: %v", e.stage, e.cause)
}

func (e *storageStageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *storageStageError) Stage() string {
	if e == nil {
		return ""
	}
	return e.stage
}

type epochMarker struct {
	ordinal               uint64
	discontinuitySequence uint64
	sequence              uint64
	sourceDiscontinuity   bool
}

// segmentScheduler belongs to one recording worker. Discovery is serialized by
// its caller; only bounded fetch execution and retry timing are concurrent.
type segmentScheduler struct {
	manager *Manager
	e       *entry
	ctx     context.Context
	cancel  context.CancelFunc

	mu                       sync.Mutex
	commitMu                 sync.Mutex
	changed                  chan struct{}
	tasks                    map[segmentTaskKey]*segmentTask
	ready                    []*segmentTask
	workers                  int
	busy                     int
	closed                   bool
	fatal                    error
	pendingSnapshots         int
	pendingMetadata          int
	ending                   bool
	endingGeneration         uint64
	latestManifestGeneration uint64
	manifestGenerationSet    bool
	directMode               bool
	wg                       sync.WaitGroup
	init                     map[string]*initFlight
	next                     uint64
	epochMarkers             map[uint64]epochMarker
	manifestWake             chan struct{}
	// drainWaitHook is an observation seam for deterministic drain tests.
	drainWaitHook func()
}

func newSegmentScheduler(parent context.Context, manager *Manager, e *entry) (*segmentScheduler, error) {
	next, err := nextArchiveOrdinal(e)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	s := &segmentScheduler{
		manager: manager, e: e, ctx: ctx, cancel: cancel,
		changed: make(chan struct{}), tasks: make(map[segmentTaskKey]*segmentTask),
		init: make(map[string]*initFlight), next: next, epochMarkers: make(map[uint64]epochMarker), manifestWake: make(chan struct{}, 1),
	}
	// A prepared handover candidate is admitted before scheduler workers start,
	// so normal discovery observes one existing task instead of allocating a
	// second ordinal or fetching the same source object again.
	e.mu.Lock()
	if candidate := e.handoverCandidate; candidate != nil {
		fingerprint, fingerprintErr := handoverRootFingerprint(e.recording)
		if fingerprintErr != nil || fingerprint != candidate.rootFingerprint || candidate.ordinal != s.next || candidate.mediaPayload == nil {
			e.mu.Unlock()
			cancel()
			return nil, ErrHandoverUnavailable
		}
		track := e.recording.Tracks["main"]
		if track == nil || candidate.epoch < track.SourceEpoch || candidate.epoch-track.SourceEpoch > 1 || candidate.ordinal == 0 || candidate.ordinal == ^uint64(0) {
			e.mu.Unlock()
			cancel()
			return nil, ErrHandoverUnavailable
		}
		key := segmentTaskKey{epoch: candidate.epoch, discontinuitySequence: candidate.source.DiscontinuitySequence, sequence: candidate.source.Sequence}
		if _, exists := s.tasks[key]; exists {
			e.mu.Unlock()
			cancel()
			return nil, ErrHandoverConflict
		}
		task := &segmentTask{
			key: key, source: cloneHandoverMediaSegment(candidate.source), ordinal: candidate.ordinal,
			available: true, observed: true, observedGeneration: 0, state: segmentTaskQueued,
			preloaded: true, stagedInitPayload: candidate.initPayload, stagedMediaPayload: candidate.mediaPayload,
		}
		s.tasks[key] = task
		s.ready = append(s.ready, task)
		s.next++
		e.handoverCandidate = nil
	}
	e.mu.Unlock()
	s.mu.Lock()
	for worker := 0; worker < minSegmentWorkers; worker++ {
		s.spawnLocked(false)
	}
	s.mu.Unlock()
	e.mu.Lock()
	e.scheduler = s
	e.mu.Unlock()
	return s, nil
}

func (s *segmentScheduler) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *segmentScheduler) spawnLocked(burst bool) {
	if s.closed || s.workers >= maxSegmentWorkers {
		return
	}
	s.workers++
	s.wg.Add(1)
	go s.worker(burst)
}

func (s *segmentScheduler) growIfSaturatedLocked() {
	if len(s.ready) > 0 && s.busy == s.workers && s.workers < maxSegmentWorkers {
		s.spawnLocked(true)
	}
}

func (s *segmentScheduler) discover(ctx context.Context, epoch uint64, playlist hls.MediaPlaylist) error {
	_, generation := currentMediaVersion(s.e)
	_, err := s.discoverAtGeneration(ctx, epoch, playlist, generation)
	return err
}

func (s *segmentScheduler) discoverAtGeneration(ctx context.Context, epoch uint64, playlist hls.MediaPlaylist, generation uint64) (bool, error) {
	for index, source := range playlist.Segments {
		if !s.manifestGenerationCurrent(generation) {
			return false, nil
		}
		if source.Gap || s.manager.hasSequenceCoordinate(s.e, epoch, source.DiscontinuitySequence, source.Sequence) || gapCoversCoordinate(s.recordingSnapshot(), "main", epoch, source.DiscontinuitySequence, source.Sequence) {
			continue
		}
		key := segmentTaskKey{epoch: epoch, discontinuitySequence: source.DiscontinuitySequence, sequence: source.Sequence}
		for {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return false, errors.New("segment scheduler is closed")
			}
			if !s.manifestGenerationCurrentLocked(generation) {
				s.mu.Unlock()
				return false, nil
			}
			if existing := s.tasks[key]; existing != nil {
				if existing.state == segmentTaskGapCommitting {
					s.mu.Unlock()
					break
				}
				if !existing.preloaded || existing.stagedMediaPayload == nil {
					existing.source = source // preserve latest signed fetch URI for a later retry
				}
				existing.available = true
				existing.observed = true
				existing.observedGeneration = generation
				if existing.awaitingManifest && generation >= existing.requiredGeneration {
					existing.awaitingManifest = false
					existing.state = segmentTaskQueued
					s.ready = append(s.ready, existing)
					s.growIfSaturatedLocked()
				} else if existing.state == segmentTaskRetryWait {
					// A re-observed segment remains a single task; its timer stays
					// authoritative and no second fetch is admitted.
				}
				s.signalLocked()
				s.mu.Unlock()
				break
			}
			// Capture commits root metadata before removing the task map entry.
			// Recheck under the same lock used for task identity allocation to
			// prevent a poll that passed the optimistic check from re-enqueueing
			// an already captured segment. A gap is similarly protected by the
			// gap-committing state until its metadata write succeeds.
			if s.manager.hasSequenceCoordinate(s.e, epoch, source.DiscontinuitySequence, source.Sequence) || gapCoversCoordinate(s.recordingSnapshot(), "main", epoch, source.DiscontinuitySequence, source.Sequence) {
				s.mu.Unlock()
				break
			}
			if len(s.tasks) >= maxSegmentTasks {
				changed := s.changed
				s.mu.Unlock()
				select {
				case <-ctx.Done():
					if err := s.manager.markRemainingPending(s.e, epoch, playlist.Segments[index:], ctx.Err()); err != nil {
						return false, err
					}
					return false, ctx.Err()
				case <-s.ctx.Done():
					if err := s.manager.markRemainingPending(s.e, epoch, playlist.Segments[index:], s.ctx.Err()); err != nil {
						return false, err
					}
					return false, s.ctx.Err()
				case <-changed:
				}
				continue
			}
			ordinal := s.next
			if ordinal == 0 || ordinal == ^uint64(0) {
				s.mu.Unlock()
				return false, errors.New("archive ordinal overflow")
			}
			s.next++
			task := &segmentTask{key: key, source: source, ordinal: ordinal, available: true, observed: true, observedGeneration: generation, state: segmentTaskQueued}
			s.tasks[key] = task
			s.ready = append(s.ready, task)
			s.growIfSaturatedLocked()
			s.signalLocked()
			s.mu.Unlock()
			break
		}
	}
	return s.manifestGenerationCurrent(generation), nil
}

// queueSnapshot sends an already-fetched manifest body through the same
// bounded writer as media payloads. The manifest poller returns as soon as
// the body is admitted; only the writer performs durable payload, sidecar,
// and recording.json writes. A stale generation may leave an unreferenced
// snapshot object, but can never mutate the current recording projection.
func (s *segmentScheduler) queueSnapshot(generation uint64, trackID, source string, payload *storage.IngestPayload, after func(*domain.Recording) error) error {
	if payload == nil {
		return errors.New("manifest snapshot payload is nil")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		payload.Release()
		return errors.New("segment scheduler is closed")
	}
	s.pendingSnapshots++
	s.wg.Add(1)
	s.signalLocked()
	s.mu.Unlock()

	id := recordingID(s.e)
	at := time.Now().UTC()
	err := s.manager.ingest.SubmitWithKind(s.ctx, payload, storage.IngestJobKindManifestSnapshot, func(data []byte) (storage.PayloadResult, error) {
		if _, current := currentMediaVersion(s.e); current != generation {
			return payload.Result(), nil
		}
		if s.manager.storageSnapshotWriteHook != nil {
			s.manager.storageSnapshotWriteHook()
		}

		s.commitMu.Lock()
		defer s.commitMu.Unlock()
		var snapshot domain.ManifestSnapshot
		var updated bool
		err := s.manager.withCanonicalMutation(s.e, func() error {
			var err error
			snapshot, err = s.manager.store.SaveSnapshot(id, trackID, source, data, at)
			if err != nil {
				return err
			}
			updated, err = s.manager.updateAtMediaGenerationWithinAuthorizedCommit(s.e, generation, func(r *domain.Recording) error {
				for _, existing := range r.Snapshots {
					if existing.StoragePath == snapshot.StoragePath {
						if after != nil {
							return after(r)
						}
						return nil
					}
				}
				r.Snapshots = append(r.Snapshots, snapshot)
				if after != nil {
					return after(r)
				}
				return nil
			})
			return err
		})
		if err != nil {
			return storage.PayloadResult{}, err
		}
		// A concurrent source refresh invalidated this snapshot while it was
		// being written. Keep its payload unreferenced rather than applying stale
		// observation or master-variant metadata to the new generation.
		_ = updated
		return storage.PayloadResult{Size: snapshot.Size, SHA256: snapshot.SHA256}, nil
	}, func(_ storage.PayloadResult, persistErr error) {
		defer s.wg.Done()
		s.mu.Lock()
		s.pendingSnapshots--
		s.signalLocked()
		s.mu.Unlock()
		if persistErr != nil {
			if errors.Is(persistErr, storage.ErrIngestCoordinatorUnavailable) {
				// Snapshot observations are refreshed by the next manifest poll. A
				// coordinator outage is not a canonical payload failure.
				return
			}
			s.failStorage("manifest snapshot commit", persistErr)
		}
	})
	if err != nil {
		s.wg.Done()
		s.mu.Lock()
		s.pendingSnapshots--
		s.signalLocked()
		s.mu.Unlock()
		payload.Release()
		if errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
			// The snapshot is an observation, not media evidence. The next
			// manifest poll can publish a fresh snapshot after admission recovers.
			return nil
		}
		if s.ctx.Err() != nil || errors.Is(err, context.Canceled) {
			if ctxErr := s.ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		return newStorageCommitFailure("manifest snapshot queue submission", err)
	}
	return nil
}

// queueRecordingCommit persists the newest in-memory root projection on the
// storage writer. It intentionally captures no recording clone: earlier
// queued segment commits may update the root before this operation executes.
func (s *segmentScheduler) queueRecordingCommit() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("segment scheduler is closed")
	}
	s.pendingMetadata++
	s.wg.Add(1)
	s.signalLocked()
	s.mu.Unlock()

	err := s.manager.ingest.SubmitCommitWithKind(s.ctx, recordingID(s.e), storage.IngestJobKindRecordingMetadata, func() error {
		if s.manager.storageMetadataWriteHook != nil {
			s.manager.storageMetadataWriteHook()
		}
		return s.manager.withCanonicalMutation(s.e, func() error {
			s.e.mu.Lock()
			if s.e.deleted {
				s.e.mu.Unlock()
				return errors.New("recording was deleted")
			}
			current := clone(s.e.recording)
			s.e.mu.Unlock()
			if current == nil {
				return errors.New("recording state could not be copied")
			}
			if err := s.manager.store.SaveRecording(current); err != nil {
				return newStorageStageError("recording root commit", err)
			}
			return nil
		})
	}, func(commitErr error) {
		defer s.wg.Done()
		s.mu.Lock()
		s.pendingMetadata--
		s.signalLocked()
		s.mu.Unlock()
		if errors.Is(commitErr, storage.ErrIngestCoordinatorUnavailable) {
			// A later manifest/segment commit will persist the current in-memory
			// root. Do not report this pre-mutation lease outage as corruption.
			return
		}
		if commitErr != nil {
			s.failStorage("recording metadata commit", commitErr)
		}
	})
	if err == nil {
		return nil
	}
	s.wg.Done()
	s.mu.Lock()
	s.pendingMetadata--
	s.signalLocked()
	s.mu.Unlock()
	if s.ctx.Err() != nil || errors.Is(err, context.Canceled) {
		if ctxErr := s.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	if errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
		// The root projection remains in memory. A later manifest or segment
		// commit persists it after coordinator admission recovers.
		return nil
	}
	return newStorageCommitFailure("recording metadata queue submission", err)
}

func (s *segmentScheduler) recordingSnapshot() *domain.Recording {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	return clone(s.e.recording)
}

// manifestGenerationCurrent rejects bodies fetched from an older media source.
// The entry check is repeated while holding the scheduler lock at mutation
// sites; no lock is held across queue backpressure or network work.
func (s *segmentScheduler) manifestGenerationCurrent(generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifestGenerationCurrentLocked(generation)
}

func (s *segmentScheduler) manifestGenerationCurrentLocked(generation uint64) bool {
	s.e.mu.Lock()
	current := s.e.mediaGeneration
	s.e.mu.Unlock()
	if current != generation || (s.manifestGenerationSet && generation < s.latestManifestGeneration) {
		return false
	}
	if !s.manifestGenerationSet || generation > s.latestManifestGeneration {
		s.latestManifestGeneration = generation
		s.manifestGenerationSet = true
		if s.ending && generation > s.endingGeneration {
			s.ending = false
			s.signalLocked()
		}
	}
	return true
}

// noteMediaGeneration runs immediately after a refreshed source is committed.
// It reopens a playlist that ended on the prior source generation.
func (s *segmentScheduler) noteMediaGeneration(generation uint64) {
	s.mu.Lock()
	reopened := false
	if !s.manifestGenerationSet || generation > s.latestManifestGeneration {
		s.latestManifestGeneration = generation
		s.manifestGenerationSet = true
		if s.ending && generation > s.endingGeneration {
			s.ending = false
			reopened = true
			s.signalLocked()
		}
	}
	s.mu.Unlock()
	if reopened {
		s.wakeManifestPoll()
	}
}

func (s *segmentScheduler) observe(epoch uint64, playlist hls.MediaPlaylist) error {
	_, generation := currentMediaVersion(s.e)
	return s.observeAtGeneration(epoch, playlist, generation)
}

func (s *segmentScheduler) observeAtGeneration(epoch uint64, playlist hls.MediaPlaylist, generation uint64) error {
	present := make(map[uint64]bool, len(playlist.Segments))
	gaps := make(map[uint64]bool)
	var max uint64
	for i, source := range playlist.Segments {
		present[source.Sequence] = true
		if source.Gap {
			gaps[source.Sequence] = true
		}
		if i == 0 || source.Sequence > max {
			max = source.Sequence
		}
	}
	var expired []*segmentTask
	s.mu.Lock()
	if !s.manifestGenerationCurrentLocked(generation) {
		s.mu.Unlock()
		return nil
	}
	ending := s.ending
	for key, task := range s.tasks {
		if key.epoch > epoch {
			continue
		}
		// A preflighted payload is already locally available even if the live
		// window advances before the target's first normal manifest poll.
		if task.preloaded {
			task.available = true
			continue
		}
		available := key.epoch == epoch && present[key.sequence] && !gaps[key.sequence]
		if !available && (key.epoch < epoch || max > key.sequence || (key.epoch == epoch && gaps[key.sequence])) {
			task.available = false
			// A coordinator retry follows a completed fetch/admission attempt,
			// not a source failure. Keep the task through its bounded retry even
			// if the live playlist slides past it while the coordinator is down.
			if (task.state == segmentTaskRetryWait || task.state == segmentTaskQueued) && task.coordinatorRetryPending {
				continue
			}
			if task.state == segmentTaskRetryWait || task.state == segmentTaskQueued || task.state == segmentTaskAwaitManifest {
				task.timerToken++
				task.state = segmentTaskGapCommitting
				task.awaitingManifest = false
				expired = append(expired, task)
			}
		} else if available {
			task.available = true
		}
	}
	for _, task := range expired {
		if task.retryCancel != nil {
			task.retryCancel()
			task.retryCancel = nil
		}
	}
	if len(expired) > 0 {
		s.ready = s.ready[:0]
		for _, task := range s.tasks {
			if task.state == segmentTaskQueued {
				s.ready = append(s.ready, task)
			}
		}
		sort.Slice(s.ready, func(i, j int) bool { return s.ready[i].ordinal < s.ready[j].ordinal })
		s.signalLocked()
	}
	s.mu.Unlock()
	for _, task := range expired {
		reason := "media left the live window after retries"
		if ending {
			reason = "stream ended before media could be captured"
		}
		if err := s.persistGap(task, reason); err != nil {
			return err
		}
	}
	return nil
}

func (s *segmentScheduler) protectedSequences(epoch uint64) map[uint64]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	protected := make(map[uint64]bool)
	for key := range s.tasks {
		if key.epoch == epoch {
			protected[key.sequence] = true
		}
	}
	return protected
}

func (s *segmentScheduler) protectedByEpoch() map[uint64]map[segmentTaskKey]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	protected := make(map[uint64]map[segmentTaskKey]bool)
	for key := range s.tasks {
		if protected[key.epoch] == nil {
			protected[key.epoch] = make(map[segmentTaskKey]bool)
		}
		protected[key.epoch][key] = true
	}
	return protected
}

func activeScheduler(e *entry) *segmentScheduler {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.scheduler
}

func (s *segmentScheduler) endList(generation uint64) bool {
	s.mu.Lock()
	if !s.manifestGenerationCurrentLocked(generation) {
		s.mu.Unlock()
		return false
	}
	s.ending = true
	s.endingGeneration = generation
	// A task waiting for a refreshed playlist cannot be retried after ENDLIST.
	// It is terminally pending; the drain then lets the recording finalize it.
	var expired []*segmentTask
	for _, task := range s.tasks {
		if task.state == segmentTaskAwaitManifest {
			if task.available && task.observed && task.observedGeneration >= task.requiredGeneration {
				task.awaitingManifest = false
				task.state = segmentTaskQueued
				s.ready = append(s.ready, task)
				s.growIfSaturatedLocked()
				continue
			}
			task.available = false
			task.timerToken++
			task.awaitingManifest = false
			task.state = segmentTaskGapCommitting
			expired = append(expired, task)
		}
	}
	for _, task := range expired {
		if task.retryCancel != nil {
			task.retryCancel()
			task.retryCancel = nil
		}
	}
	s.signalLocked()
	s.mu.Unlock()
	for _, task := range expired {
		if err := s.persistGap(task, "stream ended before refreshed media could be captured"); err != nil {
			s.failFatal(err)
			return true
		}
	}
	return true
}

func (s *segmentScheduler) wakeManifestPoll() {
	select {
	case s.manifestWake <- struct{}{}:
	default:
	}
}

func (s *segmentScheduler) worker(burst bool) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for len(s.ready) == 0 && !s.closed && s.ctx.Err() == nil {
			if burst {
				s.workers--
				s.signalLocked()
				s.mu.Unlock()
				return
			}
			changed := s.changed
			s.mu.Unlock()
			select {
			case <-s.ctx.Done():
			case <-changed:
			}
			s.mu.Lock()
		}
		if s.closed || s.ctx.Err() != nil {
			s.workers--
			s.signalLocked()
			s.mu.Unlock()
			return
		}
		task := s.ready[0]
		s.ready[0] = nil
		s.ready = s.ready[1:]
		if task.state != segmentTaskQueued || s.tasks[task.key] != task {
			s.mu.Unlock()
			continue
		}
		task.state = segmentTaskInFlight
		task.coordinatorRetryPending = false
		s.busy++
		s.growIfSaturatedLocked()
		s.signalLocked()
		s.mu.Unlock()

		err := s.acquire(task)

		s.mu.Lock()
		s.busy--
		if s.tasks[task.key] != task {
			s.signalLocked()
			s.mu.Unlock()
			continue
		}
		if errors.Is(err, errPersistQueued) {
			s.signalLocked()
			s.mu.Unlock()
			continue
		}
		// Coordinator failures happen before canonical mutation. Keep the
		// logical task and retry it after bounded backoff; do not report a
		// storage corruption or turn the failure into a source fetch error.
		if errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
			s.mu.Unlock()
			s.retryCoordinatorTask(task)
			continue
		}
		if errors.Is(err, errStorageCommit) {
			s.mu.Unlock()
			s.failStorage("segment acquisition commit", err)
			continue
		}
		if err == nil {
			delete(s.tasks, task.key)
			s.signalLocked()
			s.mu.Unlock()
			continue
		}
		if s.ctx.Err() != nil || errors.Is(err, context.Canceled) {
			s.signalLocked()
			s.mu.Unlock()
			continue
		}
		var trigger *segmentRefreshTrigger
		if errors.As(err, &trigger) {
			// acquire() performs the coordinated refresh and returns the trigger
			// only when refresh is not possible or has been exhausted.
			s.mu.Unlock()
			s.failFatal(fmt.Errorf("media source refresh failed"))
			return
		}
		if errors.Is(err, errAwaitManifest) {
			s.mu.Unlock()
			s.awaitManifest(task)
			continue
		}
		var fetchErr *FetchError
		if !errors.As(err, &fetchErr) {
			s.mu.Unlock()
			s.failFatal(err)
			return
		}
		if !task.available {
			task.state = segmentTaskGapCommitting
			s.signalLocked()
			s.mu.Unlock()
			if gapErr := s.persistGap(task, "media left the live window after retries"); gapErr != nil {
				s.failFatal(gapErr)
			}
			continue
		}
		if persistErr := s.manager.markPending(s.e, task.key.epoch, task.key.discontinuitySequence, task.key.sequence, err); persistErr != nil {
			s.mu.Unlock()
			s.failFatal(persistErr)
			return
		}
		task.attempt++
		if s.directMode {
			delete(s.tasks, task.key)
			s.signalLocked()
			s.mu.Unlock()
			continue
		}
		if s.ending && task.attempt >= segmentAttempts {
			task.state = segmentTaskGapCommitting
			s.signalLocked()
			s.mu.Unlock()
			if gapErr := s.persistGap(task, "stream ended after segment retries"); gapErr != nil {
				s.failFatal(gapErr)
				return
			}
			continue
		}
		task.state = segmentTaskRetryWait
		task.timerToken++
		token := task.timerToken
		delay := retryDelay(task.attempt - 1)
		timerCtx, timerCancel := context.WithCancel(s.ctx)
		task.retryCancel = timerCancel
		s.wg.Add(1)
		s.signalLocked()
		s.mu.Unlock()
		go s.retryTimer(timerCtx, timerCancel, task, token, delay)
	}
}

// awaitManifest atomically transitions a refreshed task to waiting for the
// next playlist, unless ENDLIST has already closed admission. In that case
// the task is terminally recorded as a gap instead of leaving drain blocked.
func (s *segmentScheduler) awaitManifest(task *segmentTask) {
	s.mu.Lock()
	if s.tasks[task.key] != task {
		s.mu.Unlock()
		return
	}
	// Refresh commits the source before it can update scheduler state. Read the
	// entry generation here as well so ENDLIST from the old source cannot win
	// that narrow interval and turn a recoverable task into a gap.
	_, currentGeneration := currentMediaVersion(s.e)
	if currentGeneration > s.latestManifestGeneration {
		s.latestManifestGeneration = currentGeneration
		s.manifestGenerationSet = true
		if s.ending && currentGeneration > s.endingGeneration {
			s.ending = false
		}
	}
	if task.available && task.observed && task.observedGeneration >= task.requiredGeneration {
		task.awaitingManifest = false
		task.state = segmentTaskQueued
		s.ready = append(s.ready, task)
		s.growIfSaturatedLocked()
		s.signalLocked()
		s.mu.Unlock()
		return
	}
	if !s.ending {
		task.state = segmentTaskAwaitManifest
		task.awaitingManifest = true
		s.signalLocked()
		s.mu.Unlock()
		s.wakeManifestPoll()
		return
	}
	task.state = segmentTaskGapCommitting
	s.signalLocked()
	s.mu.Unlock()
	if err := s.persistGap(task, "stream ended before refreshed media could be captured"); err != nil {
		s.failFatal(err)
	}
}

func (s *segmentScheduler) persistGap(task *segmentTask, reason string) error {
	if err := s.manager.markGap(s.e, task.key.epoch, task.key.discontinuitySequence, task.key.sequence, reason); err != nil {
		return err
	}
	s.mu.Lock()
	if s.tasks[task.key] == task {
		delete(s.tasks, task.key)
		s.signalLocked()
	}
	s.mu.Unlock()
	return nil
}

func retryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 7 {
		attempt = 7
	}
	return time.Duration(150+attempt*250) * time.Millisecond
}

func (s *segmentScheduler) retryTimer(ctx context.Context, cancel context.CancelFunc, task *segmentTask, token uint64, delay time.Duration) {
	defer s.wg.Done()
	defer cancel()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil || s.tasks[task.key] != task || task.timerToken != token || !task.available {
		return
	}
	task.retryCancel = nil
	task.state = segmentTaskQueued
	s.ready = append(s.ready, task)
	s.growIfSaturatedLocked()
	s.signalLocked()
}

func (s *segmentScheduler) retryCoordinatorTask(task *segmentTask) {
	if task == nil {
		return
	}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil || s.tasks[task.key] != task {
		s.mu.Unlock()
		return
	}
	if task.coordinatorRetries >= maxCoordinatorTaskRetries {
		s.mu.Unlock()
		s.failCoordinator("segment persistence", storage.ErrIngestCoordinatorUnavailable)
		return
	}
	task.coordinatorRetries++
	task.state = segmentTaskRetryWait
	task.coordinatorRetryPending = true
	task.timerToken++
	token := task.timerToken
	delay := retryDelay(task.coordinatorRetries - 1)
	timerCtx, timerCancel := context.WithCancel(s.ctx)
	if task.retryCancel != nil {
		task.retryCancel()
	}
	task.retryCancel = timerCancel
	s.wg.Add(1)
	s.signalLocked()
	s.mu.Unlock()
	go s.retryCoordinatorTimer(timerCtx, timerCancel, task, token, delay)
}

func (s *segmentScheduler) retryCoordinatorTimer(ctx context.Context, cancel context.CancelFunc, task *segmentTask, token uint64, delay time.Duration) {
	defer s.wg.Done()
	defer cancel()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil || s.tasks[task.key] != task || task.timerToken != token {
		return
	}
	task.retryCancel = nil
	task.state = segmentTaskQueued
	s.ready = append(s.ready, task)
	s.growIfSaturatedLocked()
	s.signalLocked()
}

func (s *segmentScheduler) acquire(task *segmentTask) error {
	s.mu.Lock()
	s.e.mu.Lock()
	generation := s.e.mediaGeneration
	if !task.observed || task.observedGeneration != generation || task.observedGeneration < task.requiredGeneration {
		if generation > task.requiredGeneration {
			task.requiredGeneration = generation
		}
		task.available = false
		task.observed = false
		s.signalLocked()
		s.e.mu.Unlock()
		s.mu.Unlock()
		return errAwaitManifest
	}
	media := cloneMediaSource(s.e.media)
	source := task.source
	if source.Init != nil {
		initCopy := *source.Init
		if initCopy.ByteRange != nil {
			rangeCopy := *initCopy.ByteRange
			initCopy.ByteRange = &rangeCopy
		}
		source.Init = &initCopy
	}
	epoch, ordinal := task.key.epoch, task.ordinal
	refreshCycles := task.refreshCycles
	var err error
	preloaded := task.preloaded
	stagedInit := task.stagedInitPayload
	stagedMedia := task.stagedMediaPayload
	task.stagedInitPayload = nil
	task.stagedMediaPayload = nil
	s.e.mu.Unlock()
	s.mu.Unlock()
	defer func() {
		if stagedInit != nil {
			stagedInit.Release()
		}
		if stagedMedia != nil {
			stagedMedia.Release()
		}
	}()
	initID := ""
	var initDependency *initFlight
	if source.Init != nil {
		var err error
		if stagedInit != nil {
			initID, initDependency, err = s.acquireInitWithPayload(source, epoch, media, generation, stagedInit)
			stagedInit = nil // acquireInitWithPayload consumes/releases the payload
		} else {
			initID, initDependency, err = s.acquireInit(source, epoch, media, generation)
		}
		if err != nil {
			if errors.Is(err, storage.ErrIngestClosed) {
				// Shutdown rejected a completed init payload before queue admission.
				// Do not turn the in-flight source sequence into a gap.
				s.removeTask(task)
				return nil
			}
			if errors.Is(err, errStaleMediaGeneration) {
				return s.awaitCurrentGeneration(task)
			}
			return s.handleFetchFailure(task, media, generation, refreshCycles, err)
		}
	}
	var segment domain.Segment
	var payload *storage.IngestPayload
	if preloaded && stagedMedia != nil {
		segment = makeArchiveSegment(source, epoch, ordinal, initID, stagedMedia.Result())
		payload = stagedMedia
		stagedMedia = nil // Submit owns it after successful admission below
	} else {
		segment, payload, err = s.manager.acquireMediaBuffered(s.ctx, s.e, source, epoch, ordinal, initID, media, generation)
		if err != nil {
			if errors.Is(err, errStaleMediaGeneration) {
				return s.awaitCurrentGeneration(task)
			}
			return s.handleFetchFailure(task, media, generation, refreshCycles, err)
		}
	}
	result := payload.Result()
	segment.PayloadSize, segment.SHA256 = result.Size, result.SHA256
	s.mu.Lock()
	if s.tasks[task.key] == task {
		task.state = segmentTaskBuffered
		s.signalLocked()
	}
	s.mu.Unlock()
	s.wg.Add(1)
	err = s.manager.ingest.SubmitWithKind(s.ctx, payload, storage.IngestJobKindMediaPayload, func(data []byte) (storage.PayloadResult, error) {
		return s.persistSegment(segment, initDependency, data)
	}, func(_ storage.PayloadResult, persistErr error) {
		defer s.wg.Done()
		if persistErr != nil {
			if errors.Is(persistErr, storage.ErrIngestCoordinatorUnavailable) {
				s.retryCoordinatorTask(task)
				return
			}
			s.failStorage("media payload commit", persistErr)
			return
		}
		s.mu.Lock()
		if s.tasks[task.key] == task {
			delete(s.tasks, task.key)
			s.signalLocked()
		}
		s.mu.Unlock()
	})
	if err != nil {
		s.wg.Done()
		payload.Release()
		if errors.Is(err, storage.ErrIngestClosed) {
			// The bytes were complete but service shutdown closed admission before
			// this job entered its durable queue. Discard the volatile observation
			// without classifying it as a source gap.
			s.removeTask(task)
			return nil
		}
		if s.ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return err
		}
		if errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
			return err
		}
		return newStorageCommitFailure("media payload queue submission", err)
	}
	// IngestService now owns payload and will release its reservation after the
	// canonical commit callback. Prevent the local deferred cleanup from racing
	// that ownership transfer.
	payload = nil
	s.mu.Lock()
	if s.tasks[task.key] == task {
		task.state = segmentTaskPersisting
		s.signalLocked()
	}
	s.mu.Unlock()
	return errPersistQueued
}

func (s *segmentScheduler) removeTask(task *segmentTask) {
	if task == nil {
		return
	}
	s.mu.Lock()
	if s.tasks[task.key] == task {
		delete(s.tasks, task.key)
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *segmentScheduler) persistSegment(segment domain.Segment, initDependency *initFlight, data []byte) (storage.PayloadResult, error) {
	return s.persistSegmentFrom(segment, initDependency, data, archiveindex.ClaimLiveOrigin)
}

func (s *segmentScheduler) persistSegmentFrom(segment domain.Segment, initDependency *initFlight, data []byte, source archiveindex.ClaimSource) (storage.PayloadResult, error) {
	// Init jobs are submitted before the referencing media task to the same
	// FIFO service. Wait for the durable init sidecar/root metadata before
	// publishing a media segment that refers to it.
	if initDependency != nil {
		<-initDependency.done
		if initDependency.err != nil {
			if errors.Is(initDependency.err, storage.ErrIngestCoordinatorUnavailable) {
				return storage.PayloadResult{}, initDependency.err
			}
			return storage.PayloadResult{}, newStorageCommitFailure("init payload dependency", initDependency.err)
		}
	}
	if s.manager.storageWriteHook != nil {
		s.manager.storageWriteHook()
	}
	if s.manager.storageWriteFailureHook != nil {
		if err := s.manager.storageWriteFailureHook(); err != nil {
			return storage.PayloadResult{}, err
		}
	}
	s.e.mu.Lock()
	firstHandoverCommit := s.e.handoverFirstCommitPending && !s.e.handoverFirstCommitStarted
	if firstHandoverCommit {
		s.e.handoverFirstCommitStarted = true
	}
	recordingIDValue := s.e.recording.ID
	s.e.mu.Unlock()
	if firstHandoverCommit {
		if err := runtimehook.Pause(runtimehook.BeforeTargetFirstCommit, recordingIDValue); err != nil {
			return storage.PayloadResult{}, err
		}
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.e.mu.Lock()
	var expectedOwner *OwnershipToken
	if s.e.ownership != nil {
		copy := *s.e.ownership
		expectedOwner = &copy
	}
	s.e.mu.Unlock()
	result, plannedMarker, err := s.manager.commitArchiveSegment(s.e, expectedOwner, segment, source, data)
	if err != nil {
		if firstHandoverCommit {
			s.e.mu.Lock()
			s.e.handoverFirstCommitStarted = false
			s.e.mu.Unlock()
		}
		return storage.PayloadResult{}, err
	}
	if firstHandoverCommit {
		s.e.mu.Lock()
		s.e.handoverFirstCommitPending = false
		s.e.mu.Unlock()
		// withCanonicalMutation released the cross-process owner lock, after the
		// complete payload → sidecar → recording root publication.
		if err := runtimehook.Pause(runtimehook.AfterTargetFirstCommit, recordingIDValue); err != nil {
			return result, err
		}
	}
	if plannedMarker != nil && segment.SourceEpoch > 0 {
		s.epochMarkers[segment.SourceEpoch] = *plannedMarker
	}
	return result, nil
}

var errAwaitManifest = errors.New("segment retry awaits refreshed manifest")

// errStaleMediaGeneration indicates that a scheduler-owned source URI no
// longer belongs to the recording's current media source generation.
var errStaleMediaGeneration = errors.New("media source generation changed before fetch")

// awaitCurrentGeneration records the generation that invalidated a fetch and
// keeps any concurrently observed manifest from that generation or newer.
// Lock order matches discovery and admission: scheduler, then entry.
func (s *segmentScheduler) awaitCurrentGeneration(task *segmentTask) error {
	s.mu.Lock()
	if s.tasks[task.key] == task {
		s.e.mu.Lock()
		generation := s.e.mediaGeneration
		if generation > task.requiredGeneration {
			task.requiredGeneration = generation
		}
		if !task.available || !task.observed || task.observedGeneration < generation {
			task.available = false
			task.observed = false
		}
		s.e.mu.Unlock()
		s.signalLocked()
	}
	s.mu.Unlock()
	return errAwaitManifest
}

func (s *segmentScheduler) handleFetchFailure(task *segmentTask, media adapterproto.MediaSource, generation uint64, refreshCycles int, err error) error {
	if !s.manager.shouldRefresh(media, err) {
		return err
	}
	if refreshCycles >= maxRefreshCycles {
		return makeSegmentRefreshTrigger(task.key.epoch, task.key.discontinuitySequence, task.key.sequence, err)
	}
	if !s.markRefreshing(task) {
		return errAwaitManifest
	}
	_, nextGeneration, refreshErr := s.manager.refreshMediaAtGeneration(s.ctx, s.e, media, generation)
	if refreshErr != nil {
		return makeSegmentRefreshTrigger(task.key.epoch, task.key.discontinuitySequence, task.key.sequence, err)
	}
	s.mu.Lock()
	if s.tasks[task.key] == task {
		if nextGeneration > task.requiredGeneration {
			task.requiredGeneration = nextGeneration
		}
		if nextGeneration > generation {
			task.refreshCycles++
		}
	}
	s.mu.Unlock()
	// Resolve a current segment URI from the refreshed playlist before retrying;
	// awaitManifest wakes the poller only after the task is no longer in its
	// protected Refreshing state.
	return errAwaitManifest
}

func (s *segmentScheduler) markRefreshing(task *segmentTask) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.tasks[task.key] != task {
		return false
	}
	task.state = segmentTaskRefreshing
	task.awaitingManifest = false
	s.signalLocked()
	return true
}

func (s *segmentScheduler) acquireInit(segment hls.MediaSegment, epoch uint64, media adapterproto.MediaSource, generation uint64) (string, *initFlight, error) {
	return s.acquireInitUsingPayload(segment, epoch, media, generation, nil)
}

func (s *segmentScheduler) acquireInitWithPayload(segment hls.MediaSegment, epoch uint64, media adapterproto.MediaSource, generation uint64, payload *storage.IngestPayload) (string, *initFlight, error) {
	return s.acquireInitUsingPayload(segment, epoch, media, generation, payload)
}

func (s *segmentScheduler) acquireInitUsingPayload(segment hls.MediaSegment, epoch uint64, media adapterproto.MediaSource, generation uint64, stagedPayload *storage.IngestPayload) (string, *initFlight, error) {
	source := *segment.Init
	key := initIdentity(source, epoch, segment.DiscontinuitySequence)
	var err error
	recording := s.recordingSnapshot()
	asset := domain.Segment{
		TrackID: "main", Sequence: segment.Sequence, SourceEpoch: epoch,
		DiscontinuitySequence: segment.DiscontinuitySequence,
		SourceURI:             source.URI, ByteRange: cloneRange(source.ByteRange), IsInit: true,
	}
	id := initSegmentID(source, epoch, segment.DiscontinuitySequence)
	asset.ID = id
	if initExists(recording, id) {
		if stagedPayload != nil {
			stagedPayload.Release()
		}
		return id, nil, nil
	}
	s.mu.Lock()
	if flight := s.init[key]; flight != nil {
		s.mu.Unlock()
		if stagedPayload != nil {
			stagedPayload.Release()
		}
		select {
		case <-s.ctx.Done():
			return "", nil, s.ctx.Err()
		case <-flight.queued:
			if flight.queueErr != nil {
				return "", nil, flight.queueErr
			}
			return flight.id, flight, nil
		}
	}
	flight := &initFlight{queued: make(chan struct{}), done: make(chan struct{})}
	s.init[key] = flight
	s.mu.Unlock()

	payload := stagedPayload
	if payload == nil {
		payload, err = s.manager.downloadObjectBufferedOnceAtGenerationScope(s.ctx, source.URI, source.ByteRange, recording.ID, media, s.e, generation, adapterproto.RequestScopeInit)
		if err != nil {
			s.resolveInitQueueError(key, flight, err)
			return "", nil, err
		}
	}
	asset.PayloadSize, asset.SHA256 = payload.Result().Size, payload.Result().SHA256
	s.wg.Add(1)
	err = s.manager.ingest.SubmitWithKind(s.ctx, payload, storage.IngestJobKindInitPayload, func(data []byte) (storage.PayloadResult, error) {
		if s.manager.storageWriteHook != nil {
			s.manager.storageWriteHook()
		}
		if s.manager.storageWriteFailureHook != nil {
			if hookErr := s.manager.storageWriteFailureHook(); hookErr != nil {
				return storage.PayloadResult{}, hookErr
			}
		}
		return s.persistSegmentFrom(asset, nil, data, archiveindex.ClaimLiveOrigin)
	}, func(_ storage.PayloadResult, persistErr error) {
		defer s.wg.Done()
		s.mu.Lock()
		flight.err = persistErr
		if s.init[key] == flight {
			delete(s.init, key)
		}
		close(flight.done)
		s.signalLocked()
		s.mu.Unlock()
		if persistErr != nil && !errors.Is(persistErr, storage.ErrIngestCoordinatorUnavailable) {
			s.failStorage("init payload commit", persistErr)
		}
	})
	if err != nil {
		s.wg.Done()
		payload.Release()
		s.resolveInitQueueError(key, flight, err)
		return "", nil, err
	}
	s.mu.Lock()
	flight.id = id
	close(flight.queued)
	s.signalLocked()
	s.mu.Unlock()
	return id, flight, nil
}

func initExists(recording *domain.Recording, id string) bool {
	if recording == nil || recording.Tracks["main"] == nil {
		return false
	}
	for _, init := range recording.Tracks["main"].InitSegments {
		if init.ID == id {
			return true
		}
	}
	return false
}

func (s *segmentScheduler) resolveInitQueueError(key string, flight *initFlight, err error) {
	s.mu.Lock()
	flight.queueErr, flight.err = err, err
	if s.init[key] == flight {
		delete(s.init, key)
	}
	close(flight.queued)
	close(flight.done)
	s.signalLocked()
	s.mu.Unlock()
}

func initIdentity(source hls.Map, epoch, discontinuity uint64) string {
	key := fmt.Sprintf("%s|epoch=%d|discontinuity=%d", source.URI, epoch, discontinuity)
	if source.ByteRange != nil {
		key += fmt.Sprintf("|%d@%d", source.ByteRange.Length, source.ByteRange.Offset)
	}
	return key
}

func initSegmentID(source hls.Map, epoch, discontinuity uint64) string {
	digest := sha256.Sum256([]byte(initIdentity(source, epoch, discontinuity)))
	return "init-" + hex.EncodeToString(digest[:8])
}

func (s *segmentScheduler) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fatal
}

func (s *segmentScheduler) failFatal(err error) {
	s.mu.Lock()
	if s.fatal == nil {
		s.fatal = err
	}
	s.closed = true
	s.signalLocked()
	s.mu.Unlock()
	// Preserve the manager's normal interrupted-state/error path.
	s.e.mu.Lock()
	cancel := s.e.cancel
	s.e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.cancel()
}

func (s *segmentScheduler) failStorage(stage string, cause error) {
	if errors.Is(cause, storage.ErrIngestCoordinatorUnavailable) {
		s.failCoordinator(stage, cause)
		return
	}
	failure := newStorageCommitFailure(stage, cause)
	if s.manager != nil {
		// Persist the sanitized first-failure record at the canonical failure
		// boundary, before scheduler shutdown or caller polling can lose context.
		s.manager.recordStorageFailureDiagnostic(s.e, failure)
	}
	s.failFatal(failure)
}

func (s *segmentScheduler) failCoordinator(stage string, cause error) {
	if cause == nil {
		cause = storage.ErrIngestCoordinatorUnavailable
	}
	failure := storageCoordinatorFailure{stage: stage, cause: cause}
	if s.manager != nil {
		// Coordinator exhaustion is distinct from canonical corruption, but it
		// still needs a durable, sanitized record when it terminates a recording.
		s.manager.recordStorageFailureDiagnostic(s.e, failure)
	}
	s.failFatal(failure)
}

func (s *segmentScheduler) drain(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.fatal != nil {
			err := s.fatal
			s.mu.Unlock()
			return err
		}
		if len(s.tasks) == 0 && s.pendingSnapshots == 0 && s.pendingMetadata == 0 {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		drainWaitHook := s.drainWaitHook
		s.mu.Unlock()
		if drainWaitHook != nil {
			drainWaitHook()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			if err := s.failure(); err != nil {
				return err
			}
			return s.ctx.Err()
		case <-changed:
		}
	}
}

// drainForHandover is called only after the manifest worker has stopped
// admitting manifests. It uses the same task accounting as normal drain; a
// timeout leaves the scheduler open so the source worker can resume without
// converting queued work into gaps.
func (s *segmentScheduler) drainForHandover(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("segment scheduler is closed")
	}
	s.mu.Unlock()
	return s.drain(ctx)
}

// drainEndList drains only while ENDLIST still belongs to this media source
// generation. A concurrent refresh reopens admission and lets the poll loop
// fetch a manifest from the new source instead of finalizing the recording.
func (s *segmentScheduler) drainEndList(ctx context.Context, generation uint64) (bool, error) {
	for {
		s.mu.Lock()
		if s.fatal != nil {
			err := s.fatal
			s.mu.Unlock()
			return false, err
		}
		if !s.ending || s.endingGeneration != generation || !s.manifestGenerationCurrentLocked(generation) {
			s.mu.Unlock()
			return false, nil
		}
		if len(s.tasks) == 0 && s.pendingSnapshots == 0 && s.pendingMetadata == 0 {
			s.mu.Unlock()
			return true, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.ctx.Done():
			if err := s.failure(); err != nil {
				return false, err
			}
			return false, s.ctx.Err()
		case <-changed:
		}
	}
}

func (s *segmentScheduler) close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.signalLocked()
	}
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
	var pending []segmentTaskKey
	var staged []*storage.IngestPayload
	s.mu.Lock()
	for key, task := range s.tasks {
		pending = append(pending, key)
		if task.stagedInitPayload != nil {
			staged = append(staged, task.stagedInitPayload)
			task.stagedInitPayload = nil
		}
		if task.stagedMediaPayload != nil {
			staged = append(staged, task.stagedMediaPayload)
			task.stagedMediaPayload = nil
		}
	}
	s.tasks = make(map[segmentTaskKey]*segmentTask)
	s.ready = nil
	s.signalLocked()
	s.mu.Unlock()
	for _, payload := range staged {
		payload.Release()
	}
	for _, key := range pending {
		if s.manager.hasSequenceCoordinate(s.e, key.epoch, key.discontinuitySequence, key.sequence) {
			continue
		}
		if err := s.manager.markPending(s.e, key.epoch, key.discontinuitySequence, key.sequence, context.Canceled); err != nil {
			s.failFatal(err)
			return err
		}
	}
	s.e.mu.Lock()
	if s.e.scheduler == s {
		s.e.scheduler = nil
	}
	s.e.mu.Unlock()
	return s.failure()
}
