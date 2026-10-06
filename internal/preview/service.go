package preview

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	previewWorkers              = 2
	previewQueueCapacity        = 128
	indexPersistBatch           = 32
	previewTimeout              = 2 * time.Minute
	previewTick                 = 2 * time.Second
	maxTaskRetries              = 3
	reconcileRecordingsPerPass  = 16
	maxSegmentsPerRecordingPass = 64
	maxQueuedPerRecordingPass   = 4
	maxQueuedLatestPerPass      = 2
	maxOrphanCleanupPerStart    = 128
	previewIndexCacheCap        = 16
	previewTimelineCacheCap     = 16
)

var previewIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type RecordingList func() []*domain.Recording
type RecordingGet func(string) (*domain.Recording, error)

type taskKey struct {
	recordingID string
	ordinal     uint64
}

type task struct {
	key          taskKey
	recording    *domain.Recording
	segmentStart float64
}

type activeTask struct {
	cancel context.CancelFunc
}

type timelineCache struct {
	segments []timelineFingerprint
	total    float64
	starts   []float64
}

type timelineFingerprint struct {
	id                    string
	sha256                string
	timelineOrdinal       uint64
	archiveOrdinal        uint64
	sourceEpoch           uint64
	discontinuitySequence uint64
	sequence              uint64
	durationBits          uint64
}

// Service turns committed canonical media into disposable, reusable frame
// projections. The only FFmpeg caller is worker(); acquisition has no link to
// this package and never waits for preview work.
type Service struct {
	mu             sync.Mutex
	root           string
	previewRoot    string
	policyRoot     string
	store          *storage.Store
	list           RecordingList
	get            RecordingGet
	ffmpegPath     string
	queue          chan task
	ctx            context.Context
	cancel         context.CancelFunc
	workers        sync.WaitGroup
	closed         bool
	policies       map[string]Policy
	indexes        map[string]*Index
	dirtyIndexes   map[string]int
	indexAccess    map[string]uint64
	indexClock     uint64
	onIndexPersist func(string)
	indexChecked   map[string]bool
	scanCursor     map[string]int
	timelines      map[string]timelineCache
	timelineAccess map[string]uint64
	timelineClock  uint64
	queued         map[taskKey]struct{}
	active         map[taskKey]activeTask
	deleted        map[string]bool
	changed        chan struct{}
	reconcileID    string
}

func Open(root string, store *storage.Store, list RecordingList, ffmpegPath string) (*Service, error) {
	return OpenWithGet(root, store, list, nil, ffmpegPath)
}

func OpenWithGet(root string, store *storage.Store, list RecordingList, get RecordingGet, ffmpegPath string) (*Service, error) {
	if strings.TrimSpace(root) == "" || store == nil || list == nil {
		return nil, ErrInvalid
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("prepare preview root: %w", err)
	}
	rootInfo, err := os.Lstat(absRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("preview root is unsafe")
	}
	resolved := ""
	if ffmpegPath == "" {
		resolved, _ = exec.LookPath("ffmpeg")
	} else {
		if !filepath.IsAbs(ffmpegPath) {
			return nil, fmt.Errorf("ffmpeg path must be absolute")
		}
		resolved = filepath.Clean(ffmpegPath)
	}
	if resolved != "" {
		if path, resolveErr := filepath.EvalSymlinks(resolved); resolveErr == nil {
			resolved = path
		} else {
			resolved = ""
		}
		if resolved != "" {
			if info, statErr := os.Stat(resolved); statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
				resolved = ""
			}
		}
	}
	previewRoot := filepath.Join(absRoot, "previews")
	managementRoot := filepath.Join(absRoot, "management")
	policyRoot := filepath.Join(managementRoot, "previews")
	for _, dir := range []string{previewRoot, managementRoot, policyRoot} {
		if err := ensurePrivateDir(dir); err != nil {
			return nil, fmt.Errorf("prepare preview storage: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		root: absRoot, previewRoot: previewRoot, policyRoot: policyRoot,
		store: store, list: list, get: get, ffmpegPath: resolved,
		queue: make(chan task, previewQueueCapacity), ctx: ctx, cancel: cancel,
		policies: map[string]Policy{}, indexes: map[string]*Index{}, dirtyIndexes: map[string]int{}, indexAccess: map[string]uint64{}, indexChecked: map[string]bool{}, scanCursor: map[string]int{}, timelines: map[string]timelineCache{}, timelineAccess: map[string]uint64{}, queued: map[taskKey]struct{}{},
		active: map[taskKey]activeTask{}, deleted: map[string]bool{}, changed: make(chan struct{}),
	}
	if err := s.loadPolicies(); err != nil {
		cancel()
		return nil, err
	}
	if err := s.cleanupOrphans(); err != nil {
		cancel()
		return nil, err
	}
	for i := 0; i < previewWorkers; i++ {
		s.workers.Add(1)
		go s.worker()
	}
	return s, nil
}

func (s *Service) Available() bool { return s != nil && s.ffmpegPath != "" }

func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return ErrUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.Reconcile(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	ticker := time.NewTicker(previewTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.Reconcile(); err != nil && !errors.Is(err, context.Canceled) {
				// A transient filesystem/list failure is retried on the next bounded tick.
			}
		}
	}
}

// WaitForIdle waits until all preview tasks already accepted by the service
// have finished and their dirty indexes are durably published. It does not
// stop reconciliation or prevent later work from being enqueued; callers must
// stop and join Run/Reconcile producers before using it as a handoff barrier.
func (s *Service) WaitForIdle(ctx context.Context) error {
	if s == nil {
		return ErrUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if len(s.queued) == 0 && len(s.active) == 0 {
			ids := make([]string, 0, len(s.dirtyIndexes))
			for id, dirty := range s.dirtyIndexes {
				if dirty > 0 {
					ids = append(ids, id)
				}
			}
			sort.Strings(ids)
			for _, id := range ids {
				if err := s.flushIndexLocked(id); err != nil {
					s.mu.Unlock()
					return errors.New("preview index could not be saved while waiting for idle")
				}
			}
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		for _, active := range s.active {
			active.cancel()
		}
		s.notifyLocked()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.workers.Wait(); close(done) }()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		s.mu.Lock()
		defer s.mu.Unlock()
		for id := range s.dirtyIndexes {
			if err := s.flushIndexLocked(id); err != nil {
				return errors.New("preview index could not be saved during shutdown")
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) SetMode(recordingID string, mode Mode) (Summary, error) {
	if s == nil || !previewIDPattern.MatchString(recordingID) || mode != ModeDisabled && mode != ModeSegment {
		return Summary{}, ErrInvalid
	}
	policy := Policy{Version: 1, Mode: mode}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.deleted[recordingID] {
		return Summary{}, ErrUnavailable
	}
	if err := s.persistPolicyLocked(recordingID, policy); err != nil {
		return Summary{}, errors.New("preview policy could not be saved")
	}
	s.policies[recordingID] = policy
	if mode == ModeDisabled {
		for key, active := range s.active {
			if key.recordingID == recordingID {
				active.cancel()
			}
		}
	}
	s.notifyLocked()
	return s.summaryLocked(recordingID, nil), nil
}

func (s *Service) Policy(recordingID string) Policy {
	if s == nil || !previewIDPattern.MatchString(recordingID) {
		return Policy{Version: 1, Mode: ModeDisabled}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	policy := s.policies[recordingID]
	if policy.Mode == "" {
		policy = Policy{Version: 1, Mode: ModeDisabled}
	}
	return policy
}

func (s *Service) Summary(recording *domain.Recording) Summary {
	if recording == nil || !previewIDPattern.MatchString(recording.ID) {
		return Summary{Mode: ModeDisabled, State: StateDisabled}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.summaryLocked(recording.ID, recording)
}

func (s *Service) summaryLocked(id string, recording *domain.Recording) Summary {
	policy := s.policies[id]
	if policy.Mode == "" {
		policy = Policy{Version: 1, Mode: ModeDisabled}
	}
	result := Summary{Mode: policy.Mode, State: StateDisabled}
	// Disabled previews have no index semantics. In particular, list and
	// dashboard projections must not load every historical index just to report
	// that preview generation is off.
	if policy.Mode != ModeSegment {
		return result
	}
	idx := s.indexLocked(id)
	if idx == nil {
		return Summary{Mode: policy.Mode, State: StateUnavailable}
	}
	items := idx.Items
	var latest *uint64
	if len(items) != 0 {
		value := latestFrame(items).ArchiveOrdinal
		latest = &value
	}
	result = Summary{Mode: policy.Mode, State: StateDisabled, FrameCount: len(items), LatestArchiveOrdinal: latest}
	if idx.UpdatedAt.IsZero() == false {
		updated := idx.UpdatedAt
		result.UpdatedAt = &updated
	}
	result.Available = s.ffmpegPath != "" || len(items) > 0
	if !result.Available {
		result.State = StateUnavailable
		return result
	}
	expected := 0
	if recording != nil {
		if track := primaryTrack(recording); track != nil {
			for _, segment := range track.Segments {
				if segment.ArchiveOrdinal != 0 {
					expected++
				}
			}
		}
	}
	if len(items) > expected && expected > 0 {
		expected = len(items)
	}
	active := false
	queued := false
	for key := range s.active {
		if key.recordingID == id {
			active = true
			break
		}
	}
	if !active {
		for key := range s.queued {
			if key.recordingID == id {
				queued = true
				break
			}
		}
	}
	switch {
	case expected > 0 && len(items) >= expected:
		result.State = StateReady
	case active:
		result.State = StateProcessing
	case queued:
		result.State = StateQueued
	case len(items) > 0:
		result.State = StatePartial
	default:
		failures := idx.Failures
		if expected > 0 && len(failures) >= expected {
			result.State = StateFailed
		} else {
			result.State = StateQueued
		}
	}
	if len(items) > 0 {
		selection := latestFrame(items)
		if recording != nil && recording.State != domain.StateRecording {
			selection = posterFrame(items, playbackDuration(recording)*0.25)
		}
		value := selection.ArchiveOrdinal
		result.ImageArchiveOrdinal = &value
	}
	return result
}

func (s *Service) Items(recording *domain.Recording, sampling Sampling, limit int, timeSeconds float64) (Response, error) {
	if s == nil || recording == nil || !previewIDPattern.MatchString(recording.ID) {
		return Response{}, ErrInvalid
	}
	if limit < 1 || limit > maxPreviewLimit || sampling != SamplingUniform && sampling != SamplingRecent && sampling != SamplingNearest || sampling == SamplingNearest && (math.IsNaN(timeSeconds) || math.IsInf(timeSeconds, 0) || timeSeconds < 0) {
		return Response{}, ErrInvalidSample
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	summary := s.summaryLocked(recording.ID, recording)
	idx := s.indexLocked(recording.ID)
	if idx == nil {
		return Response{}, ErrUnavailable
	}
	frames := selectFrames(idx.Items, sampling, limit, timeSeconds, playbackDuration(recording))
	return Response{RecordingID: recording.ID, Mode: summary.Mode, State: summary.State, Available: summary.Available, FrameCount: summary.FrameCount, Items: frames}, nil
}

func (s *Service) OpenFrame(recordingID string, ordinal uint64) (*os.File, Frame, error) {
	if s == nil || !previewIDPattern.MatchString(recordingID) || ordinal == 0 {
		return nil, Frame{}, ErrNotFound
	}
	s.mu.Lock()
	if s.deleted[recordingID] {
		s.mu.Unlock()
		return nil, Frame{}, ErrNotFound
	}
	idx := s.indexLocked(recordingID)
	if idx == nil {
		s.mu.Unlock()
		return nil, Frame{}, ErrNotFound
	}
	frame, exists := findFrame(idx.Items, ordinal)
	s.mu.Unlock()
	if !exists {
		return nil, Frame{}, ErrNotFound
	}
	directory := filepath.Join(s.previewRoot, recordingID)
	framesDir := filepath.Join(directory, "frames")
	if err := requirePrivateDirectory(s.previewRoot, directory, framesDir); err != nil {
		return nil, Frame{}, ErrNotFound
	}
	path := framePath(s.previewRoot, recordingID, ordinal)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != frame.Size || info.Size() <= 0 || info.Size() > maxFrameBytes {
		return nil, Frame{}, ErrNotFound
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, Frame{}, ErrNotFound
	}
	opened, statErr := f.Stat()
	if statErr != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		_ = f.Close()
		return nil, Frame{}, ErrNotFound
	}
	width, height, digest, valid := inspectJPEG(path, opened.Size())
	if !valid || width != frame.Width || height != frame.Height || digest != frame.ImageSHA256 {
		_ = f.Close()
		return nil, Frame{}, ErrNotFound
	}
	return f, frame, nil
}

func (s *Service) RetryFailed(recordingID string) error {
	if s == nil || !previewIDPattern.MatchString(recordingID) {
		return ErrInvalid
	}
	s.mu.Lock()
	if s.closed || s.deleted[recordingID] {
		s.mu.Unlock()
		return ErrUnavailable
	}
	idx := s.indexLocked(recordingID)
	if idx == nil {
		s.mu.Unlock()
		return ErrUnavailable
	}
	idx.Failures = nil
	err := s.persistIndexLocked(recordingID, idx)
	s.mu.Unlock()
	if err != nil {
		return errors.New("preview index could not be updated")
	}
	return s.Reconcile()
}

// DeleteRecording marks the ID permanently deleted for this service instance,
// cancels and joins its active extraction workers, then removes only disposable
// preview state. A queued stale task checks this tombstone before staging.
func (s *Service) DeleteRecording(recordingID string) error {
	if s == nil || !previewIDPattern.MatchString(recordingID) {
		return ErrInvalid
	}
	s.mu.Lock()
	s.deleted[recordingID] = true
	for key, active := range s.active {
		if key.recordingID == recordingID {
			active.cancel()
		}
	}
	s.notifyLocked()
	for s.recordingActiveLocked(recordingID) {
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-s.ctx.Done():
			s.mu.Lock()
			s.mu.Unlock()
			return context.Canceled
		}
		s.mu.Lock()
	}
	delete(s.policies, recordingID)
	delete(s.indexes, recordingID)
	delete(s.dirtyIndexes, recordingID)
	delete(s.indexAccess, recordingID)
	delete(s.indexChecked, recordingID)
	delete(s.scanCursor, recordingID)
	delete(s.timelines, recordingID)
	delete(s.timelineAccess, recordingID)
	for key := range s.queued {
		if key.recordingID == recordingID {
			delete(s.queued, key)
		}
	}
	s.mu.Unlock()
	policyPath := filepath.Join(s.policyRoot, recordingID+".json")
	if err := removeRegularOrMissing(policyPath); err != nil {
		return errors.New("preview policy cleanup failed")
	}
	previewPath := filepath.Join(s.previewRoot, recordingID)
	if err := removePrivateTree(previewPath); err != nil {
		return errors.New("preview projection cleanup failed")
	}
	return nil
}

func (s *Service) Reconcile() error {
	if s == nil {
		return ErrUnavailable
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	var byID map[string]*domain.Recording
	if s.get == nil {
		recordings := s.list()
		byID = make(map[string]*domain.Recording, len(recordings))
		for _, recording := range recordings {
			if recording != nil && previewIDPattern.MatchString(recording.ID) {
				byID[recording.ID] = recording
			}
		}
	}
	s.mu.Lock()
	ids := make([]string, 0, len(s.policies))
	for id := range s.policies {
		if !s.deleted[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > 1 && s.reconcileID != "" {
		start := sort.SearchStrings(ids, s.reconcileID)
		if start < len(ids) && ids[start] == s.reconcileID {
			start = (start + 1) % len(ids)
		}
		ids = append(append([]string(nil), ids[start:]...), ids[:start]...)
	}
	if !s.Available() {
		s.mu.Unlock()
		return nil
	}
	if len(ids) > reconcileRecordingsPerPass {
		ids = ids[:reconcileRecordingsPerPass]
	}
	s.mu.Unlock()
	for _, id := range ids {
		recording := byID[id]
		if s.get != nil {
			var err error
			recording, err = s.get(id)
			if err != nil {
				recording = nil
			}
		}
		s.mu.Lock()
		if s.closed || s.deleted[id] || s.policies[id].Mode != ModeSegment || s.ctx.Err() != nil {
			s.mu.Unlock()
			continue
		}
		if recording == nil {
			s.reconcileID = id
			s.mu.Unlock()
			continue
		}
		track := primaryTrack(recording)
		if track == nil {
			s.reconcileID = id
			s.mu.Unlock()
			continue
		}
		// ArchiveOrdinal remains the append/storage identity. Playback work uses
		// the current revisionable timeline projection, which can insert a late
		// prefix or repair a middle gap.
		segments := orderedSegments(track.Segments)
		idx := s.indexLocked(id)
		if idx == nil {
			// A dirty LRU entry could not be durably flushed. Keep the hard
			// resident-index bound and retry this recording on a later pass.
			s.reconcileID = id
			s.mu.Unlock()
			continue
		}
		starts, timelineChanged := s.timelineStartsAndRevisionLocked(id, segments)
		if timelineChanged {
			// A previous cursor refers to the former order. Rescan from the
			// beginning after a revision so repaired/prefixed segments are not
			// starved behind that stale cursor.
			s.scanCursor[id] = 0
		}
		indexChanged := false
		if !s.indexChecked[id] && s.recoverReadyFramesLocked(id, recording, track, segments) {
			indexChanged = true
		}
		if s.refreshFrameTimingsLocked(id, segments, starts) {
			indexChanged = true
		}
		if indexChanged {
			if err := s.persistIndexLocked(id, idx); err != nil {
				// Keep the old durable index; next pass retries reconciliation.
			}
		}
		if recording.State != domain.StateRecording && !s.recordingHasTasksLocked(id) && s.dirtyIndexes[id] > 0 {
			_ = s.flushIndexLocked(id)
		}
		queuedForRecording := 0
		// Live freshness takes priority, but every recording only gets a small
		// queue budget. The cursor then advances through history over later
		// reconciliations so old missing frames cannot starve.
		if recording.State == domain.StateRecording && len(segments) > 0 {
			latestStart := max(0, len(segments)-maxQueuedLatestPerPass)
			for i := len(segments) - 1; i >= latestStart && queuedForRecording < maxQueuedLatestPerPass; i-- {
				if s.enqueueLocked(id, recording, segments, i, starts[i]) {
					queuedForRecording++
				}
			}
		}
		cursor := s.scanCursor[id]
		if cursor < 0 || cursor >= len(segments) {
			cursor = 0
		}
		end := min(len(segments), cursor+maxSegmentsPerRecordingPass)
		for i := cursor; i < end && queuedForRecording < maxQueuedPerRecordingPass; i++ {
			if s.enqueueLocked(id, recording, segments, i, starts[i]) {
				queuedForRecording++
			}
		}
		if end >= len(segments) {
			s.scanCursor[id] = 0
		} else {
			s.scanCursor[id] = end
		}
		s.reconcileID = id
		queueFull := len(s.queue) >= cap(s.queue)
		s.mu.Unlock()
		if queueFull {
			break
		}
	}
	return nil
}

func (s *Service) enqueueLocked(id string, recording *domain.Recording, segments []domain.Segment, position int, segmentStart float64) bool {
	segment := segments[position]
	if segment.ArchiveOrdinal == 0 || s.deleted[id] {
		return false
	}
	idx := s.indexLocked(id)
	if idx == nil {
		return false
	}
	if frame, ok := findFrame(idx.Items, segment.ArchiveOrdinal); ok {
		if frame.SegmentSHA256 == segment.SHA256 && frameFileMatches(s.previewRoot, id, segment.ArchiveOrdinal, frame) {
			return false
		}
		idx.Items = removeFrame(idx.Items, segment.ArchiveOrdinal)
	}
	for _, failure := range idx.Failures {
		if failure.ArchiveOrdinal == segment.ArchiveOrdinal {
			if failure.Permanent || failure.Attempts >= maxTaskRetries || time.Now().Before(failure.RetryAfter) {
				return false
			}
		}
	}
	key := taskKey{recordingID: id, ordinal: segment.ArchiveOrdinal}
	if _, exists := s.queued[key]; exists {
		return false
	}
	if _, exists := s.active[key]; exists {
		return false
	}
	// Each queue item retains only the bounded decoder window. A queue full of
	// long-running recordings must not pin a full deep copy of every archive.
	item := task{key: key, recording: boundedRecordingSnapshot(recording, segments, position, segmentStart), segmentStart: segmentStart}
	if item.recording == nil {
		return false
	}
	select {
	case s.queue <- item:
		s.queued[key] = struct{}{}
		return true
	default:
		return false
	}
}

func boundedRecordingSnapshot(recording *domain.Recording, segments []domain.Segment, position int, segmentStart float64) *domain.Recording {
	if recording == nil || position < 0 || position >= len(segments) {
		return nil
	}
	originalTrack := primaryTrack(recording)
	if originalTrack == nil {
		return nil
	}
	target := segments[position]
	attempts, err := buildContextAttemptsAtStart(recording, originalTrack, segments, target.ArchiveOrdinal, segmentStart)
	var window []domain.Segment
	if err != nil || len(attempts) == 0 {
		window = []domain.Segment{target}
	} else {
		// The queue snapshot needs the largest bounded fallback window; avoid
		// rebuilding playback starts for the full archive for every task.
		window = attempts[len(attempts)-1].segments
	}
	track := &domain.Track{ID: originalTrack.ID, Segments: append([]domain.Segment(nil), window...), InitSegments: []domain.Segment{}}
	seenInit := make(map[string]struct{}, len(window))
	for _, media := range window {
		if media.InitSegmentID == "" {
			continue
		}
		if _, exists := seenInit[media.InitSegmentID]; exists {
			continue
		}
		seenInit[media.InitSegmentID] = struct{}{}
		if init, ok := initByID(originalTrack, media.InitSegmentID); ok {
			track.InitSegments = append(track.InitSegments, init)
		}
	}
	return &domain.Recording{ID: recording.ID, State: recording.State, Tracks: map[string]*domain.Track{track.ID: track}}
}

func (s *Service) worker() {
	defer s.workers.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case item := <-s.queue:
			s.runTask(item)
		}
	}
}

func (s *Service) runTask(item task) {
	s.mu.Lock()
	delete(s.queued, item.key)
	if s.closed || s.deleted[item.key.recordingID] || s.policies[item.key.recordingID].Mode != ModeSegment {
		s.notifyLocked()
		s.mu.Unlock()
		return
	}
	idx := s.indexLocked(item.key.recordingID)
	if idx == nil {
		s.notifyLocked()
		s.mu.Unlock()
		return
	}
	if existing, ok := findFrame(idx.Items, item.key.ordinal); ok {
		if existing.SegmentSHA256 != "" && frameFileMatches(s.previewRoot, item.key.recordingID, item.key.ordinal, existing) {
			s.notifyLocked()
			s.mu.Unlock()
			return
		}
		idx.Items = removeFrame(idx.Items, item.key.ordinal)
	}
	attempt := 1
	for _, failure := range idx.Failures {
		if failure.ArchiveOrdinal == item.key.ordinal {
			attempt = failure.Attempts + 1
			break
		}
	}
	workCtx, cancel := context.WithTimeout(s.ctx, previewTimeout)
	s.active[item.key] = activeTask{cancel: cancel}
	s.notifyLocked()
	s.mu.Unlock()

	frame, code, permanent := s.extractAtStart(workCtx, item.recording, item.key.ordinal, item.segmentStart)
	cancel()

	s.mu.Lock()
	delete(s.active, item.key)
	if !s.deleted[item.key.recordingID] && !s.closed && s.policies[item.key.recordingID].Mode == ModeSegment {
		idx = s.indexLocked(item.key.recordingID)
		if idx != nil && code == "" {
			if err := s.publishFrameLocked(item.key.recordingID, frame); err == nil {
				frame.imageData = nil
				idx.Items = upsertFrame(idx.Items, frame)
				idx.Failures = removeFailure(idx.Failures, frame.ArchiveOrdinal)
				idx.UpdatedAt = time.Now().UTC()
				_ = s.markIndexDirtyLocked(item.key.recordingID, false)
			} else {
				code, permanent = "decode_failed", false
			}
		}
		if idx != nil && code != "" {
			failurePermanent := permanent || attempt >= maxTaskRetries
			idx.Failures = upsertFailure(idx.Failures, Failure{ArchiveOrdinal: item.key.ordinal, Code: code, Attempts: attempt, Permanent: failurePermanent, RetryAfter: time.Now().Add(time.Duration(1<<min(attempt-1, 5)) * time.Second).UTC()})
			idx.UpdatedAt = time.Now().UTC()
			_ = s.markIndexDirtyLocked(item.key.recordingID, failurePermanent)
		}
	}
	if item.recording.State != domain.StateRecording && !s.recordingHasTasksLocked(item.key.recordingID) && s.dirtyIndexes[item.key.recordingID] > 0 {
		_ = s.flushIndexLocked(item.key.recordingID)
	}
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *Service) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Service) recordingActiveLocked(id string) bool {
	for key := range s.active {
		if key.recordingID == id {
			return true
		}
	}
	return false
}

func (s *Service) recordingHasTasksLocked(id string) bool {
	for key := range s.queued {
		if key.recordingID == id {
			return true
		}
	}
	return s.recordingActiveLocked(id)
}

// timelineStartsLocked maintains a per-recording cache for the current ordered
// playback projection. Archive identities remain append-only, but a projection
// can be revised when late media is discovered or a gap is repaired.
func (s *Service) timelineStartsLocked(id string, segments []domain.Segment) []float64 {
	starts, _ := s.timelineStartsAndRevisionLocked(id, segments)
	return starts
}

// timelineStartsAndRevisionLocked incrementally extends the cached timeline
// only while the entire prior ordered projection is unchanged. A late prefix,
// a middle repair, a changed duration, or a changed playback ordinal invalidates
// the affected suffix (and insertion/reordering naturally invalidates it all).
func (s *Service) timelineStartsAndRevisionLocked(id string, segments []domain.Segment) ([]float64, bool) {
	cache, exists := s.timelines[id]
	if exists {
		s.touchTimelineLocked(id)
	} else {
		if len(s.timelines) >= previewTimelineCacheCap {
			s.evictTimelineLocked()
		}
		s.touchTimelineLocked(id)
	}
	fingerprints := make([]timelineFingerprint, len(segments))
	for i, segment := range segments {
		fingerprints[i] = fingerprintTimelineSegment(segment)
	}
	validPrefix := exists && len(cache.segments) <= len(fingerprints)
	if validPrefix {
		for i := range cache.segments {
			if cache.segments[i] != fingerprints[i] {
				validPrefix = false
				break
			}
		}
	}
	revised := exists && !validPrefix
	if !validPrefix {
		cache = timelineCache{starts: make([]float64, 0, len(segments))}
		cache.segments = make([]timelineFingerprint, 0, len(segments))
	}
	for i := len(cache.segments); i < len(segments); i++ {
		segment := segments[i]
		cache.starts = append(cache.starts, cache.total)
		cache.segments = append(cache.segments, fingerprints[i])
		if validDuration(segment.Duration) && cache.total <= math.MaxFloat64-segment.Duration {
			cache.total += segment.Duration
		}
	}
	s.timelines[id] = cache
	return cache.starts, revised
}

func fingerprintTimelineSegment(segment domain.Segment) timelineFingerprint {
	return timelineFingerprint{
		id:                    segment.ID,
		sha256:                segment.SHA256,
		timelineOrdinal:       segment.TimelineOrdinal,
		archiveOrdinal:        segment.ArchiveOrdinal,
		sourceEpoch:           segment.SourceEpoch,
		discontinuitySequence: segment.DiscontinuitySequence,
		sequence:              segment.Sequence,
		durationBits:          math.Float64bits(segment.Duration),
	}
}

// refreshFrameTimingsLocked updates only disposable timeline metadata. Frame
// JPEGs remain keyed by ArchiveOrdinal and are not regenerated merely because
// an earlier segment was discovered or repaired.
func (s *Service) refreshFrameTimingsLocked(id string, segments []domain.Segment, starts []float64) bool {
	idx := s.indexLocked(id)
	if idx == nil {
		return false
	}
	segmentByOrdinal := make(map[uint64]domain.Segment, len(segments))
	startByOrdinal := make(map[uint64]float64, len(segments))
	for index, segment := range segments {
		if segment.ArchiveOrdinal != 0 {
			segmentByOrdinal[segment.ArchiveOrdinal] = segment
			if index < len(starts) {
				startByOrdinal[segment.ArchiveOrdinal] = starts[index]
			}
		}
	}
	changed := false
	for i := range idx.Items {
		frame := &idx.Items[i]
		segment, ok := segmentByOrdinal[frame.ArchiveOrdinal]
		if !ok || segment.SHA256 != frame.SegmentSHA256 {
			continue
		}
		start := startByOrdinal[frame.ArchiveOrdinal]
		offset := frame.FrameTimeSeconds - frame.SegmentStartSeconds
		if offset < 0 || math.IsNaN(offset) || math.IsInf(offset, 0) {
			offset = 0
		}
		frameTime := start + offset
		if frame.SegmentStartSeconds != start || frame.FrameTimeSeconds != frameTime || frame.SegmentDurationSeconds != segment.Duration || frame.SourceEpoch != segment.SourceEpoch || frame.Sequence != segment.Sequence {
			frame.SegmentStartSeconds = start
			frame.FrameTimeSeconds = frameTime
			frame.SegmentDurationSeconds = segment.Duration
			frame.SourceEpoch = segment.SourceEpoch
			frame.Sequence = segment.Sequence
			changed = true
		}
	}
	if changed {
		idx.UpdatedAt = time.Now().UTC()
		_ = s.markIndexDirtyLocked(id, false)
	}
	return changed
}

func (s *Service) touchTimelineLocked(id string) {
	s.timelineClock++
	s.timelineAccess[id] = s.timelineClock
}

func (s *Service) evictTimelineLocked() {
	victim := ""
	var oldest uint64
	for id := range s.timelines {
		access, ok := s.timelineAccess[id]
		if !ok {
			access = 0
		}
		if victim == "" || access < oldest || access == oldest && id < victim {
			victim, oldest = id, access
		}
	}
	if victim == "" {
		return
	}
	delete(s.timelines, victim)
	delete(s.timelineAccess, victim)
}

func (s *Service) loadPolicies() error {
	entries, err := os.ReadDir(s.policyRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		if !previewIDPattern.MatchString(id) {
			continue
		}
		path := filepath.Join(s.policyRoot, entry.Name())
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
			continue
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		var policy Policy
		if json.Unmarshal(data, &policy) != nil || policy.Version != 1 || policy.Mode != ModeDisabled && policy.Mode != ModeSegment {
			continue
		}
		_ = os.Chmod(path, 0600)
		s.policies[id] = policy
	}
	return nil
}

func (s *Service) cleanupOrphans() error {
	var present map[string]bool
	if s.get == nil {
		present = map[string]bool{}
		for _, recording := range s.list() {
			if recording != nil && previewIDPattern.MatchString(recording.ID) {
				present[recording.ID] = true
			}
		}
	}
	s.mu.Lock()
	policyCandidates := make([]string, 0, maxOrphanCleanupPerStart)
	for id := range s.policies {
		policyCandidates = append(policyCandidates, id)
		if len(policyCandidates) == maxOrphanCleanupPerStart {
			break
		}
	}
	s.mu.Unlock()
	for _, id := range policyCandidates {
		exists, definitive := s.recordingExistsForCleanup(id, present)
		if exists || !definitive {
			continue
		}
		s.mu.Lock()
		if _, exists := s.policies[id]; exists {
			s.deleted[id] = true
			delete(s.policies, id)
		}
		s.mu.Unlock()
		_ = removeRegularOrMissing(filepath.Join(s.policyRoot, id+".json"))
	}
	root, err := os.Open(s.previewRoot)
	if err != nil {
		return err
	}
	entries, readErr := root.ReadDir(maxOrphanCleanupPerStart)
	closeErr := root.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	for _, entry := range entries {
		id := entry.Name()
		if strings.HasPrefix(id, ".work-") {
			_ = removePrivateTree(filepath.Join(s.previewRoot, id))
			continue
		}
		if !previewIDPattern.MatchString(id) {
			continue
		}
		exists, definitive := s.recordingExistsForCleanup(id, present)
		if exists || !definitive {
			continue
		}
		path := filepath.Join(s.previewRoot, id)
		s.mu.Lock()
		s.deleted[id] = true
		delete(s.policies, id)
		delete(s.indexes, id)
		s.mu.Unlock()
		_ = removePrivateTree(path)
		_ = removeRegularOrMissing(filepath.Join(s.policyRoot, id+".json"))
	}
	return nil
}

func (s *Service) recordingExistsForCleanup(id string, present map[string]bool) (exists, definitive bool) {
	if s.get == nil {
		return present[id], true
	}
	recording, err := s.get(id)
	if err == nil {
		return recording != nil, true
	}
	if errors.Is(err, storage.ErrNotFound) {
		return false, true
	}
	return false, false
}

func (s *Service) indexLocked(id string) *Index {
	if idx := s.indexes[id]; idx != nil {
		s.touchIndexLocked(id)
		return idx
	}
	if !s.makeIndexRoomLocked() {
		return nil
	}
	idx := &Index{Version: ProfileVersion, Profile: defaultProfile(), Items: []Frame{}, Failures: []Failure{}}
	path := filepath.Join(s.previewRoot, id, "index.json")
	info, err := os.Lstat(path)
	if err == nil && info.Mode().IsRegular() && info.Size() > 0 && info.Size() <= maxIndexBytes {
		if data, readErr := os.ReadFile(path); readErr == nil {
			var loaded Index
			if json.Unmarshal(data, &loaded) == nil && loaded.Version == ProfileVersion && loaded.Profile == defaultProfile() && len(loaded.Items) <= 1_000_000 && len(loaded.Failures) <= 1_000_000 {
				idx = &loaded
				if idx.Items == nil {
					idx.Items = []Frame{}
				}
				if idx.Failures == nil {
					idx.Failures = []Failure{}
				}
			}
		}
	}
	s.indexes[id] = idx
	s.touchIndexLocked(id)
	return idx
}

func (s *Service) touchIndexLocked(id string) {
	s.indexClock++
	s.indexAccess[id] = s.indexClock
}

// makeIndexRoomLocked enforces a hard resident-index bound. Dirty entries are
// evicted only after their snapshot is durably written; on write failure the
// caller receives nil from indexLocked and must leave the old entry resident.
func (s *Service) makeIndexRoomLocked() bool {
	if len(s.indexes) < previewIndexCacheCap {
		return true
	}
	victim := ""
	var oldest uint64
	for id := range s.indexes {
		access, ok := s.indexAccess[id]
		if !ok {
			access = 0
		}
		if victim == "" || access < oldest || access == oldest && id < victim {
			victim, oldest = id, access
		}
	}
	if victim == "" {
		return false
	}
	if s.dirtyIndexes[victim] > 0 {
		if err := s.persistIndexLocked(victim, s.indexes[victim]); err != nil {
			return false
		}
	}
	delete(s.indexes, victim)
	delete(s.indexAccess, victim)
	return true
}

func (s *Service) persistPolicyLocked(id string, policy Policy) error {
	managementRoot := filepath.Dir(s.policyRoot)
	if err := requirePrivateDirectory(s.root, managementRoot, s.policyRoot); err != nil {
		return err
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(s.policyRoot, id+".json"), append(data, '\n'))
}

func (s *Service) persistIndexLocked(id string, idx *Index) error {
	if s.deleted[id] {
		return ErrNotFound
	}
	if idx == nil || s.indexes[id] != idx {
		return ErrUnavailable
	}
	directory := filepath.Join(s.previewRoot, id)
	if err := ensurePrivateDir(directory); err != nil {
		return err
	}
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil || len(data) > maxIndexBytes {
		return ErrInvalid
	}
	if err = atomicWrite(filepath.Join(directory, "index.json"), append(data, '\n')); err != nil {
		return err
	}
	delete(s.dirtyIndexes, id)
	if s.onIndexPersist != nil {
		s.onIndexPersist(id)
	}
	return nil
}

func (s *Service) markIndexDirtyLocked(id string, immediate bool) error {
	s.dirtyIndexes[id]++
	if immediate || s.dirtyIndexes[id] >= indexPersistBatch {
		return s.flushIndexLocked(id)
	}
	return nil
}

func (s *Service) flushIndexLocked(id string) error {
	if s.dirtyIndexes[id] == 0 {
		return nil
	}
	idx := s.indexes[id]
	if idx == nil {
		return ErrUnavailable
	}
	return s.persistIndexLocked(id, idx)
}

func (s *Service) publishFrameLocked(id string, frame Frame) error {
	if s.deleted[id] || frame.ArchiveOrdinal == 0 {
		return ErrNotFound
	}
	directory := filepath.Join(s.previewRoot, id)
	framesDir := filepath.Join(directory, "frames")
	for _, dir := range []string{directory, framesDir} {
		if err := ensurePrivateDir(dir); err != nil {
			return err
		}
	}
	path := framePath(s.previewRoot, id, frame.ArchiveOrdinal)
	if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
		// Ready frames are immutable except through a retry explicitly requested
		// after their index entry was removed or marked failed.
		idx := s.indexLocked(id)
		if idx == nil {
			return ErrUnavailable
		}
		if old, ok := findFrame(idx.Items, frame.ArchiveOrdinal); ok && old.SegmentSHA256 == frame.SegmentSHA256 {
			return nil
		}
	}
	if len(frame.imageData) == 0 || int64(len(frame.imageData)) != frame.Size {
		return ErrInvalid
	}
	tmp, err := os.CreateTemp(framesDir, ".frame-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(0600); err == nil {
		if frame.Size > maxFrameBytes {
			err = ErrInvalid
		} else {
			_, err = tmp.Write(frame.imageData)
		}
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	width, height, digest, valid := inspectJPEG(tmpPath, frame.Size)
	if !valid || width != frame.Width || height != frame.Height || digest != frame.ImageSHA256 {
		return ErrInvalid
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return err
	}
	dir, err := os.Open(framesDir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func (s *Service) recoverReadyFramesLocked(id string, recording *domain.Recording, track *domain.Track, segments []domain.Segment) bool {
	idx := s.indexLocked(id)
	if idx == nil {
		return false
	}
	changed := false
	segmentByID := make(map[uint64]domain.Segment, len(segments))
	for _, segment := range segments {
		if segment.ArchiveOrdinal != 0 {
			segmentByID[segment.ArchiveOrdinal] = segment
		}
	}
	frameByID := make(map[uint64]Frame, len(idx.Items))
	indexedOrdinals := make(map[uint64]struct{}, len(idx.Items))
	valid := make([]Frame, 0, len(idx.Items))
	for _, frame := range idx.Items {
		indexedOrdinals[frame.ArchiveOrdinal] = struct{}{}
		segment, ok := segmentByID[frame.ArchiveOrdinal]
		if !ok || segment.SHA256 != frame.SegmentSHA256 {
			changed = true
			continue
		}
		isValid := frameFileMatches(s.previewRoot, id, frame.ArchiveOrdinal, frame)
		if isValid {
			valid = append(valid, frame)
			frameByID[frame.ArchiveOrdinal] = frame
			continue
		}
		changed = true
	}
	idx.Items = valid
	starts := segmentStarts(segments)
	directory := filepath.Join(s.previewRoot, id)
	framesDir := filepath.Join(directory, "frames")
	framesDirectorySafe := requirePrivateDirectory(s.previewRoot, directory, framesDir) == nil
	for _, segment := range segments {
		if segment.ArchiveOrdinal == 0 {
			continue
		}
		if _, hadIndex := indexedOrdinals[segment.ArchiveOrdinal]; hadIndex {
			// An invalid indexed image is not an orphan worth adopting. It must
			// be regenerated from its canonical segment instead.
			continue
		}
		if !framesDirectorySafe {
			continue
		}
		if _, ok := frameByID[segment.ArchiveOrdinal]; ok {
			continue
		}
		path := framePath(s.previewRoot, id, segment.ArchiveOrdinal)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxFrameBytes {
			continue
		}
		width, height, imageHash, valid := inspectJPEG(path, info.Size())
		if !valid {
			continue
		}
		start := starts[segment.ArchiveOrdinal]
		frame := makeFrame(track.ID, segment, start, width, height, info.Size(), info.ModTime().UTC(), imageHash)
		idx.Items = append(idx.Items, frame)
		frameByID[segment.ArchiveOrdinal] = frame
		changed = true
	}
	sort.Slice(idx.Items, func(i, j int) bool { return idx.Items[i].ArchiveOrdinal < idx.Items[j].ArchiveOrdinal })
	s.indexChecked[id] = true
	if changed {
		idx.UpdatedAt = time.Now().UTC()
	}
	return changed
}

func (s *Service) extract(ctx context.Context, recording *domain.Recording, ordinal uint64) (Frame, string, bool) {
	track := primaryTrack(recording)
	if track == nil {
		return Frame{}, "unsupported", true
	}
	segments := orderedSegments(track.Segments)
	start, ok := segmentStarts(segments)[ordinal]
	if !ok {
		return Frame{}, "unsupported", true
	}
	return s.extractAtStart(ctx, recording, ordinal, start)
}

func (s *Service) extractAtStart(ctx context.Context, recording *domain.Recording, ordinal uint64, start float64) (Frame, string, bool) {
	if s.ffmpegPath == "" {
		return Frame{}, "source_unavailable", true
	}
	track := primaryTrack(recording)
	if track == nil {
		return Frame{}, "unsupported", true
	}
	segments := track.Segments
	attempts, err := buildContextAttemptsAtStart(recording, track, segments, ordinal, start)
	if err != nil {
		return Frame{}, "unsupported", true
	}
	tempDir, err := os.MkdirTemp(s.previewRoot, ".work-")
	if err != nil {
		return Frame{}, "source_unavailable", false
	}
	if err = os.Chmod(tempDir, 0700); err != nil {
		_ = os.RemoveAll(tempDir)
		return Frame{}, "source_unavailable", false
	}
	defer os.RemoveAll(tempDir)
	var lastCode string
	for attemptIndex, attempt := range attempts {
		if err := ctx.Err(); err != nil {
			return Frame{}, "timeout", false
		}
		attemptDir := filepath.Join(tempDir, fmt.Sprintf("attempt-%d", attemptIndex))
		if err := os.Mkdir(attemptDir, 0700); err != nil {
			return Frame{}, "source_unavailable", false
		}
		localNames, stageErr := s.stageWindow(ctx, recording.ID, track, attempt.segments, attemptDir)
		if stageErr != nil {
			if errors.Is(stageErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return Frame{}, "timeout", false
			}
			return Frame{}, "source_unavailable", true
		}
		playlist, playlistErr := localPlaylist(track, attempt.segments, localNames)
		if playlistErr != nil {
			return Frame{}, "unsupported", true
		}
		if err := writePrivate(filepath.Join(attemptDir, "input.m3u8"), []byte(playlist)); err != nil {
			return Frame{}, "source_unavailable", false
		}
		args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-protocol_whitelist", "file"}
		if attemptIndex == 0 {
			// The target-only fast path emits the earliest decoder-reported
			// random-access frame. A dependent target can have no keyframe of its
			// own, so bounded context fallback must decode ordinary pictures after
			// its target-boundary seek instead of filtering those pictures away.
			args = append(args, "-skip_frame", "nokey")
		}
		args = append(args, "-i", "input.m3u8")
		if attempt.targetOffset > 0 {
			// Output-side accurate seeking discards compatible context and starts
			// at the target segment boundary; it never selects a segment midpoint.
			args = append(args, "-ss", fmt.Sprintf("%.6f", attempt.targetOffset))
		}
		args = append(args, "-map", "0:v:0", "-frames:v", "1", "-vf", "scale=480:270:force_original_aspect_ratio=decrease:force_divisible_by=2,pad=480:270:(ow-iw)/2:(oh-ih)/2", "-an", "-sn", "-dn", "-c:v", "mjpeg", "-q:v", "4", "-f", "image2", "frame.jpg")
		command := exec.CommandContext(ctx, s.ffmpegPath, args...)
		stderr := &limitedBuffer{limit: 4096}
		command.Dir, command.Stdout, command.Stderr = attemptDir, io.Discard, stderr
		if runErr := command.Run(); runErr != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return Frame{}, "timeout", false
			}
			code, permanent := classifyFFmpegFailure(stderr.String())
			lastCode = code
			if code == "decode_failed" && !permanent && attemptIndex+1 < len(attempts) {
				continue
			}
			return Frame{}, code, permanent
		}
		if ctx.Err() != nil {
			return Frame{}, "timeout", false
		}
		target := attempt.segments[len(attempt.segments)-1]
		path := filepath.Join(attemptDir, "frame.jpg")
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxFrameBytes {
			lastCode = "decode_failed"
			if attemptIndex+1 < len(attempts) {
				continue
			}
			return Frame{}, lastCode, false
		}
		width, height, imageHash, ok := inspectJPEG(path, info.Size())
		if !ok {
			lastCode = "decode_failed"
			if attemptIndex+1 < len(attempts) {
				continue
			}
			return Frame{}, lastCode, false
		}
		frame := makeFrame(track.ID, target, start, width, height, info.Size(), time.Now().UTC(), imageHash)
		data, readErr := os.ReadFile(path)
		if readErr != nil || len(data) == 0 || int64(len(data)) != info.Size() || len(data) > maxFrameBytes {
			lastCode = "decode_failed"
			if attemptIndex+1 < len(attempts) {
				continue
			}
			return Frame{}, lastCode, false
		}
		frame.imageData = data
		return frame, "", false
	}
	if lastCode == "" {
		lastCode = "decode_failed"
	}
	return Frame{}, lastCode, false
}

type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	n := len(data)
	if remaining := b.limit - b.Len(); remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = b.Buffer.Write(data)
	}
	return n, nil
}

func classifyFFmpegFailure(stderr string) (string, bool) {
	message := strings.ToLower(stderr)
	if strings.Contains(message, "matches no streams") || strings.Contains(message, "does not contain any stream") || strings.Contains(message, "no video stream") {
		return "no_video_stream", true
	}
	if strings.Contains(message, "unknown decoder") || strings.Contains(message, "decoder not found") || strings.Contains(message, "unsupported codec") {
		return "unsupported", true
	}
	return "decode_failed", false
}

func (s *Service) stageWindow(ctx context.Context, recordingID string, track *domain.Track, segments []domain.Segment, directory string) (map[string]string, error) {
	local := map[string]string{}
	initByID := make(map[string]domain.Segment, len(track.InitSegments))
	for _, init := range track.InitSegments {
		initByID[init.ID] = init
	}
	for _, segment := range segments {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if segment.InitSegmentID != "" {
			init, ok := initByID[segment.InitSegmentID]
			if !ok || init.StoragePath == "" {
				return nil, ErrInvalid
			}
			if _, staged := local[init.StoragePath]; !staged {
				name := fmt.Sprintf("init-%d.mp4", len(local)+1)
				if err := copyVerified(ctx, s.store, recordingID, init.StoragePath, name, init.PayloadSize, init.SHA256, directory); err != nil {
					return nil, err
				}
				local[init.StoragePath] = name
			}
		}
		if segment.StoragePath == "" {
			return nil, ErrInvalid
		}
		if _, staged := local[segment.StoragePath]; !staged {
			ext := safeSegmentExtension(segment.StoragePath, segment.InitSegmentID != "")
			name := fmt.Sprintf("segment-%d%s", len(local)+1, ext)
			if err := copyVerified(ctx, s.store, recordingID, segment.StoragePath, name, segment.PayloadSize, segment.SHA256, directory); err != nil {
				return nil, err
			}
			local[segment.StoragePath] = name
		}
	}
	return local, nil
}
