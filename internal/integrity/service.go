// Package integrity runs bounded, restart-safe verification jobs over
// point-in-time recording snapshots. It never changes canonical recordings.
package integrity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
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
	maxActiveJobs = 64
	maxJobHistory = 5000
	maxWorkers    = 4
	defaultWorker = 1
)

var (
	recordingIDRE = regexp.MustCompile(`^[a-f0-9]{32}$`)
	jobIDRE       = regexp.MustCompile(`^[a-f0-9]{32}$`)

	ErrNotFound = errors.New("integrity job not found")
	ErrCapacity = errors.New("integrity job capacity reached")
	ErrClosed   = errors.New("integrity service is closed")
)

type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCanceled  State = "canceled"
)

const (
	ErrorCanceled             = "canceled"
	ErrorInterruptedByRestart = "interrupted_by_restart"
	ErrorPersistenceFailed    = "persistence_failed"
	ErrorVerificationFailed   = "verification_failed"
)

// Job deliberately contains no path, URI, or raw error. ErrorCode is a
// closed set of stable values and Result comes from storage's redacted model.
type Job struct {
	ID                     string                   `json:"id"`
	RecordingID            string                   `json:"recording_id"`
	State                  State                    `json:"state"`
	CreatedAt              time.Time                `json:"created_at"`
	StartedAt              *time.Time               `json:"started_at,omitempty"`
	FinishedAt             *time.Time               `json:"finished_at,omitempty"`
	ErrorCode              string                   `json:"error_code,omitempty"`
	SourceArchiveRevision  uint64                   `json:"source_archive_revision,omitempty"`
	SourceTimelineRevision uint64                   `json:"source_timeline_revision,omitempty"`
	SourceRevisionKnown    bool                     `json:"source_revision_known,omitempty"`
	Result                 *storage.IntegrityResult `json:"result,omitempty"`
}

type diskState struct {
	Version         int                                `json:"version"`
	Jobs            []Job                              `json:"jobs"`
	Results         map[string]storage.IntegrityResult `json:"results"`
	ResultRevisions map[string]sourceRevision          `json:"result_revisions,omitempty"`
}

type sourceRevision struct {
	Archive  uint64 `json:"archive_revision"`
	Timeline uint64 `json:"timeline_revision"`
	Known    bool   `json:"known"`
}

type Freshness string

const (
	FreshnessUnknown Freshness = "unknown"
	FreshnessCurrent Freshness = "current"
	FreshnessStale   Freshness = "stale"
)

// ResultProjection binds a saved integrity result to the recording snapshot
// it verified. Freshness is computed only against an explicitly supplied
// current recording snapshot.
type ResultProjection struct {
	storage.IntegrityResult
	SourceArchiveRevision   uint64    `json:"source_archive_revision,omitempty"`
	SourceTimelineRevision  uint64    `json:"source_timeline_revision,omitempty"`
	CurrentArchiveRevision  uint64    `json:"current_archive_revision,omitempty"`
	CurrentTimelineRevision uint64    `json:"current_timeline_revision,omitempty"`
	RevisionKnown           bool      `json:"revision_known"`
	Freshness               Freshness `json:"freshness"`
}

// JobProjection adds an on-demand freshness comparison without persisting a
// derived current/stale state into the durable job record.
type JobProjection struct {
	Job
	CurrentArchiveRevision  uint64    `json:"current_archive_revision,omitempty"`
	CurrentTimelineRevision uint64    `json:"current_timeline_revision,omitempty"`
	Freshness               Freshness `json:"freshness"`
}

// ProjectJob compares a captured verification snapshot with an explicitly
// supplied current recording snapshot. Missing/legacy revisions remain
// unknown; freshness never changes verification job state.
func ProjectJob(job Job, current *domain.Recording) JobProjection {
	projection := JobProjection{Job: cloneJob(job), Freshness: FreshnessUnknown}
	if current == nil || current.ID != job.RecordingID || !job.SourceRevisionKnown {
		return projection
	}
	projection.CurrentArchiveRevision = current.ArchiveRevision
	projection.CurrentTimelineRevision = current.TimelineRevision
	projection.Freshness = FreshnessCurrent
	if job.SourceArchiveRevision != current.ArchiveRevision || job.SourceTimelineRevision != current.TimelineRevision {
		projection.Freshness = FreshnessStale
	}
	return projection
}

type jobControl struct {
	snapshot         *domain.Recording
	ctx              context.Context
	cancel           context.CancelFunc
	stopCall         func() bool
	previous         *storage.IntegrityResult
	previousRevision *sourceRevision
}

// Service owns a fixed worker set and a bounded queue. State transitions and
// persistence are serialized by mu; verification itself always runs unlocked.
type Service struct {
	mu              sync.Mutex
	dir             string
	statePath       string
	store           *storage.Store
	verify          func(context.Context, *domain.Recording) storage.IntegrityResult
	jobs            map[string]Job
	order           []string
	results         map[string]storage.IntegrityResult
	resultRevisions map[string]sourceRevision
	active          map[string]string // recording ID -> queued/running job ID
	controls        map[string]*jobControl
	queue           chan string
	ctx             context.Context
	cancel          context.CancelFunc
	workers         sync.WaitGroup
	done            chan struct{}
	closed          bool
	closeOnce       sync.Once
	closeErr        error
	lastErr         error
	running         int
	changed         chan struct{}
}

// Open loads persisted history, marks work left by a prior process as failed,
// and starts a fixed number of workers. concurrency is clamped to [1, 4].
func Open(root string, store *storage.Store, concurrency int) (*Service, error) {
	if store == nil {
		return nil, errors.New("storage is required")
	}
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("data root is required")
	}
	if concurrency <= 0 {
		concurrency = defaultWorker
	}
	if concurrency > maxWorkers {
		concurrency = maxWorkers
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, errors.New("integrity job directory unavailable")
	}
	if err = ensureRootDirectory(absRoot); err != nil {
		return nil, errors.New("integrity job directory unavailable")
	}
	managementDir := filepath.Join(absRoot, "management")
	if err = makePrivateChild(absRoot, "management"); err != nil {
		return nil, errors.New("integrity job directory unavailable")
	}
	jobDir := filepath.Join(managementDir, "integrity-jobs")
	if err = makePrivateChild(managementDir, "integrity-jobs"); err != nil {
		return nil, errors.New("integrity job directory unavailable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		dir:       jobDir,
		statePath: filepath.Join(jobDir, "state.json"),
		store:     store,
		verify: func(ctx context.Context, recording *domain.Recording) storage.IntegrityResult {
			return verifyRecording(ctx, store, recording)
		},
		jobs:            make(map[string]Job),
		results:         make(map[string]storage.IntegrityResult),
		resultRevisions: make(map[string]sourceRevision),
		active:          make(map[string]string),
		controls:        make(map[string]*jobControl),
		queue:           make(chan string, maxActiveJobs),
		changed:         make(chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
		done:            make(chan struct{}),
	}
	if err = s.load(); err != nil {
		cancel()
		return nil, err
	}
	if err = s.recoverInterrupted(); err != nil {
		cancel()
		return nil, errors.New("integrity job state could not be recovered")
	}
	for i := 0; i < concurrency; i++ {
		s.workers.Add(1)
		go s.worker()
	}
	go func() {
		s.workers.Wait()
		close(s.done)
	}()
	return s, nil
}

// Start queues verification of a deep-cloned snapshot. The supplied context
// controls this job's lifetime; callers that accept durable asynchronous work
// should pass a service-lifetime context rather than an HTTP request context.
func (s *Service) Start(ctx context.Context, recording *domain.Recording) (Job, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	if recording == nil || !recordingIDRE.MatchString(recording.ID) {
		return Job{}, errors.New("invalid recording")
	}
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		header, err := s.store.LoadRecordingHeader(ctx, recording.ID)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return Job{}, err
			}
			return Job{}, errors.New("invalid recording snapshot")
		}
		recording = header
	}
	snapshot, err := cloneRecording(recording)
	if err != nil {
		return Job{}, errors.New("invalid recording snapshot")
	}
	id, err := newID()
	if err != nil {
		return Job{}, errors.New("integrity job could not be created")
	}
	jobCtx, cancel := context.WithCancel(s.ctx)
	now := time.Now().UTC()
	job := Job{
		ID: id, RecordingID: snapshot.ID, State: StateQueued, CreatedAt: now,
		SourceArchiveRevision:  snapshot.ArchiveRevision,
		SourceTimelineRevision: snapshot.TimelineRevision,
		SourceRevisionKnown:    snapshot.ArchiveRevision != 0,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return Job{}, ErrClosed
	}
	if existingID := s.active[snapshot.ID]; existingID != "" {
		existing := cloneJob(s.jobs[existingID])
		s.mu.Unlock()
		cancel()
		return existing, nil
	}
	if len(s.active) >= maxActiveJobs {
		s.mu.Unlock()
		cancel()
		return Job{}, ErrCapacity
	}
	var prunedID string
	var prunedJob Job
	prunedIndex := -1
	if len(s.order) >= maxJobHistory {
		for i, candidateID := range s.order {
			if candidate, ok := s.jobs[candidateID]; ok && terminalState(candidate.State) {
				prunedID, prunedJob, prunedIndex = candidateID, candidate, i
				break
			}
		}
		if prunedIndex < 0 {
			s.mu.Unlock()
			cancel()
			return Job{}, ErrCapacity
		}
		delete(s.jobs, prunedID)
		s.order = append(s.order[:prunedIndex], s.order[prunedIndex+1:]...)
	}
	// At most 64 active jobs are admitted and the channel has the same bound.
	// Canceled entries may briefly remain as harmless queue tombstones; if they
	// fill the channel, reject this enqueue without mutating persistent state.
	select {
	case s.queue <- id:
	default:
		if prunedIndex >= 0 {
			s.jobs[prunedID] = prunedJob
			s.order = insertString(s.order, prunedIndex, prunedID)
		}
		s.mu.Unlock()
		cancel()
		return Job{}, ErrCapacity
	}
	s.jobs[id] = job
	s.order = append(s.order, id)
	s.active[snapshot.ID] = id
	s.notifyLocked()
	control := &jobControl{snapshot: snapshot, ctx: jobCtx, cancel: cancel}
	control.stopCall = context.AfterFunc(ctx, func() { s.cancelJob(id) })
	s.controls[id] = control
	if err = s.persistLocked(); err != nil {
		delete(s.jobs, id)
		s.order = s.order[:len(s.order)-1]
		delete(s.active, snapshot.ID)
		delete(s.controls, id)
		if prunedIndex >= 0 {
			s.jobs[prunedID] = prunedJob
			s.order = insertString(s.order, prunedIndex, prunedID)
		}
		control.stopCall()
		cancel()
		// The worker may already have received this ID. It will observe the
		// missing job after this lock is released and discard the queue item.
		s.mu.Unlock()
		return Job{}, errors.New("integrity job state could not be saved")
	}
	s.mu.Unlock()
	return cloneJob(job), nil
}

func (s *Service) Get(id string) (Job, error) {
	if !jobIDRE.MatchString(id) {
		return Job{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return cloneJob(job), nil
}

// Cancel requests cancellation of a queued or running verification. A terminal
// job is returned unchanged, making repeated cancellation requests idempotent.
func (s *Service) Cancel(id string) (Job, error) {
	if !jobIDRE.MatchString(id) {
		return Job{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	if !activeState(job.State) {
		return cloneJob(job), nil
	}
	s.cancelJobLocked(id, time.Now().UTC())
	job = s.jobs[id]
	if err := s.persistLocked(); err != nil {
		s.lastErr = errors.New("integrity job state could not be saved")
		return cloneJob(job), errors.New("integrity job cancellation could not be saved")
	}
	return cloneJob(job), nil
}

func (s *Service) Status(recordingID string) (storage.IntegrityResult, bool) {
	if !recordingIDRE.MatchString(recordingID) {
		return storage.IntegrityResult{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.results[recordingID]
	if jobID := s.active[recordingID]; jobID != "" {
		if ok {
			result = cloneResult(result)
		} else {
			result = storage.IntegrityResult{Issues: []storage.IntegrityIssue{}}
		}
		result.Status = storage.IntegrityVerifying
		return result, true
	}
	if ok {
		return cloneResult(result), true
	}
	for index := len(s.order) - 1; index >= 0; index-- {
		job, exists := s.jobs[s.order[index]]
		if exists && job.RecordingID == recordingID && job.State == StateFailed {
			return storage.IntegrityResult{Status: storage.IntegrityFailed, Issues: []storage.IntegrityIssue{}}, true
		}
	}
	return storage.IntegrityResult{}, false
}

// StatusFor returns the latest integrity projection together with freshness
// relative to an explicitly supplied current recording snapshot. Older
// persisted results that predate revision binding report FreshnessUnknown.
func (s *Service) StatusFor(recording *domain.Recording) (ResultProjection, bool) {
	if recording == nil || !recordingIDRE.MatchString(recording.ID) {
		return ResultProjection{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result, exists := s.results[recording.ID]
	var revision sourceRevision
	if exists {
		result = cloneResult(result)
		revision = s.resultRevisions[recording.ID]
	}
	if jobID := s.active[recording.ID]; jobID != "" {
		if exists {
			result.Status = storage.IntegrityVerifying
		} else {
			result = storage.IntegrityResult{Status: storage.IntegrityVerifying, Issues: []storage.IntegrityIssue{}}
			if job, ok := s.jobs[jobID]; ok && job.SourceRevisionKnown {
				revision = sourceRevision{Archive: job.SourceArchiveRevision, Timeline: job.SourceTimelineRevision, Known: true}
			}
		}
		return projectResult(result, revision, recording), true
	}
	if exists {
		return projectResult(result, revision, recording), true
	}
	for index := len(s.order) - 1; index >= 0; index-- {
		job, ok := s.jobs[s.order[index]]
		if ok && job.RecordingID == recording.ID && job.State == StateFailed {
			return projectResult(storage.IntegrityResult{Status: storage.IntegrityFailed, Issues: []storage.IntegrityIssue{}}, sourceRevision{}, recording), true
		}
	}
	return ResultProjection{}, false
}

func projectResult(result storage.IntegrityResult, source sourceRevision, current *domain.Recording) ResultProjection {
	projection := ResultProjection{
		IntegrityResult:         result,
		SourceArchiveRevision:   source.Archive,
		SourceTimelineRevision:  source.Timeline,
		CurrentArchiveRevision:  current.ArchiveRevision,
		CurrentTimelineRevision: current.TimelineRevision,
		RevisionKnown:           source.Known,
		Freshness:               FreshnessUnknown,
	}
	if source.Known {
		projection.Freshness = FreshnessCurrent
		if source.Archive != current.ArchiveRevision || source.Timeline != current.TimelineRevision {
			projection.Freshness = FreshnessStale
		}
	}
	return projection
}

// InProgress reports whether verification currently owns a queued/running job
// for the validated recording ID. It is useful to serialize deletion with a
// verification route's own recording lock.
func (s *Service) InProgress(recordingID string) bool {
	if !recordingIDRE.MatchString(recordingID) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active[recordingID] != ""
}

// WaitForIdle waits until every accepted queued or running verification has
// stopped mutating job state. It does not close the service or block later
// Start calls after idle has been observed.
func (s *Service) WaitForIdle(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if len(s.active) == 0 && s.running == 0 {
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

func (s *Service) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Service) finishRunning() {
	s.mu.Lock()
	if s.running > 0 {
		s.running--
	}
	s.notifyLocked()
	s.mu.Unlock()
}

// Close cancels queued/running jobs, persists terminal states, and joins all
// workers. If ctx expires first, Close returns ctx.Err while workers continue
// only until their current non-context-aware storage verification returns.
func (s *Service) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		for _, id := range s.active {
			s.cancelJobLocked(id, time.Now().UTC())
		}
		if err := s.persistLocked(); err != nil {
			s.lastErr = errors.New("integrity job state could not be saved")
		}
		s.cancel()
		s.mu.Unlock()
	})
	select {
	case <-s.done:
		s.mu.Lock()
		err := s.lastErr
		s.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) worker() {
	defer s.workers.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case id := <-s.queue:
			s.run(id)
		}
	}
}

func (s *Service) run(id string) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	control := s.controls[id]
	if !ok || job.State != StateQueued || control == nil {
		s.mu.Unlock()
		return
	}
	if control.ctx.Err() != nil || s.closed {
		s.cancelJobLocked(id, time.Now().UTC())
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	job.State = StateRunning
	job.StartedAt = timePointer(now)
	s.jobs[id] = job
	s.running++
	s.notifyLocked()
	defer s.finishRunning()
	if previous, ok := s.results[job.RecordingID]; ok {
		copy := cloneResult(previous)
		control.previous = &copy
		if revision, found := s.resultRevisions[job.RecordingID]; found {
			revisionCopy := revision
			control.previousRevision = &revisionCopy
		}
		previous.Status = storage.IntegrityVerifying
		s.results[job.RecordingID] = previous
	} else {
		s.results[job.RecordingID] = storage.IntegrityResult{Status: storage.IntegrityVerifying, Issues: []storage.IntegrityIssue{}}
	}
	if err := s.persistLocked(); err != nil {
		s.lastErr = errors.New("integrity job state could not be saved")
		s.failLocked(id, ErrorPersistenceFailed, control.previous)
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	snapshot := control.snapshot
	jobCtx := control.ctx
	s.mu.Unlock()

	result, panicked := safeVerify(s.verify, jobCtx, snapshot)

	s.mu.Lock()
	job, ok = s.jobs[id]
	if !ok || job.State != StateRunning {
		s.mu.Unlock()
		return
	}
	if jobCtx.Err() != nil || s.closed {
		s.cancelJobLocked(id, time.Now().UTC())
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	if panicked {
		s.failLocked(id, ErrorVerificationFailed, control.previous)
		if err := s.persistLocked(); err != nil {
			s.lastErr = errors.New("integrity job state could not be saved")
		}
		s.mu.Unlock()
		return
	}
	job.State = StateCompleted
	job.FinishedAt = timePointer(time.Now().UTC())
	job.ErrorCode = ""
	job.Result = resultPointer(result)
	s.jobs[id] = job
	s.results[job.RecordingID] = cloneResult(result)
	s.resultRevisions[job.RecordingID] = sourceRevision{
		Archive:  job.SourceArchiveRevision,
		Timeline: job.SourceTimelineRevision,
		Known:    job.SourceRevisionKnown,
	}
	s.finishLocked(id)
	if err := s.persistLocked(); err != nil {
		// Do not report an unpersisted result as durable. Restore the prior
		// result and record only a fixed failure code in memory if possible.
		if control.previous == nil {
			delete(s.results, job.RecordingID)
			delete(s.resultRevisions, job.RecordingID)
		} else {
			s.results[job.RecordingID] = cloneResult(*control.previous)
			if control.previousRevision == nil {
				delete(s.resultRevisions, job.RecordingID)
			} else {
				s.resultRevisions[job.RecordingID] = *control.previousRevision
			}
		}
		job.State = StateFailed
		job.ErrorCode = ErrorPersistenceFailed
		job.Result = nil
		s.jobs[id] = job
		s.lastErr = errors.New("integrity job state could not be saved")
		_ = s.persistLocked()
	}
	s.mu.Unlock()
}

func (s *Service) cancelJob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelJobLocked(id, time.Now().UTC()) {
		if err := s.persistLocked(); err != nil {
			s.lastErr = errors.New("integrity job state could not be saved")
		}
	}
}

func (s *Service) cancelJobLocked(id string, at time.Time) bool {
	job, ok := s.jobs[id]
	if !ok || !activeState(job.State) {
		return false
	}
	control := s.controls[id]
	if control != nil && job.State == StateRunning {
		if control.previous == nil {
			delete(s.results, job.RecordingID)
		} else {
			s.results[job.RecordingID] = cloneResult(*control.previous)
		}
	}
	job.State = StateCanceled
	job.FinishedAt = timePointer(at)
	job.ErrorCode = ErrorCanceled
	job.Result = nil
	s.jobs[id] = job
	s.finishLocked(id)
	return true
}

func (s *Service) failLocked(id, code string, previous *storage.IntegrityResult) {
	job, ok := s.jobs[id]
	if !ok {
		return
	}
	if previous == nil {
		delete(s.results, job.RecordingID)
	} else {
		s.results[job.RecordingID] = cloneResult(*previous)
	}
	job.State = StateFailed
	job.FinishedAt = timePointer(time.Now().UTC())
	job.ErrorCode = code
	job.Result = nil
	s.jobs[id] = job
	s.finishLocked(id)
}

func (s *Service) finishLocked(id string) {
	job := s.jobs[id]
	if s.active[job.RecordingID] == id {
		delete(s.active, job.RecordingID)
	}
	if control := s.controls[id]; control != nil {
		if control.stopCall != nil {
			control.stopCall()
		}
		control.cancel()
		delete(s.controls, id)
	}
	s.notifyLocked()
}

func (s *Service) load() error {
	info, statErr := os.Lstat(s.statePath)
	if statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("integrity job state is invalid")
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("integrity job state could not be read")
	}
	if statErr == nil {
		if err := os.Chmod(s.statePath, 0600); err != nil {
			return errors.New("integrity job state could not be secured")
		}
	}
	data, err := os.ReadFile(s.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("integrity job state could not be read")
	}
	var disk diskState
	if err = json.Unmarshal(data, &disk); err != nil || disk.Version != 1 || len(disk.Jobs) > maxJobHistory {
		return errors.New("integrity job state is invalid")
	}
	if disk.Results == nil {
		disk.Results = make(map[string]storage.IntegrityResult)
	}
	if disk.ResultRevisions == nil {
		disk.ResultRevisions = make(map[string]sourceRevision)
	}
	for _, job := range disk.Jobs {
		if !validJob(job) {
			return errors.New("integrity job state is invalid")
		}
		if _, duplicate := s.jobs[job.ID]; duplicate {
			return errors.New("integrity job state is invalid")
		}
		s.jobs[job.ID] = cloneJob(job)
		s.order = append(s.order, job.ID)
	}
	for recordingID, result := range disk.Results {
		if !recordingIDRE.MatchString(recordingID) || !validResult(result) {
			return errors.New("integrity job state is invalid")
		}
		s.results[recordingID] = cloneResult(result)
	}
	for recordingID, revision := range disk.ResultRevisions {
		if !recordingIDRE.MatchString(recordingID) || (revision.Known && revision.Archive == 0) {
			return errors.New("integrity job state is invalid")
		}
		if _, exists := s.results[recordingID]; !exists {
			return errors.New("integrity job state is invalid")
		}
		s.resultRevisions[recordingID] = revision
	}
	sort.SliceStable(s.order, func(i, j int) bool {
		left, right := s.jobs[s.order[i]], s.jobs[s.order[j]]
		if left.CreatedAt.Equal(right.CreatedAt) {
			return left.ID < right.ID
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})
	return nil
}

func (s *Service) recoverInterrupted() error {
	now := time.Now().UTC()
	changed := false
	for id, job := range s.jobs {
		if !activeState(job.State) {
			continue
		}
		job.State = StateFailed
		job.ErrorCode = ErrorInterruptedByRestart
		job.FinishedAt = timePointer(now)
		job.Result = nil
		s.jobs[id] = job
		if result, ok := s.results[job.RecordingID]; ok && result.Status == storage.IntegrityVerifying {
			result.Status = storage.IntegrityUnknown
			result.Issues = []storage.IntegrityIssue{}
			s.results[job.RecordingID] = result
		}
		changed = true
	}
	if changed {
		return s.persistLocked()
	}
	return nil
}

func (s *Service) persistLocked() error {
	jobs := make([]Job, 0, len(s.order))
	for _, id := range s.order {
		if job, ok := s.jobs[id]; ok {
			jobs = append(jobs, cloneJob(job))
		}
	}
	results := make(map[string]storage.IntegrityResult, len(s.results))
	for id, result := range s.results {
		results[id] = cloneResult(result)
	}
	resultRevisions := make(map[string]sourceRevision, len(s.resultRevisions))
	for id, revision := range s.resultRevisions {
		resultRevisions[id] = revision
	}
	data, err := json.Marshal(diskState{Version: 1, Jobs: jobs, Results: results, ResultRevisions: resultRevisions})
	if err != nil {
		return err
	}
	return atomicWrite(s.dir, s.statePath, data)
}

func atomicWrite(dir, path string, data []byte) error {
	file, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr = directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func ensureRootDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("not a directory")
	}
	return nil
}

func makePrivateChild(parent, name string) error {
	if filepath.Base(name) != name || name == "." || name == ".." {
		return errors.New("invalid private directory")
	}
	path := filepath.Join(parent, name)
	err := os.Mkdir(path, 0700)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("not a private directory")
	}
	return os.Chmod(path, 0700)
}

func cloneRecording(recording *domain.Recording) (*domain.Recording, error) {
	if recording == nil {
		return nil, errors.New("invalid recording")
	}
	// Format-v2 roots carry bounded summaries. Discard any expanded projections
	// a caller supplied so jobs retain only the root needed by shard iterators.
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		header := *recording
		header.Gaps = nil
		header.Snapshots = nil
		header.MetadataTimeline = nil
		header.Tracks = make(map[string]*domain.Track, len(recording.Tracks))
		for id, track := range recording.Tracks {
			if track == nil {
				header.Tracks[id] = nil
				continue
			}
			trackHeader := *track
			trackHeader.Segments = nil
			trackHeader.InitSegments = nil
			header.Tracks[id] = &trackHeader
		}
		recording = &header
	}
	data, err := json.Marshal(recording)
	if err != nil {
		return nil, err
	}
	var copy domain.Recording
	if err = json.Unmarshal(data, &copy); err != nil {
		return nil, err
	}
	if !recordingIDRE.MatchString(copy.ID) {
		return nil, errors.New("invalid recording id")
	}
	return &copy, nil
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func safeVerify(verify func(context.Context, *domain.Recording) storage.IntegrityResult, ctx context.Context, recording *domain.Recording) (result storage.IntegrityResult, panicked bool) {
	defer func() {
		if recover() != nil {
			result = storage.IntegrityResult{}
			panicked = true
		}
	}()
	result = verify(ctx, recording)
	if !validResult(result) {
		return storage.IntegrityResult{}, true
	}
	return cloneResult(result), false
}

func verifyRecording(ctx context.Context, store *storage.Store, recording *domain.Recording) storage.IntegrityResult {
	if recording != nil && recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		return verifyShardedRecording(ctx, store, recording)
	}
	return store.VerifyRecordingContext(ctx, recording)
}

var errVerificationCanceled = errors.New("integrity verification canceled")

// verifyShardedRecording walks bounded v2 metadata records and streams each
// payload through SHA-256. It retains no archive-sized object list.
func verifyShardedRecording(ctx context.Context, store *storage.Store, recording *domain.Recording) storage.IntegrityResult {
	if ctx == nil {
		ctx = context.Background()
	}
	result := storage.IntegrityResult{Status: storage.IntegrityVerified, LastVerifiedAt: time.Now().UTC(), Issues: []storage.IntegrityIssue{}}
	if store == nil || recording == nil || recording.FormatVersion != storage.ShardedArchiveFormatVersion || !recordingIDRE.MatchString(recording.ID) {
		result.Status = storage.IntegrityFailed
		result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "invalid_recording"})
		return result
	}

	canceled := false
	add := func(path, expectedPrefix string, expectedSize int64, expectedHash string) {
		if ctx.Err() != nil {
			canceled = true
			return
		}
		result.ObjectsTotal++
		issuePath := ""
		if validIntegrityReference(path, expectedPrefix) {
			issuePath = path
		} else {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "invalid_reference"})
			return
		}
		info, err := store.StatPayload(recording.ID, path)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, storage.ErrObjectNotFound) {
			result.ObjectsMissing++
			result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "missing_payload", Path: issuePath})
			return
		}
		if err != nil || !info.Regular || expectedSize < 0 {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "unavailable_payload", Path: issuePath})
			return
		}
		digest, err := hex.DecodeString(expectedHash)
		if err != nil || len(digest) != sha256.Size {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "invalid_integrity_metadata", Path: issuePath})
			return
		}
		reader, err := store.OpenPayloadReader(recording.ID, path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, storage.ErrObjectNotFound) {
				result.ObjectsMissing++
				result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "missing_payload", Path: issuePath})
			} else {
				result.ObjectsCorrupt++
				result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "unavailable_payload", Path: issuePath})
			}
			return
		}
		hash := sha256.New()
		size, copyErr := io.Copy(hash, integrityContextReader{ctx: ctx, reader: reader})
		closeErr := reader.Close()
		if ctx.Err() != nil || errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			canceled = true
			return
		}
		if copyErr != nil || closeErr != nil || size != expectedSize || !equalIntegrityDigest(hash.Sum(nil), digest) {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "payload_mismatch", Path: issuePath})
			return
		}
		result.ObjectsVerified++
	}

	visitManifest := func(snapshot domain.ManifestSnapshot) error {
		if err := ctx.Err(); err != nil || canceled {
			return errVerificationCanceled
		}
		add(snapshot.StoragePath, "manifests/", snapshot.Size, snapshot.SHA256)
		if ctx.Err() != nil || canceled {
			return errVerificationCanceled
		}
		return nil
	}
	if err := store.IterateShardedManifests(ctx, recording.ID, visitManifest); err != nil {
		return incompleteShardedResult(result, ctx, err)
	}

	trackIDs := make([]string, 0, len(recording.Tracks))
	for id := range recording.Tracks {
		trackIDs = append(trackIDs, id)
	}
	sort.Strings(trackIDs)
	for _, trackID := range trackIDs {
		track := recording.Tracks[trackID]
		if track == nil || track.ID != trackID {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, storage.IntegrityIssue{Code: "invalid_track_metadata"})
			continue
		}
		visit := func(record storage.V2MediaRecord) error {
			if err := ctx.Err(); err != nil || canceled {
				return errVerificationCanceled
			}
			segment := record.Segment
			add(segment.StoragePath, "tracks/", segment.PayloadSize, segment.SHA256)
			if ctx.Err() != nil || canceled {
				return errVerificationCanceled
			}
			return nil
		}
		if err := store.IterateShardedMedia(ctx, recording.ID, trackID, visit); err != nil {
			return incompleteShardedResult(result, ctx, err)
		}
		if err := store.IterateShardedInitSegments(ctx, recording.ID, trackID, visit); err != nil {
			return incompleteShardedResult(result, ctx, err)
		}
	}
	if len(result.Issues) > 0 {
		result.Status = storage.IntegrityDegraded
	}
	return result
}

func incompleteShardedResult(result storage.IntegrityResult, ctx context.Context, err error) storage.IntegrityResult {
	if errors.Is(err, errVerificationCanceled) || ctx.Err() != nil {
		return storage.IntegrityResult{Status: storage.IntegrityUnknown, Issues: []storage.IntegrityIssue{}}
	}
	result.Status = storage.IntegrityFailed
	return result
}

func validIntegrityReference(path, prefix string) bool {
	if !strings.HasPrefix(path, prefix) || len(path) <= len(prefix) || filepath.IsAbs(path) || filepath.VolumeName(path) != "" || strings.Contains(path, "\\") || strings.ContainsRune(path, '\x00') {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	return clean == path && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

type integrityContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r integrityContextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func equalIntegrityDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func validJob(job Job) bool {
	if !jobIDRE.MatchString(job.ID) || !recordingIDRE.MatchString(job.RecordingID) || job.CreatedAt.IsZero() {
		return false
	}
	if job.SourceRevisionKnown && job.SourceArchiveRevision == 0 {
		return false
	}
	switch job.State {
	case StateQueued, StateRunning, StateCompleted, StateFailed, StateCanceled:
	default:
		return false
	}
	switch job.ErrorCode {
	case "", ErrorCanceled, ErrorInterruptedByRestart, ErrorPersistenceFailed, ErrorVerificationFailed:
	default:
		return false
	}
	if job.Result != nil && !validResult(*job.Result) {
		return false
	}
	return true
}

func validResult(result storage.IntegrityResult) bool {
	switch result.Status {
	case storage.IntegrityUnknown, storage.IntegrityVerifying, storage.IntegrityVerified, storage.IntegrityDegraded, storage.IntegrityFailed:
	default:
		return false
	}
	if result.ObjectsTotal < 0 || result.ObjectsVerified < 0 || result.ObjectsMissing < 0 || result.ObjectsCorrupt < 0 {
		return false
	}
	for _, issue := range result.Issues {
		if !validIssueCode(issue.Code) || !safeRelativeIssuePath(issue.Path) {
			return false
		}
	}
	return true
}

func safeRelativeIssuePath(path string) bool {
	if path == "" {
		return true
	}
	if filepath.IsAbs(path) || filepath.VolumeName(path) != "" || strings.Contains(path, "\\") || strings.ContainsRune(path, '\x00') || (!strings.HasPrefix(path, "tracks/") && !strings.HasPrefix(path, "manifests/")) {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	return clean == path && clean != "." && !strings.HasPrefix(clean, "../") && clean != ".."
}

func validIssueCode(code string) bool {
	switch code {
	case "invalid_recording", "invalid_reference", "missing_payload", "unavailable_payload", "invalid_integrity_metadata", "payload_mismatch", "invalid_track_metadata":
		return true
	default:
		return false
	}
}

func activeState(state State) bool { return state == StateQueued || state == StateRunning }

func terminalState(state State) bool { return !activeState(state) }

func cloneJob(job Job) Job {
	if job.StartedAt != nil {
		job.StartedAt = timePointer(*job.StartedAt)
	}
	if job.FinishedAt != nil {
		job.FinishedAt = timePointer(*job.FinishedAt)
	}
	if job.Result != nil {
		job.Result = resultPointer(*job.Result)
	}
	return job
}

func cloneResult(result storage.IntegrityResult) storage.IntegrityResult {
	result.Issues = append([]storage.IntegrityIssue(nil), result.Issues...)
	return result
}

func resultPointer(result storage.IntegrityResult) *storage.IntegrityResult {
	copy := cloneResult(result)
	return &copy
}

func timePointer(value time.Time) *time.Time { return &value }

func insertString(values []string, index int, value string) []string {
	values = append(values, "")
	copy(values[index+1:], values[index:])
	values[index] = value
	return values
}
