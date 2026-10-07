package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"
)

const (
	ingestAllocationChunk        int64 = 64 << 10
	DefaultMaxIngestPayloadBytes int64 = 512 << 20
	// MaxIngestObjects bounds completed objects awaiting persistence.
	MaxIngestObjects = 128
	// DefaultIngestGlobalBytes bounds reservations across the process. This is
	// 1 GiB: it can hold two maximum-sized (512 MiB) response bodies.
	DefaultIngestGlobalBytes int64 = 1 << 30
	// DefaultIngestPerRecordingBytes accounts for both the old and new backing
	// arrays while a maximum-sized payload grows from 256 MiB to 512 MiB.
	DefaultIngestPerRecordingBytes       int64 = 768 << 20
	DefaultIngestWriters                       = 1
	DefaultPersistAttempts                     = 5
	DefaultRetryInitialBackoff                 = 100 * time.Millisecond
	DefaultRetryMaxBackoff                     = 800 * time.Millisecond
	DefaultPoolSampleInterval                  = 5 * time.Second
	DefaultPoolMetricsRetention                = 24 * time.Hour
	runtimeResourceReleaseTimeout              = 3 * time.Second
	runtimeResourceOperationTimeout            = 3 * time.Second
	runtimeResourceReleaseAttemptTimeout       = time.Second
	runtimeResourceOperationAttempts           = 3
	runtimeResourceRetryBackoff                = 20 * time.Millisecond
	runtimeResourceOperationRetryBackoff       = 50 * time.Millisecond
	runtimeResourceReleaseRetryBase            = 100 * time.Millisecond
	runtimeResourceReleaseRetryMaximum         = 5 * time.Second
	runtimeResourceReleaseRetryBatch           = 8
	MaxIngestGlobalBytes                 int64 = 2 << 30
	MaxIngestPerRecordingBytes           int64 = 1536 << 20
	MaxIngestPayloadBytes                int64 = 1 << 30
)

var (
	ErrIngestClosed                 = errors.New("storage ingest service is closed")
	ErrIngestTooLarge               = errors.New("ingest payload exceeds size limit")
	ErrIngestReservation            = errors.New("ingest payload exceeded its reserved byte budget")
	ErrIngestSizeMismatch           = ErrPayloadSizeMismatch
	ErrCanonicalCommitFailed        = errors.New("canonical storage commit failed")
	ErrIngestCoordinatorUnavailable = errors.New("storage coordinator unavailable")
)

type canonicalCommitFailure struct {
	attempts int
	jobKind  IngestJobKind
	cause    error
}

type attemptCountedError interface {
	error
	Attempts() int
}

func (e *canonicalCommitFailure) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("canonical storage commit failed after bounded retries (%d attempts): persistence callback returned no error", e.attempts)
	}
	return fmt.Sprintf("canonical storage commit failed after bounded retries (%d attempts): %v", e.attempts, e.cause)
}

func (e *canonicalCommitFailure) Is(target error) bool {
	return target == ErrCanonicalCommitFailed
}

func (e *canonicalCommitFailure) Unwrap() error { return e.cause }

func (e *canonicalCommitFailure) Attempts() int { return e.attempts }

func (e *canonicalCommitFailure) CurrentJobKind() IngestJobKind { return e.jobKind }

func (e *canonicalCommitFailure) FirstFailureJobKind() IngestJobKind { return e.jobKind }

func (e *canonicalCommitFailure) CurrentAttempts() int { return e.attempts }

func (e *canonicalCommitFailure) FirstFailureAttempts() int { return e.attempts }

// IngestJobKind identifies bounded canonical work classes for diagnostics.
// Values describe Core-owned archive operations, not source platforms.
type IngestJobKind string

const (
	IngestJobKindCanonicalPayload  IngestJobKind = "canonical_payload"
	IngestJobKindMediaPayload      IngestJobKind = "media_payload"
	IngestJobKindInitPayload       IngestJobKind = "init_payload"
	IngestJobKindHistoricalMedia   IngestJobKind = "historical_media"
	IngestJobKindHistoricalInit    IngestJobKind = "historical_init"
	IngestJobKindManifestSnapshot  IngestJobKind = "manifest_snapshot"
	IngestJobKindRecordingMetadata IngestJobKind = "recording_metadata"
)

func (kind IngestJobKind) valid() bool {
	switch kind {
	case IngestJobKindCanonicalPayload, IngestJobKindMediaPayload,
		IngestJobKindInitPayload, IngestJobKindHistoricalMedia,
		IngestJobKindHistoricalInit, IngestJobKindManifestSnapshot,
		IngestJobKindRecordingMetadata:
		return true
	default:
		return false
	}
}

// IngestFailureDetails exposes bounded job identity and retry counts without
// exposing persistence error text or backend details.
type IngestFailureDetails interface {
	CurrentJobKind() IngestJobKind
	FirstFailureJobKind() IngestJobKind
	CurrentAttempts() int
	FirstFailureAttempts() int
}

type ingestFailureRecord struct {
	kind     IngestJobKind
	err      error
	attempts int
}

type poisonedIngestFailure struct {
	currentKind IngestJobKind
	first       ingestFailureRecord
}

func (e *poisonedIngestFailure) Error() string { return ErrCanonicalCommitFailed.Error() }

func (e *poisonedIngestFailure) Is(target error) bool {
	return target == ErrCanonicalCommitFailed
}

func (e *poisonedIngestFailure) Unwrap() error { return e.first.err }

// Attempts reports attempts for this queued job. Poisoned jobs never persist.
func (e *poisonedIngestFailure) Attempts() int { return 0 }

func (e *poisonedIngestFailure) CurrentJobKind() IngestJobKind { return e.currentKind }

func (e *poisonedIngestFailure) FirstFailureJobKind() IngestJobKind { return e.first.kind }

func (e *poisonedIngestFailure) CurrentAttempts() int { return 0 }

func (e *poisonedIngestFailure) FirstFailureAttempts() int { return e.first.attempts }

type ingestWriterAcquireFailure struct {
	kind  IngestJobKind
	cause error
}

func (e *ingestWriterAcquireFailure) Error() string { return "global storage writer is unavailable" }

func (e *ingestWriterAcquireFailure) Is(target error) bool {
	return target == ErrIngestCoordinatorUnavailable
}

func (e *ingestWriterAcquireFailure) Unwrap() error { return e.cause }

func (e *ingestWriterAcquireFailure) Attempts() int { return 0 }

func (e *ingestWriterAcquireFailure) CurrentJobKind() IngestJobKind { return e.kind }

func (e *ingestWriterAcquireFailure) FirstFailureJobKind() IngestJobKind { return e.kind }

func (e *ingestWriterAcquireFailure) CurrentAttempts() int { return 0 }

func (e *ingestWriterAcquireFailure) FirstFailureAttempts() int { return 0 }

type ingestCoordinatorFailure struct {
	operation string
	cause     error
}

func (e *ingestCoordinatorFailure) Error() string {
	return "storage coordinator " + e.operation + " failed"
}

func (e *ingestCoordinatorFailure) Is(target error) bool {
	return target == ErrIngestCoordinatorUnavailable
}

func (e *ingestCoordinatorFailure) Unwrap() error { return e.cause }

func coordinatorFailure(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	return &ingestCoordinatorFailure{operation: operation, cause: cause}
}

func callCoordinator(ctx context.Context, operation string, call func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	for attempt := 0; attempt < runtimeResourceOperationAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return coordinatorFailure(operation, err)
		}
		callCtx, cancel := context.WithTimeout(ctx, runtimeResourceOperationTimeout)
		lastErr = call(callCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil || attempt+1 == runtimeResourceOperationAttempts {
			break
		}
		if !waitCoordinatorOperationRetry(ctx, attempt) {
			lastErr = ctx.Err()
			break
		}
	}
	return coordinatorFailure(operation, lastErr)
}

func callCoordinatorRelease(operation string, call func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), runtimeResourceReleaseTimeout)
	defer cancel()
	return callCoordinatorReleaseContext(ctx, operation, call)
}

func callCoordinatorReleaseContext(ctx context.Context, operation string, call func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	for attempt := 0; attempt < runtimeResourceOperationAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			break
		}
		callCtx, callCancel := context.WithTimeout(ctx, runtimeResourceReleaseAttemptTimeout)
		lastErr = call(callCtx)
		callCancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil || attempt+1 == runtimeResourceOperationAttempts {
			break
		}
		if !waitCoordinatorRetry(ctx, attempt) {
			lastErr = ctx.Err()
			break
		}
	}
	return coordinatorFailure(operation, lastErr)
}

func waitCoordinatorRetry(ctx context.Context, attempt int) bool {
	delay := runtimeResourceRetryBackoff * time.Duration(attempt+1)
	return waitCoordinatorDelay(ctx, delay)
}

func waitCoordinatorOperationRetry(ctx context.Context, attempt int) bool {
	delay := runtimeResourceOperationRetryBackoff * time.Duration(attempt+1)
	return waitCoordinatorDelay(ctx, delay)
}

func waitCoordinatorDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// RuntimeIngestCoordinator is the transport-neutral process-wide accounting
// contract used when multiple recorder-engine generations share one Runtime
// Host. A nil coordinator preserves the original single-process limits.
// Implementations must make acquisitions idempotent by lease ID and bound the
// duration of each transport operation.
type RuntimeIngestCoordinator interface {
	SetReservation(context.Context, string, string, int64) error
	ReleaseReservation(context.Context, string, string) error
	AcquireQueue(context.Context, string) error
	ReleaseQueue(context.Context, string) error
	AcquireWriter(context.Context, string) error
	ReleaseWriter(context.Context, string) error
}

type IngestOptions struct {
	QueueObjects      int
	GlobalBytes       int64
	PerRecordingBytes int64
	MaxPayloadBytes   int64
	Writers           int
	PersistAttempts   int
	RetryBase         time.Duration
	RetryMaxBackoff   time.Duration
	SampleInterval    time.Duration
	MetricsRetention  time.Duration
}

func DefaultIngestOptions() IngestOptions {
	return IngestOptions{
		QueueObjects: MaxIngestObjects, GlobalBytes: DefaultIngestGlobalBytes,
		PerRecordingBytes: DefaultIngestPerRecordingBytes, MaxPayloadBytes: DefaultMaxIngestPayloadBytes,
		Writers: DefaultIngestWriters, PersistAttempts: DefaultPersistAttempts,
		RetryBase: DefaultRetryInitialBackoff, RetryMaxBackoff: DefaultRetryMaxBackoff,
		SampleInterval: DefaultPoolSampleInterval, MetricsRetention: DefaultPoolMetricsRetention,
	}
}

// ValidateIngestOptions applies public operational bounds. The byte ceilings
// are twice the current production defaults, permitting one step of capacity
// tuning while capping volatile payload memory at 2 GiB process-wide, 1.5 GiB
// per recording, and 1 GiB per source object. Per-recording/global budgets
// must also admit the largest transient old+new slice allocation used by
// ReadPayload.
func ValidateIngestOptions(options IngestOptions) error {
	if options.GlobalBytes <= 0 || options.GlobalBytes > MaxIngestGlobalBytes {
		return errors.New("global buffer limit must be greater than zero and at most 2 GiB")
	}
	if options.PerRecordingBytes <= 0 || options.PerRecordingBytes > MaxIngestPerRecordingBytes {
		return errors.New("per-recording buffer limit must be greater than zero and at most 1536 MiB")
	}
	if options.MaxPayloadBytes <= 0 || options.MaxPayloadBytes > MaxIngestPayloadBytes {
		return errors.New("maximum payload size must be greater than zero and at most 1 GiB")
	}
	if options.PerRecordingBytes > options.GlobalBytes {
		return errors.New("per-recording buffer limit cannot exceed the global buffer limit")
	}
	if options.MaxPayloadBytes > options.PerRecordingBytes {
		return errors.New("maximum payload size cannot exceed the per-recording buffer limit")
	}
	peak := maxReallocationPeak(options.MaxPayloadBytes)
	if options.GlobalBytes < peak || options.PerRecordingBytes < peak {
		return errors.New("global and per-recording buffer limits must each fit the maximum payload's temporary reallocation peak")
	}
	if options.QueueObjects < 1 || options.QueueObjects > MaxIngestObjects {
		return errors.New("pending storage queue capacity must be between 1 and 128 objects")
	}
	if options.Writers != 1 {
		return errors.New("writer concurrency must be 1 to preserve canonical commit ordering")
	}
	if options.PersistAttempts < 1 || options.PersistAttempts > 10 {
		return errors.New("storage persist attempts must be between 1 and 10")
	}
	if options.RetryBase < 10*time.Millisecond || options.RetryBase > 30*time.Second {
		return errors.New("initial storage retry backoff must be between 10 ms and 30 s")
	}
	if options.RetryMaxBackoff < options.RetryBase || options.RetryMaxBackoff > 5*time.Minute {
		return errors.New("maximum storage retry backoff must be at least the initial backoff and at most 5 minutes")
	}
	if options.SampleInterval < time.Second || options.SampleInterval > time.Hour {
		return errors.New("storage metrics sampling interval must be between 1 s and 1 hour")
	}
	minimumRetention := MinimumMetricsRetention(options.SampleInterval)
	if options.MetricsRetention < minimumRetention || options.MetricsRetention > 24*time.Hour {
		return fmt.Errorf("storage metrics retention must be at least %s for a sampling interval of %s and at most 24 hours", compactDuration(minimumRetention), compactDuration(options.SampleInterval))
	}
	return nil
}

func compactDuration(value time.Duration) string {
	if value%time.Hour == 0 {
		return englishCount(value/time.Hour, "hour")
	}
	if value%time.Minute == 0 {
		hours, minutes := value/time.Hour, (value%time.Hour)/time.Minute
		if hours > 0 && minutes > 0 {
			return englishCount(hours, "hour") + " " + englishCount(minutes, "minute")
		}
		if hours > 0 {
			return englishCount(hours, "hour")
		}
		return englishCount(minutes, "minute")
	}
	if value%time.Second == 0 {
		hours, minutes, seconds := value/time.Hour, (value%time.Hour)/time.Minute, (value%time.Minute)/time.Second
		if hours > 0 {
			result := englishCount(hours, "hour")
			if minutes > 0 {
				result += " " + englishCount(minutes, "minute")
			}
			if seconds > 0 {
				result += " " + englishCount(seconds, "second")
			}
			return result
		}
		if minutes > 0 && seconds > 0 {
			return englishCount(minutes, "minute") + " " + englishCount(seconds, "second")
		}
		if minutes > 0 {
			return englishCount(minutes, "minute")
		}
		return englishCount(seconds, "second")
	}
	return value.String()
}

func englishCount(count time.Duration, unit string) string {
	if count != 1 {
		unit += "s"
	}
	return fmt.Sprintf("%d %s", count, unit)
}

type IngestSnapshot struct {
	BufferCapacityBytes       int64   `json:"buffer_capacity_bytes"`
	PerRecordingCapacityBytes int64   `json:"per_recording_capacity_bytes"`
	BufferUsedBytes           int64   `json:"buffer_used_bytes"`
	ReservedBytes             int64   `json:"reserved_bytes"`
	QueueObjects              int     `json:"queue_objects"`
	QueueBytes                int64   `json:"queue_bytes"`
	OldestPersistAgeSeconds   float64 `json:"oldest_persist_age_seconds"`
	ActiveWriters             int     `json:"active_writers"`
	WriterConcurrency         int     `json:"writer_concurrency"`
	StorageErrorsTotal        uint64  `json:"storage_errors_total"`
	CoordinatorErrorsTotal    uint64  `json:"coordinator_errors_total"`
	PendingCoordinatorLeases  int     `json:"pending_coordinator_leases"`
}

type IngestService struct {
	store   *Store
	options IngestOptions
	global  RuntimeIngestCoordinator
	jobs    chan *ingestJob
	slots   chan struct{}

	mu                         sync.Mutex
	closed                     bool
	changed                    chan struct{}
	overflowing                bool
	reservedGlobal             int64
	reservedByRecording        map[string]int64
	usedBytes                  int64
	queuedBytes                int64
	queuedObjects              int
	oldestQueued               time.Time
	queueTimes                 map[*ingestJob]time.Time
	activeWriters              int
	activeSubmits              int
	storageErrors              uint64
	coordinatorErrors          uint64
	pendingCoordinatorReleases map[string]pendingCoordinatorRelease
	failedRecordings           map[string]ingestFailureRecord
	// reallocationHook is a deterministic test seam for observing transient
	// old+new backing-array reservations. Production leaves it nil.
	reallocationHook func(oldCapacity, newCapacity int64)

	workers          sync.WaitGroup
	closeOnce        sync.Once
	closeDone        chan struct{}
	samplerStop      chan struct{}
	samplerDone      chan struct{}
	releaseRetryStop chan struct{}
	releaseRetryDone chan struct{}
}

type storageErrorRecorder interface{ recordStorageError() }

type ingestJob struct {
	recordingID   string
	kind          IngestJobKind
	queueLease    string
	writerLease   string
	payload       *IngestPayload
	persist       func([]byte) (PayloadResult, error)
	complete      func(PayloadResult, error)
	queuedAt      time.Time
	queueRelease  leaseReleaseState
	writerRelease leaseReleaseState
}

// leaseReleaseState serializes release attempts for one idempotent remote
// lease. A failed attempt returns the state to unreleased, so a later call can
// retry the same ID. Concurrent callers wait for the active bounded attempt.
type leaseReleaseState struct {
	mu        sync.Mutex
	releasing bool
	released  bool
	done      chan struct{}
}

type pendingCoordinatorRelease struct {
	leaseID    string
	operation  string
	state      *leaseReleaseState
	call       func(context.Context) error
	onResolved func()
	attempts   int
	next       time.Time
}

func (s *leaseReleaseState) release(run func() error) error {
	return s.releaseContext(context.Background(), run)
}

func (s *leaseReleaseState) releaseContext(ctx context.Context, run func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		s.mu.Lock()
		if s.released {
			s.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.releasing {
			done := s.done
			s.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		s.releasing = true
		s.done = make(chan struct{})
		done := s.done
		s.mu.Unlock()

		err := run()

		s.mu.Lock()
		s.releasing = false
		s.released = err == nil
		close(done)
		s.mu.Unlock()
		return err
	}
}

func (s *leaseReleaseState) reset() {
	s.mu.Lock()
	if !s.releasing {
		s.released = false
	}
	s.mu.Unlock()
}

func (j *ingestJob) payloadBytes() int64 {
	if j == nil || j.payload == nil {
		return 0
	}
	return int64(len(j.payload.data))
}

// IngestPayload owns a bounded volatile response body. It is not canonical
// data; the archive becomes aware of bytes only after the persist callback
// finishes all durable metadata ordering.
type IngestPayload struct {
	service        *IngestService
	recordingID    string
	data           []byte
	result         PayloadResult
	reserved       int64
	used           int64
	reservationID  string
	globalReserved int64
	once           sync.Once
	globalRelease  leaseReleaseState
	reservationMu  sync.Mutex
}

func (p *IngestPayload) Result() PayloadResult { return p.result }
func (p *IngestPayload) Bytes() []byte         { return p.data }

// Release returns all accounting exactly once. Submit transfers ownership to
// the service; callers release only when Submit fails.
func (p *IngestPayload) Release() {
	if p == nil || p.service == nil {
		return
	}
	p.service.releasePayload(p)
}

func NewIngestService(store *Store, options IngestOptions) (*IngestService, error) {
	return newIngestService(store, options, nil)
}

func newIngestService(store *Store, options IngestOptions, global RuntimeIngestCoordinator) (*IngestService, error) {
	if store == nil {
		return nil, errors.New("archive store is required")
	}
	defaults := DefaultIngestOptions()
	if options.QueueObjects <= 0 {
		options.QueueObjects = defaults.QueueObjects
	}
	if options.GlobalBytes <= 0 {
		options.GlobalBytes = defaults.GlobalBytes
	}
	if options.PerRecordingBytes <= 0 {
		options.PerRecordingBytes = defaults.PerRecordingBytes
	}
	if options.MaxPayloadBytes <= 0 {
		options.MaxPayloadBytes = defaults.MaxPayloadBytes
	}
	if options.Writers <= 0 {
		options.Writers = defaults.Writers
	}
	if options.PersistAttempts <= 0 {
		options.PersistAttempts = defaults.PersistAttempts
	}
	if options.RetryBase <= 0 {
		options.RetryBase = defaults.RetryBase
	}
	if options.RetryMaxBackoff <= 0 {
		options.RetryMaxBackoff = defaults.RetryMaxBackoff
	}
	if options.SampleInterval <= 0 {
		options.SampleInterval = defaults.SampleInterval
	}
	if options.MetricsRetention <= 0 {
		options.MetricsRetention = defaults.MetricsRetention
	}
	if err := ValidateIngestOptions(options); err != nil || options.Writers > options.QueueObjects {
		return nil, errors.New("invalid bounded ingest configuration")
	}
	if backend, ok := store.StorageBackend.(*LocalFilesystemBackend); ok {
		backend.telemetry.configure(options.SampleInterval, options.MetricsRetention)
	}
	s := &IngestService{store: store, options: options, global: global, jobs: make(chan *ingestJob, options.QueueObjects), slots: make(chan struct{}, options.QueueObjects), changed: make(chan struct{}), reservedByRecording: map[string]int64{}, queueTimes: map[*ingestJob]time.Time{}, failedRecordings: map[string]ingestFailureRecord{}, pendingCoordinatorReleases: map[string]pendingCoordinatorRelease{}, closeDone: make(chan struct{}), samplerStop: make(chan struct{}), samplerDone: make(chan struct{}), releaseRetryStop: make(chan struct{}), releaseRetryDone: make(chan struct{})}
	for i := 0; i < options.Writers; i++ {
		s.workers.Add(1)
		go s.writer()
	}
	go s.sampleLoop()
	if global != nil {
		go s.releaseRetryLoop()
	} else {
		close(s.releaseRetryDone)
	}
	return s, nil
}

func newIngestLeaseID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", errors.New("ingest lease identity unavailable")
	}
	return "i-" + hex.EncodeToString(value[:]), nil
}

// ReadPayload reserves an advertised size as an allocation hint and otherwise
// reserves incrementally as the backing array grows. Content-Length is only
// an admission hint; max and exact-size checks remain authoritative.
func (s *IngestService) ReadPayload(ctx context.Context, recordingID string, src io.Reader, max, expectedSize, reservationHint int64) (*IngestPayload, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if src == nil || max <= 0 || expectedSize > max || expectedSize < -1 {
		return nil, errors.New("invalid ingest payload request")
	}
	if max > s.options.MaxPayloadBytes {
		return nil, ErrIngestTooLarge
	}
	reserve := reservationHint
	if expectedSize >= 0 {
		reserve = expectedSize
	}
	if reserve > max {
		return nil, ErrIngestTooLarge
	}
	// Unknown-length bodies reserve as their backing array grows. A declared
	// size is only a hint and is converted to the same geometric capacity used
	// by the allocation path.
	reserve = allocationCapacity(reserve, max)
	payload := &IngestPayload{service: s, recordingID: recordingID}
	if s.global != nil {
		leaseID, err := newIngestLeaseID()
		if err != nil {
			return nil, err
		}
		payload.reservationID = leaseID
	}
	if err := s.reserve(ctx, payload, reserve); err != nil {
		payload.Release()
		return nil, err
	}
	hash := sha256.New()
	buf := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			payload.Release()
			return nil, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if int64(len(payload.data))+int64(n) > max {
				payload.Release()
				return nil, ErrIngestTooLarge
			}
			if expectedSize >= 0 && int64(len(payload.data))+int64(n) > expectedSize {
				payload.Release()
				return nil, ErrIngestSizeMismatch
			}
			required := int64(len(payload.data)) + int64(n)
			allocation := allocationCapacity(required, max)
			oldCapacity := int64(cap(payload.data))
			if allocation > oldCapacity {
				// A growth temporarily retains both arrays. Account for the full
				// new allocation while the old reservation is still held. The
				// exclusive lease prevents partial buffers from deadlocking while
				// competing streams wait on the same remaining budget.
				additional := allocation
				if oldCapacity == 0 {
					additional = allocation - payload.reserved
					if additional < 0 {
						additional = 0
					}
				}
				needsLease := additional > 0 && payload.reserved > 0
				if needsLease {
					if err := s.acquireReallocationLease(); err != nil {
						payload.Release()
						return nil, err
					}
				}
				growthErr := func() error {
					if needsLease {
						defer s.releaseReallocationLease()
					}
					if err := s.reserve(ctx, payload, additional); err != nil {
						return err
					}
					if s.reallocationHook != nil {
						s.reallocationHook(oldCapacity, allocation)
					}
					grown := make([]byte, len(payload.data), int(allocation))
					copy(grown, payload.data)
					payload.data = grown
					if oldCapacity > 0 {
						payload.reserved -= oldCapacity
						s.releasePayloadReservation(payload, oldCapacity)
					}
					// A small or inaccurate Content-Length hint can reserve more
					// than the new array. Shrink after allocation succeeds.
					if excess := payload.reserved - allocation; excess > 0 {
						payload.reserved -= excess
						s.releasePayloadReservation(payload, excess)
					}
					return nil
				}()
				if growthErr != nil {
					payload.Release()
					return nil, growthErr
				}
			}
			payload.data = append(payload.data, buf[:n]...)
			_, _ = hash.Write(buf[:n])
			payload.used += int64(n)
			s.addUsed(int64(n))
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			payload.Release()
			return nil, readErr
		}
		if n == 0 {
			payload.Release()
			return nil, io.ErrNoProgress
		}
	}
	if expectedSize >= 0 && int64(len(payload.data)) != expectedSize {
		payload.Release()
		return nil, fmt.Errorf("%w: got %d bytes; expected %d", ErrIngestSizeMismatch, len(payload.data), expectedSize)
	}
	allocated := int64(cap(payload.data))
	if allocated < payload.reserved {
		excess := payload.reserved - allocated
		payload.reserved = allocated
		s.releasePayloadReservation(payload, excess)
	}
	payload.result = PayloadResult{Size: int64(len(payload.data)), SHA256: hex.EncodeToString(hash.Sum(nil))}
	return payload, nil
}

func allocationCapacity(required, maximum int64) int64 {
	if required <= 0 {
		return 0
	}
	if required >= maximum {
		return maximum
	}
	capacity := min(ingestAllocationChunk, maximum)
	for capacity < required {
		next := capacity * 2
		if next <= capacity || next > maximum {
			return maximum
		}
		capacity = next
	}
	return capacity
}

// maxReallocationPeak is the largest old+new array pair on the geometric
// growth path. It lets option validation guarantee one maximum payload can
// always finish a reallocation without exceeding either configured budget.
func maxReallocationPeak(maximum int64) int64 {
	if maximum <= 0 {
		return 0
	}
	previous := allocationCapacity(maximum-1, maximum)
	if previous == maximum {
		// maximum-1 may round to maximum; find the preceding capacity.
		previous = 0
		capacity := min(ingestAllocationChunk, maximum)
		for capacity < maximum {
			next := capacity * 2
			if next <= capacity || next >= maximum {
				previous = capacity
				break
			}
			capacity = next
		}
	}
	if previous == 0 {
		return maximum
	}
	return previous + maximum
}

// acquireReallocationLease gives one growing reader exclusive access to the
// remaining byte budget. Contenders fail fast and release their partial
// buffers, while the holder's subsequent reserve remains context-cancellable.
func (s *IngestService) acquireReallocationLease() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrIngestClosed
	}
	if s.overflowing {
		return ErrIngestReservation
	}
	s.overflowing = true
	s.signalLocked()
	return nil
}

func (s *IngestService) releaseReallocationLease() {
	s.mu.Lock()
	s.overflowing = false
	s.signalLocked()
	s.mu.Unlock()
}

func (s *IngestService) reserve(ctx context.Context, payload *IngestPayload, bytes int64) error {
	if bytes == 0 {
		return nil
	}
	if payload == nil {
		return errors.New("ingest reservation payload is required")
	}
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrIngestClosed
		}
		recordingUsed := s.reservedByRecording[payload.recordingID]
		if bytes <= s.options.GlobalBytes-s.reservedGlobal && bytes <= s.options.PerRecordingBytes-recordingUsed {
			s.reservedGlobal += bytes
			s.reservedByRecording[payload.recordingID] = recordingUsed + bytes
			s.signalLocked()
			s.mu.Unlock()
			break
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}

	if s.global != nil {
		if payload.reservationID == "" {
			s.releaseReservation(payload.recordingID, bytes)
			return errors.New("ingest reservation identity is missing")
		}
		// Record the desired amount before the RPC. If the response is lost after
		// the Host applied it, ReleasePayload still knows which lease to release.
		payload.reservationMu.Lock()
		desired := payload.globalReserved + bytes
		payload.globalReserved = desired
		if err := callCoordinator(ctx, "set reservation", func(callCtx context.Context) error {
			return s.global.SetReservation(callCtx, payload.recordingID, payload.reservationID, desired)
		}); err != nil {
			payload.reservationMu.Unlock()
			s.recordCoordinatorFailure(err)
			s.releaseReservation(payload.recordingID, bytes)
			return err
		}
		payload.globalRelease.reset()
		payload.reservationMu.Unlock()
	}
	payload.reserved += bytes
	return nil
}

func (s *IngestService) addUsed(bytes int64) {
	s.mu.Lock()
	s.usedBytes += bytes
	s.mu.Unlock()
}

func (s *IngestService) releaseReservation(recordingID string, bytes int64) {
	if bytes <= 0 {
		return
	}
	s.mu.Lock()
	s.reservedGlobal -= bytes
	s.reservedByRecording[recordingID] -= bytes
	if s.reservedByRecording[recordingID] == 0 {
		delete(s.reservedByRecording, recordingID)
	}
	s.signalLocked()
	s.mu.Unlock()
}

// releasePayloadReservation releases local bytes immediately and shrinks the
// shared lease. A failed/uncertain remote shrink intentionally leaves the
// larger Host reservation in place; that can reduce admission temporarily but
// can never under-account a still-live backing array.
func (s *IngestService) releasePayloadReservation(payload *IngestPayload, bytes int64) {
	if payload == nil || bytes <= 0 {
		return
	}
	s.releaseReservation(payload.recordingID, bytes)
	if s.global == nil {
		return
	}
	payload.reservationMu.Lock()
	defer payload.reservationMu.Unlock()
	if payload.globalReserved <= 0 {
		return
	}
	desired := payload.globalReserved - bytes
	if desired <= 0 {
		err := payload.globalRelease.release(func() error {
			return s.releaseCoordinatorLease(&payload.globalRelease, payload.reservationID, "release reservation", func(ctx context.Context) error {
				return s.global.ReleaseReservation(ctx, payload.recordingID, payload.reservationID)
			}, func() {
				payload.reservationMu.Lock()
				payload.globalReserved = 0
				payload.reservationMu.Unlock()
			})
		})
		if err == nil {
			payload.globalReserved = 0
		}
		return
	}
	err := callCoordinator(context.Background(), "set reservation", func(ctx context.Context) error {
		return s.global.SetReservation(ctx, payload.recordingID, payload.reservationID, desired)
	})
	if err == nil {
		payload.globalReserved = desired
		payload.globalRelease.reset()
	} else {
		s.recordCoordinatorFailure(err)
	}
}

func (s *IngestService) releasePayload(payload *IngestPayload) {
	payload.once.Do(func() {
		s.mu.Lock()
		s.usedBytes -= payload.used
		s.reservedGlobal -= payload.reserved
		s.reservedByRecording[payload.recordingID] -= payload.reserved
		if s.reservedByRecording[payload.recordingID] <= 0 {
			delete(s.reservedByRecording, payload.recordingID)
		}
		s.signalLocked()
		s.mu.Unlock()
		payload.data = nil
	})
	payload.reservationMu.Lock()
	defer payload.reservationMu.Unlock()
	if s.global != nil && payload.globalReserved > 0 {
		err := payload.globalRelease.release(func() error {
			return s.releaseCoordinatorLease(&payload.globalRelease, payload.reservationID, "release reservation", func(ctx context.Context) error {
				return s.global.ReleaseReservation(ctx, payload.recordingID, payload.reservationID)
			}, func() {
				payload.reservationMu.Lock()
				payload.globalReserved = 0
				payload.reservationMu.Unlock()
			})
		})
		if err == nil {
			payload.globalReserved = 0
		}
	}
}

// Submit transfers payload ownership after placing it in the bounded object
// queue. Persistence failures are retried over the same byte slice. A failed
// submission leaves ownership with the caller.
func (s *IngestService) Submit(ctx context.Context, payload *IngestPayload, persist func([]byte) (PayloadResult, error), complete func(PayloadResult, error)) error {
	return s.SubmitWithKind(ctx, payload, IngestJobKindCanonicalPayload, persist, complete)
}

// SubmitWithKind transfers payload ownership after placing it in the bounded
// object queue. Kind is bounded to Core-owned canonical work classes.
func (s *IngestService) SubmitWithKind(ctx context.Context, payload *IngestPayload, kind IngestJobKind, persist func([]byte) (PayloadResult, error), complete func(PayloadResult, error)) error {
	if payload == nil || payload.service != s || persist == nil || complete == nil {
		return errors.New("invalid ingest job")
	}
	if !kind.valid() {
		return errors.New("invalid ingest job kind")
	}
	return s.submit(ctx, &ingestJob{recordingID: payload.recordingID, kind: kind, payload: payload, persist: persist, complete: complete})
}

// SubmitCommit queues a bounded, byte-free metadata operation behind durable
// payload jobs. It is used for manifest observations so slow root-document
// writes do not hold the HLS poller; the callback must persist the latest
// recording projection when it runs instead of a stale captured snapshot.
func (s *IngestService) SubmitCommit(ctx context.Context, recordingID string, commit func() error, complete func(error)) error {
	return s.SubmitCommitWithKind(ctx, recordingID, IngestJobKindCanonicalPayload, commit, complete)
}

// SubmitCommitWithKind queues a bounded, byte-free metadata operation behind
// durable payload jobs. Kind identifies the Core-owned operation for failure
// diagnostics.
func (s *IngestService) SubmitCommitWithKind(ctx context.Context, recordingID string, kind IngestJobKind, commit func() error, complete func(error)) error {
	if recordingID == "" || commit == nil || complete == nil {
		return errors.New("invalid ingest commit")
	}
	if !kind.valid() {
		return errors.New("invalid ingest job kind")
	}
	return s.submit(ctx, &ingestJob{
		recordingID: recordingID, kind: kind,
		persist: func([]byte) (PayloadResult, error) {
			return PayloadResult{}, commit()
		},
		complete: func(_ PayloadResult, err error) { complete(err) },
	})
}

func (s *IngestService) submit(_ context.Context, job *ingestJob) error {
	// The body/metadata handed here is already complete. Acquisition context
	// cancellation (for example, a user pressing Stop) must not discard it
	// while a bounded queue is temporarily full. Submissions that began before
	// Close are allowed to wait for a slot and enter the drain; Close rejects
	// only submissions that begin after admission has stopped.
	if s.global != nil {
		var err error
		if job.queueLease, err = newIngestLeaseID(); err != nil {
			return err
		}
		if job.writerLease, err = newIngestLeaseID(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrIngestClosed
	}
	s.activeSubmits++
	s.signalLocked()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeSubmits--
		s.signalLocked()
		s.mu.Unlock()
	}()

	job.queuedAt = time.Now()
	for {
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		select {
		case s.slots <- struct{}{}:
			goto slotAcquired
		case <-changed:
		}
	}

slotAcquired:
	if s.global != nil {
		if err := callCoordinator(context.Background(), "acquire queue", func(ctx context.Context) error {
			return s.global.AcquireQueue(ctx, job.queueLease)
		}); err != nil {
			s.recordCoordinatorFailure(err)
			_ = s.releaseGlobalQueue(job)
			<-s.slots
			return &ingestCoordinatorFailure{operation: "acquire queue", cause: err}
		}
	}
	s.mu.Lock()
	s.queuedObjects++
	s.queuedBytes += job.payloadBytes()
	s.queueTimes[job] = job.queuedAt
	s.refreshOldestQueuedLocked()
	s.signalLocked()
	s.mu.Unlock()
	// The close coordinator keeps jobs open until activeSubmits reaches zero.
	s.jobs <- job
	return nil
}

func (s *IngestService) writer() {
	defer s.workers.Done()
	for job := range s.jobs {
		s.mu.Lock()
		s.queuedObjects--
		s.queuedBytes -= job.payloadBytes()
		delete(s.queueTimes, job)
		s.refreshOldestQueuedLocked()
		s.activeWriters++
		s.signalLocked()
		s.mu.Unlock()
		<-s.slots
		s.mu.Lock()
		firstFailure, failed := s.failedRecordings[job.recordingID]
		s.mu.Unlock()
		var result PayloadResult
		var err error
		writerAcquired := false
		if failed {
			err = &poisonedIngestFailure{currentKind: job.kind, first: firstFailure}
			_ = s.releaseGlobalQueue(job)
		} else if s.global != nil {
			if acquireErr := callCoordinator(context.Background(), "acquire writer", func(ctx context.Context) error {
				return s.global.AcquireWriter(ctx, job.writerLease)
			}); acquireErr != nil {
				s.recordCoordinatorFailure(acquireErr)
				err = &ingestWriterAcquireFailure{kind: job.kind, cause: acquireErr}
				_ = s.releaseGlobalWriter(job)
				_ = s.releaseGlobalQueue(job)
			} else {
				writerAcquired = true
				_ = s.releaseGlobalQueue(job)
			}
		} else {
			writerAcquired = true
		}
		if writerAcquired {
			result, err = s.persistWithRetry(job)
		}
		if writerAcquired && s.global != nil {
			_ = s.releaseGlobalWriter(job)
		}
		if err != nil {
			s.mu.Lock()
			if errors.Is(err, ErrCanonicalCommitFailed) {
				s.storageErrors++
				if _, exists := s.failedRecordings[job.recordingID]; !exists {
					s.failedRecordings[job.recordingID] = ingestFailureRecord{
						kind: job.kind, err: err, attempts: ingestFailureAttempts(err),
					}
				}
			}
			s.mu.Unlock()
			if errors.Is(err, ErrCanonicalCommitFailed) {
				if recorder, ok := s.store.StorageBackend.(storageErrorRecorder); ok {
					recorder.recordStorageError()
				}
			}
		}
		if job.payload != nil {
			job.payload.Release()
		}
		job.complete(result, err)
		s.mu.Lock()
		s.activeWriters--
		s.signalLocked()
		s.mu.Unlock()
	}
}

func (s *IngestService) releaseGlobalQueue(job *ingestJob) error {
	if s.global == nil || job == nil {
		return nil
	}
	return job.queueRelease.release(func() error {
		return s.releaseCoordinatorLease(&job.queueRelease, job.queueLease, "release queue", func(ctx context.Context) error {
			return s.global.ReleaseQueue(ctx, job.queueLease)
		}, nil)
	})
}

func (s *IngestService) releaseGlobalWriter(job *ingestJob) error {
	if s.global == nil || job == nil {
		return nil
	}
	return job.writerRelease.release(func() error {
		return s.releaseCoordinatorLease(&job.writerRelease, job.writerLease, "release writer", func(ctx context.Context) error {
			return s.global.ReleaseWriter(ctx, job.writerLease)
		}, nil)
	})
}

func (s *IngestService) recordCoordinatorFailure(err error) {
	if err == nil {
		return
	}
	var failure *ingestCoordinatorFailure
	if !errors.As(err, &failure) {
		return
	}
	s.mu.Lock()
	s.coordinatorErrors++
	s.signalLocked()
	s.mu.Unlock()
}

func (s *IngestService) releaseCoordinatorLease(state *leaseReleaseState, leaseID, operation string, call func(context.Context) error, onResolved func()) error {
	return s.releaseCoordinatorLeaseContext(state, leaseID, operation, nil, call, onResolved)
}

func (s *IngestService) releaseCoordinatorLeaseContext(state *leaseReleaseState, leaseID, operation string, ctx context.Context, call func(context.Context) error, onResolved func()) error {
	var err error
	if ctx == nil {
		err = callCoordinatorRelease(operation, call)
	} else {
		err = callCoordinatorReleaseContext(ctx, operation, call)
	}
	s.mu.Lock()
	wasPending := false
	firstPendingAttempt := false
	if err == nil {
		_, wasPending = s.pendingCoordinatorReleases[leaseID]
		delete(s.pendingCoordinatorReleases, leaseID)
	} else {
		s.coordinatorErrors++
		previous := s.pendingCoordinatorReleases[leaseID]
		firstPendingAttempt = previous.attempts == 0
		attempts := previous.attempts + 1
		s.pendingCoordinatorReleases[leaseID] = pendingCoordinatorRelease{
			leaseID: leaseID, operation: operation, state: state, call: call,
			onResolved: onResolved, attempts: attempts, next: time.Now().Add(coordinatorReleaseRetryDelay(attempts)),
		}
	}
	s.signalLocked()
	s.mu.Unlock()
	if firstPendingAttempt {
		log.Printf("storage coordinator lease release pending: operation=%q", operation)
	} else if wasPending {
		log.Printf("storage coordinator lease release retry resolved: operation=%q", operation)
	}
	return err
}

func coordinatorReleaseRetryDelay(attempt int) time.Duration {
	delay := runtimeResourceReleaseRetryBase
	for i := 1; i < attempt && delay < runtimeResourceReleaseRetryMaximum; i++ {
		if delay > runtimeResourceReleaseRetryMaximum/2 {
			return runtimeResourceReleaseRetryMaximum
		}
		delay *= 2
	}
	if delay > runtimeResourceReleaseRetryMaximum {
		return runtimeResourceReleaseRetryMaximum
	}
	return delay
}

func (s *IngestService) releaseRetryLoop() {
	defer close(s.releaseRetryDone)
	ticker := time.NewTicker(runtimeResourceReleaseRetryBase)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), runtimeResourceReleaseTimeout)
			s.retryPendingCoordinatorReleases(ctx, runtimeResourceReleaseRetryBatch, false)
			cancel()
		case <-s.releaseRetryStop:
			return
		}
	}
}

func (s *IngestService) retryPendingCoordinatorReleases(ctx context.Context, limit int, force bool) {
	s.mu.Lock()
	now := time.Now()
	entries := make([]pendingCoordinatorRelease, 0, len(s.pendingCoordinatorReleases))
	for _, pending := range s.pendingCoordinatorReleases {
		if force || !pending.next.After(now) {
			entries = append(entries, pending)
		}
	}
	s.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].leaseID < entries[j].leaseID })
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	for _, pending := range entries {
		if ctx.Err() != nil {
			return
		}
		err := pending.state.releaseContext(ctx, func() error {
			return s.releaseCoordinatorLeaseContext(pending.state, pending.leaseID, pending.operation, ctx, pending.call, pending.onResolved)
		})
		if err == nil && pending.onResolved != nil {
			pending.onResolved()
		}
	}
}

func (s *IngestService) persistWithRetry(job *ingestJob) (PayloadResult, error) {
	var result PayloadResult
	var lastErr error
	var data []byte
	if job.payload != nil {
		data = job.payload.data
	}
	attempts := 0
	for attempts < s.options.PersistAttempts {
		attempts++
		result, lastErr = job.persist(data)
		if lastErr == nil {
			return result, nil
		}
		// Store payload callbacks already measure successful writes. Callback
		// failures are counted here; avoid logging error contents or paths.
		if attempts == s.options.PersistAttempts {
			break
		}
		delay := retryBackoff(s.options.RetryBase, s.options.RetryMaxBackoff, attempts-1)
		timer := time.NewTimer(delay)
		<-timer.C
	}
	return PayloadResult{}, &canonicalCommitFailure{attempts: attempts, jobKind: job.kind, cause: lastErr}
}

func ingestFailureAttempts(err error) int {
	var counted attemptCountedError
	if errors.As(err, &counted) {
		return counted.Attempts()
	}
	return 0
}

func retryBackoff(initial, maximum time.Duration, retryIndex int) time.Duration {
	delay := initial
	for i := 0; i < retryIndex && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func (s *IngestService) Snapshot() IngestSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldest := 0.0
	var oldestQueued time.Time
	for _, queuedAt := range s.queueTimes {
		if oldestQueued.IsZero() || queuedAt.Before(oldestQueued) {
			oldestQueued = queuedAt
		}
	}
	if !oldestQueued.IsZero() {
		oldest = time.Since(oldestQueued).Seconds()
	}
	return IngestSnapshot{BufferCapacityBytes: s.options.GlobalBytes, PerRecordingCapacityBytes: s.options.PerRecordingBytes, BufferUsedBytes: s.usedBytes, ReservedBytes: s.reservedGlobal, QueueObjects: s.queuedObjects, QueueBytes: s.queuedBytes, OldestPersistAgeSeconds: oldest, ActiveWriters: s.activeWriters, WriterConcurrency: s.options.Writers, StorageErrorsTotal: s.storageErrors, CoordinatorErrorsTotal: s.coordinatorErrors, PendingCoordinatorLeases: len(s.pendingCoordinatorReleases)}
}

// Options returns the immutable runtime options used when this service was
// constructed. Settings edits do not resize a running service.
func (s *IngestService) Options() IngestOptions {
	if s == nil {
		return DefaultIngestOptions()
	}
	return s.options
}

// PoolMetrics joins the local backend observations with volatile ingest state
// without exposing the backend's physical root path.
func (s *IngestService) PoolMetrics() PoolSnapshot {
	state := s.Snapshot()
	var pool PoolSnapshot
	if backend, ok := s.store.StorageBackend.(poolMetricsWithIngest); ok {
		pool = backend.poolMetrics(state)
	} else {
		pool = s.store.StorageBackend.PoolMetrics()
	}
	pool.Buffer = PoolBuffer{UsedBytes: state.BufferUsedBytes, CapacityBytes: state.BufferCapacityBytes, ReservedBytes: state.ReservedBytes, PerRecordingCapacityBytes: state.PerRecordingCapacityBytes}
	pool.Queue = PoolQueue{Objects: state.QueueObjects, Bytes: state.QueueBytes, OldestAgeSeconds: state.OldestPersistAgeSeconds}
	pool.Writers = PoolWriters{Active: state.ActiveWriters, Limit: state.WriterConcurrency}
	return pool
}

func (s *IngestService) sampleLoop() {
	defer close(s.samplerDone)
	ticker := time.NewTicker(s.options.SampleInterval)
	defer ticker.Stop()
	s.runSampler(ticker.C, s.samplerStop)
}

// runSampler is separated from ticker construction so tests can drive exact
// sample timestamps without waiting five seconds. Production owns this loop;
// API reads do not determine whether the history continues to advance.
func (s *IngestService) runSampler(ticks <-chan time.Time, stop <-chan struct{}) {
	for {
		select {
		case now := <-ticks:
			state := s.Snapshot()
			s.store.samplePool(state, now)
		case <-stop:
			return
		}
	}
}

func (s *IngestService) signalLocked() { close(s.changed); s.changed = make(chan struct{}) }

func (s *IngestService) refreshOldestQueuedLocked() {
	s.oldestQueued = time.Time{}
	for _, queuedAt := range s.queueTimes {
		if s.oldestQueued.IsZero() || queuedAt.Before(s.oldestQueued) {
			s.oldestQueued = queuedAt
		}
	}
}

// Close stops admission and drains every accepted object. Calls may be
// repeated after a deadline to continue waiting for a blocked local writer or
// retry unresolved remote lease releases.
func (s *IngestService) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.signalLocked()
		s.mu.Unlock()
		close(s.samplerStop)
		go func() {
			s.waitForSubmits()
			close(s.jobs)
			s.workers.Wait()
			if s.global != nil {
				close(s.releaseRetryStop)
				<-s.releaseRetryDone
			}
			<-s.samplerDone
			close(s.closeDone)
		}()
	})
	select {
	case <-s.closeDone:
		if s.global != nil {
			releaseCtx, cancel := context.WithTimeout(ctx, runtimeResourceReleaseTimeout)
			s.retryPendingCoordinatorReleases(releaseCtx, runtimeResourceReleaseRetryBatch, true)
			cancel()
		}
		s.mu.Lock()
		pending := len(s.pendingCoordinatorReleases)
		s.mu.Unlock()
		if pending > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("%w: %d remote lease releases remain pending", ErrIngestCoordinatorUnavailable, pending)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *IngestService) waitForSubmits() {
	for {
		s.mu.Lock()
		if s.activeSubmits == 0 {
			s.mu.Unlock()
			return
		}
		changed := s.changed
		s.mu.Unlock()
		<-changed
	}
}

// Done is closed after the service has stopped admission, drained accepted
// jobs, and joined its writer and sampler goroutines. It can be observed by
// coordinators that must outlive a caller's Close deadline.
func (s *IngestService) Done() <-chan struct{} { return s.closeDone }
