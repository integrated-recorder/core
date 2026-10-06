// Package acquire owns recording lifecycles and the shared HLS polling and
// original-byte acquisition path.
package acquire

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/network"
	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

type Resolver interface {
	Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error)
}

// Refresher is an optional adapter operation. It is deliberately separate
// from Resolver so existing test and embedding implementations remain valid.
type Refresher interface {
	Refresh(context.Context, string, *adapterproto.ResourceRef, adapterproto.MediaSource) (adapterproto.MediaSource, error)
}

// RefreshPreparer separates validation from adapter-owned state mutation so
// Core can apply its network policy before a refresh transaction is committed.
type RefreshPreparer interface {
	PrepareRefresh(context.Context, string, *adapterproto.ResourceRef, adapterproto.MediaSource) (adapterproto.MediaSource, func() error, error)
}

// MetadataPreparer is optional so protocol-v1 adapters without metadata
// support keep working without a polling loop. The returned state commit is
// applied only after Core accepts the observation for the current media
// generation and durably stores any canonical revision.
type MetadataPreparer interface {
	PrepareMetadata(context.Context, string, *adapterproto.ResourceRef, adapterproto.MediaSource) (adapterproto.MetadataResult, func() error, bool, error)
}

// OwnershipToken aliases the Runtime Host's durable owner identity so the
// acquisition layer and Host cannot drift into subtly different token shapes.
type OwnershipToken = recordingowner.Owner

// CanonicalCommitFence authorizes a complete canonical Recording mutation.
// Implementations must hold the per-Recording cross-process fence for the
// entire callback, not just until owner validation completes.
type CanonicalCommitFence interface {
	WithCommit(OwnershipToken, func() error) error
	WithUnownedCommit(string, func() error) error
}

// CanonicalRecoveryFence serializes managed startup recovery against every
// cross-process canonical commit and invalidates all pre-restart owner tokens
// before the recovery callback is allowed to mutate archive state.
type CanonicalRecoveryFence interface {
	WithFencedRecovery(func() error) error
}

type SourceValidator func(context.Context, string) error

type entry struct {
	// persistMu serializes durable root-document writes and archive deletion.
	// Callers always acquire it before mu, and never hold mu across storage I/O.
	persistMu       sync.Mutex
	mu              sync.Mutex
	recording       *domain.Recording
	deleted         bool
	cancel          context.CancelFunc
	done            chan struct{}
	media           adapterproto.MediaSource
	mediaGeneration uint64
	refreshGate     chan struct{}
	scheduler       *segmentScheduler
	adapterID       string
	resource        *adapterproto.ResourceRef
	ownership       *OwnershipToken
	terminalErr     error
	handoverGate    chan struct{}
	handoverWake    chan struct{}
	handover        *handoverOperation
	// handoverCandidate owns bounded, non-canonical payload reservations until
	// the first scheduler adopts them or the worker is torn down.
	handoverCandidate *handoverContinuationCandidate
	// handoverFirstCommitStarted is guarded by mu and limits test barriers to
	// the first media segment adopted by a transferred Engine.
	handoverFirstCommitStarted bool
	handoverFirstCommitPending bool
}

type Manager struct {
	store           *storage.Store
	ingest          *storage.IngestService
	client          *http.Client
	resolver        Resolver
	validate        SourceValidator
	mu              sync.RWMutex
	startCreationMu sync.Mutex
	entries         map[string]*entry
	prepared        map[string]preparedHandover
	handoverMu      sync.Mutex
	// freshGeneration leaves existing archive documents read-only and does
	// not take ownership of them. It is used for a candidate Engine generation
	// that must not recover/interrupt work owned by an older Engine.
	freshGeneration      bool
	closed               bool
	starts               sync.WaitGroup
	startsWait           sync.Once
	startsDone           chan struct{}
	canonicalFence       CanonicalCommitFence
	startAttempted       bool
	terminalOwnerRelease func(OwnershipToken) error

	// fetchBoundaryHook is a deterministic test seam for scheduler-owned
	// generation checks. It is configured before recording goroutines start.
	fetchBoundaryHook func(uri string)
	// storageWriteHook is a deterministic test seam used to block asynchronous
	// canonical writes without coupling HTTP body reads to filesystem latency.
	storageWriteHook func()
	// storageSnapshotWriteHook is a deterministic test seam for manifest
	// snapshot persistence; production leaves it nil.
	storageSnapshotWriteHook func()
	// storageMetadataWriteHook blocks queued root metadata writes in deterministic
	// storage-delay tests. Production leaves it nil.
	storageMetadataWriteHook func()
	// storageWriteFailureHook injects deterministic persistence failures for
	// retry/no-redownload tests. Production leaves it nil.
	storageWriteFailureHook func() error
	// Metadata polling is fixed policy in production; the interval seam keeps
	// lifecycle tests deterministic without waiting for the production cadence.
	metadataPollInterval time.Duration
	// metadataClock is a deterministic test seam. Production leaves it nil.
	metadataClock func() time.Time
}

// OwnedRecordingState is a bounded, non-payload runtime snapshot. It avoids
// cloning full canonical segment histories for process inventory checks.
type OwnedRecordingState struct {
	ID        string
	State     domain.RecordingState
	StartedAt time.Time
}

// StartupMode makes archive ownership behavior explicit when constructing a
// recorder manager. RecoverExisting preserves the established single-process
// startup recovery semantics. FreshGeneration reads no existing archives into
// the worker registry and performs no startup recovery writes.
type StartupMode uint8

const (
	RecoverExisting StartupMode = iota
	FreshGeneration
)

var errManagerClosed = errors.New("recording manager is closed")

var (
	ErrOwnershipRequired              = errors.New("recording ownership token is required")
	ErrCanonicalFenceRequired         = errors.New("canonical commit fence is not configured")
	ErrCanonicalRecoveryFenceRequired = errors.New("managed archive recovery requires an owner fence")
	ErrInvalidOwnershipToken          = errors.New("recording ownership token is invalid")
	ErrFenceConfigurationClosed       = errors.New("canonical commit fence must be configured before recording starts")
	ErrDirectPersistRequiresQueue     = errors.New("owned recording persistence requires the buffered commit path")
	ErrHandoverUnavailable            = errors.New("recording is not eligible for handover")
	// ErrHandoverSourceRefreshRequired tells the Runtime Host that target
	// preflight found an expired/refresh-rejected source while the source Engine
	// is still active. Protocol v1 does not promise concurrent/idempotent
	// adapter refresh, so the Host must drain the source before retrying.
	ErrHandoverSourceRefreshRequired = errors.New("source must be drained before handover refresh")
	// ErrHandoverSourceBoundaryRequired tells the Runtime Host that the source
	// is live but its current manifest has no uncommitted media object. The Host
	// must drain the source before waiting for the next object so the final
	// canonical tail is stable before ownership can transfer.
	ErrHandoverSourceBoundaryRequired = errors.New("source must be drained before waiting for a continuation boundary")
	ErrHandoverConflict               = errors.New("recording handover is already in progress")
	ErrHandoverOwnerMismatch          = errors.New("recording handover owner does not match")
	ErrHandoverTargetInvalid          = errors.New("recording handover target is invalid")
	ErrHandoverOwnerNotTransferred    = errors.New("recording ownership has not transferred")
)

// ErrActiveRecording is returned when a management operation attempts to
// delete a recording whose acquisition worker is still active.
var ErrActiveRecording = errors.New("active recording cannot be deleted")

// ErrListLimit is returned when a bounded management snapshot would exceed
// the caller's maximum recording count.
var ErrListLimit = errors.New("recording list exceeds management limit")

func NewManager(store *storage.Store, client *http.Client, resolver Resolver, validate SourceValidator) (*Manager, error) {
	// NewManager is the compatibility constructor for an unmanaged, single
	// process recorder. Runtime-managed Engines must use
	// NewManagerWithFencedRecovery for recovery instead.
	return newManagerWithMode(store, client, resolver, validate, RecoverExisting)
}

// NewManagerWithMode constructs a fresh managed generation. Existing archive
// recovery is intentionally rejected here because it mutates canonical state;
// callers must provide both the commit and recovery fences through
// NewManagerWithFencedRecovery. Fresh generations may observe prior archives
// through read-only Get/List projections but own only recordings they start.
func NewManagerWithMode(store *storage.Store, client *http.Client, resolver Resolver, validate SourceValidator, mode StartupMode) (*Manager, error) {
	if mode == RecoverExisting {
		return nil, ErrCanonicalRecoveryFenceRequired
	}
	return newManagerWithMode(store, client, resolver, validate, mode)
}

// NewManagerWithFencedRecovery constructs a managed recorder and performs
// startup recovery only inside the Runtime Host's cross-process recovery
// barrier. The commit fence is installed before the barrier can mutate any
// canonical archive. The recovery barrier must invalidate old owner tokens
// and hold the same locks used by WithCommit for its entire callback.
func NewManagerWithFencedRecovery(store *storage.Store, client *http.Client, resolver Resolver, validate SourceValidator, fence CanonicalCommitFence, recovery CanonicalRecoveryFence) (*Manager, error) {
	if fence == nil || recovery == nil {
		return nil, ErrCanonicalRecoveryFenceRequired
	}
	m, err := newManagerWithMode(store, client, resolver, validate, FreshGeneration)
	if err != nil {
		return nil, err
	}
	if err := m.ConfigureCanonicalCommitFence(fence); err != nil {
		_ = m.Close(context.Background())
		return nil, err
	}
	var loaded []*domain.Recording
	if err := recovery.WithFencedRecovery(func() error {
		var loadErr error
		loaded, loadErr = store.LoadAll()
		return loadErr
	}); err != nil {
		_ = m.Close(context.Background())
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, recording := range loaded {
		m.entries[recording.ID] = &entry{recording: recording, done: closedChannel()}
	}
	m.freshGeneration = false
	return m, nil
}

func newManagerWithMode(store *storage.Store, client *http.Client, resolver Resolver, validate SourceValidator, mode StartupMode) (*Manager, error) {
	if store == nil {
		return nil, fmt.Errorf("storage is required")
	}
	if mode != RecoverExisting && mode != FreshGeneration {
		return nil, fmt.Errorf("invalid recording manager startup mode")
	}
	if client == nil {
		client = network.NewPublicHTTPClient(25 * time.Second)
	}
	if validate == nil {
		validate = network.ValidatePublicURL
	}
	var loaded []*domain.Recording
	var err error
	if mode == RecoverExisting {
		loaded, err = store.LoadAll()
		if err != nil {
			return nil, err
		}
	}
	ingest, err := store.IngestService()
	if err != nil {
		return nil, err
	}
	if resolver == nil {
		resolver = unavailableResolver{}
	}
	m := &Manager{store: store, ingest: ingest, client: client, resolver: resolver, validate: validate, entries: map[string]*entry{}, prepared: make(map[string]preparedHandover), freshGeneration: mode == FreshGeneration, startsDone: make(chan struct{})}
	for _, recording := range loaded {
		m.entries[recording.ID] = &entry{recording: recording, done: closedChannel()}
	}
	return m, nil
}

// ConfigureCanonicalCommitFence installs the Runtime Host ownership fence.
// It must be called before the Manager admits its first recording start.
func (m *Manager) ConfigureCanonicalCommitFence(fence CanonicalCommitFence) error {
	if m == nil || fence == nil {
		return ErrCanonicalFenceRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startAttempted {
		return ErrFenceConfigurationClosed
	}
	m.canonicalFence = fence
	return nil
}

// ConfigureTerminalOwnerRelease installs the Host callback used after a
// worker has durably published its terminal state. It must be configured
// before the manager admits any Recording starts.
func (m *Manager) ConfigureTerminalOwnerRelease(release func(OwnershipToken) error) error {
	if m == nil || release == nil {
		return ErrCanonicalFenceRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startAttempted {
		return ErrFenceConfigurationClosed
	}
	m.terminalOwnerRelease = release
	return nil
}

// HandoverSnapshot is the bounded, serializable continuation context needed to
// prepare another Engine. It deliberately contains no worker, scheduler,
// network connection, adapter process, or buffered media state.
type HandoverSnapshot struct {
	RecordingID string                    `json:"recording_id"`
	Owner       OwnershipToken            `json:"owner"`
	AdapterID   string                    `json:"adapter_id"`
	Media       adapterproto.MediaSource  `json:"media"`
	Resource    *adapterproto.ResourceRef `json:"resource,omitempty"`
}

// HandoverTargetIdentity is Host-selected process identity for a prepared
// target. The Engine does not choose either value.
type HandoverTargetIdentity struct {
	EngineGeneration string `json:"engine_generation"`
	WorkerInstance   string `json:"worker_instance"`
}

type handoverOperationState uint8

const (
	handoverRequested handoverOperationState = iota + 1
	handoverDraining
	handoverPaused
	handoverResuming
	handoverCompleted
	handoverAborted
)

type handoverOperation struct {
	ctx          context.Context
	expected     OwnershipToken
	state        handoverOperationState
	snapshot     HandoverSnapshot
	paused       chan struct{}
	resumed      chan struct{}
	resume       chan struct{}
	complete     chan struct{}
	pausedOnce   sync.Once
	resumedOnce  sync.Once
	resumeOnce   sync.Once
	completeOnce sync.Once
}

type preparedHandover struct {
	snapshot           HandoverSnapshot
	target             HandoverTargetIdentity
	active             bool
	media              adapterproto.MediaSource
	refreshStateCommit func() error
	continuation       *handoverContinuationCandidate
	rootFingerprint    [32]byte
}

// handoverContinuationCandidate is a bounded, volatile preflight result. Its
// payloads are governed by the shared ingest budget and are never canonical
// until the owner token has been transferred and the scheduler submits them.
type handoverContinuationCandidate struct {
	source          hls.MediaSegment
	epoch           uint64
	ordinal         uint64
	initID          string
	initPayload     *storage.IngestPayload
	mediaPayload    *storage.IngestPayload
	rootFingerprint [32]byte
}

const maxHandoverSnapshotBytes = 1 << 20

// Handover manifest probing is read-only and bounded independently from the
// caller's larger orchestration deadline.
const handoverProbeTimeout = 15 * time.Second

func (m *Manager) HandoverSnapshot(id string) (HandoverSnapshot, error) {
	e, ok := m.entry(id)
	if !ok {
		return HandoverSnapshot{}, storage.ErrNotFound
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	paused := e.handover != nil && e.handover.state == handoverPaused
	if e.recording == nil || e.recording.State != domain.StateRecording || e.ownership == nil || (e.scheduler == nil && !paused) || e.done == nil {
		return HandoverSnapshot{}, ErrHandoverUnavailable
	}
	return makeHandoverSnapshot(id, *e.ownership, e.adapterID, e.media, e.resource)
}

// PauseForHandover parks an owned Recording only after its current manifest
// boundary and all accepted canonical writes have drained. A timeout aborts
// the pause request and leaves the source worker running under its old token.
func (m *Manager) PauseForHandover(ctx context.Context, id string, expected OwnershipToken) (HandoverSnapshot, error) {
	if ctx == nil {
		return HandoverSnapshot{}, context.Canceled
	}
	e, ok := m.entry(id)
	if !ok {
		return HandoverSnapshot{}, storage.ErrNotFound
	}
	if err := acquireHandoverGate(ctx, e); err != nil {
		return HandoverSnapshot{}, err
	}
	defer releaseHandoverGate(e)

	e.mu.Lock()
	if !sameOwner(e.ownership, expected) || e.recording == nil || e.recording.State != domain.StateRecording || e.cancel == nil || e.handoverWake == nil {
		e.mu.Unlock()
		return HandoverSnapshot{}, ErrHandoverOwnerMismatch
	}
	if e.handover != nil {
		if sameOwner(&e.handover.expected, expected) && e.handover.state == handoverPaused {
			snapshot := cloneHandoverSnapshot(e.handover.snapshot)
			e.mu.Unlock()
			return snapshot, nil
		}
		e.mu.Unlock()
		return HandoverSnapshot{}, ErrHandoverConflict
	}
	op := &handoverOperation{
		ctx: ctx, expected: expected, state: handoverRequested,
		paused: make(chan struct{}), resumed: make(chan struct{}),
		resume: make(chan struct{}), complete: make(chan struct{}),
	}
	e.handover = op
	signalHandover(e)
	e.mu.Unlock()

	select {
	case <-op.paused:
		e.mu.Lock()
		if op.state == handoverPaused {
			snapshot := cloneHandoverSnapshot(op.snapshot)
			e.mu.Unlock()
			return snapshot, nil
		}
		e.mu.Unlock()
		return HandoverSnapshot{}, ErrHandoverUnavailable
	case <-e.done:
		return HandoverSnapshot{}, ErrHandoverUnavailable
	case <-ctx.Done():
		m.abortOrResumeTimedOutPause(e, op)
		return HandoverSnapshot{}, ctx.Err()
	}
}

func (m *Manager) abortOrResumeTimedOutPause(e *entry, op *handoverOperation) {
	e.mu.Lock()
	if e.handover != op {
		e.mu.Unlock()
		return
	}
	if op.state == handoverPaused {
		op.state = handoverResuming
		e.handover = nil
		op.resumeOnce.Do(func() { close(op.resume) })
		resumed := op.resumed
		e.mu.Unlock()
		<-resumed
		return
	}
	// The worker's drain is using op.ctx and will unwind as soon as the
	// caller's context is canceled. Leave the operation installed until the
	// worker observes that cancellation and republishes its normal poll loop.
	e.mu.Unlock()
	signalHandover(e)
}

// ResumeHandover restarts a parked source under the current Host owner token.
// It accepts the unchanged token for a pre-transfer abort, or a strictly newer
// epoch for the same source generation/instance after Host rollback.
func (m *Manager) ResumeHandover(id string, newOwner OwnershipToken, snapshot HandoverSnapshot) error {
	e, ok := m.entry(id)
	if !ok {
		return storage.ErrNotFound
	}
	if err := acquireHandoverGate(context.Background(), e); err != nil {
		return err
	}
	defer releaseHandoverGate(e)
	e.mu.Lock()
	op := e.handover
	if op == nil || op.state != handoverPaused || id != snapshot.RecordingID || !sameHandoverSnapshot(op.snapshot, snapshot) ||
		!validOwnershipToken(newOwner) || newOwner.RecordingID != id ||
		newOwner.EngineGeneration != op.expected.EngineGeneration || newOwner.WorkerInstance != op.expected.WorkerInstance ||
		(newOwner.Epoch != op.expected.Epoch && newOwner.Epoch <= op.expected.Epoch) {
		e.mu.Unlock()
		return ErrHandoverOwnerMismatch
	}
	e.mu.Unlock()
	if err := m.withOwnershipCommit(&newOwner, func() error { return nil }); err != nil {
		return err
	}
	e.mu.Lock()
	if e.handover != op || op.state != handoverPaused {
		e.mu.Unlock()
		return ErrHandoverConflict
	}
	ownerCopy := newOwner
	e.ownership = &ownerCopy
	e.media = cloneMediaSource(snapshot.Media)
	e.resource = cloneResourceRef(snapshot.Resource)
	op.state = handoverResuming
	op.resumeOnce.Do(func() { close(op.resume) })
	e.mu.Unlock()
	<-op.resumed
	return nil
}

// CompleteHandover detaches a fully drained, parked source Engine without
// changing the canonical Recording or releasing the Host's transferred owner.
func (m *Manager) CompleteHandover(id string, expectedOld OwnershipToken) error {
	e, ok := m.entry(id)
	if !ok {
		return storage.ErrNotFound
	}
	if err := acquireHandoverGate(context.Background(), e); err != nil {
		return err
	}
	defer releaseHandoverGate(e)
	e.mu.Lock()
	op := e.handover
	if op == nil || op.state != handoverPaused || !sameOwner(&op.expected, expectedOld) || !sameOwner(e.ownership, expectedOld) {
		e.mu.Unlock()
		return ErrHandoverOwnerMismatch
	}
	e.mu.Unlock()
	err := m.withOwnershipCommit(&expectedOld, func() error { return nil })
	if err == nil {
		return ErrHandoverOwnerNotTransferred
	}
	if !errors.Is(err, recordingowner.ErrStaleOwner) {
		return err
	}
	m.mu.Lock()
	if m.entries[id] != e {
		m.mu.Unlock()
		return ErrHandoverConflict
	}
	delete(m.entries, id)
	m.mu.Unlock()
	e.mu.Lock()
	if e.handover != op || op.state != handoverPaused {
		e.mu.Unlock()
		return ErrHandoverConflict
	}
	op.state = handoverCompleted
	op.completeOnce.Do(func() { close(op.complete) })
	e.mu.Unlock()
	<-e.done
	return nil
}

// PrepareHandoverTarget makes a read-only target candidate and proves that a
// fresh manifest contains a source segment the canonical recording can
// continue from. It never creates or mutates the canonical archive and never
// starts acquisition.
func (m *Manager) PrepareHandoverTarget(ctx context.Context, snapshot HandoverSnapshot, target HandoverTargetIdentity) error {
	return m.PrepareHandoverTargetWithSourceState(ctx, snapshot, target, false)
}

// PrepareHandoverTargetAfterSourceDrain allows refresh only after the trusted
// Runtime Host has paused and drained the source Engine. The candidate remains
// read-only and is staged in bounded ingest memory until ownership transfers.
func (m *Manager) PrepareHandoverTargetAfterSourceDrain(ctx context.Context, snapshot HandoverSnapshot, target HandoverTargetIdentity) error {
	return m.PrepareHandoverTargetWithSourceState(ctx, snapshot, target, true)
}

// PrepareHandoverTargetWithSourceState is the common implementation for the
// Host's two preflight phases. sourceDrained is an internal Host assertion; it
// must never be populated from a public product API.
func (m *Manager) PrepareHandoverTargetWithSourceState(ctx context.Context, snapshot HandoverSnapshot, target HandoverTargetIdentity, sourceDrained bool) error {
	if ctx == nil {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	validated, err := validateHandoverSnapshot(snapshot)
	if err != nil || !validTargetIdentity(target) {
		return ErrHandoverTargetInvalid
	}
	m.handoverMu.Lock()
	defer m.handoverMu.Unlock()
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return errManagerClosed
	}
	if _, exists := m.entries[validated.RecordingID]; exists {
		m.mu.RUnlock()
		return ErrHandoverConflict
	}
	existing, preparedExists := m.prepared[validated.RecordingID]
	m.mu.RUnlock()
	if preparedExists && (existing.active || existing.target != target) {
		return ErrHandoverConflict
	}
	recording, err := m.store.LoadRecordingReadOnly(validated.RecordingID)
	if err != nil || recording == nil || recording.ID != validated.RecordingID || recording.State != domain.StateRecording {
		if preparedExists {
			m.discardPreparedHandoverLocked(validated.RecordingID)
		}
		return ErrHandoverUnavailable
	}
	rootFingerprint, err := handoverRootFingerprint(recording)
	if err != nil {
		if preparedExists {
			m.discardPreparedHandoverLocked(validated.RecordingID)
		}
		return ErrHandoverUnavailable
	}
	if preparedExists {
		if sameHandoverSnapshot(existing.snapshot, validated) && existing.rootFingerprint == rootFingerprint {
			return nil
		}
		// Source recording may continue between target preparation and source
		// drain. Discard stale staged bytes before preparing against a newer
		// canonical tail; never retain reservations from a replaced attempt.
		m.mu.Lock()
		if current, exists := m.prepared[validated.RecordingID]; exists && !current.active {
			delete(m.prepared, validated.RecordingID)
			releasePreparedHandover(current)
		}
		m.mu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, handoverProbeTimeout)
	defer cancel()
	if err := runtimehook.Pause(runtimehook.DuringTargetPrepare, validated.RecordingID); err != nil {
		return err
	}
	media, refreshCommit, continuation, err := m.probeHandoverContinuation(probeCtx, validated, recording, rootFingerprint, sourceDrained)
	if err != nil {
		if errors.Is(err, ErrHandoverSourceRefreshRequired) {
			return ErrHandoverSourceRefreshRequired
		}
		if errors.Is(err, ErrHandoverSourceBoundaryRequired) {
			return ErrHandoverSourceBoundaryRequired
		}
		return ErrHandoverUnavailable
	}
	latest, latestErr := m.store.LoadRecordingReadOnly(validated.RecordingID)
	if latestErr != nil || latest == nil {
		releaseHandoverContinuation(continuation)
		return ErrHandoverUnavailable
	}
	latestFingerprint, fingerprintErr := handoverRootFingerprint(latest)
	if fingerprintErr != nil || latestFingerprint != rootFingerprint {
		releaseHandoverContinuation(continuation)
		return ErrHandoverUnavailable
	}
	if err := ctx.Err(); err != nil {
		releaseHandoverContinuation(continuation)
		return ErrHandoverUnavailable
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		releaseHandoverContinuation(continuation)
		return errManagerClosed
	}
	if _, exists := m.entries[validated.RecordingID]; exists {
		releaseHandoverContinuation(continuation)
		return ErrHandoverConflict
	}
	if existing, exists := m.prepared[validated.RecordingID]; exists {
		if existing.active || existing.target != target {
			releaseHandoverContinuation(continuation)
			return ErrHandoverConflict
		}
		delete(m.prepared, validated.RecordingID)
		releasePreparedHandover(existing)
	}
	// The source's media context can change during a speculative preparation.
	// The prepared target records the exact source snapshot and canonical root
	// revision it proved; activation checks both again before adopting bytes.
	m.prepared[validated.RecordingID] = preparedHandover{
		snapshot: validated, target: target, media: cloneMediaSource(media),
		refreshStateCommit: refreshCommit, continuation: continuation,
		rootFingerprint: rootFingerprint,
	}
	return nil
}

// probeHandoverContinuation proves read-only continuation readiness against a
// fresh playlist and the next actual, uncommitted media payload. Adapter v1
// does not retain the original resolve input/workflow answers in a recording,
// so continuation uses the current ResourceRef/MediaSource refresh contract;
// it must not fabricate a resolve/continue request.
func (m *Manager) probeHandoverContinuation(ctx context.Context, snapshot HandoverSnapshot, recording *domain.Recording, rootFingerprint [32]byte, sourceDrained bool) (adapterproto.MediaSource, func() error, *handoverContinuationCandidate, error) {
	media := cloneMediaSource(snapshot.Media)
	var refreshCommit func() error
	refreshed := false
	if due, _ := proactiveRefreshDue(media, time.Now()); due {
		if !sourceDrained {
			return adapterproto.MediaSource{}, nil, nil, ErrHandoverSourceRefreshRequired
		}
		var err error
		media, refreshCommit, err = m.prepareHandoverRefresh(ctx, snapshot, media)
		if err != nil {
			return adapterproto.MediaSource{}, nil, nil, ErrHandoverUnavailable
		}
		refreshed = true
	}
	probeEntry := &entry{recording: recording, media: cloneMediaSource(media)}
	for {
		playlist, err := m.fetchHandoverPlaylist(ctx, snapshot.RecordingID, media)
		if err != nil {
			if !refreshed && m.shouldRefresh(media, err) {
				if !sourceDrained {
					return adapterproto.MediaSource{}, nil, nil, ErrHandoverSourceRefreshRequired
				}
				media, refreshCommit, err = m.prepareHandoverRefresh(ctx, snapshot, media)
				if err == nil {
					refreshed = true
					probeEntry.media = cloneMediaSource(media)
					continue
				}
			}
			return adapterproto.MediaSource{}, nil, nil, ErrHandoverUnavailable
		}
		epoch, source, ok := findHandoverCandidate(recording, playlist)
		if !ok || source.URI == "" {
			if !playlist.EndList {
				if !sourceDrained {
					return adapterproto.MediaSource{}, nil, nil, ErrHandoverSourceBoundaryRequired
				}
				if err := waitHandoverManifestRetry(ctx, playlist); err != nil {
					return adapterproto.MediaSource{}, nil, nil, ErrHandoverUnavailable
				}
				continue
			}
			return adapterproto.MediaSource{}, nil, nil, ErrHandoverUnavailable
		}
		if err = m.validate(ctx, source.URI); err != nil {
			return adapterproto.MediaSource{}, nil, nil, ErrHandoverUnavailable
		}
		candidate, fetchErr := m.stageHandoverCandidate(ctx, probeEntry, recording, source, epoch, media, rootFingerprint)
		if fetchErr != nil {
			releaseHandoverContinuation(candidate)
			if !refreshed && m.shouldRefresh(media, fetchErr) {
				if !sourceDrained {
					return adapterproto.MediaSource{}, nil, nil, ErrHandoverSourceRefreshRequired
				}
				media, refreshCommit, fetchErr = m.prepareHandoverRefresh(ctx, snapshot, media)
				if fetchErr == nil {
					refreshed = true
					probeEntry.media = cloneMediaSource(media)
					continue
				}
			}
			return adapterproto.MediaSource{}, nil, nil, ErrHandoverUnavailable
		}
		return media, refreshCommit, candidate, nil
	}
}

func waitHandoverManifestRetry(ctx context.Context, playlist hls.MediaPlaylist) error {
	interval := time.Second
	if playlist.TargetDuration > 0 {
		interval = time.Duration(playlist.TargetDuration) * time.Second / 2
	}
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	if interval > 2*time.Second {
		interval = 2 * time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *Manager) prepareHandoverRefresh(ctx context.Context, snapshot HandoverSnapshot, current adapterproto.MediaSource) (adapterproto.MediaSource, func() error, error) {
	preparer, ok := m.resolver.(RefreshPreparer)
	if !ok {
		return adapterproto.MediaSource{}, nil, ErrHandoverUnavailable
	}
	candidate, commit, err := preparer.PrepareRefresh(ctx, snapshot.AdapterID, cloneResourceRef(snapshot.Resource), cloneMediaSource(current))
	if err != nil || adapterproto.ValidateMediaSource(candidate, []string{"hls"}) != nil || m.validate(ctx, candidate.ManifestURL) != nil {
		return adapterproto.MediaSource{}, nil, ErrHandoverUnavailable
	}
	return cloneMediaSource(candidate), commit, nil
}

func (m *Manager) fetchHandoverPlaylist(ctx context.Context, recordingID string, media adapterproto.MediaSource) (hls.MediaPlaylist, error) {
	manifestURL := media.ManifestURL
	if err := m.validate(ctx, manifestURL); err != nil {
		return hls.MediaPlaylist{}, newFetchError("manifest", 0, false, false)
	}
	payload, requestedManifestURL, err := m.fetchManifestForMedia(ctx, recordingID, media, media.ManifestURL, manifestURL, adapterproto.RequestScopeManifest)
	if err != nil {
		if payload != nil {
			payload.Release()
		}
		return hls.MediaPlaylist{}, err
	}
	if payload == nil {
		return hls.MediaPlaylist{}, ErrHandoverUnavailable
	}
	selectedURL := requestedManifestURL
	if hls.IsMasterPlaylist(payload.Bytes()) {
		master, parseErr := hlsParseMaster(payload.Bytes(), requestedManifestURL)
		payload.Release()
		if parseErr != nil {
			return hls.MediaPlaylist{}, ErrHandoverUnavailable
		}
		variant, selectErr := selectVariant(master)
		if selectErr != nil {
			return hls.MediaPlaylist{}, ErrHandoverUnavailable
		}
		payload, selectedURL, err = m.fetchManifestForMedia(ctx, recordingID, media, requestedManifestURL, variant.URI, adapterproto.RequestScopeVariant)
		if err != nil {
			if payload != nil {
				payload.Release()
			}
			return hls.MediaPlaylist{}, err
		}
		if payload == nil {
			return hls.MediaPlaylist{}, ErrHandoverUnavailable
		}
	}
	playlist, parseErr := hlsParseMedia(payload.Bytes(), selectedURL)
	payload.Release()
	if parseErr != nil {
		return hls.MediaPlaylist{}, ErrHandoverUnavailable
	}
	return playlist, nil
}

func (m *Manager) stageHandoverCandidate(ctx context.Context, e *entry, recording *domain.Recording, source hls.MediaSegment, epoch uint64, media adapterproto.MediaSource, fingerprint [32]byte) (*handoverContinuationCandidate, error) {
	ordinal, err := nextArchiveOrdinal(e)
	if err != nil || ordinal == 0 || ordinal == ^uint64(0) {
		return nil, ErrHandoverUnavailable
	}
	candidate := &handoverContinuationCandidate{source: cloneHandoverMediaSegment(source), epoch: epoch, ordinal: ordinal, rootFingerprint: fingerprint}
	if source.Init != nil {
		candidate.initID = initSegmentID(*source.Init, epoch, source.DiscontinuitySequence)
		if !initExists(recording, candidate.initID) {
			if err := m.validate(ctx, source.Init.URI); err != nil {
				return candidate, ErrHandoverUnavailable
			}
			candidate.initPayload, err = m.downloadObjectBufferedOnceAtGeneration(ctx, source.Init.URI, source.Init.ByteRange, recording.ID, media, e, 0)
			if err != nil {
				return candidate, err
			}
			if candidate.initPayload == nil || candidate.initPayload.Result().Size <= 0 {
				return candidate, ErrHandoverUnavailable
			}
		}
	}
	segment, payload, err := m.acquireMediaBuffered(ctx, e, source, epoch, ordinal, candidate.initID, media, 0)
	if err != nil {
		return candidate, err
	}
	if payload == nil {
		return candidate, ErrHandoverUnavailable
	}
	payloadResult := payload.Result()
	if payloadResult.Size <= 0 || segment.Sequence != source.Sequence || segment.SourceEpoch != epoch || segment.ArchiveOrdinal != ordinal || segment.PayloadSize != payloadResult.Size || segment.SHA256 == "" || segment.SHA256 != payloadResult.SHA256 {
		if payload != nil {
			payload.Release()
		}
		return candidate, ErrHandoverUnavailable
	}
	candidate.mediaPayload = payload
	return candidate, nil
}

func cloneHandoverMediaSegment(source hls.MediaSegment) hls.MediaSegment {
	if source.ByteRange != nil {
		copyRange := *source.ByteRange
		source.ByteRange = &copyRange
	}
	if source.Init != nil {
		copyInit := *source.Init
		if copyInit.ByteRange != nil {
			copyRange := *copyInit.ByteRange
			copyInit.ByteRange = &copyRange
		}
		source.Init = &copyInit
	}
	if source.ProgramTime != nil {
		copyTime := *source.ProgramTime
		source.ProgramTime = &copyTime
	}
	return source
}

func handoverRootFingerprint(recording *domain.Recording) ([32]byte, error) {
	if recording == nil {
		return [32]byte{}, ErrHandoverUnavailable
	}
	encoded, err := json.Marshal(recording)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func releaseHandoverContinuation(candidate *handoverContinuationCandidate) {
	if candidate == nil {
		return
	}
	if candidate.initPayload != nil {
		candidate.initPayload.Release()
		candidate.initPayload = nil
	}
	if candidate.mediaPayload != nil {
		candidate.mediaPayload.Release()
		candidate.mediaPayload = nil
	}
}

func releasePreparedHandover(prepared preparedHandover) {
	releaseHandoverContinuation(prepared.continuation)
}

// findHandoverCandidate mirrors observePlaylistAtGeneration's source-epoch
// detection and discoverAtGeneration's first-uncommitted selection without
// mutating the archive. A live-tail cursor is not enough: preflight must prove
// that an actual media object can be fetched before ownership moves.
func findHandoverCandidate(recording *domain.Recording, playlist hls.MediaPlaylist) (uint64, hls.MediaSegment, bool) {
	if recording == nil || len(playlist.Segments) == 0 {
		return 0, hls.MediaSegment{}, false
	}
	track := recording.Tracks["main"]
	if track == nil {
		return 0, hls.MediaSegment{}, false
	}
	maxSequence := playlist.Segments[0].Sequence
	for _, segment := range playlist.Segments[1:] {
		if segment.Sequence > maxSequence {
			maxSequence = segment.Sequence
		}
	}
	newEpoch := track.HasLastObservedSequence && maxSequence < track.LastObservedSequence
	if !newEpoch {
		for _, incoming := range playlist.Segments {
			for i := range track.Segments {
				captured := &track.Segments[i]
				if captured.SourceEpoch != track.SourceEpoch || captured.Sequence != incoming.Sequence {
					continue
				}
				if handoverSourceIdentityChanged(captured, incoming) {
					newEpoch = true
				}
				break
			}
			if newEpoch {
				break
			}
		}
	}
	epoch := track.SourceEpoch
	if newEpoch {
		if epoch == ^uint64(0) {
			return 0, hls.MediaSegment{}, false
		}
		epoch++
	}
	for _, incoming := range playlist.Segments {
		if incoming.Gap || gapCoversCoordinate(recording, "main", epoch, incoming.DiscontinuitySequence, incoming.Sequence) {
			continue
		}
		captured := false
		for i := range track.Segments {
			segment := &track.Segments[i]
			if segment.SourceEpoch == epoch && segment.DiscontinuitySequence == incoming.DiscontinuitySequence && segment.Sequence == incoming.Sequence {
				captured = true
				break
			}
		}
		if !captured {
			return epoch, cloneHandoverMediaSegment(incoming), true
		}
	}
	return 0, hls.MediaSegment{}, false
}

// hasHandoverContinuationCandidate is retained for focused identity tests; a
// cursor whose entire playlist tail is already committed is no longer a
// readiness proof.
func hasHandoverContinuationCandidate(recording *domain.Recording, playlist hls.MediaPlaylist) bool {
	_, _, ok := findHandoverCandidate(recording, playlist)
	return ok
}

func handoverSourceIdentityChanged(captured *domain.Segment, incoming hls.MediaSegment) bool {
	if captured.DiscontinuitySequence != incoming.DiscontinuitySequence {
		return true
	}
	if captured.SourceURI != "" && incoming.URI != "" && sourceURIIdentity(captured.SourceURI) != sourceURIIdentity(incoming.URI) {
		return true
	}
	return captured.ProgramDateTime != nil && incoming.ProgramTime != nil && !captured.ProgramDateTime.Equal(*incoming.ProgramTime)
}

// ActivatePreparedHandover adopts an existing archive only after the Host has
// durably transferred its owner token to this target Engine.
func (m *Manager) ActivatePreparedHandover(owner OwnershipToken) error {
	if !validOwnershipToken(owner) {
		return ErrInvalidOwnershipToken
	}
	if err := m.beginStart(&owner); err != nil {
		return err
	}
	defer m.starts.Done()
	m.handoverMu.Lock()
	defer m.handoverMu.Unlock()
	m.mu.Lock()
	prepared, ok := m.prepared[owner.RecordingID]
	if !ok || prepared.active || prepared.target.EngineGeneration != owner.EngineGeneration || prepared.target.WorkerInstance != owner.WorkerInstance {
		m.mu.Unlock()
		return ErrHandoverTargetInvalid
	}
	if _, exists := m.entries[owner.RecordingID]; exists {
		m.mu.Unlock()
		return ErrHandoverConflict
	}
	prepared.active = true
	m.prepared[owner.RecordingID] = prepared
	m.mu.Unlock()

	var recording *domain.Recording
	err := m.withOwnershipCommit(&owner, func() error {
		var loadErr error
		recording, loadErr = m.store.LoadRecordingReadOnly(owner.RecordingID)
		if loadErr != nil || recording == nil || recording.ID != owner.RecordingID || recording.State != domain.StateRecording {
			return ErrHandoverUnavailable
		}
		fingerprint, fingerprintErr := handoverRootFingerprint(recording)
		if fingerprintErr != nil || fingerprint != prepared.rootFingerprint || prepared.continuation == nil {
			return ErrHandoverUnavailable
		}
		entrySnapshot := &entry{recording: recording}
		nextOrdinal, ordinalErr := nextArchiveOrdinal(entrySnapshot)
		if ordinalErr != nil || nextOrdinal != prepared.continuation.ordinal {
			return ErrHandoverUnavailable
		}
		track := recording.Tracks["main"]
		if track == nil {
			return ErrHandoverUnavailable
		}
		for _, segment := range track.Segments {
			if segment.SourceEpoch == prepared.continuation.epoch && segment.Sequence == prepared.continuation.source.Sequence {
				return ErrHandoverUnavailable
			}
		}
		if prepared.refreshStateCommit != nil {
			if err := prepared.refreshStateCommit(); err != nil {
				return ErrHandoverUnavailable
			}
		}
		return nil
	})
	if err != nil {
		m.discardPreparedHandoverLocked(owner.RecordingID)
		return err
	}
	ownerCopy := owner
	workerCtx, cancel := context.WithCancel(context.Background())
	e := &entry{
		recording: recording, cancel: cancel, done: make(chan struct{}),
		media: cloneMediaSource(prepared.media), refreshGate: make(chan struct{}, 1),
		adapterID: prepared.snapshot.AdapterID, resource: cloneResourceRef(prepared.snapshot.Resource), ownership: &ownerCopy,
		handoverGate: make(chan struct{}, 1), handoverWake: make(chan struct{}, 1),
		handoverCandidate: prepared.continuation, handoverFirstCommitPending: true,
	}
	e.handoverGate <- struct{}{}
	m.mu.Lock()
	if m.closed || m.entries[owner.RecordingID] != nil {
		m.mu.Unlock()
		cancel()
		m.discardPreparedHandoverLocked(owner.RecordingID)
		return ErrHandoverConflict
	}
	m.entries[owner.RecordingID] = e
	delete(m.prepared, owner.RecordingID)
	m.mu.Unlock()
	go m.run(workerCtx, e, cloneMediaSource(prepared.media))
	return nil
}

// discardPreparedHandoverLocked is called with handoverMu held. It removes and
// releases a failed candidate so bounded ingest reservations cannot outlive
// the prepare/activation attempt.
func (m *Manager) discardPreparedHandoverLocked(recordingID string) {
	m.mu.Lock()
	prepared, exists := m.prepared[recordingID]
	delete(m.prepared, recordingID)
	m.mu.Unlock()
	if exists {
		releasePreparedHandover(prepared)
	}
}

// DiscardPreparedHandover drops only in-memory candidate state.
func (m *Manager) DiscardPreparedHandover(recordingID string) error {
	m.handoverMu.Lock()
	defer m.handoverMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	prepared, exists := m.prepared[recordingID]
	if !exists {
		return nil
	}
	if prepared.active {
		return ErrHandoverConflict
	}
	delete(m.prepared, recordingID)
	releasePreparedHandover(prepared)
	return nil
}

func makeHandoverSnapshot(id string, owner OwnershipToken, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef) (HandoverSnapshot, error) {
	return validateHandoverSnapshot(HandoverSnapshot{
		RecordingID: id, Owner: owner, AdapterID: adapterID,
		Media: cloneMediaSource(media), Resource: cloneResourceRef(resource),
	})
}

func validateHandoverSnapshot(snapshot HandoverSnapshot) (HandoverSnapshot, error) {
	if !validOwnershipToken(snapshot.Owner) || snapshot.Owner.RecordingID != snapshot.RecordingID ||
		strings.TrimSpace(snapshot.AdapterID) == "" || strings.ContainsAny(snapshot.AdapterID, "/\\") ||
		adapterproto.ValidateResourceRef(snapshot.Resource) != nil || adapterproto.ValidateMediaSource(snapshot.Media, []string{"hls"}) != nil {
		return HandoverSnapshot{}, ErrHandoverTargetInvalid
	}
	snapshot.Media = cloneMediaSource(snapshot.Media)
	snapshot.Resource = cloneResourceRef(snapshot.Resource)
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) > maxHandoverSnapshotBytes {
		return HandoverSnapshot{}, ErrHandoverTargetInvalid
	}
	return snapshot, nil
}

func cloneHandoverSnapshot(snapshot HandoverSnapshot) HandoverSnapshot {
	snapshot.Media = cloneMediaSource(snapshot.Media)
	snapshot.Resource = cloneResourceRef(snapshot.Resource)
	return snapshot
}

func sameHandoverSnapshot(a, b HandoverSnapshot) bool {
	return a.RecordingID == b.RecordingID && a.Owner == b.Owner && a.AdapterID == b.AdapterID &&
		reflect.DeepEqual(a.Media, b.Media) && reflect.DeepEqual(a.Resource, b.Resource)
}

func validTargetIdentity(target HandoverTargetIdentity) bool {
	return validRuntimeIdentity(target.EngineGeneration) && validRuntimeIdentity(target.WorkerInstance)
}

func validRuntimeIdentity(value string) bool {
	if len(value) != 32 && len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func acquireHandoverGate(ctx context.Context, e *entry) error {
	if e == nil || e.handoverGate == nil {
		return ErrHandoverUnavailable
	}
	select {
	case <-e.handoverGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseHandoverGate(e *entry) {
	e.handoverGate <- struct{}{}
}

func signalHandover(e *entry) {
	if e == nil || e.handoverWake == nil {
		return
	}
	select {
	case e.handoverWake <- struct{}{}:
	default:
	}
}

func sameOwner(current *OwnershipToken, expected OwnershipToken) bool {
	return current != nil && *current == expected
}

func cloneMediaSource(media adapterproto.MediaSource) adapterproto.MediaSource {
	data, err := json.Marshal(media)
	if err != nil {
		return media
	}
	var copy adapterproto.MediaSource
	if err = json.Unmarshal(data, &copy); err != nil {
		return media
	}
	return copy
}

func cloneResourceRef(resource *adapterproto.ResourceRef) *adapterproto.ResourceRef {
	if resource == nil {
		return nil
	}
	return &adapterproto.ResourceRef{Type: resource.Type, ID: resource.ID, Parent: cloneResourceRef(resource.Parent)}
}

func archiveResource(resource *adapterproto.ResourceRef) *domain.ResourceReference {
	if resource == nil {
		return nil
	}
	return &domain.ResourceReference{Type: resource.Type, ID: resource.ID, Parent: archiveResource(resource.Parent)}
}

func archiveProvenance(provenance *adapterproto.AdapterProvenance) *domain.AdapterProvenance {
	if provenance == nil {
		return nil
	}
	return &domain.AdapterProvenance{ID: provenance.ID, Version: provenance.Version, ProtocolVersion: provenance.ProtocolVersion, Fingerprint: provenance.Fingerprint}
}

type unavailableResolver struct{}

func (unavailableResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, fmt.Errorf("no adapter resolver is configured")
}

func (m *Manager) Start(ctx context.Context, adapterID string, input json.RawMessage, resource *adapterproto.ResourceRef, title string) (*domain.Recording, error) {
	if err := m.beginStart(nil); err != nil {
		return nil, err
	}
	defer m.starts.Done()
	if strings.TrimSpace(adapterID) == "" || strings.ContainsAny(adapterID, "/\\") {
		return nil, fmt.Errorf("adapter id is invalid")
	}
	if err := adapterproto.ValidateObject(input); err != nil {
		return nil, err
	}
	if err := adapterproto.ValidateResourceRef(resource); err != nil {
		return nil, fmt.Errorf("invalid resource reference")
	}
	media, err := m.resolver.Resolve(ctx, adapterID, input, resource)
	if err != nil {
		return nil, fmt.Errorf("adapter resolution failed")
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}
	return m.startResolved(ctx, id, adapterID, media, resource, title, nil, nil)
}

// StartResolved creates a recording from a media source already resolved by
// an adapter workflow. The shared HLS acquisition path is identical to Start.
func (m *Manager) StartResolved(ctx context.Context, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	if err := m.beginStart(nil); err != nil {
		return nil, err
	}
	defer m.starts.Done()
	id, err := newID()
	if err != nil {
		return nil, err
	}
	return m.startResolved(ctx, id, adapterID, media, resource, title, provenance, nil)
}

// StartResolvedOwned creates an already-resolved Recording under a Host-issued
// ownership token. The token's RecordingID selects the archive identity.
func (m *Manager) StartResolvedOwned(ctx context.Context, owner OwnershipToken, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	return m.StartResolvedWithIDOwned(ctx, owner, owner.RecordingID, adapterID, media, resource, title, provenance)
}

// StartResolvedWithID creates a resolved recording with a caller-allocated,
// validated recording ID. This allows a management projection to persist an
// exact cross-reference before canonical creation without adding management
// data to the archive. The acquisition path is otherwise identical to
// StartResolved.
func (m *Manager) StartResolvedWithID(ctx context.Context, id, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	if !validRecordingID(id) {
		return nil, fmt.Errorf("recording id is invalid")
	}
	if err := m.beginStart(nil); err != nil {
		return nil, err
	}
	defer m.starts.Done()
	return m.startResolved(ctx, id, adapterID, media, resource, title, provenance, nil)
}

// StartResolvedWithIDOwned creates a resolved Recording with a caller-chosen
// ID, provided that it matches the Host-issued owner token.
func (m *Manager) StartResolvedWithIDOwned(ctx context.Context, owner OwnershipToken, id, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	if !validOwnershipToken(owner) || owner.RecordingID != id || !validRecordingID(id) {
		return nil, ErrInvalidOwnershipToken
	}
	if err := m.beginStart(&owner); err != nil {
		return nil, err
	}
	defer m.starts.Done()
	return m.startResolved(ctx, id, adapterID, media, resource, title, provenance, &owner)
}

func (m *Manager) beginStart(owner *OwnershipToken) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errManagerClosed
	}
	if m.canonicalFence != nil {
		if owner == nil {
			return ErrOwnershipRequired
		}
		if !validOwnershipToken(*owner) {
			return ErrInvalidOwnershipToken
		}
	} else if owner != nil {
		return ErrCanonicalFenceRequired
	}
	m.startAttempted = true
	m.starts.Add(1)
	return nil
}

func (m *Manager) startResolved(ctx context.Context, id, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance, owner *OwnershipToken) (*domain.Recording, error) {
	m.startCreationMu.Lock()
	defer m.startCreationMu.Unlock()
	if owner != nil && (!validOwnershipToken(*owner) || owner.RecordingID != id) {
		return nil, ErrInvalidOwnershipToken
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(adapterID) == "" || strings.ContainsAny(adapterID, "/\\") {
		return nil, fmt.Errorf("adapter id is invalid")
	}
	if err := adapterproto.ValidateResourceRef(resource); err != nil {
		return nil, fmt.Errorf("invalid resource reference")
	}
	var err error
	if err = adapterproto.ValidateMediaSource(media, []string{"hls"}); err != nil {
		return nil, errors.New("adapter returned invalid media source")
	}
	if err = m.validate(ctx, media.ManifestURL); err != nil {
		return nil, fmt.Errorf("invalid resolved media URL")
	}
	acquisitionContext, err := mediaContext(media)
	if err != nil {
		return nil, err
	}
	contextDoc, err := acquisitionContextDocument(acquisitionContext)
	if err != nil {
		return nil, errors.New("recording acquisition context could not be initialized")
	}
	sessionIdentity, err := archiveindex.NewSessionIdentity(id, adapterID, archiveResource(resource), media.SessionRef)
	if err != nil {
		return nil, errors.New("recording source identity is invalid")
	}
	now := time.Now().UTC()
	classification := media.SourceURIClassification()
	recording := &domain.Recording{FormatVersion: 1, ID: id, SourceSessionID: sessionIdentity.ID, Title: title, AdapterID: adapterID, Adapter: archiveProvenance(provenance), Resource: archiveResource(resource), SourceURIClassification: classification, State: domain.StateRecording, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{
		"main": {ID: "main", SourcePlaylistURL: media.ManifestURL, NextArchiveOrdinal: 1, Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
	}}
	if err = m.withOwnershipCommit(owner, func() error {
		if _, loadErr := m.store.LoadRecordingReadOnly(id); loadErr == nil {
			return errors.New("recording already exists")
		} else if !errors.Is(loadErr, storage.ErrNotFound) {
			return errors.New("recording storage is unavailable")
		}
		return m.store.CreateRecordingWithSidecar(recording, acquisitionContextPath, contextDoc)
	}); err != nil {
		return nil, errors.New("recording storage could not be initialized")
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	var ownershipCopy *OwnershipToken
	if owner != nil {
		copy := *owner
		ownershipCopy = &copy
	}
	e := &entry{recording: recording, cancel: cancel, done: make(chan struct{}), media: cloneMediaSource(media), refreshGate: make(chan struct{}, 1), adapterID: adapterID, resource: cloneResourceRef(resource), ownership: ownershipCopy}
	if ownershipCopy != nil {
		e.handoverGate = make(chan struct{}, 1)
		e.handoverGate <- struct{}{}
		e.handoverWake = make(chan struct{}, 1)
	}
	m.mu.Lock()
	m.entries[id] = e
	m.mu.Unlock()
	go m.run(workerCtx, e, cloneMediaSource(media))
	return clone(recording), nil
}

func validOwnershipToken(owner OwnershipToken) bool {
	return validRecordingID(owner.RecordingID) && strings.TrimSpace(owner.EngineGeneration) != "" &&
		strings.TrimSpace(owner.WorkerInstance) != "" && owner.Epoch > 0
}

func (m *Manager) withOwnershipCommit(owner *OwnershipToken, commit func() error) error {
	m.mu.RLock()
	fence := m.canonicalFence
	m.mu.RUnlock()
	if owner == nil {
		if fence != nil {
			return ErrOwnershipRequired
		}
		return commit()
	}
	if !validOwnershipToken(*owner) {
		return ErrInvalidOwnershipToken
	}
	if fence == nil {
		return ErrCanonicalFenceRequired
	}
	return fence.WithCommit(*owner, commit)
}

func (m *Manager) withCanonicalCommit(e *entry, commit func() error) error {
	if e == nil {
		return ErrInvalidOwnershipToken
	}
	// Snapshot the token while holding e.mu. ResumeHandover may replace it as
	// part of a locally authorized ownership transition. No caller of this
	// helper holds e.mu: callers serialize durable mutations with persistMu and
	// acquire e.mu only inside the commit callback when updating memory state.
	e.mu.Lock()
	var owner *OwnershipToken
	if e.ownership != nil {
		copy := *e.ownership
		owner = &copy
	}
	e.mu.Unlock()

	err := m.withOwnershipCommit(owner, commit)
	if owner != nil && (errors.Is(err, recordingowner.ErrNotFound) || errors.Is(err, recordingowner.ErrStaleOwner)) {
		// This is a non-blocking, private marker compiled only into the
		// production-process runtime_e2e acceptance binaries. It observes the
		// real common-fence rejection and never affects authorization.
		_ = runtimehook.Observe(runtimehook.StaleOwnerCommitRejected, owner.RecordingID)
	}
	return err
}

// withCanonicalMutation serializes local root updates before acquiring the
// cross-process ownership fence. Multi-file commits use this helper to hold
// both locks across their complete publication order.
func (m *Manager) withCanonicalMutation(e *entry, commit func() error) error {
	if e == nil {
		return ErrInvalidOwnershipToken
	}
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	return m.withCanonicalCommit(e, commit)
}

func (m *Manager) canonicalFenceConfigured() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.canonicalFence != nil
}

func (m *Manager) Stop(id string) (*domain.Recording, error) {
	return m.StopContext(context.Background(), id)
}

// StopContext requests cancellation immediately and then waits for the worker
// only until ctx ends. A caller timeout does not undo the stop request.
func (m *Manager) StopContext(ctx context.Context, id string) (*domain.Recording, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	e, ok := m.entry(id)
	if !ok {
		return nil, storage.ErrNotFound
	}
	if e.handoverGate != nil {
		if err := acquireHandoverGate(ctx, e); err != nil {
			return nil, err
		}
		defer releaseHandoverGate(e)
	}
	e.mu.Lock()
	if e.handover != nil {
		e.mu.Unlock()
		return nil, ErrHandoverConflict
	}
	cancel := e.cancel
	done := e.done
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	recording, err := m.Get(id)
	e.mu.Lock()
	terminalErr := e.terminalErr
	e.mu.Unlock()
	if terminalErr != nil {
		return recording, terminalErr
	}
	return recording, err
}

// Delete removes one inactive recording from the manager registry and its
// archive. persistMu serializes deletion against root metadata writes while mu
// is held only for state checks/publication. Active acquisitions must be
// stopped explicitly first.
func (m *Manager) Delete(id string) error {
	return m.delete(id, false)
}

// DeleteTerminalArchive lets a current Engine remove a terminal canonical
// archive after the Runtime Host has confirmed that no Engine generation has
// an active worker for it. This keeps Control Plane out of the archive write
// path when the original owner generation has already exited.
func (m *Manager) DeleteTerminalArchive(id string) error {
	return m.delete(id, true)
}

func (m *Manager) delete(id string, allowUnownedTerminal bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[id]
	if !ok {
		if m.freshGeneration && !allowUnownedTerminal {
			// A fresh Engine can read archives from prior generations, but it must
			// never delete data it does not own.
			return storage.ErrNotFound
		}
		if allowUnownedTerminal {
			recording, err := m.store.LoadRecordingReadOnly(id)
			if err != nil {
				return err
			}
			switch recording.State {
			case domain.StateStopped, domain.StateCompleted, domain.StateInterrupted:
			default:
				return ErrActiveRecording
			}
		}
		// Store deletion is idempotent and also handles a tombstone left by an
		// interrupted previous delete. In managed mode, hold the same durable
		// owner lock used by claims and commits while proving no writer owns it.
		deleteArchive := func() error { return m.store.DeleteRecordingData(id) }
		if m.canonicalFence != nil {
			return m.canonicalFence.WithUnownedCommit(id, deleteArchive)
		}
		return deleteArchive()
	}
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	if e.deleted {
		e.mu.Unlock()
		return nil
	}
	if e.recording == nil || e.recording.State == domain.StateRecording {
		e.mu.Unlock()
		return ErrActiveRecording
	}
	e.deleted = true
	e.mu.Unlock()
	deleteArchive := func() error { return m.store.DeleteRecordingData(id) }
	var deleteErr error
	if m.canonicalFence != nil {
		deleteErr = m.canonicalFence.WithUnownedCommit(id, deleteArchive)
	} else {
		deleteErr = deleteArchive()
	}
	if deleteErr != nil {
		e.mu.Lock()
		e.deleted = false
		e.mu.Unlock()
		return deleteErr
	}
	delete(m.entries, id)
	return nil
}

// Close stops admission of new work, cancels active workers and waits for
// their durable terminal state. The caller supplies the overall shutdown
// deadline; a timed-out call may be repeated to continue waiting.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()

	m.startsWait.Do(func() {
		go func() {
			m.starts.Wait()
			close(m.startsDone)
		}()
	})
	select {
	case <-m.startsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.handoverMu.Lock()
	m.mu.Lock()
	prepared := m.prepared
	m.prepared = make(map[string]preparedHandover)
	m.mu.Unlock()
	m.handoverMu.Unlock()
	for _, candidate := range prepared {
		releasePreparedHandover(candidate)
	}

	m.mu.RLock()
	entries := make([]*entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.RUnlock()
	// Cancel every active worker before waiting for any one of them. A worker
	// may take time to finish a request or durably publish its terminal state;
	// waiting inline here could otherwise leave later recordings running when
	// the shutdown deadline expires.
	type workerWait struct {
		e    *entry
		done <-chan struct{}
	}
	waits := make([]workerWait, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		cancel, done := e.cancel, e.done
		active := e.recording.State == domain.StateRecording
		e.mu.Unlock()
		if active && cancel != nil {
			cancel()
		}
		waits = append(waits, workerWait{e: e, done: done})
	}
	// Start closing ingest admission immediately after cancelling acquisitions.
	// IngestService.Close starts its bounded drain coordinator before honoring
	// this caller's deadline; that coordinator also wakes Submit calls which
	// have completed their bodies but have not yet acquired a queue slot.
	if err := m.ingest.Close(ctx); err != nil {
		return err
	}
	for _, worker := range waits {
		select {
		case <-worker.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var terminalErrors []error
	for _, worker := range waits {
		worker.e.mu.Lock()
		if worker.e.terminalErr != nil {
			terminalErrors = append(terminalErrors, worker.e.terminalErr)
		}
		worker.e.mu.Unlock()
	}
	return errors.Join(terminalErrors...)
}

func (m *Manager) Get(id string) (*domain.Recording, error) {
	e, ok := m.entry(id)
	if !ok {
		if m.freshGeneration {
			return m.store.LoadRecordingReadOnly(id)
		}
		return nil, storage.ErrNotFound
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	copy := clone(e.recording)
	if copy == nil {
		return nil, errors.New("recording state could not be copied")
	}
	return copy, nil
}

func (m *Manager) List() []*domain.Recording {
	m.mu.RLock()
	entries := make([]*entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.RUnlock()
	result := make([]*domain.Recording, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		if recording := clone(e.recording); recording != nil {
			result = append(result, recording)
		}
		e.mu.Unlock()
	}
	if m.freshGeneration {
		if archived, err := m.store.LoadAllReadOnly(); err == nil {
			byID := make(map[string]*domain.Recording, len(archived)+len(result))
			for _, recording := range archived {
				byID[recording.ID] = recording
			}
			for _, recording := range result {
				byID[recording.ID] = recording
			}
			result = result[:0]
			for _, recording := range byID {
				result = append(result, recording)
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

// ListForManagement returns a bounded, read-only snapshot for management
// queries. It rejects oversized collections before cloning any recording,
// releases the manager lock before acquiring entry locks, and returns no
// partial snapshot when the caller's context is canceled.
func (m *Manager) ListForManagement(ctx context.Context, max int) ([]*domain.Recording, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	if len(m.entries) > max {
		m.mu.RUnlock()
		return nil, ErrListLimit
	}
	entries := make([]*entry, 0, len(m.entries))
	for _, e := range m.entries {
		if err := ctx.Err(); err != nil {
			m.mu.RUnlock()
			return nil, err
		}
		entries = append(entries, e)
	}
	m.mu.RUnlock()

	result := make([]*domain.Recording, 0, len(entries))
	lockRetry := time.NewTicker(time.Millisecond)
	defer lockRetry.Stop()
	for _, e := range entries {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if e.mu.TryLock() {
				break
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-lockRetry.C:
			}
		}
		if err := ctx.Err(); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		recording := clone(e.recording)
		e.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if recording == nil {
			return nil, errors.New("recording snapshot could not be copied")
		}
		result = append(result, recording)
	}
	if m.freshGeneration {
		archived, err := m.store.LoadAllReadOnlyLimit(max)
		if err != nil {
			if errors.Is(err, storage.ErrReadOnlyListLimit) {
				return nil, ErrListLimit
			}
			return nil, err
		}
		byID := make(map[string]*domain.Recording, len(archived)+len(result))
		for _, recording := range archived {
			byID[recording.ID] = recording
		}
		for _, recording := range result {
			byID[recording.ID] = recording
		}
		if max >= 0 && len(byID) > max {
			return nil, ErrListLimit
		}
		result = make([]*domain.Recording, 0, len(byID))
		for _, recording := range byID {
			result = append(result, recording)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Keep List's newest-first ordering and equivalent timestamp tie behavior.
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// OwnedStates returns only lightweight lifecycle state for this manager's
// worker registry. A candidate manager never includes read-only archive
// snapshots from prior generations.
func (m *Manager) OwnedStates(max int) ([]OwnedRecordingState, error) {
	if max < 0 {
		return nil, ErrListLimit
	}
	m.mu.RLock()
	if max > 0 && len(m.entries) > max {
		m.mu.RUnlock()
		return nil, ErrListLimit
	}
	entries := make([]*entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.RUnlock()
	result := make([]OwnedRecordingState, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		if e.recording != nil {
			result = append(result, OwnedRecordingState{ID: e.recording.ID, State: e.recording.State, StartedAt: e.recording.StartedAt})
		}
		e.mu.Unlock()
	}
	if max > 0 && len(result) > max {
		return nil, ErrListLimit
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StartedAt.After(result[j].StartedAt) })
	return result, nil
}

func (m *Manager) Store() *storage.Store { return m.store }

func (m *Manager) entry(id string) (*entry, bool) {
	m.mu.RLock()
	e, ok := m.entries[id]
	m.mu.RUnlock()
	return e, ok
}

func (m *Manager) update(e *entry, fn func(*domain.Recording) error) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	return m.withCanonicalCommit(e, func() error {
		return m.updateWithinAuthorizedCommit(e, fn)
	})
}

// updateWithinAuthorizedCommit applies and durably publishes a root mutation
// when the caller already holds e.persistMu and the Host commit fence.
func (m *Manager) updateWithinAuthorizedCommit(e *entry, fn func(*domain.Recording) error) error {
	e.mu.Lock()
	if e.deleted {
		e.mu.Unlock()
		return errors.New("recording was deleted")
	}
	next := clone(e.recording)
	e.mu.Unlock()
	if next == nil {
		e.mu.Lock()
		if e.terminalErr == nil {
			e.terminalErr = errors.New("recording state persistence failed")
		}
		e.mu.Unlock()
		return errors.New("recording state could not be copied")
	}
	if err := fn(next); err != nil {
		return err
	}
	if err := m.store.SaveRecording(next); err != nil {
		return errors.New("recording metadata persistence failed")
	}
	e.mu.Lock()
	e.recording = next
	e.mu.Unlock()
	return nil
}

func (m *Manager) run(ctx context.Context, e *entry, media adapterproto.MediaSource) {
	defer close(e.done)
	m.runWorker(ctx, e, media)
	e.mu.Lock()
	staged := e.handoverCandidate
	e.handoverCandidate = nil
	e.mu.Unlock()
	releaseHandoverContinuation(staged)
	// runWorker has joined its metadata monitor and scheduler and has durably
	// published any terminal root state before returning. Release is therefore
	// after the acquisition work is complete; keeping done open until this
	// callback finishes makes Manager.Close and StopContext wait for cleanup.
	e.mu.Lock()
	owner := e.ownership
	terminal := e.recording != nil && e.recording.State != domain.StateRecording
	e.mu.Unlock()
	m.mu.RLock()
	release := m.terminalOwnerRelease
	m.mu.RUnlock()
	if owner != nil && terminal && release != nil {
		_ = release(*owner)
	}
}

func (m *Manager) fail(e *entry, err error) {
	safe := safeFailureDescription(err)
	if updateErr := m.update(e, func(r *domain.Recording) error {
		if errors.Is(err, errStorageCommit) {
			// A source body was already fetched; failure to persist it is not a
			// source gap. Clear pending observations instead of misclassifying
			// backend failure as missing media.
			for _, track := range r.Tracks {
				if track == nil {
					continue
				}
				track.PendingSequences = nil
				track.PendingSegments = nil
			}
		} else {
			m.finalizePending(r, "recording ended with uncaptured media")
		}
		r.State = domain.StateInterrupted
		r.LastError = safe
		now := time.Now().UTC()
		r.StoppedAt = &now
		return nil
	}); updateErr != nil {
		m.setTerminalError(e, errors.New("recording terminal state persistence failed"))
		return
	}
	m.setTerminalError(e, errors.New(safe))
}

func (m *Manager) setTerminalError(e *entry, err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	if e.terminalErr == nil {
		e.terminalErr = err
	}
	e.mu.Unlock()
}

func safeFailureDescription(err error) string {
	if errors.Is(err, errStorageCommit) {
		return "recording storage commit failed"
	}
	var fetchErr *FetchError
	if errors.As(err, &fetchErr) {
		return fetchErr.Error()
	}
	if errors.Is(err, context.Canceled) {
		return "recording stopped"
	}
	return "recording acquisition failed"
}

// clone ensures API callers cannot mutate state protected by the manager.
func clone(r *domain.Recording) *domain.Recording {
	if r == nil {
		return nil
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	var copy domain.Recording
	if err = json.Unmarshal(data, &copy); err != nil {
		return nil
	}
	return &copy
}
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func validRecordingID(id string) bool {
	if len(id) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16 && id == strings.ToLower(id)
}
func closedChannel() chan struct{} { ch := make(chan struct{}); close(ch); return ch }

// The parser functions are variables to keep the acquisition loop easy to
// exercise through local HTTP tests without introducing another abstraction.
var (
	hlsParseMaster = parseMaster
	hlsParseMedia  = parseMedia
	selectVariant  = chooseVariant
)
