package resources

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
)

var (
	ErrOwnerAuthorityUnavailable = errors.New("recording owner authority is unavailable")
	ErrUnregisteredEngine        = errors.New("recording owner process is not a registered Engine")
	ErrOwnerRegistration         = errors.New("recording owner process registration is invalid")
	ErrOwnerLeaseMismatch        = errors.New("recording owner lease does not match")

	engineIdentityPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

const releasedOwnerHistoryLimit = 4096

type engineRegistration struct {
	generation string
	instance   string
}

type trackedOwner struct {
	owner      recordingowner.Owner
	resourceID string
	startedAt  time.Time
}

// RecordingOwnerAuthority is the Host-only bridge between authenticated
// Runtime resource IPC callers, the generation lease registry, and the
// durable cross-process canonical commit fence. Callers must register a
// process only after its Engine readiness identity has been confirmed.
type RecordingOwnerAuthority struct {
	mu          sync.Mutex
	store       *recordingowner.Store
	registry    *generation.Registry
	engines     map[string]engineRegistration
	owners      map[string]trackedOwner
	released    map[recordingowner.Owner]struct{}
	releaseFIFO []recordingowner.Owner
}

func NewRecordingOwnerAuthority(store *recordingowner.Store, registry *generation.Registry) (*RecordingOwnerAuthority, error) {
	if store == nil || registry == nil {
		return nil, ErrOwnerAuthorityUnavailable
	}
	return &RecordingOwnerAuthority{
		store: store, registry: registry,
		engines: make(map[string]engineRegistration), owners: make(map[string]trackedOwner),
		released: make(map[recordingowner.Owner]struct{}),
	}, nil
}

// RegisterEngine binds one Host-created Runtime resource identity to the
// generation and process instance confirmed by Engine readiness. Candidate
// Engines can be registered while staged; Registry.PinRecording still rejects
// their claims until their generation becomes active.
func (a *RecordingOwnerAuthority) RegisterEngine(resourceOwnerID, generationID, instanceID string) error {
	if a == nil || a.store == nil || a.registry == nil {
		return ErrOwnerAuthorityUnavailable
	}
	if validateIDs(resourceOwnerID) != nil || len(resourceOwnerID) < 2 || resourceOwnerID[0] != 'e' ||
		!engineIdentityPattern.MatchString(generationID) || !engineIdentityPattern.MatchString(instanceID) {
		return ErrOwnerRegistration
	}
	snapshot := a.registry.Snapshot()
	generationRecord, ok := snapshot.Generations[generationID]
	if !ok {
		return ErrOwnerRegistration
	}
	if !generationRecord.SupportsCurrentArchiveFormat() {
		return generation.ErrArchiveFormatUnsupported
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.engines[resourceOwnerID]; exists {
		return ErrOwnerRegistration
	}
	a.engines[resourceOwnerID] = engineRegistration{generation: generationID, instance: instanceID}
	return nil
}

// UnregisterEngine removes only the resource identity for a confirmed-dead
// child. Durable Recording owners are deliberately retained for recovery.
func (a *RecordingOwnerAuthority) UnregisterEngine(resourceOwnerID, generationID, instanceID string) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	current, exists := a.engines[resourceOwnerID]
	if !exists {
		return nil
	}
	if current.generation != generationID || current.instance != instanceID {
		return ErrOwnerRegistration
	}
	delete(a.engines, resourceOwnerID)
	return nil
}

func (a *RecordingOwnerAuthority) Claim(resourceOwnerID, recordingID string) (recordingowner.Owner, error) {
	if a == nil || a.store == nil || a.registry == nil {
		return recordingowner.Owner{}, ErrOwnerAuthorityUnavailable
	}
	if validateIDs(resourceOwnerID) != nil || (recordingID != "" && !validRecordingID(recordingID)) {
		return recordingowner.Owner{}, recordingowner.ErrInvalidIdentity
	}
	if recordingID == "" {
		var err error
		recordingID, err = newRecordingID()
		if err != nil {
			return recordingowner.Owner{}, errors.New("recording owner identity could not be generated")
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	registration, ok := a.engines[resourceOwnerID]
	if !ok {
		return recordingowner.Owner{}, ErrUnregisteredEngine
	}
	startedAt := time.Now().UTC()
	lease := generation.Lease{
		RecordingID: recordingID, EngineGeneration: registration.generation,
		WorkerInstance: registration.instance, StartedAt: startedAt,
	}
	if err := a.registry.PinRecording(lease); err != nil {
		return recordingowner.Owner{}, err
	}
	owner, err := a.store.Claim(recordingID, registration.generation, registration.instance)
	if err != nil {
		_ = a.registry.ReleaseRecordingOwner(recordingID, registration.generation, registration.instance)
		return recordingowner.Owner{}, err
	}
	a.owners[recordingID] = trackedOwner{owner: owner, resourceID: resourceOwnerID, startedAt: startedAt}
	return owner, nil
}

func (a *RecordingOwnerAuthority) Release(resourceOwnerID string, expected recordingowner.Owner) error {
	if a == nil || a.store == nil || a.registry == nil {
		return ErrOwnerAuthorityUnavailable
	}
	if validateIDs(resourceOwnerID) != nil || !validRecordingID(expected.RecordingID) ||
		!engineIdentityPattern.MatchString(expected.EngineGeneration) || !engineIdentityPattern.MatchString(expected.WorkerInstance) || expected.Epoch == 0 {
		return recordingowner.ErrInvalidIdentity
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	registration, ok := a.engines[resourceOwnerID]
	if !ok {
		return ErrUnregisteredEngine
	}
	if registration.generation != expected.EngineGeneration || registration.instance != expected.WorkerInstance {
		return recordingowner.ErrStaleOwner
	}
	if _, alreadyReleased := a.released[expected]; alreadyReleased {
		return nil
	}
	tracked, trackedOK := a.owners[expected.RecordingID]
	if trackedOK && (tracked.owner != expected || tracked.resourceID != resourceOwnerID) {
		return recordingowner.ErrStaleOwner
	}
	lease, leaseOK := a.registry.Snapshot().Leases[expected.RecordingID]
	if !leaseOK || lease.EngineGeneration != expected.EngineGeneration || lease.WorkerInstance != expected.WorkerInstance {
		return ErrOwnerLeaseMismatch
	}
	current, currentErr := a.store.Current(expected.RecordingID)
	if currentErr == nil && current != expected {
		return recordingowner.ErrStaleOwner
	}
	if currentErr != nil && !errors.Is(currentErr, recordingowner.ErrNotFound) {
		return currentErr
	}
	if currentErr == nil {
		if err := a.store.Release(expected); err != nil && !errors.Is(err, recordingowner.ErrNotFound) {
			return err
		}
	} else if !trackedOK {
		return recordingowner.ErrNotFound
	}
	if err := a.registry.ReleaseRecordingOwner(expected.RecordingID, expected.EngineGeneration, expected.WorkerInstance); err != nil {
		return err
	}
	delete(a.owners, expected.RecordingID)
	a.rememberRelease(expected)
	return nil
}

func (a *RecordingOwnerAuthority) rememberRelease(owner recordingowner.Owner) {
	a.released[owner] = struct{}{}
	a.releaseFIFO = append(a.releaseFIFO, owner)
	if len(a.releaseFIFO) > releasedOwnerHistoryLimit {
		oldest := a.releaseFIFO[0]
		a.releaseFIFO = a.releaseFIFO[1:]
		delete(a.released, oldest)
	}
}

// WithProtectedLeases serializes inventory reconciliation against the short
// interval between Host pinning and Engine canonical creation, plus active
// owner release. The callback must perform its registry reconciliation while
// this lock is held. This prevents a stale pre-create inventory snapshot from
// dropping a freshly pinned Engine lease.
func (a *RecordingOwnerAuthority) WithProtectedLeases(reconcile func([]generation.Lease) error) error {
	if a == nil || reconcile == nil {
		return ErrOwnerAuthorityUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	protected := make([]generation.Lease, 0, len(a.owners))
	for _, tracked := range a.owners {
		registration, ok := a.engines[tracked.resourceID]
		if !ok || registration.generation != tracked.owner.EngineGeneration || registration.instance != tracked.owner.WorkerInstance {
			continue
		}
		protected = append(protected, generation.Lease{
			RecordingID: tracked.owner.RecordingID, EngineGeneration: tracked.owner.EngineGeneration,
			WorkerInstance: tracked.owner.WorkerInstance, StartedAt: tracked.startedAt,
		})
	}
	sort.Slice(protected, func(i, j int) bool { return protected[i].RecordingID < protected[j].RecordingID })
	return reconcile(protected)
}

func newRecordingID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func ownerIPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrUnregisteredEngine):
		return resourceIPCError("engine_not_registered", "recording Engine is not authorized")
	case errors.Is(err, generation.ErrInvalidTransition):
		return resourceIPCError("engine_not_active", "recording Engine generation is not active")
	case errors.Is(err, generation.ErrLeaseExists), errors.Is(err, recordingowner.ErrAlreadyOwned), errors.Is(err, recordingowner.ErrStaleOwner), errors.Is(err, ErrOwnerLeaseMismatch):
		return resourceIPCError("recording_owner_conflict", "recording ownership does not match")
	case errors.Is(err, generation.ErrGenerationNotFound), errors.Is(err, recordingowner.ErrNotFound):
		return resourceIPCError("recording_owner_not_found", "recording ownership was not found")
	case errors.Is(err, generation.ErrArchiveFormatUnsupported):
		return resourceIPCError("archive_format_unsupported", "archive format is unsupported")
	case errors.Is(err, recordingowner.ErrInvalidIdentity), errors.Is(err, generation.ErrInvalidState):
		return resourceIPCError("invalid_request", "recording owner request is invalid")
	default:
		return resourceIPCError("recording_owner_failed", "recording ownership operation failed")
	}
}
