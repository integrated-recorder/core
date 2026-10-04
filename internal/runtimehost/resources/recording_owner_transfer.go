package resources

import (
	"errors"

	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
)

// Transfer moves one active Recording's Host-authorized writer from a paused
// source Engine to a ready target Engine. Callers must first prepare the target
// read-only and pause/drain the source. The durable owner file is authoritative;
// the registry lease is updated as the corresponding retirement projection.
func (a *RecordingOwnerAuthority) Transfer(expected recordingowner.Owner, targetResourceOwnerID string) (recordingowner.Owner, error) {
	return a.transfer(expected, targetResourceOwnerID, false)
}

// RollbackTransfer fences an ambiguously activated target before restoring
// ownership to its already-paused source. The source generation may be
// draining because the target was activated as the new default in a separate
// operation. Callers must confirm the source process remains alive and parked.
func (a *RecordingOwnerAuthority) RollbackTransfer(expected recordingowner.Owner, targetResourceOwnerID string) (recordingowner.Owner, error) {
	return a.transfer(expected, targetResourceOwnerID, true)
}

func (a *RecordingOwnerAuthority) transfer(expected recordingowner.Owner, targetResourceOwnerID string, rollback bool) (recordingowner.Owner, error) {
	if a == nil || a.store == nil || a.registry == nil {
		return recordingowner.Owner{}, ErrOwnerAuthorityUnavailable
	}
	if !validRecordingID(expected.RecordingID) || !engineIdentityPattern.MatchString(expected.EngineGeneration) ||
		!engineIdentityPattern.MatchString(expected.WorkerInstance) || expected.Epoch == 0 ||
		validateIDs(targetResourceOwnerID) != nil || len(targetResourceOwnerID) < 2 || targetResourceOwnerID[0] != 'e' {
		return recordingowner.Owner{}, recordingowner.ErrInvalidIdentity
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	tracked, ok := a.owners[expected.RecordingID]
	if !ok || tracked.owner != expected {
		return recordingowner.Owner{}, recordingowner.ErrStaleOwner
	}
	sourceRegistration, ok := a.engines[tracked.resourceID]
	if !ok || sourceRegistration.generation != expected.EngineGeneration || sourceRegistration.instance != expected.WorkerInstance {
		return recordingowner.Owner{}, recordingowner.ErrStaleOwner
	}
	targetRegistration, ok := a.engines[targetResourceOwnerID]
	if !ok || (targetRegistration.generation == expected.EngineGeneration && targetRegistration.instance == expected.WorkerInstance) {
		return recordingowner.Owner{}, ErrUnregisteredEngine
	}

	current, err := a.store.Current(expected.RecordingID)
	if err != nil {
		return recordingowner.Owner{}, err
	}
	if current != expected {
		return recordingowner.Owner{}, recordingowner.ErrStaleOwner
	}
	snapshot := a.registry.Snapshot()
	lease, ok := snapshot.Leases[expected.RecordingID]
	if !ok || lease.EngineGeneration != expected.EngineGeneration || lease.WorkerInstance != expected.WorkerInstance {
		return recordingowner.Owner{}, ErrOwnerLeaseMismatch
	}
	targetGeneration, ok := snapshot.Generations[targetRegistration.generation]
	targetReady := ok && targetGeneration.State == generation.StateActive && snapshot.ActiveGenerationID == targetRegistration.generation
	if rollback {
		targetReady = ok && targetGeneration.State == generation.StateDraining && snapshot.ActiveGenerationID != targetRegistration.generation
	}
	if !targetReady || targetGeneration.EngineDormant {
		return recordingowner.Owner{}, generation.ErrInvalidTransition
	}

	// Store.Transfer uses the same cross-process lock as every canonical commit.
	// At this point the source Engine has already drained accepted writes; this
	// durable tuple change is the only operation that revokes its write token.
	next, err := a.store.Transfer(expected, targetRegistration.generation, targetRegistration.instance)
	if err != nil {
		return recordingowner.Owner{}, err
	}
	// Store.Transfer has released the canonical owner lock. A crash here leaves
	// the durable owner authoritative and the registry lease as a stale
	// retirement projection; cold Host recovery fences the owner and rebuilds
	// leases only after Engine recovery.
	if err := runtimehook.Pause(runtimehook.AfterOwnerCAS, expected.RecordingID); err != nil {
		tracked.owner = next
		tracked.resourceID = targetResourceOwnerID
		tracked.startedAt = lease.StartedAt
		a.owners[expected.RecordingID] = tracked
		return next, err
	}
	targetLease := generation.Lease{
		RecordingID: expected.RecordingID, EngineGeneration: targetRegistration.generation,
		WorkerInstance: targetRegistration.instance, StartedAt: lease.StartedAt,
	}
	var leaseErr error
	if rollback {
		leaseErr = a.registry.RollbackRecordingTransfer(lease, targetLease)
	} else {
		leaseErr = a.registry.TransferRecording(lease, targetLease)
	}
	if leaseErr != nil {
		// A failed lease projection must not strand an otherwise paused source.
		// Revoke the just-issued target epoch and restore the source with a newer
		// token. If rollback itself fails, keep the durable target owner in memory
		// and fail closed: callers must not resume the stale source token.
		rollback, rollbackErr := a.store.Transfer(next, expected.EngineGeneration, expected.WorkerInstance)
		if rollbackErr == nil {
			tracked.owner = rollback
			tracked.startedAt = lease.StartedAt
			a.owners[expected.RecordingID] = tracked
			return recordingowner.Owner{}, errors.Join(leaseErr, errors.New("source owner token was advanced after registry transfer failure"))
		}
		tracked.owner = next
		tracked.resourceID = targetResourceOwnerID
		a.owners[expected.RecordingID] = tracked
		return next, errors.Join(leaseErr, rollbackErr)
	}
	tracked.owner = next
	tracked.resourceID = targetResourceOwnerID
	tracked.startedAt = lease.StartedAt
	a.owners[expected.RecordingID] = tracked
	return next, nil
}

// CurrentOwner returns the Host's current durable owner token for a known
// Recording. It is used only by internal handover recovery paths; public APIs
// must not expose owner tokens.
func (a *RecordingOwnerAuthority) CurrentOwner(recordingID string) (recordingowner.Owner, error) {
	if a == nil || a.store == nil {
		return recordingowner.Owner{}, ErrOwnerAuthorityUnavailable
	}
	if !validRecordingID(recordingID) {
		return recordingowner.Owner{}, recordingowner.ErrInvalidIdentity
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	tracked, ok := a.owners[recordingID]
	if !ok {
		return recordingowner.Owner{}, recordingowner.ErrNotFound
	}
	current, err := a.store.Current(recordingID)
	if err != nil {
		return recordingowner.Owner{}, err
	}
	if current != tracked.owner {
		return recordingowner.Owner{}, recordingowner.ErrStaleOwner
	}
	return current, nil
}

// MatchesDurableOwner checks the on-disk fencing tuple without taking the
// authority mutex. It is safe inside WithProtectedLeases callbacks, which
// intentionally hold that mutex while reconciling Engine inventory.
func (a *RecordingOwnerAuthority) MatchesDurableOwner(expected recordingowner.Owner) bool {
	if a == nil || a.store == nil || !validRecordingID(expected.RecordingID) {
		return false
	}
	current, err := a.store.Current(expected.RecordingID)
	return err == nil && current == expected
}
