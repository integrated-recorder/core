package acquire

import (
	"context"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

// ErrLifecycleConflict means the requested lifecycle transition does not
// apply to the Recording's current capture state.
var ErrLifecycleConflict = errors.New("recording lifecycle transition conflicts with current state")

// LifecycleSnapshot contains bounded lifecycle state for management reads.
// EngineOwns and OwnerEpoch are private routing hints and must not be exposed
// by public APIs.
type LifecycleSnapshot struct {
	RecordingID      string                `json:"recording_id"`
	CaptureState     domain.RecordingState `json:"capture_state"`
	ArchiveSealed    bool                  `json:"archive_sealed"`
	Repairable       bool                  `json:"repairable"`
	RecoveryState    string                `json:"recovery_state"`
	TimelineRevision uint64                `json:"timeline_revision"`
	ArchiveRevision  uint64                `json:"archive_revision"`
	EngineOwns       bool                  `json:"engine_owns"`
	OwnerEpoch       uint64                `json:"owner_epoch"`
}

// LifecycleSnapshot reads lifecycle fields under the entry lock. It does not
// clone canonical recording history.
func (m *Manager) LifecycleSnapshot(ctx context.Context, id string) (LifecycleSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return LifecycleSnapshot{}, err
	}
	e, ok := m.entry(id)
	if !ok {
		return LifecycleSnapshot{}, storage.ErrNotFound
	}
	e.mu.Lock()
	if e.deleted || e.recording == nil {
		e.mu.Unlock()
		return LifecycleSnapshot{}, storage.ErrNotFound
	}
	view := LifecycleSnapshot{
		RecordingID: id, CaptureState: e.recording.State,
		ArchiveSealed: e.recording.ArchiveSealed, Repairable: !e.recording.ArchiveSealed,
		TimelineRevision: e.recording.TimelineRevision,
		ArchiveRevision:  e.recording.ArchiveRevision, EngineOwns: e.ownership != nil,
	}
	if e.ownership != nil {
		view.OwnerEpoch = e.ownership.Epoch
	}
	sealed := view.ArchiveSealed
	e.mu.Unlock()

	if sealed {
		view.RecoveryState = "sealed"
		return view, nil
	}
	m.mu.RLock()
	scheduler := m.autoRecovery
	m.mu.RUnlock()
	view.RecoveryState = "idle"
	if scheduler != nil {
		view.RecoveryState = scheduler.stateFor(id)
	}
	return view, nil
}

// CompleteRecording explicitly marks a stopped capture as completed. It does
// not seal the archive. The recovery gate is acquired before owner claim and
// canonical fencing so a historical pass cannot race this root transition.
func (m *Manager) CompleteRecording(ctx context.Context, id string) (*domain.Recording, error) {
	e, ok := m.entry(id)
	if !ok {
		return nil, storage.ErrNotFound
	}
	if err := acquireArchiveRecoveryGate(ctx, e); err != nil {
		return nil, err
	}
	defer releaseArchiveRecoveryGate(e)

	state, err := terminalLifecycleState(e)
	if err != nil {
		return nil, err
	}
	if state == domain.StateCompleted {
		m.releaseUnrecoverableTerminalOwner(ctx, e)
		return m.Get(id)
	}
	if state != domain.StateStopped {
		return nil, ErrLifecycleConflict
	}

	owner, claimed, err := m.lifecycleOwner(ctx, e, id)
	if err != nil {
		return nil, err
	}
	var ownerPtr *OwnershipToken
	if owner != nil {
		ownerPtr = owner
	}
	err = m.withCanonicalMutationOwner(e, ownerPtr, true, func() error {
		return m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
			if recording.State != domain.StateStopped {
				return ErrLifecycleConflict
			}
			recording.State = domain.StateCompleted
			return nil
		})
	})
	if err != nil {
		if claimed {
			return nil, errors.Join(err, m.releaseLifecycleOwner(ctx, e, *owner))
		}
		return nil, err
	}
	if owner != nil && !m.retainTerminalRecoveryOwner(e, *owner) {
		if releaseErr := m.releaseLifecycleOwner(ctx, e, *owner); releaseErr != nil {
			return nil, releaseErr
		}
	}
	m.signalAutomaticArchiveRecovery(id)
	return m.Get(id)
}

// SealArchiveContext closes archive repair admission for a terminal capture.
// Capture completion remains a separate transition. The method serializes
// against historical work, then claims or reuses the exact owner token before
// the canonical commit fence.
func (m *Manager) SealArchiveContext(ctx context.Context, id string) error {
	e, ok := m.entry(id)
	if !ok {
		return storage.ErrNotFound
	}
	if err := acquireArchiveRecoveryGate(ctx, e); err != nil {
		return err
	}
	defer releaseArchiveRecoveryGate(e)

	state, err := terminalLifecycleState(e)
	if err != nil {
		return err
	}
	if state == domain.StateRecording {
		return ErrLifecycleConflict
	}
	if archiveSealed(e) {
		m.releaseUnrecoverableTerminalOwner(ctx, e)
		return nil
	}

	owner, claimed, err := m.lifecycleOwner(ctx, e, id)
	if err != nil {
		return err
	}
	if owner == nil && m.canonicalFenceConfigured() {
		return ErrOwnershipRequired
	}
	if err := m.sealArchiveUnderRecoveryGate(e, owner); err != nil {
		if claimed && owner != nil {
			return errors.Join(err, m.releaseLifecycleOwner(ctx, e, *owner))
		}
		return err
	}
	if owner != nil {
		if err := m.releaseLifecycleOwner(ctx, e, *owner); err != nil {
			return err
		}
	}
	m.signalAutomaticArchiveRecovery(id)
	return nil
}

func terminalLifecycleState(e *entry) (domain.RecordingState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil {
		return "", storage.ErrNotFound
	}
	if e.handover != nil || e.cancel != nil || !channelClosed(e.done) {
		return e.recording.State, ErrLifecycleConflict
	}
	switch e.recording.State {
	case domain.StateRecording, domain.StateStopped, domain.StateCompleted, domain.StateInterrupted:
		return e.recording.State, nil
	default:
		return e.recording.State, ErrLifecycleConflict
	}
}

func archiveSealed(e *entry) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.recording != nil && e.recording.ArchiveSealed
}

func (m *Manager) lifecycleOwner(ctx context.Context, e *entry, id string) (*OwnershipToken, bool, error) {
	e.mu.Lock()
	if e.ownership != nil {
		owner := *e.ownership
		e.mu.Unlock()
		return &owner, false, nil
	}
	e.mu.Unlock()

	m.mu.RLock()
	scheduler := m.autoRecovery
	fenced := m.canonicalFence != nil
	m.mu.RUnlock()
	if scheduler != nil {
		owner, err := scheduler.claim(ctx, id)
		if err != nil {
			return nil, false, err
		}
		return &owner, true, nil
	}
	if fenced {
		return nil, false, ErrOwnershipRequired
	}
	return nil, false, nil
}

func (m *Manager) releaseLifecycleOwner(ctx context.Context, e *entry, owner OwnershipToken) error {
	if err := m.releaseAutomaticRecoveryOwner(ctx, e, owner); err != nil {
		m.retainAutomaticRecoveryRelease(e, owner, time.Time{}, false)
		m.mu.RLock()
		scheduler := m.autoRecovery
		m.mu.RUnlock()
		if scheduler != nil {
			scheduler.retainAutomaticRecoveryReleaseForRetry(owner.RecordingID, owner, false)
		}
		return err
	}
	return nil
}

func (m *Manager) releaseUnrecoverableTerminalOwner(ctx context.Context, e *entry) {
	e.mu.Lock()
	if e.ownership == nil {
		e.mu.Unlock()
		return
	}
	owner := *e.ownership
	e.mu.Unlock()
	if !m.retainTerminalRecoveryOwner(e, owner) {
		_ = m.releaseLifecycleOwner(ctx, e, owner)
	}
}
