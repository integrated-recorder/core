package pluginregistry

import (
	"sync"
)

// Plan is a single-owner transaction prepared under the Manager operation
// gate. SourceDir is an immutable desired-source snapshot, not a mutable
// download location.
type Plan struct {
	manager    *Manager
	previous   desiredState
	proposed   desiredState
	sourceDir  string
	selection  DesiredPlugin
	mu         sync.Mutex
	closed     bool
	committed  bool
	rolledBack bool
	commitRev  uint64
	closeOnce  sync.Once
}

func (p *Plan) SourceDir() string {
	if p == nil {
		return ""
	}
	return p.sourceDir
}

// Selection returns the exact registry-approved artifact selected by an
// install/update plan. It is the zero value for an uninstall plan.
func (p *Plan) Selection() DesiredPlugin {
	if p == nil {
		return DesiredPlugin{}
	}
	selection := p.selection
	if p.selection.Attestation != nil {
		attestation := *p.selection.Attestation
		selection.Attestation = &attestation
	}
	return selection
}

// Commit atomically makes the proposed plugin selection the durable desired
// state. The revision and source-set identity are compared with the state
// observed during Prepare to reject stale plans.
func (p *Plan) Commit() error {
	if p == nil || p.manager == nil {
		return ErrInvalidConfig
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.rolledBack {
		return ErrOperationConflict
	}
	if p.committed {
		return nil
	}
	current, err := p.manager.readCurrentDesired()
	if err != nil {
		return ErrUnsafeStore
	}
	if current.Revision != p.previous.Revision || current.SourceSetID != p.previous.SourceSetID {
		return ErrOperationConflict
	}
	if current.Revision == ^uint64(0) {
		return ErrInstallFailed
	}
	proposed := p.proposed
	proposed.Revision = current.Revision + 1
	if err := p.manager.writeDesired(proposed); err != nil {
		return ErrInstallFailed
	}
	p.proposed = proposed
	p.commitRev = proposed.Revision
	p.committed = true
	p.manager.updateDesired(proposed)
	return nil
}

// Rollback restores the previous selection only while this plan's committed
// state remains current. The revision itself never moves backwards.
func (p *Plan) Rollback() error {
	if p == nil || p.manager == nil {
		return ErrInvalidConfig
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrOperationConflict
	}
	if p.rolledBack || !p.committed {
		return nil
	}
	current, err := p.manager.readCurrentDesired()
	if err != nil {
		return ErrUnsafeStore
	}
	if current.Revision != p.commitRev || current.SourceSetID != p.proposed.SourceSetID || !equalDesired(current.Plugins, p.proposed.Plugins) {
		return ErrOperationConflict
	}
	if current.Revision == ^uint64(0) {
		return ErrInstallFailed
	}
	previous := p.previous
	previous.Revision = current.Revision + 1
	if err := p.manager.writeDesired(previous); err != nil {
		return ErrInstallFailed
	}
	p.manager.updateDesired(previous)
	p.committed = false
	p.rolledBack = true
	return nil
}

// Close releases the Manager's in-memory serialization gate. It does not
// implicitly commit or roll back durable desired state.
func (p *Plan) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		manager := p.manager
		p.mu.Unlock()
		if manager != nil {
			manager.release()
		}
	})
}
