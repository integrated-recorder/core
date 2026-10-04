// Package generation persists the Runtime Host's view of immutable
// application generations and the recordings pinned to their engines.
// This is runtime-management state only; it is never written into a recording
// archive.
package generation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dltkddnr04/integrated-recorder/internal/runtimehook"
)

const (
	SchemaVersion  = 1
	maxStateBytes  = 1 << 20
	maxGenerations = 256
	maxLeases      = 10000
	// MaxInventoryRecordings bounds one Engine inventory and the aggregate
	// active Recording lease projection.
	MaxInventoryRecordings = maxLeases
)

var (
	ErrInvalidState       = errors.New("invalid generation registry state")
	ErrInvalidTransition  = errors.New("invalid generation state transition")
	ErrGenerationExists   = errors.New("generation already exists")
	ErrGenerationNotFound = errors.New("generation not found")
	ErrLeaseExists        = errors.New("recording already has a generation lease")
	ErrLeaseNotFound      = errors.New("recording generation lease not found")
	ErrLeaseMismatch      = errors.New("recording generation lease does not match")
	ErrInvalidInventory   = errors.New("engine recording inventory is invalid")
	ErrUnsafePath         = errors.New("unsafe generation registry path")

	// IDs are deliberately opaque and path-safe. UUIDs and 32-64 digit hex
	// identifiers are accepted; names, slashes, and version strings are not.
	idPattern           = regexp.MustCompile(`^(?:[0-9a-fA-F]{32,64}|[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12})$`)
	adapterSetIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	storageSetIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// State describes one generation's lifecycle.
type State string

const (
	StateStaging  State = "staging"
	StateVerified State = "verified"
	StateReady    State = "ready"
	StateActive   State = "active"
	StateDraining State = "draining"
	StateRetired  State = "retired"
	StateFailed   State = "failed"
)

// CompatibilityRange is an inclusive archive read compatibility range.
type CompatibilityRange struct {
	Minimum int `json:"minimum"`
	Maximum int `json:"maximum"`
}

// Generation is immutable release identity plus its mutable host lifecycle
// state. Release identity fields are copied from a verified release manifest.
type Generation struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Commit  string `json:"commit"`
	// AdapterSetID pins this generation to an immutable Runtime Host adapter
	// snapshot. Empty remains valid for records written before adapter sets
	// became part of generation identity.
	AdapterSetID string `json:"adapter_set_id,omitempty"`
	// StorageProviderSetID pins this generation to an immutable Storage
	// Provider executable set. Empty is read only for legacy registry records
	// and must be adopted by Runtime Host before any production process starts.
	StorageProviderSetID string    `json:"storage_provider_set_id,omitempty"`
	InstalledAt          time.Time `json:"installed_at"`
	State                State     `json:"state"`
	// EngineDormant records that this generation has no active Host-authorized
	// Engine attachment. After cold recovery, an orphan OS process may still be
	// alive; it is not an authorized writer unless it is reattached by the Host.
	// The immutable release remains installed and can be restarted if this
	// generation is rolled back.
	EngineDormant            bool               `json:"engine_dormant,omitempty"`
	ControlProtocol          int                `json:"control_protocol"`
	EngineProtocol           int                `json:"engine_protocol"`
	ArchiveReadCompatibility CompatibilityRange `json:"archive_read_compatibility"`
	ArchiveWriteEpoch        int                `json:"archive_write_epoch"`
}

// Lease is the Runtime Host's retirement reference to the Engine generation
// currently serving one active Recording. A fenced handover may move the
// lease after canonical-writer ownership transfers; StartedAt remains the
// immutable original Recording start time. Worker identity is runtime
// metadata, not canonical archive provenance.
type Lease struct {
	RecordingID      string    `json:"recording_id"`
	EngineGeneration string    `json:"engine_generation"`
	WorkerInstance   string    `json:"worker_instance"`
	StartedAt        time.Time `json:"started_at"`
}

// InventoryRecording is one active Recording observed by a Recorder Engine.
// It is an IPC projection and is not persisted independently of its Lease.
type InventoryRecording struct {
	RecordingID string    `json:"recording_id"`
	StartedAt   time.Time `json:"started_at"`
}

// EngineInventory is a successfully confirmed active-recording inventory
// from one Engine process. ObservedAt bounds the timestamps in the inventory;
// it is not itself persisted in the generation registry.
type EngineInventory struct {
	Confirmed        bool                 `json:"confirmed"`
	EngineGeneration string               `json:"engine_generation"`
	WorkerInstance   string               `json:"worker_instance"`
	ObservedAt       time.Time            `json:"observed_at"`
	Recordings       []InventoryRecording `json:"recordings"`
}

// Snapshot is the complete bounded registry state persisted by the Runtime
// Host. Callers receive independent maps from Registry.Snapshot.
type Snapshot struct {
	SchemaVersion        int    `json:"schema_version"`
	ActiveGenerationID   string `json:"active_generation_id,omitempty"`
	PreviousGenerationID string `json:"previous_generation_id,omitempty"`
	// ActivationPreviousGenerationID preserves the pre-activation rollback
	// target until the Host confirms that routing and candidate admission both
	// succeeded. It is empty for settled registry state and is additive for old
	// state files.
	ActivationPreviousGenerationID string                `json:"activation_previous_generation_id,omitempty"`
	StagedGenerationID             string                `json:"staged_generation_id,omitempty"`
	Generations                    map[string]Generation `json:"generations"`
	Leases                         map[string]Lease      `json:"leases"`
}

// Registry serializes host-generation transitions and only publishes a
// changed snapshot in memory after it has been durably written.
type Registry struct {
	mu    sync.RWMutex
	path  string
	state Snapshot
}

// Open loads or creates a private registry at the caller-provided runtime
// state path. The containing directory is made private (0700), and the state
// file is always published with mode 0600.
func Open(path string) (*Registry, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: empty registry path", ErrUnsafePath)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve registry path", ErrUnsafePath)
	}
	abs = filepath.Clean(abs)
	if filepath.Base(abs) == "." || filepath.Base(abs) == string(filepath.Separator) {
		return nil, fmt.Errorf("%w: registry filename is missing", ErrUnsafePath)
	}
	parent := filepath.Dir(abs)
	if err := ensurePrivateDirectory(parent); err != nil {
		return nil, err
	}
	// Use the resolved private directory so a symlink in the caller's path
	// cannot redirect the state file after validation.
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve registry directory", ErrUnsafePath)
	}
	registryPath := filepath.Join(resolvedParent, filepath.Base(abs))
	info, err := os.Lstat(registryPath)
	if errors.Is(err, os.ErrNotExist) {
		registry := &Registry{path: registryPath, state: emptySnapshot()}
		if err := registry.persist(registry.state); err != nil {
			return nil, fmt.Errorf("initialize generation registry: %w", err)
		}
		return registry, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect generation registry: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maxStateBytes {
		return nil, fmt.Errorf("%w: registry file is unsafe or exceeds size limit", ErrInvalidState)
	}
	if err := os.Chmod(registryPath, 0600); err != nil {
		return nil, fmt.Errorf("secure generation registry file: %w", err)
	}
	data, err := os.ReadFile(registryPath)
	if err != nil {
		return nil, fmt.Errorf("read generation registry: %w", err)
	}
	var state Snapshot
	if err := decodeStrict(data, &state); err != nil {
		return nil, fmt.Errorf("decode generation registry: %w", err)
	}
	if err := validateSnapshot(state); err != nil {
		return nil, err
	}
	return &Registry{path: registryPath, state: cloneSnapshot(state)}, nil
}

// Snapshot returns an independent copy of the durable host registry state.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneSnapshot(r.state)
}

// AdoptLegacyStorageProviderSet pins generations created before storage
// provider sets were part of generation identity to the verified bundled
// local provider set. The operation is one durable registry transaction: it
// only fills empty legacy fields and never changes an already-pinned set.
// Hosts call this before starting application processes, so no generation can
// continue through the former unpinned local-filesystem path.
func (r *Registry) AdoptLegacyStorageProviderSet(setID string) error {
	if !storageSetIDPattern.MatchString(setID) {
		return invalid("legacy storage provider set identity is invalid")
	}
	return r.change(func(next *Snapshot) error {
		for id, item := range next.Generations {
			if item.StorageProviderSetID == "" {
				item.StorageProviderSetID = setID
				next.Generations[id] = item
			}
		}
		return nil
	})
}

// Stage records an installed but not yet verified candidate generation.
func (r *Registry) Stage(g Generation) error {
	if err := validateGeneration(g); err != nil {
		return err
	}
	if g.State != StateStaging {
		return transition("new generation must start in staging")
	}
	return r.change(func(next *Snapshot) error {
		if len(next.Generations) >= maxGenerations {
			return invalid("generation registry limit reached")
		}
		if _, exists := next.Generations[g.ID]; exists {
			return ErrGenerationExists
		}
		if next.StagedGenerationID != "" {
			return transition("another candidate is already staged")
		}
		next.Generations[g.ID] = g
		next.StagedGenerationID = g.ID
		return nil
	})
}

// MarkVerified advances a staged candidate after artifact verification.
func (r *Registry) MarkVerified(id string) error {
	return r.transitionCandidate(id, StateStaging, StateVerified)
}

// MarkReady advances a verified candidate after its processes pass readiness.
func (r *Registry) MarkReady(id string) error {
	return r.transitionCandidate(id, StateVerified, StateReady)
}

// Fail marks a staged candidate unusable and clears the staged pointer. Active
// and draining generations cannot be failed through this candidate operation.
func (r *Registry) Fail(id string) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		g, ok := next.Generations[id]
		if !ok {
			return ErrGenerationNotFound
		}
		if next.StagedGenerationID != id || (g.State != StateStaging && g.State != StateVerified && g.State != StateReady) {
			return transition("only a staged candidate can fail")
		}
		g.State = StateFailed
		next.Generations[id] = g
		next.StagedGenerationID = ""
		return nil
	})
}

// Activate makes a ready staged generation the default. The previous active
// generation becomes draining and its existing recording leases do not move.
func (r *Registry) Activate(id string) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		candidate, ok := next.Generations[id]
		if !ok {
			return ErrGenerationNotFound
		}
		if next.StagedGenerationID != id || candidate.State != StateReady {
			return transition("only the ready staged generation can be activated")
		}
		next.ActivationPreviousGenerationID = next.PreviousGenerationID
		if oldID := next.ActiveGenerationID; oldID != "" {
			old := next.Generations[oldID]
			old.State = StateDraining
			next.Generations[oldID] = old
			next.PreviousGenerationID = oldID
		}
		candidate.State = StateActive
		candidate.EngineDormant = false
		next.Generations[id] = candidate
		next.ActiveGenerationID = id
		next.StagedGenerationID = ""
		return nil
	})
}

// FinalizeActivation clears the activation rollback journal after the Runtime
// Host has switched public routing and enabled the candidate's background
// work. If the Host crashes before this call, startup can still distinguish a
// half-finished activation from a settled generation state.
func (r *Registry) FinalizeActivation(id string) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		if next.ActiveGenerationID != id {
			return transition("generation does not have a pending activation")
		}
		next.ActivationPreviousGenerationID = ""
		return nil
	})
}

// AbortActivation restores the previous default and marks a candidate failed
// when process readiness succeeded but Control activation failed. It also
// restores the rollback target that existed before the failed activation.
func (r *Registry) AbortActivation(candidateID, previousID string) error {
	if !validID(candidateID) || !validID(previousID) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		if next.ActiveGenerationID != candidateID || next.PreviousGenerationID != previousID || next.ActivationPreviousGenerationID == "" {
			return transition("activation rollback does not match current generations")
		}
		candidate, candidateOK := next.Generations[candidateID]
		previous, previousOK := next.Generations[previousID]
		if !candidateOK || !previousOK || candidate.State != StateActive || previous.State != StateDraining {
			return transition("activation rollback generation state is invalid")
		}
		for _, lease := range next.Leases {
			if lease.EngineGeneration == candidateID {
				return transition("candidate generation already owns recordings")
			}
		}
		candidate.State = StateFailed
		previous.State = StateActive
		previous.EngineDormant = false
		next.Generations[candidateID] = candidate
		next.Generations[previousID] = previous
		next.ActiveGenerationID = previousID
		next.PreviousGenerationID = next.ActivationPreviousGenerationID
		next.ActivationPreviousGenerationID = ""
		return nil
	})
}

// Rollback reactivates the retained previous generation. Existing leases on
// both generations remain pinned where they were created.
func (r *Registry) Rollback() error {
	return r.change(func(next *Snapshot) error {
		if next.ActivationPreviousGenerationID != "" {
			return transition("generation activation is still in progress")
		}
		previousID := next.PreviousGenerationID
		previous, ok := next.Generations[previousID]
		if previousID == "" || !ok || previous.State != StateDraining {
			return transition("no draining previous generation is available")
		}
		currentID := next.ActiveGenerationID
		if currentID == "" {
			return transition("there is no active generation to roll back")
		}
		current := next.Generations[currentID]
		current.State = StateDraining
		next.Generations[currentID] = current
		previous.State = StateActive
		previous.EngineDormant = false
		next.Generations[previousID] = previous
		next.ActiveGenerationID = previousID
		next.PreviousGenerationID = currentID
		return nil
	})
}

// MarkEngineDormant records a Host-confirmed, intentional Engine stop for a
// rollback-retained generation. It never releases leases or removes the
// immutable release. Only a lease-free draining generation can become
// dormant.
func (r *Registry) MarkEngineDormant(id string) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		g, ok := next.Generations[id]
		if !ok {
			return ErrGenerationNotFound
		}
		if g.State != StateDraining || id == next.ActiveGenerationID {
			return transition("only a non-active draining Engine can become dormant")
		}
		for _, lease := range next.Leases {
			if lease.EngineGeneration == id {
				return transition("generation still has recording leases")
			}
		}
		g.EngineDormant = true
		next.Generations[id] = g
		return nil
	})
}

// PinRecording creates a lease on the current active engine generation.
func (r *Registry) PinRecording(lease Lease) error {
	if err := validateLease(lease); err != nil {
		return err
	}
	return r.change(func(next *Snapshot) error {
		if _, exists := next.Leases[lease.RecordingID]; exists {
			return ErrLeaseExists
		}
		if len(next.Leases) >= maxLeases {
			return invalid("generation lease limit reached")
		}
		if lease.EngineGeneration != next.ActiveGenerationID {
			return transition("new recording must use the active engine generation")
		}
		g, ok := next.Generations[lease.EngineGeneration]
		if !ok || g.State != StateActive {
			return transition("recording engine generation is not active")
		}
		next.Leases[lease.RecordingID] = lease
		return nil
	})
}

// TransferRecording moves the retirement lease for one Recording to the
// currently active Engine after the Host has durably transferred the
// Recording's canonical-writer ownership fence. The generation lease is a
// retirement projection; it is deliberately updated separately from the
// owner fence, which remains authoritative for archive writes.
//
// The archive's start time is immutable across a transfer, and an exact
// expected lease is required so a stale completion cannot move a newer lease.
func (r *Registry) TransferRecording(expected, target Lease) error {
	if err := validateLease(expected); err != nil {
		return err
	}
	if err := validateLease(target); err != nil {
		return err
	}
	if expected.RecordingID != target.RecordingID || !expected.StartedAt.Equal(target.StartedAt) || sameLease(expected, target) {
		return invalid("recording lease transfer is invalid")
	}
	return r.change(func(next *Snapshot) error {
		current, ok := next.Leases[expected.RecordingID]
		if !ok {
			return ErrLeaseNotFound
		}
		if !sameLease(current, expected) {
			return ErrLeaseMismatch
		}
		source, sourceExists := next.Generations[expected.EngineGeneration]
		if !sourceExists || source.EngineDormant || (source.State != StateActive && source.State != StateDraining && source.State != StateFailed) {
			return transition("recording source generation cannot transfer its lease")
		}
		destination, destinationExists := next.Generations[target.EngineGeneration]
		if !destinationExists || target.EngineGeneration != next.ActiveGenerationID || destination.State != StateActive || destination.EngineDormant {
			return transition("recording target generation is not the active Engine")
		}
		if err := runtimehook.Pause(runtimehook.DuringGenerationLeaseReconcile, expected.RecordingID); err != nil {
			return err
		}
		next.Leases[target.RecordingID] = target
		return nil
	})
}

// RollbackRecordingTransfer restores a Recording lease to its previous,
// deliberately-draining Engine after the active target has been fenced. It is
// narrower than TransferRecording: the current owner must be in the active
// generation and the destination must be the non-dormant draining generation.
func (r *Registry) RollbackRecordingTransfer(expected, target Lease) error {
	if err := validateLease(expected); err != nil {
		return err
	}
	if err := validateLease(target); err != nil {
		return err
	}
	if expected.RecordingID != target.RecordingID || !expected.StartedAt.Equal(target.StartedAt) || sameLease(expected, target) {
		return invalid("recording lease rollback is invalid")
	}
	return r.change(func(next *Snapshot) error {
		current, ok := next.Leases[expected.RecordingID]
		if !ok {
			return ErrLeaseNotFound
		}
		if !sameLease(current, expected) {
			return ErrLeaseMismatch
		}
		source, sourceExists := next.Generations[expected.EngineGeneration]
		if !sourceExists || source.EngineDormant || source.State != StateActive || next.ActiveGenerationID != expected.EngineGeneration {
			return transition("recording rollback source is not the active Engine")
		}
		destination, destinationExists := next.Generations[target.EngineGeneration]
		if !destinationExists || destination.EngineDormant || destination.State != StateDraining || target.EngineGeneration == next.ActiveGenerationID {
			return transition("recording rollback destination is not a live draining Engine")
		}
		next.Leases[target.RecordingID] = target
		return nil
	})
}

func sameLease(a, b Lease) bool {
	return a.RecordingID == b.RecordingID && a.EngineGeneration == b.EngineGeneration &&
		a.WorkerInstance == b.WorkerInstance && a.StartedAt.Equal(b.StartedAt)
}

// ReleaseRecording releases a matching recording lease. The expected
// generation is required so stale completion messages cannot release a newer
// lease for the same recording identifier.
func (r *Registry) ReleaseRecording(recordingID, expectedGeneration string) error {
	if !validID(recordingID) || !validID(expectedGeneration) {
		return invalid("recording or generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		lease, ok := next.Leases[recordingID]
		if !ok {
			return ErrLeaseNotFound
		}
		if lease.EngineGeneration != expectedGeneration {
			return ErrLeaseMismatch
		}
		delete(next.Leases, recordingID)
		return nil
	})
}

// ReleaseRecordingOwner releases only the lease for the complete Engine owner
// tuple. Recording owner callbacks may race with a process restart in the same
// generation, so generation identity alone is insufficient for fencing.
func (r *Registry) ReleaseRecordingOwner(recordingID, expectedGeneration, expectedInstance string) error {
	if !validID(recordingID) || !validID(expectedGeneration) || !validID(expectedInstance) {
		return invalid("recording or engine owner identity is invalid")
	}
	return r.change(func(next *Snapshot) error {
		lease, ok := next.Leases[recordingID]
		if !ok {
			return ErrLeaseNotFound
		}
		if lease.EngineGeneration != expectedGeneration || lease.WorkerInstance != expectedInstance {
			return ErrLeaseMismatch
		}
		delete(next.Leases, recordingID)
		return nil
	})
}

// ClearLeases atomically replaces the durable lease projection with an empty
// set. It is reserved for cold Host recovery after the startup Engine has
// completed owner-fenced archive recovery; callers must not use it while live
// Engines may still own Recordings. Repeating the operation is safe.
func (r *Registry) ClearLeases() error {
	return r.change(func(next *Snapshot) error {
		next.Leases = make(map[string]Lease)
		return nil
	})
}

// ReconcileColdStart atomically rebuilds the cold Host's lease projection and
// attachment state after the active Engine has completed owner-fenced archive
// recovery. Only activeGenerationID may have an authorized Engine attachment
// at that point. Other draining generations remain installed and retain their
// rollback pointers, but are marked dormant so lease reconciliation does not
// expect inventory from orphan processes that this Host did not reattach.
// EngineDormant does not assert that those OS processes were terminated.
//
// This operation is idempotent and does not retire or remove immutable
// generation records. It must only be called after the active Engine has
// passed authenticated readiness and fenced recovery has completed.
func (r *Registry) ReconcileColdStart(activeGenerationID string) error {
	if !validID(activeGenerationID) {
		return invalid("active generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		active, ok := next.Generations[activeGenerationID]
		if !ok {
			return ErrGenerationNotFound
		}
		if next.ActiveGenerationID != activeGenerationID || active.State != StateActive {
			return transition("cold recovery generation is not active")
		}

		// Publish lease clearing and dormant attachment state together. The
		// source snapshot remains in memory unless this complete state is durable.
		next.Leases = make(map[string]Lease)
		for id, generation := range next.Generations {
			if id != activeGenerationID && generation.State == StateDraining {
				generation.EngineDormant = true
				next.Generations[id] = generation
			}
		}
		return nil
	})
}

// ReconcileInventory atomically replaces the lease projection for one
// successfully confirmed Engine inventory. A draining Engine may still own
// active Recordings and is therefore eligible to report inventory.
func (r *Registry) ReconcileInventory(inventory EngineInventory) error {
	return r.ReconcileInventories([]EngineInventory{inventory})
}

// ReconcileInventories atomically applies a set of confirmed Engine
// inventories. This batch form lets callers fetch and validate every eligible
// Engine before changing any durable lease, and prevents partial publication
// if inventories conflict with one another.
func (r *Registry) ReconcileInventories(inventories []EngineInventory) error {
	if len(inventories) == 0 {
		return nil
	}
	if len(inventories) > maxGenerations {
		return fmt.Errorf("%w: too many engine inventories", ErrInvalidInventory)
	}
	return r.change(func(next *Snapshot) error {
		byGeneration := make(map[string]EngineInventory, len(inventories))
		owners := make(map[string]string)
		var total int
		for _, inventory := range inventories {
			if err := ValidateInventory(inventory); err != nil {
				return err
			}
			if _, duplicate := byGeneration[inventory.EngineGeneration]; duplicate {
				return fmt.Errorf("%w: duplicate engine generation inventory", ErrInvalidInventory)
			}
			generation, ok := next.Generations[inventory.EngineGeneration]
			if !ok {
				return ErrGenerationNotFound
			}
			if (generation.State != StateActive && generation.State != StateDraining) || generation.EngineDormant {
				return transition("recording inventory generation is not active or draining")
			}
			byGeneration[inventory.EngineGeneration] = inventory
			total += len(inventory.Recordings)
			if total > maxLeases {
				return fmt.Errorf("%w: recording inventory limit reached", ErrInvalidInventory)
			}
			for _, recording := range inventory.Recordings {
				if previous, exists := owners[recording.RecordingID]; exists {
					if previous != inventory.EngineGeneration {
						return ErrLeaseMismatch
					}
					return fmt.Errorf("%w: recording appears more than once", ErrInvalidInventory)
				}
				owners[recording.RecordingID] = inventory.EngineGeneration
				if lease, exists := next.Leases[recording.RecordingID]; exists && lease.EngineGeneration != inventory.EngineGeneration {
					return ErrLeaseMismatch
				}
			}
		}

		// Only a confirmed inventory can release absent leases. Preserve every
		// lease belonging to generations for which no inventory was supplied.
		for recordingID, lease := range next.Leases {
			_, observed := byGeneration[lease.EngineGeneration]
			if !observed {
				continue
			}
			if _, active := owners[recordingID]; !active {
				delete(next.Leases, recordingID)
			}
		}
		for _, inventory := range inventories {
			for _, recording := range inventory.Recordings {
				next.Leases[recording.RecordingID] = Lease{
					RecordingID:      recording.RecordingID,
					EngineGeneration: inventory.EngineGeneration,
					WorkerInstance:   inventory.WorkerInstance,
					StartedAt:        recording.StartedAt,
				}
			}
		}
		if len(next.Leases) > maxLeases {
			return fmt.Errorf("%w: generation lease limit reached", ErrInvalidInventory)
		}
		return nil
	})
}

// Retire makes a draining or failed generation eligible for release garbage
// collection. A generation with leases, or the active generation, cannot be
// retired. Explicit retirement also removes its previous rollback pointer.
func (r *Registry) Retire(id string) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		g, ok := next.Generations[id]
		if !ok {
			return ErrGenerationNotFound
		}
		if id == next.ActiveGenerationID || id == next.PreviousGenerationID || id == next.ActivationPreviousGenerationID || (g.State != StateDraining && g.State != StateFailed) {
			return transition("only a non-active draining or failed generation can retire")
		}
		for _, lease := range next.Leases {
			if lease.EngineGeneration == id {
				return transition("generation still has recording leases")
			}
		}
		g.State = StateRetired
		g.EngineDormant = false
		next.Generations[id] = g
		if next.StagedGenerationID == id {
			next.StagedGenerationID = ""
		}
		return nil
	})
}

// RemoveRetired removes lifecycle metadata for an already retired generation
// after the Host has stopped its processes and deleted its immutable release.
// The active and previous rollback generations, staged candidate, and any
// generation with a recording lease are never eligible for removal.
func (r *Registry) RemoveRetired(id string) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		g, ok := next.Generations[id]
		if !ok {
			return ErrGenerationNotFound
		}
		if g.State != StateRetired || id == next.ActiveGenerationID || id == next.PreviousGenerationID || id == next.ActivationPreviousGenerationID || id == next.StagedGenerationID {
			return transition("generation is retained by the runtime")
		}
		for _, lease := range next.Leases {
			if lease.EngineGeneration == id {
				return transition("generation still has recording leases")
			}
		}
		delete(next.Generations, id)
		return nil
	})
}

func (r *Registry) transitionCandidate(id string, from, to State) error {
	if !validID(id) {
		return invalid("generation identifier is invalid")
	}
	return r.change(func(next *Snapshot) error {
		g, ok := next.Generations[id]
		if !ok {
			return ErrGenerationNotFound
		}
		if next.StagedGenerationID != id || g.State != from {
			return transition(fmt.Sprintf("generation must be staged in %q state", from))
		}
		g.State = to
		next.Generations[id] = g
		return nil
	})
}

func (r *Registry) change(mutate func(*Snapshot) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := cloneSnapshot(r.state)
	if err := mutate(&next); err != nil {
		return err
	}
	if err := validateSnapshot(next); err != nil {
		return err
	}
	if err := r.persist(next); err != nil {
		return fmt.Errorf("persist generation registry: %w", err)
	}
	r.state = next
	return nil
}

func (r *Registry) persist(state Snapshot) (retErr error) {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > maxStateBytes {
		return fmt.Errorf("%w: registry exceeds size limit", ErrInvalidState)
	}
	dir := filepath.Dir(r.path)
	tmp, err := os.CreateTemp(dir, ".generation-state-*.tmp")
	if err != nil {
		return err
	}
	tempName := tmp.Name()
	defer func() {
		_ = os.Remove(tempName)
	}()
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, r.path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func validateGeneration(g Generation) error {
	if !validID(g.ID) || strings.TrimSpace(g.Version) == "" || len(g.Version) > 128 || strings.ContainsAny(g.Version, "/\\\x00") ||
		strings.TrimSpace(g.Commit) == "" || len(g.Commit) > 128 || strings.ContainsAny(g.Commit, "/\\\x00") ||
		(g.AdapterSetID != "" && !adapterSetIDPattern.MatchString(g.AdapterSetID)) ||
		(g.StorageProviderSetID != "" && !storageSetIDPattern.MatchString(g.StorageProviderSetID)) ||
		g.InstalledAt.IsZero() || !validState(g.State) || g.ControlProtocol < 1 || g.EngineProtocol < 1 ||
		g.ArchiveReadCompatibility.Minimum < 1 || g.ArchiveReadCompatibility.Maximum < g.ArchiveReadCompatibility.Minimum ||
		g.ArchiveWriteEpoch < 1 {
		return invalid("generation metadata is invalid")
	}
	return nil
}

func validateLease(lease Lease) error {
	if !validID(lease.RecordingID) || !validID(lease.EngineGeneration) || !validID(lease.WorkerInstance) || lease.StartedAt.IsZero() {
		return invalid("generation lease is invalid")
	}
	return nil
}

// ValidateInventory checks the bounded identity and timestamp fields returned
// by one confirmed Engine inventory without consulting registry state.
func ValidateInventory(inventory EngineInventory) error {
	if !inventory.Confirmed || !validID(inventory.EngineGeneration) || !validID(inventory.WorkerInstance) || inventory.ObservedAt.IsZero() {
		return fmt.Errorf("%w: confirmation or engine identity is invalid", ErrInvalidInventory)
	}
	if len(inventory.Recordings) > maxLeases {
		return fmt.Errorf("%w: recording inventory limit reached", ErrInvalidInventory)
	}
	seen := make(map[string]struct{}, len(inventory.Recordings))
	for _, recording := range inventory.Recordings {
		if !validID(recording.RecordingID) || recording.StartedAt.IsZero() || recording.StartedAt.After(inventory.ObservedAt) {
			return fmt.Errorf("%w: recording identity or timestamp is invalid", ErrInvalidInventory)
		}
		if _, exists := seen[recording.RecordingID]; exists {
			return fmt.Errorf("%w: duplicate recording identifier", ErrInvalidInventory)
		}
		seen[recording.RecordingID] = struct{}{}
	}
	return nil
}

func validateSnapshot(state Snapshot) error {
	if state.SchemaVersion != SchemaVersion || state.Generations == nil || state.Leases == nil || len(state.Generations) > maxGenerations || len(state.Leases) > maxLeases {
		return invalid("registry schema or collection size is invalid")
	}
	activeCount := 0
	for id, g := range state.Generations {
		if id != g.ID || validateGeneration(g) != nil {
			return invalid("generation map entry is invalid")
		}
		if g.State == StateActive {
			activeCount++
		}
	}
	if (state.ActiveGenerationID == "") != (activeCount == 0) || activeCount > 1 {
		return invalid("active generation pointer is inconsistent")
	}
	if state.ActiveGenerationID != "" {
		g, ok := state.Generations[state.ActiveGenerationID]
		if !ok || g.State != StateActive {
			return invalid("active generation pointer is inconsistent")
		}
	}
	if state.PreviousGenerationID != "" {
		g, ok := state.Generations[state.PreviousGenerationID]
		if !validID(state.PreviousGenerationID) || !ok || g.State != StateDraining || state.PreviousGenerationID == state.ActiveGenerationID {
			return invalid("previous generation pointer is inconsistent")
		}
	}
	if state.ActivationPreviousGenerationID != "" {
		g, ok := state.Generations[state.ActivationPreviousGenerationID]
		if !validID(state.ActivationPreviousGenerationID) || !ok || g.State != StateDraining || state.ActivationPreviousGenerationID == state.ActiveGenerationID || state.ActivationPreviousGenerationID == state.PreviousGenerationID {
			return invalid("pending activation rollback pointer is inconsistent")
		}
		if state.ActiveGenerationID == "" || state.PreviousGenerationID == "" {
			return invalid("pending activation lacks current and prior generations")
		}
	}
	if state.StagedGenerationID != "" {
		g, ok := state.Generations[state.StagedGenerationID]
		if !validID(state.StagedGenerationID) || !ok || (g.State != StateStaging && g.State != StateVerified && g.State != StateReady) {
			return invalid("staged generation pointer is inconsistent")
		}
	}
	for recordingID, lease := range state.Leases {
		if recordingID != lease.RecordingID || validateLease(lease) != nil {
			return invalid("generation lease map entry is invalid")
		}
		g, ok := state.Generations[lease.EngineGeneration]
		if !ok || (g.State != StateActive && g.State != StateDraining) || g.EngineDormant {
			return invalid("generation lease refers to an unavailable engine")
		}
	}
	for _, g := range state.Generations {
		if g.EngineDormant && g.State != StateDraining && g.State != StateRetired {
			return invalid("dormant Engine generation state is invalid")
		}
	}
	return nil
}

func decodeStrict(data []byte, target any) error {
	if len(data) == 0 || len(data) > maxStateBytes {
		return invalid("registry size is outside the allowed bound")
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return invalid("registry JSON is malformed or ambiguous")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return invalid("registry JSON is malformed")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return invalid("registry JSON has trailing data")
	}
	return nil
}

// rejectDuplicateKeys parses JSON tokens before struct decoding because the
// standard decoder otherwise accepts ambiguous duplicate object keys.
func rejectDuplicateKeys(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(d); err != nil {
		return err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func scanJSONValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(d); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("invalid object")
		}
	case '[':
		for d.More() {
			if err := scanJSONValue(d); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("invalid array")
		}
	default:
		return errors.New("invalid JSON delimiter")
	}
	return nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return fmt.Errorf("%w: create registry directory", ErrUnsafePath)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: registry directory is not a regular directory", ErrUnsafePath)
	}
	if err := os.Chmod(path, 0700); err != nil {
		return fmt.Errorf("%w: secure registry directory", ErrUnsafePath)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func cloneSnapshot(in Snapshot) Snapshot {
	out := in
	out.Generations = make(map[string]Generation, len(in.Generations))
	for id, generation := range in.Generations {
		out.Generations[id] = generation
	}
	out.Leases = make(map[string]Lease, len(in.Leases))
	for id, lease := range in.Leases {
		out.Leases[id] = lease
	}
	return out
}

func emptySnapshot() Snapshot {
	return Snapshot{SchemaVersion: SchemaVersion, Generations: map[string]Generation{}, Leases: map[string]Lease{}}
}

func validID(id string) bool { return idPattern.MatchString(id) }

func validState(state State) bool {
	switch state {
	case StateStaging, StateVerified, StateReady, StateActive, StateDraining, StateRetired, StateFailed:
		return true
	default:
		return false
	}
}

func invalid(message string) error    { return fmt.Errorf("%w: %s", ErrInvalidState, message) }
func transition(message string) error { return fmt.Errorf("%w: %s", ErrInvalidTransition, message) }
