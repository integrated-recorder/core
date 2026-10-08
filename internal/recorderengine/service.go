// Package recorderengine owns the acquisition manager and adapter runtime for
// one engine generation. Its IPC surface is intentionally narrower than the
// management API.
package recorderengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	OperationReady                  = "ready"
	OperationHeartbeat              = "heartbeat"
	OperationStartResolved          = "start_resolved"
	OperationBeginDrain             = "begin_drain"
	OperationActiveCount            = "active_recordings"
	OperationGet                    = "get"
	OperationLivePlaybackSnapshot   = "live_playback_snapshot"
	OperationList                   = "list"
	OperationStop                   = "stop"
	OperationDelete                 = "delete"
	OperationDeleteTerminal         = "delete_terminal_archive"
	OperationInventory              = "inventory"
	OperationHandoverSnapshot       = "handover_snapshot"
	OperationHandoverPause          = "handover_pause"
	OperationHandoverResume         = "handover_resume"
	OperationHandoverComplete       = "handover_complete"
	OperationHandoverPrepareTarget  = "handover_prepare_target"
	OperationHandoverActivateTarget = "handover_activate_target"
	OperationHandoverDiscardTarget  = "handover_discard_target"
	DefaultListLimit                = 2000
	MaximumListLimit                = 10000
)

type StartResolvedRequest struct {
	RecordingID string                          `json:"recording_id,omitempty"`
	AdapterID   string                          `json:"adapter_id"`
	Media       adapterproto.MediaSource        `json:"media"`
	Resource    *adapterproto.ResourceRef       `json:"resource,omitempty"`
	Title       string                          `json:"title,omitempty"`
	Provenance  *adapterproto.AdapterProvenance `json:"provenance,omitempty"`
}

type RecordingIDRequest struct {
	RecordingID string `json:"recording_id"`
}

// HandoverOwnerRequest is accepted only from the Runtime Host over the
// authenticated Engine IPC transport. Owner is a fencing token, not a request
// to transfer ownership.
type HandoverOwnerRequest struct {
	RecordingID string               `json:"recording_id"`
	Owner       recordingowner.Owner `json:"owner"`
}

type HandoverResumeRequest struct {
	RecordingID string                   `json:"recording_id"`
	Owner       recordingowner.Owner     `json:"owner"`
	Snapshot    acquire.HandoverSnapshot `json:"snapshot"`
}

type HandoverTargetRequest struct {
	Snapshot      acquire.HandoverSnapshot       `json:"snapshot"`
	Target        acquire.HandoverTargetIdentity `json:"target"`
	SourceDrained bool                           `json:"source_drained,omitempty"`
}

type HandoverTargetOwnerRequest struct {
	RecordingID string                         `json:"recording_id"`
	Owner       recordingowner.Owner           `json:"owner"`
	Target      acquire.HandoverTargetIdentity `json:"target"`
}

type HandoverTargetDiscardRequest struct {
	RecordingID string                         `json:"recording_id"`
	Target      acquire.HandoverTargetIdentity `json:"target"`
}

// HandoverSnapshotResult and HandoverResult deliberately expose only the
// requested internal recording snapshot/owner tuple. They are never routed
// through public HTTP APIs or logs.
type HandoverSnapshotResult struct {
	RecordingID  string                   `json:"recording_id"`
	GenerationID string                   `json:"generation_id"`
	InstanceID   string                   `json:"instance_id"`
	Snapshot     acquire.HandoverSnapshot `json:"snapshot"`
}

type HandoverResult struct {
	RecordingID  string                `json:"recording_id"`
	GenerationID string                `json:"generation_id"`
	InstanceID   string                `json:"instance_id"`
	Owner        *recordingowner.Owner `json:"owner,omitempty"`
	Accepted     bool                  `json:"accepted"`
}

type GenerationRequest struct {
	GenerationID string `json:"generation_id"`
}

type ListRequest struct {
	Limit int `json:"limit,omitempty"`
}

type ReadyResult struct {
	Ready                bool      `json:"ready"`
	GenerationID         string    `json:"generation_id"`
	InstanceID           string    `json:"instance_id"`
	StartedAt            time.Time `json:"started_at"`
	ProtocolVersion      int       `json:"protocol_version"`
	ActiveRecordingCount int       `json:"active_recording_count"`
}

type HeartbeatResult struct {
	GenerationID string    `json:"generation_id"`
	InstanceID   string    `json:"instance_id"`
	At           time.Time `json:"at"`
}

type ActiveRecording struct {
	RecordingID string                `json:"recording_id"`
	State       domain.RecordingState `json:"state"`
	StartedAt   time.Time             `json:"started_at"`
	Owner       *recordingowner.Owner `json:"owner,omitempty"`
}

type InventoryResult struct {
	GenerationID string            `json:"generation_id"`
	InstanceID   string            `json:"instance_id"`
	Ready        bool              `json:"ready"`
	Active       []ActiveRecording `json:"active_recordings"`
}

type DrainResult struct {
	GenerationID string `json:"generation_id"`
	InstanceID   string `json:"instance_id"`
	Draining     bool   `json:"draining"`
}

type ActiveRecordingCountResult struct {
	GenerationID string `json:"generation_id"`
	InstanceID   string `json:"instance_id"`
	Count        int    `json:"count"`
}

type Engine struct {
	manager          *acquire.Manager
	adapters         *adapterhost.Host
	generation       string
	instance         string
	startedAt        time.Time
	mu               sync.RWMutex
	admissionMu      sync.RWMutex
	draining         bool
	ready            bool
	closed           bool
	adapterCloseOnce sync.Once
	ownerClient      recordingOwnerClient
	ownerMu          sync.Mutex
	owners           map[string]recordingowner.Owner
	releasedOwners   map[recordingowner.Owner]struct{}
	releasedOrder    []recordingowner.Owner
	startSeen        bool
}

type recordingOwnerClient interface {
	ClaimRecording(context.Context, string) (recordingowner.Owner, error)
	ReleaseRecording(context.Context, recordingowner.Owner) error
}

const maxReleasedOwnerHistory = 4096

func New(manager *acquire.Manager, adapters *adapterhost.Host, generationID, instanceID string) (*Engine, error) {
	if manager == nil || adapters == nil {
		return nil, errors.New("recorder manager and adapter runtime are required")
	}
	if generationID == "" || instanceID == "" {
		return nil, errors.New("engine generation and instance identities are required")
	}
	e := &Engine{
		manager: manager, adapters: adapters, generation: generationID, instance: instanceID,
		startedAt: time.Now().UTC(), ready: true,
		owners: make(map[string]recordingowner.Owner), releasedOwners: make(map[recordingowner.Owner]struct{}),
	}
	return e, nil
}

// ConfigureRecordingOwnerClient enables Host-authorized managed starts. It is
// called before the Engine IPC server begins serving requests. With no client,
// direct development/test Engine invocation keeps the historical behavior.
func (e *Engine) ConfigureRecordingOwnerClient(client recordingOwnerClient) error {
	if e == nil || client == nil {
		return errors.New("recording owner client is required")
	}
	e.admissionMu.Lock()
	defer e.admissionMu.Unlock()
	e.mu.Lock()
	if e.startSeen || e.ownerClient != nil || e.closed {
		e.mu.Unlock()
		return errors.New("recording owner client must be configured before Engine starts")
	}
	e.ownerClient = client
	e.mu.Unlock()
	if err := e.manager.ConfigureTerminalOwnerRelease(func(ctx context.Context, owner acquire.OwnershipToken) error {
		return e.releaseOwner(ctx, owner)
	}); err != nil {
		e.mu.Lock()
		e.ownerClient = nil
		e.mu.Unlock()
		return err
	}
	if err := e.manager.ConfigureAutomaticArchiveRecovery(e.claimAutomaticRecoveryOwner); err != nil {
		e.mu.Lock()
		e.ownerClient = nil
		e.mu.Unlock()
		return err
	}
	return nil
}

func (e *Engine) claimAutomaticRecoveryOwner(ctx context.Context, recordingID string) (recordingowner.Owner, error) {
	e.admissionMu.RLock()
	draining := e.draining
	e.admissionMu.RUnlock()
	e.mu.RLock()
	client, unavailable := e.ownerClient, e.closed || draining
	e.mu.RUnlock()
	if client == nil || unavailable {
		return recordingowner.Owner{}, errors.New("engine cannot claim recovery ownership")
	}
	owner, err := client.ClaimRecording(ctx, recordingID)
	if err != nil {
		return recordingowner.Owner{}, err
	}
	e.ownerMu.Lock()
	if current, exists := e.owners[recordingID]; exists && current != owner {
		e.ownerMu.Unlock()
		_ = client.ReleaseRecording(context.Background(), owner)
		return recordingowner.Owner{}, recordingowner.ErrStaleOwner
	}
	e.owners[recordingID] = owner
	e.ownerMu.Unlock()
	return owner, nil
}

// RuntimeInstanceID is returned in the transport envelope so a Control Plane
// can pin subsequent calls to the same long-lived Engine process.
func (e *Engine) RuntimeInstanceID() string { return e.instance }

func (e *Engine) GenerationID() string { return e.generation }

func (e *Engine) Handle(ctx context.Context, operation string, payload json.RawMessage) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	closed, ready := e.closed, e.ready
	e.mu.RUnlock()
	if closed {
		return nil, publicError("engine_unavailable", "recorder engine is shutting down")
	}
	switch operation {
	case OperationReady:
		rows, err := e.manager.OwnedStates(MaximumListLimit)
		if err != nil {
			return nil, publicError("inventory_too_large", "engine inventory exceeds the supported limit")
		}
		active := 0
		for _, row := range rows {
			if row.State == domain.StateRecording {
				active++
			}
		}
		return ReadyResult{Ready: ready, GenerationID: e.generation, InstanceID: e.instance, StartedAt: e.startedAt, ProtocolVersion: runtimeipc.ProtocolVersion, ActiveRecordingCount: active}, nil
	case OperationHeartbeat:
		return HeartbeatResult{GenerationID: e.generation, InstanceID: e.instance, At: time.Now().UTC()}, nil
	case OperationBeginDrain:
		var request GenerationRequest
		if err := decodePayload(payload, &request); err != nil || request.GenerationID == "" {
			return nil, publicError("invalid_request", "engine generation identity is invalid")
		}
		if request.GenerationID != e.generation {
			return nil, publicError("generation_mismatch", "request targets another engine generation")
		}
		// The write lock waits for any start already admitted under admissionMu
		// to finish, then fences every subsequent start atomically. Existing
		// recording workers use the Manager directly and are unaffected.
		e.admissionMu.Lock()
		e.draining = true
		e.admissionMu.Unlock()
		return DrainResult{GenerationID: e.generation, InstanceID: e.instance, Draining: true}, nil
	case OperationActiveCount:
		var request GenerationRequest
		if err := decodePayload(payload, &request); err != nil || request.GenerationID == "" {
			return nil, publicError("invalid_request", "engine generation identity is invalid")
		}
		if request.GenerationID != e.generation {
			return nil, publicError("generation_mismatch", "request targets another engine generation")
		}
		count, err := e.activeRecordingCount(ctx)
		if err != nil {
			return nil, publicError("inventory_too_large", "engine inventory exceeds the supported limit")
		}
		return ActiveRecordingCountResult{GenerationID: e.generation, InstanceID: e.instance, Count: count}, nil
	case OperationStartResolved:
		var request StartResolvedRequest
		if err := decodePayload(payload, &request); err != nil {
			return nil, publicError("invalid_request", "resolved recording request is invalid")
		}
		e.admissionMu.RLock()
		defer e.admissionMu.RUnlock()
		if e.draining {
			return nil, publicError("engine_draining", "recorder engine is draining and cannot start recordings")
		}
		e.mu.Lock()
		e.startSeen = true
		ownerClient := e.ownerClient
		e.mu.Unlock()
		if ownerClient != nil {
			owner, err := ownerClient.ClaimRecording(ctx, request.RecordingID)
			if err != nil {
				return nil, publicError("recording_owner_unavailable", "Host could not authorize recording ownership")
			}
			e.ownerMu.Lock()
			e.owners[owner.RecordingID] = owner
			e.ownerMu.Unlock()
			result, err := e.manager.StartResolvedWithIDOwned(ctx, owner, owner.RecordingID, request.AdapterID, request.Media, request.Resource, request.Title, request.Provenance)
			if err != nil {
				_ = e.releaseOwner(context.Background(), owner)
				return nil, publicError("start_failed", "recording could not be started")
			}
			return result, nil
		}
		if request.RecordingID == "" {
			result, err := e.manager.StartResolved(ctx, request.AdapterID, request.Media, request.Resource, request.Title, request.Provenance)
			if err != nil {
				return nil, publicError("start_failed", "recording could not be started")
			}
			return result, nil
		}
		result, err := e.manager.StartResolvedWithID(ctx, request.RecordingID, request.AdapterID, request.Media, request.Resource, request.Title, request.Provenance)
		if err != nil {
			return nil, publicError("start_failed", "recording could not be started")
		}
		return result, nil
	case OperationGet:
		var request RecordingIDRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" {
			return nil, publicError("invalid_request", "recording identity is invalid")
		}
		result, err := e.manager.Get(request.RecordingID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, publicError("not_found", "recording was not found")
			}
			return nil, publicError("read_failed", "recording could not be read")
		}
		return result, nil
	case OperationLivePlaybackSnapshot:
		var request RecordingIDRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" {
			return nil, publicError("invalid_request", "recording identity is invalid")
		}
		result, err := e.manager.LivePlaybackSnapshot(ctx, request.RecordingID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, publicError("not_found", "recording was not found")
			}
			return nil, publicError("read_failed", "live playback snapshot could not be read")
		}
		return result, nil
	case OperationList:
		var request ListRequest
		if err := decodePayload(payload, &request); err != nil {
			return nil, publicError("invalid_request", "recording list request is invalid")
		}
		limit := request.Limit
		if limit == 0 {
			limit = DefaultListLimit
		}
		if limit < 1 || limit > MaximumListLimit {
			return nil, publicError("invalid_request", "recording list limit is outside the supported range")
		}
		result, err := e.manager.ListForManagement(ctx, limit)
		if errors.Is(err, acquire.ErrListLimit) {
			return nil, publicError("list_too_large", "recording list exceeds the requested limit")
		}
		if err != nil {
			return nil, publicError("read_failed", "recording list could not be read")
		}
		return result, nil
	case OperationStop:
		var request RecordingIDRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" {
			return nil, publicError("invalid_request", "recording identity is invalid")
		}
		result, err := e.manager.StopContext(ctx, request.RecordingID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, publicError("not_owned", "recording is not owned by this engine")
			}
			if errors.Is(err, acquire.ErrHandoverConflict) {
				return nil, publicError("handover_in_progress", "recording ownership is being transferred")
			}
			return nil, publicError("stop_failed", "recording could not be stopped")
		}
		return result, nil
	case OperationDelete:
		var request RecordingIDRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" {
			return nil, publicError("invalid_request", "recording identity is invalid")
		}
		if err := e.manager.Delete(request.RecordingID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, publicError("not_owned", "recording is not owned by this engine")
			}
			if errors.Is(err, acquire.ErrActiveRecording) {
				return nil, publicError("active_recording", "active recording cannot be deleted")
			}
			return nil, publicError("delete_failed", "recording could not be deleted")
		}
		return struct {
			Deleted bool `json:"deleted"`
		}{Deleted: true}, nil
	case OperationDeleteTerminal:
		var request RecordingIDRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" {
			return nil, publicError("invalid_request", "recording identity is invalid")
		}
		if err := e.manager.DeleteTerminalArchive(request.RecordingID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, publicError("not_found", "recording was not found")
			}
			if errors.Is(err, acquire.ErrActiveRecording) {
				return nil, publicError("active_recording", "active recording cannot be deleted")
			}
			return nil, publicError("delete_failed", "recording could not be deleted")
		}
		return struct {
			Deleted bool `json:"deleted"`
		}{Deleted: true}, nil
	case OperationInventory:
		rows, err := e.manager.OwnedStates(MaximumListLimit)
		if err != nil {
			return nil, publicError("inventory_too_large", "engine inventory exceeds the supported limit")
		}
		active := make([]ActiveRecording, 0, len(rows))
		for _, row := range rows {
			if row.State == domain.StateRecording {
				var owner *recordingowner.Owner
				if current, ok := e.currentOwner(row.ID); ok {
					copy := current
					owner = &copy
				}
				active = append(active, ActiveRecording{RecordingID: row.ID, State: row.State, StartedAt: row.StartedAt, Owner: owner})
			}
		}
		return InventoryResult{GenerationID: e.generation, InstanceID: e.instance, Ready: ready, Active: active}, nil
	case OperationHandoverSnapshot:
		var request HandoverOwnerRequest
		if err := decodePayload(payload, &request); err != nil || !e.validOwnerForThisEngine(request.RecordingID, request.Owner) {
			return nil, publicError("invalid_request", "recording ownership identity is invalid")
		}
		var snapshot acquire.HandoverSnapshot
		if !e.hasCurrentOwner(request.Owner) {
			return nil, publicError("stale_owner", "recording ownership changed")
		}
		snapshot, err := e.manager.HandoverSnapshot(request.RecordingID)
		if err != nil {
			return nil, handoverPublicError(err)
		}
		if !validHandoverSnapshot(snapshot, request.Owner) {
			return nil, publicError("handover_rejected", "recording continuation snapshot is invalid")
		}
		return HandoverSnapshotResult{RecordingID: request.RecordingID, GenerationID: e.generation, InstanceID: e.instance, Snapshot: snapshot}, nil
	case OperationHandoverPause:
		var request HandoverOwnerRequest
		if err := decodePayload(payload, &request); err != nil || !e.validOwnerForThisEngine(request.RecordingID, request.Owner) {
			return nil, publicError("invalid_request", "recording ownership identity is invalid")
		}
		var snapshot acquire.HandoverSnapshot
		if !e.hasCurrentOwner(request.Owner) {
			return nil, publicError("stale_owner", "recording ownership changed")
		}
		snapshot, err := e.manager.PauseForHandover(ctx, request.RecordingID, request.Owner)
		if err != nil {
			return nil, handoverPublicError(err)
		}
		if !validHandoverSnapshot(snapshot, request.Owner) {
			return nil, publicError("handover_rejected", "recording continuation snapshot is invalid")
		}
		return HandoverSnapshotResult{RecordingID: request.RecordingID, GenerationID: e.generation, InstanceID: e.instance, Snapshot: snapshot}, nil
	case OperationHandoverResume:
		var request HandoverResumeRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" || !e.validOwnerForThisEngine(request.RecordingID, request.Owner) || !validHandoverSnapshot(request.Snapshot, request.Snapshot.Owner) || request.Snapshot.RecordingID != request.RecordingID || request.Owner.EngineGeneration != request.Snapshot.Owner.EngineGeneration || request.Owner.WorkerInstance != request.Snapshot.Owner.WorkerInstance || request.Owner.Epoch < request.Snapshot.Owner.Epoch {
			return nil, publicError("invalid_request", "recording continuation identity is invalid")
		}
		var owner recordingowner.Owner
		e.ownerMu.Lock()
		current, currentOK := e.owners[request.RecordingID]
		if request.Snapshot.Owner.RecordingID != request.RecordingID || !currentOK || current != request.Snapshot.Owner {
			e.ownerMu.Unlock()
			return nil, publicError("stale_owner", "recording ownership changed")
		}
		err := e.manager.ResumeHandover(request.RecordingID, request.Owner, request.Snapshot)
		if err == nil {
			e.owners[request.RecordingID] = request.Owner
			owner = request.Owner
		}
		e.ownerMu.Unlock()
		if err != nil {
			return nil, handoverPublicError(err)
		}
		return handoverAck(request.RecordingID, e, &owner), nil
	case OperationHandoverComplete:
		var request HandoverOwnerRequest
		if err := decodePayload(payload, &request); err != nil || !e.validOwnerForThisEngine(request.RecordingID, request.Owner) {
			return nil, publicError("invalid_request", "recording ownership identity is invalid")
		}
		if !e.hasCurrentOwner(request.Owner) {
			return nil, publicError("stale_owner", "recording ownership changed")
		}
		err := e.manager.CompleteHandover(request.RecordingID, request.Owner)
		if err == nil {
			e.ownerMu.Lock()
			if current, ok := e.owners[request.RecordingID]; !ok || current != request.Owner {
				err = recordingowner.ErrStaleOwner
			} else {
				delete(e.owners, request.RecordingID)
			}
			e.ownerMu.Unlock()
		}
		if err != nil {
			return nil, handoverPublicError(err)
		}
		return handoverAck(request.RecordingID, e, &request.Owner), nil
	case OperationHandoverPrepareTarget:
		var request HandoverTargetRequest
		if err := decodePayload(payload, &request); err != nil || !e.validTargetIdentity(request.Target) || !validHandoverSnapshot(request.Snapshot, request.Snapshot.Owner) || (request.Snapshot.Owner.EngineGeneration == e.generation && request.Snapshot.Owner.WorkerInstance == e.instance) {
			return nil, publicError("invalid_request", "target continuation request is invalid")
		}
		if _, owned := e.currentOwner(request.Snapshot.RecordingID); owned {
			return nil, publicError("handover_rejected", "target engine already owns this recording")
		}
		if err := e.manager.PrepareHandoverTargetWithSourceState(ctx, request.Snapshot, request.Target, request.SourceDrained); err != nil {
			return nil, handoverPublicError(err)
		}
		return handoverAck(request.Snapshot.RecordingID, e, &request.Snapshot.Owner), nil
	case OperationHandoverActivateTarget:
		var request HandoverTargetOwnerRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" || !e.validTargetIdentity(request.Target) || !e.validOwnerForThisEngine(request.RecordingID, request.Owner) || request.Owner.EngineGeneration != request.Target.EngineGeneration || request.Owner.WorkerInstance != request.Target.WorkerInstance {
			return nil, publicError("invalid_request", "target ownership identity is invalid")
		}
		e.ownerMu.Lock()
		if _, exists := e.owners[request.RecordingID]; exists {
			e.ownerMu.Unlock()
			return nil, publicError("handover_rejected", "target engine already owns this recording")
		}
		if err := e.manager.ActivatePreparedHandover(request.Owner); err != nil {
			e.ownerMu.Unlock()
			return nil, handoverPublicError(err)
		}
		e.owners[request.RecordingID] = request.Owner
		e.ownerMu.Unlock()
		return handoverAck(request.RecordingID, e, &request.Owner), nil
	case OperationHandoverDiscardTarget:
		var request HandoverTargetDiscardRequest
		if err := decodePayload(payload, &request); err != nil || request.RecordingID == "" || !e.validTargetIdentity(request.Target) {
			return nil, publicError("invalid_request", "target continuation identity is invalid")
		}
		if _, owned := e.currentOwner(request.RecordingID); owned {
			return nil, publicError("handover_rejected", "an active target recording cannot be discarded")
		}
		if err := e.manager.DiscardPreparedHandover(request.RecordingID); err != nil {
			return nil, handoverPublicError(err)
		}
		return handoverAck(request.RecordingID, e, nil), nil
	default:
		return nil, publicError("unsupported_operation", "runtime operation is not supported")
	}
}

func (e *Engine) currentOwner(recordingID string) (recordingowner.Owner, bool) {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	owner, ok := e.owners[recordingID]
	return owner, ok
}

// validOwnerForThisEngine checks the complete exact fencing tuple expected by
// this process. Epoch is intentionally not compared to an internal counter:
// the Host-owned token is authoritative and the acquire commit fence performs
// the durable current-owner check.
func (e *Engine) validOwnerForThisEngine(recordingID string, owner recordingowner.Owner) bool {
	return validOwnerIdentity(recordingID, owner) && owner.EngineGeneration == e.generation && owner.WorkerInstance == e.instance
}

func (e *Engine) validTargetIdentity(target acquire.HandoverTargetIdentity) bool {
	return target.EngineGeneration == e.generation && target.WorkerInstance == e.instance && target.EngineGeneration != "" && target.WorkerInstance != ""
}

func (e *Engine) hasCurrentOwner(expected recordingowner.Owner) bool {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	current, ok := e.owners[expected.RecordingID]
	return ok && current == expected
}

func validOwnerIdentity(recordingID string, owner recordingowner.Owner) bool {
	if recordingID == "" || owner.RecordingID != recordingID || owner.Epoch == 0 || len(recordingID) != 32 {
		return false
	}
	for _, c := range recordingID {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return validHostIdentity(owner.EngineGeneration) && validHostIdentity(owner.WorkerInstance)
}

func validHostIdentity(value string) bool {
	if len(value) == 32 || len(value) == 64 {
		for _, c := range value {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
		return true
	}
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func validHandoverSnapshot(snapshot acquire.HandoverSnapshot, expected recordingowner.Owner) bool {
	if !validOwnerIdentity(snapshot.RecordingID, snapshot.Owner) || snapshot.Owner != expected || !adapterproto.IsValidIdentifier(snapshot.AdapterID) {
		return false
	}
	if adapterproto.ValidateMediaSource(snapshot.Media, []string{"hls"}) != nil || adapterproto.ValidateResourceRef(snapshot.Resource) != nil {
		return false
	}
	return true
}

func handoverAck(recordingID string, e *Engine, owner *recordingowner.Owner) HandoverResult {
	return HandoverResult{RecordingID: recordingID, GenerationID: e.generation, InstanceID: e.instance, Owner: owner, Accepted: true}
}

func handoverPublicError(err error) error {
	if errors.Is(err, recordingowner.ErrStaleOwner) {
		return publicError("stale_owner", "recording ownership changed")
	}
	if errors.Is(err, acquire.ErrHandoverSourceRefreshRequired) {
		return publicError("source_refresh_required", "source must be drained before refreshing continuation")
	}
	if errors.Is(err, acquire.ErrHandoverSourceBoundaryRequired) {
		return publicError("source_boundary_required", "source must be drained before waiting for continuation media")
	}
	return publicError("handover_failed", "recording handover operation failed")
}

func (e *Engine) releaseOwner(ctx context.Context, owner recordingowner.Owner) error {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	if current, ok := e.owners[owner.RecordingID]; ok && current != owner {
		return recordingowner.ErrStaleOwner
	}
	if _, released := e.releasedOwners[owner]; released {
		return nil
	}
	if e.ownerClient == nil {
		return errors.New("recording owner client is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	releaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := e.ownerClient.ReleaseRecording(releaseCtx, owner); err != nil {
		if errors.Is(err, recordingowner.ErrStaleOwner) || errors.Is(err, recordingowner.ErrNotFound) {
			// Host already advanced or removed this exact owner. Drop only its
			// local map entry so it cannot block a later generation claim.
			if current, ok := e.owners[owner.RecordingID]; ok && current == owner {
				delete(e.owners, owner.RecordingID)
			}
		}
		return err
	}
	delete(e.owners, owner.RecordingID)
	e.releasedOwners[owner] = struct{}{}
	e.releasedOrder = append(e.releasedOrder, owner)
	if len(e.releasedOrder) > maxReleasedOwnerHistory {
		oldest := e.releasedOrder[0]
		e.releasedOrder = e.releasedOrder[1:]
		delete(e.releasedOwners, oldest)
	}
	return nil
}

func (e *Engine) activeRecordingCount(ctx context.Context) (int, error) {
	rows, err := e.manager.OwnedStates(MaximumListLimit)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows {
		if row.State == domain.StateRecording {
			count++
		}
	}
	return count, nil
}

// Close is called only by the engine process's own shutdown path. IPC client
// disconnects never call this method.
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	e.closed = true
	e.ready = false
	e.mu.Unlock()
	closeErr := e.manager.Close(ctx)
	// Manager.Close explicitly supports retrying after a caller deadline. Keep
	// adapter processes available until all acquisition workers have drained.
	if ctx == nil || ctx.Err() == nil {
		e.adapterCloseOnce.Do(e.adapters.Close)
	}
	return closeErr
}

type operationError struct {
	code    string
	message string
}

func (e operationError) Error() string                    { return e.message }
func (e operationError) PublicIPCError() (string, string) { return e.code, e.message }

func publicError(code, message string) error { return operationError{code: code, message: message} }

func decodePayload(data json.RawMessage, dst any) error {
	if len(data) == 0 || !json.Valid(data) {
		return errors.New("missing or malformed payload")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing payload data")
	}
	return nil
}
