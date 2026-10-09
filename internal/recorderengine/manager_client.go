package recorderengine

import (
	"context"
	"errors"
	"fmt"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

// ErrManagementRootCacheInvalidationUnsupported is returned by Engines that
// predate the optional management root cache operation. Such Engines have no
// cache to refresh; all other IPC failures remain fatal to the read snapshot.
var ErrManagementRootCacheInvalidationUnsupported = errors.New("recorder engine does not support management root cache invalidation")

// ManagerClient adapts one generation-pinned Engine IPC client to the
// recordingManager method set consumed by the Control Plane. It has no local
// storage fallback: all lifecycle mutations are sent to the selected Engine.
type ManagerClient struct {
	client *runtimeipc.Client
}

var ErrEngineDraining = errors.New("recorder engine is draining")

var (
	ErrHandoverIdentity               = errors.New("recorder engine handover identity is invalid")
	ErrHandoverStaleOwner             = errors.New("recording ownership changed")
	ErrHandoverOperation              = errors.New("recorder engine handover operation failed")
	ErrHandoverSourceRefreshRequired  = acquire.ErrHandoverSourceRefreshRequired
	ErrHandoverSourceBoundaryRequired = acquire.ErrHandoverSourceBoundaryRequired
)

func NewManagerClient(client *runtimeipc.Client) (*ManagerClient, error) {
	if client == nil {
		return nil, errors.New("recorder engine IPC client is required")
	}
	return &ManagerClient{client: client}, nil
}

func (m *ManagerClient) StartResolved(ctx context.Context, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	return m.start(ctx, StartResolvedRequest{AdapterID: adapterID, Media: media, Resource: resource, Title: title, Provenance: provenance})
}

func (m *ManagerClient) StartResolvedWithID(ctx context.Context, id, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	return m.start(ctx, StartResolvedRequest{RecordingID: id, AdapterID: adapterID, Media: media, Resource: resource, Title: title, Provenance: provenance})
}

func (m *ManagerClient) start(ctx context.Context, request StartResolvedRequest) (*domain.Recording, error) {
	var recording domain.Recording
	if err := m.client.Call(ctx, OperationStartResolved, request, &recording); err != nil {
		return nil, normalizeEngineError(err)
	}
	return &recording, nil
}

// BeginDrain fences new recording admission on the specified generation. The
// engine waits for any already-admitted start call to finish before replying.
// Existing recording workers are not affected.
func (m *ManagerClient) BeginDrain(ctx context.Context, generationID string) error {
	if generationID == "" {
		return errors.New("recorder engine generation identity is required")
	}
	var result DrainResult
	if err := m.client.Call(ctx, OperationBeginDrain, GenerationRequest{GenerationID: generationID}, &result); err != nil {
		return normalizeEngineError(err)
	}
	if result.GenerationID != generationID || result.InstanceID == "" || !result.Draining {
		return errors.New("recorder engine drain response identity is invalid")
	}
	return nil
}

// ActiveRecordings returns a bounded count of this Engine's owned active
// recordings, excluding read-only archive snapshots from other generations.
func (m *ManagerClient) ActiveRecordings(ctx context.Context, generationID string) (int, error) {
	if generationID == "" {
		return 0, errors.New("recorder engine generation identity is required")
	}
	var result ActiveRecordingCountResult
	if err := m.client.Call(ctx, OperationActiveCount, GenerationRequest{GenerationID: generationID}, &result); err != nil {
		return 0, normalizeEngineError(err)
	}
	if result.GenerationID != generationID || result.InstanceID == "" || result.Count < 0 || result.Count > MaximumListLimit {
		return 0, errors.New("recorder engine active recording count response is invalid")
	}
	return result.Count, nil
}

// HandoverSnapshot reads the exact source Engine continuation snapshot. It is
// an internal Runtime Host primitive; callers must keep the returned media
// context private and must not log or expose it through public APIs.
func (m *ManagerClient) HandoverSnapshot(ctx context.Context, recordingID string, expectedOwner recordingowner.Owner) (acquire.HandoverSnapshot, error) {
	if !validClientOwner(recordingID, expectedOwner) {
		return acquire.HandoverSnapshot{}, ErrHandoverIdentity
	}
	var result HandoverSnapshotResult
	if err := m.client.Call(ctx, OperationHandoverSnapshot, HandoverOwnerRequest{RecordingID: recordingID, Owner: expectedOwner}, &result); err != nil {
		return acquire.HandoverSnapshot{}, normalizeHandoverError(err)
	}
	if !validHandoverResultIdentity(result.RecordingID, result.GenerationID, result.InstanceID, recordingID, expectedOwner.EngineGeneration, expectedOwner.WorkerInstance) || !validHandoverSnapshot(result.Snapshot, expectedOwner) {
		return acquire.HandoverSnapshot{}, ErrHandoverIdentity
	}
	return result.Snapshot, nil
}

// PauseForHandover asks the source Engine to reach a safe, drained commit
// boundary while leaving its Recording and worker recoverable for resume.
func (m *ManagerClient) PauseForHandover(ctx context.Context, recordingID string, expectedOwner recordingowner.Owner) (acquire.HandoverSnapshot, error) {
	if !validClientOwner(recordingID, expectedOwner) {
		return acquire.HandoverSnapshot{}, ErrHandoverIdentity
	}
	var result HandoverSnapshotResult
	if err := m.client.Call(ctx, OperationHandoverPause, HandoverOwnerRequest{RecordingID: recordingID, Owner: expectedOwner}, &result); err != nil {
		return acquire.HandoverSnapshot{}, normalizeHandoverError(err)
	}
	if !validHandoverResultIdentity(result.RecordingID, result.GenerationID, result.InstanceID, recordingID, expectedOwner.EngineGeneration, expectedOwner.WorkerInstance) || !validHandoverSnapshot(result.Snapshot, expectedOwner) {
		return acquire.HandoverSnapshot{}, ErrHandoverIdentity
	}
	return result.Snapshot, nil
}

// ResumeHandover re-enables the source worker with the Host-issued current
// owner tuple (which can be newer after an aborted reverse transfer).
func (m *ManagerClient) ResumeHandover(ctx context.Context, recordingID string, newOwner recordingowner.Owner, snapshot acquire.HandoverSnapshot) error {
	if !validClientOwner(recordingID, newOwner) || !validHandoverSnapshot(snapshot, snapshot.Owner) || snapshot.RecordingID != recordingID || newOwner.EngineGeneration != snapshot.Owner.EngineGeneration || newOwner.WorkerInstance != snapshot.Owner.WorkerInstance || newOwner.Epoch < snapshot.Owner.Epoch {
		return ErrHandoverIdentity
	}
	var result HandoverResult
	request := HandoverResumeRequest{RecordingID: recordingID, Owner: newOwner, Snapshot: snapshot}
	if err := m.client.Call(ctx, OperationHandoverResume, request, &result); err != nil {
		return normalizeHandoverError(err)
	}
	return validateHandoverAck(result, recordingID, newOwner, true)
}

// CompleteHandover retires a paused source worker only after the Host has
// activated the target owner. It does not release or transfer Host ownership.
func (m *ManagerClient) CompleteHandover(ctx context.Context, recordingID string, expectedOldOwner recordingowner.Owner) error {
	if !validClientOwner(recordingID, expectedOldOwner) {
		return ErrHandoverIdentity
	}
	var result HandoverResult
	if err := m.client.Call(ctx, OperationHandoverComplete, HandoverOwnerRequest{RecordingID: recordingID, Owner: expectedOldOwner}, &result); err != nil {
		return normalizeHandoverError(err)
	}
	return validateHandoverAck(result, recordingID, expectedOldOwner, true)
}

// PrepareHandoverTarget asks this exact target Engine to reconstruct a
// continuation read-only. It receives no canonical write token at this stage.
func (m *ManagerClient) PrepareHandoverTarget(ctx context.Context, snapshot acquire.HandoverSnapshot, target acquire.HandoverTargetIdentity) error {
	return m.prepareHandoverTarget(ctx, snapshot, target, false)
}

// PrepareHandoverTargetAfterSourceDrain is only used by the Runtime Host once
// the source Engine has stopped admission and drained its accepted writes.
func (m *ManagerClient) PrepareHandoverTargetAfterSourceDrain(ctx context.Context, snapshot acquire.HandoverSnapshot, target acquire.HandoverTargetIdentity) error {
	return m.prepareHandoverTarget(ctx, snapshot, target, true)
}

func (m *ManagerClient) prepareHandoverTarget(ctx context.Context, snapshot acquire.HandoverSnapshot, target acquire.HandoverTargetIdentity, sourceDrained bool) error {
	if !validHandoverSnapshot(snapshot, snapshot.Owner) || target.EngineGeneration == "" || target.WorkerInstance == "" || snapshot.Owner.EngineGeneration == target.EngineGeneration && snapshot.Owner.WorkerInstance == target.WorkerInstance {
		return ErrHandoverIdentity
	}
	var result HandoverResult
	if err := m.client.Call(ctx, OperationHandoverPrepareTarget, HandoverTargetRequest{Snapshot: snapshot, Target: target, SourceDrained: sourceDrained}, &result); err != nil {
		return normalizeHandoverError(err)
	}
	if !validHandoverResultIdentity(result.RecordingID, result.GenerationID, result.InstanceID, snapshot.RecordingID, target.EngineGeneration, target.WorkerInstance) || !result.Accepted || result.Owner == nil || *result.Owner != snapshot.Owner {
		return ErrHandoverIdentity
	}
	return nil
}

// ActivatePreparedHandover passes the exact Host-issued post-transfer token
// into the Manager. The Manager's canonical fence independently verifies it
// before starting any writer.
func (m *ManagerClient) ActivatePreparedHandover(ctx context.Context, owner recordingowner.Owner, target acquire.HandoverTargetIdentity) error {
	if !validClientOwner(owner.RecordingID, owner) || target.EngineGeneration != owner.EngineGeneration || target.WorkerInstance != owner.WorkerInstance {
		return ErrHandoverIdentity
	}
	var result HandoverResult
	request := HandoverTargetOwnerRequest{RecordingID: owner.RecordingID, Owner: owner, Target: target}
	if err := m.client.Call(ctx, OperationHandoverActivateTarget, request, &result); err != nil {
		return normalizeHandoverError(err)
	}
	return validateHandoverAck(result, owner.RecordingID, owner, true)
}

// DiscardPreparedHandover removes only target-side read-only preparation.
func (m *ManagerClient) DiscardPreparedHandover(ctx context.Context, recordingID string, target acquire.HandoverTargetIdentity) error {
	if recordingID == "" || target.EngineGeneration == "" || target.WorkerInstance == "" {
		return ErrHandoverIdentity
	}
	var result HandoverResult
	request := HandoverTargetDiscardRequest{RecordingID: recordingID, Target: target}
	if err := m.client.Call(ctx, OperationHandoverDiscardTarget, request, &result); err != nil {
		return normalizeHandoverError(err)
	}
	if !validHandoverResultIdentity(result.RecordingID, result.GenerationID, result.InstanceID, recordingID, target.EngineGeneration, target.WorkerInstance) || !result.Accepted || result.Owner != nil {
		return ErrHandoverIdentity
	}
	return nil
}

func validClientOwner(recordingID string, owner recordingowner.Owner) bool {
	return validOwnerIdentity(recordingID, owner)
}

func validHandoverResultIdentity(gotID, gotGeneration, gotInstance, wantID, wantGeneration, wantInstance string) bool {
	return gotID == wantID && gotGeneration == wantGeneration && gotInstance == wantInstance
}

func validateHandoverAck(result HandoverResult, recordingID string, owner recordingowner.Owner, requireOwner bool) error {
	if !validHandoverResultIdentity(result.RecordingID, result.GenerationID, result.InstanceID, recordingID, owner.EngineGeneration, owner.WorkerInstance) || !result.Accepted {
		return ErrHandoverIdentity
	}
	if requireOwner && (result.Owner == nil || *result.Owner != owner) {
		return ErrHandoverIdentity
	}
	return nil
}

func normalizeHandoverError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var remote *runtimeipc.RemoteError
	if !errors.As(err, &remote) {
		return ErrHandoverOperation
	}
	switch remote.Code {
	case "stale_owner":
		return ErrHandoverStaleOwner
	case "source_refresh_required":
		return ErrHandoverSourceRefreshRequired
	case "source_boundary_required":
		return ErrHandoverSourceBoundaryRequired
	case "invalid_request", "generation_mismatch", "handover_rejected":
		return ErrHandoverIdentity
	default:
		return ErrHandoverOperation
	}
}

func (m *ManagerClient) Get(id string) (*domain.Recording, error) {
	return m.GetContext(context.Background(), id)
}

func (m *ManagerClient) GetContext(ctx context.Context, id string) (*domain.Recording, error) {
	var recording domain.Recording
	if err := m.client.Call(ctx, OperationGet, RecordingIDRequest{RecordingID: id}, &recording); err != nil {
		return nil, normalizeEngineError(err)
	}
	return &recording, nil
}

// GetManagementHeader reads bounded recording root without materializing V2
// archive shards. Use Get only for workflows that need full archive history.
func (m *ManagerClient) GetManagementHeader(ctx context.Context, id string) (*domain.Recording, error) {
	if id == "" {
		return nil, errors.New("recording identity is required")
	}
	var recording domain.Recording
	if err := m.client.Call(ctx, OperationGetManagementHeader, RecordingIDRequest{RecordingID: id}, &recording); err != nil {
		return nil, normalizeEngineError(err)
	}
	return &recording, nil
}

// InvalidateManagementRootCache refreshes this Engine's process-local root ID
// snapshot before it becomes the active management read generation.
func (m *ManagerClient) InvalidateManagementRootCache(ctx context.Context) error {
	var result struct {
		Invalidated bool `json:"invalidated"`
	}
	if err := m.client.Call(ctx, OperationInvalidateManagementRootCache, nil, &result); err != nil {
		var remote *runtimeipc.RemoteError
		if errors.As(err, &remote) && remote.Code == "unsupported_operation" {
			return ErrManagementRootCacheInvalidationUnsupported
		}
		return normalizeEngineError(err)
	}
	if !result.Invalidated {
		return errors.New("recorder engine did not invalidate management root cache")
	}
	return nil
}

// LifecycleSnapshot requests a bounded lifecycle projection from this Engine.
func (m *ManagerClient) LifecycleSnapshot(ctx context.Context, id string) (acquire.LifecycleSnapshot, error) {
	var snapshot acquire.LifecycleSnapshot
	if err := m.client.Call(ctx, OperationLifecycleSnapshot, RecordingIDRequest{RecordingID: id}, &snapshot); err != nil {
		return acquire.LifecycleSnapshot{}, normalizeEngineError(err)
	}
	if err := validateLifecycleSnapshot(snapshot, id); err != nil {
		return acquire.LifecycleSnapshot{}, err
	}
	return snapshot, nil
}

func validateLifecycleSnapshot(snapshot acquire.LifecycleSnapshot, id string) error {
	if snapshot.RecordingID != id || snapshot.Repairable == snapshot.ArchiveSealed {
		return errors.New("recorder engine lifecycle snapshot is invalid")
	}
	if snapshot.ArchiveSealed {
		if snapshot.RecoveryState != "sealed" {
			return errors.New("recorder engine lifecycle snapshot is invalid")
		}
		return nil
	}
	switch snapshot.RecoveryState {
	case "idle", "scheduled", "backoff", "running":
		return nil
	default:
		return errors.New("recorder engine lifecycle snapshot is invalid")
	}
}

// LivePlaybackSnapshot reads only the bounded active live tail from this
// generation. The Engine IPC operation avoids transferring a full recording
// root for browser playlist reloads.
func (m *ManagerClient) LivePlaybackSnapshot(ctx context.Context, id string) (acquire.LivePlaybackView, error) {
	var view acquire.LivePlaybackView
	if err := m.client.Call(ctx, OperationLivePlaybackSnapshot, RecordingIDRequest{RecordingID: id}, &view); err != nil {
		return acquire.LivePlaybackView{}, normalizeEngineError(err)
	}
	if view.RecordingID != id || len(view.Segments) > acquire.LivePlaybackWindowSize ||
		len(view.Slots) > acquire.LivePlaybackWindowSize || len(view.InitSegments) > acquire.LivePlaybackWindowSize {
		return acquire.LivePlaybackView{}, errors.New("recorder engine live playback snapshot is invalid")
	}
	for _, segment := range view.Segments {
		if segment.SourceURI != "" {
			return acquire.LivePlaybackView{}, errors.New("recorder engine live playback snapshot contains source URI material")
		}
	}
	for _, slot := range view.Slots {
		if slot.Segment != nil && slot.Segment.SourceURI != "" {
			return acquire.LivePlaybackView{}, errors.New("recorder engine live playback slot contains source URI material")
		}
	}
	for _, segment := range view.InitSegments {
		if segment.SourceURI != "" {
			return acquire.LivePlaybackView{}, errors.New("recorder engine live playback init contains source URI material")
		}
	}
	return view, nil
}

// Inventory returns the active recordings owned by this exact Engine process.
// The transport envelope pins the generation; the result is checked as well
// so callers do not accidentally associate another Engine's inventory.
func (m *ManagerClient) Inventory(ctx context.Context) (InventoryResult, error) {
	var inventory InventoryResult
	if err := m.client.Call(ctx, OperationInventory, nil, &inventory); err != nil {
		return InventoryResult{}, normalizeEngineError(err)
	}
	if inventory.GenerationID == "" || inventory.InstanceID == "" {
		return InventoryResult{}, errors.New("recorder engine inventory identity is invalid")
	}
	for _, item := range inventory.Active {
		if item.RecordingID == "" || item.State != domain.StateRecording {
			return InventoryResult{}, errors.New("recorder engine inventory entry is invalid")
		}
		if item.Owner != nil && !validOwnerIdentity(item.RecordingID, *item.Owner) {
			return InventoryResult{}, ErrHandoverIdentity
		}
		if item.Owner != nil && (item.Owner.EngineGeneration != inventory.GenerationID || item.Owner.WorkerInstance != inventory.InstanceID) {
			return InventoryResult{}, ErrHandoverIdentity
		}
	}
	return inventory, nil
}

func (m *ManagerClient) List() []*domain.Recording {
	rows, err := m.ListForManagement(context.Background(), DefaultListLimit)
	if err != nil {
		return nil
	}
	return rows
}

func (m *ManagerClient) ListForManagement(ctx context.Context, limit int) ([]*domain.Recording, error) {
	if limit < 1 || limit > MaximumListLimit {
		return nil, acquire.ErrListLimit
	}
	var recordings []*domain.Recording
	if err := m.client.Call(ctx, OperationList, ListRequest{Limit: limit}, &recordings); err != nil {
		return nil, normalizeEngineError(err)
	}
	if recordings == nil {
		return []*domain.Recording{}, nil
	}
	return recordings, nil
}

// ListForManagementPage transfers one bounded summary page over Engine IPC.
// Cursors use stable recording IDs and are not a snapshot across concurrent
// creates or deletes.
func (m *ManagerClient) ListForManagementPage(ctx context.Context, afterID string, limit int) ([]*domain.Recording, string, error) {
	if limit < 1 || limit > MaximumListPageLimit || len(afterID) > 64 {
		return nil, "", acquire.ErrListLimit
	}
	var result ListPageResult
	if err := m.client.Call(ctx, OperationListPage, ListPageRequest{AfterID: afterID, Limit: limit}, &result); err != nil {
		return nil, "", normalizeEngineError(err)
	}
	if len(result.Items) > limit || result.NextCursor != "" && result.NextCursor <= afterID {
		return nil, "", errors.New("recorder engine returned an invalid recording summary page")
	}
	previousID := afterID
	for _, item := range result.Items {
		if item == nil || item.ID == "" || item.ID <= previousID {
			return nil, "", errors.New("recorder engine returned an invalid recording summary page")
		}
		previousID = item.ID
	}
	if result.NextCursor != "" && len(result.Items) > 0 && result.NextCursor < result.Items[len(result.Items)-1].ID {
		return nil, "", errors.New("recorder engine returned an invalid recording summary cursor")
	}
	if result.Items == nil {
		result.Items = []*domain.Recording{}
	}
	return result.Items, result.NextCursor, nil
}

func (m *ManagerClient) Stop(id string) (*domain.Recording, error) {
	return m.StopContext(context.Background(), id)
}

func (m *ManagerClient) StopContext(ctx context.Context, id string) (*domain.Recording, error) {
	var recording domain.Recording
	if err := m.client.Call(ctx, OperationStop, RecordingIDRequest{RecordingID: id}, &recording); err != nil {
		return nil, normalizeEngineError(err)
	}
	return &recording, nil
}

// CompleteRecording marks a stopped capture completed through its owning
// Engine. Completion does not seal the archive.
func (m *ManagerClient) CompleteRecording(ctx context.Context, id string) (*domain.Recording, error) {
	var recording domain.Recording
	if err := m.client.Call(ctx, OperationComplete, RecordingIDRequest{RecordingID: id}, &recording); err != nil {
		return nil, normalizeEngineError(err)
	}
	return &recording, nil
}

// SealArchiveContext explicitly ends repairability through the Engine's
// recovery gate and canonical owner fence.
func (m *ManagerClient) SealArchiveContext(ctx context.Context, id string) error {
	var result struct {
		Sealed bool `json:"sealed"`
	}
	if err := m.client.Call(ctx, OperationSealArchive, RecordingIDRequest{RecordingID: id}, &result); err != nil {
		return normalizeEngineError(err)
	}
	if !result.Sealed {
		return errors.New("recorder engine did not confirm archive sealing")
	}
	return nil
}

func (m *ManagerClient) Delete(id string) error {
	return m.DeleteContext(context.Background(), id)
}

func (m *ManagerClient) DeleteContext(ctx context.Context, id string) error {
	var result struct {
		Deleted bool `json:"deleted"`
	}
	if err := m.client.Call(ctx, OperationDelete, RecordingIDRequest{RecordingID: id}, &result); err != nil {
		return normalizeEngineError(err)
	}
	if !result.Deleted {
		return errors.New("recorder engine did not confirm recording deletion")
	}
	return nil
}

// DeleteTerminalArchive asks this Engine to delete a terminal archive after
// the Host-side ManagerRouter has checked every generation's live inventory.
func (m *ManagerClient) DeleteTerminalArchive(ctx context.Context, id string) error {
	var result struct {
		Deleted bool `json:"deleted"`
	}
	if err := m.client.Call(ctx, OperationDeleteTerminal, RecordingIDRequest{RecordingID: id}, &result); err != nil {
		return normalizeEngineError(err)
	}
	if !result.Deleted {
		return errors.New("recorder engine did not confirm terminal archive deletion")
	}
	return nil
}

func normalizeEngineError(err error) error {
	var remote *runtimeipc.RemoteError
	if !errors.As(err, &remote) {
		return errors.New("recorder engine IPC operation failed")
	}
	switch remote.Code {
	case "generation_mismatch":
		return errors.New("recorder engine generation identity mismatch")
	case "engine_draining":
		return ErrEngineDraining
	case "not_found", "not_owned":
		return storage.ErrNotFound
	case "active_recording":
		return acquire.ErrActiveRecording
	case "lifecycle_conflict":
		return acquire.ErrLifecycleConflict
	case "read_failed":
		return errors.New("recording lifecycle could not be read")
	case "list_too_large", "inventory_too_large":
		return acquire.ErrListLimit
	case "start_failed":
		return errors.New("recording could not be started")
	case "stop_failed":
		return errors.New("recording could not be stopped")
	case "complete_failed":
		return errors.New("recording could not be completed")
	case "seal_failed":
		return errors.New("archive could not be sealed")
	case "delete_failed":
		return errors.New("recording could not be deleted")
	default:
		if remote.Message != "" {
			return fmt.Errorf("recorder engine: %s", remote.Message)
		}
		return errors.New("recorder engine operation failed")
	}
}

var _ interface {
	StartResolved(context.Context, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error)
	StartResolvedWithID(context.Context, string, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error)
	Get(string) (*domain.Recording, error)
	List() []*domain.Recording
	ListForManagement(context.Context, int) ([]*domain.Recording, error)
	Stop(string) (*domain.Recording, error)
	LifecycleSnapshot(context.Context, string) (acquire.LifecycleSnapshot, error)
	CompleteRecording(context.Context, string) (*domain.Recording, error)
	SealArchiveContext(context.Context, string) error
	Delete(string) error
} = (*ManagerClient)(nil)
