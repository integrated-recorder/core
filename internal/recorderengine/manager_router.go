package recorderengine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

var (
	ErrGenerationNotAttached        = errors.New("recorder engine generation is not attached")
	ErrCannotDetachActiveGeneration = errors.New("active recorder engine generation cannot be detached")
	ErrGenerationHasActiveRecords   = errors.New("recorder engine generation still owns active recordings")
	ErrMultipleRecordingOwners      = errors.New("recording is owned by multiple recorder engines")
	ErrRecordingMayBeActive         = errors.New("recording ownership is uncertain; deletion was refused")
)

// GenerationInventory is the bounded active-recording inventory returned by
// one attached Engine. It intentionally excludes read-only archive snapshots.
type GenerationInventory = InventoryResult

// ManagerRouter implements the Control Plane recordingManager surface across
// multiple generation-pinned Engines. All lifecycle writes are routed over
// authenticated Engine IPC; Store is only a safe terminal-archive deletion
// fallback after every Engine explicitly disclaims ownership.
type ManagerRouter struct {
	mu                        sync.RWMutex
	mutation                  sync.RWMutex
	managementCacheMu         sync.Mutex
	managementCacheGeneration string
	active                    string
	clients                   map[string]*ManagerClient
	store                     *storage.Store
}

func NewManagerRouter(activeID string, clients map[string]*ManagerClient, store *storage.Store) (*ManagerRouter, error) {
	if activeID == "" || !validGenerationID(activeID) {
		return nil, errors.New("active recorder engine generation is invalid")
	}
	if len(clients) == 0 {
		return nil, errors.New("at least one recorder engine client is required")
	}
	attached := make(map[string]*ManagerClient, len(clients))
	for generationID, client := range clients {
		if !validGenerationID(generationID) || client == nil {
			return nil, errors.New("recorder engine client map is invalid")
		}
		attached[generationID] = client
	}
	if attached[activeID] == nil {
		return nil, ErrGenerationNotAttached
	}
	return &ManagerRouter{active: activeID, clients: attached, store: store}, nil
}

func validGenerationID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for i, char := range value {
		valid := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || (i > 0 && (char == '.' || char == '_' || char == '-'))
		if !valid {
			return false
		}
	}
	return true
}

// SetActive atomically routes subsequent starts to an already attached Engine.
// The mutation barrier waits for in-flight starts to finish before switching.
func (r *ManagerRouter) SetActive(generationID string) error {
	r.mutation.Lock()
	defer r.mutation.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients[generationID] == nil {
		return ErrGenerationNotAttached
	}
	if r.active != generationID {
		r.managementCacheMu.Lock()
		r.managementCacheGeneration = ""
		r.managementCacheMu.Unlock()
	}
	r.active = generationID
	return nil
}

// AddGeneration attaches an already configured, generation-pinned IPC client.
func (r *ManagerRouter) AddGeneration(generationID string, client *ManagerClient) error {
	if !validGenerationID(generationID) || client == nil {
		return errors.New("recorder engine generation client is invalid")
	}
	r.mutation.Lock()
	defer r.mutation.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.clients[generationID]; exists {
		return fmt.Errorf("%w: %s", ErrGenerationAlreadyAttached, generationID)
	}
	r.clients[generationID] = client
	return nil
}

var ErrGenerationAlreadyAttached = errors.New("recorder engine generation is already attached")

// DetachGeneration removes a non-default Engine only after its live inventory
// is empty. The mutation barrier prevents a start routed through this router
// from racing the inventory check.
func (r *ManagerRouter) DetachGeneration(generationID string) error {
	return r.DetachGenerationContext(context.Background(), generationID)
}

// DetachGenerationContext removes a non-default Engine only after its live
// inventory is empty. The mutation barrier prevents routed starts and active
// generation changes from racing the inventory check. The client is removed
// only after IPC completes and its identity is rechecked under r.mu.
func (r *ManagerRouter) DetachGenerationContext(ctx context.Context, generationID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mutation.Lock()
	defer r.mutation.Unlock()
	r.mu.Lock()
	if generationID == r.active {
		r.mu.Unlock()
		return ErrCannotDetachActiveGeneration
	}
	client := r.clients[generationID]
	if client == nil {
		r.mu.Unlock()
		return ErrGenerationNotAttached
	}
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	inventory, err := client.Inventory(ctx)
	if err != nil {
		return fmt.Errorf("engine inventory unavailable: %w", err)
	}
	if inventory.GenerationID != generationID {
		return errors.New("engine inventory generation identity mismatch")
	}
	if len(inventory.Active) != 0 {
		return ErrGenerationHasActiveRecords
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if generationID == r.active {
		return ErrCannotDetachActiveGeneration
	}
	if r.clients[generationID] != client {
		if r.clients[generationID] == nil {
			return ErrGenerationNotAttached
		}
		return errors.New("recorder engine generation attachment changed during detach")
	}
	delete(r.clients, generationID)
	return nil
}

// Inventory returns a stable generation-sorted snapshot of every attached
// Engine's active recordings. Any failed IPC inventory makes the whole result
// uncertain and is surfaced to the caller.
func (r *ManagerRouter) Inventory(ctx context.Context) ([]GenerationInventory, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	clients := r.clientsSnapshot()
	result := make([]GenerationInventory, 0, len(clients))
	for _, attached := range clients {
		inventory, err := attached.client.Inventory(ctx)
		if err != nil {
			return nil, fmt.Errorf("inventory for generation %s failed: %w", attached.generationID, err)
		}
		if inventory.GenerationID != attached.generationID {
			return nil, fmt.Errorf("inventory generation identity mismatch for %s", attached.generationID)
		}
		result = append(result, inventory)
	}
	return result, nil
}

func (r *ManagerRouter) StartResolved(ctx context.Context, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	client, err := r.activeClient()
	if err != nil {
		return nil, err
	}
	return client.StartResolved(ctx, adapterID, media, resource, title, provenance)
}

func (r *ManagerRouter) StartResolvedWithID(ctx context.Context, id, adapterID string, media adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	client, err := r.activeClient()
	if err != nil {
		return nil, err
	}
	return client.StartResolvedWithID(ctx, id, adapterID, media, resource, title, provenance)
}

func (r *ManagerRouter) Get(id string) (*domain.Recording, error) {
	return r.GetContext(context.Background(), id)
}

func (r *ManagerRouter) GetContext(ctx context.Context, id string) (*domain.Recording, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	clients, activeID := r.clientsAndActive()
	inventories := make([]GenerationInventory, 0, len(clients))
	for _, attached := range clients {
		inventory, err := attached.client.Inventory(ctx)
		if err != nil {
			return nil, fmt.Errorf("engine inventory is required to identify the recording owner: %w", err)
		}
		if inventory.GenerationID != attached.generationID {
			return nil, errors.New("engine inventory generation identity mismatch")
		}
		inventories = append(inventories, inventory)
	}
	owners, err := ownerMap(inventories)
	if err != nil {
		return nil, err
	}
	results := make(map[string]*domain.Recording, len(clients))
	var firstReadErr error
	for _, attached := range clients {
		recording, getErr := attached.client.GetContext(ctx, id)
		if getErr == nil {
			results[attached.generationID] = recording
			continue
		}
		if !errors.Is(getErr, storage.ErrNotFound) && firstReadErr == nil {
			firstReadErr = getErr
		}
	}
	if ownerID := owners[id]; ownerID != "" {
		if recording := results[ownerID]; recording != nil {
			return recording, nil
		}
		if firstReadErr != nil {
			return nil, firstReadErr
		}
		return nil, storage.ErrNotFound
	}
	if active := results[activeID]; active != nil {
		return active, nil
	}
	if firstReadErr != nil {
		return nil, firstReadErr
	}
	// The active generation may not have an archive snapshot during an atomic
	// deletion/read race. Prefer a deterministic generation order for terminal
	// snapshots rather than Go map iteration order.
	for _, attached := range clients {
		if recording := results[attached.generationID]; recording != nil {
			return recording, nil
		}
	}
	return nil, storage.ErrNotFound
}

// LifecycleSnapshot reads bounded lifecycle fields. Owner hints select a
// terminal recovery Engine without cloning the archive root.
func (r *ManagerRouter) LifecycleSnapshot(ctx context.Context, id string) (acquire.LifecycleSnapshot, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	clients, activeID := r.clientsAndActive()
	type snapshotResult struct {
		client   attachedClient
		snapshot acquire.LifecycleSnapshot
	}
	results := make([]snapshotResult, 0, len(clients))
	var firstReadErr error
	for _, attached := range clients {
		snapshot, err := attached.client.LifecycleSnapshot(ctx, id)
		if err != nil {
			if !errors.Is(err, storage.ErrNotFound) && firstReadErr == nil {
				firstReadErr = err
			}
			continue
		}
		results = append(results, snapshotResult{client: attached, snapshot: snapshot})
	}
	if len(results) == 0 {
		if firstReadErr != nil {
			return acquire.LifecycleSnapshot{}, firstReadErr
		}
		return acquire.LifecycleSnapshot{}, storage.ErrNotFound
	}
	var owner *snapshotResult
	for i := range results {
		candidate := &results[i]
		if !candidate.snapshot.EngineOwns {
			continue
		}
		if owner == nil || candidate.snapshot.OwnerEpoch > owner.snapshot.OwnerEpoch {
			owner = candidate
			continue
		}
		if candidate.snapshot.OwnerEpoch == owner.snapshot.OwnerEpoch {
			return acquire.LifecycleSnapshot{}, ErrMultipleRecordingOwners
		}
	}
	if owner != nil {
		return owner.snapshot, nil
	}
	for _, candidate := range results {
		if candidate.client.generationID == activeID {
			return candidate.snapshot, nil
		}
	}
	if firstReadErr != nil {
		return acquire.LifecycleSnapshot{}, firstReadErr
	}
	return results[0].snapshot, nil
}

// LivePlaybackSnapshot routes to the generation that owns the live worker and
// returns only its bounded live-tail projection. Terminal recordings have no
// active owner; in that case return only state so the HTTP layer can preserve
// its existing conflict response without cloning a terminal archive root.
func (r *ManagerRouter) LivePlaybackSnapshot(ctx context.Context, id string) (acquire.LivePlaybackView, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	clients, activeID := r.clientsAndActive()
	inventories := make([]GenerationInventory, 0, len(clients))
	for _, attached := range clients {
		inventory, err := attached.client.Inventory(ctx)
		if err != nil {
			return acquire.LivePlaybackView{}, fmt.Errorf("engine inventory is required to identify the live recording owner: %w", err)
		}
		if inventory.GenerationID != attached.generationID {
			return acquire.LivePlaybackView{}, errors.New("engine inventory generation identity mismatch")
		}
		inventories = append(inventories, inventory)
	}
	owners, err := ownerMap(inventories)
	if err != nil {
		return acquire.LivePlaybackView{}, err
	}
	if ownerID := owners[id]; ownerID != "" {
		owner := findAttached(clients, ownerID)
		if owner == nil {
			return acquire.LivePlaybackView{}, ErrGenerationNotAttached
		}
		return owner.client.LivePlaybackSnapshot(ctx, id)
	}
	active := findAttached(clients, activeID)
	if active == nil {
		return acquire.LivePlaybackView{}, ErrGenerationNotAttached
	}
	recording, err := active.client.GetContext(ctx, id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			for _, attached := range clients {
				recording, err = attached.client.GetContext(ctx, id)
				if err == nil {
					break
				}
				if !errors.Is(err, storage.ErrNotFound) {
					return acquire.LivePlaybackView{}, err
				}
			}
		}
		if err != nil {
			return acquire.LivePlaybackView{}, err
		}
	}
	return acquire.LivePlaybackView{RecordingID: id, State: recording.State}, nil
}

func (r *ManagerRouter) List() []*domain.Recording {
	rows, err := r.ListForManagement(context.Background(), DefaultListLimit)
	if err != nil {
		return nil
	}
	return rows
}

func (r *ManagerRouter) ListForManagement(ctx context.Context, limit int) ([]*domain.Recording, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	if limit < 1 || limit > MaximumListLimit {
		return nil, acquire.ErrListLimit
	}
	clients, activeID := r.clientsAndActive()
	inventories := make([]GenerationInventory, 0, len(clients))
	for _, attached := range clients {
		inventory, err := attached.client.Inventory(ctx)
		if err != nil {
			return nil, fmt.Errorf("engine inventory is required for a consistent recording list: %w", err)
		}
		if inventory.GenerationID != attached.generationID {
			return nil, errors.New("engine inventory generation identity mismatch")
		}
		inventories = append(inventories, inventory)
	}
	owners, err := ownerMap(inventories)
	if err != nil {
		return nil, err
	}
	active := findAttached(clients, activeID)
	if active == nil {
		return nil, ErrGenerationNotAttached
	}
	if err := r.ensureManagementRootCache(ctx, activeID, active.client); err != nil {
		return nil, fmt.Errorf("active Engine management snapshot could not be refreshed: %w", err)
	}
	rows, err := active.client.ListForManagement(ctx, limit)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*domain.Recording, len(rows)+len(owners))
	for _, row := range rows {
		if row == nil || row.ID == "" {
			return nil, errors.New("engine returned an invalid recording list entry")
		}
		byID[row.ID] = row
	}
	for _, recordingID := range sortedOwnerIDs(owners) {
		owner := findAttached(clients, owners[recordingID])
		if owner == nil {
			return nil, ErrGenerationNotAttached
		}
		row, getErr := owner.client.GetManagementHeader(ctx, recordingID)
		if getErr != nil {
			return nil, fmt.Errorf("live recording owner read failed: %w", getErr)
		}
		byID[recordingID] = row
	}
	if len(byID) > limit {
		return nil, acquire.ErrListLimit
	}
	result := make([]*domain.Recording, 0, len(byID))
	for _, row := range byID {
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})
	return result, nil
}

// ListForManagementPage returns one bounded page from the active Engine's
// summary index. Owner headers replace stale read-only copies without changing
// ID cursor order. Concurrent creates before the cursor may appear only in a
// later traversal; deleted rows may disappear from later pages.
func (r *ManagerRouter) ListForManagementPage(ctx context.Context, afterID string, limit int) ([]*domain.Recording, string, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	if limit < 1 || limit > MaximumListPageLimit || len(afterID) > 64 {
		return nil, "", acquire.ErrListLimit
	}
	clients, activeID := r.clientsAndActive()
	inventories := make([]GenerationInventory, 0, len(clients))
	for _, attached := range clients {
		inventory, err := attached.client.Inventory(ctx)
		if err != nil {
			return nil, "", fmt.Errorf("engine inventory is required for a consistent recording page: %w", err)
		}
		if inventory.GenerationID != attached.generationID {
			return nil, "", errors.New("engine inventory generation identity mismatch")
		}
		inventories = append(inventories, inventory)
	}
	owners, err := ownerMap(inventories)
	if err != nil {
		return nil, "", err
	}
	active := findAttached(clients, activeID)
	if active == nil {
		return nil, "", ErrGenerationNotAttached
	}
	if err := r.ensureManagementRootCache(ctx, activeID, active.client); err != nil {
		return nil, "", fmt.Errorf("active Engine management snapshot could not be refreshed: %w", err)
	}
	rows, next, err := active.client.ListForManagementPage(ctx, afterID, limit)
	if err != nil {
		return nil, "", err
	}
	for index, row := range rows {
		if row == nil || row.ID == "" || row.ID <= afterID || index > 0 && rows[index-1].ID >= row.ID {
			return nil, "", errors.New("engine returned an invalid recording summary page")
		}
		ownerID := owners[row.ID]
		if ownerID == "" || ownerID == activeID {
			continue
		}
		owner := findAttached(clients, ownerID)
		if owner == nil {
			return nil, "", ErrGenerationNotAttached
		}
		header, getErr := owner.client.GetManagementHeader(ctx, row.ID)
		if getErr != nil {
			return nil, "", fmt.Errorf("live recording owner summary read failed: %w", getErr)
		}
		if header == nil || header.ID != row.ID {
			return nil, "", errors.New("live recording owner summary identity mismatch")
		}
		rows[index] = header
	}
	return rows, next, nil
}

func (r *ManagerRouter) ensureManagementRootCache(ctx context.Context, generationID string, client *ManagerClient) error {
	r.managementCacheMu.Lock()
	defer r.managementCacheMu.Unlock()
	if r.managementCacheGeneration == generationID {
		return nil
	}
	if err := client.InvalidateManagementRootCache(ctx); err != nil {
		if errors.Is(err, ErrManagementRootCacheInvalidationUnsupported) {
			// Older Engines do not implement the root cache, so there is nothing
			// to invalidate. Do not mask transport or authorization failures.
			r.managementCacheGeneration = generationID
			return nil
		}
		return err
	}
	r.managementCacheGeneration = generationID
	return nil
}

func (r *ManagerRouter) Stop(id string) (*domain.Recording, error) {
	return r.StopContext(context.Background(), id)
}

func (r *ManagerRouter) StopContext(ctx context.Context, id string) (*domain.Recording, error) {
	r.mutation.RLock()
	defer r.mutation.RUnlock()
	clients := r.clientsSnapshot()
	if len(clients) == 0 {
		return nil, ErrGenerationNotAttached
	}
	ordered, err := r.prioritizeOwner(ctx, id, clients)
	if err != nil {
		return nil, err
	}
	for _, attached := range ordered {
		recording, err := attached.client.StopContext(ctx, id)
		if err == nil {
			return recording, nil
		}
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		// A timeout, transport failure, or any non-ownership error leaves the
		// result uncertain. Never issue another Stop after such a response.
		return nil, err
	}
	return nil, storage.ErrNotFound
}

// CompleteRecording routes the explicit stopped-to-completed transition to
// the Engine that owns the canonical archive. Completion does not seal it.
func (r *ManagerRouter) CompleteRecording(ctx context.Context, id string) (*domain.Recording, error) {
	r.mutation.Lock()
	defer r.mutation.Unlock()
	clients := r.clientsSnapshot()
	if len(clients) == 0 {
		return nil, ErrGenerationNotAttached
	}
	ordered, err := r.prioritizeLifecycleOwner(ctx, id, clients)
	if err != nil {
		return nil, err
	}
	for _, attached := range ordered {
		recording, callErr := attached.client.CompleteRecording(ctx, id)
		if callErr == nil {
			return recording, nil
		}
		if errors.Is(callErr, storage.ErrNotFound) {
			continue
		}
		return nil, callErr
	}
	return nil, storage.ErrNotFound
}

// SealArchiveContext explicitly closes repair admission for a terminal
// archive through its owning Engine.
func (r *ManagerRouter) SealArchiveContext(ctx context.Context, id string) error {
	r.mutation.Lock()
	defer r.mutation.Unlock()
	clients := r.clientsSnapshot()
	if len(clients) == 0 {
		return ErrGenerationNotAttached
	}
	ordered, err := r.prioritizeLifecycleOwner(ctx, id, clients)
	if err != nil {
		return err
	}
	for _, attached := range ordered {
		callErr := attached.client.SealArchiveContext(ctx, id)
		if callErr == nil {
			return nil
		}
		if errors.Is(callErr, storage.ErrNotFound) {
			continue
		}
		return callErr
	}
	return storage.ErrNotFound
}

func (r *ManagerRouter) Delete(id string) error {
	return r.DeleteContext(context.Background(), id)
}

func (r *ManagerRouter) DeleteContext(ctx context.Context, id string) error {
	r.mutation.Lock()
	defer r.mutation.Unlock()
	clients := r.clientsSnapshot()
	if len(clients) == 0 {
		return ErrGenerationNotAttached
	}
	inventories, err := r.inventoriesFor(ctx, clients)
	if err != nil {
		// Deletion is unsafe if an unavailable Engine might own a live worker.
		return err
	}
	owners, err := ownerMap(inventories)
	if err != nil {
		return err
	}
	ordered, err := orderByOwner(clients, owners[id])
	if err != nil {
		return err
	}
	for _, attached := range ordered {
		err := attached.client.DeleteContext(ctx, id)
		if err == nil {
			return nil
		}
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		return err
	}
	// Every Engine explicitly disclaimed ownership. Recheck the bounded active
	// inventories, then ask the current Engine to perform the terminal archive
	// mutation so Control never writes canonical archive state directly.
	inventories, err = r.inventoriesFor(ctx, clients)
	if err != nil {
		return err
	}
	owners, err = ownerMap(inventories)
	if err != nil {
		return err
	}
	if owners[id] != "" {
		return ErrRecordingMayBeActive
	}
	active := findAttached(clients, r.activeID())
	if active == nil {
		return ErrGenerationNotAttached
	}
	if err := active.client.DeleteTerminalArchive(ctx, id); err != nil {
		if errors.Is(err, acquire.ErrActiveRecording) {
			return ErrRecordingMayBeActive
		}
		return err
	}
	return nil
}

func (r *ManagerRouter) activeID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.active
}

type attachedClient struct {
	generationID string
	client       *ManagerClient
}

func (r *ManagerRouter) activeClient() (*ManagerClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	client := r.clients[r.active]
	if client == nil {
		return nil, ErrGenerationNotAttached
	}
	return client, nil
}

func (r *ManagerRouter) clientsAndActive() ([]attachedClient, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	clients := make([]attachedClient, 0, len(r.clients))
	for generationID, client := range r.clients {
		clients = append(clients, attachedClient{generationID: generationID, client: client})
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i].generationID < clients[j].generationID })
	return clients, r.active
}

func (r *ManagerRouter) clientsSnapshot() []attachedClient {
	clients, _ := r.clientsAndActive()
	return clients
}

func findAttached(clients []attachedClient, generationID string) *attachedClient {
	for i := range clients {
		if clients[i].generationID == generationID {
			return &clients[i]
		}
	}
	return nil
}

func (r *ManagerRouter) inventoriesFor(ctx context.Context, clients []attachedClient) ([]GenerationInventory, error) {
	result := make([]GenerationInventory, 0, len(clients))
	for _, attached := range clients {
		inventory, err := attached.client.Inventory(ctx)
		if err != nil {
			return nil, fmt.Errorf("inventory for generation %s failed: %w", attached.generationID, err)
		}
		if inventory.GenerationID != attached.generationID {
			return nil, fmt.Errorf("inventory generation identity mismatch for %s", attached.generationID)
		}
		result = append(result, inventory)
	}
	return result, nil
}

func ownerMap(inventories []GenerationInventory) (map[string]string, error) {
	owners := make(map[string]string)
	for _, inventory := range inventories {
		for _, active := range inventory.Active {
			if active.State != domain.StateRecording || active.RecordingID == "" {
				return nil, errors.New("engine inventory contains an invalid active recording")
			}
			if prior := owners[active.RecordingID]; prior != "" && prior != inventory.GenerationID {
				return nil, fmt.Errorf("%w: %s", ErrMultipleRecordingOwners, active.RecordingID)
			}
			owners[active.RecordingID] = inventory.GenerationID
		}
	}
	return owners, nil
}

func (r *ManagerRouter) prioritizeOwner(ctx context.Context, recordingID string, clients []attachedClient) ([]attachedClient, error) {
	inventories, err := r.inventoriesFor(ctx, clients)
	if err != nil {
		// A failed inventory can be followed by explicit ownership replies from
		// the bounded Stop/Delete calls. Keep deterministic order in that case.
		return clients, nil
	}
	owners, err := ownerMap(inventories)
	if err != nil {
		return nil, err
	}
	return orderByOwner(clients, owners[recordingID])
}

func (r *ManagerRouter) prioritizeLifecycleOwner(ctx context.Context, recordingID string, clients []attachedClient) ([]attachedClient, error) {
	var ownerID string
	var ownerEpoch uint64
	for _, attached := range clients {
		snapshot, err := attached.client.LifecycleSnapshot(ctx, recordingID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return r.prioritizeOwner(ctx, recordingID, clients)
		}
		if !snapshot.EngineOwns {
			continue
		}
		if ownerID == "" || snapshot.OwnerEpoch > ownerEpoch {
			ownerID, ownerEpoch = attached.generationID, snapshot.OwnerEpoch
			continue
		}
		if snapshot.OwnerEpoch == ownerEpoch {
			return nil, ErrMultipleRecordingOwners
		}
	}
	if ownerID != "" {
		return orderByOwner(clients, ownerID)
	}
	return r.prioritizeOwner(ctx, recordingID, clients)
}

func orderByOwner(clients []attachedClient, ownerID string) ([]attachedClient, error) {
	if ownerID == "" {
		return clients, nil
	}
	owner := findAttached(clients, ownerID)
	if owner == nil {
		return nil, ErrGenerationNotAttached
	}
	ordered := make([]attachedClient, 0, len(clients))
	ordered = append(ordered, *owner)
	for _, attached := range clients {
		if attached.generationID != ownerID {
			ordered = append(ordered, attached)
		}
	}
	return ordered, nil
}

func sortedOwnerIDs(owners map[string]string) []string {
	ids := make([]string, 0, len(owners))
	for id := range owners {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
} = (*ManagerRouter)(nil)
