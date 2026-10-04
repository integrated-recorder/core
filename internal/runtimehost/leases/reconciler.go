// Package leases reconciles durable generation leases against confirmed
// Recorder Engine inventories. It is a Host orchestration primitive; callers
// must not run it when an Engine inventory could not be confirmed.
package leases

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/integrated-recorder/core/internal/runtimehost/generation"
)

var ErrInvalidInventory = errors.New("invalid Recorder Engine inventory")

// InventorySource must return a bounded inventory for the requested Engine
// generation and honor ctx cancellation. Runtime Host IPC implementations
// should impose their own transport deadline and frame-size bound.
type InventorySource interface {
	Inventory(ctx context.Context, engine generation.Generation) (generation.EngineInventory, error)
}

// InventorySourceFunc adapts a function to InventorySource.
type InventorySourceFunc func(context.Context, generation.Generation) (generation.EngineInventory, error)

func (f InventorySourceFunc) Inventory(ctx context.Context, engine generation.Generation) (generation.EngineInventory, error) {
	return f(ctx, engine)
}

// Reconciler serializes RunOnce calls so an older inventory request can never
// race a newer request and release its leases. Registry updates remain
// independently synchronized and durable.
type Reconciler struct {
	registry *generation.Registry
	source   InventorySource
	mu       sync.Mutex
}

func New(registry *generation.Registry, source InventorySource) (*Reconciler, error) {
	if registry == nil || source == nil {
		return nil, errors.New("lease reconciler requires a registry and inventory source")
	}
	return &Reconciler{registry: registry, source: source}, nil
}

// RunOnce obtains every active/draining Engine inventory before applying any
// mutation. A source error, unconfirmed/malformed response, or cross-Engine
// duplicate therefore leaves the lease projection unchanged.
func (r *Reconciler) RunOnce(ctx context.Context) error {
	if ctx == nil {
		return errors.New("lease reconciliation context is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	snapshot := r.registry.Snapshot()
	engines := make([]generation.Generation, 0, len(snapshot.Generations))
	for _, engine := range snapshot.Generations {
		if !engine.EngineDormant && (engine.State == generation.StateActive || engine.State == generation.StateDraining) {
			engines = append(engines, engine)
		}
	}
	sort.Slice(engines, func(i, j int) bool { return engines[i].ID < engines[j].ID })

	inventories := make([]generation.EngineInventory, 0, len(engines))
	seenRecordings := make(map[string]string)
	totalRecordings := 0
	for _, engine := range engines {
		if err := ctx.Err(); err != nil {
			return err
		}
		inventory, err := r.source.Inventory(ctx, engine)
		if err != nil {
			return fmt.Errorf("read Engine inventory: %w", err)
		}
		if inventory.EngineGeneration != engine.ID {
			return fmt.Errorf("%w: response generation does not match requested Engine", ErrInvalidInventory)
		}
		if err := generation.ValidateInventory(inventory); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidInventory, err)
		}
		totalRecordings += len(inventory.Recordings)
		if totalRecordings > generation.MaxInventoryRecordings {
			return fmt.Errorf("%w: aggregate recording count exceeds limit", ErrInvalidInventory)
		}
		for _, recording := range inventory.Recordings {
			if previous, duplicate := seenRecordings[recording.RecordingID]; duplicate {
				return fmt.Errorf("%w: recording appears in Engines %s and %s", ErrInvalidInventory, previous, engine.ID)
			}
			seenRecordings[recording.RecordingID] = engine.ID
		}
		inventories = append(inventories, inventory)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.registry.ReconcileInventories(inventories)
}
