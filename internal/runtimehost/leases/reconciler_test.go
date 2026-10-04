package leases

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/generation"
)

const (
	genActive      = "11111111111111111111111111111111"
	genDraining    = "22222222222222222222222222222222"
	recActive      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	recOther       = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	workerActive   = "cccccccccccccccccccccccccccccccc"
	workerDraining = "dddddddddddddddddddddddddddddddd"
)

type sourceStub struct {
	mu          sync.Mutex
	inventories map[string]generation.EngineInventory
	errors      map[string]error
	calls       int
}

func (s *sourceStub) Inventory(ctx context.Context, engine generation.Generation) (generation.EngineInventory, error) {
	if err := ctx.Err(); err != nil {
		return generation.EngineInventory{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if err := s.errors[engine.ID]; err != nil {
		return generation.EngineInventory{}, err
	}
	return s.inventories[engine.ID], nil
}

func (s *sourceStub) set(id string, inventory generation.EngineInventory, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inventories == nil {
		s.inventories = make(map[string]generation.EngineInventory)
	}
	if s.errors == nil {
		s.errors = make(map[string]error)
	}
	s.inventories[id] = inventory
	if err != nil {
		s.errors[id] = err
	} else {
		delete(s.errors, id)
	}
}

func TestRunOnceCreatesExactActiveGenerationLease(t *testing.T) {
	r := openTwoGenerations(t)
	now := time.Now().UTC()
	started := now.Add(-3 * time.Minute)
	source := &sourceStub{}
	source.set(genActive, inventory(genActive, workerActive, now, generation.InventoryRecording{RecordingID: recActive, StartedAt: started}), nil)
	source.set(genDraining, inventory(genDraining, workerDraining, now), nil)
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, ok := r.Snapshot().Leases[recActive]
	if !ok || lease.RecordingID != recActive || lease.EngineGeneration != genActive || lease.WorkerInstance != workerActive || !lease.StartedAt.Equal(started) {
		t.Fatalf("unexpected lease projection: %+v", lease)
	}
}

func TestRunOnceReleasesOnlyLeaseOmittedByItsEngine(t *testing.T) {
	r := openTwoGenerations(t)
	now := time.Now().UTC()
	seedLeases(t, r, now)
	source := &sourceStub{}
	source.set(genActive, inventory(genActive, workerActive, now), nil)
	source.set(genDraining, inventory(genDraining, workerDraining, now, generation.InventoryRecording{RecordingID: recOther, StartedAt: now.Add(-time.Minute)}), nil)
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	leases := r.Snapshot().Leases
	if _, exists := leases[recActive]; exists {
		t.Fatal("confirmed omission did not release terminal recording")
	}
	if lease, exists := leases[recOther]; !exists || lease.EngineGeneration != genDraining {
		t.Fatalf("active inventory released another Engine's lease: %+v", leases)
	}
}

func TestRunOncePreservesAndAddsDrainingGenerationLeases(t *testing.T) {
	r := openTwoGenerations(t)
	now := time.Now().UTC()
	source := &sourceStub{}
	source.set(genActive, inventory(genActive, workerActive, now, generation.InventoryRecording{RecordingID: recActive, StartedAt: now.Add(-time.Minute)}), nil)
	source.set(genDraining, inventory(genDraining, workerDraining, now, generation.InventoryRecording{RecordingID: recOther, StartedAt: now.Add(-time.Second)}), nil)
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	leases := r.Snapshot().Leases
	if len(leases) != 2 || leases[recActive].EngineGeneration != genActive || leases[recOther].EngineGeneration != genDraining {
		t.Fatalf("active/draining inventory leases missing: %+v", leases)
	}
}

func TestRunOnceSkipsHostConfirmedDormantRollbackEngine(t *testing.T) {
	r := openTwoGenerations(t)
	if err := r.MarkEngineDormant(genActive); err != nil {
		t.Fatalf("MarkEngineDormant(): %v", err)
	}
	source := &sourceStub{}
	source.set(genDraining, inventory(genDraining, workerDraining, time.Now().UTC()), nil)
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() required an inventory from the stopped rollback Engine: %v", err)
	}
	source.mu.Lock()
	calls := source.calls
	source.mu.Unlock()
	if calls != 1 {
		t.Fatalf("inventory source calls = %d, want only the active Engine", calls)
	}
	if got := r.Snapshot().PreviousGenerationID; got != genActive {
		t.Fatalf("dormant rollback target changed during reconciliation: %s", got)
	}
}

func TestRunOnceFailureOrMalformedInventoryDoesNotMutateAnyLease(t *testing.T) {
	r := openTwoGenerations(t)
	now := time.Now().UTC()
	seedLeases(t, r, now)
	before := r.Snapshot().Leases
	source := &sourceStub{}
	source.set(genActive, inventory(genActive, workerActive, now), nil)
	source.set(genDraining, generation.EngineInventory{}, errors.New("engine unavailable"))
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.RunOnce(context.Background()); err == nil {
		t.Fatal("source failure was ignored")
	}
	assertLeasesEqual(t, before, r.Snapshot().Leases)

	source.set(genDraining, inventory(genDraining, workerDraining, now), nil)
	malformed := inventory(genActive, workerActive, now)
	malformed.Confirmed = false
	source.set(genActive, malformed, nil)
	if err := reconciler.RunOnce(context.Background()); !errors.Is(err, ErrInvalidInventory) {
		t.Fatalf("unconfirmed inventory accepted: %v", err)
	}
	assertLeasesEqual(t, before, r.Snapshot().Leases)
}

func TestRunOnceRejectsDuplicateRecordingAcrossEnginesWithoutMovingLease(t *testing.T) {
	r := openTwoGenerations(t)
	now := time.Now().UTC()
	seedLeases(t, r, now)
	before := r.Snapshot().Leases
	source := &sourceStub{}
	source.set(genActive, inventory(genActive, workerActive, now, generation.InventoryRecording{RecordingID: recActive, StartedAt: now.Add(-time.Minute)}), nil)
	source.set(genDraining, inventory(genDraining, workerDraining, now, generation.InventoryRecording{RecordingID: recActive, StartedAt: now.Add(-time.Minute)}), nil)
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.RunOnce(context.Background()); !errors.Is(err, ErrInvalidInventory) {
		t.Fatalf("cross-engine duplicate accepted: %v", err)
	}
	assertLeasesEqual(t, before, r.Snapshot().Leases)
}

func TestConcurrentSnapshotsAndRunOnceAreRaceSafe(t *testing.T) {
	r := openTwoGenerations(t)
	now := time.Now().UTC()
	source := &sourceStub{}
	source.set(genActive, inventory(genActive, workerActive, now, generation.InventoryRecording{RecordingID: recActive, StartedAt: now.Add(-time.Minute)}), nil)
	source.set(genDraining, inventory(genDraining, workerDraining, now), nil)
	reconciler, err := New(r, source)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 12
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if err := reconciler.RunOnce(context.Background()); err != nil {
					errCh <- err
				}
				return
			}
			for j := 0; j < 20; j++ {
				snapshot := r.Snapshot()
				if len(snapshot.Generations) != 2 {
					errCh <- errors.New("snapshot omitted generation")
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if got := r.Snapshot().Leases[recActive].EngineGeneration; got != genActive {
		t.Fatalf("concurrent reconciliation moved lease to %s", got)
	}
}

func openTwoGenerations(t *testing.T) *generation.Registry {
	t.Helper()
	r, err := generation.Open(filepath.Join(t.TempDir(), "runtime", "generations.json"))
	if err != nil {
		t.Fatal(err)
	}
	stageReady(t, r, genActive)
	if err := r.Activate(genActive); err != nil {
		t.Fatal(err)
	}
	stageReady(t, r, genDraining)
	if err := r.Activate(genDraining); err != nil {
		t.Fatal(err)
	}
	return r
}

func seedLeases(t *testing.T, r *generation.Registry, now time.Time) {
	t.Helper()
	if err := r.ReconcileInventories([]generation.EngineInventory{
		inventory(genActive, workerActive, now, generation.InventoryRecording{RecordingID: recActive, StartedAt: now.Add(-time.Minute)}),
		inventory(genDraining, workerDraining, now, generation.InventoryRecording{RecordingID: recOther, StartedAt: now.Add(-time.Minute)}),
	}); err != nil {
		t.Fatal(err)
	}
}

func inventory(engine, worker string, observed time.Time, recordings ...generation.InventoryRecording) generation.EngineInventory {
	return generation.EngineInventory{Confirmed: true, EngineGeneration: engine, WorkerInstance: worker, ObservedAt: observed, Recordings: recordings}
}

func assertLeasesEqual(t *testing.T, want, got map[string]generation.Lease) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("lease count changed: want=%+v got=%+v", want, got)
	}
	for id, wantLease := range want {
		if gotLease, ok := got[id]; !ok || gotLease != wantLease {
			t.Fatalf("lease changed for %s: want=%+v got=%+v", id, wantLease, gotLease)
		}
	}
}

func stageReady(t *testing.T, r *generation.Registry, id string) {
	t.Helper()
	now := time.Now().UTC()
	gen := generation.Generation{ID: id, Version: "1.0", Commit: "abc123", InstalledAt: now, State: generation.StateStaging,
		ControlProtocol: 1, EngineProtocol: 1, ArchiveReadCompatibility: generation.CompatibilityRange{Minimum: 1, Maximum: 1}, ArchiveWriteEpoch: 1}
	if err := r.Stage(gen); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkVerified(id); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkReady(id); err != nil {
		t.Fatal(err)
	}
}
