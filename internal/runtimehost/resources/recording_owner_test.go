package resources

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
)

func newOwnerAuthorityTest(t *testing.T) (*RecordingOwnerAuthority, *recordingowner.Store, *generation.Registry, string, string, string) {
	t.Helper()
	dataDir := t.TempDir()
	store, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := generation.Open(dataDir + "/runtime/state/generations.json")
	if err != nil {
		t.Fatal(err)
	}
	activeID := fmt.Sprintf("%032x", 101)
	candidateID := fmt.Sprintf("%032x", 102)
	instanceID := fmt.Sprintf("%032x", 201)
	newGeneration := func(id string) generation.Generation {
		return generation.Generation{
			ID: id, Version: "1.0.0", Commit: "test", InstalledAt: time.Now().UTC(),
			State: generation.StateStaging, ControlProtocol: 1, EngineProtocol: 1,
			ArchiveReadCompatibility: generation.CompatibilityRange{Minimum: 2, Maximum: 2}, ArchiveWriteFormat: 2,
		}
	}
	if err := registry.Stage(newGeneration(activeID)); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkVerified(activeID); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady(activeID); err != nil {
		t.Fatal(err)
	}
	if err := registry.Activate(activeID); err != nil {
		t.Fatal(err)
	}
	if err := registry.FinalizeActivation(activeID); err != nil {
		t.Fatal(err)
	}
	if err := registry.Stage(newGeneration(candidateID)); err != nil {
		t.Fatal(err)
	}
	authority, err := NewRecordingOwnerAuthority(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	return authority, store, registry, activeID, candidateID, instanceID
}

func TestRecordingOwnerAuthorityAuthorizesOnlyRegisteredActiveEngine(t *testing.T) {
	authority, _, registry, activeID, candidateID, instanceID := newOwnerAuthorityTest(t)
	const engineOwner = "e000000000000000000000000000000000-00000000000000000000000000000001"
	const controlOwner = "c000000000000000000000000000000000-00000000000000000000000000000002"
	if _, err := authority.Claim("unregistered", ""); !errors.Is(err, ErrUnregisteredEngine) {
		t.Fatalf("unregistered Claim error = %v", err)
	}
	if err := authority.RegisterEngine(controlOwner, activeID, instanceID); !errors.Is(err, ErrOwnerRegistration) {
		t.Fatalf("Control registration error = %v", err)
	}
	if err := authority.RegisterEngine(engineOwner, candidateID, fmt.Sprintf("%032x", 202)); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Claim(engineOwner, ""); !errors.Is(err, generation.ErrInvalidTransition) {
		t.Fatalf("candidate Claim error = %v", err)
	}
	if _, ok := registry.Snapshot().Leases[fmt.Sprintf("%032x", 1)]; ok {
		t.Fatal("candidate claim left a lease")
	}
	if err := authority.RegisterEngine(engineOwner, activeID, instanceID); err == nil {
		t.Fatal("duplicate resource owner registration was accepted")
	}
	// Use a new process identity for the active generation.
	const activeOwner = "e000000000000000000000000000000000-00000000000000000000000000000003"
	if err := authority.RegisterEngine(activeOwner, activeID, instanceID); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(activeOwner, "")
	if err != nil {
		t.Fatal(err)
	}
	if owner.EngineGeneration != activeID || owner.WorkerInstance != instanceID || owner.Epoch != 1 || !validRecordingID(owner.RecordingID) {
		t.Fatalf("Host returned unexpected owner tuple: %+v", owner)
	}
	if got, err := authority.store.Current(owner.RecordingID); err != nil || got != owner {
		t.Fatalf("durable owner=%+v err=%v", got, err)
	}
	if lease, ok := registry.Snapshot().Leases[owner.RecordingID]; !ok || lease.EngineGeneration != activeID || lease.WorkerInstance != instanceID {
		t.Fatalf("generation lease=%+v present=%v", lease, ok)
	}
}

func TestRecordingOwnerAuthorityRejectsEngineWithoutV2Capability(t *testing.T) {
	authority, _, registry, _, candidateID, _ := newOwnerAuthorityTest(t)
	// The helper stages a V2 candidate to exercise normal handover behavior.
	// Retire that candidate before staging the deliberately unsupported V1
	// generation because the registry permits only one staged candidate.
	if err := registry.Fail(candidateID); err != nil {
		t.Fatal(err)
	}
	legacyID := fmt.Sprintf("%032x", 303)
	legacy := generation.Generation{
		ID: legacyID, Version: "0.9.0", Commit: "development-only-v1", InstalledAt: time.Now().UTC(),
		State: generation.StateStaging, ControlProtocol: 1, EngineProtocol: 1,
		ArchiveReadCompatibility: generation.CompatibilityRange{Minimum: 1, Maximum: 1}, ArchiveWriteFormat: 1,
	}
	if err := registry.Stage(legacy); err != nil {
		t.Fatal(err)
	}
	const resourceOwner = "e000000000000000000000000000000000-00000000000000000000000000000004"
	if err := authority.RegisterEngine(resourceOwner, legacyID, fmt.Sprintf("%032x", 304)); !errors.Is(err, generation.ErrArchiveFormatUnsupported) {
		t.Fatalf("RegisterEngine(v1) = %v, want unsupported archive capability", err)
	}
}

func TestRecordingOwnerAuthorityConcurrentClaimsHaveOneWinner(t *testing.T) {
	authority, _, registry, activeID, _, instanceID := newOwnerAuthorityTest(t)
	const resourceOwner = "e000000000000000000000000000000000-00000000000000000000000000000004"
	const recordingID = "abcdef0123456789abcdef0123456789"
	if err := authority.RegisterEngine(resourceOwner, activeID, instanceID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := authority.Claim(resourceOwner, recordingID); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("successful claims=%d, want exactly one", successes)
	}
	if _, ok := registry.Snapshot().Leases[recordingID]; !ok {
		t.Fatal("winning Claim did not retain a Registry lease")
	}
}

func TestRecordingOwnerAuthorityReleaseIsExactAndIdempotent(t *testing.T) {
	authority, store, registry, activeID, _, instanceID := newOwnerAuthorityTest(t)
	const resourceOwner = "e000000000000000000000000000000000-00000000000000000000000000000005"
	if err := authority.RegisterEngine(resourceOwner, activeID, instanceID); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(resourceOwner, "abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	stale := owner
	stale.Epoch++
	if err := authority.Release(resourceOwner, stale); !errors.Is(err, recordingowner.ErrStaleOwner) {
		t.Fatalf("stale Release error = %v", err)
	}
	if err := authority.Release(resourceOwner, owner); err != nil {
		t.Fatal(err)
	}
	if err := authority.Release(resourceOwner, owner); err != nil {
		t.Fatalf("repeat terminal Release was not idempotent: %v", err)
	}
	if _, err := store.Current(owner.RecordingID); !errors.Is(err, recordingowner.ErrNotFound) {
		t.Fatalf("owner file still exists: %v", err)
	}
	if _, ok := registry.Snapshot().Leases[owner.RecordingID]; ok {
		t.Fatal("generation lease still exists after owner release")
	}
}

func TestRecordingOwnerAuthorityFailedDurableClaimUndoesPinnedLease(t *testing.T) {
	authority, store, registry, activeID, _, instanceID := newOwnerAuthorityTest(t)
	const resourceOwner = "e000000000000000000000000000000000-00000000000000000000000000000006"
	const recordingID = "abcdef0123456789abcdef0123456789"
	if err := authority.RegisterEngine(resourceOwner, activeID, instanceID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(recordingID, activeID, fmt.Sprintf("%032x", 202)); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Claim(resourceOwner, recordingID); !errors.Is(err, recordingowner.ErrAlreadyOwned) {
		t.Fatalf("conflicting durable Claim error = %v", err)
	}
	if _, ok := registry.Snapshot().Leases[recordingID]; ok {
		t.Fatal("failed durable Claim left a generation lease")
	}
}

func TestRecordingOwnerAuthorityProtectsClaimsFromInventoryReconcile(t *testing.T) {
	authority, _, _, activeID, _, instanceID := newOwnerAuthorityTest(t)
	const resourceOwner = "e000000000000000000000000000000000-00000000000000000000000000000007"
	if err := authority.RegisterEngine(resourceOwner, activeID, instanceID); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(resourceOwner, "abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := authority.WithProtectedLeases(func(leases []generation.Lease) error {
		called = true
		if len(leases) != 1 || leases[0].RecordingID != owner.RecordingID {
			t.Fatalf("protected leases=%+v", leases)
		}
		return nil
	}); err != nil || !called {
		t.Fatalf("protected lease callback called=%v err=%v", called, err)
	}
}
