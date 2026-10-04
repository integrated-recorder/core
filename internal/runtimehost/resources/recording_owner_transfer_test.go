package resources

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
)

func TestRecordingOwnerAuthorityTransfersWriterFenceAndRetirementLease(t *testing.T) {
	authority, store, registry, sourceGeneration, targetGeneration, sourceInstance := newOwnerAuthorityTest(t)
	targetInstance := fmt.Sprintf("%032x", 303)
	sourceResource := "e" + sourceGeneration + "-00000000000000000000000000000011"
	targetResource := "e" + targetGeneration + "-00000000000000000000000000000012"
	if err := authority.RegisterEngine(sourceResource, sourceGeneration, sourceInstance); err != nil {
		t.Fatal(err)
	}
	if err := authority.RegisterEngine(targetResource, targetGeneration, targetInstance); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(sourceResource, "abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	// The initial ownership claim occurs before canonical recording creation.
	// A normal Host inventory pass then replaces its provisional lease time
	// with the immutable Recording.StartedAt from the Engine's archive view.
	startedAt := time.Now().UTC().Add(-time.Second)
	if err := registry.ReconcileInventory(generation.EngineInventory{
		Confirmed: true, EngineGeneration: sourceGeneration, WorkerInstance: sourceInstance,
		ObservedAt: time.Now().UTC(), Recordings: []generation.InventoryRecording{{RecordingID: owner.RecordingID, StartedAt: startedAt}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := registry.MarkVerified(targetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady(targetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.Activate(targetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.FinalizeActivation(targetGeneration); err != nil {
		t.Fatal(err)
	}
	transferred, err := authority.Transfer(owner, targetResource)
	if err != nil {
		t.Fatalf("Transfer(): %v", err)
	}
	if transferred.RecordingID != owner.RecordingID || transferred.EngineGeneration != targetGeneration || transferred.WorkerInstance != targetInstance || transferred.Epoch != owner.Epoch+1 {
		t.Fatalf("transferred owner=%+v, source=%+v", transferred, owner)
	}
	if current, err := store.Current(owner.RecordingID); err != nil || current != transferred {
		t.Fatalf("durable owner=%+v err=%v", current, err)
	}
	if err := store.WithCommit(owner, func() error { return nil }); !errors.Is(err, recordingowner.ErrStaleOwner) {
		t.Fatalf("old source token commit error=%v, want stale owner", err)
	}
	if err := store.WithCommit(transferred, func() error { return nil }); err != nil {
		t.Fatalf("target owner commit failed: %v", err)
	}
	currentLease, ok := registry.Snapshot().Leases[owner.RecordingID]
	if !ok || currentLease.EngineGeneration != targetGeneration || currentLease.WorkerInstance != targetInstance || !currentLease.StartedAt.Equal(startedAt) {
		t.Fatalf("transferred generation lease=%+v present=%v", currentLease, ok)
	}
	if current, err := authority.CurrentOwner(owner.RecordingID); err != nil || current != transferred {
		t.Fatalf("CurrentOwner()=%+v err=%v", current, err)
	}
}

func TestRecordingOwnerAuthorityRejectsTransferToUnactivatedEngine(t *testing.T) {
	authority, store, registry, sourceGeneration, targetGeneration, sourceInstance := newOwnerAuthorityTest(t)
	sourceResource := "e" + sourceGeneration + "-00000000000000000000000000000021"
	targetResource := "e" + targetGeneration + "-00000000000000000000000000000022"
	if err := authority.RegisterEngine(sourceResource, sourceGeneration, sourceInstance); err != nil {
		t.Fatal(err)
	}
	if err := authority.RegisterEngine(targetResource, targetGeneration, fmt.Sprintf("%032x", 304)); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(sourceResource, "abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Transfer(owner, targetResource); !errors.Is(err, generation.ErrInvalidTransition) {
		t.Fatalf("Transfer() error=%v, want inactive target rejection", err)
	}
	if current, err := store.Current(owner.RecordingID); err != nil || current != owner {
		t.Fatalf("failed transfer changed durable owner=%+v err=%v", current, err)
	}
	if lease, ok := registry.Snapshot().Leases[owner.RecordingID]; !ok || lease.EngineGeneration != sourceGeneration {
		t.Fatalf("failed transfer changed source lease=%+v present=%v", lease, ok)
	}
}

func TestRecordingOwnerAuthorityRejectsStaleTransferAndUnknownTarget(t *testing.T) {
	authority, store, _, sourceGeneration, targetGeneration, sourceInstance := newOwnerAuthorityTest(t)
	sourceResource := "e" + sourceGeneration + "-00000000000000000000000000000031"
	if err := authority.RegisterEngine(sourceResource, sourceGeneration, sourceInstance); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(sourceResource, "abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Transfer(owner, "e"+targetGeneration+"-00000000000000000000000000000032"); !errors.Is(err, ErrUnregisteredEngine) {
		t.Fatalf("unregistered target error=%v", err)
	}
	stale := owner
	stale.Epoch++
	if _, err := authority.Transfer(stale, sourceResource); !errors.Is(err, recordingowner.ErrStaleOwner) {
		t.Fatalf("stale source error=%v", err)
	}
	if current, err := store.Current(owner.RecordingID); err != nil || current != owner {
		t.Fatalf("rejected transfer changed owner=%+v err=%v", current, err)
	}
}

func TestRecordingOwnerAuthorityRollbackFencesActiveTargetToDrainingSource(t *testing.T) {
	authority, store, registry, sourceGeneration, targetGeneration, sourceInstance := newOwnerAuthorityTest(t)
	targetInstance := fmt.Sprintf("%032x", 305)
	sourceResource := "e" + sourceGeneration + "-00000000000000000000000000000041"
	targetResource := "e" + targetGeneration + "-00000000000000000000000000000042"
	if err := authority.RegisterEngine(sourceResource, sourceGeneration, sourceInstance); err != nil {
		t.Fatal(err)
	}
	if err := authority.RegisterEngine(targetResource, targetGeneration, targetInstance); err != nil {
		t.Fatal(err)
	}
	owner, err := authority.Claim(sourceResource, "abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkVerified(targetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady(targetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.Activate(targetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.FinalizeActivation(targetGeneration); err != nil {
		t.Fatal(err)
	}
	targetOwner, err := authority.Transfer(owner, targetResource)
	if err != nil {
		t.Fatal(err)
	}
	restoredOwner, err := authority.RollbackTransfer(targetOwner, sourceResource)
	if err != nil {
		t.Fatalf("RollbackTransfer(): %v", err)
	}
	if restoredOwner.EngineGeneration != sourceGeneration || restoredOwner.WorkerInstance != sourceInstance || restoredOwner.Epoch != targetOwner.Epoch+1 {
		t.Fatalf("restored owner=%+v; target owner=%+v", restoredOwner, targetOwner)
	}
	if err := store.WithCommit(targetOwner, func() error { return nil }); !errors.Is(err, recordingowner.ErrStaleOwner) {
		t.Fatalf("fenced target commit error=%v, want stale owner", err)
	}
	if err := store.WithCommit(restoredOwner, func() error { return nil }); err != nil {
		t.Fatalf("restored source commit failed: %v", err)
	}
	lease, ok := registry.Snapshot().Leases[owner.RecordingID]
	if !ok || lease.EngineGeneration != sourceGeneration || lease.WorkerInstance != sourceInstance {
		t.Fatalf("restored source lease=%+v present=%v", lease, ok)
	}
}
