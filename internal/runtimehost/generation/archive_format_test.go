package generation

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCurrentArchiveCapabilityRequiresV2ReadAndWrite(t *testing.T) {
	cases := []struct {
		name  string
		read  CompatibilityRange
		write int
		want  bool
	}{
		{name: "v2 read and write", read: CompatibilityRange{Minimum: 2, Maximum: 2}, write: 2, want: true},
		{name: "v1 writer", read: CompatibilityRange{Minimum: 2, Maximum: 2}, write: 1},
		{name: "cannot read v2", read: CompatibilityRange{Minimum: 1, Maximum: 1}, write: 1},
		{name: "unknown future writer", read: CompatibilityRange{Minimum: 1, Maximum: 3}, write: 3},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			item := formatTestGeneration("11111111111111111111111111111111", StateStaging, test.read, test.write)
			if got := item.SupportsCurrentArchiveFormat(); got != test.want {
				t.Fatalf("SupportsCurrentArchiveFormat() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestActivationRejectsGenerationWithoutV2Capability(t *testing.T) {
	registry, err := Open(filepath.Join(t.TempDir(), "generations.json"))
	if err != nil {
		t.Fatal(err)
	}
	v1 := formatTestGeneration("11111111111111111111111111111111", StateStaging, CompatibilityRange{Minimum: 1, Maximum: 1}, 1)
	stageFormatTestGeneration(t, registry, v1)
	if err := registry.Activate(v1.ID); !errors.Is(err, ErrArchiveFormatUnsupported) {
		t.Fatalf("Activate(v1) = %v, want unsupported archive capability", err)
	}
	if state := registry.Snapshot(); state.ActiveGenerationID != "" || state.StagedGenerationID != v1.ID {
		t.Fatalf("rejected v1 activation mutated registry: %+v", state)
	}

	if err := registry.Fail(v1.ID); err != nil {
		t.Fatal(err)
	}
	v2 := formatTestGeneration("22222222222222222222222222222222", StateStaging, CompatibilityRange{Minimum: 2, Maximum: 2}, 2)
	stageFormatTestGeneration(t, registry, v2)
	if err := registry.Activate(v2.ID); err != nil {
		t.Fatalf("Activate(v2): %v", err)
	}
	if got := registry.Snapshot().ActiveGenerationID; got != v2.ID {
		t.Fatalf("active generation = %q, want v2 %q", got, v2.ID)
	}
}

func TestRollbackAndColdAdoptionRejectV1Generation(t *testing.T) {
	t.Run("rollback", func(t *testing.T) {
		registry := activeV2FormatTestRegistry(t)
		legacy := formatTestGeneration("33333333333333333333333333333333", StateDraining, CompatibilityRange{Minimum: 1, Maximum: 1}, 1)
		registry.mu.Lock()
		registry.state.Generations[legacy.ID] = legacy
		registry.state.PreviousGenerationID = legacy.ID
		registry.mu.Unlock()
		if err := registry.Rollback(); !errors.Is(err, ErrArchiveFormatUnsupported) {
			t.Fatalf("Rollback() = %v, want unsupported archive capability", err)
		}
		if got := registry.Snapshot().ActiveGenerationID; got != "22222222222222222222222222222222" {
			t.Fatalf("rejected rollback changed active generation to %q", got)
		}
	})

	t.Run("cold adoption", func(t *testing.T) {
		registry, err := Open(filepath.Join(t.TempDir(), "generations.json"))
		if err != nil {
			t.Fatal(err)
		}
		legacy := formatTestGeneration("44444444444444444444444444444444", StateActive, CompatibilityRange{Minimum: 1, Maximum: 1}, 1)
		registry.mu.Lock()
		registry.state.Generations[legacy.ID] = legacy
		registry.state.ActiveGenerationID = legacy.ID
		registry.mu.Unlock()
		if err := registry.ReconcileColdStart(legacy.ID); !errors.Is(err, ErrArchiveFormatUnsupported) {
			t.Fatalf("ReconcileColdStart(v1) = %v, want unsupported archive capability", err)
		}
	})
}

func activeV2FormatTestRegistry(t *testing.T) *Registry {
	t.Helper()
	registry, err := Open(filepath.Join(t.TempDir(), "generations.json"))
	if err != nil {
		t.Fatal(err)
	}
	v2 := formatTestGeneration("22222222222222222222222222222222", StateStaging, CompatibilityRange{Minimum: 2, Maximum: 2}, 2)
	stageFormatTestGeneration(t, registry, v2)
	if err := registry.Activate(v2.ID); err != nil {
		t.Fatal(err)
	}
	if err := registry.FinalizeActivation(v2.ID); err != nil {
		t.Fatal(err)
	}
	return registry
}

func stageFormatTestGeneration(t *testing.T, registry *Registry, item Generation) {
	t.Helper()
	if err := registry.Stage(item); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkVerified(item.ID); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady(item.ID); err != nil {
		t.Fatal(err)
	}
}

func formatTestGeneration(id string, state State, read CompatibilityRange, write int) Generation {
	return Generation{
		ID: id, Version: "1.0.0", Commit: id, InstalledAt: time.Now().UTC(), State: state,
		ControlProtocol: 1, EngineProtocol: 1, ArchiveReadCompatibility: read, ArchiveWriteFormat: write,
	}
}
