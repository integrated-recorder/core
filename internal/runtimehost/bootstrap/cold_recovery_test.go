package bootstrap

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

type coldReconcileProbe struct {
	calls    int
	err      error
	activeID string
}

func (p *coldReconcileProbe) ReconcileColdStart(activeGenerationID string) error {
	p.calls++
	p.activeID = activeGenerationID
	return p.err
}

func TestColdRecoveryLeaseClearRequiresSuccessfulEngineReadiness(t *testing.T) {
	probe := &coldReconcileProbe{}
	if err := reconcileColdGenerationState(probe, "recover", strings.Repeat("a", 32), false); err == nil {
		t.Fatal("cold reconciliation before Engine readiness succeeded")
	}
	if probe.calls != 0 {
		t.Fatalf("cold reconciliation calls before readiness=%d, want 0", probe.calls)
	}
	if err := reconcileColdGenerationState(probe, "fresh", strings.Repeat("a", 32), false); err != nil {
		t.Fatalf("fresh setup startup should not reconcile cold generation state: %v", err)
	}
	if probe.calls != 0 {
		t.Fatalf("fresh setup startup called cold reconciliation %d times", probe.calls)
	}
	activeID := strings.Repeat("b", 32)
	if err := reconcileColdGenerationState(probe, "recover", activeID, true); err != nil {
		t.Fatalf("ready recovered Engine did not reconcile cold generation state: %v", err)
	}
	if probe.calls != 1 || probe.activeID != activeID {
		t.Fatalf("ready recovered Engine reconciliation calls=%d active ID=%q, want one call for %q", probe.calls, probe.activeID, activeID)
	}
}

func TestColdRecoveryLeaseClearFailurePropagates(t *testing.T) {
	failure := errors.New("durable lease publication failed")
	probe := &coldReconcileProbe{err: failure}
	if err := reconcileColdGenerationState(probe, "recover", strings.Repeat("a", 32), true); !errors.Is(err, failure) {
		t.Fatalf("reconcileColdGenerationState()=%v, want failure", err)
	}
	if probe.calls != 1 {
		t.Fatalf("cold reconciliation calls=%d, want 1", probe.calls)
	}
	if err := reconcileColdGenerationState(probe, "recover", "invalid", true); err == nil {
		t.Fatal("cold reconciliation accepted an invalid active generation ID")
	}
	if probe.calls != 1 {
		t.Fatalf("invalid active generation changed reconciliation calls=%d, want 1", probe.calls)
	}
}

func TestStartupIPCRendezvousPathsAreScopedToHostBoot(t *testing.T) {
	root := bootstrapTestDir(t, "runtime-host-ipc-")
	if err := makePrivateRuntimeDirs(root); err != nil {
		t.Fatal(err)
	}
	ipcDir := filepath.Join(root, "runtime", "ipc")
	generationID := strings.Repeat("a", 32)
	bootA := strings.Repeat("b", 32)
	bootB := strings.Repeat("c", 32)

	resourceA, resourceTokenA, err := resourceIPCPaths(ipcDir, bootA)
	if err != nil {
		t.Fatal(err)
	}
	resourceB, resourceTokenB, err := resourceIPCPaths(ipcDir, bootB)
	if err != nil {
		t.Fatal(err)
	}
	engineA, engineTokenA, controlA, controlTokenA, err := generationIPCPaths(ipcDir, generationID, bootA)
	if err != nil {
		t.Fatal(err)
	}
	engineB, engineTokenB, controlB, controlTokenB, err := generationIPCPaths(ipcDir, generationID, bootB)
	if err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"resource socket": resourceA, "engine socket": engineA, "control socket": controlA,
		"resource token": resourceTokenA, "engine token": engineTokenA, "control token": controlTokenA,
	} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			t.Errorf("%s path is not a clean absolute path: %q", name, path)
		}
	}
	for name, pair := range map[string][2]string{
		"resource socket": {resourceA, resourceB}, "resource token": {resourceTokenA, resourceTokenB},
		"engine socket": {engineA, engineB}, "engine token": {engineTokenA, engineTokenB},
		"control socket": {controlA, controlB}, "control token": {controlTokenA, controlTokenB},
	} {
		if pair[0] == pair[1] {
			t.Errorf("%s reused path across Host boots: %q", name, pair[0])
		}
	}
	for _, path := range []string{resourceA, resourceB, engineA, engineB, controlA, controlB} {
		if err := validateSocketPath(path); err != nil {
			t.Errorf("socket path %q is not valid: %v", path, err)
		}
	}
}
