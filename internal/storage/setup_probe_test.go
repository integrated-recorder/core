package storage

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRunSetupProbeRepeatsAndRunsConcurrentlyWithoutResidue(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "runtime", "state"), 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		result := store.RunSetupProbe()
		if !result.WritePassed || !result.DurabilityPassed || result.FreeBytes == 0 {
			t.Fatalf("probe %d result=%+v", i, result)
		}
	}
	const concurrent = 8
	var workers sync.WaitGroup
	results := make(chan SetupProbeResult, concurrent)
	for i := 0; i < concurrent; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- store.RunSetupProbe()
		}()
	}
	workers.Wait()
	close(results)
	for result := range results {
		if !result.WritePassed || !result.DurabilityPassed {
			t.Errorf("concurrent probe result=%+v", result)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "runtime", "state"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".setup-probe-") {
			t.Fatalf("probe residue remains: %q", entry.Name())
		}
	}
	archiveEntries, err := os.ReadDir(filepath.Join(root, "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	if len(archiveEntries) != 0 {
		t.Fatalf("setup probe modified canonical recordings directory: %+v", archiveEntries)
	}
}

func TestRunSetupProbeRejectsSymlinkedRuntimeState(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "runtime", "state")); err != nil {
		t.Fatal(err)
	}
	result := store.RunSetupProbe()
	if result.WritePassed || result.DurabilityPassed {
		t.Fatalf("symlinked probe area accepted: %+v", result)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("probe wrote through runtime symlink: entries=%+v err=%v", entries, err)
	}
}

func TestRunSetupProbeUsesPhysicalObjectStoreAndLeavesCapacityUnknown(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	result := store.RunSetupProbe()
	if !result.WritePassed || !result.DurabilityPassed {
		t.Fatalf("object-backed provider probe failed: %+v", result)
	}
	if result.CapacityKnown || result.FreeBytes != 0 {
		t.Fatalf("provider without capacity reporting should remain unknown: %+v", result)
	}
	objects.mu.Lock()
	defer objects.mu.Unlock()
	if len(objects.putKeys) == 0 {
		t.Fatal("setup probe did not exercise the physical object provider")
	}
	for _, key := range objects.putKeys {
		if !strings.HasPrefix(key, "_integrated-recorder/system-probes/v1/") {
			t.Fatalf("setup probe wrote outside its reserved namespace: %q", key)
		}
	}
	for key := range objects.objects {
		if strings.HasPrefix(key, "recordings/") {
			t.Fatalf("setup probe mutated canonical archive namespace: %q", key)
		}
	}
}

func TestRunSetupProbeFailsWhenPhysicalObjectProviderFails(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	objects.failPutBefore = true
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	result := store.RunSetupProbe()
	if result.WritePassed || result.DurabilityPassed {
		t.Fatalf("failed provider write was accepted: %+v", result)
	}
	if result.CapacityKnown {
		t.Fatalf("provider failure must not synthesize capacity: %+v", result)
	}
}

func TestStorageOpenCleansOnlyStaleSetupProbeDirectories(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "runtime", "state")
	stale := filepath.Join(stateDir, ".setup-probe-interrupted")
	if err := os.MkdirAll(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "probe"), []byte("temporary"), 0600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "keep"), []byte("safe"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(stateDir, ".setup-probe-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale setup residue remains: %v", err)
	}
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("cleanup removed or followed a symlink: info=%v err=%v", info, err)
	}
	if data, err := os.ReadFile(filepath.Join(outside, "keep")); err != nil || string(data) != "safe" {
		t.Fatalf("cleanup touched symlink target: data=%q err=%v", data, err)
	}
}
