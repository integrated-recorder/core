package storage

import (
	"context"
	"errors"
	"testing"
)

func TestProbePhysicalObjectStoreExercisesAndCleansReservedNamespace(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	stale := providerProbePrefix + "stale"
	objects.replace(stale, []byte("residue from a previous process crash"))

	if err := ProbePhysicalObjectStore(context.Background(), objects); err != nil {
		t.Fatal(err)
	}
	objects.mu.Lock()
	defer objects.mu.Unlock()
	if len(objects.objects) != 0 {
		t.Fatalf("probe left non-canonical objects behind: %v", objects.objects)
	}
	for _, key := range objects.putKeys {
		if len(key) <= len(providerProbePrefix) || key[:len(providerProbePrefix)] != providerProbePrefix {
			t.Fatalf("probe wrote outside reserved namespace: %q", key)
		}
	}
}

func TestProbePhysicalObjectStoreFailsClosedOnIncompleteOperations(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*memoryPhysicalObjects)
		wantOperation string
	}{
		{name: "write before publish", setup: func(m *memoryPhysicalObjects) { m.failPutBefore = true }, wantOperation: "write"},
		{name: "reported digest mismatch", setup: func(m *memoryPhysicalObjects) { m.wrongPutHash = true }, wantOperation: "write"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := newMemoryPhysicalObjects()
			test.setup(objects)
			err := ProbePhysicalObjectStore(context.Background(), objects)
			if err == nil {
				t.Fatal("incomplete provider operation was accepted")
			}
			var probeErr *ProbeError
			if !errors.As(err, &probeErr) || probeErr.OperationCode() != test.wantOperation {
				t.Fatalf("probe failure=%v probe error=%#v, want operation %q", err, probeErr, test.wantOperation)
			}
			objects.mu.Lock()
			defer objects.mu.Unlock()
			if len(objects.objects) != 0 {
				t.Fatalf("probe failure left objects behind: %v", objects.objects)
			}
		})
	}
}

func TestProbePhysicalObjectStoreRejectsCanceledContext(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ProbePhysicalObjectStore(ctx, objects); err == nil {
		t.Fatal("canceled provider probe succeeded")
	}
	if len(objects.objects) != 0 {
		t.Fatal("canceled probe changed storage")
	}
}

func TestProbePhysicalObjectStoreDistinguishesMissingAfterDelete(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	objects.failDeleteAfter = 1
	err := ProbePhysicalObjectStore(context.Background(), objects)
	if err == nil || errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("probe did not report delete failure: %v", err)
	}
}
