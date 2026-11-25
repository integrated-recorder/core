package replication

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestImportGateSerializesSameLeaseAndAllowsDifferentLeases(t *testing.T) {
	gate := NewImportGate()
	firstRelease, err := gate.Acquire(context.Background(), "rl_one")
	if err != nil {
		t.Fatal(err)
	}
	defer firstRelease()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	waitDone := make(chan error, 1)
	go func() {
		close(started)
		_, waitErr := gate.Acquire(ctx, "rl_one")
		waitDone <- waitErr
	}()
	<-started
	waitForImportGateUsers(t, gate, "rl_one", 2)
	cancel()
	if err := <-waitDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("same-lease waiter error=%v", err)
	}

	otherRelease, err := gate.Acquire(context.Background(), "rl_two")
	if err != nil {
		t.Fatalf("different lease was blocked: %v", err)
	}
	otherRelease()

	if len(gate.entries) != 1 {
		t.Fatalf("canceled waiter or completed lease leaked gate entries: %d", len(gate.entries))
	}
}

func TestImportGateConcurrentSameLeaseNeverOverlaps(t *testing.T) {
	gate := NewImportGate()
	firstRelease, err := gate.Acquire(context.Background(), "rl_shared")
	if err != nil {
		t.Fatal(err)
	}
	active := 1
	maxActive := 1
	var mu sync.Mutex
	var workers sync.WaitGroup
	ready := make(chan struct{}, 31)
	start := make(chan struct{})
	entered := make(chan chan struct{}, 31)
	for range 31 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ready <- struct{}{}
			<-start
			release, err := gate.Acquire(context.Background(), "rl_shared")
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			proceed := make(chan struct{})
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			entered <- proceed
			<-proceed
			mu.Lock()
			active--
			mu.Unlock()
			release()
		}()
	}
	for range 31 {
		<-ready
	}
	close(start)
	waitForImportGateUsers(t, gate, "rl_shared", 32)
	mu.Lock()
	if active != 1 {
		t.Fatalf("initial import activity=%d, want 1", active)
	}
	active--
	mu.Unlock()
	firstRelease()
	for range 31 {
		var proceed chan struct{}
		select {
		case proceed = <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("serialized import did not make progress")
		}
		mu.Lock()
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		close(proceed)
	}
	workers.Wait()
	if maxActive != 1 || active != 0 {
		t.Fatalf("maximum concurrent imports=%d, final active=%d; want 1 and 0", maxActive, active)
	}
	if len(gate.entries) != 0 {
		t.Fatalf("completed imports leaked gate entries: %d", len(gate.entries))
	}
}

func waitForImportGateUsers(t *testing.T, gate *ImportGate, leaseID string, want int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		gate.mu.Lock()
		entry := gate.entries[leaseID]
		users := 0
		if entry != nil {
			users = entry.users
		}
		gate.mu.Unlock()
		if users >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("lease %s registered %d importers, want at least %d", leaseID, users, want)
		default:
			runtime.Gosched()
		}
	}
}

func TestImportGateRejectsInvalidAndCanceledRequests(t *testing.T) {
	gate := NewImportGate()
	if _, err := gate.Acquire(context.Background(), ""); !errors.Is(err, ErrImportGateKey) {
		t.Fatalf("empty key error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.Acquire(ctx, "rl_canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error=%v", err)
	}
	if len(gate.entries) != 0 {
		t.Fatalf("canceled acquisition leaked gate entries: %d", len(gate.entries))
	}
}
