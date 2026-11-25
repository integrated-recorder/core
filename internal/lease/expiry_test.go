package lease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type expiryFixture struct {
	mu       sync.Mutex
	pass     []time.Time
	err      error
	observed chan time.Time
}

func (f *expiryFixture) Expire(_ context.Context, at time.Time) ([]string, error) {
	f.mu.Lock()
	f.pass = append(f.pass, at)
	err := f.err
	f.mu.Unlock()
	if f.observed != nil {
		f.observed <- at
	}
	return nil, err
}

func TestExpiryLoopReconcilesImmediatelyAndAfterWake(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ticks := make(chan time.Time)
	done := make(chan struct{})
	first := time.Date(2026, 11, 3, 1, 2, 3, 0, time.UTC)
	second := time.Date(2026, 11, 3, 1, 3, 3, 0, time.UTC)
	var clockMu sync.Mutex
	current := first
	store := &expiryFixture{observed: make(chan time.Time, 2)}
	go func() {
		defer close(done)
		runExpiryLoop(ctx, store, func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return current
		}, ticks, nil)
	}()
	if got := <-store.observed; !got.Equal(first) {
		t.Fatalf("startup pass time=%s, want %s", got, first)
	}
	clockMu.Lock()
	current = second
	clockMu.Unlock()
	ticks <- second
	if got := <-store.observed; !got.Equal(second) {
		t.Fatalf("wake pass time=%s, want %s", got, second)
	}
	cancel()
	<-done
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.pass) != 2 || !store.pass[0].Equal(first) || !store.pass[1].Equal(second) {
		t.Fatalf("expiry passes=%v", store.pass)
	}
}

func TestExpiryLoopReportsPassFailureAndRetriesOnNextWake(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ticks := make(chan time.Time)
	store := &expiryFixture{err: errors.New("private storage path"), observed: make(chan time.Time, 2)}
	reported := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runExpiryLoop(ctx, store, time.Now, ticks, func(err error) { reported <- err })
	}()
	select {
	case err := <-reported:
		if err == nil || err.Error() != "private storage path" {
			t.Fatalf("reported error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup expiry error was not reported")
	}
	<-store.observed // Finish the startup pass before sending the retry wake.
	ticks <- time.Now()
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("expiry loop did not retry after wake")
	}
	<-store.observed
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry loop did not stop after cancellation")
	}
}

func TestMaintenanceLoopJoinsDurableOutboxPassWithExpiryLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ticks := make(chan time.Time)
	done := make(chan struct{})
	first := time.Date(2026, 11, 3, 1, 2, 3, 0, time.UTC)
	second := first.Add(time.Minute)
	var clockMu sync.Mutex
	current := first
	store := &expiryFixture{observed: make(chan time.Time, 2)}
	maintenance := make(chan time.Time, 2)
	go func() {
		defer close(done)
		runMaintenanceLoop(ctx, store, func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return current
		}, ticks, func(_ context.Context, at time.Time) error {
			maintenance <- at
			return nil
		}, nil)
	}()
	if got := <-store.observed; !got.Equal(first) {
		t.Fatalf("startup expiry time=%s, want %s", got, first)
	}
	if got := <-maintenance; !got.Equal(first) {
		t.Fatalf("startup outbox time=%s, want %s", got, first)
	}
	clockMu.Lock()
	current = second
	clockMu.Unlock()

	// The first maintenance callback publishes its observation immediately
	// before returning. Receiving that observation does not mean the loop has
	// consumed its completion result yet. Keep waking the loop until it has
	// observed the next completed expiry pass and started the next maintenance
	// pass; this tests non-overlap without racing the callback's return.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case got := <-maintenance:
			if !got.Equal(second) {
				t.Fatalf("retry outbox time=%s, want %s", got, second)
			}
			goto retryStarted
		case ticks <- second:
			select {
			case got := <-maintenance:
				if !got.Equal(second) {
					t.Fatalf("retry outbox time=%s, want %s", got, second)
				}
				goto retryStarted
			case got := <-store.observed:
				if !got.Equal(second) {
					t.Fatalf("retry expiry time=%s, want %s", got, second)
				}
			case <-deadline.C:
				t.Fatal("maintenance loop did not start the next outbox pass after a completed expiry wake")
			}
		case <-deadline.C:
			t.Fatal("maintenance loop did not accept a retry expiry wake")
		}
	}

retryStarted:
	cancel()
	<-done
}

func TestMaintenanceLoopKeepsExpiryResponsiveWhileOutboxIsSlowAndJoinsOnStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ticks := make(chan time.Time)
	done := make(chan struct{})
	store := &expiryFixture{observed: make(chan time.Time, 3)}
	maintenanceStarted := make(chan struct{}, 2)
	maintenanceCanceled := make(chan struct{})
	var maintenanceCalls atomic.Int32
	go func() {
		defer close(done)
		runMaintenanceLoop(ctx, store, time.Now, ticks, func(workCtx context.Context, _ time.Time) error {
			maintenanceCalls.Add(1)
			maintenanceStarted <- struct{}{}
			<-workCtx.Done()
			close(maintenanceCanceled)
			return workCtx.Err()
		}, nil)
	}()
	select {
	case <-store.observed:
	case <-time.After(time.Second):
		t.Fatal("startup lease expiry did not run")
	}
	select {
	case <-maintenanceStarted:
	case <-time.After(time.Second):
		t.Fatal("startup outbox pass did not start")
	}
	for i := 0; i < 2; i++ {
		ticks <- time.Now()
		select {
		case <-store.observed:
		case <-time.After(time.Second):
			t.Fatalf("lease expiry did not run on tick %d while outbox was waiting", i+1)
		}
	}
	if got := maintenanceCalls.Load(); got != 1 {
		t.Fatalf("overlapping maintenance passes started: %d, want 1", got)
	}
	cancel()
	select {
	case <-maintenanceCanceled:
	case <-time.After(time.Second):
		t.Fatal("outbox pass did not receive cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance loop returned before joining its outbox pass")
	}
}
