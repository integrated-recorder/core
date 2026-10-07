package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type leaseFaultMode uint8

const (
	leaseFailBeforeApply leaseFaultMode = iota + 1
	leaseApplyThenError
	leaseExplicitReject
)

type leaseFaultCoordinator struct {
	mu      sync.Mutex
	faults  map[string][]leaseFaultMode
	active  map[string]map[string]bool
	calls   map[string][]string
	amounts map[string][]int64
}

func newLeaseFaultCoordinator() *leaseFaultCoordinator {
	return &leaseFaultCoordinator{
		faults: map[string][]leaseFaultMode{}, active: map[string]map[string]bool{},
		calls: map[string][]string{}, amounts: map[string][]int64{},
	}
}

func (c *leaseFaultCoordinator) fail(operation string, modes ...leaseFaultMode) {
	c.mu.Lock()
	c.faults[operation] = append(c.faults[operation], modes...)
	c.mu.Unlock()
}

func (c *leaseFaultCoordinator) clearFaults(operation string) {
	c.mu.Lock()
	delete(c.faults, operation)
	c.mu.Unlock()
}

func (c *leaseFaultCoordinator) activeCount(operation string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.active[leaseFamily(operation)])
}

func leaseFamily(operation string) string {
	switch operation {
	case "set_reservation", "release_reservation":
		return "reservation"
	case "acquire_queue", "release_queue":
		return "queue"
	case "acquire_writer", "release_writer":
		return "writer"
	default:
		return operation
	}
}

func (c *leaseFaultCoordinator) callIDs(operation string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls[operation]...)
}

func (c *leaseFaultCoordinator) apply(operation, id string, acquire bool, amount int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[operation] = append(c.calls[operation], id)
	c.amounts[operation] = append(c.amounts[operation], amount)
	var fault leaseFaultMode
	if pending := c.faults[operation]; len(pending) > 0 {
		fault = pending[0]
		c.faults[operation] = pending[1:]
	}
	if fault == leaseFailBeforeApply || fault == leaseExplicitReject {
		return fmt.Errorf("synthetic %s rejection", operation)
	}
	family := leaseFamily(operation)
	if c.active[family] == nil {
		c.active[family] = map[string]bool{}
	}
	if acquire {
		c.active[family][id] = true
	} else {
		delete(c.active[family], id)
	}
	if fault == leaseApplyThenError {
		return fmt.Errorf("synthetic %s response loss after apply", operation)
	}
	return nil
}

func (c *leaseFaultCoordinator) SetReservation(_ context.Context, _, id string, bytes int64) error {
	return c.apply("set_reservation", id, true, bytes)
}

func (c *leaseFaultCoordinator) ReleaseReservation(_ context.Context, _, id string) error {
	return c.apply("release_reservation", id, false, 0)
}

func (c *leaseFaultCoordinator) AcquireQueue(_ context.Context, id string) error {
	return c.apply("acquire_queue", id, true, 0)
}

func (c *leaseFaultCoordinator) ReleaseQueue(_ context.Context, id string) error {
	return c.apply("release_queue", id, false, 0)
}

func (c *leaseFaultCoordinator) AcquireWriter(_ context.Context, id string) error {
	return c.apply("acquire_writer", id, true, 0)
}

func (c *leaseFaultCoordinator) ReleaseWriter(_ context.Context, id string) error {
	return c.apply("release_writer", id, false, 0)
}

func assertSameLeaseID(t *testing.T, ids []string, wantCalls int) {
	t.Helper()
	if len(ids) != wantCalls {
		t.Fatalf("lease calls = %d, want %d: %v", len(ids), wantCalls, ids)
	}
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Fatalf("lease ID changed across retries: %v", ids)
		}
	}
}

func waitCommit(t *testing.T, service *IngestService, coordinator *leaseFaultCoordinator, recording string, commit func() error) error {
	t.Helper()
	done := make(chan error, 1)
	if err := service.SubmitCommit(context.Background(), recording, commit, func(err error) { done <- err }); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("coordinator did not finish commit; active writer leases=%d", coordinator.activeCount("acquire_writer"))
		return nil
	}
}

func TestCoordinatorAcquireRetriesSameIDAndDoesNotPoisonRecording(t *testing.T) {
	t.Run("queue request failed before host", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_queue", leaseFailBeforeApply)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "queue-retry", func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_queue"), 2)
		assertSameLeaseID(t, coordinator.callIDs("release_queue"), 1)
		if got := service.Snapshot(); got.QueueObjects != 0 || got.PendingCoordinatorLeases != 0 {
			t.Fatalf("queue accounting after retry = %#v", got)
		}
	})

	t.Run("queue applied then response lost", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_queue", leaseApplyThenError)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "queue-uncertain", func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_queue"), 2)
		if coordinator.activeCount("acquire_queue") != 0 || coordinator.activeCount("release_queue") != 0 {
			t.Fatalf("queue lease remained after idempotent retry: %#v", service.Snapshot())
		}
	})

	t.Run("explicit queue rejection does not leak local slot", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_queue", repeatedFault(leaseExplicitReject, runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)...)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := service.SubmitCommit(context.Background(), "queue-reject", func() error { return nil }, func(error) {}); !errors.Is(err, ErrIngestCoordinatorUnavailable) {
			t.Fatalf("queue rejection = %v", err)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_queue"), runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)
		assertSameLeaseID(t, coordinator.callIDs("release_queue"), 1)
		if coordinator.activeCount("acquire_queue") != 0 {
			t.Fatal("queue lease remained after exhausted acquisition")
		}
		if got := service.Snapshot(); got.QueueObjects != 0 || got.QueueBytes != 0 || got.ActiveWriters != 0 {
			t.Fatalf("local queue accounting drifted: %#v", got)
		}
		if err := waitCommit(t, service, coordinator, "queue-recover", func() error { return nil }); err != nil {
			t.Fatalf("next queue acquisition did not progress: %v", err)
		}
	})

	t.Run("queue retries after one transport batch", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_queue", repeatedFault(leaseExplicitReject, runtimeResourceOperationAttempts)...)
		service := newSmallIngestWithCoordinator(t, coordinator)
		var commitCalls int
		if err := waitCommit(t, service, coordinator, "queue-retry-batch", func() error {
			commitCalls++
			return nil
		}); err != nil {
			t.Fatalf("queue acquisition did not recover: %v", err)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_queue"), runtimeResourceOperationAttempts+1)
		assertSameLeaseID(t, coordinator.callIDs("release_queue"), 1)
		if commitCalls != 1 || coordinator.activeCount("acquire_queue") != 0 {
			t.Fatalf("queue retry commit calls=%d active=%d", commitCalls, coordinator.activeCount("acquire_queue"))
		}
	})

	t.Run("writer applied then response lost", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_writer", leaseApplyThenError)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "writer-uncertain", func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_writer"), 2)
		assertSameLeaseID(t, coordinator.callIDs("release_writer"), 1)
		if coordinator.activeCount("acquire_writer") != 0 {
			t.Fatal("writer lease remained after successful release")
		}
	})

	t.Run("writer request failed before host", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_writer", leaseFailBeforeApply)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "writer-retry", func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_writer"), 2)
		if coordinator.activeCount("acquire_writer") != 0 {
			t.Fatal("writer lease remained after release")
		}
	})

	t.Run("explicit writer rejection is transient and does not poison", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("acquire_writer", repeatedFault(leaseExplicitReject, runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)...)
		service := newSmallIngestWithCoordinator(t, coordinator)
		var firstPersistCalls int
		firstErr := waitCommit(t, service, coordinator, "writer-recover", func() error {
			firstPersistCalls++
			return nil
		})
		if !errors.Is(firstErr, ErrIngestCoordinatorUnavailable) || errors.Is(firstErr, ErrCanonicalCommitFailed) || firstPersistCalls != 0 {
			t.Fatalf("writer acquire failure = %v, callback calls=%d", firstErr, firstPersistCalls)
		}
		assertSameLeaseID(t, coordinator.callIDs("acquire_writer"), runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)
		if err := waitCommit(t, service, coordinator, "writer-recover", func() error { return nil }); err != nil {
			t.Fatalf("same recording stayed poisoned after transient acquire failure: %v", err)
		}
		if got := service.Snapshot(); got.StorageErrorsTotal != 0 || got.QueueObjects != 0 || got.ActiveWriters != 0 {
			t.Fatalf("writer failure poisoned or drifted accounting: %#v", got)
		}
	})
}

func TestAcceptedPayloadRetriesTransientWriterAcquire(t *testing.T) {
	coordinator := newLeaseFaultCoordinator()
	// One complete callCoordinator cycle fails, followed by another transport
	// failure. The accepted payload must remain queued until the same lease ID
	// succeeds, without invoking persistence early or twice.
	coordinator.fail("acquire_writer", repeatedFault(leaseExplicitReject, runtimeResourceOperationAttempts+1)...)
	service := newSmallIngestWithCoordinator(t, coordinator)
	payload, err := service.ReadPayload(context.Background(), "writer-payload-retry", bytes.NewReader([]byte("payload")), 8, -1, 8)
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	var persistCalls int
	if err := service.SubmitWithKind(context.Background(), payload, IngestJobKindMediaPayload, func(data []byte) (PayloadResult, error) {
		persistCalls++
		if !bytes.Equal(data, []byte("payload")) {
			return PayloadResult{}, errors.New("accepted payload changed during retry")
		}
		return PayloadResult{Size: int64(len(data))}, nil
	}, func(_ PayloadResult, err error) { completed <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("accepted payload completion = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("accepted payload did not survive coordinator recovery")
	}
	assertSameLeaseID(t, coordinator.callIDs("acquire_writer"), runtimeResourceOperationAttempts+2)
	if persistCalls != 1 {
		t.Fatalf("persistence callback calls=%d, want one", persistCalls)
	}
	service.mu.Lock()
	_, poisoned := service.failedRecordings["writer-payload-retry"]
	service.mu.Unlock()
	if poisoned {
		t.Fatal("transient writer acquisition poisoned recording")
	}
	if got := service.Snapshot(); got.StorageErrorsTotal != 0 || got.QueueObjects != 0 || got.ActiveWriters != 0 {
		t.Fatalf("transient writer acquisition changed canonical accounting: %#v", got)
	}
}

func TestPermanentWriterAcquireFailureIsBoundedAndDoesNotPoison(t *testing.T) {
	coordinator := newLeaseFaultCoordinator()
	coordinator.fail("acquire_writer", repeatedFault(leaseExplicitReject, runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)...)
	service := newSmallIngestWithCoordinator(t, coordinator)
	var persistCalls int
	err := waitCommit(t, service, coordinator, "writer-permanent-outage", func() error {
		persistCalls++
		return nil
	})
	if !errors.Is(err, ErrIngestCoordinatorUnavailable) || errors.Is(err, ErrCanonicalCommitFailed) || persistCalls != 0 {
		t.Fatalf("permanent acquire failure=%v persist calls=%d", err, persistCalls)
	}
	assertSameLeaseID(t, coordinator.callIDs("acquire_writer"), runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)
	assertSameLeaseID(t, coordinator.callIDs("release_writer"), 1)
	if coordinator.activeCount("acquire_writer") != 0 {
		t.Fatal("writer lease remained after exhausted acquisition")
	}
	service.mu.Lock()
	_, poisoned := service.failedRecordings["writer-permanent-outage"]
	service.mu.Unlock()
	if poisoned {
		t.Fatal("coordinator outage poisoned canonical recording state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatalf("bounded acquisition left shutdown blocked: %v", err)
	}
}

func TestCoordinatorDependencyFailureDoesNotBecomeCanonicalFailure(t *testing.T) {
	coordinator := newLeaseFaultCoordinator()
	coordinator.fail("acquire_writer", repeatedFault(leaseExplicitReject, runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)...)
	service := newSmallIngestWithCoordinator(t, coordinator)
	var initPersistCalls int
	initErr := waitCommit(t, service, coordinator, "init-dependency", func() error {
		initPersistCalls++
		return nil
	})
	if !errors.Is(initErr, ErrIngestCoordinatorUnavailable) || errors.Is(initErr, ErrCanonicalCommitFailed) || initPersistCalls != 0 {
		t.Fatalf("init acquire failure=%v persist calls=%d", initErr, initPersistCalls)
	}

	var mediaPersistCalls int
	mediaErr := waitCommit(t, service, coordinator, "init-dependency", func() error {
		mediaPersistCalls++
		return fmt.Errorf("canonical storage commit failed: init payload dependency: %w", initErr)
	})
	if !errors.Is(mediaErr, ErrIngestCoordinatorUnavailable) || errors.Is(mediaErr, ErrCanonicalCommitFailed) || !errors.Is(mediaErr, initErr) {
		t.Fatalf("dependent media error=%v, want original coordinator failure without canonical classification", mediaErr)
	}
	var attempts attemptCountedError
	if !errors.As(mediaErr, &attempts) || attempts.Attempts() != 0 {
		t.Fatalf("dependent media attempts=%#v, want zero canonical persistence attempts", attempts)
	}
	if mediaPersistCalls != 1 {
		t.Fatalf("dependent media callback calls=%d, want one", mediaPersistCalls)
	}
	service.mu.Lock()
	_, poisoned := service.failedRecordings["init-dependency"]
	service.mu.Unlock()
	if poisoned || service.Snapshot().StorageErrorsTotal != 0 {
		t.Fatalf("coordinator dependency failure poisoned canonical state: %#v", service.Snapshot())
	}
	if err := waitCommit(t, service, coordinator, "init-dependency", func() error { return nil }); err != nil {
		t.Fatalf("later canonical commit did not progress: %v", err)
	}
}

func TestQueueAcquireExhaustionLeavesPayloadWithCaller(t *testing.T) {
	coordinator := newLeaseFaultCoordinator()
	coordinator.fail("acquire_queue", repeatedFault(leaseExplicitReject, runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)...)
	service := newSmallIngestWithCoordinator(t, coordinator)
	payload, err := service.ReadPayload(context.Background(), "queue-payload-retry", bytes.NewReader([]byte("caller")), 8, -1, 8)
	if err != nil {
		t.Fatal(err)
	}
	err = service.SubmitWithKind(context.Background(), payload, IngestJobKindMediaPayload, func([]byte) (PayloadResult, error) {
		t.Fatal("pre-admission queue failure ran canonical callback")
		return PayloadResult{}, nil
	}, func(PayloadResult, error) { t.Fatal("pre-admission queue failure completed accepted job") })
	if !errors.Is(err, ErrIngestCoordinatorUnavailable) || errors.Is(err, ErrCanonicalCommitFailed) {
		t.Fatalf("queue acquire error = %v", err)
	}
	assertSameLeaseID(t, coordinator.callIDs("acquire_queue"), runtimeResourceLeaseAcquireCycles*runtimeResourceOperationAttempts)
	assertSameLeaseID(t, coordinator.callIDs("release_queue"), 1)
	if !bytes.Equal(payload.Bytes(), []byte("caller")) {
		t.Fatal("failed pre-admission submit transferred or cleared payload ownership")
	}
	if got := service.Snapshot(); got.QueueObjects != 0 || got.QueueBytes != 0 || got.ActiveWriters != 0 {
		t.Fatalf("queue accounting after exhausted acquisition = %#v", got)
	}
	payload.Release()
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
		t.Fatalf("caller payload release left local accounting: %#v", got)
	}
}

func TestCanonicalFailureRemainsRecordingScoped(t *testing.T) {
	coordinator := newLeaseFaultCoordinator()
	service := newSmallIngestWithCoordinator(t, coordinator)
	firstCause := errors.New("recording A canonical failure")
	if err := waitCommit(t, service, coordinator, "recording-a", func() error { return firstCause }); !errors.Is(err, firstCause) {
		t.Fatalf("recording A first failure = %v", err)
	}
	if err := waitCommit(t, service, coordinator, "recording-b", func() error { return nil }); err != nil {
		t.Fatalf("recording A failure poisoned recording B: %v", err)
	}
	var followupCalls int
	followupErr := waitCommit(t, service, coordinator, "recording-a", func() error {
		followupCalls++
		return errors.New("secondary failure")
	})
	if !errors.Is(followupErr, firstCause) || followupCalls != 0 {
		t.Fatalf("recording A follow-up = %v, persist calls=%d; want original cause and no persist", followupErr, followupCalls)
	}
	if got := service.Snapshot(); got.QueueObjects != 0 || got.PendingCoordinatorLeases != 0 || got.StorageErrorsTotal == 0 {
		t.Fatalf("cross-recording failure accounting = %#v", got)
	}
}

func TestCoordinatorReleaseRetriesSameIDAndSurfacesUnresolvedLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode leaseFaultMode
	}{
		{name: "request failed before host", mode: leaseFailBeforeApply},
		{name: "host applied then response lost", mode: leaseApplyThenError},
	} {
		t.Run("queue "+tc.name, func(t *testing.T) {
			coordinator := newLeaseFaultCoordinator()
			coordinator.fail("release_queue", tc.mode)
			service := newSmallIngestWithCoordinator(t, coordinator)
			if err := waitCommit(t, service, coordinator, "queue-release", func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			assertSameLeaseID(t, coordinator.callIDs("release_queue"), 2)
			if coordinator.activeCount("acquire_queue") != 0 || service.Snapshot().PendingCoordinatorLeases != 0 {
				t.Fatalf("queue release retry left lease: %#v", service.Snapshot())
			}
		})
		t.Run("writer "+tc.name, func(t *testing.T) {
			coordinator := newLeaseFaultCoordinator()
			coordinator.fail("release_writer", tc.mode)
			service := newSmallIngestWithCoordinator(t, coordinator)
			if err := waitCommit(t, service, coordinator, "writer-release", func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			assertSameLeaseID(t, coordinator.callIDs("release_writer"), 2)
			if coordinator.activeCount("acquire_writer") != 0 || service.Snapshot().PendingCoordinatorLeases != 0 {
				t.Fatalf("writer release retry left lease: %#v", service.Snapshot())
			}
		})
	}

	t.Run("permanent queue release failure is visible at close", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("release_queue", repeatedFault(leaseExplicitReject, 30)...)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "queue-release-fail", func() error { return nil }); err != nil {
			t.Fatalf("release failure changed successful canonical result: %v", err)
		}
		assertSameLeaseID(t, coordinator.callIDs("release_queue"), runtimeResourceOperationAttempts)
		if got := service.Snapshot(); got.PendingCoordinatorLeases != 1 || got.QueueObjects != 0 || got.ActiveWriters != 0 {
			t.Fatalf("release failure accounting = %#v", got)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); !errors.Is(err, ErrIngestCoordinatorUnavailable) {
			t.Fatalf("Close error = %v, want unresolved coordinator lease", err)
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		coordinator.clearFaults("release_queue")
		if err := service.Close(ctx2); err != nil {
			t.Fatalf("Close retry = %v", err)
		}
		if got := service.Snapshot().PendingCoordinatorLeases; got != 0 {
			t.Fatalf("explicit post-close retry left %d leases", got)
		}
	})

	t.Run("permanent writer release failure is visible at close", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("release_writer", repeatedFault(leaseExplicitReject, 30)...)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "writer-release-fail", func() error { return nil }); err != nil {
			t.Fatalf("release failure changed successful canonical result: %v", err)
		}
		if got := service.Snapshot(); got.PendingCoordinatorLeases != 1 || got.QueueObjects != 0 || got.ActiveWriters != 0 {
			t.Fatalf("writer release failure accounting = %#v", got)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); !errors.Is(err, ErrIngestCoordinatorUnavailable) {
			t.Fatalf("Close error = %v, want unresolved writer lease", err)
		}
		coordinator.clearFaults("release_writer")
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		if err := service.Close(ctx2); err != nil {
			t.Fatalf("Close retry = %v", err)
		}
		if got := service.Snapshot().PendingCoordinatorLeases; got != 0 {
			t.Fatalf("explicit post-close retry left %d writer leases", got)
		}
	})

	t.Run("release failure retries later while service remains open", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("release_queue", leaseExplicitReject, leaseExplicitReject, leaseExplicitReject)
		service := newSmallIngestWithCoordinator(t, coordinator)
		if err := waitCommit(t, service, coordinator, "queue-release-later", func() error { return nil }); err != nil {
			t.Fatalf("release failure changed canonical result: %v", err)
		}
		if got := service.Snapshot(); got.PendingCoordinatorLeases != 1 {
			t.Fatalf("pending queue lease = %d, want 1", got.PendingCoordinatorLeases)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if service.Snapshot().PendingCoordinatorLeases == 0 {
				if coordinator.activeCount("acquire_queue") != 0 {
					t.Fatal("background retry cleared local state but left Host lease")
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("background release retry did not clear pending queue lease")
	})

	t.Run("release state allows retry after failed bounded attempt", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("release_writer", leaseExplicitReject, leaseExplicitReject, leaseExplicitReject)
		state := &leaseReleaseState{}
		call := func() error {
			return callCoordinatorRelease("release writer", func(ctx context.Context) error {
				return coordinator.ReleaseWriter(ctx, "same-writer-id")
			})
		}
		if err := state.release(call); !errors.Is(err, ErrIngestCoordinatorUnavailable) {
			t.Fatalf("first bounded release = %v", err)
		}
		if err := state.release(call); err != nil {
			t.Fatalf("second release did not retry: %v", err)
		}
		assertSameLeaseID(t, coordinator.callIDs("release_writer"), runtimeResourceOperationAttempts+1)
	})
}

func TestLeaseReleaseWaitHonorsCallerDeadline(t *testing.T) {
	state := &leaseReleaseState{}
	started := make(chan struct{})
	finish := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- state.release(func() error {
			close(started)
			<-finish
			return nil
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- state.releaseContext(ctx, func() error {
			t.Error("concurrent waiter started duplicate release")
			return nil
		})
	}()
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("release waiter ignored caller cancellation")
	}

	close(finish)
	if err := <-firstDone; err != nil {
		t.Fatalf("first release = %v", err)
	}
	if err := state.releaseContext(ctx, func() error {
		t.Fatal("released lease repeated its remote call")
		return nil
	}); err != nil {
		t.Fatalf("idempotent release after cancellation = %v", err)
	}
}

func repeatedFault(mode leaseFaultMode, count int) []leaseFaultMode {
	result := make([]leaseFaultMode, count)
	for i := range result {
		result[i] = mode
	}
	return result
}

func TestCoordinatorReservationRetriesAndPreservesLocalAccounting(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode leaseFaultMode
	}{
		{name: "request failed before host", mode: leaseFailBeforeApply},
		{name: "host applied then response lost", mode: leaseApplyThenError},
	} {
		t.Run("set "+tc.name, func(t *testing.T) {
			coordinator := newLeaseFaultCoordinator()
			coordinator.fail("set_reservation", tc.mode)
			service := newSmallIngestWithCoordinator(t, coordinator)
			payload, err := service.ReadPayload(context.Background(), "reservation-set", bytes.NewReader([]byte("data")), 8, 4, 4)
			if err != nil {
				t.Fatal(err)
			}
			payload.Release()
			assertSameLeaseID(t, coordinator.callIDs("set_reservation"), 2)
			assertSameLeaseID(t, coordinator.callIDs("release_reservation"), 1)
			if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 || got.PendingCoordinatorLeases != 0 {
				t.Fatalf("reservation accounting = %#v", got)
			}
		})
		t.Run("release "+tc.name, func(t *testing.T) {
			coordinator := newLeaseFaultCoordinator()
			coordinator.fail("release_reservation", tc.mode)
			service := newSmallIngestWithCoordinator(t, coordinator)
			payload, err := service.ReadPayload(context.Background(), "reservation-release", bytes.NewReader([]byte("data")), 8, 4, 4)
			if err != nil {
				t.Fatal(err)
			}
			payload.Release()
			assertSameLeaseID(t, coordinator.callIDs("release_reservation"), 2)
			if coordinator.activeCount("set_reservation") != 0 || service.Snapshot().PendingCoordinatorLeases != 0 {
				t.Fatalf("reservation release left lease: %#v", service.Snapshot())
			}
		})
	}

	t.Run("explicit set rejection compensates uncertain lease", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("set_reservation", leaseExplicitReject, leaseExplicitReject, leaseExplicitReject)
		service := newSmallIngestWithCoordinator(t, coordinator)
		_, err := service.ReadPayload(context.Background(), "reservation-reject", bytes.NewReader([]byte("data")), 8, 4, 4)
		if !errors.Is(err, ErrIngestCoordinatorUnavailable) {
			t.Fatalf("reservation set error = %v", err)
		}
		assertSameLeaseID(t, coordinator.callIDs("set_reservation"), runtimeResourceOperationAttempts)
		assertSameLeaseID(t, coordinator.callIDs("release_reservation"), 1)
		if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 || got.PendingCoordinatorLeases != 0 || coordinator.activeCount("set_reservation") != 0 {
			t.Fatalf("rejected reservation accounting = %#v", got)
		}
	})

	t.Run("background release updates payload after recovery", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("release_reservation", leaseExplicitReject, leaseExplicitReject, leaseExplicitReject)
		service := newSmallIngestWithCoordinator(t, coordinator)
		payload, err := service.ReadPayload(context.Background(), "reservation-later", bytes.NewReader([]byte("data")), 8, 4, 4)
		if err != nil {
			t.Fatal(err)
		}
		payload.Release()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if service.Snapshot().PendingCoordinatorLeases == 0 {
				payload.reservationMu.Lock()
				remaining := payload.globalReserved
				payload.reservationMu.Unlock()
				if remaining != 0 || coordinator.activeCount("set_reservation") != 0 {
					t.Fatalf("reservation not cleared: bytes=%d active=%d", remaining, coordinator.activeCount("set_reservation"))
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("background retry did not release reservation")
	})

	t.Run("permanent reservation release is surfaced at close", func(t *testing.T) {
		coordinator := newLeaseFaultCoordinator()
		coordinator.fail("release_reservation", repeatedFault(leaseExplicitReject, 30)...)
		service := newSmallIngestWithCoordinator(t, coordinator)
		payload, err := service.ReadPayload(context.Background(), "reservation-release-fail", bytes.NewReader([]byte("data")), 8, 4, 4)
		if err != nil {
			t.Fatal(err)
		}
		payload.Release()
		if got := service.Snapshot(); got.PendingCoordinatorLeases != 1 || got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
			t.Fatalf("pending reservation release = %#v", got)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); !errors.Is(err, ErrIngestCoordinatorUnavailable) {
			t.Fatalf("Close error = %v, want unresolved reservation", err)
		}
		coordinator.clearFaults("release_reservation")
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		if err := service.Close(ctx2); err != nil {
			t.Fatalf("Close retry = %v", err)
		}
		payload.reservationMu.Lock()
		reserved := payload.globalReserved
		payload.reservationMu.Unlock()
		if reserved != 0 || service.Snapshot().PendingCoordinatorLeases != 0 {
			t.Fatalf("reservation retry state = %d bytes, %#v", reserved, service.Snapshot())
		}
	})
}
