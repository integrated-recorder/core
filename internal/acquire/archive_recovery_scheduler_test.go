package acquire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

func newSchedulerUnitFixture(ids ...string) (*Manager, map[string]*entry, *automaticRecoveryScheduler) {
	entries := make(map[string]*entry, len(ids))
	manager := &Manager{entries: entries}
	for _, id := range ids {
		now := time.Now().UTC()
		media := adapterproto.MediaSource{
			Type: "hls", ManifestURL: "https://fixture.example/live.m3u8",
			HistoricalAvailability: &adapterproto.HistoricalAvailability{
				Mode:           adapterproto.HistoricalModeSequenceRanges,
				SequenceRanges: []adapterproto.HistoricalSequenceRange{{Start: 1, End: 1000}},
			},
		}
		entries[id] = &entry{
			recording: &domain.Recording{ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main"}}},
			done:      closedChannel(), media: media,
		}
	}
	scheduler := newAutomaticRecoveryScheduler(manager, func(context.Context, string) (OwnershipToken, error) {
		return OwnershipToken{}, errors.New("unexpected claim")
	})
	manager.autoRecovery = scheduler
	return manager, entries, scheduler
}

func TestAutomaticRecoveryImmediateTriggerPromotesQueuedRetryInPlace(t *testing.T) {
	for _, trigger := range []string{"live-claim", "refresh-generation"} {
		t.Run(trigger, func(t *testing.T) {
			id := "00000000000000000000000000000001"
			manager, entries, scheduler := newSchedulerUnitFixture(id)
			fixedNow := time.Now()
			scheduler.now = func() time.Time { return fixedNow }
			later := fixedNow.Add(5 * time.Minute)
			scheduler.enqueue(id, later)
			before := append([]string(nil), scheduler.queue...)
			if len(before) != 1 || !scheduler.queued[id].due.Equal(later) {
				t.Fatalf("delayed slot not established: queue=%v item=%+v", before, scheduler.queued[id])
			}
			if trigger == "refresh-generation" {
				entries[id].mu.Lock()
				entries[id].mediaGeneration++
				entries[id].mu.Unlock()
			}
			// These are the same coalesced triggers used after live claim and
			// successful source refresh publication.
			manager.signalAutomaticArchiveRecovery(id)
			if len(scheduler.queue) != 1 || scheduler.queue[0] != before[0] || !scheduler.queued[id].due.IsZero() {
				t.Fatalf("immediate trigger failed to promote same FIFO slot: queue=%v item=%+v", scheduler.queue, scheduler.queued[id])
			}
		})
	}
}

func TestAutomaticRecoveryHistoricalRefreshPromotesDelayedRetry(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	current := repairMediaContextForTest()
	refreshed := current
	refreshed.ManifestURL = "https://media.example/archive/refreshed.m3u8?hdnts=next-value"
	root := newRecoveryFairnessRoot(t, store, "00000000000000000000000000000007", current)
	fixture := &repairFixtureTransport{}
	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	defer func() {
		if closeErr := closeManager(); closeErr != nil {
			t.Errorf("close refresh manager: %v", closeErr)
		}
	}()
	manager.resolver = &refreshingResolver{next: refreshed}
	e.mu.Lock()
	e.media = current
	e.mediaGeneration = 1
	e.mu.Unlock()
	scheduler := newAutomaticRecoveryScheduler(manager, func(context.Context, string) (OwnershipToken, error) {
		return OwnershipToken{}, errors.New("unexpected recovery claim")
	})
	manager.autoRecovery = scheduler
	fixedNow := time.Now()
	scheduler.now = func() time.Time { return fixedNow }
	due := fixedNow.Add(5 * time.Minute)
	scheduler.enqueue(root.ID, due)
	owner := ownerForRepair(root.ID, 1)
	got, generation, err := manager.refreshMediaAtGenerationOwned(context.Background(), e, current, 1, &owner, true)
	if err != nil {
		t.Fatalf("refresh historical media context: %v", err)
	}
	if generation != 2 || got.ManifestURL != refreshed.ManifestURL {
		t.Fatalf("refresh result generation=%d URL=%q", generation, got.ManifestURL)
	}
	if item := scheduler.queued[root.ID]; !item.due.IsZero() {
		t.Fatalf("published source refresh did not preempt queued retry: %+v", item)
	}
	scheduler.cancel()
	manager.mu.Lock()
	manager.autoRecovery = nil
	manager.mu.Unlock()
}

func TestAutomaticRecoveryInFlightDirtyPreemptsRetryAndRecheck(t *testing.T) {
	for _, outcome := range []automaticRecoveryOutcome{{retry: true}, {recheck: true}} {
		name := "retry"
		if outcome.recheck {
			name = "recheck"
		}
		t.Run(name, func(t *testing.T) {
			id := "00000000000000000000000000000002"
			manager, entries, scheduler := newSchedulerUnitFixture(id)
			fixedNow := time.Now()
			scheduler.now = func() time.Time { return fixedNow }
			e := entries[id]
			e.recoveryAttempts = 3
			e.recoveryNoProgress = 1
			scheduler.enqueue(id, time.Time{})
			gotID, item, _, ok := scheduler.next()
			if !ok || gotID != id {
				t.Fatalf("begin pass: id=%q ok=%v", gotID, ok)
			}
			manager.signalAutomaticArchiveRecovery(id)
			scheduler.complete(id, item, outcome)
			if !scheduler.queued[id].due.IsZero() {
				t.Fatalf("dirty event did not preempt delayed outcome: %+v", scheduler.queued[id])
			}
			if e.recoveryAttempts != 3 || e.recoveryNoProgress != 1 {
				t.Fatalf("preempted outcome changed counters: attempts=%d noProgress=%d", e.recoveryAttempts, e.recoveryNoProgress)
			}

			gotID, item, _, ok = scheduler.next()
			if !ok || gotID != id {
				t.Fatalf("begin immediate follow-up: id=%q ok=%v", gotID, ok)
			}
			scheduler.complete(id, item, automaticRecoveryOutcome{retry: true})
			want := fixedNow.Add(8 * time.Second)
			if !scheduler.queued[id].due.Equal(want) || e.recoveryAttempts != 4 {
				t.Fatalf("normal backoff did not resume after follow-up: due=%v want=%v attempts=%d", scheduler.queued[id].due, want, e.recoveryAttempts)
			}
		})
	}
}

func TestAutomaticRecoveryReadySlotRunsAheadOfFutureFIFOHead(t *testing.T) {
	manager, _, scheduler := newSchedulerUnitFixture("a", "b")
	_ = manager
	fixedNow := time.Now()
	scheduler.now = func() time.Time { return fixedNow }
	scheduler.enqueue("a", fixedNow.Add(5*time.Minute))
	scheduler.enqueue("b", time.Time{})
	id, _, _, ok := scheduler.next()
	if !ok || id != "b" {
		t.Fatalf("future queue head blocked ready recording: id=%q ok=%v queue=%v", id, ok, scheduler.queue)
	}
}

func TestAutomaticRecoveryManifestEmptyObservationUsesFiniteSlowRechecks(t *testing.T) {
	const id = "00000000000000000000000000000003"
	_, entries, scheduler := newSchedulerUnitFixture(id)
	defer scheduler.cancel()
	e := entries[id]
	e.mu.Lock()
	e.media.HistoricalAvailability = &adapterproto.HistoricalAvailability{
		Mode: adapterproto.HistoricalModeManifest, HistoricalManifestURL: e.media.ManifestURL,
	}
	e.mu.Unlock()
	availability := e.media.HistoricalAvailability
	if !historicalDeclarationNeedsRecheck(availability, nil) {
		t.Fatal("empty manifest observation must remain eligible for slow recheck")
	}
	fixedNow := time.Now()
	scheduler.now = func() time.Time { return fixedNow }
	scheduler.enqueue(id, time.Time{})
	for pass := 0; pass <= maxAutomaticRecoveryNoProgressRechecks; pass++ {
		gotID, item, _, ok := scheduler.next()
		if !ok || gotID != id {
			t.Fatalf("manifest pass %d did not run: id=%q ok=%v", pass+1, gotID, ok)
		}
		scheduler.complete(id, item, automaticRecoveryOutcome{recheck: true})
		wantRechecks := pass + 1
		if wantRechecks > maxAutomaticRecoveryNoProgressRechecks {
			wantRechecks = maxAutomaticRecoveryNoProgressRechecks
		}
		if e.recoveryNoProgress != wantRechecks {
			t.Fatalf("recheck count=%d, want %d", e.recoveryNoProgress, wantRechecks)
		}
		if pass == maxAutomaticRecoveryNoProgressRechecks {
			if len(scheduler.queue) != 0 || len(scheduler.queued) != 0 {
				t.Fatalf("empty manifest did not converge after finite rechecks: queue=%v items=%v", scheduler.queue, scheduler.queued)
			}
			break
		}
		wantDue := fixedNow.Add(automaticRecoveryRecheck)
		if gotID, _, wait, ok := scheduler.next(); !ok || gotID != "" || wait != automaticRecoveryRecheck {
			t.Fatalf("empty manifest entered hot loop: id=%q wait=%s ok=%v", gotID, wait, ok)
		}
		fixedNow = wantDue
	}
}

func TestAutomaticRecoveryRescanRetainsOverflowedImmediateTrigger(t *testing.T) {
	ids := make([]string, maxAutomaticRecoveryQueue+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("%032x", i+1)
	}
	_, _, scheduler := newSchedulerUnitFixture(ids...)
	for _, id := range ids {
		scheduler.enqueue(id, time.Time{})
	}
	if len(scheduler.queue) != maxAutomaticRecoveryQueue || !scheduler.rescan {
		t.Fatalf("queue bound/rescan lost: queue=%d rescan=%v", len(scheduler.queue), scheduler.rescan)
	}
	first, item, _, ok := scheduler.next()
	if !ok || first != ids[0] {
		t.Fatalf("first ready item=%q ok=%v", first, ok)
	}
	scheduler.complete(first, item, automaticRecoveryOutcome{})
	scheduler.refill()
	if _, exists := scheduler.queued[ids[len(ids)-1]]; !exists {
		t.Fatalf("overflowed immediate trigger did not return through rescan: queue=%d rescan=%v cursor=%q last=%q", len(scheduler.queue), scheduler.rescan, scheduler.cursor, ids[len(ids)-1])
	}
	if len(scheduler.queue) > maxAutomaticRecoveryQueue {
		t.Fatalf("rescan exceeded queue bound: %d", len(scheduler.queue))
	}
}

func TestAutomaticRecoveryReleaseCoalescingKeepsEarliestDue(t *testing.T) {
	id := "00000000000000000000000000000005"
	_, _, scheduler := newSchedulerUnitFixture(id)
	fixedNow := time.Now()
	scheduler.now = func() time.Time { return fixedNow }
	queuedDue := fixedNow.Add(10 * time.Minute)
	releaseDue := fixedNow.Add(5 * time.Minute)
	scheduler.enqueue(id, queuedDue)
	scheduler.queueRelease(id, ownerForRepair(id, 10), releaseDue, false)
	item := scheduler.queued[id]
	if item.release == nil || !item.due.Equal(releaseDue) {
		t.Fatalf("release coalescing postponed the earliest obligation: %+v", item)
	}
}

func TestAutomaticRecoveryReleaseOnlyOverflowReturnsThroughRescan(t *testing.T) {
	id := "00000000000000000000000000000006"
	manager, entries, scheduler := newSchedulerUnitFixture(id)
	owner := ownerForRepair(id, 11)
	e := entries[id]
	e.ownership = &owner
	released := make([]OwnershipToken, 0, 1)
	manager.terminalOwnerRelease = func(_ context.Context, got OwnershipToken) error {
		released = append(released, got)
		return nil
	}
	scheduler.mu.Lock()
	for index := 0; index < maxAutomaticRecoveryQueue; index++ {
		filler := fmt.Sprintf("filler-%03d", index)
		scheduler.queue = append(scheduler.queue, filler)
		scheduler.queued[filler] = automaticRecoveryQueueItem{}
	}
	scheduler.mu.Unlock()
	scheduler.queueRelease(id, owner, time.Time{}, false)
	if len(scheduler.queue) != maxAutomaticRecoveryQueue || !scheduler.rescan {
		t.Fatalf("release overflow did not preserve bounded queue/rescan: queue=%d rescan=%v", len(scheduler.queue), scheduler.rescan)
	}
	for attempts := 0; attempts <= maxAutomaticRecoveryQueue; attempts++ {
		gotID, item, _, ok := scheduler.next()
		if !ok || gotID == "" {
			t.Fatalf("rescan stopped before release obligation: id=%q ok=%v at=%d", gotID, ok, attempts)
		}
		if gotID == id {
			if item.release == nil || *item.release != owner {
				t.Fatalf("rescan changed exact release token: %+v", item)
			}
			outcome := scheduler.process(gotID, item)
			scheduler.complete(gotID, item, outcome)
			if outcome.releaseErr != nil || len(released) != 1 || released[0] != owner {
				t.Fatalf("release retry not executed exactly once: outcome=%+v released=%+v", outcome, released)
			}
			if e.pendingRecoveryRelease != nil || e.ownership != nil {
				t.Fatalf("successful rescan release retained ownership: pending=%+v owner=%+v", e.pendingRecoveryRelease, e.ownership)
			}
			return
		}
		scheduler.complete(gotID, item, automaticRecoveryOutcome{})
	}
	t.Fatal("release-only overflow was not eventually retried")
}

func TestAutomaticRecoveryReleaseFailureRetriesExactTokenOnly(t *testing.T) {
	id := "00000000000000000000000000000003"
	manager, entries, scheduler := newSchedulerUnitFixture(id)
	e := entries[id]
	old := ownerForRepair(id, 7)
	stale := ownerForRepair(id, 8)
	newer := ownerForRepair(id, 9)
	e.ownership = &old
	fixedNow := time.Now()
	scheduler.now = func() time.Time { return fixedNow }
	var released []OwnershipToken
	manager.terminalOwnerRelease = func(_ context.Context, owner OwnershipToken) error {
		released = append(released, owner)
		if len(released) == 1 {
			return errors.New("temporary release failure")
		}
		return nil
	}
	scheduler.inFlight = id
	scheduler.complete(id, automaticRecoveryQueueItem{}, automaticRecoveryOutcome{})
	if len(released) != 1 || released[0] != old || !sameOwner(e.ownership, old) {
		t.Fatalf("first failed release lost owner: releases=%+v owner=%+v", released, e.ownership)
	}
	queued := scheduler.queued[id]
	if queued.release == nil || *queued.release != old || !queued.due.Equal(fixedNow.Add(time.Second)) {
		t.Fatalf("release obligation not queued with bounded delay: %+v", queued)
	}
	manager.terminalOwnerRelease = func(_ context.Context, owner OwnershipToken) error {
		released = append(released, owner)
		return nil
	}
	scheduler.now = func() time.Time { return fixedNow.Add(2 * time.Second) }
	id, item, _, ok := scheduler.next()
	if !ok || id != "00000000000000000000000000000003" || item.release == nil || *item.release != old {
		t.Fatalf("release retry changed exact token: id=%q item=%+v ok=%v", id, item, ok)
	}
	outcome := scheduler.process(id, item)
	scheduler.complete(id, item, outcome)
	if len(released) != 2 || released[1] != old {
		t.Fatalf("retry released wrong owner: %+v", released)
	}
	if e.ownership != nil || e.pendingRecoveryRelease != nil {
		t.Fatalf("successful retry retained old owner: pending=%+v owner=%+v", e.pendingRecoveryRelease, e.ownership)
	}

	e.mu.Lock()
	e.ownership = &newer
	e.mu.Unlock()
	manager.retainAutomaticRecoveryRelease(e, stale, fixedNow, false)
	manager.terminalOwnerRelease = func(_ context.Context, owner OwnershipToken) error {
		released = append(released, owner)
		return recordingowner.ErrStaleOwner
	}
	scheduler.queueRelease(id, stale, fixedNow, false)
	scheduler.now = func() time.Time { return fixedNow.Add(3 * time.Second) }
	id, item, _, ok = scheduler.next()
	if !ok || id != "00000000000000000000000000000003" || item.release == nil || *item.release != stale {
		t.Fatalf("stale retry changed exact token: id=%q item=%+v ok=%v", id, item, ok)
	}
	outcome = scheduler.process(id, item)
	scheduler.complete(id, item, outcome)
	if len(released) != 3 || released[2] != stale {
		t.Fatalf("owner change retried wrong token: %+v", released)
	}
	if !sameOwner(e.ownership, newer) {
		t.Fatalf("exact-token retry cleared newer owner: %+v", e.ownership)
	}
	if e.pendingRecoveryRelease != nil {
		t.Fatalf("successful exact-token retry retained old pending token: %+v", e.pendingRecoveryRelease)
	}
}

func TestAutomaticRecoveryCloseRetainsOwnerWhenReleaseDeadlineExpires(t *testing.T) {
	id := "00000000000000000000000000000004"
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	owner := ownerForRepair(id, 9)
	root := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main"}}}
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, &http.Client{}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureTerminalOwnerRelease(func(ctx context.Context, _ OwnershipToken) error {
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadRecordingReadOnly(id)
	if err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: loaded, done: closedChannel(), ownership: &owner}
	manager.mu.Lock()
	manager.entries[id] = e
	manager.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	closeErr := manager.Close(ctx)
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("Close error=%v, want deadline", closeErr)
	}
	if !sameOwner(e.ownership, owner) {
		t.Fatalf("Close discarded exact token after failed release: %+v", e.ownership)
	}
	// Repeated Close retries retained exact token without deadlock.
	manager.mu.Lock()
	manager.terminalOwnerRelease = func(_ context.Context, got OwnershipToken) error {
		if got != owner {
			return recordingowner.ErrStaleOwner
		}
		return nil
	}
	manager.mu.Unlock()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := manager.Close(ctx2); err != nil {
		t.Fatalf("repeat Close release retry: %v", err)
	}
	if e.ownership != nil {
		t.Fatalf("repeat Close did not clear released owner: %+v", e.ownership)
	}
}
