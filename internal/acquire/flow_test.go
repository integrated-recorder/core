package acquire

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
)

func TestAcquisitionFlowUsesObservedCoordinatesAndEphemeralActors(t *testing.T) {
	manager, e, closeManager := newLivePresentationFixture(t)
	var scheduler *segmentScheduler
	defer func() {
		if scheduler != nil {
			e.mu.Lock()
			if e.scheduler == scheduler {
				e.scheduler = nil
			}
			e.mu.Unlock()
			scheduler.cancel()
		}
		closeManager(t)
	}()
	first := commitLivePresentationSegment(t, manager, e, archiveindex.ClaimLiveOrigin, 0, 0, 501, "")
	const observedSequence = uint64(502)
	e.mu.Lock()
	track := e.recording.Tracks["main"]
	track.SourcePlaylistURL = "https://source.invalid/live.m3u8?token=must-not-leak"
	track.LivePresentation.HasEpochMapping = true
	track.LivePresentation.MappedSourceEpoch = 0
	track.LivePresentation.SourceSequenceBase = 501
	track.LivePresentation.PresentationOrdinalBase = first.LivePresentationOrdinal
	track.LivePresentation.MaxSourceSequence = 501
	track.LivePresentation.LastSequence = 501
	if track.LivePresentation.NextOrdinal < first.LivePresentationOrdinal+2 {
		track.LivePresentation.NextOrdinal = first.LivePresentationOrdinal + 2
	}
	track.PendingSegments = append(track.PendingSegments, domain.PendingSequence{SourceEpoch: 0, DiscontinuitySequence: 0, Sequence: observedSequence, LivePresentationOrdinal: 2, Duration: 2})
	e.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	scheduler = &segmentScheduler{
		manager: manager, e: e, ctx: ctx, cancel: cancel, changed: make(chan struct{}),
		tasks: map[segmentTaskKey]*segmentTask{}, init: make(map[string]*initFlight),
	}
	key := segmentTaskKey{epoch: 0, discontinuitySequence: 0, sequence: observedSequence}
	scheduler.tasks[key] = &segmentTask{key: key, ordinal: 2, available: true, observed: true, state: segmentTaskInFlight, workerID: "W1", actorID: "local:live:W1:1"}
	scheduler.latestManifestWindow = &liveManifestWindow{
		trackID: "main", epoch: 0, discontinuitySequence: 0,
		firstSequence: 501, lastSequence: 501, firstOrdinal: 1, lastOrdinal: 1,
		observedAt: time.Now().UTC(), coordinates: []liveManifestCoordinate{
			{key: segmentTaskKey{epoch: 0, discontinuitySequence: 0, sequence: 501}, presentationOrdinal: first.LivePresentationOrdinal, duration: 2 * time.Second},
		},
	}
	e.mu.Lock()
	e.scheduler = scheduler
	e.mu.Unlock()

	view, err := manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 2 || view.Segments[0].State != "present" || view.Segments[1].State != "pending" {
		t.Fatalf("observed coordinates were not projected: %#v", view.Segments)
	}
	if len(view.Actors) != 1 || view.Actors[0].WorkerID != "W1" || view.Actors[0].PresentationOrdinal != 2 {
		t.Fatalf("worker actor did not own its observed segment: %#v", view.Actors)
	}
	if view.Window == nil || view.Window.FirstSequence != 501 || view.Window.LastSequence != 501 || view.Window.FirstOrdinal != 1 || view.Window.LastOrdinal != 1 {
		t.Fatalf("manifest window does not match the accepted playlist: %#v", view.Window)
	}
	if view.Actors[0].PresentationOrdinal <= view.Window.LastOrdinal {
		t.Fatalf("scheduler-owned segment inside a slid-out coordinate should remain outside the manifest bracket: actor=%#v window=%#v", view.Actors[0], view.Window)
	}
	restarted, err := NewManagerWithMode(manager.store, &http.Client{}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	restartedView, err := restarted.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(restartedView.Actors) != 0 {
		t.Fatalf("ephemeral worker identities survived a fresh Manager generation: %#v", restartedView.Actors)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := restarted.Close(closeCtx); err != nil {
		closeCancel()
		t.Fatalf("close fresh Manager generation: %v", err)
	}
	closeCancel()
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "must-not-leak") || strings.Contains(string(encoded), "source.invalid") || strings.Contains(string(encoded), "SourceURI") {
		t.Fatalf("acquisition projection exposed source data: %s", encoded)
	}

	scheduler.mu.Lock()
	scheduler.tasks[key].state = segmentTaskRetryWait
	scheduler.tasks[key].workerID = ""
	scheduler.tasks[key].actorID = ""
	scheduler.mu.Unlock()
	view, err = manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actors) != 0 || len(view.Segments) != 2 || !view.Segments[1].Retrying {
		t.Fatalf("retry wait should show a transient outline without claiming a free worker: actors=%#v segments=%#v", view.Actors, view.Segments)
	}

	e.mu.Lock()
	e.historicalWorkers = map[segmentTaskKey]historicalAcquisitionActor{key: {actorID: "local:historical:H1:1", workerID: "H1"}}
	e.mu.Unlock()
	view, err = manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actors) != 1 || view.Actors[0].AcquisitionClass != "historical" || view.Actors[0].WorkerID != "H1" {
		t.Fatalf("historical recovery actor was not distinguished: %#v", view.Actors)
	}

	commitLivePresentationSegment(t, manager, e, archiveindex.ClaimHistorical, 0, 0, observedSequence, "")
	scheduler.mu.Lock()
	delete(scheduler.tasks, key)
	scheduler.mu.Unlock()
	e.mu.Lock()
	delete(e.historicalWorkers, key)
	e.mu.Unlock()
	view, err = manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actors) != 0 || len(view.Segments) != 2 || view.Segments[1].State != "present" {
		t.Fatalf("released worker or recovered media was projected incorrectly: actors=%#v segments=%#v", view.Actors, view.Segments)
	}
}

func TestAcquisitionFlowBoundsPagesAndDoesNotInventManifestFuture(t *testing.T) {
	manager, e, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)
	first := commitLivePresentationSegment(t, manager, e, archiveindex.ClaimLiveOrigin, 0, 0, 700, "")
	e.mu.Lock()
	track := e.recording.Tracks["main"]
	track.LivePresentation.HasEpochMapping = true
	track.LivePresentation.MappedSourceEpoch = 0
	track.LivePresentation.SourceSequenceBase = 700
	track.LivePresentation.PresentationOrdinalBase = first.LivePresentationOrdinal
	track.LivePresentation.MaxSourceSequence = 700
	track.LivePresentation.LastSequence = 700
	track.LivePresentation.NextOrdinal = 20_001
	e.mu.Unlock()
	snapshot, err := manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Segments) != 0 {
		t.Fatalf("a long archive should return only its latest bounded page, got %d cells", len(snapshot.Segments))
	}
	if snapshot.TotalOrdinals != 20_000 || snapshot.StartOrdinal != 19_905 {
		t.Fatalf("bounded page coordinates are incorrect: start=%d total=%d", snapshot.StartOrdinal, snapshot.TotalOrdinals)
	}
	if snapshot.Window != nil {
		t.Fatalf("archive ordinal bounds must not be presented as a manifest window: %#v", snapshot.Window)
	}
}

func TestAcquisitionFlowRejectsUnboundedLimit(t *testing.T) {
	manager, e, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)
	if _, err := manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, MaxAcquisitionFlowLimit+1); err != ErrAcquisitionFlowLimit {
		t.Fatalf("limit error=%v, want %v", err, ErrAcquisitionFlowLimit)
	}
}

func TestShardedAcquisitionFlowProjectsGapThenHistoricalRecovery(t *testing.T) {
	manager, e, closeManager := newShardedLivePresentationFixture(t)
	var scheduler *segmentScheduler
	defer func() {
		if scheduler != nil {
			e.mu.Lock()
			if e.scheduler == scheduler {
				e.scheduler = nil
			}
			e.mu.Unlock()
			scheduler.cancel()
		}
		closeManager(t)
	}()
	for sequence := uint64(100); sequence <= 102; sequence++ {
		commitLivePresentationSegment(t, manager, e, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	ordinal, discontinuitySequence, boundary, ok := livePresentationForCoordinate(e.recording.Tracks["main"], 0, 0, 103, false)
	if !ok || ordinal != 4 {
		t.Fatalf("expected stable slot for missing live sequence 103, got ordinal=%d ok=%t", ordinal, ok)
	}
	gap := domain.Gap{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0, FromSequence: 103, ToSequence: 103, DetectedAt: time.Now().UTC(), Reason: "fixture gap", LivePresentationOrdinal: ordinal, LiveDiscontinuity: boundary, LiveDiscontinuitySequence: discontinuitySequence, LiveDuration: 2}
	if err := manager.store.AppendShardedGap(context.Background(), e.recording.ID, gap); err != nil {
		t.Fatal(err)
	}
	if err := manager.refreshShardedHeader(e.recording); err != nil {
		t.Fatal(err)
	}
	if err := manager.loadShardedRuntimeTail(e.recording); err != nil {
		t.Fatal(err)
	}
	key := segmentTaskKey{epoch: 0, discontinuitySequence: 0, sequence: 103}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler = &segmentScheduler{manager: manager, e: e, ctx: ctx, cancel: cancel, changed: make(chan struct{}), tasks: map[segmentTaskKey]*segmentTask{}, init: make(map[string]*initFlight)}
	scheduler.latestManifestWindow = &liveManifestWindow{
		trackID: "main", epoch: 0, discontinuitySequence: 0, firstSequence: 101, lastSequence: 102,
		firstOrdinal: 2, lastOrdinal: 3, observedAt: time.Now().UTC(), coordinates: []liveManifestCoordinate{
			{key: segmentTaskKey{epoch: 0, discontinuitySequence: 0, sequence: 101}, presentationOrdinal: 2, duration: 2 * time.Second},
			{key: segmentTaskKey{epoch: 0, discontinuitySequence: 0, sequence: 102}, presentationOrdinal: 3, duration: 2 * time.Second},
		},
	}
	e.mu.Lock()
	e.scheduler = scheduler
	e.historicalWorkers = map[segmentTaskKey]historicalAcquisitionActor{key: {actorID: "local:historical:H1:1", workerID: "H1"}}
	e.mu.Unlock()
	if page, pageErr := manager.store.ReadShardedLiveSlotPage(context.Background(), e.recording.ID, "main", 0, 96); pageErr != nil {
		t.Fatalf("read live slots: %v; header=%#v", pageErr, page)
	}
	view, err := manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 4 || view.Segments[3].State != "unresolved" || !view.Segments[3].Observed {
		t.Fatalf("current unresolved gap was not red/present in the projection: %#v", view.Segments)
	}
	if view.Window == nil || view.Window.LastOrdinal != 3 || len(view.Actors) != 1 || view.Actors[0].WorkerID != "H1" || view.Actors[0].PresentationOrdinal != 4 {
		t.Fatalf("historical actor/window relationship is incorrect: window=%#v actors=%#v", view.Window, view.Actors)
	}
	commitLivePresentationSegment(t, manager, e, archiveindex.ClaimHistorical, 0, 0, 103, "")
	e.mu.Lock()
	delete(e.historicalWorkers, key)
	e.mu.Unlock()
	view, err = manager.AcquisitionFlowSnapshot(context.Background(), e.recording.ID, 0, 96)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Actors) != 0 || len(view.Segments) != 4 || view.Segments[3].State != "present" || !view.Segments[3].Recovered {
		t.Fatalf("historical recovery did not transition unresolved to present: actors=%#v segments=%#v", view.Actors, view.Segments)
	}
	var rawGapCount int
	if err := manager.store.IterateShardedGaps(context.Background(), e.recording.ID, func(domain.Gap) error {
		rawGapCount++
		return nil
	}); err != nil {
		t.Fatalf("read raw gap history after recovery: %v", err)
	}
	if rawGapCount != 1 {
		t.Fatalf("historical recovery rewrote immutable gap observations: count=%d", rawGapCount)
	}
	if view.TotalOrdinals != 4 {
		t.Fatalf("projection invented a coordinate beyond the observed archive/window: total=%d", view.TotalOrdinals)
	}
}
