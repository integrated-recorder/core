package acquire

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestLivePresentationIdentitySurvivesHistoricalPrefixGapAndWindowSlide(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)

	for sequence := uint64(100); sequence <= 105; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	// Leave one coordinate absent to represent a recoverable middle gap.
	for sequence := uint64(107); sequence <= 113; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	track := entry.recording.Tracks["main"]
	gapOrdinal, gapDiscSequence, gapBoundary, ok := livePresentationForCoordinate(track, 0, 0, 106, false)
	if !ok {
		t.Fatal("missing live coordinate has no stable presentation position")
	}
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 0, FromSequence: 106, ToSequence: 106,
		DetectedAt: time.Now().UTC(), Reason: "fixture gap",
		LivePresentationOrdinal: gapOrdinal, LiveDiscontinuity: gapBoundary,
		LiveDiscontinuitySequence: gapDiscSequence, LiveDuration: 2,
	}
	entry.recording.Gaps = append(entry.recording.Gaps, gap)
	manager.updateLivePlaybackGap(entry, gap)

	before, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Slots) != 12 || len(before.Segments) != 11 {
		t.Fatalf("live tail has %d slots and %d media, want 12 slots with one gap", len(before.Slots), len(before.Segments))
	}
	beforeIdentity := livePresentationIdentityMap(before.Segments)
	firstRevision := before.TimelineRevision

	// Historical coordinates include a late prefix and the missing middle
	// coordinate. They change the VOD projection but do not join the live tail.
	for sequence := uint64(90); sequence <= 99; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, sequence, "")
	}
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, 106, "")

	afterRepair, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterRepair.TimelineRevision <= firstRevision {
		t.Fatalf("historical repair did not revise VOD projection: revision %d <= %d", afterRepair.TimelineRevision, firstRevision)
	}
	got := livePresentationIdentityMap(afterRepair.Segments)
	for id, ordinal := range beforeIdentity {
		if current, exists := got[id]; exists && current != ordinal {
			t.Fatalf("historical repair changed live URI %s identity %d -> %d", id, ordinal, current)
		}
	}
	for _, segment := range afterRepair.Segments {
		if segment.Sequence < 100 || segment.Sequence > 113 {
			t.Fatalf("historical coordinate leaked into live presentation: %#v", segment)
		}
	}

	newSegment := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, 114, "")
	if newSegment.LiveDiscontinuity || newSegment.LivePresentationOrdinal != 15 {
		t.Fatalf("next live segment identity = %#v, want ordinal 15 without a discontinuity", newSegment)
	}
	afterSlide, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterSlide.Segments) != LivePlaybackWindowSize {
		t.Fatalf("slid live tail length=%d, want %d", len(afterSlide.Segments), LivePlaybackWindowSize)
	}
	for _, segment := range afterSlide.Segments {
		if sequence, existed := beforeIdentity[segment.ID]; existed && segment.LivePresentationOrdinal != sequence {
			t.Errorf("overlap segment %s changed identity %d -> %d", segment.ID, sequence, segment.LivePresentationOrdinal)
		}
	}
	last := afterSlide.Segments[len(afterSlide.Segments)-1]
	if last.Sequence != 114 || last.LivePresentationOrdinal != 15 {
		t.Fatalf("next live presentation = sequence %d / ordinal %d, want 114 / 15", last.Sequence, last.LivePresentationOrdinal)
	}
	if afterSlide.Segments[0].LivePresentationOrdinal != 4 {
		t.Fatalf("sliding window first ordinal=%d, want 4", afterSlide.Segments[0].LivePresentationOrdinal)
	}
	root, err := manager.Get(entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if segmentForCoordinate(root.Tracks["main"].Segments, 0, 0, 90).LivePresentationOrdinal != 0 {
		t.Fatal("historical prefix received live presentation identity")
	}
	if repaired := segmentForCoordinate(root.Tracks["main"].Segments, 0, 0, 106); repaired.LivePresentationOrdinal != gapOrdinal {
		t.Fatalf("repaired live gap lost its stable presentation identity: %#v", repaired)
	}
}

func TestLivePresentationSourceResetAndDiscontinuityStayMonotonic(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 2, 4, 500, "")
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 2, 4, 501, "")
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 2, 5, 502, "", true)
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 3, 0, 0, "")
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 3, 0, 1, "")

	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 5 {
		t.Fatalf("segments=%d, want 5", len(view.Segments))
	}
	for index, segment := range view.Segments {
		if segment.LivePresentationOrdinal != uint64(index+1) {
			t.Errorf("presentation ordinal[%d]=%d, want %d", index, segment.LivePresentationOrdinal, index+1)
		}
	}
	if !view.Segments[2].LiveDiscontinuity || !view.Segments[3].LiveDiscontinuity {
		t.Fatalf("source discontinuity/reset not recorded: %#v", view.Segments)
	}
	if view.Segments[4].LiveDiscontinuity {
		t.Fatalf("source discontinuity repeated after its boundary segment: %#v", view.Segments)
	}
	if view.Segments[0].LiveDiscontinuitySequence != 0 ||
		view.Segments[1].LiveDiscontinuitySequence != 1 ||
		view.Segments[2].LiveDiscontinuitySequence != 1 ||
		view.Segments[3].LiveDiscontinuitySequence != 2 ||
		view.Segments[4].LiveDiscontinuitySequence != 3 {
		t.Fatalf("discontinuity sequence did not advance at boundaries: %#v", view.Segments)
	}
}

func TestLivePlaybackSnapshotBoundsLongHistoryAndCopiesInitByteRange(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)
	track := entry.recording.Tracks["main"]
	track.Segments = make([]domain.Segment, 60_000)
	for index := range track.Segments {
		track.Segments[index] = domain.Segment{
			ID: fmt.Sprintf("seg-%d", index), TrackID: "main", Sequence: uint64(index),
			ArchiveOrdinal: uint64(index + 1), TimelineOrdinal: uint64(index + 1),
			StoragePath: fmt.Sprintf("objects/%d", index), PayloadSize: 1,
		}
		if index >= 60_000-LivePlaybackWindowSize {
			track.Segments[index].LivePresentationOrdinal = uint64(index + 1)
			track.Segments[index].InitSegmentID = "init-current"
		}
	}
	track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 60_001, FirstPresentationOrdinal: 1}
	byteRange := domain.ByteRange{Length: 50, Offset: 25}
	track.InitSegments = []domain.Segment{{ID: "init-current", TrackID: "main", IsInit: true, ByteRange: &byteRange}}
	entry.livePlayback = buildLivePlaybackProjection(entry.recording)

	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != LivePlaybackWindowSize || len(view.InitSegments) != 1 {
		t.Fatalf("snapshot returned %d media and %d init objects", len(view.Segments), len(view.InitSegments))
	}
	if view.Segments[0].LivePresentationOrdinal != 60_000-LivePlaybackWindowSize+1 {
		t.Fatalf("bounded snapshot starts at ordinal %d", view.Segments[0].LivePresentationOrdinal)
	}
	copyRange := view.InitSegments["init-current"].ByteRange
	if copyRange == nil || *copyRange != byteRange {
		t.Fatalf("init byte range was not copied: %#v", copyRange)
	}
	copyRange.Length = 1
	if *track.InitSegments[0].ByteRange != byteRange {
		t.Fatal("snapshot byte-range mutation escaped into canonical root")
	}
}

func TestShardedLiveSnapshotUsesBoundedCacheWithoutWriterLockOrStorageRead(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	for sequence := uint64(1); sequence <= 4; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}

	backend := manager.store.StorageBackend
	manager.store.StorageBackend = failingShardedRootReadBackend{StorageBackend: backend, failure: errors.New("unexpected archive root read")}
	entry.persistMu.Lock()
	type result struct {
		view LivePlaybackView
		err  error
	}
	done := make(chan result, 1)
	go func() {
		view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
		done <- result{view: view, err: err}
	}()
	var snapshot result
	select {
	case snapshot = <-done:
		entry.persistMu.Unlock()
	case <-time.After(2 * time.Second):
		entry.persistMu.Unlock()
		manager.store.StorageBackend = backend
		t.Fatal("V2 live snapshot waited for canonical writer lock")
	}
	manager.store.StorageBackend = backend
	if snapshot.err != nil {
		t.Fatalf("V2 live snapshot read archive storage: %v", snapshot.err)
	}
	if len(snapshot.view.Segments) != 4 || snapshot.view.Segments[3].LivePresentationOrdinal != 4 {
		t.Fatalf("V2 live snapshot returned stale or incomplete tail: %#v", snapshot.view)
	}
}

func TestStaleLiveGapObservationCannotReplaceRepairedMedia(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	gap := domain.Gap{
		TrackID: "main", FromSequence: 1, ToSequence: 1, Reason: "source manifest marked segment as a gap",
		DetectedAt: time.Now().UTC(), LivePresentationOrdinal: 1, LiveDuration: 2,
	}
	if err := manager.update(entry, func(recording *domain.Recording) error {
		recording.Gaps = append(recording.Gaps, gap)
		return nil
	}); err != nil {
		t.Fatalf("persist source gap: %v", err)
	}
	manager.updateLivePlaybackGap(entry, gap)
	segment := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, 1, "")
	if segment.ID == "" || segment.LivePresentationOrdinal != 1 {
		t.Fatalf("historical repair did not fill the reserved presentation slot: %#v", segment)
	}
	// The live worker can finish processing its old manifest observation after
	// the historical commit. Replaying that stale callback must not put the gap
	// back into either the current root projection or the HLS live cache.
	manager.updateLivePlaybackGap(entry, gap)
	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatalf("read live snapshot after stale gap callback: %v", err)
	}
	if len(view.Slots) != 1 || view.Slots[0].Ordinal != 1 || view.Slots[0].Segment == nil || view.Slots[0].Unavailable {
		t.Fatalf("stale gap replaced canonical repaired media: %#v", view.Slots)
	}
}

func TestShardedLiveSnapshotReconcilesOnlyUnavailableSlotsFromDurableTail(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	segment := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, 1, "")
	entry.mu.Lock()
	entry.livePlayback.slots[segment.LivePresentationOrdinal] = LivePlaybackSlot{
		Ordinal: segment.LivePresentationOrdinal, Unavailable: true, Duration: segment.Duration,
	}
	entry.livePlayback.checkedUnavailable[segment.LivePresentationOrdinal] = true
	entry.mu.Unlock()
	if !manager.beginShardedLiveSlotPromotion(entry, segment) {
		t.Fatal("known unavailable live slot did not enter promotion reconciliation")
	}
	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatalf("reconcile live snapshot: %v", err)
	}
	if len(view.Slots) != 1 || view.Slots[0].Segment == nil || view.Slots[0].Segment.ID != segment.ID || view.Slots[0].Unavailable {
		t.Fatalf("durable canonical media did not repair stale unavailable cache slot: %#v", view.Slots)
	}
}

type failingShardedRootReadBackend struct {
	storage.StorageBackend
	failure error
}

func (b failingShardedRootReadBackend) LoadRecordingContext(context.Context, string) (*domain.Recording, error) {
	return nil, b.failure
}

func TestLivePresentationIdentitySurvivesManagerRestart(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	for sequence := uint64(100); sequence <= 116; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	before, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeMapping := livePresentationIdentityMap(before.Segments)
	dataDir := manager.store.Root()
	closeManager(t)

	restartedStore, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewManager(restartedStore, &http.Client{}, nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if closeErr := restarted.Close(ctx); closeErr != nil {
			t.Errorf("close restarted manager: %v", closeErr)
		}
	})
	after, err := restarted.Get(entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterMapping := livePresentationIdentityMap(after.Tracks["main"].Segments)
	for id, ordinal := range beforeMapping {
		if got := afterMapping[id]; got != ordinal {
			t.Fatalf("manager restart changed live URI identity %s: before=%d after=%d (all=%v)", id, ordinal, got, afterMapping)
		}
	}
	if state := after.Tracks["main"].LivePresentation; state == nil || state.NextOrdinal != 18 {
		t.Fatalf("manager restart lost the persisted presentation high-water mark: %#v", state)
	}
}

func TestLegacyLivePresentationMigrationKeepsWindowOverlapOnNextCapture(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)
	for sequence := uint64(100); sequence <= 113; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	entry.mu.Lock()
	track := entry.recording.Tracks["main"]
	track.Segments[0].Discontinuity = true // This boundary has scrolled out of the legacy 12-segment window.
	for i := range track.Segments {
		track.Segments[i].TimelineOrdinal = uint64(i + 1)
		track.Segments[i].LivePresentationOrdinal = 0
		track.Segments[i].LiveDiscontinuity = false
		track.Segments[i].LiveDiscontinuitySequence = 0
	}
	track.LivePresentation = nil
	migrated, err := migrateLivePresentation(entry.recording)
	if err != nil || !migrated {
		entry.mu.Unlock()
		t.Fatalf("migrate legacy live identity: migrated=%t err=%v", migrated, err)
	}
	entry.livePlayback = buildLivePlaybackProjection(entry.recording)
	if err := manager.store.SaveRecording(entry.recording); err != nil {
		entry.mu.Unlock()
		t.Fatalf("persist legacy live identity migration: %v", err)
	}
	entry.mu.Unlock()
	before, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil || len(before.Segments) != LivePlaybackWindowSize {
		t.Fatalf("migrated live window: segments=%d err=%v", len(before.Segments), err)
	}
	if before.Segments[0].LiveDiscontinuitySequence != 1 {
		t.Fatalf("legacy migration lost discontinuities before the live window: %#v", before.Segments[0])
	}
	beforeMapping := livePresentationIdentityMap(before.Segments)
	if entry.recording.Tracks["main"].LivePresentation.FirstPresentationOrdinal != 3 {
		t.Fatalf("legacy first live ordinal was not persisted: %#v", entry.recording.Tracks["main"].LivePresentation)
	}
	newSegment := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, 114, "")
	if newSegment.LiveDiscontinuity || newSegment.LivePresentationOrdinal != 15 {
		t.Fatalf("capture after legacy migration started a false epoch: %#v", newSegment)
	}
	after, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil || len(after.Segments) != LivePlaybackWindowSize {
		t.Fatalf("live window after migrated capture: segments=%d err=%v", len(after.Segments), err)
	}
	afterMapping := livePresentationIdentityMap(after.Segments)
	for id, ordinal := range beforeMapping {
		if current, exists := afterMapping[id]; exists && current != ordinal {
			t.Fatalf("legacy overlap URI %s identity changed: %d -> %d", id, ordinal, current)
		}
	}
}

func TestLivePresentationRetainsInitMapAndByteRangeReferences(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)
	firstRange := domain.ByteRange{Length: 100, Offset: 0}
	secondRange := domain.ByteRange{Length: 100, Offset: 100}
	for _, init := range []domain.Segment{
		{ID: "init-a", TrackID: "main", SourceEpoch: 0, Sequence: 100, IsInit: true, SourceURI: "https://source.invalid/init.mp4", ByteRange: &firstRange},
		{ID: "init-b", TrackID: "main", SourceEpoch: 0, Sequence: 101, IsInit: true, SourceURI: "https://source.invalid/init.mp4", ByteRange: &secondRange},
	} {
		if _, _, err := manager.commitArchiveSegment(entry, nil, init, archiveindex.ClaimLiveOrigin, []byte("init-bytes-"+init.ID)); err != nil {
			t.Fatalf("commit init map %s: %v", init.ID, err)
		}
	}
	first := domain.Segment{TrackID: "main", SourceEpoch: 0, Sequence: 100, SourceURI: "https://source.invalid/media.bin", Duration: 1, InitSegmentID: "init-a", ByteRange: &firstRange}
	second := domain.Segment{TrackID: "main", SourceEpoch: 0, Sequence: 101, SourceURI: "https://source.invalid/media.bin", Duration: 1, InitSegmentID: "init-b", ByteRange: &secondRange}
	for _, segment := range []domain.Segment{first, second} {
		if _, _, err := manager.commitArchiveSegment(entry, nil, segment, archiveindex.ClaimLiveOrigin, []byte(fmt.Sprintf("media-%d", segment.Sequence))); err != nil {
			t.Fatalf("commit ranged media %d: %v", segment.Sequence, err)
		}
	}

	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 2 || view.Segments[0].InitSegmentID != "init-a" || view.Segments[1].InitSegmentID != "init-b" {
		t.Fatalf("live map relationships changed: %#v", view.Segments)
	}
	if len(view.InitSegments) != 2 {
		t.Fatalf("snapshot init map count=%d, want 2", len(view.InitSegments))
	}
	if view.InitSegments["init-a"].ByteRange == nil || *view.InitSegments["init-a"].ByteRange != firstRange || view.InitSegments["init-b"].ByteRange == nil || *view.InitSegments["init-b"].ByteRange != secondRange {
		t.Fatalf("init byte ranges changed: %#v", view.InitSegments)
	}
	if view.Segments[0].ByteRange == nil || *view.Segments[0].ByteRange != firstRange || view.Segments[1].ByteRange == nil || *view.Segments[1].ByteRange != secondRange {
		t.Fatalf("media byte ranges changed: %#v", view.Segments)
	}
}

func newLivePresentationFixture(t *testing.T) (*Manager, *entry, func(*testing.T)) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, &http.Client{}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	root := &domain.Recording{
		FormatVersion: 1, ID: "84e2f247ab144c6a850fd5c07fb1e6a1", Title: "live identity fixture",
		AdapterID: "fixture", State: domain.StateRecording, CreatedAt: now, StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {
			ID: "main", SourcePlaylistURL: "https://source.invalid/live.m3u8", NextArchiveOrdinal: 1,
			LivePresentation: &domain.LivePresentationState{NextOrdinal: 1},
			Segments:         []domain.Segment{}, InitSegments: []domain.Segment{},
		}},
	}
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: root, done: closedChannel(), livePlayback: newLivePlaybackProjection()}
	manager.mu.Lock()
	manager.entries[root.ID] = e
	manager.mu.Unlock()
	closeManager := func(t *testing.T) {
		t.Helper()
		e.mu.Lock()
		if e.recording != nil {
			e.recording.State = domain.StateInterrupted
		}
		e.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close fixture manager: %v", err)
		}
	}
	return manager, e, closeManager
}

func commitLivePresentationSegment(t *testing.T, manager *Manager, entry *entry, source archiveindex.ClaimSource, epoch, discontinuity, sequence uint64, initID string, sourceDiscontinuity ...bool) domain.Segment {
	t.Helper()
	segment := domain.Segment{
		TrackID: "main", SourceEpoch: epoch, DiscontinuitySequence: discontinuity,
		Sequence: sequence, SourceURI: fmt.Sprintf("https://source.invalid/%d-%d-%d.ts", epoch, discontinuity, sequence),
		Duration: 2, InitSegmentID: initID,
	}
	if len(sourceDiscontinuity) != 0 {
		segment.Discontinuity = sourceDiscontinuity[0]
	}
	if _, _, err := manager.commitArchiveSegment(entry, nil, segment, source, []byte(fmt.Sprintf("payload-%d-%d-%d", epoch, discontinuity, sequence))); err != nil {
		t.Fatalf("commit %s coordinate %d/%d/%d: %v", source, epoch, discontinuity, sequence, err)
	}
	root, err := manager.Get(entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	return segmentForCoordinate(root.Tracks["main"].Segments, epoch, discontinuity, sequence)
}

func segmentForCoordinate(segments []domain.Segment, epoch, discontinuity, sequence uint64) domain.Segment {
	for _, segment := range segments {
		if segment.SourceEpoch == epoch && segment.DiscontinuitySequence == discontinuity && segment.Sequence == sequence {
			return segment
		}
	}
	return domain.Segment{}
}

func livePresentationIdentityMap(segments []domain.Segment) map[string]uint64 {
	identities := make(map[string]uint64, len(segments))
	for _, segment := range segments {
		identities[segment.ID] = segment.LivePresentationOrdinal
	}
	return identities
}

func samePresentationIdentity(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for id, sequence := range left {
		if right[id] != sequence {
			return false
		}
	}
	return true
}
