package acquire

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestArchiveRevisionTracksObjectAndClaimChanges(t *testing.T) {
	manager, entry, closeManager := newLivePresentationFixture(t)
	defer closeManager(t)

	commit := func(source archiveindex.ClaimSource, sequence uint64, isInit bool, payload string) {
		t.Helper()
		segment := domain.Segment{
			TrackID: "main", Sequence: sequence, Duration: 2, IsInit: isInit,
		}
		if isInit {
			segment.ID = fmt.Sprintf("init-%d", sequence)
		}
		if _, _, err := manager.commitArchiveSegment(entry, nil, segment, source, []byte(payload)); err != nil {
			t.Fatalf("commit %s coordinate %d: %v", source, sequence, err)
		}
	}
	revision := func() uint64 {
		t.Helper()
		root, err := manager.Get(entry.recording.ID)
		if err != nil {
			t.Fatal(err)
		}
		return root.ArchiveRevision
	}

	commit(archiveindex.ClaimLiveOrigin, 1, true, "init-bytes")
	if got := revision(); got != 1 {
		t.Fatalf("init object archive revision = %d, want 1", got)
	}
	commit(archiveindex.ClaimLiveOrigin, 2, false, "media-bytes")
	if got := revision(); got != 2 {
		t.Fatalf("media object archive revision = %d, want 2", got)
	}
	commit(archiveindex.ClaimHistorical, 2, false, "media-bytes")
	if got := revision(); got != 3 {
		t.Fatalf("identical second claim archive revision = %d, want 3", got)
	}
	commit(archiveindex.ClaimPeer, 2, false, "alternate-bytes")
	if got := revision(); got != 4 {
		t.Fatalf("conflicting claim archive revision = %d, want 4", got)
	}
}

func TestAdvanceArchiveRevisionStartsLegacyAndRejectsOverflow(t *testing.T) {
	recording := &domain.Recording{}
	if err := advanceArchiveRevision(recording); err != nil || recording.ArchiveRevision != 1 {
		t.Fatalf("legacy revision advance = %d, err=%v", recording.ArchiveRevision, err)
	}
	recording.ArchiveRevision = ^uint64(0)
	if err := advanceArchiveRevision(recording); err != ErrArchiveRevisionOverflow {
		t.Fatalf("overflow error = %v, want %v", err, ErrArchiveRevisionOverflow)
	}
}

func TestLegacyKnownMissingRevisionAndReobservation(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := newRepairTestRoot(t, store, domain.StateCompleted, repairMediaContextForTest())
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{}}
	owner := ownerForRepair(root.ID, 4)
	fence := &repairOwnerFence{current: &owner, last: owner.Epoch}
	manager, entry, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	defer func() {
		if err := manager.releaseRepairOwner(context.Background(), entry, owner); err != nil {
			t.Errorf("release repair owner: %v", err)
		}
		if err := closeManager(); err != nil {
			t.Errorf("close manager: %v", err)
		}
	}()

	identity, err := archiveSessionIdentity(entry.recording, entry.recording.AdapterID, "")
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{
		SessionID: identity.ID, TrackID: "main", SourceEpoch: 0,
		DiscontinuitySequence: 7, Sequence: 100, Kind: archiveindex.ObjectMedia,
	}
	initialTimelineRevision := entry.recording.TimelineRevision
	if err := manager.recordHistoricalCoverage(entry, &owner, true, coordinate, archiveindex.CoverageKnownMissing, "declared missing"); err != nil {
		t.Fatalf("record first known_missing: %v", err)
	}
	if entry.recording.ArchiveRevision != 1 || entry.recording.TimelineRevision != initialTimelineRevision {
		t.Fatalf("first V1 known_missing revisions archive=%d timeline=%d, want archive 1 / timeline %d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision, initialTimelineRevision)
	}
	if err := manager.recordHistoricalCoverage(entry, &owner, true, coordinate, archiveindex.CoverageKnownMissing, "same missing coordinate"); err != nil {
		t.Fatalf("reobserve known_missing: %v", err)
	}
	if entry.recording.ArchiveRevision != 1 || entry.recording.TimelineRevision != initialTimelineRevision {
		t.Fatalf("identical V1 known_missing reobservation changed revisions: archive=%d timeline=%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision)
	}
	coordinate.Sequence++
	if err := manager.recordHistoricalCoverage(entry, &owner, true, coordinate, archiveindex.CoverageAcquisitionFailed, "temporary acquisition failure"); err != nil {
		t.Fatalf("record acquisition failure: %v", err)
	}
	if entry.recording.ArchiveRevision != 1 || entry.recording.TimelineRevision != initialTimelineRevision {
		t.Fatalf("ephemeral acquisition failure changed revisions: archive=%d timeline=%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision)
	}
}

func TestLegacyLiveGapChangesArchiveAndPlaybackRevisionOnce(t *testing.T) {
	base := &domain.Recording{
		FormatVersion: 1, ArchiveRevision: 4, TimelineRevision: 9,
		Tracks: map[string]*domain.Track{"main": {
			ID: "main", LivePresentation: &domain.LivePresentationState{NextOrdinal: 3, FirstPresentationOrdinal: 1},
		}},
	}
	next := clone(base)
	next.Gaps = append(next.Gaps, domain.Gap{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0,
		FromSequence: 50, ToSequence: 50, LivePresentationOrdinal: 2,
		LiveDuration: 2, Reason: "source declared gap",
	})
	if err := applyLegacyGapRevisions(base, next); err != nil {
		t.Fatal(err)
	}
	if next.ArchiveRevision != 5 || next.TimelineRevision != 10 {
		t.Fatalf("new V1 live gap revisions archive=%d timeline=%d, want 5/10", next.ArchiveRevision, next.TimelineRevision)
	}
	retry := clone(next)
	if err := applyLegacyGapRevisions(next, retry); err != nil {
		t.Fatal(err)
	}
	if retry.ArchiveRevision != next.ArchiveRevision || retry.TimelineRevision != next.TimelineRevision {
		t.Fatalf("identical V1 live gap reobservation changed revisions archive=%d timeline=%d", retry.ArchiveRevision, retry.TimelineRevision)
	}
}

func TestShardedHistoricalMissingPrefixAndRepairKeepLiveIdentity(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	for sequence := uint64(100); sequence <= 101; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	before, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	identities := livePresentationIdentityMap(before.Segments)
	entry.mu.Lock()
	archiveRevision, timelineRevision := entry.recording.ArchiveRevision, entry.recording.TimelineRevision
	entry.mu.Unlock()
	coordinate := archiveindex.Coordinate{
		SessionID: entry.recording.SourceSessionID, TrackID: "main", SourceEpoch: 0,
		DiscontinuitySequence: 0, Sequence: 90, Kind: archiveindex.ObjectMedia,
	}
	if err := manager.recordHistoricalCoverage(entry, nil, false, coordinate, archiveindex.CoverageKnownMissing, "historical candidate absent"); err != nil {
		t.Fatalf("record V2 historical known_missing: %v", err)
	}
	if entry.recording.ArchiveRevision != archiveRevision+1 || entry.recording.TimelineRevision != timelineRevision {
		t.Fatalf("historical known_missing revisions archive=%d timeline=%d, want archive=%d timeline=%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision, archiveRevision+1, timelineRevision)
	}
	if err := manager.recordHistoricalCoverage(entry, nil, false, coordinate, archiveindex.CoverageKnownMissing, "historical candidate still absent"); err != nil {
		t.Fatalf("reobserve V2 historical known_missing: %v", err)
	}
	if entry.recording.ArchiveRevision != archiveRevision+1 || entry.recording.TimelineRevision != timelineRevision {
		t.Fatalf("identical V2 historical missing reobservation changed revisions archive=%d timeline=%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision)
	}
	if err := manager.recordHistoricalCoverage(entry, nil, false, coordinate, archiveindex.CoverageAcquisitionFailed, "temporary retry state"); err != nil {
		t.Fatalf("record V2 acquisition failure: %v", err)
	}
	if entry.recording.ArchiveRevision != archiveRevision+1 || entry.recording.TimelineRevision != timelineRevision {
		t.Fatalf("ephemeral V2 acquisition failure changed revisions archive=%d timeline=%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision)
	}
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, 90, "")
	if entry.recording.ArchiveRevision <= archiveRevision+1 || entry.recording.TimelineRevision <= timelineRevision {
		t.Fatalf("repair did not revise archive/playback: archive=%d timeline=%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision)
	}
	after, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	for id, ordinal := range identities {
		if current, ok := livePresentationIdentityMap(after.Segments)[id]; !ok || current != ordinal {
			t.Fatalf("historical prefix changed live identity %s: got %d/%v, want %d", id, current, ok, ordinal)
		}
	}
}

func TestV2SealDoesNotChangeArchiveFreshnessRevision(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	if _, _, err := manager.commitArchiveSegment(entry, nil, domain.Segment{
		TrackID: "main", Sequence: 1, SourceURI: "https://source.invalid/one.ts", Duration: 2,
	}, archiveindex.ClaimLiveOrigin, []byte("immutable-segment")); err != nil {
		t.Fatal(err)
	}
	entry.mu.Lock()
	entry.recording.State = domain.StateStopped
	now := time.Now().UTC()
	entry.recording.StoppedAt = &now
	beforeArchive, beforeTimeline := entry.recording.ArchiveRevision, entry.recording.TimelineRevision
	stopped := clone(entry.recording)
	entry.mu.Unlock()
	if err := manager.saveRecordingHeader(stopped); err != nil {
		t.Fatalf("persist stopped capture state: %v", err)
	}
	if err := manager.SealArchiveContext(context.Background(), entry.recording.ID); err != nil {
		t.Fatalf("seal V2 archive: %v", err)
	}
	if !entry.recording.ArchiveSealed {
		t.Fatal("seal did not persist archive state")
	}
	if entry.recording.ArchiveRevision != beforeArchive || entry.recording.TimelineRevision != beforeTimeline {
		t.Fatalf("seal changed content freshness: archive=%d timeline=%d, want %d/%d", entry.recording.ArchiveRevision, entry.recording.TimelineRevision, beforeArchive, beforeTimeline)
	}
}

func TestLegacyManifestAppendAdvancesArchiveRevisionOnlyForNewPath(t *testing.T) {
	root := &domain.Recording{FormatVersion: 1}
	now := time.Now().UTC()
	first := domain.ManifestSnapshot{StoragePath: "manifests/main/one.m3u8", FetchedAt: now}
	changed, err := appendLegacyManifestSnapshot(root, first)
	if err != nil || !changed || root.ArchiveRevision != 1 || len(root.Snapshots) != 1 {
		t.Fatalf("first legacy manifest changed=%v revision=%d count=%d err=%v", changed, root.ArchiveRevision, len(root.Snapshots), err)
	}
	changed, err = appendLegacyManifestSnapshot(root, first)
	if err != nil || changed || root.ArchiveRevision != 1 || len(root.Snapshots) != 1 {
		t.Fatalf("duplicate legacy manifest changed=%v revision=%d count=%d err=%v", changed, root.ArchiveRevision, len(root.Snapshots), err)
	}
}

func TestArchiveRevisionClaimRootFailureReconcilesOnceAfterRestart(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	cause := errors.New("injected archive revision root failure")
	backend := &canonicalCommitFaultBackend{StorageBackend: store.StorageBackend, cause: cause}
	store.StorageBackend = backend
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{}}
	fence := &repairOwnerFence{}
	manager, entry, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	segment := domain.Segment{
		TrackID: "main", Sequence: 31, SourceEpoch: 0, DiscontinuitySequence: 7,
		SourceURI: "https://media.example/archive/revision-retry.ts", Duration: 3,
	}
	data := []byte("same immutable bytes")
	owner := ownerForRepair(root.ID, 1)
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, segment, archiveindex.ClaimLiveOrigin, data, true); err != nil {
		t.Fatalf("initial selected object commit: %v", err)
	}
	selected, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if selected.ArchiveRevision != 1 {
		t.Fatalf("initial archive revision=%d, want 1", selected.ArchiveRevision)
	}

	backend.target = "root"
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, segment, archiveindex.ClaimHistorical, data, true); !errors.Is(err, cause) {
		t.Fatalf("historical duplicate error=%v, want injected root failure", err)
	}
	failedRoot, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failedRoot.ArchiveRevision != 1 {
		t.Fatalf("failed root write changed in-memory revision to %d, want 1", failedRoot.ArchiveRevision)
	}
	identity, err := archiveSessionIdentity(failedRoot, failedRoot.AdapterID, "")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := (&Manager{store: store}).loadArchiveIndexManifest(root.ID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if intent := manifest.PendingRevision; intent == nil || intent.Revision != 2 || intent.ClaimID == "" || intent.Coordinate.Sequence != segment.Sequence {
		t.Fatalf("pending revision intent=%+v, want selected claim revision 2", intent)
	}
	if err := manager.releaseRepairOwner(context.Background(), entry, owner); err != nil {
		t.Fatal(err)
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	manager2, entry2, closeManager2 := newRepairTestManager(t, store, fixture, loaded, &repairOwnerFence{last: owner.Epoch}, nil)
	owner2 := ownerForRepair(root.ID, 2)
	if _, _, err := manager2.commitArchiveSegmentOwned(entry2, &owner2, segment, archiveindex.ClaimHistorical, data, true); err != nil {
		t.Fatalf("same claim retry after restart: %v", err)
	}
	reconciled, err := manager2.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.ArchiveRevision != 2 {
		t.Fatalf("reconciled archive revision=%d, want exactly 2", reconciled.ArchiveRevision)
	}
	inventory, err := manager2.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	var claims int
	for _, item := range inventory.Segments {
		if item.Coordinate.Sequence == segment.Sequence {
			claims = len(item.Claims)
		}
	}
	if claims != 2 {
		t.Fatalf("durable claims=%d, want selected live plus historical duplicate", claims)
	}
	if err := manager2.releaseRepairOwner(context.Background(), entry2, owner2); err != nil {
		t.Fatal(err)
	}
	if err := closeManager2(); err != nil {
		t.Fatal(err)
	}

	final, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.ArchiveRevision != 2 {
		t.Fatalf("persisted archive revision=%d, want 2 after cold reload", final.ArchiveRevision)
	}
}
