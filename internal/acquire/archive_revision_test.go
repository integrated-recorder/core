package acquire

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
