package archivecatalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
)

func TestAppendPersistsImmutableSnapshotChainAndIsIdempotent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	first := catalogFixtureSnapshot(t, 1, "", 1)
	if err := store.Append(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), first); err != nil {
		t.Fatalf("exact retry was not idempotent: %v", err)
	}
	gotFirst, err := store.Snapshot(context.Background(), first.Content.ArchiveID, first.SnapshotID)
	if err != nil || gotFirst.SnapshotID != first.SnapshotID {
		t.Fatalf("first snapshot=%+v err=%v", gotFirst, err)
	}
	second := catalogFixtureSnapshot(t, 2, first.SnapshotID, 2)
	if err := store.Append(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	head, err := store.Head(context.Background(), first.Content.ArchiveID)
	if err != nil || head.Sequence != 2 || head.SnapshotID != second.SnapshotID {
		t.Fatalf("head=%+v err=%v", head, err)
	}
	if err := archivesnapshot.VerifyChain([]archivesnapshot.Snapshot{gotFirst, head}); err != nil {
		t.Fatalf("persisted chain invalid: %v", err)
	}
	if err := store.Append(context.Background(), catalogFixtureSnapshot(t, 3, first.SnapshotID, 3)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale predecessor or sequence accepted: %v", err)
	}
}

func TestAppendRecoversSnapshotWrittenBeforeHeadCommit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := catalogFixtureSnapshot(t, 1, "", 1)
	encoded, err := archivesnapshot.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	archiveRoot := filepath.Join(store.root, snapshot.Content.ArchiveID)
	if err := ensurePrivateDir(filepath.Join(archiveRoot, snapshotDirName)); err != nil {
		t.Fatal(err)
	}
	if err := store.writeImmutableSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID, encoded); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), snapshot); err != nil {
		t.Fatalf("retry did not publish matching orphan snapshot: %v", err)
	}
	if _, err := store.Head(context.Background(), snapshot.Content.ArchiveID); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentExactAppendAcrossStoreInstances(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog")
	first, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := catalogFixtureSnapshot(t, 1, "", 1)
	const writers = 16
	var wait sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			store := first
			if i%2 == 1 {
				store = second
			}
			errs <- store.Append(context.Background(), snapshot)
		}(i)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWalkHistoryVerifiesCompleteChainNewestFirstAndHoldsHead(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	first := catalogFixtureSnapshot(t, 1, "", 1)
	second := catalogFixtureSnapshot(t, 2, first.SnapshotID, 2)
	third := catalogFixtureSnapshot(t, 3, second.SnapshotID, 3)
	for _, snapshot := range []archivesnapshot.Snapshot{first, second, third} {
		if err := store.Append(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
	}

	var visited []uint64
	header, err := store.WalkHistory(context.Background(), first.Content.ArchiveID, func(snapshot archivesnapshot.Snapshot) error {
		visited = append(visited, snapshot.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if header.Sequence != 3 || header.SnapshotID != third.SnapshotID {
		t.Fatalf("history head=%+v", header)
	}
	if len(visited) != 3 || visited[0] != 3 || visited[1] != 2 || visited[2] != 1 {
		t.Fatalf("history order=%v, want [3 2 1]", visited)
	}
}

func TestWalkHistoryFailsClosedOnMissingAncestorAfterPartialVisit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	first := catalogFixtureSnapshot(t, 1, "", 1)
	second := catalogFixtureSnapshot(t, 2, first.SnapshotID, 2)
	for _, snapshot := range []archivesnapshot.Snapshot{first, second} {
		if err := store.Append(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(store.root, first.Content.ArchiveID, snapshotDirName, first.SnapshotID+".json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	var visited []uint64
	_, err = store.WalkHistory(context.Background(), first.Content.ArchiveID, func(snapshot archivesnapshot.Snapshot) error {
		visited = append(visited, snapshot.Sequence)
		return nil
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing ancestor error=%v", err)
	}
	if len(visited) != 1 || visited[0] != 2 {
		t.Fatalf("partial visit=%v, want only newest snapshot [2]", visited)
	}
}

func TestWalkHistoryPropagatesVisitorFailureWithoutCompletion(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	first := catalogFixtureSnapshot(t, 1, "", 1)
	if err := store.Append(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("verification stopped")
	if _, err := store.WalkHistory(context.Background(), first.Content.ArchiveID, func(archivesnapshot.Snapshot) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("visitor error=%v", err)
	}
}

func TestCatalogRejectsWeakPermissionsAndTamperedFiles(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := catalogFixtureSnapshot(t, 1, "", 1)
	if err := store.Append(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, snapshot.Content.ArchiveID, headFileName)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(context.Background(), snapshot.Content.ArchiveID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("world-readable catalog head accepted: %v", err)
	}
}

func catalogFixtureSnapshot(t *testing.T, sequence uint64, previous string, mediaSequence uint64) archivesnapshot.Snapshot {
	t.Helper()
	const recordingID = "catalog-recording-0001"
	session, err := archiveindex.NewSessionIdentity(recordingID, "fixture", nil, "broadcast-fixture")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory(recordingID, session)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{byte(mediaSequence), 0x10, 0x7f}
	digest := sha256.Sum256(payload)
	_, err = archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: mediaSequence, Kind: archiveindex.ObjectMedia},
		ClaimID:    "origin-" + strconv.FormatUint(mediaSequence, 10), Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt:  time.Date(2026, 11, 2, 4, 0, int(mediaSequence), 0, time.UTC),
		PayloadPath: "recordings/local/path.ts", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		Verification: archiveindex.VerificationVerified, Duration: 2.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := archivesnapshot.Build(inventory, sequence, previous)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
