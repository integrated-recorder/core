package acquire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/replication"
	"github.com/integrated-recorder/core/internal/storage"
)

const rc12FixtureRecordingID = "4d6f3c8208754a8cbff5064a171c2490"

func TestRC12PersistedArchiveSnapshotAndRestorePreserveCanonicalPayloads(t *testing.T) {
	ctx := context.Background()
	dataRoot := t.TempDir()
	fixtureRoot := filepath.Join("testdata", "rc12-sharded-archive")
	sourceRecordingDir := fixtureRoot
	if _, err := os.Stat(sourceRecordingDir); err != nil {
		t.Fatalf("RC12 fixture missing: %v", err)
	}
	migratedRecordingDir := filepath.Join(dataRoot, "recordings", rc12FixtureRecordingID)
	if err := copyArchiveFixture(sourceRecordingDir, migratedRecordingDir); err != nil {
		t.Fatalf("copy RC12 fixture: %v", err)
	}
	before, err := archiveFixtureDigests(migratedRecordingDir)
	if err != nil {
		t.Fatal(err)
	}

	store, err := storage.New(dataRoot)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.LoadRecordingReadOnly(rc12FixtureRecordingID)
	if err != nil {
		t.Fatalf("read RC12 recording root: %v", err)
	}
	if root.FormatVersion != storage.ShardedArchiveFormatVersion || root.State != domain.StateStopped || !root.ArchiveSealed {
		t.Fatalf("unexpected RC12 root identity/lifecycle: format=%d state=%q sealed=%v", root.FormatVersion, root.State, root.ArchiveSealed)
	}
	if root.ArchiveID != "" {
		t.Fatalf("RC12 root unexpectedly contains a post-RC12 ArchiveID: %q", root.ArchiveID)
	}
	manager, _, _ := newRepairTestManager(t, store, &repairFixtureTransport{}, root, &repairOwnerFence{}, nil)
	inventory, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatalf("load canonical RC12 inventory: %v", err)
	}
	if inventory.Archive != archiveindex.ArchiveSealed || len(inventory.Segments) != 3 {
		t.Fatalf("RC12 inventory archive/segments=%q/%d, want sealed and init+2 media", inventory.Archive, len(inventory.Segments))
	}

	var title string
	if err := store.IterateShardedMetadata(ctx, root.ID, func(revision domain.MetadataRevision) error {
		if revision.Title != nil {
			title = *revision.Title
		}
		return nil
	}); err != nil || title != "RC12 persisted fixture" {
		t.Fatalf("RC12 metadata title=%q err=%v", title, err)
	}
	manifestCount := 0
	if err := store.IterateShardedManifests(ctx, root.ID, func(snapshot domain.ManifestSnapshot) error {
		manifestCount++
		reader, err := store.StorageBackend.OpenPayloadReader(root.ID, snapshot.StoragePath)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		digest := sha256.Sum256(body)
		if int64(len(body)) != snapshot.Size || hex.EncodeToString(digest[:]) != snapshot.SHA256 {
			return errors.New("RC12 source manifest payload does not match persisted size/digest")
		}
		if !bytes.Contains(body, []byte("#EXT-X-MAP")) || !bytes.Contains(body, []byte("#EXT-X-ENDLIST")) {
			return errors.New("RC12 source manifest playback markers are missing")
		}
		return nil
	}); err != nil || manifestCount != 1 {
		t.Fatalf("RC12 manifest snapshots=%d err=%v", manifestCount, err)
	}

	snapshot, err := manager.BuildArchiveSnapshot(ctx, root.ID, 1, "")
	if err != nil {
		t.Fatalf("build snapshot from persisted RC12 archive: %v", err)
	}
	if err := archivesnapshot.Verify(snapshot); err != nil {
		t.Fatalf("verify snapshot from persisted RC12 archive: %v", err)
	}
	if snapshot.Content.Archive != archiveindex.ArchiveSealed || len(snapshot.Content.Objects) != 3 {
		t.Fatalf("RC12 snapshot archive/object count=%q/%d", snapshot.Content.Archive, len(snapshot.Content.Objects))
	}
	legacyArchiveID, err := archivesnapshot.ArchiveIDForRecording(root.ID)
	if err != nil || snapshot.Content.ArchiveID != legacyArchiveID || inventory.ArchiveID != "" {
		t.Fatalf("legacy identity fallback is unstable or rewrote the old identity: snapshot=%q fallback=%q inventory=%q err=%v", snapshot.Content.ArchiveID, legacyArchiveID, inventory.ArchiveID, err)
	}
	source, err := manager.SnapshotObjectSource(ctx, root.ID, snapshot)
	if err != nil {
		t.Fatalf("bind snapshot to persisted RC12 objects: %v", err)
	}
	canonical := make(map[archiveindex.Coordinate][]byte, len(snapshot.Content.Objects))
	for _, object := range snapshot.Content.Objects {
		reader, err := source.OpenCanonical(ctx, object.Coordinate)
		if err != nil {
			t.Fatalf("open RC12 canonical coordinate %+v: %v", object.Coordinate, err)
		}
		payload, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read RC12 canonical object: read=%v close=%v", readErr, closeErr)
		}
		digest := sha256.Sum256(payload)
		if int64(len(payload)) != object.PlaintextSize || "sha256:"+hex.EncodeToString(digest[:]) != object.PlaintextSHA256 {
			t.Fatalf("RC12 canonical bytes changed for %+v", object.Coordinate)
		}
		canonical[object.Coordinate] = payload
	}

	// Exercise the real encrypt/restore path using bytes read from the exact
	// RC12 tree, not an archive created by the current branch.
	principal := authority.Principal{Kind: authority.PrincipalNode, ID: "nd_aaaaaaaaaaaaaaaaaaaaaa"}
	auth := replication.Authorization{SubjectID: "os_bbbbbbbbbbbbbbbbbbbbbb", Actor: principal, Chain: []authority.SignedGrant{{}}, Verifier: archiveRoundTripAuthorizer{}}
	replicaRoot := filepath.Join(t.TempDir(), "encrypted-replica")
	replicaStore, err := replication.Open(replicaRoot)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := replicaStore.EncryptSnapshot(ctx, snapshot, source, dek, 1, auth)
	if err != nil {
		t.Fatalf("encrypt RC12 snapshot: %v", err)
	}
	restoredRoot := filepath.Join(t.TempDir(), "restored")
	playback, err := replicaStore.RestoreSnapshot(ctx, snapshot, bundle, dek, restoredRoot, auth)
	if err != nil {
		t.Fatalf("restore encrypted RC12 snapshot: %v", err)
	}
	if playback.PlaybackPlaylist == "" || len(playback.TrackPlaylists) != 1 {
		t.Fatalf("restored RC12 archive has no HLS playback projection: %+v", playback)
	}
	for _, object := range playback.Objects {
		want := canonical[object.Coordinate]
		got, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash(object.Path)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("restored RC12 bytes changed for %+v: err=%v", object.Coordinate, err)
		}
	}
	playlist, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash(playback.TrackPlaylists[0].Path)))
	if err != nil || !bytes.Contains(playlist, []byte("#EXT-X-MAP")) || !bytes.Contains(playlist, []byte("#EXT-X-ENDLIST")) {
		t.Fatalf("restored RC12 archive is not a complete HLS VOD playlist: err=%v body=%s", err, playlist)
	}

	// Persist only the new catalog outside the RC12 recording directory and
	// prove every RC12 file remains byte-for-byte intact.
	catalog, err := archivecatalog.Open(filepath.Join(t.TempDir(), "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := manager.PersistArchiveSnapshot(ctx, catalog, root.ID)
	if err != nil || persisted.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("persist RC12 snapshot catalog entry: snapshot=%q err=%v", persisted.SnapshotID, err)
	}
	after, err := archiveFixtureDigests(migratedRecordingDir)
	if err != nil {
		t.Fatal(err)
	}
	if !equalStringMap(before, after) {
		t.Fatalf("read-only RC12 migration/snapshot path modified the source fixture: before=%v after=%v", before, after)
	}
}

func copyArchiveFixture(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
}

func archiveFixtureDigests(root string) (map[string]string, error) {
	result := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[filepath.ToSlash(rel)] = hex.EncodeToString(digest[:])
		return nil
	})
	return result, err
}

func equalStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	keys := make([]string, 0, len(a))
	for key := range a {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if a[key] != b[key] {
			return false
		}
	}
	return true
}
