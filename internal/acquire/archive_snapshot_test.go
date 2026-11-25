package acquire

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/replication"
	"github.com/integrated-recorder/core/internal/storage"
)

type archiveRoundTripAuthorizer struct{}

func (archiveRoundTripAuthorizer) Authorize(context.Context, string, string, string, string, authority.Principal, []authority.SignedGrant, time.Time) error {
	return nil
}

func TestBuildArchiveSnapshotProjectsAndVerifiesStoredCanonicalBytes(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	root := newRepairTestRoot(t, store, domain.StateRecording, media)
	manager, entry, _ := newRepairTestManager(t, store, &repairFixtureTransport{}, root, &repairOwnerFence{}, nil)
	initID := "init-snapshot-fixture"
	initPayload := []byte("init-segment-native-bytes\x00")
	initSegment := domain.Segment{
		ID: initID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 3,
		Sequence: 40, SourceURI: "https://media.example/live/init.mp4?token=init-must-not-leak", IsInit: true,
	}
	owner := ownerForRepair(root.ID, 1)
	entry.mu.Lock()
	entry.ownership = &owner
	entry.mu.Unlock()
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, initSegment, archiveindex.ClaimLiveOrigin, initPayload, false); err != nil {
		t.Fatalf("commit init fixture: %v", err)
	}
	repeatedInit := initSegment
	repeatedInit.Sequence = 42
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, repeatedInit, archiveindex.ClaimLiveOrigin, initPayload, false); err != nil {
		t.Fatalf("commit repeated init-map fixture: %v", err)
	}
	payload := []byte("original segment-native bytes\x00\x01")
	segment := domain.Segment{
		TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 3,
		Sequence: 41, ArchiveOrdinal: 1, TimelineOrdinal: 1,
		Duration: 2.5, SourceURI: "https://media.example/live/segment-41.ts?token=must-not-leak", InitSegmentID: initID,
		Discontinuity: true, ByteRange: &domain.ByteRange{Offset: 8192, Length: uint64(len(payload))},
	}
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, segment, archiveindex.ClaimLiveOrigin, payload, false); err != nil {
		t.Fatalf("commit archive fixture: %v", err)
	}
	secondPayload := []byte("second original segment bytes\x00\x02")
	secondSegment := segment
	secondSegment.Sequence = 42
	secondSegment.ArchiveOrdinal = 2
	secondSegment.TimelineOrdinal = 2
	secondSegment.ByteRange = nil
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, secondSegment, archiveindex.ClaimLiveOrigin, secondPayload, false); err != nil {
		t.Fatalf("commit second archive fixture: %v", err)
	}
	stoppedAt := time.Now().UTC()
	if err := manager.update(entry, func(recording *domain.Recording) error {
		recording.State = domain.StateStopped
		recording.StoppedAt = &stoppedAt
		return nil
	}); err != nil {
		t.Fatalf("stop archive fixture: %v", err)
	}
	if err := manager.SealArchive(owner, root.ID); err != nil {
		t.Fatalf("seal archive fixture: %v", err)
	}
	before, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := manager.BuildArchiveSnapshot(context.Background(), root.ID, 1, "")
	if err != nil {
		t.Fatalf("build snapshot from archive inventory: %v", err)
	}
	if err := archivesnapshot.Verify(snapshot); err != nil {
		t.Fatalf("verify resulting snapshot: %v", err)
	}
	if len(snapshot.Content.Objects) != 5 || snapshot.Content.Archive != archiveindex.ArchiveSealed {
		t.Fatalf("snapshot objects/archive state = %d/%q, want legacy media+two init coordinates+two media/sealed", len(snapshot.Content.Objects), snapshot.Content.Archive)
	}
	after, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Revision != after.Revision || before.Segments[0].SelectedClaimID != after.Segments[0].SelectedClaimID {
		t.Fatal("snapshot projection mutated the canonical archive inventory")
	}
	var object archivesnapshot.Object
	var initIdentity string
	initCount := 0
	mediaCount := 0
	for _, candidate := range snapshot.Content.Objects {
		if candidate.Coordinate.Kind == archiveindex.ObjectInit {
			initCount++
			if initIdentity != "" && candidate.InitIdentity != initIdentity {
				t.Fatalf("repeated RC12 init map received different snapshot identities: %q/%q", initIdentity, candidate.InitIdentity)
			}
			initIdentity = candidate.InitIdentity
		}
		if candidate.Coordinate.Kind == archiveindex.ObjectMedia {
			mediaCount++
			if candidate.Coordinate.Sequence == segment.Sequence {
				object = candidate
				if candidate.InitIdentity == "" {
					t.Fatal("media snapshot object lost its init-map identity")
				}
			}
		}
	}
	if initCount != 2 || mediaCount != 3 {
		t.Fatalf("snapshot init/media coordinate count = %d/%d, want 2/3 including legacy media", initCount, mediaCount)
	}
	if object.InitIdentity == "" || object.InitIdentity != initIdentity {
		t.Fatalf("snapshot did not retain init-map relation: media=%q init=%q", object.InitIdentity, initIdentity)
	}
	digest := sha256.Sum256(payload)
	if object.PlaintextSize != int64(len(payload)) || object.PlaintextSHA256 != "sha256:"+hex.EncodeToString(digest[:]) {
		t.Fatalf("snapshot changed canonical bytes: %+v", object)
	}
	if !object.Discontinuity || object.ByteRange == nil || *object.ByteRange != (archivesnapshot.ByteRange{Offset: 8192, Length: uint64(len(payload))}) {
		t.Fatalf("snapshot lost RC12 playback metadata: %+v", object)
	}
	encoded, err := archivesnapshot.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || containsAny(encoded, []byte("media.example"), []byte("token=must-not-leak"), []byte("recordings/")) {
		t.Fatal("snapshot serialization exposed source URL, credential query, or local payload path")
	}
	source, err := manager.SnapshotObjectSource(context.Background(), root.ID, snapshot)
	if err != nil {
		t.Fatalf("bind sealed snapshot to canonical object source: %v", err)
	}
	reader, err := source.OpenCanonical(context.Background(), object.Coordinate)
	if err != nil {
		t.Fatalf("open canonical snapshot object: %v", err)
	}
	streamed, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(streamed, payload) {
		t.Fatalf("snapshot source changed original bytes: size=%d read=%v close=%v", len(streamed), readErr, closeErr)
	}
}

func TestPersistArchiveSnapshotCreatesAppendOnlyCatalogChain(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := newRecoveryShardedTestRoot(t, store, "84e2f247ab144c6a850fd5c07fb1e6a1", domain.StateRecording, repairMediaContextForTest())
	manager, entry, _ := newRepairTestManager(t, store, &repairFixtureTransport{}, root, &repairOwnerFence{}, nil)
	owner := ownerForRepair(root.ID, 1)
	entry.mu.Lock()
	entry.ownership = &owner
	entry.mu.Unlock()
	catalog, err := archivecatalog.Open(t.TempDir() + "/catalog")
	if err != nil {
		t.Fatal(err)
	}
	commit := func(sequence, ordinal uint64, payload []byte) {
		t.Helper()
		segment := domain.Segment{
			TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 3,
			Sequence: sequence, ArchiveOrdinal: ordinal, TimelineOrdinal: ordinal,
			Duration: 2.5, SourceURI: "https://media.example/live/segment.ts",
		}
		if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, segment, archiveindex.ClaimLiveOrigin, payload, false); err != nil {
			t.Fatalf("commit sequence %d: %v", sequence, err)
		}
	}
	commit(41, 1, []byte("first original bytes"))
	unsealed, err := manager.BuildArchiveSnapshot(context.Background(), root.ID, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.SnapshotObjectSource(context.Background(), root.ID, unsealed); err == nil {
		t.Fatal("replication source was opened for a mutable, unsealed archive")
	}
	first, err := manager.PersistArchiveSnapshot(context.Background(), catalog, root.ID)
	if err != nil || first.Sequence != 1 {
		t.Fatalf("first persisted snapshot=%+v err=%v", first, err)
	}
	unchanged, err := manager.PersistArchiveSnapshot(context.Background(), catalog, root.ID)
	if err != nil || unchanged.Sequence != 1 || unchanged.SnapshotID != first.SnapshotID {
		t.Fatalf("unchanged archive was not idempotent: %+v err=%v", unchanged, err)
	}
	commit(42, 2, []byte("second original bytes"))
	second, err := manager.PersistArchiveSnapshot(context.Background(), catalog, root.ID)
	if err != nil || second.Sequence != 2 || second.PreviousSnapshotID != first.SnapshotID || len(second.Content.Objects) != 2 {
		t.Fatalf("second persisted snapshot=%+v err=%v", second, err)
	}
	head, err := catalog.Head(context.Background(), second.Content.ArchiveID)
	if err != nil || head.SnapshotID != second.SnapshotID {
		t.Fatalf("persisted catalog head=%+v err=%v", head, err)
	}
	if err := archivesnapshot.VerifyChain([]archivesnapshot.Snapshot{first, head}); err != nil {
		t.Fatalf("persisted snapshot chain failed verification: %v", err)
	}
	stoppedAt := time.Now().UTC()
	if err := manager.update(entry, func(recording *domain.Recording) error {
		recording.State = domain.StateStopped
		recording.StoppedAt = &stoppedAt
		return nil
	}); err != nil {
		t.Fatalf("stop archive before history source validation: %v", err)
	}
	if err := manager.SealArchive(owner, root.ID); err != nil {
		t.Fatalf("seal archive before history source validation: %v", err)
	}
	historySource, err := manager.SnapshotHistoryObjectSource(context.Background(), root.ID, first)
	if err != nil {
		t.Fatalf("bind an older immutable snapshot after a later append: %v", err)
	}
	if len(first.Content.Objects) != 1 {
		t.Fatalf("first snapshot object count=%d, want 1", len(first.Content.Objects))
	}
	reader, err := historySource.OpenCanonical(context.Background(), first.Content.Objects[0].Coordinate)
	if err != nil {
		t.Fatalf("open historical canonical payload: %v", err)
	}
	oldBytes, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(oldBytes, []byte("first original bytes")) {
		t.Fatalf("historical source did not preserve old snapshot bytes: %q read=%v close=%v", oldBytes, readErr, closeErr)
	}
}

func TestSealedRC12ArchiveEncryptsAndRestoresPlayableHLSFromCanonicalPayloads(t *testing.T) {
	ctx := context.Background()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "e3c1c6595b3d44b9a23143aeb941b5cc"
	mediaContext := repairMediaContextForTest()
	root := newRecoveryShardedTestRoot(t, store, recordingID, domain.StateRecording, mediaContext)
	manager, entry, _ := newRepairTestManager(t, store, &repairFixtureTransport{}, root, &repairOwnerFence{}, nil)
	owner := ownerForRepair(recordingID, 1)
	entry.mu.Lock()
	entry.ownership = &owner
	entry.mu.Unlock()

	initID := "replica-init-map"
	initBytes := []byte("original-init-object\x00")
	init := domain.Segment{
		ID: initID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 4,
		Sequence: 1, SourceURI: "https://source.invalid/init.mp4?token=private", IsInit: true,
	}
	if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, init, archiveindex.ClaimLiveOrigin, initBytes, false); err != nil {
		t.Fatalf("commit canonical init object: %v", err)
	}
	payloads := map[archiveindex.Coordinate][]byte{}
	for index, payload := range [][]byte{[]byte("original-media-one\x00"), []byte("original-media-two\x00")} {
		sequence := uint64(index + 2)
		segment := domain.Segment{
			TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 4,
			Sequence: sequence, ArchiveOrdinal: uint64(index + 1), TimelineOrdinal: uint64(index + 1),
			Duration: 2.0, SourceURI: "https://source.invalid/segment.ts?token=private", InitSegmentID: initID,
		}
		if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, segment, archiveindex.ClaimLiveOrigin, payload, false); err != nil {
			t.Fatalf("commit canonical media sequence %d: %v", sequence, err)
		}
		payloads[archiveindex.Coordinate{
			SessionID: root.SourceSessionID, TrackID: "main", SourceEpoch: 1,
			DiscontinuitySequence: 4, Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}] = payload
	}
	payloads[archiveindex.Coordinate{
		SessionID: root.SourceSessionID, TrackID: "main", SourceEpoch: 1,
		DiscontinuitySequence: 4, Sequence: 1, Kind: archiveindex.ObjectInit,
	}] = initBytes
	stoppedAt := time.Now().UTC()
	if err := manager.update(entry, func(recording *domain.Recording) error {
		recording.State = domain.StateStopped
		recording.StoppedAt = &stoppedAt
		return nil
	}); err != nil {
		t.Fatalf("stop fixture recording: %v", err)
	}
	if err := manager.SealArchive(owner, recordingID); err != nil {
		t.Fatalf("seal fixture recording: %v", err)
	}
	snapshot, err := manager.BuildArchiveSnapshot(ctx, recordingID, 1, "")
	if err != nil {
		t.Fatalf("build canonical RC12 snapshot: %v", err)
	}
	canonicalSource, err := manager.SnapshotObjectSource(ctx, recordingID, snapshot)
	if err != nil {
		t.Fatalf("bind snapshot to canonical archive: %v", err)
	}

	identity := authority.Principal{Kind: authority.PrincipalNode, ID: "nd_aaaaaaaaaaaaaaaaaaaaaa"}
	auth := replication.Authorization{
		SubjectID: "os_bbbbbbbbbbbbbbbbbbbbbb", Actor: identity,
		Chain: []authority.SignedGrant{{}}, Verifier: archiveRoundTripAuthorizer{},
	}
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
	bundle, err := replicaStore.EncryptSnapshot(ctx, snapshot, canonicalSource, dek, 1, auth)
	if err != nil {
		t.Fatalf("encrypt verified canonical snapshot: %v", err)
	}
	if len(bundle.Objects) != len(payloads) {
		t.Fatalf("encrypted object count=%d, canonical payload count=%d", len(bundle.Objects), len(payloads))
	}

	// Exercise the browser/offline-recovery key envelope contract: the
	// recovery private keys remain local to this test/client and only the DEK
	// recovered from the signed envelope is passed to RestoreSnapshot.
	recoveryEncryptionKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPublic, recoverySigningKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recoveryKeyID, err := identityprotocol.KeyID(recoveryPublic)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 1, recoveryEncryptionKey.PublicKey().Bytes(), recoveryKeyID, recoverySigningKey)
	if err != nil {
		t.Fatalf("wrap archive key for recovery recipient: %v", err)
	}
	recipientKeyID := cryptoarchive.RecipientKeyID(recoveryEncryptionKey.PublicKey().Bytes())
	wrongSignerPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := replicaStore.PersistKeyEnvelope(snapshot, envelope, wrongSignerPublic); !errors.Is(err, replication.ErrInvalid) {
		t.Fatalf("invalid envelope signer was accepted into immutable key slot: %v", err)
	}
	wrongArchiveEnvelope := envelope
	wrongArchiveEnvelope.ArchiveID = "arc_0123456789abcdefghijkl"
	if err := replicaStore.PersistKeyEnvelope(snapshot, wrongArchiveEnvelope, recoveryPublic); !errors.Is(err, replication.ErrInvalid) {
		t.Fatalf("envelope for another archive was accepted: %v", err)
	}
	if err := replicaStore.PersistKeyEnvelope(snapshot, envelope, recoveryPublic); err != nil {
		t.Fatalf("persist signed recovery envelope: %v", err)
	}
	envelopePath := filepath.Join(replicaRoot, "key-envelopes", snapshot.Content.ArchiveID, recipientKeyID, "1.json")
	envelopeInfo, err := os.Lstat(envelopePath)
	if err != nil || !envelopeInfo.Mode().IsRegular() || envelopeInfo.Mode().Perm() != 0600 || envelopeInfo.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("persisted envelope permissions=%v err=%v", envelopeInfo, err)
	}
	reopenedReplica, err := replication.Open(replicaRoot)
	if err != nil {
		t.Fatalf("reopen encrypted replica store: %v", err)
	}
	loadedEnvelope, err := reopenedReplica.LoadKeyEnvelope(snapshot, 1, recipientKeyID)
	if err != nil || loadedEnvelope != envelope {
		t.Fatalf("reopened recovery envelope=%+v err=%v", loadedEnvelope, err)
	}
	duplicateEnvelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 1, recoveryEncryptionKey.PublicKey().Bytes(), recoveryKeyID, recoverySigningKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedReplica.PersistKeyEnvelope(snapshot, duplicateEnvelope, recoveryPublic); !errors.Is(err, replication.ErrInvalid) {
		t.Fatalf("same recovery recipient and key epoch accepted a replacement envelope: %v", err)
	}
	recoveredDEK, err := cryptoarchive.OpenDEK(loadedEnvelope, cryptoarchive.EnvelopeExpectation{
		ArchiveID: snapshot.Content.ArchiveID, KeyEpoch: 1,
		RecipientKeyID: recipientKeyID,
		SignerKeyID:    recoveryKeyID, SignerPublicKey: recoveryPublic,
		RecipientPrivate: recoveryEncryptionKey.Bytes(),
	})
	if err != nil {
		t.Fatalf("recover archive DEK from signed offline envelope: %v", err)
	}
	defer recoveredDEK.Destroy()

	restoredRoot := filepath.Join(t.TempDir(), "restored")
	manifest, err := replicaStore.RestoreSnapshot(ctx, snapshot, bundle, recoveredDEK, restoredRoot, auth)
	if err != nil {
		t.Fatalf("restore complete encrypted replica: %v", err)
	}
	if manifest.PlaybackPlaylist == "" || len(manifest.TrackPlaylists) != 1 {
		t.Fatalf("restored snapshot did not contain a playable HLS projection: %+v", manifest)
	}
	for _, object := range manifest.Objects {
		want, ok := payloads[object.Coordinate]
		if !ok {
			t.Fatalf("restore exposed an unexpected coordinate: %+v", object.Coordinate)
		}
		got, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash(object.Path)))
		if err != nil || !bytes.Equal(got, want) || object.Size != int64(len(want)) {
			t.Fatalf("restored coordinate %+v changed original bytes: size=%d want=%d err=%v", object.Coordinate, len(got), len(want), err)
		}
	}
	master, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash(manifest.PlaybackPlaylist)))
	if err != nil || !bytes.Contains(master, []byte("#EXT-X-STREAM-INF")) {
		t.Fatalf("restored HLS master playlist is unavailable: err=%v", err)
	}
	trackPlaylist, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash(manifest.TrackPlaylists[0].Path)))
	if err != nil || !bytes.Contains(trackPlaylist, []byte("#EXT-X-MAP")) || !bytes.Contains(trackPlaylist, []byte("#EXT-X-ENDLIST")) {
		t.Fatalf("restored HLS track playlist is incomplete: err=%v body=%s", err, trackPlaylist)
	}
	if bytes.Contains(master, []byte("source.invalid")) || strings.Contains(string(trackPlaylist), "token=private") {
		t.Fatal("restored playback projection exposed source URL credentials")
	}
}

func containsAny(haystack []byte, needles ...[]byte) bool {
	for _, needle := range needles {
		if len(needle) > 0 && bytes.Contains(haystack, needle) {
			return true
		}
	}
	return false
}
