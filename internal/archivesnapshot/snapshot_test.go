package archivesnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
)

func TestBuildSnapshotIsContentAddressedAndSeparatesClaimProvenance(t *testing.T) {
	first := snapshotInventory(t)
	first.Session.ResourceHash = stringsOf('a', 64)
	first.Session.SessionHash = stringsOf('b', 64)
	addSnapshotClaim(t, &first, 1, "origin", archiveindex.ClaimLiveOrigin, "original media bytes")
	firstSnapshot, err := Build(first, 1, "")
	if err != nil {
		t.Fatal(err)
	}

	copyOfInventory := first
	copyOfInventory.Segments = append([]archiveindex.Segment(nil), first.Segments...)
	copyOfInventory.Segments[0].Claims = append([]archiveindex.Claim(nil), first.Segments[0].Claims...)
	copyOfInventory.Segments[0].Claims[0].Source = archiveindex.ClaimPeer
	copyOfInventory.Segments[0].Claims[0].PayloadPath = "recordings/copy/private/object.bin"
	copyOfInventory.Segments[0].Claims[0].AcquiredAt = time.Date(2026, 11, 2, 4, 5, 6, 0, time.UTC)
	secondSnapshot, err := Build(copyOfInventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if firstSnapshot.SnapshotID != secondSnapshot.SnapshotID || firstSnapshot.ContentHash != secondSnapshot.ContentHash {
		t.Fatal("the same canonical timeline received different content identity")
	}
	if firstSnapshot.ProvenanceHash == secondSnapshot.ProvenanceHash {
		t.Fatal("claim transport provenance was not isolated from content identity")
	}
	if bytes.Contains(mustMarshal(t, firstSnapshot), []byte("recordings/")) {
		t.Fatal("snapshot exposed a local payload path")
	}
	serialized := mustMarshal(t, firstSnapshot)
	for _, forbidden := range [][]byte{
		[]byte(`"resource_hash"`), []byte(`"session_hash"`),
		[]byte(stringsOf('a', 64)), []byte(stringsOf('b', 64)),
	} {
		if bytes.Contains(serialized, forbidden) {
			t.Fatalf("snapshot exposed source-identity fingerprint %q", forbidden)
		}
	}
}

func TestArchiveIdentitySurvivesDifferentLocalRecordingIDs(t *testing.T) {
	first := snapshotInventory(t)
	first.ArchiveID = "arc_AAAAAAAAAAAAAAAAAAAAAA"
	addSnapshotClaim(t, &first, 1, "origin", archiveindex.ClaimLiveOrigin, "original media bytes")
	firstSnapshot, err := Build(first, 1, "")
	if err != nil {
		t.Fatal(err)
	}

	copy := first
	copy.RecordingID = "different-local-recording-id"
	secondSnapshot, err := Build(copy, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if firstSnapshot.Content.ArchiveID != secondSnapshot.Content.ArchiveID || firstSnapshot.SnapshotID != secondSnapshot.SnapshotID {
		t.Fatalf("copy changed logical identity: first=(%s,%s) second=(%s,%s)", firstSnapshot.Content.ArchiveID, firstSnapshot.SnapshotID, secondSnapshot.Content.ArchiveID, secondSnapshot.SnapshotID)
	}
}

func TestLegacyArchiveIdentityUsesMigrationFallback(t *testing.T) {
	inventory := snapshotInventory(t)
	archiveID, err := ArchiveIDForRecording(inventory.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Content.ArchiveID != archiveID {
		t.Fatalf("legacy archive ID = %q, want migration fallback %q", snapshot.Content.ArchiveID, archiveID)
	}
}

func TestSnapshotV2BindsMediaToInitObjectAndRetainsV1ReadCompatibility(t *testing.T) {
	inventory := snapshotInventory(t)
	initCoordinate := archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectInit}
	mediaCoordinate := archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia}
	apply := func(coordinate archiveindex.Coordinate, claimID, payload string, duration float64, initIdentity string) {
		t.Helper()
		digest := sha256.Sum256([]byte(payload))
		if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: claimID, Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt:  time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC),
			PayloadPath: "private/" + claimID, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
			Verification: archiveindex.VerificationVerified, Duration: duration, InitIdentity: initIdentity,
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(initCoordinate, "init-claim", "init payload", 0, "init-source-id")
	apply(mediaCoordinate, "media-claim", "media payload", 2.5, "init-source-id")
	snapshot, err := Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SchemaVersion != 2 {
		t.Fatalf("new snapshot schema=%d, want 2", snapshot.SchemaVersion)
	}
	var initRef string
	for _, object := range snapshot.Content.Objects {
		if object.Coordinate.Kind == archiveindex.ObjectInit {
			initRef = object.InitIdentity
		}
	}
	if initRef == "" || initRef == "init-source-id" {
		t.Fatalf("init source identity was not safely projected: %q", initRef)
	}
	for _, object := range snapshot.Content.Objects {
		if object.Coordinate.Kind == archiveindex.ObjectMedia && object.InitIdentity != initRef {
			t.Fatalf("media init reference=%q, want init object identity %q", object.InitIdentity, initRef)
		}
	}
	if err := Verify(snapshot); err != nil {
		t.Fatalf("verified v2 snapshot rejected: %v", err)
	}

	legacy := snapshot
	legacy.SchemaVersion = 1
	legacyBytes, err := canonicalJSON(legacy.Content)
	if err != nil {
		t.Fatal(err)
	}
	legacy.ContentHash = hashWithDomain(contentDomainV1, legacyBytes)
	legacy.SnapshotID = snapshotID(legacy.ContentHash)
	if err := Verify(legacy); err != nil {
		t.Fatalf("v1 snapshot read compatibility rejected: %v", err)
	}

	broken := snapshot
	broken.Content.Objects = append([]Object(nil), snapshot.Content.Objects...)
	for i := range broken.Content.Objects {
		if broken.Content.Objects[i].Coordinate.Kind == archiveindex.ObjectMedia {
			broken.Content.Objects[i].InitIdentity = "initref_" + stringsOf('e', 64)
		}
	}
	brokenBytes, err := canonicalJSON(broken.Content)
	if err != nil {
		t.Fatal(err)
	}
	broken.ContentHash = hashWithDomain(contentDomainV2, brokenBytes)
	broken.SnapshotID = snapshotID(broken.ContentHash)
	if err := Verify(broken); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("snapshot with dangling init reference accepted: %v", err)
	}
}

func TestSnapshotV2AllowsRepeatedIdenticalInitMapButRejectsAmbiguousBytes(t *testing.T) {
	buildInventory := func(t *testing.T, secondInitPayload string) archiveindex.Inventory {
		t.Helper()
		inventory := snapshotInventory(t)
		mapID := "same-source-map"
		apply := func(kind archiveindex.ObjectKind, sequence uint64, claimID, payload string, duration float64) {
			t.Helper()
			coordinate := archiveindex.Coordinate{
				SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 1,
				DiscontinuitySequence: 1, Sequence: sequence, Kind: kind,
			}
			digest := sha256.Sum256([]byte(payload))
			if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
				Coordinate: coordinate, ClaimID: claimID, Source: archiveindex.ClaimLiveOrigin,
				AcquiredAt:  time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC),
				PayloadPath: "private/" + claimID, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
				Verification: archiveindex.VerificationVerified, Duration: duration, InitIdentity: mapID,
			}); err != nil {
				t.Fatal(err)
			}
		}
		apply(archiveindex.ObjectInit, 1, "init-one", "same init bytes", 0)
		apply(archiveindex.ObjectInit, 2, "init-two", secondInitPayload, 0)
		apply(archiveindex.ObjectMedia, 1, "media-one", "media bytes", 2)
		return inventory
	}

	identical, err := Build(buildInventory(t, "same init bytes"), 1, "")
	if err != nil {
		t.Fatalf("repeated identical RC12 init map rejected: %v", err)
	}
	if err := Verify(identical); err != nil {
		t.Fatalf("repeated init map snapshot did not verify: %v", err)
	}

	if _, err := Build(buildInventory(t, "different bytes"), 1, ""); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("same source init identity with different bytes was accepted: %v", err)
	}
}

func TestSnapshotChainIsAppendOnlyAndTamperingFailsClosed(t *testing.T) {
	inventory := snapshotInventory(t)
	addSnapshotClaim(t, &inventory, 1, "origin-1", archiveindex.ClaimLiveOrigin, "first bytes")
	first, err := Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	addSnapshotClaim(t, &inventory, 2, "origin-2", archiveindex.ClaimLiveOrigin, "second bytes")
	second, err := Build(inventory, 2, first.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyChain([]Snapshot{first, second}); err != nil {
		t.Fatalf("valid snapshot chain rejected: %v", err)
	}
	second.Content.Objects[0].PlaintextSHA256 = stringsOf('0', 64)
	if err := Verify(second); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("mutated content accepted: %v", err)
	}
	if err := VerifyChain([]Snapshot{second}); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("truncated history treated as genesis: %v", err)
	}
}

func TestSnapshotCanonicalRoundTripRejectsNoncanonicalAndUnknownFields(t *testing.T) {
	inventory := snapshotInventory(t)
	addSnapshotClaim(t, &inventory, 1, "origin", archiveindex.ClaimLiveOrigin, "payload")
	snapshot, err := Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Parse(encoded)
	if err != nil || !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("round trip decoded=%+v err=%v", decoded, err)
	}
	if _, err := Parse(append(append([]byte(nil), encoded...), []byte(" ")...)); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("noncanonical trailing whitespace accepted: %v", err)
	}
	unknown := bytes.Replace(encoded, []byte(`"schema_version":2`), []byte(`"unexpected":true,"schema_version":2`), 1)
	if _, err := Parse(unknown); err == nil {
		t.Fatal("snapshot with unknown field accepted")
	}
}

func TestVerifyPayloadsStreamsAndChecksExactOriginalBytes(t *testing.T) {
	inventory := snapshotInventory(t)
	payload := []byte("unaltered segment-native bytes\x00\x01")
	addSnapshotClaim(t, &inventory, 1, "origin", archiveindex.ClaimLiveOrigin, string(payload))
	snapshot, err := Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	source := mapPayloadSource{payload: payload}
	if err := VerifyPayloads(context.Background(), snapshot, source); err != nil {
		t.Fatalf("original payload failed verification: %v", err)
	}
	source.payload = []byte("transcoded payload")
	if err := VerifyPayloads(context.Background(), snapshot, source); !errors.Is(err, ErrPayloadMismatch) {
		t.Fatalf("changed bytes passed snapshot verification: %v", err)
	}
	source.err = errors.New("private provider path error")
	if err := VerifyPayloads(context.Background(), snapshot, source); !errors.Is(err, ErrPayloadSource) || bytes.Contains([]byte(err.Error()), []byte("private provider path")) {
		t.Fatalf("source failure exposed internals or wrong classification: %v", err)
	}
}

func TestCoverageReasonAndLocalObservationTimeDoNotChangeContentHash(t *testing.T) {
	first := snapshotInventory(t)
	coverage := archiveindex.Coverage{
		SessionID: first.Session.ID, TrackID: "main", SourceEpoch: 1,
		DiscontinuitySequence: 1, FromSequence: 2, ToSequence: 4,
		State: archiveindex.CoverageKnownMissing, ObservedAt: time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC), Reason: "capture window",
		Kind: archiveindex.ObjectMedia,
	}
	if err := archiveindex.ApplyCoverage(&first, coverage); err != nil {
		t.Fatal(err)
	}
	a, err := Build(first, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Coverage = append([]archiveindex.Coverage(nil), first.Coverage...)
	second.Coverage[0].ObservedAt = coverage.ObservedAt.Add(time.Hour)
	second.Coverage[0].Reason = "capture window"
	b, err := Build(second, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if a.SnapshotID != b.SnapshotID {
		t.Fatal("local coverage observation clock changed the immutable content hash")
	}
}

func TestSnapshotContentBindsDiscontinuityAndSourceByteRange(t *testing.T) {
	inventory := snapshotInventory(t)
	input := archiveindex.ClaimInput{
		Coordinate: archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 2, DiscontinuitySequence: 4, Sequence: 7, Kind: archiveindex.ObjectMedia},
		ClaimID:    "range-claim", Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC), PayloadPath: "private/path/segment.bin",
		Size: 7, SHA256: hexDigest("payload"), Verification: archiveindex.VerificationVerified,
		Duration: 2.5, Discontinuity: true, ByteRange: &domain.ByteRange{Offset: 128, Length: 7},
	}
	if _, err := archiveindex.ApplyClaim(&inventory, input); err != nil {
		t.Fatal(err)
	}
	first, err := Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	object := first.Content.Objects[0]
	if !object.Discontinuity || object.ByteRange == nil || *object.ByteRange != (ByteRange{Offset: 128, Length: 7}) {
		t.Fatalf("snapshot lost playback metadata: %+v", object)
	}

	for name, mutate := range map[string]func(*archiveindex.Segment){
		"byte range":    func(segment *archiveindex.Segment) { segment.ByteRange = &domain.ByteRange{Offset: 129, Length: 7} },
		"discontinuity": func(segment *archiveindex.Segment) { segment.Discontinuity = false },
	} {
		t.Run(name, func(t *testing.T) {
			changed := inventory
			changed.Segments = append([]archiveindex.Segment(nil), inventory.Segments...)
			changed.Segments[0] = inventory.Segments[0]
			changed.Segments[0].ByteRange = nil
			if inventory.Segments[0].ByteRange != nil {
				byteRange := *inventory.Segments[0].ByteRange
				changed.Segments[0].ByteRange = &byteRange
			}
			mutate(&changed.Segments[0])
			second, buildErr := Build(changed, 1, "")
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			if first.SnapshotID == second.SnapshotID {
				t.Fatal("playback metadata change did not change immutable snapshot identity")
			}
		})
	}

	encoded, err := Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Parse(encoded)
	if err != nil || decoded.Content.Objects[0].ByteRange == nil || *decoded.Content.Objects[0].ByteRange != *object.ByteRange || !decoded.Content.Objects[0].Discontinuity {
		t.Fatalf("metadata did not survive canonical wire round-trip: %+v err=%v", decoded.Content.Objects, err)
	}
	if bytes.Contains(encoded, []byte("private/path")) {
		t.Fatal("snapshot exposed a local payload path")
	}
}

func hexDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func snapshotInventory(t *testing.T) archiveindex.Inventory {
	t.Helper()
	session, err := archiveindex.NewSessionIdentity("recording-snapshot-01", "hls", nil, "broadcast-01")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("recording-snapshot-01", session)
	if err != nil {
		t.Fatal(err)
	}
	return inventory
}

func addSnapshotClaim(t *testing.T, inventory *archiveindex.Inventory, sequence uint64, claimID string, source archiveindex.ClaimSource, payload string) {
	t.Helper()
	digest := sha256.Sum256([]byte(payload))
	_, err := archiveindex.ApplyClaim(inventory, archiveindex.ClaimInput{
		Coordinate: archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: sequence, Kind: archiveindex.ObjectMedia},
		ClaimID:    claimID, Source: source, AcquiredAt: time.Date(2026, 11, 2, 1, 0, 0, 0, time.UTC),
		PayloadPath: "recordings/recording-snapshot-01/segments/" + claimID + ".ts", Size: int64(len(payload)),
		SHA256: hex.EncodeToString(digest[:]), Verification: archiveindex.VerificationVerified, Duration: 2.5,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mustMarshal(t *testing.T, snapshot Snapshot) []byte {
	t.Helper()
	encoded, err := Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

type mapPayloadSource struct {
	payload []byte
	err     error
}

func (source mapPayloadSource) OpenCanonical(context.Context, archiveindex.Coordinate) (io.ReadCloser, error) {
	if source.err != nil {
		return nil, source.err
	}
	return io.NopCloser(bytes.NewReader(source.payload)), nil
}

func stringsOf(value byte, count int) string { return string(bytes.Repeat([]byte{value}, count)) }
