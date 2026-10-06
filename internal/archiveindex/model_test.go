package archiveindex

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

var testTime = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

func testInventory(t *testing.T, recordingID string) Inventory {
	t.Helper()
	session, err := NewSessionIdentity(recordingID, "fixture", nil, "broadcast-42")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := NewInventory(recordingID, session)
	if err != nil {
		t.Fatal(err)
	}
	return inventory
}

func testCoord(inventory Inventory, sequence uint64) Coordinate {
	return Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 3, DiscontinuitySequence: 1, Sequence: sequence}
}

func testInput(inventory Inventory, sequence uint64, claimID, source, payload string) ClaimInput {
	digest := sha256.Sum256([]byte(payload))
	return ClaimInput{
		Coordinate: testCoord(inventory, sequence), ClaimID: claimID,
		Source: ClaimSource(source), AcquiredAt: testTime,
		PayloadPath: "recordings/" + inventory.RecordingID + "/tracks/main/" + claimID + ".m4s",
		Size:        int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		Verification: VerificationVerified, Duration: 2.5,
	}
}

func mustClaim(t *testing.T, inventory *Inventory, input ClaimInput) ClaimDisposition {
	t.Helper()
	disposition, err := ApplyClaim(inventory, input)
	if err != nil {
		t.Fatal(err)
	}
	return disposition
}

func segmentBySequence(t *testing.T, inventory Inventory, sequence uint64) Segment {
	t.Helper()
	for _, segment := range inventory.Segments {
		if segment.Coordinate.Sequence == sequence && segment.Coordinate.Kind == ObjectMedia {
			return segment
		}
	}
	t.Fatalf("segment sequence %d not found", sequence)
	return Segment{}
}

func TestLatePrefixInsertionRebuildsTimelineWithoutChangingExistingClaims(t *testing.T) {
	inventory := testInventory(t, "recording-prefix")
	mustClaim(t, &inventory, testInput(inventory, 11, "claim-11", string(ClaimLiveOrigin), "media-11"))
	mustClaim(t, &inventory, testInput(inventory, 12, "claim-12", string(ClaimLiveOrigin), "media-12"))
	before11 := segmentBySequence(t, inventory, 11)
	before12 := segmentBySequence(t, inventory, 12)
	if before11.TimelineOrdinal != 1 || before12.TimelineOrdinal != 2 {
		t.Fatalf("initial ordinals = %d,%d, want 1,2", before11.TimelineOrdinal, before12.TimelineOrdinal)
	}

	mustClaim(t, &inventory, testInput(inventory, 9, "claim-9", string(ClaimHistorical), "media-9"))
	after11 := segmentBySequence(t, inventory, 11)
	after12 := segmentBySequence(t, inventory, 12)
	after9 := segmentBySequence(t, inventory, 9)
	if after9.TimelineOrdinal != 1 || after11.TimelineOrdinal != 2 || after12.TimelineOrdinal != 3 {
		t.Fatalf("late-prefix ordinals = %d,%d,%d, want 1,2,3", after9.TimelineOrdinal, after11.TimelineOrdinal, after12.TimelineOrdinal)
	}
	if after11.ID != before11.ID || after12.ID != before12.ID || !reflect.DeepEqual(after11.Claims, before11.Claims) || !reflect.DeepEqual(after12.Claims, before12.Claims) {
		t.Fatal("prefix insertion changed existing source identity or claim metadata")
	}
	if err := inventory.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMiddleGapRepairChangesCoverageAndTimeline(t *testing.T) {
	inventory := testInventory(t, "recording-gap")
	coverage := Coverage{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 3, DiscontinuitySequence: 1, FromSequence: 20, ToSequence: 22, State: CoverageKnownMissing, ObservedAt: testTime, Reason: "not in live window"}
	if err := ApplyCoverage(&inventory, coverage); err != nil {
		t.Fatal(err)
	}
	mustClaim(t, &inventory, testInput(inventory, 20, "claim-20", string(ClaimLiveOrigin), "twenty"))
	mustClaim(t, &inventory, testInput(inventory, 22, "claim-22", string(ClaimLiveOrigin), "twenty-two"))
	if got := CoverageAt(inventory, testCoord(inventory, 21)); got != CoverageKnownMissing {
		t.Fatalf("unrepaired middle state=%q", got)
	}
	mustClaim(t, &inventory, testInput(inventory, 21, "repair-21", string(ClaimHistorical), "twenty-one"))
	if got := CoverageAt(inventory, testCoord(inventory, 21)); got != CoveragePresent {
		t.Fatalf("repaired state=%q, want present", got)
	}
	if segmentBySequence(t, inventory, 21).TimelineOrdinal != 2 {
		t.Fatal("repaired middle segment was not projected into timeline")
	}
}

func TestOutOfOrderDiscoveryUsesCoordinatesNotArrivalOrder(t *testing.T) {
	inventory := testInventory(t, "recording-order")
	for _, sequence := range []uint64{8, 4, 7, 5} {
		mustClaim(t, &inventory, testInput(inventory, sequence, "claim-"+string(rune('0'+sequence)), string(ClaimHistorical), "bytes-"+string(rune('0'+sequence))))
	}
	for sequence, ordinal := range map[uint64]uint64{4: 1, 5: 2, 7: 3, 8: 4} {
		if got := segmentBySequence(t, inventory, sequence).TimelineOrdinal; got != ordinal {
			t.Errorf("sequence %d ordinal=%d, want %d", sequence, got, ordinal)
		}
	}
}

func TestDuplicateIdenticalBytesRetainClaimsAndOneTimelineSlot(t *testing.T) {
	inventory := testInventory(t, "recording-duplicate")
	first := testInput(inventory, 30, "origin-claim", string(ClaimLiveOrigin), "same bytes")
	if got := mustClaim(t, &inventory, first); got != DispositionAccepted {
		t.Fatalf("first disposition=%q", got)
	}
	second := first
	second.ClaimID = "peer-claim"
	second.Source = ClaimPeer
	second.PayloadPath = "recordings/" + inventory.RecordingID + "/peer/30.m4s"
	if got := mustClaim(t, &inventory, second); got != DispositionDuplicate {
		t.Fatalf("duplicate disposition=%q", got)
	}
	segment := segmentBySequence(t, inventory, 30)
	if len(segment.Claims) != 2 || segment.TimelineOrdinal != 1 || segment.SelectedClaimID != "origin-claim" {
		t.Fatalf("duplicate claims changed logical projection: %#v", segment)
	}
}

func TestConflictingBytesPreserveSelectedClaimAndMarkConflict(t *testing.T) {
	inventory := testInventory(t, "recording-conflict")
	original := testInput(inventory, 40, "original", string(ClaimLiveOrigin), "original bytes")
	mustClaim(t, &inventory, original)
	conflict := testInput(inventory, 40, "alternate", string(ClaimHistorical), "other bytes")
	if got := mustClaim(t, &inventory, conflict); got != DispositionConflict {
		t.Fatalf("conflict disposition=%q", got)
	}
	segment := segmentBySequence(t, inventory, 40)
	if segment.State != CoverageConflict || segment.SelectedClaimID != "original" || segment.TimelineOrdinal != 1 {
		t.Fatalf("conflict projection=%#v", segment)
	}
	if segment.Claims[0].PayloadPath != original.PayloadPath || segment.Claims[0].SHA256 != original.SHA256 || segment.Claims[1].PayloadPath != conflict.PayloadPath {
		t.Fatal("conflict overwrote original claim metadata")
	}
}

func TestClaimIDIdempotenceAndChangedMetadataRejection(t *testing.T) {
	inventory := testInventory(t, "recording-idempotence")
	input := testInput(inventory, 50, "stable-claim", string(ClaimLiveOrigin), "bytes")
	mustClaim(t, &inventory, input)
	revision := inventory.Revision
	if got := mustClaim(t, &inventory, input); got != DispositionAccepted {
		t.Fatalf("idempotent disposition=%q", got)
	}
	if inventory.Revision != revision || len(segmentBySequence(t, inventory, 50).Claims) != 1 {
		t.Fatal("idempotent claim changed inventory")
	}
	changed := input
	changed.PayloadPath += ".changed"
	if _, err := ApplyClaim(&inventory, changed); !errors.Is(err, ErrClaimIdentityChange) {
		t.Fatalf("changed claim ID error=%v", err)
	}
}

func TestCoverageUnknownAndDistinctStates(t *testing.T) {
	inventory := testInventory(t, "recording-coverage")
	states := []struct {
		sequence uint64
		state    CoverageState
	}{
		{1, CoverageKnownMissing},
		{2, CoverageAcquisitionFailed},
		{3, CoveragePresent},
		{4, CoverageConflict},
	}
	for _, item := range states {
		coverage := Coverage{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 3, DiscontinuitySequence: 1, FromSequence: item.sequence, ToSequence: item.sequence, State: item.state, ObservedAt: testTime}
		if err := ApplyCoverage(&inventory, coverage); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range states {
		if got := CoverageAt(inventory, testCoord(inventory, item.sequence)); got != item.state {
			t.Errorf("sequence %d state=%q, want %q", item.sequence, got, item.state)
		}
	}
	if got := CoverageAt(inventory, testCoord(inventory, 99)); got != CoverageUnknown {
		t.Fatalf("absent coverage=%q, want unknown", got)
	}
}

func TestCaptureCloseDoesNotSealArchiveAndRepairRemainsPossible(t *testing.T) {
	inventory := testInventory(t, "recording-close")
	if err := CloseCapture(&inventory); err != nil {
		t.Fatal(err)
	}
	if inventory.Capture != CaptureClosed || inventory.Archive != ArchiveRepairable {
		t.Fatalf("closed inventory states = %q/%q", inventory.Capture, inventory.Archive)
	}
	mustClaim(t, &inventory, testInput(inventory, 1, "after-close", string(ClaimHistorical), "repair after close"))
	if err := SealArchive(&inventory); err != nil {
		t.Fatal(err)
	}
	if inventory.Archive != ArchiveSealed {
		t.Fatal("explicit seal did not seal archive")
	}
	if _, err := ApplyClaim(&inventory, testInput(inventory, 2, "after-seal", string(ClaimHistorical), "rejected")); !errors.Is(err, ErrSealed) {
		t.Fatalf("sealed archive accepted repair: %v", err)
	}
}

func TestLegacyAdoptionPreservesIDsPathsHashesAndDoesNotMutateInput(t *testing.T) {
	payload := "old archived bytes"
	digest := sha256.Sum256([]byte(payload))
	programTime := testTime.Add(-time.Minute)
	recording := &domain.Recording{
		ID: "legacy-recording", AdapterID: "fixture", State: domain.StateCompleted,
		CreatedAt: testTime.Add(-time.Hour), StartedAt: testTime.Add(-time.Hour),
		Resource: &domain.ResourceReference{Type: "channel", ID: "opaque-resource"},
		Tracks: map[string]*domain.Track{"main": {
			ID: "main",
			Segments: []domain.Segment{{
				ID: "old-domain-segment", TrackID: "main", SourceEpoch: 7,
				DiscontinuitySequence: 2, Sequence: 123, ArchiveOrdinal: 1,
				Duration: 4.25, ProgramDateTime: &programTime, InitSegmentID: "init-old",
				StoragePath: "tracks/main/ordinal-1.m4s", PayloadSize: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
			}},
			InitSegments: []domain.Segment{{
				ID: "old-init", TrackID: "main", SourceEpoch: 7, DiscontinuitySequence: 2, Sequence: 123,
				StoragePath: "tracks/main/init-1.mp4", PayloadSize: 3, SHA256: digestHex("init"), IsInit: true,
			}},
		}},
		Gaps: []domain.Gap{{TrackID: "main", SourceEpoch: 7, FromSequence: 124, ToSequence: 125, DetectedAt: testTime, Reason: "legacy gap"}},
	}
	before := *recording
	beforeTracks := recording.Tracks["main"].Segments[0]
	legacyID := recording.Tracks["main"].Segments[0].ID
	inventory, err := FromLegacy(recording)
	if err != nil {
		t.Fatal(err)
	}
	if !inventory.LegacyAdoption || inventory.Capture != CaptureClosed || inventory.Archive != ArchiveRepairable {
		t.Fatalf("legacy lifecycle=%v/%q/%q", inventory.LegacyAdoption, inventory.Capture, inventory.Archive)
	}
	media := segmentBySequence(t, inventory, 123)
	var legacyClaim *Claim
	for i := range media.Claims {
		if media.Claims[i].LegacySegmentID == legacyID {
			legacyClaim = &media.Claims[i]
		}
	}
	if legacyClaim == nil || legacyClaim.Source != ClaimUnknown || legacyClaim.PayloadPath != "tracks/main/ordinal-1.m4s" || legacyClaim.Size != int64(len(payload)) || legacyClaim.SHA256 != hex.EncodeToString(digest[:]) || media.Coordinate.SourceEpoch != 7 || media.Coordinate.DiscontinuitySequence != 2 || media.Duration != 4.25 || media.InitIdentity != "init-old" {
		t.Fatalf("legacy media not preserved: %#v claim=%#v", media, legacyClaim)
	}
	if len(inventory.Segments) != 2 || segmentByKind(t, inventory, ObjectInit).Claims[0].LegacySegmentID != "old-init" {
		t.Fatal("legacy init object was not represented")
	}
	if CoverageAt(inventory, Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 7, Sequence: 124}) != CoverageKnownMissing {
		t.Fatal("legacy gap was not adopted as known missing")
	}
	if recording.ID != before.ID || recording.Tracks["main"].Segments[0] != beforeTracks || recording.Tracks["main"].Segments[0].ID != legacyID {
		t.Fatal("FromLegacy mutated its input recording")
	}
	if strings.Contains(inventory.Session.ID, "opaque-resource") {
		t.Fatal("raw resource identifier leaked into session identity")
	}
}

func TestSessionIdentityRequiresExplicitSessionReferenceToCrossRecordings(t *testing.T) {
	resource := &domain.ResourceReference{Type: "channel", ID: "channel-1"}
	first, err := NewSessionIdentity("recording-a", "fixture", resource, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSessionIdentity("recording-b", "fixture", resource, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.ResourceHash == "" || strings.Contains(first.ID, "channel-1") {
		t.Fatal("resource-only identity merged recordings or exposed raw resource")
	}
	third, err := NewSessionIdentity("recording-a", "fixture", resource, "broadcast-1")
	if err != nil {
		t.Fatal(err)
	}
	fourth, err := NewSessionIdentity("recording-b", "fixture", resource, "broadcast-1")
	if err != nil {
		t.Fatal(err)
	}
	if third.ID != fourth.ID || third.SessionHash == "" {
		t.Fatal("explicit session reference did not produce stable cross-recording identity")
	}
}

func TestFromRecordingPreservesValidSourceSessionAndAdoptionIsExplicit(t *testing.T) {
	recording := &domain.Recording{
		ID: "new-recording", AdapterID: "fixture", SourceSessionID: "session-" + strings.Repeat("a", 64),
		State: domain.StateRecording, Tracks: map[string]*domain.Track{},
	}
	inventory, err := FromRecording(recording, false)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Session.ID != recording.SourceSessionID || inventory.LegacyAdoption || inventory.Capture != CaptureOpen {
		t.Fatalf("new recording reconstruction mismatch: %#v", inventory)
	}
}

func TestFromRecordingPreservesExplicitArchiveSealing(t *testing.T) {
	sealed := &domain.Recording{
		ID: "sealed-recording", AdapterID: "fixture", State: domain.StateCompleted,
		ArchiveSealed: true, Tracks: map[string]*domain.Track{},
	}
	inventory, err := FromRecording(sealed, false)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Capture != CaptureClosed || inventory.Archive != ArchiveSealed {
		t.Fatalf("sealed recording reconstructed as %q/%q", inventory.Capture, inventory.Archive)
	}
	if _, err := ApplyClaim(&inventory, testInput(inventory, 1, "late-repair", string(ClaimHistorical), "bytes")); !errors.Is(err, ErrSealed) {
		t.Fatalf("sealed recording accepted repair after reconstruction: %v", err)
	}
}

func TestInventoryEnforcesSerializedBoundAndSafePayloadPaths(t *testing.T) {
	inventory := testInventory(t, "recording-bounds")
	input := testInput(inventory, 1, "path-test", string(ClaimLiveOrigin), "bytes")
	input.PayloadPath = "tracks/../escape.m4s"
	if _, err := ApplyClaim(&inventory, input); !errors.Is(err, ErrInvalidClaim) {
		t.Fatalf("path traversal error=%v", err)
	}
	input = testInput(inventory, 1, "large-path", string(ClaimLiveOrigin), "bytes")
	input.PayloadPath = "recordings/" + strings.Repeat("x", MaxPayloadPathBytes) + "/segment.m4s"
	if _, err := ApplyClaim(&inventory, input); !errors.Is(err, ErrInvalidClaim) {
		t.Fatalf("oversized path error=%v", err)
	}

	oversized := testInventory(t, "recording-sidecar-bound")
	oversized.Coverage = make([]Coverage, 40_000)
	for i := range oversized.Coverage {
		oversized.Coverage[i] = Coverage{
			SessionID: oversized.Session.ID, TrackID: "main", FromSequence: uint64(i), ToSequence: uint64(i),
			State: CoverageKnownMissing, ObservedAt: testTime, Reason: strings.Repeat("r", MaxReasonBytes), Kind: ObjectMedia,
		}
	}
	if err := oversized.Validate(); !errors.Is(err, ErrInvalidInventory) {
		t.Fatalf("oversized inventory error=%v", err)
	}
}

func TestIndependentInventoriesAreSafeForConcurrentOperations(t *testing.T) {
	const workers = 24
	var wait sync.WaitGroup
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			inventory := testInventory(t, "parallel-"+string(rune('a'+worker)))
			for sequence := uint64(0); sequence < 20; sequence++ {
				input := testInput(inventory, sequence, "claim-"+string(rune('a'+worker))+"-"+string(rune('a'+sequence)), string(ClaimHistorical), "payload")
				if _, err := ApplyClaim(&inventory, input); err != nil {
					errCh <- err
					return
				}
			}
			if err := inventory.Validate(); err != nil {
				errCh <- err
			}
		}()
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func segmentByKind(t *testing.T, inventory Inventory, kind ObjectKind) Segment {
	t.Helper()
	for _, segment := range inventory.Segments {
		if segment.Coordinate.Kind == kind {
			return segment
		}
	}
	t.Fatalf("segment kind %q not found", kind)
	return Segment{}
}

func digestHex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
