package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestProjectGapCoverageSeparatesHistoryFromCurrentHoles(t *testing.T) {
	recording := &domain.Recording{
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Segments: []domain.Segment{
				{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, Sequence: 10},
				{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, Sequence: 12},
				// Duplicate media coordinates do not inflate present counts.
				{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, Sequence: 12},
				{TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 0, Sequence: 20},
			}},
		},
		Gaps: []domain.Gap{
			{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, FromSequence: 10, ToSequence: 12},
			// Overlapping historical observations count as distinct records but
			// contribute one unique coordinate to the aggregate coverage summary.
			{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, FromSequence: 12, ToSequence: 13},
			{TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 0, FromSequence: 20, ToSequence: 20},
		},
	}

	views, summary := projectGapCoverage(recording)
	if summary.Status != "available" || summary.GapRecords != 3 {
		t.Fatalf("summary status/count=%s/%d", summary.Status, summary.GapRecords)
	}
	if got := *summary.UniqueSequenceCount; got != 5 {
		t.Fatalf("unique gap coordinates=%d, want 5", got)
	}
	if got := *summary.PresentSequenceCount; got != 3 {
		t.Fatalf("present coordinates=%d, want 3", got)
	}
	if got := *summary.RecoveredSequenceCount; got != 3 {
		t.Fatalf("legacy present-count alias=%d, want 3", got)
	}
	if summary.HistoricalRecoveredSequenceCount != nil {
		t.Fatalf("V1 historical recovery count=%d, want omitted without provenance", *summary.HistoricalRecoveredSequenceCount)
	}
	if got := *summary.UnresolvedSequenceCount; got != 2 {
		t.Fatalf("unresolved coordinates=%d, want 2", got)
	}
	if views[0].CoverageStatus != "partial" || *views[0].PresentSequenceCount != 2 || *views[0].RecoveredSequenceCount != 2 || *views[0].UnresolvedSequenceCount != 1 {
		t.Fatalf("first gap coverage=%+v", views[0])
	}
	if views[1].CoverageStatus != "partial" || *views[1].PresentSequenceCount != 1 || *views[1].RecoveredSequenceCount != 1 || *views[1].UnresolvedSequenceCount != 1 {
		t.Fatalf("overlapping gap coverage=%+v", views[1])
	}
	if views[2].CoverageStatus != "present" || *views[2].PresentSequenceCount != 1 || *views[2].RecoveredSequenceCount != 1 {
		t.Fatalf("present epoch coverage=%+v", views[2])
	}
}

func TestProjectGapCoverageFailsClosedOnUnrepresentableRange(t *testing.T) {
	recording := &domain.Recording{Gaps: []domain.Gap{{
		TrackID: "main", FromSequence: 0, ToSequence: math.MaxUint64,
	}}}
	views, summary := projectGapCoverage(recording)
	if summary.Status != "unavailable" || summary.UniqueSequenceCount != nil {
		t.Fatalf("overflowing range summary=%+v", summary)
	}
	if len(views) != 1 || views[0].CoverageStatus != "unavailable" || views[0].SequenceCount != nil {
		t.Fatalf("overflowing range view=%+v", views)
	}
}

func TestProjectShardedGapCoverageUsesCurrentCanonicalMedia(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 24)
	header, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{storage: store}
	summary := server.projectShardedGapCoverage(context.Background(), id, header)
	if summary.Status != "available" || summary.GapRecords != 2 {
		t.Fatalf("sharded gap summary=%+v", summary)
	}
	if summary.UniqueSequenceCount == nil || *summary.UniqueSequenceCount != 4 || summary.PresentSequenceCount == nil || *summary.PresentSequenceCount != 3 ||
		summary.RecoveredSequenceCount == nil || *summary.RecoveredSequenceCount != 3 ||
		summary.HistoricalRecoveredSequenceCount == nil || *summary.HistoricalRecoveredSequenceCount != 0 ||
		summary.UnresolvedSequenceCount == nil || *summary.UnresolvedSequenceCount != 1 {
		t.Fatalf("sharded present/recovered/unresolved counts=%d/%d/%d/%d", *summary.UniqueSequenceCount, *summary.PresentSequenceCount, *summary.RecoveredSequenceCount, *summary.UnresolvedSequenceCount)
	}
}

func TestProjectShardedGapCoverageCountsCurrentHolesNotRawObservations(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "d23456789abcdef0123456789abcdef0"
	const sessionID = "session-" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion, ID: id, SourceSessionID: sessionID,
		State: domain.StateCompleted, CreatedAt: now, StartedAt: now,
		Tracks:         map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}},
		ShardedArchive: &domain.ShardedArchiveSummary{},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	for index := uint64(0); index < 7; index++ {
		sequence := uint64(100) + index
		payload := []byte(fmt.Sprintf("media-%d", sequence))
		path := fmt.Sprintf("tracks/main/segment-%020d.ts", index+1)
		stored, err := store.SavePayload(id, path, strings.NewReader(string(payload)), int64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(payload)
		if stored.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("payload digest mismatch for sequence %d", sequence)
		}
		segment := domain.Segment{
			ID: fmt.Sprintf("seg-%020d", index+1), TrackID: "main", Sequence: sequence,
			ArchiveOrdinal: index + 1, SourceURI: fmt.Sprintf("https://source.invalid/%d.ts", sequence),
			StoragePath: path, PayloadSize: int64(len(payload)), SHA256: stored.SHA256,
		}
		coordinate := archiveindex.Coordinate{
			SessionID: sessionID, TrackID: "main", Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}
		source := archiveindex.ClaimLiveOrigin
		if index == 1 {
			source = archiveindex.ClaimHistorical
		}
		claim := archiveindex.Claim{
			ID:     "claim:" + string(source) + ":" + segment.ID + ":" + stored.SHA256,
			Source: source, AcquiredAt: now, PayloadPath: path, Size: stored.Size, SHA256: stored.SHA256,
			Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
		}
		if err := store.AppendShardedMedia(context.Background(), id, storage.V2MediaRecord{
			Coordinate: coordinate, Segment: segment, SelectedClaim: &claim, ClaimState: archiveindex.CoveragePresent,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for index := uint64(0); index < 10; index++ {
		if err := store.AppendShardedGap(context.Background(), id, domain.Gap{
			TrackID: "main", FromSequence: 100 + index, ToSequence: 100 + index,
			DetectedAt: now.Add(time.Duration(index) * time.Second), Reason: "raw observation",
		}); err != nil {
			t.Fatal(err)
		}
	}
	header, err = store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	summary := (&Server{storage: store}).projectShardedGapCoverage(context.Background(), id, header)
	if summary.Status != "available" || summary.GapRecords != 10 {
		t.Fatalf("raw observation history=%+v", summary)
	}
	if summary.UniqueSequenceCount == nil || *summary.UniqueSequenceCount != 10 || summary.PresentSequenceCount == nil || *summary.PresentSequenceCount != 7 || summary.RecoveredSequenceCount == nil || *summary.RecoveredSequenceCount != 7 || summary.HistoricalRecoveredSequenceCount == nil || *summary.HistoricalRecoveredSequenceCount != 1 || summary.UnresolvedSequenceCount == nil || *summary.UnresolvedSequenceCount != 3 {
		t.Fatalf("current canonical gap coverage=%+v, want 10 observations, 7 present, 1 historical recovery, 3 unresolved", summary)
	}
}

func TestProjectShardedLiveOriginPresentIsNotHistoricalRecovery(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "e23456789abcdef0123456789abcdef0"
	const sessionID = "session-" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	now := time.Date(2026, 10, 14, 1, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion, ID: id, SourceSessionID: sessionID,
		State: domain.StateCompleted, CreatedAt: now, StartedAt: now,
		Tracks:         map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}},
		ShardedArchive: &domain.ShardedArchiveSummary{},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	const sequence = uint64(751)
	payload := []byte("live-origin-media")
	path := "tracks/main/segment-00000000000000000001.ts"
	stored, err := store.SavePayload(id, path, strings.NewReader(string(payload)), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	segment := domain.Segment{
		ID: "seg-00000000000000000001", TrackID: "main", Sequence: sequence,
		ArchiveOrdinal: 1, SourceURI: "https://source.invalid/751.ts", StoragePath: path,
		PayloadSize: stored.Size, SHA256: stored.SHA256,
	}
	coordinate := archiveindex.Coordinate{SessionID: sessionID, TrackID: "main", Sequence: sequence, Kind: archiveindex.ObjectMedia}
	claim := archiveindex.Claim{
		ID: "live_origin:" + segment.ID + ":" + hex.EncodeToString(digest[:]), Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: now, PayloadPath: path, Size: stored.Size, SHA256: stored.SHA256,
		Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
	}
	if err := store.AppendShardedMedia(context.Background(), id, storage.V2MediaRecord{
		Coordinate: coordinate, Segment: segment, SelectedClaim: &claim, ClaimState: archiveindex.CoveragePresent,
	}); err != nil {
		t.Fatalf("append live-origin media claim: %v", err)
	}
	if err := store.AppendShardedGap(context.Background(), id, domain.Gap{
		TrackID: "main", FromSequence: sequence, ToSequence: sequence, DetectedAt: now.Add(time.Second), Reason: "older false gap observation",
	}); err != nil {
		t.Fatal(err)
	}
	header, err = store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	summary := (&Server{storage: store}).projectShardedGapCoverage(context.Background(), id, header)
	if summary.Status != "available" || summary.GapRecords != 1 || summary.UniqueSequenceCount == nil || *summary.UniqueSequenceCount != 1 ||
		summary.PresentSequenceCount == nil || *summary.PresentSequenceCount != 1 ||
		summary.RecoveredSequenceCount == nil || *summary.RecoveredSequenceCount != 1 ||
		summary.HistoricalRecoveredSequenceCount == nil || *summary.HistoricalRecoveredSequenceCount != 0 ||
		summary.UnresolvedSequenceCount == nil || *summary.UnresolvedSequenceCount != 0 {
		t.Fatalf("live-origin false gap was mislabeled as recovery or unresolved: %+v", summary)
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	var apiSummary map[string]any
	if err := json.Unmarshal(encoded, &apiSummary); err != nil {
		t.Fatal(err)
	}
	if apiSummary["present_sequence_count"] != float64(1) || apiSummary["recovered_sequence_count"] != float64(1) ||
		apiSummary["historical_recovered_sequence_count"] != float64(0) || apiSummary["unresolved_sequence_count"] != float64(0) {
		t.Fatalf("serialized gap coverage does not distinguish presence/history: %s", encoded)
	}
}

func TestRecordingDetailReturnsBoundedLatestShardedGapDiagnostics(t *testing.T) {
	store, id, _ := newShardedVODFixture(t, 0)
	base := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	for index := 1; index <= storage.MaxShardedGapHistoryEntries+5; index++ {
		gap := domain.Gap{
			TrackID: "main", SourceEpoch: 2, DiscontinuitySequence: 4,
			FromSequence: uint64(10_000 - index), ToSequence: uint64(10_000 - index),
			DetectedAt: base.Add(time.Duration(index) * time.Second), Reason: fmt.Sprintf("gap-%02d", index),
		}
		if index == storage.MaxShardedGapHistoryEntries+5 {
			gap.Acquisition = &domain.GapAcquisitionDiagnostic{
				FirstObservedAt:      gapTestTimePointer(base),
				LastAttemptStartedAt: gapTestTimePointer(base.Add(time.Second)),
				LastAttemptEndedAt:   gapTestTimePointer(base.Add(2 * time.Second)),
				Attempts:             3,
				RefreshCycles:        1,
				FailureClass:         "http_status",
				HTTPStatus:           503,
				Retryable:            true,
			}
		}
		if err := store.AppendShardedGap(context.Background(), id, gap); err != nil {
			t.Fatalf("append gap %d: %v", index, err)
		}
	}
	header, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&Server{storage: store}).recordingDetail(context.Background(), header)
	if err != nil {
		t.Fatalf("recording detail: %v", err)
	}
	if response.GapHistoryStatus != "available" || !response.GapHistoryTruncated || len(response.GapHistory) != storage.MaxShardedGapHistoryEntries {
		t.Fatalf("gap history status/truncated/count=%q/%t/%d", response.GapHistoryStatus, response.GapHistoryTruncated, len(response.GapHistory))
	}
	if got := response.GapHistory[0].Reason; got != "gap-06" {
		t.Fatalf("oldest returned observation=%q, want gap-06", got)
	}
	latest := response.GapHistory[len(response.GapHistory)-1]
	if latest.Reason != "gap-55" || latest.Acquisition == nil || latest.Acquisition.Attempts != 3 || latest.Acquisition.FailureClass != "http_status" || latest.Acquisition.HTTPStatus != 503 {
		t.Fatalf("latest gap diagnostic=%+v", latest)
	}
}

func gapTestTimePointer(value time.Time) *time.Time { return &value }
