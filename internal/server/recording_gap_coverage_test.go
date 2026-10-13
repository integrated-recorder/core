package server

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestProjectGapCoverageSeparatesHistoryFromCurrentHoles(t *testing.T) {
	recording := &domain.Recording{
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Segments: []domain.Segment{
				{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, Sequence: 10},
				{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 3, Sequence: 12},
				// Duplicate media coordinates do not inflate recovery counts.
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
	if got := *summary.RecoveredSequenceCount; got != 3 {
		t.Fatalf("recovered coordinates=%d, want 3", got)
	}
	if got := *summary.UnresolvedSequenceCount; got != 2 {
		t.Fatalf("unresolved coordinates=%d, want 2", got)
	}
	if views[0].CoverageStatus != "partial" || *views[0].RecoveredSequenceCount != 2 || *views[0].UnresolvedSequenceCount != 1 {
		t.Fatalf("first gap coverage=%+v", views[0])
	}
	if views[1].CoverageStatus != "partial" || *views[1].RecoveredSequenceCount != 1 || *views[1].UnresolvedSequenceCount != 1 {
		t.Fatalf("overlapping gap coverage=%+v", views[1])
	}
	if views[2].CoverageStatus != "recovered" || *views[2].RecoveredSequenceCount != 1 {
		t.Fatalf("recovered epoch coverage=%+v", views[2])
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
	if *summary.UniqueSequenceCount != 4 || *summary.RecoveredSequenceCount != 3 || *summary.UnresolvedSequenceCount != 1 {
		t.Fatalf("sharded recovered/unresolved counts=%d/%d/%d", *summary.UniqueSequenceCount, *summary.RecoveredSequenceCount, *summary.UnresolvedSequenceCount)
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
