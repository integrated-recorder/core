package acquire

import (
	"context"
	"testing"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestCanonicalGapCommitPreservesV1RootSemantics(t *testing.T) {
	manager := &Manager{}
	track := &domain.Track{ID: "main"}
	recording := &domain.Recording{ID: "legacy", Tracks: map[string]*domain.Track{"main": track}}
	candidate := canonicalGapCandidate{
		trackID: "main", sourceEpoch: 2, discontinuitySequence: 4,
		fromSequence: 9, toSequence: 9, reason: "legacy missing coordinate",
	}
	outcome, err := manager.commitGapIfStillMissing(recording, candidate)
	if err != nil || !outcome.missing || outcome.appended != 1 || len(recording.Gaps) != 1 {
		t.Fatalf("V1 missing coordinate was not committed: outcome=%+v gaps=%#v err=%v", outcome, recording.Gaps, err)
	}

	track.Segments = append(track.Segments, domain.Segment{
		TrackID: "main", SourceEpoch: 2, DiscontinuitySequence: 4, Sequence: 10,
	})
	candidate.fromSequence, candidate.toSequence = 10, 10
	outcome, err = manager.commitGapIfStillMissing(recording, candidate)
	if err != nil || !outcome.present || outcome.appended != 0 || len(recording.Gaps) != 1 {
		t.Fatalf("V1 root-present coordinate received a gap: outcome=%+v gaps=%#v err=%v", outcome, recording.Gaps, err)
	}
	if present, err := manager.canonicalCoordinatePresent(context.Background(), recording, "main", 2, 4, 10); err != nil || !present {
		t.Fatalf("V1 canonical segment lookup=%t err=%v", present, err)
	}
}
