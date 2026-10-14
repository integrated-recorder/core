package acquire

import (
	"context"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

// canonicalGapCandidate is a source observation that may indicate a missing
// media coordinate. It is not itself authority to publish a gap.
type canonicalGapCandidate struct {
	trackID               string
	sourceEpoch           uint64
	discontinuitySequence uint64
	fromSequence          uint64
	toSequence            uint64
	reason                string
	acquisition           *domain.GapAcquisitionDiagnostic
	annotation            *liveGapAnnotation
}

type liveGapAnnotation struct {
	duration        float64
	programDateTime *time.Time
	discontinuity   bool
}

type gapCommitOutcome struct {
	present  bool
	missing  bool
	appended int
}

// canonicalCoordinatePresent resolves current archive truth. V1 keeps its
// canonical segment list in the root; V2 resolves the selected media claim
// through the bounded coordinate index and never treats the runtime root tail
// as proof of absence.
func (m *Manager) canonicalCoordinatePresent(ctx context.Context, recording *domain.Recording, trackID string, epoch, discontinuitySequence, sequence uint64) (bool, error) {
	if recording == nil {
		return false, errors.New("recording is unavailable for canonical media lookup")
	}
	track := recording.Tracks[trackID]
	if track == nil {
		return false, errors.New("track is missing for canonical media lookup")
	}
	if !isShardedRecording(recording) {
		for _, segment := range track.Segments {
			if segment.SourceEpoch == epoch && segment.DiscontinuitySequence == discontinuitySequence && segment.Sequence == sequence {
				return true, nil
			}
		}
		return false, nil
	}
	if m == nil || m.store == nil {
		return false, errors.New("canonical media store is unavailable")
	}
	coordinate := archiveindex.Coordinate{
		SessionID: recording.SourceSessionID, TrackID: trackID,
		SourceEpoch: epoch, DiscontinuitySequence: discontinuitySequence,
		Sequence: sequence, Kind: archiveindex.ObjectMedia,
	}
	_, err := m.store.LookupShardedMediaByCoordinate(ctx, recording.ID, coordinate)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// commitGapIfStillMissing must run while the caller holds entry.persistMu and
// the Runtime Host canonical commit fence. The media lookup and gap append
// therefore share the same serialization domain as canonical media commits:
// PRESENT suppresses a new gap; NOT FOUND may publish one; storage errors are
// returned and never interpreted as absence.
func (m *Manager) commitGapIfStillMissing(recording *domain.Recording, candidate canonicalGapCandidate) (gapCommitOutcome, error) {
	var outcome gapCommitOutcome
	if recording == nil || candidate.trackID == "" || candidate.toSequence < candidate.fromSequence {
		return outcome, errors.New("invalid canonical gap candidate")
	}
	track := recording.Tracks[candidate.trackID]
	if track == nil {
		return outcome, errors.New("track is missing for canonical gap commit")
	}
	flushMissing := func(from, to uint64) {
		before := len(recording.Gaps)
		addMissingRangesDS(recording, track, candidate.sourceEpoch, candidate.discontinuitySequence, from, to, candidate.reason, nil)
		outcome.appended += len(recording.Gaps) - before
		if candidate.acquisition != nil && candidate.fromSequence >= from && candidate.fromSequence <= to {
			for index := before; index < len(recording.Gaps); index++ {
				gap := &recording.Gaps[index]
				if gap.TrackID == candidate.trackID && gap.SourceEpoch == candidate.sourceEpoch && gap.DiscontinuitySequence == candidate.discontinuitySequence && candidate.fromSequence >= gap.FromSequence && candidate.fromSequence <= gap.ToSequence {
					copy := *candidate.acquisition
					gap.Acquisition = &copy
					break
				}
			}
		}
		if candidate.annotation != nil && candidate.fromSequence == candidate.toSequence {
			annotateLiveGap(recording, track, candidate.sourceEpoch, candidate.discontinuitySequence, candidate.fromSequence,
				candidate.annotation.duration, candidate.annotation.programDateTime, candidate.annotation.discontinuity)
		}
		for sequence := from; ; sequence++ {
			removePendingCoordinate(track, candidate.sourceEpoch, candidate.discontinuitySequence, sequence)
			if sequence == to {
				break
			}
		}
	}

	var missingFrom uint64
	missingRun := false
	for sequence := candidate.fromSequence; ; sequence++ {
		present, err := m.canonicalCoordinatePresent(context.Background(), recording, candidate.trackID,
			candidate.sourceEpoch, candidate.discontinuitySequence, sequence)
		if err != nil {
			return gapCommitOutcome{}, err
		}
		if present {
			outcome.present = true
			if missingRun {
				flushMissing(missingFrom, sequence-1)
				missingRun = false
			}
			removePendingCoordinate(track, candidate.sourceEpoch, candidate.discontinuitySequence, sequence)
		} else {
			outcome.missing = true
			if !missingRun {
				missingFrom = sequence
				missingRun = true
			}
		}
		if sequence == candidate.toSequence {
			if missingRun {
				flushMissing(missingFrom, sequence)
			}
			break
		}
	}
	return outcome, nil
}

func removePendingCoordinate(track *domain.Track, epoch, discontinuitySequence, sequence uint64) {
	if track == nil {
		return
	}
	track.PendingSegments = removePending(track.PendingSegments, epoch, discontinuitySequence, sequence)
	if epoch == 0 && discontinuitySequence == 0 {
		track.PendingSequences = removeSequence(track.PendingSequences, sequence)
	}
}

// commitGapCandidates applies a bounded set of observed gap candidates. The
// caller must already hold entry.persistMu and the canonical commit fence.
func (m *Manager) commitGapCandidates(recording *domain.Recording, candidates []canonicalGapCandidate) error {
	for _, candidate := range candidates {
		if _, err := m.commitGapIfStillMissing(recording, candidate); err != nil {
			return err
		}
	}
	return nil
}
