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
	deferred bool
}

// maxGapResolutionCoordinates bounds canonical coordinate lookups and the
// derived coverage-state work performed for one candidate while the recording
// commit fence is held. The V2 coverage index currently materializes fixed
// sequence buckets, so resolving an arbitrarily large source interval in one
// commit would still be unbounded even if the media lookup were range-aware.
const maxGapResolutionCoordinates uint64 = 256

func sequenceAdvanceGapBounds(hasPrevious bool, previous, next uint64) (from, to uint64, ok bool) {
	if !hasPrevious || previous == ^uint64(0) || next <= previous || next-previous <= 1 {
		return 0, 0, false
	}
	return previous + 1, next - 1, true
}

// resumeGapCandidateFromCursor skips the prefix that an earlier bounded pass
// already resolved for the same source coordinate domain. It is only used for
// inferred sequence ranges; explicit source gaps and terminal task failures
// remain independent observations.
func resumeGapCandidateFromCursor(candidate canonicalGapCandidate, cursor *domain.GapResolutionCursor) (canonicalGapCandidate, bool) {
	if cursor == nil || cursor.SourceEpoch != candidate.sourceEpoch || cursor.DiscontinuitySequence != candidate.discontinuitySequence {
		return candidate, candidate.toSequence >= candidate.fromSequence
	}
	if candidate.toSequence <= cursor.Sequence {
		return canonicalGapCandidate{}, false
	}
	if candidate.fromSequence <= cursor.Sequence {
		if cursor.Sequence == ^uint64(0) {
			return canonicalGapCandidate{}, false
		}
		candidate.fromSequence = cursor.Sequence + 1
	}
	return candidate, candidate.toSequence >= candidate.fromSequence
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
	// Compare the difference instead of computing (to-from)+1, which can
	// overflow for the full uint64 range. This primitive never scans a range
	// above the fixed budget; commitGapCandidates splits larger observations.
	if candidate.toSequence-candidate.fromSequence >= maxGapResolutionCoordinates {
		return gapCommitOutcome{deferred: true}, nil
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

// commitGapCandidates applies observed gap candidates while limiting every
// candidate's synchronous coordinate work to maxGapResolutionCoordinates.
// Remainders are returned to the caller so its durable gap-resolution cursor
// can resume them on a later manifest. The caller must already hold
// entry.persistMu and the canonical commit fence.
func (m *Manager) commitGapCandidates(recording *domain.Recording, candidates []canonicalGapCandidate) ([]canonicalGapCandidate, error) {
	var deferred []canonicalGapCandidate
	for _, candidate := range candidates {
		if candidate.toSequence < candidate.fromSequence {
			return nil, errors.New("invalid canonical gap candidate")
		}
		chunk, remainder, hasRemainder := splitCanonicalGapCandidate(candidate)
		outcome, err := m.commitGapIfStillMissing(recording, chunk)
		if err != nil {
			return nil, err
		}
		if outcome.deferred {
			return nil, errors.New("bounded canonical gap chunk was unexpectedly deferred")
		}
		if hasRemainder {
			deferred = append(deferred, remainder)
		}
	}
	return deferred, nil
}

// splitCanonicalGapCandidate bounds a candidate before any canonical lookup
// or coverage publication. The difference comparison avoids length overflow;
// when a remainder exists, the bounded end is strictly below the source end,
// so both additions are safe at uint64 boundaries.
func splitCanonicalGapCandidate(candidate canonicalGapCandidate) (chunk, remainder canonicalGapCandidate, hasRemainder bool) {
	chunk = candidate
	if candidate.toSequence < candidate.fromSequence || candidate.toSequence-candidate.fromSequence < maxGapResolutionCoordinates {
		return chunk, canonicalGapCandidate{}, false
	}
	chunk.toSequence = candidate.fromSequence + maxGapResolutionCoordinates - 1
	remainder = candidate
	remainder.fromSequence = chunk.toSequence + 1
	remainder.acquisition = nil
	remainder.annotation = nil
	return chunk, remainder, true
}
