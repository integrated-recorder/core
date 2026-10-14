package server

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

type recordingGapView struct {
	domain.Gap
	CoverageStatus                   string  `json:"coverage_status"`
	SequenceCount                    *uint64 `json:"sequence_count,omitempty"`
	PresentSequenceCount             *uint64 `json:"present_sequence_count,omitempty"`
	RecoveredSequenceCount           *uint64 `json:"recovered_sequence_count,omitempty"`
	HistoricalRecoveredSequenceCount *uint64 `json:"historical_recovered_sequence_count,omitempty"`
	UnresolvedSequenceCount          *uint64 `json:"unresolved_sequence_count,omitempty"`
}

type recordingGapCoverageSummary struct {
	Status                           string  `json:"status"`
	GapRecords                       int     `json:"gap_records"`
	UniqueSequenceCount              *uint64 `json:"unique_sequence_count,omitempty"`
	PresentSequenceCount             *uint64 `json:"present_sequence_count,omitempty"`
	RecoveredSequenceCount           *uint64 `json:"recovered_sequence_count,omitempty"`
	HistoricalRecoveredSequenceCount *uint64 `json:"historical_recovered_sequence_count,omitempty"`
	UnresolvedSequenceCount          *uint64 `json:"unresolved_sequence_count,omitempty"`
}

type gapCoordinateKey struct {
	trackID               string
	sourceEpoch           uint64
	discontinuitySequence uint64
}

type gapInterval struct {
	from uint64
	to   uint64
}

const maxGapCoverageRecords = 100_000

var errGapCoverageLimit = errors.New("gap coverage projection limit exceeded")

// projectGapCoverage compares historical gap observations with canonical
// media coordinates already present in this recording snapshot. It does not
// mutate archive coverage or reinterpret the historical gap records.
func projectGapCoverage(recording *domain.Recording) ([]recordingGapView, recordingGapCoverageSummary) {
	if recording == nil {
		return nil, recordingGapCoverageSummary{Status: "unavailable"}
	}
	present := make(map[gapCoordinateKey][]uint64)
	for trackID, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range track.Segments {
			key := gapCoordinateKey{trackID: trackID, sourceEpoch: segment.SourceEpoch, discontinuitySequence: segment.DiscontinuitySequence}
			present[key] = append(present[key], segment.Sequence)
		}
	}
	for key := range present {
		sequences := present[key]
		sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
		unique := sequences[:0]
		for _, sequence := range sequences {
			if len(unique) == 0 || unique[len(unique)-1] != sequence {
				unique = append(unique, sequence)
			}
		}
		present[key] = unique
	}

	views := make([]recordingGapView, 0, len(recording.Gaps))
	intervals := make(map[gapCoordinateKey][]gapInterval)
	allValid := true
	for _, gap := range recording.Gaps {
		view := recordingGapView{Gap: gap, CoverageStatus: "unavailable"}
		if gap.ToSequence < gap.FromSequence {
			allValid = false
			views = append(views, view)
			continue
		}
		sequenceCount, ok := inclusiveSequenceCount(gap.FromSequence, gap.ToSequence)
		if !ok {
			allValid = false
			views = append(views, view)
			continue
		}
		key := gapCoordinateKey{trackID: gap.TrackID, sourceEpoch: gap.SourceEpoch, discontinuitySequence: gap.DiscontinuitySequence}
		sequences := present[key]
		first := sort.Search(len(sequences), func(i int) bool { return sequences[i] >= gap.FromSequence })
		last := sort.Search(len(sequences), func(i int) bool { return sequences[i] > gap.ToSequence })
		presentCount := uint64(last - first)
		unresolved := sequenceCount - presentCount
		view.SequenceCount = uint64Pointer(sequenceCount)
		view.PresentSequenceCount = uint64Pointer(presentCount)
		// Keep recovered_sequence_count as its original current-presence alias.
		// Legacy V1 media has no selected-claim provenance, so the additive
		// historical count is omitted rather than inferred.
		view.RecoveredSequenceCount = uint64Pointer(presentCount)
		view.UnresolvedSequenceCount = uint64Pointer(unresolved)
		switch {
		case unresolved == 0:
			view.CoverageStatus = "present"
		case presentCount == 0:
			view.CoverageStatus = "unresolved"
		default:
			view.CoverageStatus = "partial"
		}
		views = append(views, view)
		intervals[key] = append(intervals[key], gapInterval{from: gap.FromSequence, to: gap.ToSequence})
	}

	summary := recordingGapCoverageSummary{Status: "available", GapRecords: len(recording.Gaps)}
	if !allValid {
		summary.Status = "unavailable"
		return views, summary
	}
	merged, total, ok := mergeGapIntervals(intervals)
	if !ok {
		summary.Status = "unavailable"
		return views, summary
	}
	var presentTotal uint64
	for key, ranges := range merged {
		sequences := present[key]
		for _, interval := range ranges {
			first := sort.Search(len(sequences), func(i int) bool { return sequences[i] >= interval.from })
			last := sort.Search(len(sequences), func(i int) bool { return sequences[i] > interval.to })
			countPresent := uint64(last - first)
			if math.MaxUint64-presentTotal < countPresent {
				summary.Status = "unavailable"
				return views, summary
			}
			presentTotal += countPresent
		}
	}
	summary.UniqueSequenceCount = uint64Pointer(total)
	summary.PresentSequenceCount = uint64Pointer(presentTotal)
	summary.RecoveredSequenceCount = uint64Pointer(presentTotal)
	summary.UnresolvedSequenceCount = uint64Pointer(total - presentTotal)
	return views, summary
}

func mergeGapIntervals(intervals map[gapCoordinateKey][]gapInterval) (map[gapCoordinateKey][]gapInterval, uint64, bool) {
	mergedByKey := make(map[gapCoordinateKey][]gapInterval, len(intervals))
	var total uint64
	for key, ranges := range intervals {
		sort.Slice(ranges, func(i, j int) bool {
			if ranges[i].from != ranges[j].from {
				return ranges[i].from < ranges[j].from
			}
			return ranges[i].to < ranges[j].to
		})
		merged := make([]gapInterval, 0, len(ranges))
		for _, current := range ranges {
			if len(merged) == 0 {
				merged = append(merged, current)
				continue
			}
			last := &merged[len(merged)-1]
			adjacent := last.to != math.MaxUint64 && current.from == last.to+1
			if current.from <= last.to || adjacent {
				if current.to > last.to {
					last.to = current.to
				}
				continue
			}
			merged = append(merged, current)
		}
		for _, interval := range merged {
			count, ok := inclusiveSequenceCount(interval.from, interval.to)
			if !ok || math.MaxUint64-total < count {
				return nil, 0, false
			}
			total += count
		}
		mergedByKey[key] = merged
	}
	return mergedByKey, total, true
}

func inclusiveSequenceCount(from, to uint64) (uint64, bool) {
	distance := to - from
	if distance == math.MaxUint64 {
		return 0, false
	}
	return distance + 1, true
}

func uint64Pointer(value uint64) *uint64 { return &value }

// projectShardedGapCoverage uses the bounded V2 iterators and returns only an
// aggregate. It stops at a fixed observation bound and verifies revisions so
// a concurrent repair cannot produce a mixed-snapshot count.
func (s *Server) projectShardedGapCoverage(ctx context.Context, recordingID string, header *domain.Recording) recordingGapCoverageSummary {
	summary := recordingGapCoverageSummary{Status: "unavailable"}
	if s.storage == nil || header == nil || header.ShardedArchive == nil {
		return summary
	}
	maxInt := uint64(^uint(0) >> 1)
	if header.ShardedArchive.GapCount > maxInt {
		return summary
	}
	summary.GapRecords = int(header.ShardedArchive.GapCount)
	if summary.GapRecords == 0 {
		zero := uint64(0)
		summary.Status = "available"
		summary.UniqueSequenceCount = uint64Pointer(zero)
		summary.PresentSequenceCount = uint64Pointer(zero)
		summary.RecoveredSequenceCount = uint64Pointer(zero)
		summary.HistoricalRecoveredSequenceCount = uint64Pointer(zero)
		summary.UnresolvedSequenceCount = uint64Pointer(zero)
		return summary
	}
	if summary.GapRecords > maxGapCoverageRecords {
		return summary
	}
	intervals := make(map[gapCoordinateKey][]gapInterval)
	gapRecords := 0
	err := s.storage.IterateShardedGaps(ctx, recordingID, func(gap domain.Gap) error {
		if gapRecords >= maxGapCoverageRecords {
			return errGapCoverageLimit
		}
		gapRecords++
		if gap.ToSequence < gap.FromSequence {
			return errGapCoverageLimit
		}
		key := gapCoordinateKey{trackID: gap.TrackID, sourceEpoch: gap.SourceEpoch, discontinuitySequence: gap.DiscontinuitySequence}
		intervals[key] = append(intervals[key], gapInterval{from: gap.FromSequence, to: gap.ToSequence})
		return nil
	})
	if err != nil || gapRecords != summary.GapRecords {
		return summary
	}
	merged, total, ok := mergeGapIntervals(intervals)
	if !ok {
		return summary
	}
	var present, recovered uint64
	for trackID := range header.Tracks {
		hasTrackRanges := false
		for key := range merged {
			if key.trackID == trackID {
				hasTrackRanges = true
				break
			}
		}
		if !hasTrackRanges {
			continue
		}
		err = s.storage.IterateShardedMedia(ctx, recordingID, trackID, func(record storage.V2MediaRecord) error {
			coordinate := record.Coordinate
			key := gapCoordinateKey{trackID: coordinate.TrackID, sourceEpoch: coordinate.SourceEpoch, discontinuitySequence: coordinate.DiscontinuitySequence}
			ranges := merged[key]
			index := sort.Search(len(ranges), func(i int) bool { return ranges[i].to >= coordinate.Sequence })
			if index < len(ranges) && ranges[index].from <= coordinate.Sequence {
				if present == math.MaxUint64 {
					return errGapCoverageLimit
				}
				present++
				if record.SelectedClaim != nil && record.SelectedClaim.Source == archiveindex.ClaimHistorical {
					if recovered == math.MaxUint64 {
						return errGapCoverageLimit
					}
					recovered++
				}
			}
			return nil
		})
		if err != nil {
			return summary
		}
	}
	current, err := s.storage.LoadRecordingHeader(ctx, recordingID)
	if err != nil || current == nil || current.ArchiveRevision != header.ArchiveRevision || current.TimelineRevision != header.TimelineRevision || current.ShardedArchive == nil || current.ShardedArchive.GapCount != header.ShardedArchive.GapCount || current.ShardedArchive.MediaCount != header.ShardedArchive.MediaCount {
		return summary
	}
	if recovered > present || present > total {
		return summary
	}
	summary.Status = "available"
	summary.UniqueSequenceCount = uint64Pointer(total)
	summary.PresentSequenceCount = uint64Pointer(present)
	// Preserve the existing API field as an alias for any present coordinate;
	// the new historical field carries provenance-aware recovery semantics.
	summary.RecoveredSequenceCount = uint64Pointer(present)
	summary.HistoricalRecoveredSequenceCount = uint64Pointer(recovered)
	summary.UnresolvedSequenceCount = uint64Pointer(total - present)
	return summary
}
