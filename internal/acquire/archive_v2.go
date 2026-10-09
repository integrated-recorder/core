package acquire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func isShardedRecording(recording *domain.Recording) bool {
	return recording != nil && recording.FormatVersion == storage.ShardedArchiveFormatVersion
}

func shardedHeaderCopy(recording *domain.Recording) *domain.Recording {
	header := clone(recording)
	if header == nil {
		return nil
	}
	if header.ShardedArchive == nil {
		header.ShardedArchive = &domain.ShardedArchiveSummary{}
	}
	header.Gaps = nil
	header.Snapshots = nil
	header.MetadataTimeline = nil
	for _, track := range header.Tracks {
		if track == nil {
			continue
		}
		track.Segments = nil
		track.InitSegments = nil
		track.PendingSegments = nil
		track.PendingSequences = nil
	}
	return header
}

func (m *Manager) saveRecordingHeader(recording *domain.Recording) error {
	if isShardedRecording(recording) {
		return m.store.SaveRecordingHeader(context.Background(), shardedHeaderCopy(recording))
	}
	return m.store.SaveRecording(recording)
}

func shardedGapIdentity(gap domain.Gap) string {
	gap.LivePresentationOrdinal = 0
	gap.LiveDiscontinuity = false
	gap.LiveDiscontinuitySequence = 0
	gap.LiveDuration = 0
	gap.ProgramDateTime = nil
	encoded, _ := json.Marshal(gap)
	return string(encoded)
}

func shardedGapObservationIndex(gaps []domain.Gap) map[string]domain.Gap {
	index := make(map[string]domain.Gap, len(gaps))
	for _, gap := range gaps {
		index[shardedGapIdentity(gap)] = gap
	}
	return index
}

func (m *Manager) appendShardedGapsSince(id string, previous, current []domain.Gap, revisionTarget ...*domain.Recording) error {
	e, _ := m.entry(id)
	known := make(map[string]domain.Gap, LivePlaybackWindowSize)
	for _, gap := range previous {
		known[shardedGapIdentity(gap)] = gap
	}
	if e != nil {
		e.mu.Lock()
		for key, gap := range e.shardedGapObservations {
			known[key] = gap
		}
		if e.shardedGapObservations == nil {
			e.shardedGapObservations = make(map[string]domain.Gap)
		}
		e.mu.Unlock()
	}
	var target *domain.Recording
	if len(revisionTarget) > 0 {
		target = revisionTarget[0]
	}
	for _, gap := range current {
		identity := shardedGapIdentity(gap)
		if representedByShardedGap(known, identity, gap) {
			continue
		}
		result, err := m.store.AppendShardedGapWithRevision(context.Background(), id, gap)
		if err != nil {
			return err
		}
		mergeShardedRevisionResult(target, result)
		known[identity] = gap
	}
	if e != nil {
		// Keep only keys still present in the bounded runtime projection.
		next := make(map[string]domain.Gap, len(current))
		for _, gap := range current {
			identity := shardedGapIdentity(gap)
			if representedByShardedGap(known, identity, gap) {
				next[identity] = gap
			}
		}
		e.mu.Lock()
		e.shardedGapObservations = next
		e.mu.Unlock()
	}
	return nil
}

func mergeShardedRevisionResult(recording *domain.Recording, result storage.ShardedRevisionResult) {
	if recording == nil || !isShardedRecording(recording) {
		return
	}
	if recording.ArchiveRevision < result.ArchiveRevision {
		recording.ArchiveRevision = result.ArchiveRevision
	}
	if recording.TimelineRevision < result.TimelineRevision {
		recording.TimelineRevision = result.TimelineRevision
	}
}

func gapRevisionSnapshot(recording *domain.Recording) *domain.Recording {
	if recording == nil {
		return nil
	}
	snapshot := &domain.Recording{
		FormatVersion: recording.FormatVersion, ArchiveRevision: recording.ArchiveRevision,
		TimelineRevision: recording.TimelineRevision, Gaps: append([]domain.Gap(nil), recording.Gaps...),
		Tracks: make(map[string]*domain.Track, len(recording.Tracks)),
	}
	for id, track := range recording.Tracks {
		if track == nil {
			continue
		}
		copy := &domain.Track{
			ID: track.ID, Segments: append([]domain.Segment(nil), track.Segments...),
			PendingSegments:  append([]domain.PendingSequence(nil), track.PendingSegments...),
			PendingSequences: append([]uint64(nil), track.PendingSequences...),
		}
		if track.LivePresentation != nil {
			state := *track.LivePresentation
			copy.LivePresentation = &state
		}
		snapshot.Tracks[id] = copy
	}
	return snapshot
}

// applyLegacyGapRevisions binds V1 gap-state changes to the root counters.
// V2 gap revisions publish atomically in Store.AppendShardedGapWithRevision.
func applyLegacyGapRevisions(base, next *domain.Recording) error {
	if base == nil || next == nil || isShardedRecording(next) {
		return nil
	}
	archiveNeedsCheck := next.ArchiveRevision == base.ArchiveRevision
	timelineNeedsCheck := next.TimelineRevision == base.TimelineRevision
	if !archiveNeedsCheck && !timelineNeedsCheck {
		return nil
	}
	if archiveNeedsCheck && !sameCanonicalGapState(base, next) {
		if err := advanceArchiveRevision(next); err != nil {
			return err
		}
	}
	if timelineNeedsCheck && !sameLiveGapProjection(base, next) {
		if next.TimelineRevision == ^uint64(0) {
			return errors.New("timeline revision overflow")
		}
		next.TimelineRevision++
	}
	return nil
}

type canonicalGapInterval struct {
	track string
	epoch uint64
	disc  uint64
	from  uint64
	to    uint64
}

func sameCanonicalGapState(left, right *domain.Recording) bool {
	a, b := canonicalGapIntervals(left), canonicalGapIntervals(right)
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

func canonicalGapIntervals(recording *domain.Recording) []canonicalGapInterval {
	if recording == nil {
		return nil
	}
	type key struct {
		track string
		epoch uint64
		disc  uint64
	}
	byKey := make(map[key][]canonicalGapInterval)
	present := make(map[key][]uint64)
	for trackID, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range track.Segments {
			if segment.IsInit {
				continue
			}
			identity := key{track: trackID, epoch: segment.SourceEpoch, disc: segment.DiscontinuitySequence}
			present[identity] = append(present[identity], segment.Sequence)
		}
	}
	for _, gap := range recording.Gaps {
		if gap.TrackID == "" || gap.ToSequence < gap.FromSequence {
			continue
		}
		identity := key{track: gap.TrackID, epoch: gap.SourceEpoch, disc: gap.DiscontinuitySequence}
		byKey[identity] = append(byKey[identity], canonicalGapInterval{
			track: gap.TrackID, epoch: gap.SourceEpoch, disc: gap.DiscontinuitySequence,
			from: gap.FromSequence, to: gap.ToSequence,
		})
	}
	out := make([]canonicalGapInterval, 0)
	for identity, intervals := range byKey {
		sequences := present[identity]
		sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
		sequences = compactUint64(sequences)
		for _, interval := range intervals {
			cursor := interval.from
			exhausted := false
			start := sort.Search(len(sequences), func(index int) bool { return sequences[index] >= interval.from })
			for _, sequence := range sequences[start:] {
				if sequence > interval.to {
					break
				}
				if sequence > cursor {
					part := interval
					part.to = sequence - 1
					out = append(out, part)
				}
				if sequence == ^uint64(0) {
					exhausted = true
					break
				}
				if sequence >= cursor {
					cursor = sequence + 1
				}
			}
			if !exhausted && cursor <= interval.to {
				part := interval
				part.from = cursor
				out = append(out, part)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].track != out[j].track {
			return out[i].track < out[j].track
		}
		if out[i].epoch != out[j].epoch {
			return out[i].epoch < out[j].epoch
		}
		if out[i].disc != out[j].disc {
			return out[i].disc < out[j].disc
		}
		if out[i].from != out[j].from {
			return out[i].from < out[j].from
		}
		return out[i].to < out[j].to
	})
	merged := out[:0]
	for _, interval := range out {
		if len(merged) > 0 {
			last := &merged[len(merged)-1]
			if last.track == interval.track && last.epoch == interval.epoch && last.disc == interval.disc &&
				(interval.from <= last.to || last.to != ^uint64(0) && interval.from == last.to+1) {
				if interval.to > last.to {
					last.to = interval.to
				}
				continue
			}
		}
		merged = append(merged, interval)
	}
	return merged
}

func compactUint64(values []uint64) []uint64 {
	if len(values) < 2 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

type liveGapPresentation struct {
	discontinuity bool
	discSequence  uint64
	duration      float64
	programTime   string
}

func sameLiveGapProjection(left, right *domain.Recording) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	last := maxLivePresentationOrdinal(left.Tracks["main"])
	if rightLast := maxLivePresentationOrdinal(right.Tracks["main"]); rightLast > last {
		last = rightLast
	}
	if last == 0 {
		return true
	}
	first := uint64(1)
	if last >= LivePlaybackWindowSize {
		first = last - LivePlaybackWindowSize + 1
	}
	a, b := liveGapProjection(left, first, last), liveGapProjection(right, first, last)
	if len(a) != len(b) {
		return false
	}
	for ordinal, slot := range a {
		if other, ok := b[ordinal]; !ok || other != slot {
			return false
		}
	}
	return true
}

func liveGapProjection(recording *domain.Recording, first, last uint64) map[uint64]liveGapPresentation {
	out := make(map[uint64]liveGapPresentation)
	if recording == nil {
		return out
	}
	track := recording.Tracks["main"]
	if track == nil {
		return out
	}
	present := make(map[uint64]struct{}, len(track.Segments))
	pending := make(map[uint64]struct{}, len(track.PendingSegments))
	for _, segment := range track.Segments {
		if segment.LivePresentationOrdinal >= first && segment.LivePresentationOrdinal <= last {
			present[segment.LivePresentationOrdinal] = struct{}{}
		}
	}
	for _, item := range track.PendingSegments {
		if item.LivePresentationOrdinal >= first && item.LivePresentationOrdinal <= last {
			pending[item.LivePresentationOrdinal] = struct{}{}
		}
	}
	for _, gap := range recording.Gaps {
		if gap.TrackID != track.ID || gap.LivePresentationOrdinal == 0 || gap.ToSequence < gap.FromSequence {
			continue
		}
		span := gap.ToSequence - gap.FromSequence
		if span > ^uint64(0)-gap.LivePresentationOrdinal {
			continue
		}
		gapLast := gap.LivePresentationOrdinal + span
		start, end := gap.LivePresentationOrdinal, gapLast
		if start < first {
			start = first
		}
		if end > last {
			end = last
		}
		for ordinal := start; ordinal <= end; ordinal++ {
			_, hasMedia := present[ordinal]
			_, isPending := pending[ordinal]
			if !hasMedia && !isPending {
				programTime := ""
				if gap.ProgramDateTime != nil {
					programTime = gap.ProgramDateTime.UTC().Format(time.RFC3339Nano)
				}
				out[ordinal] = liveGapPresentation{
					discontinuity: ordinal == gap.LivePresentationOrdinal && gap.LiveDiscontinuity,
					discSequence:  gap.LiveDiscontinuitySequence,
					duration:      gap.LiveDuration,
					programTime:   programTime,
				}
			}
			if ordinal == ^uint64(0) {
				break
			}
		}
	}
	return out
}

func representedByShardedGap(known map[string]domain.Gap, identity string, candidate domain.Gap) bool {
	if _, exists := known[identity]; exists {
		return true
	}
	for _, prior := range known {
		if prior.TrackID != candidate.TrackID || prior.SourceEpoch != candidate.SourceEpoch ||
			prior.DiscontinuitySequence != candidate.DiscontinuitySequence ||
			prior.Reason != candidate.Reason || prior.DetectedAt != candidate.DetectedAt ||
			candidate.FromSequence < prior.FromSequence || candidate.ToSequence > prior.ToSequence {
			continue
		}
		return true
	}
	return false
}

func (m *Manager) loadShardedRuntimeTail(recording *domain.Recording) error {
	if !isShardedRecording(recording) {
		return nil
	}
	track := recording.Tracks["main"]
	if track == nil {
		return errors.New("main track is missing")
	}
	media := make([]domain.Segment, 0, LivePlaybackWindowSize)
	gaps := make([]domain.Gap, 0, LivePlaybackWindowSize)
	seenGaps := make(map[string]struct{}, LivePlaybackWindowSize)
	neededInits := make(map[string]struct{})
	err := m.store.IterateShardedLiveSlots(context.Background(), recording.ID, track.ID, LivePlaybackWindowSize, func(slot storage.V2LiveSlot) error {
		if slot.Segment != nil {
			media = append(media, *slot.Segment)
			if slot.Segment.InitSegmentID != "" {
				neededInits[slot.Segment.InitSegmentID] = struct{}{}
			}
		}
		if slot.Gap != nil {
			identity := shardedGapIdentity(*slot.Gap)
			if _, ok := seenGaps[identity]; !ok {
				gaps = append(gaps, *slot.Gap)
				seenGaps[identity] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(media, func(i, j int) bool { return media[i].ArchiveOrdinal < media[j].ArchiveOrdinal })
	inits := make([]domain.Segment, 0, len(neededInits))
	for id := range neededInits {
		record, lookupErr := m.store.LookupShardedMediaByID(context.Background(), recording.ID, id)
		if lookupErr != nil {
			return lookupErr
		}
		if !record.Segment.IsInit || record.Segment.TrackID != track.ID {
			return storage.ErrShardedArchiveInvalid
		}
		inits = append(inits, record.Segment)
	}
	sort.Slice(inits, func(i, j int) bool {
		if inits[i].SourceEpoch != inits[j].SourceEpoch {
			return inits[i].SourceEpoch < inits[j].SourceEpoch
		}
		if inits[i].DiscontinuitySequence != inits[j].DiscontinuitySequence {
			return inits[i].DiscontinuitySequence < inits[j].DiscontinuitySequence
		}
		return inits[i].Sequence < inits[j].Sequence
	})
	track.Segments = media
	track.InitSegments = inits
	track.PendingSegments = nil
	track.PendingSequences = nil

	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].LivePresentationOrdinal != gaps[j].LivePresentationOrdinal {
			return gaps[i].LivePresentationOrdinal < gaps[j].LivePresentationOrdinal
		}
		return gaps[i].FromSequence < gaps[j].FromSequence
	})
	recording.Gaps = gaps
	return nil
}

func (m *Manager) refreshShardedRuntimeTail(recording *domain.Recording) error {
	if !isShardedRecording(recording) {
		return nil
	}
	track := recording.Tracks["main"]
	if track == nil {
		return errors.New("main track is missing")
	}
	pending := append([]domain.PendingSequence(nil), track.PendingSegments...)
	pendingSequences := append([]uint64(nil), track.PendingSequences...)
	if err := m.loadShardedRuntimeTail(recording); err != nil {
		return err
	}
	track = recording.Tracks["main"]
	selected := make(map[pendingCoordinate]struct{}, len(track.Segments))
	selectedSequences := make(map[uint64]struct{}, len(track.Segments))
	for _, segment := range track.Segments {
		selected[pendingCoordinate{epoch: segment.SourceEpoch, discontinuitySequence: segment.DiscontinuitySequence, sequence: segment.Sequence}] = struct{}{}
		if segment.SourceEpoch == 0 {
			selectedSequences[segment.Sequence] = struct{}{}
		}
	}
	for _, item := range pending {
		key := pendingCoordinate{epoch: item.SourceEpoch, discontinuitySequence: item.DiscontinuitySequence, sequence: item.Sequence}
		if _, exists := selected[key]; !exists {
			track.PendingSegments = append(track.PendingSegments, item)
		}
	}
	for _, sequence := range pendingSequences {
		if _, exists := selectedSequences[sequence]; !exists {
			track.PendingSequences = append(track.PendingSequences, sequence)
		}
	}
	return nil
}

func (m *Manager) latestShardedMetadata(id string) (*domain.MetadataRevision, error) {
	return m.store.LoadLatestShardedMetadata(context.Background(), id)
}

func mergeShardedHeader(runtime, header *domain.Recording) error {
	if runtime == nil || header == nil || runtime.ID != header.ID || !isShardedRecording(header) {
		return storage.ErrShardedArchiveInvalid
	}
	// Keep the runtime-only bounded tail while refreshing each track's scalar
	// root fields. The storage header intentionally has no media/init slices.
	for _, persisted := range header.Tracks {
		if persisted == nil {
			return storage.ErrShardedArchiveInvalid
		}
	}
	gaps, snapshots, metadata := runtime.Gaps, runtime.Snapshots, runtime.MetadataTimeline
	copy := clone(header)
	if copy == nil {
		return errors.New("recording header could not be copied")
	}
	for trackID, persisted := range copy.Tracks {
		if persisted == nil {
			return storage.ErrShardedArchiveInvalid
		}
		track := runtime.Tracks[trackID]
		if track == nil {
			continue
		}
		persisted.Segments = track.Segments
		persisted.InitSegments = track.InitSegments
		persisted.PendingSegments = track.PendingSegments
		persisted.PendingSequences = track.PendingSequences
	}
	*runtime = *copy
	runtime.Gaps = gaps
	runtime.Snapshots = snapshots
	runtime.MetadataTimeline = metadata
	return nil
}

func (m *Manager) refreshShardedHeader(runtime *domain.Recording) error {
	if !isShardedRecording(runtime) {
		return nil
	}
	header, err := m.store.LoadRecordingHeader(context.Background(), runtime.ID)
	if err != nil {
		return err
	}
	return mergeShardedHeader(runtime, header)
}

func (m *Manager) materializeShardedRecording(header *domain.Recording) (*domain.Recording, error) {
	if !isShardedRecording(header) {
		return clone(header), nil
	}
	recording := clone(header)
	if recording == nil {
		return nil, errors.New("recording could not be copied")
	}
	for trackID, track := range recording.Tracks {
		track.Segments = nil
		track.InitSegments = nil
		if err := m.store.IterateShardedTimeline(context.Background(), recording.ID, trackID, func(record storage.V2MediaRecord) error {
			track.Segments = append(track.Segments, record.Segment)
			return nil
		}); err != nil {
			return nil, err
		}
		if err := m.store.IterateShardedInitSegments(context.Background(), recording.ID, trackID, func(record storage.V2MediaRecord) error {
			track.InitSegments = append(track.InitSegments, record.Segment)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	var gaps []domain.Gap
	if err := m.store.IterateShardedGaps(context.Background(), recording.ID, func(gap domain.Gap) error {
		gaps = append(gaps, gap)
		return nil
	}); err != nil {
		return nil, err
	}
	recording.Gaps = effectiveShardedGaps(recording, gaps)
	if err := m.store.IterateShardedManifests(context.Background(), recording.ID, func(snapshot domain.ManifestSnapshot) error {
		recording.Snapshots = append(recording.Snapshots, snapshot)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := m.store.IterateShardedMetadata(context.Background(), recording.ID, func(revision domain.MetadataRevision) error {
		recording.MetadataTimeline = append(recording.MetadataTimeline, revision)
		return nil
	}); err != nil {
		return nil, err
	}
	return recording, nil
}

type shardedGapKey struct {
	trackID string
	epoch   uint64
	disc    uint64
}

// effectiveShardedGaps removes selected media coordinates from immutable gap
// observations. The full coordinate set exists only during explicit full
// materialization; live restart uses the bounded live-slot index instead.
func effectiveShardedGaps(recording *domain.Recording, gaps []domain.Gap) []domain.Gap {
	if recording == nil || len(gaps) == 0 {
		return append([]domain.Gap(nil), gaps...)
	}
	selected := make(map[shardedGapKey][]uint64)
	for trackID, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range track.Segments {
			key := shardedGapKey{trackID: trackID, epoch: segment.SourceEpoch, disc: segment.DiscontinuitySequence}
			selected[key] = append(selected[key], segment.Sequence)
		}
	}
	for key := range selected {
		sort.Slice(selected[key], func(i, j int) bool { return selected[key][i] < selected[key][j] })
	}
	out := make([]domain.Gap, 0, len(gaps))
	for _, gap := range gaps {
		sequences := selected[shardedGapKey{trackID: gap.TrackID, epoch: gap.SourceEpoch, disc: gap.DiscontinuitySequence}]
		cursor := gap.FromSequence
		exhausted := false
		for _, sequence := range sequences {
			if sequence < cursor || sequence > gap.ToSequence {
				continue
			}
			if cursor < sequence {
				fragment := shardedGapFragment(gap, cursor, sequence-1)
				out = append(out, fragment)
			}
			if sequence == ^uint64(0) {
				exhausted = true
				break
			}
			cursor = sequence + 1
			if cursor > gap.ToSequence {
				exhausted = true
				break
			}
		}
		if !exhausted && cursor <= gap.ToSequence {
			out = append(out, shardedGapFragment(gap, cursor, gap.ToSequence))
		}
	}
	return out
}

func shardedGapFragment(source domain.Gap, from, to uint64) domain.Gap {
	fragment := source
	fragment.FromSequence, fragment.ToSequence = from, to
	if source.LivePresentationOrdinal != 0 {
		offset := from - source.FromSequence
		if source.LivePresentationOrdinal <= ^uint64(0)-offset {
			fragment.LivePresentationOrdinal = source.LivePresentationOrdinal + offset
		} else {
			fragment.LivePresentationOrdinal = 0
		}
		fragment.LiveDiscontinuity = source.LiveDiscontinuity && from == source.FromSequence
		if source.LiveDiscontinuitySequence <= ^uint64(0)-offset {
			fragment.LiveDiscontinuitySequence = source.LiveDiscontinuitySequence + offset
		}
	}
	return fragment
}

func pruneShardedRuntimeTail(recording *domain.Recording) {
	if !isShardedRecording(recording) {
		return
	}
	for _, track := range recording.Tracks {
		if track == nil {
			continue
		}
		if len(track.Segments) > LivePlaybackWindowSize {
			sort.Slice(track.Segments, func(i, j int) bool {
				if track.Segments[i].LivePresentationOrdinal != track.Segments[j].LivePresentationOrdinal {
					return track.Segments[i].LivePresentationOrdinal < track.Segments[j].LivePresentationOrdinal
				}
				return track.Segments[i].ArchiveOrdinal < track.Segments[j].ArchiveOrdinal
			})
			track.Segments = append([]domain.Segment(nil), track.Segments[len(track.Segments)-LivePlaybackWindowSize:]...)
		}
		needed := make(map[string]struct{}, len(track.Segments))
		for _, segment := range track.Segments {
			if segment.InitSegmentID != "" {
				needed[segment.InitSegmentID] = struct{}{}
			}
		}
		if len(track.InitSegments) > LivePlaybackWindowSize {
			filtered := make([]domain.Segment, 0, len(needed))
			for _, init := range track.InitSegments {
				if _, ok := needed[init.ID]; ok {
					filtered = append(filtered, init)
				}
			}
			track.InitSegments = filtered
		}
	}
	if len(recording.Gaps) > LivePlaybackWindowSize {
		sort.Slice(recording.Gaps, func(i, j int) bool {
			if recording.Gaps[i].LivePresentationOrdinal != recording.Gaps[j].LivePresentationOrdinal {
				return recording.Gaps[i].LivePresentationOrdinal < recording.Gaps[j].LivePresentationOrdinal
			}
			return recording.Gaps[i].DetectedAt.Before(recording.Gaps[j].DetectedAt)
		})
		recording.Gaps = append([]domain.Gap(nil), recording.Gaps[len(recording.Gaps)-LivePlaybackWindowSize:]...)
	}
}

func appendShardedRuntimeSegment(recording *domain.Recording, segment domain.Segment) error {
	track := recording.Tracks[segment.TrackID]
	if track == nil {
		return fmt.Errorf("main track is missing")
	}
	if segment.IsInit {
		for i := range track.InitSegments {
			if track.InitSegments[i].ID == segment.ID {
				track.InitSegments[i] = segment
				pruneShardedRuntimeTail(recording)
				return nil
			}
		}
		track.InitSegments = append(track.InitSegments, segment)
	} else {
		for i := range track.Segments {
			if track.Segments[i].SourceEpoch == segment.SourceEpoch && track.Segments[i].DiscontinuitySequence == segment.DiscontinuitySequence && track.Segments[i].Sequence == segment.Sequence {
				track.Segments[i] = segment
				pruneShardedRuntimeTail(recording)
				return nil
			}
		}
		track.Segments = append(track.Segments, segment)
	}
	pruneShardedRuntimeTail(recording)
	return nil
}

// commitShardedArchiveSegment is the bounded v2 counterpart of the legacy
// root/inventory commit. It loads only the selected coordinate and its claim
// set; the supplied header is the final media visibility marker.
func (m *Manager) commitShardedArchiveSegment(e *entry, owner *OwnershipToken, segment domain.Segment, source archiveindex.ClaimSource, data []byte, result storage.PayloadResult, allowTerminalAdoption bool) (*epochMarker, error) {
	var plannedMarker *epochMarker
	var promotingOrdinal uint64
	err := m.withCanonicalMutationOwner(e, owner, allowTerminalAdoption, func() error {
		header := m.recordingSnapshotForArchive(e)
		if !isShardedRecording(header) {
			return storage.ErrShardedArchiveInvalid
		}
		if header.ArchiveSealed {
			return ErrArchiveSealed
		}
		e.mu.Lock()
		adapterID := e.adapterID
		e.mu.Unlock()
		identity, err := archiveSessionIdentity(header, adapterID, "")
		if err != nil {
			return newStorageStageError("reconstruct archive identity", err)
		}
		segment.TrackID = "main"
		segment.PayloadSize, segment.SHA256 = result.Size, result.SHA256
		coordinate := coordinateForSegment(identity.ID, segment)
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			return err
		}
		header, err = m.reconcilePendingShardedRevision(e, header, identity)
		if err != nil {
			return err
		}
		if header == nil {
			return storage.ErrNotFound
		}

		var existingRecord storage.V2MediaRecord
		existing := false
		existingRecord, err = m.store.LookupShardedMediaByCoordinate(context.Background(), header.ID, coordinate)
		if err == nil {
			existing = true
		} else if !errors.Is(err, storage.ErrNotFound) {
			return newStorageStageError("lookup sharded archive coordinate", err)
		}
		var selected *domain.Segment
		if existing {
			copy := existingRecord.Segment
			selected = &copy
			if copy.SHA256 == result.SHA256 && copy.PayloadSize == result.Size {
				segment.StoragePath = copy.StoragePath
				// The first selected source URI is immutable provenance. A refreshed
				// signed URI must not replace it on duplicate observation.
				segment.SourceURI = copy.SourceURI
			} else {
				segment.StoragePath = immutableObjectPath(segmentID, result.SHA256, segment.SourceURI)
			}
		} else {
			track := header.Tracks[segment.TrackID]
			if track == nil {
				return errors.New("main track is missing")
			}
			if !segment.IsInit {
				segment.ArchiveOrdinal = track.NextArchiveOrdinal
				if segment.ArchiveOrdinal == 0 {
					segment.ArchiveOrdinal = track.MediaHighWater + 1
				}
				if segment.ArchiveOrdinal == 0 || segment.ArchiveOrdinal == ^uint64(0) {
					return errors.New("archive ordinal overflow")
				}
				segment.ID = fmt.Sprintf("seg-%020d", segment.ArchiveOrdinal)
			}
			segment.StoragePath = immutableObjectPath(segmentID, result.SHA256, segment.SourceURI)
		}

		inventory, err := shardedCoordinateInventory(header, identity, coordinate, selected)
		if err != nil {
			return newStorageStageError("build coordinate inventory", err)
		}
		claimSet, claimSetErr := storage.V2ClaimSet{}, storage.ErrNotFound
		if existing {
			claimSet, claimSetErr = m.store.LoadShardedClaimSetByCoordinate(context.Background(), header.ID, coordinate)
		}
		if claimSetErr == nil {
			claimMetadata := segment
			if selected != nil {
				claimMetadata = *selected
			}
			for _, prior := range claimSet.Claims {
				if prior.ID == "" {
					return ErrArchiveIndexUnavailable
				}
				item := claimInputForStoredClaim(coordinate, prior, claimMetadata)
				_, applyErr := archiveindex.ApplyClaim(&inventory, item)
				if applyErr != nil {
					return newStorageStageError("replay sharded claim set", applyErr)
				}
				preferPersistedSelectedClaim(&inventory, coordinate, prior)
			}
			if len(inventory.Segments) > 0 && claimSet.SelectedClaimID != "" {
				item := &inventory.Segments[0]
				if item.SelectedClaimID != claimSet.SelectedClaimID || item.State != claimSet.State {
					return ErrArchiveIndexUnavailable
				}
			}
		} else if !errors.Is(claimSetErr, storage.ErrNotFound) {
			return newStorageStageError("load sharded claim set", claimSetErr)
		}

		input := archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: claimIdentity(source, segmentID, result.SHA256),
			Source: source, AcquiredAt: time.Now().UTC(), PayloadPath: segment.StoragePath,
			Size: result.Size, SHA256: result.SHA256, Verification: archiveindex.VerificationVerified,
			Duration: segment.Duration, ProgramDateTime: segment.ProgramDateTime, InitIdentity: segment.InitSegmentID,
		}
		if prior, ok := findInventoryClaim(inventory, input.ClaimID); ok {
			input.AcquiredAt = prior.AcquiredAt
			input.PayloadPath = prior.PayloadPath
			input.Evidence = prior.Evidence
			input.LegacySegmentID = prior.LegacySegmentID
		}
		priorRevision := inventory.Revision
		disposition, err := archiveindex.ApplyClaim(&inventory, input)
		if err != nil {
			return newStorageStageError("apply sharded archive claim", err)
		}
		claimChanged := inventory.Revision != priorRevision
		item := inventory.Segments[0]
		setClaims := make([]archiveindex.Claim, 0, len(item.Claims))
		for _, claim := range item.Claims {
			if isLegacyRootAdoptionClaim(claim, selectedSegmentID(selected)) {
				continue
			}
			setClaims = append(setClaims, claim)
		}
		if len(setClaims) == 0 {
			return ErrArchiveIndexUnavailable
		}
		set := storage.V2ClaimSet{
			SegmentID: segmentID, Coordinate: coordinate, Claims: setClaims,
			SelectedClaimID: item.SelectedClaimID, State: item.State,
		}
		var selectedClaim *archiveindex.Claim
		for index := range setClaims {
			if setClaims[index].ID == item.SelectedClaimID {
				claim := setClaims[index]
				selectedClaim = &claim
				break
			}
		}
		if selectedClaim == nil {
			return ErrArchiveIndexUnavailable
		}

		if !existing || disposition == archiveindex.DispositionConflict {
			if err := m.saveImmutablePayload(header.ID, segment.StoragePath, data, result); err != nil {
				return newStorageStageError("persist immutable archive payload", err)
			}
		}

		revision := header.ArchiveRevision
		if !existing || claimChanged {
			revision, err = nextArchiveRevisionValue(revision)
			if err != nil {
				return err
			}
			// A fresh coordinate is published by its final bounded V2 root. Its
			// immutable pages are orphan-safe until that root update, so no
			// per-segment pending-intent write/clear pair is needed. Supplemental
			// claim changes on an already-visible coordinate keep the legacy intent.
			if existing && claimChanged {
				intent := archiveRevisionIntent{Revision: revision, ClaimID: input.ClaimID, Coordinate: coordinate}
				if err := m.beginArchiveRevisionIntent(header.ID, inventory, intent); err != nil {
					return err
				}
			}
		}
		if existing && claimChanged || !existing && len(setClaims) > 1 {
			if err := m.store.SaveShardedClaimSet(context.Background(), header.ID, set); err != nil {
				return newStorageStageError("persist sharded supplemental claims", err)
			}
		}

		if existing {
			promote := source == archiveindex.ClaimLiveOrigin &&
				existingRecord.Segment.LivePresentationOrdinal == 0 &&
				existingRecord.Segment.PayloadSize == result.Size && existingRecord.Segment.SHA256 == result.SHA256
			publishExistingLive := !existingRecord.Segment.IsInit && existingRecord.Segment.LivePresentationOrdinal != 0
			var promoted domain.Segment
			if promote {
				promoted = existingRecord.Segment
				if _, err := assignLivePresentationIdentity(header, &promoted); err != nil {
					return err
				}
				promote = promoted.LivePresentationOrdinal != 0
			}
			if claimChanged {
				header.ArchiveRevision = revision
			}
			if promote || publishExistingLive {
				selectedSegment := existingRecord.Segment
				if promote {
					selectedSegment = promoted
				}
				if err := appendShardedRuntimeSegment(header, selectedSegment); err != nil {
					return err
				}
				record := existingRecord
				record.Segment = selectedSegment
				if err := m.store.PublishShardedMedia(context.Background(), shardedHeaderCopy(header), record); err != nil {
					return newStorageStageError("publish live archive index refresh", err)
				}
			} else if claimChanged {
				if err := m.store.SaveRecordingHeader(context.Background(), shardedHeaderCopy(header)); err != nil {
					return newStorageStageError("publish sharded claim revision", err)
				}
			}
			if claimChanged || promote || publishExistingLive {
				if err := m.refreshShardedHeader(header); err != nil {
					return err
				}
				if promote || publishExistingLive {
					if err := m.refreshShardedRuntimeTail(header); err != nil {
						return newStorageStageError("refresh live runtime tail", err)
					}
				}
				e.mu.Lock()
				e.recording = header
				if promote || publishExistingLive {
					e.livePlayback = buildLivePlaybackProjection(header)
				}
				e.mu.Unlock()
				if claimChanged {
					if err := m.clearArchiveRevisionIntent(header.ID, identity, revision); err != nil {
						return err
					}
				}
			}
			return nil // The previously selected immutable record stays canonical.
		}

		_ = disposition // New coordinates are accepted; conflicts require a root selection.

		if !segment.IsInit {
			// A live-origin gap already reserved a stable playlist slot. Fill
			// that slot when historical recovery later supplies its media.
			assignShardedGapPresentationIdentity(header, &segment)
			if source == archiveindex.ClaimLiveOrigin {
				track := header.Tracks[segment.TrackID]
				state := track.LivePresentation
				if segment.SourceEpoch > 0 && (state == nil || !state.HasEpochMapping || state.MappedSourceEpoch != segment.SourceEpoch) {
					sourceDiscontinuity := segment.Discontinuity
					segment.Discontinuity = true
					marker := epochMarker{ordinal: segment.ArchiveOrdinal, discontinuitySequence: segment.DiscontinuitySequence, sequence: segment.Sequence, sourceDiscontinuity: sourceDiscontinuity}
					plannedMarker = &marker
				}
				if _, err := assignLivePresentationIdentity(header, &segment); err != nil {
					return err
				}
				// The live identity mapping is established during manifest
				// observation, before this durable media commit. Preserve its
				// presentation boundary in the canonical VOD record as well; the
				// epoch-state test below cannot infer a reset after observation has
				// already advanced the mapping.
				if segment.LiveDiscontinuity {
					segment.Discontinuity = true
				}
			}
			if header.TimelineRevision == ^uint64(0) {
				return errors.New("timeline revision overflow")
			}
			header.TimelineRevision++
		}
		if header.ArchiveRevision < revision {
			header.ArchiveRevision = revision
		}
		track := header.Tracks[segment.TrackID]
		if track == nil {
			return errors.New("main track is missing")
		}
		track.PendingSegments = removePending(track.PendingSegments, segment.SourceEpoch, segment.DiscontinuitySequence, segment.Sequence)
		if segment.SourceEpoch == 0 {
			track.PendingSequences = removeSequence(track.PendingSequences, segment.Sequence)
		}
		header.Gaps = removeRepairedGap(header.Gaps, coordinate)
		header.LastError = ""
		if err := appendShardedRuntimeSegment(header, segment); err != nil {
			return err
		}
		record := storage.V2MediaRecord{Coordinate: coordinate, Segment: segment, IndexOrdinal: segment.ArchiveOrdinal, SelectedClaim: selectedClaim, ClaimState: item.State, SupplementalClaims: len(setClaims) > 1}
		if segment.IsInit {
			record.IndexOrdinal = 0
		}
		if source != archiveindex.ClaimLiveOrigin && m.beginShardedLiveSlotPromotion(e, segment) {
			promotingOrdinal = segment.LivePresentationOrdinal
		}
		if err := m.store.PublishShardedMedia(context.Background(), shardedHeaderCopy(header), record); err != nil {
			return newStorageStageError("publish sharded media", err)
		}
		if err := m.refreshShardedHeader(header); err != nil {
			return err
		}
		if !segment.IsInit && segment.LivePresentationOrdinal != 0 && source != archiveindex.ClaimLiveOrigin {
			// A historical repair can replace a persisted live gap slot. Reload
			// only the bounded durable live tail so the runtime root cannot retain
			// a stale gap after later metadata/root refreshes.
			if err := m.refreshShardedRuntimeTail(header); err != nil {
				return newStorageStageError("refresh repaired live runtime tail", err)
			}
		}
		e.mu.Lock()
		e.recording = header
		if !segment.IsInit && segment.LivePresentationOrdinal != 0 {
			// Publish the canonical tail and its cache slot under one entry lock.
			// Readers must not observe the repaired root while retaining its old gap.
			if source != archiveindex.ClaimLiveOrigin {
				e.livePlayback = buildLivePlaybackProjection(header)
			}
			m.updateLivePlaybackProjectionLocked(e, segment)
		}
		e.mu.Unlock()
		return nil
	})
	if err != nil && promotingOrdinal != 0 {
		// Root publication can succeed while a later read/response fails. Resolve
		// the live slot from durable state so the cache never keeps a false GAP.
		m.reconcileShardedLiveSlotPromotion(e, recordingID(e), promotingOrdinal)
	}
	return plannedMarker, err
}

func assignShardedGapPresentationIdentity(header *domain.Recording, segment *domain.Segment) bool {
	if header == nil || segment == nil || segment.IsInit || segment.LivePresentationOrdinal != 0 {
		return false
	}
	for _, gap := range header.Gaps {
		if gap.TrackID != segment.TrackID || gap.SourceEpoch != segment.SourceEpoch ||
			gap.DiscontinuitySequence != segment.DiscontinuitySequence ||
			segment.Sequence < gap.FromSequence || segment.Sequence > gap.ToSequence ||
			gap.LivePresentationOrdinal == 0 {
			continue
		}
		offset := segment.Sequence - gap.FromSequence
		if gap.LivePresentationOrdinal > ^uint64(0)-offset || gap.LiveDiscontinuitySequence > ^uint64(0)-offset {
			return false
		}
		segment.LivePresentationOrdinal = gap.LivePresentationOrdinal + offset
		segment.LiveDiscontinuity = gap.LiveDiscontinuity && offset == 0
		segment.LiveDiscontinuitySequence = gap.LiveDiscontinuitySequence + offset
		return true
	}
	return false
}

func selectedSegmentID(segment *domain.Segment) string {
	if segment == nil {
		return ""
	}
	return segment.ID
}

func shardedCoordinateInventory(header *domain.Recording, identity archiveindex.SessionIdentity, coordinate archiveindex.Coordinate, existing *domain.Segment) (archiveindex.Inventory, error) {
	minimal := clone(header)
	if minimal == nil {
		return archiveindex.Inventory{}, errors.New("recording header could not be copied")
	}
	minimal.Tracks = map[string]*domain.Track{}
	track := cloneTrack(header.Tracks[coordinate.TrackID])
	if track == nil {
		return archiveindex.Inventory{}, storage.ErrShardedArchiveInvalid
	}
	track.Segments = nil
	track.InitSegments = nil
	track.PendingSegments = nil
	track.PendingSequences = nil
	if existing != nil {
		if coordinate.Kind == archiveindex.ObjectInit {
			track.InitSegments = []domain.Segment{*existing}
		} else {
			track.Segments = []domain.Segment{*existing}
		}
	}
	minimal.Tracks[coordinate.TrackID] = track
	minimal.Gaps = nil
	minimal.Snapshots = nil
	minimal.MetadataTimeline = nil
	minimal.SourceSessionID = identity.ID
	return archiveindex.FromRecording(minimal, false)
}

func cloneTrack(track *domain.Track) *domain.Track {
	if track == nil {
		return nil
	}
	copy := *track
	copy.Segments = append([]domain.Segment(nil), track.Segments...)
	copy.InitSegments = append([]domain.Segment(nil), track.InitSegments...)
	copy.PendingSegments = append([]domain.PendingSequence(nil), track.PendingSegments...)
	copy.PendingSequences = append([]uint64(nil), track.PendingSequences...)
	return &copy
}

func claimInputForStoredClaim(coordinate archiveindex.Coordinate, claim archiveindex.Claim, segment domain.Segment) archiveindex.ClaimInput {
	return archiveindex.ClaimInput{
		Coordinate: coordinate, ClaimID: claim.ID, Source: claim.Source,
		AcquiredAt: claim.AcquiredAt, PayloadPath: claim.PayloadPath,
		Size: claim.Size, SHA256: claim.SHA256, Verification: claim.Verification,
		Evidence: claim.Evidence, LegacySegmentID: claim.LegacySegmentID,
		Duration: segment.Duration, ProgramDateTime: segment.ProgramDateTime,
		InitIdentity: segment.InitSegmentID,
	}
}
