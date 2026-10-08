package acquire

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

// LivePlaybackWindowSize bounds both the persisted-in-memory live tail and
// each browser-facing playlist projection.
const LivePlaybackWindowSize = 12

var errLivePresentationOverflow = errors.New("live presentation identity overflow")

// LivePlaybackView is the small, immutable snapshot needed to render one live
// HLS response. It intentionally does not expose or clone the full Recording.
type LivePlaybackView struct {
	RecordingID      string
	State            domain.RecordingState
	TrackID          string
	TrackFound       bool
	Bandwidth        int64
	TimelineRevision uint64
	Segments         []domain.Segment
	Slots            []LivePlaybackSlot
	InitSegments     map[string]domain.Segment
}

// LivePlaybackSlot represents one stable HLS media-sequence position. A slot
// can be unavailable while acquisition is pending or a canonical gap exists.
type LivePlaybackSlot struct {
	Ordinal                   uint64
	Segment                   *domain.Segment
	Unavailable               bool
	Pending                   bool
	LiveDiscontinuity         bool
	LiveDiscontinuitySequence uint64
	Duration                  float64
	ProgramDateTime           *time.Time
	InitSegmentID             string
}

type livePlaybackProjection struct {
	slots map[uint64]LivePlaybackSlot
	inits map[string]domain.Segment
}

func newLivePlaybackProjection() *livePlaybackProjection {
	return &livePlaybackProjection{slots: make(map[uint64]LivePlaybackSlot, LivePlaybackWindowSize), inits: make(map[string]domain.Segment)}
}

// LivePlaybackSnapshot copies only the bounded active live tail and init maps
// referenced by it. The canonical recording root remains available through
// Get; live playlist reloads do not clone or sort its full segment history.
func (m *Manager) LivePlaybackSnapshot(ctx context.Context, id string) (LivePlaybackView, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return LivePlaybackView{}, err
		}
	}
	e, ok := m.entry(id)
	if !ok {
		return LivePlaybackView{}, storage.ErrNotFound
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil {
		return LivePlaybackView{}, storage.ErrNotFound
	}
	view := LivePlaybackView{
		RecordingID: id, State: e.recording.State,
		TimelineRevision: e.recording.TimelineRevision,
	}
	if view.State != domain.StateRecording {
		return view, nil
	}
	track := e.recording.Tracks["main"]
	if track == nil {
		return view, nil
	}
	view.TrackFound = true
	view.TrackID = track.ID
	view.Bandwidth = track.Bandwidth
	state := track.LivePresentation
	if state == nil || state.NextOrdinal == 0 {
		// Active legacy roots are migrated at load/adoption. A request must not
		// fall back to scanning the full track when that invariant is absent.
		return view, nil
	}
	lastOrdinal := state.NextOrdinal - 1
	firstOrdinal := uint64(1)
	if lastOrdinal >= LivePlaybackWindowSize {
		firstOrdinal = lastOrdinal - LivePlaybackWindowSize + 1
	}
	if state.FirstPresentationOrdinal > firstOrdinal {
		firstOrdinal = state.FirstPresentationOrdinal
	}
	if firstOrdinal > lastOrdinal {
		return view, nil
	}
	if e.livePlayback == nil {
		// Active starts and handover adoption initialize this cache before the
		// worker starts. Do not rebuild it from the full archive on a request.
		e.livePlayback = newLivePlaybackProjection()
	}
	view.InitSegments = make(map[string]domain.Segment, len(e.livePlayback.inits))
	for ordinal := firstOrdinal; ordinal <= lastOrdinal; ordinal++ {
		slot, ok := e.livePlayback.slots[ordinal]
		if !ok {
			// A missing projection slot is unknown/in-flight, not an observed
			// source GAP. Do not publish a playlist beyond it.
			break
		}
		if slot.Pending && slot.Segment == nil || slot.Segment == nil && !slot.Unavailable {
			break
		}
		if slot.Segment != nil {
			segment := clonePlaybackSegment(*slot.Segment)
			slot.Segment = &segment
			view.Segments = append(view.Segments, segment)
			if segment.InitSegmentID != "" {
				if init, found := e.livePlayback.inits[segment.InitSegmentID]; found {
					view.InitSegments[init.ID] = clonePlaybackSegment(init)
				}
			}
		}
		if slot.ProgramDateTime != nil {
			value := *slot.ProgramDateTime
			slot.ProgramDateTime = &value
		}
		view.Slots = append(view.Slots, slot)
		if ordinal == ^uint64(0) {
			break
		}
	}
	return view, nil
}

func buildLivePlaybackProjection(recording *domain.Recording) *livePlaybackProjection {
	projection := newLivePlaybackProjection()
	if recording == nil {
		return projection
	}
	track := recording.Tracks["main"]
	if track == nil {
		return projection
	}
	lastOrdinal := maxLivePresentationOrdinal(track)
	if lastOrdinal == 0 {
		return projection
	}
	firstOrdinal := uint64(1)
	if lastOrdinal >= LivePlaybackWindowSize {
		firstOrdinal = lastOrdinal - LivePlaybackWindowSize + 1
	}
	for _, segment := range track.Segments {
		if segment.LivePresentationOrdinal < firstOrdinal || segment.LivePresentationOrdinal > lastOrdinal {
			continue
		}
		projection.slots[segment.LivePresentationOrdinal] = mediaLivePlaybackSlot(segment)
	}
	for _, gap := range recording.Gaps {
		insertLivePlaybackGapInWindow(projection, gap, firstOrdinal, lastOrdinal)
	}
	for _, pending := range track.PendingSegments {
		if pending.LivePresentationOrdinal >= firstOrdinal && pending.LivePresentationOrdinal <= lastOrdinal {
			insertLivePlaybackPending(projection, pending)
		}
	}
	pruneLivePlaybackSlots(projection, lastOrdinal)
	refreshLivePlaybackInits(projection, track)
	return projection
}

func mediaLivePlaybackSlot(segment domain.Segment) LivePlaybackSlot {
	copy := clonePlaybackSegment(segment)
	return LivePlaybackSlot{
		Ordinal: segment.LivePresentationOrdinal, Segment: &copy,
		LiveDiscontinuity:         segment.LiveDiscontinuity,
		LiveDiscontinuitySequence: segment.LiveDiscontinuitySequence,
		Duration:                  segment.Duration, ProgramDateTime: copy.ProgramDateTime,
		InitSegmentID: segment.InitSegmentID,
	}
}

func insertLivePlaybackGap(projection *livePlaybackProjection, gap domain.Gap, window int) {
	if projection == nil || gap.LivePresentationOrdinal == 0 || gap.ToSequence < gap.FromSequence {
		return
	}
	gapLast := gap.LivePresentationOrdinal + gap.ToSequence - gap.FromSequence
	if gapLast < gap.LivePresentationOrdinal {
		return
	}
	first := gap.LivePresentationOrdinal
	if gapLast-first >= uint64(window) {
		first = gapLast - uint64(window) + 1
	}
	insertLivePlaybackGapInWindow(projection, gap, first, gapLast)
}

func insertLivePlaybackGapInWindow(projection *livePlaybackProjection, gap domain.Gap, first, last uint64) {
	if projection == nil || gap.LivePresentationOrdinal == 0 || gap.ToSequence < gap.FromSequence || first > last {
		return
	}
	gapLast := gap.LivePresentationOrdinal + gap.ToSequence - gap.FromSequence
	if gapLast < gap.LivePresentationOrdinal || gapLast < first || gap.LivePresentationOrdinal > last {
		return
	}
	if first < gap.LivePresentationOrdinal {
		first = gap.LivePresentationOrdinal
	}
	if last > gapLast {
		last = gapLast
	}
	for ordinal := first; ordinal <= last; ordinal++ {
		offset := ordinal - gap.LivePresentationOrdinal
		if ordinal == 0 {
			continue
		}
		if current, ok := projection.slots[ordinal]; ok && current.Segment != nil {
			continue
		}
		projection.slots[ordinal] = LivePlaybackSlot{
			Ordinal: ordinal, Unavailable: true,
			LiveDiscontinuity:         offset == 0 && gap.LiveDiscontinuity,
			LiveDiscontinuitySequence: gap.LiveDiscontinuitySequence,
			Duration:                  gap.LiveDuration,
		}
		if ordinal == ^uint64(0) {
			break
		}
	}
}

func insertLivePlaybackPending(projection *livePlaybackProjection, pending domain.PendingSequence) {
	if projection == nil || pending.LivePresentationOrdinal == 0 {
		return
	}
	if current, ok := projection.slots[pending.LivePresentationOrdinal]; ok && current.Segment != nil {
		return
	}
	var programTime *time.Time
	if pending.ProgramDateTime != nil {
		value := *pending.ProgramDateTime
		programTime = &value
	}
	projection.slots[pending.LivePresentationOrdinal] = LivePlaybackSlot{
		Ordinal: pending.LivePresentationOrdinal, Unavailable: true, Pending: true,
		LiveDiscontinuity:         pending.LiveDiscontinuity,
		LiveDiscontinuitySequence: pending.LiveDiscontinuitySequence,
		Duration:                  pending.Duration, ProgramDateTime: programTime,
		InitSegmentID: pending.InitSegmentID,
	}
}

func pruneLivePlaybackSlots(projection *livePlaybackProjection, lastOrdinal uint64) {
	if projection == nil || lastOrdinal == 0 {
		return
	}
	first := uint64(1)
	if lastOrdinal >= LivePlaybackWindowSize {
		first = lastOrdinal - LivePlaybackWindowSize + 1
	}
	for ordinal := range projection.slots {
		if ordinal < first || ordinal > lastOrdinal {
			delete(projection.slots, ordinal)
		}
	}
}

func refreshLivePlaybackInits(projection *livePlaybackProjection, track *domain.Track) {
	if projection.inits == nil {
		projection.inits = make(map[string]domain.Segment)
	}
	needed := make(map[string]struct{}, len(projection.slots))
	for _, slot := range projection.slots {
		if slot.Segment != nil && slot.Segment.InitSegmentID != "" {
			needed[slot.Segment.InitSegmentID] = struct{}{}
		}
	}
	for id := range projection.inits {
		if _, ok := needed[id]; !ok {
			delete(projection.inits, id)
		}
	}
	for _, init := range track.InitSegments {
		if _, ok := needed[init.ID]; ok {
			projection.inits[init.ID] = clonePlaybackSegment(init)
		}
	}
}

func (m *Manager) updateLivePlaybackProjection(e *entry, segment domain.Segment) {
	if e == nil || !segment.IsInit && segment.LivePresentationOrdinal == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil {
		return
	}
	track := e.recording.Tracks["main"]
	if track == nil {
		return
	}
	state := track.LivePresentation
	if state == nil {
		state = livePresentationState(track)
	}
	if segment.LivePresentationOrdinal == 0 || state.FirstPresentationOrdinal != 0 && segment.LivePresentationOrdinal < state.FirstPresentationOrdinal {
		return
	}
	if e.livePlayback == nil {
		e.livePlayback = newLivePlaybackProjection()
	}
	if segment.IsInit {
		for _, slot := range e.livePlayback.slots {
			if slot.Segment != nil && slot.Segment.InitSegmentID == segment.ID {
				e.livePlayback.inits[segment.ID] = clonePlaybackSegment(segment)
				return
			}
		}
		return
	}
	e.livePlayback.slots[segment.LivePresentationOrdinal] = mediaLivePlaybackSlot(segment)
	pruneLivePlaybackSlots(e.livePlayback, maxLivePresentationOrdinal(track))
	refreshLivePlaybackInits(e.livePlayback, track)
}

func (m *Manager) updateLivePlaybackPending(e *entry, pending domain.PendingSequence) {
	if e == nil || pending.LivePresentationOrdinal == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil || e.recording.Tracks["main"] == nil {
		return
	}
	if e.livePlayback == nil {
		e.livePlayback = newLivePlaybackProjection()
	}
	insertLivePlaybackPending(e.livePlayback, pending)
	pruneLivePlaybackSlots(e.livePlayback, maxLivePresentationOrdinal(e.recording.Tracks["main"]))
}

func (m *Manager) updateLivePlaybackGap(e *entry, gap domain.Gap) {
	if e == nil || gap.LivePresentationOrdinal == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil || e.recording.Tracks["main"] == nil {
		return
	}
	if e.livePlayback == nil {
		e.livePlayback = newLivePlaybackProjection()
	}
	insertLivePlaybackGap(e.livePlayback, gap, LivePlaybackWindowSize)
	pruneLivePlaybackSlots(e.livePlayback, maxLivePresentationOrdinal(e.recording.Tracks["main"]))
}

func (m *Manager) refreshLivePlaybackObservation(e *entry, recording *domain.Recording, playlist []hls.MediaSegment, epoch uint64) {
	if e == nil || recording == nil || recording.Tracks["main"] == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil || e.livePlayback == nil {
		return
	}
	track := recording.Tracks["main"]
	lastOrdinal := maxLivePresentationOrdinal(track)
	state := track.LivePresentation
	if state == nil {
		state = livePresentationState(track)
	}
	pruneLivePlaybackSlots(e.livePlayback, lastOrdinal)
	for _, source := range playlist {
		ordinal, discSequence, boundary, ok := livePresentationForState(state, epoch, source.DiscontinuitySequence, source.Sequence, source.Discontinuity)
		if !ok || !liveOrdinalInWindow(ordinal, lastOrdinal) {
			continue
		}
		if source.Gap {
			continue // canonical gap entries below carry the persisted slot.
		}
		key := pendingCoordinate{epoch: epoch, discontinuitySequence: source.DiscontinuitySequence, sequence: source.Sequence}
		if current, found := e.livePlayback.slots[ordinal]; found && current.Segment != nil {
			continue
		}
		if captured, found := findTrackSegmentCoordinate(track, key); found {
			if captured.LivePresentationOrdinal != 0 {
				e.livePlayback.slots[ordinal] = mediaLivePlaybackSlot(captured)
			}
			continue
		}
		pending := domain.PendingSequence{
			SourceEpoch: epoch, DiscontinuitySequence: source.DiscontinuitySequence,
			Sequence: source.Sequence, LivePresentationOrdinal: ordinal,
			LiveDiscontinuity: boundary, LiveDiscontinuitySequence: discSequence,
			Duration: source.Duration, ProgramDateTime: source.ProgramTime,
		}
		if source.Init != nil {
			pending.InitSegmentID = initSegmentID(*source.Init, epoch, source.DiscontinuitySequence)
		}
		insertLivePlaybackPending(e.livePlayback, pending)
	}
	for _, gap := range recording.Gaps {
		insertLivePlaybackGap(e.livePlayback, gap, LivePlaybackWindowSize)
	}
	pruneLivePlaybackSlots(e.livePlayback, lastOrdinal)
	refreshLivePlaybackInits(e.livePlayback, track)
}

type pendingCoordinate struct {
	epoch, discontinuitySequence, sequence uint64
}

func findTrackSegmentCoordinate(track *domain.Track, coordinate pendingCoordinate) (domain.Segment, bool) {
	if track == nil {
		return domain.Segment{}, false
	}
	for _, segment := range track.Segments {
		if segment.SourceEpoch == coordinate.epoch && segment.DiscontinuitySequence == coordinate.discontinuitySequence && segment.Sequence == coordinate.sequence {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

func liveOrdinalInWindow(ordinal, last uint64) bool {
	if ordinal == 0 || last == 0 || ordinal > last {
		return false
	}
	first := uint64(1)
	if last >= LivePlaybackWindowSize {
		first = last - LivePlaybackWindowSize + 1
	}
	return ordinal >= first
}

func maxLivePresentationOrdinal(track *domain.Track) uint64 {
	if track != nil && track.LivePresentation != nil && track.LivePresentation.NextOrdinal > 0 {
		return track.LivePresentation.NextOrdinal - 1
	}
	state := track.LivePresentation
	if state == nil {
		state = livePresentationState(track)
	} else {
		copy := *state
		state = &copy
	}
	if state.NextOrdinal == 0 {
		return 0
	}
	return state.NextOrdinal - 1
}

// configureLivePresentationEpoch establishes one stable mapping from source
// sequence coordinates to Core presentation ordinals. Source resets start a
// new range after the prior observed range; ordinary playlist reloads retain
// the original base.
func configureLivePresentationEpoch(track *domain.Track, epoch uint64, playlist hls.MediaPlaylist) error {
	if track == nil || len(playlist.Segments) == 0 {
		return nil
	}
	minimumSequence, maximumSequence := playlist.Segments[0].Sequence, playlist.Segments[0].Sequence
	minimumDiscontinuity, maximumDiscontinuity := playlist.Segments[0].DiscontinuitySequence, playlist.Segments[0].DiscontinuitySequence
	for _, segment := range playlist.Segments[1:] {
		if segment.Sequence < minimumSequence {
			minimumSequence = segment.Sequence
		}
		if segment.Sequence > maximumSequence {
			maximumSequence = segment.Sequence
		}
		if segment.DiscontinuitySequence < minimumDiscontinuity {
			minimumDiscontinuity = segment.DiscontinuitySequence
		}
		if segment.DiscontinuitySequence > maximumDiscontinuity {
			maximumDiscontinuity = segment.DiscontinuitySequence
		}
	}
	state := track.LivePresentation
	if state == nil {
		state = livePresentationState(track)
	} else {
		copy := *state
		state = &copy
	}
	if state.HasEpochMapping && epoch < state.MappedSourceEpoch {
		return nil // stale manifest cannot move presentation mapping backward.
	}
	if state.HasEpochMapping && epoch == state.MappedSourceEpoch {
		if maximumSequence > state.MaxSourceSequence {
			state.MaxSourceSequence = maximumSequence
		}
		if state.MaxSourceSequence >= state.SourceSequenceBase {
			span := state.MaxSourceSequence - state.SourceSequenceBase
			if span == ^uint64(0) || state.PresentationOrdinalBase > ^uint64(0)-span-1 {
				return errLivePresentationOverflow
			}
			next := state.PresentationOrdinalBase + span + 1
			if state.NextOrdinal < next {
				state.NextOrdinal = next
			}
		}
		if maximumDiscontinuity > state.SourceDiscontinuityBase {
			offset := maximumDiscontinuity - state.SourceDiscontinuityBase
			if state.PresentationDiscBase > ^uint64(0)-offset {
				return errLivePresentationOverflow
			}
			state.DiscontinuitySequence = state.PresentationDiscBase + offset
		}
		state.HasLastCoordinate = true
		state.LastSourceEpoch = epoch
		state.LastDiscontinuitySequence = maximumDiscontinuity
		state.LastSequence = state.MaxSourceSequence
		track.LivePresentation = state
		return nil
	}

	baseOrdinal := state.NextOrdinal
	if baseOrdinal == 0 {
		baseOrdinal = 1
	}
	baseDiscontinuity := state.DiscontinuitySequence
	epochBoundary := state.HasEpochMapping || state.HasLastCoordinate
	firstSourceBoundary := playlist.Segments[0].Discontinuity
	boundary := epochBoundary || firstSourceBoundary
	if boundary {
		if baseDiscontinuity == ^uint64(0) {
			return errLivePresentationOverflow
		}
		baseDiscontinuity++
	}
	span := maximumSequence - minimumSequence
	if span == ^uint64(0) || baseOrdinal > ^uint64(0)-span-1 {
		return errLivePresentationOverflow
	}
	state.HasEpochMapping = true
	state.MappedSourceEpoch = epoch
	state.SourceSequenceBase = minimumSequence
	state.PresentationOrdinalBase = baseOrdinal
	if state.FirstPresentationOrdinal == 0 {
		state.FirstPresentationOrdinal = baseOrdinal
	}
	state.MaxSourceSequence = maximumSequence
	state.SourceDiscontinuityBase = minimumDiscontinuity
	state.PresentationDiscBase = baseDiscontinuity
	state.EpochBoundary = epochBoundary
	state.NextOrdinal = baseOrdinal + span + 1
	state.HasLastCoordinate = true
	state.LastSourceEpoch = epoch
	state.LastDiscontinuitySequence = maximumDiscontinuity
	state.LastSequence = maximumSequence
	if maximumDiscontinuity >= minimumDiscontinuity {
		offset := maximumDiscontinuity - minimumDiscontinuity
		if baseDiscontinuity > ^uint64(0)-offset {
			return errLivePresentationOverflow
		}
		state.DiscontinuitySequence = baseDiscontinuity + offset
	} else {
		state.DiscontinuitySequence = baseDiscontinuity
	}
	track.LivePresentation = state
	return nil
}

func clonePlaybackSegment(segment domain.Segment) domain.Segment {
	// Source URLs can carry credentials and are not needed by the playback
	// projection. Keep that material out of Engine IPC and HTTP-facing views.
	segment.SourceURI = ""
	if segment.ProgramDateTime != nil {
		value := *segment.ProgramDateTime
		segment.ProgramDateTime = &value
	}
	if segment.ByteRange != nil {
		value := *segment.ByteRange
		segment.ByteRange = &value
	}
	return segment
}

func cloneLiveTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// assignLivePresentationIdentity maps live-origin coordinates to a stable
// per-source-epoch presentation range. Mapping uses source sequence only as a
// coordinate inside its persisted epoch base; it never changes VOD identity.
func assignLivePresentationIdentity(recording *domain.Recording, segment *domain.Segment) (bool, error) {
	if recording == nil || segment == nil || segment.IsInit {
		return false, nil
	}
	track := recording.Tracks[segment.TrackID]
	if track == nil {
		return false, nil
	}
	state := livePresentationState(track)
	if segment.LivePresentationOrdinal == 0 {
		ordinal, discSequence, boundary, ok := livePresentationForCoordinate(track, segment.SourceEpoch, segment.DiscontinuitySequence, segment.Sequence, segment.Discontinuity)
		if !ok {
			if state.HasEpochMapping && segment.SourceEpoch == state.MappedSourceEpoch && segment.Sequence < state.SourceSequenceBase {
				track.LivePresentation = state
				return false, nil
			}
			baseOrdinal := state.NextOrdinal
			if baseOrdinal == 0 {
				baseOrdinal = 1
			}
			epochBoundary := state.HasEpochMapping || state.HasLastCoordinate
			boundary = epochBoundary || segment.Discontinuity
			discBase := state.DiscontinuitySequence
			if boundary {
				if discBase == ^uint64(0) {
					return false, errLivePresentationOverflow
				}
				discBase++
			}
			state.HasEpochMapping = true
			state.MappedSourceEpoch = segment.SourceEpoch
			state.SourceSequenceBase = segment.Sequence
			state.PresentationOrdinalBase = baseOrdinal
			state.MaxSourceSequence = segment.Sequence
			state.SourceDiscontinuityBase = segment.DiscontinuitySequence
			state.PresentationDiscBase = discBase
			state.EpochBoundary = epochBoundary
			ordinal = baseOrdinal
			discSequence = discBase
			if boundary {
				discSequence--
			}
			ok = true
		}
		if !ok || ordinal == 0 {
			track.LivePresentation = state
			return false, nil
		}
		segment.LivePresentationOrdinal = ordinal
		segment.LiveDiscontinuity = boundary
		segment.LiveDiscontinuitySequence = discSequence
	}
	if segment.LivePresentationOrdinal == ^uint64(0) {
		return false, errLivePresentationOverflow
	}
	if state.NextOrdinal <= segment.LivePresentationOrdinal {
		state.NextOrdinal = segment.LivePresentationOrdinal + 1
	}
	effectiveDiscontinuitySequence := segment.LiveDiscontinuitySequence
	if segment.LiveDiscontinuity && effectiveDiscontinuitySequence < ^uint64(0) {
		effectiveDiscontinuitySequence++
	}
	if effectiveDiscontinuitySequence > state.DiscontinuitySequence {
		state.DiscontinuitySequence = effectiveDiscontinuitySequence
	}
	if state.FirstPresentationOrdinal == 0 {
		state.FirstPresentationOrdinal = segment.LivePresentationOrdinal
	}
	state.HasLastCoordinate = true
	if !state.HasEpochMapping || segment.SourceEpoch >= state.MappedSourceEpoch {
		state.LastSourceEpoch = segment.SourceEpoch
		state.LastDiscontinuitySequence = segment.DiscontinuitySequence
		state.LastSequence = segment.Sequence
	}
	track.LivePresentation = state
	return true, nil
}

func livePresentationState(track *domain.Track) *domain.LivePresentationState {
	if track == nil {
		return &domain.LivePresentationState{NextOrdinal: 1}
	}
	if track.LivePresentation != nil && track.LivePresentation.NextOrdinal > 0 {
		state := *track.LivePresentation
		return &state
	}
	state := &domain.LivePresentationState{}
	if track.LivePresentation != nil {
		*state = *track.LivePresentation
	}
	var latest *domain.Segment
	var maximumOrdinal, firstPublished uint64
	for i := range track.Segments {
		segment := &track.Segments[i]
		if segment.LivePresentationOrdinal == 0 {
			continue
		}
		if segment.LivePresentationOrdinal > maximumOrdinal {
			maximumOrdinal = segment.LivePresentationOrdinal
		}
		if firstPublished == 0 || segment.LivePresentationOrdinal < firstPublished {
			firstPublished = segment.LivePresentationOrdinal
		}
		if latest == nil || segment.LivePresentationOrdinal > latest.LivePresentationOrdinal {
			latest = segment
		}
	}
	if state.FirstPresentationOrdinal == 0 && firstPublished != 0 {
		state.FirstPresentationOrdinal = firstPublished
	}
	if maximumOrdinal != 0 && (state.NextOrdinal == 0 || state.NextOrdinal <= maximumOrdinal) {
		if maximumOrdinal == ^uint64(0) {
			state.NextOrdinal = 0
		} else {
			state.NextOrdinal = maximumOrdinal + 1
		}
	}
	if latest != nil {
		if !state.HasEpochMapping {
			state.HasEpochMapping = true
			state.MappedSourceEpoch = latest.SourceEpoch
			state.SourceSequenceBase = latest.Sequence
			state.PresentationOrdinalBase = latest.LivePresentationOrdinal
			state.MaxSourceSequence = latest.Sequence
			state.SourceDiscontinuityBase = latest.DiscontinuitySequence
			state.PresentationDiscBase = latest.LiveDiscontinuitySequence
			if latest.LiveDiscontinuity && state.PresentationDiscBase < ^uint64(0) {
				state.PresentationDiscBase++
			}
		}
		if !state.HasLastCoordinate || latest.LivePresentationOrdinal >= maximumOrdinal {
			state.HasLastCoordinate = true
			state.LastSourceEpoch = latest.SourceEpoch
			state.LastDiscontinuitySequence = latest.DiscontinuitySequence
			state.LastSequence = latest.Sequence
			if latest.LiveDiscontinuitySequence > state.DiscontinuitySequence {
				state.DiscontinuitySequence = latest.LiveDiscontinuitySequence
			}
			if latest.LiveDiscontinuity && latest.LiveDiscontinuitySequence < ^uint64(0) && latest.LiveDiscontinuitySequence+1 > state.DiscontinuitySequence {
				state.DiscontinuitySequence = latest.LiveDiscontinuitySequence + 1
			}
		}
	} else if state.NextOrdinal == 0 {
		state.NextOrdinal = 1
	}
	return state
}

func livePresentationForCoordinate(track *domain.Track, epoch, discontinuitySequence, sequence uint64, discontinuity bool) (uint64, uint64, bool, bool) {
	var state *domain.LivePresentationState
	if track != nil {
		state = track.LivePresentation
	}
	if state == nil {
		state = livePresentationState(track)
	}
	return livePresentationForState(state, epoch, discontinuitySequence, sequence, discontinuity)
}

func livePresentationForState(state *domain.LivePresentationState, epoch, discontinuitySequence, sequence uint64, discontinuity bool) (uint64, uint64, bool, bool) {
	if state == nil {
		return 0, 0, false, false
	}
	if !state.HasEpochMapping || state.MappedSourceEpoch != epoch || sequence < state.SourceSequenceBase {
		return 0, 0, false, false
	}
	sequenceOffset := sequence - state.SourceSequenceBase
	if state.PresentationOrdinalBase == 0 || state.PresentationOrdinalBase > ^uint64(0)-sequenceOffset {
		return 0, 0, false, false
	}
	ordinal := state.PresentationOrdinalBase + sequenceOffset
	discSequence := state.PresentationDiscBase
	if discontinuitySequence >= state.SourceDiscontinuityBase {
		discOffset := discontinuitySequence - state.SourceDiscontinuityBase
		if discSequence > ^uint64(0)-discOffset {
			return 0, 0, false, false
		}
		discSequence += discOffset
	}
	boundary := discontinuity || state.EpochBoundary && sequence == state.SourceSequenceBase
	if boundary && discSequence > 0 {
		discSequence--
	}
	return ordinal, discSequence, boundary, true
}

// migrateLivePresentation assigns the exact identities the previous live
// playlist implied for its currently exposed tail. This one-time active-root
// migration preserves overlapping URIs during an application handover; later
// VOD timeline changes cannot rewrite those identities.
func migrateLivePresentation(recording *domain.Recording) (bool, error) {
	if recording == nil || recording.Tracks["main"] == nil {
		return false, nil
	}
	track := recording.Tracks["main"]
	changed := false
	anyIdentity := false
	for _, segment := range track.Segments {
		if segment.LivePresentationOrdinal != 0 {
			anyIdentity = true
			break
		}
	}
	if !anyIdentity && len(track.Segments) > 0 {
		indexes := make([]int, len(track.Segments))
		for i := range indexes {
			indexes[i] = i
		}
		sort.SliceStable(indexes, func(i, j int) bool {
			return legacyLiveSegmentBefore(track.Segments[indexes[i]], track.Segments[indexes[j]])
		})
		start := len(indexes) - LivePlaybackWindowSize
		if start < 0 {
			start = 0
		}
		first := track.Segments[indexes[start]]
		mediaSequence := legacyPlaybackMediaSequence(first)
		if mediaSequence == ^uint64(0) {
			return false, errLivePresentationOverflow
		}
		firstOrdinal := mediaSequence + 1
		state := &domain.LivePresentationState{}
		state.FirstPresentationOrdinal = firstOrdinal
		for position := 0; position < start; position++ {
			if legacyLiveDiscontinuity(recording, track.Segments, indexes, position) {
				if state.DiscontinuitySequence == ^uint64(0) {
					return false, errLivePresentationOverflow
				}
				state.DiscontinuitySequence++
			}
		}
		for position := start; position < len(indexes); position++ {
			index := indexes[position]
			ordinalOffset := uint64(position - start)
			if firstOrdinal >= ^uint64(0)-ordinalOffset {
				return false, errLivePresentationOverflow
			}
			segment := &track.Segments[index]
			boundary := legacyLiveDiscontinuity(recording, track.Segments, indexes, position)
			segment.LivePresentationOrdinal = firstOrdinal + ordinalOffset
			segment.LiveDiscontinuity = boundary
			segment.LiveDiscontinuitySequence = state.DiscontinuitySequence
			if boundary {
				if state.DiscontinuitySequence == ^uint64(0) {
					return false, errLivePresentationOverflow
				}
				state.DiscontinuitySequence++
			}
			state.NextOrdinal = segment.LivePresentationOrdinal + 1
			state.HasLastCoordinate = true
			state.LastSourceEpoch = segment.SourceEpoch
			state.LastDiscontinuitySequence = segment.DiscontinuitySequence
			state.LastSequence = segment.Sequence
			changed = true
		}
		latest := track.Segments[indexes[len(indexes)-1]]
		state.HasEpochMapping = true
		state.MappedSourceEpoch = latest.SourceEpoch
		state.SourceSequenceBase = latest.Sequence
		state.PresentationOrdinalBase = latest.LivePresentationOrdinal
		state.MaxSourceSequence = latest.Sequence
		state.SourceDiscontinuityBase = latest.DiscontinuitySequence
		state.PresentationDiscBase = latest.LiveDiscontinuitySequence
		if latest.LiveDiscontinuity && state.PresentationDiscBase < ^uint64(0) {
			state.PresentationDiscBase++
		}
		state.EpochBoundary = latest.LiveDiscontinuity
		track.LivePresentation = state
		return changed, nil
	}
	state := livePresentationState(track)
	if track.LivePresentation == nil || *track.LivePresentation != *state {
		track.LivePresentation = state
		changed = true
	}
	return changed, nil
}

func legacyLiveSegmentBefore(a, b domain.Segment) bool {
	if a.TimelineOrdinal != 0 || b.TimelineOrdinal != 0 {
		aPosition, bPosition := a.TimelineOrdinal, b.TimelineOrdinal
		if aPosition == 0 {
			aPosition = legacySegmentPosition(a)
		}
		if bPosition == 0 {
			bPosition = legacySegmentPosition(b)
		}
		if aPosition != bPosition {
			return aPosition < bPosition
		}
	}
	if a.ArchiveOrdinal > 0 || b.ArchiveOrdinal > 0 {
		if a.SourceEpoch != b.SourceEpoch {
			return a.SourceEpoch < b.SourceEpoch
		}
		if a.ArchiveOrdinal != b.ArchiveOrdinal {
			if a.ArchiveOrdinal == 0 {
				return a.Sequence < b.Sequence
			}
			if b.ArchiveOrdinal == 0 {
				return false
			}
			return a.ArchiveOrdinal < b.ArchiveOrdinal
		}
	}
	return a.Sequence < b.Sequence
}

func legacySegmentPosition(segment domain.Segment) uint64 {
	if segment.ArchiveOrdinal != 0 {
		return segment.ArchiveOrdinal
	}
	return segment.Sequence
}

func legacyPlaybackMediaSequence(segment domain.Segment) uint64 {
	if segment.TimelineOrdinal != 0 {
		return segment.TimelineOrdinal - 1
	}
	if segment.ArchiveOrdinal != 0 {
		return segment.ArchiveOrdinal - 1
	}
	return segment.Sequence
}

func legacyLiveDiscontinuity(recording *domain.Recording, segments []domain.Segment, indexes []int, position int) bool {
	segment := segments[indexes[position]]
	if segment.Discontinuity {
		return true
	}
	if position == 0 {
		return false
	}
	previous := segments[indexes[position-1]]
	if previous.SourceEpoch != segment.SourceEpoch || previous.DiscontinuitySequence != segment.DiscontinuitySequence {
		return true
	}
	if previous.Sequence != ^uint64(0) && segment.Sequence > previous.Sequence+1 {
		return true
	}
	for _, gap := range recording.Gaps {
		if gap.TrackID == "main" && gap.SourceEpoch == previous.SourceEpoch && gap.ToSequence > previous.Sequence && gap.FromSequence < segment.Sequence {
			return true
		}
	}
	return false
}
