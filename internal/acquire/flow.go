package acquire

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	DefaultAcquisitionFlowLimit = 96
	MaxAcquisitionFlowLimit     = 128
)

var ErrAcquisitionFlowLimit = errors.New("acquisition flow page limit is invalid")

// AcquisitionFlowSnapshot is a compact, read-only projection for the
// Recording Detail visualization. It contains source coordinates and current
// acquisition actors only; URLs, payloads, and transport errors are excluded.
type AcquisitionFlowSnapshot struct {
	RecordingID      string                   `json:"recording_id"`
	CaptureState     domain.RecordingState    `json:"capture_state"`
	Live             bool                     `json:"live"`
	ArchiveRevision  uint64                   `json:"archive_revision"`
	TimelineRevision uint64                   `json:"timeline_revision"`
	FirstOrdinal     uint64                   `json:"first_ordinal"`
	TotalOrdinals    uint64                   `json:"total_ordinals"`
	StartOrdinal     uint64                   `json:"start_ordinal"`
	HasOlder         bool                     `json:"has_older"`
	Window           *AcquisitionFlowWindow   `json:"window,omitempty"`
	Segments         []AcquisitionFlowSegment `json:"segments"`
	Actors           []AcquisitionFlowActor   `json:"actors"`
}

type AcquisitionFlowWindow struct {
	TrackID               string    `json:"track_id"`
	SourceEpoch           uint64    `json:"source_epoch"`
	DiscontinuitySequence uint64    `json:"discontinuity_sequence"`
	FirstSequence         uint64    `json:"first_sequence"`
	LastSequence          uint64    `json:"last_sequence"`
	FirstOrdinal          uint64    `json:"first_ordinal,omitempty"`
	LastOrdinal           uint64    `json:"last_ordinal,omitempty"`
	SegmentCount          int       `json:"segment_count"`
	ObservedAt            time.Time `json:"observed_at"`
}

type AcquisitionFlowSegment struct {
	TrackID               string `json:"track_id"`
	SourceEpoch           uint64 `json:"source_epoch"`
	DiscontinuitySequence uint64 `json:"discontinuity_sequence"`
	Sequence              uint64 `json:"sequence"`
	PresentationOrdinal   uint64 `json:"presentation_ordinal"`
	DurationMS            *int64 `json:"duration_ms,omitempty"`
	State                 string `json:"state"`
	Observed              bool   `json:"observed"`
	Inferred              bool   `json:"inferred,omitempty"`
	Recovered             bool   `json:"recovered,omitempty"`
	Retrying              bool   `json:"retrying,omitempty"`
	Attempt               int    `json:"attempt,omitempty"`
}

type AcquisitionFlowActor struct {
	ActorID               string `json:"actor_id"`
	NodeID                string `json:"node_id"`
	NodeLabel             string `json:"node_label"`
	WorkerID              string `json:"worker_id"`
	AcquisitionClass      string `json:"acquisition_class"`
	TrackID               string `json:"track_id"`
	SourceEpoch           uint64 `json:"source_epoch"`
	DiscontinuitySequence uint64 `json:"discontinuity_sequence"`
	Sequence              uint64 `json:"sequence"`
	PresentationOrdinal   uint64 `json:"presentation_ordinal,omitempty"`
	State                 string `json:"state"`
	Attempt               int    `json:"attempt,omitempty"`
}

type historicalAcquisitionActor struct {
	actorID  string
	workerID string
}

type acquisitionFlowTask struct {
	key      segmentTaskKey
	actorID  string
	workerID string
	state    string
	attempt  int
}

// AcquisitionFlowSnapshot returns a bounded presentation page and the current
// ephemeral actors for one recording. A zero start selects the most recent
// known page; otherwise start is an absolute presentation ordinal.
func (m *Manager) AcquisitionFlowSnapshot(ctx context.Context, id string, start uint64, limit int) (AcquisitionFlowSnapshot, error) {
	if m == nil || id == "" {
		return AcquisitionFlowSnapshot{}, storage.ErrNotFound
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return AcquisitionFlowSnapshot{}, err
	}
	if limit == 0 {
		limit = DefaultAcquisitionFlowLimit
	}
	if limit < 1 || limit > MaxAcquisitionFlowLimit {
		return AcquisitionFlowSnapshot{}, ErrAcquisitionFlowLimit
	}

	recording, scheduler, historical, err := m.acquisitionFlowState(ctx, id)
	if err != nil {
		return AcquisitionFlowSnapshot{}, err
	}
	track := recording.Tracks["main"]
	if track == nil {
		return AcquisitionFlowSnapshot{RecordingID: id, CaptureState: recording.State, Live: recording.State == domain.StateRecording, ArchiveRevision: recording.ArchiveRevision, TimelineRevision: recording.TimelineRevision, Segments: []AcquisitionFlowSegment{}, Actors: []AcquisitionFlowActor{}}, nil
	}

	var window *liveManifestWindow
	var tasks []acquisitionFlowTask
	if scheduler != nil {
		scheduler.mu.Lock()
		if !scheduler.closed {
			window = cloneLiveManifestWindow(scheduler.latestManifestWindow)
			for key, task := range scheduler.tasks {
				state := acquisitionFlowTaskState(task.state)
				if state == "" {
					continue
				}
				attempt := task.attempt + 1
				if attempt < 1 {
					attempt = 1
				}
				tasks = append(tasks, acquisitionFlowTask{key: key, actorID: task.actorID, workerID: task.workerID, state: state, attempt: attempt})
			}
		}
		scheduler.mu.Unlock()
	}

	first, total := flowOrdinalBounds(track)
	if window != nil && window.lastOrdinal > total {
		total = window.lastOrdinal
	}
	for _, pending := range track.PendingSegments {
		if pending.LivePresentationOrdinal > total {
			total = pending.LivePresentationOrdinal
		}
	}
	for _, task := range tasks {
		if ordinal := flowCoordinateOrdinal(track, task.key); ordinal > total {
			total = ordinal
		}
	}
	if total == 0 {
		return AcquisitionFlowSnapshot{
			RecordingID: id, CaptureState: recording.State, Live: recording.State == domain.StateRecording,
			ArchiveRevision: recording.ArchiveRevision, TimelineRevision: recording.TimelineRevision,
			FirstOrdinal: first, Segments: []AcquisitionFlowSegment{}, Actors: []AcquisitionFlowActor{},
		}, nil
	}
	if first == 0 {
		first = 1
	}
	if start == 0 {
		start = first
		if total-first+1 > uint64(limit) {
			start = total - uint64(limit) + 1
		}
	} else if start < first {
		start = first
	}
	end := start + uint64(limit) - 1
	if end < start || end > total {
		end = total
	}

	view := AcquisitionFlowSnapshot{
		RecordingID: id, CaptureState: recording.State, Live: recording.State == domain.StateRecording,
		ArchiveRevision: recording.ArchiveRevision, TimelineRevision: recording.TimelineRevision,
		FirstOrdinal: first, TotalOrdinals: total, StartOrdinal: start, HasOlder: start > first,
		Segments: []AcquisitionFlowSegment{}, Actors: []AcquisitionFlowActor{},
	}
	if window != nil {
		view.Window = &AcquisitionFlowWindow{
			TrackID: window.trackID, SourceEpoch: window.epoch,
			DiscontinuitySequence: window.discontinuitySequence,
			FirstSequence:         window.firstSequence, LastSequence: window.lastSequence,
			FirstOrdinal: window.firstOrdinal, LastOrdinal: window.lastOrdinal,
			SegmentCount: len(window.coordinates), ObservedAt: window.observedAt,
		}
	}

	segments := make(map[uint64]AcquisitionFlowSegment, limit+len(tasks)+len(historical))
	coordinateOrdinals := make(map[segmentTaskKey]uint64, limit+len(track.PendingSegments)+len(tasks))
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		page, pageErr := m.store.ReadShardedLiveSlotPage(ctx, id, "main", start, limit)
		if pageErr != nil && !errors.Is(pageErr, storage.ErrNotFound) {
			return AcquisitionFlowSnapshot{}, pageErr
		}
		if pageErr == nil {
			if page.StartOrdinal != 0 {
				view.StartOrdinal = page.StartOrdinal
			}
			if page.FirstOrdinal > view.FirstOrdinal {
				view.FirstOrdinal = page.FirstOrdinal
				view.HasOlder = view.StartOrdinal > view.FirstOrdinal
			}
			if page.TotalSlots > view.TotalOrdinals {
				view.TotalOrdinals = page.TotalSlots
			}
			for _, slot := range page.Slots {
				if slot.Segment != nil {
					segment := flowSegmentFromMedia(*slot.Segment, slot.Ordinal, slot.Recovered)
					segments[slot.Ordinal] = segment
					coordinateOrdinals[segmentTaskKey{epoch: slot.Segment.SourceEpoch, discontinuitySequence: slot.Segment.DiscontinuitySequence, sequence: slot.Segment.Sequence}] = slot.Ordinal
					continue
				}
				if slot.Gap == nil {
					continue
				}
				gap := slot.Gap
				sequence := gap.FromSequence
				if gap.LivePresentationOrdinal > 0 && slot.Ordinal >= gap.LivePresentationOrdinal {
					offset := slot.Ordinal - gap.LivePresentationOrdinal
					if gap.FromSequence <= ^uint64(0)-offset && sequence+offset <= gap.ToSequence {
						sequence += offset
					}
				}
				key := segmentTaskKey{epoch: gap.SourceEpoch, discontinuitySequence: gap.DiscontinuitySequence, sequence: sequence}
				coordinateOrdinals[key] = slot.Ordinal
				// A live slot is a bounded presentation projection, while the
				// coordinate index is the canonical current-state authority. A
				// stale/legacy gap slot must not make already-published media look
				// unresolved in the Segment Flow UI.
				media, lookupErr := m.store.LookupShardedMediaByCoordinate(ctx, id, archiveindex.Coordinate{
					SessionID: recording.SourceSessionID, TrackID: gap.TrackID,
					SourceEpoch: gap.SourceEpoch, DiscontinuitySequence: gap.DiscontinuitySequence,
					Sequence: sequence, Kind: archiveindex.ObjectMedia,
				})
				if lookupErr == nil {
					segments[slot.Ordinal] = flowSegmentFromMedia(media.Segment, slot.Ordinal,
						media.SelectedClaim != nil && media.SelectedClaim.Source == archiveindex.ClaimHistorical)
					continue
				}
				if !errors.Is(lookupErr, storage.ErrNotFound) {
					return AcquisitionFlowSnapshot{}, lookupErr
				}
				observed := gap.LiveDuration > 0 || flowWindowHas(window, key)
				flowSegment := AcquisitionFlowSegment{
					TrackID: "main", SourceEpoch: gap.SourceEpoch, DiscontinuitySequence: gap.DiscontinuitySequence,
					Sequence: sequence, PresentationOrdinal: slot.Ordinal, State: "unresolved",
					Observed: observed, Inferred: !observed,
				}
				if gap.LiveDuration > 0 {
					ms := int64(gap.LiveDuration * 1000)
					flowSegment.DurationMS = &ms
				}
				segments[slot.Ordinal] = flowSegment
			}
		}
	}

	for _, pending := range track.PendingSegments {
		key := segmentTaskKey{epoch: pending.SourceEpoch, discontinuitySequence: pending.DiscontinuitySequence, sequence: pending.Sequence}
		ordinal := pending.LivePresentationOrdinal
		if ordinal == 0 {
			ordinal = flowCoordinateOrdinal(track, key)
		}
		if ordinal == 0 {
			continue
		}
		coordinateOrdinals[key] = ordinal
		if ordinal < view.StartOrdinal || ordinal > end {
			continue
		}
		if current, ok := segments[ordinal]; ok && (current.State == "present" || current.State == "unresolved") {
			continue
		}
		segments[ordinal] = AcquisitionFlowSegment{TrackID: "main", SourceEpoch: key.epoch, DiscontinuitySequence: key.discontinuitySequence, Sequence: key.sequence, PresentationOrdinal: ordinal, State: "pending", Observed: true, DurationMS: durationMillis(pending.Duration)}
	}

	if recording.FormatVersion != storage.ShardedArchiveFormatVersion {
		projectLegacyFlow(track, recording.Gaps, window, view.StartOrdinal, end, segments, coordinateOrdinals)
	}

	for _, coordinate := range windowCoordinates(window) {
		ordinal := coordinate.presentationOrdinal
		if ordinal == 0 {
			ordinal = flowCoordinateOrdinal(track, coordinate.key)
		}
		if ordinal == 0 {
			continue
		}
		coordinateOrdinals[coordinate.key] = ordinal
		if ordinal < view.StartOrdinal || ordinal > end {
			continue
		}
		if _, exists := segments[ordinal]; !exists {
			segments[ordinal] = AcquisitionFlowSegment{TrackID: window.trackID, SourceEpoch: coordinate.key.epoch, DiscontinuitySequence: coordinate.key.discontinuitySequence, Sequence: coordinate.key.sequence, PresentationOrdinal: ordinal, State: "pending", Observed: true, DurationMS: durationMillis(coordinate.duration.Seconds())}
		}
	}

	for _, task := range tasks {
		ordinal := coordinateOrdinals[task.key]
		if ordinal == 0 {
			ordinal = flowCoordinateOrdinal(track, task.key)
		}
		if ordinal == 0 {
			continue
		}
		if _, exists := segments[ordinal]; !exists {
			segments[ordinal] = AcquisitionFlowSegment{TrackID: "main", SourceEpoch: task.key.epoch, DiscontinuitySequence: task.key.discontinuitySequence, Sequence: task.key.sequence, PresentationOrdinal: ordinal, State: "pending", Observed: true}
		}
		segment := segments[ordinal]
		if task.attempt > 0 || task.actorID != "" {
			segment.Attempt = task.attempt + 1
		}
		if task.state == "retry_wait" || task.state == "refreshing" {
			segment.Retrying = true
			segments[ordinal] = segment
		}
		if task.actorID == "" || task.workerID == "" {
			continue
		}
		view.Actors = append(view.Actors, AcquisitionFlowActor{
			ActorID: task.actorID, NodeID: "local", NodeLabel: "Local", WorkerID: task.workerID,
			AcquisitionClass: "live", TrackID: "main", SourceEpoch: task.key.epoch,
			DiscontinuitySequence: task.key.discontinuitySequence, Sequence: task.key.sequence,
			PresentationOrdinal: ordinal, State: task.state, Attempt: task.attempt,
		})
	}
	for key, actor := range historical {
		ordinal := coordinateOrdinals[key]
		if ordinal == 0 {
			ordinal = flowCoordinateOrdinal(track, key)
		}
		if ordinal > 0 {
			if _, exists := segments[ordinal]; !exists {
				segments[ordinal] = AcquisitionFlowSegment{TrackID: "main", SourceEpoch: key.epoch, DiscontinuitySequence: key.discontinuitySequence, Sequence: key.sequence, PresentationOrdinal: ordinal, State: "unresolved", Observed: false, Inferred: true}
			}
		}
		view.Actors = append(view.Actors, AcquisitionFlowActor{
			ActorID: actor.actorID, NodeID: "local", NodeLabel: "Local", WorkerID: actor.workerID,
			AcquisitionClass: "historical", TrackID: "main", SourceEpoch: key.epoch,
			DiscontinuitySequence: key.discontinuitySequence, Sequence: key.sequence,
			PresentationOrdinal: ordinal, State: "fetching", Attempt: 1,
		})
	}

	for _, segment := range segments {
		if segment.PresentationOrdinal >= view.StartOrdinal && segment.PresentationOrdinal <= end {
			view.Segments = append(view.Segments, segment)
		}
	}
	// Active actors stay visible in the response even when their coordinate is
	// outside the fetched page. The UI can place their bounded placeholder at
	// the exact ordinal while keeping the entire recording out of the DOM.
	for _, actor := range view.Actors {
		if actor.PresentationOrdinal == 0 || actor.PresentationOrdinal >= view.StartOrdinal && actor.PresentationOrdinal <= end {
			continue
		}
		if segment, ok := segments[actor.PresentationOrdinal]; ok {
			view.Segments = append(view.Segments, segment)
		}
	}
	sort.Slice(view.Segments, func(i, j int) bool {
		return view.Segments[i].PresentationOrdinal < view.Segments[j].PresentationOrdinal
	})
	sort.Slice(view.Actors, func(i, j int) bool {
		if view.Actors[i].PresentationOrdinal != view.Actors[j].PresentationOrdinal {
			return view.Actors[i].PresentationOrdinal < view.Actors[j].PresentationOrdinal
		}
		return view.Actors[i].WorkerID < view.Actors[j].WorkerID
	})
	return view, nil
}

func (m *Manager) acquisitionFlowState(ctx context.Context, id string) (*domain.Recording, *segmentScheduler, map[segmentTaskKey]historicalAcquisitionActor, error) {
	e, ok := m.entry(id)
	if !ok {
		recording, err := m.loadLifecycleRecording(ctx, id)
		if err != nil {
			return nil, nil, nil, err
		}
		if recording == nil || recording.ID != id {
			return nil, nil, nil, storage.ErrNotFound
		}
		return cloneAcquisitionFlowRecording(recording), nil, nil, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.deleted || e.recording == nil || e.recording.ID != id {
		return nil, nil, nil, storage.ErrNotFound
	}
	actors := make(map[segmentTaskKey]historicalAcquisitionActor, len(e.historicalWorkers))
	for key, actor := range e.historicalWorkers {
		actors[key] = actor
	}
	return cloneAcquisitionFlowRecording(e.recording), e.scheduler, actors, nil
}

func cloneAcquisitionFlowRecording(source *domain.Recording) *domain.Recording {
	if source == nil {
		return nil
	}
	copy := *source
	copy.Tracks = make(map[string]*domain.Track, 1)
	if track := source.Tracks["main"]; track != nil {
		trackCopy := *track
		trackCopy.PendingSegments = append([]domain.PendingSequence(nil), track.PendingSegments...)
		trackCopy.PendingSequences = append([]uint64(nil), track.PendingSequences...)
		if track.LivePresentation != nil {
			live := *track.LivePresentation
			trackCopy.LivePresentation = &live
		}
		if source.FormatVersion != storage.ShardedArchiveFormatVersion {
			trackCopy.Segments = append([]domain.Segment(nil), track.Segments...)
		} else {
			trackCopy.Segments = nil
		}
		copy.Tracks["main"] = &trackCopy
	}
	if source.FormatVersion != storage.ShardedArchiveFormatVersion {
		copy.Gaps = append([]domain.Gap(nil), source.Gaps...)
	} else {
		copy.Gaps = nil
	}
	return &copy
}

func cloneLiveManifestWindow(source *liveManifestWindow) *liveManifestWindow {
	if source == nil {
		return nil
	}
	copy := *source
	copy.coordinates = append([]liveManifestCoordinate(nil), source.coordinates...)
	for index := range copy.coordinates {
		if source.coordinates[index].programDateTime != nil {
			at := *source.coordinates[index].programDateTime
			copy.coordinates[index].programDateTime = &at
		}
	}
	return &copy
}

func flowOrdinalBounds(track *domain.Track) (uint64, uint64) {
	if track == nil {
		return 1, 0
	}
	first := uint64(1)
	var total uint64
	if track.LivePresentation != nil {
		if track.LivePresentation.FirstPresentationOrdinal > first {
			first = track.LivePresentation.FirstPresentationOrdinal
		}
		if track.LivePresentation.NextOrdinal > 1 {
			total = track.LivePresentation.NextOrdinal - 1
		}
	}
	if track.LiveSlotHighWater > total {
		total = track.LiveSlotHighWater
	}
	for _, pending := range track.PendingSegments {
		if pending.LivePresentationOrdinal > total {
			total = pending.LivePresentationOrdinal
		}
	}
	for _, segment := range track.Segments {
		ordinal := segment.LivePresentationOrdinal
		if ordinal == 0 {
			ordinal = segment.ArchiveOrdinal
		}
		if ordinal > total {
			total = ordinal
		}
	}
	return first, total
}

func flowCoordinateOrdinal(track *domain.Track, key segmentTaskKey) uint64 {
	if track == nil {
		return 0
	}
	for _, pending := range track.PendingSegments {
		if pending.SourceEpoch == key.epoch && pending.DiscontinuitySequence == key.discontinuitySequence && pending.Sequence == key.sequence && pending.LivePresentationOrdinal > 0 {
			return pending.LivePresentationOrdinal
		}
	}
	for _, segment := range track.Segments {
		if segment.SourceEpoch == key.epoch && segment.DiscontinuitySequence == key.discontinuitySequence && segment.Sequence == key.sequence && segment.LivePresentationOrdinal > 0 {
			return segment.LivePresentationOrdinal
		}
	}
	if ordinal, _, _, ok := livePresentationForCoordinate(track, key.epoch, key.discontinuitySequence, key.sequence, false); ok {
		return ordinal
	}
	return 0
}

func flowSegmentFromMedia(segment domain.Segment, ordinal uint64, recovered bool) AcquisitionFlowSegment {
	return AcquisitionFlowSegment{
		TrackID: segment.TrackID, SourceEpoch: segment.SourceEpoch,
		DiscontinuitySequence: segment.DiscontinuitySequence, Sequence: segment.Sequence,
		PresentationOrdinal: ordinal, DurationMS: durationMillis(segment.Duration),
		State: "present", Observed: true, Recovered: recovered,
	}
}

func durationMillis(seconds float64) *int64 {
	if seconds <= 0 {
		return nil
	}
	value := int64(seconds * 1000)
	return &value
}

func flowWindowHas(window *liveManifestWindow, key segmentTaskKey) bool {
	if window == nil {
		return false
	}
	for _, coordinate := range window.coordinates {
		if coordinate.key == key {
			return true
		}
	}
	return false
}

func windowCoordinates(window *liveManifestWindow) []liveManifestCoordinate {
	if window == nil {
		return nil
	}
	return window.coordinates
}

func projectLegacyFlow(track *domain.Track, gaps []domain.Gap, window *liveManifestWindow, start, end uint64, out map[uint64]AcquisitionFlowSegment, coordinateOrdinals map[segmentTaskKey]uint64) {
	if track == nil {
		return
	}
	for _, segment := range track.Segments {
		ordinal := segment.LivePresentationOrdinal
		if ordinal == 0 {
			ordinal = segment.ArchiveOrdinal
		}
		if ordinal == 0 {
			ordinal = segment.Sequence + 1
		}
		if ordinal < start || ordinal > end {
			continue
		}
		out[ordinal] = flowSegmentFromMedia(segment, ordinal, false)
		coordinateOrdinals[segmentTaskKey{epoch: segment.SourceEpoch, discontinuitySequence: segment.DiscontinuitySequence, sequence: segment.Sequence}] = ordinal
	}
	for _, gap := range gaps {
		if gap.TrackID != "main" {
			continue
		}
		count := gap.ToSequence - gap.FromSequence + 1
		if count > MaxAcquisitionFlowLimit {
			count = MaxAcquisitionFlowLimit
		}
		for offset := uint64(0); offset < count; offset++ {
			sequence := gap.FromSequence + offset
			ordinal := gap.LivePresentationOrdinal + offset
			if ordinal == 0 {
				ordinal, _, _, _ = livePresentationForCoordinate(track, gap.SourceEpoch, gap.DiscontinuitySequence, sequence, false)
			}
			if ordinal == 0 {
				ordinal = sequence + 1
			}
			key := segmentTaskKey{epoch: gap.SourceEpoch, discontinuitySequence: gap.DiscontinuitySequence, sequence: sequence}
			coordinateOrdinals[key] = ordinal
			if ordinal < start || ordinal > end {
				continue
			}
			observed := gap.LiveDuration > 0 || flowWindowHas(window, key)
			item := AcquisitionFlowSegment{TrackID: "main", SourceEpoch: gap.SourceEpoch, DiscontinuitySequence: gap.DiscontinuitySequence, Sequence: sequence, PresentationOrdinal: ordinal, State: "unresolved", Observed: observed, Inferred: !observed}
			if gap.LiveDuration > 0 {
				item.DurationMS = durationMillis(gap.LiveDuration)
			}
			if existing, ok := out[ordinal]; !ok || existing.State != "present" {
				out[ordinal] = item
			}
		}
	}
}

func acquisitionFlowTaskState(state segmentTaskState) string {
	switch state {
	case segmentTaskInFlight:
		return "fetching"
	case segmentTaskRetryWait:
		return "retry_wait"
	case segmentTaskRefreshing:
		return "refreshing"
	case segmentTaskQueued:
		return "queued"
	default:
		return ""
	}
}

func (m *Manager) beginHistoricalActor(e *entry, candidate historicalRecoveryWork) func() {
	if e == nil {
		return func() {}
	}
	key := segmentTaskKey{epoch: candidate.coordinate.SourceEpoch, discontinuitySequence: candidate.coordinate.DiscontinuitySequence, sequence: candidate.coordinate.Sequence}
	e.mu.Lock()
	if e.historicalWorkers == nil {
		e.historicalWorkers = make(map[segmentTaskKey]historicalAcquisitionActor)
	}
	used := make(map[string]struct{}, len(e.historicalWorkers))
	for _, actor := range e.historicalWorkers {
		used[actor.workerID] = struct{}{}
	}
	workerSlot := 1
	for {
		if _, occupied := used[fmt.Sprintf("H%d", workerSlot)]; !occupied {
			break
		}
		workerSlot++
	}
	e.nextHistoricalWorkerID++
	workerID := fmt.Sprintf("H%d", workerSlot)
	actorID := fmt.Sprintf("local:historical:%s:%d", workerID, e.nextHistoricalWorkerID)
	e.historicalWorkers[key] = historicalAcquisitionActor{actorID: actorID, workerID: workerID}
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		if current, ok := e.historicalWorkers[key]; ok && current.actorID == actorID {
			delete(e.historicalWorkers, key)
		}
		e.mu.Unlock()
	}
}
