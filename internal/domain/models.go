package domain

import "time"

type RecordingState string

const (
	StateRecording   RecordingState = "recording"
	StateStopped     RecordingState = "stopped"
	StateCompleted   RecordingState = "completed"
	StateInterrupted RecordingState = "interrupted"
)

// Recording is the self-describing root document for one captured stream.
type Recording struct {
	FormatVersion int    `json:"format_version"`
	ID            string `json:"id"`
	// SourceSessionID identifies the broadcast/resource session independently
	// from ID, which identifies this one recording execution. It is a bounded,
	// opaque Core-generated fingerprint; raw adapter session references are not
	// stored in this field.
	SourceSessionID         string             `json:"source_session_id,omitempty"`
	Title                   string             `json:"title,omitempty"`
	AdapterID               string             `json:"adapter_id,omitempty"`
	Adapter                 *AdapterProvenance `json:"adapter,omitempty"`
	Resource                *ResourceReference `json:"resource,omitempty"`
	SourceURIClassification string             `json:"source_uri_classification,omitempty"`
	// SourceURL is retained only to read and report pre-adapter recordings.
	SourceURL string         `json:"source_url,omitempty"`
	State     RecordingState `json:"state"`
	// ArchiveSealed records the explicit end of historical repairability. A
	// terminal Recording state closes capture but does not by itself seal the
	// archive.
	ArchiveSealed             bool               `json:"archive_sealed,omitempty"`
	CreatedAt                 time.Time          `json:"created_at"`
	StartedAt                 time.Time          `json:"started_at"`
	StoppedAt                 *time.Time         `json:"stopped_at,omitempty"`
	Tracks                    map[string]*Track  `json:"tracks"`
	Gaps                      []Gap              `json:"gaps,omitempty"`
	Snapshots                 []ManifestSnapshot `json:"manifest_snapshots,omitempty"`
	LastError                 string             `json:"last_error,omitempty"`
	MetadataTimeline          []MetadataRevision `json:"metadata_timeline,omitempty"`
	MetadataTimelineTruncated bool               `json:"metadata_timeline_truncated,omitempty"`
	// TimelineRevision advances whenever the playback projection is rebuilt.
	// Archive ordinals remain append-only storage/preview identities.
	TimelineRevision uint64 `json:"timeline_revision,omitempty"`
	// ArchiveRevision advances when canonical archive content changes. It is
	// independent from TimelineRevision: init objects, manifest snapshots, and
	// claims can change archive contents without changing playback order.
	ArchiveRevision uint64 `json:"archive_revision,omitempty"`
	// ShardedArchive is present only on format-v2 roots. It contains bounded
	// counters for histories stored in archive/v2/ shards; it never contains
	// per-object references.
	ShardedArchive *ShardedArchiveSummary `json:"sharded_archive,omitempty"`
}

// ShardedArchiveSummary is the bounded root summary for a format-v2 archive.
// Counts are high-water summaries; canonical records live in bounded shards.
type ShardedArchiveSummary struct {
	MediaCount               uint64  `json:"media_count,omitempty"`
	InitCount                uint64  `json:"init_count,omitempty"`
	GapCount                 uint64  `json:"gap_count,omitempty"`
	ManifestSnapshotCount    uint64  `json:"manifest_snapshot_count,omitempty"`
	MetadataRevisionCount    uint64  `json:"metadata_revision_count,omitempty"`
	ClaimCount               uint64  `json:"claim_count,omitempty"`
	CoverageObservationCount uint64  `json:"coverage_observation_count,omitempty"`
	ClaimReconcilePending    bool    `json:"claim_reconcile_pending,omitempty"`
	PayloadBytes             uint64  `json:"payload_bytes,omitempty"`
	DurationSeconds          float64 `json:"duration_seconds,omitempty"`
}

// MetadataRevision records a canonical snapshot of the known source title and
// description at the time Core observed a semantic change. Nil means unknown;
// a pointer to an empty string is an explicitly known empty value.
type MetadataRevision struct {
	ObservedAt      time.Time  `json:"observed_at"`
	SourceUpdatedAt *time.Time `json:"source_updated_at,omitempty"`
	Title           *string    `json:"title,omitempty"`
	Description     *string    `json:"description,omitempty"`
}

type Track struct {
	ID                 string `json:"id"`
	SourcePlaylistURL  string `json:"source_playlist_url"`
	Bandwidth          int64  `json:"bandwidth,omitempty"`
	SourceEpoch        uint64 `json:"source_epoch,omitempty"`
	NextArchiveOrdinal uint64 `json:"next_archive_ordinal,omitempty"`
	// LivePresentation stores the append-only identity cursor used only by the
	// active browser-facing live HLS projection. It is independent of both
	// ArchiveOrdinal and the revisionable VOD TimelineOrdinal.
	LivePresentation *LivePresentationState `json:"live_presentation,omitempty"`
	// LiveSlotHighWater is the last fully published presentation slot. It is
	// the root visibility marker for the separate bounded live-tail index.
	LiveSlotHighWater uint64 `json:"live_slot_high_water,omitempty"`
	// Format-v2 bounded summaries. They remain scalar as media shards grow.
	MediaCount              uint64  `json:"media_count,omitempty"`
	InitCount               uint64  `json:"init_count,omitempty"`
	MediaHighWater          uint64  `json:"media_high_water,omitempty"`
	PayloadBytes            uint64  `json:"payload_bytes,omitempty"`
	DurationSeconds         float64 `json:"duration_seconds,omitempty"`
	HasLastObservedSequence bool    `json:"has_last_observed_sequence,omitempty"`
	LastObservedSequence    uint64  `json:"last_observed_sequence,omitempty"`
	// PendingSequences is retained for legacy recordings where source epoch was
	// implicitly zero. Newer recordings use PendingSegments.
	PendingSequences []uint64          `json:"pending_sequences,omitempty"`
	PendingSegments  []PendingSequence `json:"pending_segments,omitempty"`
	InitSegments     []Segment         `json:"init_segments,omitempty"`
	Segments         []Segment         `json:"segments"`
}

// LivePresentationState is persisted with the recording root so an Engine
// handover or restart cannot renumber already published live media.
type LivePresentationState struct {
	NextOrdinal               uint64 `json:"next_ordinal,omitempty"`
	DiscontinuitySequence     uint64 `json:"discontinuity_sequence,omitempty"`
	HasLastCoordinate         bool   `json:"has_last_coordinate,omitempty"`
	LastSourceEpoch           uint64 `json:"last_source_epoch,omitempty"`
	LastDiscontinuitySequence uint64 `json:"last_discontinuity_sequence,omitempty"`
	LastSequence              uint64 `json:"last_sequence,omitempty"`
	HasEpochMapping           bool   `json:"has_epoch_mapping,omitempty"`
	MappedSourceEpoch         uint64 `json:"mapped_source_epoch,omitempty"`
	SourceSequenceBase        uint64 `json:"source_sequence_base,omitempty"`
	PresentationOrdinalBase   uint64 `json:"presentation_ordinal_base,omitempty"`
	MaxSourceSequence         uint64 `json:"max_source_sequence,omitempty"`
	SourceDiscontinuityBase   uint64 `json:"source_discontinuity_base,omitempty"`
	PresentationDiscBase      uint64 `json:"presentation_disc_base,omitempty"`
	EpochBoundary             bool   `json:"epoch_boundary,omitempty"`
	FirstPresentationOrdinal  uint64 `json:"first_presentation_ordinal,omitempty"`
}

type PendingSequence struct {
	SourceEpoch               uint64     `json:"source_epoch,omitempty"`
	DiscontinuitySequence     uint64     `json:"discontinuity_sequence,omitempty"`
	Sequence                  uint64     `json:"sequence"`
	LivePresentationOrdinal   uint64     `json:"live_presentation_ordinal,omitempty"`
	LiveDiscontinuity         bool       `json:"live_discontinuity,omitempty"`
	LiveDiscontinuitySequence uint64     `json:"live_discontinuity_sequence,omitempty"`
	Duration                  float64    `json:"duration,omitempty"`
	ProgramDateTime           *time.Time `json:"program_date_time,omitempty"`
	InitSegmentID             string     `json:"init_segment_id,omitempty"`
}

// ResourceReference is the stable archive representation of an opaque
// adapter-defined resource reference. It deliberately mirrors the existing
// protocol JSON shape without depending on protocol wire types.
type ResourceReference struct {
	Type   string             `json:"resource_type"`
	ID     string             `json:"resource_id"`
	Parent *ResourceReference `json:"parent,omitempty"`
}

// AdapterProvenance records which adapter implementation resolved a source.
// The JSON tags retain the original recording format representation.
type AdapterProvenance struct {
	ID              string `json:"id"`
	Name            string `json:"name,omitempty"`
	Version         string `json:"version"`
	ProtocolVersion int    `json:"protocol_version"`
	Fingerprint     string `json:"descriptor_fingerprint,omitempty"`
}

// Segment describes an original source object. Payload bytes are never decoded
// or transformed; IsInit distinguishes an HLS initialization section.
type Segment struct {
	ID                    string `json:"id"`
	TrackID               string `json:"track_id"`
	Sequence              uint64 `json:"sequence"`
	SourceEpoch           uint64 `json:"source_epoch,omitempty"`
	DiscontinuitySequence uint64 `json:"discontinuity_sequence,omitempty"`
	ArchiveOrdinal        uint64 `json:"archive_ordinal,omitempty"`
	// TimelineOrdinal is a revisionable playback position. Unlike
	// ArchiveOrdinal, it can change when a late prefix or repaired segment is
	// inserted into the timeline.
	TimelineOrdinal uint64 `json:"timeline_ordinal,omitempty"`
	// LivePresentationOrdinal is immutable once a media object enters an active
	// live playlist. It is not the archive storage ordinal or VOD timeline slot.
	LivePresentationOrdinal uint64 `json:"live_presentation_ordinal,omitempty"`
	// LiveDiscontinuity records a boundary in the immutable live presentation.
	// LiveDiscontinuitySequence counts discontinuities before this segment's
	// boundary, as required by EXT-X-DISCONTINUITY-SEQUENCE. It lets a sliding
	// playlist avoid scanning older recording history.
	LiveDiscontinuity         bool       `json:"live_discontinuity,omitempty"`
	LiveDiscontinuitySequence uint64     `json:"live_discontinuity_sequence,omitempty"`
	SourceURI                 string     `json:"source_uri"`
	Duration                  float64    `json:"duration,omitempty"`
	ProgramDateTime           *time.Time `json:"program_date_time,omitempty"`
	InitSegmentID             string     `json:"init_segment_id,omitempty"`
	ByteRange                 *ByteRange `json:"byte_range,omitempty"`
	Discontinuity             bool       `json:"discontinuity,omitempty"`
	StoragePath               string     `json:"storage_path"`
	PayloadSize               int64      `json:"payload_size"`
	SHA256                    string     `json:"sha256"`
	IsInit                    bool       `json:"is_init,omitempty"`
}

type ByteRange struct {
	Length uint64 `json:"length"`
	Offset uint64 `json:"offset"`
}

type Gap struct {
	TrackID                   string     `json:"track_id"`
	SourceEpoch               uint64     `json:"source_epoch,omitempty"`
	DiscontinuitySequence     uint64     `json:"discontinuity_sequence,omitempty"`
	FromSequence              uint64     `json:"from_sequence"`
	ToSequence                uint64     `json:"to_sequence"`
	DetectedAt                time.Time  `json:"detected_at"`
	Reason                    string     `json:"reason"`
	LivePresentationOrdinal   uint64     `json:"live_presentation_ordinal,omitempty"`
	LiveDiscontinuity         bool       `json:"live_discontinuity,omitempty"`
	LiveDiscontinuitySequence uint64     `json:"live_discontinuity_sequence,omitempty"`
	LiveDuration              float64    `json:"live_duration,omitempty"`
	ProgramDateTime           *time.Time `json:"program_date_time,omitempty"`
}

type ManifestSnapshot struct {
	TrackID     string    `json:"track_id"`
	SourceURI   string    `json:"source_uri"`
	StoragePath string    `json:"storage_path"`
	FetchedAt   time.Time `json:"fetched_at"`
	SHA256      string    `json:"sha256"`
	Size        int64     `json:"size"`
}

func (r *Recording) SegmentCount() int {
	if r != nil && r.FormatVersion == 2 && r.ShardedArchive != nil {
		if r.ShardedArchive.MediaCount > uint64(^uint(0)>>1) {
			return int(^uint(0) >> 1)
		}
		return int(r.ShardedArchive.MediaCount)
	}
	count := 0
	for _, track := range r.Tracks {
		count += len(track.Segments)
	}
	return count
}

func (r *Recording) Duration() float64 {
	if r != nil && r.FormatVersion == 2 && r.ShardedArchive != nil {
		return r.ShardedArchive.DurationSeconds
	}
	var duration float64
	for _, track := range r.Tracks {
		for _, segment := range track.Segments {
			duration += segment.Duration
		}
	}
	return duration
}
