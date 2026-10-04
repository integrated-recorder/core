// Package watch owns durable, adapter-neutral intent to record future live
// sessions. Watch state and history are management projections, never part of
// a canonical recording archive.
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
)

const (
	DefaultIntervalSeconds  = 10
	MinIntervalSeconds      = 2
	MaxIntervalSeconds      = 3600
	MaxWatches              = 5000
	MaxInputBytes           = 8 << 10
	MaxSecretBytes          = 16 << 10
	MaxAggregateSecretBytes = 32 << 20
	MaxSecretFileBytes      = 128 << 10
	MaxEventsPerWatch       = 50
	MaxEventsTotal          = 10000
	MaxRelations            = 10000
	RelationRetentionLimit  = MaxRelations
	DefaultListLimit        = 100
	MaxListLimit            = 200
)

var (
	ErrNotFound        = errors.New("watch not found")
	ErrConflict        = errors.New("watch state conflict")
	ErrInvalid         = errors.New("invalid watch")
	ErrQueueFull       = errors.New("watch check queue is full")
	ErrHandoffPaused   = errors.New("watch scheduler is paused for control generation handoff")
	ErrUnsupported     = errors.New("adapter does not support unattended watches")
	ErrServiceClosed   = errors.New("watch service is closed")
	ErrActiveRecording = errors.New("watch already has an active recording")
)

type State string

const (
	StateDisabled          State = "disabled"
	StateOffline           State = "offline"
	StateChecking          State = "checking"
	StateStarting          State = "starting"
	StateRecording         State = "recording"
	StateBackoff           State = "backoff"
	StateAttentionRequired State = "attention_required"
	StateSuppressed        State = "suppressed"
)

// Definition contains only non-secret user input. Secret control values are
// kept in a separate file and never serialized into Watch API views.
type Definition struct {
	ID                        string                    `json:"id"`
	AdapterID                 string                    `json:"adapter_id"`
	Resource                  *adapterproto.ResourceRef `json:"resource,omitempty"`
	Input                     json.RawMessage           `json:"input,omitempty"`
	Enabled                   bool                      `json:"enabled"`
	Title                     string                    `json:"title,omitempty"`
	PreviewMode               string                    `json:"preview_mode,omitempty"`
	CheckIntervalSeconds      int                       `json:"check_interval_seconds"`
	RecordingHistoryTruncated bool                      `json:"recording_history_truncated,omitempty"`
	CreatedAt                 time.Time                 `json:"created_at"`
	UpdatedAt                 time.Time                 `json:"updated_at"`
}

// Runtime is persisted separately from the definition so scheduler recovery
// can distinguish a check failure from a successful offline observation.
type Runtime struct {
	State                   State      `json:"state"`
	LastCheckAt             *time.Time `json:"last_check_at,omitempty"`
	NextCheckAt             *time.Time `json:"next_check_at,omitempty"`
	FailureCount            int        `json:"failure_count,omitempty"`
	ActiveRecordingID       string     `json:"active_recording_id,omitempty"`
	PendingRecordingID      string     `json:"pending_recording_id,omitempty"`
	SessionDigest           string     `json:"session_digest,omitempty"`
	SuppressedSessionDigest string     `json:"suppressed_session_digest,omitempty"`
	Suppressed              bool       `json:"suppressed,omitempty"`
	LastObservedLive        bool       `json:"last_observed_live,omitempty"`
	TerminalOutcome         string     `json:"terminal_outcome,omitempty"`
	LastErrorCode           string     `json:"last_error_code,omitempty"`
	StartingAt              *time.Time `json:"starting_at,omitempty"`
}

type Event struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	At          time.Time `json:"at"`
	State       State     `json:"state,omitempty"`
	RecordingID string    `json:"recording_id,omitempty"`
	Code        string    `json:"error_code,omitempty"`
}

// RecordingRelation is stored outside recording.json. SessionDigest contains
// only SHA-256(session_ref), never the opaque adapter token itself.
type RecordingRelation struct {
	RecordingID   string    `json:"recording_id"`
	WatchID       string    `json:"watch_id"`
	SessionDigest string    `json:"session_digest,omitempty"`
	PartIndex     int       `json:"part_index"`
	CreatedAt     time.Time `json:"created_at"`
}

// View is the redacted public representation. InputSecrets maps field names to
// configured flags; it contains no secret values. CurrentRecordingPreview is
// an optional, summary-only projection filled by the HTTP layer.
type View struct {
	Definition
	AdapterName             string          `json:"adapter_name,omitempty"`
	State                   State           `json:"state"`
	LastCheckAt             *time.Time      `json:"last_checked_at,omitempty"`
	NextCheckAt             *time.Time      `json:"next_check_at,omitempty"`
	FailureCount            int             `json:"failure_count,omitempty"`
	LastErrorCode           string          `json:"last_error_code,omitempty"`
	CurrentRecordingID      string          `json:"current_recording_id,omitempty"`
	CurrentRecordingState   string          `json:"current_recording_state,omitempty"`
	InputSecrets            map[string]bool `json:"input_secret_configured"`
	CurrentRecordingPreview *PreviewSummary `json:"current_recording_preview,omitempty"`
}

type PreviewSummary struct {
	Mode                 string     `json:"mode"`
	State                string     `json:"state"`
	Available            bool       `json:"available"`
	FrameCount           int        `json:"frame_count"`
	ImageArchiveOrdinal  *uint64    `json:"image_archive_ordinal,omitempty"`
	LatestArchiveOrdinal *uint64    `json:"latest_archive_ordinal,omitempty"`
	UpdatedAt            *time.Time `json:"updated_at,omitempty"`
}

type Page[T any] struct {
	Items []T `json:"items"`
}

type Snapshot struct {
	Definition Definition
	Runtime    Runtime
}

type AdapterRuntime interface {
	Descriptor(string) (adapterproto.Descriptor, error)
	ValidateResource(string, *adapterproto.ResourceRef) error
	ValidateWatchInput(string, json.RawMessage, map[string]string, *adapterproto.ResourceRef) error
	WatchCheck(context.Context, string, json.RawMessage, map[string]string, *adapterproto.ResourceRef) (adapterproto.WatchCheckResult, error)
	ResolveLegacyWithInputSecrets(context.Context, string, json.RawMessage, map[string]string, *adapterproto.ResourceRef) (adapterproto.MediaSource, error)
	Provenance(string) (adapterproto.AdapterProvenance, error)
	Get(string) (adapterhost.Adapter, error)
}

type RecordingManager interface {
	StartResolvedWithID(context.Context, string, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error)
	Get(string) (*domain.Recording, error)
	List() []*domain.Recording
}

type PreviewSetter func(recordingID, mode string) error

type Options struct {
	Now            func() time.Time
	Jitter         func(time.Duration) time.Duration
	CheckTimeout   time.Duration
	Preview        PreviewSetter
	CurrentPreview func(recordingID string) *PreviewSummary
}

type Update struct {
	AdapterID            string                    `json:"adapter_id"`
	Resource             *adapterproto.ResourceRef `json:"resource,omitempty"`
	ClearResource        bool                      `json:"clear_resource,omitempty"`
	Input                json.RawMessage           `json:"input"`
	InputSecrets         map[string]string         `json:"input_secrets,omitempty"`
	ClearInputSecrets    []string                  `json:"clear_input_secrets,omitempty"`
	Title                *string                   `json:"title,omitempty"`
	PreviewMode          string                    `json:"preview_mode,omitempty"`
	CheckIntervalSeconds *int                      `json:"check_interval_seconds,omitempty"`
	Enabled              *bool                     `json:"enabled,omitempty"`
}
