// Package archiveindex defines the transport-independent identity and claim
// model for repairable recording archives. It deliberately performs no I/O.
//
// Callers must serialize persisted read-modify-write operations with the
// recording's existing owner/persist fence. Independent Inventory values may
// be used concurrently; this package has no shared mutable state.
package archiveindex

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/domain"
)

const SchemaVersion = 1

const (
	MaxRecordingIDBytes         = 256
	MaxAdapterIDBytes           = 128
	MaxTrackIDBytes             = 128
	MaxClaimIDBytes             = 512
	MaxPayloadPathBytes         = 1024
	MaxEvidenceBytes            = 256
	MaxReasonBytes              = 256
	MaxInitIdentityBytes        = 256
	MaxSegments                 = 25_000
	MaxClaimsPerSegment         = 32
	MaxTotalClaims              = 100_000
	MaxCoverageIntervals        = 50_000
	MaxSerializedBytes          = 16 << 20
	MaxPayloadBytes       int64 = 1 << 30 // Match Core's largest accepted object.
	MaxSegmentDuration          = 24 * time.Hour
	MaxResourceDepth            = 8
	MaxResourceFieldBytes       = 1024
	MaxSessionRefBytes          = 4096
)

type ClaimSource string

const (
	ClaimUnknown    ClaimSource = "unknown"
	ClaimLiveOrigin ClaimSource = "live_origin"
	ClaimHistorical ClaimSource = "historical"
	ClaimPeer       ClaimSource = "peer"
	ClaimImport     ClaimSource = "import"
)

type Verification string

const (
	VerificationVerified   Verification = "verified"
	VerificationUnverified Verification = "unverified"
	VerificationFailed     Verification = "failed"
)

type ClaimDisposition string

const (
	DispositionAccepted  ClaimDisposition = "accepted"
	DispositionDuplicate ClaimDisposition = "duplicate"
	DispositionConflict  ClaimDisposition = "conflict"
)

type CoverageState string

const (
	CoverageUnknown           CoverageState = "unknown"
	CoverageKnownMissing      CoverageState = "known_missing"
	CoverageAcquisitionFailed CoverageState = "acquisition_failed"
	CoveragePresent           CoverageState = "present"
	CoverageConflict          CoverageState = "conflict"
)

type CaptureState string

const (
	CaptureOpen   CaptureState = "open"
	CaptureClosed CaptureState = "closed"
)

type ArchiveState string

const (
	ArchiveRepairable ArchiveState = "repairable"
	ArchiveSealed     ArchiveState = "sealed"
)

// ObjectKind is part of a coordinate so an initialization object cannot
// collide with a media segment that happens to use the same HLS sequence.
// Empty Kind is accepted as media for source compatibility with callers that
// only need the common media-segment coordinate.
type ObjectKind string

const (
	ObjectMedia ObjectKind = "media"
	ObjectInit  ObjectKind = "init"
)

// SessionIdentity contains no raw resource identifier, session reference, or
// URL. The hashes are one-way fingerprints of opaque source identity input.
type SessionIdentity struct {
	ID           string `json:"id"`
	AdapterID    string `json:"adapter_id"`
	ResourceHash string `json:"resource_hash,omitempty"`
	SessionHash  string `json:"session_hash,omitempty"`
}

// Coordinate names a logical source object independently of discovery order
// and archive ordinal.
type Coordinate struct {
	SessionID             string     `json:"session_id"`
	TrackID               string     `json:"track_id"`
	SourceEpoch           uint64     `json:"source_epoch"`
	DiscontinuitySequence uint64     `json:"discontinuity_sequence"`
	Sequence              uint64     `json:"sequence"`
	Kind                  ObjectKind `json:"kind,omitempty"`
}

type Claim struct {
	ID              string           `json:"id"`
	Source          ClaimSource      `json:"source"`
	AcquiredAt      time.Time        `json:"acquired_at"`
	PayloadPath     string           `json:"payload_path"`
	Size            int64            `json:"size"`
	SHA256          string           `json:"sha256"`
	Verification    Verification     `json:"verification"`
	Disposition     ClaimDisposition `json:"disposition"`
	Evidence        string           `json:"evidence,omitempty"`
	LegacySegmentID string           `json:"legacy_segment_id,omitempty"`
}

type Segment struct {
	ID              string        `json:"id"`
	Coordinate      Coordinate    `json:"coordinate"`
	Duration        float64       `json:"duration"`
	ProgramDateTime *time.Time    `json:"program_date_time,omitempty"`
	InitIdentity    string        `json:"init_identity,omitempty"`
	SelectedClaimID string        `json:"selected_claim_id,omitempty"`
	Claims          []Claim       `json:"claims"`
	TimelineOrdinal uint64        `json:"timeline_ordinal,omitempty"`
	State           CoverageState `json:"state"`
}

type Coverage struct {
	SessionID             string        `json:"session_id"`
	TrackID               string        `json:"track_id"`
	SourceEpoch           uint64        `json:"source_epoch"`
	DiscontinuitySequence uint64        `json:"discontinuity_sequence"`
	FromSequence          uint64        `json:"from_sequence"`
	ToSequence            uint64        `json:"to_sequence"`
	State                 CoverageState `json:"state"`
	ObservedAt            time.Time     `json:"observed_at"`
	Reason                string        `json:"reason,omitempty"`
	Kind                  ObjectKind    `json:"kind,omitempty"`
}

type Inventory struct {
	SchemaVersion  int             `json:"schema_version"`
	RecordingID    string          `json:"recording_id"`
	Session        SessionIdentity `json:"session"`
	Revision       uint64          `json:"revision"`
	Capture        CaptureState    `json:"capture_state"`
	Archive        ArchiveState    `json:"archive_state"`
	LegacyAdoption bool            `json:"legacy_adoption,omitempty"`
	Segments       []Segment       `json:"segments"`
	Coverage       []Coverage      `json:"coverage"`
}

type ClaimInput struct {
	Coordinate      Coordinate
	ClaimID         string
	Source          ClaimSource
	AcquiredAt      time.Time
	PayloadPath     string
	Size            int64
	SHA256          string
	Verification    Verification
	Evidence        string
	LegacySegmentID string
	Duration        float64
	ProgramDateTime *time.Time
	InitIdentity    string
}

var (
	ErrInvalidInventory    = errors.New("invalid archive inventory")
	ErrInvalidClaim        = errors.New("invalid archive claim")
	ErrSealed              = errors.New("archive is sealed")
	ErrClaimIdentityChange = errors.New("claim ID already exists with different metadata")
	ErrRevisionOverflow    = errors.New("archive inventory revision overflow")
)

// NewInventory creates an open capture whose archive remains repairable after
// capture closes. Revision starts at one so persisted snapshots are versioned.
func NewInventory(recordingID string, session SessionIdentity) (Inventory, error) {
	inventory := Inventory{
		SchemaVersion: SchemaVersion,
		RecordingID:   recordingID,
		Session:       session,
		Revision:      1,
		Capture:       CaptureOpen,
		Archive:       ArchiveRepairable,
		Segments:      []Segment{},
		Coverage:      []Coverage{},
	}
	if err := inventory.Validate(); err != nil {
		return Inventory{}, err
	}
	return inventory, nil
}

// NewSessionIdentity derives a stable opaque session identity. Source identity
// is shared across recordings only when the adapter supplies an explicit
// session reference. A resource alone can identify a stable channel rather
// than a broadcast, so it is retained only as a hash and the identity remains
// recording-scoped when sessionRef is absent.
func NewSessionIdentity(recordingID, adapterID string, resource *domain.ResourceReference, sessionRef string) (SessionIdentity, error) {
	if !validText(recordingID, MaxRecordingIDBytes, false) || !validToken(adapterID, MaxAdapterIDBytes) {
		return SessionIdentity{}, fmt.Errorf("%w: invalid recording or adapter identity", ErrInvalidInventory)
	}
	if len(sessionRef) > MaxSessionRefBytes || !utf8.ValidString(sessionRef) || hasControl(sessionRef) {
		return SessionIdentity{}, fmt.Errorf("%w: invalid session reference", ErrInvalidInventory)
	}
	resourceHash := ""
	if resource != nil {
		encoded, err := encodeResource(resource, 0)
		if err != nil {
			return SessionIdentity{}, err
		}
		resourceHash = digestString(encoded)
	}
	sessionHash := ""
	if sessionRef != "" {
		sessionHash = digestString([]byte(sessionRef))
	}
	identityInput := struct {
		AdapterID    string `json:"adapter_id"`
		ResourceHash string `json:"resource_hash,omitempty"`
		SessionHash  string `json:"session_hash,omitempty"`
		FallbackID   string `json:"fallback_recording_id,omitempty"`
	}{AdapterID: adapterID, ResourceHash: resourceHash, SessionHash: sessionHash}
	if sessionHash == "" {
		identityInput.FallbackID = recordingID
	}
	encoded, err := json.Marshal(identityInput)
	if err != nil {
		return SessionIdentity{}, fmt.Errorf("%w: encode identity", ErrInvalidInventory)
	}
	return SessionIdentity{ID: "session-" + digestString(encoded), AdapterID: adapterID, ResourceHash: resourceHash, SessionHash: sessionHash}, nil
}

func encodeResource(resource *domain.ResourceReference, depth int) ([]byte, error) {
	if resource == nil {
		return []byte("null"), nil
	}
	if depth >= MaxResourceDepth || !validText(resource.Type, MaxResourceFieldBytes, false) || !validText(resource.ID, MaxResourceFieldBytes, false) {
		return nil, fmt.Errorf("%w: invalid resource reference", ErrInvalidInventory)
	}
	parent, err := encodeResource(resource.Parent, depth+1)
	if err != nil {
		return nil, err
	}
	value := struct {
		Type   string          `json:"resource_type"`
		ID     string          `json:"resource_id"`
		Parent json.RawMessage `json:"parent,omitempty"`
	}{Type: resource.Type, ID: resource.ID, Parent: parent}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > MaxResourceDepth*(MaxResourceFieldBytes*2+64) {
		return nil, fmt.Errorf("%w: resource reference is too large", ErrInvalidInventory)
	}
	return encoded, nil
}

// SegmentIdentity returns a stable identity derived only from source
// coordinates, never acquisition order or a mutable timeline ordinal.
func SegmentIdentity(coordinate Coordinate) (string, error) {
	coordinate = normalizeCoordinate(coordinate)
	if err := validateCoordinate(coordinate); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(coordinate)
	if err != nil {
		return "", fmt.Errorf("%w: encode coordinate", ErrInvalidInventory)
	}
	return "segment-" + digestString(encoded), nil
}

func (inventory Inventory) Validate() error {
	if inventory.SchemaVersion != SchemaVersion || !validText(inventory.RecordingID, MaxRecordingIDBytes, false) || inventory.Revision == 0 {
		return fmt.Errorf("%w: unsupported schema, recording ID, or revision", ErrInvalidInventory)
	}
	if err := validateSession(inventory.Session); err != nil {
		return err
	}
	if inventory.Capture != CaptureOpen && inventory.Capture != CaptureClosed {
		return fmt.Errorf("%w: invalid capture state", ErrInvalidInventory)
	}
	if inventory.Archive != ArchiveRepairable && inventory.Archive != ArchiveSealed {
		return fmt.Errorf("%w: invalid archive state", ErrInvalidInventory)
	}
	if inventory.Archive == ArchiveSealed && inventory.Capture != CaptureClosed {
		return fmt.Errorf("%w: sealed archive has open capture", ErrInvalidInventory)
	}
	if len(inventory.Segments) > MaxSegments || len(inventory.Coverage) > MaxCoverageIntervals {
		return fmt.Errorf("%w: inventory count limit exceeded", ErrInvalidInventory)
	}
	upperBound, ok := inventorySizeUpperBound(inventory)
	if !ok || upperBound > MaxSerializedBytes {
		return fmt.Errorf("%w: serialized inventory exceeds %d bytes", ErrInvalidInventory, MaxSerializedBytes)
	}
	segmentIDs := make(map[string]struct{}, len(inventory.Segments))
	coordinates := make(map[Coordinate]struct{}, len(inventory.Segments))
	claimIDs := make(map[string]struct{})
	totalClaims := 0
	for i := range inventory.Segments {
		segment := inventory.Segments[i]
		coordinate := normalizeCoordinate(segment.Coordinate)
		if err := validateCoordinate(coordinate); err != nil || coordinate.SessionID != inventory.Session.ID || segment.Coordinate != coordinate {
			return fmt.Errorf("%w: invalid segment coordinate", ErrInvalidInventory)
		}
		identity, err := SegmentIdentity(coordinate)
		if err != nil || segment.ID != identity {
			return fmt.Errorf("%w: segment identity does not match coordinate", ErrInvalidInventory)
		}
		if _, ok := segmentIDs[segment.ID]; ok {
			return fmt.Errorf("%w: duplicate segment identity", ErrInvalidInventory)
		}
		if _, ok := coordinates[coordinate]; ok {
			return fmt.Errorf("%w: duplicate segment coordinate", ErrInvalidInventory)
		}
		segmentIDs[segment.ID] = struct{}{}
		coordinates[coordinate] = struct{}{}
		if !validDuration(segment.Duration, coordinate.Kind) || !validText(segment.InitIdentity, MaxInitIdentityBytes, true) || !validCoverageState(segment.State) {
			return fmt.Errorf("%w: invalid segment metadata", ErrInvalidInventory)
		}
		if segment.ProgramDateTime != nil && segment.ProgramDateTime.IsZero() {
			return fmt.Errorf("%w: invalid program date time", ErrInvalidInventory)
		}
		if len(segment.Claims) == 0 || len(segment.Claims) > MaxClaimsPerSegment {
			return fmt.Errorf("%w: invalid claim count", ErrInvalidInventory)
		}
		totalClaims += len(segment.Claims)
		if totalClaims > MaxTotalClaims {
			return fmt.Errorf("%w: claim count limit exceeded", ErrInvalidInventory)
		}
		selectedFound := segment.SelectedClaimID == ""
		var firstPayload *Claim
		differentPayloads := false
		verifiedFound := false
		for _, claim := range segment.Claims {
			if err := validateClaim(claim); err != nil {
				return err
			}
			if _, ok := claimIDs[claim.ID]; ok {
				return fmt.Errorf("%w: duplicate claim identity", ErrInvalidInventory)
			}
			claimIDs[claim.ID] = struct{}{}
			if claim.ID == segment.SelectedClaimID {
				selectedFound = claim.Verification == VerificationVerified
			}
			if claim.Verification == VerificationVerified {
				verifiedFound = true
			}
			if firstPayload == nil {
				copy := claim
				firstPayload = &copy
			} else if firstPayload.Size != claim.Size || firstPayload.SHA256 != claim.SHA256 {
				differentPayloads = true
			}
		}
		if !selectedFound {
			return fmt.Errorf("%w: selected claim is missing or unverified", ErrInvalidInventory)
		}
		if segment.State == CoveragePresent && segment.SelectedClaimID == "" {
			return fmt.Errorf("%w: present segment has no selected claim", ErrInvalidInventory)
		}
		if segment.State == CoverageUnknown && segment.SelectedClaimID != "" {
			return fmt.Errorf("%w: unknown segment has a selected claim", ErrInvalidInventory)
		}
		if verifiedFound && segment.SelectedClaimID == "" {
			return fmt.Errorf("%w: verified claim is not selected or conflicted", ErrInvalidInventory)
		}
		if differentPayloads != (segment.State == CoverageConflict) {
			return fmt.Errorf("%w: segment state does not match claim payloads", ErrInvalidInventory)
		}
		if segment.TimelineOrdinal > 0 && (!timelineSelected(segment) || coordinate.Kind != ObjectMedia) {
			return fmt.Errorf("%w: object without selected canonical bytes has timeline ordinal", ErrInvalidInventory)
		}
	}
	if err := validateTimelineOrdinals(inventory.Segments); err != nil {
		return err
	}
	for _, item := range inventory.Coverage {
		if err := validateCoverage(item); err != nil {
			return err
		}
		if item.SessionID != inventory.Session.ID || item.Kind != normalizeKind(item.Kind) {
			return fmt.Errorf("%w: coverage session mismatch", ErrInvalidInventory)
		}
	}
	encoded, err := json.Marshal(inventory)
	if err != nil || len(encoded) > MaxSerializedBytes {
		return fmt.Errorf("%w: serialized inventory exceeds %d bytes", ErrInvalidInventory, MaxSerializedBytes)
	}
	return nil
}

// ApplyClaim adds a byte claim to a logical coordinate. Existing selected
// bytes are never replaced. Reapplying an identical claim ID and metadata is
// idempotent; reusing that ID for different metadata is rejected.
func ApplyClaim(inventory *Inventory, input ClaimInput) (ClaimDisposition, error) {
	if inventory == nil {
		return "", fmt.Errorf("%w: nil inventory", ErrInvalidInventory)
	}
	if err := inventory.Validate(); err != nil {
		return "", err
	}
	if inventory.Archive == ArchiveSealed {
		return "", ErrSealed
	}
	input.Coordinate = normalizeCoordinate(input.Coordinate)
	if err := validateCoordinate(input.Coordinate); err != nil || input.Coordinate.SessionID != inventory.Session.ID {
		return "", fmt.Errorf("%w: coordinate does not belong to inventory", ErrInvalidClaim)
	}
	newClaim := Claim{
		ID: input.ClaimID, Source: input.Source, AcquiredAt: input.AcquiredAt,
		PayloadPath: input.PayloadPath, Size: input.Size, SHA256: input.SHA256,
		Verification: input.Verification, Disposition: DispositionAccepted, Evidence: input.Evidence,
		LegacySegmentID: input.LegacySegmentID,
	}
	if err := validateClaim(newClaim); err != nil {
		return "", err
	}
	if !validDuration(input.Duration, input.Coordinate.Kind) || !validText(input.InitIdentity, MaxInitIdentityBytes, true) || input.ProgramDateTime != nil && input.ProgramDateTime.IsZero() {
		return "", fmt.Errorf("%w: invalid segment metadata", ErrInvalidClaim)
	}
	for _, segment := range inventory.Segments {
		for _, prior := range segment.Claims {
			if prior.ID != newClaim.ID {
				continue
			}
			if segment.Coordinate != input.Coordinate || !sameClaimMetadata(prior, newClaim) || !sameSegmentMetadata(segment, input) {
				return "", ErrClaimIdentityChange
			}
			return prior.Disposition, nil
		}
	}

	segmentIndex := -1
	for i := range inventory.Segments {
		if inventory.Segments[i].Coordinate == input.Coordinate {
			segmentIndex = i
			break
		}
	}
	if segmentIndex < 0 && len(inventory.Segments) >= MaxSegments {
		return "", fmt.Errorf("%w: segment count limit exceeded", ErrInvalidClaim)
	}
	if segmentIndex >= 0 && len(inventory.Segments[segmentIndex].Claims) >= MaxClaimsPerSegment {
		return "", fmt.Errorf("%w: claim count limit exceeded", ErrInvalidClaim)
	}
	claimCount := 0
	for _, segment := range inventory.Segments {
		claimCount += len(segment.Claims)
	}
	if claimCount >= MaxTotalClaims {
		return "", fmt.Errorf("%w: total claim count limit exceeded", ErrInvalidClaim)
	}
	candidate := cloneInventory(*inventory)
	if err := incrementRevision(&candidate); err != nil {
		return "", err
	}

	if segmentIndex < 0 {
		segmentID, _ := SegmentIdentity(input.Coordinate)
		segment := Segment{ID: segmentID, Coordinate: input.Coordinate, Duration: input.Duration, ProgramDateTime: cloneTime(input.ProgramDateTime), InitIdentity: input.InitIdentity, State: CoverageUnknown, Claims: []Claim{}}
		newClaim.Disposition = DispositionAccepted
		segment.Claims = append(segment.Claims, newClaim)
		if newClaim.Verification == VerificationVerified {
			segment.SelectedClaimID = newClaim.ID
			segment.State = CoveragePresent
		}
		candidate.Segments = append(candidate.Segments, segment)
		segmentIndex = len(candidate.Segments) - 1
	} else {
		segment := &candidate.Segments[segmentIndex]
		newClaim.Disposition = dispositionAgainst(segment.Claims, newClaim)
		segment.Claims = append(segment.Claims, newClaim)
		if newClaim.Disposition == DispositionConflict {
			segment.State = CoverageConflict
			if newClaim.Verification == VerificationVerified && segment.SelectedClaimID == "" {
				segment.SelectedClaimID = newClaim.ID
			}
		} else if newClaim.Verification == VerificationVerified && segment.SelectedClaimID == "" {
			segment.SelectedClaimID = newClaim.ID
			segment.State = CoveragePresent
		}
	}
	if _, err := rebuildTimeline(&candidate); err != nil {
		return "", err
	}
	if err := candidate.Validate(); err != nil {
		return "", err
	}
	result := candidate.Segments[segmentIndex].Claims[len(candidate.Segments[segmentIndex].Claims)-1].Disposition
	*inventory = candidate
	return result, nil
}

// ApplyCoverage records a bounded source-coverage observation. It is
// idempotent for an identical observation. Present/conflict observations
// cannot be hidden by later missing/failure intervals; CoverageAt applies that
// precedence when overlapping observations are queried.
func ApplyCoverage(inventory *Inventory, coverage Coverage) error {
	if inventory == nil {
		return fmt.Errorf("%w: nil inventory", ErrInvalidInventory)
	}
	if err := inventory.Validate(); err != nil {
		return err
	}
	if inventory.Archive == ArchiveSealed {
		return ErrSealed
	}
	coverage.Kind = normalizeKind(coverage.Kind)
	if err := validateCoverage(coverage); err != nil {
		return err
	}
	if coverage.SessionID != inventory.Session.ID {
		return fmt.Errorf("%w: coverage session mismatch", ErrInvalidInventory)
	}
	for _, prior := range inventory.Coverage {
		if sameCoverage(prior, coverage) {
			return nil
		}
	}
	if len(inventory.Coverage) >= MaxCoverageIntervals {
		return fmt.Errorf("%w: coverage count limit exceeded", ErrInvalidInventory)
	}
	candidate := cloneInventory(*inventory)
	if err := incrementRevision(&candidate); err != nil {
		return err
	}
	candidate.Coverage = append(candidate.Coverage, coverage)
	if err := candidate.Validate(); err != nil {
		return err
	}
	*inventory = candidate
	return nil
}

// CoverageAt returns the strongest known state for a coordinate. A segment
// backed by selected bytes overrides stale missing/failure ranges.
func CoverageAt(inventory Inventory, coordinate Coordinate) CoverageState {
	coordinate = normalizeCoordinate(coordinate)
	for _, segment := range inventory.Segments {
		if segment.Coordinate == coordinate && (segment.State == CoveragePresent || segment.State == CoverageConflict) {
			return segment.State
		}
	}
	state := CoverageUnknown
	var newest time.Time
	for _, item := range inventory.Coverage {
		if !coverageContains(item, coordinate) {
			continue
		}
		if item.State == CoverageConflict {
			return CoverageConflict
		}
		if item.State == CoveragePresent {
			state = CoveragePresent
			continue
		}
		if state == CoveragePresent {
			continue
		}
		if item.ObservedAt.After(newest) {
			newest = item.ObservedAt
			state = item.State
		}
	}
	return state
}

// RebuildTimeline assigns compact one-based ordinals to present media objects
// in deterministic source-coordinate order. It increments revision only when
// the projection actually changes.
func RebuildTimeline(inventory *Inventory) error {
	if inventory == nil {
		return fmt.Errorf("%w: nil inventory", ErrInvalidInventory)
	}
	if err := inventory.Validate(); err != nil {
		return err
	}
	candidate := cloneInventory(*inventory)
	changed, err := rebuildTimeline(&candidate)
	if err != nil {
		return err
	}
	if changed {
		if err := incrementRevision(&candidate); err != nil {
			return err
		}
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	*inventory = candidate
	return nil
}

// CloseCapture stops live capture while leaving historical repair available.
func CloseCapture(inventory *Inventory) error {
	if inventory == nil {
		return fmt.Errorf("%w: nil inventory", ErrInvalidInventory)
	}
	if err := inventory.Validate(); err != nil {
		return err
	}
	if inventory.Capture == CaptureClosed {
		return nil
	}
	candidate := cloneInventory(*inventory)
	if err := incrementRevision(&candidate); err != nil {
		return err
	}
	candidate.Capture = CaptureClosed
	if err := candidate.Validate(); err != nil {
		return err
	}
	*inventory = candidate
	return nil
}

// SealArchive is the separate, explicit transition that ends repairability.
func SealArchive(inventory *Inventory) error {
	if inventory == nil {
		return fmt.Errorf("%w: nil inventory", ErrInvalidInventory)
	}
	if err := inventory.Validate(); err != nil {
		return err
	}
	if inventory.Capture != CaptureClosed {
		return fmt.Errorf("%w: capture must be closed before sealing", ErrInvalidInventory)
	}
	if inventory.Archive == ArchiveSealed {
		return nil
	}
	candidate := cloneInventory(*inventory)
	if err := incrementRevision(&candidate); err != nil {
		return err
	}
	candidate.Archive = ArchiveSealed
	if err := candidate.Validate(); err != nil {
		return err
	}
	*inventory = candidate
	return nil
}

// FromLegacy adopts a pre-index domain projection without changing it or
// touching payload objects. It marks the result as a legacy adoption.
func FromLegacy(recording *domain.Recording) (Inventory, error) {
	return FromRecording(recording, true)
}

// FromRecording builds an inventory projection from the existing domain
// recording. Set legacyAdoption only when this conversion is actually
// adopting an archive that has no archive-index provenance; callers restoring
// a newer recording after sidecar loss should pass false.
func FromRecording(recording *domain.Recording, legacyAdoption bool) (Inventory, error) {
	if recording == nil || !validText(recording.ID, MaxRecordingIDBytes, false) {
		return Inventory{}, fmt.Errorf("%w: invalid legacy recording", ErrInvalidInventory)
	}
	adapterID := recording.AdapterID
	if adapterID == "" && recording.Adapter != nil {
		adapterID = recording.Adapter.ID
	}
	if adapterID == "" {
		adapterID = "legacy"
	}
	identity, err := NewSessionIdentity(recording.ID, adapterID, recording.Resource, "")
	if err != nil {
		return Inventory{}, err
	}
	if validSessionFingerprint(recording.SourceSessionID) {
		identity.ID = recording.SourceSessionID
	} else {
		// Scope legacy identity to the recording even if a resource reference
		// was present; old source fields did not prove a stable broadcast.
		identity.ID = "session-" + digestString([]byte("legacy\x00"+recording.ID+"\x00"+adapterID+"\x00"+identity.ResourceHash))
	}
	inventory, err := NewInventory(recording.ID, identity)
	if err != nil {
		return Inventory{}, err
	}
	inventory.LegacyAdoption = legacyAdoption
	if recording.State != domain.StateRecording {
		inventory.Capture = CaptureClosed
	}
	if recording.ArchiveSealed {
		inventory.Archive = ArchiveSealed
	}
	acquiredAt := recording.StartedAt
	if acquiredAt.IsZero() {
		acquiredAt = recording.CreatedAt
	}
	if acquiredAt.IsZero() {
		acquiredAt = time.Unix(0, 0).UTC()
	}
	trackIDs := make([]string, 0, len(recording.Tracks))
	for key := range recording.Tracks {
		trackIDs = append(trackIDs, key)
	}
	sort.Strings(trackIDs)
	for _, key := range trackIDs {
		track := recording.Tracks[key]
		if track == nil {
			return Inventory{}, fmt.Errorf("%w: nil legacy track", ErrInvalidInventory)
		}
		fallbackTrack := track.ID
		if fallbackTrack == "" {
			fallbackTrack = key
		}
		for _, legacy := range track.Segments {
			if err := appendLegacySegment(&inventory, legacy, fallbackTrack, ObjectMedia, acquiredAt); err != nil {
				return Inventory{}, err
			}
		}
		for _, legacy := range track.InitSegments {
			if err := appendLegacySegment(&inventory, legacy, fallbackTrack, ObjectInit, acquiredAt); err != nil {
				return Inventory{}, err
			}
		}
	}
	for _, gap := range recording.Gaps {
		trackID := gap.TrackID
		if trackID == "" {
			return Inventory{}, fmt.Errorf("%w: legacy gap has no track", ErrInvalidInventory)
		}
		observed := gap.DetectedAt
		if observed.IsZero() {
			observed = acquiredAt
		}
		coverage := Coverage{
			SessionID: identity.ID, TrackID: trackID, SourceEpoch: gap.SourceEpoch,
			DiscontinuitySequence: gap.DiscontinuitySequence,
			FromSequence:          gap.FromSequence, ToSequence: gap.ToSequence,
			State: CoverageKnownMissing, ObservedAt: observed, Reason: gap.Reason,
			Kind: ObjectMedia,
		}
		if err := validateCoverage(coverage); err != nil {
			return Inventory{}, err
		}
		inventory.Coverage = append(inventory.Coverage, coverage)
		if len(inventory.Coverage) > MaxCoverageIntervals {
			return Inventory{}, fmt.Errorf("%w: legacy coverage limit exceeded", ErrInvalidInventory)
		}
	}
	if len(inventory.Segments) > MaxSegments {
		return Inventory{}, fmt.Errorf("%w: legacy segment limit exceeded", ErrInvalidInventory)
	}
	if _, err := rebuildTimeline(&inventory); err != nil {
		return Inventory{}, err
	}
	if err := inventory.Validate(); err != nil {
		return Inventory{}, err
	}
	return inventory, nil
}

func appendLegacySegment(inventory *Inventory, legacy domain.Segment, fallbackTrack string, kind ObjectKind, acquiredAt time.Time) error {
	if len(inventory.Segments) >= MaxSegments {
		return fmt.Errorf("%w: legacy segment limit exceeded", ErrInvalidInventory)
	}
	claimCount := 0
	for _, existing := range inventory.Segments {
		claimCount += len(existing.Claims)
	}
	if claimCount >= MaxTotalClaims {
		return fmt.Errorf("%w: legacy claim limit exceeded", ErrInvalidInventory)
	}
	trackID := legacy.TrackID
	if trackID == "" {
		trackID = fallbackTrack
	}
	coordinate := Coordinate{
		SessionID: inventory.Session.ID, TrackID: trackID,
		SourceEpoch: legacy.SourceEpoch, DiscontinuitySequence: legacy.DiscontinuitySequence,
		Sequence: legacy.Sequence, Kind: kind,
	}
	segmentID, err := SegmentIdentity(coordinate)
	if err != nil {
		return err
	}
	claimID := "legacy-" + string(kind) + ":" + legacy.ID
	if legacy.ID == "" {
		return fmt.Errorf("%w: legacy segment has no ID", ErrInvalidInventory)
	}
	segment := Segment{
		ID: segmentID, Coordinate: coordinate, Duration: legacy.Duration,
		ProgramDateTime: cloneTime(legacy.ProgramDateTime), InitIdentity: legacy.InitSegmentID,
		SelectedClaimID: claimID, TimelineOrdinal: 0, State: CoveragePresent,
		Claims: []Claim{{
			ID: claimID, Source: ClaimUnknown, AcquiredAt: acquiredAt,
			PayloadPath: legacy.StoragePath, Size: legacy.PayloadSize, SHA256: legacy.SHA256,
			Verification: VerificationVerified, Disposition: DispositionAccepted,
			Evidence: "legacy archive adoption", LegacySegmentID: legacy.ID,
		}},
	}
	if err := validateSegmentForLegacy(segment); err != nil {
		return err
	}
	for _, existing := range inventory.Segments {
		if existing.ID == segment.ID {
			return fmt.Errorf("%w: duplicate legacy coordinate", ErrInvalidInventory)
		}
	}
	inventory.Segments = append(inventory.Segments, segment)
	return nil
}

func validateSegmentForLegacy(segment Segment) error {
	if !validDuration(segment.Duration, segment.Coordinate.Kind) {
		return fmt.Errorf("%w: invalid legacy segment duration", ErrInvalidInventory)
	}
	if err := validateClaim(segment.Claims[0]); err != nil {
		return err
	}
	return nil
}

func rebuildTimeline(inventory *Inventory) (bool, error) {
	indices := make([]int, len(inventory.Segments))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return coordinateLess(inventory.Segments[indices[i]].Coordinate, inventory.Segments[indices[j]].Coordinate)
	})
	changed := false
	ordinal := uint64(0)
	for _, index := range indices {
		segment := &inventory.Segments[index]
		want := uint64(0)
		if timelineSelected(*segment) && segment.Coordinate.Kind == ObjectMedia {
			ordinal++
			want = ordinal
		}
		if segment.TimelineOrdinal != want {
			segment.TimelineOrdinal = want
			changed = true
		}
	}
	return changed, nil
}

func validateTimelineOrdinals(segments []Segment) error {
	indices := make([]int, len(segments))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return coordinateLess(segments[indices[i]].Coordinate, segments[indices[j]].Coordinate)
	})
	want := uint64(0)
	for _, index := range indices {
		segment := segments[index]
		if timelineSelected(segment) && segment.Coordinate.Kind == ObjectMedia {
			want++
			if segment.TimelineOrdinal != want {
				return fmt.Errorf("%w: timeline ordinals are not monotonic", ErrInvalidInventory)
			}
		} else if segment.TimelineOrdinal != 0 {
			return fmt.Errorf("%w: non-present object has timeline ordinal", ErrInvalidInventory)
		}
	}
	return nil
}

func timelineSelected(segment Segment) bool {
	if segment.SelectedClaimID == "" || (segment.State != CoveragePresent && segment.State != CoverageConflict) {
		return false
	}
	for _, claim := range segment.Claims {
		if claim.ID == segment.SelectedClaimID {
			return claim.Verification == VerificationVerified
		}
	}
	return false
}

func dispositionAgainst(existing []Claim, incoming Claim) ClaimDisposition {
	for _, prior := range existing {
		if prior.Size != incoming.Size || prior.SHA256 != incoming.SHA256 {
			return DispositionConflict
		}
	}
	if len(existing) > 0 {
		return DispositionDuplicate
	}
	return DispositionAccepted
}

func sameClaimMetadata(left, right Claim) bool {
	return left.ID == right.ID && left.Source == right.Source && left.AcquiredAt.Equal(right.AcquiredAt) &&
		left.PayloadPath == right.PayloadPath && left.Size == right.Size && left.SHA256 == right.SHA256 &&
		left.Verification == right.Verification && left.Evidence == right.Evidence && left.LegacySegmentID == right.LegacySegmentID
}

func sameSegmentMetadata(segment Segment, input ClaimInput) bool {
	return segment.Duration == input.Duration && sameOptionalTime(segment.ProgramDateTime, input.ProgramDateTime) && segment.InitIdentity == input.InitIdentity
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func sameCoverage(left, right Coverage) bool {
	return left.SessionID == right.SessionID && left.TrackID == right.TrackID &&
		left.SourceEpoch == right.SourceEpoch && left.DiscontinuitySequence == right.DiscontinuitySequence &&
		left.FromSequence == right.FromSequence && left.ToSequence == right.ToSequence &&
		left.State == right.State && left.ObservedAt.Equal(right.ObservedAt) &&
		left.Reason == right.Reason && normalizeKind(left.Kind) == normalizeKind(right.Kind)
}

func validateSession(session SessionIdentity) error {
	if !validSessionFingerprint(session.ID) || !validToken(session.AdapterID, MaxAdapterIDBytes) {
		return fmt.Errorf("%w: invalid session identity", ErrInvalidInventory)
	}
	for _, digest := range []string{session.ResourceHash, session.SessionHash} {
		if digest != "" && !validSHA256(digest) {
			return fmt.Errorf("%w: invalid source identity hash", ErrInvalidInventory)
		}
	}
	return nil
}

func validateCoordinate(coordinate Coordinate) error {
	coordinate = normalizeCoordinate(coordinate)
	if !strings.HasPrefix(coordinate.SessionID, "session-") || len(coordinate.SessionID) != len("session-")+sha256.Size*2 || !isLowerHex(coordinate.SessionID[len("session-"):]) || !validToken(coordinate.TrackID, MaxTrackIDBytes) {
		return fmt.Errorf("%w: invalid coordinate", ErrInvalidClaim)
	}
	if coordinate.Kind != ObjectMedia && coordinate.Kind != ObjectInit {
		return fmt.Errorf("%w: invalid coordinate object kind", ErrInvalidClaim)
	}
	return nil
}

func validSessionFingerprint(value string) bool {
	return strings.HasPrefix(value, "session-") && len(value) == len("session-")+sha256.Size*2 && isLowerHex(value[len("session-"):])
}

func validateClaim(claim Claim) error {
	if !validText(claim.ID, MaxClaimIDBytes, false) || !validClaimSource(claim.Source) || claim.AcquiredAt.IsZero() ||
		!validPayloadPath(claim.PayloadPath) || claim.Size <= 0 || claim.Size > MaxPayloadBytes || !validSHA256(claim.SHA256) ||
		!validVerification(claim.Verification) || !validDisposition(claim.Disposition) ||
		!validText(claim.Evidence, MaxEvidenceBytes, true) || !validText(claim.LegacySegmentID, MaxClaimIDBytes, true) {
		return fmt.Errorf("%w: invalid claim metadata", ErrInvalidClaim)
	}
	return nil
}

func validateCoverage(coverage Coverage) error {
	coverage.Kind = normalizeKind(coverage.Kind)
	if !strings.HasPrefix(coverage.SessionID, "session-") || len(coverage.SessionID) != len("session-")+sha256.Size*2 || !isLowerHex(coverage.SessionID[len("session-"):]) ||
		!validToken(coverage.TrackID, MaxTrackIDBytes) || coverage.FromSequence > coverage.ToSequence || !validCoverageState(coverage.State) ||
		coverage.State == CoverageUnknown || coverage.ObservedAt.IsZero() || !validText(coverage.Reason, MaxReasonBytes, true) ||
		(coverage.Kind != ObjectMedia && coverage.Kind != ObjectInit) {
		return fmt.Errorf("%w: invalid coverage interval", ErrInvalidInventory)
	}
	return nil
}

func coverageContains(coverage Coverage, coordinate Coordinate) bool {
	return coverage.SessionID == coordinate.SessionID && coverage.TrackID == coordinate.TrackID &&
		coverage.SourceEpoch == coordinate.SourceEpoch && coverage.DiscontinuitySequence == coordinate.DiscontinuitySequence &&
		coverage.Kind == coordinate.Kind && coordinate.Sequence >= coverage.FromSequence && coordinate.Sequence <= coverage.ToSequence
}

func coordinateLess(left, right Coordinate) bool {
	if left.SessionID != right.SessionID {
		return left.SessionID < right.SessionID
	}
	if left.TrackID != right.TrackID {
		return left.TrackID < right.TrackID
	}
	if left.SourceEpoch != right.SourceEpoch {
		return left.SourceEpoch < right.SourceEpoch
	}
	if left.DiscontinuitySequence != right.DiscontinuitySequence {
		return left.DiscontinuitySequence < right.DiscontinuitySequence
	}
	if left.Sequence != right.Sequence {
		return left.Sequence < right.Sequence
	}
	return left.Kind < right.Kind
}

func normalizeCoordinate(coordinate Coordinate) Coordinate {
	coordinate.Kind = normalizeKind(coordinate.Kind)
	return coordinate
}

func normalizeKind(kind ObjectKind) ObjectKind {
	if kind == "" {
		return ObjectMedia
	}
	return kind
}

func validDuration(duration float64, kind ObjectKind) bool {
	if kind == ObjectInit {
		return !math.IsNaN(duration) && !math.IsInf(duration, 0) && duration >= 0 && duration <= MaxSegmentDuration.Seconds()
	}
	return !math.IsNaN(duration) && !math.IsInf(duration, 0) && duration > 0 && duration <= MaxSegmentDuration.Seconds()
}

func validClaimSource(value ClaimSource) bool {
	switch value {
	case ClaimUnknown, ClaimLiveOrigin, ClaimHistorical, ClaimPeer, ClaimImport:
		return true
	default:
		return false
	}
}

func validVerification(value Verification) bool {
	switch value {
	case VerificationVerified, VerificationUnverified, VerificationFailed:
		return true
	default:
		return false
	}
}

func validDisposition(value ClaimDisposition) bool {
	switch value {
	case DispositionAccepted, DispositionDuplicate, DispositionConflict:
		return true
	default:
		return false
	}
}

func validCoverageState(value CoverageState) bool {
	switch value {
	case CoverageUnknown, CoverageKnownMissing, CoverageAcquisitionFailed, CoveragePresent, CoverageConflict:
		return true
	default:
		return false
	}
}

func incrementRevision(inventory *Inventory) error {
	if inventory.Revision == math.MaxUint64 {
		return ErrRevisionOverflow
	}
	inventory.Revision++
	return nil
}

func cloneInventory(source Inventory) Inventory {
	clone := source
	clone.Segments = make([]Segment, len(source.Segments))
	for i, segment := range source.Segments {
		clone.Segments[i] = segment
		clone.Segments[i].ProgramDateTime = cloneTime(segment.ProgramDateTime)
		clone.Segments[i].Claims = append([]Claim(nil), segment.Claims...)
	}
	clone.Coverage = append([]Coverage(nil), source.Coverage...)
	return clone
}

// inventorySizeUpperBound marshals only bounded individual records, never the
// complete inventory. The caller checks collection bounds first; this helper
// checks string lengths before any JSON allocation. Once this component sum
// fits, the final exact inventory encoding is safely capped at 16 MiB.
func inventorySizeUpperBound(inventory Inventory) (uint64, bool) {
	if !inventoryStringsWithinBounds(inventory) {
		return 0, false
	}
	size := uint64(2048) // Top-level keys, enum/scalar values, arrays, commas.
	addObject := func(value any) bool {
		encoded, err := json.Marshal(value)
		length := uint64(len(encoded))
		if err != nil || length >= MaxSerializedBytes || size > MaxSerializedBytes-(length+1) {
			return false
		}
		size += length + 1
		return true
	}
	if !addObject(inventory.Session) {
		return 0, false
	}
	for _, segment := range inventory.Segments {
		if !addObject(segment) {
			return 0, false
		}
	}
	for _, item := range inventory.Coverage {
		if !addObject(item) {
			return 0, false
		}
	}
	return size, true
}

func inventoryStringsWithinBounds(inventory Inventory) bool {
	if len(inventory.RecordingID) > MaxRecordingIDBytes || len(inventory.Session.ID) > 80 || len(inventory.Session.AdapterID) > MaxAdapterIDBytes || len(inventory.Session.ResourceHash) > 64 || len(inventory.Session.SessionHash) > 64 {
		return false
	}
	for _, segment := range inventory.Segments {
		if len(segment.ID) > 80 || len(segment.Coordinate.SessionID) > 80 || len(segment.Coordinate.TrackID) > MaxTrackIDBytes || len(segment.Coordinate.Kind) > 16 || len(segment.InitIdentity) > MaxInitIdentityBytes || len(segment.SelectedClaimID) > MaxClaimIDBytes {
			return false
		}
		for _, claim := range segment.Claims {
			if len(claim.ID) > MaxClaimIDBytes || len(claim.Source) > 32 || len(claim.PayloadPath) > MaxPayloadPathBytes || len(claim.SHA256) > 64 || len(claim.Verification) > 16 || len(claim.Disposition) > 16 || len(claim.Evidence) > MaxEvidenceBytes || len(claim.LegacySegmentID) > MaxClaimIDBytes {
				return false
			}
		}
	}
	for _, item := range inventory.Coverage {
		if len(item.SessionID) > 80 || len(item.TrackID) > MaxTrackIDBytes || len(item.State) > 32 || len(item.Reason) > MaxReasonBytes || len(item.Kind) > 16 {
			return false
		}
	}
	return true
}

func validPayloadPath(value string) bool {
	if !validText(value, MaxPayloadPathBytes, false) || strings.Contains(value, `\`) || strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func validToken(value string, max int) bool {
	if !validText(value, max, false) {
		return false
	}
	for _, r := range value {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune("._:-", r)) {
			return false
		}
	}
	return true
}

func validText(value string, max int, optional bool) bool {
	if optional && value == "" {
		return true
	}
	return value != "" && len(value) <= max && utf8.ValidString(value) && !hasControl(value)
}

func hasControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func digestString(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func validSHA256(value string) bool {
	return len(value) == sha256.Size*2 && isLowerHex(value)
}

func isLowerHex(value string) bool {
	if value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded)*2 == len(value)
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
