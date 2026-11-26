// Package archivesnapshot projects the existing archive index into immutable,
// content-addressed snapshots without changing local acquisition semantics.
package archivesnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
)

const (
	SchemaVersion    = 2
	maxSnapshotBytes = 16 << 20
	maxSafeInteger   = uint64(9007199254740991)
	contentDomainV1  = "IR/archive-snapshot/v1/content\x00"
	contentDomainV2  = "IR/archive-snapshot/v2/content\x00"
	provenanceDomain = "IR/archive-snapshot/v1/provenance\x00"
)

var (
	ErrInvalidSnapshot = errors.New("archive snapshot is invalid")
	ErrPayloadSource   = errors.New("canonical archive payload is unavailable")
	ErrPayloadMismatch = errors.New("canonical archive payload does not match its snapshot")
)

type Snapshot struct {
	SchemaVersion      int               `json:"schema_version"`
	Sequence           uint64            `json:"sequence"`
	PreviousSnapshotID string            `json:"previous_snapshot_id,omitempty"`
	SnapshotID         string            `json:"snapshot_id"`
	ContentHash        string            `json:"content_hash"`
	ProvenanceHash     string            `json:"provenance_hash"`
	Content            Content           `json:"content"`
	Provenance         []ClaimProvenance `json:"provenance"`
}

// Content contains only stable archive identity, timeline, canonical byte
// digests, and effective coverage. Mutable acquisition paths, timestamps, and
// claim IDs are deliberately kept out of the content hash.
type Content struct {
	ArchiveID string                       `json:"archive_id"`
	Session   archiveindex.SessionIdentity `json:"session"`
	Capture   archiveindex.CaptureState    `json:"capture_state"`
	Archive   archiveindex.ArchiveState    `json:"archive_state"`
	Objects   []Object                     `json:"objects"`
	Coverage  []Coverage                   `json:"coverage"`
}

type Object struct {
	Coordinate      archiveindex.Coordinate    `json:"coordinate"`
	TimelineOrdinal uint64                     `json:"timeline_ordinal"`
	Duration        float64                    `json:"duration"`
	ProgramDateTime string                     `json:"program_date_time,omitempty"`
	InitIdentity    string                     `json:"init_identity,omitempty"`
	Discontinuity   bool                       `json:"discontinuity,omitempty"`
	ByteRange       *ByteRange                 `json:"byte_range,omitempty"`
	State           archiveindex.CoverageState `json:"state"`
	PlaintextSize   int64                      `json:"plaintext_size"`
	PlaintextSHA256 string                     `json:"plaintext_sha256"`
}

// ByteRange captures the source playlist extent associated with a committed
// canonical object. The payload hash remains over the exact committed bytes.
type ByteRange struct {
	Length uint64 `json:"length"`
	Offset uint64 `json:"offset"`
}

// Coverage intentionally omits the observation clock so independently
// replicated copies do not acquire different content IDs from local time.
// ReasonHash binds local diagnostics without copying free-form text to peers.
type Coverage struct {
	SessionID             string                     `json:"session_id"`
	TrackID               string                     `json:"track_id"`
	SourceEpoch           uint64                     `json:"source_epoch"`
	DiscontinuitySequence uint64                     `json:"discontinuity_sequence"`
	FromSequence          uint64                     `json:"from_sequence"`
	ToSequence            uint64                     `json:"to_sequence"`
	State                 archiveindex.CoverageState `json:"state"`
	Kind                  archiveindex.ObjectKind    `json:"kind"`
	ReasonHash            string                     `json:"reason_hash,omitempty"`
}

// ClaimProvenance records how canonical bytes were observed without exposing
// local paths, raw source references, acquisition times, or arbitrary evidence.
// It has a separate hash so transport provenance can change without changing
// the content-addressed SnapshotID.
type ClaimProvenance struct {
	Coordinate      archiveindex.Coordinate       `json:"coordinate"`
	Source          archiveindex.ClaimSource      `json:"source"`
	Verification    archiveindex.Verification     `json:"verification"`
	Disposition     archiveindex.ClaimDisposition `json:"disposition"`
	PlaintextSize   int64                         `json:"plaintext_size"`
	PlaintextSHA256 string                        `json:"plaintext_sha256"`
}

// Build creates an immutable index projection. Sequence/predecessor form a
// local append-only snapshot chain; SnapshotID is content-derived and can be
// compared across nodes. New archives carry their immutable ArchiveID in the
// RC12-compatible recording root. Legacy archives without that field retain
// the migration-stable RecordingID mapping.
func Build(inventory archiveindex.Inventory, sequence uint64, previousSnapshotID string) (Snapshot, error) {
	if err := inventory.Validate(); err != nil || sequence == 0 || sequence > maxSafeInteger || (sequence == 1 && previousSnapshotID != "") || (sequence > 1 && !validSnapshotID(previousSnapshotID)) {
		return Snapshot{}, ErrInvalidSnapshot
	}
	archiveID := inventory.ArchiveID
	if archiveID == "" {
		var err error
		archiveID, err = ArchiveIDForRecording(inventory.RecordingID)
		if err != nil {
			return Snapshot{}, ErrInvalidSnapshot
		}
	}
	content := Content{
		ArchiveID: archiveID,
		Session: archiveindex.SessionIdentity{
			ID: inventory.Session.ID, AdapterID: inventory.Session.AdapterID,
		},
		Capture:  inventory.Capture,
		Archive:  inventory.Archive,
		Objects:  []Object{},
		Coverage: make([]Coverage, 0, len(inventory.Coverage)),
	}
	provenance := make([]ClaimProvenance, 0)
	for _, segment := range inventory.Segments {
		if segment.SelectedClaimID == "" {
			continue
		}
		selected := -1
		for i := range segment.Claims {
			if segment.Claims[i].ID == segment.SelectedClaimID {
				selected = i
				break
			}
		}
		if selected < 0 || segment.Claims[selected].Verification != archiveindex.VerificationVerified {
			return Snapshot{}, ErrInvalidSnapshot
		}
		claim := segment.Claims[selected]
		initIdentity := snapshotInitIdentity(segment.InitIdentity)
		object := Object{
			Coordinate: segment.Coordinate, TimelineOrdinal: segment.TimelineOrdinal,
			Duration: segment.Duration, InitIdentity: initIdentity,
			Discontinuity: segment.Discontinuity,
			State:         segment.State, PlaintextSize: claim.Size, PlaintextSHA256: "sha256:" + claim.SHA256,
		}
		if segment.ByteRange != nil {
			object.ByteRange = &ByteRange{Length: segment.ByteRange.Length, Offset: segment.ByteRange.Offset}
		}
		if segment.ProgramDateTime != nil {
			object.ProgramDateTime = segment.ProgramDateTime.UTC().Format(time.RFC3339Nano)
		}
		content.Objects = append(content.Objects, object)
		for _, candidate := range segment.Claims {
			provenance = append(provenance, ClaimProvenance{
				Coordinate: segment.Coordinate, Source: candidate.Source,
				Verification: candidate.Verification, Disposition: candidate.Disposition,
				PlaintextSize: candidate.Size, PlaintextSHA256: "sha256:" + candidate.SHA256,
			})
		}
	}
	sort.Slice(content.Objects, func(i, j int) bool { return objectLess(content.Objects[i], content.Objects[j]) })
	for _, item := range inventory.Coverage {
		coverage := Coverage{
			SessionID: item.SessionID, TrackID: item.TrackID,
			SourceEpoch: item.SourceEpoch, DiscontinuitySequence: item.DiscontinuitySequence,
			FromSequence: item.FromSequence, ToSequence: item.ToSequence,
			State: item.State, Kind: item.Kind,
		}
		if item.Reason != "" {
			coverage.ReasonHash = hashWithDomain("IR/archive-snapshot/v1/coverage-reason\x00", []byte(item.Reason))
		}
		content.Coverage = append(content.Coverage, coverage)
	}
	sort.Slice(content.Coverage, func(i, j int) bool { return coverageLess(content.Coverage[i], content.Coverage[j]) })
	sort.Slice(provenance, func(i, j int) bool { return provenanceLess(provenance[i], provenance[j]) })

	contentBytes, err := canonicalJSON(content)
	if err != nil || len(contentBytes) > maxSnapshotBytes {
		return Snapshot{}, ErrInvalidSnapshot
	}
	contentHash := hashWithDomain(contentDomainV2, contentBytes)
	provenanceBytes, err := canonicalJSON(provenance)
	if err != nil || len(provenanceBytes) > maxSnapshotBytes {
		return Snapshot{}, ErrInvalidSnapshot
	}
	snapshot := Snapshot{
		SchemaVersion: SchemaVersion, Sequence: sequence,
		PreviousSnapshotID: previousSnapshotID,
		SnapshotID:         snapshotID(contentHash), ContentHash: contentHash,
		ProvenanceHash: hashWithDomain(provenanceDomain, provenanceBytes),
		Content:        content, Provenance: provenance,
	}
	if sequence > 1 && snapshot.SnapshotID == previousSnapshotID {
		return Snapshot{}, ErrInvalidSnapshot
	}
	if err := validateSnapshotContent(snapshot.SchemaVersion, snapshot.Content, snapshot.Provenance); err != nil {
		return Snapshot{}, ErrInvalidSnapshot
	}
	return snapshot, nil
}

// snapshotInitIdentity removes the source-side opaque init ID from the public
// snapshot while retaining a deterministic edge from media objects to their
// init-map object. The same source identity hashes to the same value on every
// node; no source URI or local path is involved.
func snapshotInitIdentity(value string) string {
	if value == "" {
		return ""
	}
	return "initref_" + strings.TrimPrefix(hashWithDomain("IR/archive-snapshot/v2/init-identity\x00", []byte(value)), "sha256:")
}

// ArchiveIDForRecording is a migration-stable mapping for archives that
// predate the distributed Archive ID field. It does not expose the local
// recording ID and produces the same identifier when the archive is copied.
func ArchiveIDForRecording(recordingID string) (string, error) {
	if strings.TrimSpace(recordingID) == "" || len(recordingID) > archiveindex.MaxRecordingIDBytes {
		return "", ErrInvalidSnapshot
	}
	digest := sha256.Sum256(append([]byte("IR/archive-id/from-recording/v1\x00"), []byte(recordingID)...))
	return "arc_" + base64.RawURLEncoding.EncodeToString(digest[:16]), nil
}

func Verify(snapshot Snapshot) error {
	if (snapshot.SchemaVersion != 1 && snapshot.SchemaVersion != SchemaVersion) || snapshot.Sequence == 0 || snapshot.Sequence > maxSafeInteger ||
		(snapshot.Sequence == 1 && snapshot.PreviousSnapshotID != "") ||
		(snapshot.Sequence > 1 && !validSnapshotID(snapshot.PreviousSnapshotID)) ||
		!validArchiveID(snapshot.Content.ArchiveID) || snapshot.Content.Objects == nil || snapshot.Content.Coverage == nil || snapshot.Provenance == nil {
		return ErrInvalidSnapshot
	}
	contentBytes, err := canonicalJSON(snapshot.Content)
	if err != nil || len(contentBytes) > maxSnapshotBytes {
		return ErrInvalidSnapshot
	}
	contentDomain := contentDomainV1
	if snapshot.SchemaVersion == SchemaVersion {
		contentDomain = contentDomainV2
	}
	contentHash := hashWithDomain(contentDomain, contentBytes)
	if contentHash != snapshot.ContentHash || snapshotID(contentHash) != snapshot.SnapshotID {
		return ErrInvalidSnapshot
	}
	provenanceBytes, err := canonicalJSON(snapshot.Provenance)
	if err != nil || len(provenanceBytes) > maxSnapshotBytes || hashWithDomain(provenanceDomain, provenanceBytes) != snapshot.ProvenanceHash {
		return ErrInvalidSnapshot
	}
	return validateSnapshotContent(snapshot.SchemaVersion, snapshot.Content, snapshot.Provenance)
}

// VerifyChain checks a complete local snapshot sequence from its genesis. A
// truncated history must be verified against an independently pinned prior
// snapshot ID by the caller; this function never invents a missing parent.
func VerifyChain(snapshots []Snapshot) error {
	if len(snapshots) == 0 {
		return ErrInvalidSnapshot
	}
	for i, snapshot := range snapshots {
		if err := Verify(snapshot); err != nil || snapshot.Sequence != uint64(i+1) {
			return ErrInvalidSnapshot
		}
		if i == 0 {
			if snapshot.PreviousSnapshotID != "" {
				return ErrInvalidSnapshot
			}
		} else if snapshot.PreviousSnapshotID != snapshots[i-1].SnapshotID || snapshot.Content.ArchiveID != snapshots[i-1].Content.ArchiveID || snapshot.SnapshotID == snapshots[i-1].SnapshotID {
			return ErrInvalidSnapshot
		}
	}
	return nil
}

func Marshal(snapshot Snapshot) ([]byte, error) {
	if err := Verify(snapshot); err != nil {
		return nil, err
	}
	data, err := canonicalJSON(snapshot)
	if err != nil || len(data) > maxSnapshotBytes {
		return nil, ErrInvalidSnapshot
	}
	return data, nil
}

func Parse(data []byte) (Snapshot, error) {
	if len(data) == 0 || len(data) > maxSnapshotBytes {
		return Snapshot{}, ErrInvalidSnapshot
	}
	canonical, err := canonicalizeSnapshotJSON(data)
	if err != nil || !bytes.Equal(canonical, data) {
		return Snapshot{}, ErrInvalidSnapshot
	}
	var snapshot Snapshot
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&snapshot) != nil || decoder.Decode(&struct{}{}) != io.EOF || Verify(snapshot) != nil {
		return Snapshot{}, ErrInvalidSnapshot
	}
	return snapshot, nil
}

type PayloadSource interface {
	OpenCanonical(context.Context, archiveindex.Coordinate) (io.ReadCloser, error)
}

// VerifyPayloads streams each referenced object and checks exact size and
// plaintext SHA-256 without loading archive media into memory.
func VerifyPayloads(ctx context.Context, snapshot Snapshot, source PayloadSource) error {
	if ctx == nil || source == nil || Verify(snapshot) != nil {
		return ErrInvalidSnapshot
	}
	for _, object := range snapshot.Content.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := source.OpenCanonical(ctx, object.Coordinate)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrPayloadSource, object.Coordinate.TrackID)
		}
		hash := sha256.New()
		written, copyErr := io.Copy(hash, io.LimitReader(reader, object.PlaintextSize+1))
		closeErr := reader.Close()
		if copyErr != nil || closeErr != nil || written != object.PlaintextSize || hex.EncodeToString(hash.Sum(nil)) != strings.TrimPrefix(object.PlaintextSHA256, "sha256:") {
			return fmt.Errorf("%w: %s", ErrPayloadMismatch, object.Coordinate.TrackID)
		}
	}
	return nil
}

func validateSnapshotContent(schemaVersion int, content Content, provenance []ClaimProvenance) error {
	if content.ArchiveID == "" || content.Session.ID == "" || content.Session.AdapterID == "" ||
		(content.Capture != archiveindex.CaptureOpen && content.Capture != archiveindex.CaptureClosed) ||
		(content.Archive != archiveindex.ArchiveRepairable && content.Archive != archiveindex.ArchiveSealed) ||
		(content.Archive == archiveindex.ArchiveSealed && content.Capture != archiveindex.CaptureClosed) {
		return fmt.Errorf("%w: content header", ErrInvalidSnapshot)
	}
	validationInventory := archiveindex.Inventory{
		SchemaVersion: archiveindex.SchemaVersion, RecordingID: "snapshot-validation",
		Session: content.Session, Revision: 1, Capture: content.Capture,
		Archive: content.Archive, Segments: []archiveindex.Segment{}, Coverage: []archiveindex.Coverage{},
	}
	if err := validationInventory.Validate(); err != nil {
		return fmt.Errorf("%w: archive index projection: %v", ErrInvalidSnapshot, err)
	}
	coordinates := make(map[archiveindex.Coordinate]struct{}, len(content.Objects))
	for i, object := range content.Objects {
		_, coordinateErr := archiveindex.SegmentIdentity(object.Coordinate)
		if coordinateErr != nil || object.Coordinate.SessionID != content.Session.ID || object.PlaintextSize <= 0 || object.PlaintextSize > archiveindex.MaxPayloadBytes || !validDigest(object.PlaintextSHA256) ||
			object.Coordinate.SourceEpoch > maxSafeInteger || object.Coordinate.DiscontinuitySequence > maxSafeInteger || object.Coordinate.Sequence > maxSafeInteger || object.TimelineOrdinal > maxSafeInteger ||
			(object.Coordinate.Kind == archiveindex.ObjectMedia && (!finiteDuration(object.Duration) || object.Duration <= 0 || object.TimelineOrdinal == 0)) ||
			(object.Coordinate.Kind == archiveindex.ObjectInit && (!finiteDuration(object.Duration) || object.Duration < 0 || object.TimelineOrdinal != 0)) ||
			(object.State != archiveindex.CoveragePresent && object.State != archiveindex.CoverageConflict) || !validByteRange(object.ByteRange) ||
			(i > 0 && !objectLess(content.Objects[i-1], object)) {
			return fmt.Errorf("%w: object %d", ErrInvalidSnapshot, i)
		}
		if _, duplicate := coordinates[object.Coordinate]; duplicate {
			return fmt.Errorf("%w: duplicate object coordinate", ErrInvalidSnapshot)
		}
		coordinates[object.Coordinate] = struct{}{}
		if object.ProgramDateTime != "" {
			parsed, err := time.Parse(time.RFC3339Nano, object.ProgramDateTime)
			if err != nil || parsed.Location() != time.UTC || parsed.UTC().Format(time.RFC3339Nano) != object.ProgramDateTime {
				return fmt.Errorf("%w: object time %d", ErrInvalidSnapshot, i)
			}
		}
	}
	if schemaVersion >= 2 {
		// Playback restoration must be able to resolve every media object's
		// init-map reference without consulting a source URI or private local
		// recording metadata. The init object's opaque identity is therefore
		// content-addressed as part of snapshot v2.
		type initContent struct {
			size   int64
			digest string
		}
		initObjects := make(map[string]initContent)
		for _, object := range content.Objects {
			if object.Coordinate.Kind != archiveindex.ObjectInit || object.InitIdentity == "" {
				continue
			}
			key := object.Coordinate.TrackID + "\x00" + object.InitIdentity
			content := initContent{size: object.PlaintextSize, digest: object.PlaintextSHA256}
			if prior, duplicate := initObjects[key]; duplicate && prior != content {
				return fmt.Errorf("%w: ambiguous init identity has different content", ErrInvalidSnapshot)
			}
			// RC12 can retain the same HLS map at multiple source coordinates.
			// It is unambiguous when every occurrence binds identical bytes.
			initObjects[key] = content
		}
		for _, object := range content.Objects {
			if object.Coordinate.Kind != archiveindex.ObjectMedia || object.InitIdentity == "" {
				continue
			}
			if _, exists := initObjects[object.Coordinate.TrackID+"\x00"+object.InitIdentity]; !exists {
				return fmt.Errorf("%w: media init object is not in snapshot", ErrInvalidSnapshot)
			}
		}
	}
	for i, coverage := range content.Coverage {
		coverageCoordinate := archiveindex.Coordinate{
			SessionID: coverage.SessionID, TrackID: coverage.TrackID,
			SourceEpoch: coverage.SourceEpoch, DiscontinuitySequence: coverage.DiscontinuitySequence,
			Sequence: coverage.FromSequence, Kind: coverage.Kind,
		}
		_, coordinateErr := archiveindex.SegmentIdentity(coverageCoordinate)
		if coordinateErr != nil || coverage.SessionID != content.Session.ID || coverage.SourceEpoch > maxSafeInteger || coverage.DiscontinuitySequence > maxSafeInteger || coverage.FromSequence > maxSafeInteger || coverage.ToSequence > maxSafeInteger || coverage.FromSequence > coverage.ToSequence ||
			(coverage.Kind != archiveindex.ObjectMedia && coverage.Kind != archiveindex.ObjectInit) ||
			(coverage.State != archiveindex.CoverageKnownMissing && coverage.State != archiveindex.CoverageAcquisitionFailed && coverage.State != archiveindex.CoveragePresent && coverage.State != archiveindex.CoverageConflict) ||
			(coverage.ReasonHash != "" && !validDigest(coverage.ReasonHash)) ||
			(i > 0 && coverageLess(coverage, content.Coverage[i-1])) {
			return fmt.Errorf("%w: coverage %d", ErrInvalidSnapshot, i)
		}
	}
	for i, claim := range provenance {
		_, coordinateErr := archiveindex.SegmentIdentity(claim.Coordinate)
		if coordinateErr != nil || claim.Coordinate.SessionID != content.Session.ID || claim.PlaintextSize <= 0 || claim.PlaintextSize > archiveindex.MaxPayloadBytes || !validDigest(claim.PlaintextSHA256) ||
			claim.Coordinate.SourceEpoch > maxSafeInteger || claim.Coordinate.DiscontinuitySequence > maxSafeInteger || claim.Coordinate.Sequence > maxSafeInteger ||
			!validClaimSource(claim.Source) || !validVerification(claim.Verification) || !validDisposition(claim.Disposition) ||
			(i > 0 && provenanceLess(claim, provenance[i-1])) {
			return fmt.Errorf("%w: provenance %d", ErrInvalidSnapshot, i)
		}
	}
	return nil
}

func validByteRange(value *ByteRange) bool {
	return value == nil || value.Length > 0 && value.Offset <= ^uint64(0)-(value.Length-1)
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonicalizeSnapshotJSON(data)
}

func objectLess(a, b Object) bool {
	if a.Coordinate.TrackID != b.Coordinate.TrackID {
		return a.Coordinate.TrackID < b.Coordinate.TrackID
	}
	if a.Coordinate.Kind != b.Coordinate.Kind {
		return a.Coordinate.Kind < b.Coordinate.Kind
	}
	if a.Coordinate.Kind == archiveindex.ObjectMedia && a.TimelineOrdinal != b.TimelineOrdinal {
		return a.TimelineOrdinal < b.TimelineOrdinal
	}
	if a.Coordinate.SourceEpoch != b.Coordinate.SourceEpoch {
		return a.Coordinate.SourceEpoch < b.Coordinate.SourceEpoch
	}
	if a.Coordinate.DiscontinuitySequence != b.Coordinate.DiscontinuitySequence {
		return a.Coordinate.DiscontinuitySequence < b.Coordinate.DiscontinuitySequence
	}
	return a.Coordinate.Sequence < b.Coordinate.Sequence
}

func coverageLess(a, b Coverage) bool {
	if a.TrackID != b.TrackID {
		return a.TrackID < b.TrackID
	}
	if a.SourceEpoch != b.SourceEpoch {
		return a.SourceEpoch < b.SourceEpoch
	}
	if a.DiscontinuitySequence != b.DiscontinuitySequence {
		return a.DiscontinuitySequence < b.DiscontinuitySequence
	}
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.FromSequence != b.FromSequence {
		return a.FromSequence < b.FromSequence
	}
	if a.ToSequence != b.ToSequence {
		return a.ToSequence < b.ToSequence
	}
	if a.State != b.State {
		return a.State < b.State
	}
	return a.ReasonHash < b.ReasonHash
}

func provenanceLess(a, b ClaimProvenance) bool {
	if objectCoordinateLess(a.Coordinate, b.Coordinate) {
		return true
	}
	if objectCoordinateLess(b.Coordinate, a.Coordinate) {
		return false
	}
	if a.Source != b.Source {
		return a.Source < b.Source
	}
	if a.Disposition != b.Disposition {
		return a.Disposition < b.Disposition
	}
	if a.Verification != b.Verification {
		return a.Verification < b.Verification
	}
	if a.PlaintextSHA256 != b.PlaintextSHA256 {
		return a.PlaintextSHA256 < b.PlaintextSHA256
	}
	return a.PlaintextSize < b.PlaintextSize
}

func finiteDuration(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value <= archiveindex.MaxSegmentDuration.Seconds()
}

func validClaimSource(value archiveindex.ClaimSource) bool {
	switch value {
	case archiveindex.ClaimUnknown, archiveindex.ClaimLiveOrigin, archiveindex.ClaimHistorical, archiveindex.ClaimPeer, archiveindex.ClaimImport:
		return true
	default:
		return false
	}
}

func validVerification(value archiveindex.Verification) bool {
	switch value {
	case archiveindex.VerificationVerified, archiveindex.VerificationUnverified, archiveindex.VerificationFailed:
		return true
	default:
		return false
	}
}

func validDisposition(value archiveindex.ClaimDisposition) bool {
	switch value {
	case archiveindex.DispositionAccepted, archiveindex.DispositionDuplicate, archiveindex.DispositionConflict:
		return true
	default:
		return false
	}
}

func objectCoordinateLess(a, b archiveindex.Coordinate) bool {
	if a.TrackID != b.TrackID {
		return a.TrackID < b.TrackID
	}
	if a.SourceEpoch != b.SourceEpoch {
		return a.SourceEpoch < b.SourceEpoch
	}
	if a.DiscontinuitySequence != b.DiscontinuitySequence {
		return a.DiscontinuitySequence < b.DiscontinuitySequence
	}
	if a.Sequence != b.Sequence {
		return a.Sequence < b.Sequence
	}
	return a.Kind < b.Kind
}

func hashWithDomain(domain string, data []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write(data)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func snapshotID(contentHash string) string {
	return "snap_" + strings.TrimPrefix(contentHash, "sha256:")
}

func validSnapshotID(value string) bool {
	return len(value) == len("snap_")+64 && strings.HasPrefix(value, "snap_") && isLowerHex(value[len("snap_"):])
}
func validArchiveID(value string) bool {
	if len(value) != len("arc_")+22 || !strings.HasPrefix(value, "arc_") {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "arc_"))
	return err == nil && len(decoded) == 16 && base64.RawURLEncoding.EncodeToString(decoded) == strings.TrimPrefix(value, "arc_")
}
func validDigest(value string) bool {
	return len(value) == len("sha256:")+64 && strings.HasPrefix(value, "sha256:") && isLowerHex(value[len("sha256:"):])
}
func isLowerHex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded)*2 == len(value) && hex.EncodeToString(decoded) == value
}
