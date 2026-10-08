package acquire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	archiveIndexManifestPath = "archive-index"
	archiveIndexCoveragePath = "archive-index/coverage"
	acquisitionContextPath   = "archive-index/acquisition-context"
	archiveIndexMaxBytes     = archiveindex.MaxSerializedBytes
	maxAcquisitionContext    = 64 << 10
)

var (
	ErrArchiveIndexUnavailable = errors.New("archive index is unavailable")
	ErrArchiveIndexLimit       = errors.New("archive index limit exceeded")
	ErrArchiveSealed           = errors.New("archive is sealed")
	ErrHistoricalUnavailable   = errors.New("declared historical media is unavailable")
)

// archiveIndexManifest is deliberately small. The canonical root remains the
// source for selected media; this document only announces the supplementary
// claim/coverage format. Claim shards are derived by logical coordinate and
// coverage is kept in one bounded, non-canonical sidecar.
type archiveIndexManifest struct {
	SchemaVersion int                          `json:"schema_version"`
	RecordingID   string                       `json:"recording_id"`
	Session       archiveindex.SessionIdentity `json:"session"`
	ClaimShards   []string                     `json:"claim_shards,omitempty"`
}

type archiveClaimShard struct {
	Coordinate archiveindex.Coordinate `json:"coordinate"`
	Claims     []archiveindex.Claim    `json:"claims"`
}

type archiveCoverageDocument struct {
	SchemaVersion int                     `json:"schema_version"`
	RecordingID   string                  `json:"recording_id"`
	Coverage      []archiveindex.Coverage `json:"coverage"`
}

// acquisitionContextSidecar contains private Core request inputs. It is not
// part of recording.json or any public recording projection.
type acquisitionContextSidecar struct {
	SchemaVersion int                      `json:"schema_version"`
	Media         adapterproto.MediaSource `json:"media"`
}

func mediaContext(media adapterproto.MediaSource) (json.RawMessage, error) {
	// Persist only fields Core needs to continue requests after a restart.
	// Adapter metadata and opaque session refs are not acquisition inputs.
	copy := adapterproto.MediaSource{
		Type: media.Type, ManifestURL: media.ManifestURL,
		Headers: media.Headers, RequestPolicy: media.RequestPolicy,
		Refresh: media.Refresh, ArchivePolicy: media.ArchivePolicy,
		RefreshPolicy: media.RefreshPolicy, HistoricalAvailability: media.HistoricalAvailability,
	}
	copy = cloneMediaSource(copy)
	// The opaque session reference is already represented by SourceSessionID;
	// keeping it here would unnecessarily retain adapter-private identity data.
	copy.SessionRef = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return nil, errors.New("media acquisition context could not be encoded")
	}
	if len(encoded) == 0 || len(encoded) > maxAcquisitionContext {
		return nil, errors.New("media acquisition context exceeds limit")
	}
	return json.RawMessage(encoded), nil
}

func newAcquisitionContextSidecar(raw json.RawMessage) (acquisitionContextSidecar, error) {
	var media adapterproto.MediaSource
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if len(raw) == 0 || len(raw) > maxAcquisitionContext || decoder.Decode(&media) != nil {
		return acquisitionContextSidecar{}, ErrHistoricalUnavailable
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return acquisitionContextSidecar{}, ErrHistoricalUnavailable
	}
	if media.SessionRef != "" || adapterproto.ValidateMediaSource(media, []string{"hls"}) != nil {
		return acquisitionContextSidecar{}, ErrHistoricalUnavailable
	}
	media.SessionRef = ""
	doc := acquisitionContextSidecar{SchemaVersion: 1, Media: media}
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil || len(encoded) == 0 || len(encoded) > maxAcquisitionContext {
		return acquisitionContextSidecar{}, ErrHistoricalUnavailable
	}
	return doc, nil
}

func acquisitionContextDocument(raw json.RawMessage) (acquisitionContextSidecar, error) {
	return newAcquisitionContextSidecar(raw)
}

func (m *Manager) persistAcquisitionContext(recordingID string, raw json.RawMessage) error {
	doc, err := newAcquisitionContextSidecar(raw)
	if err != nil {
		return err
	}
	return m.store.SaveSidecar(recordingID, acquisitionContextPath, doc)
}

func (m *Manager) loadAcquisitionContext(recordingID string) (adapterproto.MediaSource, error) {
	var doc acquisitionContextSidecar
	if err := m.store.LoadSidecar(recordingID, acquisitionContextPath, maxAcquisitionContext, &doc); err != nil {
		return adapterproto.MediaSource{}, ErrHistoricalUnavailable
	}
	if doc.SchemaVersion != 1 || doc.Media.SessionRef != "" {
		return adapterproto.MediaSource{}, ErrHistoricalUnavailable
	}
	if _, err := mediaContext(doc.Media); err != nil || adapterproto.ValidateMediaSource(doc.Media, []string{"hls"}) != nil {
		return adapterproto.MediaSource{}, ErrHistoricalUnavailable
	}
	return cloneMediaSource(doc.Media), nil
}

func archiveSessionIdentity(recording *domain.Recording, adapterID string, sessionRef string) (archiveindex.SessionIdentity, error) {
	if recording == nil {
		return archiveindex.SessionIdentity{}, archiveindex.ErrInvalidInventory
	}
	if adapterID == "" {
		adapterID = recording.AdapterID
	}
	if adapterID == "" && recording.Adapter != nil {
		adapterID = recording.Adapter.ID
	}
	if adapterID == "" {
		adapterID = "legacy"
	}
	if recording.SourceSessionID == "" {
		legacy, err := archiveindex.FromRecording(recording, true)
		if err != nil {
			return archiveindex.SessionIdentity{}, err
		}
		return legacy.Session, nil
	}
	persisted, err := archiveindex.FromRecording(recording, false)
	if err != nil {
		return archiveindex.SessionIdentity{}, err
	}
	return persisted.Session, nil
}

func adapterResource(resource *domain.ResourceReference) *adapterproto.ResourceRef {
	if resource == nil {
		return nil
	}
	return &adapterproto.ResourceRef{Type: resource.Type, ID: resource.ID, Parent: adapterResource(resource.Parent)}
}

func (m *Manager) transformedRequestURL(ctx context.Context, media adapterproto.MediaSource, baseURL, targetURL string, scope adapterproto.ResourceRequestScope) (string, error) {
	var policy *adapterproto.URLTransformPolicy
	if media.RequestPolicy != nil {
		policy = media.RequestPolicy.URLTransform
	}
	transformed, err := adapterproto.ApplyURLTransform(baseURL, targetURL, scope, policy)
	if err != nil {
		return "", errors.New("media request URL is invalid")
	}
	if err := m.validate(ctx, transformed); err != nil {
		return "", errors.New("media request URL is invalid")
	}
	return transformed, nil
}

func (m *Manager) fetchManifestForMedia(ctx context.Context, recordingID string, media adapterproto.MediaSource, baseURL, targetURL string, scope adapterproto.ResourceRequestScope) (*storage.IngestPayload, string, error) {
	requestURL, err := m.transformedRequestURL(ctx, media, baseURL, targetURL, scope)
	if err != nil {
		return nil, "", err
	}
	payload, err := fetchManifestBuffered(ctx, m.client, m.ingest, recordingID, requestURL, media.Headers, media.ManifestURL, media.RequestPolicy)
	return payload, requestURL, err
}

func coordinateForSegment(sessionID string, segment domain.Segment) archiveindex.Coordinate {
	kind := archiveindex.ObjectMedia
	if segment.IsInit {
		kind = archiveindex.ObjectInit
	}
	return archiveindex.Coordinate{
		SessionID: sessionID, TrackID: segment.TrackID,
		SourceEpoch: segment.SourceEpoch, DiscontinuitySequence: segment.DiscontinuitySequence,
		Sequence: segment.Sequence, Kind: kind,
	}
}

func claimShardPath(segmentID string) string {
	return "archive-index/claims/" + segmentID
}

func isMissingArchiveSidecar(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, storage.ErrObjectNotFound)
}

func isArchiveIndexCapacityError(err error) bool {
	return errors.Is(err, archiveindex.ErrInvalidInventory) || errors.Is(err, archiveindex.ErrInvalidClaim) || errors.Is(err, archiveindex.ErrRevisionOverflow)
}

func (m *Manager) inventoryForRecording(recording *domain.Recording, allowRootOnly bool) (archiveindex.Inventory, error) {
	return m.loadArchiveInventory(recording, allowRootOnly, nil, true, true)
}

// ArchiveInventory returns a bounded projection of the canonical root and its
// durable claim/coverage supplements. It never grants mutation authority.
func (m *Manager) ArchiveInventory(id string) (archiveindex.Inventory, error) {
	if !validRecordingID(id) {
		return archiveindex.Inventory{}, storage.ErrNotFound
	}
	e, ok := m.entry(id)
	if !ok {
		recording, err := m.store.LoadRecordingReadOnly(id)
		if err != nil {
			return archiveindex.Inventory{}, err
		}
		return m.inventoryForRecording(recording, false)
	}
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	recording := m.recordingSnapshotForArchive(e)
	if recording == nil {
		return archiveindex.Inventory{}, storage.ErrNotFound
	}
	return m.inventoryForRecording(recording, false)
}

// inventoryForCoordinate keeps the live hot path bounded to the one claim
// shard that can affect the coordinate being committed. Root-derived selected
// claims and coverage remain complete without scanning old supplementary
// shards on every segment.
func (m *Manager) inventoryForCoordinate(recording *domain.Recording, coordinate archiveindex.Coordinate) (archiveindex.Inventory, error) {
	return m.loadArchiveInventory(recording, true, &coordinate, false, false)
}

func (m *Manager) loadArchiveInventory(recording *domain.Recording, allowRootOnly bool, only *archiveindex.Coordinate, includeCoverage, discoverOrphans bool) (archiveindex.Inventory, error) {
	if recording != nil {
		count, coverage := 0, len(recording.Gaps)
		for _, track := range recording.Tracks {
			if track == nil {
				return archiveindex.Inventory{}, fmt.Errorf("%w: nil track", archiveindex.ErrInvalidInventory)
			}
			count += len(track.Segments) + len(track.InitSegments)
		}
		if count > archiveindex.MaxSegments || coverage > archiveindex.MaxCoverageIntervals {
			return archiveindex.Inventory{}, ErrArchiveIndexLimit
		}
	}
	legacy := recording == nil || recording.SourceSessionID == ""
	inventory, err := archiveindex.FromRecording(recording, legacy)
	if err != nil {
		return archiveindex.Inventory{}, err
	}
	manifest := archiveIndexManifest{}
	manifestErr := m.store.LoadSidecar(recording.ID, archiveIndexManifestPath, archiveIndexMaxBytes, &manifest)
	if manifestErr == nil {
		if manifest.SchemaVersion != archiveindex.SchemaVersion || manifest.RecordingID != recording.ID || manifest.Session.ID != inventory.Session.ID {
			return archiveindex.Inventory{}, fmt.Errorf("%w: archive index identity mismatch", ErrArchiveIndexUnavailable)
		}
		if len(manifest.ClaimShards) > archiveindex.MaxSegments {
			return archiveindex.Inventory{}, ErrArchiveIndexLimit
		}
		for i, id := range manifest.ClaimShards {
			if len(id) != len("segment-")+sha256.Size*2 || !strings.HasPrefix(id, "segment-") || i > 0 && manifest.ClaimShards[i-1] >= id {
				return archiveindex.Inventory{}, fmt.Errorf("%w: archive index shard list is invalid", ErrArchiveIndexUnavailable)
			}
		}
	} else if !isMissingArchiveSidecar(manifestErr) && !errors.Is(manifestErr, storage.ErrSidecarReadUnsupported) {
		return archiveindex.Inventory{}, fmt.Errorf("%w: malformed archive index manifest", ErrArchiveIndexUnavailable)
	} else if errors.Is(manifestErr, storage.ErrSidecarReadUnsupported) && !allowRootOnly {
		return archiveindex.Inventory{}, ErrArchiveIndexUnavailable
	}

	if !errors.Is(manifestErr, storage.ErrSidecarReadUnsupported) {
		known := make(map[string]bool, len(manifest.ClaimShards))
		for _, id := range manifest.ClaimShards {
			known[id] = true
		}
		for i := range inventory.Segments {
			segment := &inventory.Segments[i]
			if only != nil && segment.Coordinate != *only {
				continue
			}
			if only == nil && !discoverOrphans && !known[segment.ID] {
				continue
			}
			shard := archiveClaimShard{}
			loadErr := m.store.LoadSidecar(recording.ID, claimShardPath(segment.ID), archiveIndexMaxBytes, &shard)
			if isMissingArchiveSidecar(loadErr) {
				if known[segment.ID] {
					return archiveindex.Inventory{}, fmt.Errorf("%w: claim shard is missing", ErrArchiveIndexUnavailable)
				}
				continue
			}
			if loadErr != nil {
				return archiveindex.Inventory{}, fmt.Errorf("%w: claim shard is unreadable", ErrArchiveIndexUnavailable)
			}
			if shard.Coordinate != segment.Coordinate || len(shard.Claims) == 0 || len(shard.Claims) >= archiveindex.MaxClaimsPerSegment {
				return archiveindex.Inventory{}, fmt.Errorf("%w: claim shard is invalid", ErrArchiveIndexUnavailable)
			}
			for _, claim := range shard.Claims {
				_, applyErr := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
					Coordinate: shard.Coordinate, ClaimID: claim.ID, Source: claim.Source,
					AcquiredAt: claim.AcquiredAt, PayloadPath: claim.PayloadPath,
					Size: claim.Size, SHA256: claim.SHA256, Verification: claim.Verification,
					Evidence: claim.Evidence, LegacySegmentID: claim.LegacySegmentID,
					Duration: segment.Duration, ProgramDateTime: segment.ProgramDateTime,
					InitIdentity: segment.InitIdentity,
				})
				if applyErr != nil {
					return archiveindex.Inventory{}, fmt.Errorf("%w: claim shard does not reconcile", ErrArchiveIndexUnavailable)
				}
				// recording.json proves which bytes are selected, but cannot encode
				// how they were acquired. When a durable claim matches those exact
				// selected bytes, replace the synthetic legacy-adoption claim in
				// this read projection so cold inventory retains real provenance.
				preferPersistedSelectedClaim(&inventory, shard.Coordinate, claim)
			}
		}
		if includeCoverage {
			coverage := archiveCoverageDocument{}
			coverageErr := m.store.LoadSidecar(recording.ID, archiveIndexCoveragePath, archiveIndexMaxBytes, &coverage)
			if coverageErr == nil {
				if coverage.SchemaVersion != archiveindex.SchemaVersion || coverage.RecordingID != recording.ID {
					return archiveindex.Inventory{}, fmt.Errorf("%w: coverage document identity mismatch", ErrArchiveIndexUnavailable)
				}
				if len(coverage.Coverage) > archiveindex.MaxCoverageIntervals {
					return archiveindex.Inventory{}, ErrArchiveIndexLimit
				}
				for _, item := range coverage.Coverage {
					if err := archiveindex.ApplyCoverage(&inventory, item); err != nil {
						return archiveindex.Inventory{}, fmt.Errorf("%w: coverage document does not reconcile", ErrArchiveIndexUnavailable)
					}
				}
			} else if !isMissingArchiveSidecar(coverageErr) && !errors.Is(coverageErr, storage.ErrSidecarReadUnsupported) {
				return archiveindex.Inventory{}, fmt.Errorf("%w: coverage document is unreadable", ErrArchiveIndexUnavailable)
			} else if errors.Is(coverageErr, storage.ErrSidecarReadUnsupported) && !allowRootOnly {
				return archiveindex.Inventory{}, ErrArchiveIndexUnavailable
			}
		}
	}
	if err := inventory.Validate(); err != nil {
		return archiveindex.Inventory{}, fmt.Errorf("%w: inventory exceeds supported bounds", ErrArchiveIndexLimit)
	}
	return inventory, nil
}

func (m *Manager) persistCoverage(recordingID string, inventory archiveindex.Inventory) error {
	if len(inventory.Coverage) > archiveindex.MaxCoverageIntervals {
		return ErrArchiveIndexLimit
	}
	doc := archiveCoverageDocument{SchemaVersion: archiveindex.SchemaVersion, RecordingID: recordingID, Coverage: append([]archiveindex.Coverage(nil), inventory.Coverage...)}
	sort.Slice(doc.Coverage, func(i, j int) bool {
		a, b := doc.Coverage[i], doc.Coverage[j]
		if a.SessionID != b.SessionID {
			return a.SessionID < b.SessionID
		}
		if a.TrackID != b.TrackID {
			return a.TrackID < b.TrackID
		}
		if a.SourceEpoch != b.SourceEpoch {
			return a.SourceEpoch < b.SourceEpoch
		}
		if a.DiscontinuitySequence != b.DiscontinuitySequence {
			return a.DiscontinuitySequence < b.DiscontinuitySequence
		}
		if a.FromSequence != b.FromSequence {
			return a.FromSequence < b.FromSequence
		}
		if a.ToSequence != b.ToSequence {
			return a.ToSequence < b.ToSequence
		}
		return a.ObservedAt.Before(b.ObservedAt)
	})
	encoded, err := json.Marshal(doc)
	if err != nil || len(encoded) > archiveIndexMaxBytes {
		return ErrArchiveIndexLimit
	}
	return m.store.SaveSidecar(recordingID, archiveIndexCoveragePath, doc)
}

func (m *Manager) persistClaimShard(recordingID string, inventory archiveindex.Inventory, segmentID string, coordinate archiveindex.Coordinate, rootSegmentID string) error {
	shard, encoded, err := archiveClaimShardForInventory(inventory, segmentID, coordinate, rootSegmentID)
	if err != nil {
		return newStorageStageError("build archive claim shard", err)
	}
	if len(encoded) == 0 {
		return nil
	}
	if err := m.store.SaveSidecar(recordingID, claimShardPath(segmentID), shard); err != nil {
		return newStorageStageError("persist archive claim shard", err)
	}
	if err := m.ensureArchiveIndexManifest(recordingID, inventory); err != nil {
		return newStorageStageError("persist archive index manifest", err)
	}
	// The shard is addressed by the stable coordinate ID and discovered by
	// walking canonical root segments. Do not append it to a growing manifest
	// on every live segment: that would rewrite O(n) metadata per commit.
	return nil
}

func (m *Manager) ensureArchiveIndexManifest(recordingID string, inventory archiveindex.Inventory) error {
	var existing archiveIndexManifest
	err := m.store.LoadSidecar(recordingID, archiveIndexManifestPath, archiveIndexMaxBytes, &existing)
	if err == nil {
		if existing.SchemaVersion != archiveindex.SchemaVersion || existing.RecordingID != recordingID || existing.Session.ID != inventory.Session.ID || len(existing.ClaimShards) > archiveindex.MaxSegments {
			return newStorageStageError("validate archive index manifest", ErrArchiveIndexUnavailable)
		}
		return nil
	}
	if errors.Is(err, storage.ErrSidecarReadUnsupported) {
		return newStorageStageError("read archive index manifest", ErrArchiveIndexUnavailable)
	}
	if !isMissingArchiveSidecar(err) {
		return newStorageStageError("read archive index manifest", ErrArchiveIndexUnavailable)
	}
	manifest := archiveIndexManifest{SchemaVersion: archiveindex.SchemaVersion, RecordingID: recordingID, Session: inventory.Session}
	if err := m.store.SaveSidecar(recordingID, archiveIndexManifestPath, manifest); err != nil {
		return newStorageStageError("write archive index manifest", err)
	}
	return nil
}

func archiveClaimShardForInventory(inventory archiveindex.Inventory, segmentID string, coordinate archiveindex.Coordinate, rootSegmentID string) (archiveClaimShard, []byte, error) {
	var claims []archiveindex.Claim
	for _, segment := range inventory.Segments {
		if segment.ID != segmentID {
			continue
		}
		for _, claim := range segment.Claims {
			if claim.ID == "legacy-media:"+rootSegmentID || claim.ID == "legacy-init:"+rootSegmentID {
				continue
			}
			// The root-selected claim is reconstructed from recording.json. Store
			// the source attestation as a shard too. On reload the exact matching
			// root-adoption claim is replaced by this richer evidence.
			if claim.PayloadPath == "" {
				return archiveClaimShard{}, nil, ErrArchiveIndexUnavailable
			}
			claims = append(claims, claim)
		}
	}
	if len(claims) == 0 {
		return archiveClaimShard{}, nil, nil
	}
	shard := archiveClaimShard{Coordinate: coordinate, Claims: claims}
	encoded, err := json.MarshalIndent(shard, "", "  ")
	encoded = append(encoded, '\n')
	if err != nil || len(encoded) > archiveIndexMaxBytes {
		return archiveClaimShard{}, nil, ErrArchiveIndexLimit
	}
	return shard, encoded, nil
}

func preferPersistedSelectedClaim(inventory *archiveindex.Inventory, coordinate archiveindex.Coordinate, persisted archiveindex.Claim) {
	if inventory == nil || persisted.PayloadPath == "" || persisted.Verification != archiveindex.VerificationVerified {
		return
	}
	for i := range inventory.Segments {
		segment := &inventory.Segments[i]
		if segment.Coordinate != coordinate || segment.SelectedClaimID == persisted.ID {
			continue
		}
		selectedIndex := -1
		for claimIndex := range segment.Claims {
			selected := segment.Claims[claimIndex]
			if selected.ID != segment.SelectedClaimID {
				continue
			}
			selectedIndex = claimIndex
			if selected.LegacySegmentID == "" || selected.PayloadPath != persisted.PayloadPath ||
				selected.Size != persisted.Size || selected.SHA256 != persisted.SHA256 {
				return
			}
			break
		}
		if selectedIndex < 0 {
			return
		}
		segment.Claims = append(segment.Claims[:selectedIndex], segment.Claims[selectedIndex+1:]...)
		segment.SelectedClaimID = persisted.ID
		segment.State = archiveindex.CoveragePresent
		for claimIndex := range segment.Claims {
			if segment.Claims[claimIndex].ID == persisted.ID {
				segment.Claims[claimIndex].Disposition = archiveindex.DispositionAccepted
				break
			}
		}
		return
	}
}

func (m *Manager) withCanonicalMutationOwner(e *entry, owner *OwnershipToken, allowTerminalAdoption bool, commit func() error) error {
	if e == nil {
		return ErrInvalidOwnershipToken
	}
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	if owner == nil {
		return m.withCanonicalCommit(e, commit)
	}
	if !validOwnershipToken(*owner) || owner.RecordingID != recordingID(e) {
		return ErrInvalidOwnershipToken
	}
	return m.withOwnershipCommit(owner, func() error {
		e.mu.Lock()
		if e.deleted || e.recording == nil {
			e.mu.Unlock()
			return storage.ErrNotFound
		}
		if e.recording.State == domain.StateRecording {
			if !sameOwner(e.ownership, *owner) {
				e.mu.Unlock()
				return recordingowner.ErrStaleOwner
			}
		} else {
			if !allowTerminalAdoption || e.cancel != nil || !channelClosed(e.done) {
				e.mu.Unlock()
				return errors.New("terminal archive is not quiescent")
			}
			if e.ownership != nil && *e.ownership != *owner && owner.Epoch <= e.ownership.Epoch {
				e.mu.Unlock()
				return ErrInvalidOwnershipToken
			}
			copy := *owner
			e.ownership = &copy
		}
		e.mu.Unlock()
		return commit()
	})
}

func channelClosed(channel <-chan struct{}) bool {
	if channel == nil {
		return false
	}
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func (m *Manager) commitArchiveSegment(e *entry, owner *OwnershipToken, segment domain.Segment, source archiveindex.ClaimSource, data []byte) (storage.PayloadResult, *epochMarker, error) {
	return m.commitArchiveSegmentOwned(e, owner, segment, source, data, false)
}

func (m *Manager) commitArchiveSegmentOwned(e *entry, owner *OwnershipToken, segment domain.Segment, source archiveindex.ClaimSource, data []byte, allowTerminalAdoption bool) (storage.PayloadResult, *epochMarker, error) {
	var result storage.PayloadResult
	var plannedMarker *epochMarker
	if len(data) == 0 || int64(len(data)) > m.ingest.Options().MaxPayloadBytes {
		return result, nil, storage.ErrIngestTooLarge
	}
	actualHash := sha256Hex(data)
	if segment.PayloadSize != 0 && segment.PayloadSize != int64(len(data)) || segment.SHA256 != "" && segment.SHA256 != actualHash {
		return result, nil, errors.New("ingested media identity changed before commit")
	}
	result = storage.PayloadResult{Size: int64(len(data)), SHA256: actualHash}

	commitErr := m.withCanonicalMutationOwner(e, owner, allowTerminalAdoption, func() error {
		rootSnapshot := m.recordingSnapshotForArchive(e)
		if rootSnapshot == nil {
			return errors.New("recording state is unavailable")
		}
		e.mu.Lock()
		adapterID, sessionRef := e.adapterID, e.media.SessionRef
		e.mu.Unlock()
		identity, err := archiveSessionIdentity(rootSnapshot, adapterID, sessionRef)
		if err != nil {
			return err
		}
		rootSnapshot.SourceSessionID = identity.ID
		segment.TrackID = "main"
		segment.PayloadSize, segment.SHA256 = result.Size, result.SHA256
		segmentID, err := archiveindex.SegmentIdentity(coordinateForSegment(identity.ID, segment))
		if err != nil {
			return err
		}
		coordinate := coordinateForSegment(identity.ID, segment)
		inventory, inventoryErr := m.inventoryForCoordinate(rootSnapshot, coordinate)
		capacityExceeded := inventoryErr != nil && errors.Is(inventoryErr, ErrArchiveIndexLimit)
		if inventoryErr != nil && !capacityExceeded {
			return newStorageStageError("load archive inventory", inventoryErr)
		}

		existing, exists := findRootSegment(rootSnapshot, coordinate)
		if exists && existing.SHA256 == result.SHA256 && existing.PayloadSize == result.Size {
			// Repeated identical live bytes are already represented by the root;
			// do not replace the immutable object. Supplementary provenance is
			// recorded when the bounded claim store is available.
			segment.StoragePath = existing.StoragePath
		} else {
			segment.StoragePath = immutableObjectPath(segmentID, result.SHA256, segment.SourceURI)
		}

		input := archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: claimIdentity(source, segmentID, result.SHA256),
			Source: source, AcquiredAt: time.Now().UTC(), PayloadPath: segment.StoragePath,
			Size: result.Size, SHA256: result.SHA256, Verification: archiveindex.VerificationVerified,
			Duration: segment.Duration, ProgramDateTime: segment.ProgramDateTime, InitIdentity: segment.InitSegmentID,
		}
		if !capacityExceeded {
			if current, ok := findInventoryClaim(inventory, input.ClaimID); ok {
				input.AcquiredAt = current.AcquiredAt
				input.PayloadPath = current.PayloadPath
				input.Evidence = current.Evidence
				input.LegacySegmentID = current.LegacySegmentID
			}
		}
		var disposition archiveindex.ClaimDisposition
		if !capacityExceeded {
			disposition, err = archiveindex.ApplyClaim(&inventory, input)
			if err != nil {
				if isArchiveIndexCapacityError(err) {
					capacityExceeded = true
				} else {
					return newStorageStageError("apply archive claim", err)
				}
			}
		}
		if capacityExceeded {
			if source != archiveindex.ClaimLiveOrigin || exists && existing.SHA256 != result.SHA256 {
				return ErrArchiveIndexLimit
			}
			if exists {
				return nil
			} // Root already durably selects these exact bytes.
			inventory = archiveindex.Inventory{}
		}
		if !capacityExceeded {
			rootSegmentID := segment.ID
			if exists {
				rootSegmentID = existing.ID
			}
			if _, _, shardErr := archiveClaimShardForInventory(inventory, segmentID, coordinate, rootSegmentID); shardErr != nil {
				if errors.Is(shardErr, ErrArchiveIndexLimit) && source == archiveindex.ClaimLiveOrigin && !exists && disposition == archiveindex.DispositionAccepted {
					// Root is the canonical projection and can reconstruct this
					// unique selected claim. Auxiliary metadata limits must not
					// interrupt normal live capture.
					capacityExceeded = true
					inventory = archiveindex.Inventory{}
				} else {
					return newStorageStageError("build archive claim shard", shardErr)
				}
			}
		}

		if exists && existing.SHA256 == result.SHA256 && existing.PayloadSize == result.Size {
			if !capacityExceeded {
				if err := m.persistClaimShard(rootSnapshot.ID, inventory, segmentID, coordinate, existing.ID); err != nil {
					return err
				}
			}
			return nil
		}
		if exists && disposition == archiveindex.DispositionConflict {
			if err := m.saveImmutablePayload(rootSnapshot.ID, segment.StoragePath, data, result); err != nil {
				return newStorageStageError("persist immutable archive payload", err)
			}
			if err := m.store.SaveSidecar(rootSnapshot.ID, segment.StoragePath, segment); err != nil {
				return newStorageStageError("persist segment sidecar", err)
			}
			if err := m.persistClaimShard(rootSnapshot.ID, inventory, segmentID, coordinate, existing.ID); err != nil {
				return err
			}
			return nil // Playback keeps the prior selected root claim.
		}
		trackForOrdinal := rootSnapshot.Tracks[segment.TrackID]
		if !segment.IsInit && trackForOrdinal == nil {
			return errors.New("main track is missing")
		}
		if !segment.IsInit {
			ordinalCollision := false
			for _, current := range trackForOrdinal.Segments {
				if current.ArchiveOrdinal == segment.ArchiveOrdinal &&
					(current.SourceEpoch != segment.SourceEpoch || current.DiscontinuitySequence != segment.DiscontinuitySequence || current.Sequence != segment.Sequence) {
					ordinalCollision = true
					break
				}
			}
			if segment.ArchiveOrdinal == 0 || ordinalCollision {
				track := rootSnapshot.Tracks[segment.TrackID]
				if track == nil {
					return errors.New("main track is missing")
				}
				segment.ArchiveOrdinal, err = nextRootArchiveOrdinal(track)
				if err != nil {
					return err
				}
			}
			segment.ID = fmt.Sprintf("seg-%020d", segment.ArchiveOrdinal)
		}
		if err := m.saveImmutablePayload(rootSnapshot.ID, segment.StoragePath, data, result); err != nil {
			return newStorageStageError("persist immutable archive payload", err)
		}

		sourceDiscontinuity := segment.Discontinuity
		var previousUpdate *domain.Segment
		if !segment.IsInit && segment.SourceEpoch > 0 {
			marker, markerExists := epochMarker{}, false
			track := rootSnapshot.Tracks["main"]
			if track != nil {
				for i := range track.Segments {
					candidate := track.Segments[i]
					if candidate.SourceEpoch != segment.SourceEpoch {
						continue
					}
					if !markerExists || coordinateBefore(candidate.DiscontinuitySequence, candidate.Sequence, marker.discontinuitySequence, marker.sequence) {
						marker = epochMarker{
							ordinal: candidate.ArchiveOrdinal, discontinuitySequence: candidate.DiscontinuitySequence,
							sequence: candidate.Sequence, sourceDiscontinuity: candidate.Discontinuity,
						}
						markerExists = true
					}
				}
			}
			if !markerExists || coordinateBefore(segment.DiscontinuitySequence, segment.Sequence, marker.discontinuitySequence, marker.sequence) {
				if markerExists {
					for i := range track.Segments {
						if track.Segments[i].SourceEpoch == segment.SourceEpoch &&
							track.Segments[i].DiscontinuitySequence == marker.discontinuitySequence &&
							track.Segments[i].Sequence == marker.sequence {
							candidate := track.Segments[i]
							// Within one HLS discontinuity sequence the prior marker was
							// the synthetic first-in-epoch marker and moves to this late
							// prefix. Preserve an actual later sequence boundary.
							candidate.Discontinuity = marker.sourceDiscontinuity && marker.discontinuitySequence != segment.DiscontinuitySequence
							previousUpdate = &candidate
							break
						}
					}
				}
				segment.Discontinuity = true
				planned := epochMarker{
					ordinal: segment.ArchiveOrdinal, discontinuitySequence: segment.DiscontinuitySequence,
					sequence: segment.Sequence, sourceDiscontinuity: sourceDiscontinuity,
				}
				plannedMarker = &planned
			}
		}
		if previousUpdate != nil {
			if err := m.store.SaveSidecar(rootSnapshot.ID, previousUpdate.StoragePath, *previousUpdate); err != nil {
				return newStorageStageError("persist prior segment sidecar", err)
			}
		}
		if err := m.store.SaveSidecar(rootSnapshot.ID, segment.StoragePath, segment); err != nil {
			return newStorageStageError("persist segment sidecar", err)
		}
		if !capacityExceeded {
			if err := m.persistClaimShard(rootSnapshot.ID, inventory, segmentID, coordinate, segment.ID); err != nil {
				return err
			}
		}

		if err := m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
			recording.SourceSessionID = identity.ID
			track := recording.Tracks[segment.TrackID]
			if track == nil {
				return errors.New("main track is missing")
			}
			segments := &track.Segments
			if segment.IsInit {
				segments = &track.InitSegments
			}
			if previousUpdate != nil {
				for i := range track.Segments {
					if track.Segments[i].ID == previousUpdate.ID {
						track.Segments[i].Discontinuity = previousUpdate.Discontinuity
						break
					}
				}
			}
			*segments = append(*segments, segment)
			sort.Slice(*segments, func(i, j int) bool {
				if (*segments)[i].ArchiveOrdinal != (*segments)[j].ArchiveOrdinal {
					return (*segments)[i].ArchiveOrdinal < (*segments)[j].ArchiveOrdinal
				}
				return (*segments)[i].ID < (*segments)[j].ID
			})
			if !segment.IsInit && segment.ArchiveOrdinal >= track.NextArchiveOrdinal {
				if segment.ArchiveOrdinal == ^uint64(0) {
					return errors.New("archive ordinal overflow")
				}
				track.NextArchiveOrdinal = segment.ArchiveOrdinal + 1
			}
			track.PendingSegments = removePending(track.PendingSegments, segment.SourceEpoch, segment.DiscontinuitySequence, segment.Sequence)
			if segment.SourceEpoch == 0 {
				track.PendingSequences = removeSequence(track.PendingSequences, segment.Sequence)
			}
			if !segment.IsInit {
				recording.Gaps = removeRepairedGap(recording.Gaps, coordinate)
			}
			if !segment.IsInit && !capacityExceeded {
				if err := applyTimelineProjection(recording, inventory); err != nil {
					return newStorageStageError("apply timeline projection", err)
				}
			} else if !segment.IsInit && assignRootTimeline(recording) {
				if recording.TimelineRevision == ^uint64(0) {
					return errors.New("timeline revision overflow")
				}
				recording.TimelineRevision++
			}
			recording.LastError = ""
			return nil
		}); err != nil {
			return newStorageStageError("persist recording root", err)
		}
		return nil
	})
	if commitErr != nil {
		var staged interface{ Stage() string }
		if !errors.As(commitErr, &staged) {
			commitErr = newStorageStageError("canonical ownership validation", commitErr)
		}
		return storage.PayloadResult{}, nil, commitErr
	}
	if source == archiveindex.ClaimLiveOrigin {
		// A live claim can change the historical coverage plan even when the
		// source manifest itself did not introduce a new gap. Coalescing keeps
		// this commit-boundary trigger bounded while the recovery worker runs.
		m.signalAutomaticArchiveRecovery(recordingID(e))
	}
	return result, plannedMarker, nil
}

func (m *Manager) recordingSnapshotForArchive(e *entry) *domain.Recording {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.recording == nil || e.deleted {
		return nil
	}
	return clone(e.recording)
}

func findInventoryClaim(inventory archiveindex.Inventory, id string) (archiveindex.Claim, bool) {
	for _, segment := range inventory.Segments {
		for _, claim := range segment.Claims {
			if claim.ID == id {
				return claim, true
			}
		}
	}
	return archiveindex.Claim{}, false
}

func nextRootArchiveOrdinal(track *domain.Track) (uint64, error) {
	if track == nil {
		return 0, errors.New("main track is missing")
	}
	candidate := track.NextArchiveOrdinal
	used := make(map[uint64]struct{}, len(track.Segments))
	var maxOrdinal uint64
	for _, segment := range track.Segments {
		if segment.ArchiveOrdinal > 0 {
			used[segment.ArchiveOrdinal] = struct{}{}
			if segment.ArchiveOrdinal > maxOrdinal {
				maxOrdinal = segment.ArchiveOrdinal
			}
		}
	}
	if candidate == 0 || candidate <= maxOrdinal {
		if maxOrdinal == ^uint64(0) {
			return 0, errors.New("archive ordinal overflow")
		}
		candidate = maxOrdinal + 1
	}
	for {
		if _, exists := used[candidate]; !exists {
			return candidate, nil
		}
		if candidate == ^uint64(0) {
			return 0, errors.New("archive ordinal overflow")
		}
		candidate++
	}
}

func (m *Manager) saveImmutablePayload(recordingID, path string, data []byte, expected storage.PayloadResult) error {
	info, err := m.store.StatPayload(recordingID, path)
	if err == nil {
		if !info.Regular || info.Size != expected.Size {
			return errors.New("immutable archive object identity mismatch")
		}
		reader, openErr := m.store.OpenPayloadReader(recordingID, path)
		if openErr != nil {
			return errors.New("immutable archive object is unavailable")
		}
		h := sha256.New()
		count, copyErr := io.Copy(h, reader)
		closeErr := reader.Close()
		if copyErr != nil || closeErr != nil || count != expected.Size || hex.EncodeToString(h.Sum(nil)) != expected.SHA256 {
			return errors.New("immutable archive object hash mismatch")
		}
		return nil
	}
	if !isMissingArchiveSidecar(err) && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	result, err := m.store.SavePayloadExact(recordingID, path, bytes.NewReader(data), m.ingest.Options().MaxPayloadBytes, expected.Size)
	if err != nil {
		return err
	}
	if result.Size != expected.Size || result.SHA256 != expected.SHA256 {
		return errors.New("stored archive object identity mismatch")
	}
	return nil
}

func applyTimelineProjection(recording *domain.Recording, inventory archiveindex.Inventory) error {
	changed := false
	ordinals := make(map[archiveindex.Coordinate]uint64, len(inventory.Segments))
	for _, item := range inventory.Segments {
		if item.Coordinate.Kind == archiveindex.ObjectMedia {
			ordinals[item.Coordinate] = item.TimelineOrdinal
		}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			return errors.New("recording contains invalid track")
		}
		for i := range track.Segments {
			coordinate := coordinateForSegment(inventory.Session.ID, track.Segments[i])
			if ordinal, ok := ordinals[coordinate]; ok && track.Segments[i].TimelineOrdinal != ordinal {
				track.Segments[i].TimelineOrdinal = ordinal
				changed = true
			}
		}
	}
	if changed {
		if recording.TimelineRevision == ^uint64(0) {
			return errors.New("timeline revision overflow")
		}
		recording.TimelineRevision++
	}
	return nil
}

func assignRootTimeline(recording *domain.Recording) bool {
	type item struct {
		track   *domain.Track
		index   int
		segment domain.Segment
	}
	items := make([]item, 0)
	for _, track := range recording.Tracks {
		if track != nil {
			for i, segment := range track.Segments {
				items = append(items, item{track: track, index: i, segment: segment})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i].segment, items[j].segment
		if a.TrackID != b.TrackID {
			return a.TrackID < b.TrackID
		}
		if a.SourceEpoch != b.SourceEpoch {
			return a.SourceEpoch < b.SourceEpoch
		}
		if a.DiscontinuitySequence != b.DiscontinuitySequence {
			return a.DiscontinuitySequence < b.DiscontinuitySequence
		}
		return a.Sequence < b.Sequence
	})
	changed := false
	for i := range items {
		want := uint64(i + 1)
		segment := &items[i].track.Segments[items[i].index]
		if segment.TimelineOrdinal != want {
			segment.TimelineOrdinal = want
			changed = true
		}
	}
	return changed
}

func immutableObjectPath(segmentID, digest, sourceURI string) string {
	return fmt.Sprintf("tracks/main/objects/%s/%s%s", segmentID, digest, extensionFor(sourceURI))
}

func claimIdentity(source archiveindex.ClaimSource, segmentID, digest string) string {
	return string(source) + ":" + segmentID + ":" + digest
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func findRootSegment(recording *domain.Recording, coordinate archiveindex.Coordinate) (domain.Segment, bool) {
	if recording == nil {
		return domain.Segment{}, false
	}
	track := recording.Tracks[coordinate.TrackID]
	if track == nil {
		return domain.Segment{}, false
	}
	list := track.Segments
	if coordinate.Kind == archiveindex.ObjectInit {
		list = track.InitSegments
	}
	for _, segment := range list {
		if coordinateForSegment(coordinate.SessionID, segment) == coordinate {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

func coordinateBefore(aDiscontinuitySequence, aSequence, bDiscontinuitySequence, bSequence uint64) bool {
	if aDiscontinuitySequence != bDiscontinuitySequence {
		return aDiscontinuitySequence < bDiscontinuitySequence
	}
	return aSequence < bSequence
}

func archiveTimelineChanged(recording *domain.Recording, inventory archiveindex.Inventory) bool {
	for _, item := range inventory.Segments {
		if item.Coordinate.Kind != archiveindex.ObjectMedia {
			continue
		}
		segment, ok := findRootSegment(recording, item.Coordinate)
		if !ok || segment.TimelineOrdinal != item.TimelineOrdinal {
			return true
		}
	}
	return false
}

func removeRepairedGap(gaps []domain.Gap, coordinate archiveindex.Coordinate) []domain.Gap {
	out := make([]domain.Gap, 0, len(gaps)+1)
	for _, gap := range gaps {
		if gap.TrackID != coordinate.TrackID || gap.SourceEpoch != coordinate.SourceEpoch || gap.DiscontinuitySequence != coordinate.DiscontinuitySequence || coordinate.Sequence < gap.FromSequence || coordinate.Sequence > gap.ToSequence {
			out = append(out, gap)
			continue
		}
		if gap.FromSequence < coordinate.Sequence {
			left := gap
			left.ToSequence = coordinate.Sequence - 1
			out = append(out, left)
		}
		if coordinate.Sequence < gap.ToSequence && coordinate.Sequence != ^uint64(0) {
			right := gap
			right.FromSequence = coordinate.Sequence + 1
			out = append(out, right)
		}
	}
	return out
}

func recordHistoricalCoverage(coordinate archiveindex.Coordinate, state archiveindex.CoverageState, reason string, observed time.Time) archiveindex.Coverage {
	return archiveindex.Coverage{
		SessionID: coordinate.SessionID, TrackID: coordinate.TrackID,
		SourceEpoch: coordinate.SourceEpoch, DiscontinuitySequence: coordinate.DiscontinuitySequence,
		FromSequence: coordinate.Sequence, ToSequence: coordinate.Sequence,
		State: state, ObservedAt: observed.UTC(), Reason: strings.TrimSpace(reason), Kind: coordinate.Kind,
	}
}
