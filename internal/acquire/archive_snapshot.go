package acquire

import (
	"context"
	"errors"
	"io"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/storage"
)

// BuildArchiveSnapshot creates a read-only immutable projection of the
// authoritative RC12 archive index and verifies every selected canonical
// payload against its declared size and SHA-256 before returning it. It does
// not rewrite recording roots, claims, media bytes, or local ownership state.
// Use PersistArchiveSnapshot to append it to the separate catalog.
func (m *Manager) BuildArchiveSnapshot(ctx context.Context, recordingID string, sequence uint64, previousSnapshotID string) (archivesnapshot.Snapshot, error) {
	if m == nil || m.store == nil || ctx == nil || !validRecordingID(recordingID) {
		return archivesnapshot.Snapshot{}, archivesnapshot.ErrInvalidSnapshot
	}
	if err := ctx.Err(); err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	inventory, err := m.ArchiveInventory(recordingID)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return buildArchiveSnapshot(ctx, m.store, recordingID, inventory, sequence, previousSnapshotID)
}

// PersistArchiveSnapshot appends the current verified archive projection to
// the separate immutable catalog. An unchanged content identity is
// idempotent and does not produce a duplicate snapshot. Archive data is read
// only; a catalog failure never rewrites the canonical recording.
func (m *Manager) PersistArchiveSnapshot(ctx context.Context, catalog *archivecatalog.Store, recordingID string) (archivesnapshot.Snapshot, error) {
	if m == nil || m.store == nil || catalog == nil || ctx == nil || !validRecordingID(recordingID) {
		return archivesnapshot.Snapshot{}, archivesnapshot.ErrInvalidSnapshot
	}
	if err := ctx.Err(); err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	inventory, err := m.ArchiveInventory(recordingID)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	current, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	archiveID := current.Content.ArchiveID
	latest, headErr := catalog.Head(ctx, archiveID)
	if headErr == nil {
		if current.SnapshotID == latest.SnapshotID {
			return latest, nil
		}
		if latest.Sequence >= 9007199254740991 {
			return archivesnapshot.Snapshot{}, archivesnapshot.ErrInvalidSnapshot
		}
		return m.buildAndAppendSnapshot(ctx, catalog, recordingID, inventory, latest.Sequence+1, latest.SnapshotID)
	}
	if !errors.Is(headErr, archivecatalog.ErrNotFound) {
		return archivesnapshot.Snapshot{}, headErr
	}
	return m.buildAndAppendSnapshot(ctx, catalog, recordingID, inventory, 1, "")
}

func (m *Manager) buildAndAppendSnapshot(ctx context.Context, catalog *archivecatalog.Store, recordingID string, inventory archiveindex.Inventory, sequence uint64, previousSnapshotID string) (archivesnapshot.Snapshot, error) {
	snapshot, err := buildArchiveSnapshot(ctx, m.store, recordingID, inventory, sequence, previousSnapshotID)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	if err := catalog.Append(ctx, snapshot); err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return snapshot, nil
}

func buildArchiveSnapshot(ctx context.Context, store *storage.Store, recordingID string, inventory archiveindex.Inventory, sequence uint64, previousSnapshotID string) (archivesnapshot.Snapshot, error) {
	snapshot, err := archivesnapshot.Build(inventory, sequence, previousSnapshotID)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	source := canonicalArchivePayloadSource{store: store, recordingID: recordingID, inventory: inventory}
	if err := archivesnapshot.VerifyPayloads(ctx, snapshot, source); err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return snapshot, nil
}

type canonicalArchivePayloadSource struct {
	store       *storage.Store
	recordingID string
	inventory   archiveindex.Inventory
}

// CanonicalSnapshotSource opens only canonical payloads selected by the
// verified archive inventory. It is intentionally read-only and exposes no
// archive mutation or owner authority.
type CanonicalSnapshotSource interface {
	OpenCanonical(context.Context, archiveindex.Coordinate) (io.ReadCloser, error)
}

// SnapshotObjectSource binds an immutable snapshot back to the current sealed
// RC12 inventory before replication reads begin. Per-object encryption still
// verifies the streamed bytes against the snapshot's committed size and hash.
func (m *Manager) SnapshotObjectSource(ctx context.Context, recordingID string, snapshot archivesnapshot.Snapshot) (CanonicalSnapshotSource, error) {
	if m == nil || m.store == nil || ctx == nil || !validRecordingID(recordingID) || archivesnapshot.Verify(snapshot) != nil || snapshot.Content.Archive != archiveindex.ArchiveSealed {
		return nil, archivesnapshot.ErrInvalidSnapshot
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inventory, err := m.ArchiveInventory(recordingID)
	if err != nil {
		return nil, err
	}
	current, err := archivesnapshot.Build(inventory, snapshot.Sequence, snapshot.PreviousSnapshotID)
	if err != nil || current.Content.Archive != archiveindex.ArchiveSealed || current.SnapshotID != snapshot.SnapshotID || current.ContentHash != snapshot.ContentHash {
		return nil, archivesnapshot.ErrInvalidSnapshot
	}
	return canonicalArchivePayloadSource{store: m.store, recordingID: recordingID, inventory: inventory}, nil
}

// SnapshotHistoryObjectSource binds an older immutable snapshot to payload
// claims still retained in the current sealed archive. Unlike
// SnapshotObjectSource, it does not rebuild the historical projection from
// today's inventory: later repair or recovery may have appended coordinates.
// Each requested coordinate must still have a verified persisted claim with
// the exact size and digest committed by that historical snapshot. Missing or
// changed bytes fail closed.
func (m *Manager) SnapshotHistoryObjectSource(ctx context.Context, recordingID string, snapshot archivesnapshot.Snapshot) (CanonicalSnapshotSource, error) {
	if m == nil || m.store == nil || ctx == nil || !validRecordingID(recordingID) ||
		archivesnapshot.Verify(snapshot) != nil {
		return nil, archivesnapshot.ErrInvalidSnapshot
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inventory, err := m.ArchiveInventory(recordingID)
	if err != nil || inventory.Archive != archiveindex.ArchiveSealed {
		return nil, archivesnapshot.ErrInvalidSnapshot
	}
	archiveID := inventory.ArchiveID
	if archiveID == "" {
		archiveID, err = archivesnapshot.ArchiveIDForRecording(inventory.RecordingID)
		if err != nil {
			return nil, archivesnapshot.ErrInvalidSnapshot
		}
	}
	if archiveID != snapshot.Content.ArchiveID {
		return nil, archivesnapshot.ErrInvalidSnapshot
	}
	segments := make(map[archiveindex.Coordinate]archiveindex.Segment, len(inventory.Segments))
	for _, segment := range inventory.Segments {
		segments[segment.Coordinate] = segment
	}
	paths := make(map[archiveindex.Coordinate]string, len(snapshot.Content.Objects))
	for _, object := range snapshot.Content.Objects {
		if object.State != archiveindex.CoveragePresent {
			continue
		}
		segment, found := segments[object.Coordinate]
		if !found {
			return nil, archivesnapshot.ErrPayloadSource
		}
		selectedPath := ""
		for _, claim := range segment.Claims {
			if claim.Verification != archiveindex.VerificationVerified || claim.PayloadPath == "" ||
				claim.Size != object.PlaintextSize || "sha256:"+claim.SHA256 != object.PlaintextSHA256 {
				continue
			}
			if claim.ID == segment.SelectedClaimID {
				selectedPath = claim.PayloadPath
				break
			}
			if selectedPath == "" {
				selectedPath = claim.PayloadPath
			}
		}
		if selectedPath == "" {
			return nil, archivesnapshot.ErrPayloadSource
		}
		paths[object.Coordinate] = selectedPath
	}
	return historicalCanonicalPayloadSource{store: m.store, recordingID: recordingID, paths: paths}, nil
}

type historicalCanonicalPayloadSource struct {
	store       *storage.Store
	recordingID string
	paths       map[archiveindex.Coordinate]string
}

func (s historicalCanonicalPayloadSource) OpenCanonical(ctx context.Context, coordinate archiveindex.Coordinate) (io.ReadCloser, error) {
	if ctx == nil || s.store == nil || s.recordingID == "" {
		return nil, archivesnapshot.ErrPayloadSource
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := s.paths[coordinate]
	if path == "" {
		return nil, archivesnapshot.ErrPayloadSource
	}
	reader, err := s.store.OpenPayloadReader(s.recordingID, path)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, io.EOF) {
			return nil, archivesnapshot.ErrPayloadSource
		}
		return nil, err
	}
	return contextReadCloser{ctx: ctx, reader: reader}, nil
}

func (s canonicalArchivePayloadSource) OpenCanonical(ctx context.Context, coordinate archiveindex.Coordinate) (io.ReadCloser, error) {
	if ctx == nil || s.store == nil || s.recordingID == "" {
		return nil, archivesnapshot.ErrPayloadSource
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, segment := range s.inventory.Segments {
		if segment.Coordinate != coordinate || segment.SelectedClaimID == "" {
			continue
		}
		for _, claim := range segment.Claims {
			if claim.ID != segment.SelectedClaimID || claim.Verification != archiveindex.VerificationVerified || claim.PayloadPath == "" {
				continue
			}
			reader, err := s.store.OpenPayloadReader(s.recordingID, claim.PayloadPath)
			if err != nil {
				if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, io.EOF) {
					return nil, archivesnapshot.ErrPayloadSource
				}
				return nil, err
			}
			return contextReadCloser{ctx: ctx, reader: reader}, nil
		}
		return nil, archivesnapshot.ErrPayloadSource
	}
	return nil, archivesnapshot.ErrPayloadSource
}

type contextReadCloser struct {
	ctx    context.Context
	reader io.ReadCloser
}

func (r contextReadCloser) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func (r contextReadCloser) Close() error { return r.reader.Close() }
