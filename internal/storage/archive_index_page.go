package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/integrated-recorder/core/internal/domain"
)

const (
	// ArchiveIndexPageMax bounds the amount of metadata returned by one browse
	// request. The archive index never reads media payload bytes.
	ArchiveIndexPageMax        = 100
	archiveIndexCursorMaxBytes = 4096
)

var (
	ErrArchiveIndexInvalidCursor = errors.New("archive index cursor is invalid")
	ErrArchiveIndexStaleCursor   = errors.New("archive index changed during pagination")
)

// ShardedArchiveIndexPage is a bounded page of V2 canonical object metadata.
// Ordering is stable and seekable: root, manifest ordinals, then per-track init
// ordinals, then per-track media ArchiveOrdinals. Track IDs are sorted
// lexicographically. ArchiveRevision changes invalidate an in-flight cursor.
type ShardedArchiveIndexPage struct {
	Entries    []ArchiveEntry `json:"entries"`
	NextCursor string         `json:"next_cursor,omitempty"`
	HasMore    bool           `json:"has_more"`
}

type archiveIndexCursor struct {
	Version  int    `json:"v"`
	ID       string `json:"id"`
	Snapshot string `json:"s"`
	Phase    uint8  `json:"p"`
	Track    uint32 `json:"t,omitempty"`
	Ordinal  uint64 `json:"o,omitempty"`
	Part     uint8  `json:"x,omitempty"`
}

const (
	archiveIndexPhaseRoot uint8 = iota
	archiveIndexPhaseManifest
	archiveIndexPhaseInit
	archiveIndexPhaseMedia
	archiveIndexPhaseDone
)

// ShardedArchiveIndexPage reads a seekable V2 index page without materializing
// the archive history or statting every referenced payload. The bounded root
// and only pages intersecting this result are read. A cursor is tied to the
// canonical archive revision; callers restart pagination if the archive changes.
func (s *Store) ShardedArchiveIndexPage(ctx context.Context, id, cursor string, limit int) (ShardedArchiveIndexPage, error) {
	if err := checkV2Context(ctx); err != nil {
		return ShardedArchiveIndexPage{}, err
	}
	if s == nil || s.StorageBackend == nil || !recordingIDPattern.MatchString(id) || limit < 1 || limit > ArchiveIndexPageMax || len(cursor) > archiveIndexCursorMaxBytes {
		return ShardedArchiveIndexPage{}, ErrArchiveIndexInvalidCursor
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return ShardedArchiveIndexPage{}, err
	}
	snapshot, err := archiveIndexSnapshot(header)
	if err != nil {
		return ShardedArchiveIndexPage{}, err
	}
	trackIDs := make([]string, 0, len(header.Tracks))
	for trackID := range header.Tracks {
		trackIDs = append(trackIDs, trackID)
	}
	sort.Strings(trackIDs)

	state := archiveIndexCursor{Version: 1, ID: id, Snapshot: snapshot, Phase: archiveIndexPhaseRoot}
	if cursor != "" {
		state, err = decodeArchiveIndexCursor(cursor)
		if err != nil || state.ID != id {
			return ShardedArchiveIndexPage{}, ErrArchiveIndexInvalidCursor
		}
		if state.Snapshot != snapshot {
			return ShardedArchiveIndexPage{}, ErrArchiveIndexStaleCursor
		}
		if !validArchiveIndexCursorState(state, trackIDs, header) {
			return ShardedArchiveIndexPage{}, ErrArchiveIndexInvalidCursor
		}
	}

	rootInfo, err := s.StatPayload(id, "recording.json")
	if err != nil || !rootInfo.Regular || rootInfo.Size < 0 {
		return ShardedArchiveIndexPage{}, errors.New("recording metadata is unavailable")
	}
	reader := archiveIndexPageReader{
		store: s, ctx: ctx, id: id, header: header, trackIDs: trackIDs, rootSize: rootInfo.Size,
		manifestPages: make(map[uint64]v2RevisionPage[json.RawMessage]),
		mediaPages:    make(map[archiveIndexMediaPageKey]v2MediaPage),
	}

	result := ShardedArchiveIndexPage{Entries: make([]ArchiveEntry, 0, limit)}
	for len(result.Entries) < limit {
		entry, ok, next, nextErr := reader.next(state)
		if nextErr != nil {
			return ShardedArchiveIndexPage{}, nextErr
		}
		if !ok {
			break
		}
		result.Entries = append(result.Entries, entry)
		state = next
	}
	// One-item lookahead determines has_more without scanning history. Keep the
	// state before lookahead as the opaque cursor for the next page.
	lookahead := state
	if _, ok, _, lookaheadErr := reader.next(state); lookaheadErr != nil {
		return ShardedArchiveIndexPage{}, lookaheadErr
	} else if ok {
		result.HasMore = true
		result.NextCursor, err = encodeArchiveIndexCursor(lookahead)
		if err != nil {
			return ShardedArchiveIndexPage{}, err
		}
	}
	return result, nil
}

type archiveIndexMediaPageKey struct {
	track string
	init  bool
	page  uint64
}

type archiveIndexPageReader struct {
	store         *Store
	ctx           context.Context
	id            string
	header        *domain.Recording
	trackIDs      []string
	rootSize      int64
	manifestPages map[uint64]v2RevisionPage[json.RawMessage]
	mediaPages    map[archiveIndexMediaPageKey]v2MediaPage
}

// next returns the entry at state and the state after that entry. It uses
// page-number/ordinal cursors, so work is proportional to page size rather
// than archive length.
func (r *archiveIndexPageReader) next(state archiveIndexCursor) (ArchiveEntry, bool, archiveIndexCursor, error) {
	for {
		if err := checkV2Context(r.ctx); err != nil {
			return ArchiveEntry{}, false, state, err
		}
		switch state.Phase {
		case archiveIndexPhaseRoot:
			state.Phase, state.Ordinal = archiveIndexPhaseManifest, 1
			return ArchiveEntry{Path: "recording.json", Kind: "recording", Size: r.rootSize}, true, state, nil
		case archiveIndexPhaseManifest:
			count := r.header.ShardedArchive.ManifestSnapshotCount
			if state.Ordinal == 0 {
				state.Ordinal = 1
			}
			if state.Ordinal > count {
				state.Phase, state.Track, state.Ordinal, state.Part = archiveIndexPhaseInit, 0, 1, 0
				continue
			}
			snapshot, err := r.manifest(state.Ordinal)
			if err != nil {
				return ArchiveEntry{}, false, state, err
			}
			if !canonicalRelativePath(snapshot.StoragePath) || !strings.HasPrefix(snapshot.StoragePath, "manifests/") || snapshot.Size < 0 || !validLowerDigest(snapshot.SHA256) {
				return ArchiveEntry{}, false, state, ErrShardedArchiveInvalid
			}
			entry := ArchiveEntry{Path: snapshot.StoragePath, Kind: "manifest", Size: snapshot.Size, SHA256: snapshot.SHA256}
			if state.Part == 1 {
				encoded, err := json.Marshal(snapshot)
				if err != nil {
					return ArchiveEntry{}, false, state, ErrShardedArchiveInvalid
				}
				entry = ArchiveEntry{Path: snapshot.StoragePath + ".json", Kind: "manifest_sidecar", Size: int64(len(encoded) + 1)}
				state.Ordinal++
				state.Part = 0
			} else {
				state.Part = 1
			}
			return entry, true, state, nil
		case archiveIndexPhaseInit, archiveIndexPhaseMedia:
			init := state.Phase == archiveIndexPhaseInit
			for state.Track < uint32(len(r.trackIDs)) {
				trackID := r.trackIDs[state.Track]
				track := r.header.Tracks[trackID]
				count := track.MediaHighWater
				if init {
					count = track.InitCount
				}
				if state.Ordinal == 0 {
					state.Ordinal = 1
				}
				if state.Ordinal > count {
					state.Track++
					state.Ordinal = 1
					continue
				}
				record, err := r.media(trackID, init, state.Ordinal)
				if err != nil {
					return ArchiveEntry{}, false, state, err
				}
				if err := validateV2Record(r.header, record); err != nil || record.IndexOrdinal != state.Ordinal {
					return ArchiveEntry{}, false, state, ErrShardedArchiveInvalid
				}
				segment := record.Segment
				if !canonicalRelativePath(segment.StoragePath) || !strings.HasPrefix(segment.StoragePath, "tracks/") {
					return ArchiveEntry{}, false, state, ErrShardedArchiveInvalid
				}
				kind := "segment"
				if init {
					kind = "init_segment"
				}
				state.Ordinal++
				return ArchiveEntry{Path: segment.StoragePath, Kind: kind, Size: segment.PayloadSize, SHA256: segment.SHA256}, true, state, nil
			}
			if init {
				state.Phase, state.Track, state.Ordinal = archiveIndexPhaseMedia, 0, 1
				continue
			}
			state.Phase = archiveIndexPhaseDone
		case archiveIndexPhaseDone:
			return ArchiveEntry{}, false, state, nil
		default:
			return ArchiveEntry{}, false, state, ErrArchiveIndexInvalidCursor
		}
	}
}

func (r *archiveIndexPageReader) manifest(ordinal uint64) (domain.ManifestSnapshot, error) {
	pageNo, offset := pageSlotWithEntries(ordinal, manifestShardMaxEntries)
	page, ok := r.manifestPages[pageNo]
	if !ok {
		var loaded v2RevisionPage[json.RawMessage]
		if err := r.store.loadV2JSON(r.ctx, r.id, fmt.Sprintf("archive/v2/manifests/%020d", pageNo), archiveShardMaxBytes, &loaded); err != nil {
			return domain.ManifestSnapshot{}, err
		}
		if loaded.Version != 1 || loaded.Number != pageNo || len(loaded.Entries) > manifestShardMaxEntries {
			return domain.ManifestSnapshot{}, ErrShardedArchiveInvalid
		}
		page = loaded
		r.manifestPages[pageNo] = loaded
	}
	if int(offset) >= len(page.Entries) {
		return domain.ManifestSnapshot{}, ErrShardedArchiveInvalid
	}
	var snapshot domain.ManifestSnapshot
	if json.Unmarshal(page.Entries[offset], &snapshot) != nil || validateV2Manifest(snapshot) != nil {
		return domain.ManifestSnapshot{}, ErrShardedArchiveInvalid
	}
	return snapshot, nil
}

func (r *archiveIndexPageReader) media(trackID string, init bool, ordinal uint64) (V2MediaRecord, error) {
	pageNo, offset := mediaPageSlot(ordinal)
	key := archiveIndexMediaPageKey{track: trackID, init: init, page: pageNo}
	page, ok := r.mediaPages[key]
	if !ok {
		loaded, err := r.store.loadV2MediaPage(r.ctx, r.id, trackID, init, pageNo)
		if err != nil {
			return V2MediaRecord{}, err
		}
		page, ok = loaded, true
		r.mediaPages[key] = page
	}
	if !ok || int(offset) >= len(page.Entries) {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	return page.Entries[offset], nil
}

func validArchiveIndexCursorState(cursor archiveIndexCursor, trackIDs []string, header *domain.Recording) bool {
	if cursor.Version != 1 || cursor.Phase > archiveIndexPhaseDone || cursor.Part > 1 {
		return false
	}
	switch cursor.Phase {
	case archiveIndexPhaseRoot:
		return cursor.Track == 0 && cursor.Ordinal == 0 && cursor.Part == 0
	case archiveIndexPhaseManifest:
		count := header.ShardedArchive.ManifestSnapshotCount
		return cursor.Track == 0 && archiveIndexOrdinalAtBoundary(cursor.Ordinal, count) && (cursor.Ordinal <= count || cursor.Part == 0)
	case archiveIndexPhaseInit, archiveIndexPhaseMedia:
		if cursor.Track > uint32(len(trackIDs)) || cursor.Part != 0 {
			return false
		}
		if cursor.Track == uint32(len(trackIDs)) {
			return cursor.Ordinal == 1
		}
		count := header.Tracks[trackIDs[cursor.Track]].MediaHighWater
		if cursor.Phase == archiveIndexPhaseInit {
			count = header.Tracks[trackIDs[cursor.Track]].InitCount
		}
		return archiveIndexOrdinalAtBoundary(cursor.Ordinal, count)
	case archiveIndexPhaseDone:
		return cursor.Part == 0
	default:
		return false
	}
}

func archiveIndexOrdinalAtBoundary(ordinal, count uint64) bool {
	return ordinal >= 1 && (ordinal <= count || count < ^uint64(0) && ordinal == count+1)
}

func archiveIndexSnapshot(header *domain.Recording) (string, error) {
	type trackSnapshot struct {
		ID             string `json:"id"`
		InitCount      uint64 `json:"init_count"`
		MediaHighWater uint64 `json:"media_high_water"`
	}
	tracks := make([]trackSnapshot, 0, len(header.Tracks))
	for id, track := range header.Tracks {
		if track == nil {
			return "", ErrShardedArchiveInvalid
		}
		tracks = append(tracks, trackSnapshot{ID: id, InitCount: track.InitCount, MediaHighWater: track.MediaHighWater})
	}
	sort.Slice(tracks, func(i, j int) bool { return tracks[i].ID < tracks[j].ID })
	snapshot := struct {
		ID                    string          `json:"id"`
		ArchiveRevision       uint64          `json:"archive_revision"`
		TimelineRevision      uint64          `json:"timeline_revision"`
		ManifestSnapshotCount uint64          `json:"manifest_snapshot_count"`
		Tracks                []trackSnapshot `json:"tracks"`
	}{
		ID: header.ID, ArchiveRevision: header.ArchiveRevision, TimelineRevision: header.TimelineRevision,
		ManifestSnapshotCount: header.ShardedArchive.ManifestSnapshotCount, Tracks: tracks,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "", ErrShardedArchiveInvalid
	}
	digest := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeArchiveIndexCursor(cursor archiveIndexCursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil || len(data) > archiveIndexCursorMaxBytes {
		return "", ErrArchiveIndexInvalidCursor
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeArchiveIndexCursor(token string) (archiveIndexCursor, error) {
	if len(token) == 0 || len(token) > archiveIndexCursorMaxBytes {
		return archiveIndexCursor{}, ErrArchiveIndexInvalidCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(data) > archiveIndexCursorMaxBytes {
		return archiveIndexCursor{}, ErrArchiveIndexInvalidCursor
	}
	var cursor archiveIndexCursor
	if json.Unmarshal(data, &cursor) != nil || cursor.Version != 1 {
		return archiveIndexCursor{}, ErrArchiveIndexInvalidCursor
	}
	return cursor, nil
}
