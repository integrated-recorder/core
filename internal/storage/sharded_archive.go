package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
)

// ShardedArchiveFormatVersion identifies the bounded-root archive layout used
// for newly-created scalable recordings. It is deliberately independent from
// legacy format 0/1 decoding.
const ShardedArchiveFormatVersion = 2

const (
	ArchiveShardMaxEntries = 256
	// MediaShardMaxEntries is the maximum media or init records stored in one
	// media page. Consumers use this for fixture/page traversal; live-index and
	// generic observation pages use ArchiveShardMaxEntries instead.
	MediaShardMaxEntries = 64
	// Canonical records carry an 8 KiB-bounded source URI. Sixty-four entries
	// keep worst-case media pages below the 1 MiB encoded page bound.
	mediaShardMaxEntries = MediaShardMaxEntries
	// A valid metadata revision can contain a 64 KiB description plus a 4 KiB
	// title. Eight entries keep metadata pages below archiveShardMaxBytes.
	metadataShardMaxEntries = 8
	manifestShardMaxEntries = 64
	maxV2SourceURIBytes     = 8 << 10
	archiveShardMaxBytes    = 1 << 20
	archiveRootMaxBytes     = 1 << 20
	archiveSidecarPageSize  = 256
	archiveLiveTailMax      = 256
)

var (
	ErrShardedArchiveUnavailable = errors.New("sharded archive capability is unavailable")
	ErrShardedArchiveConflict    = errors.New("sharded archive identity conflict")
	ErrShardedArchiveInvalid     = errors.New("sharded archive document is invalid")
	ErrArchiveSealed             = errors.New("archive is sealed")
	ErrArchiveRecoveryPending    = errors.New("archive recovery state is pending")
	shardPageName                = regexp.MustCompile(`^[0-9]{20}$`)
	mediaSegmentID               = regexp.MustCompile(`^seg-([0-9]{20})$`)
)

// V2MediaRecord stores source identity with immutable media metadata. Timeline
// ordinals in Segment are ignored on disk and derived by the ordered iterator.
type V2MediaRecord struct {
	Coordinate archiveindex.Coordinate `json:"coordinate"`
	Segment    domain.Segment          `json:"segment"`
	// IndexOrdinal is the bounded page slot. For media it equals
	// Segment.ArchiveOrdinal; init objects use a per-track init slot.
	IndexOrdinal uint64 `json:"index_ordinal,omitempty"`
	// SelectedClaim is the first-class provenance record for the selected
	// canonical bytes. Keeping the common single claim beside the immutable
	// media record avoids a second object per ordinary live segment.
	SelectedClaim      *archiveindex.Claim        `json:"selected_claim,omitempty"`
	ClaimState         archiveindex.CoverageState `json:"claim_state,omitempty"`
	SupplementalClaims bool                       `json:"supplemental_claims,omitempty"`
}

// V2ClaimSet is the bounded provenance set for one stable logical coordinate.
type V2ClaimSet struct {
	SegmentID       string                     `json:"segment_id"`
	Coordinate      archiveindex.Coordinate    `json:"coordinate"`
	SelectedClaimID string                     `json:"selected_claim_id,omitempty"`
	State           archiveindex.CoverageState `json:"state"`
	Claims          []archiveindex.Claim       `json:"claims"`
	CountedClaims   uint64                     `json:"counted_claims,omitempty"`
}

// V2LiveSlot is one bounded active-live presentation position. Exactly one of
// Segment or Gap is set. Gap records are narrowed to one presentation slot.
type V2LiveSlot struct {
	Ordinal uint64          `json:"ordinal"`
	Segment *domain.Segment `json:"segment,omitempty"`
	Gap     *domain.Gap     `json:"gap,omitempty"`
}

type V2SidecarPage struct {
	Paths      []string
	NextCursor string
}

// ShardedRevisionResult reports durable revision counters after an observation
// publication. Callers use it to merge revisions into an in-flight root copy.
type ShardedRevisionResult struct {
	ArchiveRevision  uint64
	TimelineRevision uint64
	Changed          bool
}

type v2SidecarLister interface {
	ListSidecarsContext(context.Context, string, string, string, int) (V2SidecarPage, error)
}

type v2ContextReader interface {
	LoadRecordingContext(context.Context, string) (*domain.Recording, error)
}

type recordingFormatVersionReader interface {
	RecordingFormatVersion(context.Context, string) (int, error)
}

type v2ContextWriter interface {
	SaveRecordingContext(context.Context, *domain.Recording) error
}

type v2SidecarContextReader interface {
	LoadSidecarContext(context.Context, string, string, int64, any) error
}

type v2SidecarContextWriter interface {
	SaveSidecarContext(context.Context, string, string, any) error
}

type v2MediaPage struct {
	Version int             `json:"version"`
	TrackID string          `json:"track_id"`
	Number  uint64          `json:"number"`
	Entries []V2MediaRecord `json:"entries"`
}

// v2TimelineEntry is the compact ordered playback projection for one source
// coordinate. The media page remains authoritative for payload identity and
// provenance. This page supplies direct coordinate lookup and ordered VOD
// fields without one sidecar object per segment.
type v2TimelineEntry struct {
	Coordinate              archiveindex.Coordinate `json:"coordinate"`
	ArchiveOrdinal          uint64                  `json:"archive_ordinal,omitempty"`
	InitOrdinal             uint64                  `json:"init_ordinal,omitempty"`
	ID                      string                  `json:"id"`
	Duration                float64                 `json:"duration"`
	ProgramDateTime         *time.Time              `json:"program_date_time,omitempty"`
	InitSegmentID           string                  `json:"init_segment_id,omitempty"`
	Discontinuity           bool                    `json:"discontinuity,omitempty"`
	ByteRange               *domain.ByteRange       `json:"byte_range,omitempty"`
	LivePresentationOrdinal uint64                  `json:"live_presentation_ordinal,omitempty"`
	LiveDiscontinuity       bool                    `json:"live_discontinuity,omitempty"`
	LiveDiscontinuitySeq    uint64                  `json:"live_discontinuity_sequence,omitempty"`
}

type v2TimelinePage struct {
	Version               int                     `json:"version"`
	TrackID               string                  `json:"track_id"`
	SourceEpoch           uint64                  `json:"source_epoch"`
	DiscontinuitySequence uint64                  `json:"discontinuity_sequence"`
	SequenceBucket        uint64                  `json:"sequence_bucket"`
	Kind                  archiveindex.ObjectKind `json:"kind"`
	Entries               []v2TimelineEntry       `json:"entries"`
}

type v2IndexPointer struct {
	Version        int                     `json:"version"`
	Coordinate     archiveindex.Coordinate `json:"coordinate"`
	TrackID        string                  `json:"track_id"`
	Kind           archiveindex.ObjectKind `json:"kind"`
	Page           uint64                  `json:"page"`
	Offset         uint16                  `json:"offset"`
	IndexOrdinal   uint64                  `json:"index_ordinal"`
	ArchiveOrdinal uint64                  `json:"archive_ordinal,omitempty"`
	Record         V2MediaRecord           `json:"record"`
}

type v2LiveIndexEntry struct {
	PresentationOrdinal uint64                  `json:"presentation_ordinal"`
	Coordinate          archiveindex.Coordinate `json:"coordinate"`
	ArchiveOrdinal      uint64                  `json:"archive_ordinal"`
	Gap                 *domain.Gap             `json:"gap,omitempty"`
	Reserved            bool                    `json:"reserved,omitempty"`
}

type v2LivePage struct {
	Version int                `json:"version"`
	TrackID string             `json:"track_id"`
	Number  uint64             `json:"number"`
	Entries []v2LiveIndexEntry `json:"entries"`
}

type v2RevisionPage[T any] struct {
	Version int    `json:"version"`
	Number  uint64 `json:"number"`
	Entries []T    `json:"entries"`
}

type v2Observation struct {
	Version                int                    `json:"version"`
	Ordinal                uint64                 `json:"ordinal"`
	Gap                    *domain.Gap            `json:"gap,omitempty"`
	Coverage               *archiveindex.Coverage `json:"coverage,omitempty"`
	ArchiveRevisionChange  bool                   `json:"archive_revision_change,omitempty"`
	TimelineRevisionChange bool                   `json:"timeline_revision_change,omitempty"`
}

// v2CoverageStatePage is a derived point-state index. Each page covers one
// fixed source-sequence bucket. It lets observation revision checks avoid
// replaying prior gap/coverage history. Observation logs remain authoritative.
type v2CoverageStatePage struct {
	Version               int                         `json:"version"`
	SessionID             string                      `json:"session_id"`
	TrackID               string                      `json:"track_id"`
	SourceEpoch           uint64                      `json:"source_epoch"`
	DiscontinuitySequence uint64                      `json:"discontinuity_sequence"`
	Kind                  archiveindex.ObjectKind     `json:"kind"`
	Bucket                uint64                      `json:"bucket"`
	States                [mediaShardMaxEntries]uint8 `json:"states"`
}

type v2CoverageStateIndexMarker struct {
	Version int  `json:"version"`
	Ready   bool `json:"ready"`
}

type v2CoverageStatePageUpdate struct {
	Path string
	Page v2CoverageStatePage
}

type v2CoverageMediaLookup struct {
	timelinePages map[string]v2TimelinePage
	mediaPages    map[v2CoverageMediaPageKey]v2MediaPage
}

type v2CoverageMediaPageKey struct {
	init   bool
	number uint64
}

type v2ClaimDocument struct {
	Version int        `json:"version"`
	Set     V2ClaimSet `json:"set"`
}

type v2ClaimPage struct {
	Version int          `json:"version"`
	Bucket  uint16       `json:"bucket"`
	Number  uint64       `json:"number"`
	Entries []V2ClaimSet `json:"entries"`
}

const claimPageMaxEntries = 32

type v2ClaimCountIntent struct {
	Version       int    `json:"version"`
	SegmentID     string `json:"segment_id"`
	Base          uint64 `json:"base"`
	Target        uint64 `json:"target"`
	ClaimSetCount uint64 `json:"claim_set_count"`
}

func emptyV2ClaimCountIntent(intent v2ClaimCountIntent) bool {
	return intent.Version == 1 && intent.SegmentID == "" && intent.Base == 0 && intent.Target == 0 && intent.ClaimSetCount == 0
}

// v2LivePromotionIntent makes the one multi-object live-presentation update
// restartable. Store serializes writes per recording, so one fixed intent is
// sufficient and does not grow with the number of promoted coordinates.
type v2LivePromotionIntent struct {
	Version int               `json:"version"`
	Pending bool              `json:"pending,omitempty"`
	Record  V2MediaRecord     `json:"record,omitempty"`
	Header  *domain.Recording `json:"header,omitempty"`
}

const v2LivePromotionIntentPath = "archive/v2/live-promotion-intent"

func (s *LocalFilesystemBackend) LoadRecordingContext(ctx context.Context, id string) (*domain.Recording, error) {
	if err := checkV2Context(ctx); err != nil {
		return nil, err
	}
	return s.LoadRecordingReadOnly(id)
}

func (s *LocalFilesystemBackend) SaveRecordingContext(ctx context.Context, recording *domain.Recording) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if recording == nil || recording.FormatVersion != ShardedArchiveFormatVersion {
		return ErrShardedArchiveInvalid
	}
	if err := ensureV2Path(s.recordingDir(recording.ID), "recording.json", false); err != nil {
		return err
	}
	return s.SaveRecording(recording)
}

func (s *LocalFilesystemBackend) RecordingFormatVersion(ctx context.Context, id string) (int, error) {
	if err := checkV2Context(ctx); err != nil {
		return 0, err
	}
	if !recordingIDPattern.MatchString(id) {
		return 0, ErrShardedArchiveInvalid
	}
	directory := s.recordingDir(id)
	dirInfo, err := os.Lstat(directory)
	if err != nil {
		return 0, err
	}
	if !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return 0, ErrShardedArchiveInvalid
	}
	rootPath := filepath.Join(directory, "recording.json")
	rootInfo, err := os.Lstat(rootPath)
	if err != nil {
		return 0, err
	}
	return readRecordingFormatVersionSnapshot(ctx, rootPath, rootInfo)
}

// readRecordingFormatVersionSnapshot reads the format marker from one stable,
// bounded root file descriptor. A concurrent atomic root publication may make
// the descriptor refer to either the previously observed root or the current
// root. Both are complete canonical snapshots and must remain readable.
func readRecordingFormatVersionSnapshot(ctx context.Context, path string, observed os.FileInfo) (int, error) {
	if observed == nil || !observed.Mode().IsRegular() || observed.Mode()&os.ModeSymlink != 0 || observed.Size() < 0 || observed.Size() > maxRecordingJSONBytes {
		return 0, ErrShardedArchiveInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Mode()&os.ModeSymlink != 0 || opened.Size() < 0 || opened.Size() > maxRecordingJSONBytes {
		return 0, ErrShardedArchiveInvalid
	}
	current, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	if !current.Mode().IsRegular() || current.Mode()&os.ModeSymlink != 0 {
		return 0, ErrShardedArchiveInvalid
	}
	if !os.SameFile(observed, opened) && !os.SameFile(current, opened) {
		return 0, ErrShardedArchiveInvalid
	}
	prefix, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: file}, 4096))
	if err != nil {
		return 0, err
	}
	return parseRecordingFormatPrefix(prefix)
}

func (s *LocalFilesystemBackend) SaveSidecarContext(ctx context.Context, id, relative string, value any) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if err := ensureV2Path(s.recordingDir(id), relative+".json", true); err != nil {
		return err
	}
	started := time.Now()
	path, err := s.safePath(id, relative)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil || len(data)+1 > maxObjectJSONBytes {
		return errors.New("sidecar exceeds size limit")
	}
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	data = append(data, '\n')
	err = atomicWrite(path+".json", data, 0600)
	if err != nil {
		s.telemetry.recordError()
		return err
	}
	s.telemetry.recordWrite(uint64(len(data)), time.Since(started))
	return checkV2Context(ctx)
}

func (s *LocalFilesystemBackend) LoadSidecarContext(ctx context.Context, id, relative string, maxBytes int64, output any) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	err := s.LoadSidecar(id, relative, maxBytes, output)
	if err == nil {
		return checkV2Context(ctx)
	}
	return err
}

// ListSidecarsContext lists only files beneath requested archive/v2 subtree.
// It descends in lexical order and stops after one bounded page.
func (s *LocalFilesystemBackend) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (V2SidecarPage, error) {
	if err := validateV2ListRequest(id, prefix, cursor, limit); err != nil {
		return V2SidecarPage{}, err
	}
	root := s.recordingDir(id)
	if err := requireSafeDirectory(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return V2SidecarPage{}, nil
		}
		return V2SidecarPage{}, err
	}
	relativeDir := strings.TrimSuffix(prefix, "/")
	if err := ensureV2Path(root, relativeDir, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return V2SidecarPage{}, nil
		}
		return V2SidecarPage{}, err
	}
	directory, err := s.safePath(id, relativeDir)
	if err != nil {
		return V2SidecarPage{}, err
	}
	if err := requireSafeDirectory(directory); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return V2SidecarPage{}, nil
		}
		return V2SidecarPage{}, err
	}
	paths := make([]string, 0, limit+1)
	var walk func(string, string) error
	walk = func(full, relative string) error {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		entries, err := os.ReadDir(full)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := checkV2Context(ctx); err != nil {
				return err
			}
			name := entry.Name()
			childFull := filepath.Join(full, name)
			childRelative := relative + "/" + name
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("sidecar namespace contains unsafe entry")
			}
			if entry.IsDir() {
				childPrefix := childRelative + "/"
				if cursor != "" && childPrefix <= cursor && !strings.HasPrefix(cursor, childPrefix) {
					continue
				}
				if err := walk(childFull, childRelative); err != nil {
					return err
				}
				continue
			}
			if !entry.Type().IsRegular() || !strings.HasSuffix(name, ".json") {
				continue
			}
			item := childRelative
			if item <= cursor {
				continue
			}
			paths = append(paths, item)
			if len(paths) > limit {
				return errV2ListPageFull
			}
		}
		return nil
	}
	err = walk(directory, relativeDir)
	if err != nil && !errors.Is(err, errV2ListPageFull) {
		return V2SidecarPage{}, err
	}
	page := V2SidecarPage{}
	if len(paths) > limit {
		paths = paths[:limit]
		page.NextCursor = paths[len(paths)-1]
	}
	page.Paths = paths
	return page, nil
}

func (b *ObjectStoreArchiveBackend) LoadRecordingContext(ctx context.Context, id string) (*domain.Recording, error) {
	return b.loadRecording(ctx, id)
}

func (b *ObjectStoreArchiveBackend) RecordingFormatVersion(ctx context.Context, id string) (int, error) {
	if err := checkV2Context(ctx); err != nil {
		return 0, err
	}
	if !recordingIDPattern.MatchString(id) {
		return 0, ErrShardedArchiveInvalid
	}
	key := recordingObjectKey(id, "recording.json")
	info, err := b.stat(ctx, key)
	if err != nil {
		if isObjectNotFound(err) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	if info.Size == 0 {
		return 0, ErrShardedArchiveInvalid
	}
	length := int64(4096)
	if info.Size < length {
		length = info.Size
	}
	callCtx, cancel := objectContext(ctx)
	defer cancel()
	reader, ranged, err := b.objects.OpenRange(callCtx, key, 0, length)
	if err != nil {
		return 0, err
	}
	if reader == nil || ranged.Key != key || ranged.Size != info.Size {
		if reader != nil {
			_ = reader.Close()
		}
		return 0, ErrShardedArchiveInvalid
	}
	defer reader.Close()
	prefix, err := io.ReadAll(io.LimitReader(contextReader{ctx: callCtx, reader: reader}, length))
	if err != nil || int64(len(prefix)) != length {
		return 0, ErrShardedArchiveInvalid
	}
	return parseRecordingFormatPrefix(prefix)
}

func (b *ObjectStoreArchiveBackend) SaveRecordingContext(ctx context.Context, recording *domain.Recording) error {
	if err := validateArchiveRecording(recording); err != nil {
		return err
	}
	data, err := json.MarshalIndent(recording, "", "  ")
	if err != nil || len(data)+1 > maxRecordingJSONBytes {
		return errors.New("recording metadata exceeds size limit")
	}
	return b.putBytes(ctx, recordingObjectKey(recording.ID, "recording.json"), append(data, '\n'), maxRecordingJSONBytes)
}

func (b *ObjectStoreArchiveBackend) SaveSidecarContext(ctx context.Context, id, relative string, value any) error {
	key, err := b.recordingKey(id, relative)
	if err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil || len(data)+1 > maxObjectJSONBytes {
		return errors.New("sidecar exceeds size limit")
	}
	return b.putBytes(ctx, key+".json", append(data, '\n'), maxObjectJSONBytes)
}

func (b *ObjectStoreArchiveBackend) LoadSidecarContext(ctx context.Context, id, relative string, maxBytes int64, output any) error {
	if err := validateSidecarRead(id, relative, maxBytes, output); err != nil {
		return err
	}
	key, err := b.recordingKey(id, relative)
	if err != nil {
		return err
	}
	data, _, err := b.readObject(ctx, key+".json", maxBytes)
	if err != nil {
		return err
	}
	return decodeStrictSidecar(data, output)
}

func (b *ObjectStoreArchiveBackend) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (V2SidecarPage, error) {
	if err := validateV2ListRequest(id, prefix, cursor, limit); err != nil {
		return V2SidecarPage{}, err
	}
	physicalPrefix := recordingObjectKey(id, strings.TrimSuffix(prefix, "/")) + "/"
	physicalCursor := ""
	if cursor != "" {
		physicalCursor = recordingObjectKey(id, cursor)
	}
	page, err := b.listPage(ctx, physicalPrefix, physicalCursor, limit)
	if err != nil {
		return V2SidecarPage{}, err
	}
	result := V2SidecarPage{Paths: make([]string, 0, len(page.Items))}
	base := "recordings/" + id + "/"
	for _, item := range page.Items {
		if !strings.HasPrefix(item.Key, base) || !strings.HasSuffix(item.Key, ".json") {
			return V2SidecarPage{}, ErrShardedArchiveInvalid
		}
		result.Paths = append(result.Paths, strings.TrimPrefix(item.Key, base))
	}
	if page.NextCursor != "" {
		if !strings.HasPrefix(page.NextCursor, base) || !strings.HasSuffix(page.NextCursor, ".json") {
			return V2SidecarPage{}, ErrShardedArchiveInvalid
		}
		result.NextCursor = strings.TrimPrefix(page.NextCursor, base)
	}
	return result, nil
}

var errV2ListPageFull = errors.New("v2 sidecar page full")

func validateV2ListRequest(id, prefix, cursor string, limit int) error {
	if !recordingIDPattern.MatchString(id) || limit < 1 || limit > archiveSidecarPageSize || len(cursor) > 1024 {
		return ErrShardedArchiveInvalid
	}
	if len(prefix) == 0 || len(prefix) > 1024 || !strings.HasPrefix(prefix, "archive/v2/") || !strings.HasSuffix(prefix, "/") || !canonicalRelativePath(strings.TrimSuffix(prefix, "/")) {
		return ErrShardedArchiveInvalid
	}
	if cursor != "" && (!canonicalRelativePath(cursor) || !strings.HasPrefix(cursor, prefix)) {
		return ErrShardedArchiveInvalid
	}
	return nil
}

func requireSafeDirectory(name string) error {
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("sidecar namespace is not a safe directory")
	}
	return nil
}

func ensureV2Path(root, relative string, createDirectories bool) error {
	if !recordingIDPattern.MatchString(filepath.Base(root)) || !canonicalRelativePath(relative) {
		return ErrShardedArchiveInvalid
	}
	if err := requireSafeDirectory(root); err != nil {
		return err
	}
	parts := strings.Split(filepath.ToSlash(filepath.Dir(filepath.FromSlash(relative))), "/")
	if len(parts) == 1 && parts[0] == "." {
		return nil
	}
	current := root
	for _, part := range parts {
		if part == "." || part == "" {
			continue
		}
		current = filepath.Join(current, filepath.FromSlash(part))
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) && createDirectories {
			if err := os.Mkdir(current, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrShardedArchiveInvalid
		}
	}
	return nil
}

// CreateShardedRecording creates a new format-v2 recording with a bounded
// root. All history must be empty; callers publish canonical objects later.
func (s *Store) CreateShardedRecording(header *domain.Recording) error {
	if header == nil {
		return ErrShardedArchiveInvalid
	}
	if header.FormatVersion != 0 && header.FormatVersion != ShardedArchiveFormatVersion {
		return ErrUnsupportedRecordingFormat
	}
	copy := cloneRecordingHeader(header)
	copy.FormatVersion = ShardedArchiveFormatVersion
	if copy.ShardedArchive == nil {
		copy.ShardedArchive = &domain.ShardedArchiveSummary{}
	}
	if err := validateV2Header(copy); err != nil {
		return err
	}
	return s.StorageBackend.CreateRecording(copy)
}

// CreateShardedRecordingWithSidecar atomically publishes an empty v2 header
// and the caller's initial private acquisition sidecar where supported.
func (s *Store) CreateShardedRecordingWithSidecar(header *domain.Recording, relativePath string, value any) error {
	if header == nil {
		return ErrShardedArchiveInvalid
	}
	if header.FormatVersion != 0 && header.FormatVersion != ShardedArchiveFormatVersion {
		return ErrUnsupportedRecordingFormat
	}
	copy := cloneRecordingHeader(header)
	copy.FormatVersion = ShardedArchiveFormatVersion
	if copy.ShardedArchive == nil {
		copy.ShardedArchive = &domain.ShardedArchiveSummary{}
	}
	if err := validateV2Header(copy); err != nil {
		return err
	}
	if err := s.CreateRecordingWithSidecar(copy, relativePath, value); err != nil {
		return err
	}
	return nil
}

// CreateRecording routes explicit v2 roots through bounded-root validation.
// Legacy v0/v1 behavior is unchanged.
func (s *Store) CreateRecording(recording *domain.Recording) error {
	if recording != nil && recording.FormatVersion == ShardedArchiveFormatVersion {
		copy := cloneRecordingHeader(recording)
		if copy == nil {
			return ErrShardedArchiveInvalid
		}
		if copy.ShardedArchive == nil {
			copy.ShardedArchive = &domain.ShardedArchiveSummary{}
		}
		if err := validateV2Header(copy); err != nil {
			return err
		}
		return s.StorageBackend.CreateRecording(copy)
	}
	return s.StorageBackend.CreateRecording(recording)
}

// SaveRecording preserves the historical API while preventing callers from
// accidentally placing unbounded history back into a v2 root.
func (s *Store) SaveRecording(recording *domain.Recording) error {
	if recording != nil && recording.FormatVersion == ShardedArchiveFormatVersion {
		return s.SaveRecordingHeader(context.Background(), recording)
	}
	return s.StorageBackend.SaveRecording(recording)
}

// LoadRecordingHeader reads a bounded v2 root. Legacy recordings remain
// available through LoadRecordingReadOnly and are not migrated here.
func (s *Store) LoadRecordingHeader(ctx context.Context, id string) (*domain.Recording, error) {
	if err := checkV2Context(ctx); err != nil {
		return nil, err
	}
	if !recordingIDPattern.MatchString(id) {
		return nil, ErrShardedArchiveInvalid
	}
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return nil, err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion {
		return nil, ErrShardedArchiveUnavailable
	}
	if err := validateV2Header(header); err != nil {
		return nil, err
	}
	return header, nil
}

// RecordingFormatVersion probes only the bounded root prefix. It lets read
// paths choose the v1 or v2 representation without decoding a potentially
// giant legacy recording.json first.
func (s *Store) RecordingFormatVersion(ctx context.Context, id string) (int, error) {
	if err := checkV2Context(ctx); err != nil {
		return 0, err
	}
	if !recordingIDPattern.MatchString(id) {
		return 0, ErrShardedArchiveInvalid
	}
	if reader, ok := s.StorageBackend.(recordingFormatVersionReader); ok {
		return reader.RecordingFormatVersion(ctx, id)
	}
	return 0, ErrShardedArchiveUnavailable
}

func parseRecordingFormatPrefix(prefix []byte) (int, error) {
	const field = `"format_version"`
	index := bytes.Index(prefix, []byte(field))
	if index < 0 {
		return 0, ErrShardedArchiveInvalid
	}
	index += len(field)
	for index < len(prefix) && (prefix[index] == ' ' || prefix[index] == '\n' || prefix[index] == '\r' || prefix[index] == '\t') {
		index++
	}
	if index >= len(prefix) || prefix[index] != ':' {
		return 0, ErrShardedArchiveInvalid
	}
	index++
	for index < len(prefix) && (prefix[index] == ' ' || prefix[index] == '\n' || prefix[index] == '\r' || prefix[index] == '\t') {
		index++
	}
	start := index
	for index < len(prefix) && prefix[index] >= '0' && prefix[index] <= '9' {
		index++
	}
	if start == index {
		return 0, ErrShardedArchiveInvalid
	}
	version, err := strconv.Atoi(string(prefix[start:index]))
	if err != nil || validateRecordingFormat(version) != nil {
		return 0, ErrUnsupportedRecordingFormat
	}
	return version, nil
}

// SaveRecordingHeader atomically publishes one bounded root update. Ownership
// is enforced by the caller's existing owner/epoch commit fence.
func (s *Store) SaveRecordingHeader(ctx context.Context, header *domain.Recording) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if header == nil || !recordingIDPattern.MatchString(header.ID) {
		return ErrShardedArchiveInvalid
	}
	lock := s.v2Lock(header.ID)
	lock.Lock()
	defer lock.Unlock()
	return s.saveHeaderLocked(ctx, header, nil)
}

// PublishShardedMedia writes immutable page/index objects first and the
// supplied bounded header last. The header is the only visibility marker and
// atomically carries the caller's revisions and live-presentation state.
func (s *Store) PublishShardedMedia(ctx context.Context, header *domain.Recording, record V2MediaRecord) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if header == nil || !recordingIDPattern.MatchString(header.ID) {
		return ErrShardedArchiveInvalid
	}
	lock := s.v2Lock(header.ID)
	lock.Lock()
	defer lock.Unlock()
	if err := s.reconcileLivePromotionIntentLocked(ctx, header.ID); err != nil {
		return fmt.Errorf("reconcile pending live presentation promotion: %w", err)
	}
	current, err := s.loadRecordingContext(ctx, header.ID)
	if err != nil {
		return err
	}
	if current.FormatVersion != ShardedArchiveFormatVersion {
		return ErrShardedArchiveUnavailable
	}
	if current.ArchiveSealed {
		return ErrArchiveSealed
	}
	if err := validateV2Header(header); err != nil {
		return err
	}
	if err := validateV2Record(current, record); err != nil {
		return fmt.Errorf("validate sharded media publication: %w", err)
	}
	coord, err := canonicalV2Coordinate(record.Coordinate)
	if err != nil {
		return err
	}
	record.Coordinate = coord
	record.Segment.TimelineOrdinal = 0
	track := current.Tracks[coord.TrackID]
	if track == nil {
		return ErrShardedArchiveInvalid
	}
	if existing, err := s.lookupCoordinateLocked(ctx, current, coord); err == nil {
		if sameV2Media(existing, record) {
			// A fully-visible identical retry is idempotent. Persist any caller
			// header-only state update without changing object counters. Rebuild
			// deterministic indexes too: a previous process may have stopped after
			// the media root became visible but before its live projection was
			// repaired from an earlier explicit gap slot.
			if (!existing.Segment.IsInit && existing.Segment.ArchiveOrdinal <= track.MediaHighWater) || (existing.Segment.IsInit && existing.IndexOrdinal <= track.InitCount) {
				if err := s.publishV2Indexes(ctx, header.ID, record, current); err != nil {
					return fmt.Errorf("repair sharded media indexes on retry: %w", err)
				}
				merged := cloneRecordingHeader(header)
				mergeV2HighWater(merged, current)
				if current.ShardedArchive != nil && current.ShardedArchive.ClaimReconcilePending {
					merged.ShardedArchive.ClaimReconcilePending = true
				}
				if err := s.saveHeaderLocked(ctx, merged, current); err != nil {
					return err
				}
				if record.SupplementalClaims || record.SelectedClaim == nil {
					return s.ensureVisibleClaimCount(ctx, header.ID, coord)
				}
				return nil
			}
		} else if isV2LivePromotion(existing, record) {
			merged := cloneRecordingHeader(header)
			mergeV2HighWater(merged, current)
			mergeUint64(&merged.Tracks[coord.TrackID].LiveSlotHighWater, record.Segment.LivePresentationOrdinal)
			if current.ShardedArchive != nil && current.ShardedArchive.ClaimReconcilePending {
				merged.ShardedArchive.ClaimReconcilePending = true
			}
			intent := v2LivePromotionIntent{Version: 1, Pending: true, Record: record, Header: merged}
			if err := s.saveV2JSON(ctx, header.ID, v2LivePromotionIntentPath, intent, archiveRootMaxBytes); err != nil {
				return fmt.Errorf("persist live presentation promotion intent: %w", err)
			}
			if err := s.applyV2LivePromotion(ctx, header.ID, existing, record, merged.Tracks[coord.TrackID]); err != nil {
				return fmt.Errorf("publish live presentation promotion: %w", err)
			}
			if err := s.saveHeaderLocked(ctx, merged, current); err != nil {
				return err
			}
			intent.Pending = false
			intent.Record = V2MediaRecord{}
			intent.Header = nil
			return s.saveV2JSON(ctx, header.ID, v2LivePromotionIntentPath, intent, archiveRootMaxBytes)
		} else {
			return ErrShardedArchiveConflict
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	pointer, err := s.writeV2RecordObjects(ctx, header.ID, record, current)
	if err != nil {
		return fmt.Errorf("write sharded media indexes: %w", err)
	}
	_ = pointer
	merged := cloneRecordingHeader(header)
	mergeV2HighWater(merged, current)
	if current.ShardedArchive != nil && current.ShardedArchive.ClaimReconcilePending {
		merged.ShardedArchive.ClaimReconcilePending = true
	}
	if err := advanceV2Summary(merged, record); err != nil {
		return err
	}
	if err := s.saveHeaderLocked(ctx, merged, current); err != nil {
		return err
	}
	if record.SupplementalClaims || record.SelectedClaim == nil {
		return s.ensureVisibleClaimCount(ctx, header.ID, coord)
	}
	return nil
}

func isV2LivePromotion(existing, promoted V2MediaRecord) bool {
	if existing.Segment.IsInit || promoted.Segment.IsInit || existing.Segment.LivePresentationOrdinal != 0 || promoted.Segment.LivePresentationOrdinal == 0 {
		return false
	}
	return sameV2PromotionBase(existing, promoted)
}

func sameV2PromotionBase(existing, promoted V2MediaRecord) bool {
	priorDiscontinuity := existing.Segment.Discontinuity
	if priorDiscontinuity != promoted.Segment.Discontinuity && (!promoted.Segment.LiveDiscontinuity || !promoted.Segment.Discontinuity) {
		return false
	}
	existing.IndexOrdinal = promoted.IndexOrdinal
	left, right := existing, promoted
	left.Segment.TimelineOrdinal, right.Segment.TimelineOrdinal = 0, 0
	left.IndexOrdinal, right.IndexOrdinal = 0, 0
	left.Segment.Discontinuity, right.Segment.Discontinuity = false, false
	left.Segment.LivePresentationOrdinal, right.Segment.LivePresentationOrdinal = 0, 0
	left.Segment.LiveDiscontinuity, right.Segment.LiveDiscontinuity = false, false
	left.Segment.LiveDiscontinuitySequence, right.Segment.LiveDiscontinuitySequence = 0, 0
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (s *Store) applyV2LivePromotion(ctx context.Context, id string, existing, promoted V2MediaRecord, track *domain.Track) error {
	ordinal := existing.Segment.ArchiveOrdinal
	if ordinal == 0 || promoted.Segment.ArchiveOrdinal != ordinal || promoted.Segment.LivePresentationOrdinal == 0 || track == nil {
		return ErrShardedArchiveInvalid
	}
	pageNo, offset := mediaPageSlot(ordinal)
	page, err := s.loadV2MediaPage(ctx, id, existing.Segment.TrackID, false, pageNo)
	if err != nil {
		return err
	}
	if int(offset) >= len(page.Entries) {
		return ErrShardedArchiveConflict
	}
	pageRecord := page.Entries[offset]
	if !sameV2Media(pageRecord, promoted) && !sameV2PromotionBase(pageRecord, promoted) {
		return ErrShardedArchiveConflict
	}
	promoted.IndexOrdinal = ordinal
	promoted.Segment.TimelineOrdinal = 0
	page.Entries[offset] = promoted
	if err := s.saveV2JSON(ctx, id, v2MediaPagePath(existing.Segment.TrackID, false, pageNo), page, archiveShardMaxBytes); err != nil {
		return err
	}
	if err := s.writeV2TimelineEntry(ctx, id, promoted); err != nil {
		return err
	}
	return s.writeLiveIndex(ctx, id, promoted, track)
}

// reconcileLivePromotionIntentLocked replays the bounded per-recording intent
// before any new publication. Root state remains the visibility marker: the
// intent is outside the root and live high-water changes publish only after all
// index representations are durable.
func (s *Store) reconcileLivePromotionIntentLocked(ctx context.Context, id string) error {
	var intent v2LivePromotionIntent
	err := s.loadV2JSON(ctx, id, v2LivePromotionIntentPath, archiveRootMaxBytes, &intent)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if intent.Version != 1 {
		return ErrShardedArchiveInvalid
	}
	if !intent.Pending {
		return nil
	}
	if intent.Header == nil || validateV2Header(intent.Header) != nil || intent.Header.ID != id || validateV2Record(intent.Header, intent.Record) != nil {
		return ErrShardedArchiveInvalid
	}
	current, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return err
	}
	if current.FormatVersion != ShardedArchiveFormatVersion {
		return ErrShardedArchiveUnavailable
	}
	track := current.Tracks[intent.Record.Segment.TrackID]
	if track == nil || intent.Record.Segment.ArchiveOrdinal > track.MediaHighWater {
		return ErrShardedArchiveInvalid
	}
	pageNo, offset := mediaPageSlot(intent.Record.Segment.ArchiveOrdinal)
	page, err := s.loadV2MediaPage(ctx, id, intent.Record.Segment.TrackID, false, pageNo)
	if err != nil || int(offset) >= len(page.Entries) {
		if err != nil {
			return err
		}
		return ErrShardedArchiveInvalid
	}
	existing := page.Entries[offset]
	if !sameV2Media(existing, intent.Record) && !sameV2PromotionBase(existing, intent.Record) {
		return ErrShardedArchiveConflict
	}
	desired := cloneRecordingHeader(intent.Header)
	mergeV2HighWater(desired, current)
	if current.ShardedArchive.ClaimReconcilePending {
		desired.ShardedArchive.ClaimReconcilePending = true
	}
	mergeUint64(&desired.Tracks[intent.Record.Segment.TrackID].LiveSlotHighWater, intent.Record.Segment.LivePresentationOrdinal)
	if err := reconcileV2LivePresentation(desired.Tracks[intent.Record.Segment.TrackID], intent.Record.Segment); err != nil {
		return err
	}
	if err := s.applyV2LivePromotion(ctx, id, existing, intent.Record, desired.Tracks[intent.Record.Segment.TrackID]); err != nil {
		return err
	}
	if err := s.saveHeaderLocked(ctx, desired, current); err != nil {
		return err
	}
	intent.Pending = false
	intent.Record = V2MediaRecord{}
	intent.Header = nil
	return s.saveV2JSON(ctx, id, v2LivePromotionIntentPath, intent, archiveRootMaxBytes)
}

// AppendShardedMedia is a convenience for storage tests and callers that do
// not need to atomically publish revision/live state. Production acquisition
// should use PublishShardedMedia.
func (s *Store) AppendShardedMedia(ctx context.Context, id string, record V2MediaRecord) error {
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	return s.PublishShardedMedia(ctx, header, record)
}

// LookupShardedMediaByCoordinate reads a selected v2 media/init record by
// source coordinate. Root high-water is pinned for the complete lookup.
func (s *Store) LookupShardedMediaByCoordinate(ctx context.Context, id string, coordinate archiveindex.Coordinate) (V2MediaRecord, error) {
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return V2MediaRecord{}, err
	}
	return s.lookupCoordinateLocked(ctx, header, coordinate)
}

func (s *Store) LookupShardedMediaByArchiveOrdinal(ctx context.Context, id, trackID string, ordinal uint64) (V2MediaRecord, error) {
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return V2MediaRecord{}, err
	}
	track := header.Tracks[trackID]
	if track == nil || ordinal == 0 || ordinal > track.MediaHighWater {
		return V2MediaRecord{}, ErrNotFound
	}
	page, offset := mediaPageSlot(ordinal)
	pageData, err := s.loadV2MediaPage(ctx, id, trackID, false, page)
	if err != nil {
		return V2MediaRecord{}, err
	}
	if int(offset) >= len(pageData.Entries) {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	record := pageData.Entries[offset]
	if record.Segment.IsInit || record.Segment.ArchiveOrdinal != ordinal || record.Segment.TrackID != trackID {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	return record, nil
}

// LookupShardedMediaByID resolves media IDs by bounded ordinal lookup and init
// IDs through the direct ID pointer.
func (s *Store) LookupShardedMediaByID(ctx context.Context, id, segmentID string) (V2MediaRecord, error) {
	if match := mediaSegmentID.FindStringSubmatch(segmentID); len(match) == 2 {
		ordinal, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil || ordinal == 0 {
			return V2MediaRecord{}, ErrShardedArchiveInvalid
		}
		header, err := s.LoadRecordingHeader(ctx, id)
		if err != nil {
			return V2MediaRecord{}, err
		}
		for trackID, track := range header.Tracks {
			if track != nil && ordinal <= track.MediaHighWater {
				record, lookupErr := s.LookupShardedMediaByArchiveOrdinal(ctx, id, trackID, ordinal)
				if lookupErr == nil && record.Segment.ID == segmentID {
					return record, nil
				}
			}
		}
		return V2MediaRecord{}, ErrNotFound
	}
	if !validV2OpaqueID(segmentID) {
		return V2MediaRecord{}, ErrNotFound
	}
	var pointer v2IndexPointer
	if err := s.loadV2JSON(ctx, id, v2IDPath(segmentID), archiveShardMaxBytes, &pointer); err != nil {
		return V2MediaRecord{}, err
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return V2MediaRecord{}, err
	}
	track := header.Tracks[pointer.TrackID]
	if track == nil || pointer.Kind != archiveindex.ObjectInit || pointer.IndexOrdinal > track.InitCount {
		return V2MediaRecord{}, ErrNotFound
	}
	return s.readPointerRecord(ctx, id, pointer)
}

// IterateShardedMedia visits visible media in append-only ArchiveOrdinal order.
func (s *Store) IterateShardedMedia(ctx context.Context, id, trackID string, visit func(V2MediaRecord) error) error {
	return s.iterateArchiveOrdinal(ctx, id, trackID, false, visit)
}

// IterateShardedInitSegments visits visible initialization records in their
// bounded init-slot order.
func (s *Store) IterateShardedInitSegments(ctx context.Context, id, trackID string, visit func(V2MediaRecord) error) error {
	return s.iterateArchiveOrdinal(ctx, id, trackID, true, visit)
}

// IterateShardedTimeline visits media in source-coordinate playback order and
// derives dense TimelineOrdinal values in the callback copy only.
func (s *Store) IterateShardedTimeline(ctx context.Context, id, trackID string, visit func(V2MediaRecord) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	if header.Tracks[trackID] == nil {
		return ErrNotFound
	}
	ordinal := uint64(0)
	cache := newV2MediaPageCache(s, ctx, id, trackID, 16)
	return s.iterateV2TimelineProjection(ctx, id, trackID, func(entry v2TimelineEntry, segment domain.Segment) error {
		if entry.ArchiveOrdinal > header.Tracks[trackID].MediaHighWater {
			return nil
		}
		record, err := cache.record(entry.ArchiveOrdinal)
		if err != nil {
			return err
		}
		if !timelineProjectionMatches(entry, record.Segment) {
			return ErrShardedArchiveInvalid
		}
		ordinal++
		record.Segment.TimelineOrdinal = ordinal
		return visit(record)
	})
}

// IterateShardedTimelineProjection visits the bounded playback fields stored
// in ordered timeline pages. VOD playlist generation uses this path and does
// not open media pages for every segment.
func (s *Store) IterateShardedTimelineProjection(ctx context.Context, id, trackID string, visit func(V2MediaRecord) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	track := header.Tracks[trackID]
	if track == nil {
		return ErrNotFound
	}
	ordinal := uint64(0)
	return s.iterateV2TimelineProjection(ctx, id, trackID, func(entry v2TimelineEntry, segment domain.Segment) error {
		if entry.ArchiveOrdinal > track.MediaHighWater {
			return nil
		}
		ordinal++
		segment.TimelineOrdinal = ordinal
		return visit(V2MediaRecord{Coordinate: entry.Coordinate, Segment: segment, IndexOrdinal: entry.ArchiveOrdinal})
	})
}

func (s *Store) iterateV2TimelineProjection(ctx context.Context, id, trackID string, visit func(v2TimelineEntry, domain.Segment) error) error {
	prefix := v2TimelineTrackPrefix(trackID, archiveindex.ObjectMedia)
	return s.listV2(ctx, id, prefix, func(relative string) error {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		var page v2TimelinePage
		if err := s.loadV2JSON(ctx, id, trimJSONSuffix(relative), archiveShardMaxBytes, &page); err != nil {
			return err
		}
		if page.Version != 1 || page.TrackID != trackID || page.Kind != archiveindex.ObjectMedia || len(page.Entries) > mediaShardMaxEntries {
			return ErrShardedArchiveInvalid
		}
		if len(page.Entries) == 0 {
			return ErrShardedArchiveInvalid
		}
		if err := validateV2TimelinePage(page, trackID, page.Entries[0].Coordinate); err != nil {
			return err
		}
		for _, entry := range page.Entries {
			segment := segmentFromV2TimelineEntry(entry)
			if err := visit(entry, segment); err != nil {
				return err
			}
		}
		return nil
	})
}

func segmentFromV2TimelineEntry(entry v2TimelineEntry) domain.Segment {
	coordinate := entry.Coordinate
	return domain.Segment{
		ID: entry.ID, TrackID: coordinate.TrackID, Sequence: coordinate.Sequence,
		SourceEpoch: coordinate.SourceEpoch, DiscontinuitySequence: coordinate.DiscontinuitySequence,
		ArchiveOrdinal: entry.ArchiveOrdinal, Duration: entry.Duration,
		ProgramDateTime: entry.ProgramDateTime, InitSegmentID: entry.InitSegmentID,
		Discontinuity: entry.Discontinuity, ByteRange: entry.ByteRange,
		LivePresentationOrdinal:   entry.LivePresentationOrdinal,
		LiveDiscontinuity:         entry.LiveDiscontinuity,
		LiveDiscontinuitySequence: entry.LiveDiscontinuitySeq,
	}
}

func timelineProjectionMatches(entry v2TimelineEntry, segment domain.Segment) bool {
	record := V2MediaRecord{Coordinate: entry.Coordinate, Segment: segment}
	if entry.Coordinate.Kind == archiveindex.ObjectInit {
		record.IndexOrdinal = entry.InitOrdinal
	}
	expected, err := timelineEntryForRecord(record)
	if err != nil {
		return false
	}
	a, errA := json.Marshal(expected)
	b, errB := json.Marshal(entry)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

type v2MediaPageCache struct {
	store    *Store
	ctx      context.Context
	id       string
	trackID  string
	capacity int
	pages    map[uint64]v2MediaPage
	order    []uint64
}

func newV2MediaPageCache(store *Store, ctx context.Context, id, trackID string, capacity int) *v2MediaPageCache {
	if capacity < 1 {
		capacity = 1
	}
	return &v2MediaPageCache{store: store, ctx: ctx, id: id, trackID: trackID, capacity: capacity, pages: make(map[uint64]v2MediaPage, capacity)}
}

func (c *v2MediaPageCache) record(ordinal uint64) (V2MediaRecord, error) {
	if ordinal == 0 {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	pageNo, offset := mediaPageSlot(ordinal)
	page, ok := c.pages[pageNo]
	if !ok {
		var err error
		page, err = c.store.loadV2MediaPage(c.ctx, c.id, c.trackID, false, pageNo)
		if err != nil {
			return V2MediaRecord{}, err
		}
		if len(c.pages) >= c.capacity {
			delete(c.pages, c.order[0])
			c.order = c.order[1:]
		}
		c.pages[pageNo] = page
		c.order = append(c.order, pageNo)
	}
	if int(offset) >= len(page.Entries) {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	record := page.Entries[offset]
	if record.Segment.ArchiveOrdinal != ordinal || record.Segment.TrackID != c.trackID || record.Segment.IsInit {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	return record, nil
}

func (s *Store) IterateShardedLiveTail(ctx context.Context, id, trackID string, limit int, visit func(V2MediaRecord) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	return s.IterateShardedLiveSlots(ctx, id, trackID, limit, func(slot V2LiveSlot) error {
		if slot.Segment == nil {
			return nil
		}
		return visit(V2MediaRecord{Coordinate: archiveindex.Coordinate{
			TrackID: slot.Segment.TrackID, SourceEpoch: slot.Segment.SourceEpoch,
			DiscontinuitySequence: slot.Segment.DiscontinuitySequence,
			Sequence:              slot.Segment.Sequence, Kind: archiveindex.ObjectMedia,
		}, Segment: *slot.Segment, IndexOrdinal: slot.Segment.ArchiveOrdinal})
	})
}

// LookupShardedLiveSlot reads one bounded presentation slot from the durable
// live index. It is intended for cache reconciliation of a known unavailable
// slot; normal playlist generation should use the in-memory live tail.
func (s *Store) LookupShardedLiveSlot(ctx context.Context, id, trackID string, ordinal uint64) (V2LiveSlot, error) {
	if err := checkV2Context(ctx); err != nil {
		return V2LiveSlot{}, err
	}
	if ordinal == 0 || trackID == "" {
		return V2LiveSlot{}, ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return V2LiveSlot{}, err
	}
	track := header.Tracks[trackID]
	if track == nil || ordinal > track.LiveSlotHighWater {
		return V2LiveSlot{}, ErrNotFound
	}
	entry, err := s.loadLiveEntry(ctx, id, trackID, ordinal)
	if err != nil {
		return V2LiveSlot{}, err
	}
	if entry.PresentationOrdinal != ordinal || entry.Reserved {
		return V2LiveSlot{}, ErrNotFound
	}
	slot := V2LiveSlot{Ordinal: ordinal}
	if entry.Gap != nil {
		gap := *entry.Gap
		slot.Gap = &gap
		return slot, nil
	}
	if entry.ArchiveOrdinal == 0 || entry.Coordinate.TrackID != trackID || entry.Coordinate.Kind != archiveindex.ObjectMedia {
		return V2LiveSlot{}, ErrShardedArchiveInvalid
	}
	record, err := s.lookupCoordinateLocked(ctx, header, entry.Coordinate)
	if err != nil {
		return V2LiveSlot{}, err
	}
	if record.Segment.ArchiveOrdinal != entry.ArchiveOrdinal || record.Segment.LivePresentationOrdinal != ordinal {
		return V2LiveSlot{}, ErrShardedArchiveInvalid
	}
	segment := record.Segment
	slot.Segment = &segment
	return slot, nil
}

// IterateShardedLiveSlots returns bounded live presentation slots in ordinal
// order. Missing media remains an explicit gap, not an omitted position.
func (s *Store) IterateShardedLiveSlots(ctx context.Context, id, trackID string, limit int, visit func(V2LiveSlot) error) error {
	if visit == nil || limit < 1 || limit > archiveLiveTailMax {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	track := header.Tracks[trackID]
	if track == nil || track.LivePresentation == nil {
		return ErrNotFound
	}
	next := track.LivePresentation.NextOrdinal
	if next <= 1 {
		return nil
	}
	end := next - 1
	if track.LiveSlotHighWater < end {
		end = track.LiveSlotHighWater
	}
	if end == 0 {
		return nil
	}
	start := uint64(1)
	if end >= uint64(limit) {
		start = end - uint64(limit) + 1
	}
	if track.LivePresentation.FirstPresentationOrdinal > start {
		start = track.LivePresentation.FirstPresentationOrdinal
	}
	entries := make([]v2LiveIndexEntry, 0, limit)
	ordinals := make([]uint64, 0, limit)
	for ordinal := start; ordinal <= end; ordinal++ {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		entry, err := s.loadLiveEntry(ctx, id, trackID, ordinal)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrShardedArchiveInvalid
			}
			return err
		}
		entries = append(entries, entry)
		ordinals = append(ordinals, ordinal)
	}
	started := false
	for i, entry := range entries {
		ordinal := ordinals[i]
		if entry.Reserved {
			// Leading reservations can be trimmed from the playlist base. Once a
			// visible slot was emitted, a later reservation ends this playlist so
			// HLS implicit sequence numbering never compresses the missing slot.
			if started {
				break
			}
			continue
		}
		started = true
		slot := V2LiveSlot{Ordinal: ordinal}
		if entry.Gap != nil {
			gap := *entry.Gap
			slot.Gap = &gap
		} else {
			record, err := s.LookupShardedMediaByCoordinate(ctx, id, entry.Coordinate)
			if err != nil {
				return err
			}
			if record.Segment.ArchiveOrdinal != entry.ArchiveOrdinal || record.Segment.LivePresentationOrdinal != entry.PresentationOrdinal {
				return ErrShardedArchiveInvalid
			}
			segment := record.Segment
			slot.Segment = &segment
		}
		if err := visit(slot); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) AppendShardedMetadata(ctx context.Context, id string, revision domain.MetadataRevision) error {
	if err := domain.ValidateMetadataTimeline([]domain.MetadataRevision{revision}); err != nil {
		return fmt.Errorf("validate sharded metadata revision: %w", ErrShardedArchiveInvalid)
	}
	return s.appendPagedV2(ctx, id, "metadata", revision, func(header *domain.Recording) *uint64 {
		return &header.ShardedArchive.MetadataRevisionCount
	})
}

func (s *Store) IterateShardedMetadata(ctx context.Context, id string, visit func(domain.MetadataRevision) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	return iteratePagedV2(s, ctx, id, "metadata", header.ShardedArchive.MetadataRevisionCount, func(revision domain.MetadataRevision) error {
		if err := domain.ValidateMetadataTimeline([]domain.MetadataRevision{revision}); err != nil {
			return ErrShardedArchiveInvalid
		}
		return visit(revision)
	})
}

// LoadLatestShardedMetadata returns the newest metadata revision using only
// the bounded root and final metadata page. It avoids replaying an unbounded
// metadata history during Engine adoption or restart.
func (s *Store) LoadLatestShardedMetadata(ctx context.Context, id string) (*domain.MetadataRevision, error) {
	if err := checkV2Context(ctx); err != nil {
		return nil, err
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return nil, err
	}
	count := header.ShardedArchive.MetadataRevisionCount
	if count == 0 {
		return nil, nil
	}
	pageNumber, offset := pageSlotWithEntries(count, metadataShardMaxEntries)
	var page v2RevisionPage[json.RawMessage]
	if err := s.loadV2JSON(ctx, id, fmt.Sprintf("archive/v2/metadata/%020d", pageNumber), archiveShardMaxBytes, &page); err != nil {
		return nil, err
	}
	if page.Version != 1 || page.Number != pageNumber || uint64(len(page.Entries)) > metadataShardMaxEntries || int(offset) >= len(page.Entries) {
		return nil, ErrShardedArchiveInvalid
	}
	var latest domain.MetadataRevision
	if err := json.Unmarshal(page.Entries[offset], &latest); err != nil || domain.ValidateMetadataTimeline([]domain.MetadataRevision{latest}) != nil {
		return nil, ErrShardedArchiveInvalid
	}
	return &latest, nil
}

func (s *Store) AppendShardedManifest(ctx context.Context, id string, snapshot domain.ManifestSnapshot) error {
	if err := validateV2Manifest(snapshot); err != nil {
		return fmt.Errorf("validate sharded manifest snapshot: %w", ErrShardedArchiveInvalid)
	}
	return s.appendPagedV2(ctx, id, "manifests", snapshot, func(header *domain.Recording) *uint64 {
		return &header.ShardedArchive.ManifestSnapshotCount
	})
}

func (s *Store) IterateShardedManifests(ctx context.Context, id string, visit func(domain.ManifestSnapshot) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	return iteratePagedV2(s, ctx, id, "manifests", header.ShardedArchive.ManifestSnapshotCount, func(snapshot domain.ManifestSnapshot) error {
		if err := validateV2Manifest(snapshot); err != nil {
			return err
		}
		return visit(snapshot)
	})
}

func (s *Store) AppendShardedGap(ctx context.Context, id string, gap domain.Gap) error {
	_, err := s.AppendShardedGapWithRevision(ctx, id, gap)
	return err
}

func (s *Store) AppendShardedGapWithRevision(ctx context.Context, id string, gap domain.Gap) (ShardedRevisionResult, error) {
	if err := checkV2Context(ctx); err != nil {
		return ShardedRevisionResult{}, err
	}
	if gap.TrackID == "" || gap.ToSequence < gap.FromSequence || gap.DetectedAt.IsZero() {
		return ShardedRevisionResult{}, fmt.Errorf("invalid sharded gap observation: %w", ErrShardedArchiveInvalid)
	}
	result, err := s.appendObservation(ctx, id, "gaps", v2Observation{Version: 1, Gap: &gap})
	if err != nil {
		return ShardedRevisionResult{}, fmt.Errorf("append sharded gap observation: %w", err)
	}
	return result, nil
}

func (s *Store) IterateShardedGaps(ctx context.Context, id string, visit func(domain.Gap) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	return s.iterateObservations(ctx, id, "gaps", func(observation v2Observation) error {
		if observation.Gap == nil {
			return ErrShardedArchiveInvalid
		}
		return visit(*observation.Gap)
	})
}

func (s *Store) AppendShardedCoverage(ctx context.Context, id string, coverage archiveindex.Coverage) error {
	_, err := s.AppendShardedCoverageWithRevision(ctx, id, coverage)
	return err
}

func (s *Store) AppendShardedCoverageWithRevision(ctx context.Context, id string, coverage archiveindex.Coverage) (ShardedRevisionResult, error) {
	if err := checkV2Context(ctx); err != nil {
		return ShardedRevisionResult{}, err
	}
	if coverage.TrackID == "" || coverage.ToSequence < coverage.FromSequence || coverage.ObservedAt.IsZero() {
		return ShardedRevisionResult{}, ErrShardedArchiveInvalid
	}
	return s.appendObservation(ctx, id, "coverage", v2Observation{Version: 1, Coverage: &coverage})
}

func (s *Store) IterateShardedCoverage(ctx context.Context, id string, visit func(archiveindex.Coverage) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	return s.iterateObservations(ctx, id, "coverage", func(observation v2Observation) error {
		if observation.Coverage == nil {
			return ErrShardedArchiveInvalid
		}
		return visit(*observation.Coverage)
	})
}

func (s *Store) SaveShardedClaimSet(ctx context.Context, id string, set V2ClaimSet) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if err := validateV2ClaimSet(set); err != nil {
		return fmt.Errorf("validate sharded claim set: %w", err)
	}
	if set.State == "" {
		set.State = archiveindex.CoverageUnknown
	}
	if set.CountedClaims != 0 {
		return fmt.Errorf("incoming claim set has persisted accounting marker: %w", ErrShardedArchiveInvalid)
	}
	lock := s.v2Lock(id)
	lock.Lock()
	defer lock.Unlock()
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return fmt.Errorf("load root before sharded claim publication: %w", err)
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || header.ShardedArchive == nil {
		return ErrShardedArchiveUnavailable
	}
	if header.ArchiveSealed {
		return ErrArchiveSealed
	}
	_, visibleErr := s.lookupCoordinateLocked(ctx, header, set.Coordinate)
	mediaVisible := visibleErr == nil
	if visibleErr != nil && !errors.Is(visibleErr, ErrNotFound) {
		return fmt.Errorf("resolve sharded claim media visibility: %w", visibleErr)
	}
	existing, pagePath, page, index, err := s.locateV2ClaimSet(ctx, id, set.SegmentID)
	if err == nil {
		if existing.Version != 1 || existing.Set.SegmentID != set.SegmentID || existing.Set.Coordinate != set.Coordinate {
			return fmt.Errorf("existing sharded claim identity is invalid: %w", ErrShardedArchiveInvalid)
		}
		if mediaVisible {
			if err := s.ensureClaimCountLocked(ctx, id, &existing); err != nil {
				return fmt.Errorf("account existing sharded claim set: %w", err)
			}
		}
		set.CountedClaims = existing.Set.CountedClaims
		if err := mergeV2ClaimSet(&existing.Set, set, !mediaVisible); err != nil {
			return fmt.Errorf("merge sharded claim set: %w", err)
		}
		existing.Set.CountedClaims = set.CountedClaims
		set = existing.Set
	} else if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("load existing sharded claim set: %w", err)
	}
	if mediaVisible {
		if err := s.setClaimReconcilePendingLocked(ctx, id, true); err != nil {
			return fmt.Errorf("mark sharded claim accounting pending: %w", err)
		}
	}
	doc := v2ClaimDocument{Version: 1, Set: set}
	if err := s.saveV2ClaimDocument(ctx, id, doc, pagePath, page, index); err != nil {
		return fmt.Errorf("publish sharded claim sidecar: %w", err)
	}
	if mediaVisible {
		if err := s.ensureClaimCountLocked(ctx, id, &doc); err != nil {
			return fmt.Errorf("account sharded claim set: %w", err)
		}
	}
	return nil
}

var errV2ClaimPageFound = errors.New("claim page entry found")

func v2ClaimBucket(segmentID string) uint16 {
	digest := sha256.Sum256([]byte(segmentID))
	return uint16(digest[0])<<4 | uint16(digest[1]>>4)
}

func (s *Store) locateV2ClaimSet(ctx context.Context, id, segmentID string) (v2ClaimDocument, string, v2ClaimPage, int, error) {
	if !validV2OpaqueID(segmentID) {
		return v2ClaimDocument{}, "", v2ClaimPage{}, -1, ErrNotFound
	}
	bucket := v2ClaimBucket(segmentID)
	prefix := v2ClaimPagePrefix(segmentID)
	var found v2ClaimDocument
	var foundPath string
	var foundPage v2ClaimPage
	foundIndex := -1
	var latestPath string
	var latest v2ClaimPage
	err := s.listV2(ctx, id, prefix, func(relative string) error {
		pagePath := trimJSONSuffix(relative)
		var page v2ClaimPage
		if err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page); err != nil {
			return err
		}
		pageNo, parseErr := strconv.ParseUint(path.Base(pagePath), 10, 64)
		if parseErr != nil || page.Version != 1 || page.Bucket != bucket || page.Number != pageNo || len(page.Entries) == 0 || len(page.Entries) > claimPageMaxEntries {
			return ErrShardedArchiveInvalid
		}
		if relative != v2ClaimPagePath(bucket, pageNo)+".json" {
			return ErrShardedArchiveInvalid
		}
		if err := validateV2ClaimPage(page); err != nil {
			return err
		}
		latestPath, latest = pagePath, page
		for index := range page.Entries {
			if page.Entries[index].SegmentID != segmentID {
				continue
			}
			found = v2ClaimDocument{Version: 1, Set: page.Entries[index]}
			foundPath, foundPage, foundIndex = pagePath, page, index
			return errV2ClaimPageFound
		}
		return nil
	})
	if errors.Is(err, errV2ClaimPageFound) {
		return found, foundPath, foundPage, foundIndex, nil
	}
	if err != nil {
		return v2ClaimDocument{}, "", v2ClaimPage{}, -1, err
	}
	if latestPath == "" {
		latest = v2ClaimPage{Version: 1, Bucket: bucket}
		latestPath = v2ClaimPagePath(bucket, 0)
	}
	return v2ClaimDocument{}, latestPath, latest, -1, ErrNotFound
}

func validateV2ClaimPage(page v2ClaimPage) error {
	if page.Version != 1 || len(page.Entries) == 0 || len(page.Entries) > claimPageMaxEntries {
		return ErrShardedArchiveInvalid
	}
	for index, set := range page.Entries {
		if validateV2ClaimSet(set) != nil || v2ClaimBucket(set.SegmentID) != page.Bucket || index > 0 && page.Entries[index-1].SegmentID >= set.SegmentID {
			return ErrShardedArchiveInvalid
		}
	}
	data, err := json.Marshal(page)
	if err != nil || len(data)+1 > archiveShardMaxBytes {
		return ErrShardedArchiveInvalid
	}
	return nil
}

func (s *Store) saveV2ClaimDocument(ctx context.Context, id string, doc v2ClaimDocument, pagePath string, page v2ClaimPage, index int) error {
	if doc.Version != 1 || validateV2ClaimSet(doc.Set) != nil {
		return ErrShardedArchiveInvalid
	}
	if index >= 0 {
		if index >= len(page.Entries) || page.Entries[index].SegmentID != doc.Set.SegmentID {
			return ErrShardedArchiveInvalid
		}
		page.Entries[index] = doc.Set
	} else {
		candidate := page
		candidate.Entries = append(append([]V2ClaimSet(nil), page.Entries...), doc.Set)
		sort.Slice(candidate.Entries, func(i, j int) bool { return candidate.Entries[i].SegmentID < candidate.Entries[j].SegmentID })
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return err
		}
		if len(candidate.Entries) <= claimPageMaxEntries && len(encoded)+1 <= archiveShardMaxBytes {
			page = candidate
		} else {
			if len(page.Entries) == 0 && len(encoded)+1 > archiveShardMaxBytes {
				return ErrShardedArchiveInvalid
			}
			page = v2ClaimPage{Version: 1, Bucket: v2ClaimBucket(doc.Set.SegmentID), Number: page.Number + 1, Entries: []V2ClaimSet{doc.Set}}
			pagePath = v2ClaimPagePath(page.Bucket, page.Number)
		}
	}
	if err := validateV2ClaimPage(page); err != nil {
		return err
	}
	return s.saveV2JSON(ctx, id, pagePath, page, archiveShardMaxBytes)
}

func (s *Store) ensureVisibleClaimCount(ctx context.Context, id string, coordinate archiveindex.Coordinate) error {
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		return ErrShardedArchiveInvalid
	}
	doc, _, _, _, err := s.locateV2ClaimSet(ctx, id, segmentID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if doc.Version != 1 || validateV2ClaimSet(doc.Set) != nil || doc.Set.SegmentID != segmentID {
		return ErrShardedArchiveInvalid
	}
	return s.ensureClaimCountLocked(ctx, id, &doc)
}

func (s *Store) shardedClaimCountPending(ctx context.Context, id string, coordinate archiveindex.Coordinate) (bool, error) {
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		return false, ErrShardedArchiveInvalid
	}
	doc, _, _, _, err := s.locateV2ClaimSet(ctx, id, segmentID)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if doc.Version != 1 || doc.Set.SegmentID != segmentID || validateV2ClaimSet(doc.Set) != nil {
		return false, ErrShardedArchiveInvalid
	}
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return false, err
	}
	record, err := s.lookupCoordinateLocked(ctx, header, doc.Set.Coordinate)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return doc.Set.CountedClaims < uint64(len(doc.Set.Claims)), nil
		}
		return false, err
	}
	want := uint64(len(doc.Set.Claims))
	if record.SelectedClaim != nil && claimSetContains(doc.Set, record.SelectedClaim.ID) {
		want--
	}
	return doc.Set.CountedClaims < want, nil
}

// ensureClaimCountLocked commits the bounded root claim count through a
// per-coordinate intent. The intent makes a root write with uncertain result
// idempotent across process restart; the claim sidecar is durable first.
func (s *Store) ensureClaimCountLocked(ctx context.Context, id string, doc *v2ClaimDocument) error {
	if doc == nil || doc.Set.CountedClaims > uint64(len(doc.Set.Claims)) {
		return ErrShardedArchiveInvalid
	}
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return fmt.Errorf("load sharded archive root for observation: %w", err)
	}
	record, err := s.lookupCoordinateLocked(ctx, header, doc.Set.Coordinate)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	wanted := uint64(len(doc.Set.Claims))
	if record.SelectedClaim != nil && claimSetContains(doc.Set, record.SelectedClaim.ID) {
		wanted--
	}
	if doc.Set.CountedClaims > wanted {
		return ErrShardedArchiveInvalid
	}
	if doc.Set.CountedClaims == wanted {
		if header.ShardedArchive.ClaimReconcilePending {
			unresolved, err := s.hasUnresolvedClaimCountIntent(ctx, id)
			if err != nil {
				return err
			}
			if unresolved {
				return s.reconcilePendingClaimCountLocked(ctx, id, header)
			}
			if err := s.clearClaimCountIntentLocked(ctx, id); err != nil {
				return err
			}
			return s.setClaimReconcilePendingLocked(ctx, id, false)
		}
		return nil
	}
	if !header.ShardedArchive.ClaimReconcilePending {
		header.ShardedArchive.ClaimReconcilePending = true
		if err := s.saveHeaderLocked(ctx, header, header); err != nil {
			return fmt.Errorf("publish pending sharded claim accounting: %w", err)
		}
	}
	intentPath := v2ClaimCountIntentPath()
	delta := wanted - doc.Set.CountedClaims
	var intent v2ClaimCountIntent
	err = s.loadV2JSON(ctx, id, intentPath, archiveShardMaxBytes, &intent)
	if err == nil {
		if emptyV2ClaimCountIntent(intent) {
			err = ErrNotFound
		} else if intent.Version != 1 || !validV2OpaqueID(intent.SegmentID) || intent.Target < intent.Base || intent.ClaimSetCount == 0 {
			return ErrShardedArchiveInvalid
		} else if intent.SegmentID == doc.Set.SegmentID && intent.ClaimSetCount == wanted && intent.Target-intent.Base == delta && (header.ShardedArchive.ClaimCount == intent.Base || header.ShardedArchive.ClaimCount == intent.Target) {
			// Retry the exact operation with its stable count interval.
		} else {
			unresolved, unresolvedErr := s.hasUnresolvedClaimCountIntent(ctx, id)
			if unresolvedErr != nil {
				return unresolvedErr
			}
			if unresolved {
				return s.reconcilePendingClaimCountLocked(ctx, id, header)
			}
			err = ErrNotFound
		}
	} else if errors.Is(err, ErrNotFound) {
		err = ErrNotFound
	} else {
		return err
	}
	if errors.Is(err, ErrNotFound) {
		if ^uint64(0)-header.ShardedArchive.ClaimCount < delta {
			return ErrShardedArchiveInvalid
		}
		intent = v2ClaimCountIntent{Version: 1, SegmentID: doc.Set.SegmentID, Base: header.ShardedArchive.ClaimCount, Target: header.ShardedArchive.ClaimCount + delta, ClaimSetCount: wanted}
		if err := s.saveV2JSON(ctx, id, intentPath, intent, archiveShardMaxBytes); err != nil {
			return err
		}
	}
	if header.ShardedArchive.ClaimCount == intent.Base {
		header.ShardedArchive.ClaimCount = intent.Target
		if err := advanceV2ArchiveRevision(header); err != nil {
			return err
		}
		if err := s.saveHeaderLocked(ctx, header, header); err != nil {
			return err
		}
	} else if header.ShardedArchive.ClaimCount != intent.Target {
		return ErrShardedArchiveConflict
	}
	doc.Set.CountedClaims = wanted
	if err := s.saveV2ClaimDocumentByID(ctx, id, *doc); err != nil {
		return fmt.Errorf("publish sharded claim accounting marker: %w", err)
	}
	if err := s.clearClaimCountIntentLocked(ctx, id); err != nil {
		return err
	}
	return s.setClaimReconcilePendingLocked(ctx, id, false)
}

// hasUnresolvedClaimCountIntent reports whether the fixed per-recording count
// intent still refers to a claim sidecar whose durable CountedClaims marker is
// behind the intent. The marker is written only after the root high-water, so
// a complete marker makes the old intent safe to replace.
func (s *Store) hasUnresolvedClaimCountIntent(ctx context.Context, id string) (bool, error) {
	var intent v2ClaimCountIntent
	err := s.loadV2JSON(ctx, id, v2ClaimCountIntentPath(), archiveShardMaxBytes, &intent)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if emptyV2ClaimCountIntent(intent) {
		return false, nil
	}
	if intent.Version != 1 || !validV2OpaqueID(intent.SegmentID) || intent.ClaimSetCount == 0 || intent.Target < intent.Base {
		return false, ErrShardedArchiveInvalid
	}
	doc, _, _, _, err := s.locateV2ClaimSet(ctx, id, intent.SegmentID)
	if err != nil {
		return false, err
	}
	if doc.Version != 1 || doc.Set.SegmentID != intent.SegmentID || validateV2ClaimSet(doc.Set) != nil {
		return false, ErrShardedArchiveInvalid
	}
	return doc.Set.CountedClaims < intent.ClaimSetCount, nil
}

func (s *Store) clearClaimCountIntentLocked(ctx context.Context, id string) error {
	return s.saveV2JSON(ctx, id, v2ClaimCountIntentPath(), v2ClaimCountIntent{Version: 1}, archiveShardMaxBytes)
}

func (s *Store) saveV2ClaimDocumentByID(ctx context.Context, id string, doc v2ClaimDocument) error {
	_, pagePath, page, index, err := s.locateV2ClaimSet(ctx, id, doc.Set.SegmentID)
	if err != nil {
		return err
	}
	return s.saveV2ClaimDocument(ctx, id, doc, pagePath, page, index)
}

// reconcilePendingClaimCountLocked is the bounded recovery path for a process
// that stopped between the global claim-count root and a claim-set marker.
// It is deliberately used only when the fixed single-operation intent proves
// that a prior operation is incomplete; normal claim admission stays O(1).
func (s *Store) reconcilePendingClaimCountLocked(ctx context.Context, id string, header *domain.Recording) error {
	claimCount, _, err := s.reconcileClaimCount(ctx, id, header)
	if err != nil {
		return err
	}
	header.ShardedArchive.ClaimCount = claimCount
	if err := s.clearClaimCountIntentLocked(ctx, id); err != nil {
		return err
	}
	header.ShardedArchive.ClaimReconcilePending = false
	return s.saveHeaderLocked(ctx, header, header)
}

func (s *Store) setClaimReconcilePendingLocked(ctx context.Context, id string, pending bool) error {
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || header.ShardedArchive == nil {
		return ErrShardedArchiveUnavailable
	}
	if header.ArchiveSealed {
		return ErrArchiveSealed
	}
	if header.ShardedArchive.ClaimReconcilePending == pending {
		return nil
	}
	header.ShardedArchive.ClaimReconcilePending = pending
	return s.saveHeaderLocked(ctx, header, header)
}

func (s *Store) LoadShardedClaimSet(ctx context.Context, id, segmentID string) (V2ClaimSet, error) {
	if _, err := s.LoadRecordingHeader(ctx, id); err != nil {
		return V2ClaimSet{}, err
	}
	if !validV2OpaqueID(segmentID) {
		return V2ClaimSet{}, ErrNotFound
	}
	doc, _, _, _, err := s.locateV2ClaimSet(ctx, id, segmentID)
	if err != nil {
		return V2ClaimSet{}, err
	}
	if doc.Version != 1 || doc.Set.SegmentID != segmentID || validateV2ClaimSet(doc.Set) != nil {
		return V2ClaimSet{}, ErrShardedArchiveInvalid
	}
	return doc.Set, nil
}

// LoadShardedClaimSetByCoordinate returns selected provenance stored inline
// with the media record, merged with any bounded supplemental claim page.
func (s *Store) LoadShardedClaimSetByCoordinate(ctx context.Context, id string, coordinate archiveindex.Coordinate) (V2ClaimSet, error) {
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return V2ClaimSet{}, err
	}
	record, recordErr := s.lookupCoordinateLocked(ctx, header, coordinate)
	if recordErr != nil && !errors.Is(recordErr, ErrNotFound) {
		return V2ClaimSet{}, recordErr
	}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		return V2ClaimSet{}, ErrShardedArchiveInvalid
	}
	supplemental, _, _, _, supplementalErr := s.locateV2ClaimSet(ctx, id, segmentID)
	if supplementalErr != nil && !errors.Is(supplementalErr, ErrNotFound) {
		return V2ClaimSet{}, supplementalErr
	}
	if recordErr != nil {
		if supplementalErr == nil {
			return supplemental.Set, nil
		}
		return V2ClaimSet{}, ErrNotFound
	}
	if record.SelectedClaim == nil {
		if supplementalErr == nil {
			return supplemental.Set, nil
		}
		return V2ClaimSet{}, ErrNotFound
	}
	base := V2ClaimSet{
		SegmentID: segmentID, Coordinate: record.Coordinate,
		SelectedClaimID: record.SelectedClaim.ID, State: record.ClaimState,
		Claims: []archiveindex.Claim{*record.SelectedClaim},
	}
	if supplementalErr == nil {
		if supplemental.Set.SelectedClaimID != base.SelectedClaimID || supplemental.Set.State != base.State {
			return V2ClaimSet{}, ErrShardedArchiveConflict
		}
		if err := mergeV2ClaimSet(&base, supplemental.Set, false); err != nil {
			return V2ClaimSet{}, err
		}
	}
	return base, nil
}

func (s *Store) IterateShardedClaimSets(ctx context.Context, id string, visit func(V2ClaimSet) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	return s.listV2(ctx, id, "archive/v2/claims/", func(relative string) error {
		pagePath := trimJSONSuffix(relative)
		var page v2ClaimPage
		if err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page); err != nil {
			return err
		}
		pageNo, parseErr := strconv.ParseUint(path.Base(pagePath), 10, 64)
		bucketText := path.Base(path.Dir(pagePath))
		bucketNumber, bucketErr := strconv.ParseUint(bucketText, 16, 16)
		if parseErr != nil || bucketErr != nil || page.Number != pageNo || page.Bucket != uint16(bucketNumber) || relative != v2ClaimPagePath(page.Bucket, page.Number)+".json" || validateV2ClaimPage(page) != nil {
			return ErrShardedArchiveInvalid
		}
		for _, set := range page.Entries {
			if _, err := s.lookupCoordinateLocked(ctx, header, set.Coordinate); err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return err
			}
			if err := visit(set); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReconcileShardedArchive completes consecutive media slots durably published
// before a root high-water write. It verifies payload metadata and rebuilds
// deterministic indexes before making each slot visible.
func (s *Store) ReconcileShardedArchive(ctx context.Context, id string) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	lock := s.v2Lock(id)
	lock.Lock()
	defer lock.Unlock()
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || validateV2Header(header) != nil {
		return ErrShardedArchiveUnavailable
	}
	if header.ArchiveSealed {
		return ErrArchiveSealed
	}
	if err := s.reconcileLivePromotionIntentLocked(ctx, id); err != nil {
		return fmt.Errorf("reconcile live presentation promotion: %w", err)
	}
	header, err = s.loadRecordingContext(ctx, id)
	if err != nil {
		return err
	}
	// The coverage-state index is derived from gap/coverage observations. Do
	// not create its marker during cold recovery when the canonical archive has
	// no such history; the first real observation initializes it on append.
	if header.ShardedArchive.GapCount != 0 || header.ShardedArchive.CoverageObservationCount != 0 {
		if err := s.ensureCoverageStateIndex(ctx, id, header); err != nil {
			return fmt.Errorf("reconcile coverage state index: %w", err)
		}
	}
	changed := false
	for trackID, track := range header.Tracks {
		if track == nil {
			return ErrShardedArchiveInvalid
		}
		for {
			next := track.MediaHighWater + 1
			page, offset := mediaPageSlot(next)
			pageDoc, pageErr := s.loadV2MediaPage(ctx, id, trackID, false, page)
			if errors.Is(pageErr, ErrNotFound) || pageErr == nil && int(offset) >= len(pageDoc.Entries) {
				break
			}
			if pageErr != nil {
				return pageErr
			}
			record := pageDoc.Entries[offset]
			if record.Segment.ArchiveOrdinal != next || record.Segment.TrackID != trackID || record.Segment.IsInit {
				return ErrShardedArchiveInvalid
			}
			if err := advanceV2ReconciledRevisions(header, false); err != nil {
				return err
			}
			if err := s.verifyV2Payload(ctx, id, record.Segment); err != nil {
				return err
			}
			if err := s.publishV2Indexes(ctx, id, record, header); err != nil {
				return err
			}
			if err := advanceV2Summary(header, record); err != nil {
				return err
			}
			if record.SupplementalClaims || record.SelectedClaim == nil {
				claimPending, err := s.shardedClaimCountPending(ctx, id, record.Coordinate)
				if err != nil {
					return err
				}
				if claimPending {
					header.ShardedArchive.ClaimReconcilePending = true
				}
			}
			track = header.Tracks[trackID]
			if err := s.saveHeaderLocked(ctx, header, nil); err != nil {
				return err
			}
			if record.SupplementalClaims || record.SelectedClaim == nil {
				if err := s.ensureVisibleClaimCount(ctx, id, record.Coordinate); err != nil {
					return fmt.Errorf("reconcile sharded media claims: %w", err)
				}
			}
			changed = true
		}
		for {
			next := track.InitCount + 1
			page, offset := mediaPageSlot(next)
			pageDoc, pageErr := s.loadV2MediaPage(ctx, id, trackID, true, page)
			if errors.Is(pageErr, ErrNotFound) || pageErr == nil && int(offset) >= len(pageDoc.Entries) {
				break
			}
			if pageErr != nil {
				return pageErr
			}
			record := pageDoc.Entries[offset]
			if !record.Segment.IsInit || record.Segment.TrackID != trackID || record.IndexOrdinal != 0 && record.IndexOrdinal != next {
				return ErrShardedArchiveInvalid
			}
			if err := advanceV2ReconciledRevisions(header, true); err != nil {
				return err
			}
			if err := s.verifyV2Payload(ctx, id, record.Segment); err != nil {
				return err
			}
			record.IndexOrdinal = next
			if err := s.publishV2Indexes(ctx, id, record, header); err != nil {
				return err
			}
			if err := advanceV2Summary(header, record); err != nil {
				return err
			}
			if record.SupplementalClaims || record.SelectedClaim == nil {
				claimPending, err := s.shardedClaimCountPending(ctx, id, record.Coordinate)
				if err != nil {
					return err
				}
				if claimPending {
					header.ShardedArchive.ClaimReconcilePending = true
				}
			}
			if err := s.saveHeaderLocked(ctx, header, nil); err != nil {
				return err
			}
			if record.SupplementalClaims || record.SelectedClaim == nil {
				if err := s.ensureVisibleClaimCount(ctx, id, record.Coordinate); err != nil {
					return fmt.Errorf("reconcile sharded init claims: %w", err)
				}
			}
			changed = true
		}
	}
	metadataCount, err := reconcilePagedV2(ctx, s, id, "metadata", header.ShardedArchive.MetadataRevisionCount, func(value domain.MetadataRevision) error {
		return domain.ValidateMetadataTimeline([]domain.MetadataRevision{value})
	})
	if err != nil {
		return err
	}
	if metadataCount != header.ShardedArchive.MetadataRevisionCount {
		if err := advanceV2ArchiveRevisionBy(header, metadataCount-header.ShardedArchive.MetadataRevisionCount); err != nil {
			return err
		}
		header.ShardedArchive.MetadataRevisionCount = metadataCount
		changed = true
	}
	manifestCount, err := reconcilePagedV2(ctx, s, id, "manifests", header.ShardedArchive.ManifestSnapshotCount, validateV2Manifest)
	if err != nil {
		return err
	}
	if manifestCount != header.ShardedArchive.ManifestSnapshotCount {
		if err := advanceV2ArchiveRevisionBy(header, manifestCount-header.ShardedArchive.ManifestSnapshotCount); err != nil {
			return err
		}
		header.ShardedArchive.ManifestSnapshotCount = manifestCount
		changed = true
	}
	for _, collection := range []string{"gaps", "coverage"} {
		counter := &header.ShardedArchive.GapCount
		if collection == "coverage" {
			counter = &header.ShardedArchive.CoverageObservationCount
		}
		count, observationChanged, reconcileErr := s.reconcileObservations(ctx, id, collection, *counter, header)
		if reconcileErr != nil {
			return reconcileErr
		}
		if count != *counter || observationChanged {
			*counter = count
			changed = true
		}
	}
	if header.ShardedArchive.ClaimReconcilePending {
		previousClaimCount := header.ShardedArchive.ClaimCount
		claimCount, claimMarkersChanged, err := s.reconcileClaimCount(ctx, id, header)
		if err != nil {
			return err
		}
		if claimCount > previousClaimCount {
			if err := advanceV2ArchiveRevision(header); err != nil {
				return err
			}
		}
		if err := s.clearClaimCountIntentLocked(ctx, id); err != nil {
			return err
		}
		if claimCount != header.ShardedArchive.ClaimCount || claimMarkersChanged {
			header.ShardedArchive.ClaimCount = claimCount
			changed = true
		}
		header.ShardedArchive.ClaimReconcilePending = false
		changed = true
	}
	if !changed {
		return nil
	}
	return s.saveHeaderLocked(ctx, header, nil)
}

func advanceV2ReconciledRevisions(header *domain.Recording, init bool) error {
	if header == nil || header.ArchiveRevision == ^uint64(0) {
		return ErrShardedArchiveInvalid
	}
	header.ArchiveRevision++
	if !init {
		if header.TimelineRevision == ^uint64(0) {
			return ErrShardedArchiveInvalid
		}
		header.TimelineRevision++
	}
	return nil
}

func advanceV2ArchiveRevisionBy(header *domain.Recording, delta uint64) error {
	if delta == 0 {
		return nil
	}
	if header == nil || ^uint64(0)-header.ArchiveRevision < delta {
		return ErrShardedArchiveInvalid
	}
	header.ArchiveRevision += delta
	return nil
}

func (s *Store) reconcileClaimCount(ctx context.Context, id string, header *domain.Recording) (uint64, bool, error) {
	var claimCount uint64
	markersChanged := false
	err := s.listV2(ctx, id, "archive/v2/claims/", func(relative string) error {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		pagePath := trimJSONSuffix(relative)
		var page v2ClaimPage
		if err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page); err != nil {
			return err
		}
		pageNo, parseErr := strconv.ParseUint(path.Base(pagePath), 10, 64)
		bucketNumber, bucketErr := strconv.ParseUint(path.Base(path.Dir(pagePath)), 16, 16)
		if parseErr != nil || bucketErr != nil || page.Number != pageNo || page.Bucket != uint16(bucketNumber) || relative != v2ClaimPagePath(page.Bucket, page.Number)+".json" || validateV2ClaimPage(page) != nil {
			return ErrShardedArchiveInvalid
		}
		pageChanged := false
		for index := range page.Entries {
			set := &page.Entries[index]
			record, lookupErr := s.lookupCoordinateLocked(ctx, header, set.Coordinate)
			if lookupErr != nil {
				if errors.Is(lookupErr, ErrNotFound) {
					continue
				}
				return lookupErr
			}
			count := uint64(len(set.Claims))
			if record.SelectedClaim != nil && claimSetContains(*set, record.SelectedClaim.ID) {
				count--
			}
			if ^uint64(0)-claimCount < count {
				return ErrShardedArchiveInvalid
			}
			claimCount += count
			if set.CountedClaims != count {
				set.CountedClaims = count
				pageChanged = true
			}
		}
		if pageChanged {
			if err := s.saveV2JSON(ctx, id, pagePath, page, archiveShardMaxBytes); err != nil {
				return err
			}
			markersChanged = true
		}
		return nil
	})
	return claimCount, markersChanged, err
}

func reconcilePagedV2[T any](ctx context.Context, store *Store, id, collection string, count uint64, validate func(T) error) (uint64, error) {
	pageEntries := pagedV2MaxEntries(collection)
	for count < ^uint64(0) {
		if err := checkV2Context(ctx); err != nil {
			return count, err
		}
		next := count + 1
		pageNo, offset := pageSlotWithEntries(next, pageEntries)
		var page v2RevisionPage[json.RawMessage]
		err := store.loadV2JSON(ctx, id, fmt.Sprintf("archive/v2/%s/%020d", collection, pageNo), archiveShardMaxBytes, &page)
		if errors.Is(err, ErrNotFound) {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		if page.Version != 1 || page.Number != pageNo || uint64(len(page.Entries)) > pageEntries {
			return count, ErrShardedArchiveInvalid
		}
		if int(offset) >= len(page.Entries) {
			return count, nil
		}
		// A page is the bounded read unit. Validate each consecutive orphan
		// candidate from this one decode instead of re-reading the same page once
		// per revision (which would make restart cost quadratic in page size).
		baseOrdinal := pageNo * pageEntries
		for pageOffset := int(offset); pageOffset < len(page.Entries); pageOffset++ {
			if err := checkV2Context(ctx); err != nil {
				return count, err
			}
			var value T
			if json.Unmarshal(page.Entries[pageOffset], &value) != nil || validate(value) != nil {
				return count, ErrShardedArchiveInvalid
			}
			count = baseOrdinal + uint64(pageOffset) + 1
		}
		if uint64(len(page.Entries)) < pageEntries {
			return count, nil
		}
	}
	return count, nil
}

func (s *Store) reconcilePagedCollection(ctx context.Context, id, collection string, count uint64) (uint64, error) {
	switch collection {
	case "metadata":
		return reconcilePagedV2(ctx, s, id, collection, count, func(revision domain.MetadataRevision) error {
			return domain.ValidateMetadataTimeline([]domain.MetadataRevision{revision})
		})
	case "manifests":
		return reconcilePagedV2(ctx, s, id, collection, count, validateV2Manifest)
	default:
		return count, ErrShardedArchiveInvalid
	}
}

func validateV2Manifest(snapshot domain.ManifestSnapshot) error {
	if snapshot.TrackID == "" || len(snapshot.TrackID) > archiveindex.MaxTrackIDBytes || snapshot.SourceURI == "" || len(snapshot.SourceURI) > maxV2SourceURIBytes || snapshot.FetchedAt.IsZero() || snapshot.Size < 0 || !validLowerDigest(snapshot.SHA256) || !canonicalRelativePath(snapshot.StoragePath) || len(snapshot.StoragePath) > archiveindex.MaxPayloadPathBytes {
		return ErrShardedArchiveInvalid
	}
	return nil
}

func (s *Store) reconcileObservations(ctx context.Context, id, collection string, count uint64, header *domain.Recording) (uint64, bool, error) {
	changed := false
	for count < ^uint64(0) {
		if err := checkV2Context(ctx); err != nil {
			return count, changed, err
		}
		next := count + 1
		ordinalPath := v2ObservationOrdinalPath(collection, next)
		var observation v2Observation
		err := s.loadV2JSON(ctx, id, ordinalPath, archiveShardMaxBytes, &observation)
		if errors.Is(err, ErrNotFound) {
			return count, changed, nil
		}
		if err != nil {
			return count, changed, err
		}
		if observation.Version != 1 || observation.Ordinal != next || (collection == "gaps") != (observation.Gap != nil) || (collection == "coverage") != (observation.Coverage != nil) || observation.Gap != nil && observation.Coverage != nil {
			return count, changed, ErrShardedArchiveInvalid
		}
		digestInput := observationDigestInput(observation)
		data, err := json.Marshal(digestInput)
		if err != nil {
			return count, changed, ErrShardedArchiveInvalid
		}
		digest := sha256.Sum256(data)
		coordinatePath, err := v2ObservationCoordinatePath(collection, observation, digest[:12])
		if err != nil {
			return count, changed, err
		}
		var existing v2Observation
		loadErr := s.loadV2JSON(ctx, id, coordinatePath, archiveShardMaxBytes, &existing)
		if loadErr == nil {
			if !sameV2Observation(existing, observation) || existing.Ordinal != observation.Ordinal {
				return count, changed, ErrShardedArchiveConflict
			}
		} else if errors.Is(loadErr, ErrNotFound) {
			if err := s.saveV2JSON(ctx, id, coordinatePath, observation, archiveShardMaxBytes); err != nil {
				return count, changed, err
			}
			changed = true
		} else {
			return count, changed, loadErr
		}
		candidate, candidateErr := coverageCandidateForObservation(collection, observation, header)
		if candidateErr != nil {
			return count, changed, candidateErr
		}
		_, stateUpdates, updateErr := s.coverageObservationChanges(ctx, id, header, candidate)
		if updateErr != nil {
			return count, changed, updateErr
		}
		if err := s.saveCoverageStateUpdates(ctx, id, stateUpdates); err != nil {
			return count, changed, err
		}
		if observation.ArchiveRevisionChange {
			if err := advanceV2ArchiveRevision(header); err != nil {
				return count, changed, err
			}
		}
		if observation.TimelineRevisionChange {
			if header.TimelineRevision == ^uint64(0) {
				return count, changed, ErrShardedArchiveInvalid
			}
			header.TimelineRevision++
		}
		if observation.Gap != nil {
			if err := s.writeLiveGapSlots(ctx, id, header, *observation.Gap); err != nil {
				return count, changed, err
			}
		}
		count = next
		changed = true
	}
	return count, changed, nil
}

func observationDigestInput(observation v2Observation) v2Observation {
	observation.Ordinal = 0
	observation.ArchiveRevisionChange = false
	observation.TimelineRevisionChange = false
	return observation
}

func (s *Store) iterateArchiveOrdinal(ctx context.Context, id, trackID string, init bool, visit func(V2MediaRecord) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	track := header.Tracks[trackID]
	if track == nil {
		return ErrNotFound
	}
	count := track.MediaHighWater
	if init {
		count = track.InitCount
	}
	for ordinal := uint64(1); ordinal <= count; {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		page, offset := mediaPageSlot(ordinal)
		pageData, err := s.loadV2MediaPage(ctx, id, trackID, init, page)
		if err != nil {
			return err
		}
		if int(offset) >= len(pageData.Entries) {
			return ErrShardedArchiveInvalid
		}
		pageEnd := uint64(page)*mediaShardMaxEntries + uint64(len(pageData.Entries))
		if pageEnd > count {
			pageEnd = count
		}
		for slot := uint64(ordinal); slot <= pageEnd; slot++ {
			index := slot - uint64(page)*mediaShardMaxEntries - 1
			record := pageData.Entries[index]
			if record.IndexOrdinal != 0 && record.IndexOrdinal != slot || record.Segment.TrackID != trackID || record.Segment.IsInit != init {
				return ErrShardedArchiveInvalid
			}
			if !init && record.Segment.ArchiveOrdinal != slot {
				return ErrShardedArchiveInvalid
			}
			if err := visit(record); err != nil {
				return err
			}
		}
		ordinal = pageEnd + 1
	}
	return nil
}

func (s *Store) appendPagedV2(ctx context.Context, id, collection string, value any, counter func(*domain.Recording) *uint64) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if collection != "metadata" && collection != "manifests" {
		return ErrShardedArchiveInvalid
	}
	pageEntries := pagedV2MaxEntries(collection)
	lock := s.v2Lock(id)
	lock.Lock()
	defer lock.Unlock()
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || validateV2Header(header) != nil {
		return ErrShardedArchiveUnavailable
	}
	if header.ArchiveSealed {
		return ErrArchiveSealed
	}
	count := counter(header)
	// A shard may be durable while the following bounded root counter update
	// failed. Reconcile a consecutive orphan suffix before assigning a new
	// ordinal so retry behavior is correct without requiring restart.
	reconciled, err := s.reconcilePagedCollection(ctx, id, collection, *count)
	if err != nil {
		return err
	}
	if reconciled != *count {
		*count = reconciled
		if collection == "metadata" || collection == "manifests" {
			if err := advanceV2ArchiveRevision(header); err != nil {
				return err
			}
		}
		if err := s.saveHeaderLocked(ctx, header, nil); err != nil {
			return err
		}
	}
	encodedValue, err := json.Marshal(value)
	if err != nil || len(encodedValue) > archiveShardMaxBytes {
		return ErrShardedArchiveInvalid
	}
	if *count > 0 {
		lastPage, lastOffset := pageSlotWithEntries(*count, pageEntries)
		var prior v2RevisionPage[json.RawMessage]
		if err := s.loadV2JSON(ctx, id, fmt.Sprintf("archive/v2/%s/%020d", collection, lastPage), archiveShardMaxBytes, &prior); err != nil {
			return err
		}
		if prior.Version != 1 || prior.Number != lastPage || int(lastOffset) >= len(prior.Entries) {
			return ErrShardedArchiveInvalid
		}
		if bytesEqualJSON(prior.Entries[lastOffset], encodedValue) {
			return nil
		}
	}
	ordinal := *count + 1
	pageNumber, offset := pageSlotWithEntries(ordinal, pageEntries)
	pagePath := fmt.Sprintf("archive/v2/%s/%020d", collection, pageNumber)
	var page v2RevisionPage[json.RawMessage]
	loadErr := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page)
	if loadErr != nil && !errors.Is(loadErr, ErrNotFound) {
		return loadErr
	}
	if loadErr == nil && (page.Version != 1 || page.Number != pageNumber || uint64(len(page.Entries)) > pageEntries) || page.Version != 0 && page.Version != 1 {
		return ErrShardedArchiveInvalid
	}
	if len(page.Entries) > int(offset) {
		if !bytesEqualJSON(page.Entries[offset], encodedValue) {
			return ErrShardedArchiveConflict
		}
	} else if len(page.Entries) == int(offset) {
		page.Entries = append(page.Entries, encodedValue)
	} else {
		return ErrShardedArchiveInvalid
	}
	page.Version, page.Number = 1, pageNumber
	if err := s.saveV2JSON(ctx, id, pagePath, page, archiveShardMaxBytes); err != nil {
		return err
	}
	*count = ordinal
	if collection == "metadata" || collection == "manifests" {
		if err := advanceV2ArchiveRevision(header); err != nil {
			return err
		}
	}
	return s.saveHeaderLocked(ctx, header, nil)
}

func advanceV2ArchiveRevision(recording *domain.Recording) error {
	if recording == nil || recording.ArchiveRevision == ^uint64(0) {
		return ErrShardedArchiveInvalid
	}
	if recording.ArchiveRevision == 0 {
		recording.ArchiveRevision = 1
		return nil
	}
	recording.ArchiveRevision++
	return nil
}

func iteratePagedV2[T any](s *Store, ctx context.Context, id, collection string, count uint64, visit func(T) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	pageEntries := pagedV2MaxEntries(collection)
	for ordinal := uint64(1); ordinal <= count; {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		pageNo, offset := pageSlotWithEntries(ordinal, pageEntries)
		var page v2RevisionPage[json.RawMessage]
		if err := s.loadV2JSON(ctx, id, fmt.Sprintf("archive/v2/%s/%020d", collection, pageNo), archiveShardMaxBytes, &page); err != nil {
			return err
		}
		if page.Version != 1 || page.Number != pageNo || int(offset) >= len(page.Entries) || uint64(len(page.Entries)) > pageEntries {
			return ErrShardedArchiveInvalid
		}
		pageEnd := pageNo*pageEntries + uint64(len(page.Entries))
		if pageEnd > count {
			pageEnd = count
		}
		for current := ordinal; current <= pageEnd; current++ {
			index := current - pageNo*pageEntries - 1
			var value T
			if json.Unmarshal(page.Entries[index], &value) != nil {
				return ErrShardedArchiveInvalid
			}
			if err := visit(value); err != nil {
				return err
			}
		}
		ordinal = pageEnd + 1
	}
	return nil
}

func (s *Store) appendObservation(ctx context.Context, id, collection string, value v2Observation) (ShardedRevisionResult, error) {
	if collection != "gaps" && collection != "coverage" {
		return ShardedRevisionResult{}, ErrShardedArchiveInvalid
	}
	if err := checkV2Context(ctx); err != nil {
		return ShardedRevisionResult{}, err
	}
	lock := s.v2Lock(id)
	lock.Lock()
	defer lock.Unlock()
	header, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return ShardedRevisionResult{}, err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || validateV2Header(header) != nil {
		return ShardedRevisionResult{}, ErrShardedArchiveUnavailable
	}
	if header.ArchiveSealed {
		return ShardedRevisionResult{}, ErrArchiveSealed
	}
	if err := s.ensureCoverageStateIndex(ctx, id, header); err != nil {
		return ShardedRevisionResult{}, fmt.Errorf("prepare coverage state index: %w", err)
	}
	counter := &header.ShardedArchive.GapCount
	if collection == "coverage" {
		counter = &header.ShardedArchive.CoverageObservationCount
	}
	data, err := json.Marshal(value)
	if err != nil || len(data) > archiveShardMaxBytes {
		return ShardedRevisionResult{}, ErrShardedArchiveInvalid
	}
	digestInput := observationDigestInput(value)
	digestData, err := json.Marshal(digestInput)
	if err != nil {
		return ShardedRevisionResult{}, ErrShardedArchiveInvalid
	}
	digest := sha256.Sum256(digestData)
	coordinatePath, err := v2ObservationCoordinatePath(collection, value, digest[:12])
	if err != nil {
		return ShardedRevisionResult{}, fmt.Errorf("derive sharded observation identity: %w", err)
	}
	var priorCoordinate v2Observation
	coordinateErr := s.loadV2JSON(ctx, id, coordinatePath, archiveShardMaxBytes, &priorCoordinate)
	orphanCoordinate := false
	if coordinateErr == nil {
		if !sameV2Observation(priorCoordinate, value) {
			return ShardedRevisionResult{}, fmt.Errorf("sharded observation coordinate reused with different value: %w", ErrShardedArchiveConflict)
		}
		if priorCoordinate.Ordinal <= *counter {
			return ShardedRevisionResult{ArchiveRevision: header.ArchiveRevision, TimelineRevision: header.TimelineRevision}, nil
		}
		if priorCoordinate.Ordinal != *counter+1 {
			return ShardedRevisionResult{}, fmt.Errorf("sharded observation ordinal is not next: %w", ErrShardedArchiveInvalid)
		}
		value = priorCoordinate
		orphanCoordinate = true
	} else if !errors.Is(coordinateErr, ErrNotFound) {
		return ShardedRevisionResult{}, coordinateErr
	} else {
		value.Ordinal = *counter + 1
	}
	next := *counter + 1
	ordinalPath := v2ObservationOrdinalPath(collection, next)
	var existing v2Observation
	var stateUpdates []v2CoverageStatePageUpdate
	if err := s.loadV2JSON(ctx, id, ordinalPath, archiveShardMaxBytes, &existing); err == nil {
		if !sameV2Observation(existing, value) || existing.Ordinal != next {
			return ShardedRevisionResult{}, fmt.Errorf("sharded observation ordinal reused with different value: %w", ErrShardedArchiveConflict)
		}
		value = existing
		candidate, candidateErr := coverageCandidateForObservation(collection, value, header)
		if candidateErr != nil {
			return ShardedRevisionResult{}, candidateErr
		}
		_, stateUpdates, err = s.coverageObservationChanges(ctx, id, header, candidate)
		if err != nil {
			return ShardedRevisionResult{}, err
		}
	} else if !errors.Is(err, ErrNotFound) {
		return ShardedRevisionResult{}, err
	} else {
		if orphanCoordinate {
			return ShardedRevisionResult{}, fmt.Errorf("sharded observation coordinate has no ordinal record: %w", ErrShardedArchiveInvalid)
		}
		archiveChanged, timelineChanged, updates, err := s.observationRevisionChanges(ctx, id, header, collection, value)
		if err != nil {
			return ShardedRevisionResult{}, err
		}
		stateUpdates = updates
		value.ArchiveRevisionChange = archiveChanged
		value.TimelineRevisionChange = timelineChanged
		if err := s.saveV2JSON(ctx, id, ordinalPath, value, archiveShardMaxBytes); err != nil {
			return ShardedRevisionResult{}, err
		}
	}
	archiveChanged := value.ArchiveRevisionChange
	timelineChanged := value.TimelineRevisionChange
	if err := s.saveV2JSON(ctx, id, coordinatePath, value, archiveShardMaxBytes); err != nil {
		return ShardedRevisionResult{}, fmt.Errorf("publish sharded observation coordinate index: %w", err)
	}
	if value.Gap != nil {
		if err := s.writeLiveGapSlots(ctx, id, header, *value.Gap); err != nil {
			return ShardedRevisionResult{}, fmt.Errorf("publish live gap slots: %w", err)
		}
	}
	if err := s.saveCoverageStateUpdates(ctx, id, stateUpdates); err != nil {
		return ShardedRevisionResult{}, fmt.Errorf("publish coverage state index: %w", err)
	}
	*counter = next
	if archiveChanged {
		if err := advanceV2ArchiveRevision(header); err != nil {
			return ShardedRevisionResult{}, err
		}
	}
	if timelineChanged {
		if header.TimelineRevision == ^uint64(0) {
			return ShardedRevisionResult{}, ErrShardedArchiveInvalid
		}
		header.TimelineRevision++
	}
	if err := s.saveHeaderLocked(ctx, header, nil); err != nil {
		return ShardedRevisionResult{}, fmt.Errorf("publish sharded observation root: %w", err)
	}
	return ShardedRevisionResult{ArchiveRevision: header.ArchiveRevision, TimelineRevision: header.TimelineRevision, Changed: archiveChanged || timelineChanged}, nil
}

func (s *Store) observationRevisionChanges(ctx context.Context, id string, header *domain.Recording, collection string, value v2Observation) (archiveChanged, timelineChanged bool, updates []v2CoverageStatePageUpdate, err error) {
	candidate, err := coverageCandidateForObservation(collection, value, header)
	if err != nil {
		return false, false, nil, err
	}
	archiveChanged, updates, err = s.coverageObservationChanges(ctx, id, header, candidate)
	if err != nil {
		return false, false, nil, err
	}
	if collection == "gaps" && value.Gap != nil {
		timelineChanged, err = s.gapChangesTimelineProjection(ctx, id, header, value.Gap)
	}
	return archiveChanged, timelineChanged, updates, err
}

func coverageCandidateForObservation(collection string, value v2Observation, header *domain.Recording) (archiveindex.Coverage, error) {
	switch {
	case collection == "gaps" && value.Gap != nil && header != nil:
		gap := value.Gap
		return archiveindex.Coverage{
			SessionID: header.SourceSessionID, TrackID: gap.TrackID, SourceEpoch: gap.SourceEpoch,
			DiscontinuitySequence: gap.DiscontinuitySequence, FromSequence: gap.FromSequence,
			ToSequence: gap.ToSequence, State: archiveindex.CoverageKnownMissing,
			ObservedAt: gap.DetectedAt, Reason: gap.Reason, Kind: archiveindex.ObjectMedia,
		}, nil
	case collection == "coverage" && value.Coverage != nil:
		candidate := *value.Coverage
		if candidate.Kind == "" {
			candidate.Kind = archiveindex.ObjectMedia
		}
		return candidate, nil
	default:
		return archiveindex.Coverage{}, ErrShardedArchiveInvalid
	}
}

func (s *Store) coverageObservationChanges(ctx context.Context, id string, header *domain.Recording, candidate archiveindex.Coverage) (bool, []v2CoverageStatePageUpdate, error) {
	if candidate.State == "" || candidate.ToSequence < candidate.FromSequence || candidate.SessionID == "" || candidate.TrackID == "" {
		return false, nil, ErrShardedArchiveInvalid
	}
	rank, valid := coverageStateRank(candidate.State)
	if !valid {
		return false, nil, ErrShardedArchiveInvalid
	}
	if header.Tracks[candidate.TrackID] == nil {
		return false, nil, ErrShardedArchiveInvalid
	}
	if rank == 0 {
		return false, nil, nil
	}
	mediaLookup := v2CoverageMediaLookup{
		timelinePages: make(map[string]v2TimelinePage),
		mediaPages:    make(map[v2CoverageMediaPageKey]v2MediaPage),
	}
	updates := make([]v2CoverageStatePageUpdate, 0)
	changed := false
	firstBucket := candidate.FromSequence / mediaShardMaxEntries
	lastBucket := candidate.ToSequence / mediaShardMaxEntries
	for bucket := firstBucket; ; bucket++ {
		if err := checkV2Context(ctx); err != nil {
			return false, nil, err
		}
		pagePath := v2CoverageStatePagePath(candidate, bucket)
		page := v2CoverageStatePage{
			Version: 1, SessionID: candidate.SessionID, TrackID: candidate.TrackID,
			SourceEpoch: candidate.SourceEpoch, DiscontinuitySequence: candidate.DiscontinuitySequence,
			Kind: candidate.Kind, Bucket: bucket,
		}
		if err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page); err != nil {
			if !errors.Is(err, ErrNotFound) {
				return false, nil, err
			}
		} else if validateV2CoverageStatePage(page, candidate, bucket) != nil {
			return false, nil, ErrShardedArchiveInvalid
		}
		start, end := coverageBucketRange(candidate, bucket)
		pageChanged := false
		for sequence := start; ; sequence++ {
			offset := sequence % mediaShardMaxEntries
			if page.States[offset] < rank {
				mediaRank, err := s.shardedMediaStateAt(ctx, id, header, candidate, sequence, &mediaLookup)
				if err != nil {
					return false, nil, err
				}
				if mediaRank > 0 {
					// Canonical media already determines this coordinate's effective
					// state. Cache that fact without treating the observation as a
					// canonical state change, so later ranges do not re-read the media.
					page.States[offset] = mediaRank
					pageChanged = true
				} else {
					page.States[offset] = rank
					pageChanged = true
					changed = true
				}
			}
			if sequence == end {
				break
			}
		}
		if pageChanged {
			updates = append(updates, v2CoverageStatePageUpdate{Path: pagePath, Page: page})
		}
		if bucket == lastBucket {
			break
		}
	}
	return changed, updates, nil
}

func coverageStateRank(state archiveindex.CoverageState) (uint8, bool) {
	switch state {
	case archiveindex.CoverageUnknown, "":
		return 0, true
	case archiveindex.CoverageKnownMissing:
		return 1, true
	case archiveindex.CoveragePresent:
		return 2, true
	case archiveindex.CoverageConflict:
		return 3, true
	case archiveindex.CoverageAcquisitionFailed:
		return 0, true
	default:
		return 0, false
	}
}

func coverageBucketRange(candidate archiveindex.Coverage, bucket uint64) (uint64, uint64) {
	start := bucket * mediaShardMaxEntries
	end := ^uint64(0)
	if start <= ^uint64(0)-(mediaShardMaxEntries-1) {
		end = start + mediaShardMaxEntries - 1
	}
	if start < candidate.FromSequence {
		start = candidate.FromSequence
	}
	if end > candidate.ToSequence {
		end = candidate.ToSequence
	}
	return start, end
}

func validateV2CoverageStatePage(page v2CoverageStatePage, candidate archiveindex.Coverage, bucket uint64) error {
	if page.Version != 1 || page.SessionID != candidate.SessionID || page.TrackID != candidate.TrackID || page.SourceEpoch != candidate.SourceEpoch || page.DiscontinuitySequence != candidate.DiscontinuitySequence || page.Kind != candidate.Kind || page.Bucket != bucket {
		return ErrShardedArchiveInvalid
	}
	for _, state := range page.States {
		if state > 3 {
			return ErrShardedArchiveInvalid
		}
	}
	return nil
}

func (s *Store) shardedMediaStateAt(ctx context.Context, id string, header *domain.Recording, candidate archiveindex.Coverage, sequence uint64, cache *v2CoverageMediaLookup) (uint8, error) {
	coordinate := archiveindex.Coordinate{
		SessionID: candidate.SessionID, TrackID: candidate.TrackID, SourceEpoch: candidate.SourceEpoch,
		DiscontinuitySequence: candidate.DiscontinuitySequence, Sequence: sequence, Kind: candidate.Kind,
	}
	pagePath, err := v2CoordinatePath(coordinate)
	if err != nil {
		return 0, err
	}
	page, found := cache.timelinePages[pagePath]
	if !found {
		page = v2TimelinePage{}
		err = s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page)
		if errors.Is(err, ErrNotFound) {
			cache.timelinePages[pagePath] = v2TimelinePage{}
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		if err := validateV2TimelinePage(page, coordinate.TrackID, coordinate); err != nil {
			return 0, err
		}
		cache.timelinePages[pagePath] = page
	}
	var entry *v2TimelineEntry
	for index := range page.Entries {
		if page.Entries[index].Coordinate == coordinate {
			entry = &page.Entries[index]
			break
		}
	}
	if entry == nil {
		return 0, nil
	}
	track := header.Tracks[candidate.TrackID]
	init := candidate.Kind == archiveindex.ObjectInit
	ordinal := entry.ArchiveOrdinal
	limit := track.MediaHighWater
	if init {
		ordinal, limit = entry.InitOrdinal, track.InitCount
	}
	if ordinal == 0 || ordinal > limit {
		return 0, nil
	}
	pageNo, offset := mediaPageSlot(ordinal)
	key := v2CoverageMediaPageKey{init: init, number: pageNo}
	mediaPage, exists := cache.mediaPages[key]
	if !exists {
		mediaPage, err = s.loadV2MediaPage(ctx, id, candidate.TrackID, init, pageNo)
		if err != nil {
			return 0, err
		}
		cache.mediaPages[key] = mediaPage
	}
	if int(offset) >= len(mediaPage.Entries) {
		return 0, ErrShardedArchiveInvalid
	}
	record := mediaPage.Entries[offset]
	if record.Coordinate != coordinate || record.Segment.IsInit != init {
		return 0, ErrShardedArchiveInvalid
	}
	state := record.ClaimState
	if state == "" {
		state = archiveindex.CoveragePresent
	}
	rank, valid := coverageStateRank(state)
	if !valid {
		return 0, ErrShardedArchiveInvalid
	}
	return rank, nil
}

func (s *Store) saveCoverageStateUpdates(ctx context.Context, id string, updates []v2CoverageStatePageUpdate) error {
	for _, update := range updates {
		if err := s.saveV2JSON(ctx, id, update.Path, update.Page, archiveShardMaxBytes); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureCoverageStateIndex(ctx context.Context, id string, header *domain.Recording) error {
	if header == nil || header.ShardedArchive == nil {
		return ErrShardedArchiveInvalid
	}
	var marker v2CoverageStateIndexMarker
	err := s.loadV2JSON(ctx, id, v2CoverageStateIndexMarkerPath, archiveShardMaxBytes, &marker)
	if err == nil {
		if marker.Version != 1 || !marker.Ready {
			return ErrShardedArchiveInvalid
		}
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	// Existing V2 archives may predate this derived index. Rebuild once from
	// committed observation ordinals. Normal later observations use direct
	// bounded page reads and never replay this history.
	pages := make(map[string]v2CoverageStatePage)
	for _, collection := range []string{"gaps", "coverage"} {
		count := header.ShardedArchive.GapCount
		if collection == "coverage" {
			count = header.ShardedArchive.CoverageObservationCount
		}
		for ordinal := uint64(1); ordinal <= count; ordinal++ {
			if err := checkV2Context(ctx); err != nil {
				return err
			}
			var observation v2Observation
			if err := s.loadV2JSON(ctx, id, v2ObservationOrdinalPath(collection, ordinal), archiveShardMaxBytes, &observation); err != nil {
				return err
			}
			if observation.Version != 1 || observation.Ordinal != ordinal || (collection == "gaps") != (observation.Gap != nil) || (collection == "coverage") != (observation.Coverage != nil) || observation.Gap != nil && observation.Coverage != nil {
				return ErrShardedArchiveInvalid
			}
			candidate, candidateErr := coverageCandidateForObservation(collection, observation, header)
			if candidateErr != nil {
				return candidateErr
			}
			rank, valid := coverageStateRank(candidate.State)
			if !valid {
				return ErrShardedArchiveInvalid
			}
			if rank == 0 {
				continue
			}
			firstBucket := candidate.FromSequence / mediaShardMaxEntries
			lastBucket := candidate.ToSequence / mediaShardMaxEntries
			for bucket := firstBucket; ; bucket++ {
				pagePath := v2CoverageStatePagePath(candidate, bucket)
				page, ok := pages[pagePath]
				if !ok {
					page = v2CoverageStatePage{
						Version: 1, SessionID: candidate.SessionID, TrackID: candidate.TrackID,
						SourceEpoch: candidate.SourceEpoch, DiscontinuitySequence: candidate.DiscontinuitySequence,
						Kind: candidate.Kind, Bucket: bucket,
					}
					loadErr := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page)
					if loadErr != nil && !errors.Is(loadErr, ErrNotFound) {
						return loadErr
					}
					if loadErr == nil && validateV2CoverageStatePage(page, candidate, bucket) != nil {
						return ErrShardedArchiveInvalid
					}
				}
				start, end := coverageBucketRange(candidate, bucket)
				for sequence := start; ; sequence++ {
					offset := sequence % mediaShardMaxEntries
					if page.States[offset] < rank {
						page.States[offset] = rank
					}
					if sequence == end {
						break
					}
				}
				pages[pagePath] = page
				if bucket == lastBucket {
					break
				}
			}
		}
	}
	pagePaths := make([]string, 0, len(pages))
	for pagePath := range pages {
		pagePaths = append(pagePaths, pagePath)
	}
	sort.Strings(pagePaths)
	for _, pagePath := range pagePaths {
		if err := s.saveV2JSON(ctx, id, pagePath, pages[pagePath], archiveShardMaxBytes); err != nil {
			return err
		}
	}
	return s.saveV2JSON(ctx, id, v2CoverageStateIndexMarkerPath, v2CoverageStateIndexMarker{Version: 1, Ready: true}, archiveShardMaxBytes)
}

func (s *Store) gapChangesTimelineProjection(ctx context.Context, id string, header *domain.Recording, candidate *domain.Gap) (bool, error) {
	if candidate == nil || candidate.LivePresentationOrdinal == 0 || header == nil {
		return false, nil
	}
	track := header.Tracks[candidate.TrackID]
	if track == nil || track.LivePresentation == nil || candidate.ToSequence < candidate.FromSequence {
		return false, nil
	}
	span := candidate.ToSequence - candidate.FromSequence
	if span > ^uint64(0)-candidate.LivePresentationOrdinal {
		return false, ErrShardedArchiveInvalid
	}
	gapLast := candidate.LivePresentationOrdinal + span
	last := uint64(0)
	if track.LivePresentation.NextOrdinal > 0 {
		last = track.LivePresentation.NextOrdinal - 1
	}
	if gapLast > last {
		last = gapLast
	}
	first := uint64(1)
	if last >= 12 {
		first = last - 11
	}
	if track.LivePresentation.FirstPresentationOrdinal > first {
		first = track.LivePresentation.FirstPresentationOrdinal
	}
	start, end := candidate.LivePresentationOrdinal, gapLast
	if start < first {
		start = first
	}
	if end > last {
		end = last
	}
	if start > end {
		return false, nil
	}
	for ordinal := start; ordinal <= end; ordinal++ {
		slot, err := s.LookupShardedLiveSlot(ctx, id, candidate.TrackID, ordinal)
		if err == nil {
			if slot.Segment != nil {
				continue
			}
			if slot.Gap == nil {
				return true, nil
			}
			expected := gapAtLiveOrdinal(*candidate, ordinal)
			if !sameV2LiveGapProjection(*slot.Gap, expected) {
				return true, nil
			}
		} else if errors.Is(err, ErrNotFound) {
			// A reserved or not-yet-published slot becomes an explicit live GAP.
			return true, nil
		} else {
			return false, err
		}
		if ordinal == ^uint64(0) {
			break
		}
	}
	return false, nil
}

func sameV2LiveGapProjection(left, right domain.Gap) bool {
	if left.TrackID != right.TrackID || left.SourceEpoch != right.SourceEpoch ||
		left.DiscontinuitySequence != right.DiscontinuitySequence || left.FromSequence != right.FromSequence ||
		left.ToSequence != right.ToSequence || left.LivePresentationOrdinal != right.LivePresentationOrdinal ||
		left.LiveDiscontinuitySequence != right.LiveDiscontinuitySequence || left.LiveDiscontinuity != right.LiveDiscontinuity ||
		left.LiveDuration != right.LiveDuration {
		return false
	}
	if left.ProgramDateTime == nil || right.ProgramDateTime == nil {
		return left.ProgramDateTime == nil && right.ProgramDateTime == nil
	}
	return left.ProgramDateTime.Equal(*right.ProgramDateTime)
}

func sameV2Observation(left, right v2Observation) bool {
	left, right = observationDigestInput(left), observationDigestInput(right)
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (s *Store) writeLiveGapSlots(ctx context.Context, id string, header *domain.Recording, gap domain.Gap) error {
	track := header.Tracks[gap.TrackID]
	if track == nil {
		return fmt.Errorf("live gap track missing: %w", ErrShardedArchiveInvalid)
	}
	if gap.LivePresentationOrdinal == 0 || gap.ToSequence < gap.FromSequence {
		return nil
	}
	span := gap.ToSequence - gap.FromSequence
	if span > ^uint64(0)-gap.LivePresentationOrdinal {
		return fmt.Errorf("live gap ordinal range overflow: %w", ErrShardedArchiveInvalid)
	}
	gapLast := gap.LivePresentationOrdinal + span
	if gapLast == ^uint64(0) {
		return fmt.Errorf("live gap final ordinal overflow: %w", ErrShardedArchiveInvalid)
	}
	// A live-origin gap can reserve next presentation ordinal before track
	// cursor advances. Publish reservation with GapCount root marker.
	if track.LivePresentation == nil {
		track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 1}
	}
	state := track.LivePresentation
	if state.NextOrdinal == 0 {
		state.NextOrdinal = 1
	}
	if state.NextOrdinal <= gapLast {
		state.NextOrdinal = gapLast + 1
	}
	if state.FirstPresentationOrdinal == 0 || gap.LivePresentationOrdinal < state.FirstPresentationOrdinal {
		state.FirstPresentationOrdinal = gap.LivePresentationOrdinal
	}
	if state.HasEpochMapping && state.MappedSourceEpoch == gap.SourceEpoch && gap.FromSequence >= state.SourceSequenceBase {
		sequenceOffset := gap.FromSequence - state.SourceSequenceBase
		if state.PresentationOrdinalBase > ^uint64(0)-sequenceOffset || state.PresentationOrdinalBase+sequenceOffset != gap.LivePresentationOrdinal {
			return fmt.Errorf("live gap epoch mapping mismatch: %w", ErrShardedArchiveConflict)
		}
		if gap.ToSequence > state.MaxSourceSequence {
			state.MaxSourceSequence = gap.ToSequence
		}
		maxOffset := state.MaxSourceSequence - state.SourceSequenceBase
		if state.PresentationOrdinalBase > ^uint64(0)-maxOffset-1 {
			return fmt.Errorf("live gap next ordinal overflow: %w", ErrShardedArchiveInvalid)
		}
		next := state.PresentationOrdinalBase + maxOffset + 1
		if state.NextOrdinal < next {
			state.NextOrdinal = next
		}
	}
	if !state.HasLastCoordinate || gap.SourceEpoch > state.LastSourceEpoch || gap.SourceEpoch == state.LastSourceEpoch && (gap.DiscontinuitySequence > state.LastDiscontinuitySequence || gap.DiscontinuitySequence == state.LastDiscontinuitySequence && gap.ToSequence > state.LastSequence) {
		state.HasLastCoordinate = true
		state.LastSourceEpoch = gap.SourceEpoch
		state.LastDiscontinuitySequence = gap.DiscontinuitySequence
		state.LastSequence = gap.ToSequence
	}
	if gap.LiveDiscontinuitySequence > state.DiscontinuitySequence {
		state.DiscontinuitySequence = gap.LiveDiscontinuitySequence
	}
	if gap.LiveDiscontinuity && gap.LiveDiscontinuitySequence < ^uint64(0) && state.DiscontinuitySequence <= gap.LiveDiscontinuitySequence {
		state.DiscontinuitySequence = gap.LiveDiscontinuitySequence + 1
	}
	last := state.NextOrdinal - 1
	first := uint64(1)
	if last >= 12 {
		first = last - 11
	}
	if gapLast < first || gap.LivePresentationOrdinal > last {
		return nil
	}
	// Reserve the rest of bounded live tail before publishing explicit gap.
	// The manifest can reveal slot 9 before fetches for slots 1-8 commit. Empty
	// reservations are not gaps and remain hidden from playback iteration.
	pages := make(map[uint64]*v2LivePage, 2)
	pagePaths := make(map[uint64]string, 2)
	for ordinal := first; ordinal <= last; ordinal++ {
		pageNo, offset := pageSlot(ordinal)
		doc := pages[pageNo]
		if doc == nil {
			pagePath := v2LivePagePath(gap.TrackID, pageNo)
			var loaded v2LivePage
			err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &loaded)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			if errors.Is(err, ErrNotFound) {
				loaded = v2LivePage{Version: 1, TrackID: gap.TrackID, Number: pageNo}
			}
			if loaded.Version != 1 || loaded.TrackID != gap.TrackID || loaded.Number != pageNo || len(loaded.Entries) > ArchiveShardMaxEntries {
				return fmt.Errorf("live gap page header invalid: page=%d entries=%d: %w", pageNo, len(loaded.Entries), ErrShardedArchiveInvalid)
			}
			doc = &loaded
			pages[pageNo], pagePaths[pageNo] = doc, pagePath
		}
		for len(doc.Entries) <= int(offset) {
			reservedOrdinal := pageNo*ArchiveShardMaxEntries + uint64(len(doc.Entries)) + 1
			doc.Entries = append(doc.Entries, v2LiveIndexEntry{PresentationOrdinal: reservedOrdinal, Reserved: true})
		}
		if len(doc.Entries) > ArchiveShardMaxEntries {
			return fmt.Errorf("live gap page exceeds entry bound: page=%d entries=%d: %w", pageNo, len(doc.Entries), ErrShardedArchiveInvalid)
		}
		prior := doc.Entries[offset]
		if prior.PresentationOrdinal != ordinal {
			return fmt.Errorf("live gap page ordinal mismatch: want=%d got=%d: %w", ordinal, prior.PresentationOrdinal, ErrShardedArchiveInvalid)
		}
		isGapSlot := ordinal >= gap.LivePresentationOrdinal && ordinal <= gapLast
		if !isGapSlot {
			if prior.Reserved {
				mergeUint64(&track.LiveSlotHighWater, ordinal)
				continue
			}
			// Existing media or another published gap stays authoritative.
			mergeUint64(&track.LiveSlotHighWater, ordinal)
			continue
		}
		if !prior.Reserved && prior.ArchiveOrdinal != 0 {
			// A committed media object wins over later gap observation.
			mergeUint64(&track.LiveSlotHighWater, ordinal)
			continue
		}
		expected := gapAtLiveOrdinal(gap, ordinal)
		if prior.Gap != nil && !sameV2Gap(*prior.Gap, expected) {
			return ErrShardedArchiveConflict
		}
		doc.Entries[offset] = v2LiveIndexEntry{
			PresentationOrdinal: ordinal,
			Coordinate: archiveindex.Coordinate{TrackID: gap.TrackID, SourceEpoch: gap.SourceEpoch,
				DiscontinuitySequence: gap.DiscontinuitySequence, Sequence: expected.FromSequence, Kind: archiveindex.ObjectMedia},
			Gap: &expected,
		}
		mergeUint64(&track.LiveSlotHighWater, ordinal)
		if ordinal == ^uint64(0) {
			break
		}
	}
	pageNumbers := make([]uint64, 0, len(pages))
	for pageNo := range pages {
		pageNumbers = append(pageNumbers, pageNo)
	}
	sort.Slice(pageNumbers, func(i, j int) bool { return pageNumbers[i] < pageNumbers[j] })
	for _, pageNo := range pageNumbers {
		if err := s.saveV2JSON(ctx, id, pagePaths[pageNo], pages[pageNo], archiveShardMaxBytes); err != nil {
			return err
		}
	}
	mergeUint64(&track.LiveSlotHighWater, last)
	return nil
}

func gapAtLiveOrdinal(gap domain.Gap, ordinal uint64) domain.Gap {
	copy := gap
	sequence := gap.FromSequence + (ordinal - gap.LivePresentationOrdinal)
	copy.FromSequence, copy.ToSequence = sequence, sequence
	copy.LivePresentationOrdinal = ordinal
	copy.LiveDiscontinuity = ordinal == gap.LivePresentationOrdinal && gap.LiveDiscontinuity
	return copy
}

func sameV2Gap(left, right domain.Gap) bool {
	left.LivePresentationOrdinal, right.LivePresentationOrdinal = 0, 0
	left.LiveDiscontinuity, right.LiveDiscontinuity = false, false
	left.LiveDiscontinuitySequence, right.LiveDiscontinuitySequence = 0, 0
	left.LiveDuration, right.LiveDuration = 0, 0
	left.ProgramDateTime, right.ProgramDateTime = nil, nil
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (s *Store) iterateObservations(ctx context.Context, id, collection string, visit func(v2Observation) error) error {
	header, err := s.LoadRecordingHeader(ctx, id)
	if err != nil {
		return err
	}
	count := header.ShardedArchive.GapCount
	if collection == "coverage" {
		count = header.ShardedArchive.CoverageObservationCount
	}
	return s.listV2(ctx, id, "archive/v2/"+collection+"/", func(relative string) error {
		var observation v2Observation
		if err := s.loadV2JSON(ctx, id, trimJSONSuffix(relative), archiveShardMaxBytes, &observation); err != nil {
			return err
		}
		if observation.Version != 1 {
			return ErrShardedArchiveInvalid
		}
		if observation.Ordinal == 0 || observation.Ordinal > count {
			return nil
		}
		return visit(observation)
	})
}

func (s *Store) writeV2RecordObjects(ctx context.Context, id string, record V2MediaRecord, current *domain.Recording) (v2IndexPointer, error) {
	segment := record.Segment
	track := current.Tracks[segment.TrackID]
	init := segment.IsInit
	var ordinal uint64
	if init {
		ordinal = track.InitCount + 1
	} else {
		ordinal = segment.ArchiveOrdinal
		want := track.NextArchiveOrdinal
		if want == 0 {
			want = 1
		}
		if ordinal != want || ordinal != track.MediaHighWater+1 {
			return v2IndexPointer{}, ErrShardedArchiveConflict
		}
	}
	page, offset := mediaPageSlot(ordinal)
	record.IndexOrdinal = ordinal
	pagePath := v2MediaPagePath(segment.TrackID, init, page)
	var pageDoc v2MediaPage
	err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &pageDoc)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return v2IndexPointer{}, fmt.Errorf("load sharded media page: %w", err)
	}
	if err == nil && (pageDoc.Version != 1 || pageDoc.TrackID != segment.TrackID || pageDoc.Number != page) {
		return v2IndexPointer{}, fmt.Errorf("validate sharded media page: %w", ErrShardedArchiveInvalid)
	}
	if err != nil {
		pageDoc = v2MediaPage{Version: 1, TrackID: segment.TrackID, Number: page}
	}
	if int(offset) < len(pageDoc.Entries) {
		if !sameV2Media(pageDoc.Entries[offset], record) && !sameV2MediaRetry(pageDoc.Entries[offset], record) {
			return v2IndexPointer{}, fmt.Errorf("validate sharded media page identity: %w", ErrShardedArchiveConflict)
		}
	} else if int(offset) == len(pageDoc.Entries) {
		pageDoc.Entries = append(pageDoc.Entries, record)
	} else {
		return v2IndexPointer{}, fmt.Errorf("validate sharded media page offset: %w", ErrShardedArchiveInvalid)
	}
	if err := s.saveV2JSON(ctx, id, pagePath, pageDoc, archiveShardMaxBytes); err != nil {
		return v2IndexPointer{}, fmt.Errorf("publish sharded media page: %w", err)
	}
	pointer := v2IndexPointer{Version: 1, Coordinate: record.Coordinate, TrackID: segment.TrackID, Kind: record.Coordinate.Kind, Page: page, Offset: offset, IndexOrdinal: ordinal, ArchiveOrdinal: segment.ArchiveOrdinal, Record: record}
	if err := s.writeV2TimelineEntry(ctx, id, record); err != nil {
		return v2IndexPointer{}, fmt.Errorf("publish sharded timeline page: %w", err)
	}
	if init {
		if err := s.putIdempotentPointer(ctx, id, v2IDPath(segment.ID), pointer); err != nil {
			return v2IndexPointer{}, fmt.Errorf("publish sharded init ID index: %w", err)
		}
	}
	if segment.LivePresentationOrdinal > 0 {
		if err := s.writeLiveIndex(ctx, id, record, current.Tracks[segment.TrackID]); err != nil {
			return v2IndexPointer{}, fmt.Errorf("publish sharded live index: %w", err)
		}
	}
	return pointer, nil
}

func (s *Store) publishV2Indexes(ctx context.Context, id string, record V2MediaRecord, header *domain.Recording) error {
	ordinal := record.IndexOrdinal
	if ordinal == 0 {
		ordinal = record.Segment.ArchiveOrdinal
	}
	page, offset := mediaPageSlot(ordinal)
	record.IndexOrdinal = ordinal
	pointer := v2IndexPointer{Version: 1, Coordinate: record.Coordinate, TrackID: record.Segment.TrackID, Kind: record.Coordinate.Kind, Page: page, Offset: offset, IndexOrdinal: ordinal, ArchiveOrdinal: record.Segment.ArchiveOrdinal, Record: record}
	if err := s.writeV2TimelineEntry(ctx, id, record); err != nil {
		return err
	}
	if record.Segment.IsInit {
		if err := s.putIdempotentPointer(ctx, id, v2IDPath(record.Segment.ID), pointer); err != nil {
			return err
		}
	}
	if record.Segment.LivePresentationOrdinal > 0 {
		if header == nil || header.Tracks[record.Segment.TrackID] == nil {
			return ErrShardedArchiveInvalid
		}
		return s.writeLiveIndex(ctx, id, record, header.Tracks[record.Segment.TrackID])
	}
	return nil
}

func (s *Store) writeLiveIndex(ctx context.Context, id string, record V2MediaRecord, track *domain.Track) error {
	ordinal := record.Segment.LivePresentationOrdinal
	if ordinal == 0 || ordinal == ^uint64(0) || track == nil || track.LivePresentation == nil {
		return ErrShardedArchiveInvalid
	}
	page, offset := pageSlot(ordinal)
	pagePath := v2LivePagePath(record.Segment.TrackID, page)
	var doc v2LivePage
	err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &doc)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if errors.Is(err, ErrNotFound) {
		doc = v2LivePage{Version: 1, TrackID: record.Segment.TrackID, Number: page}
	}
	entry := v2LiveIndexEntry{PresentationOrdinal: ordinal, Coordinate: record.Coordinate, ArchiveOrdinal: record.Segment.ArchiveOrdinal}
	if int(offset) < len(doc.Entries) {
		prior := doc.Entries[offset]
		if prior.Reserved && prior.PresentationOrdinal == ordinal {
			doc.Entries[offset] = entry
		} else if prior.Gap != nil {
			if prior.PresentationOrdinal != ordinal || !sameGapSourceCoordinate(prior.Coordinate, entry.Coordinate) || prior.Gap.LivePresentationOrdinal != ordinal || prior.Gap.TrackID != record.Segment.TrackID || prior.Gap.FromSequence != record.Segment.Sequence || prior.Gap.SourceEpoch != record.Segment.SourceEpoch || prior.Gap.DiscontinuitySequence != record.Segment.DiscontinuitySequence {
				return fmt.Errorf("live gap slot identity mismatch: ordinal=%t coordinate=%t track=%t sequence=%t epoch=%t discontinuity=%t: %w",
					prior.PresentationOrdinal == ordinal,
					prior.Coordinate == entry.Coordinate,
					prior.Gap.TrackID == record.Segment.TrackID,
					prior.Gap.FromSequence == record.Segment.Sequence,
					prior.Gap.SourceEpoch == record.Segment.SourceEpoch,
					prior.Gap.DiscontinuitySequence == record.Segment.DiscontinuitySequence,
					ErrShardedArchiveConflict)
			}
			doc.Entries[offset] = entry
		} else if !sameV2LiveEntry(prior, entry) {
			return ErrShardedArchiveConflict
		}
	} else {
		// Workers may finish out of order. The persisted live cursor was
		// published before those workers were admitted, so it proves which lower
		// ordinals are assigned and may be represented as invisible reservations.
		// A hole outside that assigned range remains a canonical index error.
		for len(doc.Entries) < int(offset) {
			reservedOrdinal := page*ArchiveShardMaxEntries + uint64(len(doc.Entries)) + 1
			if track.LivePresentation.NextOrdinal <= reservedOrdinal || track.LiveSlotHighWater >= reservedOrdinal {
				return fmt.Errorf("live presentation slot has unproven hole: ordinal=%d cursor=%d high-water=%d: %w", reservedOrdinal, track.LivePresentation.NextOrdinal, track.LiveSlotHighWater, ErrShardedArchiveInvalid)
			}
			doc.Entries = append(doc.Entries, v2LiveIndexEntry{PresentationOrdinal: reservedOrdinal, Reserved: true})
		}
		if int(offset) == len(doc.Entries) {
			doc.Entries = append(doc.Entries, entry)
		} else {
			return fmt.Errorf("live presentation slot has hole: ordinal=%d page=%d offset=%d entries=%d: %w", ordinal, page, offset, len(doc.Entries), ErrShardedArchiveInvalid)
		}
	}
	doc.Version, doc.TrackID, doc.Number = 1, record.Segment.TrackID, page
	if err := s.saveV2JSON(ctx, id, pagePath, doc, archiveShardMaxBytes); err != nil {
		return fmt.Errorf("save live presentation page: ordinal=%d entries=%d: %w", ordinal, len(doc.Entries), err)
	}
	return nil
}

func sameGapSourceCoordinate(left, right archiveindex.Coordinate) bool {
	return left.TrackID == right.TrackID && left.SourceEpoch == right.SourceEpoch &&
		left.DiscontinuitySequence == right.DiscontinuitySequence && left.Sequence == right.Sequence &&
		left.Kind == right.Kind
}

func sameV2LiveEntry(left, right v2LiveIndexEntry) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (s *Store) loadLiveEntry(ctx context.Context, id, trackID string, ordinal uint64) (v2LiveIndexEntry, error) {
	page, offset := pageSlot(ordinal)
	var doc v2LivePage
	if err := s.loadV2JSON(ctx, id, v2LivePagePath(trackID, page), archiveShardMaxBytes, &doc); err != nil {
		return v2LiveIndexEntry{}, err
	}
	if doc.Version != 1 || doc.TrackID != trackID || doc.Number != page || int(offset) >= len(doc.Entries) {
		return v2LiveIndexEntry{}, ErrShardedArchiveInvalid
	}
	entry := doc.Entries[offset]
	if entry.PresentationOrdinal != ordinal {
		return v2LiveIndexEntry{}, ErrShardedArchiveInvalid
	}
	if entry.Reserved {
		if entry.Gap != nil || entry.ArchiveOrdinal != 0 {
			return v2LiveIndexEntry{}, ErrShardedArchiveInvalid
		}
		return entry, nil
	}
	if (entry.Gap == nil) == (entry.ArchiveOrdinal == 0) {
		return v2LiveIndexEntry{}, ErrShardedArchiveInvalid
	}
	if entry.Gap != nil {
		if entry.Gap.TrackID != trackID || entry.Gap.LivePresentationOrdinal != ordinal || entry.Gap.FromSequence != entry.Gap.ToSequence || entry.Coordinate.TrackID != trackID || entry.Coordinate.Sequence != entry.Gap.FromSequence || entry.Coordinate.SourceEpoch != entry.Gap.SourceEpoch || entry.Coordinate.DiscontinuitySequence != entry.Gap.DiscontinuitySequence {
			return v2LiveIndexEntry{}, ErrShardedArchiveInvalid
		}
	} else if entry.Coordinate.TrackID != trackID || entry.Coordinate.Kind != archiveindex.ObjectMedia {
		return v2LiveIndexEntry{}, ErrShardedArchiveInvalid
	}
	return entry, nil
}

func (s *Store) lookupCoordinateLocked(ctx context.Context, header *domain.Recording, coordinate archiveindex.Coordinate) (V2MediaRecord, error) {
	coord, err := canonicalV2Coordinate(coordinate)
	if err != nil {
		return V2MediaRecord{}, err
	}
	track := header.Tracks[coord.TrackID]
	if track == nil {
		return V2MediaRecord{}, ErrNotFound
	}
	path, err := v2CoordinatePath(coord)
	if err != nil {
		return V2MediaRecord{}, err
	}
	var page v2TimelinePage
	if err := s.loadV2JSON(ctx, header.ID, path, archiveShardMaxBytes, &page); err != nil {
		return V2MediaRecord{}, err
	}
	if err := validateV2TimelinePage(page, coord.TrackID, coord); err != nil {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	var entry *v2TimelineEntry
	for index := range page.Entries {
		if page.Entries[index].Coordinate == coord {
			entry = &page.Entries[index]
			break
		}
	}
	if entry == nil {
		return V2MediaRecord{}, ErrNotFound
	}
	if coord.Kind == archiveindex.ObjectMedia {
		if entry.ArchiveOrdinal == 0 || entry.ArchiveOrdinal > track.MediaHighWater {
			return V2MediaRecord{}, ErrNotFound
		}
		return s.lookupMediaPageRecord(ctx, header.ID, coord, false, entry.ArchiveOrdinal)
	}
	if entry.InitOrdinal == 0 || entry.InitOrdinal > track.InitCount {
		return V2MediaRecord{}, ErrNotFound
	}
	return s.lookupMediaPageRecord(ctx, header.ID, coord, true, entry.InitOrdinal)
}

func (s *Store) lookupMediaPageRecord(ctx context.Context, id string, coordinate archiveindex.Coordinate, init bool, ordinal uint64) (V2MediaRecord, error) {
	pageNo, offset := mediaPageSlot(ordinal)
	page, err := s.loadV2MediaPage(ctx, id, coordinate.TrackID, init, pageNo)
	if err != nil {
		return V2MediaRecord{}, err
	}
	if int(offset) >= len(page.Entries) {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	record := page.Entries[offset]
	if record.Coordinate != coordinate || record.Segment.IsInit != init || init && record.IndexOrdinal != ordinal || !init && record.Segment.ArchiveOrdinal != ordinal {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	return record, nil
}

func (s *Store) writeV2TimelineEntry(ctx context.Context, id string, record V2MediaRecord) error {
	entry, err := timelineEntryForRecord(record)
	if err != nil {
		return err
	}
	coordinate := entry.Coordinate
	pagePath, err := v2CoordinatePath(coordinate)
	if err != nil {
		return err
	}
	page := v2TimelinePage{
		Version: 1, TrackID: coordinate.TrackID, SourceEpoch: coordinate.SourceEpoch,
		DiscontinuitySequence: coordinate.DiscontinuitySequence,
		SequenceBucket:        coordinate.Sequence / mediaShardMaxEntries, Kind: coordinate.Kind,
	}
	if err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &page); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		page = v2TimelinePage{
			Version: 1, TrackID: coordinate.TrackID, SourceEpoch: coordinate.SourceEpoch,
			DiscontinuitySequence: coordinate.DiscontinuitySequence,
			SequenceBucket:        coordinate.Sequence / mediaShardMaxEntries, Kind: coordinate.Kind,
		}
	}
	if err := validateV2TimelinePage(page, coordinate.TrackID, coordinate); err != nil {
		return err
	}
	for index := range page.Entries {
		if page.Entries[index].Coordinate != coordinate {
			continue
		}
		prior := page.Entries[index]
		if prior.ID != entry.ID || prior.ArchiveOrdinal != entry.ArchiveOrdinal || prior.InitOrdinal != entry.InitOrdinal {
			return ErrShardedArchiveConflict
		}
		page.Entries[index] = entry
		return s.saveV2TimelinePage(ctx, id, pagePath, page)
	}
	if len(page.Entries) >= mediaShardMaxEntries {
		return ErrShardedArchiveInvalid
	}
	page.Entries = append(page.Entries, entry)
	sort.Slice(page.Entries, func(i, j int) bool {
		return page.Entries[i].Coordinate.Sequence < page.Entries[j].Coordinate.Sequence
	})
	return s.saveV2TimelinePage(ctx, id, pagePath, page)
}

func (s *Store) saveV2TimelinePage(ctx context.Context, id, pagePath string, page v2TimelinePage) error {
	data, err := json.Marshal(page)
	if err != nil || len(data)+1 > archiveShardMaxBytes || len(page.Entries) > mediaShardMaxEntries {
		return ErrShardedArchiveInvalid
	}
	return s.saveV2JSON(ctx, id, pagePath, page, archiveShardMaxBytes)
}

func validateV2TimelinePage(page v2TimelinePage, trackID string, coordinate archiveindex.Coordinate) error {
	if page.Version != 1 || page.TrackID != trackID || page.Kind != coordinate.Kind || page.SourceEpoch != coordinate.SourceEpoch || page.DiscontinuitySequence != coordinate.DiscontinuitySequence || page.SequenceBucket != coordinate.Sequence/mediaShardMaxEntries || len(page.Entries) > mediaShardMaxEntries {
		return ErrShardedArchiveInvalid
	}
	for index, entry := range page.Entries {
		coord, err := canonicalV2Coordinate(entry.Coordinate)
		if err != nil || coord != entry.Coordinate || coord.TrackID != trackID || coord.Kind != page.Kind || coord.SourceEpoch != page.SourceEpoch || coord.DiscontinuitySequence != page.DiscontinuitySequence || coord.Sequence/mediaShardMaxEntries != page.SequenceBucket || entry.ID == "" || !isFiniteNonnegative(entry.Duration) {
			return ErrShardedArchiveInvalid
		}
		if index > 0 && page.Entries[index-1].Coordinate.Sequence >= coord.Sequence {
			return ErrShardedArchiveInvalid
		}
		if page.Kind == archiveindex.ObjectMedia && (entry.ArchiveOrdinal == 0 || entry.InitOrdinal != 0) || page.Kind == archiveindex.ObjectInit && (entry.ArchiveOrdinal != 0 || entry.InitOrdinal == 0) {
			return ErrShardedArchiveInvalid
		}
	}
	return nil
}

func timelineEntryForRecord(record V2MediaRecord) (v2TimelineEntry, error) {
	coordinate, err := canonicalV2Coordinate(record.Coordinate)
	if err != nil || coordinate != record.Coordinate || record.Segment.ID == "" || record.Segment.TrackID != coordinate.TrackID {
		return v2TimelineEntry{}, ErrShardedArchiveInvalid
	}
	entry := v2TimelineEntry{
		Coordinate: coordinate, ID: record.Segment.ID, Duration: record.Segment.Duration,
		ProgramDateTime: record.Segment.ProgramDateTime, InitSegmentID: record.Segment.InitSegmentID,
		Discontinuity: record.Segment.Discontinuity, ByteRange: record.Segment.ByteRange,
		LivePresentationOrdinal: record.Segment.LivePresentationOrdinal,
		LiveDiscontinuity:       record.Segment.LiveDiscontinuity,
		LiveDiscontinuitySeq:    record.Segment.LiveDiscontinuitySequence,
	}
	if coordinate.Kind == archiveindex.ObjectInit {
		entry.InitOrdinal = record.IndexOrdinal
	} else {
		entry.ArchiveOrdinal = record.Segment.ArchiveOrdinal
	}
	return entry, nil
}

func (s *Store) readPointerRecord(ctx context.Context, id string, pointer v2IndexPointer) (V2MediaRecord, error) {
	if pointer.Version != 1 || pointer.TrackID == "" || pointer.IndexOrdinal == 0 || pointer.Offset >= mediaShardMaxEntries {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	wantPage, wantOffset := mediaPageSlot(pointer.IndexOrdinal)
	if pointer.Page != wantPage || pointer.Offset != wantOffset || pointer.Coordinate.TrackID != pointer.TrackID || pointer.Coordinate.Kind != pointer.Kind {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	if pointer.Kind == archiveindex.ObjectMedia && (pointer.ArchiveOrdinal != pointer.IndexOrdinal || pointer.ArchiveOrdinal == 0) || pointer.Kind == archiveindex.ObjectInit && pointer.ArchiveOrdinal != 0 {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	var page v2MediaPage
	if err := s.loadV2JSON(ctx, id, v2MediaPagePath(pointer.TrackID, pointer.Kind == archiveindex.ObjectInit, pointer.Page), archiveShardMaxBytes, &page); err != nil {
		return V2MediaRecord{}, err
	}
	if page.Version != 1 || page.TrackID != pointer.TrackID || page.Number != pointer.Page || int(pointer.Offset) >= len(page.Entries) {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	record := page.Entries[pointer.Offset]
	coord, coordErr := canonicalV2Coordinate(record.Coordinate)
	segment := record.Segment
	if coordErr != nil || coord != pointer.Coordinate || record.IndexOrdinal != pointer.IndexOrdinal || segment.TrackID != pointer.TrackID || segment.Sequence != coord.Sequence || segment.SourceEpoch != coord.SourceEpoch || segment.DiscontinuitySequence != coord.DiscontinuitySequence || segment.IsInit != (pointer.Kind == archiveindex.ObjectInit) || !sameV2Media(record, pointer.Record) || pointer.Kind == archiveindex.ObjectMedia && (segment.ArchiveOrdinal != pointer.ArchiveOrdinal || segment.ID != fmt.Sprintf("seg-%020d", pointer.ArchiveOrdinal)) || pointer.Kind == archiveindex.ObjectInit && segment.ArchiveOrdinal != 0 {
		return V2MediaRecord{}, ErrShardedArchiveInvalid
	}
	return record, nil
}

func (s *Store) loadV2MediaPage(ctx context.Context, id, trackID string, init bool, page uint64) (v2MediaPage, error) {
	var doc v2MediaPage
	if err := s.loadV2JSON(ctx, id, v2MediaPagePath(trackID, init, page), archiveShardMaxBytes, &doc); err != nil {
		return v2MediaPage{}, err
	}
	if doc.Version != 1 || doc.TrackID != trackID || doc.Number != page || len(doc.Entries) > mediaShardMaxEntries {
		return v2MediaPage{}, ErrShardedArchiveInvalid
	}
	return doc, nil
}

func (s *Store) saveHeaderLocked(ctx context.Context, supplied, known *domain.Recording) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if err := validateV2Header(supplied); err != nil {
		return err
	}
	merged := cloneRecordingHeader(supplied)
	if known == nil {
		var err error
		known, err = s.loadRecordingContext(ctx, supplied.ID)
		if err != nil {
			return err
		}
	}
	if known.ID != merged.ID || known.FormatVersion != ShardedArchiveFormatVersion {
		return ErrShardedArchiveInvalid
	}
	if merged.ArchiveRevision < known.ArchiveRevision || merged.TimelineRevision < known.TimelineRevision {
		return ErrShardedArchiveConflict
	}
	mergeV2HighWater(merged, known)
	if !known.ArchiveSealed && merged.ArchiveSealed && known.ShardedArchive != nil && known.ShardedArchive.ClaimReconcilePending {
		return ErrArchiveRecoveryPending
	}
	if known.ArchiveSealed && !sealedRootUpdateAllowed(known, merged) {
		return ErrArchiveSealed
	}
	if !known.ArchiveSealed && merged.ArchiveSealed && !sameV2RootExcept(known, merged, func(recording *domain.Recording) {
		recording.ArchiveSealed = known.ArchiveSealed
	}) {
		return ErrShardedArchiveInvalid
	}
	data, err := json.Marshal(merged)
	if err != nil || len(data)+1 > archiveRootMaxBytes {
		return ErrShardedArchiveInvalid
	}
	if writer, ok := s.StorageBackend.(v2ContextWriter); ok {
		return writer.SaveRecordingContext(ctx, merged)
	}
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	return s.StorageBackend.SaveRecording(merged)
}

// sealedRootUpdateAllowed preserves capture lifecycle independence while
// freezing canonical archive state. After sealing, the only allowed root
// transition is stopped -> completed; archive content, counters, revisions,
// and the seal bit itself remain immutable.
func sealedRootUpdateAllowed(current, next *domain.Recording) bool {
	if current == nil || next == nil || !current.ArchiveSealed || !next.ArchiveSealed {
		return false
	}
	if current.State != next.State && !(current.State == domain.StateStopped && next.State == domain.StateCompleted) {
		return false
	}
	return sameV2RootExcept(current, next, func(recording *domain.Recording) {
		recording.State = current.State
	})
}

func sameV2RootExcept(left, right *domain.Recording, normalize func(*domain.Recording)) bool {
	leftCopy, rightCopy := cloneRecordingHeader(left), cloneRecordingHeader(right)
	if leftCopy == nil || rightCopy == nil {
		return false
	}
	normalize(leftCopy)
	normalize(rightCopy)
	leftData, leftErr := json.Marshal(leftCopy)
	rightData, rightErr := json.Marshal(rightCopy)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftData, rightData)
}

func (s *Store) loadRecordingContext(ctx context.Context, id string) (*domain.Recording, error) {
	if err := checkV2Context(ctx); err != nil {
		return nil, err
	}
	if reader, ok := s.StorageBackend.(v2ContextReader); ok {
		return reader.LoadRecordingContext(ctx, id)
	}
	return s.StorageBackend.LoadRecordingReadOnly(id)
}

func (s *Store) saveV2JSON(ctx context.Context, id, relative string, value any, maximum int64) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil || int64(len(data)+1) > maximum {
		return ErrShardedArchiveInvalid
	}
	if writer, ok := s.StorageBackend.(v2SidecarContextWriter); ok {
		return writer.SaveSidecarContext(ctx, id, relative, value)
	}
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	return s.StorageBackend.SaveSidecar(id, relative, value)
}

func (s *Store) loadV2JSON(ctx context.Context, id, relative string, maximum int64, output any) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	if reader, ok := s.StorageBackend.(v2SidecarContextReader); ok {
		return mapV2ReadError(reader.LoadSidecarContext(ctx, id, relative, maximum, output))
	}
	return mapV2ReadError(s.LoadSidecar(id, relative, maximum, output))
}

func mapV2ReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrObjectNotFound) || errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func (s *Store) listV2(ctx context.Context, id, prefix string, visit func(string) error) error {
	lister, ok := s.StorageBackend.(v2SidecarLister)
	if !ok {
		return ErrShardedArchiveUnavailable
	}
	cursor := ""
	for {
		if err := checkV2Context(ctx); err != nil {
			return err
		}
		page, err := lister.ListSidecarsContext(ctx, id, prefix, cursor, archiveSidecarPageSize)
		if err != nil {
			return err
		}
		previous := cursor
		for _, relative := range page.Paths {
			if relative <= previous || !strings.HasPrefix(relative, prefix) || !strings.HasSuffix(relative, ".json") {
				return ErrShardedArchiveInvalid
			}
			if err := visit(relative); err != nil {
				return err
			}
			previous = relative
		}
		if len(page.Paths) > archiveSidecarPageSize {
			return ErrShardedArchiveInvalid
		}
		if page.NextCursor == "" {
			return nil
		}
		if page.NextCursor != previous || page.NextCursor == cursor {
			return ErrShardedArchiveInvalid
		}
		cursor = page.NextCursor
	}
}

func (s *Store) putIdempotentPointer(ctx context.Context, id, relative string, pointer v2IndexPointer) error {
	var prior v2IndexPointer
	err := s.loadV2JSON(ctx, id, relative, archiveShardMaxBytes, &prior)
	if err == nil {
		if !sameV2Pointer(prior, pointer) {
			return ErrShardedArchiveConflict
		}
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	return s.saveV2JSON(ctx, id, relative, pointer, archiveShardMaxBytes)
}

func sameV2Pointer(left, right v2IndexPointer) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (s *Store) verifyV2Payload(ctx context.Context, id string, segment domain.Segment) error {
	if err := checkV2Context(ctx); err != nil {
		return err
	}
	info, err := s.StorageBackend.StatPayload(id, segment.StoragePath)
	if err != nil {
		return err
	}
	if info.Size != segment.PayloadSize || !info.Regular {
		return ErrShardedArchiveConflict
	}
	reader, err := s.StorageBackend.OpenPayloadReader(id, segment.StoragePath)
	if err != nil {
		return err
	}
	defer reader.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != segment.SHA256 {
		return ErrShardedArchiveConflict
	}
	return nil
}

func (s *Store) v2Lock(id string) *sync.Mutex {
	hash := sha256.Sum256([]byte(id))
	return &s.v2Locks[int(hash[0])%len(s.v2Locks)]
}

func validateV2Header(recording *domain.Recording) error {
	if recording == nil || recording.FormatVersion != ShardedArchiveFormatVersion || !recordingIDPattern.MatchString(recording.ID) || recording.ShardedArchive == nil {
		return ErrShardedArchiveInvalid
	}
	if len(recording.Gaps) != 0 || len(recording.Snapshots) != 0 || len(recording.MetadataTimeline) != 0 {
		return ErrShardedArchiveInvalid
	}
	if recording.Tracks == nil || len(recording.Tracks) == 0 {
		return ErrShardedArchiveInvalid
	}
	for id, track := range recording.Tracks {
		if track == nil || id == "" || track.ID != id || len(track.Segments) != 0 || len(track.InitSegments) != 0 || len(track.PendingSegments) != 0 || len(track.PendingSequences) != 0 {
			return ErrShardedArchiveInvalid
		}
	}
	data, err := json.Marshal(recording)
	if err != nil || len(data)+1 > archiveRootMaxBytes {
		return ErrShardedArchiveInvalid
	}
	return nil
}

func validateV2Record(header *domain.Recording, record V2MediaRecord) error {
	coord, err := canonicalV2Coordinate(record.Coordinate)
	if err != nil {
		return err
	}
	segment := record.Segment
	if segment.ID == "" || len(segment.ID) > archiveindex.MaxClaimIDBytes || segment.TrackID != coord.TrackID || segment.Sequence != coord.Sequence || segment.SourceEpoch != coord.SourceEpoch || segment.DiscontinuitySequence != coord.DiscontinuitySequence || segment.IsInit != (coord.Kind == archiveindex.ObjectInit) || segment.SourceURI == "" || len(segment.SourceURI) > maxV2SourceURIBytes || segment.StoragePath == "" || len(segment.StoragePath) > archiveindex.MaxPayloadPathBytes || !canonicalRelativePath(segment.StoragePath) || segment.PayloadSize < 0 || !validLowerDigest(segment.SHA256) {
		return ErrShardedArchiveInvalid
	}
	if segment.IsInit {
		if segment.ArchiveOrdinal != 0 {
			return ErrShardedArchiveInvalid
		}
	} else if segment.ArchiveOrdinal == 0 || segment.ID != fmt.Sprintf("seg-%020d", segment.ArchiveOrdinal) {
		return ErrShardedArchiveInvalid
	}
	if header.Tracks[coord.TrackID] == nil || header.SourceSessionID != "" && coord.SessionID != header.SourceSessionID {
		return ErrShardedArchiveInvalid
	}
	if !isFiniteNonnegative(segment.Duration) || segment.ProgramDateTime != nil && segment.ProgramDateTime.IsZero() {
		return ErrShardedArchiveInvalid
	}
	if record.SelectedClaim != nil {
		claim := *record.SelectedClaim
		set := V2ClaimSet{SegmentID: "", Coordinate: coord, SelectedClaimID: claim.ID, State: record.ClaimState, Claims: []archiveindex.Claim{claim}}
		set.SegmentID, err = archiveindex.SegmentIdentity(coord)
		if err != nil || validateV2ClaimSet(set) != nil || claim.PayloadPath != segment.StoragePath || claim.Size != segment.PayloadSize || claim.SHA256 != segment.SHA256 || claim.Verification != archiveindex.VerificationVerified {
			return ErrShardedArchiveInvalid
		}
	} else if record.ClaimState != "" && record.ClaimState != archiveindex.CoverageUnknown {
		return ErrShardedArchiveInvalid
	}
	return nil
}

func canonicalV2Coordinate(coordinate archiveindex.Coordinate) (archiveindex.Coordinate, error) {
	if coordinate.Kind == "" {
		coordinate.Kind = archiveindex.ObjectMedia
	}
	if coordinate.Kind != archiveindex.ObjectMedia && coordinate.Kind != archiveindex.ObjectInit {
		return archiveindex.Coordinate{}, ErrShardedArchiveInvalid
	}
	if _, err := archiveindex.SegmentIdentity(coordinate); err != nil {
		return archiveindex.Coordinate{}, ErrShardedArchiveInvalid
	}
	return coordinate, nil
}

func sameV2Media(left, right V2MediaRecord) bool {
	left.Segment.TimelineOrdinal = 0
	right.Segment.TimelineOrdinal = 0
	left.IndexOrdinal = 0
	right.IndexOrdinal = 0
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && string(a) == string(b)
}

// sameV2MediaRetry accepts the same selected canonical claim when a retry
// reconstructs its observation timestamp after pages were durable but before
// the root visibility marker committed. Claim identity and every other field
// must still match; the first persisted AcquiredAt remains authoritative.
func sameV2MediaRetry(existing, retry V2MediaRecord) bool {
	if existing.SelectedClaim == nil || retry.SelectedClaim == nil || existing.SelectedClaim.ID != retry.SelectedClaim.ID {
		return false
	}
	first := *existing.SelectedClaim
	second := *retry.SelectedClaim
	second.AcquiredAt = first.AcquiredAt
	retry.SelectedClaim = &second
	return sameV2Media(existing, retry)
}

func mergeV2HighWater(target, source *domain.Recording) {
	if target.ShardedArchive == nil {
		target.ShardedArchive = &domain.ShardedArchiveSummary{}
	}
	if source.ShardedArchive != nil {
		mergeUint64(&target.ShardedArchive.MediaCount, source.ShardedArchive.MediaCount)
		mergeUint64(&target.ShardedArchive.InitCount, source.ShardedArchive.InitCount)
		mergeUint64(&target.ShardedArchive.GapCount, source.ShardedArchive.GapCount)
		mergeUint64(&target.ShardedArchive.ManifestSnapshotCount, source.ShardedArchive.ManifestSnapshotCount)
		mergeUint64(&target.ShardedArchive.MetadataRevisionCount, source.ShardedArchive.MetadataRevisionCount)
		mergeUint64(&target.ShardedArchive.ClaimCount, source.ShardedArchive.ClaimCount)
		mergeUint64(&target.ShardedArchive.CoverageObservationCount, source.ShardedArchive.CoverageObservationCount)
		mergeUint64(&target.ShardedArchive.PayloadBytes, source.ShardedArchive.PayloadBytes)
		if source.ShardedArchive.DurationSeconds > target.ShardedArchive.DurationSeconds {
			target.ShardedArchive.DurationSeconds = source.ShardedArchive.DurationSeconds
		}
	}
	for id, oldTrack := range source.Tracks {
		newTrack := target.Tracks[id]
		if newTrack == nil {
			continue
		}
		mergeUint64(&newTrack.MediaCount, oldTrack.MediaCount)
		mergeUint64(&newTrack.InitCount, oldTrack.InitCount)
		mergeUint64(&newTrack.MediaHighWater, oldTrack.MediaHighWater)
		mergeUint64(&newTrack.LiveSlotHighWater, oldTrack.LiveSlotHighWater)
		mergeUint64(&newTrack.PayloadBytes, oldTrack.PayloadBytes)
		if oldTrack.DurationSeconds > newTrack.DurationSeconds {
			newTrack.DurationSeconds = oldTrack.DurationSeconds
		}
		mergeUint64(&newTrack.NextArchiveOrdinal, oldTrack.NextArchiveOrdinal)
		if oldTrack.LivePresentation != nil {
			if newTrack.LivePresentation == nil {
				copy := *oldTrack.LivePresentation
				newTrack.LivePresentation = &copy
			} else if newTrack.LivePresentation.NextOrdinal < oldTrack.LivePresentation.NextOrdinal {
				// The caller's source-epoch mapping fields are authoritative when
				// the cursor advances. A stale cursor cannot move backward.
				copy := *oldTrack.LivePresentation
				*newTrack.LivePresentation = copy
			}
		}
	}
}

func advanceV2Summary(header *domain.Recording, record V2MediaRecord) error {
	if header.ShardedArchive == nil {
		header.ShardedArchive = &domain.ShardedArchiveSummary{}
	}
	track := header.Tracks[record.Segment.TrackID]
	if track == nil {
		return ErrShardedArchiveInvalid
	}
	bytes := uint64(record.Segment.PayloadSize)
	duration := record.Segment.Duration
	if math.IsNaN(duration) || math.IsInf(duration, 0) || duration < 0 || ^uint64(0)-track.PayloadBytes < bytes || ^uint64(0)-header.ShardedArchive.PayloadBytes < bytes || math.IsInf(track.DurationSeconds+duration, 0) || math.IsInf(header.ShardedArchive.DurationSeconds+duration, 0) {
		return ErrShardedArchiveInvalid
	}
	track.PayloadBytes += bytes
	header.ShardedArchive.PayloadBytes += bytes
	track.DurationSeconds += duration
	header.ShardedArchive.DurationSeconds += duration
	if record.Segment.IsInit {
		track.InitCount++
		header.ShardedArchive.InitCount++
	} else {
		track.MediaCount++
		header.ShardedArchive.MediaCount++
		track.MediaHighWater = record.Segment.ArchiveOrdinal
		if record.Segment.ArchiveOrdinal == ^uint64(0) {
			track.NextArchiveOrdinal = ^uint64(0)
		} else {
			track.NextArchiveOrdinal = record.Segment.ArchiveOrdinal + 1
		}
		if err := reconcileV2LivePresentation(track, record.Segment); err != nil {
			return err
		}
		mergeUint64(&track.LiveSlotHighWater, record.Segment.LivePresentationOrdinal)
	}
	if record.SelectedClaim != nil {
		if header.ShardedArchive.ClaimCount == ^uint64(0) {
			return ErrShardedArchiveInvalid
		}
		header.ShardedArchive.ClaimCount++
	}
	return nil
}

// reconcileV2LivePresentation reconstructs the bounded cursor from a durable
// media record when a crash occurred after its page/index publication but
// before the root high-water update. Caller publishes this state with the
// recovered root. Existing larger reservation cursors are preserved.
func reconcileV2LivePresentation(track *domain.Track, segment domain.Segment) error {
	ordinal := segment.LivePresentationOrdinal
	if ordinal == 0 {
		return nil
	}
	if ordinal == ^uint64(0) {
		return ErrShardedArchiveInvalid
	}
	if track.LivePresentation == nil {
		track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 1}
	}
	state := track.LivePresentation
	if state.NextOrdinal == 0 {
		state.NextOrdinal = 1
	}
	if state.NextOrdinal <= ordinal {
		state.NextOrdinal = ordinal + 1
	}
	if state.FirstPresentationOrdinal == 0 || ordinal < state.FirstPresentationOrdinal {
		state.FirstPresentationOrdinal = ordinal
	}
	if !state.HasEpochMapping || segment.SourceEpoch > state.MappedSourceEpoch {
		epochBoundary := state.HasEpochMapping || state.HasLastCoordinate
		state.HasEpochMapping = true
		state.MappedSourceEpoch = segment.SourceEpoch
		state.SourceSequenceBase = segment.Sequence
		state.PresentationOrdinalBase = ordinal
		state.MaxSourceSequence = segment.Sequence
		state.SourceDiscontinuityBase = segment.DiscontinuitySequence
		state.PresentationDiscBase = segment.LiveDiscontinuitySequence
		if segment.LiveDiscontinuity && state.PresentationDiscBase < ^uint64(0) {
			state.PresentationDiscBase++
		}
		state.EpochBoundary = epochBoundary
	} else if segment.SourceEpoch == state.MappedSourceEpoch {
		if segment.Sequence >= state.SourceSequenceBase && segment.Sequence > state.MaxSourceSequence {
			state.MaxSourceSequence = segment.Sequence
		}
	}
	if !state.HasLastCoordinate || ordinal >= track.LiveSlotHighWater {
		state.HasLastCoordinate = true
		state.LastSourceEpoch = segment.SourceEpoch
		state.LastDiscontinuitySequence = segment.DiscontinuitySequence
		state.LastSequence = segment.Sequence
	}
	effectiveDiscontinuity := segment.LiveDiscontinuitySequence
	if segment.LiveDiscontinuity && effectiveDiscontinuity < ^uint64(0) {
		effectiveDiscontinuity++
	}
	if effectiveDiscontinuity > state.DiscontinuitySequence {
		state.DiscontinuitySequence = effectiveDiscontinuity
	}
	return nil
}

func mergeUint64(target *uint64, source uint64) {
	if source > *target {
		*target = source
	}
}

func validateV2ClaimSet(set V2ClaimSet) error {
	if !validV2OpaqueID(set.SegmentID) || len(set.Claims) == 0 || len(set.Claims) > archiveindex.MaxClaimsPerSegment {
		return ErrShardedArchiveInvalid
	}
	switch set.State {
	case archiveindex.CoverageUnknown, archiveindex.CoverageKnownMissing, archiveindex.CoverageAcquisitionFailed, archiveindex.CoveragePresent, archiveindex.CoverageConflict:
	default:
		return ErrShardedArchiveInvalid
	}
	identity, err := archiveindex.SegmentIdentity(set.Coordinate)
	if err != nil || identity != set.SegmentID {
		return ErrShardedArchiveInvalid
	}
	seen := make(map[string]struct{}, len(set.Claims))
	selectedVerified := false
	for _, claim := range set.Claims {
		if claim.ID == "" || claim.Size < 0 || claim.SHA256 != "" && !validLowerDigest(claim.SHA256) {
			return ErrShardedArchiveInvalid
		}
		if claim.ID == set.SelectedClaimID && claim.Verification == archiveindex.VerificationVerified {
			selectedVerified = true
		}
		if _, ok := seen[claim.ID]; ok {
			return ErrShardedArchiveConflict
		}
		seen[claim.ID] = struct{}{}
	}
	if set.CountedClaims > uint64(len(set.Claims)) || set.SelectedClaimID != "" && !selectedVerified || set.State == archiveindex.CoveragePresent && set.SelectedClaimID == "" || set.State == archiveindex.CoverageUnknown && set.SelectedClaimID != "" {
		return ErrShardedArchiveInvalid
	}
	data, err := json.Marshal(set)
	if err != nil || len(data)+1 > archiveShardMaxBytes {
		return ErrShardedArchiveInvalid
	}
	return nil
}

func claimSetContains(set V2ClaimSet, claimID string) bool {
	for _, claim := range set.Claims {
		if claim.ID == claimID {
			return true
		}
	}
	return false
}

func mergeV2ClaimSet(existing *V2ClaimSet, incoming V2ClaimSet, replaceSelection bool) error {
	if err := validateV2ClaimSet(incoming); err != nil {
		return err
	}
	byID := make(map[string]archiveindex.Claim, len(existing.Claims)+len(incoming.Claims))
	for _, claim := range existing.Claims {
		byID[claim.ID] = claim
	}
	for _, claim := range incoming.Claims {
		if prior, ok := byID[claim.ID]; ok {
			if !sameV2ClaimIdentity(prior, claim) {
				return fmt.Errorf("claim ID reused with different immutable metadata: %w", ErrShardedArchiveConflict)
			}
			continue
		}
		byID[claim.ID] = claim
	}
	if len(byID) > archiveindex.MaxClaimsPerSegment {
		return ErrShardedArchiveInvalid
	}
	merged := make([]archiveindex.Claim, 0, len(byID))
	for _, claim := range byID {
		merged = append(merged, claim)
	}
	sort.Slice(merged, func(i, j int) bool {
		if !merged[i].AcquiredAt.Equal(merged[j].AcquiredAt) {
			return merged[i].AcquiredAt.Before(merged[j].AcquiredAt)
		}
		return merged[i].ID < merged[j].ID
	})
	existing.Claims = merged
	if replaceSelection {
		existing.SelectedClaimID = incoming.SelectedClaimID
		existing.State = incoming.State
	} else if existing.SelectedClaimID != incoming.SelectedClaimID {
		return fmt.Errorf("visible claim selection changed: %w", ErrShardedArchiveConflict)
	} else {
		existing.State = incoming.State
	}
	if err := validateV2ClaimSet(*existing); err != nil {
		return err
	}
	return nil
}

func sameV2ClaimIdentity(left, right archiveindex.Claim) bool {
	// AcquiredAt is the first-observation timestamp. A retry after a crash may
	// reconstruct the same stable claim ID with a new attempt timestamp; retain
	// the already-published timestamp while requiring every payload/provenance
	// field to match.
	left.AcquiredAt = time.Time{}
	right.AcquiredAt = time.Time{}
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (s *Store) writeV2PageRecord(ctx context.Context, id string, record V2MediaRecord, init bool, ordinal uint64) (v2IndexPointer, error) {
	page, offset := mediaPageSlot(ordinal)
	pagePath := v2MediaPagePath(record.Segment.TrackID, init, page)
	var doc v2MediaPage
	err := s.loadV2JSON(ctx, id, pagePath, archiveShardMaxBytes, &doc)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return v2IndexPointer{}, err
	}
	if errors.Is(err, ErrNotFound) {
		doc = v2MediaPage{Version: 1, TrackID: record.Segment.TrackID, Number: page}
	}
	if int(offset) < len(doc.Entries) {
		if !sameV2Media(doc.Entries[offset], record) {
			return v2IndexPointer{}, ErrShardedArchiveConflict
		}
	} else if int(offset) == len(doc.Entries) {
		doc.Entries = append(doc.Entries, record)
	} else {
		return v2IndexPointer{}, ErrShardedArchiveInvalid
	}
	data, err := json.Marshal(doc)
	if err != nil || len(data)+1 > archiveShardMaxBytes || len(doc.Entries) > mediaShardMaxEntries {
		return v2IndexPointer{}, ErrShardedArchiveInvalid
	}
	if err := s.saveV2JSON(ctx, id, pagePath, doc, archiveShardMaxBytes); err != nil {
		return v2IndexPointer{}, err
	}
	pointer := v2IndexPointer{Version: 1, Coordinate: record.Coordinate, TrackID: record.Segment.TrackID, Kind: record.Coordinate.Kind, Page: page, Offset: offset, IndexOrdinal: ordinal, ArchiveOrdinal: record.Segment.ArchiveOrdinal}
	if err := s.writeV2TimelineEntry(ctx, id, record); err != nil {
		return v2IndexPointer{}, err
	}
	if init {
		if err := s.putIdempotentPointer(ctx, id, v2IDPath(record.Segment.ID), pointer); err != nil {
			return v2IndexPointer{}, err
		}
	}
	if !init && record.Segment.LivePresentationOrdinal > 0 {
		header, err := s.loadRecordingContext(ctx, id)
		if err != nil {
			return v2IndexPointer{}, err
		}
		if err := s.writeLiveIndex(ctx, id, record, header.Tracks[record.Segment.TrackID]); err != nil {
			return v2IndexPointer{}, err
		}
	}
	return pointer, nil
}

func (s *Store) listV2Entries(ctx context.Context, id, prefix string) ([]string, error) {
	// Kept only for deterministic diagnostics/tests; production iterators use
	// listV2 and never accumulate the whole history.
	paths := []string{}
	err := s.listV2(ctx, id, prefix, func(relative string) error {
		paths = append(paths, relative)
		return nil
	})
	return paths, err
}

func pageSlot(ordinal uint64) (uint64, uint16) {
	if ordinal == 0 {
		return 0, 0
	}
	zero := ordinal - 1
	return zero / ArchiveShardMaxEntries, uint16(zero % ArchiveShardMaxEntries)
}

func pagedV2MaxEntries(collection string) uint64 {
	switch collection {
	case "metadata":
		return metadataShardMaxEntries
	case "manifests":
		return manifestShardMaxEntries
	default:
		return ArchiveShardMaxEntries
	}
}

func mediaPageSlot(ordinal uint64) (uint64, uint16) {
	return pageSlotWithEntries(ordinal, mediaShardMaxEntries)
}

func pageSlotWithEntries(ordinal, maxEntries uint64) (uint64, uint16) {
	if ordinal == 0 || maxEntries == 0 {
		return 0, 0
	}
	zero := ordinal - 1
	return zero / maxEntries, uint16(zero % maxEntries)
}

func v2TrackKey(trackID string) string {
	digest := sha256.Sum256([]byte(trackID))
	return hex.EncodeToString(digest[:16])
}

func v2MediaPagePath(trackID string, init bool, pageNo uint64) string {
	collection := "media"
	if init {
		collection = "init"
	}
	return fmt.Sprintf("archive/v2/%s/%s/%020d", collection, v2TrackKey(trackID), pageNo)
}

func v2LivePagePath(trackID string, pageNo uint64) string {
	return fmt.Sprintf("archive/v2/live/%s/%020d", v2TrackKey(trackID), pageNo)
}

func v2CoordinatePath(coordinate archiveindex.Coordinate) (string, error) {
	coord, err := canonicalV2Coordinate(coordinate)
	if err != nil {
		return "", err
	}
	return v2TimelinePagePath(coord), nil
}

func v2TimelinePagePath(coordinate archiveindex.Coordinate) string {
	kind := "media"
	if coordinate.Kind == archiveindex.ObjectInit {
		kind = "init"
	}
	return fmt.Sprintf("archive/v2/timeline/%s/%s/%020d/%020d/%020d",
		v2TrackKey(coordinate.TrackID), kind, coordinate.SourceEpoch,
		coordinate.DiscontinuitySequence, coordinate.Sequence/mediaShardMaxEntries)
}

func v2TimelineTrackPrefix(trackID string, kind archiveindex.ObjectKind) string {
	kindPath := "media"
	if kind == archiveindex.ObjectInit {
		kindPath = "init"
	}
	return "archive/v2/timeline/" + v2TrackKey(trackID) + "/" + kindPath + "/"
}

// v2ObservationCoordinatePath stores gap/coverage records in source-coordinate
// order. The monotonically allocated ordinal lives in a separate index and is
// the visibility cutoff; this key is the ordered traversal index. A digest
// suffix keeps distinct observations at an identical coordinate addressable.
func v2ObservationCoordinatePath(collection string, observation v2Observation, digest []byte) (string, error) {
	var trackID string
	var epoch, discontinuity, from, to uint64
	var observed time.Time
	switch {
	case collection == "gaps" && observation.Gap != nil && observation.Coverage == nil:
		gap := observation.Gap
		trackID, epoch, discontinuity = gap.TrackID, gap.SourceEpoch, gap.DiscontinuitySequence
		from, to, observed = gap.FromSequence, gap.ToSequence, gap.DetectedAt
	case collection == "coverage" && observation.Coverage != nil && observation.Gap == nil:
		coverage := observation.Coverage
		trackID, epoch, discontinuity = coverage.TrackID, coverage.SourceEpoch, coverage.DiscontinuitySequence
		from, to, observed = coverage.FromSequence, coverage.ToSequence, coverage.ObservedAt
	default:
		return "", ErrShardedArchiveInvalid
	}
	if trackID == "" || to < from || observed.IsZero() || len(digest) == 0 {
		return "", ErrShardedArchiveInvalid
	}
	// Bias the signed nanosecond timestamp so fixed-width hex remains ordered.
	orderedTime := uint64(observed.UTC().UnixNano()) ^ (uint64(1) << 63)
	sequenceBucket := from / ArchiveShardMaxEntries
	digestHex := hex.EncodeToString(digest)
	return fmt.Sprintf("archive/v2/%s/%s/%020d/%020d/%020d/%020d/%020d/%016x/%s/%s", collection, v2TrackKey(trackID), epoch, discontinuity, sequenceBucket, from, to, orderedTime, digestHex[:2], digestHex), nil
}

func v2IDPath(id string) string {
	digest := sha256.Sum256([]byte(id))
	hexDigest := hex.EncodeToString(digest[:])
	return "archive/v2/id/" + hexDigest[:2] + "/" + hexDigest
}

func v2ClaimCountIntentPath() string {
	return "archive/v2/claim-count-intent"
}

func v2ClaimPath(segmentID string) string {
	return v2ClaimPagePath(v2ClaimBucket(segmentID), 0)
}

func v2ClaimPagePrefix(segmentID string) string {
	return fmt.Sprintf("archive/v2/claims/%03x/", v2ClaimBucket(segmentID))
}

func v2ClaimPagePath(bucket uint16, number uint64) string {
	return fmt.Sprintf("archive/v2/claims/%03x/%020d", bucket, number)
}

func v2ObservationOrdinalPath(collection string, ordinal uint64) string {
	page, offset := pageSlot(ordinal)
	return fmt.Sprintf("archive/v2/%s-ordinal/%020d/%03d", collection, page, offset)
}

const v2CoverageStateIndexMarkerPath = "archive/v2/coverage-state-index"

func v2CoverageStatePagePath(candidate archiveindex.Coverage, bucket uint64) string {
	kind := "media"
	if candidate.Kind == archiveindex.ObjectInit {
		kind = "init"
	}
	return fmt.Sprintf("archive/v2/coverage-state/%s/%s/%s/%020d/%020d/%020d",
		v2TrackKey(candidate.SessionID), v2TrackKey(candidate.TrackID), kind,
		candidate.SourceEpoch, candidate.DiscontinuitySequence, bucket)
}

func trimJSONSuffix(value string) string { return strings.TrimSuffix(value, ".json") }

func validV2OpaqueID(value string) bool {
	return value != "" && len(value) <= archiveindex.MaxClaimIDBytes && !strings.ContainsAny(value, "/\\\x00")
}

func isFiniteNonnegative(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func observationTime(value v2Observation) time.Time {
	if value.Gap != nil {
		return value.Gap.DetectedAt
	}
	if value.Coverage != nil {
		return value.Coverage.ObservedAt
	}
	return time.Time{}
}

func cloneRecordingHeader(recording *domain.Recording) *domain.Recording {
	if recording == nil {
		return nil
	}
	data, err := json.Marshal(recording)
	if err != nil {
		return nil
	}
	var copy domain.Recording
	if json.Unmarshal(data, &copy) != nil {
		return nil
	}
	return &copy
}

func checkV2Context(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	return ctx.Err()
}

func bytesEqualJSON(left, right []byte) bool {
	var compactLeft, compactRight bytes.Buffer
	if json.Compact(&compactLeft, left) != nil || json.Compact(&compactRight, right) != nil {
		return false
	}
	return bytes.Equal(compactLeft.Bytes(), compactRight.Bytes())
}

func mergeV2HeaderCounts(target, source *domain.Recording) {
	mergeV2HighWater(target, source)
}

func sidecarPrefixValid(prefix string) bool {
	return len(prefix) <= 1024 && strings.HasPrefix(prefix, "archive/v2/") && path.Clean(prefix) == strings.TrimSuffix(prefix, "/")
}
