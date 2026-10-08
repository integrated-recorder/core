package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

var deletionTombstonePattern = regexp.MustCompile(`^\.deleting-([a-f0-9]{32})-([a-f0-9]{32})$`)

// ArchiveEntry describes one canonical recording file without exposing source
// URIs or filesystem paths. Sidecars are projections of canonical metadata;
// payload entries include the integrity digest stored in recording.json.
type ArchiveEntry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

type IntegrityStatus string

const (
	IntegrityUnknown   IntegrityStatus = "unknown"
	IntegrityVerifying IntegrityStatus = "verifying"
	IntegrityVerified  IntegrityStatus = "verified"
	IntegrityDegraded  IntegrityStatus = "degraded"
	IntegrityFailed    IntegrityStatus = "failed"
)

// IntegrityIssue contains a stable issue code and, only when it is safe, a
// recording-relative path. It never includes a source URI or absolute path.
type IntegrityIssue struct {
	Code string `json:"code"`
	Path string `json:"path,omitempty"`
}

type IntegrityResult struct {
	Status          IntegrityStatus  `json:"status"`
	LastVerifiedAt  time.Time        `json:"last_verified_at"`
	ObjectsTotal    int              `json:"objects_total"`
	ObjectsVerified int              `json:"objects_verified"`
	ObjectsMissing  int              `json:"objects_missing"`
	ObjectsCorrupt  int              `json:"objects_corrupt"`
	Issues          []IntegrityIssue `json:"issues"`
}

// StorageStats contains a snapshot of canonical archive and filesystem usage.
// ArchiveRoot is internal-only so an HTTP layer cannot accidentally expose an
// absolute machine path by serializing this value directly.
type StorageStats struct {
	ArchiveRoot              string `json:"-"`
	CapacityKnown            bool   `json:"capacity_known"`
	FilesystemTotalBytes     uint64 `json:"filesystem_total_bytes"`
	FilesystemUsedBytes      uint64 `json:"filesystem_used_bytes"`
	FilesystemAvailableBytes uint64 `json:"filesystem_available_bytes"`
	RecordingBytes           uint64 `json:"recording_bytes"`
	RecordingCount           int    `json:"recording_count"`
	SegmentCount             int    `json:"segment_count"`
	InitSegmentCount         int    `json:"init_segment_count"`
	ManifestCount            int    `json:"manifest_count"`
}

type archiveReference struct {
	path string
	kind string
	size int64
	hash string
}

// ArchiveIndex lists only recording.json and files directly referenced by its
// canonical media metadata. Unreferenced files are deliberately omitted.
func (s *LocalFilesystemBackend) ArchiveIndex(recording *domain.Recording) ([]ArchiveEntry, error) {
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		return nil, errors.New("invalid recording")
	}
	rootInfo, err := s.archiveFileInfo(recording.ID, "recording.json")
	if err != nil || !rootInfo.Mode().IsRegular() {
		return nil, errors.New("recording metadata is unavailable")
	}
	entries := []ArchiveEntry{{Path: "recording.json", Kind: "recording", Size: rootInfo.Size()}}
	seen := map[string]struct{}{"recording.json": {}}
	add := func(path, kind, expectedPrefix, digest string) error {
		if !archiveReferencePath(path, expectedPrefix) {
			return errors.New("invalid canonical archive reference")
		}
		if _, duplicate := seen[path]; duplicate {
			return errors.New("duplicate canonical archive reference")
		}
		info, statErr := s.archiveFileInfo(recording.ID, path)
		if statErr != nil || !info.Mode().IsRegular() {
			return errors.New("canonical archive payload is unavailable")
		}
		seen[path] = struct{}{}
		entries = append(entries, ArchiveEntry{Path: path, Kind: kind, Size: info.Size(), SHA256: digest})
		sidecar := path + ".json"
		if _, duplicate := seen[sidecar]; duplicate {
			return errors.New("duplicate canonical archive reference")
		}
		sidecarInfo, sidecarErr := s.archiveFileInfo(recording.ID, sidecar)
		if errors.Is(sidecarErr, os.ErrNotExist) {
			return nil
		}
		if sidecarErr != nil || !sidecarInfo.Mode().IsRegular() {
			return errors.New("canonical archive sidecar is unavailable")
		}
		seen[sidecar] = struct{}{}
		entries = append(entries, ArchiveEntry{Path: sidecar, Kind: kind + "_sidecar", Size: sidecarInfo.Size()})
		return nil
	}
	for _, snapshot := range recording.Snapshots {
		if err := add(snapshot.StoragePath, "manifest", "manifests/", snapshot.SHA256); err != nil {
			return nil, err
		}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			return nil, errors.New("recording contains invalid track metadata")
		}
		for _, segment := range track.Segments {
			if err := add(segment.StoragePath, "segment", "tracks/", segment.SHA256); err != nil {
				return nil, err
			}
		}
		for _, segment := range track.InitSegments {
			if err := add(segment.StoragePath, "init_segment", "tracks/", segment.SHA256); err != nil {
				return nil, err
			}
		}
	}
	sortArchiveEntries(entries)
	return entries, nil
}

// RecordingDirectoryBytes returns the bytes physically present under one
// recording directory. It does not require every canonical reference to
// exist, so management queries can still report a degraded archive's size.
// Symlinks and non-regular objects fail closed instead of being followed.
func (s *LocalFilesystemBackend) RecordingDirectoryBytes(recordingID string) (int64, error) {
	return s.RecordingDirectoryBytesContext(context.Background(), recordingID)
}

// RecordingDirectoryBytesContext returns physical bytes below one recording
// and stops walking when ctx is canceled.
func (s *LocalFilesystemBackend) RecordingDirectoryBytesContext(ctx context.Context, recordingID string) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !recordingIDPattern.MatchString(recordingID) {
		return 0, errors.New("invalid recording id")
	}
	root := s.recordingDir(recordingID)
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return 0, errors.New("recording directory unavailable")
	}
	var total int64
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return errors.New("recording directory could not be read")
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("symlink in recording directory")
		}
		metadata, statErr := entry.Info()
		if statErr != nil {
			return errors.New("recording directory could not be read")
		}
		if metadata.IsDir() {
			return nil
		}
		if !metadata.Mode().IsRegular() || metadata.Size() < 0 {
			return errors.New("recording directory contains invalid files")
		}
		if total > math.MaxInt64-metadata.Size() {
			return ErrArchiveSizeOverflow
		}
		total += metadata.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// VerifyRecording streams each canonical payload through SHA-256 using bounded
// memory. It does not mutate the recording document or payloads.
func (s *LocalFilesystemBackend) VerifyRecording(recording *domain.Recording) IntegrityResult {
	return s.VerifyRecordingContext(context.Background(), recording)
}

// VerifyRecordingContext streams canonical objects through SHA-256 while
// honoring cancellation between reads and object boundaries.
func (s *LocalFilesystemBackend) VerifyRecordingContext(ctx context.Context, recording *domain.Recording) IntegrityResult {
	if ctx == nil {
		ctx = context.Background()
	}
	result := IntegrityResult{Status: IntegrityVerified, LastVerifiedAt: time.Now().UTC(), Issues: []IntegrityIssue{}}
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		result.Status = IntegrityFailed
		result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_recording"})
		return result
	}
	canceled := false
	add := func(path, expectedPrefix string, expectedSize int64, expectedHash string) {
		if ctx.Err() != nil {
			canceled = true
			return
		}
		result.ObjectsTotal++
		safePath := path
		if !archiveReferencePath(path, expectedPrefix) {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_reference"})
			return
		}
		info, err := s.archiveFileInfo(recording.ID, path)
		if errors.Is(err, os.ErrNotExist) {
			result.ObjectsMissing++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "missing_payload", Path: safePath})
			return
		}
		if err != nil || !info.Mode().IsRegular() || expectedSize < 0 {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "unavailable_payload", Path: safePath})
			return
		}
		digest, decodeErr := hex.DecodeString(expectedHash)
		if decodeErr != nil || len(digest) != sha256.Size {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_integrity_metadata", Path: safePath})
			return
		}
		file, openErr := s.OpenPayloadReader(recording.ID, path)
		if openErr != nil {
			if errors.Is(openErr, os.ErrNotExist) {
				result.ObjectsMissing++
				result.Issues = append(result.Issues, IntegrityIssue{Code: "missing_payload", Path: safePath})
			} else {
				result.ObjectsCorrupt++
				result.Issues = append(result.Issues, IntegrityIssue{Code: "unavailable_payload", Path: safePath})
			}
			return
		}
		h := sha256.New()
		size, copyErr := io.Copy(h, contextReader{ctx: ctx, reader: file})
		closeErr := file.Close()
		if ctx.Err() != nil || errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			canceled = true
			return
		}
		if copyErr != nil || closeErr != nil || size != expectedSize || !equalDigest(h.Sum(nil), digest) {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "payload_mismatch", Path: safePath})
			return
		}
		result.ObjectsVerified++
	}
	for _, snapshot := range recording.Snapshots {
		add(snapshot.StoragePath, "manifests/", snapshot.Size, snapshot.SHA256)
		if canceled {
			break
		}
	}
	for _, track := range recording.Tracks {
		if canceled {
			break
		}
		if track == nil {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_track_metadata"})
			continue
		}
		for _, segment := range track.Segments {
			if canceled {
				break
			}
			add(segment.StoragePath, "tracks/", segment.PayloadSize, segment.SHA256)
		}
		for _, segment := range track.InitSegments {
			if canceled {
				break
			}
			add(segment.StoragePath, "tracks/", segment.PayloadSize, segment.SHA256)
		}
	}
	if len(result.Issues) > 0 {
		result.Status = IntegrityDegraded
	}
	if canceled {
		return IntegrityResult{Status: IntegrityUnknown, Issues: []IntegrityIssue{}}
	}
	return result
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

// StorageStats returns canonical object counts and physical bytes below the
// recordings directory. Symlinks are counted neither as files nor traversed.
func (s *LocalFilesystemBackend) StorageStats() (StorageStats, error) {
	stats := StorageStats{ArchiveRoot: s.root}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.root, &fs); err != nil {
		return StorageStats{}, errors.New("filesystem statistics unavailable")
	}
	blockSize := uint64(fs.Bsize)
	if fs.Bfree > fs.Blocks || fs.Bavail > fs.Blocks {
		return StorageStats{}, errors.New("filesystem statistics are inconsistent")
	}
	total, ok := checkedByteProduct(fs.Blocks, blockSize)
	if !ok {
		return StorageStats{}, errors.New("filesystem statistics overflow")
	}
	used, ok := checkedByteProduct(fs.Blocks-fs.Bfree, blockSize)
	if !ok {
		return StorageStats{}, errors.New("filesystem statistics overflow")
	}
	available, ok := checkedByteProduct(fs.Bavail, blockSize)
	if !ok {
		return StorageStats{}, errors.New("filesystem statistics overflow")
	}
	stats.FilesystemTotalBytes = total
	stats.FilesystemUsedBytes = used
	stats.FilesystemAvailableBytes = available
	stats.CapacityKnown = true

	base := filepath.Join(s.root, "recordings")
	baseInfo, err := os.Lstat(base)
	if err != nil || baseInfo.Mode()&os.ModeSymlink != 0 || !baseInfo.IsDir() {
		return StorageStats{}, errors.New("recordings directory unavailable")
	}
	err = filepath.WalkDir(base, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.New("recordings storage could not be read")
		}
		if path == base || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return errors.New("recordings storage could not be read")
		}
		if info.Mode().IsRegular() {
			if info.Size() < 0 || uint64(info.Size()) > math.MaxUint64-stats.RecordingBytes {
				return errors.New("recordings storage size overflow")
			}
			stats.RecordingBytes += uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		return StorageStats{}, err
	}

	children, err := os.ReadDir(base)
	if err != nil {
		return StorageStats{}, errors.New("recordings storage could not be read")
	}
	for _, child := range children {
		if !recordingIDPattern.MatchString(child.Name()) || child.Type()&os.ModeSymlink != 0 || !child.IsDir() {
			continue
		}
		id := child.Name()
		recording, readErr := s.readCanonicalRecording(id)
		if readErr != nil {
			return StorageStats{}, errors.New("canonical recording metadata unavailable")
		}
		stats.RecordingCount++
		stats.ManifestCount += len(recording.Snapshots)
		for _, track := range recording.Tracks {
			if track == nil {
				return StorageStats{}, errors.New("canonical recording metadata invalid")
			}
			stats.SegmentCount += len(track.Segments)
			stats.InitSegmentCount += len(track.InitSegments)
		}
	}
	return stats, nil
}

// DeleteRecordingData atomically hides a recording by renaming its directory
// to a random tombstone before removing it. Calls for an already absent ID are
// successful, making retries idempotent.
func (s *LocalFilesystemBackend) DeleteRecordingData(id string) error {
	if !recordingIDPattern.MatchString(id) {
		return errors.New("invalid recording id")
	}
	base := filepath.Join(s.root, "recordings")
	baseInfo, err := os.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || baseInfo.Mode()&os.ModeSymlink != 0 || !baseInfo.IsDir() {
		return errors.New("recordings directory unavailable")
	}
	if err := removeDeletionTombstones(base, id); err != nil {
		return errors.New("pending recording deletion could not be completed")
	}
	source := s.recordingDir(id)
	info, err := os.Lstat(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("recording data is not a safe directory")
	}
	var tombstone string
	for attempt := 0; attempt < 8; attempt++ {
		random := make([]byte, 16)
		if _, err = rand.Read(random); err != nil {
			return errors.New("could not allocate recording tombstone")
		}
		candidate := filepath.Join(base, ".deleting-"+id+"-"+hex.EncodeToString(random))
		if _, err = os.Lstat(candidate); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("recording tombstone is unavailable")
		}
		if err = os.Rename(source, candidate); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return errors.New("recording could not be moved to deletion tombstone")
		}
		tombstone = candidate
		break
	}
	if tombstone == "" {
		return errors.New("could not allocate recording tombstone")
	}
	if err = syncDirectory(base); err != nil {
		return errors.New("recording tombstone could not be made durable")
	}
	if err = os.RemoveAll(tombstone); err != nil {
		return errors.New("recording tombstone could not be removed")
	}
	if err = syncDirectory(base); err != nil {
		return errors.New("recording deletion could not be made durable")
	}
	return nil
}

func removeDeletionTombstones(base, recordingID string) error {
	entries, err := os.ReadDir(base)
	if err != nil {
		return err
	}
	changed := false
	for _, entry := range entries {
		match := deletionTombstonePattern.FindStringSubmatch(entry.Name())
		if len(match) != 3 || match[1] != recordingID {
			continue
		}
		path := filepath.Join(base, entry.Name())
		info, statErr := os.Lstat(path)
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				continue
			}
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("unsafe recording deletion tombstone")
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return syncDirectory(base)
	}
	return nil
}

func (s *LocalFilesystemBackend) readCanonicalRecording(id string) (*domain.Recording, error) {
	file, err := s.openArchiveFile(id, "recording.json")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var recording domain.Recording
	if err = json.NewDecoder(file).Decode(&recording); err != nil {
		return nil, err
	}
	if recording.ID != id {
		return nil, errors.New("recording identity mismatch")
	}
	return &recording, nil
}

func (s *LocalFilesystemBackend) archiveFileInfo(id, relative string) (os.FileInfo, error) {
	_, err := s.safePath(id, relative)
	if err != nil || !canonicalRelativePath(relative) {
		return nil, errors.New("invalid archive path")
	}
	root := s.recordingDir(id)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, errors.New("unsafe recording directory")
	}
	components := strings.Split(relative, "/")
	current := root
	for index, component := range components {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			return nil, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("symlink in archive path")
		}
		if index < len(components)-1 && !info.IsDir() {
			return nil, errors.New("non-directory archive path component")
		}
		if index == len(components)-1 && !info.Mode().IsRegular() {
			return nil, errors.New("archive object is not a regular file")
		}
	}
	file, err := os.Open(current)
	if err != nil {
		return nil, err
	}
	openedInfo, statErr := file.Stat()
	closeErr := file.Close()
	pathInfo, pathErr := os.Lstat(current)
	if statErr != nil || closeErr != nil || pathErr != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !openedInfo.Mode().IsRegular() || !os.SameFile(openedInfo, pathInfo) {
		return nil, errors.New("archive object changed during inspection")
	}
	return openedInfo, nil
}

func (s *LocalFilesystemBackend) openArchiveFile(id, relative string) (*os.File, error) {
	if _, err := s.archiveFileInfo(id, relative); err != nil {
		return nil, err
	}
	path, err := s.safePath(id, relative)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	pathInfo, pathErr := os.Lstat(path)
	if err != nil || pathErr != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || !os.SameFile(info, pathInfo) {
		_ = file.Close()
		return nil, errors.New("archive object is unavailable")
	}
	return file, nil
}

func archiveReferencePath(path, expectedPrefix string) bool {
	return strings.HasPrefix(path, expectedPrefix) && canonicalRelativePath(path)
}

func canonicalRelativePath(path string) bool {
	if path == "" || strings.ContainsAny(path, "\\\x00") || filepath.IsAbs(path) || filepath.VolumeName(path) != "" {
		return false
	}
	if filepath.ToSlash(filepath.Clean(filepath.FromSlash(path))) != path {
		return false
	}
	for _, component := range strings.Split(path, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

func sortArchiveEntries(entries []ArchiveEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
}

func equalDigest(actual, expected []byte) bool {
	return len(actual) == sha256.Size && len(expected) == sha256.Size && bytes.Equal(actual, expected)
}

func checkedByteProduct(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
