// Package archivecatalog stores immutable, content-addressed archive snapshots
// separately from the canonical recording writer. Catalog failure never
// rewrites or deletes the original archive.
package archivecatalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
)

const (
	maxHeadBytes     = 4 << 10
	privateDirMode   = 0700
	privateFileMode  = 0600
	catalogVersion   = 1
	headFileName     = "head.json"
	snapshotDirName  = "snapshots"
	archiveIDPrefix  = "arc_"
	snapshotIDPrefix = "snap_"
)

var (
	ErrInvalid     = errors.New("archive catalog data is invalid")
	ErrNotFound    = errors.New("archive snapshot was not found")
	ErrConflict    = errors.New("archive snapshot chain conflicts")
	ErrUnavailable = errors.New("archive catalog is unavailable")
)

type head struct {
	Version    int    `json:"catalog_version"`
	ArchiveID  string `json:"archive_id"`
	Sequence   string `json:"sequence"`
	SnapshotID string `json:"snapshot_id"`
}

// HistoryHead identifies the immutable snapshot head held stable while a
// WalkHistory callback runs. Sequence is encoded as an integer because it is
// local catalog metadata, not a wire-format number.
type HistoryHead struct {
	Sequence   uint64
	SnapshotID string
}

// Store persists snapshots in separate immutable files and advances one
// atomic head pointer only after each snapshot is durable. It is a local
// catalog, not a trust root: callers still pin/verify remote identity and
// authority independently.
type Store struct{ root string }

func Open(root string) (*Store, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, ErrInvalid
	}
	if err := ensurePrivateDir(root); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

// Append atomically adds the next immutable snapshot. Repeating the exact
// latest snapshot is idempotent. A sequence gap, predecessor mismatch, or
// rewrite is rejected; an orphan snapshot from a crash before head publication
// can be reused only when its canonical bytes exactly match the retry.
func (s *Store) Append(ctx context.Context, snapshot archivesnapshot.Snapshot) error {
	if s == nil || ctx == nil || archivesnapshot.Verify(snapshot) != nil || !validArchiveID(snapshot.Content.ArchiveID) {
		return ErrInvalid
	}
	archiveRoot := filepath.Join(s.root, snapshot.Content.ArchiveID)
	if err := ensurePrivateDir(archiveRoot); err != nil {
		return err
	}
	unlock, err := s.lock(ctx, archiveRoot)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	encoded, err := archivesnapshot.Marshal(snapshot)
	if err != nil {
		return ErrInvalid
	}
	current, readErr := s.readHead(archiveRoot)
	if errors.Is(readErr, os.ErrNotExist) {
		if snapshot.Sequence != 1 || snapshot.PreviousSnapshotID != "" {
			return ErrConflict
		}
	} else if readErr != nil {
		return readErr
	} else {
		currentSequence, parseErr := strconv.ParseUint(current.Sequence, 10, 64)
		if parseErr != nil || current.Version != catalogVersion || current.ArchiveID != snapshot.Content.ArchiveID {
			return ErrInvalid
		}
		if snapshot.Sequence == currentSequence && snapshot.SnapshotID == current.SnapshotID {
			prior, err := s.readSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID)
			if err != nil {
				return err
			}
			priorBytes, _ := archivesnapshot.Marshal(prior)
			if bytes.Equal(priorBytes, encoded) {
				return nil
			}
			return ErrConflict
		}
		if currentSequence == ^uint64(0) || snapshot.Sequence != currentSequence+1 || snapshot.PreviousSnapshotID != current.SnapshotID {
			return ErrConflict
		}
		prior, err := s.readSnapshot(snapshot.Content.ArchiveID, current.SnapshotID)
		if err != nil || prior.Sequence != currentSequence || prior.SnapshotID != current.SnapshotID {
			return ErrInvalid
		}
	}

	if err := ensurePrivateDir(filepath.Join(archiveRoot, snapshotDirName)); err != nil {
		return err
	}
	if err := s.writeImmutableSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID, encoded); err != nil {
		return err
	}
	newHead := head{Version: catalogVersion, ArchiveID: snapshot.Content.ArchiveID, Sequence: strconv.FormatUint(snapshot.Sequence, 10), SnapshotID: snapshot.SnapshotID}
	return writeAtomic(archiveRoot, headFileName, newHead, maxHeadBytes)
}

// Head returns the snapshot named by the current durable head pointer.
func (s *Store) Head(ctx context.Context, archiveID string) (archivesnapshot.Snapshot, error) {
	if s == nil || ctx == nil || !validArchiveID(archiveID) {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	archiveRoot := filepath.Join(s.root, archiveID)
	unlock, err := s.lock(ctx, archiveRoot)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	current, err := s.readHead(archiveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return archivesnapshot.Snapshot{}, ErrNotFound
	}
	if err != nil || current.ArchiveID != archiveID {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	snapshot, err := s.readSnapshot(archiveID, current.SnapshotID)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	sequence, err := strconv.ParseUint(current.Sequence, 10, 64)
	if err != nil || snapshot.Sequence != sequence || snapshot.SnapshotID != current.SnapshotID {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	return snapshot, nil
}

// Snapshot loads one immutable entry. It does not assert that the entry is the
// latest snapshot or that its full predecessor chain is available.
func (s *Store) Snapshot(ctx context.Context, archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	if s == nil || ctx == nil || !validArchiveID(archiveID) || !validSnapshotID(snapshotID) {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	archiveRoot := filepath.Join(s.root, archiveID)
	unlock, err := s.lock(ctx, archiveRoot)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return s.readSnapshot(archiveID, snapshotID)
}

// WalkHistory verifies and visits the complete immutable snapshot chain for
// archiveID, newest first, through sequence 1. The catalog lock is held for
// the entire walk, so Append cannot advance the head while a caller performs
// local per-snapshot work. Callbacks must not call this Store recursively.
//
// The callback may have run for newer snapshots before a missing or corrupt
// ancestor is found. Callers must treat a non-nil return as an incomplete
// history and must not publish a completion/authorization transition. Any
// callback side effects should be idempotent so a complete retry is safe.
func (s *Store) WalkHistory(ctx context.Context, archiveID string, visit func(archivesnapshot.Snapshot) error) (HistoryHead, error) {
	if s == nil || ctx == nil || visit == nil || !validArchiveID(archiveID) {
		return HistoryHead{}, ErrInvalid
	}
	archiveRoot := filepath.Join(s.root, archiveID)
	unlock, err := s.lock(ctx, archiveRoot)
	if err != nil {
		return HistoryHead{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return HistoryHead{}, err
	}
	current, err := s.readHead(archiveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return HistoryHead{}, ErrNotFound
	}
	if err != nil || current.ArchiveID != archiveID {
		return HistoryHead{}, ErrInvalid
	}
	sequence, err := strconv.ParseUint(current.Sequence, 10, 64)
	if err != nil || sequence == 0 {
		return HistoryHead{}, ErrInvalid
	}
	header := HistoryHead{Sequence: sequence, SnapshotID: current.SnapshotID}
	snapshotID := current.SnapshotID
	for expected := sequence; expected > 0; expected-- {
		if err := ctx.Err(); err != nil {
			return HistoryHead{}, err
		}
		snapshot, err := s.readSnapshot(archiveID, snapshotID)
		if err != nil || snapshot.Content.ArchiveID != archiveID || snapshot.Sequence != expected || snapshot.SnapshotID != snapshotID {
			if err != nil {
				return HistoryHead{}, err
			}
			return HistoryHead{}, ErrInvalid
		}
		if expected == 1 {
			if snapshot.PreviousSnapshotID != "" {
				return HistoryHead{}, ErrInvalid
			}
		} else if !validSnapshotID(snapshot.PreviousSnapshotID) || snapshot.PreviousSnapshotID == snapshotID {
			return HistoryHead{}, ErrInvalid
		}
		if err := visit(snapshot); err != nil {
			return HistoryHead{}, err
		}
		if expected > 1 {
			snapshotID = snapshot.PreviousSnapshotID
		}
	}
	return header, nil
}

func (s *Store) readHead(archiveRoot string) (head, error) {
	path := filepath.Join(archiveRoot, headFileName)
	file, err := os.Open(path)
	if err != nil {
		return head{}, err
	}
	defer file.Close()
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != privateFileMode || !os.SameFile(info, opened) || info.Size() <= 0 || info.Size() > maxHeadBytes {
		return head{}, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(file, maxHeadBytes+1))
	if err != nil || len(data) > maxHeadBytes {
		return head{}, ErrInvalid
	}
	var value head
	if err := strictDecode(data, &value); err != nil || value.Version != catalogVersion || !validArchiveID(value.ArchiveID) || !positiveDecimal(value.Sequence) || !validSnapshotID(value.SnapshotID) {
		return head{}, ErrInvalid
	}
	return value, nil
}

func (s *Store) readSnapshot(archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	path := filepath.Join(s.root, archiveID, snapshotDirName, snapshotID+".json")
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return archivesnapshot.Snapshot{}, ErrNotFound
	}
	if err != nil {
		return archivesnapshot.Snapshot{}, ErrUnavailable
	}
	defer file.Close()
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != privateFileMode || !os.SameFile(info, opened) || info.Size() <= 0 || info.Size() > 16<<20 {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(file, (16<<20)+1))
	if err != nil || len(data) > 16<<20 {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	snapshot, err := archivesnapshot.Parse(data)
	if err != nil || snapshot.Content.ArchiveID != archiveID || snapshot.SnapshotID != snapshotID {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	return snapshot, nil
}

func (s *Store) writeImmutableSnapshot(archiveID, snapshotID string, data []byte) error {
	dir := filepath.Join(s.root, archiveID, snapshotDirName)
	path := filepath.Join(dir, snapshotID+".json")
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != privateFileMode || info.Size() <= 0 || info.Size() > 16<<20 {
			return ErrInvalid
		}
		existing, err := os.ReadFile(path)
		if err != nil {
			return ErrUnavailable
		}
		if !bytes.Equal(existing, data) {
			return ErrConflict
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrUnavailable
	}
	if err := writeAtomic(dir, snapshotID+".json", data, 16<<20); err != nil {
		return err
	}
	return nil
}

func writeAtomic(dir, name string, value any, maximum int) error {
	var data []byte
	switch typed := value.(type) {
	case []byte:
		data = append([]byte(nil), typed...)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return ErrInvalid
		}
		data = encoded
	}
	if len(data) == 0 || len(data) > maximum {
		return ErrInvalid
	}
	tmp, err := os.CreateTemp(dir, ".catalog-*.tmp")
	if err != nil {
		return ErrUnavailable
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(privateFileMode); err != nil {
		_ = tmp.Close()
		return ErrUnavailable
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return ErrUnavailable
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return ErrUnavailable
	}
	if err := tmp.Close(); err != nil {
		return ErrUnavailable
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, name)); err != nil {
		return ErrUnavailable
	}
	directory, err := os.Open(dir)
	if err != nil {
		return ErrUnavailable
	}
	syncErr, closeErr := directory.Sync(), directory.Close()
	if syncErr != nil || closeErr != nil {
		return ErrUnavailable
	}
	return nil
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, privateDirMode); err != nil {
		return ErrUnavailable
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != privateDirMode {
		return ErrInvalid
	}
	return nil
}

func strictDecode(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	canonical, err := json.Marshal(output)
	if err != nil || !bytes.Equal(data, canonical) {
		return ErrInvalid
	}
	return nil
}

func validArchiveID(value string) bool {
	if !strings.HasPrefix(value, archiveIDPrefix) || len(value) != len(archiveIDPrefix)+22 {
		return false
	}
	for _, r := range value[len(archiveIDPrefix):] {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func validSnapshotID(value string) bool {
	if !strings.HasPrefix(value, snapshotIDPrefix) || len(value) != len(snapshotIDPrefix)+64 {
		return false
	}
	for _, r := range value[len(snapshotIDPrefix):] {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func positiveDecimal(value string) bool {
	if value == "" || value == "0" || len(value) > 20 || (len(value) > 1 && value[0] == '0') {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}
