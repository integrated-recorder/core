// Package storagediagnostic stores private, bounded diagnostics for failures
// in canonical recording persistence. These records are management metadata;
// they are not part of canonical archive roots or exports.
package storagediagnostic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	SchemaVersion  = 1
	maxRecordBytes = 32 << 10
	maxChainItems  = 12
)

var (
	ErrNotFound      = errors.New("storage failure diagnostic not found")
	ErrInvalidRecord = errors.New("storage failure diagnostic is invalid")
	recordingIDRE    = regexp.MustCompile(`^[a-f0-9]{32}$`)
	stageRE          = regexp.MustCompile(`^[A-Za-z0-9 ._:-]{1,128}$`)
	errorTypeRE      = regexp.MustCompile(`^[A-Za-z0-9_.*\[\]]{1,160}$`)
)

type Classification string

const (
	ClassificationCanonicalCommitFailed  Classification = "canonical_commit_failed"
	ClassificationCoordinatorUnavailable Classification = "coordinator_unavailable"
	ClassificationOther                  Classification = "other"
)

// IngestSnapshot contains only bounded numeric runtime counters.
type IngestSnapshot struct {
	BufferUsedBytes          int64   `json:"buffer_used_bytes"`
	ReservedBytes            int64   `json:"reserved_bytes"`
	QueueObjects             int     `json:"queue_objects"`
	QueueBytes               int64   `json:"queue_bytes"`
	OldestPersistAgeSeconds  float64 `json:"oldest_persist_age_seconds"`
	ActiveWriters            int     `json:"active_writers"`
	WriterConcurrency        int     `json:"writer_concurrency"`
	StorageErrorsTotal       uint64  `json:"storage_errors_total"`
	CoordinatorErrorsTotal   uint64  `json:"coordinator_errors_total"`
	PendingCoordinatorLeases int     `json:"pending_coordinator_leases"`
}

// Diagnostic intentionally has no free-form error message, URL, path, header,
// or credential field. Error chains are represented by static Go type names,
// known categories, and safe operation metadata only.
type Diagnostic struct {
	Version              int            `json:"version"`
	RecordedAt           time.Time      `json:"recorded_at"`
	RecordingID          string         `json:"recording_id"`
	Classification       Classification `json:"classification"`
	StageChain           []string       `json:"stage_chain,omitempty"`
	ErrorTypeChain       []string       `json:"error_type_chain,omitempty"`
	ErrorCategories      []string       `json:"error_categories,omitempty"`
	FileOperation        string         `json:"file_operation,omitempty"`
	ErrnoCode            int            `json:"errno_code,omitempty"`
	CurrentJobKind       string         `json:"current_job_kind,omitempty"`
	FirstFailureJobKind  string         `json:"first_failure_job_kind,omitempty"`
	CurrentAttempts      int            `json:"current_attempts"`
	FirstFailureAttempts int            `json:"first_failure_attempts"`
	IngestSnapshot       IngestSnapshot `json:"ingest_snapshot"`
	RecordingState       string         `json:"recording_state,omitempty"`
	SourceClass          string         `json:"source_class,omitempty"`
}

// Store writes diagnostics below DATA_DIR/management/diagnostics. The final
// hard link is an atomic no-replace publication, so concurrent Engine
// generations preserve the first diagnostic without a shared lock protocol.
type Store struct {
	dir string
}

func Open(dataDir string) (*Store, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, ErrInvalidRecord
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil || filepath.Clean(abs) != abs {
		return nil, ErrInvalidRecord
	}
	managementDir := filepath.Join(abs, "management")
	diagnosticsDir := filepath.Join(managementDir, "diagnostics")
	recordingsDir := filepath.Join(diagnosticsDir, "recordings")
	for _, dir := range []string{managementDir, diagnosticsDir, recordingsDir} {
		if err := ensurePrivateDirectory(dir); err != nil {
			return nil, errors.New("storage diagnostic directory is unavailable")
		}
	}
	for _, dir := range []string{abs, managementDir, diagnosticsDir} {
		if err := syncDir(dir); err != nil {
			return nil, errors.New("storage diagnostic directory could not be synchronized")
		}
	}
	return &Store{dir: recordingsDir}, nil
}

// RecordFirst atomically creates the recording's diagnostic if none exists.
// It returns the authoritative saved record and whether this call published
// it. Existing records are never replaced.
func (s *Store) RecordFirst(d Diagnostic) (saved Diagnostic, created bool, err error) {
	if s == nil || validate(d) != nil {
		return Diagnostic{}, false, ErrInvalidRecord
	}
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil || len(data)+1 > maxRecordBytes {
		return Diagnostic{}, false, ErrInvalidRecord
	}
	data = append(data, '\n')
	destination := s.path(d.RecordingID)
	tmp, err := os.CreateTemp(s.dir, ".projection-storage-failure-*.tmp")
	if err != nil {
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be staged")
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be staged")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be staged")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be staged")
	}
	if err := tmp.Close(); err != nil {
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be staged")
	}
	if err := os.Link(tmpName, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			prior, readErr := s.Read(d.RecordingID)
			return prior, false, readErr
		}
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be published")
	}
	if err := syncDir(s.dir); err != nil {
		return Diagnostic{}, false, errors.New("storage failure diagnostic could not be synchronized")
	}
	return d, true, nil
}

func (s *Store) Read(recordingID string) (Diagnostic, error) {
	if s == nil || !recordingIDRE.MatchString(recordingID) {
		return Diagnostic{}, ErrNotFound
	}
	path := s.path(recordingID)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Diagnostic{}, ErrNotFound
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxRecordBytes {
		return Diagnostic{}, ErrInvalidRecord
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > maxRecordBytes {
		return Diagnostic{}, ErrInvalidRecord
	}
	var d Diagnostic
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&d) != nil {
		return Diagnostic{}, ErrInvalidRecord
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Diagnostic{}, ErrInvalidRecord
	}
	if validate(d) != nil || d.RecordingID != recordingID {
		return Diagnostic{}, ErrInvalidRecord
	}
	return d, nil
}

func (s *Store) Delete(recordingID string) error {
	if s == nil || !recordingIDRE.MatchString(recordingID) {
		return ErrInvalidRecord
	}
	path := s.path(recordingID)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidRecord
	}
	if err := os.Remove(path); err != nil {
		return errors.New("storage failure diagnostic could not be removed")
	}
	if err := syncDir(s.dir); err != nil {
		return errors.New("storage failure diagnostic removal could not be synchronized")
	}
	return nil
}

func (s *Store) path(recordingID string) string {
	return filepath.Join(s.dir, recordingID+".json")
}

func validate(d Diagnostic) error {
	if d.Version != SchemaVersion || !recordingIDRE.MatchString(d.RecordingID) || d.RecordedAt.IsZero() {
		return ErrInvalidRecord
	}
	if d.Classification != ClassificationCanonicalCommitFailed && d.Classification != ClassificationCoordinatorUnavailable && d.Classification != ClassificationOther {
		return ErrInvalidRecord
	}
	if len(d.StageChain) > maxChainItems || len(d.ErrorTypeChain) > maxChainItems || len(d.ErrorCategories) > maxChainItems {
		return ErrInvalidRecord
	}
	for _, stage := range d.StageChain {
		if !stageRE.MatchString(stage) {
			return ErrInvalidRecord
		}
	}
	for _, typeName := range d.ErrorTypeChain {
		if !errorTypeRE.MatchString(typeName) {
			return ErrInvalidRecord
		}
	}
	for _, category := range d.ErrorCategories {
		if !validErrorCategory(category) {
			return ErrInvalidRecord
		}
	}
	if d.FileOperation != "" && !validFileOperation(d.FileOperation) || d.ErrnoCode < 0 ||
		d.CurrentAttempts < 0 || d.FirstFailureAttempts < 0 ||
		d.IngestSnapshot.BufferUsedBytes < 0 || d.IngestSnapshot.ReservedBytes < 0 ||
		d.IngestSnapshot.QueueObjects < 0 || d.IngestSnapshot.QueueBytes < 0 ||
		d.IngestSnapshot.OldestPersistAgeSeconds < 0 || d.IngestSnapshot.ActiveWriters < 0 ||
		d.IngestSnapshot.WriterConcurrency < 0 || d.IngestSnapshot.PendingCoordinatorLeases < 0 {
		return ErrInvalidRecord
	}
	if d.RecordingState != "" && !regexp.MustCompile(`^[a-z_]{1,32}$`).MatchString(d.RecordingState) {
		return ErrInvalidRecord
	}
	if d.SourceClass != "" && d.SourceClass != "live" && d.SourceClass != "historical" && d.SourceClass != "unknown" {
		return ErrInvalidRecord
	}
	if d.CurrentJobKind != "" && !validJobKind(d.CurrentJobKind) || d.FirstFailureJobKind != "" && !validJobKind(d.FirstFailureJobKind) {
		return ErrInvalidRecord
	}
	return nil
}

func validJobKind(kind string) bool {
	switch kind {
	case "canonical_payload", "media_payload", "init_payload", "historical_media", "historical_init", "manifest_snapshot", "recording_metadata":
		return true
	default:
		return false
	}
}

func validErrorCategory(category string) bool {
	switch category {
	case "not_found", "permission_denied", "payload_size_mismatch", "canonical_commit_failed", "coordinator_unavailable", "ingest_closed", "ingest_too_large", "ingest_reservation", "short_write", "io_error":
		return true
	default:
		return false
	}
}

func validFileOperation(operation string) bool {
	switch operation {
	case "open", "create", "read", "write", "close", "sync", "rename", "link", "remove", "mkdir", "stat", "chmod", "filesystem":
		return true
	default:
		return false
	}
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("private directory is not a real directory")
	}
	return os.Chmod(path, 0700)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
