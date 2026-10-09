// Package storage persists recordings as self-describing directories. The JSON
// documents and original payload files are the source of truth; no DB is used.
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
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

var recordingIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

const (
	legacyRecordingFormatVersion  = 0
	currentRecordingFormatVersion = 1
)

const maxSidecarReadBytes int64 = 16 << 20

// ErrPayloadSizeMismatch marks a complete source response whose byte count
// did not match an exact range request. Callers may retry the source fetch;
// ordinary local I/O errors remain distinguishable and are not retried.
var ErrPayloadSizeMismatch = errors.New("payload size mismatch")

// ErrArchiveSizeOverflow marks a physical archive size that cannot be
// represented by the API's signed byte count.
var ErrArchiveSizeOverflow = errors.New("recording archive size overflow")

// ErrReadOnlyListLimit marks a read-only archive snapshot that exceeds its
// caller's count bound.
var ErrReadOnlyListLimit = errors.New("read-only recording list exceeds limit")

// ErrUnsupportedRecordingFormat marks a canonical root format this Core
// cannot safely interpret. Unknown roots must remain untouched so a newer
// format's fields cannot be lost by decoding and re-encoding an older model.
var ErrUnsupportedRecordingFormat = errors.New("unsupported recording format version")

// ErrSidecarReadUnsupported marks a backend that does not expose the optional
// read-only sidecar capability. It is intentionally safe to return to callers.
var ErrSidecarReadUnsupported = errors.New("storage backend does not support sidecar reads")

// ErrAtomicRecordingCreationUnsupported marks a backend that cannot publish an
// initial private sidecar and recording root as one visible recording.
var ErrAtomicRecordingCreationUnsupported = errors.New("storage backend does not support atomic recording creation with sidecar")

// StorageBackend is the set of canonical archive operations currently needed
// by the application. Store is the stable archive facade. New preserves the
// legacy single-process development/test backend; production Control and
// Recorder Engine processes use NewWithObjectStore with a generation-pinned
// Storage Provider Protocol process.
//
// The methods speak recording IDs and logical recording-relative paths. Root
// is retained for internal diagnostics and existing tests only; it is not a
// canonical locator and must never be included in API responses.
type StorageBackend interface {
	RecoveryIssues() []RecoveryIssue
	HasCanonicalPayloadIssue(string) bool
	NewRecordingDir(string) error
	CreateRecording(*domain.Recording) error
	SaveRecording(*domain.Recording) error
	LoadAll() ([]*domain.Recording, error)
	LoadAllReadOnly() ([]*domain.Recording, error)
	LoadAllReadOnlyLimit(int) ([]*domain.Recording, error)
	LoadRecordingReadOnly(string) (*domain.Recording, error)
	SavePayload(string, string, io.Reader, int64) (PayloadResult, error)
	SavePayloadExact(string, string, io.Reader, int64, int64) (PayloadResult, error)
	SaveSidecar(string, string, any) error
	SaveSnapshot(string, string, string, []byte, time.Time) (domain.ManifestSnapshot, error)
	OpenPayloadReader(string, string) (io.ReadCloser, error)
	OpenPayloadRangeReaderContext(context.Context, string, string, int64, int64) (io.ReadCloser, error)
	StatPayload(string, string) (ObjectInfo, error)
	ArchiveIndex(*domain.Recording) ([]ArchiveEntry, error)
	RecordingDirectoryBytes(string) (int64, error)
	VerifyRecording(*domain.Recording) IntegrityResult
	VerifyRecordingContext(context.Context, *domain.Recording) IntegrityResult
	StorageStats() (StorageStats, error)
	DeleteRecordingData(string) error
	PoolMetrics() PoolSnapshot
}

// Store is the archive storage facade. The embedding preserves the public
// method surface while allowing callers and tests to depend on the facade
// rather than the local filesystem implementation.
type Store struct {
	StorageBackend
	root             string // internal/test compatibility only; never part of API models
	v2Locks          [64]sync.Mutex
	ingestMu         sync.RWMutex
	ingest           *IngestService
	ingestOptions    IngestOptions
	runtimeIngest    RuntimeIngestCoordinator
	telemetryMu      sync.RWMutex
	runtimeTelemetry RuntimeStorageTelemetry
}

// LoadSidecar reads one bounded JSON sidecar when the backend supports the
// optional read capability. It deliberately does not expand StorageBackend,
// so existing third-party and test backends remain source-compatible.
func (s *Store) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	if err := validateSidecarRead(id, relativePath, maxBytes, output); err != nil {
		return err
	}
	reader, ok := s.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	})
	if !ok {
		return ErrSidecarReadUnsupported
	}
	return reader.LoadSidecar(id, relativePath, maxBytes, output)
}

// RecordingDirectoryBytesContext returns the physical bytes below one
// recording while allowing providers with context-aware enumeration to stop
// work when the caller no longer needs the read-model result. The optional
// capability keeps existing StorageBackend implementations source-compatible.
func (s *Store) RecordingDirectoryBytesContext(ctx context.Context, recordingID string) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s == nil || s.StorageBackend == nil {
		return 0, errors.New("storage backend is unavailable")
	}
	if backend, ok := s.StorageBackend.(interface {
		RecordingDirectoryBytesContext(context.Context, string) (int64, error)
	}); ok {
		return backend.RecordingDirectoryBytesContext(ctx, recordingID)
	}
	return s.StorageBackend.RecordingDirectoryBytes(recordingID)
}

// CreateRecordingWithSidecar atomically initializes one recording with a
// bounded private sidecar. The root becomes visible only after backend has
// durably published both documents.
func (s *Store) CreateRecordingWithSidecar(recording *domain.Recording, relativePath string, value any) error {
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) || !validInitialSidecarPath(relativePath) || value == nil {
		return errors.New("invalid recording sidecar initialization")
	}
	root := recording
	if recording.FormatVersion == ShardedArchiveFormatVersion {
		root = cloneRecordingHeader(recording)
		if root == nil {
			return ErrShardedArchiveInvalid
		}
		if root.ShardedArchive == nil {
			root.ShardedArchive = &domain.ShardedArchiveSummary{}
		}
		if err := validateV2Header(root); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil || int64(len(data)+1) > maxSidecarReadBytes {
		return errors.New("recording sidecar initialization exceeds size limit")
	}
	creator, ok := s.StorageBackend.(interface {
		CreateRecordingWithSidecar(*domain.Recording, string, any) error
	})
	if !ok {
		return ErrAtomicRecordingCreationUnsupported
	}
	return creator.CreateRecordingWithSidecar(root, relativePath, value)
}

func validateSidecarRead(id, relativePath string, maxBytes int64, output any) error {
	if !recordingIDPattern.MatchString(id) || !canonicalRelativePath(relativePath) {
		return errors.New("invalid sidecar reference")
	}
	if maxBytes <= 0 || maxBytes > maxSidecarReadBytes {
		return errors.New("sidecar read limit is invalid")
	}
	if output == nil {
		return errors.New("sidecar output is required")
	}
	return nil
}

func validInitialSidecarPath(relativePath string) bool {
	if len(relativePath) > 1024 || !canonicalRelativePath(relativePath) {
		return false
	}
	path := relativePath + ".json"
	return path != "recording.json" && !strings.HasPrefix(path, "recording.json/")
}

func decodeStrictSidecar(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return errors.New("sidecar JSON is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("sidecar JSON contains trailing data")
	}
	return nil
}

// LocalFilesystemBackend is retained for the legacy monolithic development
// command and unit tests. Production Runtime Host generations never instantiate
// it; their physical object I/O always crosses the pinned provider protocol.
// Canonical metadata stores only logical relative paths in either backend.
type LocalFilesystemBackend struct {
	root         string
	telemetry    *telemetry
	setupProbeMu sync.Mutex

	issuesMu sync.RWMutex
	issues   []RecoveryIssue
}

// RecoveryIssue describes preserved data that could not be safely attached to
// a recording. It intentionally contains no filesystem path, URI, or payload
// content.
type RecoveryIssue struct {
	ID      string `json:"id,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func New(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("data directory is empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(abs, "recordings"), 0755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	if err = os.Chmod(filepath.Join(abs, "recordings"), 0700); err != nil {
		return nil, err
	}
	backend := &LocalFilesystemBackend{root: abs}
	backend.telemetry = newTelemetry()
	// Setup probes are disposable. If the Host was terminated while a probe
	// was running, remove only stale, private probe directories when a later
	// application generation opens storage.
	cleanupSetupProbeResidue(filepath.Join(abs, "runtime", "state"))
	return &Store{StorageBackend: backend, root: abs, ingestOptions: DefaultIngestOptions()}, nil
}

// NewWithObjectStore creates a Store whose canonical archive semantics remain
// owned by Core while physical logical-key placement is delegated to objects.
// The provider is never given recording/domain objects, only validated keys
// and byte streams. Production Control and Recorder Engine processes use this
// constructor with the exact Storage Provider Protocol set pinned by Runtime
// Host generation state.
func NewWithObjectStore(root string, objects PhysicalObjectStore) (*Store, error) {
	if strings.TrimSpace(root) == "" || objects == nil {
		return nil, errors.New("object storage configuration is invalid")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = ensureStorageRoot(abs); err != nil {
		return nil, errors.New("storage staging area is unavailable")
	}
	runtimeDir := filepath.Join(abs, "runtime")
	if err = ensurePrivateStorageDirectory(runtimeDir); err != nil {
		return nil, errors.New("storage staging area is unavailable")
	}
	stageDir := filepath.Join(runtimeDir, "storage-staging")
	if err = ensurePrivateStorageDirectory(stageDir); err != nil {
		return nil, errors.New("storage staging area is unavailable")
	}
	// Writers in old and candidate generations may use this same staging
	// parent concurrently. Each object publication receives a private child
	// directory; opening a new generation must never clean another Engine's
	// admitted in-flight payload.
	backend := &ObjectStoreArchiveBackend{root: abs, stageDir: stageDir, objects: objects, telemetry: newTelemetry()}
	if identity, ok := objects.(PhysicalObjectStoreIdentity); ok {
		id, name := identity.StorageProviderIdentity()
		if id == "local" {
			backend.poolID = PoolIDLocalPrimary
			backend.poolName = "기본 보관 저장소"
			backend.poolKind = "local"
		} else if id != "" && name != "" {
			backend.poolID = "storage-" + id
			backend.poolName = name
			backend.poolKind = "remote"
		}
	}
	return &Store{StorageBackend: backend, root: abs, ingestOptions: DefaultIngestOptions()}, nil
}

func (s *Store) Root() string { return s.root }

// OpenPayload is a local-filesystem compatibility helper for existing tests
// and callers that still need an *os.File. Production storage consumers should
// use the backend-neutral OpenPayloadReader and StatPayload methods instead.
func (s *Store) OpenPayload(id, relativePath string) (*os.File, error) {
	backend, ok := s.StorageBackend.(*LocalFilesystemBackend)
	if !ok {
		return nil, errors.New("concrete file access is unavailable for this storage backend")
	}
	return backend.OpenPayload(id, relativePath)
}

var _ StorageBackend = (*LocalFilesystemBackend)(nil)

// recordingDir remains an unexported test-compatibility helper. Production
// consumers use logical IDs and paths through StorageBackend operations.
func (s *Store) recordingDir(id string) string { return filepath.Join(s.root, "recordings", id) }

// IngestService returns the single bounded local commit service for this
// archive facade. The service is lazy so read-only storage users do not spawn
// workers they do not need.
func (s *Store) IngestService() (*IngestService, error) {
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()
	if s.ingest != nil {
		return s.ingest, nil
	}
	service, err := newIngestService(s, s.ingestOptions, s.runtimeIngest)
	if err != nil {
		return nil, err
	}
	s.ingest = service
	return service, nil
}

// ConfigureRuntimeIngestCoordinator connects this Store to the Runtime Host's
// process-wide admission authority before its lazy ingest service is created.
// A nil coordinator is not accepted here; callers that need the traditional
// single-process behavior simply leave it unconfigured.
func (s *Store) ConfigureRuntimeIngestCoordinator(coordinator RuntimeIngestCoordinator) error {
	if coordinator == nil {
		return errors.New("runtime ingest coordinator configuration is invalid")
	}
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()
	if s.ingest != nil {
		return errors.New("ingest service is already constructed; restart required")
	}
	if s.runtimeIngest != nil {
		return errors.New("runtime ingest coordinator is already configured")
	}
	s.runtimeIngest = coordinator
	return nil
}

// ConfigureRuntimeStorageTelemetry attaches the Host-wide I/O projection
// before managed process metrics are read. Direct development Stores leave it
// unset and retain process-local telemetry behavior.
func (s *Store) ConfigureRuntimeStorageTelemetry(telemetry RuntimeStorageTelemetry) error {
	if telemetry == nil {
		return errors.New("runtime storage telemetry configuration is invalid")
	}
	s.telemetryMu.Lock()
	defer s.telemetryMu.Unlock()
	if s.runtimeTelemetry != nil {
		return errors.New("runtime storage telemetry is already configured")
	}
	s.runtimeTelemetry = telemetry
	return nil
}

// ConfigureIngestOptions sets fixed-lifetime ingest limits before the lazy
// service is created. A running queue/buffer cannot be resized safely.
func (s *Store) ConfigureIngestOptions(options IngestOptions) error {
	if err := ValidateIngestOptions(options); err != nil {
		return err
	}
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()
	if s.ingest != nil {
		return errors.New("ingest service is already constructed; restart required")
	}
	s.ingestOptions = options
	if backend, ok := s.StorageBackend.(*LocalFilesystemBackend); ok {
		backend.telemetry.configure(options.SampleInterval, options.MetricsRetention)
	} else if backend, ok := s.StorageBackend.(*ObjectStoreArchiveBackend); ok {
		backend.telemetry.configure(options.SampleInterval, options.MetricsRetention)
	}
	return nil
}

// IngestOptions returns the running service's immutable options, or the
// configured startup options before the service is constructed.
func (s *Store) IngestOptions() IngestOptions {
	s.ingestMu.RLock()
	defer s.ingestMu.RUnlock()
	if s.ingest != nil {
		return s.ingest.Options()
	}
	return s.ingestOptions
}

func (s *Store) MetricsSamplingInterval() time.Duration {
	return s.IngestOptions().SampleInterval
}

func (s *Store) MetricsRetention() time.Duration {
	return s.IngestOptions().MetricsRetention
}

// PoolMetrics returns one coherent local-primary pool snapshot, including the
// volatile ingest service when acquisition has started.
func (s *Store) PoolMetrics() PoolSnapshot {
	s.ingestMu.RLock()
	service := s.ingest
	s.ingestMu.RUnlock()
	var ingest IngestSnapshot
	var pool PoolSnapshot
	if service != nil {
		ingest = service.Snapshot()
		pool = service.PoolMetrics()
	} else {
		options := s.IngestOptions()
		ingest = IngestSnapshot{
			BufferCapacityBytes: options.GlobalBytes, PerRecordingCapacityBytes: options.PerRecordingBytes,
			WriterConcurrency: options.Writers,
		}
		pool = s.StorageBackend.PoolMetrics()
	}
	s.publishRuntimeStorageIOTotals(ingest)
	s.telemetryMu.RLock()
	runtimeTelemetry := s.runtimeTelemetry
	s.telemetryMu.RUnlock()
	if runtimeTelemetry != nil {
		if snapshot, err := runtimeTelemetry.StorageTelemetrySnapshot(); err == nil {
			pool.Throughput = snapshot.Throughput
			pool.EstimatedCeiling = snapshot.EstimatedCeiling
			pool.ErrorsTotal = snapshot.ErrorsTotal
			pool.Samples = append([]PoolSample(nil), snapshot.Samples...)
			pool.Buffer = PoolBuffer{
				UsedBytes: snapshot.Ingest.BufferUsedBytes, CapacityBytes: snapshot.Ingest.BufferCapacityBytes,
				ReservedBytes:             snapshot.Ingest.ReservedBytes,
				PerRecordingCapacityBytes: snapshot.Ingest.PerRecordingCapacityBytes,
			}
			pool.Queue = PoolQueue{
				Objects: snapshot.Ingest.QueueObjects, Bytes: snapshot.Ingest.QueueBytes,
				OldestAgeSeconds: snapshot.Ingest.OldestPersistAgeSeconds,
			}
			pool.Writers = PoolWriters{Active: snapshot.Ingest.ActiveWriters, Limit: snapshot.Ingest.WriterConcurrency}
		}
	}
	return pool
}

// samplePool advances the local backend sample and publishes its cumulative
// counters once per configured sampling tick. No IPC operation occurs in an
// individual Read or Write call.
func (s *Store) samplePool(ingest IngestSnapshot, now time.Time) {
	if backend, ok := s.StorageBackend.(interface {
		samplePool(IngestSnapshot, time.Time)
	}); ok {
		backend.samplePool(ingest, now)
	}
	s.publishRuntimeStorageIOTotals(ingest)
}

func (s *Store) publishRuntimeStorageIOTotals(ingest IngestSnapshot) {
	s.telemetryMu.RLock()
	runtimeTelemetry := s.runtimeTelemetry
	s.telemetryMu.RUnlock()
	backend, ok := s.StorageBackend.(interface {
		ioTotals() (readBytes, writeBytes, errors uint64)
	})
	if runtimeTelemetry == nil || !ok {
		return
	}
	readBytes, writeBytes, errorsTotal := backend.ioTotals()
	_ = runtimeTelemetry.ReportStorageIOTotals(readBytes, writeBytes, errorsTotal, ingest)
}

// PoolMetricsWindow returns bounded in-memory samples from at most the
// configured retention window. The current snapshot is always included.
func (s *Store) PoolMetricsWindow(window time.Duration) PoolSnapshot {
	retention := s.MetricsRetention()
	if window <= 0 || window > retention {
		window = retention
	}
	pool := s.PoolMetrics()
	cutoff := time.Now().Add(-window)
	first := 0
	for first < len(pool.Samples) && pool.Samples[first].At.Before(cutoff) {
		first++
	}
	pool.Samples = append([]PoolSample(nil), pool.Samples[first:]...)
	return pool
}

func (s *LocalFilesystemBackend) recordStorageError() { s.telemetry.recordError() }

func (s *LocalFilesystemBackend) ioTotals() (readBytes, writeBytes, errors uint64) {
	return s.telemetry.ioTotals()
}

// RecoveryIssues returns a snapshot of the most recent LoadAll recovery
// findings. Issues contain only safe identifiers and fixed messages.
func (s *LocalFilesystemBackend) RecoveryIssues() []RecoveryIssue {
	s.issuesMu.RLock()
	defer s.issuesMu.RUnlock()
	return append([]RecoveryIssue(nil), s.issues...)
}

// HasCanonicalPayloadIssue reports whether startup recovery found a missing or
// integrity-mismatched payload referenced by this recording's canonical media
// timeline. It performs no filesystem reads and is intended for fail-closed
// playback projection checks.
func (s *LocalFilesystemBackend) HasCanonicalPayloadIssue(recordingID string) bool {
	s.issuesMu.RLock()
	defer s.issuesMu.RUnlock()
	for _, issue := range s.issues {
		if issue.ID == recordingID && strings.HasPrefix(issue.Code, "canonical_payload_") {
			return true
		}
	}
	return false
}

func (s *LocalFilesystemBackend) setRecoveryIssues(issues []RecoveryIssue) {
	s.issuesMu.Lock()
	s.issues = append([]RecoveryIssue(nil), issues...)
	s.issuesMu.Unlock()
}

func (s *LocalFilesystemBackend) addRecoveryIssue(issue RecoveryIssue) {
	s.issuesMu.Lock()
	s.issues = append(s.issues, issue)
	s.issuesMu.Unlock()
}

func (s *LocalFilesystemBackend) NewRecordingDir(id string) error {
	if !recordingIDPattern.MatchString(id) {
		return fmt.Errorf("invalid recording id")
	}
	for _, dir := range []string{"manifests", "tracks/main"} {
		if err := os.MkdirAll(filepath.Join(s.recordingDir(id), dir), 0700); err != nil {
			return err
		}
	}
	for _, dir := range []string{s.recordingDir(id), filepath.Join(s.recordingDir(id), "manifests"), filepath.Join(s.recordingDir(id), "tracks"), filepath.Join(s.recordingDir(id), "tracks", "main")} {
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
	}
	return nil
}

// CreateRecording durably publishes a recording directory only after its
// initial self-describing root document exists. A crash before the final
// rename leaves an identifiable hidden staging directory for LoadAll to
// report; it never exposes a half-created final recording directory.
func (s *LocalFilesystemBackend) CreateRecording(recording *domain.Recording) (err error) {
	return s.createRecording(recording, "", nil)
}

// CreateRecordingWithSidecar stages both documents below a private directory
// and publishes their parent with one directory rename. A visible recording
// therefore always has its private initial sidecar.
func (s *LocalFilesystemBackend) CreateRecordingWithSidecar(recording *domain.Recording, relativePath string, value any) error {
	if !validInitialSidecarPath(relativePath) || value == nil {
		return errors.New("invalid recording sidecar initialization")
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil || int64(len(data)+1) > maxSidecarReadBytes {
		return errors.New("recording sidecar initialization exceeds size limit")
	}
	return s.createRecording(recording, relativePath, data)
}

func (s *LocalFilesystemBackend) createRecording(recording *domain.Recording, sidecarRelative string, sidecarData []byte) (err error) {
	started := time.Now()
	var written uint64
	defer func() {
		if err != nil {
			s.telemetry.recordError()
		} else {
			s.telemetry.recordWrite(written, time.Since(started))
		}
	}()
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		return fmt.Errorf("invalid recording")
	}
	if err := validateRecordingFormat(recording.FormatVersion); err != nil {
		return err
	}
	if recording.FormatVersion == ShardedArchiveFormatVersion {
		if err := validateV2Header(recording); err != nil {
			return err
		}
	}
	if err := domain.ValidateMetadataTimeline(recording.MetadataTimeline); err != nil {
		return fmt.Errorf("invalid recording metadata timeline")
	}
	base := filepath.Join(s.root, "recordings")
	final := s.recordingDir(recording.ID)
	if _, err := os.Lstat(final); err == nil {
		return fmt.Errorf("recording already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	stage, err := os.MkdirTemp(base, ".incomplete-"+recording.ID+"-")
	if err != nil {
		return err
	}
	if err = os.Chmod(stage, 0700); err != nil {
		return err
	}
	for _, dir := range []string{"manifests", "tracks", "tracks/main"} {
		path := filepath.Join(stage, dir)
		if err = os.MkdirAll(path, 0700); err != nil {
			return err
		}
		if err = os.Chmod(path, 0700); err != nil {
			return err
		}
	}
	if recording.Tracks == nil {
		recording.Tracks = map[string]*domain.Track{}
	}
	data, err := json.MarshalIndent(recording, "", "  ")
	if err != nil {
		return err
	}
	written = uint64(len(data) + 1)
	if sidecarRelative != "" {
		stageSidecar, pathErr := s.safePathForRoot(stage, sidecarRelative+".json")
		if pathErr != nil || !within(stage, stageSidecar) {
			return errors.New("invalid recording sidecar path")
		}
		if err = atomicWrite(stageSidecar, append(sidecarData, '\n'), 0600); err != nil {
			return err
		}
		written += uint64(len(sidecarData) + 1)
	}
	rootPath := filepath.Join(stage, "recording.json")
	if err = atomicWrite(rootPath, append(data, '\n'), 0600); err != nil {
		return err
	}
	// Persist all directory entries below the staging root before publishing
	// the root directory's name in the parent.
	dirs := []string{filepath.Join(stage, "tracks", "main"), filepath.Join(stage, "tracks"), filepath.Join(stage, "manifests")}
	if sidecarRelative != "" {
		for dir := filepath.Dir(filepath.Join(stage, filepath.FromSlash(sidecarRelative+".json"))); within(stage, dir) && dir != stage; dir = filepath.Dir(dir) {
			dirs = append(dirs, dir)
		}
	}
	dirs = append(dirs, stage)
	for _, dir := range dirs {
		if err = syncDirectory(dir); err != nil {
			return err
		}
	}
	if err = os.Rename(stage, final); err != nil {
		return err
	}
	return syncDirectory(base)
}

// safePathForRoot validates a recording-relative path against an arbitrary
// staging root without deriving authority from a not-yet-published recording.
func (s *LocalFilesystemBackend) safePathForRoot(root, relative string) (string, error) {
	if root == "" || !canonicalRelativePath(relative) {
		return "", errors.New("invalid relative storage path")
	}
	full := filepath.Join(root, filepath.FromSlash(relative))
	if !within(root, full) {
		return "", errors.New("storage path escapes recording directory")
	}
	return full, nil
}

func (s *LocalFilesystemBackend) SaveRecording(recording *domain.Recording) error {
	started := time.Now()
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		return fmt.Errorf("invalid recording")
	}
	if err := validateRecordingFormat(recording.FormatVersion); err != nil {
		s.telemetry.recordError()
		return err
	}
	if recording.FormatVersion == ShardedArchiveFormatVersion {
		if err := validateV2Header(recording); err != nil {
			s.telemetry.recordError()
			return err
		}
	}
	if err := domain.ValidateMetadataTimeline(recording.MetadataTimeline); err != nil {
		return fmt.Errorf("invalid recording metadata timeline")
	}
	data, err := json.MarshalIndent(recording, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	err = atomicWrite(filepath.Join(s.recordingDir(recording.ID), "recording.json"), data, 0600)
	if err != nil {
		s.telemetry.recordError()
	} else {
		s.telemetry.recordWrite(uint64(len(data)), time.Since(started))
	}
	return err
}

func (s *LocalFilesystemBackend) LoadAll() ([]*domain.Recording, error) {
	s.setRecoveryIssues(nil)
	base := filepath.Join(s.root, "recordings")
	if err := os.Chmod(base, 0700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var recordings []*domain.Recording
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".deleting-") {
			match := deletionTombstonePattern.FindStringSubmatch(entry.Name())
			if len(match) != 3 {
				s.addRecoveryIssue(RecoveryIssue{Code: "deletion_tombstone_invalid", Message: "recording deletion residue could not be identified safely"})
				continue
			}
			if cleanupErr := removeDeletionTombstones(base, match[1]); cleanupErr != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: match[1], Code: "deletion_cleanup_failed", Message: "a pending recording deletion could not be completed"})
			}
			continue
		}
		if !entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), ".incomplete-") {
			id := strings.TrimPrefix(entry.Name(), ".incomplete-")
			if i := strings.IndexByte(id, '-'); i >= 0 {
				id = id[:i]
			}
			if !recordingIDPattern.MatchString(id) {
				id = ""
			}
			if err = tightenRecordingTree(filepath.Join(base, entry.Name())); err != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "incomplete_permissions_unavailable", Message: "incomplete recording data could not be restricted safely"})
				continue
			}
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "incomplete_creation", Message: "incomplete recording creation data was preserved"})
			continue
		}
		if !recordingIDPattern.MatchString(entry.Name()) {
			continue
		}
		id := entry.Name()
		recordingDir := filepath.Join(base, id)
		dirInfo, dirErr := os.Lstat(recordingDir)
		if dirErr != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "permissions_unavailable", Message: "recording could not be restricted safely"})
			continue
		}
		path := filepath.Join(recordingDir, "recording.json")
		fileInfo, fileErr := os.Lstat(path)
		if fileErr != nil {
			code := "metadata_unavailable"
			if errors.Is(fileErr, os.ErrNotExist) {
				code = "metadata_missing"
			}
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: code, Message: "recording metadata is unavailable; directory data was preserved"})
			continue
		}
		if !fileInfo.Mode().IsRegular() || fileInfo.Mode()&os.ModeSymlink != 0 || fileInfo.Size() < 0 || fileInfo.Size() > 64<<20 {
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_unavailable", Message: "recording metadata is unavailable; directory data was preserved"})
			continue
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			code := "metadata_unavailable"
			if errors.Is(readErr, os.ErrNotExist) {
				code = "metadata_missing"
			}
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: code, Message: "recording metadata is unavailable; directory data was preserved"})
			continue
		}
		var recording domain.Recording
		if err = json.Unmarshal(data, &recording); err != nil {
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_invalid", Message: "recording metadata is invalid; directory data was preserved"})
			continue
		}
		if recording.ID != id {
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_id_mismatch", Message: "recording metadata identity does not match its directory"})
			continue
		}
		if recording.FormatVersion == ShardedArchiveFormatVersion {
			if err := os.Chmod(recordingDir, 0700); err != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "permissions_unavailable", Message: "recording could not be restricted safely"})
				continue
			}
			if err := os.Chmod(path, 0600); err != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "permissions_unavailable", Message: "recording metadata could not be restricted safely"})
				continue
			}
		} else if err := tightenRecordingTree(recordingDir); err != nil {
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "permissions_unavailable", Message: "recording could not be restricted safely"})
			continue
		}
		if err := validateRecordingFormat(recording.FormatVersion); err != nil {
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_format_unsupported", Message: "recording format is unsupported; archive data was preserved"})
			continue
		}
		changed := false
		if err := domain.ValidateMetadataTimeline(recording.MetadataTimeline); err != nil {
			// Preserve the media archive if a source-controlled projection is
			// malformed or over-bound. Drop only that projection and surface a
			// recovery issue instead of interpreting it as valid history.
			recording.MetadataTimeline = nil
			recording.MetadataTimelineTruncated = true
			changed = true
			s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_timeline_invalid", Message: "source metadata timeline was invalid and could not be restored"})
		}
		if recording.Tracks == nil {
			recording.Tracks = map[string]*domain.Track{}
		}
		if recording.FormatVersion == ShardedArchiveFormatVersion {
			if err := validateV2Header(&recording); err != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_invalid", Message: "sharded recording header is invalid; archive data was preserved"})
				continue
			}
		} else {
			reconcileChanged, reconcileErr := s.reconcileRecording(&recording)
			changed = changed || reconcileChanged
			if reconcileErr != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "reconciliation_incomplete", Message: "recording payload reconciliation was incomplete"})
			}
		}
		invalidTrack := false
		for _, track := range recording.Tracks {
			if track == nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "track_invalid", Message: "recording contains an invalid track and was preserved"})
				invalidTrack = true
			} else {
				sortTrack(track)
			}
		}
		if invalidTrack {
			continue
		}
		if recording.State == domain.StateRecording {
			now := time.Now().UTC()
			recording.State = domain.StateInterrupted
			recording.StoppedAt = &now
			if recording.LastError == "" {
				recording.LastError = "server restarted while recording was active"
			}
			for _, track := range recording.Tracks {
				addPendingGap := func(epoch, sequence uint64) {
					covered := false
					for _, gap := range recording.Gaps {
						if gap.TrackID == track.ID && gap.SourceEpoch == epoch && sequence >= gap.FromSequence && sequence <= gap.ToSequence {
							covered = true
							break
						}
					}
					if !covered {
						recording.Gaps = append(recording.Gaps, domain.Gap{TrackID: track.ID, SourceEpoch: epoch, FromSequence: sequence, ToSequence: sequence, DetectedAt: now, Reason: "server restarted before pending segment could be captured"})
					}
				}
				for _, sequence := range track.PendingSequences {
					// Older recordings did not persist epochs on pending sequences.
					addPendingGap(0, sequence)
				}
				for _, pending := range track.PendingSegments {
					addPendingGap(pending.SourceEpoch, pending.Sequence)
				}
				track.PendingSequences = nil
				track.PendingSegments = nil
			}
			changed = true
		}
		if changed {
			if err = s.SaveRecording(&recording); err != nil {
				s.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_recovery_write_failed", Message: "recovered metadata could not be durably saved"})
			}
		}
		recordings = append(recordings, &recording)
	}
	return recordings, nil
}

// LoadAll adds bounded v2 shard reconciliation to the backend's legacy
// startup recovery. Backends return v2 header documents without materializing
// archive history; this facade reconciles orphan publication candidates and
// refreshes returned headers.
func (s *Store) LoadAll() ([]*domain.Recording, error) {
	recordings, err := s.StorageBackend.LoadAll()
	if err != nil {
		return nil, err
	}
	for _, recording := range recordings {
		if recording == nil || recording.FormatVersion != ShardedArchiveFormatVersion {
			continue
		}
		// A sealed v2 root is an immutable recovery boundary. It is already
		// fully canonical by construction; startup must not attempt to adopt
		// orphan candidates into it or rewrite its archive state.
		if recording.ArchiveSealed {
			continue
		}
		if err := s.ReconcileShardedArchive(context.Background(), recording.ID); err != nil {
			return nil, err
		}
		header, err := s.LoadRecordingHeader(context.Background(), recording.ID)
		if err != nil {
			return nil, err
		}
		*recording = *header
	}
	return recordings, nil
}

// LoadAllReadOnly returns a snapshot of recording.json documents without
// running startup recovery, tightening permissions, reconciling sidecars, or
// publishing writes. It is used by fresh Engine generations that must observe
// archives owned by another Engine without taking ownership of them.
func (s *LocalFilesystemBackend) LoadAllReadOnly() ([]*domain.Recording, error) {
	return s.loadAllReadOnly(0, false)
}

// LoadAllReadOnlyLimit is the bounded variant used by management snapshots.
func (s *LocalFilesystemBackend) LoadAllReadOnlyLimit(max int) ([]*domain.Recording, error) {
	if max < 0 {
		return nil, ErrReadOnlyListLimit
	}
	return s.loadAllReadOnly(max, true)
}

func (s *LocalFilesystemBackend) loadAllReadOnly(max int, bounded bool) ([]*domain.Recording, error) {
	base := filepath.Join(s.root, "recordings")
	baseInfo, err := os.Lstat(base)
	if err != nil {
		return nil, err
	}
	if !baseInfo.IsDir() || baseInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("recording root is not a safe directory")
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var recordings []*domain.Recording
	for _, entry := range entries {
		if !entry.IsDir() || !recordingIDPattern.MatchString(entry.Name()) {
			continue
		}
		recording, loadErr := s.LoadRecordingReadOnly(entry.Name())
		if errors.Is(loadErr, os.ErrNotExist) {
			continue
		}
		if loadErr != nil {
			// A management snapshot is best-effort over individually malformed
			// archive documents, matching LoadAll's preservation behavior without
			// creating recovery side effects.
			continue
		}
		recordings = append(recordings, recording)
		if bounded && len(recordings) > max {
			return nil, ErrReadOnlyListLimit
		}
	}
	sort.Slice(recordings, func(i, j int) bool { return recordings[i].CreatedAt.After(recordings[j].CreatedAt) })
	return recordings, nil
}

// LoadRecordingReadOnly reads a single canonical root document without
// changing archive state. It strictly validates the opaque recording ID and
// rejects symlinked directories or metadata files.
func (s *LocalFilesystemBackend) LoadRecordingReadOnly(id string) (*domain.Recording, error) {
	if !recordingIDPattern.MatchString(id) {
		return nil, errors.New("invalid recording id")
	}
	base := filepath.Join(s.root, "recordings")
	baseInfo, err := os.Lstat(base)
	if err != nil {
		return nil, err
	}
	if !baseInfo.IsDir() || baseInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("recording root is not a safe directory")
	}
	dir := filepath.Join(base, id)
	dirInfo, err := os.Lstat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("recording directory is not a safe directory")
	}
	path := filepath.Join(dir, "recording.json")
	pathInfo, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("recording metadata is not a regular file")
	}
	data, err := readRecordingRootSnapshot(path, pathInfo)
	if err != nil {
		return nil, err
	}
	var recording domain.Recording
	if err := json.Unmarshal(data, &recording); err != nil {
		return nil, errors.New("recording metadata is invalid")
	}
	if recording.ID != id {
		return nil, errors.New("recording metadata identity mismatch")
	}
	if err := validateRecordingFormat(recording.FormatVersion); err != nil {
		return nil, err
	}
	if recording.FormatVersion == ShardedArchiveFormatVersion {
		if err := validateV2Header(&recording); err != nil {
			return nil, err
		}
	}
	if err := domain.ValidateMetadataTimeline(recording.MetadataTimeline); err != nil {
		return nil, errors.New("recording metadata timeline is invalid")
	}
	if recording.Tracks == nil {
		recording.Tracks = map[string]*domain.Track{}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			return nil, errors.New("recording contains an invalid track")
		}
		sortTrack(track)
	}
	return &recording, nil
}

// readRecordingRootSnapshot reads from one already-open file descriptor. A
// concurrent atomic root replacement can make the path refer to a different
// regular file between Lstat and Open; the descriptor still provides a stable
// old-or-new snapshot, so inode mismatch alone is not an error. Recheck the
// path after opening to preserve explicit symlink/non-regular rejection.
func readRecordingRootSnapshot(path string, observed os.FileInfo) ([]byte, error) {
	if observed == nil || !observed.Mode().IsRegular() || observed.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("recording metadata is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() {
		return nil, errors.New("recording metadata is not a regular file")
	}
	currentInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !currentInfo.Mode().IsRegular() || currentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("recording metadata is not a regular file")
	}
	if !os.SameFile(observed, openedInfo) && !os.SameFile(currentInfo, openedInfo) {
		// More than one replacement raced this open, or the opened file did not
		// belong to either the observed or current regular root. Refuse an
		// ambiguous snapshot while allowing one atomic publication in either
		// direction.
		return nil, errors.New("recording metadata changed during read")
	}
	// Canonical roots may grow with segment metadata. Keep this read bounded;
	// the limit is substantially above ordinary manifests while preventing an
	// accidental unbounded allocation in a read-only candidate process.
	const maxRecordingDocumentBytes = 64 << 20
	if openedInfo.Size() < 0 || openedInfo.Size() > maxRecordingDocumentBytes {
		return nil, errors.New("recording metadata exceeds read limit")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxRecordingDocumentBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxRecordingDocumentBytes {
		return nil, errors.New("recording metadata exceeds read limit")
	}
	return data, nil
}

func validateRecordingFormat(version int) error {
	if version == legacyRecordingFormatVersion || version == currentRecordingFormatVersion || version == ShardedArchiveFormatVersion {
		return nil
	}
	return ErrUnsupportedRecordingFormat
}

func (s *LocalFilesystemBackend) reconcileRecording(recording *domain.Recording) (bool, error) {
	root := s.recordingDir(recording.ID)
	knownPaths := make(map[string]struct{})
	for _, snapshot := range recording.Snapshots {
		knownPaths[filepath.ToSlash(snapshot.StoragePath)] = struct{}{}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range track.Segments {
			knownPaths[filepath.ToSlash(segment.StoragePath)] = struct{}{}
		}
		for _, segment := range track.InitSegments {
			knownPaths[filepath.ToSlash(segment.StoragePath)] = struct{}{}
		}
	}

	changed := false
	var walkErr error
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			walkErr = err
			return filepath.SkipAll
		}
		if entry.IsDir() {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			walkErr = infoErr
			return filepath.SkipAll
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			walkErr = relErr
			return filepath.SkipAll
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "manifests/") {
			if strings.HasSuffix(rel, ".m3u8.json") {
				payloadRel := strings.TrimSuffix(rel, ".json")
				s.reconcileManifestSidecar(recording, rel, payloadRel, &knownPaths, &changed)
			} else if strings.HasSuffix(rel, ".m3u8") {
				if _, known := knownPaths[rel]; !known {
					if _, sidecarErr := os.Lstat(filepath.Join(root, filepath.FromSlash(rel+".json"))); errors.Is(sidecarErr, os.ErrNotExist) {
						s.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "orphan_manifest", Message: "manifest payload without committed metadata was preserved"})
					}
				}
			}
			return nil
		}
		if !strings.HasPrefix(rel, "tracks/") || rel == "tracks/" {
			return nil
		}
		if strings.HasSuffix(rel, ".json") {
			payloadRel := strings.TrimSuffix(rel, ".json")
			s.reconcileSidecar(recording, rel, payloadRel, &knownPaths, &changed)
			return nil
		}
		if strings.HasPrefix(filepath.Base(rel), ".") && strings.HasSuffix(rel, ".tmp") {
			return nil
		}
		if _, ok := knownPaths[rel]; ok {
			return nil
		}
		if _, sidecarErr := os.Lstat(filepath.Join(root, filepath.FromSlash(rel+".json"))); errors.Is(sidecarErr, os.ErrNotExist) {
			s.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "orphan_payload", Message: "track payload without committed metadata was preserved"})
		}
		return nil
	})
	// recording.json is canonical, so sidecar reconciliation alone cannot
	// detect a payload that disappeared after both metadata writes completed.
	// Verify every canonical reference during recovery and preserve the record
	// for inspection; playback will fail closed when it cannot open the file.
	seen := map[string]bool{}
	verify := func(relative string, expectedSize int64, expectedHash string, unavailableCode, mismatchCode string) {
		// The same object may legitimately back multiple references, but each
		// canonical expectation still needs validation. Include the expected
		// integrity tuple in the deduplication key so a conflicting reference
		// to the same path cannot bypass verification.
		seenKey := fmt.Sprintf("%s\x00%d\x00%s", relative, expectedSize, expectedHash)
		if seen[seenKey] {
			return
		}
		seen[seenKey] = true
		issue := func(code, message string) {
			s.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: message})
		}
		if _, err := s.safePath(recording.ID, relative); err != nil || relative == "" {
			issue(unavailableCode, "canonical payload reference is invalid or unavailable; metadata was preserved")
			return
		}
		file, err := s.OpenPayloadReader(recording.ID, relative)
		if err != nil {
			issue(unavailableCode, "canonical payload is unavailable; metadata was preserved")
			return
		}
		hash := sha256.New()
		size, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		digest, decodeErr := hex.DecodeString(expectedHash)
		if copyErr != nil || closeErr != nil || decodeErr != nil || len(digest) != sha256.Size || size != expectedSize || !bytes.Equal(hash.Sum(nil), digest) {
			issue(mismatchCode, "canonical payload failed size or integrity verification; metadata was preserved")
		}
	}
	for _, snapshot := range recording.Snapshots {
		verify(snapshot.StoragePath, snapshot.Size, snapshot.SHA256, "manifest_payload_unavailable", "manifest_payload_mismatch")
	}
	for _, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range track.Segments {
			verify(segment.StoragePath, segment.PayloadSize, segment.SHA256, "canonical_payload_unavailable", "canonical_payload_mismatch")
		}
		for _, segment := range track.InitSegments {
			verify(segment.StoragePath, segment.PayloadSize, segment.SHA256, "canonical_payload_unavailable", "canonical_payload_mismatch")
		}
	}
	return changed, walkErr
}

func (s *LocalFilesystemBackend) reconcileManifestSidecar(recording *domain.Recording, sidecarRel, payloadRel string, knownPaths *map[string]struct{}, changed *bool) {
	issue := func(code, message string) {
		s.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: message})
	}
	data, err := os.ReadFile(filepath.Join(s.recordingDir(recording.ID), filepath.FromSlash(sidecarRel)))
	if err != nil {
		issue("manifest_sidecar_unavailable", "manifest metadata sidecar could not be read")
		return
	}
	var snapshot domain.ManifestSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil || snapshot.TrackID == "" || snapshot.StoragePath != payloadRel || snapshot.Size < 0 || len(snapshot.SHA256) != sha256.Size*2 {
		issue("manifest_sidecar_invalid", "manifest metadata sidecar is invalid")
		return
	}
	if _, ok := recording.Tracks[snapshot.TrackID]; !ok {
		issue("manifest_track_unknown", "manifest metadata refers to an undeclared track")
		return
	}
	digestBytes, err := hex.DecodeString(snapshot.SHA256)
	if err != nil || len(digestBytes) != sha256.Size {
		issue("manifest_sidecar_invalid", "manifest metadata sidecar has an invalid integrity digest")
		return
	}
	if _, err = s.safePath(recording.ID, payloadRel); err != nil {
		issue("manifest_path_invalid", "manifest storage path is invalid")
		return
	}
	file, err := s.OpenPayloadReader(recording.ID, payloadRel)
	if err != nil {
		issue("manifest_payload_unavailable", "manifest payload for committed metadata is unavailable")
		return
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || size != snapshot.Size || !bytes.Equal(hash.Sum(nil), digestBytes) {
		issue("manifest_payload_mismatch", "manifest payload failed size or integrity verification")
		return
	}
	for _, existing := range recording.Snapshots {
		if existing.StoragePath == payloadRel {
			if existing.SHA256 != snapshot.SHA256 || existing.Size != snapshot.Size || existing.TrackID != snapshot.TrackID {
				issue("manifest_sidecar_conflict", "manifest metadata conflicts with the canonical recording document")
			}
			return
		}
	}
	if _, found := (*knownPaths)[payloadRel]; found {
		issue("manifest_sidecar_conflict", "manifest storage path is already assigned in the canonical recording document")
		return
	}
	recording.Snapshots = append(recording.Snapshots, snapshot)
	(*knownPaths)[payloadRel] = struct{}{}
	*changed = true
}

func (s *LocalFilesystemBackend) reconcileSidecar(recording *domain.Recording, sidecarRel, payloadRel string, knownPaths *map[string]struct{}, changed *bool) {
	issue := func(code, message string) {
		s.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: message})
	}
	data, err := os.ReadFile(filepath.Join(s.recordingDir(recording.ID), filepath.FromSlash(sidecarRel)))
	if err != nil {
		issue("sidecar_unavailable", "segment metadata sidecar could not be read")
		return
	}
	var segment domain.Segment
	if err = json.Unmarshal(data, &segment); err != nil {
		issue("sidecar_invalid", "segment metadata sidecar is invalid")
		return
	}
	if segment.ID == "" || segment.TrackID == "" || segment.StoragePath != payloadRel || segment.PayloadSize < 0 || len(segment.SHA256) != sha256.Size*2 {
		issue("sidecar_invalid", "segment metadata sidecar does not describe its stored payload")
		return
	}
	digestBytes, err := hex.DecodeString(segment.SHA256)
	if err != nil || len(digestBytes) != sha256.Size {
		issue("sidecar_invalid", "segment metadata sidecar has an invalid integrity digest")
		return
	}
	track, ok := recording.Tracks[segment.TrackID]
	if !ok || track == nil || track.ID != segment.TrackID {
		issue("sidecar_track_unknown", "segment metadata refers to an undeclared track")
		return
	}
	if _, err = s.safePath(recording.ID, payloadRel); err != nil {
		issue("sidecar_path_invalid", "segment metadata storage path is invalid")
		return
	}
	file, err := s.OpenPayloadReader(recording.ID, payloadRel)
	if err != nil {
		issue("sidecar_payload_unavailable", "segment payload for committed metadata is unavailable")
		return
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || size != segment.PayloadSize || !bytes.Equal(hash.Sum(nil), digestBytes) {
		issue("sidecar_payload_mismatch", "segment payload failed size or integrity verification")
		return
	}

	if existing, found := findSegment(track, segment.ID, payloadRel); found {
		if existing.StoragePath != segment.StoragePath || existing.SHA256 != segment.SHA256 || existing.PayloadSize != segment.PayloadSize || existing.IsInit != segment.IsInit {
			issue("sidecar_conflict", "segment metadata conflicts with the canonical recording document")
		}
		return
	}
	if _, found := (*knownPaths)[payloadRel]; found {
		issue("sidecar_conflict", "segment storage path is already assigned in the canonical recording document")
		return
	}
	segment.StoragePath = payloadRel
	if segment.IsInit {
		track.InitSegments = append(track.InitSegments, segment)
	} else {
		track.Segments = append(track.Segments, segment)
	}
	(*knownPaths)[payloadRel] = struct{}{}
	*changed = true
}

func findSegment(track *domain.Track, id, storagePath string) (domain.Segment, bool) {
	for _, segment := range track.Segments {
		if segment.ID == id || segment.StoragePath == storagePath {
			return segment, true
		}
	}
	for _, segment := range track.InitSegments {
		if segment.ID == id || segment.StoragePath == storagePath {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

type PayloadResult struct {
	Size   int64
	SHA256 string
}

// SavePayload writes exactly the bytes read from src and publishes them only
// after a complete write. The hash is calculated over those same bytes.
func (s *LocalFilesystemBackend) SavePayload(id, relativePath string, src io.Reader, limit int64) (PayloadResult, error) {
	started := time.Now()
	result, err := s.savePayload(id, relativePath, src, limit, -1)
	s.recordPayloadWrite(result, started, err)
	return result, err
}

// SavePayloadExact also requires a specific byte count before publishing the
// file, which is used for HLS byte-range responses.
func (s *LocalFilesystemBackend) SavePayloadExact(id, relativePath string, src io.Reader, limit, expectedSize int64) (PayloadResult, error) {
	started := time.Now()
	result, err := s.savePayload(id, relativePath, src, limit, expectedSize)
	s.recordPayloadWrite(result, started, err)
	return result, err
}

func (s *LocalFilesystemBackend) recordPayloadWrite(result PayloadResult, started time.Time, err error) {
	if err != nil {
		s.telemetry.recordError()
		return
	}
	s.telemetry.recordWrite(uint64(result.Size), time.Since(started))
}

func (s *LocalFilesystemBackend) savePayload(id, relativePath string, src io.Reader, limit, expectedSize int64) (PayloadResult, error) {
	destination, err := s.safePath(id, relativePath)
	if err != nil {
		return PayloadResult{}, err
	}
	if err = os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		return PayloadResult{}, err
	}
	if err = os.Chmod(filepath.Dir(destination), 0700); err != nil {
		return PayloadResult{}, err
	}
	if err = s.syncRecordingDirectories(id, filepath.Dir(destination)); err != nil {
		return PayloadResult{}, err
	}
	f, err := os.CreateTemp(filepath.Dir(destination), ".payload-*.tmp")
	if err != nil {
		return PayloadResult{}, err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	hash := sha256.New()
	reader := io.Reader(src)
	if limit > 0 {
		reader = io.LimitReader(src, limit+1)
	}
	n, copyErr := io.Copy(io.MultiWriter(f, hash), reader)
	if copyErr == nil && limit > 0 && n > limit {
		copyErr = fmt.Errorf("payload exceeds %d bytes", limit)
	}
	if copyErr == nil && expectedSize >= 0 && n != expectedSize {
		copyErr = fmt.Errorf("%w: got %d bytes; expected %d", ErrPayloadSizeMismatch, n, expectedSize)
	}
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return PayloadResult{}, copyErr
	}
	if err = os.Rename(tmp, destination); err != nil {
		return PayloadResult{}, err
	}
	if err = syncDirectory(filepath.Dir(destination)); err != nil {
		return PayloadResult{}, err
	}
	return PayloadResult{Size: n, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (s *LocalFilesystemBackend) SaveSidecar(id, relativePath string, v any) error {
	started := time.Now()
	path, err := s.safePath(id, relativePath)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	err = atomicWrite(path+".json", data, 0600)
	if err != nil {
		s.telemetry.recordError()
	} else {
		s.telemetry.recordWrite(uint64(len(data)), time.Since(started))
	}
	return err
}

// LoadSidecar is an optional, read-only JSON sidecar capability. The read is
// bounded and uses the same path containment checks as canonical payload reads.
func (s *LocalFilesystemBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	if err := validateSidecarRead(id, relativePath, maxBytes, output); err != nil {
		return err
	}
	started := time.Now()
	file, info, err := s.openPayload(id, relativePath+".json")
	if err != nil {
		s.telemetry.recordError()
		return err
	}
	defer file.Close()
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		s.telemetry.recordError()
		return errors.New("sidecar exceeds size limit")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		s.telemetry.recordError()
		return errors.New("sidecar is unavailable")
	}
	if int64(len(data)) > maxBytes {
		s.telemetry.recordError()
		return errors.New("sidecar exceeds size limit")
	}
	if err = decodeStrictSidecar(data, output); err != nil {
		s.telemetry.recordError()
		return err
	}
	s.telemetry.recordRead(uint64(len(data)), 1, time.Since(started))
	return nil
}

func (s *LocalFilesystemBackend) SaveSnapshot(id, trackID, sourceURI string, data []byte, at time.Time) (domain.ManifestSnapshot, error) {
	if len(data) > 4<<20 {
		return domain.ManifestSnapshot{}, fmt.Errorf("manifest exceeds size limit")
	}
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	name := fmt.Sprintf("manifests/%s-%d-%s.m3u8", safeName(trackID), at.UnixNano(), digest[:12])
	if _, err := s.SavePayload(id, name, bytes.NewReader(data), 4<<20); err != nil {
		return domain.ManifestSnapshot{}, err
	}
	snapshot := domain.ManifestSnapshot{TrackID: trackID, SourceURI: sourceURI, StoragePath: name, FetchedAt: at, SHA256: digest, Size: int64(len(data))}
	if err := s.SaveSidecar(id, name, snapshot); err != nil {
		return domain.ManifestSnapshot{}, err
	}
	return snapshot, nil
}

func (s *LocalFilesystemBackend) OpenPayload(id, relativePath string) (*os.File, error) {
	f, _, err := s.openPayload(id, relativePath)
	if err != nil {
		s.telemetry.recordError()
		return nil, err
	}
	return f, nil
}

// ObjectInfo is a filesystem-independent view of one canonical object.
type ObjectInfo struct {
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at"`
	Regular    bool      `json:"regular"`
}

func (s *LocalFilesystemBackend) StatPayload(id, relativePath string) (ObjectInfo, error) {
	path, err := s.safePath(id, relativePath)
	if err != nil {
		return ObjectInfo{}, err
	}
	resolvedRoot, err := filepath.EvalSymlinks(s.recordingDir(id))
	if err != nil {
		return ObjectInfo{}, err
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return ObjectInfo{}, err
	}
	if !within(resolvedRoot, resolvedPath) {
		return ObjectInfo{}, fmt.Errorf("payload path escapes recording directory")
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Size: info.Size(), ModifiedAt: info.ModTime(), Regular: info.Mode().IsRegular()}, nil
}

func (s *LocalFilesystemBackend) OpenPayloadReader(id, relativePath string) (io.ReadCloser, error) {
	f, _, err := s.openPayload(id, relativePath)
	if err != nil {
		s.telemetry.recordError()
		return nil, err
	}
	return &meteredReadCloser{File: f, telemetry: s.telemetry}, nil
}

// OpenPayloadRangeReaderContext opens only the requested byte range of a
// canonical payload. The logical path is validated through the same safe
// opener as full payload reads, and the section reader cannot read beyond the
// requested length.
func (s *LocalFilesystemBackend) OpenPayloadRangeReaderContext(ctx context.Context, id, relativePath string, offset, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || length > MaxObjectBytes || offset > MaxObjectBytes-length {
		return nil, fmt.Errorf("invalid payload range")
	}
	f, info, err := s.openPayload(id, relativePath)
	if err != nil {
		s.telemetry.recordError()
		return nil, err
	}
	if !info.Mode().IsRegular() || offset > info.Size()-length {
		_ = f.Close()
		return nil, fmt.Errorf("invalid payload range")
	}
	return &meteredSectionReadCloser{
		reader:    io.NewSectionReader(f, offset, length),
		file:      f,
		telemetry: s.telemetry,
	}, nil
}

type meteredSectionReadCloser struct {
	reader    io.Reader
	file      *os.File
	telemetry *telemetry
	closeOnce sync.Once
	closeErr  error
}

func (r *meteredSectionReadCloser) Read(p []byte) (int, error) {
	started := time.Now()
	n, err := r.reader.Read(p)
	if n > 0 {
		r.telemetry.recordRead(uint64(n), 1, time.Since(started))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.telemetry.recordError()
	}
	return n, err
}

func (r *meteredSectionReadCloser) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.file.Close()
		if r.closeErr != nil {
			r.telemetry.recordError()
		}
	})
	return r.closeErr
}

type meteredReadCloser struct {
	*os.File
	telemetry *telemetry
	closeOnce sync.Once
	closeErr  error
}

func (r *meteredReadCloser) Read(p []byte) (int, error) {
	started := time.Now()
	n, err := r.File.Read(p)
	if n > 0 {
		// Record each successful filesystem read immediately so active playback
		// and export throughput is visible before the reader is closed. Only the
		// OS Read call is timed; consumer think time between calls is excluded.
		r.telemetry.recordRead(uint64(n), 1, time.Since(started))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.telemetry.recordError()
	}
	return n, err
}

// WriteTo deliberately funnels optimized io.Copy calls back through Read so
// bytes remain observable. Embedding *os.File otherwise promotes its native
// WriteTo implementation, which bypasses this wrapper's Read method.
func (r *meteredReadCloser) WriteTo(dst io.Writer) (int64, error) {
	return io.Copy(dst, meteredReadOnly{reader: r})
}

type meteredReadOnly struct{ reader io.Reader }

func (r meteredReadOnly) Read(p []byte) (int, error) { return r.reader.Read(p) }

func (r *meteredReadCloser) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.File.Close()
		if r.closeErr != nil {
			r.telemetry.recordError()
		}
	})
	return r.closeErr
}

func (s *LocalFilesystemBackend) openPayload(id, relativePath string) (*os.File, os.FileInfo, error) {
	path, err := s.safePath(id, relativePath)
	if err != nil {
		return nil, nil, err
	}
	resolvedRoot, err := filepath.EvalSymlinks(s.recordingDir(id))
	if err != nil {
		return nil, nil, err
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, nil, err
	}
	if !within(resolvedRoot, resolvedPath) {
		return nil, nil, fmt.Errorf("payload path escapes recording directory")
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("payload is not a regular file")
	}
	f, err := os.Open(resolvedPath)
	return f, info, err
}

func (s *LocalFilesystemBackend) recordingDir(id string) string {
	return filepath.Join(s.root, "recordings", id)
}

func (s *LocalFilesystemBackend) safePath(id, relative string) (string, error) {
	if !recordingIDPattern.MatchString(id) {
		return "", fmt.Errorf("invalid recording id")
	}
	if filepath.IsAbs(relative) || relative == "" {
		return "", fmt.Errorf("invalid relative storage path")
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("storage path escapes recording directory")
	}
	root := s.recordingDir(id)
	full := filepath.Join(root, clean)
	if !within(root, full) {
		return "", fmt.Errorf("storage path escapes recording directory")
	}
	return full, nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".metadata-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (s *LocalFilesystemBackend) syncRecordingDirectories(id, leaf string) error {
	root := s.recordingDir(id)
	for dir := leaf; ; dir = filepath.Dir(dir) {
		if !within(root, dir) {
			return fmt.Errorf("storage directory escapes recording")
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
		if dir == root {
			return nil
		}
	}
}

// syncDirectory makes rename and directory-entry updates durable where the
// platform/filesystem supports syncing directory descriptors. Windows does
// not expose this through os.File.Sync; POSIX EINVAL is the common unsupported
// fallback. Other I/O failures are returned to the caller.
func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		if errors.Is(syncErr, syscall.EINVAL) || errors.Is(syncErr, syscall.ENOTSUP) {
			return closeErr
		}
		return syncErr
	}
	return closeErr
}

func tightenRecordingTree(root string) error {
	if err := os.Chmod(root, 0700); err != nil {
		return err
	}
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.Chmod(path, 0700)
		}
		if info.Mode().IsRegular() {
			return os.Chmod(path, 0600)
		}
		return nil
	})
}

func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func safeName(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "main"
	}
	return b.String()
}
func sortTrack(track *domain.Track) {
	sortSegments := func(segments []domain.Segment) {
		sort.SliceStable(segments, func(i, j int) bool {
			a, b := segments[i], segments[j]
			if a.ArchiveOrdinal != 0 || b.ArchiveOrdinal != 0 {
				if a.ArchiveOrdinal == 0 {
					return false
				}
				if b.ArchiveOrdinal == 0 {
					return true
				}
				return a.ArchiveOrdinal < b.ArchiveOrdinal
			}
			if a.SourceEpoch != b.SourceEpoch {
				return a.SourceEpoch < b.SourceEpoch
			}
			return a.Sequence < b.Sequence
		})
	}
	sortSegments(track.Segments)
	sortSegments(track.InitSegments)
}

var ErrNotFound = errors.New("recording not found")
