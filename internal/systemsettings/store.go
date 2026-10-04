// Package systemsettings stores the small set of Core settings that can be
// applied by the running management plane.
package systemsettings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/storage"
)

const (
	settingsDirectory    = "management"
	settingsFilename     = "system-settings.json"
	maxSettingsBytes     = 16 * 1024
	defaultTheme         = "system"
	defaultConcurrency   = 2
	defaultRetentionDays = 30
)

var (
	ErrInvalidSettings = errors.New("invalid system settings")
	ErrUnsafePath      = errors.New("unsafe system settings path")
)

// Settings is the public, self-describing system settings document.
type Settings struct {
	UI        UISettings        `json:"ui"`
	Integrity IntegritySettings `json:"integrity"`
	Retention RetentionSettings `json:"retention"`
	Storage   StorageSettings   `json:"storage"`
}

// StorageSettings contains operational limits for volatile ingest and
// recorder-originated storage metrics. It is management state, never archive
// metadata. Values use stable byte, count, and millisecond units.
type StorageSettings struct {
	IngestMemory    IngestMemorySettings    `json:"ingest_memory"`
	QueueWriter     QueueWriterSettings     `json:"queue_writer"`
	FailureHandling FailureHandlingSettings `json:"failure_handling"`
	Observability   ObservabilitySettings   `json:"observability"`
}

type IngestMemorySettings struct {
	GlobalBufferBytes       int64 `json:"global_buffer_bytes"`
	PerRecordingBufferBytes int64 `json:"per_recording_buffer_bytes"`
	MaxPayloadBytes         int64 `json:"max_payload_bytes"`
}

type QueueWriterSettings struct {
	PendingQueueCapacity int `json:"pending_queue_capacity"`
	WriterConcurrency    int `json:"writer_concurrency"`
}

type FailureHandlingSettings struct {
	PersistAttempts       int   `json:"persist_attempts"`
	RetryInitialBackoffMS int64 `json:"retry_initial_backoff_ms"`
	RetryMaxBackoffMS     int64 `json:"retry_max_backoff_ms"`
}

type ObservabilitySettings struct {
	SamplingIntervalMS int64 `json:"sampling_interval_ms"`
	MetricsRetentionMS int64 `json:"metrics_retention_ms"`
}

type UISettings struct {
	Theme string `json:"theme"`
}

type IntegritySettings struct {
	Concurrency int `json:"concurrency"`
}

// RetentionSettings controls the optional deletion of old completed
// recordings. Cleanup is disabled unless explicitly enabled.
type RetentionSettings struct {
	Enabled            bool `json:"enabled"`
	CompletedAfterDays int  `json:"completed_after_days"`
}

// Patch updates only the fields provided by the caller.
type Patch struct {
	UITheme              *string          `json:"ui_theme,omitempty"`
	IntegrityConcurrency *int             `json:"integrity_concurrency,omitempty"`
	RetentionEnabled     *bool            `json:"retention_enabled,omitempty"`
	RetentionAfterDays   *int             `json:"retention_completed_after_days,omitempty"`
	Storage              *StorageSettings `json:"storage,omitempty"`
}

// Store serializes updates and publishes a new in-memory snapshot only after
// the corresponding file has been durably replaced.
type Store struct {
	mu        sync.RWMutex
	path      string
	directory string
	settings  Settings
}

// Open loads the system settings from <dataRoot>/management. Missing settings
// are initialized with the supported defaults.
func Open(dataRoot string) (*Store, error) {
	if strings.TrimSpace(dataRoot) == "" {
		return nil, fmt.Errorf("%w: empty data root", ErrUnsafePath)
	}
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve data root", ErrUnsafePath)
	}
	if err := ensureDataRoot(root); err != nil {
		return nil, err
	}
	dir := filepath.Join(root, settingsDirectory)
	if err := ensurePrivateDirectory(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, settingsFilename)
	store := &Store{path: path, directory: dir, settings: defaultSettings()}

	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := store.write(store.settings); err != nil {
			return nil, fmt.Errorf("initialize system settings: %w", err)
		}
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect system settings: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnsafePath
	}
	if info.Size() < 0 || info.Size() > maxSettingsBytes {
		return nil, fmt.Errorf("%w: settings file exceeds size limit", ErrInvalidSettings)
	}
	loaded, migratedLegacyAttempts, err := readSettings(path)
	if err != nil {
		return nil, err
	}
	if err := validate(loaded); err != nil {
		return nil, err
	}
	if migratedLegacyAttempts {
		// Rewrite the validated legacy value through the existing atomic private
		// persistence path. Its numeric meaning remains total persist attempts.
		if err := store.write(loaded); err != nil {
			return nil, fmt.Errorf("migrate system settings: %w", err)
		}
	} else if err := os.Chmod(path, 0600); err != nil {
		return nil, fmt.Errorf("secure system settings file: %w", err)
	}
	store.settings = loaded
	return store, nil
}

// Current returns a value copy of the current settings.
func (s *Store) Current() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

// IntegrityConcurrency returns the currently configured worker count.
func (s *Store) IntegrityConcurrency() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings.Integrity.Concurrency
}

// IngestOptions converts persisted storage settings into runtime configuration.
// Callers apply it before constructing the acquisition manager because queue,
// buffer, and sampler structures are fixed for that service lifetime.
func (settings StorageSettings) IngestOptions() storage.IngestOptions {
	return storage.IngestOptions{
		QueueObjects:      settings.QueueWriter.PendingQueueCapacity,
		GlobalBytes:       settings.IngestMemory.GlobalBufferBytes,
		PerRecordingBytes: settings.IngestMemory.PerRecordingBufferBytes,
		MaxPayloadBytes:   settings.IngestMemory.MaxPayloadBytes,
		Writers:           settings.QueueWriter.WriterConcurrency,
		PersistAttempts:   settings.FailureHandling.PersistAttempts,
		RetryBase:         time.Duration(settings.FailureHandling.RetryInitialBackoffMS) * time.Millisecond,
		RetryMaxBackoff:   time.Duration(settings.FailureHandling.RetryMaxBackoffMS) * time.Millisecond,
		SampleInterval:    time.Duration(settings.Observability.SamplingIntervalMS) * time.Millisecond,
		MetricsRetention:  time.Duration(settings.Observability.MetricsRetentionMS) * time.Millisecond,
	}
}

// Update validates and persists a partial settings update atomically.
func (s *Store) Update(patch Patch) (Settings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := s.settings
	if patch.UITheme != nil {
		candidate.UI.Theme = *patch.UITheme
	}
	if patch.IntegrityConcurrency != nil {
		candidate.Integrity.Concurrency = *patch.IntegrityConcurrency
	}
	if patch.RetentionEnabled != nil {
		candidate.Retention.Enabled = *patch.RetentionEnabled
	}
	if patch.RetentionAfterDays != nil {
		candidate.Retention.CompletedAfterDays = *patch.RetentionAfterDays
	}
	if patch.Storage != nil {
		candidate.Storage = *patch.Storage
	}
	if err := validate(candidate); err != nil {
		return s.settings, err
	}
	if err := s.write(candidate); err != nil {
		return s.settings, fmt.Errorf("persist system settings: %w", err)
	}
	s.settings = candidate
	return candidate, nil
}

func validate(settings Settings) error {
	switch settings.UI.Theme {
	case "system", "light", "dark":
	default:
		return fmt.Errorf("%w: unsupported UI theme", ErrInvalidSettings)
	}
	if settings.Integrity.Concurrency < 1 || settings.Integrity.Concurrency > 4 {
		return fmt.Errorf("%w: integrity concurrency is out of range", ErrInvalidSettings)
	}
	if settings.Retention.CompletedAfterDays < 1 || settings.Retention.CompletedAfterDays > 3650 {
		return fmt.Errorf("%w: retention age is out of range", ErrInvalidSettings)
	}
	// Validate persisted millisecond values before converting them to
	// time.Duration; multiplication by time.Millisecond can overflow int64.
	storageSettings := settings.Storage
	if storageSettings.FailureHandling.RetryInitialBackoffMS < 10 || storageSettings.FailureHandling.RetryInitialBackoffMS > 30_000 {
		return fmt.Errorf("%w: initial storage retry backoff must be between 10 ms and 30 s", ErrInvalidSettings)
	}
	if storageSettings.FailureHandling.RetryMaxBackoffMS < storageSettings.FailureHandling.RetryInitialBackoffMS || storageSettings.FailureHandling.RetryMaxBackoffMS > 300_000 {
		return fmt.Errorf("%w: maximum storage retry backoff must be at least the initial backoff and at most 5 minutes", ErrInvalidSettings)
	}
	if storageSettings.Observability.SamplingIntervalMS < 1_000 || storageSettings.Observability.SamplingIntervalMS > 3_600_000 {
		return fmt.Errorf("%w: storage metrics sampling interval must be between 1 s and 1 hour", ErrInvalidSettings)
	}
	if storageSettings.Observability.MetricsRetentionMS < 1 || storageSettings.Observability.MetricsRetentionMS > 86_400_000 {
		return fmt.Errorf("%w: storage metrics retention must be between 1 ms and 24 hours", ErrInvalidSettings)
	}
	if err := storage.ValidateIngestOptions(settings.Storage.IngestOptions()); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSettings, err)
	}
	return nil
}

func defaultStorageSettings() StorageSettings {
	options := storage.DefaultIngestOptions()
	return StorageSettings{
		IngestMemory: IngestMemorySettings{
			GlobalBufferBytes:       options.GlobalBytes,
			PerRecordingBufferBytes: options.PerRecordingBytes,
			MaxPayloadBytes:         options.MaxPayloadBytes,
		},
		QueueWriter: QueueWriterSettings{
			PendingQueueCapacity: options.QueueObjects,
			WriterConcurrency:    options.Writers,
		},
		FailureHandling: FailureHandlingSettings{
			PersistAttempts:       options.PersistAttempts,
			RetryInitialBackoffMS: options.RetryBase.Milliseconds(),
			RetryMaxBackoffMS:     options.RetryMaxBackoff.Milliseconds(),
		},
		Observability: ObservabilitySettings{
			SamplingIntervalMS: options.SampleInterval.Milliseconds(),
			MetricsRetentionMS: options.MetricsRetention.Milliseconds(),
		},
	}
}

func defaultSettings() Settings {
	return Settings{
		UI:        UISettings{Theme: defaultTheme},
		Integrity: IntegritySettings{Concurrency: defaultConcurrency},
		Retention: RetentionSettings{Enabled: false, CompletedAfterDays: defaultRetentionDays},
		Storage:   defaultStorageSettings(),
	}
}

func readSettings(path string) (Settings, bool, error) {
	settings := defaultSettings()
	f, err := os.Open(path)
	if err != nil {
		return settings, false, fmt.Errorf("open system settings: %w", err)
	}
	defer f.Close()
	openedInfo, err := f.Stat()
	if err != nil {
		return settings, false, fmt.Errorf("inspect system settings: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(openedInfo, pathInfo) {
		return settings, false, ErrUnsafePath
	}
	data, err := io.ReadAll(io.LimitReader(f, maxSettingsBytes+1))
	if err != nil {
		return settings, false, fmt.Errorf("read system settings: %w", err)
	}
	if len(data) == 0 || len(data) > maxSettingsBytes {
		return settings, false, fmt.Errorf("%w: invalid settings size", ErrInvalidSettings)
	}
	data, migrated, err := migrateLegacyPersistAttempts(data)
	if err != nil {
		return settings, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&settings); err != nil {
		return settings, false, fmt.Errorf("%w: malformed settings document", ErrInvalidSettings)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return settings, false, fmt.Errorf("%w: trailing data", ErrInvalidSettings)
	}
	return settings, migrated, nil
}

// migrateLegacyPersistAttempts accepts the former retry_attempts key only in
// persisted settings files. It preserves strict decoding for every other key,
// rejects ambiguous documents containing both names, and emits only the
// canonical persist_attempts key on the next atomic save.
func migrateLegacyPersistAttempts(data []byte) ([]byte, bool, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, false, fmt.Errorf("%w: malformed settings document", ErrInvalidSettings)
	}
	if root == nil {
		return data, false, nil
	}
	failureRaw, exists := root["storage"]
	if !exists {
		return data, false, nil
	}
	var storageObject map[string]json.RawMessage
	if err := json.Unmarshal(failureRaw, &storageObject); err != nil {
		// The strict typed decoder below returns the authoritative malformed
		// document error for a non-object storage value.
		return data, false, nil
	}
	failureRaw, exists = storageObject["failure_handling"]
	if !exists {
		return data, false, nil
	}
	var failureObject map[string]json.RawMessage
	if err := json.Unmarshal(failureRaw, &failureObject); err != nil {
		return data, false, nil
	}
	legacy, hasLegacy := failureObject["retry_attempts"]
	_, hasCanonical := failureObject["persist_attempts"]
	if !hasLegacy {
		return data, false, nil
	}
	if hasCanonical {
		return nil, false, fmt.Errorf("%w: both legacy retry_attempts and persist_attempts are present", ErrInvalidSettings)
	}
	failureObject["persist_attempts"] = legacy
	delete(failureObject, "retry_attempts")
	failureJSON, err := json.Marshal(failureObject)
	if err != nil {
		return nil, false, fmt.Errorf("%w: malformed settings document", ErrInvalidSettings)
	}
	storageObject["failure_handling"] = failureJSON
	storageJSON, err := json.Marshal(storageObject)
	if err != nil {
		return nil, false, fmt.Errorf("%w: malformed settings document", ErrInvalidSettings)
	}
	root["storage"] = storageJSON
	migrated, err := json.Marshal(root)
	if err != nil {
		return nil, false, fmt.Errorf("%w: malformed settings document", ErrInvalidSettings)
	}
	if len(migrated) > maxSettingsBytes {
		return nil, false, fmt.Errorf("%w: settings file exceeds size limit", ErrInvalidSettings)
	}
	return migrated, true, nil
}

func (s *Store) write(settings Settings) error {
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > maxSettingsBytes {
		return fmt.Errorf("%w: settings document exceeds size limit", ErrInvalidSettings)
	}
	if err := checkRegularOrMissing(s.path); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.directory, ".system-settings-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := checkRegularOrMissing(s.path); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	return syncDirectory(s.directory)
}

func checkRegularOrMissing(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafePath
	}
	return nil
}

func ensureDataRoot(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return fmt.Errorf("create data root: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafePath
	}
	return nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create settings directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafePath
	}
	if err := os.Chmod(path, 0700); err != nil {
		return fmt.Errorf("secure settings directory: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
