package systemsettings

import (
	"os"
	"path/filepath"
	"testing"
)

// This is the persisted system-settings shape emitted by RC12. In particular,
// RC12 predates the replication policy field added in v0.2.
const rc12SystemSettings = `{
  "ui": {"theme": "dark"},
  "integrity": {"concurrency": 2},
  "retention": {"enabled": true, "completed_after_days": 45},
  "storage": {
    "ingest_memory": {
      "global_buffer_bytes": 1610612736,
      "per_recording_buffer_bytes": 1073741824,
      "max_payload_bytes": 268435456
    },
    "queue_writer": {"pending_queue_capacity": 96, "writer_concurrency": 1},
    "failure_handling": {
      "persist_attempts": 4,
      "retry_initial_backoff_ms": 250,
      "retry_max_backoff_ms": 2000
    },
    "observability": {"sampling_interval_ms": 10000, "metrics_retention_ms": 3600000}
  }
}`

func TestOpenRC12SettingsPreservesValuesAndDefaultsNewReplicationPolicy(t *testing.T) {
	root := t.TempDir()
	management := filepath.Join(root, settingsDirectory)
	if err := os.Mkdir(management, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(management, settingsFilename)
	fixture := []byte(rc12SystemSettings + "\n")
	if err := os.WriteFile(path, fixture, 0600); err != nil {
		t.Fatal(err)
	}

	store, err := Open(root)
	if err != nil {
		t.Fatalf("open RC12 settings: %v", err)
	}
	got := store.Current()
	if got.UI.Theme != "dark" || got.Integrity.Concurrency != 2 ||
		!got.Retention.Enabled || got.Retention.CompletedAfterDays != 45 {
		t.Fatalf("RC12 settings changed on open: %+v", got)
	}
	if got.Replication.ProviderCapacityBytes != "0" {
		t.Fatalf("new provider capacity default = %q, want unlimited-compatible zero", got.Replication.ProviderCapacityBytes)
	}
	wantStorage := StorageSettings{
		IngestMemory:    IngestMemorySettings{GlobalBufferBytes: 1610612736, PerRecordingBufferBytes: 1073741824, MaxPayloadBytes: 268435456},
		QueueWriter:     QueueWriterSettings{PendingQueueCapacity: 96, WriterConcurrency: 1},
		FailureHandling: FailureHandlingSettings{PersistAttempts: 4, RetryInitialBackoffMS: 250, RetryMaxBackoffMS: 2000},
		Observability:   ObservabilitySettings{SamplingIntervalMS: 10000, MetricsRetentionMS: 3600000},
	}
	if got.Storage != wantStorage {
		t.Fatalf("RC12 storage settings changed on open: got=%+v want=%+v", got.Storage, wantStorage)
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != string(fixture) {
		t.Fatalf("opening RC12 settings rewrote the legacy document: err=%v", err)
	}

	// A normal v0.2 settings update must retain the RC12 values and materialize
	// the safe default for the newly introduced setting.
	light := "light"
	updated, err := store.Update(Patch{UITheme: &light})
	if err != nil {
		t.Fatalf("update migrated RC12 settings: %v", err)
	}
	if updated.UI.Theme != light || updated.Retention.CompletedAfterDays != 45 ||
		updated.Replication.ProviderCapacityBytes != "0" || updated.Storage != wantStorage {
		t.Fatalf("updating RC12 settings lost existing values or changed new-policy default: %+v", updated)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatalf("reopen upgraded settings: %v", err)
	}
	if reopened.Current() != updated {
		t.Fatalf("persisted settings mismatch after RC12-compatible update: got=%+v want=%+v", reopened.Current(), updated)
	}
}
