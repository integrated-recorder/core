package systemsettings

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestOpenDefaultsAndPermissions(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	want := defaultSettings()
	if got := store.Current(); got != want {
		t.Fatalf("Current() = %#v, want %#v", got, want)
	}
	if got := store.IntegrityConcurrency(); got != 2 {
		t.Fatalf("IntegrityConcurrency() = %d, want 2", got)
	}
	if got := store.Current().Replication.ProviderCapacityBytes; got != "0" {
		t.Fatalf("provider capacity default = %q, want unlimited (0)", got)
	}
	if mode := modeOf(t, filepath.Join(root, "management")); mode.Perm() != 0700 {
		t.Fatalf("management mode = %04o, want 0700", mode.Perm())
	}
	if mode := modeOf(t, store.path); mode.Perm() != 0600 {
		t.Fatalf("settings mode = %04o, want 0600", mode.Perm())
	}
}

func TestUpdatePersistsAndReloads(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	theme := "dark"
	concurrency := 4
	got, err := store.Update(Patch{UITheme: &theme, IntegrityConcurrency: &concurrency})
	if err != nil {
		t.Fatal(err)
	}
	want := defaultSettings()
	want.UI.Theme = "dark"
	want.Integrity.Concurrency = 4
	if got != want {
		t.Fatalf("Update() = %#v, want %#v", got, want)
	}
	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Current(); got != want {
		t.Fatalf("reloaded Current() = %#v, want %#v", got, want)
	}
}

func TestUpdateRejectsInvalidValuesWithoutEchoingThem(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		patch Patch
	}{
		{name: "theme", patch: Patch{UITheme: ptr("neon-secret-value")}},
		{name: "low concurrency", patch: Patch{IntegrityConcurrency: ptr(0)}},
		{name: "high concurrency", patch: Patch{IntegrityConcurrency: ptr(5)}},
		{name: "retention below range", patch: Patch{RetentionAfterDays: ptr(0)}},
		{name: "retention above range", patch: Patch{RetentionAfterDays: ptr(3651)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := store.Current()
			_, err := store.Update(test.patch)
			if !errors.Is(err, ErrInvalidSettings) {
				t.Fatalf("Update() error = %v, want ErrInvalidSettings", err)
			}
			if got := store.Current(); got != before {
				t.Fatalf("invalid update changed settings: %#v", got)
			}
			if test.patch.UITheme != nil && contains(err.Error(), *test.patch.UITheme) {
				t.Fatalf("error echoed input value: %v", err)
			}
		})
	}
}

func TestOpenBackfillsRetentionDefaultsForExistingSettings(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "management"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "management", settingsFilename)
	if err := os.WriteFile(path, []byte(`{"ui":{"theme":"dark"},"integrity":{"concurrency":3}}`), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	want := RetentionSettings{Enabled: false, CompletedAfterDays: 30}
	if got := store.Current().Retention; got != want {
		t.Fatalf("existing settings retention = %#v, want %#v", got, want)
	}
	if got := store.Current().Storage; got != defaultStorageSettings() {
		t.Fatalf("existing settings storage defaults = %#v", got)
	}
	if got := store.Current().Replication.ProviderCapacityBytes; got != "0" {
		t.Fatalf("existing settings provider capacity default = %q, want 0", got)
	}
	if _, err := store.Update(Patch{RetentionEnabled: ptr(true)}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Current().Retention; got != (RetentionSettings{Enabled: true, CompletedAfterDays: 30}) {
		t.Fatalf("retention after partial update/reload = %#v", got)
	}
}

func TestReplicationProviderCapacityPersistsAndRequiresRestart(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if store.InitialProviderCapacityBytes() != "0" {
		t.Fatal("unexpected startup provider capacity")
	}
	capacity := "1048576"
	updated, err := store.Update(Patch{ReplicationProviderCapacityBytes: &capacity})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Replication.ProviderCapacityBytes != capacity || store.InitialProviderCapacityBytes() != "0" {
		t.Fatalf("updated capacity=%q startup capacity=%q", updated.Replication.ProviderCapacityBytes, store.InitialProviderCapacityBytes())
	}
	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Current().Replication.ProviderCapacityBytes; got != capacity {
		t.Fatalf("reloaded provider capacity=%q, want %q", got, capacity)
	}
	if reloaded.InitialProviderCapacityBytes() != capacity {
		t.Fatal("reloaded startup capacity does not match persisted capacity")
	}
}

func TestReplicationSettingsRejectsNullAndUnknownFields(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "management"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "management", settingsFilename)
	for _, raw := range []string{
		`{"ui":{"theme":"system"},"replication":{"provider_capacity_bytes":null}}`,
		`{"ui":{"theme":"system"},"replication":null}`,
		`{"ui":{"theme":"system"},"replication":{"provider_capacity_bytes":"0","unexpected":true}}`,
	} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(root); !errors.Is(err, ErrInvalidSettings) {
			t.Fatalf("Open(%s) error = %v, want invalid settings", raw, err)
		}
	}
}

func TestReplicationProviderCapacityRejectsNonCanonicalAndOutOfRangeValues(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "00", "01", "+1", "-1", " 1", "1 ", "1.0", "1e3", "18446744073709551616", "1125899906842625"} {
		t.Run(value, func(t *testing.T) {
			before := store.Current()
			_, updateErr := store.Update(Patch{ReplicationProviderCapacityBytes: &value})
			if !errors.Is(updateErr, ErrInvalidSettings) {
				t.Fatalf("Update(%q) error = %v, want ErrInvalidSettings", value, updateErr)
			}
			if strings.Contains(updateErr.Error(), value) && value != "" {
				t.Fatalf("validation error echoed input: %v", updateErr)
			}
			if got := store.Current(); got != before {
				t.Fatal("invalid capacity update changed settings")
			}
		})
	}
	maximum := "1125899906842624"
	if _, err := store.Update(Patch{ReplicationProviderCapacityBytes: &maximum}); err != nil {
		t.Fatalf("protocol maximum rejected: %v", err)
	}
}

func TestOpenMigratesLegacyRetryAttemptsToPersistAttempts(t *testing.T) {
	root := t.TempDir()
	management := filepath.Join(root, "management")
	if err := os.Mkdir(management, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(management, settingsFilename)
	legacy := `{"ui":{"theme":"dark"},"integrity":{"concurrency":2},"storage":{"failure_handling":{"retry_attempts":5,"retry_initial_backoff_ms":100,"retry_max_backoff_ms":800}}}`
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Current().Storage.FailureHandling.PersistAttempts; got != 5 {
		t.Fatalf("legacy value became %d total attempts, want 5", got)
	}
	if got := store.Current().Storage.IngestMemory; got != defaultStorageSettings().IngestMemory {
		t.Fatalf("migration did not backfill other storage defaults: %#v", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	failure := stored["storage"].(map[string]any)["failure_handling"].(map[string]any)
	if failure["persist_attempts"] != float64(5) {
		t.Fatalf("canonical key not persisted: %#v", failure)
	}
	if _, exists := failure["retry_attempts"]; exists {
		t.Fatalf("legacy key remained after migration: %#v", failure)
	}
	if mode := modeOf(t, path); mode.Perm() != 0600 {
		t.Fatalf("migrated settings mode = %04o, want 0600", mode.Perm())
	}
}

func TestOpenRejectsAmbiguousLegacyAndCanonicalPersistAttemptKeys(t *testing.T) {
	root := t.TempDir()
	management := filepath.Join(root, "management")
	if err := os.Mkdir(management, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(management, settingsFilename)
	data := `{"storage":{"failure_handling":{"retry_attempts":5,"persist_attempts":4,"retry_initial_backoff_ms":100,"retry_max_backoff_ms":800}}}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); !errors.Is(err, ErrInvalidSettings) {
		t.Fatalf("Open() error = %v, want ambiguous settings rejection", err)
	}
}

func TestStorageSettingsDefaultsPersistAndReload(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defaults := defaultStorageSettings()
	if defaults.IngestMemory.GlobalBufferBytes != 1<<30 || defaults.IngestMemory.PerRecordingBufferBytes != 768<<20 || defaults.IngestMemory.MaxPayloadBytes != 512<<20 || defaults.QueueWriter.PendingQueueCapacity != 128 || defaults.QueueWriter.WriterConcurrency != 1 || defaults.FailureHandling.PersistAttempts != 5 || defaults.FailureHandling.RetryInitialBackoffMS != 100 || defaults.FailureHandling.RetryMaxBackoffMS != 800 || defaults.Observability.SamplingIntervalMS != 5000 || defaults.Observability.MetricsRetentionMS != 86400000 {
		t.Fatalf("storage defaults changed: %#v", defaults)
	}
	updated := defaults
	updated.IngestMemory.GlobalBufferBytes = 1536 << 20
	updated.IngestMemory.PerRecordingBufferBytes = 1024 << 20
	updated.IngestMemory.MaxPayloadBytes = 512 << 20
	updated.Observability.SamplingIntervalMS = 10000
	updated.Observability.MetricsRetentionMS = 3600000
	if _, err = store.Update(Patch{Storage: &updated}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Current().Storage; got != updated {
		t.Fatalf("reloaded storage settings = %#v, want %#v", got, updated)
	}
}

func TestStorageSettingsRejectInvalidLimits(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := defaultStorageSettings()
	for _, test := range []struct {
		name   string
		change func(*StorageSettings)
		want   string
	}{
		{"per-recording above global", func(s *StorageSettings) {
			s.IngestMemory.PerRecordingBufferBytes = s.IngestMemory.GlobalBufferBytes + 1
		}, "per-recording"},
		{"payload above per-recording", func(s *StorageSettings) { s.IngestMemory.MaxPayloadBytes = 800 << 20 }, "maximum payload size cannot exceed"},
		{"reallocation peak", func(s *StorageSettings) {
			s.IngestMemory.GlobalBufferBytes, s.IngestMemory.PerRecordingBufferBytes, s.IngestMemory.MaxPayloadBytes = 300<<10, 300<<10, 256<<10
		}, "reallocation peak"},
		{"zero global", func(s *StorageSettings) { s.IngestMemory.GlobalBufferBytes = 0 }, "global buffer"},
		{"negative max payload", func(s *StorageSettings) { s.IngestMemory.MaxPayloadBytes = -1 }, "maximum payload"},
		{"too large global", func(s *StorageSettings) { s.IngestMemory.GlobalBufferBytes = 3 << 30 }, "at most 2 GiB"},
		{"too large payload", func(s *StorageSettings) { s.IngestMemory.MaxPayloadBytes = 2 << 30 }, "at most 1 GiB"},
		{"zero queue", func(s *StorageSettings) { s.QueueWriter.PendingQueueCapacity = 0 }, "queue capacity"},
		{"queue too large", func(s *StorageSettings) { s.QueueWriter.PendingQueueCapacity = 129 }, "queue capacity"},
		{"writer concurrency", func(s *StorageSettings) { s.QueueWriter.WriterConcurrency = 2 }, "writer concurrency"},
		{"persist attempts", func(s *StorageSettings) { s.FailureHandling.PersistAttempts = 11 }, "persist attempts"},
		{"negative persist attempts", func(s *StorageSettings) { s.FailureHandling.PersistAttempts = -1 }, "persist attempts"},
		{"initial backoff", func(s *StorageSettings) { s.FailureHandling.RetryInitialBackoffMS = 1 }, "initial storage retry"},
		{"overflowing duration", func(s *StorageSettings) { s.FailureHandling.RetryInitialBackoffMS = int64(^uint64(0) >> 1) }, "initial storage retry"},
		{"max below initial", func(s *StorageSettings) { s.FailureHandling.RetryMaxBackoffMS = 99 }, "maximum storage retry"},
		{"sampling interval", func(s *StorageSettings) { s.Observability.SamplingIntervalMS = 0 }, "sampling interval"},
		{"retention below sampling", func(s *StorageSettings) { s.Observability.MetricsRetentionMS = 4999 }, "retention"},
		{"retention too large", func(s *StorageSettings) { s.Observability.MetricsRetentionMS = 86400001 }, "retention"},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.change(&candidate)
			before := store.Current()
			_, updateErr := store.Update(Patch{Storage: &candidate})
			if !errors.Is(updateErr, ErrInvalidSettings) || !strings.Contains(updateErr.Error(), test.want) {
				t.Fatalf("Update() error = %v, want invalid settings mentioning %q", updateErr, test.want)
			}
			if got := store.Current(); got != before {
				t.Fatalf("invalid storage settings changed current settings")
			}
		})
	}
}

func TestStorageSettingsMetricsRetentionReachability(t *testing.T) {
	valid := []struct {
		name        string
		samplingMS  int64
		retentionMS int64
	}{
		{"1s/5m", 1_000, 300_000},
		{"5s/5m", 5_000, 300_000},
		{"7s/5m1s", 7_000, 301_000},
		{"10s/5m", 10_000, 300_000},
		{"1m/20m", 60_000, 1_200_000},
		{"1h/20h", 3_600_000, 72_000_000},
		{"1h/24h", 3_600_000, 86_400_000},
	}
	for _, test := range valid {
		t.Run(test.name+" accepted", func(t *testing.T) {
			store, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			candidate := defaultStorageSettings()
			candidate.Observability.SamplingIntervalMS = test.samplingMS
			candidate.Observability.MetricsRetentionMS = test.retentionMS
			if _, err := store.Update(Patch{Storage: &candidate}); err != nil {
				t.Fatalf("Update() rejected reachable boundary: %v", err)
			}
		})
	}

	invalid := []struct {
		name        string
		samplingMS  int64
		retentionMS int64
		want        string
	}{
		{"1s/1s", 1_000, 1_000, "at least 5 minutes"},
		{"7s/5m", 7_000, 300_000, "at least 5 minutes 1 second"},
		{"7s/5m1s minus 1ms", 7_000, 300_999, "at least 5 minutes 1 second"},
		{"1s/5m minus 1ms", 1_000, 299_999, "at least 5 minutes"},
		{"5s/5m minus 1ms", 5_000, 299_999, "at least 5 minutes"},
		{"10s/5m minus 1ms", 10_000, 299_999, "at least 5 minutes"},
		{"1m/5m", 60_000, 300_000, "at least 20 minutes"},
		{"1h/1h", 3_600_000, 3_600_000, "at least 20 hours"},
		{"1m/one millisecond below", 60_000, 1_199_999, "at least 20 minutes"},
		{"1h/one millisecond below", 3_600_000, 71_999_999, "at least 20 hours"},
	}
	for _, test := range invalid {
		t.Run(test.name+" rejected", func(t *testing.T) {
			store, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			candidate := defaultStorageSettings()
			candidate.Observability.SamplingIntervalMS = test.samplingMS
			candidate.Observability.MetricsRetentionMS = test.retentionMS
			before := store.Current().Storage
			_, err = store.Update(Patch{Storage: &candidate})
			if !errors.Is(err, ErrInvalidSettings) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Update() error = %v, want invalid settings containing %q", err, test.want)
			}
			if got := store.Current().Storage; got != before {
				t.Fatalf("invalid observability settings changed stored state")
			}
		})
	}
}

func TestOpenRejectsUnknownAndTrailingJSON(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{name: "unknown field", data: `{"ui":{"theme":"system"},"integrity":{"concurrency":2},"extra":"x"}`},
		{name: "unknown nested field after legacy migration support", data: `{"storage":{"failure_handling":{"retry_attempts":5,"retry_initial_backoff_ms":100,"retry_max_backoff_ms":800,"other":"x"}}}`},
		{name: "trailing object", data: `{"ui":{"theme":"system"},"integrity":{"concurrency":2}} {}`},
		{name: "trailing garbage", data: `{"ui":{"theme":"system"},"integrity":{"concurrency":2}}x`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "management"), 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "management", settingsFilename)
			if err := os.WriteFile(path, []byte(test.data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(root); !errors.Is(err, ErrInvalidSettings) {
				t.Fatalf("Open() error = %v, want ErrInvalidSettings", err)
			}
		})
	}
}

func TestUpdatePersistenceFailureLeavesMemoryUnchanged(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	before := store.Current()
	if err := os.Remove(store.path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(store.path, 0700); err != nil {
		t.Fatal(err)
	}
	theme := "light"
	if _, err := store.Update(Patch{UITheme: &theme}); err == nil {
		t.Fatal("Update() succeeded with a directory at the settings file path")
	}
	if got := store.Current(); got != before {
		t.Fatalf("failed update changed in-memory state: %#v", got)
	}
}

func TestOpenRejectsSymlinkPaths(t *testing.T) {
	t.Run("management directory", func(t *testing.T) {
		root := t.TempDir()
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(root, "management")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := Open(root); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open() error = %v, want ErrUnsafePath", err)
		}
	})

	t.Run("settings file", func(t *testing.T) {
		root := t.TempDir()
		outside := filepath.Join(t.TempDir(), "settings.json")
		if err := os.WriteFile(outside, []byte(`{"ui":{"theme":"system"},"integrity":{"concurrency":2}}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, "management"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "management", settingsFilename)); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := Open(root); !errors.Is(err, ErrUnsafePath) {
			t.Fatalf("Open() error = %v, want ErrUnsafePath", err)
		}
	})
}

func TestConcurrentUpdateAndCurrent(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	themes := []string{"system", "light", "dark"}
	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				theme := themes[(worker+i)%len(themes)]
				concurrency := (worker+i)%4 + 1
				if _, err := store.Update(Patch{UITheme: &theme, IntegrityConcurrency: &concurrency}); err != nil {
					t.Errorf("Update() error = %v", err)
					return
				}
				got := store.Current()
				if err := validate(got); err != nil {
					t.Errorf("Current() returned invalid settings: %#v: %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if _, err := Open(filepath.Dir(store.directory)); err != nil {
		t.Fatalf("reload after concurrent writes: %v", err)
	}
}

func modeOf(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}

func ptr[T any](value T) *T { return &value }

func contains(s, part string) bool {
	for i := 0; i+len(part) <= len(s); i++ {
		if s[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
