package server

import (
	"errors"
	"net/http"
	"sort"

	"github.com/integrated-recorder/core/internal/systemsettings"
)

type settingsPutRequest struct {
	UI        *settingsUIRequest              `json:"ui,omitempty"`
	Integrity *settingsIntegrityRequest       `json:"integrity,omitempty"`
	Retention *settingsRetentionRequest       `json:"retention,omitempty"`
	Storage   *systemsettings.StorageSettings `json:"storage,omitempty"`
}

type settingsUIRequest struct {
	Theme *string `json:"theme,omitempty"`
}

type settingsIntegrityRequest struct {
	Concurrency *int `json:"concurrency,omitempty"`
}

type settingsRetentionRequest struct {
	Enabled            *bool `json:"enabled,omitempty"`
	CompletedAfterDays *int  `json:"completed_after_days,omitempty"`
}

func (s *Server) registerSettingsRoutes() {
	s.mux.HandleFunc("GET /api/settings", s.settingsGet)
	s.mux.HandleFunc("PUT /api/settings", s.settingsPut)
	s.registerRetentionRoutes()
}

func (s *Server) settingsGet(w http.ResponseWriter, r *http.Request) {
	if s.settings == nil {
		writeError(w, http.StatusServiceUnavailable, "system settings are unavailable")
		return
	}
	s.writeSettings(w, http.StatusOK, s.settings.Current())
}

func (s *Server) settingsPut(w http.ResponseWriter, r *http.Request) {
	if s.settings == nil {
		writeError(w, http.StatusServiceUnavailable, "system settings are unavailable")
		return
	}
	var request settingsPutRequest
	if err := decodeJSONBody(w, r, 16<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid settings request")
		return
	}
	if request.UI != nil && request.UI.Theme == nil ||
		request.Integrity != nil && request.Integrity.Concurrency == nil ||
		request.Retention != nil && request.Retention.Enabled == nil && request.Retention.CompletedAfterDays == nil ||
		request.UI == nil && request.Integrity == nil && request.Retention == nil && request.Storage == nil {
		writeError(w, http.StatusBadRequest, "settings request contains no values")
		return
	}
	patch := systemsettings.Patch{}
	if request.UI != nil {
		patch.UITheme = request.UI.Theme
	}
	if request.Integrity != nil {
		patch.IntegrityConcurrency = request.Integrity.Concurrency
	}
	if request.Retention != nil {
		patch.RetentionEnabled = request.Retention.Enabled
		patch.RetentionAfterDays = request.Retention.CompletedAfterDays
	}
	if request.Storage != nil {
		patch.Storage = request.Storage
	}
	settings, err := s.settings.Update(patch)
	if err != nil {
		if errors.Is(err, systemsettings.ErrInvalidSettings) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "could not save system settings")
		return
	}
	s.writeSettings(w, http.StatusOK, settings)
}

func (s *Server) writeSettings(w http.ResponseWriter, status int, settings systemsettings.Settings) {
	restartRequired := []string{}
	if s.initialIntegrityConcurrency > 0 && settings.Integrity.Concurrency != s.initialIntegrityConcurrency {
		restartRequired = append(restartRequired, "integrity.concurrency")
	}
	restartRequired = append(restartRequired, changedStorageSettings(s.effectiveStorage, settings.Storage)...)
	sort.Strings(restartRequired)
	writeJSON(w, status, map[string]any{
		"settings":          settings,
		"effective_storage": s.effectiveStorage,
		"restart_required":  restartRequired,
	})
}

func changedStorageSettings(effective, stored systemsettings.StorageSettings) []string {
	var changed []string
	if effective.IngestMemory.GlobalBufferBytes != stored.IngestMemory.GlobalBufferBytes {
		changed = append(changed, "storage.ingest_memory.global_buffer_bytes")
	}
	if effective.IngestMemory.PerRecordingBufferBytes != stored.IngestMemory.PerRecordingBufferBytes {
		changed = append(changed, "storage.ingest_memory.per_recording_buffer_bytes")
	}
	if effective.IngestMemory.MaxPayloadBytes != stored.IngestMemory.MaxPayloadBytes {
		changed = append(changed, "storage.ingest_memory.max_payload_bytes")
	}
	if effective.QueueWriter.PendingQueueCapacity != stored.QueueWriter.PendingQueueCapacity {
		changed = append(changed, "storage.queue_writer.pending_queue_capacity")
	}
	if effective.QueueWriter.WriterConcurrency != stored.QueueWriter.WriterConcurrency {
		changed = append(changed, "storage.queue_writer.writer_concurrency")
	}
	if effective.FailureHandling.PersistAttempts != stored.FailureHandling.PersistAttempts {
		changed = append(changed, "storage.failure_handling.persist_attempts")
	}
	if effective.FailureHandling.RetryInitialBackoffMS != stored.FailureHandling.RetryInitialBackoffMS {
		changed = append(changed, "storage.failure_handling.retry_initial_backoff_ms")
	}
	if effective.FailureHandling.RetryMaxBackoffMS != stored.FailureHandling.RetryMaxBackoffMS {
		changed = append(changed, "storage.failure_handling.retry_max_backoff_ms")
	}
	if effective.Observability.SamplingIntervalMS != stored.Observability.SamplingIntervalMS {
		changed = append(changed, "storage.observability.sampling_interval_ms")
	}
	if effective.Observability.MetricsRetentionMS != stored.Observability.MetricsRetentionMS {
		changed = append(changed, "storage.observability.metrics_retention_ms")
	}
	return changed
}
