package server

import (
	"net/http"
	"time"

	"github.com/dltkddnr04/integrated-recorder/internal/storage"
)

type storagePoolsResponse struct {
	Items []storagePoolView `json:"items"`
}

type storagePoolView struct {
	ID               string                `json:"id"`
	DisplayName      string                `json:"display_name"`
	Kind             string                `json:"kind"`
	Role             string                `json:"role"`
	Health           string                `json:"health"`
	Capacity         storage.PoolCapacity  `json:"capacity"`
	CapacityKnown    bool                  `json:"capacity_known"`
	Throughput       storageThroughputView `json:"throughput"`
	EstimatedCeiling storageCeilingView    `json:"estimated_ceiling"`
	Buffer           storageBufferView     `json:"buffer"`
	Queue            storageQueueView      `json:"queue"`
	Writers          storageWritersView    `json:"writers"`
	ErrorsTotal      uint64                `json:"errors_total"`
}

type storageThroughputView struct {
	ReadBytesPerSecond  uint64  `json:"read_bytes_per_second"`
	WriteBytesPerSecond uint64  `json:"write_bytes_per_second"`
	ReadBytesTotal      uint64  `json:"read_bytes_total"`
	WriteBytesTotal     uint64  `json:"write_bytes_total"`
	ReadLatencyMillis   float64 `json:"read_latency_ms"`
	WriteLatencyMillis  float64 `json:"write_latency_ms"`
}

type storageCeilingView struct {
	ReadBytesPerSecond  uint64 `json:"read_bytes_per_second,omitempty"`
	WriteBytesPerSecond uint64 `json:"write_bytes_per_second,omitempty"`
	Source              string `json:"source"`
}

type storageBufferView struct {
	UsedBytes                 int64   `json:"used_bytes"`
	CapacityBytes             int64   `json:"capacity_bytes"`
	PerRecordingCapacityBytes int64   `json:"per_recording_capacity_bytes"`
	ReservedBytes             int64   `json:"reserved_bytes"`
	Utilization               float64 `json:"utilization"`
}

type storageQueueView struct {
	Objects          int     `json:"objects"`
	Bytes            int64   `json:"bytes"`
	OldestAgeSeconds float64 `json:"oldest_age_seconds"`
}

type storageWritersView struct {
	Active int `json:"active"`
	Limit  int `json:"limit"`
}

type storageMetricsResponse struct {
	PoolID                string                `json:"pool_id"`
	SampleIntervalSeconds int                   `json:"sample_interval_seconds"`
	SampleIntervalMS      int64                 `json:"sample_interval_ms"`
	Items                 []storageMetricSample `json:"items"`
}

type setupStorageTestResponse struct {
	Status         string  `json:"status"`
	CapacityKnown  bool    `json:"capacity_known"`
	FreeBytes      *uint64 `json:"free_bytes,omitempty"`
	WriteTest      string  `json:"write_test"`
	DurabilityTest string  `json:"durability_test"`
	DiagnosticCode string  `json:"diagnostic_code,omitempty"`
}

func (s *Server) setupStorageTest(w http.ResponseWriter, _ *http.Request) {
	response := setupStorageTestResponse{Status: "error", WriteTest: "failed", DurabilityTest: "failed", DiagnosticCode: "storage_probe_failed"}
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}
	probe := s.storage.RunSetupProbe()
	response.CapacityKnown = probe.CapacityKnown
	if probe.CapacityKnown {
		freeBytes := probe.FreeBytes
		response.FreeBytes = &freeBytes
	}
	if probe.WritePassed {
		response.WriteTest = "passed"
	}
	if probe.DurabilityPassed {
		response.DurabilityTest = "passed"
	}
	if !probe.WritePassed {
		response.Status = "error"
	} else if !probe.DurabilityPassed || !probe.CapacityKnown || probe.FreeBytes < 1<<30 {
		response.Status = "warning"
		response.DiagnosticCode = "storage_probe_warning"
	} else {
		response.Status = "ready"
		response.DiagnosticCode = ""
	}
	writeJSON(w, http.StatusOK, response)
}

type storageMetricSample struct {
	At                  time.Time `json:"at"`
	ReadBytesPerSecond  uint64    `json:"read_bytes_per_second"`
	WriteBytesPerSecond uint64    `json:"write_bytes_per_second"`
	BufferUsedBytes     int64     `json:"buffer_used_bytes"`
	PersistQueueBytes   int64     `json:"persist_queue_bytes"`
}

func (s *Server) storagePools(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage_unavailable"})
		return
	}
	snapshot := s.storage.PoolMetrics()
	writeJSON(w, http.StatusOK, storagePoolsResponse{Items: []storagePoolView{poolView(snapshot)}})
}

func (s *Server) storagePoolMetrics(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage_unavailable"})
		return
	}
	poolID := r.PathValue("pool_id")
	if poolID != s.storage.PoolMetrics().ID {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "storage_pool_not_found"})
		return
	}
	window, _, ok := storageMetricsWindow(r.URL.Query().Get("window"))
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_storage_metrics_window"})
		return
	}
	snapshot := s.storage.PoolMetricsWindow(window)
	items := make([]storageMetricSample, 0, len(snapshot.Samples))
	for _, sample := range snapshot.Samples {
		items = append(items, storageMetricSample{
			At: sample.At, ReadBytesPerSecond: sample.ReadBytesPerSecond,
			WriteBytesPerSecond: sample.WriteBytesPerSecond,
			BufferUsedBytes:     sample.BufferUsedBytes, PersistQueueBytes: sample.PersistQueueBytes,
		})
	}
	configuredInterval := s.storage.MetricsSamplingInterval()
	intervalSeconds := int((configuredInterval + time.Second - 1) / time.Second)
	writeJSON(w, http.StatusOK, storageMetricsResponse{PoolID: poolID, SampleIntervalSeconds: intervalSeconds, SampleIntervalMS: configuredInterval.Milliseconds(), Items: items})
}

func storageMetricsWindow(value string) (time.Duration, int, bool) {
	switch value {
	case "", "1h":
		return time.Hour, 5, true
	case "6h":
		return 6 * time.Hour, 5, true
	case "24h":
		return 24 * time.Hour, 5, true
	default:
		return 0, 0, false
	}
}

func poolView(snapshot storage.PoolSnapshot) storagePoolView {
	utilization := 0.0
	if snapshot.Buffer.CapacityBytes > 0 {
		utilization = float64(snapshot.Buffer.UsedBytes) / float64(snapshot.Buffer.CapacityBytes)
	}
	return storagePoolView{
		ID: snapshot.ID, DisplayName: snapshot.DisplayName, Kind: snapshot.Kind,
		Role: snapshot.Role, Health: snapshot.Health, Capacity: snapshot.Capacity, CapacityKnown: snapshot.CapacityKnown,
		Throughput: storageThroughputView{
			ReadBytesPerSecond:  snapshot.Throughput.ReadBytesPerSecond,
			WriteBytesPerSecond: snapshot.Throughput.WriteBytesPerSecond,
			ReadBytesTotal:      snapshot.Throughput.ReadBytesTotal,
			WriteBytesTotal:     snapshot.Throughput.WriteBytesTotal,
			ReadLatencyMillis:   snapshot.Throughput.ReadLatencyMillis,
			WriteLatencyMillis:  snapshot.Throughput.WriteLatencyMillis,
		},
		EstimatedCeiling: storageCeilingView{
			ReadBytesPerSecond:  snapshot.EstimatedCeiling.ReadBytesPerSecond,
			WriteBytesPerSecond: snapshot.EstimatedCeiling.WriteBytesPerSecond,
			Source:              snapshot.EstimatedCeiling.Source,
		},
		Buffer: storageBufferView{
			UsedBytes:                 snapshot.Buffer.UsedBytes,
			CapacityBytes:             snapshot.Buffer.CapacityBytes,
			PerRecordingCapacityBytes: snapshot.Buffer.PerRecordingCapacityBytes,
			ReservedBytes:             snapshot.Buffer.ReservedBytes,
			Utilization:               utilization,
		},
		Queue: storageQueueView{
			Objects:          snapshot.Queue.Objects,
			Bytes:            snapshot.Queue.Bytes,
			OldestAgeSeconds: snapshot.Queue.OldestAgeSeconds,
		},
		Writers:     storageWritersView{Active: snapshot.Writers.Active, Limit: snapshot.Writers.Limit},
		ErrorsTotal: snapshot.ErrorsTotal,
	}
}
