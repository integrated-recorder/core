package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/recordquery"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	maxSearchQuery           = 128
	maxSearchResults         = 50
	defaultSearchResults     = 20
	maxGlobalSearchScan      = 10000
	metadataTimelineAPILimit = 256
	globalSearchTimeout      = 2 * time.Second
)

var (
	errInvalidRecordingReadModel  = errors.New("recording read-model metadata is invalid")
	errRecordingReadModelOverflow = errors.New("recording read-model value overflow")
)

func (s *Server) registerProductRoutes() {
	s.registerDerivativeRoutes()
	s.mux.HandleFunc("GET /api/v2/recordings", s.recordingsQuery)
	s.mux.HandleFunc("GET /api/dashboard", s.dashboard)
	s.mux.HandleFunc("GET /api/system/storage", s.systemStorage)
	s.mux.HandleFunc("GET /api/storage/pools", s.storagePools)
	s.mux.HandleFunc("GET /api/storage/pools/{pool_id}/metrics", s.storagePoolMetrics)
	s.mux.HandleFunc("GET /api/system/info", s.systemInfo)
	s.mux.HandleFunc("GET /api/recordings/{id}/tags", s.recordingTagsGet)
	s.mux.HandleFunc("PUT /api/recordings/{id}/tags", s.recordingTagsPut)
	s.mux.HandleFunc("DELETE /api/recordings/{id}", s.recordingDelete)
	s.mux.HandleFunc("GET /api/recordings/{id}/archive/index", s.archiveIndex)
	s.mux.HandleFunc("GET /api/recordings/{id}/integrity", s.integrityGet)
	s.mux.HandleFunc("POST /api/recordings/{id}/integrity/verify", s.integrityStart)
	s.mux.HandleFunc("GET /api/integrity/jobs/{job_id}", s.integrityJobGet)
	s.mux.HandleFunc("POST /api/integrity/jobs/{job_id}/cancel", s.integrityJobCancel)
	s.mux.HandleFunc("GET /api/resolve-workflows", s.workflowList)
	s.mux.HandleFunc("GET /api/workflow-history", s.workflowHistoryList)
	s.mux.HandleFunc("GET /api/workflow-history/{id}", s.workflowHistoryGet)
	s.mux.HandleFunc("GET /api/adapters/{id}/resources", s.resourceList)
	s.mux.HandleFunc("GET /api/adapters/{id}/resources/search", s.resourceSearch)
	s.mux.HandleFunc("GET /api/search", s.globalSearch)
	s.mux.HandleFunc("GET /api/recordings/{id}/events", s.recordingEvents)
	s.mux.HandleFunc("GET /api/recordings/{id}/metadata", s.recordingMetadataGet)
	s.mux.HandleFunc("GET /api/audit", s.auditList)
	s.mux.HandleFunc("GET /api/notifications", s.notificationList)
	s.mux.HandleFunc("POST /api/notifications/sync", s.notificationSync)
	s.mux.HandleFunc("POST /api/notifications/{id}/read", s.notificationRead)
	s.mux.HandleFunc("POST /api/notifications/read-all", s.notificationReadAll)
}

func (s *Server) productLock(id string) *sync.RWMutex {
	return &s.productLocks[recordLockIndex(id)]
}

func recordLockIndex(id string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return h.Sum32() % 64
}

// recordingQueryItem builds a page item. It does not enumerate provider
// objects; physical archive size stays unknown until a bounded projection
// exists.
func (s *Server) recordingQueryItem(ctx context.Context, recording *domain.Recording) (recordquery.Item, error) {
	return s.recordingQueryBaseItem(ctx, recording, true)
}

func (s *Server) recordingQueryBaseItem(ctx context.Context, recording *domain.Recording, includeSearchMetadata bool) (recordquery.Item, error) {
	statistics, err := s.deriveRecordingStatistics(ctx, recording, false)
	if err != nil {
		return recordquery.Item{}, err
	}
	item := recordquery.Item{
		ID: recording.ID, Title: recording.Title, AdapterID: recording.AdapterID,
		State: string(recording.State), StartedAt: recording.StartedAt, CreatedAt: recording.CreatedAt,
		DurationSeconds: statistics.DurationSeconds, ArchiveSizeBytes: statistics.ArchiveSizeBytes,
		MediaPayloadSizeBytes: statistics.MediaPayloadSizeBytes, ManifestSizeBytes: statistics.ManifestSizeBytes,
		InitPayloadSizeBytes: statistics.InitPayloadSizeBytes, SegmentCount: statistics.SegmentCount,
		InitSegmentCount: statistics.InitSegmentCount, ManifestSnapshotCount: statistics.ManifestSnapshotCount,
		GapCount: statistics.GapCount, GapSegmentCount: statistics.GapSegmentCount,
		GapDurationSeconds: statistics.GapDurationSeconds, Integrity: string(statistics.Integrity),
		StatisticsStatus: statistics.Status, UnavailableFields: append([]string{}, statistics.UnavailableFields...),
		Tags: []string{},
	}
	if recording.Resource != nil {
		item.ResourceType, item.ResourceID = recording.Resource.Type, recording.Resource.ID
	}
	if recording.Adapter != nil {
		item.AdapterName = recording.Adapter.Name
	}
	if includeSearchMetadata && item.AdapterName == "" && s.adapters != nil && recording.AdapterID != "" {
		if adapter, err := s.adapters.Get(recording.AdapterID); err == nil && adapter.Descriptor != nil {
			item.AdapterName = adapter.Descriptor.Name
		}
	}
	if includeSearchMetadata && s.products != nil {
		tags, err := s.products.Tags(recording.ID)
		if err != nil {
			item.StatisticsStatus = "partial"
			item.UnavailableFields = append(item.UnavailableFields, "tags")
			s.reportReadModelFailure(recording.ID, "tags", "management_unavailable")
		} else {
			item.Tags = tags
		}
	}
	return item, nil
}

func (s *Server) enrichRecordingQueryItem(recording *domain.Recording, item *recordquery.Item, includeSearchMetadata bool) {
	if recording == nil || item == nil {
		return
	}
	if !includeSearchMetadata && s.products != nil {
		tags, err := s.products.Tags(recording.ID)
		if err != nil {
			item.StatisticsStatus = "partial"
			item.UnavailableFields = appendUnique(item.UnavailableFields, "tags")
			s.reportReadModelFailure(recording.ID, "tags", "management_unavailable")
		} else {
			item.Tags = tags
		}
	}
	if item.AdapterName == "" && recording.Adapter != nil {
		item.AdapterName = recording.Adapter.Name
	}
	if item.AdapterName == "" && s.adapters != nil && recording.AdapterID != "" {
		if adapter, err := s.adapters.Get(recording.AdapterID); err == nil && adapter.Descriptor != nil {
			item.AdapterName = adapter.Descriptor.Name
		}
	}
	if s.previews != nil {
		previewSummary := s.previews.Summary(recording)
		item.Preview = &previewSummary
	}
}

func appendUnique(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func addInt64(target *int64, amount int64) bool {
	if amount < 0 || *target > math.MaxInt64-amount {
		return false
	}
	*target += amount
	return true
}

func (s *Server) deriveRecordingStatistics(ctx context.Context, recording *domain.Recording, includePhysicalArchiveSize bool) (recordingStatistics, error) {
	statistics, err := s.projectRecordingStatistics(ctx, recording, includePhysicalArchiveSize)
	if err != nil {
		category := "invalid_metadata"
		if errors.Is(err, errRecordingReadModelOverflow) {
			category = "overflow"
		}
		recordingID := ""
		if recording != nil {
			recordingID = recording.ID
		}
		s.reportReadModelFailure(recordingID, "statistics_projection", category)
	}
	return statistics, err
}

func (s *Server) projectRecordingStatistics(ctx context.Context, recording *domain.Recording, includePhysicalArchiveSize bool) (recordingStatistics, error) {
	if recording == nil {
		return recordingStatistics{}, errInvalidRecordingReadModel
	}
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		return s.projectShardedRecordingStatistics(recording)
	}
	statistics := recordingStatistics{
		MediaPayloadSizeBytes: 0, ManifestSizeBytes: 0, InitPayloadSizeBytes: 0,
		SegmentCount: 0, InitSegmentCount: 0,
		ManifestSnapshotCount: len(recording.Snapshots), DurationSeconds: 0,
		GapCount: len(recording.Gaps), GapDurationSeconds: nil, Integrity: storage.IntegrityUnknown,
		Status: "complete", UnavailableFields: []string{},
	}
	for _, track := range recording.Tracks {
		if track == nil {
			return recordingStatistics{}, errInvalidRecordingReadModel
		}
		if len(track.Segments) > int(^uint(0)>>1)-statistics.SegmentCount {
			return recordingStatistics{}, errRecordingReadModelOverflow
		}
		statistics.SegmentCount += len(track.Segments)
		for _, segment := range track.Segments {
			if math.IsNaN(segment.Duration) || math.IsInf(segment.Duration, 0) || segment.Duration < 0 {
				return recordingStatistics{}, errInvalidRecordingReadModel
			}
			statistics.DurationSeconds += segment.Duration
			if math.IsInf(statistics.DurationSeconds, 0) {
				return recordingStatistics{}, errRecordingReadModelOverflow
			}
			if segment.PayloadSize < 0 || !addInt64(&statistics.MediaPayloadSizeBytes, segment.PayloadSize) {
				if segment.PayloadSize < 0 {
					return recordingStatistics{}, errInvalidRecordingReadModel
				}
				return recordingStatistics{}, errRecordingReadModelOverflow
			}
		}
		for _, segment := range track.InitSegments {
			if segment.PayloadSize < 0 {
				return recordingStatistics{}, errInvalidRecordingReadModel
			}
			if !addInt64(&statistics.InitPayloadSizeBytes, segment.PayloadSize) {
				return recordingStatistics{}, errRecordingReadModelOverflow
			}
		}
		if len(track.InitSegments) > int(^uint(0)>>1)-statistics.InitSegmentCount {
			return recordingStatistics{}, errRecordingReadModelOverflow
		}
		statistics.InitSegmentCount += len(track.InitSegments)
	}
	for _, snapshot := range recording.Snapshots {
		if snapshot.Size < 0 {
			return recordingStatistics{}, errInvalidRecordingReadModel
		}
		if !addInt64(&statistics.ManifestSizeBytes, snapshot.Size) {
			return recordingStatistics{}, errRecordingReadModelOverflow
		}
	}
	for _, gap := range recording.Gaps {
		if gap.ToSequence < gap.FromSequence {
			return recordingStatistics{}, errInvalidRecordingReadModel
		}
		n := gap.ToSequence - gap.FromSequence + 1
		maxInt := int(^uint(0) >> 1)
		if n == 0 || statistics.GapSegmentCount < 0 || n > uint64(maxInt-statistics.GapSegmentCount) {
			return recordingStatistics{}, errRecordingReadModelOverflow
		}
		statistics.GapSegmentCount += int(n)
	}
	statistics.Integrity = s.integrityStatusFor(recording)
	if !includePhysicalArchiveSize {
		statistics.Status = "partial"
		statistics.UnavailableFields = append(statistics.UnavailableFields, "archive_size_bytes")
		return statistics, nil
	}
	var archiveBytes int64
	var archiveErr error
	if s.storage == nil {
		archiveErr = errors.New("storage backend is unavailable")
	} else {
		archiveBytes, archiveErr = s.storage.RecordingDirectoryBytesContext(ctx, recording.ID)
	}
	if archiveErr != nil {
		statistics.Status = "partial"
		statistics.UnavailableFields = append(statistics.UnavailableFields, "archive_size_bytes")
		category := "provider_list_unavailable"
		if errors.Is(archiveErr, storage.ErrArchiveSizeOverflow) {
			category = "overflow"
		}
		if ctx != nil && ctx.Err() != nil {
			archiveErr = ctx.Err()
		}
		if !errors.Is(archiveErr, context.Canceled) && !errors.Is(archiveErr, context.DeadlineExceeded) {
			s.reportReadModelFailure(recording.ID, "archive_size", category)
		}
	} else {
		statistics.ArchiveSizeBytes = &archiveBytes
	}
	return statistics, nil
}

func (s *Server) projectShardedRecordingStatistics(recording *domain.Recording) (recordingStatistics, error) {
	if recording.ShardedArchive == nil {
		return recordingStatistics{}, errInvalidRecordingReadModel
	}
	maxInt := uint64(^uint(0) >> 1)
	counts := []uint64{recording.ShardedArchive.InitCount, recording.ShardedArchive.ManifestSnapshotCount, recording.ShardedArchive.GapCount}
	for _, count := range counts {
		if count > maxInt {
			return recordingStatistics{}, errRecordingReadModelOverflow
		}
	}
	if recording.ShardedArchive.PayloadBytes > math.MaxInt64 {
		return recordingStatistics{}, errRecordingReadModelOverflow
	}
	statistics := recordingStatistics{
		MediaPayloadSizeBytes: int64(recording.ShardedArchive.PayloadBytes),
		SegmentCount:          recording.SegmentCount(),
		InitSegmentCount:      int(recording.ShardedArchive.InitCount),
		ManifestSnapshotCount: int(recording.ShardedArchive.ManifestSnapshotCount),
		GapCount:              int(recording.ShardedArchive.GapCount),
		DurationSeconds:       recording.Duration(),
		Integrity:             s.integrityStatusFor(recording),
		Status:                "partial",
		UnavailableFields: []string{
			"archive_size_bytes", "init_payload_size_bytes",
			"manifest_size_bytes", "gap_segment_count", "gap_duration_seconds",
		},
	}
	if math.IsNaN(statistics.DurationSeconds) || math.IsInf(statistics.DurationSeconds, 0) || statistics.DurationSeconds < 0 {
		return recordingStatistics{}, errInvalidRecordingReadModel
	}
	return statistics, nil
}

// integrityStatusFor projects a saved integrity result against the current
// canonical recording snapshot. Stale or revision-unknown results are not
// current archive health; an active verifier remains visible as verifying.
func (s *Server) integrityStatusFor(recording *domain.Recording) storage.IntegrityStatus {
	if s.integrity == nil || recording == nil {
		return storage.IntegrityUnknown
	}
	projection, ok := s.integrity.StatusFor(recording)
	if !ok {
		return storage.IntegrityUnknown
	}
	if projection.Status == storage.IntegrityVerifying {
		return storage.IntegrityVerifying
	}
	if projection.Freshness != integrity.FreshnessCurrent {
		return storage.IntegrityUnknown
	}
	return projection.Status
}

func (s *Server) reportReadModelFailure(recordingID, component, category string) {
	if s.logs == nil {
		return
	}
	if len(recordingID) != 32 {
		recordingID = "invalid"
	} else {
		for _, character := range recordingID {
			if character < '0' || character > '9' {
				if character < 'a' || character > 'f' {
					recordingID = "invalid"
					break
				}
			}
		}
	}
	switch component {
	case "archive_size", "tags", "statistics_projection":
	default:
		component = "other"
	}
	switch category {
	case "provider_list_unavailable", "invalid_metadata", "overflow", "management_unavailable":
	default:
		category = "other"
	}
	s.logs.Add("warn", "recording-read-model", fmt.Sprintf("recording=%s component=%s category=%s", recordingID, component, category))
}

func (s *Server) recordingDetail(ctx context.Context, recording *domain.Recording) (recordingDetail, error) {
	statistics, err := s.deriveRecordingStatistics(ctx, recording, recording == nil || recording.FormatVersion != storage.ShardedArchiveFormatVersion)
	if err != nil {
		return recordingDetail{}, err
	}
	response := detail(recording)
	response.Statistics = &statistics
	if s.previews != nil {
		previewSummary := s.previews.Summary(recording)
		response.Preview = &previewSummary
	}
	return response, nil
}

func (s *Server) recordingsQuery(w http.ResponseWriter, r *http.Request) {
	query, err := recordquery.ParseQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid recording query")
		return
	}
	if s.manager == nil {
		writeError(w, http.StatusServiceUnavailable, "recording manager is unavailable")
		return
	}
	recordings, err := listManagementSummaries(r.Context(), s.manager)
	if errors.Is(err, acquire.ErrListLimit) {
		writeError(w, http.StatusServiceUnavailable, "recording summary index exceeds supported bounds")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "recording summaries are temporarily unavailable")
		return
	}
	items := make([]recordquery.Item, 0, len(recordings))
	byID := make(map[string]*domain.Recording, len(recordings))
	includeSearchMetadata := query.Q != "" || query.Tag != ""
	tagsPartial := false
	for _, listed := range recordings {
		if listed == nil || listed.ID == "" {
			writeError(w, http.StatusServiceUnavailable, "recording summary metadata is invalid")
			return
		}
		item, itemErr := s.recordingQueryBaseItem(r.Context(), listed, includeSearchMetadata)
		if itemErr != nil {
			writeError(w, http.StatusServiceUnavailable, "recording summary metadata is invalid")
			return
		}
		if includeSearchMetadata {
			for _, unavailable := range item.UnavailableFields {
				if unavailable == "tags" {
					tagsPartial = true
					break
				}
			}
		}
		items = append(items, item)
		byID[listed.ID] = listed
	}
	page, err := recordquery.Page(items, query)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid recording query")
		return
	}
	for index := range page.Items {
		if recording := byID[page.Items[index].ID]; recording != nil {
			s.enrichRecordingQueryItem(recording, &page.Items[index], includeSearchMetadata)
		}
	}
	if tagsPartial {
		writeJSON(w, http.StatusOK, map[string]any{
			"items": page.Items, "next_cursor": page.NextCursor, "total": page.Total,
			"partial_errors": []string{"tags"}, "total_is_partial": true,
		})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) systemStorage(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		writeError(w, http.StatusServiceUnavailable, "storage statistics are unavailable")
		return
	}
	recordings, err := listManagementSummaries(r.Context(), s.manager)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "recording summaries are temporarily unavailable")
		return
	}
	stats, unavailable := s.managementStorageStats(recordings)
	writeJSON(w, http.StatusOK, map[string]any{
		"archive_root":               "recordings/",
		"filesystem_total_bytes":     stats.FilesystemTotalBytes,
		"filesystem_used_bytes":      stats.FilesystemUsedBytes,
		"filesystem_available_bytes": stats.FilesystemAvailableBytes,
		"recordings_bytes":           stats.RecordingBytes,
		"recordings_bytes_known":     stats.RecordingBytesKnown,
		"recording_count":            stats.RecordingCount,
		"segment_count":              stats.SegmentCount,
		"init_segment_count":         stats.InitSegmentCount,
		"manifest_count":             stats.ManifestCount,
		"gap_count":                  stats.GapCount,
		"canonical_payload_bytes":    stats.CanonicalPayloadBytes,
		"capacity_known":             stats.CapacityKnown,
		"statistics_status":          stats.Status,
		"unavailable_fields":         unavailable,
	})
}

func (s *Server) systemInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version": s.version, "commit": s.commit, "build_time": s.buildInfo.BuildTime,
		"release_channel": s.buildInfo.ReleaseChannel, "runtime_protocol_version": s.buildInfo.RuntimeProtocolVersion,
		"go_version": runtime.Version(),
		"goos":       runtime.GOOS, "goarch": runtime.GOARCH, "started_at": s.startedAt,
		"uptime_seconds":   int64(time.Since(s.startedAt).Seconds()),
		"export_available": s.derivatives != nil && s.derivatives.Available(),
	})
}

type dashboardResponse struct {
	ActiveRecordingsCount int                `json:"active_recordings_count"`
	CompletedLast24Hours  int                `json:"completed_last_24h"`
	InterruptedLast24H    int                `json:"interrupted_last_24h"`
	RecordingsTotal       int                `json:"recordings_total"`
	SegmentsTotal         int                `json:"segments_total"`
	GapsTotal             int                `json:"gaps_total"`
	ArchiveBytes          uint64             `json:"archive_bytes"`
	ArchiveBytesKnown     bool               `json:"archive_bytes_known"`
	CanonicalPayloadBytes uint64             `json:"canonical_payload_bytes"`
	StatisticsStatus      string             `json:"statistics_status"`
	UnavailableFields     []string           `json:"unavailable_fields"`
	FilesystemTotalBytes  uint64             `json:"filesystem_total_bytes"`
	FilesystemFreeBytes   uint64             `json:"filesystem_free_bytes"`
	FilesystemUsedBytes   uint64             `json:"filesystem_used_bytes"`
	Integrity             map[string]int     `json:"integrity"`
	Adapters              map[string]int     `json:"adapters"`
	ExportAvailable       bool               `json:"export_available"`
	RecentRecordings      []recordingSummary `json:"recent_recordings"`
	ActiveRecordingItems  []recordingSummary `json:"active_recordings"`
	Watches               map[string]int     `json:"watches,omitempty"`
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		writeError(w, http.StatusServiceUnavailable, "dashboard data is unavailable")
		return
	}
	items, err := listManagementSummaries(r.Context(), s.manager)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "recording summaries are temporarily unavailable")
		return
	}
	stats, unavailable := s.managementStorageStats(items)
	now := time.Now().UTC()
	cutoff := now.Add(-24 * time.Hour)
	result := dashboardResponse{
		RecordingsTotal: len(items), ArchiveBytes: stats.RecordingBytes, ArchiveBytesKnown: false,
		CanonicalPayloadBytes: stats.CanonicalPayloadBytes, StatisticsStatus: stats.Status, UnavailableFields: unavailable,
		FilesystemTotalBytes: stats.FilesystemTotalBytes, FilesystemFreeBytes: stats.FilesystemAvailableBytes,
		FilesystemUsedBytes: stats.FilesystemUsedBytes,
		Integrity:           map[string]int{"verified": 0, "degraded": 0, "failed": 0, "unknown": 0, "verifying": 0},
		Adapters:            map[string]int{"total": 0, "ready": 0, "unavailable": 0, "failed": 0, "rejected": 0},
		ExportAvailable:     s.derivatives != nil && s.derivatives.Available(),
		RecentRecordings:    []recordingSummary{}, ActiveRecordingItems: []recordingSummary{},
	}
	if s.watches != nil {
		result.Watches = s.watches.Summary()
	}
	for _, item := range items {
		result.SegmentsTotal += item.SegmentCount()
		result.GapsTotal += recordingGapCount(item)
		if item.State == domain.StateRecording {
			result.ActiveRecordingsCount++
			if len(result.ActiveRecordingItems) < 10 {
				result.ActiveRecordingItems = append(result.ActiveRecordingItems, s.recordingSummary(item))
			}
		}
		if item.StoppedAt != nil && !item.StoppedAt.Before(cutoff) {
			if item.State == domain.StateCompleted {
				result.CompletedLast24Hours++
			}
			if item.State == domain.StateInterrupted {
				result.InterruptedLast24H++
			}
		}
		status := string(s.integrityStatusFor(item))
		if _, ok := result.Integrity[status]; !ok {
			status = string(storage.IntegrityUnknown)
		}
		result.Integrity[status]++
	}
	for i := 0; i < len(items) && i < 10; i++ {
		result.RecentRecordings = append(result.RecentRecordings, s.recordingSummary(items[i]))
	}
	if s.adapters != nil {
		for _, adapter := range s.adapters.List() {
			result.Adapters["total"]++
			if _, ok := result.Adapters[adapter.Status.State]; ok {
				result.Adapters[adapter.Status.State]++
			}
		}
	}
	writeJSON(w, http.StatusOK, result)
}

type managementStorageStats struct {
	storage.StorageStats
	CanonicalPayloadBytes uint64 `json:"canonical_payload_bytes"`
	GapCount              int    `json:"gap_count"`
	Status                string `json:"statistics_status"`
	RecordingBytesKnown   bool   `json:"recordings_bytes_known"`
}

func (s *Server) managementStorageStats(recordings []*domain.Recording) (managementStorageStats, []string) {
	result := managementStorageStats{Status: "partial"}
	missing := []string{"recording_archive_bytes"}
	capacityUnavailable := true
	if s.storage != nil {
		capacity, err := s.storage.StorageCapacityStats()
		if err == nil && capacity.CapacityKnown {
			result.CapacityKnown = true
			result.FilesystemTotalBytes = capacity.FilesystemTotalBytes
			result.FilesystemUsedBytes = capacity.FilesystemUsedBytes
			result.FilesystemAvailableBytes = capacity.FilesystemAvailableBytes
			capacityUnavailable = false
		}
	}
	if capacityUnavailable {
		missing = append(missing, "filesystem_capacity")
	}
	for _, recording := range recordings {
		if recording == nil {
			result.Status = "partial"
			continue
		}
		result.RecordingCount++
		segments := recording.SegmentCount()
		if segments > 0 && result.SegmentCount > int(^uint(0)>>1)-segments {
			result.Status = "partial"
			missing = appendUnique(missing, "segment_count")
		} else {
			result.SegmentCount += segments
		}
		if recording.FormatVersion == storage.ShardedArchiveFormatVersion && recording.ShardedArchive != nil {
			if recording.ShardedArchive.InitCount <= uint64(^uint(0)>>1)-uint64(result.InitSegmentCount) {
				result.InitSegmentCount += int(recording.ShardedArchive.InitCount)
			} else {
				result.Status = "partial"
				missing = appendUnique(missing, "init_segment_count")
			}
			if recording.ShardedArchive.ManifestSnapshotCount <= uint64(^uint(0)>>1)-uint64(result.ManifestCount) {
				result.ManifestCount += int(recording.ShardedArchive.ManifestSnapshotCount)
			} else {
				result.Status = "partial"
				missing = appendUnique(missing, "manifest_count")
			}
			if recording.ShardedArchive.PayloadBytes <= ^uint64(0)-result.CanonicalPayloadBytes {
				result.CanonicalPayloadBytes += recording.ShardedArchive.PayloadBytes
			} else {
				result.Status = "partial"
				missing = appendUnique(missing, "canonical_payload_bytes")
			}
			maxInt := uint64(^uint(0) >> 1)
			if recording.ShardedArchive.GapCount <= maxInt-uint64(result.GapCount) {
				result.GapCount += int(recording.ShardedArchive.GapCount)
			} else {
				result.Status = "partial"
				missing = appendUnique(missing, "gap_count")
			}
			continue
		}
		for _, track := range recording.Tracks {
			if track == nil {
				result.Status = "partial"
				missing = appendUnique(missing, "recording_counts")
				continue
			}
			result.InitSegmentCount += len(track.InitSegments)
		}
		result.ManifestCount += len(recording.Snapshots)
		result.GapCount += len(recording.Gaps)
	}
	return result, missing
}

func recordingGapCount(recording *domain.Recording) int {
	if recording == nil {
		return 0
	}
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion && recording.ShardedArchive != nil {
		maxInt := uint64(^uint(0) >> 1)
		if recording.ShardedArchive.GapCount > maxInt {
			return int(maxInt)
		}
		return int(recording.ShardedArchive.GapCount)
	}
	return len(recording.Gaps)
}

func (s *Server) recordingTagsGet(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "management storage is unavailable")
		return
	}
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	if _, err := s.recordingSnapshot(r.Context(), r.PathValue("id")); err != nil {
		writeStorageError(w, err)
		return
	}
	tags, err := s.products.Tags(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "recording tags are unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tags": tags})
}

func (s *Server) recordingTagsPut(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Tags []string `json:"tags"`
	}
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid tags request")
		return
	}
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "management storage is unavailable")
		return
	}
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	if _, err := s.recordingSnapshot(r.Context(), r.PathValue("id")); err != nil {
		writeStorageError(w, err)
		return
	}
	if err := s.products.SetTags(r.PathValue("id"), request.Tags); err != nil {
		writeError(w, http.StatusBadRequest, "tags update was rejected")
		return
	}
	auditRecorded := s.auditMutation(w, r, "recording_tags_updated", r.PathValue("id"))
	tags, _ := s.products.Tags(r.PathValue("id"))
	writeJSON(w, http.StatusOK, map[string]any{"tags": tags, "audit_recorded": auditRecorded})
}

func (s *Server) recordingDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validRecordingPathID(id) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	lock := s.productLock(id)
	lock.Lock()
	defer lock.Unlock()
	if s.integrity != nil && s.integrity.InProgress(id) {
		writeError(w, http.StatusConflict, "recording integrity verification is active")
		return
	}
	if s.derivatives != nil && s.derivatives.InProgress(id) {
		writeError(w, http.StatusConflict, "recording export is active")
		return
	}
	err := s.manager.Delete(id)
	if errors.Is(err, acquire.ErrActiveRecording) {
		writeError(w, http.StatusConflict, "active recordings must be stopped before deletion")
		return
	}
	if err != nil {
		writeStorageError(w, err)
		return
	}
	// Canonical deletion succeeded. Audit it before secondary bookkeeping so a
	// projection cleanup failure cannot erase the primary mutation's audit.
	s.auditMutation(w, r, "recording_deleted", id)
	s.cleanupPreviewProjection(id)
	if s.products != nil {
		if err = s.products.ForgetRecording(id); err != nil {
			writeError(w, http.StatusInternalServerError, "recording was deleted but management metadata cleanup is pending")
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func validRecordingPathID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, character := range id {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func (s *Server) archiveIndex(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	if r.Context().Err() != nil {
		return
	}
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		entries, err := s.shardedArchiveIndex(r.Context(), recording)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "archive index is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"recording_id": id, "entries": entries})
		return
	}
	entries, err := s.storage.ArchiveIndex(recording)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "archive index is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recording_id": id, "entries": entries})
}

func (s *Server) integrityGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if s.integrity == nil {
		writeError(w, http.StatusServiceUnavailable, "integrity verification is unavailable")
		return
	}
	result, ok := s.integrity.StatusFor(recording)
	if !ok {
		result = integrity.ResultProjection{
			IntegrityResult: storage.IntegrityResult{Status: storage.IntegrityUnknown, Issues: []storage.IntegrityIssue{}},
			Freshness:       integrity.FreshnessUnknown,
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) integrityStart(w http.ResponseWriter, r *http.Request) {
	if s.integrity == nil {
		writeError(w, http.StatusServiceUnavailable, "integrity verification is unavailable")
		return
	}
	id := r.PathValue("id")
	lock := s.productLock(id)
	lock.RLock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err == nil && recording.State == domain.StateRecording {
		err = acquire.ErrActiveRecording
	}
	if err == nil {
		var job integrity.Job
		job, err = s.integrity.Start(context.Background(), recording)
		lock.RUnlock()
		if err != nil {
			writeError(w, http.StatusConflict, "integrity job could not be started")
			return
		}
		if s.products != nil {
			s.auditMutation(w, r, "integrity_requested", id)
		}
		s.appendRecordingEvent(id, "integrity_started", job.CreatedAt, 0, "integrity verification started")
		writeJSON(w, http.StatusAccepted, job)
		return
	}
	lock.RUnlock()
	if errors.Is(err, acquire.ErrActiveRecording) {
		writeError(w, http.StatusConflict, "active recordings cannot be verified")
		return
	}
	writeStorageError(w, err)
}

func (s *Server) integrityJobGet(w http.ResponseWriter, r *http.Request) {
	if s.integrity == nil {
		writeError(w, http.StatusServiceUnavailable, "integrity jobs are unavailable")
		return
	}
	job, err := s.integrity.Get(r.PathValue("job_id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "integrity job not found")
		return
	}
	recording, err := s.recordingSnapshot(r.Context(), job.RecordingID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, integrity.ProjectJob(job, recording))
}

func (s *Server) integrityJobCancel(w http.ResponseWriter, r *http.Request) {
	if s.integrity == nil {
		writeError(w, http.StatusServiceUnavailable, "integrity jobs are unavailable")
		return
	}
	var request struct{}
	if err := decodeJSONBody(w, r, 1024, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid integrity cancellation request")
		return
	}
	job, err := s.integrity.Cancel(r.PathValue("job_id"))
	if err != nil {
		if errors.Is(err, integrity.ErrNotFound) {
			writeError(w, http.StatusNotFound, "integrity job not found")
		} else {
			writeError(w, http.StatusServiceUnavailable, "integrity job cancellation could not be saved")
		}
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) workflowList(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeJSON(w, http.StatusOK, []adapterhost.WorkflowSummary{})
		return
	}
	state, adapterID := r.URL.Query().Get("state"), r.URL.Query().Get("adapter")
	if len(state) > 64 || len(adapterID) > 128 {
		writeError(w, http.StatusBadRequest, "invalid workflow filter")
		return
	}
	items := s.adapters.ListWorkflows()
	filtered := make([]adapterhost.WorkflowSummary, 0, len(items))
	for _, item := range items {
		if state != "" && item.State != state || adapterID != "" && item.AdapterID != adapterID {
			continue
		}
		filtered = append(filtered, item)
	}
	writeJSON(w, http.StatusOK, filtered)
}

func (s *Server) workflowHistoryList(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow history is unavailable")
		return
	}
	limit, err := boundedLimit(r, 100, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow history limit")
		return
	}
	items := s.products.WorkflowHistory("", r.URL.Query().Get("adapter"), r.URL.Query().Get("state"), limit)
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) workflowHistoryGet(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "workflow history is unavailable")
		return
	}
	items := s.products.WorkflowHistory(r.PathValue("id"), "", "", 5000)
	if len(items) == 0 {
		writeError(w, http.StatusNotFound, "workflow history not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workflow_id": r.PathValue("id"), "events": items})
}

func (s *Server) resourceList(w http.ResponseWriter, r *http.Request)   { s.resourceBrowse(w, r, false) }
func (s *Server) resourceSearch(w http.ResponseWriter, r *http.Request) { s.resourceBrowse(w, r, true) }
func (s *Server) resourceBrowse(w http.ResponseWriter, r *http.Request, search bool) {
	if s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter resource browsing is unavailable")
		return
	}
	parent, err := resourceParentQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid parent resource")
		return
	}
	query := ""
	if search {
		query = strings.TrimSpace(r.URL.Query().Get("q"))
		if query == "" {
			writeError(w, http.StatusBadRequest, "q is required")
			return
		}
	}
	limit, err := boundedLimit(r, 20, 50)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid resource page limit")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	page, err := s.adapters.BrowseResources(ctx, r.PathValue("id"), parent, r.URL.Query().Get("resource_type"), query, r.URL.Query().Get("cursor"), limit)
	if errors.Is(err, adapterhost.ErrUnsupportedResourceBrowse) {
		writeError(w, http.StatusNotImplemented, "adapter resource browsing is not supported")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "adapter resource query failed")
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func resourceParentQuery(r *http.Request) (*adapterproto.ResourceRef, error) {
	raw := r.URL.Query().Get("parent")
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 8192 {
		return nil, errors.New("resource parent is too large")
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var ref adapterproto.ResourceRef
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&ref); err != nil {
		return nil, err
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("resource parent must contain one JSON object")
	}
	if err = adapterproto.ValidateResourceRef(&ref); err != nil {
		return nil, err
	}
	return &ref, nil
}

func (s *Server) globalSearch(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	for key := range values {
		if key != "q" && key != "limit" {
			writeError(w, http.StatusBadRequest, "unknown search query parameter")
			return
		}
	}
	if len(values["q"]) > 1 {
		writeError(w, http.StatusBadRequest, "search query must occur once")
		return
	}
	query := ""
	if len(values["q"]) == 1 {
		if len(values["q"][0]) > maxSearchQuery {
			writeError(w, http.StatusBadRequest, "search query exceeds limit")
			return
		}
		query = strings.TrimSpace(values["q"][0])
	}
	if len(query) > maxSearchQuery {
		writeError(w, http.StatusBadRequest, "search query exceeds limit")
		return
	}
	limit, err := boundedLimit(r, defaultSearchResults, maxSearchResults)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid search limit")
		return
	}
	if query == "" {
		writeJSON(w, http.StatusOK, map[string]any{"results": []any{}})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), globalSearchTimeout)
	defer cancel()
	searchFailure := func(err error) {
		switch {
		case errors.Is(err, context.Canceled):
			// The client has gone away; there is no useful response to send.
			return
		case errors.Is(err, context.DeadlineExceeded):
			writeError(w, http.StatusGatewayTimeout, "global search timed out")
		case errors.Is(err, acquire.ErrListLimit):
			writeError(w, http.StatusServiceUnavailable, "global search recording scan exceeds its limit")
		default:
			writeError(w, http.StatusServiceUnavailable, "global search is temporarily unavailable")
		}
	}
	recordings, err := listManagementSummaries(ctx, s.manager)
	if err != nil {
		searchFailure(err)
		return
	}
	lower := strings.ToLower(query)
	results := make([]map[string]any, 0, limit)
	resources := make(map[string]map[string]any)
	tagsPartial := false
	add := func(item map[string]any) bool {
		if len(results) >= limit {
			return false
		}
		results = append(results, item)
		return true
	}
	for _, recording := range recordings {
		if err = ctx.Err(); err != nil {
			searchFailure(err)
			return
		}
		tags := []string{}
		if s.products != nil {
			tags, err = s.products.Tags(recording.ID)
			if err != nil {
				tagsPartial = true
				s.reportReadModelFailure(recording.ID, "tags", "management_unavailable")
				tags = []string{}
			}
		}
		resourceID, resourceType := "", ""
		if recording.Resource != nil {
			resourceID, resourceType = recording.Resource.ID, recording.Resource.Type
		}
		adapterName := ""
		if recording.Adapter != nil {
			adapterName = recording.Adapter.Name
		}
		blob := strings.ToLower(strings.Join(append([]string{recording.ID, recording.Title, recording.AdapterID, adapterName, resourceID}, tags...), " "))
		if strings.Contains(blob, lower) {
			if !add(map[string]any{"type": "recording", "id": recording.ID, "title": recording.Title, "state": recording.State, "adapter_id": recording.AdapterID, "resource_type": resourceType, "resource_id": resourceID}) {
				break
			}
		}
		for resource := recording.Resource; resource != nil; resource = resource.Parent {
			if err = ctx.Err(); err != nil {
				searchFailure(err)
				return
			}
			resourceBlob := strings.ToLower(recording.AdapterID + " " + resource.Type + " " + resource.ID)
			if !strings.Contains(resourceBlob, lower) {
				continue
			}
			key := recording.AdapterID + "\x00" + resource.Type + "\x00" + resource.ID
			if _, exists := resources[key]; !exists && len(resources) < maxSearchResults {
				resources[key] = map[string]any{"type": "resource", "adapter_id": recording.AdapterID, "resource_type": resource.Type, "resource_id": resource.ID, "display_name": resource.ID}
			}
		}
	}
	if err = ctx.Err(); err != nil {
		searchFailure(err)
		return
	}
	resourceKeys := make([]string, 0, len(resources))
	for key := range resources {
		resourceKeys = append(resourceKeys, key)
	}
	sort.Strings(resourceKeys)
	for _, key := range resourceKeys {
		if err = ctx.Err(); err != nil {
			searchFailure(err)
			return
		}
		if !add(resources[key]) {
			break
		}
	}
	if len(results) < limit && s.adapters != nil {
		for _, adapter := range s.adapters.List() {
			if err = ctx.Err(); err != nil {
				searchFailure(err)
				return
			}
			d := adapter.Descriptor
			if d == nil {
				continue
			}
			if strings.Contains(strings.ToLower(d.ID+" "+d.Name), lower) {
				if !add(map[string]any{"type": "adapter", "id": d.ID, "name": d.Name, "state": adapter.Status.State}) {
					break
				}
			}
		}
	}
	if len(results) < limit && s.adapters != nil {
		for _, workflow := range s.adapters.ListWorkflows() {
			if err = ctx.Err(); err != nil {
				searchFailure(err)
				return
			}
			if strings.Contains(strings.ToLower(workflow.WorkflowID+" "+workflow.AdapterID), lower) {
				if !add(map[string]any{"type": "workflow", "id": workflow.WorkflowID, "adapter_id": workflow.AdapterID, "state": workflow.State}) {
					break
				}
			}
		}
	}
	if err = ctx.Err(); err != nil {
		searchFailure(err)
		return
	}
	response := map[string]any{"results": results}
	if tagsPartial {
		response["partial_errors"] = []string{"tags"}
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) recordingEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	events := make([]management.RecordingEvent, 0)
	add := func(kind string, at time.Time, count int, message string) {
		events = append(events, management.RecordingEvent{ID: stableRecordingEventID(id, kind, at, count), RecordingID: id, Type: kind, At: at, Count: count, Message: message})
	}
	add("recording_started", recording.StartedAt, 0, "")
	var firstManifestAt time.Time
	manifestCount := 0
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		if s.storage == nil {
			writeError(w, http.StatusServiceUnavailable, "recording history is unavailable")
			return
		}
		err = s.storage.IterateShardedManifests(r.Context(), id, func(snapshot domain.ManifestSnapshot) error {
			if manifestCount == 0 || snapshot.FetchedAt.Before(firstManifestAt) {
				firstManifestAt = snapshot.FetchedAt
			}
			if manifestCount < int(^uint(0)>>1) {
				manifestCount++
			}
			return nil
		})
	} else if len(recording.Snapshots) > 0 {
		first := recording.Snapshots[0]
		for _, snapshot := range recording.Snapshots[1:] {
			if snapshot.FetchedAt.Before(first.FetchedAt) {
				first = snapshot
			}
		}
		firstManifestAt = first.FetchedAt
		manifestCount = len(recording.Snapshots)
	}
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if manifestCount > 0 {
		add("manifest_observed", firstManifestAt, manifestCount, "manifest observed")
	}
	var firstGapAt time.Time
	gapCount := 0
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		if s.storage == nil {
			writeError(w, http.StatusServiceUnavailable, "recording history is unavailable")
			return
		}
		err = s.storage.IterateShardedGaps(r.Context(), id, func(gap domain.Gap) error {
			if gapCount == 0 || gap.DetectedAt.Before(firstGapAt) {
				firstGapAt = gap.DetectedAt
			}
			if gapCount < int(^uint(0)>>1) {
				gapCount++
			}
			return nil
		})
	} else if len(recording.Gaps) > 0 {
		firstGapAt = recording.Gaps[0].DetectedAt
		gapCount = len(recording.Gaps)
	}
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if gapCount > 0 {
		add("gap_detected", firstGapAt, gapCount, "gap detected")
	}
	if recording.StoppedAt != nil {
		kind, message := "recording_stopped", "recording stopped"
		if recording.State == domain.StateCompleted {
			kind, message = "recording_completed", "recording completed"
		}
		if recording.State == domain.StateInterrupted {
			kind, message = "recording_interrupted", "recording interrupted"
		}
		add(kind, *recording.StoppedAt, 0, message)
	}
	if s.products != nil {
		projected, projectionErr := s.products.RecordingEvents(id, 2000)
		if projectionErr == nil {
			events = append(events, projected...)
		}
	}
	if s.integrity != nil {
		// Completion is a historical event. Keep it visible after an archive
		// mutation, but derive the event from the revision-aware projection.
		if result, ok := s.integrity.StatusFor(recording); ok && !result.LastVerifiedAt.IsZero() {
			add("integrity_completed", result.LastVerifiedAt, result.ObjectsVerified, "integrity verification completed")
		}
	}
	if s.derivatives != nil {
		for _, job := range s.derivatives.List(id) {
			if job.StartedAt != nil {
				events = append(events, management.RecordingEvent{ID: "export-" + job.ID + "-started", RecordingID: id, Type: "export_started", At: job.StartedAt.UTC(), Message: "export started"})
			}
			if job.FinishedAt == nil {
				continue
			}
			switch job.State {
			case derivative.StateCompleted:
				events = append(events, management.RecordingEvent{ID: "export-" + job.ID + "-completed", RecordingID: id, Type: "export_completed", At: job.FinishedAt.UTC(), Message: "export completed"})
			case derivative.StateFailed:
				events = append(events, management.RecordingEvent{ID: "export-" + job.ID + "-failed", RecordingID: id, Type: "export_failed", At: job.FinishedAt.UTC(), Message: "export failed"})
			case derivative.StateCanceled:
				events = append(events, management.RecordingEvent{ID: "export-" + job.ID + "-canceled", RecordingID: id, Type: "export_canceled", At: job.FinishedAt.UTC(), Message: "export canceled"})
			}
		}
	}
	seen := make(map[string]struct{}, len(events))
	unique := events[:0]
	for _, event := range events {
		if _, ok := seen[event.ID]; ok {
			continue
		}
		seen[event.ID] = struct{}{}
		unique = append(unique, event)
	}
	events = unique
	sort.Slice(events, func(i, j int) bool {
		if events[i].At.Equal(events[j].At) {
			return events[i].ID < events[j].ID
		}
		return events[i].At.After(events[j].At)
	})
	limit, limitErr := boundedLimit(r, 100, 500)
	if limitErr != nil {
		writeError(w, http.StatusBadRequest, "invalid event limit")
		return
	}
	if len(events) > limit {
		events = events[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": events})
}

type recordingMetadataResponse struct {
	Current   *domain.MetadataRevision  `json:"current,omitempty"`
	Items     []domain.MetadataRevision `json:"items"`
	Truncated bool                      `json:"truncated"`
}

func (s *Server) recordingMetadataGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	items := make([]domain.MetadataRevision, 0, min(metadataTimelineAPILimit, len(recording.MetadataTimeline)))
	truncated := recording.MetadataTimelineTruncated
	var current *domain.MetadataRevision
	if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		if s.storage == nil {
			writeError(w, http.StatusServiceUnavailable, "recording metadata history is unavailable")
			return
		}
		err = s.storage.IterateShardedMetadata(r.Context(), id, func(revision domain.MetadataRevision) error {
			latest := revision
			current = &latest
			if len(items) == metadataTimelineAPILimit {
				copy(items, items[1:])
				items = items[:len(items)-1]
				truncated = true
			}
			items = append(items, revision)
			return nil
		})
		if err != nil {
			writeStorageError(w, err)
			return
		}
	} else {
		items = append(items, recording.MetadataTimeline...)
		if len(items) > metadataTimelineAPILimit {
			items = items[len(items)-metadataTimelineAPILimit:]
			truncated = true
		}
		if len(recording.MetadataTimeline) > 0 {
			latest := recording.MetadataTimeline[len(recording.MetadataTimeline)-1]
			current = &latest
		}
	}
	response := recordingMetadataResponse{Items: items, Truncated: truncated}
	response.Current = current
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) notificationList(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "notifications are unavailable")
		return
	}
	unread := r.URL.Query().Get("unread") == "true"
	limit, err := boundedLimit(r, 100, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid notification limit")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": s.products.Notifications(unread, limit)})
}

func (s *Server) notificationSync(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "notifications are unavailable")
		return
	}
	var request struct{}
	if err := decodeJSONBody(w, r, 1024, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid notification sync request")
		return
	}
	if err := s.syncRecordingNotifications(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "notifications could not be synchronized")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// syncRecordingNotifications projects recent terminal recording states into
// the bounded notification store. Stable IDs make polling idempotent, and old
// archives do not create an initial flood when the UI is first opened.
func (s *Server) syncRecordingNotifications() error {
	if s.products == nil || s.manager == nil {
		return nil
	}
	add := func(notification management.Notification) error {
		err := s.products.AddNotification(notification)
		if errors.Is(err, management.ErrDuplicateNotification) {
			return nil
		}
		return err
	}
	recordings, err := listManagementSummaries(context.Background(), s.manager)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	for _, recording := range recordings {
		if recording.StoppedAt == nil || recording.StoppedAt.Before(cutoff) {
			continue
		}
		kind := ""
		switch recording.State {
		case domain.StateCompleted:
			kind = "recording_completed"
		case domain.StateInterrupted:
			kind = "recording_interrupted"
		}
		if kind == "" {
			continue
		}
		id := "recording-" + recording.ID + "-" + string(recording.State)
		if err := add(management.Notification{ID: id, Type: kind, At: recording.StoppedAt.UTC(), ObjectID: recording.ID}); err != nil {
			return err
		}
	}
	if s.adapters != nil {
		for _, item := range s.adapters.List() {
			if item.Status.State != "unavailable" && item.Status.State != "rejected" && item.Status.State != "failed" {
				continue
			}
			id := "adapter-" + item.Status.ID + "-" + item.Status.State
			if err := add(management.Notification{ID: id, Type: "adapter_unavailable", At: time.Now().UTC()}); err != nil {
				return err
			}
		}
	}
	if s.integrity != nil {
		for _, recording := range recordings {
			result, ok := s.integrity.StatusFor(recording)
			if !ok || result.Freshness != integrity.FreshnessCurrent || result.Status != storage.IntegrityFailed || result.LastVerifiedAt.IsZero() {
				continue
			}
			id := "integrity-" + recording.ID + "-" + strconv.FormatInt(result.LastVerifiedAt.UnixNano(), 10)
			if err := add(management.Notification{ID: id, Type: "integrity_failure", At: result.LastVerifiedAt.UTC(), ObjectID: recording.ID}); err != nil {
				return err
			}
		}
	}
	if s.derivatives != nil {
		for _, job := range s.derivatives.List("") {
			if job.State != derivative.StateFailed || job.FinishedAt == nil || job.FinishedAt.Before(cutoff) {
				continue
			}
			id := "export-" + job.ID + "-failed"
			if err := add(management.Notification{ID: id, Type: "export_failed", At: job.FinishedAt.UTC(), ObjectID: job.RecordingID}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) notificationRead(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "notifications are unavailable")
		return
	}
	if err := s.products.MarkNotificationRead(r.PathValue("id")); err != nil {
		writeError(w, http.StatusNotFound, "notification not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) notificationReadAll(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "notifications are unavailable")
		return
	}
	if err := s.products.MarkAllNotificationsRead(); err != nil {
		writeError(w, http.StatusInternalServerError, "notification state could not be saved")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) appendAudit(kind, objectID string) error {
	return s.appendAuditAs(kind, objectID, nil)
}

func (s *Server) appendAuditAs(kind, objectID string, actor *management.AuditActor) error {
	if s.products == nil {
		return nil
	}
	if actor == nil {
		actor = &management.AuditActor{Type: management.AuditActorSystem}
	}
	err := s.products.AppendAudit(management.AuditEvent{ID: randomProductID(), Type: kind, At: time.Now().UTC(), ObjectID: objectID, Actor: actor})
	if err != nil && s.logs != nil {
		s.logs.Add("error", "audit", "audit append failed after primary mutation")
	}
	return err
}

func (s *Server) appendWorkflowHistory(progress adapterhost.WorkflowProgress, state string) {
	if s.products == nil || progress.WorkflowID == "" || progress.AdapterID == "" {
		return
	}
	var resource *management.ResourceRef
	if progress.Resource != nil {
		resource = managementResource(progress.Resource)
	}
	var challenge *management.ChallengeSummary
	if progress.Challenge != nil {
		fieldCount := len(progress.Challenge.Schema.Fields)
		hasSecret := false
		for _, field := range progress.Challenge.Schema.Fields {
			if field.Control == "secret" {
				hasSecret = true
			}
		}
		if fieldCount == 0 && progress.Challenge.Prompt != nil {
			fieldCount = len(progress.Challenge.Prompt.Fields)
			for _, field := range progress.Challenge.Prompt.Fields {
				if field.Control == "secret" {
					hasSecret = true
				}
			}
		}
		summary := &management.ChallengeSummary{FieldCount: fieldCount, HasSecretFields: hasSecret}
		if progress.Challenge.Prompt != nil {
			summary.Title = safeWorkflowPromptType(progress.Challenge.Prompt.Type)
		}
		challenge = summary
	}
	event := management.WorkflowHistoryEvent{ID: randomProductID(), WorkflowID: progress.WorkflowID, AdapterID: progress.AdapterID, State: state, At: time.Now().UTC(), Resource: resource, Challenge: challenge}
	_ = s.products.AppendWorkflowHistory(event)
}

func safeWorkflowPromptType(value string) string {
	switch value {
	case "action", "prompt", "secret_prompt", "navigate", "display", "status", "complete", "error":
		return value
	default:
		return ""
	}
}

func (s *Server) flushWorkflowLifecycleEvents() {
	if s.adapters == nil {
		return
	}
	if s.products == nil {
		s.adapters.DiscardWorkflowLifecycleEvents()
		return
	}
	for _, event := range s.adapters.PendingWorkflowLifecycleEvents() {
		var resource *management.ResourceRef
		if event.Resource != nil {
			resource = managementResource(event.Resource)
		}
		var challenge *management.ChallengeSummary
		if event.Challenge != nil {
			challenge = &management.ChallengeSummary{
				// The existing history projection has a title slot but no prompt
				// type field. Store only the protocol's bounded prompt type enum in
				// that slot; never persist adapter-provided title/message text.
				Title:           safeWorkflowPromptType(event.Challenge.PromptType),
				FieldCount:      event.Challenge.FieldCount,
				HasSecretFields: event.Challenge.HasSecretFields,
			}
		}
		persisted := management.WorkflowHistoryEvent{
			ID: event.ID, WorkflowID: event.WorkflowID, AdapterID: event.AdapterID,
			State: event.State, At: event.At, Resource: resource, Challenge: challenge,
		}
		err := s.products.AppendWorkflowHistory(persisted)
		if err == nil || err.Error() == "duplicate workflow history event identifier" {
			s.adapters.AckWorkflowLifecycleEvent(event.ID)
			continue
		}
		// Keep the failed event and all following events queued. A later HTTP
		// request retries persistence using the same stable event IDs.
		return
	}
}

func managementResource(ref *adapterproto.ResourceRef) *management.ResourceRef {
	if ref == nil {
		return nil
	}
	return &management.ResourceRef{Type: ref.Type, ID: ref.ID, Parent: managementResource(ref.Parent)}
}

func (s *Server) appendRecordingEvent(recordingID, kind string, at time.Time, count int, message string) {
	if s.products == nil || recordingID == "" || at.IsZero() {
		return
	}
	event := management.RecordingEvent{ID: stableRecordingEventID(recordingID, kind, at, count), RecordingID: recordingID, Type: kind, At: at.UTC(), Count: count, Message: message}
	_ = s.products.AppendRecordingEvent(event)
}

func randomProductID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(raw[:])
}

func stableRecordingEventID(recordingID, kind string, at time.Time, count int) string {
	digest := sha256.Sum256([]byte(recordingID + "\x00" + kind + "\x00" + at.UTC().Format(time.RFC3339Nano) + "\x00" + strconv.Itoa(count)))
	return hex.EncodeToString(digest[:16])
}

func boundedLimit(r *http.Request, defaultValue, max int) (int, error) {
	values := r.URL.Query()["limit"]
	if len(values) == 0 || len(values) == 1 && values[0] == "" {
		return defaultValue, nil
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("limit must occur once")
	}
	value, err := strconv.Atoi(values[0])
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid limit")
	}
	if value == 0 {
		return defaultValue, nil
	}
	if value > max {
		return max, nil
	}
	return value, nil
}
