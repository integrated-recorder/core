package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/applog"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recordquery"
	"github.com/integrated-recorder/core/internal/storage"
)

type readModelStorageBackend struct {
	storage.StorageBackend
	failID              string
	directoryBytesCalls atomic.Int32
	storageStatsCalls   atomic.Int32
	blockForStop        bool
	started             chan struct{}
	canceled            chan struct{}
	afterSaveRecording  func(*domain.Recording)
}

func (b *readModelStorageBackend) RecordingDirectoryBytesContext(ctx context.Context, id string) (int64, error) {
	b.directoryBytesCalls.Add(1)
	if id != b.failID {
		return b.StorageBackend.RecordingDirectoryBytes(id)
	}
	if b.blockForStop {
		select {
		case b.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case b.canceled <- struct{}{}:
		default:
		}
		return 0, ctx.Err()
	}
	return 0, errors.New("provider list failed for https://private.invalid/archive?token=secret /private/archive/root")
}

func (b *readModelStorageBackend) StorageStats() (storage.StorageStats, error) {
	b.storageStatsCalls.Add(1)
	return storage.StorageStats{}, errors.New("full archive stats unavailable")
}

func (b *readModelStorageBackend) SaveRecording(recording *domain.Recording) error {
	if err := b.StorageBackend.SaveRecording(recording); err != nil {
		return err
	}
	if b.afterSaveRecording != nil {
		b.afterSaveRecording(recording)
	}
	return nil
}

func (b *readModelStorageBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	reader, ok := b.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	})
	if !ok {
		return storage.ErrSidecarReadUnsupported
	}
	return reader.LoadSidecar(id, relativePath, maxBytes, output)
}

func (b *readModelStorageBackend) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (storage.V2SidecarPage, error) {
	lister, ok := b.StorageBackend.(interface {
		ListSidecarsContext(context.Context, string, string, string, int) (storage.V2SidecarPage, error)
	})
	if !ok {
		return storage.V2SidecarPage{}, storage.ErrSidecarReadUnsupported
	}
	return lister.ListSidecarsContext(ctx, id, prefix, cursor, limit)
}

func TestRecordingDetailDegradesArchiveSizeFailure(t *testing.T) {
	store, _, recording := readModelFixture(t, strings.Repeat("e", 32), domain.StateRecording)
	backend := &readModelStorageBackend{StorageBackend: store.StorageBackend, failID: recording.ID}
	store.StorageBackend = backend
	logs := applog.NewStore()
	handler := NewWithOptions(&fixedRecordingManager{recording: recording}, nil, nil, Options{Storage: store, Logs: logs})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		State      domain.RecordingState `json:"state"`
		Statistics recordingStatistics   `json:"statistics"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != domain.StateRecording || result.Statistics.Status != "partial" || result.Statistics.ArchiveSizeBytes != nil ||
		result.Statistics.SegmentCount != 1 || result.Statistics.MediaPayloadSizeBytes == 0 ||
		!containsString(result.Statistics.UnavailableFields, "archive_size_bytes") {
		t.Fatalf("detail did not preserve canonical fields with partial statistics: %+v", result)
	}
	if strings.Contains(response.Body.String(), "private.invalid") || strings.Contains(response.Body.String(), "token=secret") || strings.Contains(response.Body.String(), "/private/archive/root") {
		t.Fatalf("storage error leaked in detail: %s", response.Body.String())
	}
	entries, err := logs.Query(applog.Query{Component: "recording-read-model", Limit: 10})
	if err != nil || len(entries.Items) != 1 {
		t.Fatalf("read-model diagnostic entries=%+v err=%v", entries.Items, err)
	}
	if !strings.Contains(entries.Items[0].Message, "component=archive_size") || !strings.Contains(entries.Items[0].Message, "category=provider_list_unavailable") ||
		strings.Contains(entries.Items[0].Message, "private.invalid") || strings.Contains(entries.Items[0].Message, "token=secret") || strings.Contains(entries.Items[0].Message, "/private/archive/root") {
		t.Fatalf("unsafe diagnostic: %+v", entries.Items[0])
	}
	if entries.Items[0].At.IsZero() {
		t.Fatal("diagnostic timestamp missing")
	}
}

func TestRecordingsListDegradesOnlyAffectedItem(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	failed := writeProductRecording(t, store, strings.Repeat("f", 32), domain.StateCompleted)
	healthy := writeProductRecording(t, store, strings.Repeat("1", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.StorageBackend = &readModelStorageBackend{StorageBackend: store.StorageBackend, failID: failed.ID}
	handler := New(manager, nil, nil)

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v2/recordings", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var page recordquery.Result
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("list omitted recordings: %+v", page.Items)
	}
	byID := map[string]recordquery.Item{}
	for _, item := range page.Items {
		byID[item.ID] = item
	}
	partial, ok := byID[failed.ID]
	if !ok || partial.State != string(domain.StateCompleted) || partial.ArchiveSizeBytes != nil || partial.StatisticsStatus != "partial" || !containsString(partial.UnavailableFields, "archive_size_bytes") {
		t.Fatalf("unknown-size item projection=%+v", partial)
	}
	complete, ok := byID[healthy.ID]
	if !ok || complete.ArchiveSizeBytes != nil || complete.StatisticsStatus != "partial" || !containsString(complete.UnavailableFields, "archive_size_bytes") {
		t.Fatalf("healthy root-only projection=%+v", complete)
	}
	if calls := store.StorageBackend.(*readModelStorageBackend).directoryBytesCalls.Load(); calls != 0 {
		t.Fatalf("list enumerated provider archive bytes %d times", calls)
	}
}

func TestV2RecordingDetailUsesBoundedRoot(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := v2ManagementFixture(strings.Repeat("3", 32), time.Now().UTC(), 100_000)
	if err := store.CreateShardedRecording(root); err != nil {
		t.Fatal(err)
	}
	manager := &fixedRecordingManager{recording: root}
	handler := NewWithOptions(manager, nil, nil, Options{Storage: store})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+root.ID, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", response.Code, response.Body.String())
	}
	if manager.getCalls != 0 {
		t.Fatalf("V2 detail materialized archive through Manager.Get %d times", manager.getCalls)
	}
	var result struct {
		SegmentCount int `json:"segment_count"`
		Statistics   struct {
			SegmentCount          int    `json:"segment_count"`
			MediaPayloadSizeBytes int64  `json:"media_payload_size_bytes"`
			ArchiveSizeBytes      *int64 `json:"archive_size_bytes"`
			Status                string `json:"status"`
		} `json:"statistics"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.SegmentCount != 100_000 || result.Statistics.SegmentCount != 100_000 || result.Statistics.MediaPayloadSizeBytes != 18_800_000 || result.Statistics.ArchiveSizeBytes != nil || result.Statistics.Status != "partial" {
		t.Fatalf("V2 bounded detail=%+v", result)
	}
}

func TestRecordingListUsesSummaryPageWithoutArchiveDetails(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &readModelStorageBackend{StorageBackend: store.StorageBackend}
	store.StorageBackend = backend
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	recordings := []*domain.Recording{
		v2ManagementFixture(strings.Repeat("4", 32), base, 1_000),
		v2ManagementFixture(strings.Repeat("5", 32), base.Add(time.Minute), 10_000),
		v2ManagementFixture(strings.Repeat("6", 32), base.Add(2*time.Minute), 100_000),
	}
	manager := &fixedRecordingManager{recordings: recordings}
	handler := NewWithOptions(manager, nil, nil, Options{Storage: store})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v2/recordings?sort=-created_at&limit=1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var page recordquery.Result
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 3 || len(page.Items) != 1 || page.Items[0].ID != recordings[2].ID || page.Items[0].SegmentCount != 100_000 || page.Items[0].MediaPayloadSizeBytes != 18_800_000 {
		t.Fatalf("summary page=%+v", page)
	}
	if manager.getCalls != 0 {
		t.Fatalf("summary list materialized archive through Manager.Get %d times", manager.getCalls)
	}
	if calls := backend.directoryBytesCalls.Load(); calls != 0 {
		t.Fatalf("summary list enumerated physical archive size %d times", calls)
	}
}

func TestDashboardAndStorageStatsDegradeWithoutTreeWalk(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &readModelStorageBackend{StorageBackend: store.StorageBackend}
	store.StorageBackend = backend
	recording := v2ManagementFixture(strings.Repeat("7", 32), time.Now().UTC(), 100_000)
	manager := &fixedRecordingManager{recording: recording}
	handler := NewWithOptions(manager, nil, nil, Options{Storage: store})

	for _, path := range []string{"/api/dashboard", "/api/system/storage"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, response.Code, response.Body.String())
		}
		if !strings.Contains(response.Body.String(), `"statistics_status":"partial"`) || !strings.Contains(response.Body.String(), `"unavailable_fields"`) {
			t.Fatalf("%s omitted partial storage state: %s", path, response.Body.String())
		}
		if path == "/api/system/storage" && !strings.Contains(response.Body.String(), `"recordings_bytes_known":false`) {
			t.Fatalf("%s did not mark aggregate archive bytes unknown: %s", path, response.Body.String())
		}
	}
	if calls := backend.storageStatsCalls.Load(); calls != 0 {
		t.Fatalf("dashboard/storage endpoint invoked full StorageStats %d times", calls)
	}
	if calls := backend.directoryBytesCalls.Load(); calls != 0 {
		t.Fatalf("dashboard/storage endpoint enumerated archive directory %d times", calls)
	}

	// A storage control-plane outage is a derived-statistics failure. Recording
	// summaries remain authoritative and both endpoints stay available.
	nilStorageHandler := NewWithOptions(manager, nil, nil, Options{})
	for _, path := range []string{"/api/dashboard", "/api/system/storage"} {
		response := httptest.NewRecorder()
		nilStorageHandler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s with unavailable storage status=%d body=%s", path, response.Code, response.Body.String())
		}
		if !strings.Contains(response.Body.String(), `"statistics_status":"partial"`) || !strings.Contains(response.Body.String(), `"filesystem_capacity"`) {
			t.Fatalf("%s did not identify unavailable storage capacity: %s", path, response.Body.String())
		}
	}
}

func v2ManagementFixture(id string, created time.Time, mediaCount uint64) *domain.Recording {
	return &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion,
		ID:            id,
		Title:         "bounded fixture",
		State:         domain.StateCompleted,
		CreatedAt:     created,
		StartedAt:     created,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", MediaCount: mediaCount, MediaHighWater: mediaCount},
		},
		ShardedArchive: &domain.ShardedArchiveSummary{
			MediaCount: mediaCount, DurationSeconds: float64(mediaCount),
			PayloadBytes: mediaCount * 188, InitCount: 1, GapCount: 2,
		},
	}
}

func TestRecordingStatisticsListingHonorsRequestCancellation(t *testing.T) {
	store, manager, recording := readModelFixture(t, strings.Repeat("2", 32), domain.StateRecording)
	backend := &readModelStorageBackend{
		StorageBackend: store.StorageBackend, failID: recording.ID, blockForStop: true,
		started: make(chan struct{}, 1), canceled: make(chan struct{}, 1),
	}
	store.StorageBackend = backend
	handler := New(manager, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID, nil).WithContext(ctx)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("archive-size provider call did not start")
	}
	cancel()
	select {
	case <-backend.canceled:
	case <-time.After(time.Second):
		t.Fatal("provider did not observe request cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("detail handler did not return after cancellation")
	}
}

func TestActiveRecordingContinuesCommittingDuringPartialStatisticsReads(t *testing.T) {
	segmentCommitted := make(chan struct{}, 1)
	allowManifest := make(chan struct{})
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			select {
			case <-allowManifest:
			case <-r.Context().Done():
				return
			}
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,\nsegment.ts\n")
		case "/segment.ts":
			_, _ = io.WriteString(w, "live-segment-during-read-model-failure")
		default:
			http.NotFound(w, r)
		}
	}))
	defer source.Close()

	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, source.Client(), nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := manager.Close(ctx); err != nil {
				t.Errorf("close active fixture manager: %v", err)
			}
		}
	})
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: source.URL + "/live.m3u8"}, nil, "active read-model fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &readModelStorageBackend{
		StorageBackend: store.StorageBackend,
		failID:         recording.ID,
		afterSaveRecording: func(recording *domain.Recording) {
			if recording.FormatVersion == storage.ShardedArchiveFormatVersion && recording.Tracks["main"] != nil && recording.Tracks["main"].MediaHighWater > 0 {
				select {
				case segmentCommitted <- struct{}{}:
				default:
				}
			}
		},
	}
	store.StorageBackend = backend
	close(allowManifest)
	handler := NewWithOptions(manager, nil, nil, Options{Storage: store})
	requestDetail := func() recordingDetail {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("detail during active recording status=%d body=%s", response.Code, response.Body.String())
		}
		var result recordingDetail
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.State != domain.StateRecording || result.Statistics == nil || result.Statistics.Status != "partial" || result.Statistics.ArchiveSizeBytes != nil {
			t.Fatalf("active recording read model=%+v", result)
		}
		return result
	}
	requestDetail()
	select {
	case <-segmentCommitted:
	case <-time.After(5 * time.Second):
		t.Fatal("live segment was not committed while partial statistics were served")
	}
	deadline := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	var current *domain.Recording
	for {
		var getErr error
		current, getErr = manager.Get(recording.ID)
		if getErr != nil {
			t.Fatalf("read recording after segment commit: %v", getErr)
		}
		if current.SegmentCount() > 0 {
			break
		}
		select {
		case <-deadline.C:
			track := current.Tracks["main"]
			t.Fatalf("committed segment is not visible in manager state: format=%d media_count=%d media_high_water=%d live_slots=%d", current.FormatVersion, current.ShardedArchive.MediaCount, track.MediaHighWater, track.LiveSlotHighWater)
		case <-ticker.C:
		}
	}
	result := requestDetail()
	if result.SegmentCount == 0 {
		t.Fatal("detail did not expose the segment committed during partial statistics reads")
	}
	if _, err := manager.Stop(recording.ID); err != nil {
		t.Fatalf("stop active fixture recording: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Fatalf("close fixture manager: %v", err)
	}
	closed = true
}

func TestRecordingNotFoundRemainsAuthoritative(t *testing.T) {
	_, manager, _ := readModelFixture(t, strings.Repeat("3", 32), domain.StateCompleted)
	handler := New(manager, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+strings.Repeat("4", 32), nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing recording status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestInvalidCanonicalRecordingMetadataRemainsFailure(t *testing.T) {
	_, _, recording := readModelFixture(t, strings.Repeat("4", 32), domain.StateCompleted)
	recording.Tracks["main"].Segments[0].PayloadSize = -1
	logs := applog.NewStore()
	handler := NewWithOptions(&fixedRecordingManager{recording: recording}, nil, nil, Options{Logs: logs})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID, nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("invalid canonical metadata status=%d body=%s", response.Code, response.Body.String())
	}
	entries, err := logs.Query(applog.Query{Component: "recording-read-model", Limit: 10})
	if err != nil || len(entries.Items) != 1 || !strings.Contains(entries.Items[0].Message, "category=invalid_metadata") {
		t.Fatalf("canonical projection diagnostic=%+v err=%v", entries.Items, err)
	}
}

func readModelFixture(t *testing.T, id string, state domain.RecordingState) (*storage.Store, *acquire.Manager, *domain.Recording) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, id, state)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store, manager, recording
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

type fixedRecordingManager struct {
	recording  *domain.Recording
	recordings []*domain.Recording
	getCalls   int
}

func (m *fixedRecordingManager) StartResolved(context.Context, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	return nil, errors.New("not implemented")
}

func (m *fixedRecordingManager) StartResolvedWithID(context.Context, string, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	return nil, errors.New("not implemented")
}

func (m *fixedRecordingManager) Get(id string) (*domain.Recording, error) {
	m.getCalls++
	for _, recording := range m.rows() {
		if recording != nil && id == recording.ID {
			return recording, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *fixedRecordingManager) List() []*domain.Recording {
	return m.rows()
}

func (m *fixedRecordingManager) ListForManagement(context.Context, int) ([]*domain.Recording, error) {
	return m.rows(), nil
}

func (m *fixedRecordingManager) rows() []*domain.Recording {
	if len(m.recordings) > 0 {
		return append([]*domain.Recording(nil), m.recordings...)
	}
	if m.recording == nil {
		return nil
	}
	return []*domain.Recording{m.recording}
}

func (m *fixedRecordingManager) LifecycleSnapshot(_ context.Context, id string) (acquire.LifecycleSnapshot, error) {
	recording, err := m.Get(id)
	if err != nil {
		return acquire.LifecycleSnapshot{}, err
	}
	return acquire.LifecycleSnapshot{
		RecordingID: recording.ID, CaptureState: recording.State,
		ArchiveSealed: recording.ArchiveSealed, Repairable: !recording.ArchiveSealed,
		RecoveryState: "idle", TimelineRevision: recording.TimelineRevision,
		ArchiveRevision: recording.ArchiveRevision,
	}, nil
}

func (m *fixedRecordingManager) Stop(string) (*domain.Recording, error) {
	return nil, errors.New("not implemented")
}

func (m *fixedRecordingManager) CompleteRecording(context.Context, string) (*domain.Recording, error) {
	return nil, errors.New("not implemented")
}

func (m *fixedRecordingManager) SealArchiveContext(context.Context, string) error {
	return errors.New("not implemented")
}

func (m *fixedRecordingManager) Delete(string) error { return errors.New("not implemented") }
