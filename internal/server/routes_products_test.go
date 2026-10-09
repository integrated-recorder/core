package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/applog"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/systemsettings"
)

func TestRecordingManagementAPIsUseCanonicalArchive(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("a", 32), domain.StateCompleted)
	recording.ArchiveRevision = 1
	sourceTitle, sourceDescription := "source title sentinel", "plain source description"
	recording.MetadataTimeline = []domain.MetadataRevision{{ObservedAt: time.Date(2026, 9, 28, 14, 0, 0, 0, time.UTC), Title: &sourceTitle, Description: &sourceDescription}}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	integrityService, err := integrity.Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer integrityService.Close(context.Background())
	handler := NewWithOptions(manager, nil, nil, Options{Management: products, Integrity: integrityService})

	query := httptest.NewRecorder()
	handler.ServeHTTP(query, httptest.NewRequest(http.MethodGet, "/api/v2/recordings?q=resource-stable&has_gaps=true&sort=-size&limit=1", nil))
	if query.Code != http.StatusOK {
		t.Fatalf("recording query status=%d body=%s", query.Code, query.Body.String())
	}
	var page struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(query.Body.Bytes(), &page); err != nil || page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("recording query response=%s err=%v", query.Body.String(), err)
	}
	if page.Items[0]["archive_size_bytes"] != nil || page.Items[0]["statistics_status"] != "partial" || page.Items[0]["gap_duration_seconds"] != nil || page.Items[0]["resource_id"] != "resource-stable" {
		t.Fatalf("recording query statistics=%v", page.Items[0])
	}

	tagsPut := httptest.NewRecorder()
	handler.ServeHTTP(tagsPut, httptest.NewRequest(http.MethodPut, "/api/recordings/"+recording.ID+"/tags", strings.NewReader(`{"tags":["Important","concert"]}`)))
	if tagsPut.Code != http.StatusOK || !strings.Contains(tagsPut.Body.String(), `"tags":["concert","important"]`) {
		t.Fatalf("tags put=%d %s", tagsPut.Code, tagsPut.Body.String())
	}
	tagsGet := httptest.NewRecorder()
	handler.ServeHTTP(tagsGet, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/tags", nil))
	if tagsGet.Code != http.StatusOK || strings.Contains(tagsGet.Body.String(), "Important") {
		t.Fatalf("tags get=%d %s", tagsGet.Code, tagsGet.Body.String())
	}

	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/archive/index", nil))
	if index.Code != http.StatusConflict || !strings.Contains(index.Body.String(), `"error_code":"archive_index_unsupported_format"`) || strings.Contains(index.Body.String(), root) {
		t.Fatalf("archive index=%d %s", index.Code, index.Body.String())
	}
	metadata := httptest.NewRecorder()
	handler.ServeHTTP(metadata, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/metadata", nil))
	if metadata.Code != http.StatusOK || !strings.Contains(metadata.Body.String(), sourceTitle) || !strings.Contains(metadata.Body.String(), sourceDescription) || strings.Contains(metadata.Body.String(), "private.invalid") || strings.Contains(metadata.Body.String(), root) || strings.Contains(metadata.Body.String(), "manifest_url") || !strings.Contains(metadata.Body.String(), `"items":[`) {
		t.Fatalf("metadata projection=%d %s", metadata.Code, metadata.Body.String())
	}
	detailResponse := httptest.NewRecorder()
	handler.ServeHTTP(detailResponse, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID, nil))
	if detailResponse.Code != http.StatusOK || strings.Contains(detailResponse.Body.String(), "metadata_timeline") || strings.Contains(detailResponse.Body.String(), sourceTitle) {
		t.Fatalf("regular recording detail leaked metadata history: %d %s", detailResponse.Code, detailResponse.Body.String())
	}
	verify := httptest.NewRecorder()
	handler.ServeHTTP(verify, httptest.NewRequest(http.MethodPost, "/api/recordings/"+recording.ID+"/integrity/verify", nil))
	if verify.Code != http.StatusAccepted {
		t.Fatalf("integrity start=%d %s", verify.Code, verify.Body.String())
	}
	var job integrity.Job
	if err := json.Unmarshal(verify.Body.Bytes(), &job); err != nil || job.ID == "" {
		t.Fatalf("integrity job=%s err=%v", verify.Body.String(), err)
	}
	waitForIntegrityJob(t, handler, job.ID)
	integrityGet := httptest.NewRecorder()
	handler.ServeHTTP(integrityGet, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/integrity", nil))
	if integrityGet.Code != http.StatusOK || !strings.Contains(integrityGet.Body.String(), `"status":"verified"`) {
		t.Fatalf("integrity status=%d %s", integrityGet.Code, integrityGet.Body.String())
	}

	dashboard := httptest.NewRecorder()
	handler.ServeHTTP(dashboard, httptest.NewRequest(http.MethodGet, "/api/dashboard", nil))
	if dashboard.Code != http.StatusOK || !strings.Contains(dashboard.Body.String(), `"recordings_total":1`) || !strings.Contains(dashboard.Body.String(), `"active_recordings_count":0`) || !strings.Contains(dashboard.Body.String(), `"active_recordings":[]`) || !strings.Contains(dashboard.Body.String(), `"integrity":{"degraded":0,"failed":0,"unknown":0,"verified":1,"verifying":0}`) {
		t.Fatalf("dashboard=%d %s", dashboard.Code, dashboard.Body.String())
	}
	storageStats := httptest.NewRecorder()
	handler.ServeHTTP(storageStats, httptest.NewRequest(http.MethodGet, "/api/system/storage", nil))
	if storageStats.Code != http.StatusOK || strings.Contains(storageStats.Body.String(), root) {
		t.Fatalf("storage stats=%d %s", storageStats.Code, storageStats.Body.String())
	}
	search := httptest.NewRecorder()
	handler.ServeHTTP(search, httptest.NewRequest(http.MethodGet, "/api/search?q=resource-stable", nil))
	if search.Code != http.StatusOK || !strings.Contains(search.Body.String(), `"type":"resource"`) {
		t.Fatalf("global search=%d %s", search.Code, search.Body.String())
	}
	if got := products.Audit(10); len(got) != 2 || got[0].Type != "integrity_requested" || got[1].Type != "recording_tags_updated" {
		t.Fatalf("tag audit=%+v", got)
	}
}

func TestStaleIntegrityIsUnknownInReadModelsAndDoesNotNotify(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recordingID := strings.Repeat("c", 32)
	recording := writeProductRecording(t, store, recordingID, domain.StateStopped)
	recording.ArchiveRevision = 7
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}

	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Seed a completed failed result bound to revision 7. This isolates the
	// read-model behavior from verifier scheduling and makes staleness exact.
	verifiedAt := time.Now().UTC().Truncate(time.Second)
	type resultRevision struct {
		Archive  uint64 `json:"archive_revision"`
		Timeline uint64 `json:"timeline_revision"`
		Known    bool   `json:"known"`
	}
	state := struct {
		Version         int                                `json:"version"`
		Jobs            []integrity.Job                    `json:"jobs"`
		Results         map[string]storage.IntegrityResult `json:"results"`
		ResultRevisions map[string]resultRevision          `json:"result_revisions"`
	}{
		Version: 1,
		Jobs:    []integrity.Job{{ID: strings.Repeat("d", 32), RecordingID: recordingID, State: integrity.StateCompleted, CreatedAt: verifiedAt, SourceArchiveRevision: 7, SourceRevisionKnown: true}},
		Results: map[string]storage.IntegrityResult{recordingID: {
			Status: storage.IntegrityFailed, LastVerifiedAt: verifiedAt, ObjectsTotal: 1, ObjectsCorrupt: 1,
			Issues: []storage.IntegrityIssue{{Code: "payload_mismatch", Path: "tracks/main/00000001.ts"}},
		}},
		ResultRevisions: map[string]resultRevision{recordingID: {Archive: 7, Known: true}},
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	jobDir := filepath.Join(root, "management", "integrity-jobs")
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "state.json"), stateBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	integrityService, err := integrity.Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer integrityService.Close(context.Background())
	handler := NewWithOptions(manager, nil, nil, Options{Management: products, Integrity: integrityService, Storage: store})
	verified, ok := integrityService.StatusFor(recording)
	if !ok || verified.Status != storage.IntegrityFailed || verified.Freshness != integrity.FreshnessCurrent || verified.LastVerifiedAt.IsZero() {
		t.Fatalf("fixture integrity result=%+v present=%v, want current failed result", verified, ok)
	}

	current, err := store.LoadRecordingReadOnly(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	current.ArchiveRevision = 8
	if err := store.SaveRecording(current); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager, err = acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	handler = NewWithOptions(manager, nil, nil, Options{Management: products, Integrity: integrityService, Storage: store})
	currentSnapshot, err := manager.Get(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	stale, ok := integrityService.StatusFor(currentSnapshot)
	if !ok || stale.Status != storage.IntegrityFailed || stale.Freshness != integrity.FreshnessStale {
		t.Fatalf("fixture stale projection=%+v present=%v, want stale failed result", stale, ok)
	}

	detail := httptest.NewRecorder()
	handler.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recordingID, nil))
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), `"state":"stopped"`) || !strings.Contains(detail.Body.String(), `"integrity":"unknown"`) {
		t.Fatalf("detail status=%d body=%s, want stopped recording and unknown integrity", detail.Code, detail.Body.String())
	}

	dashboard := httptest.NewRecorder()
	handler.ServeHTTP(dashboard, httptest.NewRequest(http.MethodGet, "/api/dashboard", nil))
	if dashboard.Code != http.StatusOK || !strings.Contains(dashboard.Body.String(), `"integrity":{"degraded":0,"failed":0,"unknown":1,"verified":0,"verifying":0}`) {
		t.Fatalf("dashboard status=%d body=%s, want stale failure counted as unknown", dashboard.Code, dashboard.Body.String())
	}

	events := httptest.NewRecorder()
	handler.ServeHTTP(events, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recordingID+"/events", nil))
	if events.Code != http.StatusOK || !strings.Contains(events.Body.String(), `"type":"integrity_completed"`) {
		t.Fatalf("events status=%d body=%s, want historical completion event retained", events.Code, events.Body.String())
	}

	sync := httptest.NewRecorder()
	handler.ServeHTTP(sync, httptest.NewRequest(http.MethodPost, "/api/notifications/sync", strings.NewReader(`{}`)))
	if sync.Code != http.StatusNoContent {
		t.Fatalf("notification sync status=%d body=%s", sync.Code, sync.Body.String())
	}
	for _, notification := range products.Notifications(false, 100) {
		if notification.Type == "integrity_failure" {
			t.Fatalf("stale failed result generated current integrity notification: %+v", notification)
		}
	}
}

func TestRecordingMetadataAPIUsesEmptyArrayForLegacyArchive(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	legacy := writeProductRecording(t, store, strings.Repeat("b", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := New(manager, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+legacy.ID+"/metadata", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"items":[]`) || strings.Contains(response.Body.String(), `"items":null`) {
		t.Fatalf("legacy recording metadata response=%d %s, want an empty array", response.Code, response.Body.String())
	}
}

func TestGlobalSearchRejectsRepeatedAndUnknownParameters(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := New(manager, nil, nil)

	for _, target := range []string{
		"/api/search?q=alpha&q=beta",
		"/api/search?q=alpha&limit=1&limit=2",
		"/api/search?q=alpha&unexpected=true",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != http.StatusBadRequest {
			t.Errorf("GET %s status=%d body=%s, want 400", target, response.Code, response.Body.String())
		}
	}
}

func TestTagLookupFailureMarksRecordingQueriesAndSearchPartial(t *testing.T) {
	_, fixtureManager, recording := readModelFixture(t, strings.Repeat("8", 32), domain.StateCompleted)
	t.Cleanup(func() {
		if err := fixtureManager.Close(context.Background()); err != nil {
			t.Errorf("close fixture manager: %v", err)
		}
	})
	// Keep canonical fields usable while making the management tag lookup fail.
	// A hidden tag could have matched these filters, so the API must mark its
	// results/totals partial instead of presenting the visible subset as exact.
	recording.ID = "invalid/id"
	recording.Title = "visible title"
	manager := &fixedRecordingManager{recording: recording}
	products, err := management.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	logs := applog.NewStore()
	handler := NewWithOptions(manager, nil, nil, Options{Management: products, Logs: logs})

	for _, target := range []string{
		"/api/v2/recordings?q=hidden-tag-match",
		"/api/v2/recordings?tag=hidden-tag-match",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, target, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", target, response.Code, response.Body.String())
		}
		var page struct {
			Items          []map[string]any `json:"items"`
			Total          int              `json:"total"`
			PartialErrors  []string         `json:"partial_errors"`
			TotalIsPartial bool             `json:"total_is_partial"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode GET %s: %v", target, err)
		}
		if response.Code != http.StatusOK || page.Total != 0 || len(page.Items) != 0 ||
			!page.TotalIsPartial || len(page.PartialErrors) != 1 || page.PartialErrors[0] != "tags" {
			t.Fatalf("GET %s did not identify an incomplete tag-filter result: %s", target, response.Body.String())
		}
	}

	// A result matched by canonical text remains visible, with the per-item tag
	// availability signal retained alongside the query-level partial marker.
	visible := httptest.NewRecorder()
	handler.ServeHTTP(visible, httptest.NewRequest(http.MethodGet, "/api/v2/recordings?q=visible", nil))
	var visiblePage struct {
		Items []struct {
			UnavailableFields []string `json:"unavailable_fields"`
		} `json:"items"`
		PartialErrors  []string `json:"partial_errors"`
		TotalIsPartial bool     `json:"total_is_partial"`
	}
	if visible.Code != http.StatusOK || json.Unmarshal(visible.Body.Bytes(), &visiblePage) != nil ||
		len(visiblePage.Items) != 1 || !containsString(visiblePage.Items[0].UnavailableFields, "tags") ||
		!visiblePage.TotalIsPartial || len(visiblePage.PartialErrors) != 1 || visiblePage.PartialErrors[0] != "tags" {
		t.Fatalf("visible recording lost per-item/query partial state: status=%d body=%s", visible.Code, visible.Body.String())
	}

	search := httptest.NewRecorder()
	handler.ServeHTTP(search, httptest.NewRequest(http.MethodGet, "/api/search?q=hidden-tag-match", nil))
	var searchResult struct {
		Results       []map[string]any `json:"results"`
		PartialErrors []string         `json:"partial_errors"`
	}
	if search.Code != http.StatusOK || json.Unmarshal(search.Body.Bytes(), &searchResult) != nil ||
		len(searchResult.Results) != 0 || len(searchResult.PartialErrors) != 1 || searchResult.PartialErrors[0] != "tags" {
		t.Fatalf("global search did not return a partial marker: status=%d body=%s", search.Code, search.Body.String())
	}

	entries, err := logs.Query(applog.Query{Component: "recording-read-model", Limit: 10})
	if err != nil || len(entries.Items) == 0 {
		t.Fatalf("tag failure diagnostic missing: entries=%+v err=%v", entries.Items, err)
	}
	for _, entry := range entries.Items {
		if !strings.Contains(entry.Message, "component=tags") || !strings.Contains(entry.Message, "category=management_unavailable") ||
			strings.Contains(entry.Message, "invalid/id") {
			t.Fatalf("tag failure diagnostic unsafe or incomplete: %+v", entry)
		}
	}
}

func TestGlobalSearchCancellationReturnsNoPartialResults(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	writeProductRecording(t, store, strings.Repeat("c", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := New(manager, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/search?q=resource-stable", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Body.Len() != 0 {
		t.Fatalf("canceled search returned partial response: status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestWorkflowLifecycleEventsPersistAndAcknowledgeOnlyAfterWrite(t *testing.T) {
	root := t.TempDir()
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	// Force the first projection write to fail. The host event must remain
	// queued and keep its ID until a later request can persist it.
	historyPath := filepath.Join(root, "management", "workflow-history.json")
	if err := os.Mkdir(historyPath, 0700); err != nil {
		t.Fatal(err)
	}
	adapterDir := t.TempDir()
	writeWorkflowServerTestAdapter(t, adapterDir)
	host, err := adapterhost.Discover(context.Background(), adapterDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	progress, err := host.BeginResolution(context.Background(), "workflow-test", json.RawMessage(`{"opaque_input":"resource-one"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, host, nil, Options{Management: products})
	request := httptest.NewRequest(http.MethodDelete, "/api/resolve-workflows/"+progress.WorkflowID, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("cancel status=%d body=%s", response.Code, response.Body.String())
	}
	pending := host.PendingWorkflowLifecycleEvents()
	if len(pending) != 1 || pending[0].State != "canceled" {
		t.Fatalf("failed history write did not retain canceled event: %#v", pending)
	}
	eventID := pending[0].ID
	if err := os.Remove(historyPath); err != nil {
		t.Fatal(err)
	}
	// The middleware flushes after any completed request, not only the
	// cancellation route that generated the event.
	flush := httptest.NewRecorder()
	handler.ServeHTTP(flush, httptest.NewRequest(http.MethodGet, "/api/workflow-history", nil))
	if flush.Code != http.StatusOK {
		t.Fatalf("flush request status=%d body=%s", flush.Code, flush.Body.String())
	}
	if events := host.PendingWorkflowLifecycleEvents(); len(events) != 0 {
		t.Fatalf("persisted event was not acknowledged: %#v", events)
	}
	history := products.WorkflowHistory(progress.WorkflowID, "", "", 10)
	if len(history) != 1 || history[0].ID != eventID || history[0].State != "canceled" {
		t.Fatalf("persisted workflow lifecycle history=%#v want event id %q", history, eventID)
	}
	if history[0].Challenge == nil || history[0].Challenge.FieldCount != 2 || !history[0].Challenge.HasSecretFields {
		t.Fatalf("persisted challenge summary=%#v", history[0].Challenge)
	}
	encoded, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"credential", "required answer", "https://127.0.0.1"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("workflow history leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestAdapterWorkflowErrorIsFailedGatewayResponseNotPendingChallenge(t *testing.T) {
	root := t.TempDir()
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	adapterDir := t.TempDir()
	writeFailedWorkflowServerTestAdapter(t, adapterDir)
	host, err := adapterhost.Discover(context.Background(), adapterDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	handler := NewWithOptions(nil, host, nil, Options{Management: products})
	body := `{"adapter_id":"history-error","input":{"manifest_url":"https://media.example/live.m3u8"}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/recordings", strings.NewReader(body)))
	if response.Code != http.StatusBadGateway || strings.Contains(response.Body.String(), "202") {
		t.Fatalf("adapter error status=%d body=%s", response.Code, response.Body.String())
	}
	if workflows := host.ListWorkflows(); len(workflows) != 0 {
		t.Fatalf("adapter error created a pending challenge: %#v", workflows)
	}
	history := products.WorkflowHistory("", "history-error", "failed", 10)
	if len(history) != 1 || history[0].WorkflowID == "" || history[0].Challenge == nil {
		t.Fatalf("failed workflow history=%#v", history)
	}
	if events := host.PendingWorkflowLifecycleEvents(); len(events) != 0 {
		t.Fatalf("persisted failed event was not acknowledged: %#v", events)
	}
}

func writeFailedWorkflowServerTestAdapter(t *testing.T, dir string) {
	t.Helper()
	script := `#!/bin/sh
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":"\([^\"]*\)".*/\1/p')
  method=$(printf '%s\n' "$line" | sed -n 's/.*"method":"\([^\"]*\)".*/\1/p')
  case "$method" in
    describe)
      printf '%s\n' '{"protocol_version":1,"id":"'"$request_id"'","result":{"id":"history-error","name":"History Error","version":"1","protocol_version":1,"capabilities":["resolve_workflow"],"input_schema":{"fields":[{"key":"manifest_url","control":"text","label":"Manifest URL","required":true}]},"configuration_schema":{"fields":[]},"resource_types":[],"media_types":["hls"]}}'
      ;;
    resolve.begin)
      workflow_id=$(printf '%s\n' "$line" | sed -n 's/.*"workflow_id":"\([^\"]*\)".*/\1/p')
      printf '%s\n' '{"protocol_version":1,"id":"'"$request_id"'","result":{"state":"error","workflow_id":"'"$workflow_id"'","challenge":{"schema":{"fields":[{"key":"secret","control":"secret","label":"Secret"},{"key":"ordinary","control":"text","label":"Ordinary","default":"error-default-sentinel"}]},"prompt":{"type":"secret_prompt","interaction_id":"error-prompt","title":"error-title-sentinel","message":"error-message-sentinel","fields":[{"key":"secret","control":"secret","label":"error-field-sentinel"}],"data":{"answer":"error-data-sentinel"}}}}}'
      ;;
    shutdown)
      printf '%s\n' '{"protocol_version":1,"id":"'"$request_id"'","result":{"stopped":true}}'
      ;;
    *)
      printf '%s\n' '{"protocol_version":1,"id":"'"$request_id"'","error":{"code":"unsupported_method","message":"unsupported"}}'
      ;;
  esac
done
`
	if err := os.WriteFile(filepath.Join(dir, "integrated-recorder-adapter-history-error"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestRecordingDeleteRejectsActiveAndDeletesWholeArchiveIdempotently(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	completed := writeProductRecording(t, store, strings.Repeat("b", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := products.SetTags(completed.ID, []string{"remove-me"}); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{Management: products})

	activeStore, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	requestStarted := make(chan struct{})
	var requestStartedOnce sync.Once
	activeClient := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		requestStartedOnce.Do(func() { close(requestStarted) })
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	activeManager, err := acquire.NewManager(activeStore, activeClient, nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	active, err := activeManager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://stream.invalid/live.m3u8"}, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("active acquisition did not issue its manifest request")
	}
	activeHandler := NewWithOptions(activeManager, nil, nil, Options{})
	activeDelete := httptest.NewRecorder()
	activeHandler.ServeHTTP(activeDelete, httptest.NewRequest(http.MethodDelete, "/api/recordings/"+active.ID, nil))
	if activeDelete.Code != http.StatusConflict {
		t.Fatalf("active delete status=%d body=%s", activeDelete.Code, activeDelete.Body.String())
	}
	if _, err := activeManager.Stop(active.ID); err != nil {
		t.Fatalf("stop active fixture: %v", err)
	}
	delete := httptest.NewRecorder()
	handler.ServeHTTP(delete, httptest.NewRequest(http.MethodDelete, "/api/recordings/"+completed.ID, nil))
	if delete.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", delete.Code, delete.Body.String())
	}
	if _, err := manager.Get(completed.ID); err == nil {
		t.Fatal("deleted recording remains in manager")
	}
	if _, err := products.Tags(completed.ID); err != nil {
		t.Fatal(err)
	} else if tags, _ := products.Tags(completed.ID); len(tags) != 0 {
		t.Fatalf("deleted recording tags remain: %v", tags)
	}
	if _, err := store.LoadAll(); err != nil {
		t.Fatalf("remaining archive cannot reload: %v", err)
	}
	again := httptest.NewRecorder()
	handler.ServeHTTP(again, httptest.NewRequest(http.MethodDelete, "/api/recordings/"+completed.ID, nil))
	if again.Code != http.StatusNoContent {
		t.Fatalf("idempotent delete=%d %s", again.Code, again.Body.String())
	}
}

func TestRecordingDeleteAuditsBeforeManagementCleanupFailure(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("c", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := products.SetTags(recording.ID, []string{"cleanup-failure"}); err != nil {
		t.Fatal(err)
	}
	projectionPath := filepath.Join(root, "management", "recordings", recording.ID+".json")
	if err := os.Remove(projectionPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(projectionPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projectionPath, "unsafe-entry"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{Management: products})
	principal := authn.Principal{UserID: "usr-0123456789abcdef0123456789abcdef", Login: "owner", Role: authn.RoleOwner}
	request := httptest.NewRequest(http.MethodDelete, "/api/recordings/"+recording.ID, nil)
	request = request.WithContext(authn.WithPrincipal(request.Context(), principal))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || response.Header().Get("X-Audit-Status") != "recorded" || !strings.Contains(response.Body.String(), "recording was deleted but management metadata cleanup is pending") {
		t.Fatalf("delete cleanup failure response=%d audit=%q body=%s", response.Code, response.Header().Get("X-Audit-Status"), response.Body.String())
	}
	if _, err := manager.Get(recording.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("canonical recording survived deletion: %v", err)
	}
	events := products.Audit(10)
	if len(events) != 1 || events[0].Type != "recording_deleted" || events[0].ObjectID != recording.ID || events[0].Actor == nil || events[0].Actor.Type != management.AuditActorUser || events[0].Actor.UserID != principal.UserID {
		t.Fatalf("canonical deletion audit=%+v", events)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestAuthMiddlewareProtectsProductMutationsAndRequiresCSRF(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapBytes, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	bootstrapToken := strings.TrimSpace(string(bootstrapBytes))
	if err := auth.Bootstrap(bootstrapToken, "correct horse battery staple"); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{Management: products, Auth: auth})
	spa := httptest.NewRecorder()
	spaRequest := httptest.NewRequest(http.MethodGet, "/recordings/example-id", nil)
	spaRequest.Header.Set("Accept", "text/html")
	handler.ServeHTTP(spa, spaRequest)
	if spa.Code != http.StatusOK || !strings.Contains(spa.Body.String(), `id="root"`) {
		t.Fatalf("unauthenticated browser deep link should receive the login-capable SPA shell: %d", spa.Code)
	}

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/dashboard", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated dashboard=%d %s", unauthorized.Code, unauthorized.Body.String())
	}
	login := httptest.NewRecorder()
	loginRequest := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"password":"correct horse battery staple"}`))
	handler.ServeHTTP(login, loginRequest)
	if login.Code != http.StatusOK || login.Result().Cookies() == nil {
		t.Fatalf("login=%d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	loggedIn := httptest.NewRequest(http.MethodGet, "/api/dashboard", nil)
	for _, cookie := range cookies {
		loggedIn.AddCookie(cookie)
	}
	protected := httptest.NewRecorder()
	handler.ServeHTTP(protected, loggedIn)
	if protected.Code != http.StatusOK {
		t.Fatalf("authenticated dashboard=%d %s", protected.Code, protected.Body.String())
	}
	withoutCSRF := httptest.NewRecorder()
	badMutation := httptest.NewRequest(http.MethodPost, "/api/notifications/read-all", strings.NewReader(`{}`))
	for _, cookie := range cookies {
		badMutation.AddCookie(cookie)
	}
	handler.ServeHTTP(withoutCSRF, badMutation)
	if withoutCSRF.Code != http.StatusForbidden {
		t.Fatalf("mutation without CSRF=%d %s", withoutCSRF.Code, withoutCSRF.Body.String())
	}
	var session map[string]any
	sessionRequest := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	for _, cookie := range cookies {
		sessionRequest.AddCookie(cookie)
	}
	sessionRecorder := httptest.NewRecorder()
	handler.ServeHTTP(sessionRecorder, sessionRequest)
	if err := json.Unmarshal(sessionRecorder.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	csrf, _ := session["csrf_token"].(string)
	mutation := httptest.NewRequest(http.MethodPost, "/api/notifications/read-all", strings.NewReader(`{}`))
	for _, cookie := range cookies {
		mutation.AddCookie(cookie)
	}
	mutation.Header.Set("X-CSRF-Token", csrf)
	mutationRecorder := httptest.NewRecorder()
	handler.ServeHTTP(mutationRecorder, mutation)
	if mutationRecorder.Code != http.StatusNoContent {
		t.Fatalf("CSRF-protected mutation=%d %s", mutationRecorder.Code, mutationRecorder.Body.String())
	}
}

func TestSystemSettingsAPIReportsRestartRequiredAndValidatesStrictly(t *testing.T) {
	root := t.TempDir()
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Settings: settings, InitialIntegrityConcurrency: settings.IntegrityConcurrency()})

	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"theme":"system"`) || !strings.Contains(get.Body.String(), `"concurrency":2`) {
		t.Fatalf("settings get=%d %s", get.Code, get.Body.String())
	}
	if !strings.Contains(get.Body.String(), `"effective_storage"`) || !strings.Contains(get.Body.String(), `"global_buffer_bytes":1073741824`) {
		t.Fatalf("settings get did not return effective storage defaults: %s", get.Body.String())
	}

	update := httptest.NewRecorder()
	handler.ServeHTTP(update, httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(`{"ui":{"theme":"dark"},"integrity":{"concurrency":3}}`)))
	if update.Code != http.StatusOK || !strings.Contains(update.Body.String(), `"restart_required":["integrity.concurrency"]`) {
		t.Fatalf("settings update=%d %s", update.Code, update.Body.String())
	}
	if current := settings.Current(); current.UI.Theme != "dark" || current.Integrity.Concurrency != 3 {
		t.Fatalf("settings were not persisted: %+v", current)
	}

	storageSettings := settings.Current().Storage
	storageSettings.Observability.SamplingIntervalMS = 10000
	storageSettings.FailureHandling.PersistAttempts = 4
	storageBody, err := json.Marshal(map[string]any{"storage": storageSettings})
	if err != nil {
		t.Fatal(err)
	}
	storageUpdate := httptest.NewRecorder()
	handler.ServeHTTP(storageUpdate, httptest.NewRequest(http.MethodPut, "/api/settings", bytes.NewReader(storageBody)))
	wantRestart := `"restart_required":["integrity.concurrency","storage.failure_handling.persist_attempts","storage.observability.sampling_interval_ms"]`
	if storageUpdate.Code != http.StatusOK || !strings.Contains(storageUpdate.Body.String(), wantRestart) || !strings.Contains(storageUpdate.Body.String(), `"sampling_interval_ms":10000`) || !strings.Contains(storageUpdate.Body.String(), `"persist_attempts":4`) || strings.Contains(storageUpdate.Body.String(), `"retry_attempts"`) {
		t.Fatalf("storage settings update=%d %s", storageUpdate.Code, storageUpdate.Body.String())
	}

	// Returning a changed value to the startup snapshot clears its individual
	// restart marker while unrelated integrity restart state remains.
	storageSettings = settings.Current().Storage
	storageSettings.Observability.SamplingIntervalMS = 5000
	storageSettings.FailureHandling.PersistAttempts = 5
	storageBody, _ = json.Marshal(map[string]any{"storage": storageSettings})
	revert := httptest.NewRecorder()
	handler.ServeHTTP(revert, httptest.NewRequest(http.MethodPut, "/api/settings", bytes.NewReader(storageBody)))
	if revert.Code != http.StatusOK || strings.Contains(revert.Body.String(), "storage.observability.sampling_interval_ms") {
		t.Fatalf("reverted storage setting remained restart-required: %d %s", revert.Code, revert.Body.String())
	}

	invalidStorage := settings.Current().Storage
	invalidStorage.QueueWriter.WriterConcurrency = 2
	invalidBody, _ := json.Marshal(map[string]any{"storage": invalidStorage})
	invalidStorageResponse := httptest.NewRecorder()
	handler.ServeHTTP(invalidStorageResponse, httptest.NewRequest(http.MethodPut, "/api/settings", bytes.NewReader(invalidBody)))
	if invalidStorageResponse.Code != http.StatusBadRequest || !strings.Contains(invalidStorageResponse.Body.String(), "writer concurrency must be 1") || strings.Contains(invalidStorageResponse.Body.String(), `"writer_concurrency":2`) {
		t.Fatalf("invalid storage settings response=%d %s", invalidStorageResponse.Code, invalidStorageResponse.Body.String())
	}

	belowReachableRetention := settings.Current().Storage
	belowReachableRetention.Observability.SamplingIntervalMS = 60_000
	belowReachableRetention.Observability.MetricsRetentionMS = 300_000
	belowReachableBody, _ := json.Marshal(map[string]any{"storage": belowReachableRetention})
	beforeInvalidObservability := settings.Current().Storage
	belowReachableResponse := httptest.NewRecorder()
	handler.ServeHTTP(belowReachableResponse, httptest.NewRequest(http.MethodPut, "/api/settings", bytes.NewReader(belowReachableBody)))
	if belowReachableResponse.Code != http.StatusBadRequest || !strings.Contains(belowReachableResponse.Body.String(), "at least 20 minutes") {
		t.Fatalf("unreachable metrics retention response=%d %s", belowReachableResponse.Code, belowReachableResponse.Body.String())
	}
	if got := settings.Current().Storage; got != beforeInvalidObservability {
		t.Fatal("invalid observability settings changed stored state")
	}

	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(`{"ui":{"theme":"dark"},"unknown":true}`)))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("unknown settings field status=%d body=%s", invalid.Code, invalid.Body.String())
	}
}

func TestUnavailableExportIsAdvertisedAndNotImplemented(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{BuildInfo: buildinfo.Info{
		Version: "1.2.3", Commit: strings.Repeat("a", 40), BuildTime: "2026-09-29T00:00:00Z",
		ReleaseChannel: "stable", RuntimeProtocolVersion: buildinfo.RuntimeProtocolVersion,
	}})
	info := httptest.NewRecorder()
	handler.ServeHTTP(info, httptest.NewRequest(http.MethodGet, "/api/system/info", nil))
	if info.Code != http.StatusOK || !strings.Contains(info.Body.String(), `"export_available":false`) ||
		!strings.Contains(info.Body.String(), `"build_time":"2026-09-29T00:00:00Z"`) ||
		!strings.Contains(info.Body.String(), `"release_channel":"stable"`) ||
		!strings.Contains(info.Body.String(), `"runtime_protocol_version":1`) {
		t.Fatalf("system info=%d %s", info.Code, info.Body.String())
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/recordings/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/exports", strings.NewReader(`{"format":"mkv"}`)))
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("unavailable export status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestThumbnailRoutesReportUnavailableAndMissingProjection(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PATH", t.TempDir())
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("c", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	derivatives, err := derivative.Open(root, store, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer derivatives.Close(context.Background())
	handler := NewWithOptions(manager, nil, nil, Options{Derivatives: derivatives})

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/thumbnail", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing thumbnail status=%d body=%s", missing.Code, missing.Body.String())
	}

	unavailable := httptest.NewRecorder()
	handler.ServeHTTP(unavailable, httptest.NewRequest(http.MethodPost, "/api/recordings/"+recording.ID+"/thumbnail/regenerate", nil))
	if unavailable.Code != http.StatusNotImplemented {
		t.Fatalf("thumbnail regeneration status=%d body=%s", unavailable.Code, unavailable.Body.String())
	}
}

func writeProductRecording(t *testing.T, store *storage.Store, id string, state domain.RecordingState) *domain.Recording {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{
		FormatVersion: 1, ID: id, Title: "Fixture", AdapterID: "fixture",
		Adapter:                 &domain.AdapterProvenance{ID: "fixture", Name: "Fixture adapter", Version: "1.0", ProtocolVersion: 1},
		Resource:                &domain.ResourceReference{Type: "opaque.alpha", ID: "resource-stable"},
		SourceURIClassification: "sensitive", State: state, CreatedAt: now.Add(-time.Minute), StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {ID: "main", SourcePlaylistURL: "https://private.invalid/playlist.m3u8?sig=hidden", Segments: []domain.Segment{}}},
		Gaps:   []domain.Gap{{TrackID: "main", SourceEpoch: 0, FromSequence: 2, ToSequence: 3, DetectedAt: now.Add(time.Second), Reason: "fixture"}},
	}
	if state != domain.StateRecording {
		stopped := now.Add(4 * time.Second)
		recording.StoppedAt = &stopped
	}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	payload := []byte("original-media-payload")
	result, err := store.SavePayload(id, "tracks/main/00000001.ts", bytes.NewReader(payload), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{ID: "segment-1", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1, SourceURI: "https://private.invalid/segment.ts?sig=hidden", Duration: 4, StoragePath: "tracks/main/00000001.ts", PayloadSize: result.Size, SHA256: result.SHA256}
	if err = store.SaveSidecar(id, segment.StoragePath, segment); err != nil {
		t.Fatal(err)
	}
	initBytes := []byte("original-init-payload")
	initResult, err := store.SavePayload(id, "tracks/main/init.mp4", bytes.NewReader(initBytes), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	init := domain.Segment{ID: "init-1", TrackID: "main", SourceURI: "https://private.invalid/init.mp4?sig=hidden", StoragePath: "tracks/main/init.mp4", PayloadSize: initResult.Size, SHA256: initResult.SHA256, IsInit: true}
	if err = store.SaveSidecar(id, init.StoragePath, init); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.SaveSnapshot(id, "main", "https://private.invalid/index.m3u8?sig=hidden", []byte("#EXTM3U\n"), now)
	if err != nil {
		t.Fatal(err)
	}
	segment.InitSegmentID = init.ID
	recording.Tracks["main"].Segments = []domain.Segment{segment}
	recording.Tracks["main"].InitSegments = []domain.Segment{init}
	recording.Snapshots = []domain.ManifestSnapshot{manifest}
	recording.State = state
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	return recording
}

func waitForIntegrityJob(t *testing.T, handler http.Handler, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/integrity/jobs/"+id, nil))
		if response.Code == http.StatusOK && strings.Contains(response.Body.String(), `"state":"completed"`) {
			return
		}
		if response.Code != http.StatusOK {
			t.Fatalf("integrity job status=%d %s", response.Code, response.Body.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("integrity job did not complete")
}

func TestDeletePathInputCannotTraverse(t *testing.T) {
	// ServeMux wildcard values are decoded path components. A traversal attempt
	// must be rejected as a missing or invalid recording ID, never used as a path.
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := New(manager, nil, nil)
	request := httptest.NewRequest(http.MethodDelete, "/api/recordings/..%2f..%2fetc%2fpasswd", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code == http.StatusNoContent || response.Code == http.StatusInternalServerError {
		t.Fatalf("path traversal response=%d %s", response.Code, response.Body.String())
	}
}
