package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/recordquery"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

type pagedSummaryEngine struct {
	mu                  sync.Mutex
	items               []*domain.Recording
	listCalls           int
	maxPageLimit        int
	maxPageJSONBytes    int
	legacyListCalls     int
	fullGetCalls        int
	invalidateCallCount int
}

func (e *pagedSummaryEngine) Handle(ctx context.Context, operation string, payload json.RawMessage) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch operation {
	case recorderengine.OperationInventory:
		return recorderengine.InventoryResult{GenerationID: "fixture-generation", InstanceID: "fixture-engine", Ready: true, Active: []recorderengine.ActiveRecording{}}, nil
	case recorderengine.OperationInvalidateManagementRootCache:
		e.mu.Lock()
		e.invalidateCallCount++
		e.mu.Unlock()
		return struct {
			Invalidated bool `json:"invalidated"`
		}{Invalidated: true}, nil
	case recorderengine.OperationList:
		e.mu.Lock()
		e.legacyListCalls++
		e.mu.Unlock()
		return nil, fmt.Errorf("legacy unpaged list must not be called")
	case recorderengine.OperationGet:
		e.mu.Lock()
		e.fullGetCalls++
		e.mu.Unlock()
		return nil, fmt.Errorf("full recording Get must not be called")
	case recorderengine.OperationListPage:
		var request recorderengine.ListPageRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			return nil, err
		}
		if request.Limit < 1 || request.Limit > recorderengine.MaximumListPageLimit {
			return nil, fmt.Errorf("page limit %d outside bound", request.Limit)
		}
		start := sort.Search(len(e.items), func(index int) bool { return e.items[index].ID > request.AfterID })
		end := start + request.Limit
		if end > len(e.items) {
			end = len(e.items)
		}
		items := append([]*domain.Recording(nil), e.items[start:end]...)
		next := ""
		if end < len(e.items) && len(items) > 0 {
			next = items[len(items)-1].ID
		}
		result := recorderengine.ListPageResult{Items: items, NextCursor: next}
		encoded, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		e.mu.Lock()
		e.listCalls++
		if request.Limit > e.maxPageLimit {
			e.maxPageLimit = request.Limit
		}
		if len(encoded) > e.maxPageJSONBytes {
			e.maxPageJSONBytes = len(encoded)
		}
		e.mu.Unlock()
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported fixture operation %q", operation)
	}
}

func TestManagementSummaryPagingCrossesTenThousandOverEngineIPC(t *testing.T) {
	const recordingCount = 10_025
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	items := make([]*domain.Recording, 0, recordingCount)
	for index := 0; index < recordingCount; index++ {
		title := "bounded fixture"
		if index == recordingCount-1 {
			title = "needle beyond boundary"
		}
		items = append(items, v2ManagementFixture(
			fmt.Sprintf("%032x", index+1), base.Add(time.Duration(index)*time.Second), uint64(index+1),
		))
		items[index].Title = title
	}
	engine := &pagedSummaryEngine{items: items}
	token := []byte("0123456789abcdef0123456789abcdef")
	tempRoot, err := os.MkdirTemp("/tmp", "ir-page-ipc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempRoot) })
	socket := filepath.Join(tempRoot, "engine.sock")
	ipcServer, err := runtimeipc.NewServer(socket, "fixture-generation", token, engine)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stopServer := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- ipcServer.Serve(serveCtx) }()
	t.Cleanup(func() {
		stopServer()
		if err := <-serveDone; err != nil {
			t.Errorf("Engine IPC server: %v", err)
		}
	})
	client, err := runtimeipc.NewClient(socket, "fixture-generation", token, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	managerClient, err := recorderengine.NewManagerClient(client)
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	router, err := recorderengine.NewManagerRouter("fixture-generation", map[string]*recorderengine.ManagerClient{
		"fixture-generation": managerClient,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(router, nil, nil, Options{Storage: store})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v2/recordings?q=needle&sort=-created_at&limit=20", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("filtered list status=%d body=%s", response.Code, response.Body.String())
	}
	var filtered recordquery.Result
	if err := json.Unmarshal(response.Body.Bytes(), &filtered); err != nil {
		t.Fatal(err)
	}
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].ID != fmt.Sprintf("%032x", recordingCount) {
		t.Fatalf("filtered item beyond 10k boundary = %#v", filtered)
	}

	all, err := listManagementSummaries(context.Background(), router)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != recordingCount {
		t.Fatalf("paged summary count=%d, want %d", len(all), recordingCount)
	}
	query, err := recordquery.ParseQuery(url.Values{"sort": {"-created_at"}, "limit": {"100"}})
	if err != nil {
		t.Fatal(err)
	}
	prepared := make([]recordquery.Item, 0, len(all))
	server := handler
	for _, recording := range all {
		item, err := server.recordingQueryBaseItem(context.Background(), recording, false)
		if err != nil {
			t.Fatal(err)
		}
		prepared = append(prepared, item)
	}
	seen := make(map[string]struct{}, recordingCount)
	position := 0
	for {
		page, err := recordquery.Page(prepared, query)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != recordingCount {
			t.Fatalf("cursor page total=%d, want %d", page.Total, recordingCount)
		}
		for _, item := range page.Items {
			wantID := fmt.Sprintf("%032x", recordingCount-position)
			if item.ID != wantID {
				t.Fatalf("cursor item %d=%q, want %q", position, item.ID, wantID)
			}
			if _, duplicate := seen[item.ID]; duplicate {
				t.Fatalf("cursor traversal duplicated %q", item.ID)
			}
			seen[item.ID] = struct{}{}
			position++
		}
		if page.NextCursor == "" {
			break
		}
		query.Cursor = page.NextCursor
	}
	if position != recordingCount || len(seen) != recordingCount {
		t.Fatalf("cursor traversal visited %d unique summaries, want %d", position, recordingCount)
	}

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.maxPageLimit > recorderengine.MaximumListPageLimit {
		t.Fatalf("IPC page limit=%d, maximum=%d", engine.maxPageLimit, recorderengine.MaximumListPageLimit)
	}
	if engine.maxPageJSONBytes > runtimeipc.MaxFrameBytes/2 {
		t.Fatalf("IPC page payload=%d bytes, response payload bound=%d", engine.maxPageJSONBytes, runtimeipc.MaxFrameBytes/2)
	}
	if engine.legacyListCalls != 0 || engine.fullGetCalls != 0 {
		t.Fatalf("unpaged/full paths called: list=%d get=%d", engine.legacyListCalls, engine.fullGetCalls)
	}
	if engine.listCalls < 2*(recordingCount/recorderengine.MaximumListPageLimit) {
		t.Fatalf("only %d bounded IPC pages served", engine.listCalls)
	}
}
