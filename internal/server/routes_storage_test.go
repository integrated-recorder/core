package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestStoragePoolAPIProjectsOnlyBoundedRecorderMetrics(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultIngestOptions()
	options.SampleInterval = 7 * time.Second
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := New(manager, nil, nil)

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/storage/pools", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("pool list status=%d body=%s", list.Code, list.Body.String())
	}
	var pools storagePoolsResponse
	if err := json.Unmarshal(list.Body.Bytes(), &pools); err != nil {
		t.Fatal(err)
	}
	if len(pools.Items) != 1 || pools.Items[0].ID != storage.PoolIDLocalPrimary {
		t.Fatalf("pool list=%+v", pools.Items)
	}
	pool := pools.Items[0]
	if pool.Writers.Limit != 1 || pool.Buffer.CapacityBytes <= 0 || pool.Capacity.TotalBytes == 0 || !pool.CapacityKnown {
		t.Fatalf("pool omits actual capacity or writer/buffer state: %+v", pool)
	}
	if strings.Contains(list.Body.String(), root) || strings.Contains(list.Body.String(), "recordings/") {
		t.Fatalf("pool response exposed a physical archive location: %s", list.Body.String())
	}
	if !strings.Contains(list.Body.String(), `"read_bytes_per_second"`) || !strings.Contains(list.Body.String(), `"write_bytes_per_second"`) {
		t.Fatalf("pool response must name byte-rate units explicitly: %s", list.Body.String())
	}

	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/api/storage/pools/local-primary/metrics?window=1h", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", metrics.Code, metrics.Body.String())
	}
	var response storageMetricsResponse
	if err := json.Unmarshal(metrics.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.PoolID != storage.PoolIDLocalPrimary || response.SampleIntervalSeconds != 7 || response.SampleIntervalMS != 7000 || response.Items == nil {
		t.Fatalf("metrics response=%+v", response)
	}
	if strings.Contains(metrics.Body.String(), root) {
		t.Fatalf("metrics response exposed a physical path: %s", metrics.Body.String())
	}

	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/api/storage/pools/not-a-pool/metrics?window=1h", want: http.StatusNotFound},
		{path: "/api/storage/pools/local-primary/metrics?window=7d", want: http.StatusBadRequest},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != test.want {
			t.Errorf("GET %s status=%d want=%d body=%s", test.path, response.Code, test.want, response.Body.String())
		}
	}
}

func TestStoragePoolProjectionPreservesUnknownCapacity(t *testing.T) {
	view := poolView(storage.PoolSnapshot{ID: "remote-primary", CapacityKnown: false})
	if view.CapacityKnown {
		t.Fatal("unknown provider capacity was projected as known")
	}
	body, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var projected map[string]json.RawMessage
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatal(err)
	}
	if got := string(projected["capacity_known"]); got != "false" {
		t.Fatalf("capacity_known JSON=%q, want false: %s", got, body)
	}
}

func TestStoragePoolAPIRequiresSessionWhenAuthIsEnabled(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap(strings.TrimSpace(string(bootstrap)), "storage-api-test-strong-password"); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{Auth: auth})
	for _, path := range []string{"/api/storage/pools", "/api/storage/pools/local-primary/metrics?window=1h"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Errorf("unauthenticated GET %s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}
