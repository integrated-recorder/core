package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestIntegrityJobCancellationRoute(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("f", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := integrity.Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	handler := NewWithOptions(manager, nil, nil, Options{Integrity: service})
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/integrity/jobs/"+job.ID+"/cancel", strings.NewReader(`{}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%s", response.Code, response.Body.String())
	}
	var current integrity.Job
	if err := json.Unmarshal(response.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if current.ID != job.ID || (current.State != integrity.StateCanceled && current.State != integrity.StateCompleted) {
		t.Fatalf("unexpected cancellation result: %+v", current)
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodPost, "/api/integrity/jobs/"+strings.Repeat("1", 32)+"/cancel", strings.NewReader(`{}`)))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing job cancel status=%d body=%s", missing.Code, missing.Body.String())
	}
}
