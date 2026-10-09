package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/storage"
)

type blockingIntegrityBackend struct {
	storage.StorageBackend
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingIntegrityBackend) VerifyRecordingContext(ctx context.Context, recording *domain.Recording) storage.IntegrityResult {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return b.StorageBackend.VerifyRecordingContext(ctx, recording)
}

func TestIntegrityStatusIncludesBoundedActiveJobProgress(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("d", 32), domain.StateCompleted)
	backend := &blockingIntegrityBackend{
		StorageBackend: store.StorageBackend,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	store.StorageBackend = backend
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := integrity.Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(backend.release)
		if err := service.Close(context.Background()); err != nil {
			t.Errorf("close integrity service: %v", err)
		}
	}()
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.started:
	case <-time.After(2 * time.Second):
		t.Fatal("integrity worker did not reach the blocked payload read")
	}

	handler := NewWithOptions(manager, nil, nil, Options{Integrity: service})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+recording.ID+"/integrity", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("integrity status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Status    string `json:"status"`
		ActiveJob *struct {
			ID       string `json:"id"`
			State    string `json:"state"`
			Progress struct {
				Current       uint64 `json:"current"`
				Total         uint64 `json:"total"`
				Phase         string `json:"phase"`
				Indeterminate bool   `json:"indeterminate"`
			} `json:"progress"`
		} `json:"active_job"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != string(storage.IntegrityVerifying) || body.ActiveJob == nil || body.ActiveJob.ID != job.ID || body.ActiveJob.State != string(integrity.StateRunning) || body.ActiveJob.Progress.Phase != "checking_objects" || !body.ActiveJob.Progress.Indeterminate || body.ActiveJob.Progress.Total == 0 {
		t.Fatalf("integrity active progress=%+v body=%s", body, response.Body.String())
	}
}
