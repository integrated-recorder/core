package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestRecordingLifecycleEndpointsKeepCaptureAndArchiveSeparate(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const stoppedID = "a123456789abcdef0123456789abcdef"
	const sealFirstID = "b123456789abcdef0123456789abcdef"
	writeProductRecording(t, store, stoppedID, domain.StateStopped)
	writeProductRecording(t, store, sealFirstID, domain.StateStopped)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	handler := New(manager, nil, nil)

	complete := httptest.NewRecorder()
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/api/recordings/"+stoppedID+"/complete", nil))
	if complete.Code != http.StatusOK || !strings.Contains(complete.Body.String(), `"state":"completed"`) || strings.Contains(complete.Body.String(), `"archive_sealed":true`) {
		t.Fatalf("complete status=%d body=%s", complete.Code, complete.Body.String())
	}
	completed, err := manager.Get(stoppedID)
	if err != nil || completed.State != domain.StateCompleted || completed.ArchiveSealed {
		t.Fatalf("completion state=%#v err=%v", completed, err)
	}

	lifecycle := httptest.NewRecorder()
	handler.ServeHTTP(lifecycle, httptest.NewRequest(http.MethodGet, "/api/recordings/"+stoppedID+"/lifecycle", nil))
	if lifecycle.Code != http.StatusOK || !strings.Contains(lifecycle.Body.String(), `"capture_state":"completed"`) || !strings.Contains(lifecycle.Body.String(), `"archive_sealed":false`) || !strings.Contains(lifecycle.Body.String(), `"repairable":true`) || !strings.Contains(lifecycle.Body.String(), `"recovery_state":"idle"`) || strings.Contains(lifecycle.Body.String(), `"engine_owns"`) {
		t.Fatalf("completed repairable lifecycle=%d %s", lifecycle.Code, lifecycle.Body.String())
	}

	seal := httptest.NewRecorder()
	handler.ServeHTTP(seal, httptest.NewRequest(http.MethodPost, "/api/recordings/"+stoppedID+"/seal", nil))
	if seal.Code != http.StatusOK || !strings.Contains(seal.Body.String(), `"sealed":true`) {
		t.Fatalf("seal status=%d body=%s", seal.Code, seal.Body.String())
	}
	sealedView := httptest.NewRecorder()
	handler.ServeHTTP(sealedView, httptest.NewRequest(http.MethodGet, "/api/recordings/"+stoppedID+"/lifecycle", nil))
	if sealedView.Code != http.StatusOK || !strings.Contains(sealedView.Body.String(), `"capture_state":"completed"`) || !strings.Contains(sealedView.Body.String(), `"archive_sealed":true`) || !strings.Contains(sealedView.Body.String(), `"repairable":false`) || !strings.Contains(sealedView.Body.String(), `"recovery_state":"sealed"`) {
		t.Fatalf("sealed lifecycle=%d body=%s", sealedView.Code, sealedView.Body.String())
	}
	completed, err = manager.Get(stoppedID)
	if err != nil || completed.State != domain.StateCompleted || !completed.ArchiveSealed {
		t.Fatalf("sealed completion state=%#v err=%v", completed, err)
	}
	// Both operations are idempotent when retried after a successful transition.
	for _, path := range []string{"/complete", "/seal"} {
		retry := httptest.NewRecorder()
		handler.ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/api/recordings/"+stoppedID+path, nil))
		if retry.Code != http.StatusOK {
			t.Fatalf("idempotent %s status=%d body=%s", path, retry.Code, retry.Body.String())
		}
	}

	sealStopped := httptest.NewRecorder()
	handler.ServeHTTP(sealStopped, httptest.NewRequest(http.MethodPost, "/api/recordings/"+sealFirstID+"/seal", nil))
	if sealStopped.Code != http.StatusOK || !strings.Contains(sealStopped.Body.String(), `"sealed":true`) {
		t.Fatalf("seal-first status=%d body=%s", sealStopped.Code, sealStopped.Body.String())
	}
	sealedStopped, err := manager.Get(sealFirstID)
	if err != nil || sealedStopped.State != domain.StateStopped || !sealedStopped.ArchiveSealed {
		t.Fatalf("seal changed capture state: recording=%#v err=%v", sealedStopped, err)
	}
	completeSealed := httptest.NewRecorder()
	handler.ServeHTTP(completeSealed, httptest.NewRequest(http.MethodPost, "/api/recordings/"+sealFirstID+"/complete", nil))
	if completeSealed.Code != http.StatusOK {
		t.Fatalf("complete after seal status=%d body=%s", completeSealed.Code, completeSealed.Body.String())
	}
	sealedCompleted, err := manager.Get(sealFirstID)
	if err != nil || sealedCompleted.State != domain.StateCompleted || !sealedCompleted.ArchiveSealed {
		t.Fatalf("completion after seal changed archive lifecycle: recording=%#v err=%v", sealedCompleted, err)
	}
}

func TestRecordingLifecycleEndpointReturnsConflictForInterruptedCompletion(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "c123456789abcdef0123456789abcdef"
	writeProductRecording(t, store, id, domain.StateInterrupted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	handler := New(manager, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/recordings/"+id+"/complete", nil))
	if response.Code != http.StatusConflict || strings.Contains(response.Body.String(), "recording storage") {
		t.Fatalf("interrupted completion status=%d body=%s", response.Code, response.Body.String())
	}
	recording, err := manager.Get(id)
	if err != nil || recording.State != domain.StateInterrupted || recording.ArchiveSealed {
		t.Fatalf("conflicting completion mutated recording=%#v err=%v", recording, err)
	}
}
