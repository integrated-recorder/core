package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storagediagnostic"
)

func TestStorageFailureDiagnosticRequiresAdministratorAndReturnsSafeRecord(t *testing.T) {
	root := t.TempDir()
	diagnostics, err := storagediagnostic.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	if _, _, err := diagnostics.RecordFirst(storagediagnostic.Diagnostic{
		Version: storagediagnostic.SchemaVersion, RecordedAt: time.Date(2026, 10, 8, 1, 2, 3, 0, time.UTC),
		RecordingID: id, Classification: storagediagnostic.ClassificationCanonicalCommitFailed,
		StageChain: []string{"media payload commit"}, ErrorTypeChain: []string{"*os.PathError"},
		ErrorCategories: []string{"canonical_commit_failed", "io_error"}, FileOperation: "rename", ErrnoCode: 28,
		CurrentJobKind: "media_payload", FirstFailureJobKind: "media_payload", CurrentAttempts: 2, FirstFailureAttempts: 2,
		IngestSnapshot: storagediagnostic.IngestSnapshot{BufferUsedBytes: 1, ReservedBytes: 2, QueueObjects: 1, QueueBytes: 1, WriterConcurrency: 1},
		RecordingState: "recording", SourceClass: "live",
	}); err != nil {
		t.Fatal(err)
	}

	publicAPI := NewWithOptions(nil, nil, nil, Options{StorageDiagnostics: diagnostics})
	noAuth := httptest.NewRecorder()
	publicAPI.ServeHTTP(noAuth, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/diagnostics/storage", nil))
	if noAuth.Code != http.StatusForbidden || strings.Contains(noAuth.Body.String(), "stage_chain") {
		t.Fatalf("diagnostic without administrator auth: %d %s", noAuth.Code, noAuth.Body.String())
	}

	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap(strings.TrimSpace(string(bootstrap)), "storage-diagnostic-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.Login("storage-diagnostic-test-password")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, StorageDiagnostics: diagnostics})
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/diagnostics/storage", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated diagnostic status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/diagnostics/storage", nil)
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(response.Body.String(), `"classification":"canonical_commit_failed"`) ||
		!strings.Contains(response.Body.String(), `"stage_chain":["media payload commit"]`) || strings.Contains(response.Body.String(), root) {
		t.Fatalf("authenticated diagnostic response=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}

	archive, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, archive, id, domain.StateInterrupted)
	recording.LastError = "recording storage commit failed https://private.invalid/?token=private-token"
	if err := archive.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(archive, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	publicHandler := NewWithOptions(manager, nil, nil, Options{Storage: archive, Auth: auth, StorageDiagnostics: diagnostics})
	publicRequest := httptest.NewRequest(http.MethodGet, "/api/recordings/"+id, nil)
	publicRequest.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	publicResponse := httptest.NewRecorder()
	publicHandler.ServeHTTP(publicResponse, publicRequest)
	if publicResponse.Code != http.StatusOK || !strings.Contains(publicResponse.Body.String(), `"last_error":"acquisition error"`) ||
		strings.Contains(publicResponse.Body.String(), "private.invalid") || strings.Contains(publicResponse.Body.String(), "private-token") ||
		strings.Contains(publicResponse.Body.String(), "stage_chain") {
		t.Fatalf("public recording response exposed internal diagnostics: status=%d body=%s", publicResponse.Code, publicResponse.Body.String())
	}
}
