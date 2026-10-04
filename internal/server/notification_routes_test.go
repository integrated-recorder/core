package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestNotificationProjectionUsesExplicitSyncAndReadOnlyList(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	writeProductRecording(t, store, strings.Repeat("e", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{Management: products})

	list := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/notifications?unread=true", nil))
		return response
	}
	before := list()
	if before.Code != http.StatusOK || strings.Contains(before.Body.String(), "recording_completed") {
		t.Fatalf("notification GET should not create a projection: %d %s", before.Code, before.Body.String())
	}

	syncResponse := httptest.NewRecorder()
	handler.ServeHTTP(syncResponse, httptest.NewRequest(http.MethodPost, "/api/notifications/sync", strings.NewReader(`{}`)))
	if syncResponse.Code != http.StatusNoContent {
		t.Fatalf("notification sync status=%d body=%s", syncResponse.Code, syncResponse.Body.String())
	}
	after := list()
	if after.Code != http.StatusOK || !strings.Contains(after.Body.String(), "recording_completed") {
		t.Fatalf("notification sync did not produce the real recording event: %d %s", after.Code, after.Body.String())
	}

	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, httptest.NewRequest(http.MethodPost, "/api/notifications/sync", strings.NewReader(`{"extra":true}`)))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("unknown notification sync field status=%d body=%s", bad.Code, bad.Body.String())
	}
}
