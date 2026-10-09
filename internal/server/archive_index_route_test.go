package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/storage"
)

func TestArchiveIndexMissingRecordingReturnsCodedNotFound(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Storage: store})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/archive/index", nil))
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"error_code":"recording_not_found"`) {
		t.Fatalf("missing archive index status=%d body=%s", response.Code, response.Body.String())
	}
}
