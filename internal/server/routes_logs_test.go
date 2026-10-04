package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/applog"
)

func TestLogHandlerReturnsBoundedJSONPage(t *testing.T) {
	store := applog.NewStore()
	store.Add("info", "server", "one")
	store.Add("warn", "acquire", "two")
	handler := NewLogHandler(store)

	request := httptest.NewRequest(http.MethodGet, "/api/logs?limit=1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := response.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q", got)
	}
	var body struct {
		Items      []applog.Entry `json:"items"`
		NextCursor string         `json:"next_cursor"`
		Total      *int           `json:"total"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Items[0].Message != "two" || body.NextCursor == "" {
		t.Fatalf("response = %#v", body)
	}
	if body.Total != nil {
		t.Fatalf("HTTP contract unexpectedly exposed total: %s", response.Body.String())
	}
}

func TestLogHandlerRejectsInvalidMethodPathAndQuery(t *testing.T) {
	handler := NewLogHandler(applog.NewStore())
	cases := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodPost, "/api/logs", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/logs/other", http.StatusNotFound},
		{http.MethodGet, "/api/logs?limit=1&limit=2", http.StatusBadRequest},
		{http.MethodGet, "/api/logs?unknown=x", http.StatusBadRequest},
		{http.MethodGet, "/api/logs?cursor=../../secret", http.StatusBadRequest},
		{http.MethodGet, "/api/logs?bad=%zz", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status = %d body=%s, want %d", response.Code, response.Body.String(), tc.status)
			}
			if tc.status == http.StatusMethodNotAllowed && response.Header().Get("Allow") != http.MethodGet {
				t.Fatalf("Allow = %q", response.Header().Get("Allow"))
			}
			if tc.status == http.StatusBadRequest && !strings.Contains(response.Body.String(), "invalid log query") {
				t.Fatalf("error was not generic: %s", response.Body.String())
			}
		})
	}
}

func TestLogHandlerUnavailableWhenStoreNil(t *testing.T) {
	response := httptest.NewRecorder()
	NewLogHandler(nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/logs", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestServerExposesOnlySafeHTTPActivityInLogAPI(t *testing.T) {
	store := applog.NewStore()
	handler := NewWithOptions(nil, nil, nil, Options{Logs: store})

	request := httptest.NewRequest(http.MethodGet, "/unmatched/path?token=do-not-store", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("request status = %d, want 404", response.Code)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/logs", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("logs status = %d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "do-not-store") || strings.Contains(response.Body.String(), "/unmatched/path") {
		t.Fatalf("HTTP request details leaked into log API: %s", response.Body.String())
	}
	var page struct {
		Items []applog.Entry `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Component != "http" || !strings.Contains(page.Items[0].Message, "404") {
		t.Fatalf("safe HTTP activity was not recorded: %#v", page.Items)
	}
}
