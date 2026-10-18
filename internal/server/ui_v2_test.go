package server

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedSPAHasLocalProductionAssets(t *testing.T) {
	page, err := staticFiles.ReadFile("static/ui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(page), "/static/ui/assets/") {
		t.Fatalf("built SPA entry does not reference versioned local assets: %s", page)
	}
	if strings.Contains(string(page), "cdn.jsdelivr.net") || strings.Contains(string(page), "unpkg.com") || strings.Contains(string(page), "<script>") {
		t.Fatal("built SPA must not load third-party or inline scripts")
	}
	if err := fs.WalkDir(staticFiles, "static/ui", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasPrefix(path, "static/ui/assets/") {
			return err
		}
		requestPath := "/" + path
		response := httptest.NewRecorder()
		New(nil, nil, nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		if response.Code != http.StatusOK || response.Body.Len() == 0 {
			t.Errorf("asset %s: status=%d size=%d", requestPath, response.Code, response.Body.Len())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSPARoutesAreRestrictedAndDirectLoadsWork(t *testing.T) {
	handler := New(nil, nil, nil)
	for _, path := range []string{
		"/", "/login", "/recordings", "/recordings/abc123", "/adapters/example", "/sources", "/sources/example",
		"/plugins", "/workflows/id", "/settings", "/account/preferences", "/account/security", "/account/sessions",
		"/system", "/system/users", "/system/updates", "/system/diagnostics", "/system/audit",
		"/storage", "/storage/locations", "/storage/retention", "/storage/advanced", "/storage/local-primary",
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Accept", "text/html")
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/html") {
			t.Errorf("SPA route %s: status=%d", path, response.Code)
		}
	}
	for _, path := range []string{"/unknown", "/api/not-a-route"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Errorf("unknown route %s received status %d", path, response.Code)
		}
	}
	for _, path := range []string{"/recordings/a/b", "/recordings/..", "/adapters/", "/sources/", "/sources/a/b", "/workflows/id/extra", "/storage/a/b", "/storage/..", "/system/users/extra", "/account/security/extra"} {
		if isSPARoute(path) {
			t.Errorf("unexpected SPA route match for %s", path)
		}
	}
	nonHTML := httptest.NewRecorder()
	nonHTMLRequest := httptest.NewRequest(http.MethodGet, "/system", nil)
	nonHTMLRequest.Header.Set("Accept", "application/json")
	handler.ServeHTTP(nonHTML, nonHTMLRequest)
	if nonHTML.Code != http.StatusNotFound {
		t.Fatalf("non-browser route received SPA: status=%d", nonHTML.Code)
	}
}

func TestAuthPublicRoutesExposeOnlySPAAndStaticGetRequests(t *testing.T) {
	for _, path := range []string{
		"/", "/login", "/recordings", "/recordings/id", "/adapters/id", "/sources", "/sources/id", "/plugins",
		"/workflows/id", "/settings", "/account/preferences", "/account/security", "/account/sessions",
		"/system", "/system/users", "/system/updates", "/system/diagnostics", "/system/audit",
		"/storage", "/storage/locations", "/storage/retention", "/storage/advanced", "/storage/local-primary", "/static/ui/assets/app.js",
	} {
		if !authRoutePublic(httptest.NewRequest(http.MethodGet, path, nil)) {
			t.Errorf("expected GET %s to be public for SPA bootstrap", path)
		}
	}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/recordings", nil),
		httptest.NewRequest(http.MethodGet, "/recordings/id/extra", nil),
		httptest.NewRequest(http.MethodGet, "/api/dashboard", nil),
		httptest.NewRequest(http.MethodPost, "/static/ui/assets/app.js", nil),
	} {
		if authRoutePublic(request) {
			t.Errorf("unexpected public route: %s %s", request.Method, request.URL.Path)
		}
	}
}
