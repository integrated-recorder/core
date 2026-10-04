package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dltkddnr04/integrated-recorder/internal/authn"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/installation"
	"github.com/dltkddnr04/integrated-recorder/internal/storage"
)

func newManagedSetupServer(t *testing.T) (*Server, string, authn.Session) {
	t.Helper()
	base := os.TempDir()
	if resolved, resolveErr := filepath.EvalSymlinks(base); resolveErr == nil {
		base = resolved
	}
	root, err := os.MkdirTemp(base, "control-installation-gate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	archive, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	return newManagedSetupServerWithStorage(t, root, archive)
}

func newManagedSetupServerWithStorage(t *testing.T, root string, archive *storage.Store) (*Server, string, authn.Session) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "runtime", "state"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := installation.Reconcile(root, installation.AdminMissing, false); err != nil {
		t.Fatal(err)
	}
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap(strings.TrimSpace(string(token)), "server-setup-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.Login("server-setup-test-password")
	if err != nil {
		t.Fatal(err)
	}
	return NewWithOptions(nil, nil, nil, Options{Storage: archive, Auth: auth, InstallationManaged: true}), root, session
}

func TestIncompleteInstallationAllowsOnlySetupReadinessRoutes(t *testing.T) {
	handler, root, session := newManagedSetupServer(t)
	sessionRequest := httptest.NewRequest(http.MethodGet, "/api/auth/session", nil)
	sessionRequest.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	sessionResponse := httptest.NewRecorder()
	handler.ServeHTTP(sessionResponse, sessionRequest)
	if sessionResponse.Code != http.StatusOK || strings.Contains(sessionResponse.Body.String(), "bootstrap-token") || strings.Contains(sessionResponse.Body.String(), root) {
		t.Fatalf("auth session response exposed setup filesystem details: status=%d body=%s", sessionResponse.Code, sessionResponse.Body.String())
	}
	if strings.Contains(sessionResponse.Body.String(), "bootstrap_token_path") {
		t.Fatalf("auth session retained legacy bootstrap path field: %s", sessionResponse.Body.String())
	}
	for _, path := range []string{"/recordings", "/settings", "/storage", "/api/recordings", "/api/watches", "/api/settings", "/api/storage/pools"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"code":"installation_incomplete"`) {
			t.Errorf("GET %s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	for _, path := range []string{"/api/system/storage", "/api/system/info", "/api/adapters", "/api/auth/session"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code == http.StatusConflict {
			t.Errorf("read-only setup route %s was gated: %s", path, response.Body.String())
		}
	}
	for _, path := range []string{"/api/recordings", "/api/watches"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
		request.Header.Set("X-CSRF-Token", session.CSRFToken)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"code":"installation_incomplete"`) {
			t.Errorf("POST %s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestSetupStorageTestRequiresAuthAndCSRFAndReturnsBoundedProjection(t *testing.T) {
	handler, root, session := newManagedSetupServer(t)
	unauth := httptest.NewRecorder()
	handler.ServeHTTP(unauth, httptest.NewRequest(http.MethodPost, "/api/setup/storage-test", strings.NewReader(`{}`)))
	if unauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated storage test status=%d", unauth.Code)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/setup/storage-test", strings.NewReader(`{}`))
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	noCSRF := httptest.NewRecorder()
	handler.ServeHTTP(noCSRF, request)
	if noCSRF.Code != http.StatusForbidden {
		t.Fatalf("storage test without CSRF status=%d", noCSRF.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/api/setup/storage-test", strings.NewReader(`{}`))
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	request.Header.Set("X-CSRF-Token", session.CSRFToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("storage test status=%d body=%s", response.Code, response.Body.String())
	}
	var got setupStorageTestResponse
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "ready" && got.Status != "warning" {
		t.Fatalf("storage self-test failed: %+v", got)
	}
	if got.WriteTest != "passed" || got.DurabilityTest != "passed" || !got.CapacityKnown || got.FreeBytes == nil || *got.FreeBytes == 0 {
		t.Fatalf("storage self-test did not verify bytes: %+v", got)
	}
	if strings.Contains(response.Body.String(), root) || strings.Contains(response.Body.String(), "permission denied") {
		t.Fatalf("storage test exposed path or raw error: %s", response.Body.String())
	}
	entries, err := os.ReadDir(filepath.Join(root, "runtime", "state"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("storage test left diagnostic artifacts: %+v err=%v", entries, err)
	}
}

func TestSetupStorageTestReportsUnknownProviderCapacityWithoutFailingProbe(t *testing.T) {
	base := os.TempDir()
	if resolved, resolveErr := filepath.EvalSymlinks(base); resolveErr == nil {
		base = resolved
	}
	root, err := os.MkdirTemp(base, "control-installation-object-store-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	objects := newRangeTrackingObjects()
	archive, err := storage.NewWithObjectStore(root, objects)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, session := newManagedSetupServerWithStorage(t, root, archive)
	request := httptest.NewRequest(http.MethodPost, "/api/setup/storage-test", strings.NewReader(`{}`))
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	request.Header.Set("X-CSRF-Token", session.CSRFToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("storage test status=%d body=%s", response.Code, response.Body.String())
	}
	var got setupStorageTestResponse
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "warning" || got.CapacityKnown || got.FreeBytes != nil {
		t.Fatalf("unknown provider capacity should be a warning with no numeric claim: %+v body=%s", got, response.Body.String())
	}
	if got.WriteTest != "passed" || got.DurabilityTest != "passed" {
		t.Fatalf("provider-backed write/durability probe did not pass: %+v", got)
	}
	if strings.Contains(response.Body.String(), `"free_bytes"`) || strings.Contains(response.Body.String(), root) {
		t.Fatalf("unknown capacity or private path leaked as numeric/path data: %s", response.Body.String())
	}
}
