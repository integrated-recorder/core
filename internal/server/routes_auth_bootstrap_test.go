package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
)

func TestHTTPFirstRunBootstrapPersistsSecureUserSession(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	setupTokenPath := filepath.Join(root, filepath.FromSlash(auth.BootstrapTokenRelativePath()))
	setupTokenBytes, err := os.ReadFile(setupTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	setupToken := strings.TrimSpace(string(setupTokenBytes))
	if setupToken == "" {
		t.Fatal("local first-run setup token is empty")
	}
	setupTokenInfo, err := os.Stat(setupTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := setupTokenInfo.Mode().Perm(); got != 0600 {
		t.Fatalf("setup token permissions=%#o, want 0600", got)
	}

	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, ForceSecureCookies: true})
	initialSession := httptest.NewRecorder()
	handler.ServeHTTP(initialSession, httptest.NewRequest(http.MethodGet, "/api/auth/session", nil))
	if initialSession.Code != http.StatusOK {
		t.Fatalf("initial auth session status=%d body=%s", initialSession.Code, initialSession.Body.String())
	}
	var initial struct {
		AuthEnabled    bool `json:"auth_enabled"`
		Authenticated  bool `json:"authenticated"`
		NeedsBootstrap bool `json:"needs_bootstrap"`
	}
	if err := json.Unmarshal(initialSession.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	if !initial.AuthEnabled || initial.Authenticated || !initial.NeedsBootstrap {
		t.Fatalf("initial auth state=%+v, want first-run bootstrap state", initial)
	}

	const password = "first-run-owner-password"
	bootstrapBody, err := json.Marshal(authCredentialRequest{Token: setupToken, Password: password})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := httptest.NewRecorder()
	handler.ServeHTTP(bootstrap, httptest.NewRequest(http.MethodPost, "/api/auth/bootstrap", strings.NewReader(string(bootstrapBody))))
	if bootstrap.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", bootstrap.Code, bootstrap.Body.String())
	}
	if strings.Contains(bootstrap.Body.String(), setupToken) || strings.Contains(bootstrap.Body.String(), password) || strings.Contains(bootstrap.Body.String(), "bootstrap-token") {
		t.Fatalf("bootstrap response exposed setup material: %s", bootstrap.Body.String())
	}
	var bootstrapResponse struct {
		AuthEnabled    bool      `json:"auth_enabled"`
		Authenticated  bool      `json:"authenticated"`
		NeedsBootstrap bool      `json:"needs_bootstrap"`
		CSRFToken      string    `json:"csrf_token"`
		ExpiresAt      time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(bootstrap.Body.Bytes(), &bootstrapResponse); err != nil {
		t.Fatal(err)
	}
	if !bootstrapResponse.AuthEnabled || !bootstrapResponse.Authenticated || bootstrapResponse.NeedsBootstrap || bootstrapResponse.CSRFToken == "" {
		t.Fatalf("bootstrap response state=%+v", bootstrapResponse)
	}

	var sessionCookie, csrfCookie *http.Cookie
	for _, cookie := range bootstrap.Result().Cookies() {
		switch cookie.Name {
		case authn.SessionCookieName:
			sessionCookie = cookie
		case csrfCookieName:
			csrfCookie = cookie
		}
	}
	if sessionCookie == nil || sessionCookie.Value == "" || !sessionCookie.HttpOnly || !sessionCookie.Secure || sessionCookie.SameSite != http.SameSiteStrictMode || sessionCookie.Path != "/" {
		t.Fatalf("session cookie missing secure attributes: %+v", sessionCookie)
	}
	if csrfCookie == nil || csrfCookie.Value != bootstrapResponse.CSRFToken || csrfCookie.HttpOnly || !csrfCookie.Secure || csrfCookie.SameSite != http.SameSiteStrictMode || csrfCookie.Path != "/" {
		t.Fatalf("CSRF cookie mismatch or unsafe attributes: %+v", csrfCookie)
	}
	if strings.Contains(bootstrap.Body.String(), sessionCookie.Value) {
		t.Fatal("bootstrap response body exposed session token")
	}
	if _, err := os.Stat(setupTokenPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumed setup token remains on disk: %v", err)
	}

	replayBody, err := json.Marshal(authCredentialRequest{Token: setupToken, Password: password})
	if err != nil {
		t.Fatal(err)
	}
	replay := httptest.NewRecorder()
	handler.ServeHTTP(replay, httptest.NewRequest(http.MethodPost, "/api/auth/bootstrap", strings.NewReader(string(replayBody))))
	if replay.Code != http.StatusConflict || strings.Contains(replay.Body.String(), setupToken) {
		t.Fatalf("bootstrap replay status=%d body=%s", replay.Code, replay.Body.String())
	}

	reopened, err := authn.Open(root)
	if err != nil {
		t.Fatalf("reopen auth store: %v", err)
	}
	session, err := reopened.Authenticate(sessionCookie.Value)
	if err != nil || session.UserID == "" || session.Login != "owner" || session.Role != authn.RoleOwner {
		t.Fatalf("reopened session=%+v err=%v", session, err)
	}
	if !reopened.ValidCSRF(sessionCookie.Value, csrfCookie.Value) {
		t.Fatal("reopened auth store lost session-bound CSRF validation")
	}

	reopenedHandler := NewWithOptions(nil, nil, nil, Options{Auth: reopened, ForceSecureCookies: true})
	badCSRF := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	badCSRF.AddCookie(sessionCookie)
	badCSRF.Header.Set("X-CSRF-Token", "invalid-csrf-token")
	badCSRFResponse := httptest.NewRecorder()
	reopenedHandler.ServeHTTP(badCSRFResponse, badCSRF)
	if badCSRFResponse.Code != http.StatusForbidden {
		t.Fatalf("bad-CSRF mutation status=%d body=%s", badCSRFResponse.Code, badCSRFResponse.Body.String())
	}
	if _, err := reopened.Authenticate(sessionCookie.Value); err != nil {
		t.Fatalf("bad-CSRF mutation revoked session: %v", err)
	}

	logout := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	logout.AddCookie(sessionCookie)
	logout.Header.Set("X-CSRF-Token", csrfCookie.Value)
	logoutResponse := httptest.NewRecorder()
	reopenedHandler.ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("valid logout status=%d body=%s", logoutResponse.Code, logoutResponse.Body.String())
	}
	if _, err := reopened.Authenticate(sessionCookie.Value); err == nil {
		t.Fatal("valid logout left persisted session active")
	}
}
