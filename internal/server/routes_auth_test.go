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

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestLoginAcceptsOptionalIdentityAndPersistsBoundSession(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath()))
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(tokenBytes)), "owner-login-test-password"); err != nil {
		t.Fatal(err)
	}
	fixtureUser, err := service.CreateUser("fixture-user", "fixture-login-test-password", authn.RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: service})

	login := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"login":"fixture-user","password":"fixture-login-test-password"}`))
	handler.ServeHTTP(login, request)
	if login.Code != http.StatusOK {
		t.Fatalf("named login status=%d body=%s", login.Code, login.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(login.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if _, exposed := response["user_id"]; exposed {
		t.Fatalf("login response exposed internal user identity: %s", login.Body.String())
	}
	var sessionToken string
	for _, cookie := range login.Result().Cookies() {
		if cookie.Name == authn.SessionCookieName {
			sessionToken = cookie.Value
		}
	}
	if sessionToken == "" {
		t.Fatal("named login did not issue a session cookie")
	}

	reopened, err := authn.Open(root)
	if err != nil {
		t.Fatalf("reopen auth store: %v", err)
	}
	session, err := reopened.Authenticate(sessionToken)
	if err != nil || session.UserID != fixtureUser.ID || session.Login != fixtureUser.Login {
		t.Fatalf("reopened session=%+v err=%v, want user=%s", session, err, fixtureUser.ID)
	}
	csrf, _ := response["csrf_token"].(string)
	if csrf == "" || !reopened.ValidCSRF(sessionToken, csrf) {
		t.Fatal("reopened user-bound session lost its CSRF binding")
	}

	badIdentity := httptest.NewRecorder()
	badIdentityRequest := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"login":"missing-user","password":"fixture-login-test-password"}`))
	handler.ServeHTTP(badIdentity, badIdentityRequest)
	badPassword := httptest.NewRecorder()
	badPasswordRequest := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"login":"fixture-user","password":"wrong-login-test-password"}`))
	handler.ServeHTTP(badPassword, badPasswordRequest)
	if badIdentity.Code != http.StatusUnauthorized || badPassword.Code != badIdentity.Code || badIdentity.Body.String() != badPassword.Body.String() {
		t.Fatalf("login failure leaked identity status/body: identity=%d %q password=%d %q", badIdentity.Code, badIdentity.Body.String(), badPassword.Code, badPassword.Body.String())
	}
}

func TestAuthenticatedUserPreferencesAreSessionScopedAndDurable(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tokenBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath())))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(tokenBytes)), "prefs-owner-password"); err != nil {
		t.Fatal(err)
	}
	owner, err := service.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	other, err := service.CreateUser("prefs-other", "prefs-other-password", authn.RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	ownerSession, err := service.Login("prefs-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	otherSession, err := service.LoginAs("prefs-other", "prefs-other-password")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: service})

	get := func(token string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "/api/user/preferences", nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: token})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	put := func(token, csrf, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPut, "/api/user/preferences", strings.NewReader(body))
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: token})
		request.Header.Set("X-CSRF-Token", csrf)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	ownerDefault := get(ownerSession.Token)
	if ownerDefault.Code != http.StatusOK || ownerDefault.Body.String() != "{\"locale\":\"system\",\"theme\":\"system\",\"timezone\":\"system\"}\n" {
		t.Fatalf("owner default preferences status=%d body=%s", ownerDefault.Code, ownerDefault.Body.String())
	}
	ownerPrefsBody := `{"locale":"ko-KR","theme":"dark","timezone":"Asia/Seoul"}`
	if response := put(ownerSession.Token, ownerSession.CSRFToken, ownerPrefsBody); response.Code != http.StatusOK {
		t.Fatalf("owner preference update status=%d body=%s", response.Code, response.Body.String())
	}
	otherPrefsBody := `{"locale":"en-US","theme":"light","timezone":"America/Los_Angeles"}`
	if response := put(otherSession.Token, otherSession.CSRFToken, otherPrefsBody); response.Code != http.StatusOK {
		t.Fatalf("other preference update status=%d body=%s", response.Code, response.Body.String())
	}
	if response := put(ownerSession.Token, ownerSession.CSRFToken, `{"user_id":"`+other.ID+`","locale":"en-US","theme":"dark","timezone":"system"}`); response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"error_code":"preferences_invalid"`) {
		t.Fatalf("forged target preference update status=%d body=%s", response.Code, response.Body.String())
	}
	if got := get(otherSession.Token); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"locale":"en-US"`) || !strings.Contains(got.Body.String(), `"theme":"light"`) {
		t.Fatalf("other user's preferences crossed session boundary: status=%d body=%s", got.Code, got.Body.String())
	}
	if got := get(ownerSession.Token); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"locale":"ko-KR"`) || strings.Contains(got.Body.String(), "user_id") {
		t.Fatalf("owner preference response=%d %s", got.Code, got.Body.String())
	}
	targetedRead := httptest.NewRecorder()
	targetedRequest := httptest.NewRequest(http.MethodGet, "/api/user/preferences?user_id="+other.ID, nil)
	targetedRequest.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: ownerSession.Token})
	handler.ServeHTTP(targetedRead, targetedRequest)
	if targetedRead.Code != http.StatusOK || !strings.Contains(targetedRead.Body.String(), `"locale":"ko-KR"`) {
		t.Fatalf("user_id query altered self preference read: status=%d body=%s", targetedRead.Code, targetedRead.Body.String())
	}

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/api/user/preferences", nil))
	if unauthenticated.Code != http.StatusUnauthorized || !strings.Contains(unauthenticated.Body.String(), `"error_code":"authentication_required"`) {
		t.Fatalf("unauthenticated preference read status=%d body=%s", unauthenticated.Code, unauthenticated.Body.String())
	}
	noCSRF := httptest.NewRecorder()
	noCSRFRequest := httptest.NewRequest(http.MethodPut, "/api/user/preferences", strings.NewReader(ownerPrefsBody))
	noCSRFRequest.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: ownerSession.Token})
	handler.ServeHTTP(noCSRF, noCSRFRequest)
	if noCSRF.Code != http.StatusForbidden {
		t.Fatalf("preference update without CSRF status=%d body=%s", noCSRF.Code, noCSRF.Body.String())
	}

	reopened, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	reopenedHandler := NewWithOptions(nil, nil, nil, Options{Auth: reopened})
	for _, tc := range []struct {
		name  string
		token string
		want  string
	}{{"owner", ownerSession.Token, `"locale":"ko-KR"`}, {"other", otherSession.Token, `"locale":"en-US"`}} {
		request := httptest.NewRequest(http.MethodGet, "/api/user/preferences", nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: tc.token})
		response := httptest.NewRecorder()
		reopenedHandler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), tc.want) {
			t.Errorf("reopened %s preferences status=%d body=%s", tc.name, response.Code, response.Body.String())
		}
	}
	if owner.ID == other.ID {
		t.Fatal("fixture users must have independent identities")
	}
}

func TestLogoutRevokesSessionAndClearsCookies(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tokenBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath())))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(tokenBytes)), "logout-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := service.Login("logout-test-password")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: service})
	request := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	request.Header.Set("X-CSRF-Token", session.CSRFToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("logout status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := service.Authenticate(session.Token); err == nil {
		t.Fatal("logout left session valid")
	}
	cleared := map[string]bool{}
	for _, cookie := range response.Result().Cookies() {
		if cookie.MaxAge == -1 {
			cleared[cookie.Name] = true
		}
	}
	if !cleared[authn.SessionCookieName] || !cleared[csrfCookieName] {
		t.Fatalf("logout did not clear both cookies: %+v", response.Result().Cookies())
	}
}

func TestPermissionForRequestExplicitlyProtectsMutations(t *testing.T) {
	owner := authn.Principal{UserID: "usr-0123456789abcdef0123456789abcdef", Login: "owner", Role: authn.RoleOwner}
	member := authn.Principal{UserID: "usr-1123456789abcdef0123456789abcdef", Login: "member", Role: "member"}
	for _, tc := range []struct {
		method string
		path   string
		want   authn.Permission
	}{
		{http.MethodPost, "/api/watches", authn.PermissionSettingsManage},
		{http.MethodPut, "/api/watches/watch-1", authn.PermissionSettingsManage},
		{http.MethodPost, "/api/watches/watch-1/check", authn.PermissionRecordingControl},
		{http.MethodPost, "/api/retention/run", authn.PermissionRecordingDelete},
		{http.MethodPost, "/api/recordings", authn.PermissionRecordingControl},
		{http.MethodDelete, "/api/recordings/0123456789abcdef0123456789abcdef", authn.PermissionRecordingDelete},
		{http.MethodGet, "/api/user/preferences", authn.PermissionUserPreferences},
		{http.MethodPut, "/api/user/preferences", authn.PermissionUserPreferences},
	} {
		got := permissionForRequest(httptest.NewRequest(tc.method, tc.path, nil))
		if got != tc.want || !authn.HasPermission(owner, got) {
			t.Errorf("permissionForRequest(%s %s)=%q want %q", tc.method, tc.path, got, tc.want)
		}
	}
	unknownMutation := permissionForRequest(httptest.NewRequest(http.MethodPost, "/api/not-classified", nil))
	if authn.HasPermission(owner, unknownMutation) {
		t.Fatalf("unclassified mutation was granted: %q", unknownMutation)
	}
	unknownKnownNamespace := permissionForRequest(httptest.NewRequest(http.MethodPost, "/api/watches/watch-1/unknown", nil))
	if authn.HasPermission(owner, unknownKnownNamespace) {
		t.Fatalf("unclassified namespace mutation was granted: %q", unknownKnownNamespace)
	}
	if got := permissionForRequest(httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)); got != authn.PermissionSettingsManage {
		t.Fatalf("logout permission=%q", got)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		got := permissionForRequest(httptest.NewRequest(method, "/api/not-classified", nil))
		if authn.HasPermission(owner, got) {
			t.Errorf("unknown %s route was granted permission %q", method, got)
		}
	}
	for _, path := range []string{
		"/api/storage/pools/primary/unknown",
		"/api/storage/pools/primary/metrics/extra",
		"/api/exports/",
		"/api/integrity/jobs/",
		"/api/integrity/jobs//cancel",
		"/api/recordings/",
		"/api/recordings/0123456789abcdef0123456789abcdef/play/segments/",
	} {
		got := permissionForRequest(httptest.NewRequest(http.MethodGet, path, nil))
		if authn.HasPermission(owner, got) {
			t.Errorf("unregistered read route %s was granted permission %q", path, got)
		}
	}
	if got := permissionForRequest(httptest.NewRequest(http.MethodHead, "/api/audit", nil)); got != authn.PermissionAuditRead {
		t.Fatalf("HEAD audit permission=%q, want audit.read", got)
	}
	if !authn.HasPermission(member, authn.PermissionUserPreferences) || authn.HasPermission(member, authn.PermissionSettingsManage) {
		t.Fatal("self preference permission should be available to an authenticated non-owner without granting system settings")
	}
}

func TestAuthenticatedUnknownReadRoutesAreDenied(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tokenBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath())))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(tokenBytes)), "unknown-route-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := service.Login("unknown-route-test-password")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: service})
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodOptions} {
		request := httptest.NewRequest(method, "/api/not-classified", nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Errorf("authenticated unknown %s route status=%d body=%s", method, response.Code, response.Body.String())
		}
	}
}

func TestHTTPAuditBindsIndependentUserSessionsAndLogoutIsPerSession(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	recording := writeProductRecording(t, store, strings.Repeat("d", 32), domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tokenBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath())))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(tokenBytes)), "owner-a-password-123"); err != nil {
		t.Fatal(err)
	}
	userB, err := service.CreateUser("fixture-user-b", "user-b-password-123", authn.RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	sessionA, err := service.Login("owner-a-password-123")
	if err != nil {
		t.Fatal(err)
	}
	sessionB, err := service.LoginAs(userB.Login, "user-b-password-123")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(manager, nil, nil, Options{Management: products, Auth: service})
	for _, item := range []struct {
		session authn.Session
		tags    string
	}{{sessionB, `{"tags":["from-b"]}`}, {sessionA, `{"tags":["from-a"]}`}} {
		request := httptest.NewRequest(http.MethodPut, "/api/recordings/"+recording.ID+"/tags", strings.NewReader(item.tags))
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: item.session.Token})
		request.Header.Set("X-CSRF-Token", item.session.CSRFToken)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Header().Get("X-Audit-Status") != "recorded" {
			t.Fatalf("audited mutation status=%d audit=%q body=%s", response.Code, response.Header().Get("X-Audit-Status"), response.Body.String())
		}
	}
	actors := map[string]bool{}
	for _, event := range products.Audit(10) {
		if event.Type == "recording_tags_updated" && event.Actor != nil && event.Actor.Type == management.AuditActorUser {
			actors[event.Actor.UserID] = true
		}
	}
	if !actors[sessionA.UserID] || !actors[sessionB.UserID] || len(actors) != 2 {
		t.Fatalf("audit actors=%v want independent users %q and %q", actors, sessionA.UserID, sessionB.UserID)
	}
	logout := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	logout.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: sessionA.Token})
	logout.Header.Set("X-CSRF-Token", sessionA.CSRFToken)
	logoutResponse := httptest.NewRecorder()
	handler.ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("owner A logout status=%d body=%s", logoutResponse.Code, logoutResponse.Body.String())
	}
	if _, err := service.Authenticate(sessionA.Token); err == nil {
		t.Fatal("owner A session remained valid after logout")
	}
	if session, err := service.Authenticate(sessionB.Token); err != nil || session.UserID != sessionB.UserID {
		t.Fatalf("owner B session changed after owner A logout: session=%+v err=%v", session, err)
	}
}

func TestHTTPLogoutDoesNotClaimSuccessWhenRevocationFails(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath()))
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(tokenBytes)), "logout-failure-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := service.Login("logout-failure-test-password")
	if err != nil {
		t.Fatal(err)
	}
	server := NewWithOptions(nil, nil, nil, Options{Auth: service})
	server.logoutSession = func(string) error { return errors.New("/private/auth/sessions detail") }

	request := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	request.Header.Set("X-CSRF-Token", session.CSRFToken)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "/private/auth/sessions") {
		t.Fatalf("failed logout status=%d body=%s", response.Code, response.Body.String())
	}
	if len(response.Result().Cookies()) != 0 {
		t.Fatalf("failed logout cleared retry credentials: cookies=%v", response.Result().Cookies())
	}
	if _, err := service.Authenticate(session.Token); err != nil {
		t.Fatalf("session should remain valid after failed revoke: %v", err)
	}

	server.logoutSession = service.Logout
	retry := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	retry.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	retry.Header.Set("X-CSRF-Token", session.CSRFToken)
	retryResponse := httptest.NewRecorder()
	server.ServeHTTP(retryResponse, retry)
	if retryResponse.Code != http.StatusNoContent {
		t.Fatalf("durable logout retry status=%d body=%s", retryResponse.Code, retryResponse.Body.String())
	}
	if _, err := service.Authenticate(session.Token); err == nil {
		t.Fatal("session remained valid after successful logout retry")
	}
}
