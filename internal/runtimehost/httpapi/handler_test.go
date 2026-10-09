package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/plugintrust"
)

type fakeAuditAppender struct {
	requests []controlplane.AuditAppendRequest
	err      error
}

func (f *fakeAuditAppender) AppendAudit(_ context.Context, request controlplane.AuditAppendRequest) error {
	f.requests = append(f.requests, request)
	return f.err
}

type fakeController struct {
	status        Status
	err           error
	adapterResult AdapterReconcileResult
	adapterErr    error
	pluginStatus  PluginStatus
	pluginErr     error
	calls         map[string]int
	principal     authn.Principal
}

func (f *fakeController) call(ctx context.Context, name string) (Status, error) {
	if principal, ok := authn.PrincipalFromContext(ctx); ok {
		f.principal = principal
	}
	if f.calls == nil {
		f.calls = make(map[string]int)
	}
	f.calls[name]++
	return f.status, f.err
}

func (f *fakeController) Status(ctx context.Context) (Status, error) { return f.call(ctx, "status") }
func (f *fakeController) Check(ctx context.Context) (Status, error)  { return f.call(ctx, "check") }
func (f *fakeController) Stage(ctx context.Context) (Status, error)  { return f.call(ctx, "stage") }
func (f *fakeController) Activate(ctx context.Context) (Status, error) {
	return f.call(ctx, "activate")
}
func (f *fakeController) Rollback(ctx context.Context) (Status, error) {
	return f.call(ctx, "rollback")
}
func (f *fakeController) ReconcileAdapters(context.Context) (AdapterReconcileResult, error) {
	if f.calls == nil {
		f.calls = make(map[string]int)
	}
	f.calls["reconcile_adapters"]++
	return f.adapterResult, f.adapterErr
}
func (f *fakeController) pluginCall(ctx context.Context, name string) (PluginStatus, error) {
	if principal, ok := authn.PrincipalFromContext(ctx); ok {
		f.principal = principal
	}
	if f.calls == nil {
		f.calls = make(map[string]int)
	}
	f.calls[name]++
	return f.pluginStatus, f.pluginErr
}
func (f *fakeController) PluginStatus(ctx context.Context) (PluginStatus, error) {
	return f.pluginCall(ctx, "plugin_status")
}
func (f *fakeController) RefreshPlugins(ctx context.Context) (PluginStatus, error) {
	return f.pluginCall(ctx, "plugin_refresh")
}
func (f *fakeController) InstallPlugin(ctx context.Context, id string) (PluginStatus, error) {
	if id != "demo" {
		return PluginStatus{}, NewControllerError("plugin_not_found")
	}
	return f.pluginCall(ctx, "plugin_install")
}
func (f *fakeController) UpdatePlugin(ctx context.Context, id string) (PluginStatus, error) {
	if id != "demo" {
		return PluginStatus{}, NewControllerError("plugin_not_found")
	}
	return f.pluginCall(ctx, "plugin_update")
}
func (f *fakeController) UninstallPlugin(ctx context.Context, id string) (PluginStatus, error) {
	if id != "demo" {
		return PluginStatus{}, NewControllerError("plugin_not_found")
	}
	return f.pluginCall(ctx, "plugin_uninstall")
}

type authFixture struct {
	service *authn.Service
	session authn.Session
}

var (
	fixtureOnce sync.Once
	fixture     authFixture
	fixtureErr  error
)

func testAuth(t *testing.T) authFixture {
	t.Helper()
	fixtureOnce.Do(func() {
		root, err := os.MkdirTemp("", "runtime-httpapi-auth-")
		if err != nil {
			fixtureErr = err
			return
		}
		service, err := authn.Open(root)
		if err != nil {
			fixtureErr = err
			return
		}
		bootstrapPath := filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath()))
		token, err := os.ReadFile(bootstrapPath)
		if err != nil {
			fixtureErr = err
			return
		}
		if err := service.Bootstrap(strings.TrimSpace(string(token)), "runtime-httpapi-test-password"); err != nil {
			fixtureErr = err
			return
		}
		session, err := service.Login("runtime-httpapi-test-password")
		if err != nil {
			fixtureErr = err
			return
		}
		fixture = authFixture{service: service, session: session}
	})
	if fixtureErr != nil {
		t.Fatalf("initialize auth fixture: %v", fixtureErr)
	}
	return fixture
}

func validStatus() Status {
	return Status{
		Host:                    BuildIdentity{Version: "1.2.3", Commit: strings.Repeat("a", 40), BuildTime: "2026-09-30T01:02:03Z", ReleaseChannel: "stable", RuntimeProtocolVersion: 1},
		Application:             BuildIdentity{Version: "1.2.3", Commit: strings.Repeat("a", 40), BuildTime: "2026-09-30T01:02:03Z", ReleaseChannel: "stable", RuntimeProtocolVersion: 1},
		ActiveControl:           &GenerationSummary{ID: strings.Repeat("a", 32), Version: "1.2.3", Commit: strings.Repeat("a", 40), InstalledAt: time.Date(2026, 9, 30, 1, 2, 3, 0, time.UTC), State: "active", ActiveRecordings: 1},
		DefaultEngine:           &GenerationSummary{ID: strings.Repeat("a", 32), Version: "1.2.3", Commit: strings.Repeat("a", 40), InstalledAt: time.Date(2026, 9, 30, 1, 2, 3, 0, time.UTC), State: "active", ActiveRecordings: 1},
		ActiveGenerations:       []GenerationSummary{},
		DrainingGenerations:     []GenerationSummary{},
		VerificationState:       "verified",
		UpdateUnavailableReason: "",
	}
}

func TestRoutesDispatchAndFallback(t *testing.T) {
	controller := &fakeController{status: validStatus(), adapterResult: AdapterReconcileResult{State: "activated", ActiveAdapterCount: 2, GenerationID: strings.Repeat("b", 32)}}
	fallbackCalls := 0
	api, err := New(nil, true, false, controller, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.WriteHeader(http.StatusTeapot)
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodGet, Endpoint, http.StatusOK},
		{http.MethodPost, checkPath, http.StatusOK},
		{http.MethodPost, stagePath, http.StatusOK},
		{http.MethodPost, activatePath, http.StatusOK},
		{http.MethodPost, rollbackPath, http.StatusOK},
		{http.MethodPost, AdaptersEndpoint, http.StatusOK},
		{http.MethodPost, Endpoint, http.StatusTeapot},
		{http.MethodGet, checkPath, http.StatusTeapot},
		{http.MethodGet, Endpoint + "/unknown", http.StatusTeapot},
	} {
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
		if recorder.Code != tc.want {
			t.Errorf("%s %s status = %d, want %d", tc.method, tc.path, recorder.Code, tc.want)
		}
	}
	if fallbackCalls != 3 {
		t.Fatalf("fallback calls = %d, want 3", fallbackCalls)
	}
	for _, name := range []string{"status", "check", "stage", "activate", "rollback", "reconcile_adapters"} {
		if controller.calls[name] != 1 {
			t.Errorf("controller %s calls = %d, want 1", name, controller.calls[name])
		}
	}
}

func TestRuntimeMutationAuditUsesPrincipalAndDoesNotFailPrimaryMutation(t *testing.T) {
	fixture := testAuth(t)
	audit := &fakeAuditAppender{err: errors.New("private storage failure")}
	controller := &fakeController{status: validStatus()}
	api, err := NewWithAudit(fixture.service, false, false, controller, http.NotFoundHandler(), audit)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, stagePath, nil)
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: fixture.session.Token})
	request.Header.Set("X-CSRF-Token", fixture.session.CSRFToken)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("audit failure changed successful primary status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Audit-Status") != "failed" {
		t.Fatalf("audit status=%q", response.Header().Get("X-Audit-Status"))
	}
	if len(audit.requests) != 1 {
		t.Fatalf("audit requests=%+v", audit.requests)
	}
	got := audit.requests[0]
	if got.Action != controlplane.AuditRuntimeUpdateStaged || got.ObjectID != "runtime-update" || got.ActorType != "user" || got.UserID != fixture.session.UserID {
		t.Fatalf("authenticated audit request=%+v", got)
	}
}

func TestRuntimeMutationAuditUsesSystemActorWithoutPrincipal(t *testing.T) {
	audit := &fakeAuditAppender{}
	controller := &fakeController{status: validStatus()}
	api, err := NewWithAudit(nil, true, false, controller, http.NotFoundHandler(), audit)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, rollbackPath, nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Audit-Status") != "recorded" {
		t.Fatalf("system audit response=%d audit=%q body=%s", response.Code, response.Header().Get("X-Audit-Status"), response.Body.String())
	}
	if len(audit.requests) != 1 || audit.requests[0].ActorType != "system" || audit.requests[0].UserID != "" || audit.requests[0].Action != controlplane.AuditRuntimeUpdateRolledBack {
		t.Fatalf("system audit request=%+v", audit.requests)
	}
}

func TestPluginRoutesDispatchAndRejectMalformedIdentifiers(t *testing.T) {
	controller := &fakeController{pluginStatus: PluginStatus{State: "ready", Plugins: []PluginStatusItem{{ID: "demo", Name: "Demo", AvailableVersion: "1.2.0", Installed: false, Trust: plugintrust.NewCustomRegistry()}}}}
	api, err := New(nil, true, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{http.MethodGet, PluginsEndpoint, "", http.StatusOK},
		{http.MethodPost, PluginsEndpoint + "/refresh", `{}`, http.StatusOK},
		{http.MethodPost, PluginsEndpoint + "/demo/install", `{}`, http.StatusOK},
		{http.MethodPost, PluginsEndpoint + "/demo/update", `{}`, http.StatusOK},
		{http.MethodDelete, PluginsEndpoint + "/demo", `{}`, http.StatusOK},
		{http.MethodPost, PluginsEndpoint + "/../install", `{}`, http.StatusNotFound},
		{http.MethodPost, PluginsEndpoint + "/demo/install", `{"url":"https://example.invalid/plugin"}`, http.StatusBadRequest},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		api.ServeHTTP(recorder, request)
		if recorder.Code != tc.want {
			t.Errorf("%s %s status = %d, want %d (%s)", tc.method, tc.path, recorder.Code, tc.want, recorder.Body.String())
		}
	}
	for _, name := range []string{"plugin_status", "plugin_refresh", "plugin_install", "plugin_update", "plugin_uninstall"} {
		if controller.calls[name] != 1 {
			t.Errorf("controller %s calls = %d, want 1", name, controller.calls[name])
		}
	}
}

func TestPluginStatusAcceptsMaximumRegistryVersionLength(t *testing.T) {
	version := strings.Repeat("v", 128)
	status := PluginStatus{State: "ready", Plugins: []PluginStatusItem{{
		ID: "demo", Name: "Demo", AvailableVersion: version, InstalledVersion: "v", Installed: true, UpdateAvailable: true,
		Trust: plugintrust.NewOperator(),
	}}}
	if err := status.Validate(); err != nil {
		t.Fatalf("valid bounded registry version rejected: %v", err)
	}
}

func TestPluginStatusRejectsInvalidTrustTuple(t *testing.T) {
	status := PluginStatus{State: "ready", Plugins: []PluginStatusItem{{
		ID: "demo", Name: "Demo", Trust: plugintrust.Attestation{Provenance: plugintrust.Registry, Authority: plugintrust.Custom, Publisher: plugintrust.FirstParty, Reviewed: true},
	}}}
	if err := status.Validate(); err == nil {
		t.Fatal("invalid registry/custom first-party trust tuple accepted")
	}
}

func TestPluginMutationRequiresAuthenticationAndCSRF(t *testing.T) {
	fixture := testAuth(t)
	controller := &fakeController{pluginStatus: PluginStatus{State: "ready", Plugins: []PluginStatusItem{}}}
	api, err := New(fixture.service, false, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	request := func(token, csrf string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, PluginsEndpoint+"/refresh", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: token})
		}
		if csrf != "" {
			req.Header.Set("X-CSRF-Token", csrf)
		}
		api.ServeHTTP(recorder, req)
		return recorder
	}
	if got := request("", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("anonymous refresh status = %d", got)
	}
	if got := request(fixture.session.Token, "bad").Code; got != http.StatusForbidden {
		t.Fatalf("bad CSRF refresh status = %d", got)
	}
	if got := request(fixture.session.Token, fixture.session.CSRFToken).Code; got != http.StatusOK {
		t.Fatalf("authenticated refresh status = %d", got)
	}
	if controller.principal.UserID != fixture.session.UserID || controller.principal.Role != authn.RoleOwner {
		t.Fatalf("plugin controller received principal=%+v, want authenticated owner %q", controller.principal, fixture.session.UserID)
	}
}

func TestStatusGETRequiresAuthenticationButNotCSRF(t *testing.T) {
	fixture := testAuth(t)
	controller := &fakeController{status: validStatus()}
	api, err := New(fixture.service, false, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	unauthenticated := httptest.NewRecorder()
	api.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, Endpoint, nil))
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET status = %d", unauthenticated.Code)
	}

	request := httptest.NewRequest(http.MethodGet, Endpoint, nil)
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: fixture.session.Token})
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authenticated GET without CSRF status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var got Status
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if got.Host.Version != "1.2.3" || got.ActiveControl == nil || got.ActiveControl.ActiveRecordings != 1 {
		t.Fatalf("unexpected status DTO: %+v", got)
	}
	if strings.Contains(recorder.Body.String(), "/") && strings.Contains(recorder.Body.String(), "tmp") {
		t.Fatalf("status leaked path-like content: %s", recorder.Body.String())
	}
}

func TestEveryPostRequiresAuthenticationAndCSRF(t *testing.T) {
	fixture := testAuth(t)
	controller := &fakeController{status: validStatus(), adapterResult: AdapterReconcileResult{State: "unchanged"}}
	api, err := New(fixture.service, false, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{checkPath, stagePath, activatePath, rollbackPath, AdaptersEndpoint} {
		unauthenticated := httptest.NewRecorder()
		api.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodPost, path, nil))
		if unauthenticated.Code != http.StatusUnauthorized {
			t.Errorf("POST %s without auth status = %d", path, unauthenticated.Code)
		}

		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: fixture.session.Token})
		noCSRF := httptest.NewRecorder()
		api.ServeHTTP(noCSRF, request)
		if noCSRF.Code != http.StatusForbidden {
			t.Errorf("POST %s without CSRF status = %d", path, noCSRF.Code)
		}

		request = httptest.NewRequest(http.MethodPost, path, nil)
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: fixture.session.Token})
		request.Header.Set("X-CSRF-Token", fixture.session.CSRFToken)
		allowed := httptest.NewRecorder()
		api.ServeHTTP(allowed, request)
		if allowed.Code != http.StatusOK {
			t.Errorf("POST %s with auth+CSRF status = %d, body=%s", path, allowed.Code, allowed.Body.String())
		}
	}
}

func TestAdapterReconcileRouteReturnsOnlySafeProjection(t *testing.T) {
	controller := &fakeController{adapterResult: AdapterReconcileResult{State: "rejected", ActiveAdapterCount: 1, RejectedCount: 2}}
	api, err := New(nil, true, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, AdaptersEndpoint, strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("reconcile status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var got AdapterReconcileResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != "rejected" || got.ActiveAdapterCount != 1 || got.RejectedCount != 2 {
		t.Fatalf("unexpected reconciliation result: %+v", got)
	}
	for _, forbidden := range []string{"/tmp", "sha256", "descriptor", "pid", "credential"} {
		if strings.Contains(strings.ToLower(recorder.Body.String()), forbidden) {
			t.Fatalf("reconcile response leaked %q: %s", forbidden, recorder.Body.String())
		}
	}
}

func TestAdapterReconcileRoutePreservesRejectedCountAfterActivation(t *testing.T) {
	controller := &fakeController{adapterResult: AdapterReconcileResult{
		State: "activated", ActiveAdapterCount: 2, RejectedCount: 1, GenerationID: strings.Repeat("b", 32),
	}}
	api, err := New(nil, true, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, AdaptersEndpoint, strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	api.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("reconcile status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var got AdapterReconcileResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != "activated" || got.ActiveAdapterCount != 2 || got.RejectedCount != 1 || got.GenerationID != strings.Repeat("b", 32) {
		t.Fatalf("partial activation result was not preserved: %+v", got)
	}
}

func TestAdapterReconcileSetupIncompleteUsesSafeConflict(t *testing.T) {
	controller := &fakeController{adapterErr: NewControllerError("installation_incomplete")}
	api, err := New(nil, true, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, AdaptersEndpoint, nil))
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), `"code":"installation_incomplete"`) {
		t.Fatalf("setup-incomplete reconcile response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestPostBodyIsEmptyOrEmptyJSONObjectAndBounded(t *testing.T) {
	controller := &fakeController{status: validStatus()}
	api, err := New(nil, true, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body        string
		contentType string
		want        int
	}{
		{"", "", http.StatusOK},
		{" \n\t", "", http.StatusOK},
		{"{}", "application/json", http.StatusOK},
		{" { } ", "application/json; charset=utf-8", http.StatusOK},
		{`{"activate":true}`, "application/json", http.StatusBadRequest},
		{"[]", "application/json", http.StatusBadRequest},
		{"{}", "text/plain", http.StatusBadRequest},
		{strings.Repeat(" ", MaxRequestBody+1), "", http.StatusBadRequest},
	} {
		request := httptest.NewRequest(http.MethodPost, stagePath, strings.NewReader(tc.body))
		if tc.contentType != "" {
			request.Header.Set("Content-Type", tc.contentType)
		}
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, request)
		if recorder.Code != tc.want {
			t.Errorf("body %q content-type %q status = %d, want %d", tc.body[:min(len(tc.body), 32)], tc.contentType, recorder.Code, tc.want)
		}
	}
	request := httptest.NewRequest(http.MethodPost, stagePath, strings.NewReader("{}"))
	request.ContentLength = MaxRequestBody + 1
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized declared body status = %d", recorder.Code)
	}
}

func TestControllerInvocationAndErrorMapping(t *testing.T) {
	controller := &fakeController{status: validStatus()}
	api, err := New(nil, true, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	api.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, checkPath, nil))
	if controller.calls["check"] != 1 {
		t.Fatalf("check calls = %d", controller.calls["check"])
	}

	controller.err = NewControllerError("verification_failed")
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, stagePath, nil))
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), "verification_failed") {
		t.Fatalf("safe controller error response = %d %s", recorder.Code, recorder.Body.String())
	}

	controller.err = errors.New("open /private/runtime/token?signature=sensitive failed")
	recorder = httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, stagePath, nil))
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "private") || strings.Contains(recorder.Body.String(), "sensitive") {
		t.Fatalf("arbitrary controller error was exposed: %d %s", recorder.Code, recorder.Body.String())
	}

	controller.err = &ControllerError{StatusCode: http.StatusInternalServerError, Code: "internal_error", Message: "/tmp/key"}
	recorder = httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, stagePath, nil))
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "/tmp/key") {
		t.Fatalf("malformed typed controller error was exposed: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestUnsafeStatusRejectedAndHeadersAlwaysSet(t *testing.T) {
	controller := &fakeController{status: validStatus()}
	api, err := New(nil, true, true, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	controller.status.Application.Version = "/data/runtime/releases/secret"
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, Endpoint, nil))
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "/data") {
		t.Fatalf("unsafe status was exposed: %d %s", recorder.Code, recorder.Body.String())
	}
	assertJSONSecurityHeaders(t, recorder)

	controller.status = validStatus()
	controller.status.AvailableRelease = &ReleaseSummary{
		Version: "1.3.0", Commit: strings.Repeat("b", 40), BuildTime: "2026-09-30T01:02:03Z",
		ReleaseChannel: "stable", NotesSummary: "literal <img src=x> release text",
	}
	controller.status.UpdatesAvailable = true
	recorder = httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, Endpoint, nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `\u003cimg`) {
		t.Fatalf("bounded release summary was not safely JSON-encoded: %d %s", recorder.Code, recorder.Body.String())
	}
	controller.status.AvailableRelease.NotesSummary = strings.Repeat("x", 513)
	recorder = httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, Endpoint, nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("oversized release summary status = %d, want 500", recorder.Code)
	}
	controller.status = validStatus()
	recorder = httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, Endpoint, nil))
	assertJSONSecurityHeaders(t, recorder)
}

func TestInvalidDependenciesRejected(t *testing.T) {
	controller := &fakeController{status: validStatus()}
	if _, err := New(nil, false, false, controller, http.NotFoundHandler()); err == nil {
		t.Fatal("New accepted missing auth service in auth-enabled mode")
	}
	if _, err := New(nil, true, false, controller, nil); err == nil {
		t.Fatal("New accepted nil fallback")
	}
	if _, err := New(nil, true, false, nil, http.NotFoundHandler()); err == nil {
		t.Fatal("New accepted nil controller")
	}
}

func assertJSONSecurityHeaders(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if got := recorder.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
}
