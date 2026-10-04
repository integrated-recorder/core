package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
)

type setupControllerFixture struct {
	validated   int
	activated   int
	validateErr error
	readyErr    error
	order       []string
}

func (f *setupControllerFixture) ValidateInstallation(context.Context) error {
	f.validated++
	f.order = append(f.order, "validate")
	return f.validateErr
}

func (f *setupControllerFixture) InstallationReady(context.Context) error {
	f.activated++
	f.order = append(f.order, "ready")
	return f.readyErr
}

func setupFixture(t *testing.T) (http.Handler, *installation.Store, authFixture, *setupControllerFixture, string) {
	t.Helper()
	base := os.TempDir()
	if resolved, resolveErr := filepath.EvalSymlinks(base); resolveErr == nil {
		base = resolved
	}
	root, err := os.MkdirTemp(base, "runtime-setup-api-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	store, err := installation.Reconcile(root, installation.AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(strings.TrimSpace(string(token)), "runtime-setup-api-password"); err != nil {
		t.Fatal(err)
	}
	session, err := service.Login("runtime-setup-api-password")
	if err != nil {
		t.Fatal(err)
	}
	controller := &setupControllerFixture{}
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	handler, err := NewWithSetup(service, false, store, buildinfo.Info{Version: "1.2.3", ReleaseChannel: "stable"}, controller, fallback)
	if err != nil {
		t.Fatal(err)
	}
	return handler, store, authFixture{service: service, session: session}, controller, root
}

func setupRequest(t *testing.T, handler http.Handler, fixture authFixture, method, path, body string, csrf bool) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: fixture.session.Token})
	if csrf {
		request.Header.Set("X-CSRF-Token", fixture.session.CSRFToken)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestSetupStatusIsBoundedAndDoesNotExposeCredentialsOrPaths(t *testing.T) {
	handler, store, _, _, root := setupFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, SetupEndpoint+"/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got SetupStatus
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != installation.StateUninitialized || !got.AdministratorConfigured || got.ClaimRequired || got.RecoveryRequired || got.Version != "1.2.3" || got.ReleaseChannel != "stable" {
		t.Fatalf("unexpected status: %+v", got)
	}
	if strings.Contains(response.Body.String(), root) || strings.Contains(response.Body.String(), "bootstrap-token") || strings.Contains(response.Body.String(), "runtime/") {
		t.Fatalf("setup status leaked a path or credential reference: %s", response.Body.String())
	}
	if store.Snapshot().State != installation.StateUninitialized {
		t.Fatal("GET status changed installation state")
	}
}

func TestSetupBeginAndCompleteRequireSessionAndCSRFAndOrderValidation(t *testing.T) {
	handler, store, fixture, controller, _ := setupFixture(t)
	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodPost, SetupEndpoint+"/begin", strings.NewReader("{}")))
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated begin status=%d", unauthenticated.Code)
	}
	if response := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/begin", "{}", false); response.Code != http.StatusForbidden {
		t.Fatalf("begin without CSRF status=%d body=%s", response.Code, response.Body.String())
	}
	if response := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/begin", `{"unexpected":true}`, true); response.Code != http.StatusBadRequest {
		t.Fatalf("begin with non-empty body status=%d", response.Code)
	}
	begin := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/begin", `{}`, true)
	if begin.Code != http.StatusOK || store.Snapshot().State != installation.StateSetupInProgress {
		t.Fatalf("begin status=%d state=%s body=%s", begin.Code, store.Snapshot().State, begin.Body.String())
	}
	if response := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/complete", `{}`, false); response.Code != http.StatusForbidden {
		t.Fatalf("complete without CSRF status=%d", response.Code)
	}
	complete := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/complete", `{}`, true)
	if complete.Code != http.StatusOK || store.Snapshot().State != installation.StateReady {
		t.Fatalf("complete status=%d state=%s body=%s", complete.Code, store.Snapshot().State, complete.Body.String())
	}
	if controller.validated != 1 || controller.activated != 1 || strings.Join(controller.order, ",") != "validate,ready" {
		t.Fatalf("controller calls/order=%+v", controller)
	}
	if again := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/complete", `{}`, true); again.Code != http.StatusOK {
		t.Fatalf("idempotent completion status=%d body=%s", again.Code, again.Body.String())
	}
	if controller.validated != 1 || controller.activated != 2 {
		t.Fatalf("idempotent activation counts validate=%d ready=%d", controller.validated, controller.activated)
	}
}

func TestSetupCompletionDoesNotCommitWhenControlValidationFails(t *testing.T) {
	handler, store, fixture, controller, _ := setupFixture(t)
	if _, err := store.Begin(true); err != nil {
		t.Fatal(err)
	}
	controller.validateErr = context.DeadlineExceeded
	response := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/complete", `{}`, true)
	if response.Code != http.StatusConflict || store.Snapshot().State != installation.StateSetupInProgress || controller.activated != 0 {
		t.Fatalf("failed validation response=%d state=%s calls=%+v body=%s", response.Code, store.Snapshot().State, controller, response.Body.String())
	}
}

func TestSetupCompletionReadySignalFailureIsRetryableAfterDurableCommit(t *testing.T) {
	handler, store, fixture, controller, _ := setupFixture(t)
	if _, err := store.Begin(true); err != nil {
		t.Fatal(err)
	}
	controller.readyErr = context.DeadlineExceeded
	first := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/complete", `{}`, true)
	if first.Code != http.StatusServiceUnavailable || store.Snapshot().State != installation.StateReady {
		t.Fatalf("activation failure response=%d state=%s body=%s", first.Code, store.Snapshot().State, first.Body.String())
	}
	controller.readyErr = nil
	second := setupRequest(t, handler, fixture, http.MethodPost, SetupEndpoint+"/complete", `{}`, true)
	if second.Code != http.StatusOK || controller.validated != 1 || controller.activated != 2 {
		t.Fatalf("retry response=%d calls=%+v body=%s", second.Code, controller, second.Body.String())
	}
}

func TestSetupIncompleteBlocksUpdateMutationsButAllowsReadCheck(t *testing.T) {
	handler, _, fixture, _, _ := setupFixture(t)
	for _, path := range []string{stagePath, activatePath, rollbackPath} {
		response := setupRequest(t, handler, fixture, http.MethodPost, path, `{}`, true)
		if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"code":"installation_incomplete"`) {
			t.Errorf("POST %s response=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	if response := setupRequest(t, handler, fixture, http.MethodPost, checkPath, `{}`, true); response.Code != http.StatusAccepted {
		t.Fatalf("read-only update check fallback status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSetupStatusHasOnlyFrozenPublicFields(t *testing.T) {
	handler, _, _, _, _ := setupFixture(t)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, SetupEndpoint+"/status", nil))
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{"state", "administrator_configured", "claim_required", "recovery_required", "auth_disabled", "version", "release_channel"}
	for _, key := range want {
		if _, ok := fields[key]; !ok {
			t.Errorf("status omitted %q", key)
		}
	}
	for key := range fields {
		found := false
		for _, allowed := range append(want, "diagnostic_code") {
			if key == allowed {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("unexpected public status field %q", key)
		}
	}
}
