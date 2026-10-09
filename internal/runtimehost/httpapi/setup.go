package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
)

const SetupEndpoint = "/api/setup"

// InstallationController performs only the Host -> active Control lifecycle
// calls required to validate a setup and activate its background services.
type InstallationController interface {
	ValidateInstallation(context.Context) error
	InstallationReady(context.Context) error
}

// NewWithSetup adds Host-owned setup lifecycle endpoints while keeping the
// existing update handler/fallback chain intact.
func NewWithSetup(auth *authn.Service, authDisabled bool, store *installation.Store, build buildinfo.Info, controller InstallationController, next http.Handler) (http.Handler, error) {
	if store == nil || controller == nil || next == nil || (!authDisabled && auth == nil) {
		return nil, errors.New("installation API dependencies are incomplete")
	}
	audit, _ := controller.(AuditAppender)
	return &setupHandler{auth: auth, authDisabled: authDisabled, store: store, build: build, controller: controller, audit: audit, next: next}, nil
}

type setupHandler struct {
	auth         *authn.Service
	authDisabled bool
	store        *installation.Store
	build        buildinfo.Info
	controller   InstallationController
	audit        AuditAppender
	next         http.Handler
}

type SetupStatus struct {
	State                   installation.State `json:"state"`
	AdministratorConfigured bool               `json:"administrator_configured"`
	ClaimRequired           bool               `json:"claim_required"`
	RecoveryRequired        bool               `json:"recovery_required"`
	AuthDisabled            bool               `json:"auth_disabled"`
	Version                 string             `json:"version"`
	ReleaseChannel          string             `json:"release_channel"`
	DiagnosticCode          string             `json:"diagnostic_code,omitempty"`
}

type setupError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *setupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if updateMutationPath(r.Method, r.URL.Path) && !h.store.Ready() {
		writeSetupError(w, http.StatusConflict, "installation_incomplete", "설치 설정을 완료한 뒤 이용할 수 있습니다.")
		return
	}
	if r.URL.Path == SetupEndpoint || r.URL.Path == SetupEndpoint+"/status" {
		if r.Method != http.MethodGet {
			writeSetupError(w, http.StatusMethodNotAllowed, "method_not_allowed", "요청을 처리할 수 없습니다.")
			return
		}
		writeSetupJSON(w, http.StatusOK, h.status())
		return
	}
	if r.URL.Path == SetupEndpoint+"/begin" {
		if r.Method != http.MethodPost {
			writeSetupError(w, http.StatusMethodNotAllowed, "method_not_allowed", "요청을 처리할 수 없습니다.")
			return
		}
		var authorized bool
		r, authorized = h.authorize(w, r)
		if !authorized || !validateSetupEmptyBody(w, r) {
			return
		}
		status := h.status()
		snapshot, err := h.store.Begin(status.AdministratorConfigured || h.authDisabled)
		if err != nil {
			writeSetupError(w, http.StatusConflict, safeInstallationCode(err), "설치 단계를 시작할 수 없습니다.")
			return
		}
		appendRuntimeAuditBestEffort(w, r, h.audit, controlplane.AuditInstallationSetupBegun, "installation")
		writeSetupJSON(w, http.StatusOK, h.statusFrom(snapshot))
		return
	}
	if r.URL.Path == SetupEndpoint+"/complete" {
		if r.Method != http.MethodPost {
			writeSetupError(w, http.StatusMethodNotAllowed, "method_not_allowed", "요청을 처리할 수 없습니다.")
			return
		}
		var authorized bool
		r, authorized = h.authorize(w, r)
		if !authorized || !validateSetupEmptyBody(w, r) {
			return
		}
		state := h.store.Snapshot().State
		if state == installation.StateRecoveryRequired {
			writeSetupError(w, http.StatusConflict, "installation_recovery_required", "설치 상태를 복구해야 합니다.")
			return
		}
		if state != installation.StateReady {
			if err := h.controller.ValidateInstallation(r.Context()); err != nil {
				writeSetupError(w, http.StatusConflict, "installation_validation_failed", "설치 검증을 통과하지 못했습니다.")
				return
			}
			if _, err := h.store.Complete(); err != nil {
				writeSetupError(w, http.StatusConflict, safeInstallationCode(err), "설치 완료 상태를 저장하지 못했습니다.")
				return
			}
			appendRuntimeAuditBestEffort(w, r, h.audit, controlplane.AuditInstallationSetupCompleted, "installation")
		}
		// This call is idempotent and is repeated on a ready-state retry. If a
		// crash happened after the durable commit, Host/Control startup also
		// reconciles service activation from installation.json.
		if err := h.controller.InstallationReady(r.Context()); err != nil {
			writeSetupError(w, http.StatusServiceUnavailable, "installation_activation_pending", "설치는 저장되었으며 운영 서비스를 시작하는 중입니다. 다시 시도해 주세요.")
			return
		}
		writeSetupJSON(w, http.StatusOK, h.status())
		return
	}
	h.next.ServeHTTP(w, r)
}

func (h *setupHandler) status() SetupStatus {
	return h.statusFrom(h.store.Snapshot())
}

func (h *setupHandler) statusFrom(snapshot installation.Snapshot) SetupStatus {
	adminConfigured := h.authDisabled
	if !h.authDisabled && h.auth != nil {
		configured, err := h.auth.AdministratorConfigured()
		adminConfigured = err == nil && configured
	}
	diagnostic := snapshot.DiagnosticCode
	return SetupStatus{
		State: snapshot.State, AdministratorConfigured: adminConfigured,
		ClaimRequired:    !h.authDisabled && !adminConfigured && snapshot.State == installation.StateUninitialized,
		RecoveryRequired: snapshot.State == installation.StateRecoveryRequired,
		AuthDisabled:     h.authDisabled, Version: safeBuildVersion(h.build.Version),
		ReleaseChannel: safeReleaseChannel(h.build.ReleaseChannel), DiagnosticCode: diagnostic,
	}
}

func safeBuildVersion(value string) string {
	if len(value) == 0 || len(value) > 128 {
		return "unknown"
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || stringsContains(".+_-", r)) {
			return "unknown"
		}
	}
	return value
}

func safeReleaseChannel(value string) string {
	switch value {
	case "stable", "prerelease", "development":
		return value
	default:
		return "development"
	}
}

func stringsContains(values string, target rune) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func updateMutationPath(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	switch path {
	case stagePath, activatePath, rollbackPath:
		return true
	default:
		return false
	}
}

func (h *setupHandler) authorize(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if h.authDisabled {
		return r, true
	}
	token := authn.SessionToken(r)
	session, err := h.auth.Authenticate(token)
	if err != nil {
		writeSetupError(w, http.StatusUnauthorized, "authentication_required", "인증이 필요합니다.")
		return r, false
	}
	principal := authn.Principal{UserID: session.UserID, Login: session.Login, Role: session.Role}
	if !authn.HasPermission(principal, authn.PermissionSettingsManage) {
		writeSetupError(w, http.StatusForbidden, "permission_denied", "권한이 없습니다.")
		return r, false
	}
	if !h.auth.ValidCSRF(token, r.Header.Get("X-CSRF-Token")) {
		writeSetupError(w, http.StatusForbidden, "csrf_rejected", "요청 검증에 실패했습니다.")
		return r, false
	}
	return r.WithContext(authn.WithPrincipal(r.Context(), principal)), true
}

func validateSetupEmptyBody(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil || r.ContentLength > MaxRequestBody {
		writeSetupError(w, http.StatusBadRequest, "invalid_request", "요청 형식이 올바르지 않습니다.")
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRequestBody))
	if err != nil {
		writeSetupError(w, http.StatusBadRequest, "invalid_request", "요청 형식이 올바르지 않습니다.")
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil || fields == nil || len(fields) != 0 {
		writeSetupError(w, http.StatusBadRequest, "invalid_request", "요청 형식이 올바르지 않습니다.")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeSetupError(w, http.StatusBadRequest, "invalid_request", "요청 형식이 올바르지 않습니다.")
		return false
	}
	return true
}

func safeInstallationCode(err error) string {
	switch {
	case errors.Is(err, installation.ErrRecoveryRequired):
		return "installation_recovery_required"
	case errors.Is(err, installation.ErrAlreadyReady):
		return "installation_already_ready"
	default:
		return "installation_transition_rejected"
	}
}

func writeSetupJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeSetupError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(setupError{Code: code, Message: message})
}
