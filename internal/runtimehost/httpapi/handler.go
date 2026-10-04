package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/dltkddnr04/integrated-recorder/internal/authn"
)

const (
	checkPath    = Endpoint + "/check"
	stagePath    = Endpoint + "/stage"
	activatePath = Endpoint + "/activate"
	rollbackPath = Endpoint + "/rollback"
)

// Controller is the host-owned update operation boundary. Implementations
// perform orchestration; the HTTP package only authenticates, validates,
// bounds, and projects their results.
type Controller interface {
	Status(context.Context) (Status, error)
	Check(context.Context) (Status, error)
	Stage(context.Context) (Status, error)
	Activate(context.Context) (Status, error)
	Rollback(context.Context) (Status, error)
}

// AdapterReconciler is implemented by Host controllers that can import a new
// immutable adapter set and activate an application generation for it.
type AdapterReconciler interface {
	ReconcileAdapters(context.Context) (AdapterReconcileResult, error)
}

// ControllerError is a deliberately small, safe error projection. Use
// NewControllerError so only predefined public code/message pairs are returned.
// Arbitrary controller errors and malformed ControllerErrors become a generic
// 500 response without logging or echoing their details.
type ControllerError struct {
	StatusCode int
	Code       string
	Message    string
}

type safeErrorDefinition struct {
	status  int
	message string
}

var safeControllerErrors = map[string]safeErrorDefinition{
	"invalid_request":                               {http.StatusBadRequest, "요청 형식이 올바르지 않습니다."},
	"update_unavailable":                            {http.StatusServiceUnavailable, "업데이트 기능을 사용할 수 없습니다."},
	"plugin_registry_unavailable":                   {http.StatusServiceUnavailable, "플러그인 레지스트리를 사용할 수 없습니다."},
	"plugin_not_found":                              {http.StatusNotFound, "플러그인을 찾을 수 없습니다."},
	"plugin_platform_unsupported":                   {http.StatusConflict, "이 플러그인은 현재 플랫폼을 지원하지 않습니다."},
	"plugin_download_failed":                        {http.StatusBadGateway, "플러그인을 다운로드하지 못했습니다."},
	"plugin_verification_failed":                    {http.StatusUnprocessableEntity, "다운로드한 플러그인을 검증하지 못했습니다."},
	"plugin_identity_mismatch":                      {http.StatusUnprocessableEntity, "플러그인 식별 정보가 레지스트리와 일치하지 않습니다."},
	"plugin_type_mismatch":                          {http.StatusUnprocessableEntity, "플러그인 종류가 설치 작업과 일치하지 않습니다."},
	"plugin_install_failed":                         {http.StatusBadGateway, "플러그인 설치를 완료하지 못했습니다."},
	"plugin_operation_conflict":                     {http.StatusConflict, "다른 Runtime 변경 작업이 진행 중입니다."},
	"storage_provider_not_installed":                {http.StatusNotFound, "스토리지 제공자를 찾을 수 없습니다."},
	"storage_provider_not_configured":               {http.StatusConflict, "스토리지 제공자 설정이 필요합니다."},
	"storage_provider_config_managed":               {http.StatusConflict, "이 번들 스토리지 제공자의 설정은 Runtime Host가 관리합니다."},
	"storage_provider_unavailable":                  {http.StatusServiceUnavailable, "스토리지 제공자를 사용할 수 없습니다."},
	"storage_provider_probe_failed":                 {http.StatusBadGateway, "스토리지 연결 검사를 완료하지 못했습니다."},
	"storage_provider_identity_mismatch":            {http.StatusUnprocessableEntity, "스토리지 제공자 식별 정보가 일치하지 않습니다."},
	"storage_provider_protocol_unsupported":         {http.StatusUnprocessableEntity, "지원하지 않는 스토리지 프로토콜입니다."},
	"storage_backend_in_use":                        {http.StatusConflict, "현재 보관소가 사용 중입니다."},
	"storage_backend_switch_requires_empty_archive": {http.StatusConflict, "기존 보관 데이터가 없는 경우에만 기본 스토리지를 변경할 수 있습니다."},
	"storage_operation_conflict":                    {http.StatusConflict, "다른 Runtime 변경 작업이 진행 중입니다."},
	"storage_activation_failed":                     {http.StatusBadGateway, "기본 스토리지를 활성화하지 못했습니다."},
	"update_check_failed":                           {http.StatusBadGateway, "업데이트 확인에 실패했습니다."},
	"no_update_available":                           {http.StatusNotFound, "사용 가능한 업데이트가 없습니다."},
	"operation_conflict":                            {http.StatusConflict, "다른 업데이트 작업이 진행 중입니다."},
	"verification_failed":                           {http.StatusUnprocessableEntity, "릴리스 검증에 실패했습니다."},
	"candidate_not_ready":                           {http.StatusConflict, "후보 릴리스가 준비되지 않았습니다."},
	"release_incompatible":                          {http.StatusConflict, "현재 실행 환경과 호환되지 않는 릴리스입니다."},
	"stage_failed":                                  {http.StatusBadGateway, "릴리스를 준비하지 못했습니다."},
	"activation_failed":                             {http.StatusBadGateway, "릴리스를 활성화하지 못했습니다."},
	"rollback_unavailable":                          {http.StatusConflict, "롤백할 릴리스가 없습니다."},
	"rollback_failed":                               {http.StatusBadGateway, "이전 릴리스로 롤백하지 못했습니다."},
	"installation_incomplete":                       {http.StatusConflict, "설치 설정을 완료한 뒤 이용할 수 있습니다."},
	"adapter_reconcile_failed":                      {http.StatusBadGateway, "어댑터를 다시 확인하지 못했습니다."},
	"internal_error":                                {http.StatusInternalServerError, "요청을 처리하지 못했습니다."},
}

// NewControllerError returns a predefined safe public error. Unknown codes
// produce nil so callers cannot accidentally create an unsafe error response.
func NewControllerError(code string) *ControllerError {
	definition, ok := safeControllerErrors[code]
	if !ok {
		return nil
	}
	return &ControllerError{StatusCode: definition.status, Code: code, Message: definition.message}
}

func (e *ControllerError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code
}

type handler struct {
	auth         *authn.Service
	authDisabled bool
	controller   Controller
	next         http.Handler
}

// New constructs the Runtime Host update API. authDisabled is intended only
// for bootstrap callers that have already applied their loopback-only policy.
func New(auth *authn.Service, authDisabled, forceSecureCookies bool, controller Controller, next http.Handler) (http.Handler, error) {
	// The flag is accepted to keep host construction aligned with the existing
	// auth policy. This API does not issue cookies; authn owns cookie attributes.
	_ = forceSecureCookies
	if (!authDisabled && auth == nil) || controller == nil || next == nil {
		return nil, errors.New("runtime update API dependencies are incomplete")
	}
	return &handler{
		auth: auth, authDisabled: authDisabled, controller: controller, next: next,
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if operation, id, ok := routeStorageOperation(r.Method, r.URL.Path); ok {
		h.serveStorageOperation(w, r, operation, id)
		return
	}
	if operation, id, ok := routePluginOperation(r.Method, r.URL.Path); ok {
		h.servePluginOperation(w, r, operation, id)
		return
	}
	operation, ok := routeOperation(r.Method, r.URL.Path)
	if !ok {
		h.next.ServeHTTP(w, r)
		return
	}
	setResponseHeaders(w)
	if !h.authorize(w, r, operation != operationStatus) {
		return
	}
	if operation != operationStatus {
		if err := validateEmptyBody(r); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
	}
	if operation == operationAdapterReconcile {
		reconciler, ok := h.controller.(AdapterReconciler)
		if !ok {
			writeAPIError(w, http.StatusServiceUnavailable, "adapter_reconcile_failed", safeControllerErrors["adapter_reconcile_failed"].message)
			return
		}
		result, err := reconciler.ReconcileAdapters(r.Context())
		if err != nil {
			writeControllerError(w, err)
			return
		}
		if err := result.Validate(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
		encoded, err := json.Marshal(result)
		if err != nil || len(encoded) > maxResponseBytes {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append(encoded, '\n'))
		return
	}

	var status Status
	var err error
	switch operation {
	case operationStatus:
		status, err = h.controller.Status(r.Context())
	case operationCheck:
		status, err = h.controller.Check(r.Context())
	case operationStage:
		status, err = h.controller.Stage(r.Context())
	case operationActivate:
		status, err = h.controller.Activate(r.Context())
	case operationRollback:
		status, err = h.controller.Rollback(r.Context())
	}
	if err != nil {
		writeControllerError(w, err)
		return
	}
	if err := status.Validate(); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	encoded, err := json.Marshal(status)
	if err != nil || len(encoded) > maxResponseBytes {
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(encoded, '\n'))
}

type operation uint8

const (
	operationUnknown operation = iota
	operationStatus
	operationCheck
	operationStage
	operationActivate
	operationRollback
	operationAdapterReconcile
)

func routeOperation(method, path string) (operation, bool) {
	switch {
	case method == http.MethodGet && path == Endpoint:
		return operationStatus, true
	case method == http.MethodPost && path == checkPath:
		return operationCheck, true
	case method == http.MethodPost && path == stagePath:
		return operationStage, true
	case method == http.MethodPost && path == activatePath:
		return operationActivate, true
	case method == http.MethodPost && path == rollbackPath:
		return operationRollback, true
	case method == http.MethodPost && path == AdaptersEndpoint:
		return operationAdapterReconcile, true
	default:
		return operationUnknown, false
	}
}

func (h *handler) authorize(w http.ResponseWriter, r *http.Request, mutation bool) bool {
	if h.authDisabled {
		return true
	}
	token := authn.SessionToken(r)
	if _, err := h.auth.Authenticate(token); err != nil {
		writeAPIError(w, http.StatusUnauthorized, "authentication_required", "인증이 필요합니다.")
		return false
	}
	if mutation && !h.auth.ValidCSRF(token, r.Header.Get("X-CSRF-Token")) {
		writeAPIError(w, http.StatusForbidden, "csrf_rejected", "요청 검증에 실패했습니다.")
		return false
	}
	return true
}

func validateEmptyBody(r *http.Request) error {
	if r.Body == nil {
		return nil
	}
	if r.ContentLength > MaxRequestBody {
		return errors.New("request body exceeds size limit")
	}
	reader := io.LimitReader(r.Body, MaxRequestBody+1)
	body, err := io.ReadAll(reader)
	if err != nil {
		return errors.New("request body could not be read")
	}
	if len(body) > MaxRequestBody {
		return errors.New("request body exceeds size limit")
	}
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("request body must be an empty JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil || len(fields) != 0 {
		return errors.New("request body must be an empty JSON object")
	}
	return nil
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeControllerError(w http.ResponseWriter, err error) {
	var controllerError *ControllerError
	if errors.As(err, &controllerError) && controllerError != nil {
		definition, ok := safeControllerErrors[controllerError.Code]
		if ok && controllerError.StatusCode == definition.status && controllerError.Message == definition.message {
			writeAPIError(w, definition.status, controllerError.Code, definition.message)
			return
		}
	}
	writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
}

func setResponseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func writeAPIError(w http.ResponseWriter, status int, code, message string) {
	encoded, err := json.Marshal(errorResponse{Code: code, Message: message})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}
