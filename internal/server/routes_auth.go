package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
)

const csrfCookieName = "ir_csrf"

func (s *Server) registerAuthRoutes() {
	s.mux.HandleFunc("GET /api/auth/session", s.authSession)
	s.mux.HandleFunc("POST /api/auth/bootstrap", s.authBootstrap)
	s.mux.HandleFunc("POST /api/auth/login", s.authLogin)
	s.mux.HandleFunc("POST /api/auth/logout", s.authLogout)
}

func (s *Server) authSession(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeJSON(w, http.StatusOK, map[string]any{"auth_enabled": false, "authenticated": true, "needs_bootstrap": false})
		return
	}
	if s.auth.NeedsBootstrap() {
		writeJSON(w, http.StatusOK, map[string]any{"auth_enabled": true, "authenticated": false, "needs_bootstrap": true})
		return
	}
	token := authn.SessionToken(r)
	session, err := s.auth.Authenticate(token)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"auth_enabled": true, "authenticated": false, "needs_bootstrap": false})
		return
	}
	csrf := ""
	if cookie, cookieErr := r.Cookie(csrfCookieName); cookieErr == nil {
		csrf = cookie.Value
	}
	writeJSON(w, http.StatusOK, map[string]any{"auth_enabled": true, "authenticated": true, "needs_bootstrap": false, "csrf_token": csrf, "expires_at": session.ExpiresAt})
}

type authCredentialRequest struct {
	Login    string `json:"login,omitempty"`
	Token    string `json:"token,omitempty"`
	Password string `json:"password"`
}

func (s *Server) authBootstrap(w http.ResponseWriter, r *http.Request) {
	if s.installationManaged {
		state := installationStateForServer(s)
		if state != installation.StateUninitialized {
			writeError(w, http.StatusConflict, "administrator setup is unavailable")
			return
		}
	}
	if s.auth == nil || !s.auth.NeedsBootstrap() {
		writeError(w, http.StatusConflict, "administrator setup is unavailable")
		return
	}
	var request authCredentialRequest
	if err := decodeJSONBody(w, r, 16<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid bootstrap request")
		return
	}
	if err := s.auth.Bootstrap(request.Token, request.Password); err != nil {
		writeError(w, http.StatusUnauthorized, "administrator setup was rejected")
		return
	}
	s.finishLogin(w, r, request.Password)
}

func (s *Server) authLogin(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeError(w, http.StatusNotFound, "authentication is disabled")
		return
	}
	var request authCredentialRequest
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid login request")
		return
	}
	s.finishLoginAs(w, r, request.Login, request.Password)
}

func (s *Server) finishLogin(w http.ResponseWriter, r *http.Request, password string) {
	s.finishLoginAs(w, r, "", password)
}

func (s *Server) finishLoginAs(w http.ResponseWriter, r *http.Request, login, password string) {
	var session authn.Session
	var err error
	if login == "" {
		session, err = s.auth.Login(password)
	} else {
		session, err = s.auth.LoginAs(login, password)
	}
	if err != nil {
		writeError(w, http.StatusUnauthorized, "login failed")
		return
	}
	authn.SetSessionCookie(w, r, session, s.forceSecureCookie)
	http.SetCookie(w, &http.Cookie{
		Name: csrfCookieName, Value: session.CSRFToken, Path: "/", Expires: session.ExpiresAt,
		MaxAge: maxInt(1, int(time.Until(session.ExpiresAt)/time.Second)), HttpOnly: false,
		Secure: (r.TLS != nil) || s.forceSecureCookie, SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, map[string]any{"auth_enabled": s.auth != nil, "authenticated": true, "needs_bootstrap": false, "csrf_token": session.CSRFToken, "expires_at": session.ExpiresAt})
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Server) authLogout(w http.ResponseWriter, r *http.Request) {
	if s.auth != nil {
		logout := s.logoutSession
		if logout == nil {
			logout = s.auth.Logout
		}
		if err := logout(authn.SessionToken(r)); err != nil {
			writeError(w, http.StatusServiceUnavailable, "logout could not be completed")
			return
		}
	}
	authn.ClearSessionCookie(w, r, s.forceSecureCookie)
	http.SetCookie(w, &http.Cookie{Name: csrfCookieName, Value: "", Path: "/", MaxAge: -1, Expires: time.Unix(1, 0), Secure: r.TLS != nil || s.forceSecureCookie, SameSite: http.SameSiteStrictMode})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authRoutePublic(r) {
			next.ServeHTTP(w, r)
			return
		}
		token := authn.SessionToken(r)
		session, err := s.auth.Authenticate(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			csrf := r.Header.Get("X-CSRF-Token")
			if csrf == "" || !s.auth.ValidCSRF(token, csrf) {
				writeError(w, http.StatusForbidden, "CSRF validation failed")
				return
			}
		}
		principal := authn.Principal{UserID: session.UserID, Login: session.Login, Role: session.Role}
		request := r.WithContext(authn.WithPrincipal(r.Context(), principal))
		authn.RequirePermission(permissionForRequest(r), next).ServeHTTP(w, request)
	})
}

func permissionForRequest(r *http.Request) authn.Permission {
	path := r.URL.Path
	method := r.Method
	if method == http.MethodHead {
		// Go's GET ServeMux patterns also serve HEAD. Classify HEAD exactly as
		// the corresponding read route, while leaving unknown paths denied.
		method = http.MethodGet
	}
	unclassified := authn.Permission("permission.unclassified")
	if path == "/api/auth/logout" && r.Method == http.MethodPost {
		return authn.PermissionSettingsManage
	}
	if path == "/api/audit" && method == http.MethodGet {
		return authn.PermissionAuditRead
	}
	if path == "/api/system/storage" && method == http.MethodGet || path == "/api/storage/pools" && method == http.MethodGet {
		return authn.PermissionStorageManage
	}
	if strings.HasPrefix(path, "/api/storage/pools/") && method == http.MethodGet {
		parts := strings.Split(strings.TrimPrefix(path, "/api/storage/pools/"), "/")
		if len(parts) == 2 && parts[0] != "" && parts[1] == "metrics" {
			return authn.PermissionStorageManage
		}
	}
	if path == "/api/setup/storage-test" && method == http.MethodPost || path == "/api/settings" && (method == http.MethodGet || method == http.MethodPut) || path == "/api/system/info" && method == http.MethodGet {
		return authn.PermissionSettingsManage
	}
	if path == "/api/retention/candidates" && method == http.MethodGet {
		return authn.PermissionSettingsManage
	}
	if path == "/api/retention/run" && method == http.MethodPost {
		return authn.PermissionRecordingDelete
	}
	if path == "/api/dashboard" && method == http.MethodGet || path == "/api/v2/recordings" && method == http.MethodGet || path == "/api/search" && method == http.MethodGet {
		return authn.PermissionRecordingRead
	}
	if path == "/api/logs" && method == http.MethodGet {
		return authn.PermissionSettingsManage
	}
	if strings.HasPrefix(path, "/api/exports/") {
		suffix := strings.TrimPrefix(path, "/api/exports/")
		parts := strings.Split(suffix, "/")
		if len(parts) == 1 && parts[0] != "" && (method == http.MethodGet || method == http.MethodDelete) || len(parts) == 2 && parts[0] != "" && parts[1] == "download" && method == http.MethodGet {
			if method == http.MethodDelete {
				return authn.PermissionRecordingControl
			}
			return authn.PermissionRecordingRead
		}
	}
	if strings.HasPrefix(path, "/api/integrity/jobs/") {
		parts := strings.Split(strings.TrimPrefix(path, "/api/integrity/jobs/"), "/")
		if len(parts) == 1 && parts[0] != "" && method == http.MethodGet {
			return authn.PermissionRecordingRead
		}
		if len(parts) == 2 && parts[0] != "" && parts[1] == "cancel" && method == http.MethodPost {
			return authn.PermissionRecordingControl
		}
	}
	if permission, matched := watchPermission(method, path); matched {
		return permission
	}
	if permission, matched := adapterPermission(method, path); matched {
		return permission
	}
	if permission, matched := workflowPermission(method, path); matched {
		return permission
	}
	if path == "/api/recordings" {
		switch method {
		case http.MethodGet:
			return authn.PermissionRecordingRead
		case http.MethodPost:
			return authn.PermissionRecordingControl
		default:
			return unclassified
		}
	}
	if permission, matched := recordingPermission(method, path); matched {
		return permission
	}
	if path == "/api/notifications" && method == http.MethodGet || path == "/api/notifications/sync" && method == http.MethodPost || path == "/api/notifications/read-all" && method == http.MethodPost {
		return authn.PermissionSettingsManage
	}
	if strings.HasPrefix(path, "/api/notifications/") && method == http.MethodPost {
		parts := strings.Split(strings.TrimPrefix(path, "/api/notifications/"), "/")
		if len(parts) == 2 && parts[0] != "" && parts[1] == "read" {
			return authn.PermissionSettingsManage
		}
	}
	if path == "/api/runtime/update" && method == http.MethodGet || path == "/api/runtime/update/check" && method == http.MethodPost || path == "/api/runtime/update/stage" && method == http.MethodPost || path == "/api/runtime/update/activate" && method == http.MethodPost || path == "/api/runtime/update/rollback" && method == http.MethodPost {
		return authn.PermissionUpdateManage
	}
	return unclassified
}

func watchPermission(method, path string) (authn.Permission, bool) {
	if path == "/api/watches" {
		if method == http.MethodGet || method == http.MethodPost {
			return authn.PermissionSettingsManage, true
		}
		return "", false
	}
	if !strings.HasPrefix(path, "/api/watches/") {
		return "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/api/watches/"), "/")
	if len(parts) == 1 && parts[0] != "" && (method == http.MethodGet || method == http.MethodPut || method == http.MethodDelete) {
		return authn.PermissionSettingsManage, true
	}
	if len(parts) == 2 {
		switch {
		case parts[0] != "" && method == http.MethodPost && parts[1] == "check":
			return authn.PermissionRecordingControl, true
		case parts[0] != "" && method == http.MethodPost && (parts[1] == "enable" || parts[1] == "disable"):
			return authn.PermissionSettingsManage, true
		case parts[0] != "" && method == http.MethodGet && (parts[1] == "recordings" || parts[1] == "events"):
			return authn.PermissionSettingsManage, true
		}
	}
	return "", false
}

func adapterPermission(method, path string) (authn.Permission, bool) {
	if path == "/api/adapters" && method == http.MethodGet {
		return authn.PermissionPluginManage, true
	}
	if !strings.HasPrefix(path, "/api/adapters/") {
		return "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/api/adapters/"), "/")
	if len(parts) == 1 && parts[0] != "" && method == http.MethodGet {
		return authn.PermissionPluginManage, true
	}
	if len(parts) == 2 && parts[0] != "" {
		switch {
		case method == http.MethodGet && (parts[1] == "icon" || parts[1] == "schema" || parts[1] == "config" || parts[1] == "resources"):
			return authn.PermissionPluginManage, true
		case method == http.MethodPut && parts[1] == "config":
			return authn.PermissionPluginManage, true
		case method == http.MethodPost && (parts[1] == "restart" || parts[1] == "enable" || parts[1] == "disable"):
			return authn.PermissionPluginManage, true
		}
	}
	if len(parts) == 3 && parts[0] != "" && method == http.MethodGet && parts[1] == "resources" && parts[2] == "search" {
		return authn.PermissionPluginManage, true
	}
	return "", false
}

func workflowPermission(method, path string) (authn.Permission, bool) {
	if path == "/api/resolve-workflows" && method == http.MethodGet || path == "/api/workflow-history" && method == http.MethodGet {
		return authn.PermissionPluginManage, true
	}
	if strings.HasPrefix(path, "/api/workflow-history/") && method == http.MethodGet && strings.TrimPrefix(path, "/api/workflow-history/") != "" && strings.Count(strings.TrimPrefix(path, "/api/workflow-history/"), "/") == 0 {
		return authn.PermissionPluginManage, true
	}
	if !strings.HasPrefix(path, "/api/resolve-workflows/") {
		return "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/api/resolve-workflows/"), "/")
	if len(parts) == 1 && parts[0] != "" && (method == http.MethodGet || method == http.MethodDelete) || len(parts) == 2 && parts[0] != "" && method == http.MethodPost && parts[1] == "continue" {
		return authn.PermissionPluginManage, true
	}
	return "", false
}

func recordingPermission(method, path string) (authn.Permission, bool) {
	if !strings.HasPrefix(path, "/api/recordings/") {
		return "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/api/recordings/"), "/")
	if parts[0] == "" {
		return "", false
	}
	if len(parts) == 1 {
		switch method {
		case http.MethodGet:
			return authn.PermissionRecordingRead, true
		case http.MethodDelete:
			return authn.PermissionRecordingDelete, true
		}
		return "", false
	}
	suffix := strings.Join(parts[1:], "/")
	if method == http.MethodGet {
		parts := strings.Split(suffix, "/")
		if suffix == "diagnostics/storage" || suffix == "lifecycle" || suffix == "play/master.m3u8" || suffix == "play/live/master.m3u8" || suffix == "archive/index" || suffix == "integrity" || suffix == "exports" || suffix == "thumbnail" || suffix == "previews" || suffix == "events" || suffix == "metadata" || suffix == "tags" || len(parts) == 4 && parts[0] == "play" && parts[1] == "tracks" && parts[2] != "" && parts[3] == "playlist.m3u8" || len(parts) == 5 && parts[0] == "play" && parts[1] == "live" && parts[2] == "tracks" && parts[3] != "" && parts[4] == "playlist.m3u8" || len(parts) == 3 && parts[0] == "play" && parts[1] == "segments" && parts[2] != "" || len(parts) == 2 && parts[0] == "previews" && parts[1] != "" {
			return authn.PermissionRecordingRead, true
		}
		return "", false
	}
	if method == http.MethodPost {
		if suffix == "stop" || suffix == "complete" || suffix == "seal" || suffix == "integrity/verify" || suffix == "exports" || suffix == "thumbnail/regenerate" || suffix == "previews" {
			return authn.PermissionRecordingControl, true
		}
	}
	if method == http.MethodPut && suffix == "tags" {
		return authn.PermissionRecordingControl, true
	}
	return "", false
}

func authRoutePublic(r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if r.URL.Path == "/healthz" || r.URL.Path == "/api/auth/session" || isSPARoute(r.URL.Path) || strings.HasPrefix(r.URL.Path, "/static/") {
			return true
		}
	}
	if r.Method == http.MethodPost && (r.URL.Path == "/api/auth/login" || r.URL.Path == "/api/auth/bootstrap") {
		return true
	}
	return false
}
