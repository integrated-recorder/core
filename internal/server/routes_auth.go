package server

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
)

const csrfCookieName = authn.CSRFCookieName

// secureBrowserRequest trusts the TLS marker only because the Runtime Host's
// fixed local proxy strips any client-provided copy and sets it from the
// actual incoming TLS connection. Direct Control Plane HTTP tests and local
// development continue to use ordinary request TLS state.
func secureBrowserRequest(r *http.Request) bool {
	return r.TLS != nil || r.Header.Get("X-IR-HTTPS") == "1"
}

func (s *Server) registerAuthRoutes() {
	s.mux.HandleFunc("GET /api/auth/session", s.authSession)
	s.mux.HandleFunc("POST /api/auth/bootstrap", s.authBootstrap)
	s.mux.HandleFunc("POST /api/auth/login", s.authLogin)
	s.mux.HandleFunc("POST /api/auth/logout", s.authLogout)
	s.mux.HandleFunc("GET /api/user/preferences", s.userPreferencesGet)
	s.mux.HandleFunc("PUT /api/user/preferences", s.userPreferencesPut)
	s.registerAccountRoutes()
}

func (s *Server) userPreferencesGet(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	preferences, err := s.auth.GetUserPreferences(principal.UserID)
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "preferences_unavailable", "Preferences could not be loaded.")
		return
	}
	writeJSON(w, http.StatusOK, preferences)
}

func (s *Server) userPreferencesPut(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	var preferences authn.UserPreferences
	if err := decodeJSONBody(w, r, 4<<10, &preferences); err != nil {
		writeCodedError(w, http.StatusBadRequest, "preferences_invalid", "Preferences are invalid.")
		return
	}
	if err := s.auth.SetUserPreferences(principal.UserID, preferences); err != nil {
		switch {
		case errors.Is(err, authn.ErrInvalidPreferences):
			writeCodedError(w, http.StatusBadRequest, "preferences_invalid", "Preferences are invalid.")
		case errors.Is(err, authn.ErrUnsupportedPreferencesVersion):
			writeCodedError(w, http.StatusServiceUnavailable, "preferences_unavailable", "Preferences could not be updated.")
		default:
			writeCodedError(w, http.StatusServiceUnavailable, "preferences_unavailable", "Preferences could not be updated.")
		}
		return
	}
	writeJSON(w, http.StatusOK, preferences)
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
	Login    string `json:"login"`
	Password string `json:"password"`
}

func (s *Server) authBootstrap(w http.ResponseWriter, r *http.Request) {
	if !sameOriginBootstrapRequest(r) {
		writeCodedError(w, http.StatusForbidden, "bootstrap_origin_invalid", "Open the setup page from this application before creating its first account.")
		return
	}
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
	if err := s.auth.Bootstrap(strings.TrimSpace(request.Login), request.Password); err != nil {
		if errors.Is(err, authn.ErrInvalidLogin) {
			writeCodedError(w, http.StatusBadRequest, "login_invalid", "Choose a valid login name.")
			return
		}
		writeError(w, http.StatusUnauthorized, "administrator setup was rejected")
		return
	}
	login := strings.TrimSpace(request.Login)
	if user, err := s.auth.FindByLogin(login); err == nil {
		// Bootstrap has no authenticated user actor yet, so this is recorded as
		// a system-created account. Audit remains secondary to the successful
		// durable bootstrap mutation.
		s.auditMutation(w, r, "user_created", user.ID)
	}
	s.finishLoginAs(w, r, login, request.Password)
}

// The first Owner endpoint is unauthenticated by design, so require a
// browser same-origin request and an Origin authority matching Host. No
// forwarded headers are trusted here; a TLS proxy preserves the public Host.
func sameOriginBootstrapRequest(r *http.Request) bool {
	origin, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") || origin.Host == "" || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" {
		return false
	}
	requestHost, err := normalizedOriginHost(r.Host, origin.Scheme)
	if err != nil {
		return false
	}
	originHost, err := normalizedOriginHost(origin.Host, origin.Scheme)
	return err == nil && strings.EqualFold(requestHost, originHost)
}

func normalizedOriginHost(host, scheme string) (string, error) {
	parsed, err := url.Parse("//" + host)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Path != "" {
		return "", errors.New("invalid host")
	}
	name := strings.ToLower(parsed.Hostname())
	if name == "" {
		return "", errors.New("invalid host")
	}
	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return name + ":" + port, nil
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
	if strings.TrimSpace(request.Login) == "" {
		writeCodedError(w, http.StatusUnauthorized, "login_required", "Enter your login name and password.")
		return
	}
	s.finishLoginAs(w, r, strings.TrimSpace(request.Login), request.Password)
}

func (s *Server) finishLoginAs(w http.ResponseWriter, r *http.Request, login, password string) {
	var session authn.Session
	var err error
	session, err = s.auth.LoginAs(login, password)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "login failed")
		return
	}
	secure := secureBrowserRequest(r) || s.forceSecureCookie
	authn.SetSessionCookie(w, r, session, secure)
	http.SetCookie(w, &http.Cookie{
		Name: csrfCookieName, Value: session.CSRFToken, Path: "/", Expires: session.ExpiresAt,
		MaxAge: maxInt(1, int(time.Until(session.ExpiresAt)/time.Second)), HttpOnly: false,
		Secure: secure, SameSite: http.SameSiteStrictMode,
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
	http.SetCookie(w, &http.Cookie{Name: csrfCookieName, Value: "", Path: "/", MaxAge: -1, Expires: time.Unix(1, 0), Secure: secureBrowserRequest(r) || s.forceSecureCookie, SameSite: http.SameSiteStrictMode})
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
			writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			csrf := r.Header.Get("X-CSRF-Token")
			if csrf == "" || !s.auth.ValidCSRF(token, csrf) {
				writeCodedError(w, http.StatusForbidden, "csrf_invalid", "Request validation failed.")
				return
			}
		}
		principal := authn.Principal{UserID: session.UserID, Login: session.Login, Roles: session.Roles, Permissions: session.Permissions, Role: session.Role}
		request := r.WithContext(authn.WithPrincipal(r.Context(), principal))
		authn.RequirePermission(permissionForRequest(r), next).ServeHTTP(w, request)
	})
}

func permissionForRequest(r *http.Request) authn.Permission {
	return routePermission(r.Method, r.URL.Path)
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
