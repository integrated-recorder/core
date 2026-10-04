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
	s.finishLogin(w, r, request.Password)
}

func (s *Server) finishLogin(w http.ResponseWriter, r *http.Request, password string) {
	session, err := s.auth.Login(password)
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
		s.auth.Logout(authn.SessionToken(r))
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
		if _, err := s.auth.Authenticate(token); err != nil {
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
		next.ServeHTTP(w, r)
	})
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
