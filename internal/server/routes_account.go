package server

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
)

func (s *Server) registerAccountRoutes() {
	s.mux.HandleFunc("GET /api/user", s.accountSelf)
	s.mux.HandleFunc("POST /api/user/password", s.accountChangePassword)
	s.mux.HandleFunc("GET /api/user/sessions", s.accountSessions)
	s.mux.HandleFunc("DELETE /api/user/sessions/{sessionID}", s.accountRevokeSession)
	s.mux.HandleFunc("POST /api/user/sessions/revoke-others", s.accountRevokeOtherSessions)
	s.mux.HandleFunc("GET /api/users", s.usersList)
	s.mux.HandleFunc("POST /api/users", s.usersCreate)
	s.mux.HandleFunc("POST /api/users/{userID}/disable", s.userDisable)
	s.mux.HandleFunc("POST /api/users/{userID}/enable", s.userEnable)
}

func (s *Server) accountSelf(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	user, err := s.auth.GetUser(principal.UserID)
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "account_unavailable", "Account information could not be loaded.")
		return
	}
	writeJSON(w, http.StatusOK, user)
}

func (s *Server) accountChangePassword(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	var request struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := decodeJSONBody(w, r, 16<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "credentials_invalid", "Credentials could not be verified.")
		return
	}
	if err := s.auth.ChangePassword(principal.UserID, request.CurrentPassword, request.NewPassword); err != nil {
		switch {
		case errors.Is(err, authn.ErrInvalidPassword):
			writeCodedError(w, http.StatusBadRequest, "password_policy", "The new password does not meet the password policy.")
		case errors.Is(err, authn.ErrCurrentPassword):
			writeCodedError(w, http.StatusUnauthorized, "credentials_invalid", "Credentials could not be verified.")
		default:
			writeCodedError(w, http.StatusServiceUnavailable, "credentials_unavailable", "The password could not be changed.")
		}
		return
	}
	s.auditMutation(w, r, "password_changed", principal.UserID)
	clearAccountCookies(w, r, s.forceSecureCookie)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) accountSessions(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	items, err := s.auth.ListSessions(principal.UserID, authn.SessionToken(r))
	if errors.Is(err, authn.ErrUnauthenticated) {
		clearAccountCookies(w, r, s.forceSecureCookie)
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "sessions_unavailable", "Active sessions could not be loaded.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) accountRevokeSession(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	sessionID := strings.TrimSpace(r.PathValue("sessionID"))
	wasCurrent, err := s.auth.RevokeSession(principal.UserID, sessionID, authn.SessionToken(r))
	if errors.Is(err, authn.ErrUnauthenticated) {
		clearAccountCookies(w, r, s.forceSecureCookie)
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if errors.Is(err, authn.ErrUserNotFound) {
		writeCodedError(w, http.StatusNotFound, "session_not_found", "The session could not be found.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "session_revoke_failed", "The session could not be revoked.")
		return
	}
	s.auditMutation(w, r, "session_revoked", sessionID)
	if wasCurrent {
		clearAccountCookies(w, r, s.forceSecureCookie)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) accountRevokeOtherSessions(w http.ResponseWriter, r *http.Request) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	count, err := s.auth.RevokeOtherSessions(principal.UserID, authn.SessionToken(r))
	if errors.Is(err, authn.ErrUnauthenticated) {
		clearAccountCookies(w, r, s.forceSecureCookie)
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "sessions_revoke_failed", "Other sessions could not be revoked.")
		return
	}
	if count > 0 {
		s.auditMutation(w, r, "sessions_revoked", principal.UserID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"revoked_count": count})
}

func (s *Server) usersList(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "users_unavailable", "Users could not be loaded.")
		return
	}
	users, err := s.auth.ListUsers()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "users_unavailable", "Users could not be loaded.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": users})
}

func (s *Server) usersCreate(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "users_unavailable", "The user could not be created.")
		return
	}
	var request struct {
		Login    string `json:"login"`
		Password string `json:"password"`
	}
	if err := decodeJSONBody(w, r, 16<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "user_invalid", "The user could not be created.")
		return
	}
	user, err := s.auth.CreateUser(request.Login, request.Password, authn.RoleMember)
	if errors.Is(err, authn.ErrDuplicateLogin) {
		writeCodedError(w, http.StatusConflict, "user_exists", "The user could not be created.")
		return
	}
	if err != nil {
		if errors.Is(err, authn.ErrInvalidCredentials) {
			writeCodedError(w, http.StatusBadRequest, "user_invalid", "Check the login name and password policy.")
		} else {
			writeCodedError(w, http.StatusServiceUnavailable, "users_unavailable", "The user could not be created.")
		}
		return
	}
	s.auditMutation(w, r, "user_created", user.ID)
	writeJSON(w, http.StatusCreated, user)
}

func (s *Server) userDisable(w http.ResponseWriter, r *http.Request) {
	s.setUserEnabled(w, r, false)
}

func (s *Server) userEnable(w http.ResponseWriter, r *http.Request) {
	s.setUserEnabled(w, r, true)
}

func (s *Server) setUserEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	userID := strings.TrimSpace(r.PathValue("userID"))
	if !enabled && userID == principal.UserID {
		writeCodedError(w, http.StatusConflict, "self_disable_rejected", "The current account cannot be disabled.")
		return
	}
	user, err := s.auth.SetUserEnabled(userID, enabled)
	if errors.Is(err, authn.ErrLastOwner) {
		writeCodedError(w, http.StatusConflict, "last_owner_protected", "The last active owner cannot be disabled.")
		return
	}
	if errors.Is(err, authn.ErrUserNotFound) {
		writeCodedError(w, http.StatusNotFound, "user_not_found", "The user could not be found.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "users_unavailable", "The user state could not be changed.")
		return
	}
	action := "user_disabled"
	if enabled {
		action = "user_enabled"
	}
	s.auditMutation(w, r, action, user.ID)
	writeJSON(w, http.StatusOK, user)
}

func clearAccountCookies(w http.ResponseWriter, r *http.Request, forceSecure bool) {
	authn.ClearSessionCookie(w, r, forceSecure)
	http.SetCookie(w, &http.Cookie{Name: csrfCookieName, Value: "", Path: "/", MaxAge: -1, Expires: time.Unix(1, 0), Secure: r.TLS != nil || forceSecure, SameSite: http.SameSiteStrictMode})
}
