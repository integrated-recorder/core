package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/integrated-recorder/core/internal/runtimehost/installation"
)

func (s *Server) installationIsReady() bool {
	return installationStateForServer(s) == installation.StateReady
}

func installationStateForServer(s *Server) installation.State {
	if s == nil || !s.installationManaged {
		return installation.StateReady
	}
	if s.storage == nil {
		return installation.StateRecoveryRequired
	}
	return installation.ReadOnly(s.storage.Root()).State
}

func (s *Server) installationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.installationIsReady() || setupModeRouteAllowed(r) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": "installation_incomplete", "message": "설치 설정을 완료한 뒤 이용할 수 있습니다.",
		})
	})
}

func setupModeRouteAllowed(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if r.URL.Path == "/healthz" || r.URL.Path == "/api/auth/session" || strings.HasPrefix(r.URL.Path, "/static/") || r.URL.Path == "/" || r.URL.Path == "/setup" || r.URL.Path == "/login" {
			return true
		}
		switch r.URL.Path {
		case "/api/setup/status", "/api/system/info", "/api/system/storage", "/api/adapters":
			return true
		}
	case http.MethodPost:
		switch r.URL.Path {
		case "/api/auth/bootstrap", "/api/auth/login", "/api/auth/logout", "/api/setup/storage-test":
			return true
		}
	}
	return false
}
