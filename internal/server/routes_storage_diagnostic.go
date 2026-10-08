package server

import (
	"errors"
	"net/http"

	"github.com/integrated-recorder/core/internal/storagediagnostic"
)

func (s *Server) storageFailureDiagnostic(w http.ResponseWriter, r *http.Request) {
	// Diagnostics are administrator-only even in deployments that disable the
	// normal API authentication middleware for loopback development.
	if s.auth == nil {
		writeError(w, http.StatusForbidden, "administrator authentication is required")
		return
	}
	if !validRecordingPathID(r.PathValue("id")) {
		writeError(w, http.StatusNotFound, "storage diagnostic not found")
		return
	}
	if s.storageDiagnostics == nil {
		writeError(w, http.StatusServiceUnavailable, "storage diagnostics are unavailable")
		return
	}
	diagnostic, err := s.storageDiagnostics.Read(r.PathValue("id"))
	if errors.Is(err, storagediagnostic.ErrNotFound) {
		writeError(w, http.StatusNotFound, "storage diagnostic not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "storage diagnostics are unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, diagnostic)
}
