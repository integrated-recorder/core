package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/peerapi"
)

type peerConnectRequest struct {
	IdentityID string `json:"identity_id"`
	NodeID     string `json:"node_id"`
}

func (s *Server) registerPeerSessionRoutes() {
	s.mux.HandleFunc("GET /api/peer-sessions", s.peerSessionsList)
	s.mux.HandleFunc("POST /api/peer-sessions", s.peerSessionsConnect)
	s.mux.HandleFunc("DELETE /api/peer-sessions/{nodeID}", s.peerSessionsDisconnect)
}

func (s *Server) peerSessionsList(w http.ResponseWriter, r *http.Request) {
	if !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer networking is not configured on this Control Plane.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": s.peerSessions.List()})
}

func (s *Server) peerSessionsConnect(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer networking is not configured on this Control Plane.")
		return
	}
	var request peerConnectRequest
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil || request.IdentityID == "" || request.NodeID == "" {
		writeCodedError(w, http.StatusBadRequest, "peer_target_invalid", "The peer identity and Node ID are required.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	if err := s.peerSessions.Connect(ctx, request.IdentityID, request.NodeID); err != nil {
		if errors.Is(err, peerapi.ErrPeerAlreadyConnected) {
			writeCodedError(w, http.StatusConflict, "peer_already_connected", "A session for this Node is already active or connecting.")
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			writeCodedError(w, http.StatusGatewayTimeout, "peer_connection_timeout", "The authenticated peer connection did not complete in time.")
			return
		}
		writeCodedError(w, http.StatusBadGateway, "peer_connection_failed", "The authenticated peer connection could not be established.")
		return
	}
	_ = s.appendAuditForRequest(r, "peer_session_connected", request.NodeID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) peerSessionsDisconnect(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer networking is not configured on this Control Plane.")
		return
	}
	nodeID := r.PathValue("nodeID")
	if err := s.peerSessions.Disconnect(nodeID); err != nil {
		if errors.Is(err, peerapi.ErrPeerNotConnected) {
			writeCodedError(w, http.StatusNotFound, "peer_session_not_found", "No active peer session exists for this Node.")
			return
		}
		writeCodedError(w, http.StatusServiceUnavailable, "peer_session_close_failed", "The peer session could not be closed safely.")
		return
	}
	_ = s.appendAuditForRequest(r, "peer_session_disconnected", nodeID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) peerSessionsAvailable() bool {
	if s == nil || s.peerSessions == nil {
		return false
	}
	if availability, ok := s.peerSessions.(interface{ Available() bool }); ok {
		return availability.Available()
	}
	return true
}
