package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/management"
)

// AppendRuntimeAudit is the narrow active-Control sink for Runtime Host
// mutation events. Callers reach it only through the authenticated lifecycle
// IPC bridge; ordinary HTTP requests cannot supply actor identity.
func (s *Server) AppendRuntimeAudit(ctx context.Context, request controlplane.AuditAppendRequest) error {
	if ctx == nil || ctx.Err() != nil || request.Validate() != nil || s == nil || s.products == nil {
		return errors.New("runtime audit append is unavailable")
	}
	actor := &management.AuditActor{Type: request.ActorType, UserID: request.UserID}
	err := s.products.AppendAudit(management.AuditEvent{
		ID: randomProductID(), Type: request.Action, At: time.Now().UTC(), ObjectID: request.ObjectID, Actor: actor,
	})
	if err != nil && s.logs != nil {
		s.logs.Add("error", "audit", "audit append failed after Runtime Host mutation")
		return errors.New("runtime audit append failed")
	}
	return err
}

// appendAuditForRequest records the authenticated actor for an HTTP mutation.
// Audit is a secondary projection: callers must preserve a successful primary
// mutation if this write fails.
func (s *Server) appendAuditForRequest(r *http.Request, kind, objectID string) error {
	if s.products == nil {
		return nil
	}
	actor := &management.AuditActor{Type: management.AuditActorSystem}
	if principal, ok := authn.PrincipalFromContext(r.Context()); ok {
		actor = &management.AuditActor{Type: management.AuditActorUser, UserID: principal.UserID}
	}
	err := s.products.AppendAudit(management.AuditEvent{
		ID: randomProductID(), Type: kind, At: time.Now().UTC(), ObjectID: objectID, Actor: actor,
	})
	if err != nil && s.logs != nil {
		// Keep the failure observable without exposing paths, credentials, or
		// provider error text in API responses or logs.
		s.logs.Add("error", "audit", "audit append failed after primary mutation")
	}
	return err
}

func (s *Server) auditMutation(w http.ResponseWriter, r *http.Request, kind, objectID string) bool {
	if s.products == nil {
		w.Header().Set("X-Audit-Status", "unavailable")
		return false
	}
	if err := s.appendAuditForRequest(r, kind, objectID); err != nil {
		w.Header().Set("X-Audit-Status", "failed")
		return false
	}
	w.Header().Set("X-Audit-Status", "recorded")
	return true
}

func (s *Server) auditList(w http.ResponseWriter, r *http.Request) {
	if s.products == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"code": "audit_unavailable", "error": "audit events are unavailable",
		})
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"code": "invalid_audit_limit", "error": "invalid audit limit",
			})
			return
		}
		limit = parsed
	}
	items, next, err := s.products.AuditPage(limit, r.URL.Query().Get("cursor"))
	if errors.Is(err, management.ErrInvalidAuditCursor) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"code": "invalid_audit_cursor", "error": "invalid audit cursor",
		})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"code": "audit_unavailable", "error": "audit events are unavailable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}
