package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/integrated-recorder/core/internal/watch"
)

func (s *Server) registerWatchRoutes() {
	s.mux.HandleFunc("POST /api/watches", s.watchCreate)
	s.mux.HandleFunc("GET /api/watches", s.watchList)
	s.mux.HandleFunc("GET /api/watches/{id}", s.watchGet)
	s.mux.HandleFunc("PUT /api/watches/{id}", s.watchUpdate)
	s.mux.HandleFunc("DELETE /api/watches/{id}", s.watchDelete)
	s.mux.HandleFunc("POST /api/watches/{id}/enable", s.watchEnable)
	s.mux.HandleFunc("POST /api/watches/{id}/disable", s.watchDisable)
	s.mux.HandleFunc("POST /api/watches/{id}/check", s.watchCheck)
	s.mux.HandleFunc("GET /api/watches/{id}/recordings", s.watchRecordings)
	s.mux.HandleFunc("GET /api/watches/{id}/events", s.watchEvents)
}

func (s *Server) watchCreate(w http.ResponseWriter, r *http.Request) {
	var request watch.Update
	if err := decodeJSONBody(w, r, 256<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid Watch request")
		return
	}
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	view, err := s.watches.Create(request)
	if err != nil {
		writeWatchError(w, err)
		return
	}
	if err := s.appendAudit("watch_created", view.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "Watch was created but audit record could not be saved")
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (s *Server) watchList(w http.ResponseWriter, r *http.Request) {
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": s.watches.List()})
}

func (s *Server) watchGet(w http.ResponseWriter, r *http.Request) {
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	view, err := s.watches.Get(r.PathValue("id"))
	if err != nil {
		writeWatchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) watchUpdate(w http.ResponseWriter, r *http.Request) {
	var request watch.Update
	if err := decodeJSONBody(w, r, 256<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid Watch request")
		return
	}
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	view, err := s.watches.Update(r.PathValue("id"), request)
	if err != nil {
		writeWatchError(w, err)
		return
	}
	if err := s.appendAudit("watch_updated", view.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "Watch was updated but audit record could not be saved")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) watchDelete(w http.ResponseWriter, r *http.Request) {
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	id := r.PathValue("id")
	if err := s.watches.Delete(id); err != nil {
		writeWatchError(w, err)
		return
	}
	if err := s.appendAudit("watch_deleted", id); err != nil {
		writeError(w, http.StatusInternalServerError, "Watch was deleted but audit record could not be saved")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) watchEnable(w http.ResponseWriter, r *http.Request)  { s.watchSetEnabled(w, r, true) }
func (s *Server) watchDisable(w http.ResponseWriter, r *http.Request) { s.watchSetEnabled(w, r, false) }

func (s *Server) watchSetEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	view, err := s.watches.SetEnabled(r.PathValue("id"), enabled)
	if err != nil {
		writeWatchError(w, err)
		return
	}
	kind := "watch_enabled"
	if !enabled {
		kind = "watch_disabled"
	}
	if err := s.appendAudit(kind, view.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "Watch state changed but audit record could not be saved")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) watchCheck(w http.ResponseWriter, r *http.Request) {
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "automatic recording is unavailable")
		return
	}
	id := r.PathValue("id")
	if err := s.appendAudit("manual_watch_check", id); err != nil {
		writeError(w, http.StatusInternalServerError, "Watch check audit record could not be saved")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 27*time.Second)
	defer cancel()
	if err := s.watches.ManualCheck(ctx, id); err != nil {
		writeWatchError(w, err)
		return
	}
	view, err := s.watches.Get(id)
	if err != nil {
		writeWatchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) watchRecordings(w http.ResponseWriter, r *http.Request) {
	if s.watches == nil || s.manager == nil {
		writeError(w, http.StatusServiceUnavailable, "Watch recording history is unavailable")
		return
	}
	limit, err := watchQueryLimit(r, watch.MaxListLimit, 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid Watch recording limit")
		return
	}
	relations, truncated, err := s.watches.RelationsPage(r.PathValue("id"), limit)
	if err != nil {
		writeWatchError(w, err)
		return
	}
	items := make([]recordingSummary, 0, len(relations))
	for _, relation := range relations {
		recording, getErr := s.manager.Get(relation.RecordingID)
		if getErr != nil {
			continue
		}
		items = append(items, s.recordingSummary(recording))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "truncated": truncated, "retention_limit": watch.RelationRetentionLimit})
}

func (s *Server) watchEvents(w http.ResponseWriter, r *http.Request) {
	if s.watches == nil {
		writeError(w, http.StatusServiceUnavailable, "Watch events are unavailable")
		return
	}
	limit, err := watchQueryLimit(r, watch.MaxEventsPerWatch, watch.MaxEventsPerWatch)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid Watch event limit")
		return
	}
	items, err := s.watches.Events(r.PathValue("id"), limit)
	if err != nil {
		writeWatchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func watchQueryLimit(r *http.Request, maximum, defaultValue int) (int, error) {
	value := r.URL.Query().Get("limit")
	if value == "" {
		return defaultValue, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > maximum {
		return 0, errors.New("invalid limit")
	}
	return limit, nil
}

func writeWatchError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, watch.ErrNotFound):
		writeError(w, http.StatusNotFound, "Watch not found")
	case errors.Is(err, watch.ErrConflict), errors.Is(err, watch.ErrActiveRecording):
		writeError(w, http.StatusConflict, "Watch cannot be changed in its current state")
	case errors.Is(err, watch.ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, "Watch settings are invalid")
	case errors.Is(err, watch.ErrUnsupported):
		writeError(w, http.StatusNotImplemented, "This adapter does not support automatic recording")
	case errors.Is(err, watch.ErrQueueFull), errors.Is(err, watch.ErrServiceClosed):
		writeError(w, http.StatusServiceUnavailable, "Watch scheduler is temporarily unavailable")
	default:
		writeError(w, http.StatusServiceUnavailable, "Watch operation could not be completed")
	}
}
