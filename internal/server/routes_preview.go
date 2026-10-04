package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/storage"
)

func (s *Server) registerPreviewRoutes() {
	s.mux.HandleFunc("GET /api/recordings/{id}/previews", s.previewList)
	s.mux.HandleFunc("GET /api/recordings/{id}/previews/{ordinal}", s.previewFrame)
	s.mux.HandleFunc("POST /api/recordings/{id}/previews", s.previewEnable)
}

func (s *Server) cleanupPreviewProjection(id string) {
	if s.previews == nil {
		return
	}
	if err := s.previews.DeleteRecording(id); err != nil && s.logs != nil {
		s.logs.Add("error", "preview", "deleted recording preview cleanup failed")
	}
}

func (s *Server) previewList(w http.ResponseWriter, r *http.Request) {
	if s.previews == nil || s.manager == nil {
		writeError(w, http.StatusNotImplemented, "preview generation is unavailable")
		return
	}
	id := r.PathValue("id")
	if !validRecordingPathID(id) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.manager.Get(id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	query := r.URL.Query()
	for key, values := range query {
		if key != "sampling" && key != "limit" && key != "time_seconds" || len(values) != 1 {
			writeError(w, http.StatusBadRequest, "invalid preview query")
			return
		}
	}
	sampling := preview.SamplingUniform
	if raw := query.Get("sampling"); raw != "" {
		sampling = preview.Sampling(raw)
	}
	limit := 48
	if raw := query.Get("limit"); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > 100 {
			writeError(w, http.StatusBadRequest, "invalid preview limit")
			return
		}
		limit = parsed
	}
	timeSeconds := 0.0
	if raw := query.Get("time_seconds"); raw != "" {
		timeSeconds, err = strconv.ParseFloat(raw, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid preview time")
			return
		}
	} else if sampling == preview.SamplingNearest {
		writeError(w, http.StatusBadRequest, "nearest sampling requires time_seconds")
		return
	}
	response, err := s.previews.Items(recording, sampling, limit, timeSeconds)
	if err != nil {
		writePreviewError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) previewFrame(w http.ResponseWriter, r *http.Request) {
	if s.previews == nil || s.manager == nil {
		writeError(w, http.StatusNotImplemented, "preview generation is unavailable")
		return
	}
	id := r.PathValue("id")
	if !validRecordingPathID(id) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	ordinal, err := strconv.ParseUint(r.PathValue("ordinal"), 10, 64)
	if err != nil || ordinal == 0 {
		writeError(w, http.StatusNotFound, "preview frame not found")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	file, frame, err := s.previews.OpenFrame(id, ordinal)
	if err != nil {
		writePreviewError(w, err)
		return
	}
	defer file.Close()
	servePreviewImage(w, r, file, frame)
}

func (s *Server) previewEnable(w http.ResponseWriter, r *http.Request) {
	if s.previews == nil || s.manager == nil {
		writeError(w, http.StatusNotImplemented, "preview generation is unavailable")
		return
	}
	var request struct {
		Mode preview.Mode `json:"mode"`
	}
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil || request.Mode != preview.ModeSegment {
		writeError(w, http.StatusBadRequest, "invalid preview policy")
		return
	}
	id := r.PathValue("id")
	if !validRecordingPathID(id) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.manager.Get(id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if _, err = s.previews.SetMode(id, request.Mode); err != nil {
		writePreviewError(w, err)
		return
	}
	if err = s.previews.Reconcile(); err != nil && !errors.Is(err, context.Canceled) {
		// Policy is durable and reconciliation will retry on the next service tick.
	}
	writeJSON(w, http.StatusAccepted, s.previews.Summary(recording))
}

func servePreviewImage(w http.ResponseWriter, r *http.Request, file io.ReadSeeker, frame preview.Frame) {
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, max-age=300")
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", frame.ImageSHA256))
	w.Header().Set("Content-Length", strconv.FormatInt(frame.Size, 10))
	http.ServeContent(w, r, fmt.Sprintf("preview-%d.jpg", frame.ArchiveOrdinal), frame.GeneratedAt, file)
}

func writePreviewError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, preview.ErrInvalid), errors.Is(err, preview.ErrInvalidSample):
		writeError(w, http.StatusBadRequest, "invalid preview request")
	case errors.Is(err, preview.ErrNotFound), errors.Is(err, storage.ErrNotFound):
		writeError(w, http.StatusNotFound, "preview is not available")
	case errors.Is(err, preview.ErrActive):
		writeError(w, http.StatusConflict, "preview is unavailable for an active recording")
	case errors.Is(err, preview.ErrUnavailable):
		writeError(w, http.StatusNotImplemented, "preview generation is unavailable")
	default:
		writeError(w, http.StatusInternalServerError, "preview operation failed")
	}
}
