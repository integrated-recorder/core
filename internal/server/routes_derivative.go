package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/management"
)

func (s *Server) registerDerivativeRoutes() {
	s.mux.HandleFunc("POST /api/recordings/{id}/exports", s.exportStart)
	s.mux.HandleFunc("GET /api/recordings/{id}/exports", s.exportList)
	s.mux.HandleFunc("GET /api/exports/{job_id}", s.exportGet)
	s.mux.HandleFunc("GET /api/exports/{job_id}/download", s.exportDownload)
	s.mux.HandleFunc("DELETE /api/exports/{job_id}", s.exportDelete)
	s.registerPreviewRoutes()
	s.mux.HandleFunc("GET /api/recordings/{id}/thumbnail", s.thumbnailGet)
	s.mux.HandleFunc("POST /api/recordings/{id}/thumbnail/regenerate", s.thumbnailRegenerate)
}

func (s *Server) thumbnailGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validRecordingPathID(id) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if s.previews != nil {
		summary := s.previews.Summary(recording)
		if summary.ImageArchiveOrdinal != nil {
			file, frame, err := s.previews.OpenFrame(id, *summary.ImageArchiveOrdinal)
			if err == nil {
				defer file.Close()
				servePreviewImage(w, r, file, frame)
				return
			}
		}
	}
	if s.derivatives != nil {
		file, thumbnail, err := s.derivatives.OpenThumbnail(id)
		if err == nil {
			defer file.Close()
			w.Header().Set("Content-Type", thumbnail.ContentType)
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Cache-Control", "private, no-store")
			http.ServeContent(w, r, "thumbnail.jpg", thumbnail.UpdatedAt, file)
			return
		}
		if !errors.Is(err, derivative.ErrThumbnailNotFound) {
			writeError(w, http.StatusInternalServerError, "thumbnail is unavailable")
			return
		}
	}
	writeError(w, http.StatusNotFound, "thumbnail is not available")
}

func (s *Server) thumbnailRegenerate(w http.ResponseWriter, r *http.Request) {
	if s.previews == nil {
		writeError(w, http.StatusNotImplemented, "thumbnail generation is unavailable")
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
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if _, err := s.previews.SetMode(id, "segment"); err != nil {
		writeError(w, http.StatusServiceUnavailable, "preview policy could not be saved")
		return
	}
	if err := s.previews.RetryFailed(id); err != nil {
		writePreviewError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, s.previews.Summary(recording))
}

func writeThumbnailError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusRequestTimeout, "thumbnail generation was canceled")
	case errors.Is(err, derivative.ErrActive):
		writeError(w, http.StatusConflict, "stop the recording before generating a thumbnail")
	case errors.Is(err, derivative.ErrUnavailable):
		writeError(w, http.StatusNotImplemented, "thumbnail generation is unavailable")
	case errors.Is(err, derivative.ErrUnsupported):
		writeError(w, http.StatusUnprocessableEntity, "archive media cannot produce a thumbnail")
	case errors.Is(err, derivative.ErrThumbnailFailed):
		writeError(w, http.StatusInternalServerError, "thumbnail generation failed")
	default:
		writeError(w, http.StatusInternalServerError, "thumbnail generation failed")
	}
}

func (s *Server) exportStart(w http.ResponseWriter, r *http.Request) {
	if !s.exportAvailable(w) {
		return
	}
	var request struct {
		Format string `json:"format,omitempty"`
	}
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid export request")
		return
	}
	if request.Format != "" && request.Format != "mkv" {
		writeError(w, http.StatusBadRequest, "unsupported export format")
		return
	}
	id := r.PathValue("id")
	if !validRecordingPathID(id) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		lock.RUnlock()
		writeStorageError(w, err)
		return
	}
	job, err := s.derivatives.Start(r.Context(), recording)
	lock.RUnlock()
	if err == nil {
		if s.products != nil {
			if auditErr := s.products.AppendAudit(management.AuditEvent{ID: randomProductID(), Type: "export_requested", At: time.Now().UTC(), ObjectID: id}); auditErr != nil {
				w.Header().Set("X-Export-Audit", "failed")
			} else {
				w.Header().Set("X-Export-Audit", "recorded")
			}
		}
		writeJSON(w, http.StatusAccepted, job)
		return
	}
	writeDerivativeError(w, err)
}

func (s *Server) exportList(w http.ResponseWriter, r *http.Request) {
	if !s.exportAvailable(w) {
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
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": s.derivatives.ListWithFreshness(id, recording), "available": s.derivatives.Available()})
}

func (s *Server) exportGet(w http.ResponseWriter, r *http.Request) {
	if !s.exportAvailable(w) {
		return
	}
	job, err := s.derivatives.Get(r.PathValue("job_id"))
	if err != nil {
		writeDerivativeError(w, err)
		return
	}
	recording, err := s.recordingSnapshot(r.Context(), job.RecordingID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, derivative.ProjectJob(job, recording))
}

func (s *Server) exportDownload(w http.ResponseWriter, r *http.Request) {
	if !s.exportAvailable(w) {
		return
	}
	file, job, err := s.derivatives.OpenDownload(r.PathValue("job_id"))
	if err != nil {
		writeDerivativeError(w, err)
		return
	}
	defer file.Close()
	w.Header().Set("Content-Type", "video/x-matroska")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", job.OutputName))
	w.Header().Set("Cache-Control", "private, no-store")
	modified := time.Time{}
	if job.FinishedAt != nil {
		modified = *job.FinishedAt
	}
	http.ServeContent(w, r, job.OutputName, modified, file)
}

func (s *Server) exportDelete(w http.ResponseWriter, r *http.Request) {
	if !s.exportAvailable(w) {
		return
	}
	job, err := s.derivatives.Get(r.PathValue("job_id"))
	if err != nil {
		writeDerivativeError(w, err)
		return
	}
	if job.State == derivative.StateQueued || job.State == derivative.StateRunning {
		job, err = s.derivatives.Cancel(job.ID)
		if err != nil {
			writeDerivativeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, job)
		return
	}
	if err := s.derivatives.Delete(job.ID); err != nil {
		writeDerivativeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) exportAvailable(w http.ResponseWriter) bool {
	if s.derivatives == nil || !s.derivatives.Available() {
		writeError(w, http.StatusNotImplemented, "remux export is unavailable")
		return false
	}
	if s.manager == nil && s.storage == nil {
		writeError(w, http.StatusServiceUnavailable, "recording manager is unavailable")
		return false
	}
	return true
}

func writeDerivativeError(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusRequestTimeout, "export request was canceled")
	case errors.Is(err, derivative.ErrNotFound):
		writeError(w, http.StatusNotFound, "export job not found")
	case errors.Is(err, derivative.ErrActive), errors.Is(err, derivative.ErrConflict):
		writeError(w, http.StatusConflict, "export is not available in the current state")
	case errors.Is(err, derivative.ErrCapacity):
		writeError(w, http.StatusTooManyRequests, "export capacity is full")
	case errors.Is(err, derivative.ErrUnsupported):
		writeError(w, http.StatusUnprocessableEntity, "recording cannot be remuxed")
	case errors.Is(err, derivative.ErrUnavailable):
		writeError(w, http.StatusNotImplemented, "remux export is unavailable")
	default:
		writeError(w, http.StatusInternalServerError, "export operation failed")
	}
}
