package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	retentionScanLimit     = 100000
	retentionDeleteLimit   = 100
	retentionResponseLimit = 100
	retentionInterval      = 24 * time.Hour
)

type retentionCandidate struct {
	ID        string    `json:"id"`
	StoppedAt time.Time `json:"stopped_at"`
}

type retentionScan struct {
	Candidates []retentionCandidate
}

type retentionRunResult struct {
	CandidateCount int      `json:"candidate_count"`
	DeletedCount   int      `json:"deleted_count"`
	DeletedIDs     []string `json:"deleted_ids"`
	auditFailed    bool     `json:"-"`
}

func (s *Server) registerRetentionRoutes() {
	s.mux.HandleFunc("GET /api/retention/candidates", s.retentionCandidatesGet)
	s.mux.HandleFunc("POST /api/retention/run", s.retentionRunPost)
}

// RunRetention performs a startup pass, then checks once every 24 hours. The
// caller owns ctx and should join this method before closing the manager.
func (s *Server) RunRetention(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.runScheduledRetentionPassWithAdmission(ctx) {
		return
	}
	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.runScheduledRetentionPassWithAdmission(ctx) {
				return
			}
		}
	}
}

func (s *Server) runScheduledRetentionPassWithAdmission(ctx context.Context) bool {
	if s.backgroundMutationGate == nil {
		s.runScheduledRetentionPass(ctx)
		return ctx.Err() == nil
	}
	release, err := s.backgroundMutationGate.WaitAdmission(ctx)
	if err != nil {
		return false
	}
	defer release()
	s.runScheduledRetentionPass(ctx)
	return ctx.Err() == nil
}

func (s *Server) runScheduledRetentionPass(ctx context.Context) {
	if _, err := s.runRetentionPass(ctx); err != nil && !errors.Is(err, errRetentionDisabled) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if s.logs != nil {
			s.logs.Add("error", "retention", "scheduled retention pass failed")
		}
	}
}

func (s *Server) retentionCandidatesGet(w http.ResponseWriter, r *http.Request) {
	if s.settings == nil || s.manager == nil || s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "retention management is unavailable")
		return
	}
	settings := s.settings.Current().Retention
	scan, err := s.scanRetentionCandidates(r.Context(), settings.CompletedAfterDays)
	if err != nil {
		writeRetentionScanError(w, err)
		return
	}
	items := scan.Candidates
	if len(items) > retentionResponseLimit {
		items = items[:retentionResponseLimit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": settings.Enabled, "candidate_count": len(scan.Candidates),
		"candidates": items, "items_limit": retentionResponseLimit,
	})
}

func (s *Server) retentionRunPost(w http.ResponseWriter, r *http.Request) {
	var empty map[string]json.RawMessage
	if err := decodeJSONBody(w, r, 1024, &empty); err != nil {
		writeError(w, http.StatusBadRequest, "invalid retention request")
		return
	}
	if empty == nil || len(empty) != 0 {
		writeError(w, http.StatusBadRequest, "invalid retention request")
		return
	}
	if s.settings == nil || s.manager == nil || s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "retention management is unavailable")
		return
	}
	if !s.settings.Current().Retention.Enabled {
		writeError(w, http.StatusConflict, "retention cleanup is disabled")
		return
	}
	var actor *management.AuditActor
	if principal, ok := authn.PrincipalFromContext(r.Context()); ok {
		actor = &management.AuditActor{Type: management.AuditActorUser, UserID: principal.UserID}
	}
	result, err := s.runRetentionPassAs(r.Context(), actor)
	if result.auditFailed {
		w.Header().Set("X-Audit-Status", "failed")
	} else if result.DeletedCount > 0 {
		w.Header().Set("X-Audit-Status", "recorded")
	} else {
		w.Header().Set("X-Audit-Status", "not_needed")
	}
	if errors.Is(err, errRetentionDisabled) {
		writeError(w, http.StatusConflict, "retention cleanup is disabled")
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		writeJSON(w, http.StatusRequestTimeout, result)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "retention cleanup could not be completed", "result": result})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

var errRetentionDisabled = errors.New("retention cleanup is disabled")

func (s *Server) runRetentionPass(ctx context.Context) (retentionRunResult, error) {
	return s.runRetentionPassAs(ctx, nil)
}

func (s *Server) runRetentionPassAs(ctx context.Context, actor *management.AuditActor) (retentionRunResult, error) {
	result := retentionRunResult{DeletedIDs: []string{}}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case <-s.retentionGate:
	}
	defer func() { s.retentionGate <- struct{}{} }()
	if s.settings == nil || s.manager == nil || s.products == nil {
		return result, errors.New("retention management is unavailable")
	}
	settings := s.settings.Current().Retention
	if !settings.Enabled {
		return result, errRetentionDisabled
	}
	scan, err := s.scanRetentionCandidates(ctx, settings.CompletedAfterDays)
	if err != nil {
		return result, err
	}
	result.CandidateCount = len(scan.Candidates)
	limit := len(scan.Candidates)
	if limit > retentionDeleteLimit {
		limit = retentionDeleteLimit
	}
	for _, candidate := range scan.Candidates[:limit] {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		lock := s.productLock(candidate.ID)
		lock.Lock()
		deleted, auditFailed, deleteErr := s.deleteRetentionCandidateLockedAs(ctx, candidate.ID, settings.CompletedAfterDays, actor)
		lock.Unlock()
		result.auditFailed = result.auditFailed || auditFailed
		if deleted {
			result.DeletedIDs = append(result.DeletedIDs, candidate.ID)
			result.DeletedCount++
		}
		if deleteErr != nil {
			return result, deleteErr
		}
	}
	return result, nil
}

func (s *Server) scanRetentionCandidates(ctx context.Context, days int) (retentionScan, error) {
	if err := ctx.Err(); err != nil {
		return retentionScan{}, err
	}
	if days < 1 || days > 3650 {
		return retentionScan{}, errors.New("retention policy is invalid")
	}
	recordings, err := s.manager.ListForManagement(ctx, retentionScanLimit)
	if err != nil {
		return retentionScan{}, err
	}
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	scan := retentionScan{Candidates: make([]retentionCandidate, 0)}
	for _, listed := range recordings {
		if err := ctx.Err(); err != nil {
			return retentionScan{}, err
		}
		if listed == nil || !retentionAgeEligible(listed, cutoff) {
			continue
		}
		lock := s.productLock(listed.ID)
		lock.RLock()
		recording, getErr := s.recordingSnapshot(ctx, listed.ID)
		if errors.Is(getErr, storage.ErrNotFound) {
			lock.RUnlock()
			continue
		}
		if getErr != nil {
			lock.RUnlock()
			return retentionScan{}, getErr
		}
		eligible := retentionAgeEligible(recording, cutoff)
		if eligible {
			tags, tagsErr := s.products.Tags(recording.ID)
			if tagsErr != nil {
				lock.RUnlock()
				return retentionScan{}, tagsErr
			}
			eligible = len(tags) == 0 && !s.recordingJobInProgress(recording.ID)
		}
		lock.RUnlock()
		if eligible {
			scan.Candidates = append(scan.Candidates, retentionCandidate{ID: recording.ID, StoppedAt: recording.StoppedAt.UTC()})
		}
	}
	if err := ctx.Err(); err != nil {
		return retentionScan{}, err
	}
	sort.Slice(scan.Candidates, func(i, j int) bool {
		if scan.Candidates[i].StoppedAt.Equal(scan.Candidates[j].StoppedAt) {
			return scan.Candidates[i].ID < scan.Candidates[j].ID
		}
		return scan.Candidates[i].StoppedAt.Before(scan.Candidates[j].StoppedAt)
	})
	return scan, nil
}

func retentionAgeEligible(recording *domain.Recording, cutoff time.Time) bool {
	return recording != nil && recording.State == domain.StateCompleted && recording.ArchiveSealed &&
		recording.StoppedAt != nil && recording.StoppedAt.Before(cutoff)
}

func (s *Server) recordingJobInProgress(id string) bool {
	return s.integrity != nil && s.integrity.InProgress(id) || s.derivatives != nil && s.derivatives.InProgress(id)
}

// deleteRetentionCandidateLocked re-reads all mutable eligibility facts while
// holding the same per-recording write lock as explicit deletion and job APIs.
func (s *Server) deleteRetentionCandidateLocked(ctx context.Context, id string, days int) (bool, error) {
	deleted, _, err := s.deleteRetentionCandidateLockedAs(ctx, id, days, nil)
	return deleted, err
}

func (s *Server) deleteRetentionCandidateLockedAs(ctx context.Context, id string, days int, actor *management.AuditActor) (bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	recording, err := s.recordingSnapshot(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	cutoff := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)
	if !retentionAgeEligible(recording, cutoff) || s.recordingJobInProgress(id) {
		return false, false, nil
	}
	tags, err := s.products.Tags(id)
	if err != nil {
		return false, false, err
	}
	if len(tags) != 0 {
		return false, false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	if err := s.manager.Delete(id); err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, acquire.ErrActiveRecording) {
			return false, false, nil
		}
		return false, false, err
	}
	s.cleanupPreviewProjection(id)
	// Record canonical deletion before best-effort management projection cleanup.
	auditFailed := s.appendAuditAs("recording_deleted", id, actor) != nil
	if err := s.products.ForgetRecording(id); err != nil {
		return true, auditFailed, err
	}
	return true, auditFailed, nil
}

func writeRetentionScanError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusRequestTimeout, "retention candidate scan was canceled")
		return
	}
	if errors.Is(err, acquire.ErrListLimit) {
		writeError(w, http.StatusServiceUnavailable, "retention candidate scan exceeded its limit")
		return
	}
	writeError(w, http.StatusServiceUnavailable, "retention candidates are unavailable")
}
