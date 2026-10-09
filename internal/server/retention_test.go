package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/systemsettings"
)

func TestRetentionSettingsAPIAndHandlerCompatibility(t *testing.T) {
	root := t.TempDir()
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	s := NewWithOptions(nil, nil, nil, Options{Settings: settings})
	var handler http.Handler = s
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"retention":{"enabled":false,"completed_after_days":30}`) {
		t.Fatalf("settings GET=%d %s", get.Code, get.Body.String())
	}
	update := httptest.NewRecorder()
	handler.ServeHTTP(update, httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(`{"retention":{"enabled":true,"completed_after_days":45}}`)))
	if update.Code != http.StatusOK || !strings.Contains(update.Body.String(), `"enabled":true`) || !strings.Contains(update.Body.String(), `"completed_after_days":45`) {
		t.Fatalf("settings update=%d %s", update.Code, update.Body.String())
	}
	for _, body := range []string{`{"retention":{}}`, `{"retention":{"unknown":true}}`} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid settings body %s: status=%d body=%s", body, response.Code, response.Body.String())
		}
	}
}

func TestRetentionCandidatesFilterAndRedact(t *testing.T) {
	s, store, products, _ := newRetentionFixture(t, false)
	now := time.Now().UTC()
	oldID := strings.Repeat("a", 32)
	unsealedID := strings.Repeat("0", 32)
	recentID := strings.Repeat("b", 32)
	interruptedID := strings.Repeat("c", 32)
	taggedID := strings.Repeat("d", 32)
	stoppedID := strings.Repeat("e", 32)
	createRetentionRecording(t, store, oldID, domain.StateCompleted, now.Add(-60*24*time.Hour))
	unsealed := &domain.Recording{
		FormatVersion: 1, ID: unsealedID, State: domain.StateCompleted,
		CreatedAt: now.Add(-61 * 24 * time.Hour), StartedAt: now.Add(-61 * 24 * time.Hour),
		StoppedAt: timePointer(now.Add(-60 * 24 * time.Hour)),
		Tracks:    map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}},
	}
	if err := store.CreateRecording(unsealed); err != nil {
		t.Fatal(err)
	}
	createRetentionRecording(t, store, recentID, domain.StateCompleted, now.Add(-2*24*time.Hour))
	createRetentionRecording(t, store, interruptedID, domain.StateInterrupted, now.Add(-60*24*time.Hour))
	createRetentionRecording(t, store, taggedID, domain.StateCompleted, now.Add(-60*24*time.Hour))
	createRetentionRecording(t, store, stoppedID, domain.StateStopped, now.Add(-60*24*time.Hour))
	if err := products.SetTags(taggedID, []string{"protected"}); err != nil {
		t.Fatal(err)
	}
	// NewManager discovers the just-created fixture archives.
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.manager = manager
	response := httptest.NewRecorder()
	s.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/retention/candidates", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("candidate preview=%d %s", response.Code, response.Body.String())
	}
	var body struct {
		Enabled        bool                 `json:"enabled"`
		CandidateCount int                  `json:"candidate_count"`
		Candidates     []retentionCandidate `json:"candidates"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Enabled || body.CandidateCount != 1 || len(body.Candidates) != 1 || body.Candidates[0].ID != oldID {
		t.Fatalf("candidate preview = %#v", body)
	}
	if strings.Contains(response.Body.String(), unsealedID) {
		t.Fatalf("unsealed completed recording appeared as retention candidate: %s", response.Body.String())
	}
	if strings.Contains(response.Body.String(), "source_uri") || strings.Contains(response.Body.String(), "storage_path") || strings.Contains(response.Body.String(), "private.invalid") {
		t.Fatalf("candidate response exposed path or source: %s", response.Body.String())
	}
}

func TestRetentionDisabledRunDoesNotDeleteAndEnabledRunClearsProjections(t *testing.T) {
	s, store, products, settings := newRetentionFixture(t, false)
	id := strings.Repeat("f", 32)
	createRetentionRecording(t, store, id, domain.StateCompleted, time.Now().UTC().Add(-60*24*time.Hour))
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.manager = manager
	if err := products.SetTags(id, []string{"old"}); err != nil {
		t.Fatal(err)
	}
	disabled := httptest.NewRecorder()
	s.ServeHTTP(disabled, httptest.NewRequest(http.MethodPost, "/api/retention/run", strings.NewReader(`{}`)))
	if disabled.Code != http.StatusConflict {
		t.Fatalf("disabled run=%d %s", disabled.Code, disabled.Body.String())
	}
	if _, err := manager.Get(id); err != nil {
		t.Fatalf("disabled cleanup deleted recording: %v", err)
	}
	if err := products.SetTags(id, nil); err != nil {
		t.Fatal(err)
	}
	if err := products.AppendRecordingEvent(management.RecordingEvent{ID: "retention-test-event", RecordingID: id, Type: "recording_completed", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if _, err := settings.Update(systemsettings.Patch{RetentionEnabled: retentionPtr(true)}); err != nil {
		t.Fatal(err)
	}
	enabled := httptest.NewRecorder()
	principal := authn.Principal{UserID: "usr-0123456789abcdef0123456789abcdef", Login: "owner", Role: authn.RoleOwner}
	runRequest := httptest.NewRequest(http.MethodPost, "/api/retention/run", strings.NewReader(`{}`))
	runRequest = runRequest.WithContext(authn.WithPrincipal(runRequest.Context(), principal))
	s.ServeHTTP(enabled, runRequest)
	if enabled.Code != http.StatusOK || enabled.Header().Get("X-Audit-Status") != "recorded" || !strings.Contains(enabled.Body.String(), `"deleted_count":1`) || !strings.Contains(enabled.Body.String(), id) {
		t.Fatalf("enabled run=%d %s", enabled.Code, enabled.Body.String())
	}
	if _, err := manager.Get(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("recording remains after cleanup: %v", err)
	}
	if tags, err := products.Tags(id); err != nil || len(tags) != 0 {
		t.Fatalf("recording tags after cleanup=%v err=%v", tags, err)
	}
	if events, err := products.RecordingEvents(id, 10); err != nil || len(events) != 0 {
		t.Fatalf("recording events after cleanup=%v err=%v", events, err)
	}
	audit := products.Audit(10)
	if len(audit) != 1 || audit[0].Type != "recording_deleted" || audit[0].ObjectID != id || audit[0].Actor == nil || audit[0].Actor.Type != management.AuditActorUser || audit[0].Actor.UserID != principal.UserID {
		t.Fatalf("delete audit=%#v", audit)
	}
	for _, body := range []string{"", `null`, `{"unexpected":true}`} {
		response := httptest.NewRecorder()
		s.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/retention/run", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid run body %q: status=%d body=%s", body, response.Code, response.Body.String())
		}
	}
}

func TestRetentionDeletionCleansPreviewPolicyAndProjection(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := settings.Update(systemsettings.Patch{RetentionEnabled: retentionPtr(true)}); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("9", 32)
	createRetentionRecording(t, store, id, domain.StateCompleted, time.Now().UTC().Add(-60*24*time.Hour))
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	previews, err := preview.OpenWithGet(root, store, manager.List, manager.Get, filepath.Join(root, "missing-ffmpeg"))
	if err != nil {
		t.Fatal(err)
	}
	defer previews.Close(context.Background())
	if _, err := previews.SetMode(id, preview.ModeSegment); err != nil {
		t.Fatal(err)
	}
	projection := filepath.Join(root, "previews", id, "frames")
	if err := os.MkdirAll(projection, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projection, "000000000001.jpg"), []byte("disposable"), 0600); err != nil {
		t.Fatal(err)
	}
	api := NewWithOptions(manager, nil, nil, Options{Management: products, Settings: settings, Previews: previews})
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/retention/run", strings.NewReader(`{}`)))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"deleted_count":1`) {
		t.Fatalf("retention run status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Lstat(filepath.Join(root, "previews", id)); !os.IsNotExist(err) {
		t.Fatalf("retention deletion left preview projection: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "management", "previews", id+".json")); !os.IsNotExist(err) {
		t.Fatalf("retention deletion left preview policy: %v", err)
	}
}

func TestRetentionAuditsCanonicalDeleteBeforeProjectionCleanupFailure(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := settings.Update(systemsettings.Patch{RetentionEnabled: retentionPtr(true)}); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("8", 32)
	createRetentionRecording(t, store, id, domain.StateCompleted, time.Now().UTC().Add(-60*24*time.Hour))
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	projectionPath := filepath.Join(root, "management", "recordings", id+".json")
	if err := os.Mkdir(projectionPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(projectionPath, "unsafe-entry"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	api := NewWithOptions(manager, nil, nil, Options{Management: products, Settings: settings})
	principal := authn.Principal{UserID: "usr-0123456789abcdef0123456789abcdef", Login: "owner", Role: authn.RoleOwner}
	request := httptest.NewRequest(http.MethodPost, "/api/retention/run", strings.NewReader(`{}`))
	request = request.WithContext(authn.WithPrincipal(request.Context(), principal))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("X-Audit-Status") != "recorded" || !strings.Contains(response.Body.String(), `"deleted_count":1`) {
		t.Fatalf("cleanup failure response=%d audit=%q body=%s", response.Code, response.Header().Get("X-Audit-Status"), response.Body.String())
	}
	if _, err := manager.Get(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("canonical recording survived deletion: %v", err)
	}
	events := products.Audit(10)
	if len(events) != 1 || events[0].Type != "recording_deleted" || events[0].ObjectID != id || events[0].Actor == nil || events[0].Actor.Type != management.AuditActorUser || events[0].Actor.UserID != principal.UserID {
		t.Fatalf("canonical deletion audit=%+v", events)
	}
}

func TestRetentionCandidatesExcludeActiveDerivativeJob(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "99999999999999999999999999999999"
	recording := writeProductRecording(t, store, id, domain.StateCompleted)
	old := time.Now().UTC().Add(-60 * 24 * time.Hour)
	recording.StoppedAt = &old
	recording.CreatedAt = old
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := settings.Update(systemsettings.Patch{RetentionEnabled: retentionPtr(true)}); err != nil {
		t.Fatal(err)
	}
	releasePath := filepath.Join(root, "release-export")
	ffmpeg := filepath.Join(root, "fake-ffmpeg")
	script := "#!/bin/sh\nwhile [ ! -f \"$RETENTION_RELEASE_FILE\" ]; do /bin/sleep 0.01; done\nprintf artifact > output.mkv\n"
	if err := os.WriteFile(ffmpeg, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RETENTION_RELEASE_FILE", releasePath)
	derivatives, err := derivative.Open(root, store, ffmpeg, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.WriteFile(releasePath, []byte("release"), 0600)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = derivatives.Close(ctx)
	}()
	if _, err := derivatives.Start(context.Background(), recording); err != nil {
		t.Fatal(err)
	}
	if !derivatives.InProgress(id) {
		t.Fatal("fixture derivative job did not become active")
	}
	s := NewWithOptions(manager, nil, nil, Options{Management: products, Derivatives: derivatives, Settings: settings})
	response := httptest.NewRecorder()
	s.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/retention/candidates", nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), id) {
		t.Fatalf("active derivative candidate preview=%d %s", response.Code, response.Body.String())
	}
}

func TestRetentionRunWaitIsCancelableAndPassesSerialize(t *testing.T) {
	s, store, _, _ := newRetentionFixture(t, true)
	id := strings.Repeat("1", 32)
	createRetentionRecording(t, store, id, domain.StateCompleted, time.Now().UTC().Add(-60*24*time.Hour))
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.manager = manager
	// Occupy the pass gate to verify a waiting caller can cancel without
	// starting a scan or deleting anything.
	<-s.retentionGate
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.runRetentionPass(ctx); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled pass error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retention gate wait did not cancel")
	}
	s.retentionGate <- struct{}{}
	if _, err := manager.Get(id); err != nil {
		t.Fatalf("canceled pass deleted recording: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan retentionRunResult, 2)
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := s.runRetentionPass(context.Background())
			results <- result
			errCh <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errCh)
	deleted := 0
	for result := range results {
		deleted += result.DeletedCount
	}
	for err := range errCh {
		if err != nil {
			t.Fatalf("serialized pass error=%v", err)
		}
	}
	if deleted != 1 {
		t.Fatalf("concurrent passes deleted %d recordings, want 1", deleted)
	}
}

func TestRetentionAgeEligibilityRequiresCompletedState(t *testing.T) {
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	old := cutoff.Add(-time.Second)
	for _, state := range []domain.RecordingState{domain.StateRecording, domain.StateStopped, domain.StateInterrupted} {
		if retentionAgeEligible(&domain.Recording{State: state, StoppedAt: &old}, cutoff) {
			t.Fatalf("state %q was eligible for retention", state)
		}
	}
	if retentionAgeEligible(&domain.Recording{State: domain.StateCompleted, ArchiveSealed: true}, cutoff) {
		t.Fatal("completed recording without stopped_at was eligible")
	}
	if retentionAgeEligible(&domain.Recording{State: domain.StateCompleted, StoppedAt: &old}, cutoff) {
		t.Fatal("old unsealed completed recording was eligible")
	}
	if !retentionAgeEligible(&domain.Recording{State: domain.StateCompleted, ArchiveSealed: true, StoppedAt: &old}, cutoff) {
		t.Fatal("old sealed completed recording was not eligible")
	}
}

func TestRetentionDeleteRechecksArchiveSeal(t *testing.T) {
	s, store, _, _ := newRetentionFixture(t, true)
	id := strings.Repeat("7", 32)
	old := time.Now().UTC().Add(-60 * 24 * time.Hour)
	createRetentionRecording(t, store, id, domain.StateCompleted, old)
	recording, err := store.LoadRecordingReadOnly(id)
	if err != nil {
		t.Fatal(err)
	}
	recording.ArchiveSealed = false
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.manager = manager

	lock := s.productLock(id)
	lock.Lock()
	deleted, err := s.deleteRetentionCandidateLocked(context.Background(), id, 30)
	lock.Unlock()
	if err != nil || deleted {
		t.Fatalf("unsealed retention delete deleted=%v err=%v", deleted, err)
	}
	if _, err := manager.Get(id); err != nil {
		t.Fatalf("unsealed recording was deleted: %v", err)
	}
}

func newRetentionFixture(t *testing.T, enabled bool) (*Server, *storage.Store, *management.Store, *systemsettings.Store) {
	t.Helper()
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := settings.Update(systemsettings.Patch{RetentionEnabled: &enabled}); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := NewWithOptions(manager, nil, nil, Options{Management: products, Settings: settings})
	return s, store, products, settings
}

func createRetentionRecording(t *testing.T, store *storage.Store, id string, state domain.RecordingState, stoppedAt time.Time) *domain.Recording {
	t.Helper()
	created := stoppedAt.Add(-time.Hour)
	recording := &domain.Recording{
		FormatVersion: 1, ID: id, State: state, CreatedAt: created, StartedAt: created,
		ArchiveSealed: state == domain.StateCompleted,
		Tracks:        map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}},
	}
	if state != domain.StateRecording {
		recording.StoppedAt = &stoppedAt
	}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	return recording
}

func timePointer(value time.Time) *time.Time { return &value }

func retentionPtr[T any](value T) *T { return &value }
