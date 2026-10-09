package integrity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestVerificationCompletesPersistsAndReloads(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []byte("original"))
	recording.ArchiveRevision = 7
	recording.TimelineRevision = 3
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 2)
	if err != nil {
		t.Fatal(err)
	}
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitJobState(t, service, job.ID, StateCompleted)
	if completed.Result == nil || completed.Result.Status != storage.IntegrityVerified || completed.Result.ObjectsTotal != 1 || completed.Result.ObjectsVerified != 1 {
		t.Fatalf("unexpected completed verification: %#v", completed)
	}
	if !completed.SourceRevisionKnown || completed.SourceArchiveRevision != 7 || completed.SourceTimelineRevision != 3 {
		t.Fatalf("job did not retain source revisions: %+v", completed)
	}
	if projected := ProjectJob(completed, recording); projected.Freshness != FreshnessCurrent || projected.State != StateCompleted {
		t.Fatalf("completed job projection=%+v, want current without state change", projected)
	}
	assertPrivateMode(t, filepath.Join(root, "management"), 0700)
	assertPrivateMode(t, filepath.Join(root, "management", "integrity-jobs"), 0700)
	assertPrivateMode(t, filepath.Join(root, "management", "integrity-jobs", "state.json"), 0600)
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloaded, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, reloaded)
	reloadedJob, err := reloaded.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedJob.State != StateCompleted || reloadedJob.Result == nil || reloadedJob.Result.ObjectsVerified != 1 {
		t.Fatalf("job did not reload: %#v", reloadedJob)
	}
	result, ok := reloaded.Status(recording.ID)
	if !ok || result.Status != storage.IntegrityVerified || result.ObjectsVerified != 1 {
		t.Fatalf("latest result did not reload: %#v, present=%t", result, ok)
	}
	current, ok := reloaded.StatusFor(recording)
	if !ok || current.Freshness != FreshnessCurrent || !current.RevisionKnown || current.SourceArchiveRevision != 7 || current.SourceTimelineRevision != 3 {
		t.Fatalf("reloaded result freshness=%+v present=%t, want current at source revisions", current, ok)
	}
	changed := *recording
	changed.ArchiveRevision++
	stale, ok := reloaded.StatusFor(&changed)
	if !ok || stale.Freshness != FreshnessStale || stale.Status != storage.IntegrityVerified {
		t.Fatalf("archive revision change should preserve verified result but mark it stale: %+v present=%t", stale, ok)
	}
	projected := ProjectJob(reloadedJob, &changed)
	if projected.Freshness != FreshnessStale || projected.State != StateCompleted {
		t.Fatalf("job projection after archive revision change=%+v, want stale completed job", projected)
	}
	timelineOnly := *recording
	timelineOnly.TimelineRevision++
	current, ok = reloaded.StatusFor(&timelineOnly)
	if !ok || current.Freshness != FreshnessCurrent || current.SourceArchiveRevision != 7 || current.SourceTimelineRevision != 3 || current.CurrentTimelineRevision != 4 {
		t.Fatalf("timeline-only change should remain current for payload integrity: %+v present=%t", current, ok)
	}
	projected = ProjectJob(reloadedJob, &timelineOnly)
	if projected.Freshness != FreshnessCurrent || projected.CurrentTimelineRevision != 4 {
		t.Fatalf("timeline-only job projection=%+v, want current", projected)
	}
}

func TestActiveVerificationProjectsItsCapturedArchiveRevision(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "efefefefefefefefefefefefefefefef", []byte("original"))
	recording.ArchiveRevision = 7
	recording.TimelineRevision = 3
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)

	prior, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	if completed := waitJobState(t, service, prior.ID, StateCompleted); completed.Result == nil || completed.Result.Status != storage.IntegrityVerified {
		t.Fatalf("initial verification did not complete successfully: %+v", completed)
	}

	// Begin a new verification against a newer archive snapshot while retaining
	// the previously persisted result at revision 7.
	recording.ArchiveRevision = 8
	recording.TimelineRevision = 4
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	service.verify = func(_ context.Context, _ *domain.Recording) storage.IntegrityResult {
		entered <- struct{}{}
		<-release
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	active, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("second verification did not start")
	}

	projection, ok := service.StatusFor(recording)
	if !ok || projection.Status != storage.IntegrityVerifying || projection.Freshness != FreshnessCurrent || !projection.RevisionKnown || projection.SourceArchiveRevision != 8 || projection.SourceTimelineRevision != 4 {
		t.Fatalf("active verification projection=%+v present=%t, want current at active job revision 8", projection, ok)
	}
	service.mu.Lock()
	priorRevision := service.resultRevisions[recording.ID]
	service.mu.Unlock()
	if !priorRevision.Known || priorRevision.Archive != 7 {
		t.Fatalf("active projection overwrote persisted prior result revision: %+v", priorRevision)
	}

	close(release)
	completed := waitJobState(t, service, active.ID, StateCompleted)
	if completed.SourceArchiveRevision != 8 || completed.SourceTimelineRevision != 4 {
		t.Fatalf("completed active job lost captured revisions: %+v", completed)
	}
}

func TestLegacyIntegrityResultFreshnessIsUnknown(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "abababababababababababababababab", []byte("original"))
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)
	result := storage.IntegrityResult{Status: storage.IntegrityVerified, ObjectsTotal: 1, ObjectsVerified: 1, Issues: []storage.IntegrityIssue{}}
	service.mu.Lock()
	service.results[recording.ID] = result
	service.mu.Unlock()
	service.mu.Lock()
	err = service.persistLocked()
	service.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, reloaded)
	projection, ok := reloaded.StatusFor(recording)
	if !ok || projection.Freshness != FreshnessUnknown || projection.RevisionKnown {
		t.Fatalf("unbound legacy result freshness=%+v present=%t, want unknown", projection, ok)
	}
}

func TestLegacyRecordingVerificationRemainsFreshnessUnknown(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd", []byte("original"))
	if recording.ArchiveRevision != 0 {
		t.Fatalf("fixture ArchiveRevision=%d, want legacy zero", recording.ArchiveRevision)
	}
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	completed := waitJobState(t, service, job.ID, StateCompleted)
	if completed.SourceRevisionKnown {
		t.Fatalf("legacy verification job incorrectly claims known revision: %+v", completed)
	}
	if projected := ProjectJob(completed, recording); projected.Freshness != FreshnessUnknown {
		t.Fatalf("legacy job projection freshness=%q, want unknown", projected.Freshness)
	}
	projection, ok := service.StatusFor(recording)
	if !ok || projection.Freshness != FreshnessUnknown {
		t.Fatalf("legacy verification freshness=%+v present=%t, want unknown", projection, ok)
	}
}

func TestVerificationReportsModifiedAndMissingPayload(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remove    bool
		wantIssue string
	}{
		{name: "modified", wantIssue: "payload_mismatch"},
		{name: "missing", remove: true, wantIssue: "missing_payload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			store, recording := makeIntegrityArchive(t, root, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", []byte("original"))
			path := filepath.Join(store.Root(), "recordings", recording.ID, recording.Tracks["main"].Segments[0].StoragePath)
			if tc.remove {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte("modified"), 0600); err != nil {
				t.Fatal(err)
			}
			service, err := Open(root, store, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer closeService(t, service)
			job, err := service.Start(context.Background(), recording)
			if err != nil {
				t.Fatal(err)
			}
			completed := waitJobState(t, service, job.ID, StateCompleted)
			if completed.Result == nil || completed.Result.Status != storage.IntegrityDegraded || len(completed.Result.Issues) != 1 || completed.Result.Issues[0].Code != tc.wantIssue {
				t.Fatalf("unexpected verification result: %#v", completed)
			}
		})
	}
}

func TestV2IntegrityStreamsShardedMediaInitAndManifest(t *testing.T) {
	root := t.TempDir()
	store, header, mediaPath := makeShardedIntegrityArchive(t, root, "abababababababababababababababab")
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)

	staleHeader := *header
	staleHeader.ArchiveRevision = 2
	staleHeader.TimelineRevision = 1
	job, err := service.Start(context.Background(), &staleHeader)
	if err != nil {
		t.Fatal(err)
	}
	if !job.SourceRevisionKnown || job.SourceArchiveRevision != header.ArchiveRevision || job.SourceTimelineRevision != header.TimelineRevision {
		t.Fatalf("v2 integrity job did not capture current canonical header revisions: job=%+v header_archive=%d header_timeline=%d", job, header.ArchiveRevision, header.TimelineRevision)
	}
	completed := waitJobState(t, service, job.ID, StateCompleted)
	if completed.Result == nil || completed.Result.Status != storage.IntegrityVerified || completed.Result.ObjectsTotal != 3 || completed.Result.ObjectsVerified != 3 {
		t.Fatalf("v2 verification result=%+v, want three verified objects", completed.Result)
	}

	if _, err := store.SavePayload(header.ID, mediaPath, bytes.NewReader([]byte("modified")), storage.MaxObjectBytes); err != nil {
		t.Fatal(err)
	}
	second, err := service.Start(context.Background(), header)
	if err != nil {
		t.Fatal(err)
	}
	degraded := waitJobState(t, service, second.ID, StateCompleted)
	if degraded.Result == nil || degraded.Result.Status != storage.IntegrityDegraded || degraded.Result.ObjectsTotal != 3 || degraded.Result.ObjectsVerified != 2 || degraded.Result.ObjectsCorrupt != 1 || len(degraded.Result.Issues) != 1 || degraded.Result.Issues[0].Code != "payload_mismatch" {
		t.Fatalf("v2 corrupted payload result=%+v", degraded.Result)
	}
}

func TestDuplicateQueuedOrRunningVerificationCoalesces(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "cccccccccccccccccccccccccccccccc", []byte("original"))
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	service.verify = func(_ context.Context, _ *domain.Recording) storage.IntegrityResult {
		entered <- struct{}{}
		<-release
		return storage.IntegrityResult{Status: storage.IntegrityVerified, LastVerifiedAt: time.Now().UTC(), Issues: []storage.IntegrityIssue{}}
	}
	first, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("verification did not start")
	}
	if status, ok := service.Status(recording.ID); !ok || status.Status != storage.IntegrityVerifying {
		t.Fatalf("active verification status = %#v, present=%t; want verifying", status, ok)
	}
	second, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || second.State != StateRunning {
		t.Fatalf("duplicate did not coalesce: first=%#v second=%#v", first, second)
	}
	close(release)
	waitJobState(t, service, first.ID, StateCompleted)
}

func TestWaitForIdleDrainsQueuedAndRunningVerificationsWithoutClosingService(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	service.verify = func(ctx context.Context, _ *domain.Recording) storage.IntegrityResult {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	first, err := service.Start(context.Background(), &domain.Recording{ID: "12121212121212121212121212121212", Tracks: map[string]*domain.Track{}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first verification did not start")
	}
	second, err := service.Start(context.Background(), &domain.Recording{ID: "34343434343434343434343434343434", Tracks: map[string]*domain.Track{}})
	if err != nil {
		t.Fatal(err)
	}
	if job, err := service.Get(second.ID); err != nil || job.State != StateQueued {
		t.Fatalf("second job=%+v err=%v, want queued behind blocked worker", job, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = service.WaitForIdle(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForIdle error=%v, want deadline while verifications are blocked", err)
	}
	if job, err := service.Get(first.ID); err != nil || job.State != StateRunning {
		t.Fatalf("WaitForIdle canceled or closed the running job: %+v err=%v", job, err)
	}
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.WaitForIdle(ctx); err != nil {
		t.Fatalf("WaitForIdle after releasing worker: %v", err)
	}
	for _, id := range []string{first.ID, second.ID} {
		job, err := service.Get(id)
		if err != nil || job.State != StateCompleted {
			t.Fatalf("job %s did not complete before idle: %+v err=%v", id, job, err)
		}
	}
	if err := service.WaitForIdle(nil); err != nil {
		t.Fatalf("WaitForIdle(nil) on idle service: %v", err)
	}
	if _, err := service.Start(context.Background(), &domain.Recording{ID: "56565656565656565656565656565656", Tracks: map[string]*domain.Track{}}); err != nil {
		t.Fatalf("WaitForIdle closed the service: %v", err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerAndActiveJobBounds(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 100)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, maxWorkers+1)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	service.verify = func(_ context.Context, _ *domain.Recording) storage.IntegrityResult {
		current := active.Add(1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	for i := 1; i <= maxActiveJobs; i++ {
		recording := &domain.Recording{ID: fmt.Sprintf("%032x", i), Tracks: map[string]*domain.Track{}}
		if _, err = service.Start(context.Background(), recording); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	for i := 0; i < maxWorkers; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("bounded workers did not begin work")
		}
	}
	if got := maximum.Load(); got != maxWorkers {
		t.Fatalf("active verification workers=%d, want %d", got, maxWorkers)
	}
	if _, err = service.Start(context.Background(), &domain.Recording{ID: "ffffffffffffffffffffffffffffffff", Tracks: map[string]*domain.Track{}}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("65th active job error=%v, want ErrCapacity", err)
	}
	for i := 1; i <= maxActiveJobs; i++ {
		if !service.InProgress(fmt.Sprintf("%032x", i)) {
			t.Fatalf("recording %d should be active", i)
		}
	}
	close(release)
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got > maxWorkers {
		t.Fatalf("worker bound exceeded: %d", got)
	}
}

func TestContextCancellationCancelsQueuedAndRunningJobs(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	service.verify = func(ctx context.Context, _ *domain.Recording) storage.IntegrityResult {
		entered <- struct{}{}
		<-ctx.Done()
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	runningCtx, cancelRunning := context.WithCancel(context.Background())
	running, err := service.Start(runningCtx, &domain.Recording{ID: "11111111111111111111111111111111", Tracks: map[string]*domain.Track{}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("running verification did not start")
	}
	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	queued, err := service.Start(queuedCtx, &domain.Recording{ID: "22222222222222222222222222222222", Tracks: map[string]*domain.Track{}})
	if err != nil {
		t.Fatal(err)
	}
	cancelRunning()
	cancelQueued()
	if got := waitJobState(t, service, running.ID, StateCanceled); got.ErrorCode != ErrorCanceled {
		t.Fatalf("running cancellation result: %#v", got)
	}
	if got := waitJobState(t, service, queued.ID, StateCanceled); got.ErrorCode != ErrorCanceled {
		t.Fatalf("queued cancellation result: %#v", got)
	}
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExplicitCancelStopsAndPersistsRunningVerification(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	service.verify = func(ctx context.Context, _ *domain.Recording) storage.IntegrityResult {
		entered <- struct{}{}
		<-ctx.Done()
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	job, err := service.Start(context.Background(), &domain.Recording{ID: "abababababababababababababababab", Tracks: map[string]*domain.Track{}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("verification did not start")
	}
	canceled, err := service.Cancel(job.ID)
	if err != nil || canceled.State != StateCanceled {
		t.Fatalf("Cancel() = %+v, %v", canceled, err)
	}
	if got := waitJobState(t, service, job.ID, StateCanceled); got.ErrorCode != ErrorCanceled {
		t.Fatalf("terminal canceled job = %+v", got)
	}
	repeated, err := service.Cancel(job.ID)
	if err != nil || repeated.State != StateCanceled {
		t.Fatalf("repeated Cancel() = %+v, %v", repeated, err)
	}
	if service.InProgress(job.RecordingID) {
		t.Fatal("canceled verification still owns its recording")
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close(context.Background())
	loadedJob, err := reloaded.Get(job.ID)
	if err != nil || loadedJob.State != StateCanceled {
		t.Fatalf("canceled job did not persist: %+v, %v", loadedJob, err)
	}
}

func TestRestartRecoveryFailsPersistedActiveJobs(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	job := Job{ID: "33333333333333333333333333333333", RecordingID: "44444444444444444444444444444444", State: StateRunning, CreatedAt: time.Now().UTC(), StartedAt: timePointer(time.Now().UTC())}
	result := storage.IntegrityResult{Status: storage.IntegrityVerifying, Issues: []storage.IntegrityIssue{}}
	jobDir := filepath.Join(root, "management", "integrity-jobs")
	if err = os.MkdirAll(jobDir, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(diskState{Version: 1, Jobs: []Job{job}, Results: map[string]storage.IntegrityResult{job.RecordingID: result}})
	if err != nil {
		t.Fatal(err)
	}
	if err = atomicWrite(jobDir, filepath.Join(jobDir, "state.json"), data); err != nil {
		t.Fatal(err)
	}

	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)
	recovered, err := service.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != StateFailed || recovered.ErrorCode != ErrorInterruptedByRestart || recovered.FinishedAt == nil {
		t.Fatalf("stuck job was not recovered: %#v", recovered)
	}
	status, ok := service.Status(job.RecordingID)
	if !ok || status.Status != storage.IntegrityUnknown {
		t.Fatalf("verifying status was not reset: %#v, present=%t", status, ok)
	}
}

func TestCloseJoinsWorkersRespectingCallerDeadline(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	service.verify = func(ctx context.Context, _ *domain.Recording) storage.IntegrityResult {
		entered <- struct{}{}
		<-ctx.Done()
		<-release
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	if _, err = service.Start(context.Background(), &domain.Recording{ID: "55555555555555555555555555555555", Tracks: map[string]*domain.Track{}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("verification did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = service.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error=%v, want deadline", err)
	}
	close(release)
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-service.done:
	default:
		t.Fatal("Close returned before workers joined")
	}
}

func TestJobSerializationDoesNotExposeURIsPathsOrRawErrors(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "66666666666666666666666666666666", []byte("source"))
	recording.SourceURL = "https://user:password@example.invalid/watch?token=credential"
	recording.Tracks["main"].Segments[0].SourceURI = "https://cdn.invalid/segment?sig=private"
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	job = waitJobState(t, service, job.ID, StateCompleted)
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(encoded)
	for _, secret := range []string{"password", "credential", "cdn.invalid", store.Root(), "https://user", "https://cdn"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("job serialization leaked %q: %s", secret, serialized)
		}
	}
}

func TestSnapshotIsDeepCloned(t *testing.T) {
	root := t.TempDir()
	store, recording := makeIntegrityArchive(t, root, "77777777777777777777777777777777", []byte("original"))
	service, err := Open(root, store, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer closeService(t, service)
	entered := make(chan *domain.Recording, 1)
	release := make(chan struct{})
	service.verify = func(_ context.Context, snapshot *domain.Recording) storage.IntegrityResult {
		entered <- snapshot
		<-release
		return storage.IntegrityResult{Status: storage.IntegrityVerified, Issues: []storage.IntegrityIssue{}}
	}
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot *domain.Recording
	select {
	case snapshot = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("verification did not begin")
	}
	recording.Title = "mutated"
	recording.Tracks["main"].Segments[0].StoragePath = "outside"
	if snapshot.Title == recording.Title || snapshot.Tracks["main"].Segments[0].StoragePath == "outside" {
		t.Fatal("verification did not receive a deep snapshot")
	}
	close(release)
	waitJobState(t, service, job.ID, StateCompleted)
}

func TestSafeResultRejectsUnsafeIssuePath(t *testing.T) {
	result := storage.IntegrityResult{Status: storage.IntegrityDegraded, Issues: []storage.IntegrityIssue{{Code: "missing_payload", Path: "../secret"}}}
	if validResult(result) {
		t.Fatal("unsafe issue path accepted")
	}
	result.Issues[0].Path = "tracks/main/segment.m4s"
	if !validResult(result) {
		t.Fatal("safe recording-relative issue path rejected")
	}
	result.Issues[0].Code = "https:token=secret"
	if validResult(result) {
		t.Fatal("unrecognized issue code accepted")
	}
}

func makeIntegrityArchive(t *testing.T, root, id string, payload []byte) (*storage.Store, *domain.Recording) {
	t.Helper()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	recording := &domain.Recording{
		FormatVersion: 1,
		ID:            id,
		State:         domain.StateStopped,
		CreatedAt:     now,
		StartedAt:     now,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
	}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	path := "tracks/main/00000001.ts"
	payloadResult, err := store.SavePayload(id, path, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	track.Segments = []domain.Segment{{
		ID: "segment-1", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1,
		SourceURI: "https://source.invalid/segment.ts", StoragePath: path,
		PayloadSize: payloadResult.Size, SHA256: payloadResult.SHA256,
	}}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	return store, recording
}

func makeShardedIntegrityArchive(t *testing.T, root, id string) (*storage.Store, *domain.Recording, string) {
	t.Helper()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	sessionID := "session-" + strings.Repeat("a", 64)
	header := &domain.Recording{
		FormatVersion:    2,
		ID:               id,
		SourceSessionID:  sessionID,
		State:            domain.StateStopped,
		CreatedAt:        now,
		StartedAt:        now,
		ArchiveRevision:  7,
		TimelineRevision: 3,
		Tracks:           map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}},
		ShardedArchive:   &domain.ShardedArchiveSummary{},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	addObject := func(path string, data []byte) storage.PayloadResult {
		t.Helper()
		result, err := store.SavePayload(id, path, bytes.NewReader(data), storage.MaxObjectBytes)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	initPath := "tracks/main/init.mp4"
	initPayload := []byte("init-payload")
	initObject := addObject(initPath, initPayload)
	initSegment := domain.Segment{ID: "init-one", TrackID: "main", Sequence: 1, SourceURI: "https://source.invalid/init.mp4", StoragePath: initPath, PayloadSize: initObject.Size, SHA256: initObject.SHA256, IsInit: true}
	initCoordinate := archiveindex.Coordinate{SessionID: sessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectInit}
	if err := store.AppendShardedMedia(ctx, id, storage.V2MediaRecord{Coordinate: initCoordinate, Segment: initSegment}); err != nil {
		t.Fatal(err)
	}
	mediaPath := "tracks/main/segment.ts"
	mediaPayload := []byte("media-payload")
	mediaObject := addObject(mediaPath, mediaPayload)
	mediaSegment := domain.Segment{ID: "seg-00000000000000000001", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1, SourceURI: "https://source.invalid/segment.ts", Duration: 2, InitSegmentID: initSegment.ID, StoragePath: mediaPath, PayloadSize: mediaObject.Size, SHA256: mediaObject.SHA256}
	mediaCoordinate := archiveindex.Coordinate{SessionID: sessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	if err := store.AppendShardedMedia(ctx, id, storage.V2MediaRecord{Coordinate: mediaCoordinate, Segment: mediaSegment}); err != nil {
		t.Fatal(err)
	}
	manifestPath := "manifests/main/manifest.m3u8"
	manifestPayload := []byte("#EXTM3U\n")
	manifestObject := addObject(manifestPath, manifestPayload)
	if err := store.AppendShardedManifest(ctx, id, domain.ManifestSnapshot{TrackID: "main", SourceURI: "https://source.invalid/playlist.m3u8", StoragePath: manifestPath, FetchedAt: now, SHA256: manifestObject.SHA256, Size: manifestObject.Size}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadRecordingHeader(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return store, loaded, mediaPath
}

func waitJobState(t *testing.T, service *Service, id string, want State) Job {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := service.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == want {
			return job
		}
		select {
		case <-deadline.C:
			t.Fatalf("job %s state=%s, want %s", id, job.State, want)
		case <-ticker.C:
		}
	}
}

func closeService(t *testing.T, service *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Errorf("close integrity service: %v", err)
	}
}

func assertPrivateMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s permissions=%#o, want %#o", path, got, want)
	}
}
