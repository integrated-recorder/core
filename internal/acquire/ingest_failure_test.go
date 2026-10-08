package acquire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storagediagnostic"
)

type firstFailureSidecarBackend struct {
	storage.StorageBackend
	cause  error
	failed atomic.Bool
}

func (b *firstFailureSidecarBackend) SaveSidecar(id, relativePath string, value any) error {
	if strings.HasPrefix(relativePath, "archive-index/claims/") && b.failed.CompareAndSwap(false, true) {
		return b.cause
	}
	return b.StorageBackend.SaveSidecar(id, relativePath, value)
}

func newIngestFailureArchive(t *testing.T, backend storage.StorageBackend, state domain.RecordingState) (*Manager, *entry, OwnershipToken, *storage.Store, func()) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	root := newRepairTestRoot(t, store, state, media)
	if backend != nil {
		store.StorageBackend = backend
	}
	options := storage.DefaultIngestOptions()
	options.PersistAttempts = 1
	options.RetryBase = 10 * time.Millisecond
	options.RetryMaxBackoff = 10 * time.Millisecond
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, &http.Client{Transport: &repairFixtureTransport{}}, nil, nil, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	fence := &repairOwnerFence{}
	if err := manager.ConfigureCanonicalCommitFence(fence); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	owner := ownerForRepair(root.ID, 1)
	e := &entry{recording: loaded, done: closedChannel(), adapterID: loaded.AdapterID, media: media}
	e.ownership = &owner
	manager.mu.Lock()
	manager.entries[root.ID] = e
	manager.mu.Unlock()
	closeManager := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close manager: %v", err)
		}
	}
	t.Cleanup(closeManager)
	return manager, e, owner, store, closeManager
}

func readFailureTestPayload(t *testing.T, store *storage.Store, recordingID string, body []byte) *storage.IngestPayload {
	t.Helper()
	service, err := store.IngestService()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := service.ReadPayload(context.Background(), recordingID, bytes.NewReader(body), 1<<20, int64(len(body)), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestHistoricalFirstFailureSurvivesQueuedLivePoison(t *testing.T) {
	base, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	root := newRepairTestRoot(t, base, domain.StateRecording, media)
	cause := errors.New("synthetic canonical claim-store failure https://private.invalid/media.m4v?token=signed-secret Authorization: Bearer header-secret")
	backend := &firstFailureSidecarBackend{StorageBackend: base.StorageBackend, cause: cause}
	base.StorageBackend = backend
	options := storage.DefaultIngestOptions()
	options.PersistAttempts = 1
	options.RetryBase = 10 * time.Millisecond
	options.RetryMaxBackoff = 10 * time.Millisecond
	if err := base.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(base, &http.Client{Transport: &repairFixtureTransport{}}, nil, nil, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	fence := &repairOwnerFence{}
	if err := manager.ConfigureCanonicalCommitFence(fence); err != nil {
		t.Fatal(err)
	}
	diagnostics, err := storagediagnostic.Open(base.Root())
	if err != nil {
		t.Fatal(err)
	}
	manager.storageDiagnostics = diagnostics
	loaded, err := base.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	owner := ownerForRepair(root.ID, 1)
	e := &entry{recording: loaded, done: closedChannel(), adapterID: loaded.AdapterID, media: media, ownership: &owner}
	manager.mu.Lock()
	manager.entries[root.ID] = e
	manager.mu.Unlock()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close manager: %v", err)
		}
	})

	historicalBytes := []byte("historical claim payload")
	historical := domain.Segment{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7, Sequence: 13,
		ArchiveOrdinal: 8, TimelineOrdinal: 2, Duration: 4,
		SourceURI: "https://media.example/archive/13.ts",
	}
	firstErr := manager.commitHistoricalPayload(context.Background(), e, owner, false, historical,
		readFailureTestPayload(t, base, root.ID, historicalBytes))
	if firstErr == nil || !errors.Is(firstErr, cause) || !errors.Is(firstErr, storage.ErrCanonicalCommitFailed) {
		t.Fatalf("historical commit error = %v, want canonical failure wrapping backend cause", firstErr)
	}

	e.mu.Lock()
	diagnostic := e.storageFailureDiagnostic
	e.mu.Unlock()
	if diagnostic == nil || !errors.Is(diagnostic, cause) || !strings.Contains(diagnostic.Error(), "historical media payload commit") {
		t.Fatalf("first historical storage diagnostic = %v, want stage and first backend cause", diagnostic)
	}
	var firstDetails storage.IngestFailureDetails
	if !errors.As(diagnostic, &firstDetails) || firstDetails.CurrentJobKind() != storage.IngestJobKindHistoricalMedia ||
		firstDetails.FirstFailureJobKind() != storage.IngestJobKindHistoricalMedia || firstDetails.CurrentAttempts() != 1 || firstDetails.FirstFailureAttempts() != 1 {
		t.Fatalf("first historical failure details = %#v", firstDetails)
	}
	durable, err := diagnostics.Read(root.ID)
	if err != nil {
		t.Fatalf("read durable first failure: %v", err)
	}
	if durable.Classification != storagediagnostic.ClassificationCanonicalCommitFailed || durable.SourceClass != "historical" ||
		durable.CurrentJobKind != string(storage.IngestJobKindHistoricalMedia) || durable.FirstFailureJobKind != string(storage.IngestJobKindHistoricalMedia) ||
		durable.CurrentAttempts != 1 || durable.FirstFailureAttempts != 1 || len(durable.StageChain) == 0 || durable.StageChain[0] != "historical media payload commit" {
		t.Fatalf("durable historical failure details = %+v", durable)
	}
	if len(durable.StageChain) < 2 || durable.StageChain[1] != "persist archive claim shard" {
		t.Fatalf("durable operation stage chain = %v, want claim shard persistence", durable.StageChain)
	}
	encoded, err := os.ReadFile(filepath.Join(base.Root(), "management", "diagnostics", "recordings", root.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private.invalid", "signed-secret", "header-secret", "Authorization"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("durable diagnostic leaked %q", secret)
		}
	}

	liveCalls := atomic.Int32{}
	liveDone := make(chan error, 1)
	liveBytes := []byte("live follow-up payload")
	livePayload := readFailureTestPayload(t, base, root.ID, liveBytes)
	service, err := base.IngestService()
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SubmitWithKind(context.Background(), livePayload, storage.IngestJobKindMediaPayload,
		func([]byte) (storage.PayloadResult, error) {
			liveCalls.Add(1)
			return storage.PayloadResult{}, errors.New("follow-up callback must not run")
		}, func(_ storage.PayloadResult, err error) { liveDone <- err }); err != nil {
		livePayload.Release()
		t.Fatal(err)
	}
	var followupErr error
	select {
	case followupErr = <-liveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("poisoned live follow-up did not complete")
	}
	if !errors.Is(followupErr, cause) || !errors.Is(followupErr, storage.ErrCanonicalCommitFailed) {
		t.Fatalf("poisoned live error = %v, want first historical cause", followupErr)
	}
	if liveCalls.Load() != 0 {
		t.Fatalf("poisoned live persistence callback ran %d times", liveCalls.Load())
	}
	afterPoison, err := diagnostics.Read(root.ID)
	if err != nil || afterPoison.StageChain[0] != durable.StageChain[0] || afterPoison.FirstFailureJobKind != durable.FirstFailureJobKind {
		t.Fatalf("poisoned follow-up replaced the first durable diagnostic: %+v err=%v", afterPoison, err)
	}
	var followupDetails storage.IngestFailureDetails
	if !errors.As(followupErr, &followupDetails) || followupDetails.CurrentJobKind() != storage.IngestJobKindMediaPayload ||
		followupDetails.FirstFailureJobKind() != storage.IngestJobKindHistoricalMedia || followupDetails.CurrentAttempts() != 0 ||
		followupDetails.FirstFailureAttempts() != 1 {
		t.Fatalf("poisoned follow-up details = %#v", followupDetails)
	}
	if got := safeFailureDescription(newStorageCommitFailure("media payload commit", followupErr)); got != "recording storage commit failed" {
		t.Fatalf("public failure description = %q", got)
	}
	current, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(current.Tracks["main"].Segments); got != 1 {
		t.Fatalf("poisoned live payload changed canonical root: segment count=%d", got)
	}
}

func TestStorageDiagnosticClassifiesArchiveAndFenceCauses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"archive index unavailable", fmt.Errorf("%w: claim shard is unreadable", ErrArchiveIndexUnavailable), "archive_index_unavailable"},
		{"invalid archive claim", fmt.Errorf("%w: invalid segment metadata", archiveindex.ErrInvalidClaim), "archive_claim_invalid"},
		{"stale owner", recordingowner.ErrStaleOwner, "stale_owner"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := buildStorageFailureDiagnostic(strings.Repeat("a", 32), "recording", newStorageCommitFailure("historical media payload commit", test.err), storage.IngestSnapshot{})
			for _, category := range got.ErrorCategories {
				if category == test.want {
					return
				}
			}
			t.Fatalf("categories = %v, want %q", got.ErrorCategories, test.want)
		})
	}
}

func TestConcurrentLiveAndHistoricalCommitsShareOrderedIngestWriter(t *testing.T) {
	manager, e, owner, store, _ := newIngestFailureArchive(t, nil, domain.StateRecording)
	recordingID := e.recording.ID
	type commitResult struct {
		sequence uint64
		err      error
	}
	results := make(chan commitResult, 3)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	defer func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	}()
	for _, job := range []struct {
		sequence uint64
		claim    archiveindex.ClaimSource
		kind     storage.IngestJobKind
		body     string
	}{
		{sequence: 13, claim: archiveindex.ClaimHistorical, kind: storage.IngestJobKindHistoricalMedia, body: "shared-concurrent-payload"},
		{sequence: 13, claim: archiveindex.ClaimLiveOrigin, kind: storage.IngestJobKindMediaPayload, body: "shared-concurrent-payload"},
		{sequence: 14, claim: archiveindex.ClaimLiveOrigin, kind: storage.IngestJobKindMediaPayload, body: "live-concurrent-tail"},
	} {
		job := job
		payload := readFailureTestPayload(t, store, recordingID, []byte(job.body))
		if err := manager.ingest.SubmitWithKind(context.Background(), payload, job.kind, func(data []byte) (storage.PayloadResult, error) {
			if job.sequence == 13 && job.claim == archiveindex.ClaimHistorical {
				close(firstStarted)
				<-releaseFirst
			}
			segment := domain.Segment{
				TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7,
				Sequence: job.sequence, Duration: 2,
				SourceURI: "https://media.example/concurrent/" + job.body + ".ts",
			}
			result, _, err := manager.commitArchiveSegmentOwned(e, &owner, segment, job.claim, data, false)
			return result, err
		}, func(_ storage.PayloadResult, err error) { results <- commitResult{sequence: job.sequence, err: err} }); err != nil {
			payload.Release()
			t.Fatal(err)
		}
		if job.sequence == 13 && job.claim == archiveindex.ClaimHistorical {
			select {
			case <-firstStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("historical writer callback did not start")
			}
		}
	}
	close(releaseFirst)
	for range 3 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("sequence %d commit failed: %v", result.sequence, result.err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent canonical commits did not complete")
		}
	}

	current, err := manager.Get(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	track := current.Tracks["main"]
	if len(track.Segments) != 3 || current.TimelineRevision <= 1 {
		t.Fatalf("canonical projection after concurrent commits: segments=%d revision=%d", len(track.Segments), current.TimelineRevision)
	}
	for index, want := range []struct {
		sequence uint64
		body     string
	}{
		{sequence: 12, body: "legacy-selected-tail"},
		{sequence: 13, body: "shared-concurrent-payload"},
		{sequence: 14, body: "live-concurrent-tail"},
	} {
		segment := track.Segments[index]
		if segment.Sequence != want.sequence || segment.ArchiveOrdinal != uint64(index+7) || segment.TimelineOrdinal != uint64(index+1) {
			t.Fatalf("segment[%d] ordering = sequence %d archive %d timeline %d", index, segment.Sequence, segment.ArchiveOrdinal, segment.TimelineOrdinal)
		}
		if segment.PayloadSize != int64(len(want.body)) || segment.SHA256 != sha256Hex([]byte(want.body)) {
			t.Fatalf("segment[%d] payload identity changed: size=%d hash=%q", index, segment.PayloadSize, segment.SHA256)
		}
	}
	inventory, err := manager.ArchiveInventory(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		sequence uint64
		claim    archiveindex.ClaimSource
	}{
		{sequence: 13, claim: archiveindex.ClaimHistorical},
		{sequence: 14, claim: archiveindex.ClaimLiveOrigin},
	} {
		found := false
		for _, item := range inventory.Segments {
			if item.Coordinate.Sequence != expected.sequence {
				continue
			}
			for _, claim := range item.Claims {
				if claim.Source == expected.claim && claim.Disposition == archiveindex.DispositionAccepted {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("sequence %d lacks accepted %s claim", expected.sequence, expected.claim)
		}
	}
	sequence13Accepted, sequence13Duplicate := 0, 0
	for _, item := range inventory.Segments {
		if item.Coordinate.Sequence == 13 {
			for _, claim := range item.Claims {
				if claim.Source == archiveindex.ClaimHistorical && claim.Disposition == archiveindex.DispositionAccepted {
					sequence13Accepted++
				}
				if claim.Source == archiveindex.ClaimLiveOrigin && claim.Disposition == archiveindex.DispositionDuplicate {
					sequence13Duplicate++
				}
			}
		}
	}
	if sequence13Accepted != 1 || sequence13Duplicate != 1 {
		t.Fatalf("identical coordinate dispositions: accepted=%d duplicate=%d, want one each", sequence13Accepted, sequence13Duplicate)
	}
}

func TestHistoricalInitFailureDiagnosticStoresOnlyFilesystemOperationAndErrno(t *testing.T) {
	manager, e, owner, store, _ := newIngestFailureArchive(t, nil, domain.StateRecording)
	diagnostics, err := storagediagnostic.Open(store.Root())
	if err != nil {
		t.Fatal(err)
	}
	manager.storageDiagnostics = diagnostics
	pathSecret := "/private/archive?token=filesystem-secret"
	cause := &os.PathError{Op: "rename", Path: pathSecret, Err: syscall.ENOSPC}
	store.StorageBackend = &firstFailureSidecarBackend{StorageBackend: store.StorageBackend, cause: cause}
	segment := domain.Segment{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7, Sequence: 13,
		Duration: 4, IsInit: true, SourceURI: "https://media.example/init.mp4",
	}
	err = manager.commitHistoricalPayload(context.Background(), e, owner, false, segment,
		readFailureTestPayload(t, store, e.recording.ID, []byte("historical init bytes")))
	if err == nil || !errors.Is(err, syscall.ENOSPC) || !errors.Is(err, storage.ErrCanonicalCommitFailed) {
		t.Fatalf("historical init commit error=%v, want canonical ENOSPC", err)
	}
	diagnostic, err := diagnostics.Read(e.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostic.FileOperation != "rename" || diagnostic.ErrnoCode != int(syscall.ENOSPC) ||
		diagnostic.SourceClass != "historical" || diagnostic.CurrentJobKind != string(storage.IngestJobKindHistoricalInit) ||
		len(diagnostic.StageChain) == 0 || diagnostic.StageChain[0] != "historical init payload commit" {
		t.Fatalf("historical init diagnostic=%+v", diagnostic)
	}
	encoded, err := os.ReadFile(filepath.Join(store.Root(), "management", "diagnostics", "recordings", e.recording.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), pathSecret) || strings.Contains(string(encoded), "filesystem-secret") {
		t.Fatal("filesystem path or query credential was persisted")
	}
}

type failingStorageDiagnosticRecorder struct {
	err   error
	calls atomic.Int32
}

func (r *failingStorageDiagnosticRecorder) RecordFirst(d storagediagnostic.Diagnostic) (storagediagnostic.Diagnostic, bool, error) {
	r.calls.Add(1)
	return storagediagnostic.Diagnostic{}, false, r.err
}

func TestDiagnosticPersistenceFailureDoesNotReplaceCanonicalFailure(t *testing.T) {
	manager, e, _, _, _ := newIngestFailureArchive(t, nil, domain.StateRecording)
	persistFailure := errors.New("private diagnostic filesystem failure")
	recorder := &failingStorageDiagnosticRecorder{err: persistFailure}
	manager.storageDiagnostics = recorder
	cause := errors.New("original canonical cause")
	failure := newStorageCommitFailure("media payload commit", cause)
	manager.recordStorageFailureDiagnostic(e, failure)

	e.mu.Lock()
	got := e.storageFailureDiagnostic
	writeFailures := e.storageDiagnosticPersistenceFailures
	e.mu.Unlock()
	if !errors.Is(got, cause) || !errors.Is(got, errStorageCommit) || writeFailures != 1 || recorder.calls.Load() != 1 {
		t.Fatalf("canonical failure changed after diagnostic write error: err=%v write_failures=%d calls=%d", got, writeFailures, recorder.calls.Load())
	}
	if safeFailureDescription(got) != "recording storage commit failed" {
		t.Fatalf("public error=%q", safeFailureDescription(got))
	}
}

func TestTerminalCoordinatorFailurePersistsSeparateClassification(t *testing.T) {
	manager, e, _, store, _ := newIngestFailureArchive(t, nil, domain.StateRecording)
	diagnostics, err := storagediagnostic.Open(store.Root())
	if err != nil {
		t.Fatal(err)
	}
	manager.storageDiagnostics = diagnostics
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &segmentScheduler{manager: manager, e: e, ctx: ctx, cancel: cancel, changed: make(chan struct{})}
	scheduler.failCoordinator("media payload commit", storage.ErrIngestCoordinatorUnavailable)
	if !errors.Is(scheduler.failure(), storage.ErrIngestCoordinatorUnavailable) || errors.Is(scheduler.failure(), errStorageCommit) {
		t.Fatalf("coordinator failure classification changed: %v", scheduler.failure())
	}
	diagnostic, err := diagnostics.Read(e.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostic.Classification != storagediagnostic.ClassificationCoordinatorUnavailable ||
		len(diagnostic.StageChain) != 1 || diagnostic.StageChain[0] != "media payload commit" {
		t.Fatalf("coordinator diagnostic=%+v", diagnostic)
	}
}

func TestConcurrentStorageFailureDiagnosticUsesTheFirstMemoryAndDurableCause(t *testing.T) {
	manager, e, _, store, _ := newIngestFailureArchive(t, nil, domain.StateRecording)
	diagnostics, err := storagediagnostic.Open(store.Root())
	if err != nil {
		t.Fatal(err)
	}
	manager.storageDiagnostics = diagnostics
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, stage := range []string{"live media payload commit", "historical media payload commit"} {
		stage := stage
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			manager.recordStorageFailureDiagnostic(e, newStorageCommitFailure(stage, errors.New(stage+" cause")))
		}()
	}
	close(start)
	wg.Wait()
	e.mu.Lock()
	memoryFailure := e.storageFailureDiagnostic
	e.mu.Unlock()
	var memoryStage interface{ Stage() string }
	if !errors.As(memoryFailure, &memoryStage) {
		t.Fatalf("first in-memory diagnostic has no stage: %v", memoryFailure)
	}
	durable, err := diagnostics.Read(e.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(durable.StageChain) == 0 || durable.StageChain[0] != memoryStage.Stage() {
		t.Fatalf("first diagnostic diverged across memory/disk: memory=%q durable=%+v", memoryStage.Stage(), durable)
	}
}
