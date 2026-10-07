package acquire

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
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
	cause := errors.New("synthetic canonical claim-store failure")
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
