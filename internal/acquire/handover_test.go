package acquire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	handoverRecordingID = "99999999999999999999999999999999"
	handoverGenA        = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	handoverGenB        = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	handoverWorkerA     = "11111111111111111111111111111111"
	handoverWorkerB     = "22222222222222222222222222222222"
)

type handoverFixture struct {
	mu               sync.Mutex
	max              int
	endList          bool
	manifestObserved chan int
	failManifestPath string
	failSegmentPath  string
	emptyPayloadPath string
	includeInitMap   bool
	invalidSegment   bool
	requests         atomic.Int64
	segmentRequests  atomic.Int64
	metadata         string
	metaCalls        atomic.Int64
}

func (f *handoverFixture) RoundTrip(request *http.Request) (*http.Response, error) {
	var body []byte
	invalidPayload := false
	switch {
	case strings.HasSuffix(request.URL.Path, ".m3u8"):
		f.requests.Add(1)
		f.mu.Lock()
		max := f.max
		endList := f.endList
		observed := f.manifestObserved
		failPath := f.failManifestPath
		includeInitMap := f.includeInitMap
		f.mu.Unlock()
		if request.URL.Path == failPath {
			return nil, errors.New("fixture manifest unavailable")
		}
		if request.URL.Path == "/master.m3u8" {
			body = []byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nvariant.m3u8\n")
			break
		}
		var manifest strings.Builder
		manifest.WriteString("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n")
		for i := 1; i <= max; i++ {
			if includeInitMap && i == 1 {
				manifest.WriteString("#EXT-X-MAP:URI=\"init.mp4\"\n")
			}
			fmt.Fprintf(&manifest, "#EXTINF:1.0,\nsegment-%06d.ts\n", i)
		}
		if endList {
			manifest.WriteString("#EXT-X-ENDLIST\n")
		}
		body = []byte(manifest.String())
		if observed != nil {
			select {
			case observed <- max:
			default:
			}
		}
	case strings.HasPrefix(request.URL.Path, "/segment-"):
		f.segmentRequests.Add(1)
		f.mu.Lock()
		failPath := f.failSegmentPath
		emptyPath := f.emptyPayloadPath
		invalidPayload = f.invalidSegment
		f.mu.Unlock()
		if request.URL.Path == failPath {
			return nil, errors.New("fixture segment unavailable")
		}
		var sequence int
		if _, err := fmt.Sscanf(request.URL.Path, "/segment-%06d.ts", &sequence); err != nil {
			return nil, err
		}
		if request.URL.Path != emptyPath {
			body = []byte(fmt.Sprintf("source-segment-%06d", sequence))
		}
	case request.URL.Path == "/init.mp4":
		f.mu.Lock()
		emptyPath := f.emptyPayloadPath
		f.mu.Unlock()
		if request.URL.Path != emptyPath {
			body = []byte("fixture-init-payload")
		}
	default:
		return nil, fmt.Errorf("unexpected fixture request path %q", request.URL.Path)
	}
	header := make(http.Header)
	if invalidPayload {
		header.Set("Content-Encoding", "gzip")
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       request,
	}, nil
}

type handoverMetadataResolver struct{ fixture *handoverFixture }

func (r *handoverMetadataResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, errors.New("not used in handover fixture")
}

type handoverRefreshResolver struct {
	fixture   *handoverFixture
	next      adapterproto.MediaSource
	refreshes atomic.Int64
	committed atomic.Bool
	fail      bool
}

func (r *handoverRefreshResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, errors.New("not used in handover fixture")
}

func (r *handoverRefreshResolver) PrepareMetadata(context.Context, string, *adapterproto.ResourceRef, adapterproto.MediaSource) (adapterproto.MetadataResult, func() error, bool, error) {
	r.fixture.metaCalls.Add(1)
	r.fixture.mu.Lock()
	title := r.fixture.metadata
	r.fixture.mu.Unlock()
	return adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &title}}, func() error { return nil }, true, nil
}

func (r *handoverRefreshResolver) PrepareRefresh(_ context.Context, adapterID string, resource *adapterproto.ResourceRef, current adapterproto.MediaSource) (adapterproto.MediaSource, func() error, error) {
	r.refreshes.Add(1)
	if adapterID != "fixture" || resource != nil || current.ManifestURL == "" || r.fail {
		return adapterproto.MediaSource{}, nil, errors.New("fixture refresh failed")
	}
	return cloneMediaSource(r.next), func() error { r.committed.Store(true); return nil }, nil
}

func (r *handoverMetadataResolver) PrepareMetadata(context.Context, string, *adapterproto.ResourceRef, adapterproto.MediaSource) (adapterproto.MetadataResult, func() error, bool, error) {
	r.fixture.metaCalls.Add(1)
	r.fixture.mu.Lock()
	title := r.fixture.metadata
	r.fixture.mu.Unlock()
	return adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &title}}, func() error { return nil }, true, nil
}

func newHandoverManager(t *testing.T, store *storage.Store, ownerStore *recordingowner.Store, fixture *handoverFixture, _ bool) *Manager {
	t.Helper()
	return newHandoverManagerWithValidator(t, store, ownerStore, fixture, func(context.Context, string) error { return nil })
}

func newHandoverManagerWithValidator(t *testing.T, store *storage.Store, ownerStore *recordingowner.Store, fixture *handoverFixture, validate SourceValidator) *Manager {
	t.Helper()
	return newHandoverManagerWithResolver(t, store, ownerStore, fixture, validate, &handoverMetadataResolver{fixture: fixture})
}

func newHandoverManagerWithResolver(t *testing.T, store *storage.Store, ownerStore *recordingowner.Store, fixture *handoverFixture, validate SourceValidator, resolver Resolver) *Manager {
	t.Helper()
	// Each handover fixture starts from a controlled empty temporary archive.
	// Recovery is tested through the fenced startup constructor separately.
	manager, err := NewManagerWithMode(store, &http.Client{Transport: fixture}, resolver, validate, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(ownerStore); err != nil {
		t.Fatal(err)
	}
	manager.metadataPollInterval = 8 * time.Millisecond
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close handover manager: %v", err)
		}
	})
	return manager
}

type blockingHistoricalHandoverTransport struct {
	base     *handoverFixture
	manifest []byte
	seen     chan struct{}
	resume   chan struct{}
}

func (t *blockingHistoricalHandoverTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/history.m3u8" {
		select {
		case <-t.seen:
		default:
			close(t.seen)
		}
		select {
		case <-t.resume:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(bytes.NewReader(t.manifest)), ContentLength: int64(len(t.manifest)), Request: request,
		}, nil
	}
	return t.base.RoundTrip(request)
}

func TestHandoverPauseDrainsAutomaticHistoricalRecovery(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "title"}
	transport := &blockingHistoricalHandoverTransport{
		base:     fixture,
		manifest: []byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1.0,\nsegment-000000.ts\n#EXT-X-ENDLIST\n"),
		seen:     make(chan struct{}), resume: make(chan struct{}),
	}
	manager, err := NewManagerWithMode(store, &http.Client{Transport: transport}, &handoverMetadataResolver{fixture: fixture}, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(owners); err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureTerminalOwnerRelease(func(_ context.Context, owner OwnershipToken) error { return owners.Release(owner) }); err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureAutomaticArchiveRecovery(func(_ context.Context, id string) (OwnershipToken, error) {
		return owners.Claim(id, handoverGenA, handoverWorkerA)
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close recovery handover manager: %v", err)
		}
	})

	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	media := adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://fixture.invalid/live.m3u8",
		HistoricalAvailability: &adapterproto.HistoricalAvailability{
			Mode:                  adapterproto.HistoricalModeSequenceRanges,
			SequenceRanges:        []adapterproto.HistoricalSequenceRange{{Start: 0, End: 0}},
			HistoricalManifestURL: "https://fixture.invalid/history.m3u8",
		},
	}
	recording, err := manager.StartResolvedWithIDOwned(context.Background(), owner, handoverRecordingID, "fixture", media, nil, "handover recovery fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.seen:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic historical recovery did not start")
	}

	type pauseResult struct {
		snapshot HandoverSnapshot
		err      error
	}
	paused := make(chan pauseResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		snapshot, pauseErr := manager.PauseForHandover(ctx, recording.ID, owner)
		paused <- pauseResult{snapshot: snapshot, err: pauseErr}
	}()
	select {
	case result := <-paused:
		t.Fatalf("handover returned before historical request drained: err=%v", result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.resume)
	var result pauseResult
	select {
	case result = <-paused:
	case <-time.After(3 * time.Second):
		t.Fatal("handover did not finish after historical recovery drained")
	}
	if result.err != nil {
		t.Fatalf("PauseForHandover: %v", result.err)
	}
	current, err := manager.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.SegmentCount() != 2 || segmentAtSequence(t, current.Tracks["main"].Segments, 0).TimelineOrdinal != 1 || segmentAtSequence(t, current.Tracks["main"].Segments, 1).TimelineOrdinal != 2 {
		t.Fatalf("recovery was not drained before snapshot: %#v", current.Tracks["main"].Segments)
	}
	rootAtPause := recordingRootBytes(t, store, recording.ID)
	time.Sleep(80 * time.Millisecond)
	if afterPause := recordingRootBytes(t, store, recording.ID); !bytes.Equal(rootAtPause, afterPause) {
		t.Fatal("canonical archive changed after handover snapshot")
	}
	if err := manager.ResumeHandover(recording.ID, owner, result.snapshot); err != nil {
		t.Fatal(err)
	}
}

func newHandoverStores(t *testing.T) (*storage.Store, *recordingowner.Store, string) {
	t.Helper()
	dataDir := t.TempDir()
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	owners, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	return store, owners, dataDir
}

func claimHandoverOwner(t *testing.T, owners *recordingowner.Store, generation, worker string) OwnershipToken {
	t.Helper()
	owner, err := owners.Claim(handoverRecordingID, generation, worker)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func startHandoverRecording(t *testing.T, manager *Manager, owner OwnershipToken) *domain.Recording {
	t.Helper()
	recording, err := manager.StartResolvedWithIDOwned(context.Background(), owner, handoverRecordingID, "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.invalid/live.m3u8"}, nil, "handover fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	return recording
}

func startHandoverRecordingAtURL(t *testing.T, manager *Manager, owner OwnershipToken, manifestURL string) *domain.Recording {
	t.Helper()
	recording, err := manager.StartResolvedWithIDOwned(context.Background(), owner, handoverRecordingID, "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: manifestURL}, nil, "handover fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	return recording
}

func waitHandover(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func recordingRootBytes(t *testing.T, store *storage.Store, id string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(store.Root(), "recordings", id, "recording.json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func setHandoverFixtureMax(fixture *handoverFixture, max int) {
	fixture.mu.Lock()
	fixture.max = max
	fixture.mu.Unlock()
}

func setHandoverFixtureEndList(fixture *handoverFixture, endList bool) {
	fixture.mu.Lock()
	fixture.endList = endList
	fixture.mu.Unlock()
}

func handoverSchedulerAvailable(manager *Manager, id string) bool {
	e, ok := manager.entry(id)
	if !ok {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.scheduler != nil
}

func TestHandoverPauseDrainsAcceptedStorageTaskAndSnapshotsLatestMedia(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "before"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	writeStarted, releaseWrite := make(chan struct{}), make(chan struct{})
	var once sync.Once
	manager.storageWriteHook = func() {
		once.Do(func() { close(writeStarted) })
		<-releaseWrite
	}
	recording := startHandoverRecording(t, manager, owner)
	select {
	case <-writeStarted:
	case <-time.After(3 * time.Second):
		close(releaseWrite)
		t.Fatal("accepted segment write did not reach storage")
	}
	e, _ := manager.entry(recording.ID)
	e.mu.Lock()
	e.media.Headers = map[string]string{"X-Fixture-Context": "latest"}
	e.mu.Unlock()
	paused := make(chan HandoverSnapshot, 1)
	pauseErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		snapshot, err := manager.PauseForHandover(ctx, recording.ID, owner)
		paused <- snapshot
		pauseErr <- err
	}()
	select {
	case <-paused:
		t.Fatal("pause returned before the accepted storage task drained")
	case <-time.After(40 * time.Millisecond):
	}
	if current, err := manager.Get(recording.ID); err != nil || current.State != domain.StateRecording || current.SegmentCount() != 0 || len(current.Gaps) != 0 {
		t.Fatalf("canonical root changed before blocked write completed: recording=%#v err=%v", current, err)
	}
	close(releaseWrite)
	snapshot := <-paused
	if err := <-pauseErr; err != nil {
		t.Fatal(err)
	}
	if snapshot.Owner != owner || snapshot.RecordingID != recording.ID || snapshot.Media.Headers["X-Fixture-Context"] != "latest" {
		t.Fatalf("handover snapshot lost the current owner/media context: %#v", snapshot)
	}
	current, err := manager.Get(recording.ID)
	if err != nil || current.State != domain.StateRecording || current.SegmentCount() != 1 || len(current.Gaps) != 0 {
		t.Fatalf("pause did not drain accepted canonical write cleanly: recording=%#v err=%v", current, err)
	}
}

func TestHandoverWaitsForBlockedSegmentBodyThenTargetContinues(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := newBlockedSegmentHandoverHTTPFixture(1)
	var releaseSegmentOnce sync.Once
	releaseSegment := func() { releaseSegmentOnce.Do(func() { close(fixture.releaseSegmentOne) }) }
	server := httptest.NewServer(fixture)
	t.Cleanup(server.Close)

	resolver := &handoverMetadataResolver{fixture: &handoverFixture{metadata: "title"}}
	source, err := NewManagerWithMode(store, server.Client(), resolver, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.ConfigureCanonicalCommitFence(owners); err != nil {
		t.Fatal(err)
	}
	source.metadataPollInterval = 8 * time.Millisecond
	ownerTransferred, sourceDetached := false, false
	var oldOwner OwnershipToken
	t.Cleanup(func() {
		if ownerTransferred && !sourceDetached {
			if err := source.CompleteHandover(handoverRecordingID, oldOwner); err != nil && !errors.Is(err, storage.ErrNotFound) {
				t.Errorf("detach source manager during cleanup: %v", err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := source.Close(ctx); err != nil {
			t.Errorf("close source manager: %v", err)
		}
	})
	t.Cleanup(releaseSegment)

	oldOwner = claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording, err := source.StartResolvedWithIDOwned(context.Background(), oldOwner, handoverRecordingID, "fixture", adapterproto.MediaSource{
		Type: "hls", ManifestURL: server.URL + "/live.m3u8",
	}, nil, "handover fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-fixture.segmentOneStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("old Engine did not start segment 1 response")
	}
	drainWaiting := observeHandoverDrainWait(t, source, recording.ID)
	if current, err := store.LoadRecordingReadOnly(recording.ID); err != nil || current.SegmentCount() != 0 || len(current.Gaps) != 0 {
		t.Fatalf("partial segment body changed canonical archive: recording=%#v err=%v", current, err)
	}

	type pauseResult struct {
		snapshot HandoverSnapshot
		err      error
	}
	paused := make(chan pauseResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		snapshot, pauseErr := source.PauseForHandover(ctx, recording.ID, oldOwner)
		paused <- pauseResult{snapshot: snapshot, err: pauseErr}
	}()
	e, _ := source.entry(recording.ID)
	waitHandoverState(t, e, handoverDraining)
	select {
	case <-drainWaiting:
	case <-time.After(3 * time.Second):
		t.Fatal("handover did not enter scheduler drain with the accepted segment pending")
	}
	select {
	case result := <-paused:
		t.Fatalf("handover completed while segment HTTP body remained blocked: err=%v", result.err)
	default:
	}
	assertHandoverStillDraining(t, e)
	if current, err := store.LoadRecordingReadOnly(recording.ID); err != nil || current.SegmentCount() != 0 || len(current.Gaps) != 0 {
		t.Fatalf("archive changed before blocked body completed: recording=%#v err=%v", current, err)
	}

	releaseSegment()
	select {
	case <-fixture.segmentOneFinished:
	case <-time.After(3 * time.Second):
		t.Fatal("segment 1 HTTP body did not finish after release")
	}
	var sourceSnapshot HandoverSnapshot
	select {
	case result := <-paused:
		if result.err != nil {
			t.Fatalf("PauseForHandover: %v", result.err)
		}
		sourceSnapshot = result.snapshot
	case <-time.After(3 * time.Second):
		t.Fatal("handover did not finish after segment body and commit drained")
	}
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.State != domain.StateRecording || current.SegmentCount() != 1 || len(current.Gaps) != 0 {
		t.Fatalf("handover snapshot preceded complete segment commit: recording=%#v err=%v", current, err)
	}
	oldLiveView, err := source.LivePlaybackSnapshot(context.Background(), recording.ID)
	if err != nil || len(oldLiveView.Segments) != 1 || oldLiveView.Segments[0].LivePresentationOrdinal == 0 {
		t.Fatalf("old Engine live identity unavailable at segment boundary: view=%#v err=%v", oldLiveView, err)
	}
	oldLiveIdentity := map[string]uint64{oldLiveView.Segments[0].ID: oldLiveView.Segments[0].LivePresentationOrdinal}

	fixture.setMax(3)
	rootBeforeTarget := recordingRootBytes(t, store, recording.ID)
	target, err := NewManagerWithMode(store, server.Client(), resolver, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ConfigureCanonicalCommitFence(owners); err != nil {
		t.Fatal(err)
	}
	target.metadataPollInterval = 8 * time.Millisecond
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := target.Close(ctx); err != nil {
			t.Errorf("close target manager: %v", err)
		}
	})
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), sourceSnapshot, identity); err != nil {
		t.Fatalf("prepare target: %v", err)
	}
	if !bytes.Equal(rootBeforeTarget, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("target preparation rewrote source canonical root")
	}
	newOwner, err := owners.Transfer(oldOwner, handoverGenB, handoverWorkerB)
	if err != nil {
		t.Fatal(err)
	}
	ownerTransferred = true
	if err := target.ActivatePreparedHandover(newOwner); err != nil {
		t.Fatalf("activate target: %v", err)
	}
	if err := source.CompleteHandover(recording.ID, oldOwner); err != nil {
		t.Fatalf("complete source handover: %v", err)
	}
	sourceDetached = true
	waitHandover(t, func() bool {
		current, err := target.Get(recording.ID)
		return err == nil && current.SegmentCount() == 3
	}, "target captures after transferred segment")
	newLiveView, err := target.LivePlaybackSnapshot(context.Background(), recording.ID)
	if err != nil {
		t.Fatalf("target live playback snapshot: %v", err)
	}
	for _, segment := range newLiveView.Segments {
		if previous, existed := oldLiveIdentity[segment.ID]; existed && segment.LivePresentationOrdinal != previous {
			t.Fatalf("handover renumbered existing live URI %s: %d -> %d", segment.ID, previous, segment.LivePresentationOrdinal)
		}
	}
	if len(newLiveView.Segments) != 3 || newLiveView.Segments[2].LivePresentationOrdinal != oldLiveView.Segments[0].LivePresentationOrdinal+2 {
		t.Fatalf("new Engine did not continue presentation sequence: %#v", newLiveView.Segments)
	}
	current, err = store.LoadRecordingReadOnly(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertHandoverFixtureArchive(t, store, current, 3)
}

func TestHandoverWaitsForCanonicalCommitBarrier(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "title"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	oldOwner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	writeStarted, releaseWrite := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWrite) }) }
	t.Cleanup(release)
	manager.storageWriteHook = func() {
		once.Do(func() { close(writeStarted) })
		<-releaseWrite
	}
	recording := startHandoverRecording(t, manager, oldOwner)
	select {
	case <-writeStarted:
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("accepted media commit did not reach canonical barrier")
	}
	drainWaiting := observeHandoverDrainWait(t, manager, recording.ID)

	type pauseResult struct {
		snapshot HandoverSnapshot
		err      error
	}
	paused := make(chan pauseResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		snapshot, err := manager.PauseForHandover(ctx, recording.ID, oldOwner)
		paused <- pauseResult{snapshot: snapshot, err: err}
	}()
	e, _ := manager.entry(recording.ID)
	waitHandoverState(t, e, handoverDraining)
	select {
	case <-drainWaiting:
	case <-time.After(3 * time.Second):
		t.Fatal("handover did not enter scheduler drain with the canonical commit pending")
	}
	select {
	case result := <-paused:
		t.Fatalf("handover completed while canonical commit was blocked: err=%v", result.err)
	default:
	}
	assertHandoverStillDraining(t, e)
	if current, err := store.LoadRecordingReadOnly(recording.ID); err != nil || current.SegmentCount() != 0 || len(current.Gaps) != 0 {
		t.Fatalf("canonical root changed before commit barrier release: recording=%#v err=%v", current, err)
	}
	release()
	var snapshot HandoverSnapshot
	select {
	case result := <-paused:
		if result.err != nil {
			t.Fatalf("PauseForHandover: %v", result.err)
		}
		snapshot = result.snapshot
	case <-time.After(3 * time.Second):
		t.Fatal("handover did not finish after canonical commit")
	}
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.State != domain.StateRecording || current.SegmentCount() != 1 || len(current.Gaps) != 0 {
		t.Fatalf("handover completed before canonical commit became visible: recording=%#v err=%v", current, err)
	}
	if snapshot.RecordingID != recording.ID || snapshot.Owner != oldOwner {
		t.Fatalf("handover snapshot identity/owner = %#v", snapshot)
	}
	fixture.mu.Lock()
	fixture.max = 3
	fixture.mu.Unlock()
	target, err := NewManagerWithMode(store, &http.Client{Transport: fixture}, &handoverMetadataResolver{fixture: fixture}, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ConfigureCanonicalCommitFence(owners); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if closeErr := target.Close(ctx); closeErr != nil {
			t.Errorf("close target manager: %v", closeErr)
		}
	})
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); err != nil {
		t.Fatalf("prepare target after canonical commit: %v", err)
	}
	newOwner, err := owners.Transfer(oldOwner, handoverGenB, handoverWorkerB)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ActivatePreparedHandover(newOwner); err != nil {
		t.Fatalf("activate target after canonical commit: %v", err)
	}
	if err := manager.CompleteHandover(recording.ID, oldOwner); err != nil {
		t.Fatalf("complete source handover after canonical commit: %v", err)
	}
	waitHandover(t, func() bool {
		current, getErr := target.Get(recording.ID)
		return getErr == nil && current.SegmentCount() == 3
	}, "target continuation after canonical commit")
	continued, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertHandoverFixtureArchive(t, store, continued, 3)
}

type blockedSegmentHandoverHTTPFixture struct {
	mu                 sync.Mutex
	max                int
	segmentOneStarted  chan struct{}
	releaseSegmentOne  chan struct{}
	segmentOneFinished chan struct{}
	segmentOneOnce     sync.Once
}

func newBlockedSegmentHandoverHTTPFixture(max int) *blockedSegmentHandoverHTTPFixture {
	return &blockedSegmentHandoverHTTPFixture{
		max: max, segmentOneStarted: make(chan struct{}),
		releaseSegmentOne: make(chan struct{}), segmentOneFinished: make(chan struct{}),
	}
}

func (f *blockedSegmentHandoverHTTPFixture) setMax(max int) {
	f.mu.Lock()
	f.max = max
	f.mu.Unlock()
}

func (f *blockedSegmentHandoverHTTPFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/live.m3u8":
		f.mu.Lock()
		max := f.max
		f.mu.Unlock()
		manifest := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n"
		for sequence := 1; sequence <= max; sequence++ {
			manifest += fmt.Sprintf("#EXTINF:1.0,\nsegment-%06d.ts\n", sequence)
		}
		_, _ = io.WriteString(w, manifest)
	case strings.HasPrefix(r.URL.Path, "/segment-"):
		var sequence int
		if _, err := fmt.Sscanf(r.URL.Path, "/segment-%06d.ts", &sequence); err != nil {
			http.Error(w, "invalid segment path", http.StatusBadRequest)
			return
		}
		payload := []byte(fmt.Sprintf("source-segment-%06d", sequence))
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if sequence == 1 {
			f.segmentOneOnce.Do(func() {
				_, _ = w.Write(payload[:len(payload)/2])
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				close(f.segmentOneStarted)
				select {
				case <-f.releaseSegmentOne:
				case <-r.Context().Done():
				}
				_, _ = w.Write(payload[len(payload)/2:])
				close(f.segmentOneFinished)
			})
			return
		}
		_, _ = w.Write(payload)
	default:
		http.NotFound(w, r)
	}
}

func waitHandoverState(t *testing.T, e *entry, wanted handoverOperationState) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		e.mu.Lock()
		state := handoverOperationState(0)
		if e.handover != nil {
			state = e.handover.state
		}
		e.mu.Unlock()
		if state == wanted {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for handover state %v; got %v", wanted, state)
		default:
			runtime.Gosched()
		}
	}
}

func observeHandoverDrainWait(t *testing.T, manager *Manager, recordingID string) <-chan struct{} {
	t.Helper()
	e, ok := manager.entry(recordingID)
	if !ok {
		t.Fatalf("recording entry %s is missing", recordingID)
	}
	e.mu.Lock()
	scheduler := e.scheduler
	e.mu.Unlock()
	if scheduler == nil {
		t.Fatal("recording scheduler is missing")
	}
	waiting := make(chan struct{}, 1)
	scheduler.mu.Lock()
	scheduler.drainWaitHook = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	scheduler.mu.Unlock()
	return waiting
}

func assertHandoverStillDraining(t *testing.T, e *entry) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handover == nil || e.handover.state != handoverDraining {
		t.Fatalf("handover advanced before accepted work drained: %#v", e.handover)
	}
}

func assertHandoverFixtureArchive(t *testing.T, store *storage.Store, recording *domain.Recording, count int) {
	t.Helper()
	if recording != nil && recording.FormatVersion == storage.ShardedArchiveFormatVersion {
		track := recording.Tracks["main"]
		if track == nil {
			t.Fatal("main track is missing")
		}
		track.Segments = nil
		if err := store.IterateShardedMedia(context.Background(), recording.ID, "main", func(record storage.V2MediaRecord) error {
			track.Segments = append(track.Segments, record.Segment)
			return nil
		}); err != nil {
			t.Fatalf("iterate sharded handover media: %v", err)
		}
		var gaps []domain.Gap
		if err := store.IterateShardedGaps(context.Background(), recording.ID, func(gap domain.Gap) error {
			gaps = append(gaps, gap)
			return nil
		}); err != nil {
			t.Fatalf("iterate sharded handover gaps: %v", err)
		}
		recording.Gaps = gaps
	}
	if recording.ID != handoverRecordingID || recording.State != domain.StateRecording || recording.SegmentCount() != count || len(recording.Gaps) != 0 {
		t.Fatalf("recording continuity failed: %#v", recording)
	}
	track := recording.Tracks["main"]
	if track == nil {
		t.Fatal("main track is missing")
	}
	if len(track.Segments) != count {
		t.Fatalf("main track segment count=%d, want %d", len(track.Segments), count)
	}
	seen := make(map[uint64]struct{}, count)
	for index, segment := range track.Segments {
		wantSequence := uint64(index + 1)
		if segment.Sequence != wantSequence || segment.ArchiveOrdinal != wantSequence {
			t.Fatalf("segment %d identity/ordinal=%d/%d, want %d/%d", index, segment.Sequence, segment.ArchiveOrdinal, wantSequence, wantSequence)
		}
		if _, duplicate := seen[segment.Sequence]; duplicate {
			t.Fatalf("duplicate segment sequence %d", segment.Sequence)
		}
		seen[segment.Sequence] = struct{}{}
		payload, err := store.OpenPayloadReader(recording.ID, segment.StoragePath)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(payload)
		_ = payload.Close()
		if readErr != nil || string(data) != fmt.Sprintf("source-segment-%06d", wantSequence) {
			t.Fatalf("segment %d payload=%q err=%v", wantSequence, data, readErr)
		}
	}
}

func TestHandoverPauseTimeoutKeepsSourceRecordingAndAllowsFurtherSegments(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "title"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	writeStarted, releaseWrite := make(chan struct{}), make(chan struct{})
	var once sync.Once
	manager.storageWriteHook = func() {
		once.Do(func() { close(writeStarted) })
		<-releaseWrite
	}
	recording := startHandoverRecording(t, manager, owner)
	select {
	case <-writeStarted:
	case <-time.After(3 * time.Second):
		close(releaseWrite)
		t.Fatal("accepted segment write did not reach storage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := manager.PauseForHandover(ctx, recording.ID, owner); !errors.Is(err, context.DeadlineExceeded) {
		close(releaseWrite)
		t.Fatalf("PauseForHandover error = %v, want deadline exceeded", err)
	}
	close(releaseWrite)
	waitHandover(t, func() bool {
		current, err := manager.Get(recording.ID)
		return err == nil && current.SegmentCount() >= 1
	}, "source commit after pause timeout")
	setHandoverFixtureMax(fixture, 2)
	waitHandover(t, func() bool {
		current, err := manager.Get(recording.ID)
		return err == nil && current.SegmentCount() >= 2
	}, "source acquisition after pause timeout")
	current, err := manager.Get(recording.ID)
	if err != nil || current.State != domain.StateRecording || len(current.Gaps) != 0 {
		t.Fatalf("timed out pause disturbed source recording: recording=%#v err=%v", current, err)
	}
}

func TestHandoverAbortCanResumeSameSourceOwner(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "title"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecording(t, manager, owner)
	waitHandover(t, func() bool {
		current, err := manager.Get(recording.ID)
		return err == nil && current.SegmentCount() == 1
	}, "initial source segment")
	snapshot, err := manager.PauseForHandover(context.Background(), recording.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ResumeHandover(recording.ID, owner, snapshot); err != nil {
		t.Fatal(err)
	}
	setHandoverFixtureMax(fixture, 2)
	waitHandover(t, func() bool {
		current, err := manager.Get(recording.ID)
		return err == nil && current.SegmentCount() == 2
	}, "source resumed under unchanged owner")
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.State != domain.StateRecording || current.SegmentCount() != 2 || len(current.Gaps) != 0 {
		t.Fatalf("resumed source changed recording continuity: recording=%#v err=%v", current, err)
	}
}

func TestHandoverPausedRecordingRejectsConcurrentStopUntilOwnerResumes(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "title"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecording(t, manager, owner)
	waitHandover(t, func() bool {
		current, err := manager.Get(recording.ID)
		return err == nil && current.SegmentCount() == 1
	}, "initial source segment")
	snapshot, err := manager.PauseForHandover(context.Background(), recording.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.StopContext(context.Background(), recording.ID); !errors.Is(err, ErrHandoverConflict) {
		t.Fatalf("StopContext() while ownership is paused = %v, want handover conflict", err)
	}
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.State != domain.StateRecording || current.SegmentCount() != 1 || len(current.Gaps) != 0 {
		t.Fatalf("rejected concurrent stop changed canonical recording: recording=%#v err=%v", current, err)
	}
	if err := manager.ResumeHandover(recording.ID, owner, snapshot); err != nil {
		t.Fatal(err)
	}
	stopped, err := manager.StopContext(context.Background(), recording.ID)
	if err != nil || stopped.State != domain.StateStopped {
		t.Fatalf("StopContext() after handover resumed = recording=%#v err=%v, want stopped", stopped, err)
	}
}

func TestHandoverCompleteDetachesParkedSourceWithoutTerminalMutation(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 1, metadata: "title"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecording(t, manager, owner)
	waitHandover(t, func() bool {
		current, err := manager.Get(recording.ID)
		return err == nil && current.SegmentCount() == 1
	}, "initial source segment")
	snapshot, err := manager.PauseForHandover(context.Background(), recording.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = owners.Transfer(owner, handoverGenB, handoverWorkerB); err != nil {
		t.Fatal(err)
	}
	if err = manager.CompleteHandover(recording.ID, owner); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.entry(recording.ID); ok {
		t.Fatal("completed source remained in manager inventory")
	}
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.State != domain.StateRecording || current.StoppedAt != nil || len(current.Gaps) != 0 || current.SegmentCount() != 1 {
		t.Fatalf("source completion mutated canonical state: snapshot=%#v recording=%#v err=%v", snapshot, current, err)
	}
	if ownerNow, err := owners.Current(recording.ID); err != nil || ownerNow.EngineGeneration != handoverGenB {
		t.Fatalf("source completion released or changed Host owner: owner=%#v err=%v", ownerNow, err)
	}
}

func TestHandoverPrepareIsReadOnlyAndRejectsInactiveArchive(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 0, metadata: "title"}
	source := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecording(t, source, owner)
	waitHandover(t, func() bool { return handoverSchedulerAvailable(source, recording.ID) }, "source scheduler")
	snapshot, err := source.PauseForHandover(context.Background(), recording.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	setHandoverFixtureMax(fixture, 1)
	target := newHandoverManager(t, store, owners, fixture, true)
	before := recordingRootBytes(t, store, recording.ID)
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err = target.PrepareHandoverTarget(context.Background(), snapshot, identity); err != nil {
		t.Fatal(err)
	}
	// A source may advance or refresh its MediaSource between the first
	// read-only target preparation and the drained boundary. A later handover
	// attempt must be able to replace an inactive candidate prepared from an
	// older snapshot rather than becoming stuck behind it.
	refreshedSnapshot := cloneHandoverSnapshot(snapshot)
	refreshedSnapshot.Media.ManifestURL = "https://fixture.invalid/refreshed.m3u8"
	if err = target.PrepareHandoverTarget(context.Background(), refreshedSnapshot, identity); err != nil {
		t.Fatalf("replace inactive target candidate with refreshed source snapshot: %v", err)
	}
	target.mu.RLock()
	target.mu.RLock()
	prepared := target.prepared[recording.ID]
	target.mu.RUnlock()
	target.mu.RUnlock()
	if prepared.snapshot.Media.ManifestURL != refreshedSnapshot.Media.ManifestURL {
		t.Fatalf("target retained stale candidate media source: got %q want %q", prepared.snapshot.Media.ManifestURL, refreshedSnapshot.Media.ManifestURL)
	}
	failedSnapshot := cloneHandoverSnapshot(refreshedSnapshot)
	failedSnapshot.Media.ManifestURL = "https://fixture.invalid/fail.m3u8"
	fixture.mu.Lock()
	fixture.failManifestPath = "/fail.m3u8"
	fixture.mu.Unlock()
	if err = target.PrepareHandoverTarget(context.Background(), failedSnapshot, identity); !errors.Is(err, ErrHandoverUnavailable) {
		t.Fatalf("failed re-probe = %v, want unavailable", err)
	}
	target.mu.RLock()
	preparedAfterFailure, hasPreparedAfterFailure := target.prepared[recording.ID]
	target.mu.RUnlock()
	if hasPreparedAfterFailure || preparedAfterFailure.continuation != nil {
		t.Fatal("failed replacement probe retained stale staged payloads")
	}
	if err = target.PrepareHandoverTarget(context.Background(), refreshedSnapshot, identity); err != nil {
		t.Fatalf("prepare again after failed replacement: %v", err)
	}
	after := recordingRootBytes(t, store, recording.ID)
	if !bytes.Equal(before, after) {
		t.Fatal("preparing a handover target rewrote recording.json")
	}
	if err = target.ActivatePreparedHandover(OwnershipToken{RecordingID: recording.ID, EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB, Epoch: 2}); !errors.Is(err, recordingowner.ErrStaleOwner) {
		t.Fatalf("target activation before owner transfer = %v, want stale owner", err)
	}
	if err = target.DiscardPreparedHandover(recording.ID); err != nil {
		t.Fatal(err)
	}
	if err = source.ResumeHandover(recording.ID, owner, snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err = source.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
	stopped, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	stopped.State = domain.StateStopped
	if err = store.SaveRecording(stopped); err != nil {
		t.Fatal(err)
	}
	if err = target.PrepareHandoverTarget(context.Background(), snapshot, identity); !errors.Is(err, ErrHandoverUnavailable) {
		t.Fatalf("preparing inactive archive = %v, want unavailable", err)
	}
}

func TestHandoverPrepareRequiresDrainForEmptyLivePlaylist(t *testing.T) {
	fixture := &handoverFixture{max: 0, metadata: "title"}
	store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", -1)
	defer func() {
		resumeAndStopHandoverSource(t, source, owner, snapshot)
	}()
	target := newHandoverManager(t, store, owners, fixture, true)
	before := recordingRootBytes(t, store, recording.ID)
	segmentRequests := fixture.segmentRequests.Load()
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); !errors.Is(err, ErrHandoverSourceBoundaryRequired) {
		t.Fatalf("PrepareHandoverTarget()=%v, want source-boundary retry", err)
	}
	if !bytes.Equal(before, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("failed continuation probe mutated the canonical root")
	}
	if got := fixture.segmentRequests.Load(); got != segmentRequests {
		t.Fatalf("continuation probe fetched media payloads: before=%d after=%d", segmentRequests, got)
	}
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.SegmentCount() != 0 {
		t.Fatalf("probe changed canonical segment count: recording=%#v err=%v", current, err)
	}
}

func TestHandoverPrepareFetchesFirstUncommittedMediaWithoutMutation(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title"}
	store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
	defer func() {
		resumeAndStopHandoverSource(t, source, owner, snapshot)
	}()
	target := newHandoverManager(t, store, owners, fixture, true)
	setHandoverFixtureMax(fixture, 2)
	before := recordingRootBytes(t, store, recording.ID)
	segmentRequests := fixture.segmentRequests.Load()
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); err != nil {
		t.Fatalf("matching live tail plus next media preflight: %v", err)
	}
	if !bytes.Equal(before, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("preparing from an already-recorded live tail rewrote recording.json")
	}
	if got := fixture.segmentRequests.Load() - segmentRequests; got != 1 {
		t.Fatalf("preflight media fetches=%d, want exactly next uncommitted segment", got)
	}
	if got := target.ingest.Snapshot().BufferUsedBytes; got == 0 {
		t.Fatal("preflight did not retain bounded staged media bytes")
	}
	current, err := store.LoadRecordingReadOnly(recording.ID)
	if err != nil || current.SegmentCount() != 1 {
		t.Fatalf("continuation probe changed canonical segment count: recording=%#v err=%v", current, err)
	}
	if err = target.DiscardPreparedHandover(recording.ID); err != nil {
		t.Fatal(err)
	}
	if got := target.ingest.Snapshot().BufferUsedBytes; got != 0 {
		t.Fatalf("discard left ingest reservation: %d bytes", got)
	}
}

func TestHandoverPrepareRejectsManifestTransportFailure(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title", failManifestPath: "/failed.m3u8"}
	store, owners, source, owner, _, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
	sourceSnapshot := cloneHandoverSnapshot(snapshot)
	defer func() {
		resumeAndStopHandoverSource(t, source, owner, sourceSnapshot)
	}()
	target := newHandoverManager(t, store, owners, fixture, true)
	snapshot.Media.ManifestURL = "https://fixture.invalid/failed.m3u8"
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); !errors.Is(err, ErrHandoverUnavailable) {
		t.Fatalf("PrepareHandoverTarget()=%v, want unavailable", err)
	}
}

func TestHandoverLiveTailRequiresDrainAndStagesNextCandidate(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title"}
	store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
	defer func() { resumeAndStopHandoverSource(t, source, owner, snapshot) }()
	target := newHandoverManager(t, store, owners, fixture, true)
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	beforeRoot := recordingRootBytes(t, store, recording.ID)
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); !errors.Is(err, ErrHandoverSourceBoundaryRequired) {
		t.Fatalf("live tail preflight = %v, want source-boundary retry", err)
	}
	if target.ingest.Snapshot().BufferUsedBytes != 0 {
		t.Fatal("live-tail retry reserved candidate memory")
	}
	if !bytes.Equal(beforeRoot, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("live-tail retry mutated canonical root")
	}

	observed := make(chan int, 8)
	fixture.mu.Lock()
	fixture.manifestObserved = observed
	fixture.mu.Unlock()
	prepareResult := make(chan error, 1)
	go func() {
		prepareResult <- target.PrepareHandoverTargetAfterSourceDrain(context.Background(), snapshot, identity)
	}()
	select {
	case latest := <-observed:
		if latest != 1 {
			t.Fatalf("first drained probe saw %d segments, want committed live tail 1", latest)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drained target did not fetch an initial fresh manifest")
	}
	// The first manifest was observed at the committed tail. Publish one new
	// source object while the source Engine is paused; the target must poll a
	// fresh manifest and stage that exact next object before returning ready.
	setHandoverFixtureMax(fixture, 2)
	select {
	case err := <-prepareResult:
		if err != nil {
			t.Fatalf("drained target did not stage the newly published continuation: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("drained target did not observe the next continuation within the bounded probe window")
	}
	prepared := target.prepared[recording.ID]
	if prepared.continuation == nil || prepared.continuation.source.Sequence != 2 || prepared.continuation.mediaPayload == nil {
		t.Fatalf("drained target staged the wrong continuation: %#v", prepared.continuation)
	}
	if got := target.ingest.Snapshot().BufferUsedBytes; got == 0 {
		t.Fatal("drained target did not retain the media payload in bounded RAM staging")
	}
	if !bytes.Equal(beforeRoot, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("drained target preflight mutated canonical root before owner transfer")
	}
	if current, err := owners.Current(recording.ID); err != nil || current != owner {
		t.Fatalf("drained target preflight moved ownership before transfer: owner=%+v err=%v", current, err)
	}
	if err := target.DiscardPreparedHandover(recording.ID); err != nil {
		t.Fatal(err)
	}
	if got := target.ingest.Snapshot().BufferUsedBytes; got != 0 {
		t.Fatalf("discard left %d bytes reserved", got)
	}
}

func TestHandoverEndListAtCommittedTailIsNotSourceBoundaryRetry(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title"}
	store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
	defer func() { resumeAndStopHandoverSource(t, source, owner, snapshot) }()
	setHandoverFixtureEndList(fixture, true)
	target := newHandoverManager(t, store, owners, fixture, true)
	err := target.PrepareHandoverTarget(context.Background(), snapshot, HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB})
	if !errors.Is(err, ErrHandoverUnavailable) || errors.Is(err, ErrHandoverSourceBoundaryRequired) {
		t.Fatalf("ENDLIST without continuation = %v, want safe unavailable without drain retry", err)
	}
	if target.ingest.Snapshot().BufferUsedBytes != 0 {
		t.Fatalf("ENDLIST failure leaked %d bytes", target.ingest.Snapshot().BufferUsedBytes)
	}
	if _, err := owners.Current(recording.ID); err != nil {
		t.Fatalf("ENDLIST probe lost ownership state: %v", err)
	}
}

func TestHandoverPreflightUsesDeferredRefreshAndReusesFetchedCandidate(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title"}
	store, owners, source, oldOwner, recording, sourceSnapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
	setHandoverFixtureMax(fixture, 2)
	targetSnapshot := cloneHandoverSnapshot(sourceSnapshot)
	expired := time.Now().Add(-time.Minute).UTC()
	targetSnapshot.Media.RefreshPolicy = &adapterproto.RefreshPolicy{ExpiresAt: &expired, RefreshBeforeSeconds: 5}
	resolver := &handoverRefreshResolver{
		fixture: fixture,
		next:    adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.invalid/refreshed.m3u8"},
	}
	target := newHandoverManagerWithResolver(t, store, owners, fixture, func(context.Context, string) error { return nil }, resolver)
	beforeRoot := recordingRootBytes(t, store, recording.ID)
	segmentRequests := fixture.segmentRequests.Load()
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), targetSnapshot, identity); !errors.Is(err, ErrHandoverSourceRefreshRequired) {
		t.Fatalf("pre-drain expired source prepare = %v, want refresh-required retry", err)
	}
	if resolver.refreshes.Load() != 0 || fixture.segmentRequests.Load() != segmentRequests || target.ingest.Snapshot().BufferUsedBytes != 0 {
		t.Fatalf("pre-drain retry performed target work: refreshes=%d segment_requests=%d reserved=%d", resolver.refreshes.Load(), fixture.segmentRequests.Load()-segmentRequests, target.ingest.Snapshot().BufferUsedBytes)
	}
	if !bytes.Equal(beforeRoot, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("pre-drain refresh retry mutated canonical root")
	}
	if err := target.PrepareHandoverTargetAfterSourceDrain(context.Background(), targetSnapshot, identity); err != nil {
		t.Fatalf("prepare refreshed continuation: %v", err)
	}
	if resolver.refreshes.Load() != 1 || resolver.committed.Load() {
		t.Fatalf("refresh count/commit before ownership = %d/%v, want 1/false", resolver.refreshes.Load(), resolver.committed.Load())
	}
	if got := fixture.segmentRequests.Load() - segmentRequests; got != 1 {
		t.Fatalf("preflight candidate fetch count=%d, want 1", got)
	}
	if got := target.ingest.Snapshot().BufferUsedBytes; got == 0 {
		t.Fatal("preflight did not hold candidate in bounded ingest memory")
	}
	prepared := target.prepared[recording.ID]
	if prepared.media.ManifestURL != resolver.next.ManifestURL || prepared.continuation == nil || prepared.continuation.source.Sequence != 2 {
		t.Fatalf("prepared refreshed candidate = %#v", prepared)
	}
	if !bytes.Equal(beforeRoot, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("refresh preflight mutated canonical root before owner transfer")
	}
	newOwner, err := owners.Transfer(oldOwner, handoverGenB, handoverWorkerB)
	if err != nil {
		t.Fatal(err)
	}
	if err = target.ActivatePreparedHandover(newOwner); err != nil {
		t.Fatalf("activate prepared target: %v", err)
	}
	if !resolver.committed.Load() {
		t.Fatal("deferred adapter state mutation was not applied after ownership validation")
	}
	waitHandover(t, func() bool {
		current, getErr := target.Get(recording.ID)
		return getErr == nil && current.SegmentCount() == 2
	}, "staged media commit")
	if got := fixture.segmentRequests.Load() - segmentRequests; got != 1 {
		t.Fatalf("candidate was fetched again after activation: requests=%d", got)
	}
	if err = source.CompleteHandover(recording.ID, oldOwner); err != nil {
		t.Fatal(err)
	}
	if _, err = target.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
}

func TestHandoverPreflightRefreshAndCandidateFailuresReleaseReservations(t *testing.T) {
	t.Run("refresh failure", func(t *testing.T) {
		fixture := &handoverFixture{max: 1, metadata: "title"}
		store, owners, source, owner, recording, sourceSnapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
		defer func() { resumeAndStopHandoverSource(t, source, owner, sourceSnapshot) }()
		targetSnapshot := cloneHandoverSnapshot(sourceSnapshot)
		expired := time.Now().Add(-time.Minute).UTC()
		targetSnapshot.Media.RefreshPolicy = &adapterproto.RefreshPolicy{ExpiresAt: &expired, RefreshBeforeSeconds: 1}
		resolver := &handoverRefreshResolver{fixture: fixture, fail: true}
		target := newHandoverManagerWithResolver(t, store, owners, fixture, func(context.Context, string) error { return nil }, resolver)
		before := recordingRootBytes(t, store, recording.ID)
		identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
		if err := target.PrepareHandoverTarget(context.Background(), targetSnapshot, identity); !errors.Is(err, ErrHandoverSourceRefreshRequired) {
			t.Fatalf("pre-drain refresh failure probe = %v, want refresh-required retry", err)
		}
		if resolver.refreshes.Load() != 0 {
			t.Fatalf("pre-drain probe called adapter refresh %d times", resolver.refreshes.Load())
		}
		if err := target.PrepareHandoverTargetAfterSourceDrain(context.Background(), targetSnapshot, identity); !errors.Is(err, ErrHandoverUnavailable) {
			t.Fatalf("drained refresh failure = %v, want unavailable", err)
		}
		if resolver.refreshes.Load() != 1 || target.ingest.Snapshot().BufferUsedBytes != 0 {
			t.Fatalf("refresh failure side effects: calls=%d reserved=%d", resolver.refreshes.Load(), target.ingest.Snapshot().BufferUsedBytes)
		}
		if !bytes.Equal(before, recordingRootBytes(t, store, recording.ID)) {
			t.Fatal("failed refresh changed canonical root")
		}
		if current, err := owners.Current(recording.ID); err != nil || current != owner {
			t.Fatalf("failed refresh changed owner: %#v err=%v", current, err)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*handoverFixture)
	}{
		{name: "candidate transport failure", mutate: func(f *handoverFixture) { f.mu.Lock(); f.failSegmentPath = "/segment-000002.ts"; f.mu.Unlock() }},
		{name: "invalid candidate encoding", mutate: func(f *handoverFixture) { f.mu.Lock(); f.invalidSegment = true; f.mu.Unlock() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &handoverFixture{max: 1, metadata: "title"}
			store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
			defer func() { resumeAndStopHandoverSource(t, source, owner, snapshot) }()
			setHandoverFixtureMax(fixture, 2)
			// Fault injection begins only after source segment 1 is committed.
			test.mutate(fixture)
			target := newHandoverManager(t, store, owners, fixture, true)
			before := recordingRootBytes(t, store, recording.ID)
			if err := target.PrepareHandoverTarget(context.Background(), snapshot, HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}); !errors.Is(err, ErrHandoverUnavailable) {
				t.Fatalf("candidate preflight failure = %v, want unavailable", err)
			}
			if got := target.ingest.Snapshot().BufferUsedBytes; got != 0 {
				t.Fatalf("failed preflight leaked %d bytes of ingest reservation", got)
			}
			if !bytes.Equal(before, recordingRootBytes(t, store, recording.ID)) {
				t.Fatal("failed candidate preflight changed canonical root")
			}
			if current, err := owners.Current(recording.ID); err != nil || current != owner {
				t.Fatalf("failed candidate preflight changed owner: %#v err=%v", current, err)
			}
		})
	}
}

func TestHandoverPreflightRejectsEmptyCandidateObjectsWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name       string
		emptyPath  string
		includeMap bool
	}{
		{name: "media segment", emptyPath: "/segment-000002.ts"},
		{name: "new init object", emptyPath: "/init.mp4", includeMap: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &handoverFixture{max: 1, metadata: "title"}
			store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
			defer func() {
				// The source and preflight target share this fixture. Clear the
				// target-only response and playlist mutations before resuming the
				// source scheduler.
				fixture.mu.Lock()
				fixture.max = 1
				fixture.emptyPayloadPath = ""
				fixture.includeInitMap = false
				fixture.mu.Unlock()
				resumeAndStopHandoverSource(t, source, owner, snapshot)
			}()
			setHandoverFixtureMax(fixture, 2)
			fixture.mu.Lock()
			fixture.emptyPayloadPath = test.emptyPath
			fixture.includeInitMap = test.includeMap
			fixture.mu.Unlock()

			beforeRoot := recordingRootBytes(t, store, recording.ID)
			beforePayloads := snapshotHandoverTrackFiles(t, store, recording.ID)
			target := newHandoverManager(t, store, owners, fixture, true)
			identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
			if err := target.PrepareHandoverTargetAfterSourceDrain(context.Background(), snapshot, identity); !errors.Is(err, ErrHandoverUnavailable) {
				t.Fatalf("empty %s candidate returned %v, want handover unavailable", test.name, err)
			}
			if got := target.ingest.Snapshot().BufferUsedBytes; got != 0 {
				t.Fatalf("empty %s candidate leaked %d bytes of ingest reservation", test.name, got)
			}
			target.mu.RLock()
			prepared, found := target.prepared[recording.ID]
			target.mu.RUnlock()
			if found || prepared.continuation != nil {
				t.Fatalf("empty %s candidate retained prepared target state: found=%t prepared=%#v", test.name, found, prepared)
			}
			if current, err := owners.Current(recording.ID); err != nil || current != owner {
				t.Fatalf("empty %s candidate changed durable source owner: owner=%+v err=%v want=%+v", test.name, current, err, owner)
			}
			if !bytes.Equal(beforeRoot, recordingRootBytes(t, store, recording.ID)) {
				t.Fatalf("empty %s candidate changed canonical recording root", test.name)
			}
			if afterPayloads := snapshotHandoverTrackFiles(t, store, recording.ID); !bytes.Equal(beforePayloads, afterPayloads) {
				t.Fatalf("empty %s candidate changed canonical media payloads or sidecars", test.name)
			}
			current, err := store.LoadRecordingReadOnly(recording.ID)
			if err != nil || current.State != domain.StateRecording || current.SegmentCount() != 1 || len(current.Gaps) != 0 {
				t.Fatalf("empty %s candidate changed recording continuation state: recording=%#v err=%v", test.name, current, err)
			}
		})
	}
}

func snapshotHandoverTrackFiles(t *testing.T, store *storage.Store, recordingID string) []byte {
	t.Helper()
	root := filepath.Join(store.Root(), "recordings", recordingID)
	files := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(filepath.ToSlash(relative), "tracks/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(relative)] = string(data)
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot canonical media files: %v", err)
	}
	data, err := json.Marshal(files)
	if err != nil {
		t.Fatalf("encode canonical media file snapshot: %v", err)
	}
	return data
}

func TestHandoverPrepareValidatesMediaURLs(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title"}
	store, owners, source, owner, _, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 1)
	sourceSnapshot := cloneHandoverSnapshot(snapshot)
	defer func() {
		resumeAndStopHandoverSource(t, source, owner, sourceSnapshot)
	}()
	var validated []string
	validator := func(_ context.Context, raw string) error {
		validated = append(validated, raw)
		if strings.Contains(raw, "/blocked") {
			return errors.New("rejected by fixture validator")
		}
		return nil
	}
	target := newHandoverManagerWithValidator(t, store, owners, fixture, validator)
	snapshot.Media.ManifestURL = "https://fixture.invalid/blocked.m3u8"
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); !errors.Is(err, ErrHandoverUnavailable) {
		t.Fatalf("invalid media URL PrepareHandoverTarget()=%v, want unavailable", err)
	}
	if len(validated) != 1 || validated[0] != snapshot.Media.ManifestURL {
		t.Fatalf("invalid URL validation calls=%q, want only the rejected manifest URL", validated)
	}
}

func TestHandoverPrepareProbesMasterVariantAndStagesCandidate(t *testing.T) {
	fixture := &handoverFixture{max: 1, metadata: "title"}
	store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/master.m3u8", 1)
	defer func() {
		resumeAndStopHandoverSource(t, source, owner, snapshot)
	}()
	setHandoverFixtureMax(fixture, 2)
	target := newHandoverManager(t, store, owners, fixture, true)
	before := recordingRootBytes(t, store, recording.ID)
	requestsBefore := fixture.requests.Load()
	segmentRequests := fixture.segmentRequests.Load()
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); err != nil {
		t.Fatalf("master/variant continuation probe: %v", err)
	}
	if got := fixture.requests.Load() - requestsBefore; got < 2 {
		t.Fatalf("master playlist requests added=%d, want master and selected media playlist", got)
	}
	if got := fixture.segmentRequests.Load() - segmentRequests; got != 1 {
		t.Fatalf("candidate media fetches=%d, want exactly one", got)
	}
	if !bytes.Equal(before, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("master/variant continuation probe mutated the canonical root")
	}
}

func TestHandoverCandidateUsesExistingSourceIdentityEpochRules(t *testing.T) {
	recording := &domain.Recording{Tracks: map[string]*domain.Track{
		"main": {
			SourceEpoch: 2, HasLastObservedSequence: true, LastObservedSequence: 7,
			Segments: []domain.Segment{{SourceEpoch: 2, Sequence: 7, DiscontinuitySequence: 3, SourceURI: "https://fixture.invalid/segment.ts?token=old"}},
		},
	}}
	playlist := hls.MediaPlaylist{EndList: true, Segments: []hls.MediaSegment{{Sequence: 7, DiscontinuitySequence: 3, URI: "https://fixture.invalid/segment.ts?token=new"}}}
	if hasHandoverContinuationCandidate(recording, playlist) {
		t.Fatal("query-only signed URI change was treated as a new source identity")
	}
	playlist.Segments[0].URI = "https://fixture.invalid/replacement.ts?token=new"
	if !hasHandoverContinuationCandidate(recording, playlist) {
		t.Fatal("changed stable URI identity for the same sequence was not treated as a new epoch candidate")
	}
}

func TestHandoverPreflightRequiresUncommittedIdentityMatchingMedia(t *testing.T) {
	recording := &domain.Recording{Tracks: map[string]*domain.Track{
		"main": {
			SourceEpoch: 2, HasLastObservedSequence: true, LastObservedSequence: 7,
			Segments: []domain.Segment{
				{SourceEpoch: 2, Sequence: 6, DiscontinuitySequence: 3, SourceURI: "https://fixture.invalid/segment-6.ts"},
				{SourceEpoch: 2, Sequence: 7, DiscontinuitySequence: 3, SourceURI: "https://fixture.invalid/segment-7.ts"},
			},
		},
	}}
	playlist := hls.MediaPlaylist{Segments: []hls.MediaSegment{
		{Sequence: 6, DiscontinuitySequence: 3, URI: "https://fixture.invalid/segment-6.ts"},
		{Sequence: 7, DiscontinuitySequence: 3, URI: "https://fixture.invalid/segment-7.ts"},
	}}
	if hasHandoverContinuationCandidate(recording, playlist) {
		t.Fatal("all-committed live tail was accepted without an uncommitted media candidate")
	}

	mismatched := playlist
	mismatched.Segments = append([]hls.MediaSegment(nil), playlist.Segments...)
	mismatched.Segments[1].URI = "https://fixture.invalid/replacement.ts"
	epoch, candidate, ok := findHandoverCandidate(recording, mismatched)
	if !ok || epoch != 3 || candidate.Sequence != 6 {
		t.Fatalf("same sequence with changed stable source identity should use next epoch: epoch=%d candidate=%#v ok=%v", epoch, candidate, ok)
	}

	endList := playlist
	endList.EndList = true
	if hasHandoverContinuationCandidate(recording, endList) {
		t.Fatal("ENDLIST with no uncommitted media was accepted as continuation")
	}
}

func pausedHandoverRecording(t *testing.T, fixture *handoverFixture, manifestURL string, waitForSegments int) (*storage.Store, *recordingowner.Store, *Manager, OwnershipToken, *domain.Recording, HandoverSnapshot) {
	t.Helper()
	store, owners, _ := newHandoverStores(t)
	source := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecordingAtURL(t, source, owner, manifestURL)
	waitHandover(t, func() bool { return handoverSchedulerAvailable(source, recording.ID) }, "source scheduler")
	if waitForSegments >= 0 {
		if recording.FormatVersion == storage.ShardedArchiveFormatVersion {
			// V2's root keeps a bounded media count. Observe that durable counter
			// directly instead of materializing the complete recording on every
			// poll; this keeps race runs from starving the serial ingest writer.
			deadline := time.NewTimer(30 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				header, err := store.LoadRecordingHeader(context.Background(), recording.ID)
				if err == nil && header.SegmentCount() == waitForSegments {
					break
				}
				select {
				case <-ticker.C:
				case <-deadline.C:
					t.Fatalf("timed out waiting for expected source segment count %d", waitForSegments)
				}
			}
		} else {
			waitHandover(t, func() bool {
				current, err := source.Get(recording.ID)
				return err == nil && current.SegmentCount() == waitForSegments
			}, "expected source segment count")
		}
	}
	snapshot, err := source.PauseForHandover(context.Background(), recording.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	return store, owners, source, owner, recording, snapshot
}

func resumeAndStopHandoverSource(t *testing.T, source *Manager, owner OwnershipToken, snapshot HandoverSnapshot) {
	t.Helper()
	if err := source.ResumeHandover(snapshot.RecordingID, owner, snapshot); err != nil {
		t.Errorf("resume source after probe test: %v", err)
		return
	}
	if _, err := source.Stop(snapshot.RecordingID); err != nil {
		t.Errorf("stop source after probe test: %v", err)
	}
}

func TestHandoverTargetAdoptsAfterTransferWithoutRootRewriteAndContinuesOrdinal(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{max: 2, metadata: "before"}
	source := newHandoverManager(t, store, owners, fixture, false)
	oldOwner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecording(t, source, oldOwner)
	if recording.FormatVersion != storage.ShardedArchiveFormatVersion {
		t.Fatalf("handover fixture format=%d, want V2 format %d", recording.FormatVersion, storage.ShardedArchiveFormatVersion)
	}
	waitHandover(t, func() bool {
		current, err := source.Get(recording.ID)
		return err == nil && current.SegmentCount() == 2 && len(current.MetadataTimeline) >= 1
	}, "initial segments and metadata")
	fixture.mu.Lock()
	fixture.metadata = "before transfer"
	fixture.mu.Unlock()
	waitHandover(t, func() bool {
		current, err := source.Get(recording.ID)
		return err == nil && len(current.MetadataTimeline) == 2 && current.MetadataTimeline[1].Title != nil && *current.MetadataTimeline[1].Title == "before transfer"
	}, "metadata revision before transfer")
	snapshot, err := source.PauseForHandover(context.Background(), recording.ID, oldOwner)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	metaCallsAtPause := fixture.metaCalls.Load()
	fixture.mu.Unlock()
	time.Sleep(30 * time.Millisecond)
	if got := fixture.metaCalls.Load(); got != metaCallsAtPause {
		t.Fatalf("source metadata monitor continued while parked: calls before=%d after=%d", metaCallsAtPause, got)
	}
	target := newHandoverManager(t, store, owners, fixture, true)
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	before := recordingRootBytes(t, store, recording.ID)
	// The parked source will not poll after its drained snapshot. Advance the
	// fixture so the target's fresh probe can see continuation media beyond the
	// already committed source window.
	setHandoverFixtureMax(fixture, 4)
	segmentRequestsBeforePrepare := fixture.segmentRequests.Load()
	if err = target.PrepareHandoverTarget(context.Background(), snapshot, identity); err != nil {
		t.Fatal(err)
	}
	segmentRequestsAfterPrepare := fixture.segmentRequests.Load()
	if segmentRequestsAfterPrepare-segmentRequestsBeforePrepare != 1 {
		t.Fatalf("target preflight fetches=%d, want exactly candidate sequence 3", segmentRequestsAfterPrepare-segmentRequestsBeforePrepare)
	}
	if !bytes.Equal(before, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("target prepare modified the canonical root")
	}
	newOwner, err := owners.Transfer(oldOwner, handoverGenB, handoverWorkerB)
	if err != nil {
		t.Fatal(err)
	}
	sourceEntry, _ := source.entry(recording.ID)
	if err = source.withCanonicalMutation(sourceEntry, func() error {
		t.Fatal("stale source canonical callback ran after transfer")
		return nil
	}); !errors.Is(err, recordingowner.ErrStaleOwner) {
		t.Fatalf("stale source write = %v, want stale owner", err)
	}
	rootAtTransfer := recordingRootBytes(t, store, recording.ID)
	if err = target.ActivatePreparedHandover(newOwner); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rootAtTransfer, recordingRootBytes(t, store, recording.ID)) {
		t.Fatal("target activation rewrote canonical recording root")
	}
	setHandoverFixtureMax(fixture, 4)
	fixture.mu.Lock()
	fixture.metadata = "after transfer"
	fixture.mu.Unlock()
	waitHandover(t, func() bool {
		current, err := target.Get(recording.ID)
		return err == nil && current.SegmentCount() == 4 && len(current.MetadataTimeline) == 3
	}, "target continuation and metadata")
	if got := fixture.segmentRequests.Load() - segmentRequestsAfterPrepare; got != 1 {
		t.Fatalf("activation fetched staged candidate again or missed next segment: extra requests=%d, want only sequence 4", got)
	}
	// V2 LoadRecordingReadOnly returns the bounded root. Get is the explicit
	// full archive projection used by this assertion.
	current, err := target.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ID != recording.ID || current.State != domain.StateRecording || current.SegmentCount() != 4 || len(current.Gaps) != 0 || len(current.MetadataTimeline) != 3 {
		t.Fatalf("target did not continue same archive cleanly: %#v", current)
	}
	track := current.Tracks["main"]
	if len(track.Segments) != 4 || track.NextArchiveOrdinal != 5 {
		t.Fatalf("target ordinals did not continue: next=%d segments=%#v", track.NextArchiveOrdinal, track.Segments)
	}
	if current.FormatVersion != storage.ShardedArchiveFormatVersion || current.ShardedArchive == nil ||
		current.ShardedArchive.MediaCount != 4 || track.MediaHighWater != 4 || track.LivePresentation == nil ||
		track.LivePresentation.NextOrdinal != 5 {
		t.Fatalf("V2 root high-water did not survive handover: format=%d summary=%#v track=%#v", current.FormatVersion, current.ShardedArchive, track)
	}
	assertHandoverShardedTimeline(t, store, recording.ID, 4)
	for i, segment := range track.Segments {
		if segment.Sequence != uint64(i+1) || segment.ArchiveOrdinal != uint64(i+1) {
			t.Fatalf("segment[%d] identity/ordinal = %d/%d", i, segment.Sequence, segment.ArchiveOrdinal)
		}
		payload, openErr := store.OpenPayloadReader(recording.ID, segment.StoragePath)
		if openErr != nil {
			t.Fatal(openErr)
		}
		data, readErr := io.ReadAll(payload)
		_ = payload.Close()
		if readErr != nil || string(data) != fmt.Sprintf("source-segment-%06d", i+1) {
			t.Fatalf("segment payload %d = %q err=%v", i+1, data, readErr)
		}
	}
	if current.MetadataTimeline[0].Title == nil || *current.MetadataTimeline[0].Title != "before" || current.MetadataTimeline[1].Title == nil || *current.MetadataTimeline[1].Title != "before transfer" || current.MetadataTimeline[2].Title == nil || *current.MetadataTimeline[2].Title != "after transfer" {
		t.Fatalf("metadata timeline did not continue: %#v", current.MetadataTimeline)
	}
	// A fresh generation reads the sharded header and timeline indices after the
	// ownership handoff, matching a restart without changing canonical root state.
	restarted, err := NewManagerWithMode(store, &http.Client{Transport: fixture}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := restarted.Close(ctx); err != nil {
			t.Errorf("close read-only restarted manager: %v", err)
		}
	})
	restartedRecording, err := restarted.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restartedRecording.FormatVersion != storage.ShardedArchiveFormatVersion || restartedRecording.Tracks["main"].NextArchiveOrdinal != 5 ||
		restartedRecording.Tracks["main"].LivePresentation == nil || restartedRecording.Tracks["main"].LivePresentation.NextOrdinal != 5 {
		t.Fatalf("V2 root high-water changed after restart: %#v", restartedRecording.Tracks["main"])
	}
	assertHandoverShardedTimeline(t, store, recording.ID, 4)
	if err = source.CompleteHandover(recording.ID, oldOwner); err != nil {
		t.Fatal(err)
	}
}

func TestShardedHandoverPreflightSkipsCommittedPlaylistPrefixOutsideLiveTail(t *testing.T) {
	// The live presentation tail is 12 slots. Thirteen committed objects are
	// enough to put a committed playlist prefix outside that bounded tail.
	fixture := &handoverFixture{max: 13, metadata: "before"}
	store, owners, source, owner, recording, snapshot := pausedHandoverRecording(t, fixture, "https://fixture.invalid/live.m3u8", 13)
	defer func() {
		resumeAndStopHandoverSource(t, source, owner, snapshot)
	}()
	target := newHandoverManager(t, store, owners, fixture, true)
	identity := HandoverTargetIdentity{EngineGeneration: handoverGenB, WorkerInstance: handoverWorkerB}
	setHandoverFixtureMax(fixture, 14)
	if err := target.PrepareHandoverTarget(context.Background(), snapshot, identity); err != nil {
		t.Fatalf("prepare V2 target from a playlist retaining its committed prefix: %v", err)
	}
	target.mu.RLock()
	prepared, ok := target.prepared[recording.ID]
	target.mu.RUnlock()
	if !ok || prepared.continuation == nil {
		t.Fatal("target did not retain the staged continuation candidate")
	}
	if got := prepared.continuation.source.Sequence; got != 14 {
		t.Fatalf("V2 target staged sequence %d from an already committed prefix, want first continuation sequence 14", got)
	}
	if got := fixture.segmentRequests.Load(); got != 14 {
		t.Fatalf("target candidate fetch count=%d, want exactly one fetch beyond the 13 committed objects", got)
	}
}

func assertHandoverShardedTimeline(t *testing.T, store *storage.Store, recordingID string, want int) {
	t.Helper()
	ordinal := uint64(0)
	err := store.IterateShardedTimeline(context.Background(), recordingID, "main", func(record storage.V2MediaRecord) error {
		ordinal++
		if record.Segment.TimelineOrdinal != ordinal || record.Segment.ArchiveOrdinal != ordinal || record.Segment.LivePresentationOrdinal != ordinal {
			return fmt.Errorf("timeline entry %d has archive=%d timeline=%d live=%d", ordinal, record.Segment.ArchiveOrdinal, record.Segment.TimelineOrdinal, record.Segment.LivePresentationOrdinal)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("iterate V2 handover timeline: %v", err)
	}
	if ordinal != uint64(want) {
		t.Fatalf("V2 timeline yielded %d entries, want %d", ordinal, want)
	}
}

func TestHandoverSnapshotClonesMutableMediaAndResource(t *testing.T) {
	store, owners, _ := newHandoverStores(t)
	fixture := &handoverFixture{metadata: "title"}
	manager := newHandoverManager(t, store, owners, fixture, false)
	owner := claimHandoverOwner(t, owners, handoverGenA, handoverWorkerA)
	recording := startHandoverRecording(t, manager, owner)
	waitHandover(t, func() bool { return handoverSchedulerAvailable(manager, recording.ID) }, "source scheduler")
	e, _ := manager.entry(recording.ID)
	e.mu.Lock()
	e.resource = &adapterproto.ResourceRef{Type: "fixture", ID: "resource-1", Parent: &adapterproto.ResourceRef{Type: "channel", ID: "parent-original"}}
	e.media.Headers = map[string]string{"X-Fixture": "original"}
	e.mu.Unlock()
	snapshot, err := manager.HandoverSnapshot(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	invalid := snapshot
	invalid.Media.ManifestURL = ""
	if _, err := makeHandoverSnapshot(invalid.RecordingID, invalid.Owner, invalid.AdapterID, invalid.Media, invalid.Resource); !errors.Is(err, ErrHandoverTargetInvalid) {
		t.Fatalf("invalid source context accepted: %v", err)
	}
	snapshot.Media.Headers["X-Fixture"] = "caller mutation"
	snapshot.Resource.Parent.ID = "caller mutation"
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.media.Headers["X-Fixture"] != "original" || e.resource.Parent.ID != "parent-original" {
		t.Fatal("handover snapshot exposed mutable entry state")
	}
}
