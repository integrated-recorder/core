package acquire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

type repairOwnerFence struct {
	mu      sync.Mutex
	current *OwnershipToken
	last    uint64
}

func (f *repairOwnerFence) WithCommit(owner OwnershipToken, commit func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current == nil {
		if owner.Epoch <= f.last {
			return ErrInvalidOwnershipToken
		}
		copy := owner
		f.current = &copy
	} else if *f.current != owner {
		return ErrInvalidOwnershipToken
	}
	return commit()
}

func (f *repairOwnerFence) WithUnownedCommit(_ string, commit func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return commit()
}

func (f *repairOwnerFence) WithFencedRecovery(recover func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current != nil {
		if f.current.Epoch > f.last {
			f.last = f.current.Epoch
		}
		f.current = nil
	}
	return recover()
}

func (f *repairOwnerFence) Release(owner OwnershipToken) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.current == nil || *f.current != owner {
		return ErrInvalidOwnershipToken
	}
	f.last = owner.Epoch
	f.current = nil
	return nil
}

type repairFixtureTransport struct {
	mu            sync.Mutex
	manifest      []byte
	manifests     map[string][]byte
	objects       map[string][]byte
	failPath      string
	failRemaining int
	requests      []string
	blockManifest bool
	manifestSeen  chan struct{}
	continueFetch chan struct{}
}

type repairFailingSidecarBackend struct {
	storage.StorageBackend
	mu                 sync.Mutex
	failClaimShardOnce bool
	failRootSaveOnce   bool
}

func (b *repairFailingSidecarBackend) SaveSidecar(id, relativePath string, value any) error {
	b.mu.Lock()
	if b.failClaimShardOnce && strings.HasPrefix(relativePath, "archive-index/claims/") {
		b.failClaimShardOnce = false
		b.mu.Unlock()
		return errors.New("injected claim shard failure")
	}
	b.mu.Unlock()
	return b.StorageBackend.SaveSidecar(id, relativePath, value)
}

func (b *repairFailingSidecarBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	reader, ok := b.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	})
	if !ok {
		return storage.ErrSidecarReadUnsupported
	}
	return reader.LoadSidecar(id, relativePath, maxBytes, output)
}

func (b *repairFailingSidecarBackend) SaveRecording(recording *domain.Recording) error {
	b.mu.Lock()
	if b.failRootSaveOnce {
		b.failRootSaveOnce = false
		b.mu.Unlock()
		return errors.New("injected root save failure")
	}
	b.mu.Unlock()
	return b.StorageBackend.SaveRecording(recording)
}

func (t *repairFixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	requestURL := request.URL.String()
	t.requests = append(t.requests, requestURL)
	if request.URL.Path == "/archive/index.m3u8" && t.blockManifest {
		seen, resume := t.manifestSeen, t.continueFetch
		t.blockManifest = false
		t.mu.Unlock()
		close(seen)
		select {
		case <-resume:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		t.mu.Lock()
	}
	status := http.StatusOK
	var body []byte
	if manifest, ok := t.manifests[request.URL.Path]; ok {
		body = append([]byte(nil), manifest...)
	} else if request.URL.Path == "/archive/index.m3u8" {
		body = append([]byte(nil), t.manifest...)
	} else {
		if request.URL.Path == t.failPath && t.failRemaining > 0 {
			t.failRemaining--
			status = http.StatusServiceUnavailable
		} else if found, ok := t.objects[request.URL.Path]; ok {
			body = append([]byte(nil), found...)
		} else {
			status = http.StatusNotFound
		}
	}
	t.mu.Unlock()
	return &http.Response{
		StatusCode: status, Header: make(http.Header),
		Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: request,
	}, nil
}

func (t *repairFixtureTransport) requestedURLs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.requests...)
}

func (t *repairFixtureTransport) fail(path string, count int) {
	t.mu.Lock()
	t.failPath, t.failRemaining = path, count
	t.mu.Unlock()
}

func (t *repairFixtureTransport) setManifest(body []byte) {
	t.mu.Lock()
	t.manifest = append([]byte(nil), body...)
	t.mu.Unlock()
}

func (t *repairFixtureTransport) setBlockManifest(seen, resume chan struct{}) {
	t.mu.Lock()
	t.blockManifest, t.manifestSeen, t.continueFetch = true, seen, resume
	t.mu.Unlock()
}

func repairMediaContextForTest() adapterproto.MediaSource {
	return adapterproto.MediaSource{
		Type:        "hls",
		ManifestURL: "https://media.example/archive/index.m3u8?hdnts=signed-value",
		SessionRef:  "raw-session-reference-must-not-be-persisted",
		RequestPolicy: &adapterproto.RequestPolicy{URLTransform: &adapterproto.URLTransformPolicy{Rules: []adapterproto.URLTransformRule{
			{Scopes: []adapterproto.ResourceRequestScope{adapterproto.RequestScopeManifest}, QueryParameters: []adapterproto.QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}}},
			{Scopes: []adapterproto.ResourceRequestScope{adapterproto.RequestScopeMedia, adapterproto.RequestScopeInit},
				PathSuffix:      &adapterproto.PathSuffixRewrite{From: ".ts", To: ".m4v"},
				QueryParameters: []adapterproto.QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}}},
		}}},
		HistoricalAvailability: &adapterproto.HistoricalAvailability{
			Mode:           adapterproto.HistoricalModeSequenceRanges,
			SequenceRanges: []adapterproto.HistoricalSequenceRange{{Start: 10, End: 12}},
		},
	}
}

func repairManifestFixture() []byte {
	return []byte(`#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:10
#EXT-X-DISCONTINUITY-SEQUENCE:7
#EXT-X-MAP:URI="init.ts"
#EXTINF:4,
prefix.ts
#EXTINF:4,
#EXT-X-GAP
missing.ts
#EXTINF:4,
tail.ts
#EXT-X-ENDLIST
`)
}

func newRepairTestRoot(t *testing.T, store *storage.Store, state domain.RecordingState, media adapterproto.MediaSource) *domain.Recording {
	t.Helper()
	const id = "84e2f247ab144c6a850fd5c07fb1e6a1"
	contextBytes, err := mediaContext(media)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	root := &domain.Recording{
		FormatVersion: 1, ID: id, Title: "legacy repair fixture", AdapterID: "fixture",
		SourceURIClassification: "public", State: state, CreatedAt: now, StartedAt: now,
		TimelineRevision: 1,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", SourceEpoch: 0, SourcePlaylistURL: media.ManifestURL, NextArchiveOrdinal: 8,
				Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
		Gaps: []domain.Gap{
			{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7, FromSequence: 10, ToSequence: 11, DetectedAt: now, Reason: "legacy missing range"},
			{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 8, FromSequence: 10, ToSequence: 10, DetectedAt: now, Reason: "different discontinuity"},
		},
	}
	if state != domain.StateRecording {
		root.StoppedAt = &now
	}
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	legacyBytes := []byte("legacy-selected-tail")
	legacyPath := "tracks/main/legacy-tail.ts"
	result, err := store.SavePayloadExact(id, legacyPath, bytes.NewReader(legacyBytes), 1<<20, int64(len(legacyBytes)))
	if err != nil {
		t.Fatal(err)
	}
	root.Tracks["main"].Segments = append(root.Tracks["main"].Segments, domain.Segment{
		ID: "legacy-tail", TrackID: "main", Sequence: 12, SourceEpoch: 0, DiscontinuitySequence: 7,
		ArchiveOrdinal: 7, TimelineOrdinal: 1, SourceURI: "https://media.example/archive/tail.ts",
		Duration: 4, StoragePath: legacyPath, PayloadSize: result.Size, SHA256: result.SHA256,
	})
	if err := store.SaveRecording(root); err != nil {
		t.Fatal(err)
	}
	contextDoc, err := newAcquisitionContextSidecar(contextBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSidecar(root.ID, acquisitionContextPath, contextDoc); err != nil {
		t.Fatal(err)
	}
	return root
}

func newRecoveryFairnessRoot(t *testing.T, store *storage.Store, id string, media adapterproto.MediaSource) *domain.Recording {
	t.Helper()
	contextBytes, err := mediaContext(media)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	root := &domain.Recording{
		FormatVersion: 1, ID: id, Title: "recovery fairness fixture", AdapterID: "fixture",
		SourceURIClassification: "public", State: domain.StateCompleted, CreatedAt: now, StartedAt: now,
		StoppedAt: &now, TimelineRevision: 1,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", SourceEpoch: 0, SourcePlaylistURL: media.ManifestURL, NextArchiveOrdinal: 1,
				Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
	}
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	contextDoc, err := newAcquisitionContextSidecar(contextBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSidecar(root.ID, acquisitionContextPath, contextDoc); err != nil {
		t.Fatal(err)
	}
	return root
}

type repairMultiOwnerFence struct {
	mu      sync.Mutex
	current map[string]OwnershipToken
	last    map[string]uint64
}

func newRepairMultiOwnerFence() *repairMultiOwnerFence {
	return &repairMultiOwnerFence{current: make(map[string]OwnershipToken), last: make(map[string]uint64)}
}

func (f *repairMultiOwnerFence) WithCommit(owner OwnershipToken, commit func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, exists := f.current[owner.RecordingID]
	if !exists {
		if owner.Epoch <= f.last[owner.RecordingID] {
			return ErrInvalidOwnershipToken
		}
	} else if current != owner {
		return ErrInvalidOwnershipToken
	}
	if err := commit(); err != nil {
		return err
	}
	f.current[owner.RecordingID] = owner
	return nil
}

func (f *repairMultiOwnerFence) WithUnownedCommit(_ string, commit func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return commit()
}

func (f *repairMultiOwnerFence) WithFencedRecovery(recover func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, owner := range f.current {
		if owner.Epoch > f.last[id] {
			f.last[id] = owner.Epoch
		}
	}
	f.current = make(map[string]OwnershipToken)
	return recover()
}

func (f *repairMultiOwnerFence) Release(owner OwnershipToken) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, exists := f.current[owner.RecordingID]; !exists || current != owner {
		return ErrInvalidOwnershipToken
	}
	f.last[owner.RecordingID] = owner.Epoch
	delete(f.current, owner.RecordingID)
	return nil
}

func newRepairTestManager(t *testing.T, store *storage.Store, fixture *repairFixtureTransport, root *domain.Recording, fence *repairOwnerFence, validate SourceValidator) (*Manager, *entry, func() error) {
	t.Helper()
	if validate == nil {
		validate = func(ctx context.Context, raw string) error {
			parsed, err := url.Parse(raw)
			if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil {
				return errors.New("invalid fixture URL")
			}
			return nil
		}
	}
	manager, err := NewManagerWithMode(store, &http.Client{Transport: fixture}, nil, validate, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(fence); err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureTerminalOwnerRelease(func(_ context.Context, owner OwnershipToken) error { return fence.Release(owner) }); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: loaded, done: closedChannel(), adapterID: loaded.AdapterID}
	manager.mu.Lock()
	manager.entries[root.ID] = e
	manager.mu.Unlock()
	var closeOnce sync.Once
	var closeErr error
	closeManager := func() error {
		closeOnce.Do(func() {
			// These fixtures do not run a worker goroutine. Mark the synthetic entry
			// quiescent before Manager.Close waits for its lifecycle channel.
			e.mu.Lock()
			if e.recording != nil && e.recording.State == domain.StateRecording {
				e.recording.State = domain.StateInterrupted
			}
			e.cancel = nil
			if !channelClosed(e.done) {
				close(e.done)
			}
			e.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			closeErr = manager.Close(ctx)
		})
		return closeErr
	}
	t.Cleanup(func() {
		if err := closeManager(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close repair manager: %v", err)
		}
	})
	return manager, e, closeManager
}

func ownerForRepair(id string, epoch uint64) OwnershipToken {
	return OwnershipToken{RecordingID: id, EngineGeneration: "host-generation", WorkerInstance: "repair-worker", Epoch: epoch}
}

func TestAutomaticArchiveRecoveryRepairsLatePrefixWithoutManualPass(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{
		"/archive/prefix.m4v": []byte("automatic-prefix-exact-bytes"),
		"/archive/init.m4v":   []byte("automatic-prefix-init"),
	}}
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	root.Tracks["main"].SourceEpoch = 1
	root.Tracks["main"].Segments[0].SourceEpoch = 1
	root.Tracks["main"].Segments[0].Discontinuity = true
	for i := range root.Gaps {
		root.Gaps[i].SourceEpoch = 1
	}
	if err := store.SaveRecording(root); err != nil {
		t.Fatal(err)
	}
	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	e.mu.Lock()
	e.media = media
	e.mu.Unlock()
	manager.freshGeneration = false // Model a fenced managed startup, not a candidate generation.
	owner := ownerForRepair(root.ID, 1)
	var claims atomic.Int32
	if err := manager.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (OwnershipToken, error) {
		claims.Add(1)
		return owner, nil
	}); err != nil {
		t.Fatal(err)
	}
	waitForRepairCondition(t, "automatic prefix commit", func() bool {
		current, getErr := manager.Get(root.ID)
		if getErr != nil {
			return false
		}
		for _, segment := range current.Tracks["main"].Segments {
			if segment.Sequence == 10 {
				return true
			}
		}
		return false
	})
	current, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	prefix := segmentAtSequence(t, current.Tracks["main"].Segments, 10)
	tail := segmentAtSequence(t, current.Tracks["main"].Segments, 12)
	if prefix.TimelineOrdinal != 1 || prefix.ArchiveOrdinal != 8 || tail.TimelineOrdinal != 2 || tail.ArchiveOrdinal != 7 {
		t.Fatalf("automatic prefix changed append-only identity or failed projection: prefix=%#v tail=%#v", prefix, tail)
	}
	if current.TimelineRevision <= 1 || current.Duration() != 8 {
		t.Fatalf("automatic prefix timeline revision/duration = %d/%v", current.TimelineRevision, current.Duration())
	}
	assertStoredPayload(t, store, root.ID, prefix.StoragePath, []byte("automatic-prefix-exact-bytes"))
	if claims.Load() != 1 {
		t.Fatalf("terminal recovery owner claims = %d, want one Host claim", claims.Load())
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}
	fence.mu.Lock()
	stillOwned := fence.current != nil
	fence.mu.Unlock()
	if stillOwned {
		t.Fatal("Manager.Close retained terminal owner after recovery scheduler stopped")
	}
}

func TestHistoricalGapOnlyPassUsesBoundedRecoveryWorkAndContinues(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const gapCount = 129
	var manifest strings.Builder
	fmt.Fprintf(&manifest, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n")
	for sequence := 1; sequence <= gapCount; sequence++ {
		fmt.Fprintf(&manifest, "#EXTINF:2,\n#EXT-X-GAP\nsegment-%d.m4s\n", sequence)
	}
	media := repairMediaContextForTest()
	media.HistoricalAvailability.SequenceRanges = []adapterproto.HistoricalSequenceRange{{Start: 1, End: gapCount}}
	fixture := &repairFixtureTransport{manifest: []byte(manifest.String()), objects: map[string][]byte{}}
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	root.Gaps = nil
	if err := store.SaveRecording(root); err != nil {
		t.Fatal(err)
	}
	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	e.mu.Lock()
	e.media = media
	e.recording.Gaps = nil
	e.mu.Unlock()
	owner := ownerForRepair(root.ID, 1)
	first, err := manager.repairDeclaredHistoryPass(context.Background(), owner, root.ID)
	if err != nil {
		t.Fatalf("first bounded GAP pass: %v", err)
	}
	if !first.more {
		t.Fatal("GAP-only manifest did not request continuation after work budget")
	}
	inventory, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	countMissing := func() int {
		missing := 0
		for sequence := uint64(1); sequence <= gapCount; sequence++ {
			coordinate := archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", DiscontinuitySequence: 0, Sequence: sequence, Kind: archiveindex.ObjectMedia}
			if archiveindex.CoverageAt(inventory, coordinate) == archiveindex.CoverageKnownMissing {
				missing++
			}
		}
		return missing
	}
	if count := countMissing(); count > maxHistoricalRecoveryWorkPerPass {
		t.Fatalf("first pass published %d coordinate states, budget=%d", count, maxHistoricalRecoveryWorkPerPass)
	}
	for passes := 1; first.more && passes < 8; passes++ {
		first, err = manager.repairDeclaredHistoryPass(context.Background(), owner, root.ID)
		if err != nil {
			t.Fatalf("GAP continuation pass %d: %v", passes+1, err)
		}
	}
	if first.more {
		t.Fatal("GAP-only manifest did not converge after bounded continuations")
	}
	inventory, err = manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if count := countMissing(); count != gapCount {
		t.Fatalf("GAP continuation did not make forward progress: coverage=%d want=%d", count, gapCount)
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticRecoveryGapOnlyPassYieldsToNextRecording(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const (
		aID       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		bID       = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		gapCount  = 512
		bManifest = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:2,\nb.ts\n#EXT-X-ENDLIST\n"
	)
	mediaA := repairMediaContextForTest()
	mediaA.ManifestURL = "https://media.example/a/index.m3u8?hdnts=signed-value"
	mediaA.HistoricalAvailability.SequenceRanges = []adapterproto.HistoricalSequenceRange{{Start: 1, End: gapCount}}
	mediaB := repairMediaContextForTest()
	mediaB.ManifestURL = "https://media.example/b/index.m3u8?hdnts=signed-value"
	mediaB.HistoricalAvailability.SequenceRanges = []adapterproto.HistoricalSequenceRange{{Start: 1, End: 1}}
	var gapManifest strings.Builder
	gapManifest.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:1\n")
	for sequence := 1; sequence <= gapCount; sequence++ {
		fmt.Fprintf(&gapManifest, "#EXTINF:2,\n#EXT-X-GAP\nsegment-%d.ts\n", sequence)
	}
	fixture := &repairFixtureTransport{
		manifests: map[string][]byte{
			"/a/index.m3u8": []byte(gapManifest.String()),
			"/b/index.m3u8": []byte(bManifest),
		},
		objects: map[string][]byte{"/b/b.m4v": []byte("recording-b-media")},
	}
	aRoot := newRecoveryFairnessRoot(t, store, aID, mediaA)
	bRoot := newRecoveryFairnessRoot(t, store, bID, mediaB)
	fence := newRepairMultiOwnerFence()
	manager, err := NewManagerWithMode(store, &http.Client{Transport: fixture}, nil, func(_ context.Context, raw string) error {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil {
			return errors.New("invalid fixture URL")
		}
		return nil
	}, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(fence); err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureTerminalOwnerRelease(func(_ context.Context, owner OwnershipToken) error { return fence.Release(owner) }); err != nil {
		t.Fatal(err)
	}
	manager.freshGeneration = false
	for _, setup := range []struct {
		root  *domain.Recording
		media adapterproto.MediaSource
	}{{aRoot, mediaA}, {bRoot, mediaB}} {
		loaded, loadErr := store.LoadRecordingReadOnly(setup.root.ID)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		manager.entries[setup.root.ID] = &entry{recording: loaded, done: closedChannel(), adapterID: loaded.AdapterID, media: setup.media}
	}
	claim := func(_ context.Context, id string) (OwnershipToken, error) {
		switch id {
		case aID:
			return ownerForRepair(id, 1), nil
		case bID:
			return ownerForRepair(id, 1), nil
		default:
			return OwnershipToken{}, errors.New("unexpected recording claim")
		}
	}
	scheduler := newAutomaticRecoveryScheduler(manager, claim)
	t.Cleanup(func() {
		scheduler.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := manager.Close(ctx); closeErr != nil {
			t.Errorf("close fairness manager: %v", closeErr)
		}
	})
	fixedNow := time.Now()
	scheduler.now = func() time.Time { return fixedNow }
	scheduler.enqueue(aID, time.Time{})
	scheduler.enqueue(bID, time.Time{})
	firstID, firstItem, _, ok := scheduler.next()
	if !ok || firstID != aID {
		t.Fatalf("large history did not start first: id=%q ok=%v", firstID, ok)
	}
	aOutcome := scheduler.process(firstID, firstItem)
	if !aOutcome.more {
		t.Fatal("large GAP-only history did not yield a continuation")
	}
	aInventory, err := manager.ArchiveInventory(aID)
	if err != nil {
		t.Fatal(err)
	}
	aPublished := 0
	for sequence := uint64(1); sequence <= gapCount; sequence++ {
		coordinate := archiveindex.Coordinate{SessionID: aInventory.Session.ID, TrackID: "main", Sequence: sequence, Kind: archiveindex.ObjectMedia}
		if archiveindex.CoverageAt(aInventory, coordinate) == archiveindex.CoverageKnownMissing {
			aPublished++
		}
	}
	if aPublished == 0 || aPublished > maxHistoricalRecoveryWorkPerPass {
		t.Fatalf("A first pass mutations=%d, want 1..%d", aPublished, maxHistoricalRecoveryWorkPerPass)
	}
	scheduler.complete(firstID, firstItem, aOutcome)
	secondID, secondItem, _, ok := scheduler.next()
	if !ok || secondID != bID {
		t.Fatalf("B did not run after A's bounded pass: id=%q ok=%v queue=%v", secondID, ok, scheduler.queue)
	}
	bOutcome := scheduler.process(secondID, secondItem)
	if bOutcome.retry || bOutcome.more {
		t.Fatalf("B's single fetchable coordinate did not finish: %+v", bOutcome)
	}
	currentB, err := manager.Get(bID)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(currentB.Tracks["main"].Segments); got != 1 {
		t.Fatalf("B persisted segments=%d, want 1 before A history drained", got)
	}
	scheduler.complete(secondID, secondItem, bOutcome)
}

func TestAutomaticArchiveRecoveryRetriesMiddleGapAfterBackoff(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	manifest := bytes.Replace(repairManifestFixture(), []byte("#EXT-X-GAP\nmissing.ts"), []byte("middle.ts"), 1)
	fixture := &repairFixtureTransport{manifest: manifest, objects: map[string][]byte{
		"/archive/prefix.m4v": []byte("retry-prefix"),
		"/archive/middle.m4v": []byte("retry-middle-exact-bytes"),
		"/archive/init.m4v":   []byte("retry-init"),
	}}
	fixture.fail("/archive/middle.m4v", 1)
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	e.mu.Lock()
	e.media = media
	e.mu.Unlock()
	manager.freshGeneration = false
	owner := ownerForRepair(root.ID, 1)
	var claims atomic.Int32
	if err := manager.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (OwnershipToken, error) {
		claims.Add(1)
		return owner, nil
	}); err != nil {
		t.Fatal(err)
	}
	waitForRepairCondition(t, "middle acquisition failure coverage", func() bool {
		inventory, inventoryErr := manager.ArchiveInventory(root.ID)
		if inventoryErr != nil {
			return false
		}
		coordinate := archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7, Sequence: 11, Kind: archiveindex.ObjectMedia}
		return archiveindex.CoverageAt(inventory, coordinate) == archiveindex.CoverageAcquisitionFailed
	})
	firstMiddleRequests := 0
	for _, requested := range fixture.requestedURLs() {
		if strings.Contains(requested, "/archive/middle.m4v") {
			firstMiddleRequests++
		}
	}
	if firstMiddleRequests != 1 {
		t.Fatalf("middle requests before scheduler backoff = %d, want one failed request", firstMiddleRequests)
	}
	waitForRepairCondition(t, "automatic retry after capped backoff", func() bool {
		count := 0
		for _, requested := range fixture.requestedURLs() {
			if strings.Contains(requested, "/archive/middle.m4v") {
				count++
			}
		}
		return count >= 2
	})
	waitForRepairCondition(t, "repaired middle gap projection", func() bool {
		current, getErr := manager.Get(root.ID)
		if getErr != nil || len(current.Tracks["main"].Segments) != 3 {
			return false
		}
		middle, ok := findSegmentBySequence(current.Tracks["main"].Segments, 11)
		return ok && middle.TimelineOrdinal == 2 && current.TimelineRevision > 1
	})
	current, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Duration() != 12 || segmentAtSequence(t, current.Tracks["main"].Segments, 12).TimelineOrdinal != 3 {
		t.Fatalf("VOD projection after automatic gap repair: duration=%v segments=%#v", current.Duration(), current.Tracks["main"].Segments)
	}
	if claims.Load() != 1 {
		t.Fatalf("recovery retry claimed a new owner instead of retaining generation lease: claims=%d", claims.Load())
	}
	assertStoredPayload(t, store, root.ID, segmentAtSequence(t, current.Tracks["main"].Segments, 11).StoragePath, []byte("retry-middle-exact-bytes"))
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}
}

func TestLiveCommitAndFailurePublicationWakeAutomaticRecovery(t *testing.T) {
	for _, trigger := range []string{"live-claim", "acquisition-failure"} {
		t.Run(trigger, func(t *testing.T) {
			store, err := storage.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			media := repairMediaContextForTest()
			fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{}}
			root := newRepairTestRoot(t, store, domain.StateCompleted, media)
			fence := &repairOwnerFence{}
			manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
			e.mu.Lock()
			e.media = media
			e.mu.Unlock()
			owner := ownerForRepair(root.ID, 1)
			if err := manager.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (OwnershipToken, error) {
				return owner, nil
			}); err != nil {
				t.Fatal(err)
			}

			switch trigger {
			case "live-claim":
				segment := domain.Segment{
					TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7, Sequence: 13,
					ArchiveOrdinal: 8, TimelineOrdinal: 2, Duration: 4,
					SourceURI: "https://media.example/archive/live-13.ts",
				}
				if _, _, err := manager.commitArchiveSegmentOwned(e, &owner, segment, archiveindex.ClaimLiveOrigin, []byte("live-claim-bytes"), true); err != nil {
					t.Fatalf("publish live claim: %v", err)
				}
			case "acquisition-failure":
				e.mu.Lock()
				e.ownership = &owner
				e.mu.Unlock()
				if err := manager.markPending(e, 0, 7, 11, newFetchError("segment", http.StatusServiceUnavailable, true, true)); err != nil {
					t.Fatalf("publish acquisition failure: %v", err)
				}
			}

			waitForRepairCondition(t, "automatic recovery after "+trigger, func() bool {
				for _, requested := range fixture.requestedURLs() {
					if strings.Contains(requested, "/archive/index.m3u8") {
						return true
					}
				}
				return false
			})
			if err := closeManager(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAutomaticArchiveRecoveryResumesAfterRestartWithoutRefetchingPresentClaims(t *testing.T) {
	dataDir := t.TempDir()
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	manifest := bytes.Replace(repairManifestFixture(), []byte("#EXT-X-GAP\nmissing.ts"), []byte("middle.ts"), 1)
	fixture := &repairFixtureTransport{manifest: manifest, objects: map[string][]byte{
		"/archive/prefix.m4v": []byte("restart-prefix-bytes"),
		"/archive/middle.m4v": []byte("restart-middle-bytes"),
		"/archive/init.m4v":   []byte("restart-init-bytes"),
	}}
	fixture.fail("/archive/middle.m4v", 1)
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	fenceBeforeRestart := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fenceBeforeRestart, nil)
	e.mu.Lock()
	e.media = media
	e.mu.Unlock()
	firstOwner := ownerForRepair(root.ID, 1)
	if err := manager.RepairDeclaredHistory(context.Background(), firstOwner, root.ID); err != nil {
		t.Fatalf("initial partial historical pass: %v", err)
	}
	beforeRestart, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	prefixBefore := segmentAtSequence(t, beforeRestart.Tracks["main"].Segments, 10)
	if _, ok := findSegmentBySequence(beforeRestart.Tracks["main"].Segments, 11); ok {
		t.Fatal("fixture did not leave a partial repair for restart")
	}
	countRequested := func(fragment string) int {
		count := 0
		for _, requested := range fixture.requestedURLs() {
			if strings.Contains(requested, fragment) {
				count++
			}
		}
		return count
	}
	if prefixRequests := countRequested("/archive/prefix.m4v"); prefixRequests != 1 {
		t.Fatalf("initial prefix fetch count=%d, want 1", prefixRequests)
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}

	restartedStore, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	fenceAfterRestart := &repairOwnerFence{}
	validate := SourceValidator(func(_ context.Context, raw string) error {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errors.New("invalid fixture URL")
		}
		return nil
	})
	restarted, err := NewManagerWithFencedRecovery(restartedStore, &http.Client{Transport: fixture}, nil, validate, fenceAfterRestart, fenceAfterRestart)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ConfigureTerminalOwnerRelease(func(_ context.Context, owner OwnershipToken) error { return fenceAfterRestart.Release(owner) }); err != nil {
		t.Fatal(err)
	}
	requestsBeforeOwnerConfig := len(fixture.requestedURLs())
	time.Sleep(25 * time.Millisecond)
	if requestsAfterLoad := len(fixture.requestedURLs()); requestsAfterLoad != requestsBeforeOwnerConfig {
		t.Fatalf("recovered archive started network work before Host owner callback configuration: before=%d after=%d", requestsBeforeOwnerConfig, requestsAfterLoad)
	}
	var claims atomic.Int32
	secondOwner := ownerForRepair(root.ID, 2)
	if err := restarted.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (OwnershipToken, error) {
		claims.Add(1)
		return secondOwner, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := restarted.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close restarted automatic recovery manager: %v", err)
		}
	})
	waitForRepairCondition(t, "automatic repair after cold restart", func() bool {
		current, getErr := restarted.Get(root.ID)
		if getErr != nil {
			return false
		}
		_, present := findSegmentBySequence(current.Tracks["main"].Segments, 11)
		return present
	})
	afterRestart, err := restarted.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	prefixAfter := segmentAtSequence(t, afterRestart.Tracks["main"].Segments, 10)
	if prefixAfter.StoragePath != prefixBefore.StoragePath || prefixAfter.ArchiveOrdinal != prefixBefore.ArchiveOrdinal || prefixAfter.SHA256 != prefixBefore.SHA256 {
		t.Fatalf("restart changed existing immutable claim identity: before=%#v after=%#v", prefixBefore, prefixAfter)
	}
	assertStoredPayload(t, restartedStore, root.ID, prefixAfter.StoragePath, []byte("restart-prefix-bytes"))
	if prefixRequests := countRequested("/archive/prefix.m4v"); prefixRequests != 1 {
		t.Fatalf("restart refetched already-present prefix %d times", prefixRequests-1)
	}
	if middleRequests := countRequested("/archive/middle.m4v"); middleRequests != 2 {
		t.Fatalf("restart middle requests=%d, want one failed pre-crash request and one recovery request", middleRequests)
	}
	if claims.Load() != 1 {
		t.Fatalf("restart recovery owner claims=%d, want one authenticated Host claim", claims.Load())
	}
}

func TestSlowHistoricalRequestDoesNotBlockLiveCanonicalCommit(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{
		"/archive/prefix.m4v": []byte("concurrent-prefix"),
		"/archive/middle.m4v": []byte("concurrent-middle"),
		"/archive/init.m4v":   []byte("concurrent-init"),
	}}
	historicalSeen, resumeHistorical := make(chan struct{}), make(chan struct{})
	fixture.setBlockManifest(historicalSeen, resumeHistorical)
	root := newRepairTestRoot(t, store, domain.StateRecording, media)
	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	owner := ownerForRepair(root.ID, 1)
	_, cancelLive := context.WithCancel(context.Background())
	e.mu.Lock()
	e.media = media
	e.ownership = &owner
	e.cancel = cancelLive
	e.done = make(chan struct{})
	e.mu.Unlock()
	var claims atomic.Int32
	if err := manager.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (OwnershipToken, error) {
		claims.Add(1)
		return owner, nil
	}); err != nil {
		t.Fatal(err)
	}
	manager.signalAutomaticArchiveRecovery(root.ID)
	select {
	case <-historicalSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic historical request did not start")
	}

	liveSegment := domain.Segment{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 7, Sequence: 13,
		ArchiveOrdinal: 8, TimelineOrdinal: 2, Duration: 4,
		SourceURI: "https://media.example/archive/live-13.ts",
	}
	committed := make(chan error, 1)
	go func() {
		_, _, commitErr := manager.commitArchiveSegment(e, &owner, liveSegment, archiveindex.ClaimLiveOrigin, []byte("live-during-history"))
		committed <- commitErr
	}()
	select {
	case commitErr := <-committed:
		if commitErr != nil {
			t.Fatalf("live commit while historical fetch blocked: %v", commitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("historical network request blocked the live canonical commit")
	}
	current, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := findSegmentBySequence(current.Tracks["main"].Segments, 13); !present {
		t.Fatal("live commit did not publish while historical request remained blocked")
	}
	cancelLive()
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}
	if claims.Load() != 0 {
		t.Fatalf("live historical recovery claimed a replacement owner %d times", claims.Load())
	}
}

func findSegmentBySequence(segments []domain.Segment, sequence uint64) (domain.Segment, bool) {
	for _, segment := range segments {
		if segment.Sequence == sequence {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

func waitForRepairCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestRepairDeclaredHistoryLegacyRootTransformRetryRestartAndSeal(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	fixture := &repairFixtureTransport{
		manifest: repairManifestFixture(),
		objects: map[string][]byte{
			"/archive/prefix.m4v": []byte("repaired-prefix-exact-bytes"),
			"/archive/middle.m4v": []byte("middle-segment-exact-bytes"),
			"/archive/init.m4v":   []byte("init-segment-exact-bytes"),
		},
	}
	fixture.fail("/archive/prefix.m4v", 1)
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	// Prior capture first observed sequence 12 in source epoch 1. A late
	// sequence-10 prefix must move the synthetic marker by coordinate order.
	root.Tracks["main"].SourceEpoch = 1
	root.Tracks["main"].Segments[0].SourceEpoch = 1
	root.Tracks["main"].Segments[0].Discontinuity = true
	for i := range root.Gaps {
		root.Gaps[i].SourceEpoch = 1
	}
	if err := store.SaveRecording(root); err != nil {
		t.Fatal(err)
	}
	fence := &repairOwnerFence{}
	validated := make(map[string]bool)
	var validationMu sync.Mutex
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, func(_ context.Context, raw string) error {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil || parsed.Scheme != "https" || parsed.Host != "media.example" || parsed.User != nil {
			return errors.New("URL rejected by fixture policy")
		}
		validationMu.Lock()
		validated[raw] = true
		validationMu.Unlock()
		return nil
	})
	ctx := context.Background()
	firstOwner := ownerForRepair(root.ID, 1)
	if err := manager.RepairDeclaredHistory(ctx, firstOwner, root.ID); err != nil {
		t.Fatalf("first declared-history pass: %v", err)
	}
	afterFailure, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: afterFailure.Session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 7, Sequence: 10, Kind: archiveindex.ObjectMedia}
	if state := archiveindex.CoverageAt(afterFailure, coordinate); state != archiveindex.CoverageAcquisitionFailed {
		t.Fatalf("failed acquisition coverage = %q, want acquisition_failed", state)
	}
	missingCoordinate := coordinate
	missingCoordinate.Sequence = 11
	if state := archiveindex.CoverageAt(afterFailure, missingCoordinate); state != archiveindex.CoverageKnownMissing {
		t.Fatalf("manifest-declared gap coverage = %q, want known_missing", state)
	}

	// A later source window now exposes the previously declared missing
	// coordinate as media. Repair must replace the old coverage projection.
	fixture.setManifest(bytes.Replace(repairManifestFixture(), []byte("#EXT-X-GAP\nmissing.ts"), []byte("middle.ts"), 1))
	secondOwner := ownerForRepair(root.ID, 2)
	if err := manager.RepairDeclaredHistory(ctx, secondOwner, root.ID); err != nil {
		t.Fatalf("retry declared-history pass: %v", err)
	}
	repaired, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	track := repaired.Tracks["main"]
	if len(track.Segments) != 3 {
		t.Fatalf("selected segments after repair = %#v", track.Segments)
	}
	var prefix, middle, tail domain.Segment
	for _, segment := range track.Segments {
		switch segment.Sequence {
		case 10:
			prefix = segment
		case 11:
			middle = segment
		case 12:
			tail = segment
		}
	}
	if prefix.ID == "" || prefix.ArchiveOrdinal != 8 || prefix.TimelineOrdinal != 1 {
		t.Fatalf("late prefix projection = %#v", prefix)
	}
	if middle.ID == "" || middle.ArchiveOrdinal != 9 || middle.TimelineOrdinal != 2 {
		t.Fatalf("repaired middle gap projection = %#v", middle)
	}
	if tail.ID != "legacy-tail" || tail.ArchiveOrdinal != 7 || tail.TimelineOrdinal != 3 {
		t.Fatalf("legacy selected tail changed unexpectedly: %#v", tail)
	}
	if !prefix.Discontinuity || tail.Discontinuity {
		t.Fatalf("late prefix did not move epoch marker: prefix=%#v tail=%#v", prefix, tail)
	}
	if repaired.TimelineRevision <= 1 {
		t.Fatalf("timeline revision = %d, want revision after prefix insertion", repaired.TimelineRevision)
	}
	if len(repaired.Gaps) != 1 || repaired.Gaps[0].DiscontinuitySequence != 8 {
		t.Fatalf("DS-aware gap repair = %#v", repaired.Gaps)
	}
	rootJSON, marshalErr := json.Marshal(repaired)
	var persistedContext acquisitionContextSidecar
	contextErr := store.LoadSidecar(root.ID, acquisitionContextPath, maxAcquisitionContext, &persistedContext)
	contextJSON, contextMarshalErr := json.Marshal(persistedContext)
	if len(repaired.SourceSessionID) == 0 || marshalErr != nil || bytes.Contains(rootJSON, []byte("acquisition_context")) || bytes.Contains(rootJSON, []byte("raw-session-reference-must-not-be-persisted")) || contextErr != nil || contextMarshalErr != nil || bytes.Contains(contextJSON, []byte("raw-session-reference-must-not-be-persisted")) {
		t.Fatalf("legacy adoption/private context invariant failed: source session=%q root=%s context=%s rootErr=%v contextErr=%v", repaired.SourceSessionID, rootJSON, contextJSON, marshalErr, contextErr)
	}
	assertStoredPayload(t, store, root.ID, prefix.StoragePath, []byte("repaired-prefix-exact-bytes"))
	assertStoredPayload(t, store, root.ID, middle.StoragePath, []byte("middle-segment-exact-bytes"))
	assertStoredPayload(t, store, root.ID, "tracks/main/legacy-tail.ts", []byte("legacy-selected-tail"))
	if len(track.InitSegments) != 1 || track.InitSegments[0].IsInit != true {
		t.Fatalf("historical init object missing: %#v", track.InitSegments)
	}
	assertStoredPayload(t, store, root.ID, track.InitSegments[0].StoragePath, []byte("init-segment-exact-bytes"))
	wantPrefixDigest := sha256.Sum256([]byte("repaired-prefix-exact-bytes"))
	if got := hex.EncodeToString(wantPrefixDigest[:]); got != prefix.SHA256 {
		t.Fatalf("prefix recorded hash = %s, want %s", prefix.SHA256, got)
	}
	wantMiddleDigest := sha256.Sum256([]byte("middle-segment-exact-bytes"))
	if got := hex.EncodeToString(wantMiddleDigest[:]); got != middle.SHA256 {
		t.Fatalf("middle recorded hash = %s, want %s", middle.SHA256, got)
	}
	assertTransformedRequest(t, fixture.requestedURLs(), "/archive/index.m3u8", ".m4v", "__bgda__", "signed-value")
	validationMu.Lock()
	for _, requested := range fixture.requestedURLs() {
		if !validated[requested] {
			validationMu.Unlock()
			t.Fatalf("request URL was not validated immediately before fetch: %s", requested)
		}
	}
	validationMu.Unlock()

	// Same bytes from a second origin are a duplicate claim; different bytes
	// become a conflict claim while the selected root object remains unchanged.
	thirdOwner := ownerForRepair(root.ID, 3)
	e.mu.Lock()
	e.ownership = &thirdOwner
	e.mu.Unlock()
	duplicate := domain.Segment{
		TrackID: "main", Sequence: 10, SourceEpoch: 1, DiscontinuitySequence: 7,
		SourceURI: "https://media.example/archive/prefix.ts", Duration: 4,
	}
	if _, _, err := manager.commitArchiveSegmentOwned(e, &thirdOwner, duplicate, archiveindex.ClaimPeer, []byte("repaired-prefix-exact-bytes"), true); err != nil {
		t.Fatalf("identical alternate claim: %v", err)
	}
	conflict := duplicate
	if _, _, err := manager.commitArchiveSegmentOwned(e, &thirdOwner, conflict, archiveindex.ClaimPeer, []byte("different-peer-bytes"), true); err != nil {
		t.Fatalf("conflicting alternate claim: %v", err)
	}
	selectedAfterConflict, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	selectedPrefix := segmentAtSequence(t, selectedAfterConflict.Tracks["main"].Segments, 10)
	if selectedPrefix.StoragePath != prefix.StoragePath || selectedPrefix.SHA256 != prefix.SHA256 || selectedPrefix.TimelineOrdinal != 1 {
		t.Fatalf("conflict replaced selected bytes/projection: %#v", selectedPrefix)
	}
	conflictInventory, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state := archiveindex.CoverageAt(conflictInventory, coordinate); state != archiveindex.CoverageConflict {
		t.Fatalf("conflict inventory state = %q", state)
	}
	var conflictSegment *archiveindex.Segment
	for i := range conflictInventory.Segments {
		if conflictInventory.Segments[i].Coordinate == coordinate {
			conflictSegment = &conflictInventory.Segments[i]
			break
		}
	}
	if conflictSegment == nil || len(conflictSegment.Claims) < 3 || conflictSegment.SelectedClaimID == "" {
		t.Fatalf("duplicate/conflict claim evidence missing: %#v", conflictSegment)
	}
	assertStoredPayload(t, store, root.ID, prefix.StoragePath, []byte("repaired-prefix-exact-bytes"))
	conflictClaimFound := false
	for _, claim := range conflictSegment.Claims {
		if claim.SHA256 != prefix.SHA256 && claim.PayloadPath != prefix.StoragePath {
			assertStoredPayload(t, store, root.ID, claim.PayloadPath, []byte("different-peer-bytes"))
			conflictClaimFound = true
		}
	}
	if !conflictClaimFound {
		t.Fatal("conflicting bytes were not retained at a distinct immutable path")
	}
	if err := manager.releaseRepairOwner(context.Background(), e, thirdOwner); err != nil {
		t.Fatal(err)
	}

	// A host-issued stale epoch cannot reacquire the released terminal archive.
	if err := manager.RepairDeclaredHistory(ctx, secondOwner, root.ID); !errors.Is(err, ErrInvalidOwnershipToken) {
		t.Fatalf("stale epoch repair error = %v", err)
	}

	// Sidecars and root are reloaded by a cold manager without payload rewrites.
	loadedRoot, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}
	manager2, err := NewManagerWithMode(store, &http.Client{Transport: fixture}, nil, nil, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	fence2 := &repairOwnerFence{last: 3}
	if err := manager2.ConfigureCanonicalCommitFence(fence2); err != nil {
		t.Fatal(err)
	}
	if err := manager2.ConfigureTerminalOwnerRelease(func(_ context.Context, owner OwnershipToken) error { return fence2.Release(owner) }); err != nil {
		t.Fatal(err)
	}
	manager2.entries[root.ID] = &entry{recording: loadedRoot, done: closedChannel(), adapterID: loadedRoot.AdapterID}
	var manager2CloseOnce sync.Once
	manager2Close := func() { manager2CloseOnce.Do(func() { _ = manager2.Close(context.Background()) }) }
	t.Cleanup(manager2Close)
	coldInventory, err := manager2.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatalf("restart inventory: %v", err)
	}
	if archiveindex.CoverageAt(coldInventory, coordinate) != archiveindex.CoverageConflict {
		t.Fatalf("restart lost conflict evidence: %#v", coldInventory)
	}
	var coldSelected *archiveindex.Segment
	for i := range coldInventory.Segments {
		if coldInventory.Segments[i].Coordinate == coordinate {
			coldSelected = &coldInventory.Segments[i]
			break
		}
	}
	if coldSelected == nil {
		t.Fatal("restart lost selected prefix inventory")
	}
	selectedClaimFound := false
	for _, claim := range coldSelected.Claims {
		if claim.ID == coldSelected.SelectedClaimID && claim.Source == archiveindex.ClaimHistorical && claim.SHA256 == prefix.SHA256 {
			selectedClaimFound = true
		}
	}
	if !selectedClaimFound {
		t.Fatalf("restart lost selected historical provenance: %#v", coldSelected)
	}
	if err := manager2.SealArchive(ownerForRepair(root.ID, 4), root.ID); err != nil {
		t.Fatalf("explicit seal: %v", err)
	}
	if err := manager2.RepairDeclaredHistory(ctx, ownerForRepair(root.ID, 5), root.ID); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("sealed archive repair error = %v", err)
	}
	sealed, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil || !sealed.ArchiveSealed {
		t.Fatalf("root seal persistence: sealed=%v err=%v", sealed != nil && sealed.ArchiveSealed, err)
	}
	if !bytes.Equal(mustLoadPayload(t, store, root.ID, "tracks/main/legacy-tail.ts"), []byte("legacy-selected-tail")) {
		t.Fatal("legacy archive payload was rewritten")
	}
	manager2Close()
}

func TestRepairDeclaredHistorySharesActiveFenceWithLiveCommit(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{
		"/archive/prefix.m4v": []byte("historical-prefix"),
		"/archive/init.m4v":   []byte("historical-init"),
	}}
	root := newRepairTestRoot(t, store, domain.StateRecording, media)
	owner := ownerForRepair(root.ID, 9)
	fence := &repairOwnerFence{current: &owner, last: owner.Epoch}
	manager, e, _ := newRepairTestManager(t, store, fixture, root, fence, nil)
	e.done = make(chan struct{})
	e.cancel = func() {}
	e.media = media
	e.mediaGeneration = 1
	e.ownership = &owner
	seenManifest, continueManifest := make(chan struct{}), make(chan struct{})
	fixture.setBlockManifest(seenManifest, continueManifest)
	repairDone := make(chan error, 1)
	go func() { repairDone <- manager.RepairDeclaredHistory(context.Background(), owner, root.ID) }()
	select {
	case <-seenManifest:
	case <-time.After(3 * time.Second):
		t.Fatal("historical repair did not reach manifest transport")
	}
	liveSegment := domain.Segment{TrackID: "main", Sequence: 13, SourceEpoch: 0, DiscontinuitySequence: 7, SourceURI: "https://media.example/archive/live.ts", Duration: 4}
	if _, _, err := manager.commitArchiveSegment(e, &owner, liveSegment, archiveindex.ClaimLiveOrigin, []byte("concurrent-live-bytes")); err != nil {
		t.Fatalf("live commit during history fetch: %v", err)
	}
	close(continueManifest)
	select {
	case err := <-repairDone:
		if err != nil {
			t.Fatalf("historical repair after live commit: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("historical repair did not finish")
	}
	recording, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if segmentAtSequence(t, recording.Tracks["main"].Segments, 10).TimelineOrdinal != 1 ||
		segmentAtSequence(t, recording.Tracks["main"].Segments, 12).TimelineOrdinal != 2 ||
		segmentAtSequence(t, recording.Tracks["main"].Segments, 13).TimelineOrdinal != 3 {
		t.Fatalf("concurrent live/history timeline = %#v", recording.Tracks["main"].Segments)
	}
	if err := manager.update(e, func(root *domain.Recording) error {
		root.State = domain.StateInterrupted
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.cancel = nil
	close(e.done)
	e.mu.Unlock()
}

func TestCommitOrdinalCollisionReassignsPendingLiveOrdinal(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{}}
	root := newRepairTestRoot(t, store, domain.StateRecording, media)
	owner := ownerForRepair(root.ID, 10)
	fence := &repairOwnerFence{current: &owner, last: owner.Epoch}
	manager, e, _ := newRepairTestManager(t, store, fixture, root, fence, nil)
	e.mu.Lock()
	e.ownership = &owner
	e.done = make(chan struct{})
	e.cancel = func() {}
	e.mu.Unlock()

	historical := domain.Segment{
		TrackID: "main", Sequence: 10, SourceEpoch: 0, DiscontinuitySequence: 7,
		SourceURI: "https://media.example/archive/historical.ts", Duration: 4,
	}
	if _, _, err := manager.commitArchiveSegment(e, &owner, historical, archiveindex.ClaimHistorical, []byte("historical-first")); err != nil {
		t.Fatalf("historical commit: %v", err)
	}
	// Simulate a stale allocator high-water mark after a historical insert.
	// The next live task already carries ordinal 8, so commit must skip both
	// the selected ordinal and the stale value under the same fence.
	if err := manager.update(e, func(recording *domain.Recording) error {
		recording.Tracks["main"].NextArchiveOrdinal = 8
		return nil
	}); err != nil {
		t.Fatalf("stale high-water fixture: %v", err)
	}
	pendingLive := domain.Segment{
		TrackID: "main", Sequence: 11, SourceEpoch: 0, DiscontinuitySequence: 7,
		ArchiveOrdinal: 8, SourceURI: "https://media.example/archive/live.ts", Duration: 4,
	}
	if _, _, err := manager.commitArchiveSegment(e, &owner, pendingLive, archiveindex.ClaimLiveOrigin, []byte("pending-live")); err != nil {
		t.Fatalf("live commit with colliding pending ordinal: %v", err)
	}
	recording, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	gotHistorical := segmentAtSequence(t, track.Segments, 10)
	gotLive := segmentAtSequence(t, track.Segments, 11)
	if gotHistorical.ArchiveOrdinal != 8 || gotLive.ArchiveOrdinal != 9 {
		t.Fatalf("archive ordinals after serialized collision = historical:%d live:%d", gotHistorical.ArchiveOrdinal, gotLive.ArchiveOrdinal)
	}
	if gotHistorical.TimelineOrdinal != 1 || gotLive.TimelineOrdinal != 2 || segmentAtSequence(t, track.Segments, 12).TimelineOrdinal != 3 {
		t.Fatalf("coordinate timeline order = %#v", track.Segments)
	}
	seen := map[uint64]bool{}
	for _, segment := range track.Segments {
		if seen[segment.ArchiveOrdinal] {
			t.Fatalf("duplicate append-only archive ordinal %d: %#v", segment.ArchiveOrdinal, track.Segments)
		}
		seen[segment.ArchiveOrdinal] = true
	}
	if err := manager.update(e, func(recording *domain.Recording) error {
		recording.State = domain.StateInterrupted
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.cancel = nil
	close(e.done)
	e.mu.Unlock()
}

func TestSelectedClaimShardRetryPreservesHistoricalProvenance(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	backend := &repairFailingSidecarBackend{StorageBackend: store.StorageBackend, failClaimShardOnce: true, failRootSaveOnce: true}
	store.StorageBackend = backend
	fixture := &repairFixtureTransport{manifest: repairManifestFixture(), objects: map[string][]byte{}}
	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	segment := domain.Segment{
		TrackID: "main", Sequence: 30, SourceEpoch: 0, DiscontinuitySequence: 7,
		SourceURI: "https://media.example/archive/retry.ts", Duration: 4,
	}
	data := []byte("persisted-before-provenance")
	firstOwner := ownerForRepair(root.ID, 1)
	if _, _, err := manager.commitArchiveSegmentOwned(e, &firstOwner, segment, archiveindex.ClaimHistorical, data, true); err == nil {
		t.Fatal("expected injected claim shard failure")
	}
	identity, err := archiveSessionIdentity(root, root.AdapterID, "")
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: identity.ID, TrackID: "main", DiscontinuitySequence: 7, Sequence: 30, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	expectedPath := immutableObjectPath(segmentID, sha256Hex(data), segment.SourceURI)
	assertStoredPayload(t, store, root.ID, expectedPath, data)
	if _, err := manager.Get(root.ID); err != nil {
		t.Fatal(err)
	}
	inventoryAfterShardFailure, err := manager.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range inventoryAfterShardFailure.Segments {
		if item.Coordinate == coordinate {
			t.Fatalf("uncommitted orphan became visible after shard failure: %#v", item)
		}
	}
	if err := manager.releaseRepairOwner(context.Background(), e, firstOwner); err != nil {
		t.Fatal(err)
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}

	// Cold restart sees neither an unreferenced payload nor a claim shard as a
	// canonical media object. The next attempt writes the shard, then a forced
	// root failure exercises the crash window immediately before publication.
	loadedRoot, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	manager2, e2, closeManager2 := newRepairTestManager(t, store, fixture, loadedRoot, &repairOwnerFence{last: 1}, nil)
	inventoryAfterRestart, err := manager2.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range inventoryAfterRestart.Segments {
		if item.Coordinate == coordinate {
			t.Fatalf("orphan object became visible after restart: %#v", item)
		}
	}
	secondOwner := ownerForRepair(root.ID, 2)
	if _, _, err := manager2.commitArchiveSegmentOwned(e2, &secondOwner, segment, archiveindex.ClaimHistorical, data, true); err == nil {
		t.Fatal("expected injected root save failure after claim shard publication")
	}
	if err := manager2.releaseRepairOwner(context.Background(), e2, secondOwner); err != nil {
		t.Fatal(err)
	}
	inventoryBeforeRetry, err := manager2.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range inventoryBeforeRetry.Segments {
		if item.Coordinate == coordinate {
			t.Fatalf("claim shard became visible before canonical root commit: %#v", item)
		}
	}
	if err := closeManager2(); err != nil {
		t.Fatal(err)
	}

	loadedAgain, err := store.LoadRecordingReadOnly(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	manager3, e3, closeManager3 := newRepairTestManager(t, store, fixture, loadedAgain, &repairOwnerFence{last: 2}, nil)
	thirdOwner := ownerForRepair(root.ID, 3)
	if _, _, err := manager3.commitArchiveSegmentOwned(e3, &thirdOwner, segment, archiveindex.ClaimHistorical, data, true); err != nil {
		t.Fatalf("idempotent post-restart provenance retry: %v", err)
	}
	committed, err := manager3.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	selected := segmentAtSequence(t, committed.Tracks["main"].Segments, 30)
	if selected.StoragePath != expectedPath || selected.SHA256 != sha256Hex(data) || selected.ArchiveOrdinal != 8 {
		t.Fatalf("retry changed selected immutable object: %#v", selected)
	}
	assertStoredPayload(t, store, root.ID, selected.StoragePath, data)
	if err := manager3.releaseRepairOwner(context.Background(), e3, thirdOwner); err != nil {
		t.Fatal(err)
	}
	inventory, err := manager3.ArchiveInventory(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, item := range inventory.Segments {
		if item.Coordinate != coordinate {
			continue
		}
		for _, claim := range item.Claims {
			if claim.ID == item.SelectedClaimID && claim.Source == archiveindex.ClaimHistorical && claim.SHA256 == selected.SHA256 {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("restart lost selected historical provenance: %#v", inventory)
	}
	if err := closeManager3(); err != nil {
		t.Fatal(err)
	}
}

func TestRepairDeclaredHistoryFillsMiddleGap(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	manifest := bytes.Replace(repairManifestFixture(), []byte("#EXT-X-GAP\nmissing.ts"), []byte("middle.ts"), 1)
	fixture := &repairFixtureTransport{manifest: manifest, objects: map[string][]byte{
		"/archive/prefix.m4v": []byte("prefix-for-middle-gap"),
		"/archive/middle.m4v": []byte("middle-gap-exact-bytes"),
		"/archive/init.m4v":   []byte("middle-gap-init"),
	}}
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	fence := &repairOwnerFence{}
	manager, _, _ := newRepairTestManager(t, store, fixture, root, fence, nil)
	owner := ownerForRepair(root.ID, 1)
	if err := manager.RepairDeclaredHistory(context.Background(), owner, root.ID); err != nil {
		t.Fatalf("middle-gap repair: %v", err)
	}
	repaired, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	track := repaired.Tracks["main"]
	if len(track.Segments) != 3 || segmentAtSequence(t, track.Segments, 10).TimelineOrdinal != 1 ||
		segmentAtSequence(t, track.Segments, 11).TimelineOrdinal != 2 || segmentAtSequence(t, track.Segments, 12).TimelineOrdinal != 3 {
		t.Fatalf("middle-gap timeline not rebuilt: %#v", track.Segments)
	}
	if segmentAtSequence(t, track.Segments, 11).ArchiveOrdinal != 9 || segmentAtSequence(t, track.Segments, 12).ArchiveOrdinal != 7 {
		t.Fatalf("append-only archive ordinals changed: %#v", track.Segments)
	}
	if len(repaired.Gaps) != 1 || repaired.Gaps[0].DiscontinuitySequence != 8 {
		t.Fatalf("repaired DS7 gaps or changed unrelated DS8 gap: %#v", repaired.Gaps)
	}
	assertStoredPayload(t, store, root.ID, segmentAtSequence(t, track.Segments, 11).StoragePath, []byte("middle-gap-exact-bytes"))
	if repaired.Duration() != 12 {
		t.Fatalf("VOD duration after gap repair = %v, want 12", repaired.Duration())
	}
}

func TestTransformedRequestURLRejectsUnsafeTargets(t *testing.T) {
	manager := &Manager{validate: func(context.Context, string) error { return nil }}
	media := repairMediaContextForTest()
	if _, err := manager.transformedRequestURL(context.Background(), media, media.ManifestURL, "file:///etc/passwd", adapterproto.RequestScopeMedia); err == nil {
		t.Fatal("non-HTTP transformed URL was accepted")
	}
}

func TestHistoricalRequestTransformUsesFetchedManifestBaseForRelativeMedia(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	media.ManifestURL = "https://media.example/archive/index.ts?hdnts=signed-value"
	media.HistoricalAvailability.HistoricalManifestURL = media.ManifestURL
	media.RequestPolicy.URLTransform.Rules[0].PathSuffix = &adapterproto.PathSuffixRewrite{From: ".ts", To: ".m3u8"}
	fixture := &repairFixtureTransport{
		manifest: []byte("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-DISCONTINUITY-SEQUENCE:7\n#EXTINF:4,\nrelative.ts\n#EXT-X-ENDLIST\n"),
		objects:  map[string][]byte{"/archive/relative.m4v": []byte("transformed-relative-media")},
	}
	root := newRepairTestRoot(t, store, domain.StateCompleted, media)
	validated := map[string]bool{}
	var validationMu sync.Mutex
	validate := func(_ context.Context, raw string) error {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil || parsed.Scheme != "https" || parsed.Host != "media.example" || parsed.User != nil {
			return errors.New("invalid fixture URL")
		}
		validationMu.Lock()
		validated[raw] = true
		validationMu.Unlock()
		return nil
	}
	manager, _, _ := newRepairTestManager(t, store, fixture, root, &repairOwnerFence{}, validate)
	if err := manager.RepairDeclaredHistory(context.Background(), ownerForRepair(root.ID, 1), root.ID); err != nil {
		t.Fatalf("declared-history repair: %v", err)
	}
	recording, err := manager.Get(root.ID)
	if err != nil {
		t.Fatal(err)
	}
	segment := segmentAtSequence(t, recording.Tracks["main"].Segments, 10)
	if segment.SourceURI != "https://media.example/archive/relative.ts" {
		t.Fatalf("relative segment resolved against raw instead of fetched manifest URL: %q", segment.SourceURI)
	}
	assertStoredPayload(t, store, root.ID, segment.StoragePath, []byte("transformed-relative-media"))
	requests := fixture.requestedURLs()
	want := map[string]string{
		"/archive/index.m3u8":   "signed-value",
		"/archive/relative.m4v": "signed-value",
	}
	seen := make(map[string]bool, len(want))
	for _, raw := range requests {
		parsed, parseErr := url.Parse(raw)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		if wantValue, ok := want[parsed.Path]; ok {
			if parsed.Query().Get("__bgda__") != wantValue {
				t.Errorf("request %q lacks propagated query value", raw)
			}
			seen[parsed.Path] = true
		}
		validationMu.Lock()
		wasValidated := validated[raw]
		validationMu.Unlock()
		if !wasValidated {
			t.Errorf("transformed request was not validated immediately before fetch: %q", raw)
		}
	}
	for path := range want {
		if !seen[path] {
			t.Errorf("expected transformed request path %q absent; requests=%v", path, requests)
		}
	}
}

func assertStoredPayload(t *testing.T, store *storage.Store, recordingID, objectPath string, want []byte) {
	t.Helper()
	got := mustLoadPayload(t, store, recordingID, objectPath)
	if !bytes.Equal(got, want) {
		t.Fatalf("payload %q = %q, want %q", objectPath, got, want)
	}
}

func mustLoadPayload(t *testing.T, store *storage.Store, recordingID, objectPath string) []byte {
	t.Helper()
	reader, err := store.OpenPayloadReader(recordingID, objectPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func segmentAtSequence(t *testing.T, segments []domain.Segment, sequence uint64) domain.Segment {
	t.Helper()
	for _, segment := range segments {
		if segment.Sequence == sequence {
			return segment
		}
	}
	t.Fatalf("segment sequence %d absent from %#v", sequence, segments)
	return domain.Segment{}
}

func assertTransformedRequest(t *testing.T, requests []string, manifestPath, suffix, queryName, queryValue string) {
	t.Helper()
	foundManifest, foundObject := false, false
	for _, raw := range requests {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Path == manifestPath {
			foundManifest = parsed.Query().Get(queryName) == queryValue
		}
		if strings.HasSuffix(parsed.Path, suffix) && (path.Base(parsed.Path) == "prefix.m4v" || path.Base(parsed.Path) == "init.m4v") {
			foundObject = foundObject || parsed.Query().Get(queryName) == queryValue
		}
	}
	if !foundManifest || !foundObject {
		t.Fatalf("transformed manifest/object requests missing: %v", requests)
	}
}
