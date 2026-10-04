package acquire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

var (
	errOwnershipTestDenied = errors.New("test ownership denied")
	errOwnershipTestNested = errors.New("nested ownership fence call")
)

type ownershipTestFence struct {
	mu        sync.Mutex
	expected  OwnershipToken
	deny      bool
	calls     int
	callbacks int
	nested    int
	active    bool
}

func (f *ownershipTestFence) WithCommit(owner OwnershipToken, commit func() error) error {
	f.mu.Lock()
	f.calls++
	if f.active {
		f.nested++
		f.mu.Unlock()
		return errOwnershipTestNested
	}
	if owner != f.expected {
		f.mu.Unlock()
		return ErrInvalidOwnershipToken
	}
	if f.deny {
		f.mu.Unlock()
		return errOwnershipTestDenied
	}
	f.active = true
	f.callbacks++
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active = false
		f.mu.Unlock()
	}()
	return commit()
}

func (f *ownershipTestFence) WithUnownedCommit(_ string, commit func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active || f.deny {
		return errOwnershipTestDenied
	}
	f.callbacks++
	return commit()
}

func (f *ownershipTestFence) counts() (calls, callbacks, nested int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.callbacks, f.nested
}

type ownershipRoundTripper struct{}

func (ownershipRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString("#EXTM3U\n#EXT-X-TARGETDURATION:3600\n#EXT-X-MEDIA-SEQUENCE:1\n")),
		Request:    request,
	}, nil
}

func ownershipTestToken(id string) OwnershipToken {
	return OwnershipToken{RecordingID: id, EngineGeneration: "engine-a", WorkerInstance: "worker-1", Epoch: 4}
}

func newOwnershipTestManager(t *testing.T, fence *ownershipTestFence) (*Manager, *storage.Store) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{Transport: ownershipRoundTripper{}}, nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(fence); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close manager: %v", err)
		}
	})
	return manager, store
}

func ownershipTestRecording(id string) *domain.Recording {
	now := time.Now().UTC()
	return &domain.Recording{
		FormatVersion: 1,
		ID:            id,
		Title:         "ownership test",
		State:         domain.StateRecording,
		CreatedAt:     now,
		StartedAt:     now,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", NextArchiveOrdinal: 1, Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
	}
}

func TestConfiguredManagerRejectsUnownedStart(t *testing.T) {
	const id = "abcdef0123456789abcdef0123456789"
	fence := &ownershipTestFence{expected: ownershipTestToken(id)}
	manager, store := newOwnershipTestManager(t, fence)
	if _, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}, nil, "unowned", nil); !errors.Is(err, ErrOwnershipRequired) {
		t.Fatalf("unowned StartResolved error = %v, want %v", err, ErrOwnershipRequired)
	}
	if _, err := store.LoadRecordingReadOnly(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unowned start created an archive: err=%v", err)
	}
	calls, callbacks, _ := fence.counts()
	if calls != 0 || callbacks != 0 {
		t.Fatalf("unowned start reached fence: calls=%d callbacks=%d", calls, callbacks)
	}
}

func TestOwnedStartPersistsWithinFenceAndRetainsToken(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	owner := ownershipTestToken(id)
	fence := &ownershipTestFence{expected: owner}
	manager, _ := newOwnershipTestManager(t, fence)
	recording, err := manager.StartResolvedWithIDOwned(context.Background(), owner, id, "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}, nil, "owned", nil)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("owned start did not register an entry")
	}
	e.mu.Lock()
	gotOwner := e.ownership
	e.mu.Unlock()
	if gotOwner == nil || *gotOwner != owner {
		t.Fatalf("entry owner = %#v, want %#v", gotOwner, owner)
	}
	if recording.ID != id {
		t.Fatalf("recording id = %q, want %q", recording.ID, id)
	}
	calls, callbacks, _ := fence.counts()
	if calls != 1 || callbacks != 1 {
		t.Fatalf("initial creation fence calls=%d callbacks=%d, want 1 each", calls, callbacks)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := manager.StopContext(ctx, id); err != nil {
		t.Fatalf("stop owned recording: %v", err)
	}
}

func TestOwnedStartFenceDenialDoesNotCreateRecording(t *testing.T) {
	const id = "fedcba9876543210fedcba9876543210"
	owner := ownershipTestToken(id)
	fence := &ownershipTestFence{expected: owner, deny: true}
	manager, store := newOwnershipTestManager(t, fence)
	_, err := manager.StartResolvedWithIDOwned(context.Background(), owner, id, "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}, nil, "denied", nil)
	if err == nil {
		t.Fatal("owned start succeeded after fence denial")
	}
	calls, callbacks, _ := fence.counts()
	if calls != 1 || callbacks != 0 {
		t.Fatalf("denied start fence calls=%d callbacks=%d, want 1/0", calls, callbacks)
	}
	if _, err := store.LoadRecordingReadOnly(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("denied start published recording: err=%v", err)
	}
}

func TestCanonicalRootMutationDeniedWithoutArchiveWrite(t *testing.T) {
	const id = "a123456789abcdef0123456789abcdef"
	owner := ownershipTestToken(id)
	fence := &ownershipTestFence{expected: owner, deny: true}
	manager, store := newOwnershipTestManager(t, fence)
	root := ownershipTestRecording(id)
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: root, ownership: &owner}
	if err := manager.update(e, func(r *domain.Recording) error { r.LastError = "must not persist"; return nil }); !errors.Is(err, errOwnershipTestDenied) {
		t.Fatalf("fenced root update error = %v", err)
	}
	loaded, err := store.LoadRecordingReadOnly(id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LastError != "" {
		t.Fatalf("denied root update persisted LastError=%q", loaded.LastError)
	}
	if e.recording.LastError != "" {
		t.Fatalf("denied root update published in-memory LastError=%q", e.recording.LastError)
	}
	calls, callbacks, _ := fence.counts()
	if calls != 1 || callbacks != 0 {
		t.Fatalf("root mutation fence calls=%d callbacks=%d, want 1/0", calls, callbacks)
	}
}

func TestDeniedMetadataFenceSuppressesRootAndAdapterStateCommit(t *testing.T) {
	const id = "e123456789abcdef0123456789abcdef"
	owner := ownershipTestToken(id)
	fence := &ownershipTestFence{expected: owner, deny: true}
	manager, store := newOwnershipTestManager(t, fence)
	root := ownershipTestRecording(id)
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: root, ownership: &owner, mediaGeneration: 1}
	stateCommitCalled := false
	_, _, err := manager.commitMetadataObservation(e, 1, time.Now().UTC(), adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("new title")}}, func() error {
		stateCommitCalled = true
		return nil
	})
	if !errors.Is(err, errOwnershipTestDenied) {
		t.Fatalf("denied metadata observation error = %v", err)
	}
	if stateCommitCalled || len(e.recording.MetadataTimeline) != 0 {
		t.Fatalf("denied metadata observation committed state=%v timeline=%#v", stateCommitCalled, e.recording.MetadataTimeline)
	}
	loaded, err := store.LoadRecordingReadOnly(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.MetadataTimeline) != 0 {
		t.Fatalf("denied metadata observation persisted timeline=%#v", loaded.MetadataTimeline)
	}
	calls, callbacks, _ := fence.counts()
	if calls != 1 || callbacks != 0 {
		t.Fatalf("metadata fence calls=%d callbacks=%d, want 1/0", calls, callbacks)
	}
}

func TestFencedManagerRejectsLegacyDirectPayloadPersistence(t *testing.T) {
	const id = "f123456789abcdef0123456789abcdef"
	fence := &ownershipTestFence{expected: ownershipTestToken(id)}
	manager, store := newOwnershipTestManager(t, fence)
	root := ownershipTestRecording(id)
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.downloadObject(context.Background(), "https://media.example/segment.ts", nil, id, "tracks/main/00000000000000000001.ts", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}); !errors.Is(err, ErrDirectPersistRequiresQueue) {
		t.Fatalf("direct fenced download error = %v, want %v", err, ErrDirectPersistRequiresQueue)
	}
	if _, err := store.StatPayload(id, "tracks/main/00000000000000000001.ts"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("direct fenced download wrote payload: %v", err)
	}
}

func TestSegmentPayloadSidecarAndRootUseOneFence(t *testing.T) {
	const id = "b123456789abcdef0123456789abcdef"
	owner := ownershipTestToken(id)
	fence := &ownershipTestFence{expected: owner}
	manager, store := newOwnershipTestManager(t, fence)
	root := ownershipTestRecording(id)
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: root, ownership: &owner}
	scheduler := &segmentScheduler{manager: manager, e: e, epochMarkers: make(map[uint64]epochMarker)}
	segment := domain.Segment{
		ID: "segment-1", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1,
		SourceURI: "https://media.example/1.ts", Duration: 1,
		StoragePath: "tracks/main/00000000000000000001.ts",
	}
	payload := []byte("immutable-source-bytes")
	if _, err := scheduler.persistSegment(segment, nil, payload); err != nil {
		t.Fatal(err)
	}
	calls, callbacks, nested := fence.counts()
	if calls != 1 || callbacks != 1 || nested != 0 {
		t.Fatalf("segment transaction fence calls=%d callbacks=%d nested=%d, want 1/1/0", calls, callbacks, nested)
	}
	info, err := store.StatPayload(id, segment.StoragePath)
	if err != nil || info.Size != int64(len(payload)) {
		t.Fatalf("segment payload info=%#v err=%v", info, err)
	}
	loaded, err := store.LoadRecordingReadOnly(id)
	if err != nil || loaded.SegmentCount() != 1 || loaded.Tracks["main"].Segments[0].SHA256 == "" {
		t.Fatalf("segment root was not published: recording=%#v err=%v", loaded, err)
	}
}

func TestDeniedSegmentFenceSuppressesPayloadSidecarAndRoot(t *testing.T) {
	const id = "c123456789abcdef0123456789abcdef"
	owner := ownershipTestToken(id)
	fence := &ownershipTestFence{expected: owner, deny: true}
	manager, store := newOwnershipTestManager(t, fence)
	root := ownershipTestRecording(id)
	if err := store.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: root, ownership: &owner}
	scheduler := &segmentScheduler{manager: manager, e: e, epochMarkers: make(map[uint64]epochMarker)}
	segment := domain.Segment{ID: "segment-1", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1, SourceURI: "https://media.example/1.ts", StoragePath: "tracks/main/00000000000000000001.ts"}
	if _, err := scheduler.persistSegment(segment, nil, []byte("must-not-commit")); !errors.Is(err, errOwnershipTestDenied) {
		t.Fatalf("denied segment commit error = %v", err)
	}
	if _, err := store.StatPayload(id, segment.StoragePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("denied segment payload stat error = %v, want not-exist", err)
	}
	if loaded, loadErr := store.LoadRecordingReadOnly(id); loadErr != nil || loaded.SegmentCount() != 0 {
		t.Fatalf("denied segment changed root: recording=%#v err=%v", loaded, loadErr)
	}
	// The fence rejected the whole closure, so neither the adjacent sidecar nor
	// the root publication callback could have run.
	calls, callbacks, nested := fence.counts()
	if calls != 1 || callbacks != 0 || nested != 0 {
		t.Fatalf("denied segment fence calls=%d callbacks=%d nested=%d, want 1/0/0", calls, callbacks, nested)
	}
}

func TestLegacyNilFenceStartRemainsAvailable(t *testing.T) {
	const id = "d123456789abcdef0123456789abcdef"
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{Transport: ownershipRoundTripper{}}, nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	defer func() { _ = manager.Close(ctx) }()
	recording, err := manager.StartResolvedWithID(context.Background(), id, "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"}, nil, "legacy", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadRecordingReadOnly(id); err != nil {
		t.Fatalf("legacy unowned start did not create archive: %v", err)
	}
	if _, err := manager.StopContext(ctx, recording.ID); err != nil {
		t.Fatalf("stop legacy recording: %v", err)
	}
}
