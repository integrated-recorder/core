package acquire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

type refreshingResolver struct {
	initial adapterproto.MediaSource
	next    adapterproto.MediaSource
	calls   atomic.Int32
	refresh func(context.Context, adapterproto.MediaSource) (adapterproto.MediaSource, error)
}

func (r *refreshingResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return r.initial, nil
}

func (r *refreshingResolver) Refresh(ctx context.Context, _ string, _ *adapterproto.ResourceRef, current adapterproto.MediaSource) (adapterproto.MediaSource, error) {
	r.calls.Add(1)
	if r.refresh != nil {
		return r.refresh(ctx, current)
	}
	return r.next, nil
}

type preparedRefreshResolver struct {
	next      adapterproto.MediaSource
	committed atomic.Bool
}

func (r *preparedRefreshResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, nil
}

func (r *preparedRefreshResolver) PrepareRefresh(context.Context, string, *adapterproto.ResourceRef, adapterproto.MediaSource) (adapterproto.MediaSource, func() error, error) {
	return r.next, func() error { r.committed.Store(true); return nil }, nil
}

func TestValidateContentRangeStrictlyMatchesRequestedRange(t *testing.T) {
	want := domain.ByteRange{Offset: 100, Length: 20}
	for _, test := range []struct {
		value string
		valid bool
	}{
		{"bytes 100-119/200", true},
		{"bytes 100-119/120", true},
		{"bytes 100-119/119", false},
		{"bytes 100-119/200/extra", false},
		{"bytes 100-119/*", false},
		{"bytes 100-119/+200", false},
		{"bytes 100-120/200", false},
		{"bytes 101-120/200", false},
		{"bytes 100-119/200 extra", false},
		{"bytes  100-119/200", false},
		{"items 100-119/200", false},
		{"bytes -1-119/200", false},
		{"bytes 100-18446744073709551615/18446744073709551615", false},
	} {
		t.Run(test.value, func(t *testing.T) {
			err := validateContentRange(test.value, want)
			if (err == nil) != test.valid {
				t.Fatalf("validateContentRange(%q) error = %v, valid=%v", test.value, err, test.valid)
			}
			if err != nil && strings.Contains(err.Error(), test.value) {
				t.Fatalf("error echoed untrusted header: %v", err)
			}
		})
	}
	if err := validateContentRange("bytes 18446744073709551615-18446744073709551615/18446744073709551615", domain.ByteRange{Offset: ^uint64(0), Length: 1}); err == nil {
		t.Fatal("overflowing requested range was accepted")
	}
}

func TestSequenceResetUsesArchiveOrdinalAndDoesNotRedownloadLaterDuplicate(t *testing.T) {
	var polls atomic.Int32
	var segmentRequests atomic.Int32
	firstCaptured, secondCaptured := make(chan struct{}), make(chan struct{})
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			poll := polls.Add(1)
			sequence := "100"
			if poll > 1 {
				sequence = "0"
			}
			_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%s\n#EXTINF:1,title\nchunk.ts\n", sequence)
		case "/chunk.ts":
			request := segmentRequests.Add(1)
			if request == 1 {
				_, _ = w.Write([]byte{0x00, 0x10, 0xff})
				close(firstCaptured)
				return
			}
			_, _ = w.Write([]byte{0x80, 0x20, 0x01, 0xfe})
			if request == 2 {
				close(secondCaptured)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), fixedMediaResolver{media: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "sequence reset", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, firstCaptured, "first epoch segment")
	waitSignal(t, secondCaptured, "reset epoch segment")
	time.Sleep(650 * time.Millisecond) // allow one repeated sequence-zero poll
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := segmentRequests.Load(); got != 2 {
		t.Fatalf("segment requests = %d, want one per epoch", got)
	}
	segments := stopped.Tracks["main"].Segments
	if len(segments) != 2 {
		t.Fatalf("segments = %#v", segments)
	}
	if segments[0].SourceEpoch != 0 || segments[0].Sequence != 100 || segments[0].ArchiveOrdinal != 1 || segments[0].Discontinuity {
		t.Fatalf("first segment = %#v", segments[0])
	}
	if segments[1].SourceEpoch != 1 || segments[1].Sequence != 0 || segments[1].ArchiveOrdinal != 2 || !segments[1].Discontinuity {
		t.Fatalf("reset segment = %#v", segments[1])
	}
	wantBytes := [][]byte{{0x00, 0x10, 0xff}, {0x80, 0x20, 0x01, 0xfe}}
	for i, segment := range segments {
		stored, readErr := os.ReadFile(filepath.Join(store.Root(), "recordings", recording.ID, filepath.FromSlash(segment.StoragePath)))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(stored) != string(wantBytes[i]) {
			t.Fatalf("segment %d was transformed: %x != %x", i, stored, wantBytes[i])
		}
		digest := sha256.Sum256(wantBytes[i])
		if segment.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("segment %d hash = %s", i, segment.SHA256)
		}
	}
	reloaded, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	again, err := reloaded.Get(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := again.Tracks["main"].Segments; len(got) != 2 || got[0].ArchiveOrdinal != 1 || got[1].ArchiveOrdinal != 2 || got[1].SourceEpoch != 1 {
		t.Fatalf("reload order = %#v", got)
	}
}

func TestSegmentHTTPStatusRefreshReplacesSourceAndRetriesLatestPlaylist(t *testing.T) {
	const oldHeader = "Bearer old-proof"
	const newHeader = "Bearer fresh-proof"
	payload := []byte{0xff, 0x00, 0x43, 0x80}
	var oldManifest, oldSegment, newManifest, newSegment atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old.m3u8":
			oldManifest.Add(1)
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:4\n#EXTINF:1,old\nold.ts\n")
		case "/old.ts":
			oldSegment.Add(1)
			if r.Header.Get("Authorization") != oldHeader {
				http.Error(w, "old auth missing", http.StatusUnauthorized)
				return
			}
			http.Error(w, "expired", http.StatusForbidden)
		case "/new.m3u8":
			newManifest.Add(1)
			if r.Header.Get("Authorization") != newHeader {
				http.Error(w, "fresh auth missing", http.StatusUnauthorized)
				return
			}
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:4\n#EXTINF:1,new\nnew.ts\n")
		case "/new.ts":
			newSegment.Add(1)
			if r.Header.Get("Authorization") != newHeader {
				http.Error(w, "fresh auth missing", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	policy := &adapterproto.RefreshPolicy{OnHTTPStatus: []int{http.StatusForbidden}}
	requestPolicy := &adapterproto.RequestPolicy{HeaderForwarding: &adapterproto.HeaderForwardingPolicy{Mode: adapterproto.HeaderForwardingAllowlist, Origins: []string{server.URL}}}
	resolver := &refreshingResolver{
		initial: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/old.m3u8", Headers: map[string]string{"Authorization": oldHeader}, RefreshPolicy: policy},
		next:    adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8", Headers: map[string]string{"Authorization": newHeader}, RequestPolicy: requestPolicy, SessionRef: "session-two", Refresh: json.RawMessage(`{"generation":2}`), ArchivePolicy: &adapterproto.ArchivePolicy{SourceURI: "public"}},
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", resolver.initial, nil, "refresh fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForSegmentCount(t, manager, recording.ID, 1)
	if newSegment.Load() == 0 {
		t.Fatal("refreshed segment was not acquired")
	}
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls.Load() != 1 || oldManifest.Load() != 1 || oldSegment.Load() != 1 || newManifest.Load() != 1 || newSegment.Load() != 1 {
		t.Fatalf("refresh/request counts = refresh:%d old manifest:%d old segment:%d new manifest:%d new segment:%d", resolver.calls.Load(), oldManifest.Load(), oldSegment.Load(), newManifest.Load(), newSegment.Load())
	}
	if len(stopped.Tracks["main"].Segments) != 1 {
		t.Fatalf("captured segments = %#v", stopped.Tracks["main"].Segments)
	}
	stored, err := os.ReadFile(filepath.Join(store.Root(), "recordings", recording.ID, filepath.FromSlash(stopped.Tracks["main"].Segments[0].StoragePath)))
	if err != nil || string(stored) != string(payload) {
		t.Fatalf("refreshed bytes = %x, err=%v", stored, err)
	}
	entry, _ := manager.entry(recording.ID)
	entry.mu.Lock()
	current := cloneMediaSource(entry.media)
	entry.mu.Unlock()
	if current.ManifestURL != resolver.next.ManifestURL || current.Headers["Authorization"] != newHeader || current.SessionRef != "session-two" || string(current.Refresh) != string(resolver.next.Refresh) || current.SourceURIClassification() != "public" || current.RequestPolicy == nil {
		t.Fatalf("refresh was not atomically applied: %#v", current)
	}
}

func TestStaleManifestResponseIsDiscardedAfterSegmentRefresh(t *testing.T) {
	oldPoll2Started := make(chan struct{})
	releaseOldPoll2 := make(chan struct{})
	var releaseOnce sync.Once
	var oldManifests, oldSegments, staleSegments, newManifests, freshSegments atomic.Int32
	var oldPoll2Once sync.Once
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old.m3u8":
			if oldManifests.Add(1) == 1 {
				_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:4\n#EXTINF:1,initial\nold.ts\n")
				return
			}
			oldPoll2Once.Do(func() { close(oldPoll2Started) })
			select {
			case <-r.Context().Done():
				return
			case <-releaseOldPoll2:
			}
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:4\n#EXTINF:1,stale\nstale.ts\n")
		case "/old.ts":
			oldSegments.Add(1)
			http.Error(w, "expired", http.StatusForbidden)
		case "/new.m3u8":
			newManifests.Add(1)
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:4\n#EXTINF:1,fresh\nfresh.ts\n")
		case "/stale.ts":
			staleSegments.Add(1)
			_, _ = io.WriteString(w, "stale-payload")
		case "/fresh.ts":
			freshSegments.Add(1)
			_, _ = io.WriteString(w, "fresh-payload")
		default:
			http.NotFound(w, r)
		}
	}))
	defer func() {
		releaseOnce.Do(func() { close(releaseOldPoll2) })
		server.Close()
	}()

	refreshCommitted := make(chan struct{})
	var commitOnce sync.Once
	resolver := &refreshingResolver{
		initial: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/old.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{OnHTTPStatus: []int{http.StatusForbidden}}},
		next:    adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"},
	}
	resolver.refresh = func(ctx context.Context, _ adapterproto.MediaSource) (adapterproto.MediaSource, error) {
		select {
		case <-ctx.Done():
			return adapterproto.MediaSource{}, ctx.Err()
		case <-oldPoll2Started:
		}
		commitOnce.Do(func() { close(refreshCommitted) })
		return resolver.next, nil
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", resolver.initial, nil, "stale manifest", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, oldPoll2Started, "blocked old manifest request")
	waitSignal(t, refreshCommitted, "segment refresh result")
	waitForAwaitManifest(t, manager, recording.ID)
	releaseOnce.Do(func() { close(releaseOldPoll2) })
	waitForSegmentCount(t, manager, recording.ID, 1)
	stopped, err := manager.Stop(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls.Load() != 1 || oldManifests.Load() != 2 || oldSegments.Load() != 1 || newManifests.Load() != 1 || freshSegments.Load() != 1 || staleSegments.Load() != 0 {
		t.Fatalf("refresh=%d old manifests=%d old segments=%d fresh manifests=%d fresh segments=%d stale segments=%d", resolver.calls.Load(), oldManifests.Load(), oldSegments.Load(), newManifests.Load(), freshSegments.Load(), staleSegments.Load())
	}
	if len(stopped.Tracks["main"].Segments) != 1 || stopped.Tracks["main"].Segments[0].SourceURI != server.URL+"/fresh.ts" {
		t.Fatalf("recorded stale or missing source segment: %#v", stopped.Tracks["main"].Segments)
	}
}

func waitForAwaitManifest(t *testing.T, manager *Manager, id string) {
	t.Helper()
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording entry disappeared")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		scheduler := activeScheduler(e)
		if scheduler == nil {
			t.Fatal("segment scheduler is not active")
		}
		scheduler.mu.Lock()
		waiting := false
		for _, task := range scheduler.tasks {
			waiting = waiting || task.state == segmentTaskAwaitManifest
		}
		changed := scheduler.changed
		scheduler.mu.Unlock()
		if waiting {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("refreshed segment did not await the new manifest")
		case <-changed:
		}
	}
}

func TestRefreshSensitiveSourceClassificationIsDurablySticky(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	recording := &domain.Recording{
		FormatVersion:           1,
		ID:                      id,
		State:                   domain.StateRecording,
		SourceURIClassification: "public",
		Tracks:                  map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}},
	}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	resolver := &preparedRefreshResolver{next: adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://media.example/live.m3u8",
		ArchivePolicy: &adapterproto.ArchivePolicy{SourceURI: "sensitive"},
	}}
	manager := &Manager{store: store, resolver: resolver, validate: func(context.Context, string) error { return nil }}
	e := &entry{recording: recording, media: adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/old.m3u8", ArchivePolicy: &adapterproto.ArchivePolicy{SourceURI: "public"}}}
	if _, err = manager.refreshMedia(context.Background(), e, e.media); err != nil {
		t.Fatal(err)
	}
	if !resolver.committed.Load() {
		t.Fatal("adapter state was not committed after URI classification was persisted")
	}
	e.mu.Lock()
	stored := clone(e.recording)
	e.mu.Unlock()
	if stored == nil || stored.SourceURIClassification != "sensitive" {
		t.Fatalf("refreshed source classification = %#v", stored)
	}
}

func TestRefreshThenMissingSourceSegmentBecomesEpochScopedGap(t *testing.T) {
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:4\n#EXTINF:1,expired\nold.ts\n")
		case "/old.ts":
			http.Error(w, "expired", http.StatusForbidden)
		case "/new.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:5\n#EXTINF:1,next\nnext.ts\n")
		case "/next.ts":
			_, _ = io.WriteString(w, "next-original")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	resolver := &refreshingResolver{
		initial: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/old.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{OnHTTPStatus: []int{http.StatusForbidden}}},
		next:    adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/new.m3u8"},
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", resolver.initial, nil, "missing after refresh", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		current, _ := manager.Get(recording.ID)
		if len(current.Gaps) > 0 && current.SegmentCount() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	current, _ := manager.Get(recording.ID)
	if len(current.Gaps) != 1 || current.Gaps[0].SourceEpoch != 0 || current.Gaps[0].FromSequence != 4 || current.Gaps[0].ToSequence != 4 || current.SegmentCount() != 1 {
		t.Fatalf("failed pre-refresh media was not recorded as a gap: %#v", current)
	}
	if _, err = manager.Stop(recording.ID); err != nil {
		t.Fatal(err)
	}
}

func TestDeclaredStatusRefreshHasBoundedRetryCycles(t *testing.T) {
	var segmentRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:9\n#EXTINF:1,persistent failure\nsegment.ts\n")
			return
		}
		segmentRequests.Add(1)
		http.Error(w, "expired", http.StatusForbidden)
	}))
	defer server.Close()
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{OnHTTPStatus: []int{http.StatusForbidden}}}
	resolver := &refreshingResolver{initial: media, next: media}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", media, nil, "bounded refresh", nil)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := manager.entry(recording.ID)
	waitDone(t, e.done)
	if resolver.calls.Load() != maxRefreshCycles || segmentRequests.Load() != maxRefreshCycles+1 {
		t.Fatalf("refresh loop was not bounded: refreshes=%d segment requests=%d", resolver.calls.Load(), segmentRequests.Load())
	}
	current, err := manager.Get(recording.ID)
	if err != nil || current.State != domain.StateInterrupted || len(current.Gaps) != 1 || current.Gaps[0].FromSequence != 9 {
		t.Fatalf("bounded refresh terminal state = %#v, err=%v", current, err)
	}
}

func TestProactiveRefreshBeforeManifestFetch(t *testing.T) {
	var oldRequests, freshRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/expired.m3u8":
			oldRequests.Add(1)
			http.Error(w, "must not be fetched", http.StatusForbidden)
		case "/fresh.m3u8":
			freshRequests.Add(1)
			_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,final\nfinal.ts\n#EXT-X-ENDLIST\n")
		case "/final.ts":
			_, _ = w.Write([]byte("final-original-bytes"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	resolver := &refreshingResolver{
		initial: adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/expired.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{ExpiresAt: timePtr(time.Now().Add(-time.Minute)), RefreshBeforeSeconds: 30}},
		next:    adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/fresh.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{ExpiresAt: timePtr(time.Now().Add(5 * time.Minute)), RefreshBeforeSeconds: 30}},
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", resolver.initial, nil, "proactive refresh", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	var stopped *domain.Recording
	for time.Now().Before(deadline) {
		stopped, _ = manager.Get(recording.ID)
		if stopped != nil && stopped.State == domain.StateCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stopped == nil || stopped.State != domain.StateCompleted {
		t.Fatalf("recording state = %#v", stopped)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resolver.calls.Load() != 1 || oldRequests.Load() != 0 || freshRequests.Load() != 1 {
		t.Fatalf("proactive refresh counts = refresh:%d old:%d new:%d", resolver.calls.Load(), oldRequests.Load(), freshRequests.Load())
	}
}

func TestProactiveRefreshAtExpiryWithZeroLead(t *testing.T) {
	now := time.Now().UTC()
	media := adapterproto.MediaSource{RefreshPolicy: &adapterproto.RefreshPolicy{ExpiresAt: timePtr(now), RefreshBeforeSeconds: 0}}
	if due, key := proactiveRefreshDue(media, now); !due || key == "" {
		t.Fatalf("refresh at declared expiry = due %v, key %q", due, key)
	}
	if due, _ := proactiveRefreshDue(media, now.Add(-time.Second)); due {
		t.Fatal("source was refreshed before its zero-lead expiry")
	}
}

func TestProactiveRefreshWithStillDueExpiryIsBounded(t *testing.T) {
	var manifestRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		manifestRequests.Add(1)
		_, _ = fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n")
	}))
	defer server.Close()
	media := adapterproto.MediaSource{
		Type:        "hls",
		ManifestURL: server.URL + "/live.m3u8",
		RefreshPolicy: &adapterproto.RefreshPolicy{
			ExpiresAt:            timePtr(time.Now().Add(-time.Minute)),
			RefreshBeforeSeconds: 60,
		},
	}
	resolver := &refreshingResolver{
		initial: media,
		refresh: func(_ context.Context, current adapterproto.MediaSource) (adapterproto.MediaSource, error) {
			current.RefreshPolicy = &adapterproto.RefreshPolicy{
				ExpiresAt:            timePtr(time.Now().Add(30 * time.Second)),
				RefreshBeforeSeconds: 60,
			}
			return current, nil
		},
	}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	recording, err := manager.StartResolved(context.Background(), "fixture", media, nil, "bounded proactive refresh", nil)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := manager.entry(recording.ID)
	waitDone(t, e.done)
	current, err := manager.Get(recording.ID)
	if err != nil || current.State != domain.StateInterrupted || current.LastError != "recording acquisition failed" {
		t.Fatalf("proactive refresh terminal state = %#v, err=%v", current, err)
	}
	if got := resolver.calls.Load(); got != maxRefreshCycles {
		t.Fatalf("proactive refresh attempts=%d, want bounded %d", got, maxRefreshCycles)
	}
	if got := manifestRequests.Load(); got != 0 {
		t.Fatalf("manifest was fetched despite refresh remaining due (%d requests)", got)
	}
}

func TestRefreshRejectsSSRFBeforeCommittingAdapterState(t *testing.T) {
	resolver := &preparedRefreshResolver{next: adapterproto.MediaSource{Type: "hls", ManifestURL: "http://127.0.0.1:8123/live.m3u8?token=private"}}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, resolver, func(context.Context, string) error { return errors.New("private address") })
	if err != nil {
		t.Fatal(err)
	}
	current := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://public.example/live.m3u8"}
	e := &entry{media: current, adapterID: "fixture", done: closedChannel()}
	_, err = manager.refreshMedia(context.Background(), e, current)
	if err == nil || strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "private") {
		t.Fatalf("unsafe refresh error = %v", err)
	}
	if resolver.committed.Load() {
		t.Fatal("adapter state committed before Core SSRF validation")
	}
	if got := currentMedia(e); got.ManifestURL != current.ManifestURL {
		t.Fatalf("failed refresh changed current media: %#v", got)
	}
}

func TestUndeclaredHTTPStatusDoesNotRefreshAndErrorIsSanitized(t *testing.T) {
	const credential = "do-not-persist-this-token"
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "body-do-not-persist", http.StatusForbidden)
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8?token=" + credential}
	recording, err := manager.StartResolved(context.Background(), "fixture", media, nil, "safe error", nil)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := manager.entry(recording.ID)
	waitDone(t, e.done)
	stopped, stopErr := manager.Stop(recording.ID)
	if stopErr == nil || !strings.Contains(stopErr.Error(), "HTTP 403") || strings.Contains(stopErr.Error(), credential) {
		t.Fatalf("Stop error = %v", stopErr)
	}
	if stopped.LastError == "" || strings.Contains(stopped.LastError, credential) || strings.Contains(stopped.LastError, "body-do-not-persist") || strings.Contains(stopped.LastError, server.URL) {
		t.Fatalf("unsanitized LastError = %q", stopped.LastError)
	}
}

func TestManagerShutdownPersistsStoppedAndReloads(t *testing.T) {
	observed := make(chan struct{})
	var observedOnce sync.Once
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedOnce.Do(func() { close(observed) })
		<-r.Context().Done()
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "shutdown", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, observed, "active request")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	stopped, err := manager.Get(recording.ID)
	if err != nil || stopped.State != domain.StateStopped || stopped.StoppedAt == nil {
		t.Fatalf("shutdown state = %#v, err=%v", stopped, err)
	}
	if _, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "closed", nil); !errors.Is(err, errManagerClosed) {
		t.Fatalf("start after close error = %v", err)
	}
	reloaded, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	previous, err := reloaded.Get(recording.ID)
	if err != nil || previous.State != domain.StateStopped {
		t.Fatalf("reloaded shutdown state = %#v, err=%v", previous, err)
	}
}

func TestManagerCloseCancelsAllWorkersBeforeWaiting(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var cancelled [2]atomic.Bool
	for i := range cancelled {
		id := fmt.Sprintf("%032x", i+1)
		done := make(chan struct{})
		manager.entries[id] = &entry{
			recording: &domain.Recording{ID: id, State: domain.StateRecording},
			cancel:    func() { cancelled[i].Store(true) },
			done:      done,
		}
		defer close(done)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err = manager.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want deadline while workers remain unfinished", err)
	}
	for i := range cancelled {
		if !cancelled[i].Load() {
			t.Fatalf("worker %d was not canceled before Close waited", i)
		}
	}
}

func TestCancellationFinalizesUnattemptedManifestSegmentsAsGaps(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, &http.Client{}, emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	const id = "aabbccddeeff00112233445566778899"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	recording := &domain.Recording{ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	e := &entry{recording: recording}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = manager.process(e, ctx, hls.MediaPlaylist{TargetDuration: 1, Segments: []hls.MediaSegment{{Sequence: 20}, {Sequence: 21}}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("process cancellation error = %v", err)
	}
	if len(e.recording.Tracks["main"].PendingSegments) != 2 {
		t.Fatalf("unattempted segments were not retained as pending: %#v", e.recording.Tracks["main"].PendingSegments)
	}
	if err = manager.update(e, func(r *domain.Recording) error {
		manager.finalizePending(r, "recording stopped before pending media could be captured")
		r.State = domain.StateStopped
		now := time.Now().UTC()
		r.StoppedAt = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := e.recording.Gaps; len(got) != 2 || got[0].FromSequence != 20 || got[0].ToSequence != 20 || got[1].FromSequence != 21 || got[1].ToSequence != 21 || got[0].SourceEpoch != 0 || got[1].SourceEpoch != 0 {
		t.Fatalf("canceled manifest segments were not finalized as a gap: %#v", got)
	}
}

func TestStopReportsTerminalPersistenceFailureWithoutPublishingMutation(t *testing.T) {
	observed := make(chan struct{})
	var observedOnce sync.Once
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedOnce.Do(func() { close(observed) })
		<-r.Context().Done()
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "disk failure", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, observed, "active request")
	metadata := filepath.Join(store.Root(), "recordings", recording.ID, "recording.json")
	if err = os.Remove(metadata); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(metadata, 0700); err != nil {
		t.Fatal(err)
	}
	returned, stopErr := manager.Stop(recording.ID)
	if stopErr == nil || !strings.Contains(stopErr.Error(), "persistence failed") || strings.Contains(stopErr.Error(), store.Root()) {
		t.Fatalf("Stop did not report a sanitized persistence failure: %v", stopErr)
	}
	if returned == nil || returned.State != domain.StateRecording {
		t.Fatalf("failed transaction was published in memory: %#v", returned)
	}
	if closeErr := manager.Close(context.Background()); closeErr == nil || !strings.Contains(closeErr.Error(), "persistence failed") {
		t.Fatalf("Close did not collect terminal persistence failure: %v", closeErr)
	}
}

func TestMasterClassificationIgnoresTagTextInsideComment(t *testing.T) {
	payload := []byte{0x00, 0xff, 0x90}
	server := newIPv4Server(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			_, _ = fmt.Fprint(w, "#EXTM3U\n# explanatory comment mentions #EXT-X-STREAM-INF: but is not a tag\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,segment title\nseg.ts\n#EXT-X-ENDLIST\n")
		case "/seg.ts":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), emptyResolver{}, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "classifier", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, _ := manager.Get(recording.ID)
		if current.State == domain.StateCompleted {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	current, _ := manager.Get(recording.ID)
	if current.State != domain.StateCompleted || current.SegmentCount() != 1 {
		t.Fatalf("comment text misclassified as master: %#v", current)
	}
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func waitSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recording worker")
	}
}
