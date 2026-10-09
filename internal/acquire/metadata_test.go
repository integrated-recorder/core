package acquire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/streammeta"
)

func TestMetadataObservationDeduplicatesAndPreservesKnownEmpty(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{store: store}
	e := &entry{recording: recording, mediaGeneration: 7}
	firstTitle, firstDescription := "A", "hello"
	sourceTime := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	first := adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &firstTitle, Description: &firstDescription}, SourceUpdatedAt: &sourceTime}
	committed := 0
	active, truncated, err := manager.commitMetadataObservation(e, 7, time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC), first, func() error { committed++; return nil })
	if err != nil || !active || truncated || committed != 1 || len(e.recording.MetadataTimeline) != 1 {
		t.Fatalf("first observation active=%v truncated=%v committed=%d timeline=%#v err=%v", active, truncated, committed, e.recording.MetadataTimeline, err)
	}
	if e.recording.ArchiveRevision != 1 {
		t.Fatalf("first canonical metadata change archive revision=%d, want 1", e.recording.ArchiveRevision)
	}
	// A source timestamp-only change is not a semantic content revision.
	sourceTime = sourceTime.Add(time.Minute)
	active, _, err = manager.commitMetadataObservation(e, 7, time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC), first, func() error { committed++; return nil })
	if err != nil || !active || committed != 2 || len(e.recording.MetadataTimeline) != 1 {
		t.Fatalf("duplicate observation timeline=%#v err=%v", e.recording.MetadataTimeline, err)
	}
	if e.recording.ArchiveRevision != 1 {
		t.Fatalf("duplicate metadata observation archive revision=%d, want unchanged 1", e.recording.ArchiveRevision)
	}
	empty := ""
	clearDescription := adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Description: &empty}}
	active, _, err = manager.commitMetadataObservation(e, 7, time.Date(2026, 9, 29, 12, 1, 0, 0, time.UTC), clearDescription, nil)
	if err != nil || !active || len(e.recording.MetadataTimeline) != 2 {
		t.Fatalf("description clear timeline=%#v err=%v", e.recording.MetadataTimeline, err)
	}
	if e.recording.ArchiveRevision != 2 {
		t.Fatalf("second canonical metadata change archive revision=%d, want 2", e.recording.ArchiveRevision)
	}
	last := e.recording.MetadataTimeline[1]
	if last.Title == nil || *last.Title != "A" || last.Description == nil || *last.Description != "" {
		t.Fatalf("partial clear did not retain title / known empty description: %#v", last)
	}
	unknown := adapterproto.MetadataResult{}
	active, _, err = manager.commitMetadataObservation(e, 7, time.Date(2026, 9, 29, 12, 2, 0, 0, time.UTC), unknown, nil)
	if err != nil || !active || len(e.recording.MetadataTimeline) != 2 || e.recording.MetadataTimeline[1].Description == nil || *e.recording.MetadataTimeline[1].Description != "" {
		t.Fatalf("unknown field was treated as a clear: %#v err=%v", e.recording.MetadataTimeline, err)
	}

	staleCommitCalled := false
	e.mediaGeneration++
	active, _, err = manager.commitMetadataObservation(e, 7, time.Date(2026, 9, 29, 12, 3, 0, 0, time.UTC), adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("stale")}}, func() error { staleCommitCalled = true; return nil })
	if err != nil || active || staleCommitCalled || len(e.recording.MetadataTimeline) != 2 {
		t.Fatalf("stale generation applied metadata/state: active=%v commit=%v timeline=%#v err=%v", active, staleCommitCalled, e.recording.MetadataTimeline, err)
	}

	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || len(loaded[0].MetadataTimeline) != 2 || loaded[0].MetadataTimeline[1].Description == nil || *loaded[0].MetadataTimeline[1].Description != "" {
		t.Fatalf("metadata did not survive archive reload: %#v err=%v", loaded, err)
	}
}

func TestShardedMetadataObservationAdvancesArchiveRevisionOncePerSemanticChange(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "acdef0123456789abcdef0123456789a"
	identity, err := archiveindex.NewSessionIdentity(id, "fixture", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	recording := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion, ShardedArchive: &domain.ShardedArchiveSummary{},
		ID: id, SourceSessionID: identity.ID, State: domain.StateRecording, CreatedAt: now, StartedAt: now,
		ArchiveRevision: 1, Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}},
	}
	if err := store.CreateShardedRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{store: store}
	e := &entry{recording: recording, mediaGeneration: 3}
	firstTitle := "first"
	first := adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &firstTitle}}
	if active, _, err := manager.commitMetadataObservation(e, 3, now, first, nil); err != nil || !active {
		t.Fatalf("first V2 metadata observation active=%v err=%v", active, err)
	}
	if e.recording.ArchiveRevision != 2 {
		t.Fatalf("first V2 metadata revision=%d, want 2", e.recording.ArchiveRevision)
	}
	if active, _, err := manager.commitMetadataObservation(e, 3, now.Add(time.Second), first, nil); err != nil || !active {
		t.Fatalf("duplicate V2 metadata observation active=%v err=%v", active, err)
	}
	if e.recording.ArchiveRevision != 2 {
		t.Fatalf("duplicate V2 metadata revision=%d, want unchanged 2", e.recording.ArchiveRevision)
	}
	secondTitle := "second"
	second := adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &secondTitle}}
	if active, _, err := manager.commitMetadataObservation(e, 3, now.Add(2*time.Second), second, nil); err != nil || !active {
		t.Fatalf("changed V2 metadata observation active=%v err=%v", active, err)
	}
	header, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil || header.ArchiveRevision != 3 || header.ShardedArchive.MetadataRevisionCount != 2 {
		t.Fatalf("V2 metadata root revision=%d count=%d err=%v, want revision 3/count 2", header.ArchiveRevision, header.ShardedArchive.MetadataRevisionCount, err)
	}
}

func TestMetadataTimelineBoundIsExplicitAndStopsFurtherObservation(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	title := "same known title"
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{}, MetadataTimeline: make([]domain.MetadataRevision, streammeta.MaxTimelineRevisions)}
	for i := range recording.MetadataTimeline {
		recording.MetadataTimeline[i] = domain.MetadataRevision{ObservedAt: time.Date(2026, 9, 1, 0, i%60, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute), Title: &title}
	}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{store: store}
	e := &entry{recording: recording, mediaGeneration: 1}
	active, truncated, err := manager.commitMetadataObservation(e, 1, time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC), adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("changed title")}}, nil)
	if err != nil || !active || !truncated || !e.recording.MetadataTimelineTruncated || len(e.recording.MetadataTimeline) != streammeta.MaxTimelineRevisions {
		t.Fatalf("bound transition active=%v truncated=%v flag=%v count=%d err=%v", active, truncated, e.recording.MetadataTimelineTruncated, len(e.recording.MetadataTimeline), err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || !loaded[0].MetadataTimelineTruncated || len(loaded[0].MetadataTimeline) != streammeta.MaxTimelineRevisions {
		t.Fatalf("explicit truncation was not durable: count=%d truncated=%v err=%v", len(loaded[0].MetadataTimeline), loaded[0].MetadataTimelineTruncated, err)
	}
}

func TestMetadataObservedAtIsCapturedBeforeCanonicalLockWait(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, 9, 29, 12, 34, 56, 0, time.UTC)
	clockCalled := make(chan struct{})
	manager := &Manager{
		store: store, metadataPollInterval: time.Hour,
		metadataClock: func() time.Time { close(clockCalled); return observedAt },
		resolver:      &metadataSequenceResolver{results: []adapterproto.MetadataResult{{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("received")}}}, calls: make(chan int, 1)},
	}
	e := &entry{recording: recording, mediaGeneration: 1, media: adapterproto.MediaSource{Type: "hls", ManifestURL: "https://example.invalid/live.m3u8"}}
	e.persistMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); manager.runMetadataMonitor(ctx, e) }()
	select {
	case <-clockCalled:
	case <-time.After(time.Second):
		e.persistMu.Unlock()
		cancel()
		<-done
		t.Fatal("metadata observation did not reach Core validation")
	}
	// The monitor has captured the receipt timestamp but is unable to enter the
	// serialized canonical mutation until this lock is released.
	e.persistMu.Unlock()
	deadline := time.After(time.Second)
	for {
		e.mu.Lock()
		ready := len(e.recording.MetadataTimeline) > 0
		e.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("metadata revision was not persisted")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-done
	if got := e.recording.MetadataTimeline[0].ObservedAt; !got.Equal(observedAt) {
		t.Fatalf("observed_at shifted while waiting for persistence: got %s want %s", got, observedAt)
	}
}

func TestMetadataMonitorObservesChangesWithoutChangingRecordingTitle(t *testing.T) {
	var segmentRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live.m3u8":
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1.0,\nsegment.ts\n"))
		case "/segment.ts":
			segmentRequests++
			_, _ = w.Write([]byte("original segment bytes"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	responses := []adapterproto.MetadataResult{
		{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("A"), Description: ptrMetadataString("hello")}},
		{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("A"), Description: ptrMetadataString("hello")}, SourceUpdatedAt: metadataTime(1)},
		{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("B"), Description: ptrMetadataString("")}},
		{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("B")}},
		{Metadata: adapterproto.MetadataResult{}.Metadata},
	}
	resolver := &metadataSequenceResolver{results: responses, calls: make(chan int, 16)}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	manager.metadataPollInterval = 8 * time.Millisecond
	started, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "User chosen recording title", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close(context.Background()) }()
	for i := 0; i < 5; i++ {
		select {
		case <-resolver.calls:
		case <-time.After(3 * time.Second):
			t.Fatalf("metadata observation %d did not arrive", i+1)
		}
	}
	terminal, err := manager.Stop(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != domain.StateStopped || terminal.Title != "User chosen recording title" || terminal.SegmentCount() == 0 || len(terminal.Gaps) != 0 {
		t.Fatalf("metadata monitoring altered recording/acquisition semantics: state=%s title=%q segments=%d gaps=%#v", terminal.State, terminal.Title, terminal.SegmentCount(), terminal.Gaps)
	}
	if len(terminal.MetadataTimeline) != 2 {
		t.Fatalf("semantic duplicate observations were stored: %#v", terminal.MetadataTimeline)
	}
	if terminal.MetadataTimeline[0].Title == nil || *terminal.MetadataTimeline[0].Title != "A" || terminal.MetadataTimeline[0].Description == nil || *terminal.MetadataTimeline[0].Description != "hello" {
		t.Fatalf("initial metadata revision = %#v", terminal.MetadataTimeline[0])
	}
	if terminal.MetadataTimeline[1].Title == nil || *terminal.MetadataTimeline[1].Title != "B" || terminal.MetadataTimeline[1].Description == nil || *terminal.MetadataTimeline[1].Description != "" {
		t.Fatalf("changed metadata revision = %#v", terminal.MetadataTimeline[1])
	}
	if segmentRequests == 0 {
		t.Fatal("the canonical source segment was not acquired")
	}
}

func TestMetadataFailureIsBestEffortAndMonitorJoinsOnStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/live.m3u8" {
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1.0,\nsegment.ts\n"))
			return
		}
		_, _ = w.Write([]byte("source"))
	}))
	defer server.Close()
	resolver := &metadataSequenceResolver{failures: 1, unsupportedOnFailure: true, results: []adapterproto.MetadataResult{{Metadata: adapterproto.StreamMetadata{Title: ptrMetadataString("recovered")}}}, calls: make(chan int, 8)}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, server.Client(), resolver, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	manager.metadataPollInterval = 5 * time.Millisecond
	started, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + "/live.m3u8"}, nil, "title", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close(context.Background()) }()
	for i := 0; i < 2; i++ {
		select {
		case <-resolver.calls:
		case <-time.After(3 * time.Second):
			t.Fatal("metadata error was not retried with bounded delay")
		}
	}
	// Metadata polling and media acquisition are independent workers. Wait for
	// the source segment's canonical commit before stopping so this test checks
	// failure isolation rather than scheduler timing under the race detector.
	waitForSegmentCount(t, manager, started.ID, 1)
	terminal, err := manager.Stop(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != domain.StateStopped || terminal.SegmentCount() == 0 || len(terminal.Gaps) != 0 || len(terminal.MetadataTimeline) != 1 {
		t.Fatalf("metadata failure affected capture: state=%s segments=%d gaps=%#v timeline=%#v", terminal.State, terminal.SegmentCount(), terminal.Gaps, terminal.MetadataTimeline)
	}
	select {
	case <-manager.entries[started.ID].done:
	default:
		t.Fatal("recording worker/metadata monitor was not joined by Stop")
	}
}

type metadataSequenceResolver struct {
	mu                   sync.Mutex
	results              []adapterproto.MetadataResult
	failures             int
	unsupportedOnFailure bool
	count                int
	calls                chan int
}

func (r *metadataSequenceResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, errors.New("not used")
}

func (r *metadataSequenceResolver) PrepareMetadata(_ context.Context, _ string, _ *adapterproto.ResourceRef, _ adapterproto.MediaSource) (adapterproto.MetadataResult, func() error, bool, error) {
	r.mu.Lock()
	r.count++
	count := r.count
	failure := count <= r.failures
	index := count - r.failures - 1
	var result adapterproto.MetadataResult
	if !failure && len(r.results) > 0 {
		if index >= len(r.results) {
			index = len(r.results) - 1
		}
		result = r.results[index]
	}
	r.mu.Unlock()
	select {
	case r.calls <- count:
	default:
	}
	if failure {
		return adapterproto.MetadataResult{}, nil, !r.unsupportedOnFailure, errors.New("fixture observation failed")
	}
	return result, func() error { return nil }, true, nil
}

func ptrMetadataString(value string) *string { return &value }
func metadataTime(seconds int) *time.Time {
	value := time.Date(2026, 9, 29, 12, 0, seconds, 0, time.UTC)
	return &value
}
