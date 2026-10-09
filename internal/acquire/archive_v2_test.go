package acquire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestShardedRecoveryStreamsBeyondLegacyArchiveLimit(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "74e2f247ab144c6a850fd5c07fb1e6a1"
	identity, err := archiveindex.NewSessionIdentity(id, "fixture", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	header := &domain.Recording{
		FormatVersion:  storage.ShardedArchiveFormatVersion,
		ShardedArchive: &domain.ShardedArchiveSummary{},
		ID:             id, SourceSessionID: identity.ID, Title: "large sharded recovery fixture", AdapterID: "fixture",
		State: domain.StateInterrupted, CreatedAt: now, StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {
			ID: "main", SourcePlaylistURL: "https://source.invalid/archive.m3u8", NextArchiveOrdinal: 1,
		}},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}

	// Legacy Inventory rejects more than archiveindex.MaxSegments. Build valid
	// v2 pages directly so this scale regression stays fast and deterministic.
	type mediaPage struct {
		Version int                     `json:"version"`
		TrackID string                  `json:"track_id"`
		Number  uint64                  `json:"number"`
		Entries []storage.V2MediaRecord `json:"entries"`
	}
	trackDigest := sha256.Sum256([]byte("main"))
	trackKey := hex.EncodeToString(trackDigest[:16])
	count := uint64(archiveindex.MaxSegments + 1)
	for pageNo := uint64(0); pageNo*storage.MediaShardMaxEntries < count; pageNo++ {
		page := mediaPage{Version: 1, TrackID: "main", Number: pageNo, Entries: make([]storage.V2MediaRecord, 0, storage.MediaShardMaxEntries)}
		start := pageNo*storage.MediaShardMaxEntries + 1
		end := min(start+storage.MediaShardMaxEntries-1, count)
		for ordinal := start; ordinal <= end; ordinal++ {
			segmentID := fmt.Sprintf("seg-%020d", ordinal)
			segment := domain.Segment{
				ID: segmentID, TrackID: "main", Sequence: ordinal,
				StoragePath: "payload/" + segmentID + ".ts", PayloadSize: 1,
				SHA256:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Duration: 1, ArchiveOrdinal: ordinal,
			}
			coordinate := archiveindex.Coordinate{
				SessionID: identity.ID, TrackID: "main", Sequence: ordinal,
				Kind: archiveindex.ObjectMedia,
			}
			page.Entries = append(page.Entries, storage.V2MediaRecord{Coordinate: coordinate, Segment: segment, IndexOrdinal: ordinal})
		}
		relative := fmt.Sprintf("archive/v2/media/%s/%020d", trackKey, pageNo)
		if err := store.StorageBackend.SaveSidecar(id, relative, page); err != nil {
			t.Fatalf("save media page %d: %v", pageNo, err)
		}
	}
	header.Tracks["main"].MediaCount = count
	header.Tracks["main"].MediaHighWater = count
	header.Tracks["main"].NextArchiveOrdinal = count + 1
	header.Tracks["main"].PayloadBytes = count
	header.Tracks["main"].DurationSeconds = float64(count)
	header.ShardedArchive.MediaCount = count
	header.ShardedArchive.PayloadBytes = count
	header.ShardedArchive.DurationSeconds = float64(count)
	if err := store.SaveRecordingHeader(context.Background(), header); err != nil {
		t.Fatal(err)
	}

	manager := &Manager{store: store}
	target := hls.MediaSegment{Sequence: archiveindex.MaxSegments + 1}
	epochs, states, err := manager.resolveShardedRecoveryCoordinates(
		context.Background(), id, header.Tracks["main"], []hls.MediaSegment{target}, []bool{true},
	)
	if err != nil {
		t.Fatalf("resolve recovery coordinates beyond legacy limit: %v", err)
	}
	if len(epochs) != 1 || epochs[0] != 0 || len(states) != 1 || states[0] != archiveindex.CoveragePresent {
		t.Fatalf("unexpected recovery result beyond legacy limit: epochs=%v states=%v", epochs, states)
	}
	if _, err := manager.ArchiveInventory(id); !errors.Is(err, ErrArchiveIndexLimit) {
		t.Fatalf("cold archive inventory error=%v, want explicit bounded projection limit", err)
	}
}

func TestShardedHistoricalRepairFillsReservedLiveGapSlot(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, &http.Client{}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	id := "84e2f247ab144c6a850fd5c07fb1e6a1"
	identity, err := archiveindex.NewSessionIdentity(id, "fixture", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	root := &domain.Recording{
		FormatVersion:  storage.ShardedArchiveFormatVersion,
		ShardedArchive: &domain.ShardedArchiveSummary{},
		ID:             id, SourceSessionID: identity.ID, Title: "sharded gap fixture", AdapterID: "fixture",
		State: domain.StateRecording, CreatedAt: now, StartedAt: now, ArchiveRevision: 1,
		Tracks: map[string]*domain.Track{"main": {
			ID: "main", SourcePlaylistURL: "https://source.invalid/live.m3u8", NextArchiveOrdinal: 1,
			LivePresentation: &domain.LivePresentationState{NextOrdinal: 1},
		}},
	}
	if err := store.CreateShardedRecording(root); err != nil {
		t.Fatal(err)
	}
	entry := &entry{recording: root, done: closedChannel(), livePlayback: newLivePlaybackProjection()}
	manager.mu.Lock()
	manager.entries[id] = entry
	manager.mu.Unlock()
	closeManager := func() {
		t.Helper()
		entry.mu.Lock()
		entry.recording.State = domain.StateInterrupted
		entry.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close fixture manager: %v", err)
		}
	}
	defer closeManager()

	for sequence := uint64(100); sequence <= 105; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	ordinal, discontinuitySequence, boundary, ok := livePresentationForCoordinate(entry.recording.Tracks["main"], 0, 0, 106, false)
	if !ok || ordinal == 0 {
		t.Fatal("missing live coordinate has no reserved presentation slot")
	}
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 0, FromSequence: 106, ToSequence: 106,
		DetectedAt: time.Now().UTC(), Reason: "fixture gap", LivePresentationOrdinal: ordinal,
		LiveDiscontinuity: boundary, LiveDiscontinuitySequence: discontinuitySequence,
	}
	if err := store.AppendShardedGap(context.Background(), id, gap); err != nil {
		t.Fatal(err)
	}
	if err := manager.refreshShardedHeader(entry.recording); err != nil {
		t.Fatal(err)
	}
	if err := manager.loadShardedRuntimeTail(entry.recording); err != nil {
		t.Fatal(err)
	}
	entry.livePlayback = buildLivePlaybackProjection(entry.recording)
	if len(entry.recording.Gaps) != 1 || entry.recording.Gaps[0].LivePresentationOrdinal != ordinal {
		t.Fatalf("restart tail lost reserved gap slot: %#v", entry.recording.Gaps)
	}
	for sequence := uint64(107); sequence <= 113; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}

	prefix := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, 90, "")
	if prefix.LivePresentationOrdinal != 0 {
		t.Fatalf("historical prefix got live ordinal %d", prefix.LivePresentationOrdinal)
	}
	repaired := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, 106, "")
	if repaired.LivePresentationOrdinal != ordinal || repaired.LiveDiscontinuitySequence != discontinuitySequence {
		t.Fatalf("historical gap repair lost reserved live identity: got %#v, want ordinal %d and discontinuity sequence %d", repaired, ordinal, discontinuitySequence)
	}

	view, err := manager.LivePlaybackSnapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range view.Slots {
		if slot.Ordinal == ordinal {
			if slot.Segment == nil || slot.Segment.Sequence != 106 || slot.Unavailable {
				t.Fatalf("reserved live slot not replaced by repaired media: %#v", slot)
			}
			return
		}
	}
	t.Fatalf("live snapshot omitted repaired slot %d: %#v", ordinal, view.Slots)
}

func TestShardedHistoricalPrefixDoesNotAdvanceLiveCursor(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)

	for sequence := uint64(100); sequence < 104; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	before := entry.recording.Tracks["main"].LivePresentation.NextOrdinal
	prefix := commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, 90, "")
	after := entry.recording.Tracks["main"].LivePresentation.NextOrdinal
	if prefix.LivePresentationOrdinal != 0 || after != before {
		t.Fatalf("historical prefix changed live cursor: ordinal=%d next=%d, want 0 and %d", prefix.LivePresentationOrdinal, after, before)
	}
	if prefix.ID != fmt.Sprintf("seg-%020d", prefix.ArchiveOrdinal) {
		t.Fatalf("unexpected canonical archive identity: %#v", prefix)
	}
}

func TestShardedGapPollingDoesNotAppendDuplicateObservations(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 2, DiscontinuitySequence: 3,
		FromSequence: 40, ToSequence: 42, DetectedAt: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC),
		Reason: "poll gap",
	}
	for i := 0; i < 2; i++ {
		var previous []domain.Gap
		if i == 1 {
			previous = []domain.Gap{gap}
			entry.mu.Lock()
			entry.shardedGapObservations = nil
			entry.mu.Unlock()
		}
		if err := manager.appendShardedGapsSince(entry.recording.ID, previous, []domain.Gap{gap}); err != nil {
			t.Fatal(err)
		}
	}
	header, err := manager.store.LoadRecordingHeader(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if count := header.ShardedArchive.GapCount; count != 1 {
		t.Fatalf("gap observation count=%d, want 1 after repeated poll", count)
	}
	count := 0
	if err := manager.store.IterateShardedGaps(context.Background(), entry.recording.ID, func(domain.Gap) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("iterated %d gap observations, want 1", count)
	}
}

func TestShardedHeaderRefreshPreservesLiveInitReferences(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	initURI := "https://source.invalid/init-one.mp4"
	initID := initSegmentID(hls.Map{URI: initURI}, 0, 0)
	initSegment := domain.Segment{ID: initID, TrackID: "main", SourceURI: initURI, Sequence: 100, IsInit: true}
	if _, _, err := manager.commitArchiveSegment(entry, nil, initSegment, archiveindex.ClaimLiveOrigin, []byte("init payload")); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(100); sequence < 103; sequence++ {
		segment := domain.Segment{TrackID: "main", Sequence: sequence, SourceURI: fmt.Sprintf("https://source.invalid/%d.ts", sequence), Duration: 1, InitSegmentID: initID}
		if _, _, err := manager.commitArchiveSegment(entry, nil, segment, archiveindex.ClaimLiveOrigin, []byte(fmt.Sprintf("payload-%d", sequence))); err != nil {
			t.Fatalf("commit media %d: %v", sequence, err)
		}
	}
	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(entry.recording.Tracks["main"].InitSegments); got != 1 {
		t.Fatalf("runtime init tail has %d entries, want 1", got)
	}
	if got := len(view.InitSegments); got != 1 {
		t.Fatalf("live snapshot init map has %d entries, want 1", got)
	}
	init, ok := view.InitSegments[initID]
	if !ok {
		t.Fatalf("live snapshot omitted referenced init %q", initID)
	}
	for _, object := range append(append([]domain.Segment(nil), view.Segments...), init) {
		info, statErr := manager.store.StatPayload(entry.recording.ID, object.StoragePath)
		if statErr != nil || !info.Regular || info.Size != object.PayloadSize {
			t.Fatalf("live snapshot payload %q unavailable: info=%#v err=%v", object.ID, info, statErr)
		}
	}
}

func TestShardedInitMapReuseAfterTailEvictionSurvivesRestart(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)

	const originalSequence = uint64(100)
	reusedURI := "https://source.invalid/shared-init.mp4"
	reusedID := initSegmentID(hls.Map{URI: reusedURI}, 0, 0)
	for index := 0; index < LivePlaybackWindowSize+1; index++ {
		uri := fmt.Sprintf("https://source.invalid/init-%02d.mp4", index)
		if index == 0 {
			uri = reusedURI
		}
		sequence := originalSequence + uint64(index)
		segment := domain.Segment{
			ID: initSegmentID(hls.Map{URI: uri}, 0, 0), TrackID: "main",
			Sequence: sequence, SourceURI: uri, IsInit: true,
		}
		if _, _, err := manager.commitArchiveSegment(entry, nil, segment, archiveindex.ClaimLiveOrigin, []byte(fmt.Sprintf("init-payload-%d", index))); err != nil {
			t.Fatalf("commit init map %d: %v", index, err)
		}
	}
	if initExists(entry.recording, reusedID) {
		t.Fatal("old init map remains in bounded runtime tail; fixture did not exercise persisted reuse")
	}
	header, err := manager.store.LoadRecordingHeader(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := header.Tracks["main"].InitCount; got != LivePlaybackWindowSize+1 {
		t.Fatalf("persisted init count=%d, want %d", got, LivePlaybackWindowSize+1)
	}

	// A fresh generation sees only the bounded root. Re-observing the original
	// map at a later sequence must resolve its durable ID pointer, not download
	// or attempt to publish the same ID at a second source coordinate.
	restarted, err := NewManagerWithMode(manager.store, &http.Client{}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := restarted.Close(ctx); err != nil {
			t.Errorf("close restarted manager: %v", err)
		}
	})
	restartedHeader, err := restarted.store.LoadRecordingReadOnly(entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	restarted.client = &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("unexpected download")), Request: request}, nil
	})}
	scheduler := &segmentScheduler{
		manager: restarted, e: newShardedRuntimeEntry(restartedHeader), ctx: context.Background(),
		init: make(map[string]*initFlight), changed: make(chan struct{}),
	}
	source := hls.Map{URI: reusedURI}
	id, flight, err := scheduler.acquireInitUsingPayload(hls.MediaSegment{Sequence: 999, Init: &source}, 0, adapterproto.MediaSource{Type: "hls", ManifestURL: "https://source.invalid/live.m3u8"}, 0, nil)
	if err != nil {
		t.Fatalf("reuse persisted init map after restart: %v", err)
	}
	if id != reusedID || flight != nil {
		t.Fatalf("reused init result id=%q flight=%v, want existing id %q and no write", id, flight != nil, reusedID)
	}
	if got := requests; got != 0 {
		t.Fatalf("re-observed persisted map caused %d source requests, want 0", got)
	}
	stored, err := restarted.store.LookupShardedMediaByID(context.Background(), entry.recording.ID, reusedID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Segment.Sequence != originalSequence || stored.Segment.ID != reusedID {
		t.Fatalf("stable init identity changed: sequence=%d ID=%q", stored.Segment.Sequence, stored.Segment.ID)
	}
	after, err := restarted.store.LoadRecordingHeader(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := after.Tracks["main"].InitCount; got != LivePlaybackWindowSize+1 {
		t.Fatalf("init count changed after duplicate map observation: got %d, want %d", got, LivePlaybackWindowSize+1)
	}
}

func TestShardedLivePresentationSourceResetAndDiscontinuityStayMonotonic(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)

	for _, coordinate := range []struct {
		epoch, discontinuity, sequence uint64
		boundary                       bool
	}{
		{2, 4, 500, false},
		{2, 4, 501, false},
		{2, 5, 502, true},
		{3, 0, 0, false},
		{3, 0, 1, false},
	} {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin,
			coordinate.epoch, coordinate.discontinuity, coordinate.sequence, "", coordinate.boundary)
	}
	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 5 {
		t.Fatalf("V2 live segment count=%d, want 5", len(view.Segments))
	}
	for index, segment := range view.Segments {
		if segment.LivePresentationOrdinal != uint64(index+1) {
			t.Errorf("V2 presentation ordinal[%d]=%d, want %d", index, segment.LivePresentationOrdinal, index+1)
		}
	}
	if !view.Segments[2].LiveDiscontinuity || !view.Segments[3].LiveDiscontinuity || view.Segments[4].LiveDiscontinuity {
		t.Fatalf("V2 source discontinuity/reset boundaries changed: %#v", view.Segments)
	}
	wantDiscontinuities := []uint64{0, 1, 1, 2, 3}
	for index, want := range wantDiscontinuities {
		if got := view.Segments[index].LiveDiscontinuitySequence; got != want {
			t.Errorf("V2 discontinuity sequence[%d]=%d, want %d", index, got, want)
		}
	}
}

func TestShardedPlaylistSequenceResetCreatesMonotonicEpoch(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)

	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 4, 500, "")
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 4, 501, "")
	track := entry.recording.Tracks["main"]
	track.HasLastObservedSequence = true
	track.LastObservedSequence = 501
	if err := manager.saveRecordingHeader(entry.recording); err != nil {
		t.Fatalf("persist old source high-water: %v", err)
	}

	playlist, err := hls.ParseMedia([]byte(`#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:1
#EXT-X-DISCONTINUITY-SEQUENCE:7
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-MAP:URI="init-new.bin",BYTERANGE="4@4"
#EXT-X-BYTERANGE:4@8
#EXTINF:1.0,
media-new.bin
`), "https://source.invalid/live.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	updated, err := manager.observePlaylistAtGeneration(entry, playlist, entry.mediaGeneration)
	if err != nil || !updated {
		t.Fatalf("observe V2 reset playlist: updated=%v err=%v", updated, err)
	}
	track = entry.recording.Tracks["main"]
	if track.SourceEpoch != 1 || !track.HasLastObservedSequence || track.LastObservedSequence != 0 {
		t.Fatalf("V2 reset state epoch=%d observed=%v/%d, want epoch 1 and sequence 0", track.SourceEpoch, track.HasLastObservedSequence, track.LastObservedSequence)
	}
	if len(track.PendingSegments) != 1 {
		t.Fatalf("V2 reset pending coordinates=%#v, want one", track.PendingSegments)
	}
	pending := track.PendingSegments[0]
	mapRange := domain.ByteRange{Length: 4, Offset: 4}
	mapID := initSegmentID(hls.Map{URI: "https://source.invalid/init-new.bin", ByteRange: &mapRange}, 1, 7)
	if pending.SourceEpoch != 1 || pending.Sequence != 0 || pending.DiscontinuitySequence != 7 || pending.LivePresentationOrdinal != 3 ||
		!pending.LiveDiscontinuity || pending.LiveDiscontinuitySequence != 0 || pending.InitSegmentID != mapID {
		t.Fatalf("V2 reset pending identity=%#v, want epoch 1, seq 0, source disc 7, LPO 3, first live disc 0, map %q", pending, mapID)
	}
	mediaRange := domain.ByteRange{Length: 4, Offset: 8}
	init := domain.Segment{
		ID: mapID, TrackID: "main", Sequence: 0, SourceEpoch: 1, DiscontinuitySequence: 7,
		SourceURI: "https://source.invalid/init-new.bin", ByteRange: &mapRange, IsInit: true,
	}
	if _, _, err := manager.commitArchiveSegment(entry, nil, init, archiveindex.ClaimLiveOrigin, []byte("init")); err != nil {
		t.Fatalf("commit reset init map: %v", err)
	}
	segment := domain.Segment{
		TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 7, Sequence: 0,
		SourceURI: "https://source.invalid/media-new.bin", ByteRange: &mediaRange, Duration: 1,
		InitSegmentID: mapID, Discontinuity: pending.LiveDiscontinuity,
	}
	if _, _, err := manager.commitArchiveSegment(entry, nil, segment, archiveindex.ClaimLiveOrigin, []byte("media")); err != nil {
		t.Fatalf("commit reset media: %v", err)
	}
	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 3 {
		t.Fatalf("V2 live reset tail has %d segments, want 3", len(view.Segments))
	}
	reset := view.Segments[2]
	if reset.SourceEpoch != 1 || reset.Sequence != 0 || reset.DiscontinuitySequence != 7 || reset.ArchiveOrdinal != 3 ||
		reset.LivePresentationOrdinal != 3 || !reset.LiveDiscontinuity || reset.LiveDiscontinuitySequence != 0 ||
		reset.ByteRange == nil || *reset.ByteRange != mediaRange {
		t.Fatalf("V2 reset media identity=%#v", reset)
	}
	if init := view.InitSegments[mapID]; init.ByteRange == nil || *init.ByteRange != mapRange {
		t.Fatalf("V2 reset init map range=%v, want %#v", init.ByteRange, mapRange)
	}
}

func TestShardedLivePresentationRetainsInitMapByteRanges(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)

	uri := "https://source.invalid/init.bin"
	ranges := []domain.ByteRange{{Length: 100, Offset: 0}, {Length: 100, Offset: 100}}
	ids := make([]string, len(ranges))
	for index, byteRange := range ranges {
		ids[index] = initSegmentID(hls.Map{URI: uri, ByteRange: &byteRange}, 0, 0)
		init := domain.Segment{
			ID: ids[index], TrackID: "main", Sequence: uint64(100 + index),
			SourceURI: uri, ByteRange: &byteRange, IsInit: true,
		}
		if _, _, err := manager.commitArchiveSegment(entry, nil, init, archiveindex.ClaimLiveOrigin, []byte(fmt.Sprintf("init-range-%d", index))); err != nil {
			t.Fatalf("commit init map range %d: %v", index, err)
		}
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, uint64(100+index), ids[index])
	}
	if ids[0] == ids[1] {
		t.Fatal("distinct init byte ranges received the same V2 map identity")
	}
	view, err := manager.LivePlaybackSnapshot(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != 2 || len(view.InitSegments) != 2 {
		t.Fatalf("V2 live map projection has %d media and %d init maps, want 2 each", len(view.Segments), len(view.InitSegments))
	}
	for index, id := range ids {
		init, ok := view.InitSegments[id]
		if !ok || init.ByteRange == nil || *init.ByteRange != ranges[index] {
			t.Fatalf("V2 init map %q range=%v, want %#v", id, init.ByteRange, ranges[index])
		}
		if view.Segments[index].InitSegmentID != id {
			t.Fatalf("V2 media %d references init %q, want %q", index, view.Segments[index].InitSegmentID, id)
		}
	}
}

func TestShardedLiveWorkerKeepsLatestPresentationSlot(t *testing.T) {
	var manifest strings.Builder
	manifest.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:100\n#EXT-X-MAP:URI=\"init-one.mp4\"\n")
	for index := 0; index < 14; index++ {
		if index == 1 || index == 5 {
			manifest.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if index == 6 {
			manifest.WriteString("#EXT-X-MAP:URI=\"init-two.mp4\"\n")
		}
		manifest.WriteString("#EXTINF:1.0,\n")
		if index == 8 {
			// The latest live observation reserves ordinal 9 as a source gap.
			manifest.WriteString("#EXT-X-GAP\n")
		}
		fmt.Fprintf(&manifest, "seg-%03d.ts\n", index+1)
	}
	latestMediaRequested := make(chan struct{}, 1)
	client := &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
		body := "fixture payload"
		if request.URL.Path == "/live.m3u8" {
			body = manifest.String()
		} else if request.URL.Path == "/seg-014.ts" {
			select {
			case latestMediaRequested <- struct{}{}:
			default:
			}
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(body)), Request: request,
			ContentLength: int64(len(body)),
		}, nil
	})}
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, client, nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close manager: %v", err)
		}
	}()
	started, err := manager.StartResolved(context.Background(), "fixture", adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://source.invalid/live.m3u8",
	}, nil, "live worker fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Fatal("latest media request was not admitted")
	case <-latestMediaRequested:
	}
	entry, ok := manager.entry(started.ID)
	if !ok {
		t.Fatal("recording entry disappeared")
	}
	scheduler := activeScheduler(entry)
	if scheduler == nil {
		t.Fatal("live scheduler disappeared before draining")
	}
	if err := scheduler.drain(ctx); err != nil {
		t.Fatalf("drain live media commits: %v", err)
	}
	recording, err := manager.Get(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recording.State != domain.StateRecording {
		t.Fatalf("recording became terminal: state=%s error=%s", recording.State, recording.LastError)
	}
	view, err := manager.LivePlaybackSnapshot(context.Background(), started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Segments) != LivePlaybackWindowSize-1 || len(view.Slots) != LivePlaybackWindowSize {
		var slotOrdinals, mediaOrdinals []uint64
		for _, slot := range view.Slots {
			slotOrdinals = append(slotOrdinals, slot.Ordinal)
		}
		for _, segment := range view.Segments {
			mediaOrdinals = append(mediaOrdinals, segment.LivePresentationOrdinal)
		}
		t.Fatalf("live tail has %d media and %d slots, want %d media and %d slots; slot ordinals=%v media ordinals=%v", len(view.Segments), len(view.Slots), LivePlaybackWindowSize-1, LivePlaybackWindowSize, slotOrdinals, mediaOrdinals)
	}
	last := view.Segments[len(view.Segments)-1]
	if last.Sequence != 113 || last.LivePresentationOrdinal != 14 {
		t.Fatalf("latest live segment is sequence %d ordinal %d, want 113 / 14", last.Sequence, last.LivePresentationOrdinal)
	}
	gapFound := false
	for _, slot := range view.Slots {
		if slot.Ordinal == 9 {
			gapFound = slot.Unavailable && slot.Segment == nil
		}
	}
	if !gapFound {
		t.Fatalf("persisted live tail omitted reserved gap slot 9: %#v", view.Slots)
	}
	if len(view.InitSegments) != 3 {
		t.Fatalf("live tail has %d referenced init objects, want 3 across source discontinuities", len(view.InitSegments))
	}
}

func newShardedLivePresentationFixture(t *testing.T) (*Manager, *entry, func(*testing.T)) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, &http.Client{}, nil, func(context.Context, string) error { return nil }, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	root := &domain.Recording{
		FormatVersion:  storage.ShardedArchiveFormatVersion,
		ShardedArchive: &domain.ShardedArchiveSummary{},
		ID:             "94e2f247ab144c6a850fd5c07fb1e6a1", Title: "sharded live identity fixture",
		AdapterID: "fixture", State: domain.StateRecording, CreatedAt: now, StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {
			ID: "main", SourcePlaylistURL: "https://source.invalid/live.m3u8", NextArchiveOrdinal: 1,
			LivePresentation: &domain.LivePresentationState{NextOrdinal: 1},
		}},
	}
	identity, err := archiveindex.NewSessionIdentity(root.ID, root.AdapterID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	root.SourceSessionID = identity.ID
	if err := store.CreateShardedRecording(root); err != nil {
		t.Fatal(err)
	}
	entry := &entry{recording: root, done: closedChannel(), livePlayback: newLivePlaybackProjection()}
	manager.mu.Lock()
	manager.entries[root.ID] = entry
	manager.mu.Unlock()
	closeManager := func(t *testing.T) {
		t.Helper()
		entry.mu.Lock()
		if entry.recording != nil {
			entry.recording.State = domain.StateInterrupted
		}
		entry.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Close(ctx); err != nil {
			t.Errorf("close fixture manager: %v", err)
		}
	}
	return manager, entry, closeManager
}

func newShardedRuntimeEntry(recording *domain.Recording) *entry {
	return &entry{recording: recording}
}
