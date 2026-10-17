package acquire

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestManifestHistoricalAvailabilityAutomaticSchedulerRepairsSlidingWindow(t *testing.T) {
	// Keep the high-volume sliding-window case on V2, where archive work is
	// bounded by sharded indexes. Separate repair tests continue covering V1.
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	media := repairMediaContextForTest()
	media.ManifestURL = "https://media.example/archive/index.m3u8?auth=synthetic-test-token"
	media.SessionRef = "live-instance-777"
	media.RequestPolicy = &adapterproto.RequestPolicy{URLTransform: &adapterproto.URLTransformPolicy{Rules: []adapterproto.URLTransformRule{{
		Scopes:          []adapterproto.ResourceRequestScope{adapterproto.RequestScopeMedia},
		QueryParameters: []adapterproto.QueryParameterPropagation{{From: "auth", To: "media_token"}},
	}}}}
	media.HistoricalAvailability = &adapterproto.HistoricalAvailability{
		Mode: adapterproto.HistoricalModeManifest, HistoricalManifestURL: media.ManifestURL,
	}

	fixture := &repairFixtureTransport{
		manifest: manifestWindowForAutomaticRecovery(100, 200),
		objects:  make(map[string][]byte),
	}
	for sequence := uint64(100); sequence <= 250; sequence++ {
		fixture.objects[fmt.Sprintf("/archive/segment-%d.ts", sequence)] = historicalRecoveryPayload(sequence)
	}

	root := newRecoveryShardedTestRoot(t, store, "84e2f247ab144c6a850fd5c07fb1e6a1", domain.StateRecording, media)

	fence := &repairOwnerFence{}
	manager, e, closeManager := newRepairTestManager(t, store, fixture, root, fence, nil)
	owner := ownerForRepair(root.ID, 1)
	_, cancelLive := context.WithCancel(context.Background())
	t.Cleanup(cancelLive)
	e.mu.Lock()
	e.media = media
	e.ownership = &owner
	e.cancel = cancelLive
	e.done = make(chan struct{})
	e.mu.Unlock()

	for sequence := uint64(190); sequence < 200; sequence++ {
		commitManifestRecoveryLiveOrigin(t, manager, e, owner, sequence)
	}
	var ownerClaims atomic.Int32
	if err := manager.ConfigureAutomaticArchiveRecovery(func(context.Context, string) (OwnershipToken, error) {
		ownerClaims.Add(1)
		return owner, nil
	}); err != nil {
		t.Fatal(err)
	}
	manager.autoRecovery.mu.Lock()
	manager.autoRecovery.testPassCompleted = make(chan string, 8)
	manager.autoRecovery.mu.Unlock()
	// This normal live-origin commit creates the final persisted tail coordinate
	// and triggers the real Core-owned scheduler.
	commitManifestRecoveryLiveOrigin(t, manager, e, owner, 200)

	waitForManifestRecoveryRecheck(t, manager, root.ID)
	firstRequests := fixture.requestedURLs()
	firstRoot := requireManifestRecoveryWindow(t, manager, store, root.ID, 100, 200)
	if !manifestRecoveryTimelineMatchesRecording(firstRoot, 101, 100, 1, 200, 101, 202) {
		t.Fatal("automatic manifest backfill did not reconcile the first sharded window")
	}
	assertManifestRecoveryMediaRequests(t, firstRequests, 100, 189)
	firstRevision := firstRoot.TimelineRevision
	firstPrefix := snapshotManifestRecoverySegments(t, firstRoot, store, root.ID, 100, 149)

	fixture.setManifest(manifestWindowForAutomaticRecovery(150, 250))
	// A later live-origin claim wakes the delayed manifest recheck. Sequence 251
	// is beyond the DVR snapshot, leaving 201..250 for historical acquisition.
	if _, err := manager.observePlaylistAtGeneration(e, hls.MediaPlaylist{
		TargetDuration: 2, MediaSequence: 251, DiscontinuitySequence: 0,
		Segments: []hls.MediaSegment{{Sequence: 251, DiscontinuitySequence: 0, URI: "https://media.example/live/segment-251.ts", Duration: 2}},
	}, e.mediaGeneration); err != nil {
		t.Fatalf("observe live sequence 251 after sliding window: %v", err)
	}
	commitManifestRecoveryLiveOrigin(t, manager, e, owner, 251)

	waitForManifestRecoveryRecheck(t, manager, root.ID)
	allRequests := fixture.requestedURLs()
	secondRequests := allRequests[len(firstRequests):]
	updatedRoot := requireManifestRecoveryWindow(t, manager, store, root.ID, 100, 251)
	if !manifestRecoveryTimelineMatchesRecording(updatedRoot, 152, 100, 1, 251, 152, 304) {
		t.Fatal("automatic sliding-window recovery did not reconcile the full sharded timeline")
	}
	if updatedRoot.TimelineRevision <= firstRevision {
		t.Fatalf("timeline revision=%d, first revision=%d", updatedRoot.TimelineRevision, firstRevision)
	}
	assertManifestRecoveryMediaRequests(t, secondRequests, 201, 250)
	assertManifestRecoverySnapshotsUnchanged(t, updatedRoot, store, root.ID, firstPrefix)

	if ownerClaims.Load() != 0 {
		t.Fatalf("active recording used terminal recovery owner callback %d times", ownerClaims.Load())
	}
	if err := closeManager(); err != nil {
		t.Fatal(err)
	}
}

func manifestWindowForAutomaticRecovery(first, last uint64) []byte {
	var body strings.Builder
	fmt.Fprintf(&body, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:%d\n#EXT-X-DISCONTINUITY-SEQUENCE:0\n", first)
	for sequence := first; sequence <= last; sequence++ {
		fmt.Fprintf(&body, "#EXTINF:2,\nsegment-%d.ts\n", sequence)
	}
	return []byte(body.String())
}

func historicalRecoveryPayload(sequence uint64) []byte {
	return []byte(fmt.Sprintf("historical-segment-%03d", sequence))
}

func liveRecoveryPayload(sequence uint64) []byte {
	return []byte(fmt.Sprintf("live-origin-segment-%03d", sequence))
}

func commitManifestRecoveryLiveOrigin(t *testing.T, manager *Manager, e *entry, owner OwnershipToken, sequence uint64) {
	t.Helper()
	segment := domain.Segment{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0,
		Sequence: sequence, ArchiveOrdinal: sequence - 189, TimelineOrdinal: sequence - 189,
		Duration: 2, SourceURI: fmt.Sprintf("https://media.example/live/segment-%d.ts", sequence),
	}
	if _, _, err := manager.commitArchiveSegmentOwned(e, &owner, segment, archiveindex.ClaimLiveOrigin, liveRecoveryPayload(sequence), false); err != nil {
		t.Fatalf("commit live-origin sequence %d: %v", sequence, err)
	}
}

func waitForManifestRecoveryRecheck(t *testing.T, manager *Manager, recordingID string) {
	t.Helper()
	manager.mu.RLock()
	scheduler := manager.autoRecovery
	manager.mu.RUnlock()
	if scheduler == nil {
		t.Fatal("automatic recovery scheduler is unavailable")
	}
	scheduler.mu.Lock()
	completed := scheduler.testPassCompleted
	scheduler.mu.Unlock()
	if completed == nil {
		t.Fatal("automatic recovery completion signal is unavailable")
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		scheduler.mu.Lock()
		item, queued := scheduler.queued[recordingID]
		finished := scheduler.inFlight != recordingID && queued && !item.due.IsZero()
		scheduler.mu.Unlock()
		if finished {
			return
		}
		select {
		case completedID := <-completed:
			if completedID == recordingID {
				continue
			}
		case <-timer.C:
			t.Fatal("timed out waiting for automatic recovery scheduler to finish pass")
		}
	}
}

func manifestRecoveryTimelineMatchesRecording(recording *domain.Recording, count int, firstSequence, firstOrdinal, lastSequence, lastOrdinal uint64, duration float64) bool {
	if recording == nil || recording.Tracks["main"] == nil || len(recording.Tracks["main"].Segments) != count || recording.Duration() != duration {
		return false
	}
	first, firstExists := findSegmentBySequence(recording.Tracks["main"].Segments, firstSequence)
	last, lastExists := findSegmentBySequence(recording.Tracks["main"].Segments, lastSequence)
	return firstExists && lastExists && first.TimelineOrdinal == firstOrdinal && last.TimelineOrdinal == lastOrdinal
}

func requireManifestRecoveryWindow(t *testing.T, manager *Manager, store *storage.Store, recordingID string, first, last uint64) *domain.Recording {
	t.Helper()
	current, err := manager.Get(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	wantCount := int(last - first + 1)
	if len(current.Tracks["main"].Segments) != wantCount {
		t.Fatalf("stored media count=%d, want %d", len(current.Tracks["main"].Segments), wantCount)
	}
	for sequence := first; sequence <= last; sequence++ {
		segment := segmentAtSequence(t, current.Tracks["main"].Segments, sequence)
		wantOrdinal := sequence - first + 1
		if segment.TimelineOrdinal != wantOrdinal {
			t.Fatalf("sequence %d timeline ordinal=%d, want %d", sequence, segment.TimelineOrdinal, wantOrdinal)
		}
		wantPayload := historicalRecoveryPayload(sequence)
		if sequence >= 190 && sequence <= 200 || sequence == 251 {
			wantPayload = liveRecoveryPayload(sequence)
		}
		if segment.PayloadSize != int64(len(wantPayload)) || segment.SHA256 != sha256Hex(wantPayload) {
			t.Fatalf("sequence %d stored identity size=%d hash=%q", sequence, segment.PayloadSize, segment.SHA256)
		}
		assertStoredPayload(t, store, recordingID, segment.StoragePath, wantPayload)
	}

	inventory, err := manager.ArchiveInventory(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := first; sequence <= last; sequence++ {
		coordinate := archiveindex.Coordinate{
			SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 0,
			DiscontinuitySequence: 0, Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}
		if state := archiveindex.CoverageAt(inventory, coordinate); state != archiveindex.CoveragePresent {
			t.Fatalf("sequence %d archive coverage=%s, want present", sequence, state)
		}
		claim, ok := selectedManifestRecoveryClaim(inventory, coordinate)
		if !ok || claim.Verification != archiveindex.VerificationVerified {
			t.Fatalf("sequence %d selected claim is not verified: %#v", sequence, claim)
		}
		wantSource := archiveindex.ClaimHistorical
		if sequence >= 190 && sequence <= 200 || sequence == 251 {
			wantSource = archiveindex.ClaimLiveOrigin
		}
		if claim.Source != wantSource {
			t.Fatalf("sequence %d selected claim source=%s, want %s", sequence, claim.Source, wantSource)
		}
	}
	return current
}

func selectedManifestRecoveryClaim(inventory archiveindex.Inventory, coordinate archiveindex.Coordinate) (archiveindex.Claim, bool) {
	for _, segment := range inventory.Segments {
		if segment.Coordinate != coordinate {
			continue
		}
		for _, claim := range segment.Claims {
			if claim.ID == segment.SelectedClaimID {
				return claim, true
			}
		}
	}
	return archiveindex.Claim{}, false
}

func assertManifestRecoveryMediaRequests(t *testing.T, requests []string, first, last uint64) {
	t.Helper()
	want := make(map[uint64]bool, last-first+1)
	for sequence := first; sequence <= last; sequence++ {
		want[sequence] = true
	}
	seenManifest := false
	seenMedia := make(map[uint64]bool, len(want))
	for _, raw := range requests {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		query := parsed.Query()
		if parsed.Path == "/archive/index.m3u8" {
			seenManifest = true
			if query.Get("auth") != "synthetic-test-token" || query.Has("media_token") {
				t.Fatalf("manifest request received media-only transform: %q", raw)
			}
			continue
		}
		if !strings.HasPrefix(parsed.Path, "/archive/segment-") {
			continue
		}
		if !strings.HasSuffix(parsed.Path, ".ts") {
			t.Fatalf("unexpected historical media path: %q", raw)
		}
		if query.Get("media_token") != "synthetic-test-token" || query.Has("auth") {
			t.Fatalf("media request has wrong scoped query transform: %q", raw)
		}
		sequenceText := strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/archive/segment-"), ".ts")
		sequence, parseErr := strconv.ParseUint(sequenceText, 10, 64)
		if parseErr != nil {
			t.Fatalf("parse media sequence from %q: %v", raw, parseErr)
		}
		if !want[sequence] {
			t.Fatalf("unexpected historical media request for sequence %d: %q", sequence, raw)
		}
		seenMedia[sequence] = true
	}
	if !seenManifest {
		t.Fatal("automatic recovery did not request historical manifest")
	}
	if len(seenMedia) != len(want) {
		t.Fatalf("historical media requests=%d, want %d; seen=%v", len(seenMedia), len(want), seenMedia)
	}
	for sequence := range want {
		if !seenMedia[sequence] {
			t.Fatalf("historical media sequence %d was not requested", sequence)
		}
	}
}

type manifestRecoverySegmentSnapshot struct {
	id             string
	storagePath    string
	sha256         string
	payloadSize    int64
	archiveOrdinal uint64
	bytes          []byte
}

func snapshotManifestRecoverySegments(t *testing.T, recording *domain.Recording, store *storage.Store, recordingID string, first, last uint64) map[uint64]manifestRecoverySegmentSnapshot {
	t.Helper()
	snapshot := make(map[uint64]manifestRecoverySegmentSnapshot, last-first+1)
	for sequence := first; sequence <= last; sequence++ {
		segment := segmentAtSequence(t, recording.Tracks["main"].Segments, sequence)
		snapshot[sequence] = manifestRecoverySegmentSnapshot{
			id: segment.ID, storagePath: segment.StoragePath, sha256: segment.SHA256,
			payloadSize: segment.PayloadSize, archiveOrdinal: segment.ArchiveOrdinal,
			bytes: mustLoadPayload(t, store, recordingID, segment.StoragePath),
		}
	}
	return snapshot
}

func assertManifestRecoverySnapshotsUnchanged(t *testing.T, recording *domain.Recording, store *storage.Store, recordingID string, snapshot map[uint64]manifestRecoverySegmentSnapshot) {
	t.Helper()
	for sequence, before := range snapshot {
		after := segmentAtSequence(t, recording.Tracks["main"].Segments, sequence)
		if after.ID != before.id || after.StoragePath != before.storagePath || after.SHA256 != before.sha256 ||
			after.PayloadSize != before.payloadSize || after.ArchiveOrdinal != before.archiveOrdinal {
			t.Fatalf("historical sequence %d immutable identity changed: before=%+v after=%+v", sequence, before, after)
		}
		bytesAfter := mustLoadPayload(t, store, recordingID, after.StoragePath)
		if !bytes.Equal(bytesAfter, before.bytes) {
			t.Fatalf("historical sequence %d stored bytes changed", sequence)
		}
	}
}
