package acquire

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestCanonicalGapCommitPreservesV1RootSemantics(t *testing.T) {
	manager := &Manager{}
	track := &domain.Track{ID: "main"}
	recording := &domain.Recording{ID: "legacy", Tracks: map[string]*domain.Track{"main": track}}
	candidate := canonicalGapCandidate{
		trackID: "main", sourceEpoch: 2, discontinuitySequence: 4,
		fromSequence: 9, toSequence: 9, reason: "legacy missing coordinate",
	}
	outcome, err := manager.commitGapIfStillMissing(recording, candidate)
	if err != nil || !outcome.missing || outcome.appended != 1 || len(recording.Gaps) != 1 {
		t.Fatalf("V1 missing coordinate was not committed: outcome=%+v gaps=%#v err=%v", outcome, recording.Gaps, err)
	}

	track.Segments = append(track.Segments, domain.Segment{
		TrackID: "main", SourceEpoch: 2, DiscontinuitySequence: 4, Sequence: 10,
	})
	candidate.fromSequence, candidate.toSequence = 10, 10
	outcome, err = manager.commitGapIfStillMissing(recording, candidate)
	if err != nil || !outcome.present || outcome.appended != 0 || len(recording.Gaps) != 1 {
		t.Fatalf("V1 root-present coordinate received a gap: outcome=%+v gaps=%#v err=%v", outcome, recording.Gaps, err)
	}
	if present, err := manager.canonicalCoordinatePresent(context.Background(), recording, "main", 2, 4, 10); err != nil || !present {
		t.Fatalf("V1 canonical segment lookup=%t err=%v", present, err)
	}
}

func TestShardedGapCandidateSubtractsCanonicalPresentCoordinates(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	for _, sequence := range []uint64{103, 107, 108} {
		segment := domain.Segment{
			TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0, Sequence: sequence,
			SourceURI: "https://source.invalid/historical.ts", Duration: 2,
		}
		if _, _, err := manager.commitArchiveSegment(entry, nil, segment, archiveindex.ClaimHistorical, []byte("historical payload")); err != nil {
			t.Fatalf("commit canonical present coordinate %d: %v", sequence, err)
		}
	}
	if err := manager.update(entry, func(recording *domain.Recording) error {
		_, err := manager.commitGapIfStillMissing(recording, canonicalGapCandidate{
			trackID: "main", sourceEpoch: 0, discontinuitySequence: 0,
			fromSequence: 100, toSequence: 110, reason: "range fixture",
		})
		return err
	}); err != nil {
		t.Fatalf("commit candidate range: %v", err)
	}
	var gaps []domain.Gap
	if err := manager.store.IterateShardedGaps(context.Background(), entry.recording.ID, func(gap domain.Gap) error {
		gaps = append(gaps, gap)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := [][2]uint64{{100, 102}, {104, 106}, {109, 110}}
	if len(gaps) != len(want) {
		t.Fatalf("gap intervals=%#v, want %#v", gaps, want)
	}
	for i, interval := range want {
		if gaps[i].FromSequence != interval[0] || gaps[i].ToSequence != interval[1] {
			t.Errorf("gap[%d]=%d..%d, want %d..%d", i, gaps[i].FromSequence, gaps[i].ToSequence, interval[0], interval[1])
		}
	}
}

type gapResolutionCountingBackend struct {
	storage.StorageBackend
	mu            sync.Mutex
	timelineReads int
	failTimeline  error
}

func (b *gapResolutionCountingBackend) LoadSidecarContext(ctx context.Context, id, relative string, maximum int64, output any) error {
	if strings.HasPrefix(relative, "archive/v2/timeline/") {
		b.mu.Lock()
		b.timelineReads++
		failure := b.failTimeline
		b.mu.Unlock()
		if failure != nil {
			return failure
		}
	}
	if reader, ok := b.StorageBackend.(interface {
		LoadSidecarContext(context.Context, string, string, int64, any) error
	}); ok {
		return reader.LoadSidecarContext(ctx, id, relative, maximum, output)
	}
	return b.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	}).LoadSidecar(id, relative, maximum, output)
}

func (b *gapResolutionCountingBackend) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (storage.V2SidecarPage, error) {
	lister, ok := b.StorageBackend.(interface {
		ListSidecarsContext(context.Context, string, string, string, int) (storage.V2SidecarPage, error)
	})
	if !ok {
		return storage.V2SidecarPage{}, storage.ErrShardedArchiveUnavailable
	}
	return lister.ListSidecarsContext(ctx, id, prefix, cursor, limit)
}

func (b *gapResolutionCountingBackend) timelineReadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.timelineReads
}

func TestHugePlaylistSequenceJumpUsesBoundedGapResolutionAndResumes(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	backend := &gapResolutionCountingBackend{StorageBackend: manager.store.StorageBackend}
	manager.store.StorageBackend = backend
	track := entry.recording.Tracks["main"]
	track.HasLastObservedSequence = true
	track.LastObservedSequence = 1000
	track.Segments = []domain.Segment{{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0, Sequence: 1000}}
	if err := manager.saveRecordingHeader(entry.recording); err != nil {
		t.Fatalf("save initial source high-water: %v", err)
	}
	playlist, err := hls.ParseMedia([]byte(`#EXTM3U
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:9000000000000000000
#EXTINF:2,
live.ts
`), "https://source.invalid/live.m3u8")
	if err != nil {
		t.Fatalf("parse large but valid source sequence: %v", err)
	}
	if _, err := manager.observePlaylistAtGeneration(entry, playlist, entry.mediaGeneration); err != nil {
		t.Fatalf("observe playlist with huge sequence jump: %v", err)
	}
	if got := backend.timelineReadCount(); got > int(2*maxGapResolutionCoordinates+8) {
		t.Fatalf("first transition did %d canonical timeline reads, budget is %d", got, 2*maxGapResolutionCoordinates+8)
	}
	track = entry.recording.Tracks["main"]
	if !track.HasLastObservedSequence || track.LastObservedSequence != 9_000_000_000_000_000_000 {
		t.Fatalf("source high-water changed: observed=%v/%d", track.HasLastObservedSequence, track.LastObservedSequence)
	}
	if cursor := track.GapResolutionCursor; cursor == nil || cursor.SourceEpoch != 0 || cursor.DiscontinuitySequence != 0 || cursor.Sequence != 1000+maxGapResolutionCoordinates {
		t.Fatalf("bounded gap cursor=%#v, want sequence %d", cursor, 1000+maxGapResolutionCoordinates)
	}
	persisted, err := manager.store.LoadRecordingHeader(context.Background(), entry.recording.ID)
	if err != nil {
		t.Fatalf("load persisted bounded gap cursor: %v", err)
	}
	if cursor := persisted.Tracks["main"].GapResolutionCursor; cursor == nil || cursor.Sequence != 1000+maxGapResolutionCoordinates {
		t.Fatalf("bounded cursor was not durable across restart: %#v", cursor)
	}
	var gaps []domain.Gap
	if err := manager.store.IterateShardedGaps(context.Background(), entry.recording.ID, func(gap domain.Gap) error {
		gaps = append(gaps, gap)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 1 || gaps[0].FromSequence != 1001 || gaps[0].ToSequence != 1000+maxGapResolutionCoordinates {
		t.Fatalf("first bounded gap=%#v", gaps)
	}

	backend.mu.Lock()
	backend.timelineReads = 0
	backend.mu.Unlock()
	if _, err := manager.observePlaylistAtGeneration(entry, playlist, entry.mediaGeneration); err != nil {
		t.Fatalf("resume deferred gap on next observation: %v", err)
	}
	if got := backend.timelineReadCount(); got > int(2*maxGapResolutionCoordinates+8) {
		t.Fatalf("resumed transition did %d canonical timeline reads, budget is %d", got, 2*maxGapResolutionCoordinates+8)
	}
	if cursor := entry.recording.Tracks["main"].GapResolutionCursor; cursor == nil || cursor.Sequence != 1000+2*maxGapResolutionCoordinates {
		t.Fatalf("resumed gap cursor=%#v, want sequence %d", cursor, 1000+2*maxGapResolutionCoordinates)
	}
}

func TestHugeInPlaylistSequenceJumpResumesAfterBoundedPrefix(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	backend := &gapResolutionCountingBackend{StorageBackend: manager.store.StorageBackend}
	manager.store.StorageBackend = backend
	track := entry.recording.Tracks["main"]
	track.HasLastObservedSequence = true
	track.LastObservedSequence = 1000
	track.Segments = []domain.Segment{{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0, Sequence: 1000}}
	if err := manager.saveRecordingHeader(entry.recording); err != nil {
		t.Fatalf("save initial source high-water: %v", err)
	}
	playlist, err := hls.ParseMedia([]byte(`#EXTM3U
#EXT-X-TARGETDURATION:2
#EXT-X-MEDIA-SEQUENCE:1000
#EXTINF:2,
first.ts
#EXTINF:2,
far-ahead.ts
`), "https://source.invalid/live.m3u8")
	if err != nil {
		t.Fatalf("parse playlist with a large internal sequence jump: %v", err)
	}
	playlist.Segments[1].Sequence = 9_000_000_000_000_000_000
	if _, err := manager.observePlaylistAtGeneration(entry, playlist, entry.mediaGeneration); err != nil {
		t.Fatalf("observe first large internal jump: %v", err)
	}
	if got := backend.timelineReadCount(); got > int(2*maxGapResolutionCoordinates+8) {
		t.Fatalf("first internal transition did %d canonical timeline reads, budget is %d", got, 2*maxGapResolutionCoordinates+8)
	}
	firstCursor := entry.recording.Tracks["main"].GapResolutionCursor
	if firstCursor == nil || firstCursor.Sequence != 1000+maxGapResolutionCoordinates {
		t.Fatalf("first bounded cursor=%#v, want %d", firstCursor, 1000+maxGapResolutionCoordinates)
	}

	backend.mu.Lock()
	backend.timelineReads = 0
	backend.mu.Unlock()
	if _, err := manager.observePlaylistAtGeneration(entry, playlist, entry.mediaGeneration); err != nil {
		t.Fatalf("resume large internal jump: %v", err)
	}
	if got := backend.timelineReadCount(); got > int(2*maxGapResolutionCoordinates+8) {
		t.Fatalf("resumed internal transition did %d canonical timeline reads, budget is %d", got, 2*maxGapResolutionCoordinates+8)
	}
	secondCursor := entry.recording.Tracks["main"].GapResolutionCursor
	if secondCursor == nil || secondCursor.Sequence != 1000+2*maxGapResolutionCoordinates {
		t.Fatalf("resumed internal cursor=%#v, want %d", secondCursor, 1000+2*maxGapResolutionCoordinates)
	}
	var gaps []domain.Gap
	if err := manager.store.IterateShardedGaps(context.Background(), entry.recording.ID, func(gap domain.Gap) error {
		gaps = append(gaps, gap)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 2 || gaps[0].FromSequence != 1001 || gaps[0].ToSequence != 1000+maxGapResolutionCoordinates ||
		gaps[1].FromSequence != 1001+maxGapResolutionCoordinates || gaps[1].ToSequence != 1000+2*maxGapResolutionCoordinates {
		t.Fatalf("internal jump gaps did not resume at the deferred boundary: %#v", gaps)
	}
}

func TestCanonicalGapStorageLookupErrorFailsClosed(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	failure := errors.New("injected canonical index read failure")
	manager.store.StorageBackend = &gapResolutionCountingBackend{
		StorageBackend: manager.store.StorageBackend,
		failTimeline:   failure,
	}
	err := manager.update(entry, func(recording *domain.Recording) error {
		_, err := manager.commitGapIfStillMissing(recording, canonicalGapCandidate{
			trackID: "main", sourceEpoch: 0, discontinuitySequence: 0,
			fromSequence: 901, toSequence: 901, reason: "must fail closed",
		})
		return err
	})
	if !errors.Is(err, failure) {
		t.Fatalf("gap candidate error=%v, want injected canonical read failure", err)
	}
	var count int
	if err := manager.store.IterateShardedGaps(context.Background(), entry.recording.ID, func(domain.Gap) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("storage read failure was treated as absence and published %d gaps", count)
	}
}

func TestCanonicalGapCandidateChunkArithmeticAtUint64Boundaries(t *testing.T) {
	max := uint64(math.MaxUint64)
	candidate := canonicalGapCandidate{trackID: "main", fromSequence: 0, toSequence: max, reason: "full range"}
	chunk, remainder, hasRemainder := splitCanonicalGapCandidate(candidate)
	if !hasRemainder || chunk.fromSequence != 0 || chunk.toSequence != maxGapResolutionCoordinates-1 || remainder.fromSequence != maxGapResolutionCoordinates || remainder.toSequence != max {
		t.Fatalf("full-range split overflowed: chunk=%#v remainder=%#v deferred=%v", chunk, remainder, hasRemainder)
	}
	nearMax := canonicalGapCandidate{trackID: "main", fromSequence: max - 300, toSequence: max, reason: "near max"}
	chunk, remainder, hasRemainder = splitCanonicalGapCandidate(nearMax)
	if !hasRemainder || chunk.toSequence != max-45 || remainder.fromSequence != max-44 || remainder.toSequence != max {
		t.Fatalf("near-max split overflowed: chunk=%#v remainder=%#v deferred=%v", chunk, remainder, hasRemainder)
	}
	if from, to, ok := sequenceAdvanceGapBounds(true, max-2, max); !ok || from != max-1 || to != max-1 {
		t.Fatalf("near-max sequence advance=(%d,%d,%v)", from, to, ok)
	}
	if _, _, ok := sequenceAdvanceGapBounds(true, max, max); ok {
		t.Fatal("max sequence high-water unexpectedly advanced and wrapped")
	}
	if _, _, ok := sequenceAdvanceGapBounds(true, 10, 11); ok {
		t.Fatal("adjacent sequence was treated as a gap")
	}
	if from, to, ok := sequenceAdvanceGapBounds(true, 0, 2); !ok || from != 1 || to != 1 {
		t.Fatalf("zero-boundary sequence gap=(%d,%d,%v)", from, to, ok)
	}
	if _, _, ok := sequenceAdvanceGapBounds(false, 0, max); ok {
		t.Fatal("missing prior observation unexpectedly generated a range")
	}
}

func TestShardedGapCandidatePresentAndMissingRaceSemantics(t *testing.T) {
	manager, entry, closeManager := newShardedLivePresentationFixture(t)
	defer closeManager(t)
	for sequence := uint64(100); sequence <= 102; sequence++ {
		commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimLiveOrigin, 0, 0, sequence, "")
	}
	const sequence = uint64(103)
	if err := manager.update(entry, func(recording *domain.Recording) error {
		_, err := manager.commitGapIfStillMissing(recording, canonicalGapCandidate{
			trackID: "main", sourceEpoch: 0, discontinuitySequence: 0,
			fromSequence: sequence, toSequence: sequence, reason: "gap wins first",
		})
		return err
	}); err != nil {
		t.Fatalf("commit genuine gap first: %v", err)
	}
	commitLivePresentationSegment(t, manager, entry, archiveindex.ClaimHistorical, 0, 0, sequence, "")
	var rawGapCount int
	if err := manager.store.IterateShardedGaps(context.Background(), entry.recording.ID, func(domain.Gap) error {
		rawGapCount++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if rawGapCount != 1 {
		t.Fatalf("later media rewrote immutable GAP history: count=%d", rawGapCount)
	}
	flow, err := manager.AcquisitionFlowSnapshot(context.Background(), entry.recording.ID, 0, 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, segment := range flow.Segments {
		if segment.Sequence == sequence {
			if segment.State != "present" || !segment.Recovered {
				t.Fatalf("gap-first then historical media projected as %#v", segment)
			}
			return
		}
	}
	t.Fatalf("acquisition flow omitted recovered coordinate %d", sequence)
}

func TestV1GapCandidateStillUsesCanonicalRootSegments(t *testing.T) {
	manager := &Manager{}
	track := &domain.Track{ID: "main", Segments: []domain.Segment{{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0, Sequence: 40,
	}}}
	recording := &domain.Recording{ID: "legacy", Tracks: map[string]*domain.Track{"main": track}}
	outcome, err := manager.commitGapIfStillMissing(recording, canonicalGapCandidate{
		trackID: "main", fromSequence: 40, toSequence: 40, reason: "legacy present",
	})
	if err != nil || !outcome.present || len(recording.Gaps) != 0 {
		t.Fatalf("V1 present root segment failed to suppress GAP: outcome=%#v gaps=%#v err=%v", outcome, recording.Gaps, err)
	}
	outcome, err = manager.commitGapIfStillMissing(recording, canonicalGapCandidate{
		trackID: "main", fromSequence: 41, toSequence: 41, reason: "legacy absent",
	})
	if err != nil || !outcome.missing || len(recording.Gaps) != 1 || recording.Gaps[0].FromSequence != 41 {
		t.Fatalf("V1 absent root coordinate did not create GAP: outcome=%#v gaps=%#v err=%v", outcome, recording.Gaps, err)
	}
}

func TestCanonicalGapCandidateSpanLimitIsInclusive(t *testing.T) {
	candidate := canonicalGapCandidate{trackID: "main", fromSequence: 10, toSequence: 10 + maxGapResolutionCoordinates - 1}
	chunk, remainder, hasRemainder := splitCanonicalGapCandidate(candidate)
	if hasRemainder || chunk.toSequence != candidate.toSequence {
		t.Fatalf("exactly %d coordinates should be resolved in one pass: chunk=%#v remainder=%#v deferred=%v", maxGapResolutionCoordinates, chunk, remainder, hasRemainder)
	}
	candidate.toSequence++
	chunk, remainder, hasRemainder = splitCanonicalGapCandidate(candidate)
	if !hasRemainder || chunk.toSequence != candidate.fromSequence+maxGapResolutionCoordinates-1 || remainder.fromSequence != chunk.toSequence+1 {
		t.Fatalf("one-over-budget range was not split: chunk=%#v remainder=%#v deferred=%v", chunk, remainder, hasRemainder)
	}
}
