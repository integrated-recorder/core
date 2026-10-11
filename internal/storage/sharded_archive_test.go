package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
)

func newShardedArchiveTestStore(t *testing.T, id string) (*Store, *domain.Recording) {
	t.Helper()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion:  ShardedArchiveFormatVersion,
		ShardedArchive: &domain.ShardedArchiveSummary{},
		ID:             id, SourceSessionID: "session-" + strings.Repeat("a", 64),
		Title: "sharded storage test", AdapterID: "fixture",
		State: domain.StateRecording, CreatedAt: now, StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	return store, header
}

func testV2Claim(coordinate archiveindex.Coordinate, id string, acquiredAt time.Time) archiveindex.Claim {
	return archiveindex.Claim{
		ID: id, Source: archiveindex.ClaimLiveOrigin, AcquiredAt: acquiredAt,
		PayloadPath: "tracks/main/objects/fixture.ts", Size: 7,
		SHA256: strings.Repeat("b", 64), Verification: archiveindex.VerificationVerified,
		Disposition: archiveindex.DispositionAccepted,
	}
}

func TestShardedClaimOrphanRetryPreservesIdentityAndReplacesSelection(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "b4e2f247ab144c6a850fd5c07fb1e6a1")
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 42, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	time1 := time.Date(2026, 10, 9, 0, 1, 0, 0, time.UTC)
	time2 := time1.Add(time.Second)
	first := testV2Claim(coordinate, "live_origin:"+segmentID+":"+strings.Repeat("b", 64), time1)
	set := V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: first.ID, State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{first}}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
		t.Fatalf("persist orphan claim candidate: %v", err)
	}

	// A retry can reconstruct the same stable claim identity with a fresh
	// attempt timestamp. Existing candidate metadata stays first-writer-wins.
	retry := first
	retry.AcquiredAt = time2
	second := testV2Claim(coordinate, "historical:"+segmentID+":"+strings.Repeat("c", 64), time2)
	set = V2ClaimSet{
		SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: second.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{retry, second},
	}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
		t.Fatalf("merge retry after orphan claim candidate: %v", err)
	}
	loaded, err := store.LoadShardedClaimSet(ctx, header.ID, segmentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Claims) != 2 || loaded.SelectedClaimID != second.ID || loaded.State != archiveindex.CoveragePresent {
		t.Fatalf("orphan claims not unioned with new selection: %#v", loaded)
	}
	for _, claim := range loaded.Claims {
		if claim.ID == first.ID && !claim.AcquiredAt.Equal(time1) {
			t.Fatalf("retry changed first claim timestamp: got %s want %s", claim.AcquiredAt, time1)
		}
	}

	conflicting := retry
	conflicting.SHA256 = strings.Repeat("d", 64)
	set.Claims = []archiveindex.Claim{conflicting, second}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); !errors.Is(err, ErrShardedArchiveConflict) {
		t.Fatalf("changed immutable claim identity error=%v, want conflict", err)
	}
}

func TestSaveRecordingHeaderRejectsStaleRevisionAfterClaimAdmission(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "b5e2f247ab144c6a850fd5c07fb1e6a1")
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("claim revision fixture")
	payloadPath := "tracks/main/objects/claim-revision.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 1, 0, 0, time.UTC)
	selected := archiveindex.Claim{
		ID: "live_origin:" + segmentID + ":" + stored.SHA256, Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: now, PayloadPath: payloadPath, Size: int64(len(payload)), SHA256: stored.SHA256,
		Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
	}
	record := v2ScaleMediaRecord(coordinate, 1, payloadPath, int64(len(payload)), stored.SHA256)
	record.SelectedClaim, record.ClaimState = &selected, archiveindex.CoveragePresent
	if err := store.PublishShardedMedia(ctx, header, record); err != nil {
		t.Fatalf("publish canonical media: %v", err)
	}
	alternate := selected
	alternate.ID = "historical:" + segmentID + ":" + stored.SHA256
	alternate.Source = archiveindex.ClaimHistorical
	alternate.AcquiredAt = now.Add(time.Second)
	set := V2ClaimSet{
		SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: selected.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{selected, alternate},
	}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
		t.Fatalf("admit supplemental claim: %v", err)
	}
	current, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ArchiveRevision <= header.ArchiveRevision {
		t.Fatalf("claim mutation did not advance canonical revision: stale=%d current=%d", header.ArchiveRevision, current.ArchiveRevision)
	}
	if err := store.SaveRecordingHeader(ctx, header); !errors.Is(err, ErrShardedArchiveConflict) {
		t.Fatalf("stale root save error=%v, want ErrShardedArchiveConflict", err)
	}
}

func TestPublishShardedMediaRejectsStaleHeaderAfterMetadataAppend(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "b6e2f247ab144c6a850fd5c07fb1e6a1")
	payload := []byte("stale media header fixture")
	payloadPath := "tracks/main/objects/stale-header.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	title := "metadata advances archive revision"
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{
		ObservedAt: time.Date(2026, 10, 9, 0, 1, 0, 0, time.UTC), Title: &title,
	}); err != nil {
		t.Fatalf("append metadata: %v", err)
	}
	current, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ArchiveRevision <= header.ArchiveRevision {
		t.Fatalf("metadata mutation did not advance revision: stale=%d current=%d", header.ArchiveRevision, current.ArchiveRevision)
	}

	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	record := v2ScaleMediaRecord(coordinate, 1, payloadPath, int64(len(payload)), stored.SHA256)
	if err := store.PublishShardedMedia(ctx, header, record); !errors.Is(err, ErrShardedArchiveConflict) {
		t.Fatalf("stale media publication error=%v, want ErrShardedArchiveConflict", err)
	}
	if err := store.PublishShardedMedia(ctx, current, record); err != nil {
		t.Fatalf("retry media publication from current root: %v", err)
	}
	visible, err := store.LookupShardedMediaByArchiveOrdinal(ctx, header.ID, "main", 1)
	if err != nil || visible.Segment.ID != record.Segment.ID {
		t.Fatalf("retry did not publish canonical media: record=%#v err=%v", visible, err)
	}
}

func TestCreateShardedRecordingAcceptsOnlyUnspecifiedOrCurrentFormat(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int
		wantErr bool
	}{
		{name: "unspecified", version: 0},
		{name: "current", version: ShardedArchiveFormatVersion},
		{name: "legacy", version: 1, wantErr: true},
		{name: "future", version: ShardedArchiveFormatVersion + 1, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			header := shardedArchiveHeaderForTest("a4e2f247ab144c6a850fd5c07fb1e6a1")
			header.FormatVersion = test.version
			err = store.CreateShardedRecording(header)
			if test.wantErr {
				if !errors.Is(err, ErrUnsupportedRecordingFormat) {
					t.Fatalf("create version %d error=%v, want unsupported format", test.version, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("create version %d: %v", test.version, err)
			}
			persisted, err := store.LoadRecordingHeader(context.Background(), header.ID)
			if err != nil || persisted.FormatVersion != ShardedArchiveFormatVersion {
				t.Fatalf("persisted format=%d err=%v, want %d", persisted.FormatVersion, err, ShardedArchiveFormatVersion)
			}
		})
	}
}

func TestCreateShardedRecordingWithSidecarRejectsExplicitOtherFormats(t *testing.T) {
	for _, version := range []int{1, ShardedArchiveFormatVersion + 1} {
		store, err := New(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		header := shardedArchiveHeaderForTest(fmt.Sprintf("b4e2f247ab144c6a850fd5c07fb1e6a%d", version))
		header.FormatVersion = version
		if err := store.CreateShardedRecordingWithSidecar(header, "archive/acquisition/context", map[string]string{"kind": "fixture"}); !errors.Is(err, ErrUnsupportedRecordingFormat) {
			t.Fatalf("create sidecar version %d error=%v, want unsupported format", version, err)
		}
	}
}

func shardedArchiveHeaderForTest(id string) *domain.Recording {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	return &domain.Recording{
		FormatVersion: ShardedArchiveFormatVersion, ShardedArchive: &domain.ShardedArchiveSummary{},
		ID: id, SourceSessionID: "session-" + strings.Repeat("a", 64),
		State: domain.StateRecording, CreatedAt: now, StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}},
	}
}

func TestShardedCoverageRevisionTracksSemanticStateChanges(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "c4e2f247ab144c6a850fd5c07fb1e6a3")
	now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	coverage := archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
		DiscontinuitySequence: 2, FromSequence: 71, ToSequence: 71,
		State: archiveindex.CoverageKnownMissing, ObservedAt: now,
		Reason: "declared missing", Kind: archiveindex.ObjectMedia,
	}
	first, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || first.ArchiveRevision != 1 || first.TimelineRevision != 0 || !first.Changed {
		t.Fatalf("first known_missing result=%+v err=%v, want archive 1/timeline 0/changed", first, err)
	}
	coverage.ObservedAt = now.Add(time.Minute)
	second, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || second.ArchiveRevision != 1 || second.TimelineRevision != 0 || second.Changed {
		t.Fatalf("same known_missing reobservation result=%+v err=%v, want no revision change", second, err)
	}
	failure := coverage
	failure.FromSequence, failure.ToSequence = 72, 72
	failure.State, failure.Reason, failure.ObservedAt = archiveindex.CoverageAcquisitionFailed, "temporary fetch failure", now.Add(2*time.Minute)
	third, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, failure)
	if err != nil || third.ArchiveRevision != 1 || third.TimelineRevision != 0 || third.Changed {
		t.Fatalf("acquisition_failed result=%+v err=%v, want durable retry state without archive revision", third, err)
	}
	fourth, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, failure)
	if err != nil || fourth.ArchiveRevision != 1 || fourth.TimelineRevision != 0 || fourth.Changed {
		t.Fatalf("repeated acquisition_failed result=%+v err=%v, want no revision change", fourth, err)
	}
	persisted, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || persisted.ArchiveRevision != 1 || persisted.TimelineRevision != 0 {
		t.Fatalf("persisted revisions archive=%d timeline=%d err=%v", persisted.ArchiveRevision, persisted.TimelineRevision, err)
	}
	coverageCount := 0
	if err := store.IterateShardedCoverage(ctx, header.ID, func(observation archiveindex.Coverage) error {
		coverageCount++
		if observation.State == archiveindex.CoverageAcquisitionFailed && observation.Reason != "temporary fetch failure" {
			t.Fatalf("recovery observation was not retained: %+v", observation)
		}
		return nil
	}); err != nil || coverageCount != 3 {
		t.Fatalf("coverage recovery observations count=%d err=%v, want known, reobserved, and acquisition-failed records", coverageCount, err)
	}
}

func TestShardedCoverageStateIndexPreservesRevisionSemantics(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "f4e2f247ab144c6a850fd5c07fb1e6a4")
	now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	coverage := archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
		DiscontinuitySequence: 2, FromSequence: 71, ToSequence: 71,
		State: archiveindex.CoverageAcquisitionFailed, ObservedAt: now,
		Reason: "temporary fetch failure", Kind: archiveindex.ObjectMedia,
	}
	appendCoverage := func(state archiveindex.CoverageState, observed time.Time) ShardedRevisionResult {
		t.Helper()
		coverage.State, coverage.ObservedAt = state, observed
		result, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
		if err != nil {
			t.Fatalf("append %s coverage: %v", state, err)
		}
		return result
	}
	if result := appendCoverage(archiveindex.CoverageAcquisitionFailed, now); result.ArchiveRevision != 0 || result.Changed {
		t.Fatalf("acquisition_failed result=%+v, want no canonical change", result)
	}
	if result := appendCoverage(archiveindex.CoverageKnownMissing, now.Add(time.Minute)); result.ArchiveRevision != 1 || !result.Changed {
		t.Fatalf("unknown->known_missing result=%+v, want revision 1", result)
	}
	if result := appendCoverage(archiveindex.CoverageKnownMissing, now.Add(2*time.Minute)); result.ArchiveRevision != 1 || result.Changed {
		t.Fatalf("known_missing reobservation result=%+v, want no-op", result)
	}
	if result := appendCoverage(archiveindex.CoveragePresent, now.Add(3*time.Minute)); result.ArchiveRevision != 2 || !result.Changed {
		t.Fatalf("known_missing->present result=%+v, want revision 2", result)
	}
	if result := appendCoverage(archiveindex.CoverageKnownMissing, now.Add(4*time.Minute)); result.ArchiveRevision != 2 || result.Changed {
		t.Fatalf("present->known_missing reobservation result=%+v, want no-op", result)
	}
	if result := appendCoverage(archiveindex.CoverageConflict, now.Add(5*time.Minute)); result.ArchiveRevision != 3 || !result.Changed {
		t.Fatalf("present->conflict result=%+v, want revision 3", result)
	}
	if result := appendCoverage(archiveindex.CoveragePresent, now.Add(6*time.Minute)); result.ArchiveRevision != 3 || result.Changed {
		t.Fatalf("conflict->present reobservation result=%+v, want no-op", result)
	}
}

func TestShardedCoverageStateIndexRebuildsAfterRestart(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "04e2f247ab144c6a850fd5c07fb1e6a4")
	now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	coverage := archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
		DiscontinuitySequence: 2, FromSequence: 71, ToSequence: 71,
		State: archiveindex.CoverageKnownMissing, ObservedAt: now, Kind: archiveindex.ObjectMedia,
	}
	first, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || first.ArchiveRevision != 1 || !first.Changed {
		t.Fatalf("first coverage result=%+v err=%v", first, err)
	}
	marker := filepath.Join(store.Root(), "recordings", header.ID, filepath.FromSlash(v2CoverageStateIndexMarkerPath+".json"))
	if err := os.Remove(marker); err != nil {
		t.Fatalf("remove derived index marker: %v", err)
	}
	reopened, err := New(store.Root())
	if err != nil {
		t.Fatal(err)
	}
	coverage.ObservedAt = now.Add(time.Minute)
	second, err := reopened.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || second.ArchiveRevision != 1 || second.Changed {
		t.Fatalf("recovered known_missing result=%+v err=%v, want no-op", second, err)
	}
	coverage.State, coverage.ObservedAt = archiveindex.CoveragePresent, now.Add(2*time.Minute)
	third, err := reopened.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || third.ArchiveRevision != 2 || !third.Changed {
		t.Fatalf("recovered present result=%+v err=%v, want revision 2", third, err)
	}
}

func TestShardedEmptyCoverageRecoverySkipsIndexMarkerUntilObservation(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "24e2f247ab144c6a850fd5c07fb1e6a4")
	marker := filepath.Join(store.Root(), "recordings", header.ID, filepath.FromSlash(v2CoverageStateIndexMarkerPath+".json"))

	if err := store.ReconcileShardedArchive(ctx, header.ID); err != nil {
		t.Fatalf("reconcile empty archive: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty archive coverage marker stat error=%v, want not found", err)
	}

	now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	coverage := archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
		DiscontinuitySequence: 2, FromSequence: 71, ToSequence: 71,
		State: archiveindex.CoverageKnownMissing, ObservedAt: now, Kind: archiveindex.ObjectMedia,
	}
	first, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || !first.Changed || first.ArchiveRevision != 1 {
		t.Fatalf("first coverage result=%+v err=%v, want changed archive revision 1", first, err)
	}
	var published v2CoverageStateIndexMarker
	if err := store.loadV2JSON(ctx, header.ID, v2CoverageStateIndexMarkerPath, archiveShardMaxBytes, &published); err != nil {
		t.Fatalf("first real observation did not initialize coverage index: %v", err)
	}
	if published.Version != 1 || !published.Ready {
		t.Fatalf("first observation marker=%+v, want ready v1", published)
	}

	local := store.StorageBackend.(*LocalFilesystemBackend)
	counter := &countingV2Backend{StorageBackend: local, local: local}
	store.StorageBackend = counter
	coverage.FromSequence, coverage.ToSequence = 72, 72
	coverage.ObservedAt = now.Add(time.Minute)
	if _, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage); err != nil {
		t.Fatalf("append after index initialization: %v", err)
	}
	stats := counter.snapshot()
	if stats.coverageStatePageReads != 1 || stats.coverageStatePageWrites != 1 || stats.listRequests != 0 {
		t.Fatalf("post-initialization coverage work reads=%d writes=%d list=%d, want 1/1/0", stats.coverageStatePageReads, stats.coverageStatePageWrites, stats.listRequests)
	}
}

func TestShardedCoverageStateIndexCachesCanonicalMediaState(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "14e2f247ab144c6a850fd5c07fb1e6a4")
	coordinate := archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
		DiscontinuitySequence: 2, Sequence: 71, Kind: archiveindex.ObjectMedia,
	}
	segment := domain.Segment{ID: "seg-00000000000000000001", TrackID: "main", ArchiveOrdinal: 1, Duration: 2}
	mediaRecord := V2MediaRecord{Coordinate: coordinate, Segment: segment, ClaimState: archiveindex.CoveragePresent}
	mediaPage := v2MediaPage{Version: 1, TrackID: "main", Number: 0, Entries: []V2MediaRecord{mediaRecord}}
	timelinePage := v2TimelinePage{
		Version: 1, TrackID: coordinate.TrackID, SourceEpoch: coordinate.SourceEpoch,
		DiscontinuitySequence: coordinate.DiscontinuitySequence, SequenceBucket: coordinate.Sequence / mediaShardMaxEntries,
		Kind: coordinate.Kind, Entries: []v2TimelineEntry{{Coordinate: coordinate, ArchiveOrdinal: 1, ID: segment.ID, Duration: segment.Duration}},
	}
	if err := store.saveV2JSON(ctx, header.ID, v2MediaPagePath(coordinate.TrackID, false, 0), mediaPage, archiveShardMaxBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.saveV2JSON(ctx, header.ID, v2TimelinePagePath(coordinate), timelinePage, archiveShardMaxBytes); err != nil {
		t.Fatal(err)
	}
	header.Tracks[coordinate.TrackID].MediaHighWater = 1
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	local := store.StorageBackend.(*LocalFilesystemBackend)
	counter := &countingV2Backend{StorageBackend: local, local: local}
	store.StorageBackend = counter
	now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	coverage := archiveindex.Coverage{
		SessionID: coordinate.SessionID, TrackID: coordinate.TrackID, SourceEpoch: coordinate.SourceEpoch,
		DiscontinuitySequence: coordinate.DiscontinuitySequence, FromSequence: coordinate.Sequence,
		ToSequence: coordinate.Sequence, State: archiveindex.CoverageKnownMissing, ObservedAt: now,
		Kind: archiveindex.ObjectMedia,
	}
	first, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || first.Changed || first.ArchiveRevision != 0 {
		t.Fatalf("known_missing over existing canonical media result=%+v err=%v, want no canonical revision", first, err)
	}
	counter.mu.Lock()
	counter.stats = countedV2IO{}
	counter.mu.Unlock()
	coverage.ObservedAt = now.Add(time.Minute)
	coverage.Reason = "reobserved"
	second, err := store.AppendShardedCoverageWithRevision(ctx, header.ID, coverage)
	if err != nil || second.Changed || second.ArchiveRevision != 0 {
		t.Fatalf("repeated known_missing over canonical media result=%+v err=%v, want no-op", second, err)
	}
	stats := counter.snapshot()
	if stats.coverageStatePageReads != 1 || stats.timelinePageReads != 0 || stats.mediaPageReads != 0 || stats.coverageStatePageWrites != 0 {
		t.Fatalf("repeated observation reread canonical media: stats=%+v, want only one derived index page read", stats)
	}
}

func TestShardedCoverageObservationTouchesBoundedIndexPages(t *testing.T) {
	readsByCollection := make(map[string]map[uint64]int)
	for _, history := range []uint64{1_000, 10_000} {
		for _, collection := range []string{"coverage", "gap"} {
			t.Run(fmt.Sprintf("%s_history_%d", collection, history), func(t *testing.T) {
				ctx := context.Background()
				store, header := newShardedArchiveTestStore(t, fmt.Sprintf("%032x", history+100))
				if collection == "coverage" {
					header.ShardedArchive.CoverageObservationCount = history
				} else {
					header.ShardedArchive.GapCount = history
				}
				if err := store.SaveRecordingHeader(ctx, header); err != nil {
					t.Fatal(err)
				}
				local := store.StorageBackend.(*LocalFilesystemBackend)
				counter := &countingV2Backend{StorageBackend: local, local: local}
				store.StorageBackend = counter
				now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
				candidate := archiveindex.Coverage{
					SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
					DiscontinuitySequence: 2, FromSequence: 0, ToSequence: 0,
					State: archiveindex.CoverageKnownMissing, ObservedAt: now, Kind: archiveindex.ObjectMedia,
				}
				if err := store.saveV2JSON(ctx, header.ID, v2CoverageStateIndexMarkerPath, v2CoverageStateIndexMarker{Version: 1, Ready: true}, archiveShardMaxBytes); err != nil {
					t.Fatal(err)
				}
				pageCount := (history + mediaShardMaxEntries - 1) / mediaShardMaxEntries
				for bucket := uint64(0); bucket < pageCount; bucket++ {
					page := v2CoverageStatePage{
						Version: 1, SessionID: candidate.SessionID, TrackID: candidate.TrackID,
						SourceEpoch: candidate.SourceEpoch, DiscontinuitySequence: candidate.DiscontinuitySequence,
						Kind: candidate.Kind, Bucket: bucket,
					}
					start, end := coverageBucketRange(archiveindex.Coverage{
						SessionID: candidate.SessionID, TrackID: candidate.TrackID, SourceEpoch: candidate.SourceEpoch,
						DiscontinuitySequence: candidate.DiscontinuitySequence, FromSequence: 0,
						ToSequence: history - 1, Kind: candidate.Kind,
					}, bucket)
					for sequence := start; ; sequence++ {
						page.States[sequence%mediaShardMaxEntries] = 1
						if sequence == end {
							break
						}
					}
					if err := store.saveV2JSON(ctx, header.ID, v2CoverageStatePagePath(candidate, bucket), page, archiveShardMaxBytes); err != nil {
						t.Fatal(err)
					}
				}
				candidate.FromSequence, candidate.ToSequence = history, history
				candidate.ObservedAt = now.Add(time.Minute)
				counter.mu.Lock()
				counter.stats = countedV2IO{}
				counter.mu.Unlock()
				var result ShardedRevisionResult
				var err error
				if collection == "coverage" {
					result, err = store.AppendShardedCoverageWithRevision(ctx, header.ID, candidate)
				} else {
					result, err = store.AppendShardedGapWithRevision(ctx, header.ID, domain.Gap{
						TrackID: candidate.TrackID, SourceEpoch: candidate.SourceEpoch,
						DiscontinuitySequence: candidate.DiscontinuitySequence,
						FromSequence:          history, ToSequence: history, DetectedAt: candidate.ObservedAt, Reason: "declared gap",
					})
				}
				if err != nil || !result.Changed {
					t.Fatalf("append %s at history %d result=%+v err=%v, want canonical change", collection, history, result, err)
				}
				stats := counter.snapshot()
				if stats.coverageStatePageReads != 1 || stats.coverageStatePageWrites != 1 || stats.listRequests != 0 {
					t.Fatalf("%s history %d coverage index reads=%d writes=%d list=%d; want 1/1/0 (sidecars=%d)", collection, history, stats.coverageStatePageReads, stats.coverageStatePageWrites, stats.listRequests, stats.sidecarReads)
				}
				if readsByCollection[collection] == nil {
					readsByCollection[collection] = make(map[uint64]int)
				}
				readsByCollection[collection][history] = stats.sidecarReads
			})
		}
	}
	for _, collection := range []string{"coverage", "gap"} {
		if readsByCollection[collection][1_000] != readsByCollection[collection][10_000] {
			t.Fatalf("%s history sidecar reads grew with history: 1k=%d 10k=%d", collection, readsByCollection[collection][1_000], readsByCollection[collection][10_000])
		}
	}
}

func TestShardedUnknownCoverageObservationDoesNotScanSequenceRange(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "a4e2f247ab144c6a850fd5c07fb1e6a1")
	local := store.StorageBackend.(*LocalFilesystemBackend)
	counter := &countingV2Backend{StorageBackend: local, local: local}
	store.StorageBackend = counter
	candidate := archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: 4,
		DiscontinuitySequence: 2, FromSequence: 0, ToSequence: 10_000,
		State: archiveindex.CoverageUnknown, Kind: archiveindex.ObjectMedia,
	}

	changed, updates, err := store.coverageObservationChanges(ctx, header.ID, header, candidate)
	if err != nil || changed || len(updates) != 0 {
		t.Fatalf("unknown coverage result changed=%t updates=%d err=%v, want no-op", changed, len(updates), err)
	}
	stats := counter.snapshot()
	if stats.coverageStatePageReads != 0 || stats.mediaPageReads != 0 || stats.timelinePageReads != 0 || stats.sidecarReads != 0 {
		t.Fatalf("unknown coverage observation touched archive history: %+v", stats)
	}
}

func TestSealedShardedArchiveRejectsMutationAndSkipsOrphanAdoption(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "d4e2f247ab144c6a850fd5c07fb1e6a3")
	header.State = domain.StateStopped
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	header.ShardedArchive.ClaimReconcilePending = true
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatalf("mark pending claim accounting: %v", err)
	}
	header.ArchiveSealed = true
	if err := store.SaveRecordingHeader(ctx, header); !errors.Is(err, ErrArchiveRecoveryPending) {
		t.Fatalf("seal with pending claim accounting=%v, want ErrArchiveRecoveryPending", err)
	}
	header.ArchiveSealed = false
	header.ShardedArchive.ClaimReconcilePending = false
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	header.ArchiveSealed = true
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatalf("seal empty v2 archive: %v", err)
	}

	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	claim := archiveindex.Claim{
		ID: "historical:" + segmentID + ":" + strings.Repeat("b", 64), Source: archiveindex.ClaimHistorical,
		AcquiredAt: time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC), PayloadPath: "tracks/main/objects/late.ts",
		Size: 1, SHA256: strings.Repeat("b", 64), Verification: archiveindex.VerificationVerified,
		Disposition: archiveindex.DispositionAccepted,
	}
	record := v2ScaleMediaRecord(coordinate, 1, claim.PayloadPath, claim.Size, claim.SHA256)
	record.SelectedClaim, record.ClaimState = &claim, archiveindex.CoveragePresent
	if err := store.AppendShardedMedia(ctx, header.ID, record); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("media append after seal=%v, want ErrArchiveSealed", err)
	}
	title := "late metadata"
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: claim.AcquiredAt, Title: &title}); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("metadata append after seal=%v, want ErrArchiveSealed", err)
	}
	if err := store.AppendShardedManifest(ctx, header.ID, domain.ManifestSnapshot{
		TrackID: "main", SourceURI: "https://source.invalid/live.m3u8", StoragePath: "manifests/main/late.m3u8",
		FetchedAt: claim.AcquiredAt, SHA256: strings.Repeat("c", 64), Size: 1,
	}); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("manifest append after seal=%v, want ErrArchiveSealed", err)
	}
	if err := store.AppendShardedGap(ctx, header.ID, domain.Gap{
		TrackID: "main", FromSequence: 2, ToSequence: 2, DetectedAt: claim.AcquiredAt, Reason: "late gap",
	}); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("gap append after seal=%v, want ErrArchiveSealed", err)
	}
	if err := store.AppendShardedCoverage(ctx, header.ID, archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", FromSequence: 2, ToSequence: 2,
		State: archiveindex.CoverageKnownMissing, ObservedAt: claim.AcquiredAt, Kind: archiveindex.ObjectMedia,
	}); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("coverage append after seal=%v, want ErrArchiveSealed", err)
	}
	if err := store.SaveShardedClaimSet(ctx, header.ID, V2ClaimSet{
		SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim},
	}); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("claim append after seal=%v, want ErrArchiveSealed", err)
	}
	if err := store.ReconcileShardedArchive(ctx, header.ID); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("explicit reconciliation after seal=%v, want ErrArchiveSealed", err)
	}

	// Model a durable page whose root publication failed immediately before a
	// seal. Startup must not promote that orphan into the sealed archive.
	pageNo, offset := mediaPageSlot(record.Segment.ArchiveOrdinal)
	if offset != 0 {
		t.Fatal("fixture must use first media slot")
	}
	orphan := v2MediaPage{Version: 1, TrackID: "main", Number: pageNo, Entries: []V2MediaRecord{record}}
	if err := writeShardedFixtureJSON(store, header.ID, v2MediaPagePath("main", false, pageNo), orphan); err != nil {
		t.Fatalf("write orphan page: %v", err)
	}
	root := store.Root()
	reopened, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reopened.LoadAll()
	if err != nil || len(rows) != 1 || !rows[0].ArchiveSealed || rows[0].ShardedArchive.MediaCount != 0 {
		t.Fatalf("sealed startup adopted orphan: rows=%+v err=%v", rows, err)
	}
	if _, err := reopened.LookupShardedMediaByCoordinate(ctx, header.ID, coordinate); !errors.Is(err, ErrNotFound) {
		t.Fatalf("sealed orphan lookup=%v, want ErrNotFound", err)
	}

	sealedHeader, err := reopened.LoadRecordingHeader(ctx, header.ID)
	if err != nil {
		t.Fatal(err)
	}
	sealedHeader.ArchiveSealed = false
	if err := reopened.SaveRecordingHeader(ctx, sealedHeader); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("unseal root update=%v, want ErrArchiveSealed", err)
	}
	sealedHeader.ArchiveSealed = true
	sealedHeader.Title = "changed after seal"
	if err := reopened.SaveRecordingHeader(ctx, sealedHeader); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("canonical root edit after seal=%v, want ErrArchiveSealed", err)
	}
	sealedHeader.Title = header.Title
	sealedHeader.State = domain.StateCompleted
	if err := reopened.SaveRecordingHeader(ctx, sealedHeader); err != nil {
		t.Fatalf("stopped-to-completed transition after seal: %v", err)
	}
}

func TestShardedManifestRevisionAdvancesOncePerNewSnapshot(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "e4e2f247ab144c6a850fd5c07fb1e6a5")
	now := time.Date(2026, 10, 9, 3, 0, 0, 0, time.UTC)
	snapshot := domain.ManifestSnapshot{
		TrackID: "main", SourceURI: "https://source.invalid/live.m3u8",
		StoragePath: "manifests/main/one.m3u8", FetchedAt: now,
		SHA256: strings.Repeat("a", 64), Size: 1,
	}
	if err := store.AppendShardedManifest(ctx, header.ID, snapshot); err != nil {
		t.Fatalf("append first manifest: %v", err)
	}
	first, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || first.ArchiveRevision != 1 || first.ShardedArchive.ManifestSnapshotCount != 1 {
		t.Fatalf("first manifest root revision=%d count=%d err=%v", first.ArchiveRevision, first.ShardedArchive.ManifestSnapshotCount, err)
	}
	if err := store.AppendShardedManifest(ctx, header.ID, snapshot); err != nil {
		t.Fatalf("retry identical manifest: %v", err)
	}
	duplicate, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || duplicate.ArchiveRevision != 1 || duplicate.ShardedArchive.ManifestSnapshotCount != 1 {
		t.Fatalf("identical retry root revision=%d count=%d err=%v, want unchanged", duplicate.ArchiveRevision, duplicate.ShardedArchive.ManifestSnapshotCount, err)
	}
	snapshot.StoragePath = "manifests/main/two.m3u8"
	snapshot.FetchedAt = now.Add(time.Minute)
	snapshot.SHA256 = strings.Repeat("b", 64)
	if err := store.AppendShardedManifest(ctx, header.ID, snapshot); err != nil {
		t.Fatalf("append second manifest: %v", err)
	}
	final, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || final.ArchiveRevision != 2 || final.ShardedArchive.ManifestSnapshotCount != 2 {
		t.Fatalf("second manifest root revision=%d count=%d err=%v, want revision 2/count 2", final.ArchiveRevision, final.ShardedArchive.ManifestSnapshotCount, err)
	}
}

func TestShardedLiveGapAdvancesTimelineOnlyWhenLiveProjectionChanges(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "d4e2f247ab144c6a850fd5c07fb1e6a4")
	track := header.Tracks["main"]
	track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 2, FirstPresentationOrdinal: 1}
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0,
		FromSequence: 10, ToSequence: 10, DetectedAt: time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC),
		Reason: "live source gap", LivePresentationOrdinal: 1, LiveDuration: 2,
	}
	first, err := store.AppendShardedGapWithRevision(ctx, header.ID, gap)
	if err != nil || first.ArchiveRevision != 1 || first.TimelineRevision != 1 || !first.Changed {
		t.Fatalf("first live gap result=%+v err=%v, want archive/timeline revision 1", first, err)
	}
	second, err := store.AppendShardedGapWithRevision(ctx, header.ID, gap)
	if err != nil || second.ArchiveRevision != 1 || second.TimelineRevision != 1 || second.Changed {
		t.Fatalf("identical live gap result=%+v err=%v, want no revision change", second, err)
	}
	updated := gap
	updated.LiveDuration = 3
	third, err := store.AppendShardedGapWithRevision(ctx, header.ID, updated)
	if err != nil || third.ArchiveRevision != 1 || third.TimelineRevision != 2 || !third.Changed {
		t.Fatalf("live gap presentation update=%+v err=%v, want timeline revision only", third, err)
	}
	fourth, err := store.AppendShardedGapWithRevision(ctx, header.ID, updated)
	if err != nil || fourth.ArchiveRevision != 1 || fourth.TimelineRevision != 2 || fourth.Changed {
		t.Fatalf("repeated live gap update=%+v err=%v, want no revision change", fourth, err)
	}
	root, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || root.ArchiveRevision != 1 || root.TimelineRevision != 2 {
		t.Fatalf("persisted revisions archive=%d timeline=%d err=%v", root.ArchiveRevision, root.TimelineRevision, err)
	}
}

func TestShardedLiveGapRootFailureRetryPreservesTimelineRevision(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "e4e2f247ab144c6a850fd5c07fb1e6a4")
	track := header.Tracks["main"]
	track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 2, FirstPresentationOrdinal: 1}
	track.LiveSlotHighWater = 1 // The reserved slot is already inside the published live window.
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0,
		FromSequence: 10, ToSequence: 10, DetectedAt: time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC),
		Reason: "live source gap", LivePresentationOrdinal: 1, LiveDuration: 2,
	}
	local := store.StorageBackend.(*LocalFilesystemBackend)
	injectedFailure := errors.New("injected live gap root publication failure")
	failing := &failBeforeV2RootSave{LocalFilesystemBackend: local, failure: injectedFailure, matches: func(candidate *domain.Recording) bool {
		return candidate.ShardedArchive.GapCount == 1 && candidate.TimelineRevision == 1
	}}
	store.StorageBackend = failing
	if _, err := store.AppendShardedGapWithRevision(ctx, header.ID, gap); !errors.Is(err, injectedFailure) {
		t.Fatalf("first live gap append error=%v, want injected root failure", err)
	}
	if !failing.fired {
		t.Fatal("injected root failure did not reach the live gap publication boundary")
	}
	store.StorageBackend = local
	partial, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || partial.ShardedArchive.GapCount != 0 || partial.ArchiveRevision != 0 || partial.TimelineRevision != 0 {
		t.Fatalf("failed root publication changed visible state: root=%+v err=%v", partial, err)
	}

	retried, err := store.AppendShardedGapWithRevision(ctx, header.ID, gap)
	if err != nil || !retried.Changed || retried.ArchiveRevision != 1 || retried.TimelineRevision != 1 {
		t.Fatalf("live gap retry result=%+v err=%v, want one archive/timeline revision", retried, err)
	}
	duplicate, err := store.AppendShardedGapWithRevision(ctx, header.ID, gap)
	if err != nil || duplicate.Changed || duplicate.ArchiveRevision != 1 || duplicate.TimelineRevision != 1 {
		t.Fatalf("live gap duplicate result=%+v err=%v, want stable revision", duplicate, err)
	}
}

func TestShardedLiveGapRootFailureRestartReconciliationPreservesTimelineRevision(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "f4e2f247ab144c6a850fd5c07fb1e6a9")
	track := header.Tracks["main"]
	track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 2, FirstPresentationOrdinal: 1}
	track.LiveSlotHighWater = 1 // The live slot is reserved before its gap page is published.
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0,
		FromSequence: 10, ToSequence: 10, DetectedAt: time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC),
		Reason: "live source gap", LivePresentationOrdinal: 1, LiveDuration: 2,
	}
	local := store.StorageBackend.(*LocalFilesystemBackend)
	injectedFailure := errors.New("injected live gap root publication failure before restart")
	failing := &failBeforeV2RootSave{LocalFilesystemBackend: local, failure: injectedFailure, matches: func(candidate *domain.Recording) bool {
		return candidate.ShardedArchive.GapCount == 1 && candidate.ArchiveRevision == 1 && candidate.TimelineRevision == 1
	}}
	store.StorageBackend = failing
	if _, err := store.AppendShardedGapWithRevision(ctx, header.ID, gap); !errors.Is(err, injectedFailure) {
		t.Fatalf("first live gap append error=%v, want injected root failure", err)
	}
	if !failing.fired {
		t.Fatal("injected root failure did not reach the live gap publication boundary")
	}
	store.StorageBackend = local
	slot, err := store.LookupShardedLiveSlot(ctx, header.ID, "main", 1)
	if err != nil || slot.Gap == nil || slot.Segment != nil {
		t.Fatalf("reserved live gap slot=%+v err=%v, want durable gap before root retry", slot, err)
	}

	reopened, err := New(local.root)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ReconcileShardedArchive(ctx, header.ID); err != nil {
		t.Fatalf("reconcile gap after restart: %v", err)
	}
	root, err := reopened.LoadRecordingHeader(ctx, header.ID)
	if err != nil || root.ShardedArchive.GapCount != 1 || root.ArchiveRevision != 1 || root.TimelineRevision != 1 {
		t.Fatalf("reconciled root gap=%d archive=%d timeline=%d err=%v", root.ShardedArchive.GapCount, root.ArchiveRevision, root.TimelineRevision, err)
	}
	if err := reopened.ReconcileShardedArchive(ctx, header.ID); err != nil {
		t.Fatalf("repeat gap reconciliation: %v", err)
	}
	root, err = reopened.LoadRecordingHeader(ctx, header.ID)
	if err != nil || root.ArchiveRevision != 1 || root.TimelineRevision != 1 {
		t.Fatalf("repeat reconciliation changed revisions: archive=%d timeline=%d err=%v", root.ArchiveRevision, root.TimelineRevision, err)
	}
}

func TestShardedRestartReconciliationAdvancesOrphanArchiveRevisionsOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 9, 4, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		append   func(*Store, string) error
		matches  func(*domain.Recording) bool
		asserted func(*testing.T, *Store, string)
	}{
		{
			name: "metadata",
			append: func(store *Store, id string) error {
				title := "durable orphan metadata"
				return store.AppendShardedMetadata(ctx, id, domain.MetadataRevision{ObservedAt: now, Title: &title})
			},
			matches: func(root *domain.Recording) bool { return root.ShardedArchive.MetadataRevisionCount == 1 },
			asserted: func(t *testing.T, store *Store, id string) {
				t.Helper()
				root, err := store.LoadRecordingHeader(ctx, id)
				if err != nil || root.ShardedArchive.MetadataRevisionCount != 1 || root.ArchiveRevision != 1 {
					t.Fatalf("metadata reconcile root=%+v err=%v", root, err)
				}
				count := 0
				if err := store.IterateShardedMetadata(ctx, id, func(domain.MetadataRevision) error { count++; return nil }); err != nil || count != 1 {
					t.Fatalf("metadata iteration count=%d err=%v", count, err)
				}
			},
		},
		{
			name: "manifest",
			append: func(store *Store, id string) error {
				return store.AppendShardedManifest(ctx, id, domain.ManifestSnapshot{
					TrackID: "main", SourceURI: "https://source.invalid/live.m3u8",
					StoragePath: "manifests/main/orphan.m3u8", FetchedAt: now,
					SHA256: strings.Repeat("a", 64), Size: 1,
				})
			},
			matches: func(root *domain.Recording) bool { return root.ShardedArchive.ManifestSnapshotCount == 1 },
			asserted: func(t *testing.T, store *Store, id string) {
				t.Helper()
				root, err := store.LoadRecordingHeader(ctx, id)
				if err != nil || root.ShardedArchive.ManifestSnapshotCount != 1 || root.ArchiveRevision != 1 {
					t.Fatalf("manifest reconcile root=%+v err=%v", root, err)
				}
				count := 0
				if err := store.IterateShardedManifests(ctx, id, func(domain.ManifestSnapshot) error { count++; return nil }); err != nil || count != 1 {
					t.Fatalf("manifest iteration count=%d err=%v", count, err)
				}
			},
		},
		{
			name: "coverage",
			append: func(store *Store, id string) error {
				return store.AppendShardedCoverage(ctx, id, archiveindex.Coverage{
					SessionID: "session-" + strings.Repeat("a", 64), TrackID: "main", SourceEpoch: 2,
					DiscontinuitySequence: 1, FromSequence: 77, ToSequence: 77,
					State: archiveindex.CoverageKnownMissing, ObservedAt: now, Reason: "declared missing", Kind: archiveindex.ObjectMedia,
				})
			},
			matches: func(root *domain.Recording) bool { return root.ShardedArchive.CoverageObservationCount == 1 },
			asserted: func(t *testing.T, store *Store, id string) {
				t.Helper()
				root, err := store.LoadRecordingHeader(ctx, id)
				if err != nil || root.ShardedArchive.CoverageObservationCount != 1 || root.ArchiveRevision != 1 {
					t.Fatalf("coverage reconcile root=%+v err=%v", root, err)
				}
				count := 0
				if err := store.IterateShardedCoverage(ctx, id, func(archiveindex.Coverage) error { count++; return nil }); err != nil || count != 1 {
					t.Fatalf("coverage iteration count=%d err=%v", count, err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, header := newShardedArchiveTestStore(t, "f4e2f247ab144c6a850fd5c07fb1e6a8")
			local := store.StorageBackend.(*LocalFilesystemBackend)
			failure := errors.New("injected orphan root publication failure")
			store.StorageBackend = &failBeforeV2RootSave{LocalFilesystemBackend: local, failure: failure, matches: test.matches}
			if err := test.append(store, header.ID); !errors.Is(err, failure) {
				t.Fatalf("append error=%v, want injected root failure", err)
			}
			store.StorageBackend = local
			before, err := store.LoadRecordingHeader(ctx, header.ID)
			if err != nil || before.ArchiveRevision != 0 {
				t.Fatalf("root before restart archive revision=%d err=%v", before.ArchiveRevision, err)
			}
			reopened, err := New(local.root)
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.ReconcileShardedArchive(ctx, header.ID); err != nil {
				t.Fatalf("reconcile orphan after restart: %v", err)
			}
			test.asserted(t, reopened, header.ID)
			if err := reopened.ReconcileShardedArchive(ctx, header.ID); err != nil {
				t.Fatalf("repeat reconciliation: %v", err)
			}
			test.asserted(t, reopened, header.ID)
		})
	}
}

func TestShardedClaimPagesPackAndRollover(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	header := v2ScaleHeader("ab42f247ab144c6a850fd5c07fb1e6a1")
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	const wantSets = claimPageMaxEntries + 8
	coordinates := make([]archiveindex.Coordinate, 0, wantSets)
	var bucket uint16
	for sequence := uint64(1); len(coordinates) < wantSets; sequence++ {
		coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: sequence, Kind: archiveindex.ObjectMedia}
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		candidateBucket := v2ClaimBucket(segmentID)
		if len(coordinates) == 0 {
			bucket = candidateBucket
		} else if candidateBucket != bucket {
			continue
		}
		claim := testV2Claim(coordinate, "historical:"+segmentID+":"+strings.Repeat("b", 64), time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
		claim.Source = archiveindex.ClaimHistorical
		set := V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID, State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim}}
		if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
			t.Fatalf("save supplemental claim %d: %v", len(coordinates), err)
		}
		coordinates = append(coordinates, coordinate)
	}
	pageCount, entryCount := 0, 0
	if err := store.listV2(ctx, header.ID, "archive/v2/claims/", func(relative string) error {
		pageCount++
		var page v2ClaimPage
		if err := store.loadV2JSON(ctx, header.ID, trimJSONSuffix(relative), archiveShardMaxBytes, &page); err != nil {
			return err
		}
		entryCount += len(page.Entries)
		if len(page.Entries) == 0 || len(page.Entries) > claimPageMaxEntries {
			return ErrShardedArchiveInvalid
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if pageCount != 2 || entryCount != wantSets {
		t.Fatalf("claim page packing pages=%d entries=%d; want 2 pages / %d entries", pageCount, entryCount, wantSets)
	}
	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, coordinate := range coordinates {
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		set, err := restarted.LoadShardedClaimSet(ctx, header.ID, segmentID)
		if err != nil || set.SegmentID != segmentID {
			t.Fatalf("claim lookup after restart id=%s set=%#v err=%v", segmentID, set, err)
		}
	}
}

func TestShardedClaimPublicationRetryReconcilesVisibleCount(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "b5e2f247ab144c6a850fd5c07fb1e6a2")
	payload := []byte("claim accounting retry")
	payloadPath := "tracks/main/objects/claim.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if info, err := store.StorageBackend.StatPayload(header.ID, payloadPath); err != nil || !info.Regular || info.Size != int64(len(payload)) {
		t.Fatalf("claim fixture payload was not durable: info=%#v err=%v", info, err)
	}
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	record := v2ScaleMediaRecord(coordinate, 1, payloadPath, int64(len(payload)), stored.SHA256)
	if err := store.AppendShardedMedia(ctx, header.ID, record); err != nil {
		t.Fatal(err)
	}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	claim := archiveindex.Claim{
		ID: "live_origin:" + segmentID + ":" + stored.SHA256, Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC), PayloadPath: payloadPath,
		Size: int64(len(payload)), SHA256: stored.SHA256, Verification: archiveindex.VerificationVerified,
		Disposition: archiveindex.DispositionAccepted,
	}
	set := V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID, State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim}}
	backend := store.StorageBackend
	injectedFailure := errors.New("injected claim sidecar response loss")
	store.StorageBackend = &failAfterV2SidecarWrite{StorageBackend: backend, relative: v2ClaimPath(segmentID), failure: injectedFailure}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); !errors.Is(err, injectedFailure) {
		t.Fatalf("claim publication error=%v, want injected response loss", err)
	}
	store.StorageBackend = backend
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
		t.Fatalf("retry visible claim publication: %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		root, err := store.LoadRecordingHeader(ctx, header.ID)
		if err != nil || root.ShardedArchive.ClaimCount != 1 || root.ShardedArchive.ClaimReconcilePending {
			t.Fatalf("claim root after retry=%#v err=%v", root.ShardedArchive, err)
		}
		loaded, err := store.LoadShardedClaimSet(ctx, header.ID, segmentID)
		if err != nil || loaded.CountedClaims != 1 || len(loaded.Claims) != 1 {
			t.Fatalf("claim set after retry=%#v err=%v", loaded, err)
		}
		if attempt == 0 {
			if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
				t.Fatalf("duplicate claim retry: %v", err)
			}
		}
	}
}

func TestSupplementalClaimMutationAdvancesArchiveRevisionOnce(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "b6e2f247ab144c6a850fd5c07fb1e6a3")
	header.ArchiveRevision = 1
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	selectedPath := "tracks/main/objects/selected.ts"
	selectedBytes := []byte("selected")
	selectedPayload, err := store.StorageBackend.SavePayloadExact(header.ID, selectedPath, bytes.NewReader(selectedBytes), 1024, int64(len(selectedBytes)))
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC)
	selected := archiveindex.Claim{
		ID: "live_origin:" + segmentID + ":" + selectedPayload.SHA256, Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: observedAt, PayloadPath: selectedPath, Size: selectedPayload.Size, SHA256: selectedPayload.SHA256,
		Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
	}
	record := v2ScaleMediaRecord(coordinate, 1, selectedPath, selectedPayload.Size, selectedPayload.SHA256)
	record.SelectedClaim, record.ClaimState = &selected, archiveindex.CoveragePresent
	if err := store.AppendShardedMedia(ctx, header.ID, record); err != nil {
		t.Fatal(err)
	}

	alternate := selected
	alternate.ID = "historical:" + segmentID + ":" + selectedPayload.SHA256
	alternate.Source = archiveindex.ClaimHistorical
	alternate.AcquiredAt = observedAt.Add(time.Second)
	set := V2ClaimSet{
		SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: selected.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{selected, alternate},
	}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
		t.Fatalf("append supplemental claim: %v", err)
	}
	root, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || root.ArchiveRevision != 2 || root.ShardedArchive.ClaimCount != 2 {
		t.Fatalf("supplemental claim root revision=%d count=%d err=%v, want revision 2 / two total claims", root.ArchiveRevision, root.ShardedArchive.ClaimCount, err)
	}
	if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
		t.Fatalf("duplicate supplemental claim retry: %v", err)
	}
	root, err = store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || root.ArchiveRevision != 2 || root.ShardedArchive.ClaimCount != 2 {
		t.Fatalf("duplicate claim changed root revision=%d count=%d err=%v", root.ArchiveRevision, root.ShardedArchive.ClaimCount, err)
	}
}

func TestShardedClaimCountIntentIsClearedBetweenCoordinates(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "b7e2f247ab144c6a850fd5c07fb1e6a3")
	for sequence := uint64(1); sequence <= 2; sequence++ {
		coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: sequence, Kind: archiveindex.ObjectMedia}
		payload := []byte(fmt.Sprintf("claim-count-%d", sequence))
		payloadPath := fmt.Sprintf("tracks/main/objects/claim-%d.ts", sequence)
		stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		record := v2ScaleMediaRecord(coordinate, sequence, payloadPath, stored.Size, stored.SHA256)
		if err := store.AppendShardedMedia(ctx, header.ID, record); err != nil {
			t.Fatalf("append media coordinate %d: %v", sequence, err)
		}
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		claim := archiveindex.Claim{
			ID: "live_origin:" + segmentID + ":" + stored.SHA256, Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt: time.Date(2026, 10, 9, 2, int(sequence), 0, 0, time.UTC), PayloadPath: payloadPath,
			Size: stored.Size, SHA256: stored.SHA256, Verification: archiveindex.VerificationVerified,
			Disposition: archiveindex.DispositionAccepted,
		}
		set := V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID, State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim}}
		if err := store.SaveShardedClaimSet(ctx, header.ID, set); err != nil {
			t.Fatalf("persist claims for coordinate %d: %v", sequence, err)
		}
	}
	root, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || root.ShardedArchive.ClaimCount != 2 || root.ShardedArchive.ClaimReconcilePending {
		t.Fatalf("claim count root=%#v err=%v, want two complete claims", root.ShardedArchive, err)
	}
	var intent v2ClaimCountIntent
	if err := store.loadV2JSON(ctx, header.ID, v2ClaimCountIntentPath(), archiveShardMaxBytes, &intent); err != nil || !emptyV2ClaimCountIntent(intent) {
		t.Fatalf("resolved fixed claim intent=%#v err=%v, want empty bounded sentinel", intent, err)
	}
	count := 0
	if err := store.IterateShardedClaimSets(ctx, header.ID, func(V2ClaimSet) error { count++; return nil }); err != nil || count != 2 {
		t.Fatalf("claim iterator count=%d err=%v, want two", count, err)
	}
}

func TestShardedOrphanClaimDoesNotBlockDifferentCoordinateAfterRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "b6e2f247ab144c6a850fd5c07fb1e6a2"
	header := v2ScaleHeader(id)
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}

	// Claim sidecars are keyed by logical coordinate identity, not ArchiveOrdinal.
	// This models a crash after claim durability but before media publication.
	orphanCoordinate := archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: 100, Kind: archiveindex.ObjectMedia,
	}
	orphanID, err := archiveindex.SegmentIdentity(orphanCoordinate)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 4, 0, 0, 0, time.UTC)
	orphanClaim := testV2Claim(orphanCoordinate, "historical:"+orphanID+":"+strings.Repeat("b", 64), now)
	orphanSet := V2ClaimSet{
		SegmentID: orphanID, Coordinate: orphanCoordinate, SelectedClaimID: orphanClaim.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{orphanClaim},
	}
	if err := store.SaveShardedClaimSet(ctx, id, orphanSet); err != nil {
		t.Fatalf("save uncommitted claim candidate: %v", err)
	}

	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := restarted.LoadAll()
	if err != nil || len(loaded) != 1 || loaded[0].ShardedArchive.ClaimCount != 0 {
		t.Fatalf("restart exposed orphan claim: rows=%d err=%v", len(loaded), err)
	}
	visibleSets := 0
	if err := restarted.IterateShardedClaimSets(ctx, id, func(V2ClaimSet) error {
		visibleSets++
		return nil
	}); err != nil || visibleSets != 0 {
		t.Fatalf("orphan claim iteration count=%d err=%v", visibleSets, err)
	}
	if _, err := restarted.LoadShardedClaimSet(ctx, id, orphanID); err != nil {
		t.Fatalf("durable orphan candidate unexpectedly disappeared: %v", err)
	}

	// Different coordinate can safely reuse next ArchiveOrdinal. Its identity
	// maps to a different claim sidecar, so orphan cannot shadow new source data.
	payload := []byte("replacement coordinate payload")
	payloadPath := "tracks/main/objects/replacement.ts"
	stored, err := restarted.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	visibleCoordinate := archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia,
	}
	visibleRecord := v2ScaleMediaRecord(visibleCoordinate, 1, payloadPath, int64(len(payload)), stored.SHA256)
	if err := restarted.AppendShardedMedia(ctx, id, visibleRecord); err != nil {
		t.Fatalf("publish different coordinate at reused archive ordinal: %v", err)
	}
	visibleID, err := archiveindex.SegmentIdentity(visibleCoordinate)
	if err != nil {
		t.Fatal(err)
	}
	visibleClaim := testV2Claim(visibleCoordinate, "live_origin:"+visibleID+":"+stored.SHA256, now.Add(time.Second))
	visibleSet := V2ClaimSet{
		SegmentID: visibleID, Coordinate: visibleCoordinate, SelectedClaimID: visibleClaim.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{visibleClaim},
	}
	if err := restarted.SaveShardedClaimSet(ctx, id, visibleSet); err != nil {
		t.Fatalf("save visible coordinate claims: %v", err)
	}
	visibleSets = 0
	if err := restarted.IterateShardedClaimSets(ctx, id, func(set V2ClaimSet) error {
		visibleSets++
		if set.Coordinate != visibleCoordinate {
			return fmt.Errorf("unexpected visible claim coordinate: %#v", set.Coordinate)
		}
		return nil
	}); err != nil || visibleSets != 1 {
		t.Fatalf("visible claim iteration count=%d err=%v", visibleSets, err)
	}
	finalHeader, err := restarted.LoadRecordingHeader(ctx, id)
	if err != nil || finalHeader.ShardedArchive.ClaimCount != 1 {
		t.Fatalf("visible claim count=%v err=%v", finalHeader, err)
	}
}

func TestShardedLiveGapSlotPromotesAndRepairsOnIdenticalMediaRetry(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "c4e2f247ab144c6a850fd5c07fb1e6a2")
	payload := []byte("gap-fill-payload")
	payloadPath := "tracks/main/objects/gap-fill.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	coordinate := archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: 108, Kind: archiveindex.ObjectMedia,
	}
	track := header.Tracks["main"]
	track.LivePresentation = &domain.LivePresentationState{
		NextOrdinal: 9, FirstPresentationOrdinal: 1,
		HasEpochMapping: true, MappedSourceEpoch: 0, SourceSequenceBase: 100,
		PresentationOrdinalBase: 1, MaxSourceSequence: 107,
	}
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	gap := domain.Gap{
		TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0,
		FromSequence: 108, ToSequence: 108, Reason: "source_gap", DetectedAt: now,
		LivePresentationOrdinal: 9,
	}
	if err := store.AppendShardedGap(ctx, header.ID, gap); err != nil {
		t.Fatalf("append explicit gap reservation: %v", err)
	}
	segment := domain.Segment{
		ID: "seg-00000000000000000001", TrackID: "main", Sequence: 108,
		ArchiveOrdinal: 1, LivePresentationOrdinal: 9, SourceURI: "https://example.invalid/live/108.ts", StoragePath: payloadPath,
		PayloadSize: int64(len(payload)), SHA256: stored.SHA256, Duration: 2,
	}
	record := V2MediaRecord{Coordinate: coordinate, Segment: segment, IndexOrdinal: 1}
	header, err = store.LoadRecordingHeader(ctx, header.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PublishShardedMedia(ctx, header, record); err != nil {
		t.Fatalf("publish media into reserved gap slot: %v", err)
	}
	assertLiveGapPromotion := func(phase string) {
		t.Helper()
		var found bool
		err := store.IterateShardedLiveSlots(ctx, header.ID, "main", 12, func(slot V2LiveSlot) error {
			if slot.Ordinal != 9 {
				return nil
			}
			found = true
			if slot.Segment == nil || slot.Gap != nil || slot.Segment.ID != segment.ID {
				return fmt.Errorf("slot 9 after %s = %#v, want media %s", phase, slot, segment.ID)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("iterate live slots after %s: %v", phase, err)
		}
		if !found {
			t.Fatalf("slot 9 missing after %s", phase)
		}
	}
	assertLiveGapPromotion("initial publication")

	// Simulate an older uncertain projection write leaving the gap page stale
	// after the canonical media/header became visible. An identical retry must
	// repair the live index without changing the canonical object.
	pageNo, offset := pageSlot(9)
	pagePath := v2LivePagePath("main", pageNo)
	var page v2LivePage
	if err := store.loadV2JSON(ctx, header.ID, pagePath, archiveShardMaxBytes, &page); err != nil {
		t.Fatal(err)
	}
	page.Entries[offset] = v2LiveIndexEntry{
		PresentationOrdinal: 9,
		Coordinate:          archiveindex.Coordinate{TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 0, Sequence: 108, Kind: archiveindex.ObjectMedia},
		Gap:                 func() *domain.Gap { copy := gap; return &copy }(),
	}
	if err := store.saveV2JSON(ctx, header.ID, pagePath, page, archiveShardMaxBytes); err != nil {
		t.Fatal(err)
	}
	header, err = store.LoadRecordingHeader(ctx, header.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PublishShardedMedia(ctx, header, record); err != nil {
		t.Fatalf("retry identical media and repair live slot: %v", err)
	}
	assertLiveGapPromotion("identical retry")
}

func TestShardedLiveIndexUsesOnlyRootProvenReservations(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "d5e2f247ab144c6a850fd5c07fb1e6a3")
	header.Tracks["main"].LivePresentation = &domain.LivePresentationState{NextOrdinal: 4}
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	payload := []byte("live-order-payload")
	payloadPath := "tracks/main/objects/live-order.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	appendLive := func(sequence, archiveOrdinal, liveOrdinal uint64) error {
		record := v2ScaleMediaRecord(archiveindex.Coordinate{
			SessionID: header.SourceSessionID, TrackID: "main", Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}, archiveOrdinal, payloadPath, int64(len(payload)), stored.SHA256)
		record.Segment.LivePresentationOrdinal = liveOrdinal
		return store.AppendShardedMedia(ctx, header.ID, record)
	}
	if err := appendLive(100, 1, 1); err != nil {
		t.Fatalf("append first live slot: %v", err)
	}
	if err := appendLive(102, 2, 3); err != nil {
		t.Fatalf("append later live slot with root-proven reservation: %v", err)
	}
	var tail []V2LiveSlot
	if err := store.IterateShardedLiveSlots(ctx, header.ID, "main", 12, func(slot V2LiveSlot) error {
		tail = append(tail, slot)
		return nil
	}); err != nil || len(tail) != 1 || tail[0].Ordinal != 1 || tail[0].Segment == nil {
		t.Fatalf("tail compressed reserved slot: %#v err=%v", tail, err)
	}
	if err := appendLive(101, 3, 2); err != nil {
		t.Fatalf("fill reserved live slot: %v", err)
	}
	tail = nil
	if err := store.IterateShardedLiveSlots(ctx, header.ID, "main", 12, func(slot V2LiveSlot) error {
		tail = append(tail, slot)
		return nil
	}); err != nil || len(tail) != 3 {
		t.Fatalf("filled tail count=%d err=%v", len(tail), err)
	}
	for index, slot := range tail {
		if slot.Ordinal != uint64(index+1) || slot.Segment == nil || slot.Segment.Sequence != uint64(100+index) {
			t.Fatalf("tail slot %d = %#v", index, slot)
		}
	}
	badHeader := v2ScaleHeader("d6e2f247ab144c6a850fd5c07fb1e6a3")
	badHeader.Tracks["main"].LivePresentation = &domain.LivePresentationState{NextOrdinal: 4}
	bad := v2ScaleMediaRecord(archiveindex.Coordinate{
		SessionID: badHeader.SourceSessionID, TrackID: "main", Sequence: 105, Kind: archiveindex.ObjectMedia,
	}, 1, payloadPath, int64(len(payload)), stored.SHA256)
	bad.Segment.LivePresentationOrdinal = 5
	if err := store.writeLiveIndex(ctx, badHeader.ID, bad, badHeader.Tracks["main"]); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("unproven live hole error=%v, want invalid", err)
	}
}

func TestShardedMediaPageFailureBeforePublicationKeepsPayloadRetryable(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "d7e2f247ab144c6a850fd5c07fb1e6a3")
	payload := []byte("payload survives unpublished metadata")
	payloadPath := "tracks/main/objects/retry.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	record := v2ScaleMediaRecord(archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia,
	}, 1, payloadPath, int64(len(payload)), stored.SHA256)
	backend := store.StorageBackend
	injectedFailure := errors.New("injected failure before atomic page publication")
	store.StorageBackend = &failBeforeV2SidecarWrite{
		StorageBackend: backend,
		relative:       v2MediaPagePath("main", false, 0),
		failure:        injectedFailure,
	}
	if err := store.AppendShardedMedia(ctx, header.ID, record); !errors.Is(err, injectedFailure) {
		t.Fatalf("media append error=%v, want injected pre-publication failure", err)
	}
	store.StorageBackend = backend
	if _, err := store.LookupShardedMediaByArchiveOrdinal(ctx, header.ID, "main", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unpublished media became visible: %v", err)
	}
	rootAfterFailure, err := store.LoadRecordingHeader(ctx, header.ID)
	if err != nil || rootAfterFailure.Tracks["main"].MediaHighWater != 0 {
		t.Fatalf("root after pre-publication failure high-water=%d err=%v", rootAfterFailure.Tracks["main"].MediaHighWater, err)
	}
	info, err := store.StorageBackend.StatPayload(header.ID, payloadPath)
	if err != nil || !info.Regular || info.Size != int64(len(payload)) {
		t.Fatalf("durable payload after metadata failure info=%#v err=%v", info, err)
	}
	if err := store.AppendShardedMedia(ctx, header.ID, record); err != nil {
		t.Fatalf("retry same accepted payload after unpublished metadata: %v", err)
	}
	visible, err := store.LookupShardedMediaByArchiveOrdinal(ctx, header.ID, "main", 1)
	if err != nil || visible.Segment.SHA256 != stored.SHA256 {
		t.Fatalf("retried media=%#v err=%v", visible, err)
	}
}

func TestShardedArchiveObjectStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	header := v2ScaleHeader("d8e2f247ab144c6a850fd5c07fb1e6a3")
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	payload := []byte("object-store-v2-payload")
	payloadPath := "tracks/main/objects/object-store.ts"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if info, err := store.StorageBackend.StatPayload(header.ID, payloadPath); err != nil || !info.Regular || info.Size != int64(len(payload)) {
		t.Fatalf("object-store payload was not durable: info=%#v err=%v", info, err)
	}
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	record := v2ScaleMediaRecord(coordinate, 1, payloadPath, int64(len(payload)), stored.SHA256)
	if err := store.PublishShardedMedia(ctx, header, record); err != nil {
		t.Fatalf("publish object-store media: %v", err)
	}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	claim := archiveindex.Claim{
		ID: "live_origin:" + segmentID + ":" + stored.SHA256, Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC), PayloadPath: payloadPath,
		Size: int64(len(payload)), SHA256: stored.SHA256, Verification: archiveindex.VerificationVerified,
		Disposition: archiveindex.DispositionAccepted,
	}
	if err := store.SaveShardedClaimSet(ctx, header.ID, V2ClaimSet{
		SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID,
		State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim},
	}); err != nil {
		t.Fatalf("save object-store claim set: %v", err)
	}
	title := "object store title"
	now := time.Date(2026, 10, 9, 1, 1, 0, 0, time.UTC)
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: now, Title: &title}); err != nil {
		t.Fatalf("append object-store metadata: %v", err)
	}
	if err := store.AppendShardedManifest(ctx, header.ID, domain.ManifestSnapshot{
		TrackID: "main", SourceURI: "https://example.invalid/live.m3u8", StoragePath: "manifests/main/1.m3u8",
		FetchedAt: now, SHA256: strings.Repeat("a", 64), Size: 1,
	}); err != nil {
		t.Fatalf("append object-store manifest: %v", err)
	}
	if err := store.AppendShardedGap(ctx, header.ID, domain.Gap{TrackID: "main", FromSequence: 2, ToSequence: 2, DetectedAt: now, Reason: "source_gap"}); err != nil {
		t.Fatalf("append object-store gap: %v", err)
	}
	if err := store.AppendShardedCoverage(ctx, header.ID, archiveindex.Coverage{
		SessionID: header.SourceSessionID, TrackID: "main", FromSequence: 1, ToSequence: 2,
		State: archiveindex.CoveragePresent, ObservedAt: now, Kind: archiveindex.ObjectMedia,
	}); err != nil {
		t.Fatalf("append object-store coverage: %v", err)
	}

	reopened, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reopened.LoadAll()
	if err != nil || len(rows) != 1 || rows[0].ID != header.ID || rows[0].FormatVersion != ShardedArchiveFormatVersion {
		t.Fatalf("object-store LoadAll rows=%#v err=%v", rows, err)
	}
	lookedUp, err := reopened.LookupShardedMediaByCoordinate(ctx, header.ID, coordinate)
	if err != nil || lookedUp.Segment.SHA256 != stored.SHA256 {
		t.Fatalf("object-store media lookup=%#v err=%v", lookedUp, err)
	}
	for _, iterate := range []struct {
		name string
		fn   func() (int, error)
	}{
		{name: "timeline", fn: func() (int, error) {
			count := 0
			err := reopened.IterateShardedTimeline(ctx, header.ID, "main", func(V2MediaRecord) error { count++; return nil })
			return count, err
		}},
		{name: "claims", fn: func() (int, error) {
			count := 0
			err := reopened.IterateShardedClaimSets(ctx, header.ID, func(V2ClaimSet) error { count++; return nil })
			return count, err
		}},
		{name: "metadata", fn: func() (int, error) {
			count := 0
			err := reopened.IterateShardedMetadata(ctx, header.ID, func(domain.MetadataRevision) error { count++; return nil })
			return count, err
		}},
		{name: "manifests", fn: func() (int, error) {
			count := 0
			err := reopened.IterateShardedManifests(ctx, header.ID, func(domain.ManifestSnapshot) error { count++; return nil })
			return count, err
		}},
		{name: "gaps", fn: func() (int, error) {
			count := 0
			err := reopened.IterateShardedGaps(ctx, header.ID, func(domain.Gap) error { count++; return nil })
			return count, err
		}},
		{name: "coverage", fn: func() (int, error) {
			count := 0
			err := reopened.IterateShardedCoverage(ctx, header.ID, func(archiveindex.Coverage) error { count++; return nil })
			return count, err
		}},
	} {
		t.Run(iterate.name, func(t *testing.T) {
			count, err := iterate.fn()
			if err != nil || count != 1 {
				t.Fatalf("object-store %s count=%d err=%v", iterate.name, count, err)
			}
		})
	}
	loadedSet, err := reopened.LoadShardedClaimSet(ctx, header.ID, segmentID)
	if err != nil || loadedSet.CountedClaims != 1 || loadedSet.SelectedClaimID != claim.ID {
		t.Fatalf("object-store claim lookup=%#v err=%v", loadedSet, err)
	}
}

func TestShardedRolloverRestartReconcilesDurableMediaAndClaim(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "d5e2f247ab144c6a850fd5c07fb1e6a2"
	header := v2ScaleHeader(id)
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	payload := []byte("rollover-reconcile-payload")
	payloadPath := "tracks/main/objects/rollover.ts"
	stored, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	page := v2MediaPage{Version: 1, TrackID: "main", Number: 0, Entries: make([]V2MediaRecord, 0, mediaShardMaxEntries)}
	for ordinal := uint64(1); ordinal <= mediaShardMaxEntries; ordinal++ {
		coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: ordinal, Kind: archiveindex.ObjectMedia}
		record := v2ScaleMediaRecord(coordinate, ordinal, payloadPath, int64(len(payload)), stored.SHA256)
		page.Entries = append(page.Entries, record)
		if err := store.writeV2TimelineEntry(ctx, id, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeShardedFixtureJSON(store, id, v2MediaPagePath("main", false, 0), page); err != nil {
		t.Fatal(err)
	}
	track := header.Tracks["main"]
	track.MediaCount = mediaShardMaxEntries
	track.MediaHighWater = mediaShardMaxEntries
	track.NextArchiveOrdinal = mediaShardMaxEntries + 1
	track.PayloadBytes = mediaShardMaxEntries * uint64(len(payload))
	track.DurationSeconds = float64(mediaShardMaxEntries) * 2
	header.ShardedArchive.MediaCount = mediaShardMaxEntries
	header.ShardedArchive.PayloadBytes = track.PayloadBytes
	header.ShardedArchive.DurationSeconds = track.DurationSeconds
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}

	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: mediaShardMaxEntries + 1, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 1, 2, 3, 0, time.UTC)
	claim := testV2Claim(coordinate, "historical:"+segmentID+":"+stored.SHA256, now)
	claimSet := V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID, State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim}}
	if err := store.SaveShardedClaimSet(ctx, id, claimSet); err != nil {
		t.Fatalf("save claim before media visibility: %v", err)
	}
	record := v2ScaleMediaRecord(coordinate, mediaShardMaxEntries+1, payloadPath, int64(len(payload)), stored.SHA256)
	backend := store.StorageBackend
	injectedFailure := errors.New("injected post-publication failure")
	store.StorageBackend = &failAfterV2SidecarWrite{StorageBackend: backend, relative: v2MediaPagePath("main", false, 1), failure: injectedFailure}
	if err := store.PublishShardedMedia(ctx, header, record); !errors.Is(err, injectedFailure) {
		t.Fatalf("publish media failure=%v, want injected post-publication failure", err)
	}
	store.StorageBackend = backend

	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := restarted.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Tracks["main"].MediaHighWater != mediaShardMaxEntries+1 || rows[0].ShardedArchive.ClaimCount != 1 {
		t.Fatalf("restart reconciliation header=%#v err=%v", rows, err)
	}
	loadedSet, err := restarted.LoadShardedClaimSet(ctx, id, segmentID)
	if err != nil || loadedSet.CountedClaims != 1 || loadedSet.SelectedClaimID != claim.ID {
		t.Fatalf("restart claim reconciliation set=%#v err=%v", loadedSet, err)
	}
	count := uint64(0)
	if err := restarted.IterateShardedMedia(ctx, id, "main", func(got V2MediaRecord) error {
		count++
		if got.Segment.ArchiveOrdinal != count {
			return fmt.Errorf("reconciled archive ordinal %d at position %d", got.Segment.ArchiveOrdinal, count)
		}
		return nil
	}); err != nil || count != mediaShardMaxEntries+1 {
		t.Fatalf("restart iteration count=%d err=%v", count, err)
	}
}

func TestShardedOrphanMediaRootWriteKeepsClaimReconciliationPending(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "d6e2f247ab144c6a850fd5c07fb1e6a3"
	header := v2ScaleHeader(id)
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	claim := testV2Claim(coordinate, "historical:"+segmentID+":"+strings.Repeat("b", 64), time.Now().UTC())
	claimSet := V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID, State: archiveindex.CoveragePresent, Claims: []archiveindex.Claim{claim}}
	if err := store.SaveShardedClaimSet(ctx, id, claimSet); err != nil {
		t.Fatal(err)
	}
	payload := []byte("orphan media payload")
	payloadPath := "tracks/main/objects/orphan.ts"
	stored, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	record := v2ScaleMediaRecord(coordinate, 1, payloadPath, int64(len(payload)), stored.SHA256)
	if err := writeShardedFixtureJSON(store, id, v2MediaPagePath("main", false, 0), v2MediaPage{Version: 1, TrackID: "main", Number: 0, Entries: []V2MediaRecord{record}}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected root response loss")
	local := store.StorageBackend.(*LocalFilesystemBackend)
	store.StorageBackend = &failAfterV2RootSave{LocalFilesystemBackend: local, failure: failure, matches: func(saved *domain.Recording) bool {
		return saved.Tracks["main"].MediaHighWater == 1 && saved.ShardedArchive.ClaimReconcilePending
	}}
	if err := store.ReconcileShardedArchive(ctx, id); !errors.Is(err, failure) {
		t.Fatalf("reconcile error=%v, want root response loss", err)
	}
	store.StorageBackend = local
	partial, err := store.LoadRecordingHeader(ctx, id)
	if err != nil || !partial.ShardedArchive.ClaimReconcilePending {
		t.Fatalf("root did not retain claim reconciliation marker: pending=%v err=%v", partial.ShardedArchive.ClaimReconcilePending, err)
	}
	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := restarted.LoadAll()
	if err != nil || len(rows) != 1 || rows[0].ShardedArchive.ClaimCount != 1 || rows[0].ShardedArchive.ClaimReconcilePending {
		t.Fatalf("restart claim accounting rows=%#v err=%v", rows, err)
	}
	set, err := restarted.LoadShardedClaimSet(ctx, id, segmentID)
	if err != nil || set.CountedClaims != 1 || set.SelectedClaimID != claim.ID {
		t.Fatalf("restart claim marker=%#v err=%v", set, err)
	}
}

func TestShardedUncommittedMediaRetryPreservesFirstClaimTimestamp(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "da42f247ab144c6a850fd5c07fb1e6a5"
	header := v2ScaleHeader(id)
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	payload := []byte("retryable-live-payload")
	const payloadPath = "tracks/main/objects/retryable-live.ts"
	result, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 41, Kind: archiveindex.ObjectMedia}
	segmentID, err := archiveindex.SegmentIdentity(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	firstObserved := time.Date(2026, 10, 9, 1, 2, 3, 0, time.UTC)
	claim := archiveindex.Claim{
		ID: "live_origin:" + segmentID + ":" + result.SHA256, Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: firstObserved, PayloadPath: payloadPath, Size: result.Size, SHA256: result.SHA256,
		Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
	}
	record := V2MediaRecord{
		Coordinate: coordinate, IndexOrdinal: 1,
		Segment: domain.Segment{
			ID: "seg-00000000000000000001", TrackID: "main", Sequence: coordinate.Sequence,
			ArchiveOrdinal: 1, SourceURI: "fixture://source/41", StoragePath: payloadPath, PayloadSize: result.Size,
			SHA256: result.SHA256, Duration: 2,
		},
		SelectedClaim: &claim, ClaimState: archiveindex.CoveragePresent,
	}
	if err := validateV2ClaimSet(V2ClaimSet{SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: claim.ID, State: record.ClaimState, Claims: []archiveindex.Claim{claim}}); err != nil {
		t.Fatalf("test claim is invalid: %v", err)
	}
	if err := validateV2Record(header, record); err != nil {
		t.Fatalf("test media record is invalid: %v", err)
	}
	failure := errors.New("injected root publication failure")
	backend := store.StorageBackend.(*LocalFilesystemBackend)
	store.StorageBackend = &failBeforeV2RootSave{
		LocalFilesystemBackend: backend, failure: failure,
		matches: func(candidate *domain.Recording) bool {
			return candidate.Tracks["main"].MediaHighWater == 1
		},
	}
	if err := store.PublishShardedMedia(ctx, header, record); !errors.Is(err, failure) {
		t.Fatalf("first publish error=%v, want injected root failure", err)
	}
	if got, err := store.LoadRecordingHeader(ctx, id); err != nil || got.Tracks["main"].MediaHighWater != 0 {
		t.Fatalf("orphan page became visible before root commit: high-water=%v err=%v", got.Tracks["main"].MediaHighWater, err)
	}

	retryClaim := claim
	retryClaim.AcquiredAt = firstObserved.Add(7 * time.Second)
	retry := record
	retry.SelectedClaim = &retryClaim
	if err := store.PublishShardedMedia(ctx, header, retry); err != nil {
		t.Fatalf("same-process retry after orphan page: %v", err)
	}
	visible, err := store.LookupShardedMediaByCoordinate(ctx, id, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	if visible.SelectedClaim == nil || visible.SelectedClaim.ID != claim.ID || !visible.SelectedClaim.AcquiredAt.Equal(firstObserved) {
		t.Fatalf("retry did not preserve first durable selected claim: %#v", visible.SelectedClaim)
	}
	final, err := store.LoadRecordingHeader(ctx, id)
	if err != nil || final.Tracks["main"].MediaHighWater != 1 || final.ShardedArchive.ClaimCount != 1 {
		t.Fatalf("root after retry high-water=%v claims=%v err=%v", final.Tracks["main"].MediaHighWater, final.ShardedArchive.ClaimCount, err)
	}
}

func TestShardedLivePromotionIntentReconcilesPartialPublication(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "d7e2f247ab144c6a850fd5c07fb1e6a4"
	header := v2ScaleHeader(id)
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 5, Kind: archiveindex.ObjectMedia}
	base := v2ScaleMediaRecord(coordinate, 1, "tracks/main/objects/promoted.ts", 4, strings.Repeat("c", 64))
	if err := store.AppendShardedMedia(ctx, id, base); err != nil {
		t.Fatal(err)
	}
	header, err = store.LoadRecordingHeader(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	header.Tracks["main"].LivePresentation = &domain.LivePresentationState{
		NextOrdinal: 2, FirstPresentationOrdinal: 1, HasEpochMapping: true,
		MappedSourceEpoch: 0, SourceSequenceBase: 5, PresentationOrdinalBase: 1,
		MaxSourceSequence: 5, HasLastCoordinate: true, LastSequence: 5,
	}
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	promoted := base
	promoted.Segment.LivePresentationOrdinal = 1
	backend := store.StorageBackend
	failure := errors.New("injected crash after live media page publication")
	store.StorageBackend = &failAfterV2SidecarWrite{StorageBackend: backend, relative: v2MediaPagePath("main", false, 0), failure: failure}
	if err := store.PublishShardedMedia(ctx, header, promoted); !errors.Is(err, failure) {
		t.Fatalf("promotion error=%v, want injected media page response loss", err)
	}
	store.StorageBackend = backend
	partial, err := store.LoadRecordingHeader(ctx, id)
	if err != nil || partial.Tracks["main"].LiveSlotHighWater != 0 {
		t.Fatalf("partial promotion became root-visible: high-water=%d err=%v", partial.Tracks["main"].LiveSlotHighWater, err)
	}
	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := restarted.LoadAll()
	if err != nil || len(rows) != 1 || rows[0].Tracks["main"].LiveSlotHighWater != 1 {
		t.Fatalf("restart promotion root=%#v err=%v", rows, err)
	}
	loaded, err := restarted.LookupShardedMediaByCoordinate(ctx, id, coordinate)
	if err != nil || loaded.Segment.LivePresentationOrdinal != 1 || loaded.Segment.ArchiveOrdinal != 1 {
		t.Fatalf("restart promoted media=%#v err=%v", loaded, err)
	}
	var slots []V2LiveSlot
	if err := restarted.IterateShardedLiveSlots(ctx, id, "main", 12, func(slot V2LiveSlot) error {
		slots = append(slots, slot)
		return nil
	}); err != nil || len(slots) != 1 || slots[0].Segment == nil || slots[0].Segment.Sequence != 5 {
		t.Fatalf("restart live slots=%#v err=%v", slots, err)
	}
}

func TestShardedTimelineRejectsDivergentCoordinateProjection(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "d8e2f247ab144c6a850fd5c07fb1e6a5")
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	record := v2ScaleMediaRecord(coordinate, 1, "tracks/main/objects/pointer.ts", 4, strings.Repeat("d", 64))
	if err := store.AppendShardedMedia(ctx, header.ID, record); err != nil {
		t.Fatal(err)
	}
	path, err := v2CoordinatePath(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	var page v2TimelinePage
	if err := store.LoadSidecar(header.ID, path, archiveShardMaxBytes, &page); err != nil {
		t.Fatal(err)
	}
	page.Entries[0].ID = "seg-00000000000000000999"
	if err := writeShardedFixtureJSON(store, header.ID, path, page); err != nil {
		t.Fatal(err)
	}
	if err := store.IterateShardedTimeline(ctx, header.ID, "main", func(V2MediaRecord) error { return nil }); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("timeline iterator error=%v, want invalid divergent pointer", err)
	}
}

func TestV2LiveCommitMetadataWritesStayBoundedAt1KAnd10K(t *testing.T) {
	for _, count := range []uint64{1_000, 10_000} {
		t.Run(fmt.Sprintf("history_%d", count), func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			store, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			id := fmt.Sprintf("a%031x", count)
			header := v2ScaleHeader(id)
			if err := store.CreateShardedRecording(header); err != nil {
				t.Fatal(err)
			}
			payload := []byte("bounded hot path")
			payloadPath := "tracks/main/objects/shared.ts"
			payloadResult, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
			if err != nil {
				t.Fatal(err)
			}
			mediaPages := make(map[uint64]*v2MediaPage)
			timelinePages := make(map[string]*v2TimelinePage)
			for ordinal := uint64(1); ordinal <= count; ordinal++ {
				coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: ordinal * 2, Kind: archiveindex.ObjectMedia}
				record := v2ScaleMediaRecord(coordinate, ordinal, payloadPath, payloadResult.Size, payloadResult.SHA256)
				pageNo, _ := mediaPageSlot(ordinal)
				mediaPage := mediaPages[pageNo]
				if mediaPage == nil {
					mediaPage = &v2MediaPage{Version: 1, TrackID: "main", Number: pageNo}
					mediaPages[pageNo] = mediaPage
				}
				mediaPage.Entries = append(mediaPage.Entries, record)
				timelinePath, err := v2CoordinatePath(coordinate)
				if err != nil {
					t.Fatal(err)
				}
				timelinePage := timelinePages[timelinePath]
				if timelinePage == nil {
					timelinePage = &v2TimelinePage{Version: 1, TrackID: "main", SourceEpoch: coordinate.SourceEpoch, DiscontinuitySequence: coordinate.DiscontinuitySequence, SequenceBucket: coordinate.Sequence / mediaShardMaxEntries, Kind: archiveindex.ObjectMedia}
					timelinePages[timelinePath] = timelinePage
				}
				entry, err := timelineEntryForRecord(record)
				if err != nil {
					t.Fatal(err)
				}
				timelinePage.Entries = append(timelinePage.Entries, entry)
			}
			for pageNo, page := range mediaPages {
				if err := writeShardedFixtureJSON(store, id, v2MediaPagePath("main", false, pageNo), page); err != nil {
					t.Fatal(err)
				}
			}
			for pagePath, page := range timelinePages {
				if err := writeShardedFixtureJSON(store, id, pagePath, page); err != nil {
					t.Fatal(err)
				}
			}
			track := header.Tracks["main"]
			track.MediaCount, track.MediaHighWater, track.NextArchiveOrdinal = count, count, count+1
			track.PayloadBytes = count * uint64(payloadResult.Size)
			track.DurationSeconds = float64(count) * 2
			track.LivePresentation = &domain.LivePresentationState{NextOrdinal: 2, FirstPresentationOrdinal: 1}
			header.ShardedArchive.MediaCount = count
			header.ShardedArchive.PayloadBytes = track.PayloadBytes
			header.ShardedArchive.DurationSeconds = track.DurationSeconds
			header.ArchiveRevision, header.TimelineRevision = count, count
			if err := store.SaveRecordingHeader(ctx, header); err != nil {
				t.Fatal(err)
			}

			coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: count*2 + 2, Kind: archiveindex.ObjectMedia}
			segmentID, err := archiveindex.SegmentIdentity(coordinate)
			if err != nil {
				t.Fatal(err)
			}
			nextPayloadPath := "tracks/main/objects/next.ts"
			claim := archiveindex.Claim{
				ID: "live_origin:" + segmentID + ":" + payloadResult.SHA256, Source: archiveindex.ClaimLiveOrigin,
				AcquiredAt: time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), PayloadPath: nextPayloadPath,
				Size: payloadResult.Size, SHA256: payloadResult.SHA256, Verification: archiveindex.VerificationVerified,
				Disposition: archiveindex.DispositionAccepted,
			}
			record := v2ScaleMediaRecord(coordinate, count+1, nextPayloadPath, payloadResult.Size, payloadResult.SHA256)
			record.Segment.LivePresentationOrdinal = 1
			record.SelectedClaim, record.ClaimState = &claim, archiveindex.CoveragePresent

			counter := &countingV2Backend{StorageBackend: store.StorageBackend, local: store.StorageBackend.(*LocalFilesystemBackend)}
			store.StorageBackend = counter
			if _, err := store.StorageBackend.SavePayloadExact(id, nextPayloadPath, bytes.NewReader(payload), 1024, int64(len(payload))); err != nil {
				t.Fatalf("persist append payload: %v", err)
			}
			if err := store.AppendShardedMedia(ctx, id, record); err != nil {
				t.Fatalf("append after %d entries: %v", count, err)
			}
			stats := counter.snapshot()
			if stats.payloadWrites != 1 || stats.sidecarWrites != 3 || stats.rootWrites != 1 || stats.mediaPageWrites != 1 || stats.timelinePageWrites != 1 || stats.livePageWrites != 1 || stats.claimPageWrites != 0 {
				t.Fatalf("history=%d durable puts payload=%d sidecars=%d roots=%d stats=%#v; want one payload + three bounded pages + one root", count, stats.payloadWrites, stats.sidecarWrites, stats.rootWrites, stats)
			}
			if stats.bytesWritten > 512<<10 || stats.bytesRead > 256<<10 {
				t.Fatalf("history=%d I/O grew with history: %#v", count, stats)
			}
			t.Logf("history=%d puts=%d (payload=%d metadata=%d), metadata bytes written=%d read=%d", count, stats.payloadWrites+stats.sidecarWrites+stats.rootWrites, stats.payloadWrites, stats.sidecarWrites+stats.rootWrites, stats.bytesWritten, stats.bytesRead)
			counter.mu.Lock()
			counter.stats = countedV2IO{}
			counter.mu.Unlock()
			visited := uint64(0)
			if err := store.IterateShardedTimelineProjection(ctx, id, "main", func(V2MediaRecord) error { visited++; return nil }); err != nil {
				t.Fatal(err)
			}
			readStats := counter.snapshot()
			wantTimelinePages := int((count*2 + mediaShardMaxEntries - 1) / mediaShardMaxEntries)
			if visited != count+1 || readStats.timelinePageReads != wantTimelinePages || readStats.mediaPageReads != 0 {
				t.Fatalf("history=%d VOD projection visits=%d reads=%#v; want %d timeline pages and no media page reads", count, visited, readStats, wantTimelinePages)
			}
			counter.mu.Lock()
			counter.stats = countedV2IO{}
			counter.mu.Unlock()
			repairCoordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: count + 1, Kind: archiveindex.ObjectMedia}
			repairID, err := archiveindex.SegmentIdentity(repairCoordinate)
			if err != nil {
				t.Fatal(err)
			}
			repairPath := "tracks/main/objects/repair.ts"
			repairPayload, err := store.StorageBackend.SavePayloadExact(id, repairPath, bytes.NewReader(payload), 1024, int64(len(payload)))
			if err != nil {
				t.Fatal(err)
			}
			repairClaim := archiveindex.Claim{
				ID: "historical:" + repairID + ":" + repairPayload.SHA256, Source: archiveindex.ClaimHistorical,
				AcquiredAt: time.Date(2026, 10, 9, 0, 0, 1, 0, time.UTC), PayloadPath: repairPath,
				Size: repairPayload.Size, SHA256: repairPayload.SHA256, Verification: archiveindex.VerificationVerified,
				Disposition: archiveindex.DispositionAccepted,
			}
			repairRecord := v2ScaleMediaRecord(repairCoordinate, count+2, repairPath, repairPayload.Size, repairPayload.SHA256)
			repairRecord.SelectedClaim, repairRecord.ClaimState = &repairClaim, archiveindex.CoveragePresent
			if err := store.AppendShardedMedia(ctx, id, repairRecord); err != nil {
				t.Fatalf("append historical repair after %d entries: %v", count, err)
			}
			repairStats := counter.snapshot()
			if repairStats.payloadWrites != 1 || repairStats.sidecarWrites != 2 || repairStats.mediaPageWrites != 1 || repairStats.timelinePageWrites != 1 || repairStats.livePageWrites != 0 || repairStats.claimPageWrites != 0 || repairStats.rootWrites != 1 {
				t.Fatalf("history=%d repair publications=%#v; want payload + media/timeline pages + root only", count, repairStats)
			}
			t.Logf("history=%d repair puts=%d media/timeline pages=%d/%d root=%d", count, repairStats.payloadWrites+repairStats.sidecarWrites+repairStats.rootWrites, repairStats.mediaPageWrites, repairStats.timelinePageWrites, repairStats.rootWrites)
		})
	}
}

func TestV2SmallLiveCommitThroughputSmoke(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "db42f247ab144c6a850fd5c07fb1e6a6"
	header := v2ScaleHeader(id)
	header.Tracks["main"].LivePresentation = &domain.LivePresentationState{NextOrdinal: 1}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	counter := &countingV2Backend{StorageBackend: store.StorageBackend, local: store.StorageBackend.(*LocalFilesystemBackend)}
	store.StorageBackend = counter
	payload := bytes.Repeat([]byte("fixture-media-"), 8)
	const segments = 32
	latencies := make([]time.Duration, 0, segments)
	var payloadWrites, sidecarWrites, rootWrites int
	var metadataBytes, payloadBytes int64
	started := time.Now()
	for ordinal := uint64(1); ordinal <= segments; ordinal++ {
		current, err := store.LoadRecordingHeader(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		counter.mu.Lock()
		counter.stats = countedV2IO{}
		counter.mu.Unlock()

		coordinate := archiveindex.Coordinate{SessionID: current.SourceSessionID, TrackID: "main", Sequence: ordinal, Kind: archiveindex.ObjectMedia}
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		payloadPath := fmt.Sprintf("tracks/main/objects/%s.m4s", segmentID)
		commitStarted := time.Now()
		stored, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
		if err != nil {
			t.Fatalf("persist synthetic live payload %d: %v", ordinal, err)
		}
		claim := archiveindex.Claim{
			ID: "live_origin:" + segmentID + ":" + stored.SHA256, Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt: time.Date(2026, 10, 9, 0, 0, int(ordinal), 0, time.UTC), PayloadPath: payloadPath,
			Size: stored.Size, SHA256: stored.SHA256, Verification: archiveindex.VerificationVerified,
			Disposition: archiveindex.DispositionAccepted,
		}
		record := V2MediaRecord{
			Coordinate: coordinate,
			Segment: domain.Segment{
				ID: fmt.Sprintf("seg-%020d", ordinal), TrackID: "main", Sequence: ordinal,
				ArchiveOrdinal: ordinal, LivePresentationOrdinal: ordinal,
				SourceURI: fmt.Sprintf("fixture://live/%d.m4s", ordinal), StoragePath: payloadPath,
				PayloadSize: stored.Size, SHA256: stored.SHA256, Duration: 2,
			},
			SelectedClaim: &claim, ClaimState: archiveindex.CoveragePresent,
		}
		next := cloneRecordingHeader(current)
		next.ArchiveRevision++
		next.TimelineRevision++
		if err := store.PublishShardedMedia(ctx, next, record); err != nil {
			t.Fatalf("publish synthetic live media %d: %v", ordinal, err)
		}
		latencies = append(latencies, time.Since(commitStarted))
		stats := counter.snapshot()
		payloadWrites += stats.payloadWrites
		sidecarWrites += stats.sidecarWrites
		rootWrites += stats.rootWrites
		metadataBytes += stats.bytesWritten - stats.payloadBytesWritten
		payloadBytes += stats.payloadBytesWritten
	}
	wall := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	quantile := func(p float64) time.Duration {
		index := int(math.Ceil(p*float64(len(latencies)))) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(latencies) {
			index = len(latencies) - 1
		}
		return latencies[index]
	}
	final, err := store.LoadRecordingHeader(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if final.ShardedArchive.MediaCount != segments || final.Tracks["main"].MediaHighWater != segments {
		t.Fatalf("synthetic live commit count=%d/%d, want %d", final.ShardedArchive.MediaCount, final.Tracks["main"].MediaHighWater, segments)
	}
	if payloadWrites != segments || sidecarWrites != segments*3 || rootWrites != segments {
		t.Fatalf("synthetic live publication counts payload=%d sidecar=%d root=%d; want %d/%d/%d", payloadWrites, sidecarWrites, rootWrites, segments, segments*3, segments)
	}
	t.Logf("synthetic live commits=%d duration=2s/segment wall=%s throughput=%.1f segments/s commit_latency_p50=%s p95=%s p99=%s max_queue_depth=0 oldest_queue_age=0s provider_puts_per_segment=%.1f metadata_bytes_per_segment=%.0f payload_bytes_per_segment=%.0f",
		segments, wall.Round(time.Millisecond), float64(segments)/wall.Seconds(), quantile(.50).Round(time.Microsecond), quantile(.95).Round(time.Microsecond), quantile(.99).Round(time.Microsecond),
		float64(payloadWrites+sidecarWrites+rootWrites)/segments, float64(metadataBytes)/segments, float64(payloadBytes)/segments)
}

func TestShardedMetadataRolloverRestartReconcilesPublishedPage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	header := v2ScaleHeader("e6e2f247ab144c6a850fd5c07fb1e6a2")
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 2, 0, 0, 0, time.UTC)
	for ordinal := 1; ordinal <= metadataShardMaxEntries; ordinal++ {
		title := fmt.Sprintf("metadata-%03d", ordinal)
		if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: now.Add(time.Duration(ordinal) * time.Second), Title: &title}); err != nil {
			t.Fatalf("append metadata %d: %v", ordinal, err)
		}
	}
	title := fmt.Sprintf("metadata-%03d", metadataShardMaxEntries+1)
	backend := store.StorageBackend
	injectedFailure := errors.New("injected post-publication failure")
	store.StorageBackend = &failAfterV2SidecarWrite{StorageBackend: backend, relative: "archive/v2/metadata/00000000000000000001", failure: injectedFailure}
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: now.Add(time.Duration(metadataShardMaxEntries+1) * time.Second), Title: &title}); !errors.Is(err, injectedFailure) {
		t.Fatalf("metadata append failure=%v, want injected post-publication failure", err)
	}
	store.StorageBackend = backend

	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := restarted.LoadAll()
	if err != nil || len(rows) != 1 || rows[0].ShardedArchive.MetadataRevisionCount != metadataShardMaxEntries+1 {
		t.Fatalf("metadata rollover recovery rows=%#v err=%v", rows, err)
	}
	count := 0
	if err := restarted.IterateShardedMetadata(ctx, header.ID, func(revision domain.MetadataRevision) error {
		count++
		if !revision.ObservedAt.Equal(now.Add(time.Duration(count) * time.Second)) {
			return fmt.Errorf("metadata timestamp at %d = %s", count, revision.ObservedAt)
		}
		return nil
	}); err != nil || count != metadataShardMaxEntries+1 {
		t.Fatalf("metadata rollover iteration count=%d err=%v", count, err)
	}
}

func TestShardedPagedAppendRetriesOrphanPageBeforeNextAppend(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 9, 2, 15, 0, 0, time.UTC)
	tests := []struct {
		name       string
		collection string
		pageLimit  int
		append     func(*Store, int) error
		count      func(*domain.Recording) uint64
		iterate    func(*Store, string) (int, error)
	}{
		{
			name: "metadata", collection: "metadata", pageLimit: metadataShardMaxEntries,
			append: func(store *Store, ordinal int) error {
				title := fmt.Sprintf("metadata-%03d", ordinal)
				return store.AppendShardedMetadata(ctx, "e8e2f247ab144c6a850fd5c07fb1e6a3", domain.MetadataRevision{
					ObservedAt: now.Add(time.Duration(ordinal) * time.Second), Title: &title,
				})
			},
			count: func(header *domain.Recording) uint64 { return header.ShardedArchive.MetadataRevisionCount },
			iterate: func(store *Store, id string) (int, error) {
				count := 0
				err := store.IterateShardedMetadata(ctx, id, func(domain.MetadataRevision) error { count++; return nil })
				return count, err
			},
		},
		{
			name: "manifests", collection: "manifests", pageLimit: manifestShardMaxEntries,
			append: func(store *Store, ordinal int) error {
				return store.AppendShardedManifest(ctx, "e9e2f247ab144c6a850fd5c07fb1e6a3", domain.ManifestSnapshot{
					TrackID: "main", SourceURI: fmt.Sprintf("https://example.invalid/manifest/%03d.m3u8", ordinal),
					StoragePath: fmt.Sprintf("manifests/main/%03d.m3u8", ordinal),
					FetchedAt:   now.Add(time.Duration(ordinal) * time.Second), SHA256: strings.Repeat("a", 64), Size: 1,
				})
			},
			count: func(header *domain.Recording) uint64 { return header.ShardedArchive.ManifestSnapshotCount },
			iterate: func(store *Store, id string) (int, error) {
				count := 0
				err := store.IterateShardedManifests(ctx, id, func(domain.ManifestSnapshot) error { count++; return nil })
				return count, err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			id := "e8e2f247ab144c6a850fd5c07fb1e6a3"
			if test.collection == "manifests" {
				id = "e9e2f247ab144c6a850fd5c07fb1e6a3"
			}
			header := v2ScaleHeader(id)
			if err := store.CreateShardedRecording(header); err != nil {
				t.Fatal(err)
			}
			for ordinal := 1; ordinal <= test.pageLimit; ordinal++ {
				if err := test.append(store, ordinal); err != nil {
					t.Fatalf("seed append %d: %v", ordinal, err)
				}
			}
			orphanOrdinal := test.pageLimit + 1
			injectedFailure := errors.New("injected durable-page response loss")
			backend := store.StorageBackend
			store.StorageBackend = &failAfterV2SidecarWrite{
				StorageBackend: backend,
				relative:       fmt.Sprintf("archive/v2/%s/%020d", test.collection, 1),
				failure:        injectedFailure,
			}
			if err := test.append(store, orphanOrdinal); !errors.Is(err, injectedFailure) {
				t.Fatalf("orphan append error=%v, want injected response loss", err)
			}
			store.StorageBackend = backend

			// Identical same-process retry must reconcile the durable page before
			// deciding the append is already complete.
			if err := test.append(store, orphanOrdinal); err != nil {
				t.Fatalf("retry identical orphan append: %v", err)
			}
			if err := test.append(store, orphanOrdinal+1); err != nil {
				t.Fatalf("append following orphan retry: %v", err)
			}
			visible, err := store.LoadRecordingHeader(ctx, id)
			if err != nil || test.count(visible) != uint64(orphanOrdinal+1) {
				t.Fatalf("same-process root count=%d err=%v", test.count(visible), err)
			}
			iterated, err := test.iterate(store, id)
			if err != nil || iterated != orphanOrdinal+1 {
				t.Fatalf("same-process iteration=%d err=%v", iterated, err)
			}

			restarted, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := restarted.LoadAll()
			if err != nil || len(rows) != 1 || test.count(rows[0]) != uint64(orphanOrdinal+1) {
				t.Fatalf("restart root rows=%d count=%d err=%v", len(rows), func() uint64 {
					if len(rows) == 1 {
						return test.count(rows[0])
					}
					return 0
				}(), err)
			}
			iterated, err = test.iterate(restarted, id)
			if err != nil || iterated != orphanOrdinal+1 {
				t.Fatalf("restart iteration=%d err=%v", iterated, err)
			}
		})
	}
}

func TestShardedMetadataLargeAllowedEntryRollsOverByCount(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "e7e2f247ab144c6a850fd5c07fb1e6a3")
	now := time.Date(2026, 10, 9, 2, 30, 0, 0, time.UTC)
	description := strings.Repeat("d", 64<<10)
	for ordinal := 1; ordinal <= metadataShardMaxEntries+1; ordinal++ {
		revision := domain.MetadataRevision{ObservedAt: now.Add(time.Duration(ordinal) * time.Second), Description: &description}
		if err := store.AppendShardedMetadata(ctx, header.ID, revision); err != nil {
			t.Fatalf("append maximum-size metadata revision %d: %v", ordinal, err)
		}
	}
	for pageNo, wantEntries := range []int{metadataShardMaxEntries, 1} {
		var page v2RevisionPage[json.RawMessage]
		if err := store.loadV2JSON(ctx, header.ID, fmt.Sprintf("archive/v2/metadata/%020d", pageNo), archiveShardMaxBytes, &page); err != nil {
			t.Fatalf("load metadata page %d: %v", pageNo, err)
		}
		if len(page.Entries) != wantEntries {
			t.Fatalf("metadata page %d entries=%d want=%d", pageNo, len(page.Entries), wantEntries)
		}
		if size := statSidecarBytes(t, store.root, header.ID, fmt.Sprintf("archive/v2/metadata/%020d", pageNo)); size > archiveShardMaxBytes {
			t.Fatalf("metadata page %d size=%d exceeds %d", pageNo, size, archiveShardMaxBytes)
		}
	}
	count := 0
	if err := store.IterateShardedMetadata(ctx, header.ID, func(revision domain.MetadataRevision) error {
		count++
		if revision.Description == nil || len(*revision.Description) != 64<<10 {
			return errors.New("maximum-size description lost")
		}
		return nil
	}); err != nil || count != metadataShardMaxEntries+1 {
		t.Fatalf("large metadata iteration count=%d err=%v", count, err)
	}
}

func TestShardedMediaInitAndManifestMaxURIEntriesRollOver(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "f8e2f247ab144c6a850fd5c07fb1e6a4")
	maxURI := "https://" + strings.Repeat("u", maxV2SourceURIBytes-len("https://"))
	if len(maxURI) != maxV2SourceURIBytes {
		t.Fatalf("fixture URI length=%d, want=%d", len(maxURI), maxV2SourceURIBytes)
	}
	payload := []byte("maximum-source-uri-payload")
	payloadPath := "tracks/main/objects/max-uri.bin"
	stored, err := store.StorageBackend.SavePayloadExact(header.ID, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := uint64(1); ordinal <= mediaShardMaxEntries+1; ordinal++ {
		coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: ordinal, Kind: archiveindex.ObjectMedia}
		record := v2ScaleMediaRecord(coordinate, ordinal, payloadPath, int64(len(payload)), stored.SHA256)
		record.Segment.SourceURI = maxURI
		if err := store.AppendShardedMedia(ctx, header.ID, record); err != nil {
			t.Fatalf("append max-URI media %d: %v", ordinal, err)
		}
	}
	for ordinal := uint64(1); ordinal <= mediaShardMaxEntries+1; ordinal++ {
		coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: ordinal + 1000, Kind: archiveindex.ObjectInit}
		segment := domain.Segment{
			ID: fmt.Sprintf("init-%03d", ordinal), TrackID: "main", Sequence: ordinal + 1000,
			SourceURI: maxURI, StoragePath: payloadPath, PayloadSize: int64(len(payload)),
			SHA256: stored.SHA256, IsInit: true,
		}
		if err := store.AppendShardedMedia(ctx, header.ID, V2MediaRecord{Coordinate: coordinate, Segment: segment}); err != nil {
			t.Fatalf("append max-URI init %d: %v", ordinal, err)
		}
	}
	for _, fixture := range []struct {
		init bool
		want int
	}{{false, mediaShardMaxEntries}, {true, mediaShardMaxEntries}} {
		for pageNo, wantEntries := range []int{fixture.want, 1} {
			pagePath := v2MediaPagePath("main", fixture.init, uint64(pageNo))
			page, err := store.loadV2MediaPage(ctx, header.ID, "main", fixture.init, uint64(pageNo))
			if err != nil || len(page.Entries) != wantEntries {
				t.Fatalf("media page %s/%d entries=%d err=%v want=%d", pagePath, pageNo, len(page.Entries), err, wantEntries)
			}
			if size := statSidecarBytes(t, store.root, header.ID, pagePath); size > archiveShardMaxBytes {
				t.Fatalf("media page %s size=%d exceeds %d", pagePath, size, archiveShardMaxBytes)
			}
		}
	}
	if err := store.IterateShardedMedia(ctx, header.ID, "main", func(record V2MediaRecord) error {
		if len(record.Segment.SourceURI) != maxV2SourceURIBytes {
			return errors.New("media source URI was truncated")
		}
		return nil
	}); err != nil {
		t.Fatalf("iterate maximum URI media: %v", err)
	}
	if err := store.IterateShardedInitSegments(ctx, header.ID, "main", func(record V2MediaRecord) error {
		if len(record.Segment.SourceURI) != maxV2SourceURIBytes {
			return errors.New("init source URI was truncated")
		}
		return nil
	}); err != nil {
		t.Fatalf("iterate maximum URI init: %v", err)
	}

	manifestStore, manifestHeader := newShardedArchiveTestStore(t, "f9e2f247ab144c6a850fd5c07fb1e6a5")
	now := time.Date(2026, 10, 9, 3, 0, 0, 0, time.UTC)
	for ordinal := 1; ordinal <= manifestShardMaxEntries+1; ordinal++ {
		snapshot := domain.ManifestSnapshot{
			TrackID: "main", SourceURI: maxURI,
			StoragePath: fmt.Sprintf("manifests/main/%03d.m3u8", ordinal),
			FetchedAt:   now.Add(time.Duration(ordinal) * time.Second),
			SHA256:      strings.Repeat("a", 64), Size: 1,
		}
		if err := manifestStore.AppendShardedManifest(ctx, manifestHeader.ID, snapshot); err != nil {
			t.Fatalf("append max-URI manifest %d: %v", ordinal, err)
		}
	}
	for pageNo, wantEntries := range []int{manifestShardMaxEntries, 1} {
		var page v2RevisionPage[json.RawMessage]
		if err := manifestStore.loadV2JSON(ctx, manifestHeader.ID, fmt.Sprintf("archive/v2/manifests/%020d", pageNo), archiveShardMaxBytes, &page); err != nil {
			t.Fatalf("load manifest page %d: %v", pageNo, err)
		}
		if len(page.Entries) != wantEntries {
			t.Fatalf("manifest page %d entries=%d want=%d", pageNo, len(page.Entries), wantEntries)
		}
		if size := statSidecarBytes(t, manifestStore.root, manifestHeader.ID, fmt.Sprintf("archive/v2/manifests/%020d", pageNo)); size > archiveShardMaxBytes {
			t.Fatalf("manifest page %d size=%d exceeds %d", pageNo, size, archiveShardMaxBytes)
		}
	}
}

func TestShardedMediaRestartReconcilesLivePresentationCursor(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "d4e2f247ab144c6a850fd5c07fb1e6a3"
	header := v2ScaleHeader(id)
	header.Tracks["main"].LivePresentation = &domain.LivePresentationState{NextOrdinal: 1}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	payload := []byte("live-recovery-payload")
	payloadPath := "tracks/main/objects/live-recovery.ts"
	stored, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	record := v2ScaleMediaRecord(archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: 100, Kind: archiveindex.ObjectMedia,
	}, 1, payloadPath, int64(len(payload)), stored.SHA256)
	record.Segment.LivePresentationOrdinal = 1
	record.Segment.LiveDiscontinuitySequence = 0
	backend := store.StorageBackend
	injectedFailure := errors.New("injected crash after durable media page")
	store.StorageBackend = &failAfterV2SidecarWrite{
		StorageBackend: backend,
		relative:       v2MediaPagePath("main", false, 0),
		failure:        injectedFailure,
	}
	if err := store.PublishShardedMedia(ctx, header, record); !errors.Is(err, injectedFailure) {
		t.Fatalf("publish interrupted error=%v, want injected crash", err)
	}

	restarted, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	recordings, err := restarted.LoadAll()
	if err != nil || len(recordings) != 1 {
		t.Fatalf("restart load all: count=%d err=%v", len(recordings), err)
	}
	recovered := recordings[0].Tracks["main"]
	if recovered.LivePresentation == nil || recovered.LivePresentation.NextOrdinal != 2 || recovered.LiveSlotHighWater != 1 {
		t.Fatalf("live cursor not reconciled: state=%#v slot high-water=%d", recovered.LivePresentation, recovered.LiveSlotHighWater)
	}
	var tail []V2LiveSlot
	if err := restarted.IterateShardedLiveSlots(ctx, id, "main", 12, func(slot V2LiveSlot) error {
		tail = append(tail, slot)
		return nil
	}); err != nil || len(tail) != 1 || tail[0].Ordinal != 1 || tail[0].Segment == nil || tail[0].Segment.ID != record.Segment.ID {
		t.Fatalf("recovered live tail=%#v err=%v", tail, err)
	}

	second := v2ScaleMediaRecord(archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: 101, Kind: archiveindex.ObjectMedia,
	}, 2, payloadPath, int64(len(payload)), stored.SHA256)
	second.Segment.LivePresentationOrdinal = recovered.LivePresentation.NextOrdinal
	second.Segment.LiveDiscontinuitySequence = recovered.LivePresentation.DiscontinuitySequence
	if err := restarted.PublishShardedMedia(ctx, recordings[0], second); err != nil {
		t.Fatalf("publish next live slot after restart: %v", err)
	}
	updated, err := restarted.LoadRecordingHeader(ctx, id)
	if err != nil || updated.Tracks["main"].LivePresentation.NextOrdinal != 3 {
		t.Fatalf("next live cursor=%#v err=%v", updated.Tracks["main"].LivePresentation, err)
	}
}

func TestShardedMetadataAndManifestRejectInvalidRecords(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, "f7e2f247ab144c6a850fd5c07fb1e6a2")
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: time.Now().UTC()}); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("metadata without fields error=%v, want invalid", err)
	}
	if err := store.AppendShardedManifest(ctx, header.ID, domain.ManifestSnapshot{}); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("empty manifest error=%v, want invalid", err)
	}
	title := "valid"
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: time.Now().UTC(), Title: &title}); err != nil {
		t.Fatal(err)
	}
	if err := store.IterateShardedMetadata(ctx, header.ID, nil); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("nil metadata iterator error=%v, want invalid", err)
	}
	if err := store.IterateShardedManifests(ctx, header.ID, nil); !errors.Is(err, ErrShardedArchiveInvalid) {
		t.Fatalf("nil manifest iterator error=%v, want invalid", err)
	}
}

// TestShardedArchiveLocalFilesystemScaleAcceptance exercises real local Store
// readers/recovery and one production append over a synthetic 100k archive.
// Fixture seeding writes bounded shard files directly to avoid 100k root
// fsyncs; archive access and final publication still use Store APIs/backend.
func TestShardedArchiveLocalFilesystemScaleAcceptance(t *testing.T) {
	if os.Getenv("IR_RUN_SCALE_TESTS") != "1" {
		t.Skip("set IR_RUN_SCALE_TESTS=1 to run large local filesystem acceptance")
	}
	if testing.Short() {
		t.Skip("large local filesystem acceptance")
	}
	ctx := context.Background()
	const mediaCount uint64 = 100_000
	const claimCount uint64 = 150_000
	const supplementalClaimCount uint64 = 50_000
	const metadataCount uint64 = 10_000
	const claimsPerCoordinate = 2
	root := newScaleAcceptanceTempDir(t)
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	id := "f223bc9335de40ff9bfb8f3b23de6701"
	header := v2ScaleHeader(id)
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	emptyRootBytes := statRootBytes(t, root, id)
	payload := []byte("scale-fixture-payload")
	payloadPath := "tracks/main/objects/scale-fixture.ts"
	payloadResult, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	sharedSHA := payloadResult.SHA256
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	sourceSequence := func(archiveOrdinal uint64) uint64 {
		if archiveOrdinal > mediaCount/2 {
			return archiveOrdinal + 1 // leave source coordinate mediaCount/2+1 for late repair
		}
		return archiveOrdinal
	}

	// 100k canonical media records and bounded media/timeline pages.
	page := v2MediaPage{Version: 1, TrackID: "main"}
	timelinePages := make(map[string]*v2TimelinePage)
	var root10KBytes int64
	for ordinal := uint64(1); ordinal <= mediaCount; ordinal++ {
		coordinate := archiveindex.Coordinate{
			SessionID: header.SourceSessionID, TrackID: "main", Sequence: sourceSequence(ordinal),
			Kind: archiveindex.ObjectMedia,
		}
		record := v2ScaleMediaRecord(coordinate, ordinal, payloadPath, int64(len(payload)), sharedSHA)
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		selectedClaim := archiveindex.Claim{
			ID: "live_origin:" + segmentID + ":" + sharedSHA, Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt: now, PayloadPath: payloadPath, Size: int64(len(payload)), SHA256: sharedSHA,
			Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
		}
		record.SelectedClaim, record.ClaimState = &selectedClaim, archiveindex.CoveragePresent
		if ordinal >= mediaCount-2 {
			record.Segment.LivePresentationOrdinal = ordinal - (mediaCount - 3)
		}
		pageNo, _ := mediaPageSlot(ordinal)
		if len(page.Entries) == 0 {
			page.Number = pageNo
		}
		page.Entries = append(page.Entries, record)
		timelinePath, err := v2CoordinatePath(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		timelinePage := timelinePages[timelinePath]
		if timelinePage == nil {
			timelinePage = &v2TimelinePage{Version: 1, TrackID: "main", SourceEpoch: coordinate.SourceEpoch, DiscontinuitySequence: coordinate.DiscontinuitySequence, SequenceBucket: coordinate.Sequence / mediaShardMaxEntries, Kind: archiveindex.ObjectMedia}
			timelinePages[timelinePath] = timelinePage
		}
		entry, err := timelineEntryForRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		timelinePage.Entries = append(timelinePage.Entries, entry)
		if ordinal%mediaShardMaxEntries == 0 || ordinal == mediaCount {
			if err := writeShardedFixtureJSON(store, id, v2MediaPagePath("main", false, pageNo), page); err != nil {
				t.Fatal(err)
			}
			page = v2MediaPage{Version: 1, TrackID: "main"}
		}
		if ordinal == mediaCount/10 {
			// The checkpoint is not page-aligned. Publish current bounded page
			// snapshot without resetting it, so root size is measured with all
			// first 10k media records durable.
			if ordinal%mediaShardMaxEntries != 0 {
				pageSnapshot := page
				pageSnapshot.Entries = append([]V2MediaRecord(nil), page.Entries...)
				if err := writeShardedFixtureJSON(store, id, v2MediaPagePath("main", false, pageNo), pageSnapshot); err != nil {
					t.Fatal(err)
				}
			}
			track := header.Tracks["main"]
			track.MediaCount = ordinal
			track.MediaHighWater = ordinal
			track.NextArchiveOrdinal = ordinal + 1
			track.PayloadBytes = ordinal * uint64(len(payload))
			track.DurationSeconds = float64(ordinal) * 2
			header.ShardedArchive.MediaCount = ordinal
			header.ShardedArchive.ClaimCount = ordinal
			header.ShardedArchive.PayloadBytes = track.PayloadBytes
			header.ShardedArchive.DurationSeconds = track.DurationSeconds
			if err := store.SaveRecordingHeader(ctx, header); err != nil {
				t.Fatal(err)
			}
			root10KBytes = statRootBytes(t, root, id)
		}
	}
	for timelinePath, timelinePage := range timelinePages {
		if err := writeShardedFixtureJSON(store, id, timelinePath, timelinePage); err != nil {
			t.Fatal(err)
		}
	}
	track := header.Tracks["main"]
	track.MediaCount = mediaCount
	track.MediaHighWater = mediaCount
	track.NextArchiveOrdinal = mediaCount + 1
	track.PayloadBytes = mediaCount * uint64(len(payload))
	track.DurationSeconds = float64(mediaCount) * 2
	track.LiveSlotHighWater = 3
	track.LivePresentation = &domain.LivePresentationState{
		NextOrdinal: 4, FirstPresentationOrdinal: 1, HasEpochMapping: true,
		MappedSourceEpoch: 0, SourceSequenceBase: sourceSequence(mediaCount - 2),
		PresentationOrdinalBase: 1, MaxSourceSequence: sourceSequence(mediaCount),
	}
	livePage := v2LivePage{Version: 1, TrackID: "main", Number: 0}
	for presentationOrdinal := uint64(1); presentationOrdinal <= 3; presentationOrdinal++ {
		archiveOrdinal := mediaCount - 3 + presentationOrdinal
		coordinate := archiveindex.Coordinate{
			SessionID: header.SourceSessionID, TrackID: "main", Sequence: sourceSequence(archiveOrdinal),
			Kind: archiveindex.ObjectMedia,
		}
		livePage.Entries = append(livePage.Entries, v2LiveIndexEntry{
			PresentationOrdinal: presentationOrdinal, Coordinate: coordinate, ArchiveOrdinal: archiveOrdinal,
		})
	}
	if err := writeShardedFixtureJSON(store, id, v2LivePagePath("main", 0), livePage); err != nil {
		t.Fatal(err)
	}
	header.ShardedArchive.MediaCount = mediaCount
	header.ShardedArchive.ClaimCount = mediaCount
	header.ShardedArchive.PayloadBytes = track.PayloadBytes
	header.ShardedArchive.DurationSeconds = track.DurationSeconds
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}

	// 100k selected live claims live inline in media pages. Add 50k alternate
	// historical claims through 50k supplemental sets, packed into bounded claim
	// pages. Exercise one set through public admission above the former global
	// ceiling, fixture-seed the rest to keep runtime bounded.
	setCount := int(supplementalClaimCount)
	apiClaimIndex := 0
	apiClaimCount := mediaCount
	claimPages := make(map[uint16][]V2ClaimSet)
	nextClaimPage := make(map[uint16]uint64)
	flushClaimPages := func() {
		t.Helper()
		for bucket, sets := range claimPages {
			sort.Slice(sets, func(i, j int) bool { return sets[i].SegmentID < sets[j].SegmentID })
			for start := 0; start < len(sets); start += claimPageMaxEntries {
				end := start + claimPageMaxEntries
				if end > len(sets) {
					end = len(sets)
				}
				page := v2ClaimPage{
					Version: 1, Bucket: bucket, Number: nextClaimPage[bucket],
					Entries: append([]V2ClaimSet(nil), sets[start:end]...),
				}
				if err := writeShardedFixtureJSON(store, id, v2ClaimPagePath(bucket, page.Number), page); err != nil {
					t.Fatal(err)
				}
				nextClaimPage[bucket]++
			}
		}
		clear(claimPages)
	}
	for index := 0; index < setCount; index++ {
		coordinate := archiveindex.Coordinate{
			SessionID: header.SourceSessionID, TrackID: "main", Sequence: uint64(index + 1),
			Kind: archiveindex.ObjectMedia,
		}
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		claims := make([]archiveindex.Claim, 0, claimsPerCoordinate)
		selected := "live_origin:" + segmentID + ":" + sharedSHA
		claims = append(claims,
			archiveindex.Claim{
				ID: selected, Source: archiveindex.ClaimLiveOrigin, AcquiredAt: now,
				PayloadPath: payloadPath, Size: int64(len(payload)), SHA256: sharedSHA,
				Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
			},
			archiveindex.Claim{
				ID: "historical:" + segmentID + ":" + sharedSHA, Source: archiveindex.ClaimHistorical,
				AcquiredAt: now.Add(time.Second), PayloadPath: payloadPath, Size: int64(len(payload)), SHA256: sharedSHA,
				Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
			},
		)
		set := V2ClaimSet{
			SegmentID: segmentID, Coordinate: coordinate, SelectedClaimID: selected,
			State: archiveindex.CoveragePresent, Claims: claims,
		}
		if err := validateV2ClaimSet(set); err != nil {
			t.Fatalf("fixture claim set %d: %v", index, err)
		}
		if index == apiClaimIndex {
			// Establish a real on-disk >100k baseline, then exercise public
			// admission above the former global ceiling. The remaining sets are
			// fixture-seeded to keep this scale test bounded in runtime.
			flushClaimPages()
			header.ShardedArchive.ClaimCount = apiClaimCount
			if err := store.SaveRecordingHeader(ctx, header); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveShardedClaimSet(ctx, id, set); err != nil {
				t.Fatalf("admit claim set above 100k: %v", err)
			}
			got, err := store.LoadRecordingHeader(ctx, id)
			if err != nil {
				t.Fatalf("load header after claim API admission: %v", err)
			}
			if got.ShardedArchive.ClaimCount != apiClaimCount+1 {
				t.Fatalf("claim API admission count=%d, want %d", got.ShardedArchive.ClaimCount, apiClaimCount+1)
			}
			// Public claim admission advances the root revision. Continue the
			// fixture's later bounded summary updates from the current root so
			// SaveRecordingHeader does not reject a stale revision snapshot.
			header = got
			set.CountedClaims = 1
			_, pagePath, _, _, err := store.locateV2ClaimSet(ctx, id, segmentID)
			if err != nil {
				t.Fatalf("locate admitted claim page: %v", err)
			}
			pageNumber, err := strconv.ParseUint(path.Base(pagePath), 10, 64)
			if err != nil {
				t.Fatalf("parse admitted claim page number: %v", err)
			}
			bucket := v2ClaimBucket(segmentID)
			nextClaimPage[bucket] = pageNumber + 1
		}
		if index != apiClaimIndex {
			set.CountedClaims = 1
			bucket := v2ClaimBucket(segmentID)
			claimPages[bucket] = append(claimPages[bucket], set)
		}
	}
	flushClaimPages()

	// 10k metadata observations exceed old count and byte ceilings. Seed the
	// actual local shard layout in bounded pages; separate API rollover tests
	// exercise append publication and restart reconciliation.
	metadataBytes := int64(0)
	largeTitle := strings.Repeat("m", 600)
	metadataPage := v2RevisionPage[json.RawMessage]{Version: 1}
	for ordinal := uint64(1); ordinal <= metadataCount; ordinal++ {
		title := fmt.Sprintf("%08d-%s", ordinal, largeTitle)
		revision := domain.MetadataRevision{ObservedAt: now.Add(time.Duration(ordinal) * time.Second), Title: &title}
		encoded, err := json.Marshal(revision)
		if err != nil {
			t.Fatal(err)
		}
		if err := domain.ValidateMetadataTimeline([]domain.MetadataRevision{revision}); err != nil {
			t.Fatalf("metadata fixture revision %d: %v", ordinal, err)
		}
		metadataPage.Entries = append(metadataPage.Entries, encoded)
		metadataBytes += int64(len(encoded))
		if ordinal%metadataShardMaxEntries == 0 || ordinal == metadataCount {
			pageNumber := (ordinal - 1) / metadataShardMaxEntries
			metadataPage.Number = pageNumber
			if err := writeShardedFixtureJSON(store, id, fmt.Sprintf("archive/v2/metadata/%020d", pageNumber), metadataPage); err != nil {
				t.Fatal(err)
			}
			metadataPage = v2RevisionPage[json.RawMessage]{Version: 1}
		}
	}
	if metadataBytes <= 4<<20 {
		t.Fatalf("metadata fixture is %d bytes, want >4 MiB", metadataBytes)
	}

	header.ShardedArchive.MetadataRevisionCount = metadataCount
	header.ShardedArchive.ClaimCount = claimCount
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	root100KBytes := statRootBytes(t, root, id)
	if root10KBytes == 0 || root100KBytes > 16<<10 || root100KBytes-root10KBytes > 512 || root100KBytes < emptyRootBytes-128 {
		t.Fatalf("root grew with history: empty=%d 10k=%d 100k=%d", emptyRootBytes, root10KBytes, root100KBytes)
	}
	t.Logf("root bytes: empty=%d 10k=%d 100k=%d; media shards=%d; metadata bytes=%d", emptyRootBytes, root10KBytes, root100KBytes, (mediaCount+mediaShardMaxEntries-1)/mediaShardMaxEntries, metadataBytes)

	// Full local backend restart returns bounded root, reconciles, preserves
	// high-water, then permits archive and coordinate lookups.
	reopened, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	all, err := reopened.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].FormatVersion != ShardedArchiveFormatVersion || all[0].Tracks["main"].MediaHighWater != mediaCount || all[0].ShardedArchive.ClaimCount != claimCount {
		t.Fatalf("restart lost v2 high-water: recordings=%d", len(all))
	}
	for _, ordinal := range []uint64{1, mediaCount / 2, mediaCount} {
		record, err := reopened.LookupShardedMediaByArchiveOrdinal(ctx, id, "main", ordinal)
		if err != nil || record.Segment.ArchiveOrdinal != ordinal {
			t.Fatalf("lookup archive ordinal %d: record=%#v err=%v", ordinal, record, err)
		}
		byCoordinate, err := reopened.LookupShardedMediaByCoordinate(ctx, id, record.Coordinate)
		if err != nil || byCoordinate.Segment.ID != record.Segment.ID {
			t.Fatalf("lookup coordinate %d: err=%v", ordinal, err)
		}
	}
	mediaSeen := uint64(0)
	if err := reopened.IterateShardedMedia(ctx, id, "main", func(record V2MediaRecord) error {
		mediaSeen++
		if record.Segment.ArchiveOrdinal != mediaSeen {
			return fmt.Errorf("media ordinal %d at position %d", record.Segment.ArchiveOrdinal, mediaSeen)
		}
		return nil
	}); err != nil || mediaSeen != mediaCount {
		t.Fatalf("media iteration count=%d err=%v", mediaSeen, err)
	}
	timelineSeen := uint64(0)
	if err := reopened.IterateShardedTimeline(ctx, id, "main", func(record V2MediaRecord) error {
		timelineSeen++
		if record.Segment.TimelineOrdinal != timelineSeen {
			return fmt.Errorf("timeline ordinal %d at position %d", record.Segment.TimelineOrdinal, timelineSeen)
		}
		return nil
	}); err != nil || timelineSeen != mediaCount {
		t.Fatalf("timeline iteration count=%d err=%v", timelineSeen, err)
	}
	claimSets, claimsRead := 0, uint64(0)
	if err := reopened.IterateShardedClaimSets(ctx, id, func(set V2ClaimSet) error {
		claimSets++
		claimsRead += uint64(len(set.Claims))
		return nil
	}); err != nil || claimSets != setCount || claimsRead != supplementalClaimCount*2 {
		t.Fatalf("supplemental claim iteration sets=%d claims=%d err=%v, want %d/%d", claimSets, claimsRead, err, setCount, supplementalClaimCount*2)
	}
	inlineClaims := uint64(0)
	if err := reopened.IterateShardedMedia(ctx, id, "main", func(record V2MediaRecord) error {
		if record.SelectedClaim != nil {
			inlineClaims++
		}
		return nil
	}); err != nil || inlineClaims != mediaCount || inlineClaims+supplementalClaimCount != claimCount {
		t.Fatalf("unique claim count inline=%d supplemental=%d err=%v; want %d", inlineClaims, supplementalClaimCount, err, claimCount)
	}
	claimPageCount := 0
	if err := store.listV2(ctx, id, "archive/v2/claims/", func(string) error { claimPageCount++; return nil }); err != nil {
		t.Fatal(err)
	}
	if claimPageCount >= setCount/2 {
		t.Fatalf("supplemental claim pages=%d for %d sets; expected substantial page packing", claimPageCount, setCount)
	}
	if rootHeader, err := reopened.LoadRecordingHeader(ctx, id); err != nil || rootHeader.ShardedArchive.ClaimCount != claimCount {
		t.Fatalf("claim admission summary=%v err=%v, want %d", rootHeader, err, claimCount)
	}
	firstCoordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectMedia}
	firstSegmentID, err := archiveindex.SegmentIdentity(firstCoordinate)
	if err != nil {
		t.Fatal(err)
	}
	firstClaims, err := reopened.LoadShardedClaimSet(ctx, id, firstSegmentID)
	if err != nil {
		t.Fatal(err)
	}
	firstClaims.CountedClaims = 0
	if err := reopened.SaveShardedClaimSet(ctx, id, firstClaims); err != nil {
		t.Fatalf("duplicate claim-set admission: %v", err)
	}
	firstClaims.Claims[0].SHA256 = strings.Repeat("e", 64)
	if err := reopened.SaveShardedClaimSet(ctx, id, firstClaims); !errors.Is(err, ErrShardedArchiveConflict) {
		t.Fatalf("conflicting claim identity err=%v, want conflict", err)
	}
	metadataRead := uint64(0)
	if err := reopened.AppendShardedMetadata(ctx, id, domain.MetadataRevision{ObservedAt: now.Add(time.Duration(metadataCount+1) * time.Second), Title: &largeTitle}); err != nil {
		t.Fatalf("append metadata revision 10001: %v", err)
	}
	if err := reopened.IterateShardedMetadata(ctx, id, func(revision domain.MetadataRevision) error {
		metadataRead++
		if revision.ObservedAt.Before(now) {
			return errors.New("metadata order regressed")
		}
		return nil
	}); err != nil || metadataRead != metadataCount+1 {
		t.Fatalf("metadata iteration count=%d err=%v", metadataRead, err)
	}
	latestMetadata, err := reopened.LoadLatestShardedMetadata(ctx, id)
	if err != nil || latestMetadata == nil || latestMetadata.Title == nil || *latestMetadata.Title != largeTitle || !latestMetadata.ObservedAt.Equal(now.Add(time.Duration(metadataCount+1)*time.Second)) {
		t.Fatalf("latest metadata lookup=%+v err=%v", latestMetadata, err)
	}
	appendHeader, err := reopened.LoadRecordingHeader(ctx, id)
	if err != nil {
		t.Fatalf("load current root before media append: %v", err)
	}

	// Count local backend object I/O during append 100,001. No prior history
	// page or whole recording is read or rewritten.
	counter := &countingV2Backend{StorageBackend: reopened.StorageBackend, local: reopened.StorageBackend.(*LocalFilesystemBackend)}
	reopened.StorageBackend = counter
	appendOrdinal := mediaCount + 1
	coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: "main", Sequence: sourceSequence(mediaCount) + 1, Kind: archiveindex.ObjectMedia}
	appendRecord := v2ScaleMediaRecord(coordinate, appendOrdinal, payloadPath, int64(len(payload)), sharedSHA)
	appendRecord.Segment.LivePresentationOrdinal = 4
	pageNo, _ := mediaPageSlot(appendOrdinal)
	pagePath := v2MediaPagePath("main", false, pageNo)
	pageBefore, rootBefore := statSidecarBytes(t, root, id, pagePath), statRootBytes(t, root, id)
	if err := reopened.PublishShardedMedia(ctx, appendHeader, appendRecord); err != nil {
		t.Fatalf("publish media 100001: %v", err)
	}
	stats := counter.snapshot()
	pageAfter, rootAfter := statSidecarBytes(t, root, id, pagePath), statRootBytes(t, root, id)
	if pageAfter <= pageBefore || rootBefore == 0 || rootAfter == 0 {
		t.Fatalf("append missed page/root write: page %d -> %d root %d -> %d", pageBefore, pageAfter, rootBefore, rootAfter)
	}
	// A live media append writes one bounded media page plus coordinate/live
	// pointers. ArchiveOrdinal lookup uses the page slot directly, so no full
	// recording-wide ordinal index is rewritten.
	if stats.sidecarWrites > 3 || stats.rootWrites != 1 || stats.sidecarReads > 8 || stats.rootReads > 2 || stats.bytesRead > 128<<10 || stats.bytesWritten > 2<<20 {
		t.Fatalf("append amplification not bounded: %#v", stats)
	}
	updated, err := reopened.LoadRecordingHeader(ctx, id)
	if err != nil || updated.Tracks["main"].MediaHighWater != appendOrdinal {
		t.Fatalf("100001st append high-water=%v err=%v", updated, err)
	}
	t.Logf("100001st append I/O: sidecar reads=%d writes=%d root reads=%d writes=%d bytes read=%d written=%d; page bytes %d -> %d", stats.sidecarReads, stats.sidecarWrites, stats.rootReads, stats.rootWrites, stats.bytesRead, stats.bytesWritten, pageBefore, pageAfter)

	// Late historical sequence fills one source-coordinate hole in a 100k
	// archive. Existing media metadata pages stay immutable by ArchiveOrdinal.
	before, err := reopened.LookupShardedMediaByArchiveOrdinal(ctx, id, "main", mediaCount/2)
	if err != nil {
		t.Fatal(err)
	}
	after, err := reopened.LookupShardedMediaByArchiveOrdinal(ctx, id, "main", mediaCount/2+1)
	if err != nil {
		t.Fatal(err)
	}
	liveBefore, err := reopened.LookupShardedMediaByArchiveOrdinal(ctx, id, "main", mediaCount-2)
	if err != nil {
		t.Fatal(err)
	}
	repairCoordinate := archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", Sequence: mediaCount/2 + 1,
		Kind: archiveindex.ObjectMedia,
	}
	repairRecord := v2ScaleMediaRecord(repairCoordinate, appendOrdinal+1, payloadPath, int64(len(payload)), sharedSHA)
	if err := reopened.PublishShardedMedia(ctx, updated, repairRecord); err != nil {
		t.Fatalf("publish 100k historical gap repair: %v", err)
	}
	for _, prior := range []V2MediaRecord{before, after, liveBefore} {
		current, err := reopened.LookupShardedMediaByArchiveOrdinal(ctx, id, "main", prior.Segment.ArchiveOrdinal)
		if err != nil || current.Segment.ArchiveOrdinal != prior.Segment.ArchiveOrdinal || current.Segment.LivePresentationOrdinal != prior.Segment.LivePresentationOrdinal || current.Segment.SHA256 != prior.Segment.SHA256 {
			t.Fatalf("repair changed prior media identity ordinal=%d current=%#v err=%v", prior.Segment.ArchiveOrdinal, current, err)
		}
	}
	ordered := make([]uint64, 0, 3)
	if err := reopened.IterateShardedTimeline(ctx, id, "main", func(record V2MediaRecord) error {
		if record.Segment.Sequence >= mediaCount/2 && record.Segment.Sequence <= mediaCount/2+2 {
			ordered = append(ordered, record.Segment.Sequence)
		}
		return nil
	}); err != nil {
		t.Fatalf("iterate repaired 100k timeline: %v", err)
	}
	if len(ordered) != 3 || ordered[0] != mediaCount/2 || ordered[1] != mediaCount/2+1 || ordered[2] != mediaCount/2+2 {
		t.Fatalf("historical repair timeline order=%v", ordered)
	}
	var liveOrdinals []uint64
	if err := reopened.IterateShardedLiveSlots(ctx, id, "main", 12, func(slot V2LiveSlot) error {
		if slot.Segment != nil {
			liveOrdinals = append(liveOrdinals, slot.Segment.LivePresentationOrdinal)
		}
		return nil
	}); err != nil || len(liveOrdinals) != 4 || liveOrdinals[0] != 1 || liveOrdinals[1] != 2 || liveOrdinals[2] != 3 || liveOrdinals[3] != 4 {
		t.Fatalf("historical repair changed live presentation tail=%v err=%v", liveOrdinals, err)
	}
}

func v2ScaleHeader(id string) *domain.Recording {
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	return &domain.Recording{
		FormatVersion: ShardedArchiveFormatVersion, ShardedArchive: &domain.ShardedArchiveSummary{},
		ID: id, SourceSessionID: "session-" + strings.Repeat("a", 64), Title: "scale fixture", AdapterID: "fixture",
		State: domain.StateRecording, CreatedAt: now, StartedAt: now,
		Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}},
	}
}

func v2ScaleMediaRecord(coordinate archiveindex.Coordinate, ordinal uint64, path string, size int64, digest string) V2MediaRecord {
	return V2MediaRecord{Coordinate: coordinate, IndexOrdinal: ordinal, Segment: domain.Segment{
		ID: fmt.Sprintf("seg-%020d", ordinal), TrackID: coordinate.TrackID, Sequence: coordinate.Sequence,
		SourceEpoch: coordinate.SourceEpoch, DiscontinuitySequence: coordinate.DiscontinuitySequence,
		ArchiveOrdinal: ordinal, Duration: 2, SourceURI: "fixture://source/media", StoragePath: path, PayloadSize: size, SHA256: digest,
	}}
}

func writeShardedFixtureJSON(store *Store, id, relative string, value any) error {
	// Match saveV2JSON's compact canonical sidecar encoding. Indented fixture
	// pages can be much larger than production pages and hide bounded append
	// growth when the production writer compacts the page on its next update.
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	path := filepath.Join(store.root, "recordings", id, filepath.FromSlash(relative+".json"))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

func statRootBytes(t *testing.T, root, id string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(root, "recordings", id, "recording.json"))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func statSidecarBytes(t *testing.T, root, id, relative string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(root, "recordings", id, filepath.FromSlash(relative+".json")))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

type countedV2IO struct {
	sidecarReads, sidecarWrites int
	rootReads, rootWrites       int
	payloadWrites               int
	mediaPageWrites             int
	timelinePageWrites          int
	livePageWrites              int
	claimPageWrites             int
	timelinePageReads           int
	mediaPageReads              int
	coverageStatePageReads      int
	coverageStatePageWrites     int
	listRequests                int
	payloadBytesWritten         int64
	bytesRead, bytesWritten     int64
}

type countingV2Backend struct {
	StorageBackend
	local *LocalFilesystemBackend
	mu    sync.Mutex
	stats countedV2IO
}

type failAfterV2SidecarWrite struct {
	StorageBackend
	relative string
	failure  error
	fired    bool
}

type failAfterV2RootSave struct {
	*LocalFilesystemBackend
	failure error
	matches func(*domain.Recording) bool
	fired   bool
}

type failBeforeV2RootSave struct {
	*LocalFilesystemBackend
	failure error
	matches func(*domain.Recording) bool
	fired   bool
}

func (b *failBeforeV2RootSave) SaveRecordingContext(ctx context.Context, recording *domain.Recording) error {
	if !b.fired && b.matches != nil && b.matches(recording) {
		b.fired = true
		return b.failure
	}
	return b.LocalFilesystemBackend.SaveRecordingContext(ctx, recording)
}

func (b *failAfterV2RootSave) SaveRecordingContext(ctx context.Context, recording *domain.Recording) error {
	if err := b.LocalFilesystemBackend.SaveRecordingContext(ctx, recording); err != nil {
		return err
	}
	if !b.fired && b.matches != nil && b.matches(recording) {
		b.fired = true
		return b.failure
	}
	return nil
}

type failBeforeV2SidecarWrite struct {
	StorageBackend
	relative string
	failure  error
	fired    bool
}

func (b *failBeforeV2SidecarWrite) SaveSidecarContext(ctx context.Context, id, relative string, value any) error {
	if relative == b.relative && !b.fired {
		b.fired = true
		return b.failure
	}
	writer, ok := b.StorageBackend.(v2SidecarContextWriter)
	if !ok {
		return ErrShardedArchiveUnavailable
	}
	return writer.SaveSidecarContext(ctx, id, relative, value)
}

func (b *failBeforeV2SidecarWrite) LoadSidecarContext(ctx context.Context, id, relative string, limit int64, output any) error {
	reader, ok := b.StorageBackend.(v2SidecarContextReader)
	if !ok {
		return ErrSidecarReadUnsupported
	}
	return reader.LoadSidecarContext(ctx, id, relative, limit, output)
}

func (b *failBeforeV2SidecarWrite) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (V2SidecarPage, error) {
	lister, ok := b.StorageBackend.(v2SidecarLister)
	if !ok {
		return V2SidecarPage{}, ErrShardedArchiveUnavailable
	}
	return lister.ListSidecarsContext(ctx, id, prefix, cursor, limit)
}

func (b *failAfterV2SidecarWrite) SaveSidecarContext(ctx context.Context, id, relative string, value any) error {
	writer, ok := b.StorageBackend.(v2SidecarContextWriter)
	if !ok {
		return ErrShardedArchiveUnavailable
	}
	if err := writer.SaveSidecarContext(ctx, id, relative, value); err != nil {
		return err
	}
	if relative == b.relative && !b.fired {
		b.fired = true
		return b.failure
	}
	return nil
}

func (b *failAfterV2SidecarWrite) LoadSidecarContext(ctx context.Context, id, relative string, limit int64, output any) error {
	reader, ok := b.StorageBackend.(v2SidecarContextReader)
	if !ok {
		return ErrSidecarReadUnsupported
	}
	return reader.LoadSidecarContext(ctx, id, relative, limit, output)
}

func (b *failAfterV2SidecarWrite) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (V2SidecarPage, error) {
	lister, ok := b.StorageBackend.(v2SidecarLister)
	if !ok {
		return V2SidecarPage{}, ErrShardedArchiveUnavailable
	}
	return lister.ListSidecarsContext(ctx, id, prefix, cursor, limit)
}

func (b *countingV2Backend) LoadRecordingContext(ctx context.Context, id string) (*domain.Recording, error) {
	b.mu.Lock()
	b.stats.rootReads++
	b.stats.bytesRead += b.fileSize(id, "recording.json")
	b.mu.Unlock()
	return b.local.LoadRecordingContext(ctx, id)
}

func (b *countingV2Backend) SaveRecordingContext(ctx context.Context, recording *domain.Recording) error {
	encoded, _ := json.MarshalIndent(recording, "", "  ")
	b.mu.Lock()
	b.stats.rootWrites++
	b.stats.bytesWritten += int64(len(encoded) + 1)
	b.mu.Unlock()
	return b.local.SaveRecordingContext(ctx, recording)
}

func (b *countingV2Backend) SavePayloadExact(id, relativePath string, reader io.Reader, limit, expectedSize int64) (PayloadResult, error) {
	result, err := b.StorageBackend.SavePayloadExact(id, relativePath, reader, limit, expectedSize)
	if err == nil {
		b.mu.Lock()
		b.stats.payloadWrites++
		b.stats.payloadBytesWritten += result.Size
		b.mu.Unlock()
	}
	return result, err
}

func (b *countingV2Backend) LoadSidecarContext(ctx context.Context, id, relative string, limit int64, output any) error {
	b.mu.Lock()
	b.stats.sidecarReads++
	if strings.HasPrefix(relative, "archive/v2/timeline/") {
		b.stats.timelinePageReads++
	}
	if strings.HasPrefix(relative, "archive/v2/media/") {
		b.stats.mediaPageReads++
	}
	if strings.HasPrefix(relative, "archive/v2/coverage-state/") {
		b.stats.coverageStatePageReads++
	}
	b.stats.bytesRead += b.fileSize(id, relative+".json")
	b.mu.Unlock()
	return b.local.LoadSidecarContext(ctx, id, relative, limit, output)
}

func (b *countingV2Backend) SaveSidecarContext(ctx context.Context, id, relative string, value any) error {
	encoded, _ := json.Marshal(value)
	b.mu.Lock()
	b.stats.sidecarWrites++
	switch {
	case strings.HasPrefix(relative, "archive/v2/media/"):
		b.stats.mediaPageWrites++
	case strings.HasPrefix(relative, "archive/v2/timeline/"):
		b.stats.timelinePageWrites++
	case strings.HasPrefix(relative, "archive/v2/live/"):
		b.stats.livePageWrites++
	case strings.HasPrefix(relative, "archive/v2/claims/"):
		b.stats.claimPageWrites++
	case strings.HasPrefix(relative, "archive/v2/coverage-state/"):
		b.stats.coverageStatePageWrites++
	}
	b.stats.bytesWritten += int64(len(encoded) + 1)
	b.mu.Unlock()
	return b.local.SaveSidecarContext(ctx, id, relative, value)
}

func (b *countingV2Backend) ListSidecarsContext(ctx context.Context, id, prefix, cursor string, limit int) (V2SidecarPage, error) {
	b.mu.Lock()
	b.stats.listRequests++
	b.mu.Unlock()
	return b.local.ListSidecarsContext(ctx, id, prefix, cursor, limit)
}

func (b *countingV2Backend) RecordingFormatVersion(ctx context.Context, id string) (int, error) {
	return b.local.RecordingFormatVersion(ctx, id)
}

func (b *countingV2Backend) fileSize(id, relative string) int64 {
	info, err := os.Stat(filepath.Join(b.local.root, "recordings", id, filepath.FromSlash(relative)))
	if err != nil {
		return 0
	}
	return info.Size()
}

func (b *countingV2Backend) snapshot() countedV2IO {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}
