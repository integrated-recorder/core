package preview

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestDisabledPolicyNeverInvokesFFmpeg(t *testing.T) {
	fixture := newPreviewFixture(t, 3, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if got := fixture.invocations(); got != 0 {
		t.Fatalf("FFmpeg invocations with default disabled policy = %d, want 0", got)
	}
	if got := service.Summary(fixture.recording).FrameCount; got != 0 {
		t.Fatalf("disabled recording has %d frames", got)
	}
	if _, err := os.Lstat(filepath.Join(fixture.root, "previews", fixture.recording.ID)); !os.IsNotExist(err) {
		t.Fatalf("disabled recording created preview staging/projection state: %v", err)
	}
}

func previewTestID(value int) string { return fmt.Sprintf("%032x", value) }

func mustReadDir(t *testing.T, path string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func readCapturedArgs(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read captured FFmpeg args %s: %v", path, err)
	}
	var args []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line != "" {
			args = append(args, line)
		}
	}
	return args
}

func capturedFFmpegArgs(t *testing.T, directory string) []string {
	t.Helper()
	for _, entry := range mustReadDir(t, directory) {
		if strings.HasPrefix(entry.Name(), "args-") {
			return readCapturedArgs(t, filepath.Join(directory, entry.Name()))
		}
	}
	t.Fatal("FFmpeg invocation arguments were not captured")
	return nil
}

func assertKeyframeDecodeArgs(t *testing.T, args []string) {
	t.Helper()
	skip := -1
	input := -1
	for i, arg := range args {
		if arg == "-skip_frame" && i+1 < len(args) && args[i+1] == "nokey" {
			skip = i
		}
		if arg == "-i" {
			input = i
		}
	}
	if skip < 0 || input < 0 || skip >= input {
		t.Fatalf("FFmpeg args must apply generic -skip_frame nokey before input: %v", args)
	}
}

func assertNoKeyframeFilter(t *testing.T, args []string) {
	t.Helper()
	for _, arg := range args {
		if arg == "-skip_frame" {
			t.Fatalf("context fallback must decode ordinary dependent pictures: %v", args)
		}
	}
}

func hasFFmpegOption(args []string, option string) bool {
	for _, arg := range args {
		if arg == option {
			return true
		}
	}
	return false
}

func assertNoSeek(t *testing.T, args []string) {
	t.Helper()
	for _, arg := range args {
		if arg == "-ss" {
			t.Fatalf("target-only extraction must not seek into the segment: %v", args)
		}
	}
}

func assertSeek(t *testing.T, args []string, want float64) {
	t.Helper()
	for i, arg := range args {
		if arg == "-ss" {
			if i+1 >= len(args) {
				t.Fatalf("-ss has no value: %v", args)
			}
			got, err := strconv.ParseFloat(args[i+1], 64)
			if err != nil || math.Abs(got-want) > 0.000001 {
				t.Fatalf("fallback seek=%q, want target boundary %.6f: args=%v", args[i+1], want, args)
			}
			return
		}
	}
	t.Fatalf("fallback context attempt has no target-boundary seek %.6f: %v", want, args)
}

func TestDisabledSummaryDoesNotLoadPreviewIndex(t *testing.T) {
	fixture := newPreviewFixture(t, 0, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	id := "00000000000000000000000000000001"
	service.mu.Lock()
	idx := service.indexLocked(id)
	if idx == nil {
		service.mu.Unlock()
		t.Fatal("could not create initial index")
	}
	idx.Items = append(idx.Items, Frame{ArchiveOrdinal: 1, ImageSHA256: strings.Repeat("a", 64)})
	if err := service.persistIndexLocked(id, idx); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	delete(service.indexes, id)
	delete(service.indexAccess, id)
	service.mu.Unlock()

	recording := &domain.Recording{ID: id, State: domain.StateCompleted}
	summary := service.Summary(recording)
	if summary.Mode != ModeDisabled || summary.FrameCount != 0 {
		t.Fatalf("default-disabled summary = %#v", summary)
	}
	service.mu.Lock()
	_, loaded := service.indexes[id]
	service.mu.Unlock()
	if loaded {
		t.Fatal("default-disabled summary loaded the full preview index")
	}
}

func TestPreviewIndexCacheIsBoundedAndReloadsDurableEntries(t *testing.T) {
	fixture := newPreviewFixture(t, 0, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	firstID := previewTestID(1)
	frameData := validJPEGFixture(t)
	width, height, imageHash, valid := inspectJPEGBytes(t, frameData)
	if !valid {
		t.Fatal("test JPEG fixture is invalid")
	}
	framesDir := filepath.Join(fixture.root, "previews", firstID, "frames")
	if err := ensurePrivateDir(filepath.Dir(framesDir)); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateDir(framesDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(framePath(filepath.Join(fixture.root, "previews"), firstID, 1), frameData, 0600); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	service.policies[firstID] = Policy{Version: 1, Mode: ModeSegment}
	idx := service.indexLocked(firstID)
	if idx == nil {
		service.mu.Unlock()
		t.Fatal("could not create first index")
	}
	idx.Items = []Frame{{ArchiveOrdinal: 1, TrackID: "main", SegmentStartSeconds: 0, SegmentDurationSeconds: 2, FrameTimeSeconds: 0, SegmentSHA256: strings.Repeat("a", 64), ImageSHA256: imageHash, Size: int64(len(frameData)), Width: width, Height: height}}
	idx.Failures = []Failure{{ArchiveOrdinal: 2, Code: "decode_failed", Attempts: 1, Permanent: false}}
	if err := service.persistIndexLocked(firstID, idx); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	// Make the entry dirty so the LRU eviction must durably flush before it can
	// release the resident metadata.
	idx.Failures[0].Attempts = 7
	if err := service.markIndexDirtyLocked(firstID, false); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	service.mu.Unlock()
	for i := 2; i <= previewIndexCacheCap+8; i++ {
		id := previewTestID(i)
		service.mu.Lock()
		service.policies[id] = Policy{Version: 1, Mode: ModeSegment}
		service.mu.Unlock()
		_ = service.Summary(&domain.Recording{ID: id, State: domain.StateCompleted})
		service.mu.Lock()
		resident := len(service.indexes)
		service.mu.Unlock()
		if resident > previewIndexCacheCap {
			t.Fatalf("resident preview indexes = %d, cap %d", resident, previewIndexCacheCap)
		}
	}
	service.mu.Lock()
	_, stillResident := service.indexes[firstID]
	service.mu.Unlock()
	if stillResident {
		t.Fatal("least recently used index was not evicted")
	}

	_ = service.Summary(&domain.Recording{ID: firstID, State: domain.StateCompleted})
	service.mu.Lock()
	reloaded := service.indexes[firstID]
	service.mu.Unlock()
	if reloaded == nil || len(reloaded.Items) != 1 || len(reloaded.Failures) != 1 || reloaded.Failures[0].Attempts != 7 {
		t.Fatalf("durable index did not reload ready frame/failure: %#v", reloaded)
	}
	opened, frame, err := service.OpenFrame(firstID, 1)
	if err != nil {
		t.Fatalf("reloaded frame did not open as a valid ready projection: %v", err)
	}
	_ = opened.Close()
	if frame.ImageSHA256 != imageHash {
		t.Fatalf("reloaded frame hash = %q, want %q", frame.ImageSHA256, imageHash)
	}
	if got := fixture.invocations(); got != 0 {
		t.Fatalf("cache eviction/reload invoked FFmpeg %d times", got)
	}
}

func TestDirtyPreviewIndexCacheEntrySurvivesFailedFlush(t *testing.T) {
	fixture := newPreviewFixture(t, 0, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	firstID := previewTestID(1)
	service.mu.Lock()
	service.policies[firstID] = Policy{Version: 1, Mode: ModeSegment}
	first := service.indexLocked(firstID)
	if first == nil {
		service.mu.Unlock()
		t.Fatal("could not create first index")
	}
	if err := service.persistIndexLocked(firstID, first); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	for i := 2; i <= previewIndexCacheCap; i++ {
		id := previewTestID(i)
		service.policies[id] = Policy{Version: 1, Mode: ModeSegment}
		if service.indexLocked(id) == nil {
			service.mu.Unlock()
			t.Fatal("failed to fill bounded index cache")
		}
	}
	first.Failures = []Failure{{ArchiveOrdinal: 9, Code: "decode_failed", Attempts: 2}}
	if err := service.markIndexDirtyLocked(firstID, false); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	service.mu.Unlock()

	victimPath := filepath.Join(fixture.root, "previews", firstID)
	backupPath := victimPath + ".saved"
	if err := os.Rename(victimPath, backupPath); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, victimPath); err != nil {
		_ = os.Rename(backupPath, victimPath)
		t.Fatal(err)
	}
	defer func() {
		_ = os.Remove(victimPath)
		_ = os.Rename(backupPath, victimPath)
	}()

	service.mu.Lock()
	service.policies[previewTestID(previewIndexCacheCap+1)] = Policy{Version: 1, Mode: ModeSegment}
	newIndex := service.indexLocked(previewTestID(previewIndexCacheCap + 1))
	resident := len(service.indexes)
	kept := service.indexes[firstID] == first && service.dirtyIndexes[firstID] > 0
	service.mu.Unlock()
	if newIndex != nil || resident != previewIndexCacheCap || !kept {
		t.Fatalf("failed dirty eviction changed cache unsafely: loaded=%v resident=%d keptDirty=%v", newIndex != nil, resident, kept)
	}
}

func TestPreviewTimelineCacheIsBoundedAndKeepsExactStarts(t *testing.T) {
	fixture := newPreviewFixture(t, 0, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	segments := []domain.Segment{{ArchiveOrdinal: 1, Duration: 2}, {ArchiveOrdinal: 2, Duration: 3}, {ArchiveOrdinal: 3, Duration: 4}}
	for i := 1; i <= previewTimelineCacheCap+9; i++ {
		id := previewTestID(i)
		service.mu.Lock()
		starts := service.timelineStartsLocked(id, segments)
		resident := len(service.timelines)
		validStarts := len(starts) == 3 && starts[0] == 0 && starts[1] == 2 && starts[2] == 5
		service.mu.Unlock()
		if resident > previewTimelineCacheCap {
			t.Fatalf("resident timeline caches = %d, cap %d", resident, previewTimelineCacheCap)
		}
		if !validStarts {
			t.Fatalf("timeline starts for %s = %v, want [0 2 5]", id, starts)
		}
	}
	service.mu.Lock()
	starts := service.timelineStartsLocked(previewTestID(1), segments)
	validStarts := len(starts) == 3 && starts[0] == 0 && starts[1] == 2 && starts[2] == 5
	service.mu.Unlock()
	if !validStarts {
		t.Fatalf("evicted timeline did not rebuild exact starts: %v", starts)
	}
}

func TestPreviewTimelineRevisionRepositionsStableArchiveFrames(t *testing.T) {
	fixture := newPreviewFixture(t, 0, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	id := fixture.recording.ID
	initial := orderedSegments([]domain.Segment{
		{ID: "first", ArchiveOrdinal: 1, TimelineOrdinal: 1, Duration: 2},
		{ID: "tail", ArchiveOrdinal: 2, TimelineOrdinal: 2, Duration: 4},
	})
	service.mu.Lock()
	initialStarts, changed := service.timelineStartsAndRevisionLocked(id, initial)
	if changed || len(initialStarts) != 2 || initialStarts[0] != 0 || initialStarts[1] != 2 {
		service.mu.Unlock()
		t.Fatalf("initial starts=%v revised=%t", initialStarts, changed)
	}
	generated := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	service.indexes[id] = &Index{Version: ProfileVersion, Profile: defaultProfile(), Items: []Frame{
		{ArchiveOrdinal: 1, FrameTimeSeconds: 0, SegmentStartSeconds: 0, SegmentDurationSeconds: 2, SegmentSHA256: "first-hash", ImageSHA256: "first-image", GeneratedAt: generated},
		{ArchiveOrdinal: 2, FrameTimeSeconds: 2, SegmentStartSeconds: 2, SegmentDurationSeconds: 4, SegmentSHA256: "tail-hash", ImageSHA256: "tail-image", GeneratedAt: generated},
		{ArchiveOrdinal: 4, FrameTimeSeconds: 99, SegmentStartSeconds: 99, SegmentDurationSeconds: 3, SegmentSHA256: "repair-hash", ImageSHA256: "repair-image", GeneratedAt: generated},
	}}
	service.mu.Unlock()

	// A newly discovered prefix and a repaired middle segment both change the
	// playback projection, while the original payload/frame identities stay put.
	current := orderedSegments([]domain.Segment{
		{ID: "tail", ArchiveOrdinal: 2, TimelineOrdinal: 4, Duration: 4, SHA256: "tail-hash"},
		{ID: "first", ArchiveOrdinal: 1, TimelineOrdinal: 2, Duration: 2, SHA256: "first-hash"},
		{ID: "prefix", ArchiveOrdinal: 3, TimelineOrdinal: 1, Duration: 1, SHA256: "prefix-hash"},
		{ID: "repair", ArchiveOrdinal: 4, TimelineOrdinal: 3, Duration: 3, SHA256: "repair-hash"},
	})
	service.mu.Lock()
	starts, revised := service.timelineStartsAndRevisionLocked(id, current)
	if !revised || len(starts) != 4 || starts[0] != 0 || starts[1] != 1 || starts[2] != 3 || starts[3] != 6 {
		service.mu.Unlock()
		t.Fatalf("revised starts=%v revised=%t, want [0 1 3 6] and a revision", starts, revised)
	}
	if !service.refreshFrameTimingsLocked(id, current, starts) {
		service.mu.Unlock()
		t.Fatal("cached frame timeline metadata was not refreshed")
	}
	idx := service.indexes[id]
	framesByOrdinal := make(map[uint64]Frame, len(idx.Items))
	for _, frame := range idx.Items {
		framesByOrdinal[frame.ArchiveOrdinal] = frame
	}
	service.mu.Unlock()
	for ordinal, wantStart := range map[uint64]float64{1: 1, 2: 6, 4: 3} {
		frame := framesByOrdinal[ordinal]
		if frame.SegmentStartSeconds != wantStart || frame.FrameTimeSeconds != wantStart {
			t.Errorf("archive frame %d projected at start=%v time=%v, want %v", ordinal, frame.SegmentStartSeconds, frame.FrameTimeSeconds, wantStart)
		}
		if frame.GeneratedAt != generated || frame.ImageSHA256 == "" {
			t.Errorf("timeline-only revision changed the generated frame asset metadata for archive ordinal %d: %+v", ordinal, frame)
		}
	}

	recording := &domain.Recording{ID: id, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: current}}}
	response, err := service.Items(recording, SamplingNearest, 1, 3.2)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || response.Items[0].ArchiveOrdinal != 4 || response.Items[0].FrameTimeSeconds != 3 {
		t.Fatalf("nearest seek after timeline revision = %+v, want repaired archive ordinal 4 at 3s", response.Items)
	}
}

func TestPreviewLegacyTimelineFallsBackToArchiveOrder(t *testing.T) {
	segments := orderedSegments([]domain.Segment{
		{ID: "later", ArchiveOrdinal: 2, Sequence: 1, Duration: 3},
		{ID: "earlier", ArchiveOrdinal: 1, Sequence: 99, Duration: 2},
	})
	if len(segments) != 2 || segments[0].ID != "earlier" || segments[1].ID != "later" {
		t.Fatalf("legacy segments without TimelineOrdinal order = %+v", segments)
	}
	starts := segmentStarts(segments)
	if starts[1] != 0 || starts[2] != 2 {
		t.Fatalf("legacy archive timeline starts = %v, want archive 1 at 0 and archive 2 at 2", starts)
	}
}

func TestSegmentPreviewSamplingDoesNotDecodeAgainAndRestartReusesIndex(t *testing.T) {
	fixture := newPreviewFixture(t, 12, false, "normal")
	service := fixture.open(t)
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, fixture.recording, 12)
	if got := fixture.invocations(); got != 12 {
		t.Fatalf("FFmpeg invocations = %d, want one per committed segment", got)
	}
	var response Response
	for _, count := range []int{24, 48, 96} {
		sample, err := service.Items(fixture.recording, SamplingUniform, count, 0)
		if err != nil || len(sample.Items) != 12 {
			t.Fatalf("uniform sample count for limit %d = %d, err=%v; want 12 unique available frames", count, len(sample.Items), err)
		}
		response = sample
	}
	if _, err := service.Items(fixture.recording, SamplingNearest, 1, 17.25); err != nil {
		t.Fatal(err)
	}
	if got := fixture.invocations(); got != 12 {
		t.Fatalf("presentation sampling started FFmpeg: invocations=%d", got)
	}
	for i, frame := range response.Items {
		if i > 0 && (frame.FrameTimeSeconds <= response.Items[i-1].FrameTimeSeconds || frame.ArchiveOrdinal == response.Items[i-1].ArchiveOrdinal) {
			t.Fatalf("uniform selection is not unique ascending: %#v", response.Items)
		}
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	service = fixture.open(t)
	defer service.Close(context.Background())
	reconcileUntilFrameCount(t, service, fixture.recording, 12)
	if got := fixture.invocations(); got != 12 {
		t.Fatalf("restart re-decoded ready frames: invocations=%d", got)
	}
}

func TestIndexSnapshotBatchingAndCrashRecoveryOfPublishedFrames(t *testing.T) {
	const segmentCount = indexPersistBatch + 8
	fixture := newPreviewFixture(t, segmentCount, false, "normal")
	fixture.recording.State = domain.StateRecording
	service := fixture.open(t)
	var writes atomic.Int64
	service.onIndexPersist = func(string) { writes.Add(1) }
	service.mu.Lock()
	if err := service.persistIndexLocked(fixture.recording.ID, service.indexLocked(fixture.recording.ID)); err != nil {
		service.mu.Unlock()
		t.Fatal(err)
	}
	service.mu.Unlock()
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		_ = service.Reconcile()
		service.mu.Lock()
		writeCount := writes.Load()
		count := len(service.indexLocked(fixture.recording.ID).Items)
		service.mu.Unlock()
		return writeCount >= 2 && count >= indexPersistBatch
	})
	indexPath := filepath.Join(fixture.root, "previews", fixture.recording.ID, "index.json")
	crashSnapshot, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, fixture.recording, segmentCount)
	service.mu.Lock()
	itemsBeforeClose := len(service.indexLocked(fixture.recording.ID).Items)
	writesBeforeClose := writes.Load()
	service.mu.Unlock()
	if itemsBeforeClose != segmentCount || writesBeforeClose >= segmentCount {
		t.Fatalf("index snapshot writes scaled per frame: items=%d writes=%d", itemsBeforeClose, writesBeforeClose)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	writesAfterClose := writes.Load()
	service.mu.Unlock()
	if writesAfterClose >= segmentCount {
		t.Fatalf("batched index still wrote once per frame: writes=%d frames=%d", writesAfterClose, segmentCount)
	}
	// Restore the durable batch snapshot to model a crash after later frame
	// publication but before the next index snapshot was committed.
	if err := os.WriteFile(indexPath, crashSnapshot, 0600); err != nil {
		t.Fatal(err)
	}
	service = fixture.open(t)
	defer service.Close(context.Background())
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if got := service.Summary(fixture.recording).FrameCount; got != segmentCount {
		t.Fatalf("startup did not recover published frames omitted from snapshot: got %d want %d", got, segmentCount)
	}
	if got := fixture.invocations(); got != segmentCount {
		t.Fatalf("startup re-decoded already published frames: invocations=%d want=%d", got, segmentCount)
	}
}

func TestPreviewExtractionUsesTargetOnlyFastPathAndStagesItsInit(t *testing.T) {
	fixture := newPreviewFixture(t, 5, true, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	frame, code, _ := service.extract(context.Background(), fixture.recording, 5)
	if code != "" {
		t.Fatalf("context extraction failed: %s", code)
	}
	entries, err := os.ReadDir(fixture.captureDir)
	if err != nil {
		t.Fatal(err)
	}
	var playlist string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "playlist-") {
			data, readErr := os.ReadFile(filepath.Join(fixture.captureDir, entry.Name()))
			if readErr != nil {
				t.Fatal(readErr)
			}
			playlist = string(data)
			break
		}
	}
	if playlist == "" || !strings.Contains(playlist, "#EXT-X-MAP") {
		t.Fatalf("staged HLS playlist has no initialization map: %q", playlist)
	}
	assertKeyframeDecodeArgs(t, capturedFFmpegArgs(t, fixture.captureDir))
	assertNoSeek(t, capturedFFmpegArgs(t, fixture.captureDir))
	if got := strings.Count(playlist, "#EXTINF:"); got != 1 {
		t.Fatalf("target-only fast path staged %d media segments, want 1: %s", got, playlist)
	}
	if frame.SegmentStartSeconds != 8 || frame.FrameTimeSeconds != 8 {
		t.Fatalf("target frame timing = start %v/frame %v, want 8/8", frame.SegmentStartSeconds, frame.FrameTimeSeconds)
	}
	var stagedSegments, stagedInits int
	for _, entry := range mustReadDir(t, fixture.captureDir) {
		if strings.HasPrefix(entry.Name(), "staged-") {
			switch {
			case strings.Contains(entry.Name(), "segment-"):
				stagedSegments++
			case strings.Contains(entry.Name(), "init-"):
				stagedInits++
			}
		}
	}
	if stagedSegments != 1 || stagedInits != 1 {
		t.Fatalf("target-only stage contained %d media/%d init files, want 1/1", stagedSegments, stagedInits)
	}
}

func TestPreviewExtractionAddsContextOnlyAfterTargetDecodeFailure(t *testing.T) {
	fixture := newPreviewFixture(t, 5, false, "fallback")
	service := fixture.open(t)
	defer service.Close(context.Background())
	frame, code, _ := service.extract(context.Background(), fixture.recording, 5)
	if code != "" || frame.ArchiveOrdinal != 5 {
		t.Fatalf("context fallback extraction = ordinal %d, code %q", frame.ArchiveOrdinal, code)
	}
	var playlists []string
	for _, entry := range mustReadDir(t, fixture.captureDir) {
		if strings.HasPrefix(entry.Name(), "playlist-") {
			data, err := os.ReadFile(filepath.Join(fixture.captureDir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			playlists = append(playlists, string(data))
		}
	}
	counts := map[int]bool{}
	for _, playlist := range playlists {
		counts[strings.Count(playlist, "#EXTINF:")] = true
	}
	if len(playlists) != 2 || !counts[1] || !counts[2] {
		t.Fatalf("decode attempts did not use target-only then one prior segment: %#v", playlists)
	}
	if got := fixture.invocations(); got != 2 {
		t.Fatalf("fallback invocation count = %d, want target + first context attempt", got)
	}
	if frame.FrameTimeSeconds != frame.SegmentStartSeconds {
		t.Fatalf("earliest-frame timestamp must be segment start: %#v", frame)
	}
}

func TestBuildContextAtStartUsesCachedPlaybackPrefix(t *testing.T) {
	fixture := newPreviewFixture(t, 5, false, "normal")
	track := fixture.recording.Tracks["main"]
	window, start, targetOffset, err := buildContextAtStart(fixture.recording, track, track.Segments, 5, 123.5)
	if err != nil {
		t.Fatal(err)
	}
	if start != 123.5 {
		t.Fatalf("context rebuilt playback prefix: start=%v want cached 123.5", start)
	}
	if len(window) != 4 || targetOffset != 6 {
		t.Fatalf("bounded target offset/context = len %d offset %v, want 4/6", len(window), targetOffset)
	}
}

func TestDiscontinuityStopsContextAndMarksLocalPlaylist(t *testing.T) {
	fixture := newPreviewFixture(t, 5, true, "normal")
	track := fixture.recording.Tracks["main"]
	track.Segments[2].Discontinuity = true
	track.Segments[2].SourceEpoch = 2
	track.Segments[2].DiscontinuitySequence = 1
	track.Segments[3].SourceEpoch = 2
	track.Segments[3].DiscontinuitySequence = 1
	track.Segments[4].SourceEpoch = 2
	track.Segments[4].DiscontinuitySequence = 1
	service := fixture.open(t)
	defer service.Close(context.Background())
	window, _, _, err := buildContextAtStart(fixture.recording, track, track.Segments, 5, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != 3 || window[0].ArchiveOrdinal != 3 || !window[0].Discontinuity || window[2].ArchiveOrdinal != 5 {
		t.Fatalf("decoder context crossed discontinuity: %#v", window)
	}
	tempDir := t.TempDir()
	local, err := service.stageWindow(context.Background(), fixture.recording.ID, track, window, tempDir)
	if err != nil {
		t.Fatal(err)
	}
	playlist, err := localPlaylist(track, window, local)
	if err != nil {
		t.Fatal(err)
	}
	marker := strings.Index(playlist, "#EXT-X-DISCONTINUITY\n")
	firstSegment := strings.Index(playlist, "#EXTINF:")
	if marker < 0 || firstSegment < 0 || marker > firstSegment || strings.Count(playlist, "#EXT-X-DISCONTINUITY") != 1 {
		t.Fatalf("local HLS context does not preserve its starting discontinuity boundary: %s", playlist)
	}
}

func TestUniformSamplingUsesFullPlaybackDurationWhenTailIsMissing(t *testing.T) {
	fixture := newPreviewFixture(t, 50, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	service.mu.Lock()
	service.indexes[fixture.recording.ID] = &Index{Version: ProfileVersion, Profile: defaultProfile(), Items: []Frame{
		{ArchiveOrdinal: 5, FrameTimeSeconds: 10},
		{ArchiveOrdinal: 10, FrameTimeSeconds: 20},
		{ArchiveOrdinal: 20, FrameTimeSeconds: 40},
		{ArchiveOrdinal: 30, FrameTimeSeconds: 60},
	}}
	service.mu.Unlock()
	response, err := service.Items(fixture.recording, SamplingUniform, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 2 || response.Items[0].FrameTimeSeconds != 20 || response.Items[1].FrameTimeSeconds != 60 {
		t.Fatalf("uniform sample compressed to indexed preview range instead of full 100s recording: %#v", response.Items)
	}
}

func TestSameSizeMutatedJPEGIsReextractedFromCanonicalSegment(t *testing.T) {
	fixture := newPreviewFixture(t, 1, false, "normal")
	baseJPEG, err := os.ReadFile(filepath.Join(fixture.root, "fake.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	originalJPEG := jpegWithComment(baseJPEG, "AAAA")
	if err := os.WriteFile(filepath.Join(fixture.root, "fake.jpg"), originalJPEG, 0600); err != nil {
		t.Fatal(err)
	}
	service := fixture.open(t)
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, fixture.recording, 1)
	service.mu.Lock()
	originalFrame, ok := findFrame(service.indexLocked(fixture.recording.ID).Items, 1)
	service.mu.Unlock()
	if !ok {
		t.Fatal("frame was not indexed")
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := framePath(filepath.Join(fixture.root, "previews"), fixture.recording.ID, 1)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mutated := jpegWithComment(original, "BBBB")
	if len(mutated) != len(original) {
		t.Fatal("test mutation changed JPEG size")
	}
	if err := os.WriteFile(path, mutated, 0600); err != nil {
		t.Fatal(err)
	}
	width, height, digest, valid := inspectJPEG(path, int64(len(mutated)))
	if !valid || digest == originalFrame.ImageSHA256 || width != originalFrame.Width || height != originalFrame.Height {
		t.Fatalf("test did not create a same-size valid but changed JPEG: valid=%t dimensions=%dx%d digest=%s", valid, width, height, digest)
	}
	service = fixture.open(t)
	defer service.Close(context.Background())
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, fixture.recording, 1)
	if fixture.invocations() < 2 {
		t.Fatalf("corrupt same-size image was not re-extracted: invocations=%d", fixture.invocations())
	}
	imageFile, repaired, err := service.OpenFrame(fixture.recording.ID, 1)
	if err != nil {
		t.Fatalf("mutated indexed JPEG was not repaired: %v", err)
	}
	_ = imageFile.Close()
	if repaired.ImageSHA256 != originalFrame.ImageSHA256 || repaired.SegmentSHA256 != originalFrame.SegmentSHA256 {
		t.Fatalf("repaired frame metadata does not match canonical source: got=%+v want=%+v", repaired, originalFrame)
	}
}

func jpegWithComment(original []byte, comment string) []byte {
	if len(comment) != 4 || len(original) < 2 || original[0] != 0xff || original[1] != 0xd8 {
		return nil
	}
	if marker := bytes.Index(original, []byte{0xff, 0xfe, 0x00, 0x06}); marker >= 0 && marker+8 <= len(original) {
		result := append([]byte(nil), original...)
		copy(result[marker+4:marker+8], comment)
		return result
	}
	marker := []byte{0xff, 0xfe, 0x00, 0x06, comment[0], comment[1], comment[2], comment[3]}
	result := make([]byte, 0, len(original)+len(marker))
	result = append(result, original[:2]...)
	result = append(result, marker...)
	result = append(result, original[2:]...)
	return result
}

func TestFFmpegMPEGTSPreviewIndexUsesCanonicalBytesAndTimeline(t *testing.T) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	fixtureDir := t.TempDir()
	manifestPath := filepath.Join(fixtureDir, "source.m3u8")
	segmentPattern := filepath.Join(fixtureDir, "segment-%03d.ts")
	command := exec.Command(ffmpegPath, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-t", "5", "-an", "-c:v", "mpeg2video", "-g", "25", "-q:v", "4", "-f", "hls", "-hls_time", "1", "-hls_list_size", "0", "-hls_flags", "independent_segments", "-hls_segment_filename", segmentPattern, manifestPath)
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Skipf("local ffmpeg cannot generate MPEG-TS fixture: %v (%s)", runErr, output)
	}
	segments, durations := readMPEGTSFixture(t, manifestPath, fixtureDir)
	if len(segments) < 3 {
		t.Fatalf("generated fixture has only %d segments", len(segments))
	}
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "1234567890abcdef1234567890abcdef"
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	wantPayloads := make([][]byte, len(segments))
	for i, name := range segments {
		payload, readErr := os.ReadFile(filepath.Join(fixtureDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		wantPayloads[i] = payload
		path := fmt.Sprintf("tracks/main/%06d.ts", i+1)
		saved, saveErr := store.SavePayload(id, path, bytes.NewReader(payload), 8<<20)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		track.Segments = append(track.Segments, domain.Segment{ID: fmt.Sprintf("ts-%d", i+1), TrackID: "main", Sequence: uint64(50 + i), ArchiveOrdinal: uint64(i + 1), SourceEpoch: 1, Duration: durations[i], StoragePath: path, PayloadSize: saved.Size, SHA256: saved.SHA256})
	}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	list := func() []*domain.Recording { return []*domain.Recording{recording} }
	service, err := Open(root, store, list, ffmpegPath)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	if _, err := service.SetMode(id, ModeSegment); err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, recording, len(segments))
	items, err := service.Items(recording, SamplingRecent, 100, 0)
	if err != nil || len(items.Items) != len(segments) {
		t.Fatalf("preview index item count=%d err=%v want=%d", len(items.Items), err, len(segments))
	}
	var elapsed float64
	for i, frame := range items.Items {
		segment := track.Segments[i]
		if frame.ArchiveOrdinal != uint64(i+1) || frame.SegmentSHA256 != segment.SHA256 || math.Abs(frame.SegmentStartSeconds-elapsed) > 0.001 || math.Abs(frame.FrameTimeSeconds-elapsed) > 0.03 {
			t.Fatalf("preview mapping[%d]=%+v segment=%+v elapsed=%f", i, frame, segment, elapsed)
		}
		imageFile, _, openErr := service.OpenFrame(id, frame.ArchiveOrdinal)
		if openErr != nil {
			t.Fatalf("open frame %d: %v", frame.ArchiveOrdinal, openErr)
		}
		cfg, format, decodeErr := image.DecodeConfig(imageFile)
		_ = imageFile.Close()
		if decodeErr != nil || format != "jpeg" || cfg.Width <= 0 || cfg.Height <= 0 {
			t.Fatalf("frame %d is not a decodable JPEG: cfg=%+v format=%q err=%v", frame.ArchiveOrdinal, cfg, format, decodeErr)
		}
		payloadFile, payloadErr := store.OpenPayload(id, segment.StoragePath)
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		gotPayload, readErr := io.ReadAll(payloadFile)
		_ = payloadFile.Close()
		if readErr != nil || !bytes.Equal(gotPayload, wantPayloads[i]) {
			t.Fatalf("canonical TS segment %d changed during extraction: read err=%v", i+1, readErr)
		}
		elapsed += durations[i]
	}
	invocations := countImageFiles(filepath.Join(root, "previews", id, "frames"))
	if invocations != len(segments) {
		t.Fatalf("preview files=%d want one per media segment=%d", invocations, len(segments))
	}
}

func TestFFmpegMPEGTSFallbackUsesPriorLongGOPContext(t *testing.T) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	fixtureDir := t.TempDir()
	manifestPath := filepath.Join(fixtureDir, "source.m3u8")
	segmentPattern := filepath.Join(fixtureDir, "segment-%03d.ts")
	generate := exec.Command(ffmpegPath,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-t", "8", "-an",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-g", "250", "-keyint_min", "250", "-sc_threshold", "0", "-x264-params", "repeat-headers=0",
		"-f", "hls", "-hls_time", "2", "-hls_list_size", "0", "-hls_flags", "split_by_time",
		"-hls_segment_filename", segmentPattern, manifestPath,
	)
	if output, runErr := generate.CombinedOutput(); runErr != nil {
		t.Skipf("local FFmpeg lacks required libx264/MPEG-TS HLS split_by_time support: %v (%s)", runErr, output)
	}
	names, durations := readMPEGTSFixture(t, manifestPath, fixtureDir)
	if len(names) < 2 || len(durations) < 2 {
		t.Fatalf("long-GOP split_by_time fixture has %d segments; want at least two", len(names))
	}

	// Prove that the second segment cannot decode alone before exercising the
	// service's target-only then prior-context fallback.
	targetOnlyPath := filepath.Join(fixtureDir, "target-only.m3u8")
	targetOnly := fmt.Sprintf("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:3\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:%.6f,\n%s\n#EXT-X-ENDLIST\n", durations[1], names[1])
	if err := os.WriteFile(targetOnlyPath, []byte(targetOnly), 0600); err != nil {
		t.Fatal(err)
	}
	directOutput := filepath.Join(fixtureDir, "target-only.jpg")
	direct := exec.Command(ffmpegPath,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-xerror",
		"-protocol_whitelist", "file", "-skip_frame", "nokey", "-i", targetOnlyPath,
		"-map", "0:v:0", "-frames:v", "1", "-vf", "scale=480:270:force_original_aspect_ratio=decrease:force_divisible_by=2,pad=480:270:(ow-iw)/2:(oh-ih)/2",
		"-an", "-sn", "-dn", "-c:v", "mjpeg", "-q:v", "4", "-f", "image2", directOutput,
	)
	direct.Dir = fixtureDir
	direct.Stdout, direct.Stderr = io.Discard, io.Discard
	directErr := direct.Run()
	directDecodeFailed := directErr != nil
	if directErr == nil {
		info, statErr := os.Stat(directOutput)
		if statErr == nil && validJPEGPath(directOutput, info.Size()) {
			t.Skip("generated split_by_time segment unexpectedly decodes alone on this FFmpeg build; fixture does not exercise fallback")
		}
		directDecodeFailed = statErr != nil || !validJPEGPath(directOutput, info.Size())
	}
	if !directDecodeFailed {
		t.Fatal("target-only decode unexpectedly succeeded; generated fixture does not require prior context")
	}

	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "34567890abcdef1234567890abcdef12"
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	wantPayloads := make([][]byte, len(names))
	for i, name := range names {
		payload, readErr := os.ReadFile(filepath.Join(fixtureDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		wantPayloads[i] = payload
		path := fmt.Sprintf("tracks/main/%06d.ts", i+1)
		saved, saveErr := store.SavePayload(id, path, bytes.NewReader(payload), 8<<20)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		track.Segments = append(track.Segments, domain.Segment{ID: fmt.Sprintf("long-gop-%d", i+1), TrackID: "main", Sequence: uint64(i + 1), ArchiveOrdinal: uint64(i + 1), SourceEpoch: 1, Duration: durations[i], StoragePath: path, PayloadSize: saved.Size, SHA256: saved.SHA256})
	}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	captureDir := t.TempDir()
	t.Setenv("IR_PREVIEW_FALLBACK_CAPTURE_DIR", captureDir)
	t.Setenv("IR_PREVIEW_FALLBACK_REAL_FFMPEG", ffmpegPath)
	wrapperPath := filepath.Join(t.TempDir(), "ffmpeg-capture-wrapper")
	wrapper := "#!/bin/sh\nprintf x > \"$IR_PREVIEW_FALLBACK_CAPTURE_DIR/call-$$\"\nprintf '%s\\n' \"$@\" > \"$IR_PREVIEW_FALLBACK_CAPTURE_DIR/args-$$.txt\"\ncp input.m3u8 \"$IR_PREVIEW_FALLBACK_CAPTURE_DIR/playlist-$$.m3u8\"\nfor staged in segment-* init-*; do if [ -f \"$staged\" ]; then cp \"$staged\" \"$IR_PREVIEW_FALLBACK_CAPTURE_DIR/staged-$$-$staged\"; fi; done\nexec \"$IR_PREVIEW_FALLBACK_REAL_FFMPEG\" \"$@\"\n"
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, func() []*domain.Recording { return []*domain.Recording{recording} }, wrapperPath)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	if _, err := service.SetMode(id, ModeSegment); err != nil {
		t.Fatal(err)
	}
	// Segment one is an H.264 random-access segment and must take the target-only
	// path. Segment two is deliberately split inside a long GOP and must fall
	// back to compatible prior context.
	firstFrame, firstCode, firstPermanent := service.extract(context.Background(), recording, 1)
	if firstCode != "" || firstPermanent || firstFrame.ArchiveOrdinal != 1 {
		t.Fatalf("H.264 target-only fast path = ordinal %d, code=%q permanent=%t", firstFrame.ArchiveOrdinal, firstCode, firstPermanent)
	}
	frame, code, permanent := service.extract(context.Background(), recording, 2)
	if code != "" || permanent || frame.ArchiveOrdinal != 2 {
		t.Fatalf("prior-context extraction = frame ordinal %d, code=%q permanent=%t", frame.ArchiveOrdinal, code, permanent)
	}
	width, height, _, valid := inspectJPEGBytes(t, frame.imageData)
	if !valid || width != frame.Width || height != frame.Height || width <= 0 || height <= 0 {
		t.Fatalf("fallback result is not a valid JPEG: frame=%+v dimensions=%dx%d valid=%t", frame, width, height, valid)
	}
	if frame.SegmentSHA256 != track.Segments[1].SHA256 || math.Abs(frame.SegmentStartSeconds-durations[0]) > 0.001 || math.Abs(frame.FrameTimeSeconds-durations[0]) > 0.001 {
		t.Fatalf("fallback frame metadata does not map to target segment start: frame=%+v target=%+v", frame, track.Segments[1])
	}
	type attempt struct {
		id       string
		segments int
		args     []string
		playlist string
	}
	var attempts []attempt
	invocations := 0
	for _, entry := range mustReadDir(t, captureDir) {
		if strings.HasPrefix(entry.Name(), "call-") {
			invocations++
		}
		if strings.HasPrefix(entry.Name(), "playlist-") {
			id := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "playlist-"), ".m3u8")
			data, readErr := os.ReadFile(filepath.Join(captureDir, entry.Name()))
			if readErr != nil {
				t.Fatal(readErr)
			}
			attempts = append(attempts, attempt{
				id:       id,
				segments: strings.Count(string(data), "#EXTINF:"),
				args:     readCapturedArgs(t, filepath.Join(captureDir, "args-"+id+".txt")),
				playlist: string(data),
			})
		}
	}
	if invocations != 3 || len(attempts) != 3 {
		t.Fatalf("service did not run H.264 fast path plus target-only/context fallback: invocations=%d attempts=%v", invocations, attempts)
	}
	var h264FastPath, fallbackTarget, fallbackContext bool
	for _, got := range attempts {
		stagedPayload := func(name string) []byte {
			data, readErr := os.ReadFile(filepath.Join(captureDir, "staged-"+got.id+"-"+name))
			if readErr != nil {
				t.Fatalf("read staged input %s: %v", name, readErr)
			}
			return data
		}
		switch {
		case got.segments == 1 && bytes.Equal(stagedPayload("segment-1.ts"), wantPayloads[0]):
			h264FastPath = true
			assertKeyframeDecodeArgs(t, got.args)
			assertNoSeek(t, got.args)
		case got.segments == 1 && bytes.Equal(stagedPayload("segment-1.ts"), wantPayloads[1]):
			fallbackTarget = true
			assertKeyframeDecodeArgs(t, got.args)
			assertNoSeek(t, got.args)
		case got.segments == 2 && bytes.Equal(stagedPayload("segment-1.ts"), wantPayloads[0]) && bytes.Equal(stagedPayload("segment-2.ts"), wantPayloads[1]):
			fallbackContext = true
			assertNoKeyframeFilter(t, got.args)
			assertSeek(t, got.args, durations[0])
		default:
			t.Fatalf("unexpected H.264 decoder input: segments=%d playlist=%s args=%v", got.segments, got.playlist, got.args)
		}
	}
	if !h264FastPath || !fallbackTarget || !fallbackContext {
		t.Fatalf("missing expected H.264 extraction attempts: fast=%t target=%t context=%t", h264FastPath, fallbackTarget, fallbackContext)
	}
	for i, segment := range track.Segments {
		payloadFile, payloadErr := store.OpenPayload(id, segment.StoragePath)
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		gotPayload, readErr := io.ReadAll(payloadFile)
		_ = payloadFile.Close()
		if readErr != nil || !bytes.Equal(gotPayload, wantPayloads[i]) {
			t.Fatalf("canonical H.264 TS segment %d changed during preview extraction: read err=%v", i+1, readErr)
		}
	}
}

func TestFFmpegFMP4PreviewExtractionIncludesInitializationMap(t *testing.T) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	fixtureDir := t.TempDir()
	manifestPath := filepath.Join(fixtureDir, "source.m3u8")
	segmentPattern := filepath.Join(fixtureDir, "segment-%03d.m4s")
	command := exec.Command(ffmpegPath, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-t", "6", "-an", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-g", "25", "-keyint_min", "25", "-sc_threshold", "0", "-f", "hls", "-hls_time", "1", "-hls_list_size", "0", "-hls_flags", "independent_segments", "-hls_segment_type", "fmp4", "-hls_fmp4_init_filename", "init.mp4", "-hls_segment_filename", segmentPattern, manifestPath)
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Skipf("local ffmpeg cannot generate HLS fMP4 with the available encoder/muxer: %v (%s)", runErr, output)
	}
	initName, names, durations := readFMP4Fixture(t, manifestPath)
	if initName == "" || len(names) < 3 {
		t.Fatalf("generated fMP4 fixture lacks init or enough media segments: init=%q segments=%d", initName, len(names))
	}
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "234567890abcdef1234567890abcdef1"
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	initBytes, err := os.ReadFile(filepath.Join(fixtureDir, initName))
	if err != nil {
		t.Fatal(err)
	}
	initPath := "tracks/main/init.mp4"
	initSaved, err := store.SavePayload(id, initPath, bytes.NewReader(initBytes), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	track.InitSegments = append(track.InitSegments, domain.Segment{ID: "init-fmp4", TrackID: "main", StoragePath: initPath, PayloadSize: initSaved.Size, SHA256: initSaved.SHA256, IsInit: true})
	wantPayloads := make([][]byte, len(names))
	for i, name := range names {
		payload, readErr := os.ReadFile(filepath.Join(fixtureDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		wantPayloads[i] = payload
		path := fmt.Sprintf("tracks/main/%06d.m4s", i+1)
		saved, saveErr := store.SavePayload(id, path, bytes.NewReader(payload), 8<<20)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		track.Segments = append(track.Segments, domain.Segment{ID: fmt.Sprintf("fmp4-%d", i+1), TrackID: "main", Sequence: uint64(i + 1), ArchiveOrdinal: uint64(i + 1), SourceEpoch: 1, Duration: durations[i], InitSegmentID: "init-fmp4", StoragePath: path, PayloadSize: saved.Size, SHA256: saved.SHA256})
	}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	captureDir := t.TempDir()
	t.Setenv("IR_PREVIEW_FMP4_CAPTURE_DIR", captureDir)
	t.Setenv("IR_PREVIEW_FMP4_REAL_FFMPEG", ffmpegPath)
	wrapperPath := filepath.Join(t.TempDir(), "ffmpeg-fmp4-capture-wrapper")
	wrapper := "#!/bin/sh\nprintf x > \"$IR_PREVIEW_FMP4_CAPTURE_DIR/call-$$\"\nprintf '%s\\n' \"$@\" > \"$IR_PREVIEW_FMP4_CAPTURE_DIR/args-$$.txt\"\ncp input.m3u8 \"$IR_PREVIEW_FMP4_CAPTURE_DIR/playlist-$$.m3u8\"\nfor staged in segment-* init-*; do if [ -f \"$staged\" ]; then cp \"$staged\" \"$IR_PREVIEW_FMP4_CAPTURE_DIR/staged-$$-$staged\"; fi; done\nexec \"$IR_PREVIEW_FMP4_REAL_FFMPEG\" \"$@\"\n"
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, func() []*domain.Recording { return []*domain.Recording{recording} }, wrapperPath)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	if _, err := service.SetMode(id, ModeSegment); err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, recording, len(names))
	response, err := service.Items(recording, SamplingRecent, 100, 0)
	if err != nil || len(response.Items) != len(names) {
		t.Fatalf("fMP4 preview index items=%d err=%v want=%d", len(response.Items), err, len(names))
	}
	var elapsed float64
	for i, frame := range response.Items {
		if frame.ArchiveOrdinal != uint64(i+1) || frame.TrackID != "main" || frame.SegmentSHA256 != track.Segments[i].SHA256 || math.Abs(frame.SegmentStartSeconds-elapsed) > 0.001 || math.Abs(frame.FrameTimeSeconds-elapsed) > 0.05 {
			t.Fatalf("fMP4 preview mapping[%d]=%+v segment=%+v elapsed=%f", i, frame, track.Segments[i], elapsed)
		}
		imageFile, _, openErr := service.OpenFrame(id, frame.ArchiveOrdinal)
		if openErr != nil {
			t.Fatalf("fMP4 frame %d was not published: %v", frame.ArchiveOrdinal, openErr)
		}
		_, format, decodeErr := image.DecodeConfig(imageFile)
		_ = imageFile.Close()
		if decodeErr != nil || format != "jpeg" {
			t.Fatalf("fMP4 frame %d is not a valid JPEG: format=%q err=%v", frame.ArchiveOrdinal, format, decodeErr)
		}
		payloadFile, payloadErr := store.OpenPayload(id, track.Segments[i].StoragePath)
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		gotPayload, readErr := io.ReadAll(payloadFile)
		_ = payloadFile.Close()
		if readErr != nil || !bytes.Equal(gotPayload, wantPayloads[i]) {
			t.Fatalf("canonical fMP4 segment %d changed: read err=%v", i+1, readErr)
		}
		elapsed += durations[i]
	}
	var targetOnlyInitAttempt bool
	for _, entry := range mustReadDir(t, captureDir) {
		if !strings.HasPrefix(entry.Name(), "playlist-") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "playlist-"), ".m3u8")
		data, readErr := os.ReadFile(filepath.Join(captureDir, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		args := readCapturedArgs(t, filepath.Join(captureDir, "args-"+id+".txt"))
		if hasFFmpegOption(args, "-ss") {
			assertNoKeyframeFilter(t, args)
		} else {
			assertKeyframeDecodeArgs(t, args)
			assertNoSeek(t, args)
		}
		if strings.Count(string(data), "#EXTINF:") != 1 {
			continue
		}
		var mediaCount, initCount int
		for _, staged := range mustReadDir(t, captureDir) {
			if !strings.HasPrefix(staged.Name(), "staged-"+id+"-") {
				continue
			}
			name := strings.TrimPrefix(staged.Name(), "staged-"+id+"-")
			if strings.HasPrefix(name, "segment-") && strings.HasSuffix(name, ".m4s") {
				mediaCount++
			}
			if strings.HasPrefix(name, "init-") && strings.HasSuffix(name, ".mp4") {
				initCount++
			}
		}
		if strings.Contains(string(data), "#EXT-X-MAP") && mediaCount == 1 && initCount == 1 {
			assertNoSeek(t, args)
			targetOnlyInitAttempt = true
			break
		}
	}
	if !targetOnlyInitAttempt {
		t.Fatal("fMP4 fast path did not stage exactly the required init section and one target fragment")
	}
}

func TestFFmpegHEVCTSPreviewUsesTargetOnlyFastPath(t *testing.T) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	encoders, err := exec.Command(ffmpegPath, "-hide_banner", "-encoders").CombinedOutput()
	if err != nil || !bytes.Contains(encoders, []byte("libx265")) {
		t.Skip("local FFmpeg does not include the libx265 encoder")
	}
	fixtureDir := t.TempDir()
	manifestPath := filepath.Join(fixtureDir, "source.m3u8")
	segmentPattern := filepath.Join(fixtureDir, "segment-%03d.ts")
	generate := exec.Command(ffmpegPath,
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-t", "4", "-an",
		"-c:v", "libx265", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-g", "25",
		"-x265-params", "log-level=error:keyint=25:min-keyint=25:scenecut=0:pools=1",
		"-f", "hls", "-hls_time", "1", "-hls_list_size", "0", "-hls_flags", "independent_segments",
		"-hls_segment_filename", segmentPattern, manifestPath,
	)
	if output, runErr := generate.CombinedOutput(); runErr != nil {
		t.Skipf("local FFmpeg could not generate an HEVC MPEG-TS HLS fixture: %v (%s)", runErr, output)
	}
	names, durations := readMPEGTSFixture(t, manifestPath, fixtureDir)
	if len(names) < 2 || len(durations) < 2 {
		t.Fatalf("HEVC fixture has %d segments; want at least two", len(names))
	}

	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "4567890abcdef1234567890abcdef123"
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	wantPayloads := make([][]byte, len(names))
	for i, name := range names {
		payload, readErr := os.ReadFile(filepath.Join(fixtureDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		wantPayloads[i] = payload
		path := fmt.Sprintf("tracks/main/%06d.ts", i+1)
		saved, saveErr := store.SavePayload(id, path, bytes.NewReader(payload), 8<<20)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		track.Segments = append(track.Segments, domain.Segment{ID: fmt.Sprintf("hevc-%d", i+1), TrackID: "main", Sequence: uint64(i + 1), ArchiveOrdinal: uint64(i + 1), SourceEpoch: 1, Duration: durations[i], StoragePath: path, PayloadSize: saved.Size, SHA256: saved.SHA256})
	}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}

	captureDir := t.TempDir()
	t.Setenv("IR_PREVIEW_HEVC_CAPTURE_DIR", captureDir)
	t.Setenv("IR_PREVIEW_HEVC_REAL_FFMPEG", ffmpegPath)
	wrapperPath := filepath.Join(t.TempDir(), "ffmpeg-hevc-capture-wrapper")
	wrapper := "#!/bin/sh\nprintf x > \"$IR_PREVIEW_HEVC_CAPTURE_DIR/call-$$\"\nprintf '%s\\n' \"$@\" > \"$IR_PREVIEW_HEVC_CAPTURE_DIR/args-$$.txt\"\ncp input.m3u8 \"$IR_PREVIEW_HEVC_CAPTURE_DIR/playlist-$$.m3u8\"\nfor staged in segment-* init-*; do if [ -f \"$staged\" ]; then cp \"$staged\" \"$IR_PREVIEW_HEVC_CAPTURE_DIR/staged-$$-$staged\"; fi; done\nexec \"$IR_PREVIEW_HEVC_REAL_FFMPEG\" \"$@\"\n"
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	service, err := Open(root, store, func() []*domain.Recording { return []*domain.Recording{recording} }, wrapperPath)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	frame, code, permanent := service.extract(context.Background(), recording, 2)
	if code != "" || permanent || frame.ArchiveOrdinal != 2 {
		t.Fatalf("HEVC target-only extraction = frame ordinal %d, code=%q permanent=%t", frame.ArchiveOrdinal, code, permanent)
	}
	if _, _, _, valid := inspectJPEGBytes(t, frame.imageData); !valid {
		t.Fatal("HEVC fast path did not produce a decodable JPEG")
	}
	args := capturedFFmpegArgs(t, captureDir)
	assertKeyframeDecodeArgs(t, args)
	assertNoSeek(t, args)
	entries := mustReadDir(t, captureDir)
	var invocationCount, playlistCount, stagedMediaCount int
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, "call-"):
			invocationCount++
		case strings.HasPrefix(name, "playlist-"):
			playlistCount++
			data, readErr := os.ReadFile(filepath.Join(captureDir, name))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Count(string(data), "#EXTINF:") != 1 {
				t.Fatalf("HEVC fast path playlist contains context segments: %s", data)
			}
		case strings.Contains(name, "-segment-") && strings.HasSuffix(name, ".ts"):
			stagedMediaCount++
			data, readErr := os.ReadFile(filepath.Join(captureDir, name))
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !bytes.Equal(data, wantPayloads[1]) {
				t.Fatal("HEVC fast path staged a media segment other than the target")
			}
		}
	}
	if invocationCount != 1 || playlistCount != 1 || stagedMediaCount != 1 {
		t.Fatalf("HEVC target-only fast path counts: ffmpeg=%d playlists=%d staged media=%d", invocationCount, playlistCount, stagedMediaCount)
	}
	for i, segment := range track.Segments {
		payloadFile, payloadErr := store.OpenPayload(id, segment.StoragePath)
		if payloadErr != nil {
			t.Fatal(payloadErr)
		}
		gotPayload, readErr := io.ReadAll(payloadFile)
		_ = payloadFile.Close()
		if readErr != nil || !bytes.Equal(gotPayload, wantPayloads[i]) {
			t.Fatalf("canonical HEVC TS segment %d changed during preview extraction: read err=%v", i+1, readErr)
		}
	}
}

// BenchmarkPreviewExtractionTargetOnlyVsMidpointContext compares the new
// target-only keyframe-filtered path with a clearly labeled previous midpoint
// plus three-segment-context baseline using the same local MPEG-TS fixture.
// Run with -benchtime=1x for non-gating wall-time/invocation/JPEG measurements.
func BenchmarkPreviewExtractionTargetOnlyVsMidpointContext(b *testing.B) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		b.Skip("ffmpeg is not installed")
	}
	fixtureDir := b.TempDir()
	manifestPath := filepath.Join(fixtureDir, "source.m3u8")
	segmentPattern := filepath.Join(fixtureDir, "segment-%03d.ts")
	generate := exec.Command(ffmpegPath, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-t", "8", "-an", "-c:v", "mpeg2video", "-g", "25", "-q:v", "4", "-f", "hls", "-hls_time", "1", "-hls_list_size", "0", "-hls_flags", "independent_segments", "-hls_segment_filename", segmentPattern, manifestPath)
	if output, runErr := generate.CombinedOutput(); runErr != nil {
		b.Skipf("local ffmpeg cannot generate MPEG-TS benchmark fixture: %v (%s)", runErr, output)
	}
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		b.Fatal(err)
	}
	var durations []float64
	var files []string
	var pendingDuration float64
	for _, line := range strings.Split(string(manifest), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#EXTINF:") {
			value := strings.TrimSuffix(strings.TrimPrefix(line, "#EXTINF:"), ",")
			pendingDuration, err = strconv.ParseFloat(value, 64)
			if err != nil {
				b.Fatal(err)
			}
			continue
		}
		if line != "" && !strings.HasPrefix(line, "#") && pendingDuration > 0 {
			durations = append(durations, pendingDuration)
			files = append(files, line)
			pendingDuration = 0
		}
	}
	if len(files) < 4 {
		b.Skipf("generated fixture has only %d segments", len(files))
	}
	target := len(files) - 1
	contextStart := max(0, target-maxPrevious)
	writeList := func(path string, first, last int) error {
		var out strings.Builder
		fmt.Fprintf(&out, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n")
		for i := first; i <= last; i++ {
			fmt.Fprintf(&out, "#EXTINF:%.6f,\n%s\n", durations[i], files[i])
		}
		out.WriteString("#EXT-X-ENDLIST\n")
		return os.WriteFile(path, []byte(out.String()), 0600)
	}
	targetPlaylist := filepath.Join(fixtureDir, "target-only.m3u8")
	contextPlaylist := filepath.Join(fixtureDir, "midpoint-context.m3u8")
	if err := writeList(targetPlaylist, target, target); err != nil {
		b.Fatal(err)
	}
	if err := writeList(contextPlaylist, contextStart, target); err != nil {
		b.Fatal(err)
	}
	var targetOffset float64
	for i := contextStart; i < target; i++ {
		targetOffset += durations[i]
	}
	baselineOffset := targetOffset + durations[target]*0.5
	outputDir := b.TempDir()
	benchmarkCase := func(name, playlist string, seek float64, withXError, keyframeOnly bool) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				output := filepath.Join(outputDir, fmt.Sprintf("%s-%d.jpg", name, i))
				args := []string{"-nostdin", "-hide_banner", "-loglevel", "error"}
				if withXError {
					args = append(args, "-xerror")
				}
				args = append(args, "-protocol_whitelist", "file")
				if keyframeOnly {
					args = append(args, "-skip_frame", "nokey")
				}
				args = append(args, "-i", playlist)
				if seek > 0 {
					args = append(args, "-ss", fmt.Sprintf("%.6f", seek))
				}
				args = append(args, "-map", "0:v:0", "-frames:v", "1", "-vf", "scale=480:270:force_original_aspect_ratio=decrease:force_divisible_by=2,pad=480:270:(ow-iw)/2:(oh-ih)/2", "-an", "-sn", "-dn", "-c:v", "mjpeg", "-q:v", "4", "-f", "image2", output)
				command := exec.Command(ffmpegPath, args...)
				command.Dir = fixtureDir
				command.Stdout, command.Stderr = io.Discard, io.Discard
				if err := command.Run(); err != nil {
					b.Fatal(err)
				}
				if i == 0 {
					info, err := os.Stat(output)
					if err != nil || !validJPEGPath(output, info.Size()) {
						b.Fatalf("FFmpeg output is not a valid JPEG: %v", err)
					}
				}
			}
			b.ReportMetric(1, "ffmpeg/op")
			b.ReportMetric(1, "jpeg/op")
		})
	}
	benchmarkCase("comparison_midpoint_3_context_baseline", contextPlaylist, baselineOffset, true, false)
	benchmarkCase("earliest_random_access_target_only", targetPlaylist, 0, true, true)
}

func readFMP4Fixture(t *testing.T, manifestPath string) (string, []string, []float64) {
	t.Helper()
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var initName string
	var names []string
	var durations []float64
	var duration float64
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#EXT-X-MAP:") {
			value := strings.TrimPrefix(line, "#EXT-X-MAP:URI=\"")
			if end := strings.IndexByte(value, '"'); end >= 0 {
				initName = value[:end]
			}
		} else if strings.HasPrefix(line, "#EXTINF:") {
			value := strings.TrimSuffix(strings.TrimPrefix(line, "#EXTINF:"), ",")
			duration, err = strconv.ParseFloat(value, 64)
			if err != nil {
				t.Fatal(err)
			}
		} else if line != "" && !strings.HasPrefix(line, "#") {
			names = append(names, line)
			durations = append(durations, duration)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return initName, names, durations
}

func readMPEGTSFixture(t *testing.T, manifestPath, root string) ([]string, []float64) {
	t.Helper()
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var names []string
	var durations []float64
	var duration float64
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#EXTINF:") {
			value := strings.TrimSuffix(strings.TrimPrefix(line, "#EXTINF:"), ",")
			duration, err = strconv.ParseFloat(value, 64)
			if err != nil {
				t.Fatal(err)
			}
		} else if line != "" && !strings.HasPrefix(line, "#") {
			if _, err := os.Stat(filepath.Join(root, line)); err != nil {
				t.Fatal(err)
			}
			names = append(names, line)
			durations = append(durations, duration)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return names, durations
}

func countImageFiles(directory string) int {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".jpg") {
			count++
		}
	}
	return count
}

func TestUnsupportedPreviewFailureIsSuppressed(t *testing.T) {
	fixture := newPreviewFixture(t, 1, false, "unsupported")
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(serviceFailures(service, fixture.recording.ID)) == 1 })
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := fixture.invocations(); got != 1 {
		t.Fatalf("deterministic unsupported segment retried: %d invocations", got)
	}
	if failure := serviceFailures(service, fixture.recording.ID)[0]; !failure.Permanent || failure.Code != "no_video_stream" {
		t.Fatalf("unexpected unsupported result: %#v", failure)
	}
}

func TestReconcileDoesNotWaitForBlockedPreviewWorker(t *testing.T) {
	fixture := newPreviewFixture(t, 130, false, "block")
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(service.queue) < cap(service.queue) && time.Now().Before(deadline) {
		if err := service.Reconcile(); err != nil {
			t.Fatal(err)
		}
	}
	if len(service.queue) != cap(service.queue) {
		t.Fatalf("did not saturate bounded queue: %d/%d", len(service.queue), cap(service.queue))
	}
	started := time.Now()
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("reconciliation waited on extraction workers: %s", elapsed)
	}
	waitFor(t, func() bool { return fixture.invocations() > 0 })
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWaitForIdleDrainsQueuedAndActivePreviewTasksAndFlushesIndex(t *testing.T) {
	fixture := newPreviewFixture(t, 3, false, "block")
	fixture.recording.State = domain.StateRecording
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fixture.invocations() >= previewWorkers })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err := service.WaitForIdle(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForIdle error=%v, want deadline while preview work is blocked", err)
	}
	if err := os.WriteFile(filepath.Join(fixture.captureDir, "release"), []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.WaitForIdle(ctx); err != nil {
		t.Fatalf("WaitForIdle after releasing workers: %v", err)
	}
	if got := service.Summary(fixture.recording).FrameCount; got != 3 {
		t.Fatalf("preview frame count=%d, want all three accepted tasks", got)
	}
	service.mu.Lock()
	dirty := service.dirtyIndexes[fixture.recording.ID]
	service.mu.Unlock()
	if dirty != 0 {
		t.Fatalf("WaitForIdle left %d dirty index updates", dirty)
	}
	if _, err := os.Stat(filepath.Join(fixture.root, "previews", fixture.recording.ID, "index.json")); err != nil {
		t.Fatalf("WaitForIdle did not flush the live preview index: %v", err)
	}
}

func TestQueuedTaskSnapshotBoundsRecordingMetadata(t *testing.T) {
	segments := make([]domain.Segment, 10_000)
	for i := range segments {
		segments[i] = domain.Segment{ID: fmt.Sprintf("segment-%d", i+1), TrackID: "main", ArchiveOrdinal: uint64(i + 1), SourceEpoch: 1, Duration: 2, PayloadSize: 1024, SHA256: strings.Repeat("a", 64), StoragePath: fmt.Sprintf("tracks/main/%06d.ts", i+1)}
	}
	recording := &domain.Recording{ID: "abcdef0123456789abcdef0123456789", State: domain.StateRecording, SourceURL: "must not be retained", SourceURIClassification: "private", Gaps: []domain.Gap{{TrackID: "main"}}, Snapshots: []domain.ManifestSnapshot{{TrackID: "main"}}, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: segments, InitSegments: []domain.Segment{{ID: "unused", IsInit: true, StoragePath: "tracks/main/unused.mp4", PayloadSize: 10, SHA256: strings.Repeat("b", 64)}}}}}
	snapshot := boundedRecordingSnapshot(recording, segments, len(segments)-1, float64(len(segments)-1)*2)
	if snapshot == nil {
		t.Fatal("bounded recording snapshot was not created")
	}
	track := primaryTrack(snapshot)
	if track == nil || len(track.Segments) > maxPrevious+1 || len(track.InitSegments) > maxPrevious+1 {
		t.Fatalf("task snapshot retained unbounded segment context: track=%+v", track)
	}
	if len(snapshot.Tracks) != 1 || len(snapshot.Gaps) != 0 || len(snapshot.Snapshots) != 0 || snapshot.SourceURL != "" || snapshot.SourceURIClassification != "" {
		t.Fatalf("task snapshot retained unrelated recording data: %#v", snapshot)
	}
}

func TestLiveReconciliationEventuallyQueuesHistoricalBackfill(t *testing.T) {
	fixture := newPreviewFixture(t, 100, false, "block")
	fixture.recording.State = domain.StateRecording
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fixture.invocations() > 0 })
	for i := 0; i < 3; i++ {
		if err := service.Reconcile(); err != nil {
			t.Fatal(err)
		}
		service.mu.Lock()
		_, queued := service.queued[taskKey{recordingID: fixture.recording.ID, ordinal: 65}]
		_, active := service.active[taskKey{recordingID: fixture.recording.ID, ordinal: 65}]
		service.mu.Unlock()
		if queued || active {
			return
		}
	}
	t.Fatal("historical ordinal 65 starved while new live-tail segments were prioritized")
}

func TestOpenFrameRejectsTombstonedRecording(t *testing.T) {
	fixture := newPreviewFixture(t, 1, false, "normal")
	service := fixture.open(t)
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	reconcileUntilFrameCount(t, service, fixture.recording, 1)
	if err := service.DeleteRecording(fixture.recording.ID); err != nil {
		t.Fatal(err)
	}
	if file, _, err := service.OpenFrame(fixture.recording.ID, 1); err == nil {
		_ = file.Close()
		t.Fatal("deleted recording still served an indexed preview frame")
	}
	_ = service.Close(context.Background())
}

func TestStartupOrphanProjectionCleanupIsBounded(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	validID := strings.Repeat("f", 32)
	now := time.Now().UTC()
	valid := &domain.Recording{FormatVersion: 1, ID: validID, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err := store.CreateRecording(valid); err != nil {
		t.Fatal(err)
	}
	previewRoot := filepath.Join(root, "previews")
	if err := os.MkdirAll(previewRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(previewRoot, validID), 0700); err != nil {
		t.Fatal(err)
	}
	orphanCount := maxOrphanCleanupPerStart + 17
	for i := 1; i <= orphanCount; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := os.Mkdir(filepath.Join(previewRoot, id), 0700); err != nil {
			t.Fatal(err)
		}
	}
	service, err := Open(root, store, func() []*domain.Recording { return []*domain.Recording{valid} }, filepath.Join(root, "missing-ffmpeg"))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	entries, err := os.ReadDir(previewRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < orphanCount-maxOrphanCleanupPerStart+1 || len(entries) >= orphanCount+1 {
		t.Fatalf("startup orphan cleanup was not limited to one bounded batch: remaining=%d initial=%d budget=%d", len(entries), orphanCount+1, maxOrphanCleanupPerStart)
	}
	if _, err := os.Lstat(filepath.Join(previewRoot, validID)); err != nil {
		t.Fatalf("valid recording projection was removed: %v", err)
	}
}

func TestReconciliationUsesBoundedIncrementalSegmentScan(t *testing.T) {
	fixture := newPreviewFixture(t, 1000, false, "normal")
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	firstCursor := service.scanCursor[fixture.recording.ID]
	queued := len(service.queued)
	service.mu.Unlock()
	if firstCursor != maxSegmentsPerRecordingPass || queued > maxQueuedPerRecordingPass {
		t.Fatalf("first reconciliation scanned cursor=%d queued=%d, budget=%d/%d", firstCursor, queued, maxSegmentsPerRecordingPass, maxQueuedPerRecordingPass)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	secondCursor := service.scanCursor[fixture.recording.ID]
	service.mu.Unlock()
	if secondCursor != 2*maxSegmentsPerRecordingPass {
		t.Fatalf("second reconciliation cursor=%d want %d", secondCursor, 2*maxSegmentsPerRecordingPass)
	}
}

func TestDeleteRecordingJoinsWorkerAndPreventsProjectionRecreation(t *testing.T) {
	fixture := newPreviewFixture(t, 8, false, "block")
	service := fixture.open(t)
	defer service.Close(context.Background())
	if _, err := service.SetMode(fixture.recording.ID, ModeSegment); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return fixture.invocations() > 0 })
	if err := service.DeleteRecording(fixture.recording.ID); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(fixture.root, "management", "previews", fixture.recording.ID+".json")); !os.IsNotExist(err) {
		t.Fatalf("preview policy survived deletion: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(fixture.root, "previews", fixture.recording.ID)); !os.IsNotExist(err) {
		t.Fatalf("preview projection survived deletion: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := os.Lstat(filepath.Join(fixture.root, "previews", fixture.recording.ID)); !os.IsNotExist(err) {
		t.Fatalf("stale queued task recreated a preview directory: %v", err)
	}
}

type previewFixture struct {
	root       string
	store      *storage.Store
	recording  *domain.Recording
	ffmpeg     string
	captureDir string
	fakeMode   string
	list       func() []*domain.Recording
}

func newPreviewFixture(t *testing.T, segmentCount int, withInit bool, mode string) *previewFixture {
	t.Helper()
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	now := time.Now().UTC().Truncate(time.Second)
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateCompleted, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	track := recording.Tracks["main"]
	if withInit {
		payload := []byte("init payload")
		result, saveErr := store.SavePayload(id, "tracks/main/init.mp4", bytes.NewReader(payload), 1<<20)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		track.InitSegments = append(track.InitSegments, domain.Segment{ID: "init-1", TrackID: "main", StoragePath: "tracks/main/init.mp4", PayloadSize: result.Size, SHA256: result.SHA256, IsInit: true})
	}
	for i := 0; i < segmentCount; i++ {
		payload := []byte(fmt.Sprintf("canonical segment payload %d", i+1))
		storagePath := fmt.Sprintf("tracks/main/%06d.ts", i+1)
		result, saveErr := store.SavePayload(id, storagePath, bytes.NewReader(payload), 1<<20)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		segment := domain.Segment{ID: fmt.Sprintf("segment-%d", i+1), TrackID: "main", Sequence: uint64(i + 100), ArchiveOrdinal: uint64(i + 1), SourceEpoch: 1, Duration: 2, StoragePath: storagePath, PayloadSize: result.Size, SHA256: result.SHA256}
		if withInit {
			segment.InitSegmentID = "init-1"
		}
		track.Segments = append(track.Segments, segment)
	}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	imagePath := filepath.Join(root, "fake.jpg")
	imageData := validJPEGFixture(t)
	if err := os.WriteFile(imagePath, imageData, 0600); err != nil {
		t.Fatal(err)
	}
	captureDir := filepath.Join(root, "captures")
	return &previewFixture{
		root: root, store: store, recording: recording,
		ffmpeg: executable, captureDir: captureDir, fakeMode: mode,
		list: func() []*domain.Recording { return []*domain.Recording{recording} },
	}
}

func (f *previewFixture) open(t *testing.T) *Service {
	t.Helper()
	t.Setenv("IR_PREVIEW_CAPTURE_DIR", f.captureDir)
	t.Setenv("IR_PREVIEW_JPEG_PATH", filepath.Join(f.root, "fake.jpg"))
	t.Setenv("IR_PREVIEW_FAKE_MODE", f.fakeMode)
	shim := filepath.Join(f.root, "ffmpeg-test-shim")
	script := `#!/bin/sh
mkdir -p "$IR_PREVIEW_CAPTURE_DIR"
printf x > "$IR_PREVIEW_CAPTURE_DIR/call-$$"
printf '%s\n' "$@" > "$IR_PREVIEW_CAPTURE_DIR/args-$$.txt"
cp input.m3u8 "$IR_PREVIEW_CAPTURE_DIR/playlist-$$.m3u8" 2>/dev/null || true
if [ "$IR_PREVIEW_FAKE_MODE" = "block" ]; then
  while [ ! -f "$IR_PREVIEW_CAPTURE_DIR/release" ]; do sleep 0.01; done
fi
for staged in segment-* init-*; do
  if [ -f "$staged" ]; then cp "$staged" "$IR_PREVIEW_CAPTURE_DIR/staged-$$-$staged"; fi
done
if [ "$IR_PREVIEW_FAKE_MODE" = "fallback" ]; then
  count=$(grep -c '^#EXTINF:' input.m3u8)
  if [ "$count" -eq 1 ]; then
    echo "Invalid data found when processing input" >&2
    exit 1
  fi
fi
if [ "$IR_PREVIEW_FAKE_MODE" = "unsupported" ]; then
  echo "Stream map '0:v:0' matches no streams" >&2
  exit 1
fi
cp "$IR_PREVIEW_JPEG_PATH" frame.jpg
`
	if err := os.WriteFile(shim, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	returnService, err := Open(f.root, f.store, f.list, shim)
	if err != nil {
		t.Fatal(err)
	}
	return returnService
}

func (f *previewFixture) invocations() int {
	entries, err := os.ReadDir(f.captureDir)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "call-") {
			count++
		}
	}
	return count
}

func validJPEGFixture(t *testing.T) []byte {
	t.Helper()
	imageValue := image.NewRGBA(image.Rect(0, 0, 32, 18))
	for y := 0; y < 18; y++ {
		for x := 0; x < 32; x++ {
			imageValue.Set(x, y, color.RGBA{R: uint8(x * 7), G: uint8(y * 13), B: uint8(x + y), A: 255})
		}
	}
	var data bytes.Buffer
	if err := jpeg.Encode(&data, imageValue, &jpeg.Options{Quality: 75}); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func inspectJPEGBytes(t *testing.T, data []byte) (int, int, string, bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.jpg")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return inspectJPEG(path, int64(len(data)))
}

func reconcileUntilFrameCount(t *testing.T, service *Service, recording *domain.Recording, count int) {
	t.Helper()
	waitFor(t, func() bool {
		_ = service.Reconcile()
		return service.Summary(recording).FrameCount >= count
	})
}

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before deadline")
}

func serviceFailures(service *Service, id string) []Failure {
	service.mu.Lock()
	defer service.mu.Unlock()
	return append([]Failure(nil), service.indexLocked(id).Failures...)
}
