package derivative

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

// TestFakeFFmpegHelper is invoked only by the fake executable generated in
// tests. The production service itself always starts the configured binary
// directly and never invokes a shell.
func TestFakeFFmpegHelper(t *testing.T) {
	mode := os.Getenv("DERIVATIVE_FAKE_FFMPEG")
	if mode == "" {
		return
	}
	args := os.Args
	separator := -1
	for i, arg := range args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		os.Exit(41)
	}
	ffmpegArgs := args[separator+1:]
	if marker := os.Getenv("DERIVATIVE_FAKE_INVOKED"); marker != "" {
		_ = os.WriteFile(marker, []byte("invoked"), 0600)
	}
	if marker := os.Getenv("DERIVATIVE_FAKE_PLAYLIST"); marker != "" {
		playlist, err := os.ReadFile("input.m3u8")
		if err != nil || os.WriteFile(marker, playlist, 0600) != nil {
			os.Exit(46)
		}
	}
	validArgs := contains(ffmpegArgs, "-protocol_whitelist") && containsPair(ffmpegArgs, "-c", "copy") && containsPair(ffmpegArgs, "-f", "matroska")
	if !validArgs {
		os.Exit(42)
	}
	if mode == "wait" {
		marker := os.Getenv("DERIVATIVE_FAKE_STARTED")
		if marker != "" {
			_ = os.WriteFile(marker, []byte("started"), 0600)
		}
		for {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if mode == "wait-release" {
		marker := os.Getenv("DERIVATIVE_FAKE_STARTED")
		if marker != "" {
			_ = os.WriteFile(marker, []byte("started"), 0600)
		}
		release := os.Getenv("DERIVATIVE_FAKE_RELEASE")
		for release == "" {
			time.Sleep(10 * time.Millisecond)
			release = os.Getenv("DERIVATIVE_FAKE_RELEASE")
		}
		for {
			if _, err := os.Stat(release); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if mode == "fail" {
		os.Exit(43)
	}
	if len(ffmpegArgs) == 0 {
		os.Exit(44)
	}
	output := ffmpegArgs[len(ffmpegArgs)-1]
	contents := []byte("matroska-fixture")
	if err := os.WriteFile(output, contents, 0600); err != nil {
		os.Exit(45)
	}
	os.Exit(0)
}

func TestExportHappyPathIsProjectionAndDownloadable(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	ffmpeg := fakeFFmpeg(t)
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "success")
	service := openService(t, root, store, ffmpeg, 1)
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	job = waitTerminal(t, service, job.ID)
	if job.State != StateCompleted || job.Size != int64(len("matroska-fixture")) || job.OutputName != "recording-"+recording.ID+".mkv" {
		t.Fatalf("unexpected completed job: %+v", job)
	}
	if job.SourceRevisionKnown || ProjectJob(job, recording).Freshness != FreshnessUnknown {
		t.Fatalf("legacy revision-zero export must have unknown freshness: job=%+v projection=%+v", job, ProjectJob(job, recording))
	}
	f, downloaded, err := service.OpenDownload(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(f)
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || string(data) != "matroska-fixture" || downloaded.ID != job.ID {
		t.Fatalf("download failed: %q %v %v", data, readErr, closeErr)
	}

	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load archive: count=%d err=%v", len(loaded), err)
	}
	payload, err := store.OpenPayload(recording.ID, loaded[0].Tracks["main"].Segments[0].StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := io.ReadAll(payload)
	_ = payload.Close()
	if err != nil || string(canonical) != "original-segment-bytes" {
		t.Fatalf("canonical payload changed: %q err=%v", canonical, err)
	}
}

func TestExportFreshnessTracksCanonicalAndTimelineRevisions(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	recording.ArchiveRevision = 4
	recording.TimelineRevision = 10
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "success")
	service, err := Open(root, store, fakeFFmpeg(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	first, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	first = waitTerminal(t, service, first.ID)
	if first.State != StateCompleted || !first.SourceRevisionKnown || first.SourceArchiveRevision != 4 || first.SourceTimelineRevision != 10 {
		t.Fatalf("export did not bind its source revisions: %+v", first)
	}
	if got := ProjectJob(first, recording).Freshness; got != FreshnessCurrent {
		t.Fatalf("fresh export freshness=%q, want current", got)
	}

	// Historical repair changes the canonical media/timeline snapshot. The
	// completed artifact remains downloadable but is no longer current.
	repaired := *recording
	repaired.ArchiveRevision = 5
	repaired.TimelineRevision = 11
	if got := ProjectJob(first, &repaired).Freshness; got != FreshnessStale {
		t.Fatalf("old export freshness after repair=%q, want stale", got)
	}

	second, err := service.Start(context.Background(), &repaired)
	if err != nil {
		t.Fatal(err)
	}
	second = waitTerminal(t, service, second.ID)
	if second.State != StateCompleted || second.SourceArchiveRevision != 5 || second.SourceTimelineRevision != 11 {
		t.Fatalf("re-export did not bind repaired revisions: %+v", second)
	}
	if got := ProjectJob(second, &repaired).Freshness; got != FreshnessCurrent {
		t.Fatalf("re-export freshness=%q, want current", got)
	}

	// Metadata-only edits do not alter the media snapshot and therefore do not
	// stale a remux derived from that snapshot.
	metadataOnly := repaired
	metadataOnly.Title = "Updated title"
	if got := ProjectJob(second, &metadataOnly).Freshness; got != FreshnessCurrent {
		t.Fatalf("metadata-only change made media export stale: %q", got)
	}

	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(root, store, fakeFFmpeg(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	service = reloaded
	loaded, err := service.Get(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.SourceRevisionKnown || loaded.SourceArchiveRevision != 5 || loaded.SourceTimelineRevision != 11 {
		t.Fatalf("source revisions did not survive restart: %+v", loaded)
	}
	if got := ProjectJob(loaded, &metadataOnly).Freshness; got != FreshnessCurrent {
		t.Fatalf("reloaded export freshness=%q, want current", got)
	}
}

func TestV2ExportUsesShardedTimelineOrder(t *testing.T) {
	root, store, recording := makeShardedDerivativeRecording(t)
	playlistPath := filepath.Join(t.TempDir(), "playlist.txt")
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "success")
	t.Setenv("DERIVATIVE_FAKE_PLAYLIST", playlistPath)
	service := openService(t, root, store, fakeFFmpeg(t), 1)
	maxInput := int64(len("init-payload") + len("archive-first") + len("timeline-first"))
	if err := service.validateShardedSourceWithLimit(context.Background(), recording, maxInput); err != nil {
		t.Fatalf("validation rejected exact byte ceiling with repeated init reference: %v", err)
	}
	if err := service.validateShardedSourceWithLimit(context.Background(), recording, maxInput-1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("validation below exact byte ceiling err=%v, want ErrUnsupported", err)
	}
	staleHeader := *recording
	staleHeader.ArchiveRevision = 2
	staleHeader.TimelineRevision = 3
	job, err := service.Start(context.Background(), &staleHeader)
	if err != nil {
		t.Fatal(err)
	}
	job = waitTerminal(t, service, job.ID)
	if job.State != StateCompleted || !job.SourceRevisionKnown || job.SourceArchiveRevision != 4 || job.SourceTimelineRevision != 10 {
		t.Fatalf("v2 export did not complete from captured revisions: %+v", job)
	}
	playlist, err := os.ReadFile(playlistPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(playlist)
	firstTimelineItem := strings.Index(text, "#EXTINF:2,\nobject-000001.mp4")
	secondTimelineItem := strings.Index(text, "#EXTINF:3,\nobject-000002.m4s")
	if firstTimelineItem < 0 || secondTimelineItem < 0 || firstTimelineItem > secondTimelineItem {
		t.Fatalf("v2 export playlist did not follow timeline order:\n%s", text)
	}
	if strings.Count(text, "#EXT-X-MAP:") != 1 || strings.Contains(text, "source.invalid") {
		t.Fatalf("v2 playlist init mapping or source URI safety failed:\n%s", text)
	}
}

func TestDiskStringSetDeduplicatesLargeRepeatedInitInput(t *testing.T) {
	root := t.TempDir()
	set, err := newDiskStringSet(root)
	if err != nil {
		t.Fatal(err)
	}
	const count = 10_000
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("init-%05d", i)
		seen, err := set.SeenOrAdd(id)
		if err != nil {
			t.Fatalf("add %q: %v", id, err)
		}
		if seen {
			t.Fatalf("new init ID %q reported as already seen", id)
		}
	}
	for i := count - 1; i >= 0; i-- {
		id := fmt.Sprintf("init-%05d", i)
		seen, err := set.SeenOrAdd(id)
		if err != nil {
			t.Fatalf("repeat %q: %v", id, err)
		}
		if !seen {
			t.Fatalf("repeated init ID %q reported as new", id)
		}
	}
	if err := set.Close(); err != nil {
		t.Fatalf("remove temporary membership set: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("temporary membership data leaked: %d entries remain", len(entries))
	}
}

func TestOpenCleansInterruptedInitValidationSet(t *testing.T) {
	root, store, _ := fixtureRecording(t)
	scratch := filepath.Join(root, "exports", ".seen-init-interrupted")
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, "marker"), []byte("init-id"), 0600); err != nil {
		t.Fatal(err)
	}
	openService(t, root, store, fakeFFmpeg(t), 1)
	if _, err := os.Lstat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted validation scratch remains: %v", err)
	}
}

func TestRealFFmpegRemuxWhenAvailable(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is not installed")
	}
	fixture := exec.Command(ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=64x64:r=10:d=1", "-an", "-threads", "1", "-c:v", "mpeg2video", "-f", "mpegts", "pipe:1")
	input, err := fixture.Output()
	if err != nil || len(input) == 0 {
		t.Skip("ffmpeg lacks a minimal MPEG-TS fixture encoder")
	}
	root, store, recording := fixtureRecordingWithData(t, input)
	service := openService(t, root, store, ffmpeg, 1)
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	job = waitTerminal(t, service, job.ID)
	if job.State != StateCompleted {
		t.Fatalf("real remux failed: %+v", job)
	}
	file, _, err := service.OpenDownload(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != string([]byte{0x1a, 0x45, 0xdf, 0xa3}) {
		t.Fatalf("output is not Matroska: %x err=%v", magic, err)
	}
}

func TestMissingFFmpegIsExplicitlyUnavailable(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	emptyPath := t.TempDir()
	t.Setenv("PATH", emptyPath)
	service := openService(t, root, store, "", 1)
	if service.Available() {
		t.Fatal("unexpected available backend")
	}
	if _, err := service.Start(context.Background(), recording); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start err=%v", err)
	}
}

func TestOpenThumbnailReadsLegacyJPEGWithoutRunningFFmpeg(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	service := openService(t, root, store, fakeFFmpeg(t), 1)
	legacyPath := filepath.Join(root, "thumbnails", recording.ID+".jpg")
	want := jpegFixture(t)
	if err := os.WriteFile(legacyPath, want, 0600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "ffmpeg-invoked")
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "success")
	t.Setenv("DERIVATIVE_FAKE_INVOKED", marker)
	file, summary, err := service.OpenThumbnail(recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("legacy JPEG read mismatch: bytes=%d err=%v", len(got), err)
	}
	info, err := file.Stat()
	if err != nil || summary.RecordingID != recording.ID || summary.ContentType != "image/jpeg" || summary.Size != int64(len(want)) || summary.UpdatedAt.IsZero() || info.Mode().Perm() != 0600 {
		t.Fatalf("unexpected legacy thumbnail summary/stat: summary=%+v info=%v err=%v", summary, info, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy read unexpectedly invoked FFmpeg: %v", err)
	}
}

func TestOpenThumbnailRejectsMissingInvalidOversizedAndUnsafeFiles(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	service := openService(t, root, store, fakeFFmpeg(t), 1)
	if _, _, err := service.OpenThumbnail(recording.ID); !errors.Is(err, ErrThumbnailNotFound) {
		t.Fatalf("missing file err=%v", err)
	}
	if _, _, err := service.OpenThumbnail("../" + recording.ID); !errors.Is(err, ErrThumbnailNotFound) {
		t.Fatalf("malformed ID err=%v", err)
	}
	thumbnailPath := filepath.Join(root, "thumbnails", recording.ID+".jpg")
	if err := os.WriteFile(thumbnailPath, []byte{0xff, 0xd8, 0xff, 0xd9}, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.OpenThumbnail(recording.ID); !errors.Is(err, ErrThumbnailNotFound) {
		t.Fatalf("invalid JPEG err=%v", err)
	}
	if err := os.Truncate(thumbnailPath, maxThumbnailBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.OpenThumbnail(recording.ID); !errors.Is(err, ErrThumbnailNotFound) {
		t.Fatalf("oversized file err=%v", err)
	}
	if err := os.Remove(thumbnailPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.jpg")
	if err := os.WriteFile(outside, jpegFixture(t), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, thumbnailPath); err != nil {
		t.Logf("symlink unavailable: %v", err)
	} else {
		if _, _, err := service.OpenThumbnail(recording.ID); !errors.Is(err, ErrThumbnailNotFound) {
			t.Fatalf("symlink err=%v", err)
		}
		if err := os.Remove(thumbnailPath); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(thumbnailPath, 0700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.OpenThumbnail(recording.ID); !errors.Is(err, ErrThumbnailNotFound) {
		t.Fatalf("non-regular file err=%v", err)
	}
}

func jpegFixture(t *testing.T) []byte {
	t.Helper()
	imageData := image.NewRGBA(image.Rect(0, 0, 16, 9))
	for y := 0; y < 9; y++ {
		for x := 0; x < 16; x++ {
			imageData.Set(x, y, color.RGBA{R: uint8(x * 10), G: uint8(y * 20), B: 80, A: 255})
		}
	}
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, imageData, &jpeg.Options{Quality: 75}); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestDuplicateExportCoalescesAndCancelCleansPartial(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	ffmpeg := fakeFFmpeg(t)
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "wait")
	t.Setenv("DERIVATIVE_FAKE_STARTED", marker)
	service := openService(t, root, store, ffmpeg, 1)
	first, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Start(context.Background(), recording)
	if err != nil || second.ID != first.ID {
		t.Fatalf("duplicate did not coalesce: %+v %v", second, err)
	}
	if !service.InProgress(recording.ID) {
		t.Fatal("job was not marked active")
	}
	waitFile(t, marker)
	if _, err := service.Cancel(first.ID); err != nil {
		t.Fatal(err)
	}
	job := waitTerminal(t, service, first.ID)
	if job.State != StateCanceled {
		t.Fatalf("state=%s", job.State)
	}
	if _, err := os.Stat(filepath.Join(root, "exports", first.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial output remains: %v", err)
	}
	if service.InProgress(recording.ID) {
		t.Fatal("canceled export remains active")
	}
}

func TestWaitForIdleDrainsQueuedAndRunningExportsWithoutClosingService(t *testing.T) {
	root, store, firstRecording := fixtureRecording(t)
	secondRecording := addFixtureRecording(t, store, firstRecording, "fedcba9876543210fedcba9876543210")
	ffmpeg := fakeFFmpeg(t)
	started := filepath.Join(t.TempDir(), "started")
	release := filepath.Join(t.TempDir(), "release")
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "wait-release")
	t.Setenv("DERIVATIVE_FAKE_STARTED", started)
	t.Setenv("DERIVATIVE_FAKE_RELEASE", release)
	service := openService(t, root, store, ffmpeg, 1)
	first, err := service.Start(context.Background(), firstRecording)
	if err != nil {
		t.Fatal(err)
	}
	waitFile(t, started)
	second, err := service.Start(context.Background(), secondRecording)
	if err != nil {
		t.Fatal(err)
	}
	if second.State != StateQueued {
		t.Fatalf("second job state=%s, want queued behind blocked worker", second.State)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = service.WaitForIdle(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForIdle error=%v, want deadline while exports are blocked", err)
	}
	if job, getErr := service.Get(first.ID); getErr != nil || job.State != StateRunning {
		t.Fatalf("timeout closed or canceled the running export: job=%+v err=%v", job, getErr)
	}
	if err := os.WriteFile(release, []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.WaitForIdle(ctx); err != nil {
		t.Fatalf("WaitForIdle after releasing worker: %v", err)
	}
	for _, id := range []string{first.ID, second.ID} {
		job, err := service.Get(id)
		if err != nil || job.State != StateCompleted {
			t.Fatalf("job %s did not finish before idle: %+v err=%v", id, job, err)
		}
	}
	if _, err := service.Start(context.Background(), firstRecording); err != nil {
		t.Fatalf("WaitForIdle closed the service: %v", err)
	}
}

func TestFailedFFmpegDoesNotRetainPartialArtifact(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	ffmpeg := fakeFFmpeg(t)
	t.Setenv("DERIVATIVE_FAKE_FFMPEG", "fail")
	service := openService(t, root, store, ffmpeg, 1)
	job, err := service.Start(context.Background(), recording)
	if err != nil {
		t.Fatal(err)
	}
	job = waitTerminal(t, service, job.ID)
	if job.State != StateFailed || job.ErrorCode != "ffmpeg_failed" {
		t.Fatalf("job=%+v", job)
	}
	if _, err := os.Stat(filepath.Join(root, "exports", job.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failure staging remains: %v", err)
	}
}

func TestRestartMarksQueuedAndRunningJobsInterrupted(t *testing.T) {
	root, store, _ := fixtureRecording(t)
	id := strings.Repeat("a", 32)
	recordingID := strings.Repeat("b", 32)
	stateDir := filepath.Join(root, "management", "exports")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	state := persistedState{Version: 1, Jobs: []Job{
		{ID: id, RecordingID: recordingID, Kind: jobKindExport, State: StateQueued, CreatedAt: time.Now().UTC()},
		{ID: strings.Repeat("c", 32), RecordingID: recordingID, Kind: jobKindExport, State: StateRunning, CreatedAt: time.Now().UTC()},
	}}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(stateDir, "state.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	service := openService(t, root, store, fakeFFmpeg(t), 1)
	for _, job := range service.List("") {
		if job.State != StateFailed || job.ErrorCode != "interrupted_by_restart" {
			t.Fatalf("job=%+v", job)
		}
	}
	if err := service.Delete("../" + id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unsafe delete err=%v", err)
	}
}

func TestUnavailableStateReturnsStableErrorsAndRejectsActiveRecording(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	service := openService(t, root, store, fakeFFmpeg(t), 1)
	recording.State = domain.StateRecording
	if _, err := service.Start(context.Background(), recording); !errors.Is(err, ErrActive) {
		t.Fatalf("active err=%v", err)
	}
	if err := service.Delete(strings.Repeat("d", 32)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing delete err=%v", err)
	}
}

func TestBuildPlaylistUsesArchiveOrderAndInitMap(t *testing.T) {
	root, store, recording := fixtureRecording(t)
	init := domain.Segment{ID: "init-1", TrackID: "main", StoragePath: "tracks/main/init.m4s", SHA256: strings.Repeat("0", 64), PayloadSize: 1, IsInit: true}
	first := recording.Tracks["main"].Segments[0]
	first.ArchiveOrdinal, first.Sequence, first.InitSegmentID = 2, 11, "init-1"
	second := first
	second.ID, second.StoragePath, second.Sequence, second.ArchiveOrdinal = "seg-early", "tracks/main/earlier.m4s", 10, 1
	recording.Tracks["main"].Segments = []domain.Segment{first, second}
	recording.Tracks["main"].InitSegments = []domain.Segment{init}
	local := map[string]string{first.StoragePath: "object-000002.bin", second.StoragePath: "object-000003.bin", init.StoragePath: "object-000001.bin"}
	playlist, err := buildPlaylist(recording, local)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Index(playlist, "object-000003.bin") > strings.Index(playlist, "object-000002.bin") || !strings.Contains(playlist, `#EXT-X-MAP:URI="object-000001.bin"`) {
		t.Fatalf("playlist order/map incorrect:\n%s", playlist)
	}
	_ = root
	_ = store
}

func fixtureRecording(t *testing.T) (string, *storage.Store, *domain.Recording) {
	return fixtureRecordingWithData(t, []byte("original-segment-bytes"))
}

func fixtureRecordingWithData(t *testing.T, data []byte) (string, *storage.Store, *domain.Recording) {
	t.Helper()
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "0123456789abcdef0123456789abcdef"
	if err := store.NewRecordingDir(recordingID); err != nil {
		t.Fatal(err)
	}
	const relative = "tracks/main/00000001.ts"
	result, err := store.SavePayload(recordingID, relative, strings.NewReader(string(data)), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{ID: "segment-1", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1, StoragePath: relative, SourceURI: "https://source.example/private?token=must-not-escape", Duration: 2, PayloadSize: result.Size, SHA256: result.SHA256}
	if err := store.SaveSidecar(recordingID, relative, segment); err != nil {
		t.Fatal(err)
	}
	recording := &domain.Recording{FormatVersion: 1, ID: recordingID, Title: "Fixture", State: domain.StateStopped, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{segment}}}}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	return root, store, recording
}

func makeShardedDerivativeRecording(t *testing.T) (string, *storage.Store, *domain.Recording) {
	t.Helper()
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "1123456789abcdef0123456789abcdef"
	now := time.Now().UTC()
	sessionID := "session-" + strings.Repeat("b", 64)
	header := &domain.Recording{
		FormatVersion:    2,
		ID:               recordingID,
		SourceSessionID:  sessionID,
		Title:            "Sharded fixture",
		State:            domain.StateStopped,
		CreatedAt:        now,
		StartedAt:        now,
		ArchiveRevision:  4,
		TimelineRevision: 10,
		Tracks:           map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}},
		ShardedArchive:   &domain.ShardedArchiveSummary{},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	addPayload := func(path string, payload []byte) storage.PayloadResult {
		t.Helper()
		result, err := store.SavePayload(recordingID, path, bytes.NewReader(payload), storage.MaxObjectBytes)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	initPath := "tracks/main/init.mp4"
	initPayload := []byte("init-payload")
	initObject := addPayload(initPath, initPayload)
	initSegment := domain.Segment{ID: "init-one", TrackID: "main", Sequence: 1, SourceURI: "https://source.invalid/init.mp4", StoragePath: initPath, PayloadSize: initObject.Size, SHA256: initObject.SHA256, IsInit: true}
	initCoordinate := archiveindex.Coordinate{SessionID: sessionID, TrackID: "main", Sequence: 1, Kind: archiveindex.ObjectInit}
	if err := store.AppendShardedMedia(ctx, recordingID, storage.V2MediaRecord{Coordinate: initCoordinate, Segment: initSegment}); err != nil {
		t.Fatal(err)
	}
	for _, media := range []struct {
		sequence uint64
		ordinal  uint64
		uri      string
		path     string
		duration float64
		payload  []byte
	}{
		{sequence: 2, ordinal: 1, uri: "https://source.invalid/second.ts?sig=secret", path: "tracks/main/segment-1.ts", duration: 3, payload: []byte("archive-first")},
		{sequence: 1, ordinal: 2, uri: "https://source.invalid/first.mp4?sig=secret", path: "tracks/main/segment-2.mp4", duration: 2, payload: []byte("timeline-first")},
	} {
		object := addPayload(media.path, media.payload)
		segment := domain.Segment{
			ID: fmt.Sprintf("seg-%020d", media.ordinal), TrackID: "main", Sequence: media.sequence,
			ArchiveOrdinal: media.ordinal, SourceURI: media.uri, Duration: media.duration,
			InitSegmentID: initSegment.ID, StoragePath: media.path, PayloadSize: object.Size, SHA256: object.SHA256,
		}
		coordinate := archiveindex.Coordinate{SessionID: sessionID, TrackID: "main", Sequence: media.sequence, Kind: archiveindex.ObjectMedia}
		if err := store.AppendShardedMedia(ctx, recordingID, storage.V2MediaRecord{Coordinate: coordinate, Segment: segment}); err != nil {
			t.Fatal(err)
		}
	}
	recording, err := store.LoadRecordingHeader(ctx, recordingID)
	if err != nil {
		t.Fatal(err)
	}
	return root, store, recording
}

func addFixtureRecording(t *testing.T, store *storage.Store, template *domain.Recording, id string) *domain.Recording {
	t.Helper()
	copy, err := cloneRecording(template)
	if err != nil {
		t.Fatal(err)
	}
	copy.ID = id
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	segment := &copy.Tracks["main"].Segments[0]
	source, err := store.OpenPayload(template.ID, segment.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(source)
	closeErr := source.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read fixture source: %v %v", err, closeErr)
	}
	result, err := store.SavePayload(id, segment.StoragePath, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	segment.SHA256, segment.PayloadSize = result.SHA256, result.Size
	if err := store.SaveSidecar(id, segment.StoragePath, *segment); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveRecording(copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func fakeFFmpeg(t *testing.T) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
	path := filepath.Join(t.TempDir(), "ffmpeg-fake")
	script := "#!/bin/sh\nexec " + quote(binary) + " -test.run=^TestFakeFFmpegHelper$ -- \"$@\"\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func openService(t *testing.T, root string, store *storage.Store, ffmpeg string, workers int) *Service {
	t.Helper()
	service, err := Open(root, store, ffmpeg, workers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return service
}

func waitTerminal(t *testing.T, service *Service, id string) Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := service.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if !active(job.State) {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, _ := service.Get(id)
	t.Fatalf("job did not complete: %+v", job)
	return Job{}
}

func waitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("helper process did not start")
}

func contains(args []string, value string) bool {
	for _, arg := range args {
		if arg == value {
			return true
		}
	}
	return false
}

func containsPair(args []string, first, second string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == first && args[i+1] == second {
			return true
		}
	}
	return false
}

func TestErrorCodesDoNotExposeCommandOrSourceURI(t *testing.T) {
	job := Job{ErrorCode: "ffmpeg_failed", OutputName: "recording-0123456789abcdef0123456789abcdef.mkv"}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"/path/to", "token", "source.example", "private"} {
		if strings.Contains(strings.ToLower(string(encoded)), forbidden) {
			t.Fatalf("unsafe job output contains %q: %s", forbidden, encoded)
		}
	}
	if !strings.HasPrefix(fmt.Sprint(job.ErrorCode), "ffmpeg_") {
		t.Fatal("unexpected error code")
	}
}
