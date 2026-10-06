package preview

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func primaryTrack(recording *domain.Recording) *domain.Track {
	if recording == nil || len(recording.Tracks) == 0 {
		return nil
	}
	if track := recording.Tracks["main"]; track != nil && track.ID != "" {
		return track
	}
	if len(recording.Tracks) == 1 {
		for _, track := range recording.Tracks {
			if track != nil && track.ID != "" {
				return track
			}
		}
	}
	return nil
}

func orderedSegments(input []domain.Segment) []domain.Segment {
	segments := append([]domain.Segment(nil), input...)
	sort.SliceStable(segments, func(i, j int) bool {
		a, b := segments[i], segments[j]
		if a.TimelineOrdinal != 0 || b.TimelineOrdinal != 0 {
			aPosition := playbackPosition(a)
			bPosition := playbackPosition(b)
			if aPosition != bPosition {
				return aPosition < bPosition
			}
		}
		// ArchiveOrdinal is stable append/storage identity, and remains the
		// ordering fallback for legacy archives without a timeline projection.
		if a.ArchiveOrdinal != 0 || b.ArchiveOrdinal != 0 {
			if a.ArchiveOrdinal == 0 {
				return false
			}
			if b.ArchiveOrdinal == 0 {
				return true
			}
			if a.ArchiveOrdinal != b.ArchiveOrdinal {
				return a.ArchiveOrdinal < b.ArchiveOrdinal
			}
		}
		if a.SourceEpoch != b.SourceEpoch {
			return a.SourceEpoch < b.SourceEpoch
		}
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}
		return a.ID < b.ID
	})
	return segments
}

func segmentStarts(segments []domain.Segment) map[uint64]float64 {
	segments = orderedSegments(segments)
	starts := make(map[uint64]float64, len(segments))
	var elapsed float64
	for _, segment := range segments {
		if segment.ArchiveOrdinal != 0 {
			starts[segment.ArchiveOrdinal] = elapsed
		}
		if validDuration(segment.Duration) && elapsed <= math.MaxFloat64-segment.Duration {
			elapsed += segment.Duration
		}
	}
	return starts
}

func playbackDuration(recording *domain.Recording) float64 {
	track := primaryTrack(recording)
	if track == nil {
		return 0
	}
	var duration float64
	for _, segment := range orderedSegments(track.Segments) {
		if validDuration(segment.Duration) && duration <= math.MaxFloat64-segment.Duration {
			duration += segment.Duration
		}
	}
	return duration
}

type decodeAttempt struct {
	segments     []domain.Segment
	targetOffset float64
}

func buildContext(recording *domain.Recording, track *domain.Track, segments []domain.Segment, ordinal uint64) ([]domain.Segment, float64, float64, error) {
	if recording == nil || track == nil || len(segments) == 0 || ordinal == 0 {
		return nil, 0, 0, ErrInvalid
	}
	start, ok := segmentStarts(segments)[ordinal]
	if !ok {
		return nil, 0, 0, ErrNotFound
	}
	return buildContextAtStart(recording, track, segments, ordinal, start)
}

// buildContextAtStart accepts a stable ArchiveOrdinal identity even when the
// current playback projection places that segment elsewhere in the timeline.
func buildContextAtStart(recording *domain.Recording, track *domain.Track, segments []domain.Segment, ordinal uint64, start float64) ([]domain.Segment, float64, float64, error) {
	segments = orderedSegments(segments)
	attempts, err := buildContextAttemptsAtStart(recording, track, segments, ordinal, start)
	if err != nil {
		return nil, 0, 0, err
	}
	last := attempts[len(attempts)-1]
	return last.segments, start, last.targetOffset, nil
}

// buildContextAttemptsAtStart returns deterministic decoder inputs in
// increasing context order: target-only first, then one to three compatible
// predecessors. The caller may stage them one at a time, so healthy segments
// avoid copying or decoding their history.
func buildContextAttemptsAtStart(recording *domain.Recording, track *domain.Track, segments []domain.Segment, ordinal uint64, start float64) ([]decodeAttempt, error) {
	if recording == nil || track == nil || len(segments) == 0 || ordinal == 0 || math.IsNaN(start) || math.IsInf(start, 0) || start < 0 {
		return nil, ErrInvalid
	}
	targetIndex := segmentIndexByArchiveOrdinal(segments, ordinal)
	if targetIndex < 0 {
		return nil, ErrNotFound
	}
	target := segments[targetIndex]
	if !validDuration(target.Duration) || target.PayloadSize <= 0 || target.PayloadSize > maxContextBytes {
		return nil, ErrInvalid
	}
	if target.InitSegmentID != "" {
		init, ok := initByID(track, target.InitSegmentID)
		if !ok || init.PayloadSize <= 0 || init.PayloadSize > maxContextBytes-target.PayloadSize {
			return nil, ErrInvalid
		}
	}
	attempts := []decodeAttempt{{segments: []domain.Segment{target}}}
	window := []domain.Segment{target}
	bytes := target.PayloadSize
	if target.InitSegmentID != "" {
		init, ok := initByID(track, target.InitSegmentID)
		if !ok || init.PayloadSize <= 0 || init.PayloadSize > maxContextBytes-bytes {
			return nil, ErrInvalid
		}
		bytes += init.PayloadSize
	}
	windowDuration := target.Duration
	var targetOffset float64
	for i, included := targetIndex-1, 0; i >= 0 && included < maxPrevious; i, included = i-1, included+1 {
		candidate := segments[i]
		later := segments[i+1]
		if later.Discontinuity || candidate.SourceEpoch != target.SourceEpoch || candidate.DiscontinuitySequence != target.DiscontinuitySequence || candidate.InitSegmentID != target.InitSegmentID {
			break
		}
		if !validDuration(candidate.Duration) || candidate.PayloadSize <= 0 || candidate.PayloadSize > maxContextBytes-bytes || windowDuration > maxContextDuration-candidate.Duration {
			break
		}
		bytes += candidate.PayloadSize
		windowDuration += candidate.Duration
		targetOffset += candidate.Duration
		window = append([]domain.Segment{candidate}, window...)
		attempts = append(attempts, decodeAttempt{segments: append([]domain.Segment(nil), window...), targetOffset: targetOffset})
		// This segment starts a discontinuity region. It is valid context for
		// the target, but no earlier source epoch may be carried across it.
		if candidate.Discontinuity {
			break
		}
	}
	return attempts, nil
}

// segmentIndexByArchiveOrdinal keeps the fast path for legacy append-ordered
// slices, but falls back to an identity scan when a revisionable timeline has
// moved archive identities into a different playback order.
func segmentIndexByArchiveOrdinal(segments []domain.Segment, ordinal uint64) int {
	archiveOrdered := true
	for i := 1; i < len(segments); i++ {
		if segments[i-1].ArchiveOrdinal > segments[i].ArchiveOrdinal {
			archiveOrdered = false
			break
		}
	}
	if archiveOrdered {
		index := sort.Search(len(segments), func(i int) bool { return segments[i].ArchiveOrdinal >= ordinal })
		if index < len(segments) && segments[index].ArchiveOrdinal == ordinal {
			return index
		}
		return -1
	}
	for index := range segments {
		if segments[index].ArchiveOrdinal == ordinal {
			return index
		}
	}
	return -1
}

func playbackPosition(segment domain.Segment) uint64 {
	if segment.TimelineOrdinal != 0 {
		return segment.TimelineOrdinal
	}
	if segment.ArchiveOrdinal != 0 {
		return segment.ArchiveOrdinal
	}
	return segment.Sequence
}

func validDuration(duration float64) bool {
	return duration > 0 && !math.IsNaN(duration) && !math.IsInf(duration, 0)
}

func initByID(track *domain.Track, id string) (domain.Segment, bool) {
	for _, segment := range track.InitSegments {
		if segment.ID == id && segment.IsInit && segment.StoragePath != "" {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

func localPlaylist(track *domain.Track, segments []domain.Segment, local map[string]string) (string, error) {
	if track == nil || len(segments) == 0 {
		return "", ErrInvalid
	}
	maxDuration := 0.0
	for _, segment := range segments {
		if !validDuration(segment.Duration) || local[segment.StoragePath] == "" {
			return "", ErrInvalid
		}
		if segment.Duration > maxDuration {
			maxDuration = segment.Duration
		}
		if segment.InitSegmentID != "" {
			init, ok := initByID(track, segment.InitSegmentID)
			if !ok || local[init.StoragePath] == "" {
				return "", ErrInvalid
			}
		}
	}
	targetDuration := int(math.Ceil(maxDuration))
	if targetDuration < 1 {
		targetDuration = 1
	}
	var out strings.Builder
	fmt.Fprintf(&out, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:0\n", targetDuration)
	lastInit := "\x00"
	for _, segment := range segments {
		if segment.Discontinuity {
			out.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if segment.InitSegmentID != lastInit {
			if segment.InitSegmentID != "" {
				init, _ := initByID(track, segment.InitSegmentID)
				fmt.Fprintf(&out, "#EXT-X-MAP:URI=\"%s\"\n", local[init.StoragePath])
			}
			lastInit = segment.InitSegmentID
		}
		fmt.Fprintf(&out, "#EXTINF:%.6f,\n%s\n", segment.Duration, local[segment.StoragePath])
	}
	out.WriteString("#EXT-X-ENDLIST\n")
	return out.String(), nil
}

func copyVerified(ctx context.Context, store *storage.Store, id, relative, name string, expectedSize int64, expectedHash, directory string) error {
	if expectedSize <= 0 || expectedSize > maxContextBytes || len(expectedHash) != 64 {
		return ErrInvalid
	}
	info, err := store.StatPayload(id, relative)
	if err != nil {
		return err
	}
	if !info.Regular || info.Size != expectedSize {
		return ErrInvalid
	}
	f, err := store.OpenPayloadReader(id, relative)
	if err != nil {
		return err
	}
	defer f.Close()
	dst, err := os.OpenFile(filepath.Join(directory, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	hash := sha256.New()
	reader := &contextReader{ctx: ctx, reader: io.LimitReader(f, maxContextBytes+1)}
	count, copyErr := io.Copy(io.MultiWriter(dst, hash), reader)
	if copyErr == nil {
		copyErr = dst.Sync()
	}
	closeErr := dst.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if count != expectedSize || hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(expectedHash) {
		return ErrInvalid
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func safeSegmentExtension(path string, hasInit bool) string {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".ts", ".m4s", ".mp4":
		return ext
	default:
		if hasInit {
			return ".m4s"
		}
		return ".ts"
	}
}

func makeFrame(trackID string, segment domain.Segment, start float64, width, height int, size int64, generated time.Time, imageHash string) Frame {
	return Frame{ArchiveOrdinal: segment.ArchiveOrdinal, TrackID: trackID, SourceEpoch: segment.SourceEpoch, Sequence: segment.Sequence, SegmentStartSeconds: start, SegmentDurationSeconds: segment.Duration, FrameTimeSeconds: start, SegmentSHA256: strings.ToLower(segment.SHA256), Width: width, Height: height, Size: size, GeneratedAt: generated.UTC(), ImageSHA256: imageHash}
}

func inspectJPEG(path string, size int64) (int, int, string, bool) {
	if size <= 0 || size > maxFrameBytes {
		return 0, 0, "", false
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, "", false
	}
	defer f.Close()
	config, format, err := image.DecodeConfig(io.LimitReader(f, size))
	if err != nil || format != "jpeg" || config.Width <= 0 || config.Height <= 0 || config.Width > 480 || config.Height > 270 {
		return 0, 0, "", false
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, "", false
	}
	if _, _, err = image.Decode(io.LimitReader(f, size)); err != nil {
		return 0, 0, "", false
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, "", false
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, io.LimitReader(f, size)); err != nil {
		return 0, 0, "", false
	}
	return config.Width, config.Height, hex.EncodeToString(hash.Sum(nil)), true
}

func validJPEGPath(path string, size int64) bool { _, _, _, ok := inspectJPEG(path, size); return ok }

func frameFileMatches(previewRoot, recordingID string, ordinal uint64, expected Frame) bool {
	directory := filepath.Join(previewRoot, recordingID)
	framesDir := filepath.Join(directory, "frames")
	if requirePrivateDirectory(previewRoot, directory, framesDir) != nil {
		return false
	}
	path := framePath(previewRoot, recordingID, ordinal)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != expected.Size || info.Size() <= 0 || info.Size() > maxFrameBytes {
		return false
	}
	width, height, digest, valid := inspectJPEG(path, info.Size())
	return valid && width == expected.Width && height == expected.Height && digest == expected.ImageSHA256
}

func validJPEG(file *os.File, size int64) bool {
	if file == nil || size <= 0 || size > maxFrameBytes {
		return false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	config, format, err := image.DecodeConfig(io.LimitReader(file, size))
	if err != nil || format != "jpeg" || config.Width <= 0 || config.Height <= 0 || config.Width > 480 || config.Height > 270 {
		return false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	_, _, err = image.Decode(io.LimitReader(file, size))
	return err == nil
}

func selectFrames(items []Frame, sampling Sampling, limit int, seconds, duration float64) []Frame {
	frames := append([]Frame(nil), items...)
	sort.Slice(frames, func(i, j int) bool { return frames[i].FrameTimeSeconds < frames[j].FrameTimeSeconds })
	if len(frames) == 0 {
		return []Frame{}
	}
	if limit > len(frames) {
		limit = len(frames)
	}
	switch sampling {
	case SamplingRecent:
		return append([]Frame(nil), frames[len(frames)-limit:]...)
	case SamplingNearest:
		return []Frame{nearest(frames, seconds)}
	case SamplingUniform:
		if len(frames) <= limit {
			return frames
		}
		if !validDuration(duration) {
			duration = frames[len(frames)-1].FrameTimeSeconds
		}
		result := make([]Frame, 0, limit)
		seen := make(map[uint64]bool, limit)
		for bucket := 0; bucket < limit; bucket++ {
			target := (float64(bucket) + 0.5) * duration / float64(limit)
			frame := nearest(frames, target)
			if seen[frame.ArchiveOrdinal] || len(result) > 0 && frame.FrameTimeSeconds <= result[len(result)-1].FrameTimeSeconds {
				continue
			}
			seen[frame.ArchiveOrdinal] = true
			result = append(result, frame)
		}
		return result
	default:
		return []Frame{}
	}
}

func nearest(items []Frame, seconds float64) Frame {
	index := sort.Search(len(items), func(i int) bool { return items[i].FrameTimeSeconds >= seconds })
	if index == 0 {
		return items[0]
	}
	if index >= len(items) {
		return items[len(items)-1]
	}
	if seconds-items[index-1].FrameTimeSeconds <= items[index].FrameTimeSeconds-seconds {
		return items[index-1]
	}
	return items[index]
}

func posterFrame(items []Frame, seconds float64) Frame {
	if len(items) == 0 {
		return Frame{}
	}
	return nearest(items, seconds)
}

func latestFrame(items []Frame) Frame {
	if len(items) == 0 {
		return Frame{}
	}
	latest := items[0]
	for _, frame := range items[1:] {
		if frame.FrameTimeSeconds > latest.FrameTimeSeconds || frame.FrameTimeSeconds == latest.FrameTimeSeconds && frame.ArchiveOrdinal > latest.ArchiveOrdinal {
			latest = frame
		}
	}
	return latest
}

func findFrame(items []Frame, ordinal uint64) (Frame, bool) {
	for _, item := range items {
		if item.ArchiveOrdinal == ordinal {
			return item, true
		}
	}
	return Frame{}, false
}

func removeFrame(items []Frame, ordinal uint64) []Frame {
	result := items[:0]
	for _, item := range items {
		if item.ArchiveOrdinal != ordinal {
			result = append(result, item)
		}
	}
	return result
}

func segmentByOrdinal(segments []domain.Segment, ordinal uint64) (domain.Segment, bool) {
	for _, segment := range segments {
		if segment.ArchiveOrdinal == ordinal {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

func upsertFrame(items []Frame, frame Frame) []Frame {
	for i, item := range items {
		if item.ArchiveOrdinal == frame.ArchiveOrdinal {
			items[i] = frame
			sort.Slice(items, func(a, b int) bool { return items[a].ArchiveOrdinal < items[b].ArchiveOrdinal })
			return items
		}
	}
	items = append(items, frame)
	sort.Slice(items, func(a, b int) bool { return items[a].ArchiveOrdinal < items[b].ArchiveOrdinal })
	return items
}

func upsertFailure(items []Failure, failure Failure) []Failure {
	for i, item := range items {
		if item.ArchiveOrdinal == failure.ArchiveOrdinal {
			items[i] = failure
			return items
		}
	}
	return append(items, failure)
}

func removeFailure(items []Failure, ordinal uint64) []Failure {
	result := items[:0]
	for _, item := range items {
		if item.ArchiveOrdinal != ordinal {
			result = append(result, item)
		}
	}
	return result
}

func framePath(root, id string, ordinal uint64) string {
	return filepath.Join(root, id, "frames", fmt.Sprintf("%012d.jpg", ordinal))
}

func cloneRecording(recording *domain.Recording) (*domain.Recording, error) {
	data, err := json.Marshal(recording)
	if err != nil {
		return nil, err
	}
	var copy domain.Recording
	if err = json.Unmarshal(data, &copy); err != nil {
		return nil, err
	}
	return &copy, nil
}

func atomicWrite(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(directory, ".tmp-*")
	if err != nil {
		return err
	}
	tempPath := tmp.Name()
	defer os.Remove(tempPath)
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tempPath, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func writePrivate(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func requirePrivateDirectory(dirs ...string) error {
	for _, path := range dirs {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrNotFound
		}
	}
	return nil
}

func ensurePrivateDir(path string) error {
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe preview directory parent")
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe preview directory")
	}
	return os.Chmod(path, 0700)
}

func removeRegularOrMissing(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	return os.Remove(path)
}

func removePrivateTree(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("unsafe preview path")
	}
	return os.RemoveAll(path)
}
