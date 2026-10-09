package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

var (
	errShardedTimelineIdentityUnavailable = errors.New("recording playback timeline is unavailable")
	errShardedTimelineMissingInit         = errors.New("recording references a missing init segment")
)

type shardedGapMessage struct {
	gap  *domain.Gap
	err  error
	done bool
}

type shardedGapStream struct {
	items chan shardedGapMessage
	track string
	gap   *domain.Gap
	done  bool
	err   error
}

func newShardedGapStream(ctx context.Context, store *storage.Store, recordingID string) *shardedGapStream {
	stream := &shardedGapStream{items: make(chan shardedGapMessage, 1)}
	go func() {
		err := store.IterateShardedGaps(ctx, recordingID, func(gap domain.Gap) error {
			select {
			case stream.items <- shardedGapMessage{gap: &gap}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		select {
		case stream.items <- shardedGapMessage{err: err, done: true}:
		case <-ctx.Done():
		}
	}()
	return stream
}

func (stream *shardedGapStream) next() error {
	if stream.gap != nil || stream.done {
		return stream.err
	}
	for {
		message, ok := <-stream.items
		if !ok {
			stream.done = true
			return nil
		}
		if message.done {
			stream.done = true
			stream.err = message.err
			return stream.err
		}
		if message.gap.TrackID == stream.track {
			stream.gap = message.gap
			return nil
		}
	}
}

func (stream *shardedGapStream) hasGapBetween(trackID string, previous, next domain.Segment) (bool, error) {
	if next.Sequence <= previous.Sequence || previous.Sequence == ^uint64(0) || previous.SourceEpoch != next.SourceEpoch || previous.DiscontinuitySequence != next.DiscontinuitySequence {
		return false, nil
	}
	stream.track = trackID
	for {
		if err := stream.next(); err != nil {
			return false, err
		}
		gap := stream.gap
		if gap == nil {
			return false, nil
		}
		if gap.SourceEpoch < previous.SourceEpoch || gap.SourceEpoch == previous.SourceEpoch && gap.DiscontinuitySequence < previous.DiscontinuitySequence {
			stream.gap = nil
			continue
		}
		if gap.SourceEpoch > next.SourceEpoch || gap.SourceEpoch == next.SourceEpoch && gap.DiscontinuitySequence > next.DiscontinuitySequence {
			return false, nil
		}
		if gap.FromSequence >= next.Sequence {
			return false, nil
		}
		overlaps := gap.ToSequence > previous.Sequence+1 && gap.FromSequence < next.Sequence
		stream.gap = nil
		if overlaps {
			return true, nil
		}
	}
}

func (stream *shardedGapStream) finish() error {
	stream.gap = nil
	for !stream.done {
		if err := stream.next(); err != nil {
			return err
		}
		stream.gap = nil
	}
	return stream.err
}

// shardedHeader identifies format-v2 archives without loading their media
// history. A failed probe falls through to the unchanged format-v1 path.
func (s *Server) shardedHeader(ctx context.Context, id string) (*domain.Recording, bool, error) {
	if s.storage == nil || !validRecordingPathID(id) {
		return nil, false, nil
	}
	version, err := s.storage.RecordingFormatVersion(ctx, id)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, storage.ErrShardedArchiveUnavailable) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if version != storage.ShardedArchiveFormatVersion {
		return nil, false, nil
	}
	header, err := s.storage.LoadRecordingHeader(ctx, id)
	if err != nil {
		return nil, true, err
	}
	if header == nil || header.FormatVersion != storage.ShardedArchiveFormatVersion {
		return nil, true, storage.ErrShardedArchiveInvalid
	}
	return header, true, nil
}

// recordingSnapshot loads the bounded v2 root when available and preserves the
// historical manager-backed read path for legacy recordings.
func (s *Server) recordingSnapshot(ctx context.Context, id string) (*domain.Recording, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	header, isSharded, err := s.shardedHeader(ctx, id)
	if err != nil {
		return nil, err
	}
	if isSharded {
		return header, nil
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if s.manager == nil {
		return nil, storage.ErrNotFound
	}
	return s.manager.Get(id)
}

func (s *Server) serveShardedVODMasterPlaylist(w http.ResponseWriter, r *http.Request) bool {
	if r.Context().Err() != nil {
		return true
	}
	header, ok, err := s.shardedHeader(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStorageError(w, err)
		return true
	}
	if !ok {
		return r.Context().Err() != nil
	}
	if header.State == domain.StateRecording {
		writeError(w, http.StatusConflict, "VOD playback is available after recording stops")
		return true
	}
	track := header.Tracks["main"]
	if track == nil {
		http.NotFound(w, r)
		return true
	}
	if track.MediaCount == 0 {
		writeError(w, http.StatusConflict, "recording has no captured media segments yet")
		return true
	}
	if s.storage.HasCanonicalPayloadIssue(header.ID) {
		writeError(w, http.StatusServiceUnavailable, "recording media payload is unavailable")
		return true
	}
	bandwidth := track.Bandwidth
	if bandwidth <= 0 {
		bandwidth = 1_000_000
	}
	playlist := fmt.Sprintf("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=%d\n/api/recordings/%s/play/tracks/main/playlist.m3u8\n", bandwidth, header.ID)
	writePlaylist(w, playlist)
	return true
}

func (s *Server) serveShardedVODTrackPlaylist(w http.ResponseWriter, r *http.Request) bool {
	if r.Context().Err() != nil {
		return true
	}
	header, ok, err := s.shardedHeader(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStorageError(w, err)
		return true
	}
	if !ok {
		return r.Context().Err() != nil
	}
	if header.State == domain.StateRecording {
		writeError(w, http.StatusConflict, "VOD playback is available after recording stops")
		return true
	}
	trackID := r.PathValue("track")
	track := header.Tracks[trackID]
	if track == nil {
		http.NotFound(w, r)
		return true
	}
	if track.MediaCount == 0 {
		writeError(w, http.StatusConflict, "recording has no captured media segments yet")
		return true
	}
	if s.storage.HasCanonicalPayloadIssue(header.ID) {
		writeError(w, http.StatusServiceUnavailable, "recording media payload is unavailable")
		return true
	}

	gapCtx, stopGaps := context.WithCancel(r.Context())
	defer stopGaps()
	gaps := newShardedGapStream(gapCtx, s.storage, header.ID)

	var entries strings.Builder
	var first domain.Segment
	var previous domain.Segment
	var previousInit string
	maxDuration := 0.0
	lastDiscontinuity := false
	count := uint64(0)
	var renderErr error
	err = s.storage.IterateShardedTimelineProjection(r.Context(), header.ID, trackID, func(record storage.V2MediaRecord) error {
		segment := record.Segment
		if segment.TimelineOrdinal == 0 {
			renderErr = errShardedTimelineIdentityUnavailable
			return renderErr
		}
		if count == 0 {
			first = segment
		} else {
			gap, gapErr := gaps.hasGapBetween(trackID, previous, segment)
			if gapErr != nil {
				renderErr = gapErr
				return renderErr
			}
			if previous.SourceEpoch != segment.SourceEpoch || previous.DiscontinuitySequence != segment.DiscontinuitySequence || gap || hasGapBetweenEpoch(nil, trackID, previous.SourceEpoch, previous.Sequence, segment.Sequence) {
				fmt.Fprintln(&entries, "#EXT-X-DISCONTINUITY")
				lastDiscontinuity = true
			}
		}
		if segment.Discontinuity && !lastDiscontinuity {
			fmt.Fprintln(&entries, "#EXT-X-DISCONTINUITY")
			lastDiscontinuity = true
		}
		if segment.InitSegmentID != "" && segment.InitSegmentID != previousInit {
			initRecord, lookupErr := s.storage.LookupShardedMediaByID(r.Context(), header.ID, segment.InitSegmentID)
			if lookupErr != nil {
				if errors.Is(lookupErr, storage.ErrNotFound) {
					renderErr = errShardedTimelineMissingInit
				} else {
					renderErr = lookupErr
				}
				return renderErr
			}
			init := initRecord.Segment
			if !init.IsInit || init.TrackID != trackID || init.ID != segment.InitSegmentID {
				renderErr = errShardedTimelineMissingInit
				return renderErr
			}
			uri := fmt.Sprintf("/api/recordings/%s/play/segments/%s", header.ID, init.ID)
			fmt.Fprintf(&entries, "#EXT-X-MAP:URI=%s\n", strconv.Quote(uri))
			previousInit = segment.InitSegmentID
		}
		if segment.ProgramDateTime != nil {
			fmt.Fprintf(&entries, "#EXT-X-PROGRAM-DATE-TIME:%s\n", segment.ProgramDateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
		}
		fmt.Fprintf(&entries, "#EXTINF:%s,\n/api/recordings/%s/play/segments/%s\n", strconv.FormatFloat(segment.Duration, 'f', -1, 64), header.ID, segment.ID)
		if segment.Duration > maxDuration {
			maxDuration = segment.Duration
		}
		previous = segment
		lastDiscontinuity = false
		count++
		return nil
	})
	if err != nil {
		if errors.Is(renderErr, errShardedTimelineMissingInit) {
			writeError(w, http.StatusInternalServerError, renderErr.Error())
		} else if errors.Is(renderErr, errShardedTimelineIdentityUnavailable) {
			writeError(w, http.StatusServiceUnavailable, renderErr.Error())
		} else if renderErr != nil {
			writeStorageError(w, renderErr)
		} else {
			writeStorageError(w, err)
		}
		return true
	}
	if err := gaps.finish(); err != nil {
		writeStorageError(w, err)
		return true
	}
	if count == 0 {
		writeError(w, http.StatusServiceUnavailable, "recording playback timeline is unavailable")
		return true
	}
	target := int(math.Ceil(maxDuration))
	if target < 1 {
		target = 1
	}
	var playlist strings.Builder
	fmt.Fprintf(&playlist, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:%d\n", target, playbackMediaSequence(first))
	playlist.WriteString(entries.String())
	playlist.WriteString("#EXT-X-ENDLIST\n")
	writePlaylist(w, playlist.String())
	return true
}

func (s *Server) serveShardedSegment(w http.ResponseWriter, r *http.Request) bool {
	if r.Context().Err() != nil {
		return true
	}
	header, ok, err := s.shardedHeader(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStorageError(w, err)
		return true
	}
	if !ok {
		return r.Context().Err() != nil
	}
	record, err := s.storage.LookupShardedMediaByID(r.Context(), header.ID, r.PathValue("segmentID"))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			http.NotFound(w, r)
		} else {
			writeStorageError(w, err)
		}
		return true
	}
	segment := record.Segment
	if r.Context().Err() != nil {
		return true
	}
	info, err := s.storage.StatPayload(header.ID, segment.StoragePath)
	if err != nil || !info.Regular || info.Size < 0 || info.Size != segment.PayloadSize {
		writeError(w, http.StatusNotFound, "stored payload is unavailable")
		return true
	}
	if r.Context().Err() != nil {
		return true
	}

	var payload io.ReadCloser
	status := http.StatusOK
	contentLength := segment.PayloadSize
	contentRange := ""
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		offset, length, valid := parseSingleByteRange(rangeHeader, info.Size)
		if !valid {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
			w.Header().Set("Content-Length", "0")
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Type", mediaContentType(segment.StoragePath))
			w.Header().Set("Cache-Control", "private, no-store")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return true
		}
		payload, err = s.storage.OpenPayloadRangeReaderContext(r.Context(), header.ID, segment.StoragePath, offset, length)
		if err != nil {
			writeError(w, http.StatusNotFound, "stored payload is unavailable")
			return true
		}
		status = http.StatusPartialContent
		contentLength = length
		contentRange = fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, info.Size)
	} else {
		payload, err = s.storage.OpenPayloadRangeReaderContext(r.Context(), header.ID, segment.StoragePath, 0, info.Size)
		if err != nil {
			writeError(w, http.StatusNotFound, "stored payload is unavailable")
			return true
		}
	}
	defer payload.Close()
	if err = http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeError(w, http.StatusInternalServerError, "stored payload could not be streamed")
		return true
	}
	w.Header().Set("Content-Type", mediaContentType(segment.StoragePath))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Accept-Ranges", "bytes")
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
		w.WriteHeader(status)
	}
	_, _ = io.Copy(w, payload)
	return true
}
