package derivative

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/integrated-recorder/core/internal/domain"
)

func primaryTrack(recording *domain.Recording) (*domain.Track, error) {
	if recording == nil || len(recording.Tracks) == 0 {
		return nil, ErrUnsupported
	}
	if main := recording.Tracks["main"]; main != nil {
		if main.ID == "" || len(main.Segments) == 0 {
			return nil, ErrUnsupported
		}
		return main, nil
	}
	if len(recording.Tracks) != 1 {
		return nil, ErrUnsupported
	}
	for _, track := range recording.Tracks {
		if track == nil || track.ID == "" || len(track.Segments) == 0 {
			return nil, ErrUnsupported
		}
		return track, nil
	}
	return nil, ErrUnsupported
}

// buildPlaylist makes a private, local-only VOD projection. The canonical
// source playlist and all source payloads remain untouched.
func buildPlaylist(recording *domain.Recording, localNames map[string]string) (string, error) {
	track, err := primaryTrack(recording)
	if err != nil {
		return "", err
	}
	segments := append([]domain.Segment(nil), track.Segments...)
	seenOrdinal := make(map[uint64]bool, len(segments))
	for _, segment := range segments {
		if segment.ArchiveOrdinal == 0 {
			continue
		}
		if seenOrdinal[segment.ArchiveOrdinal] {
			return "", ErrUnsupported
		}
		seenOrdinal[segment.ArchiveOrdinal] = true
	}
	// Match the archive loader's compatibility ordering: persisted archive
	// ordinals win, while legacy zero-ordinal segments retain epoch/sequence
	// ordering and follow ordinal-bearing entries.
	sort.SliceStable(segments, func(i, j int) bool {
		a, b := segments[i], segments[j]
		if a.ArchiveOrdinal != 0 || b.ArchiveOrdinal != 0 {
			if a.ArchiveOrdinal == 0 {
				return false
			}
			if b.ArchiveOrdinal == 0 {
				return true
			}
			return a.ArchiveOrdinal < b.ArchiveOrdinal
		}
		if a.SourceEpoch != b.SourceEpoch {
			return a.SourceEpoch < b.SourceEpoch
		}
		return a.Sequence < b.Sequence
	})
	initByID := make(map[string]domain.Segment, len(track.InitSegments))
	for _, init := range track.InitSegments {
		if init.ID == "" || init.StoragePath == "" || !init.IsInit {
			return "", ErrUnsupported
		}
		if _, duplicate := initByID[init.ID]; duplicate {
			return "", ErrUnsupported
		}
		initByID[init.ID] = init
	}
	maxDuration := 0.0
	for _, segment := range segments {
		if segment.TrackID != track.ID || segment.StoragePath == "" || math.IsNaN(segment.Duration) || math.IsInf(segment.Duration, 0) || segment.Duration <= 0 {
			return "", ErrUnsupported
		}
		if segment.Duration > maxDuration {
			maxDuration = segment.Duration
		}
		if localNames[segment.StoragePath] == "" {
			return "", ErrUnsupported
		}
		if segment.InitSegmentID != "" {
			init, exists := initByID[segment.InitSegmentID]
			if !exists || localNames[init.StoragePath] == "" {
				return "", ErrUnsupported
			}
		}
	}
	target := int64(math.Ceil(maxDuration))
	if target < 1 {
		target = 1
	}
	var playlist strings.Builder
	fmt.Fprintf(&playlist, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:0\n", target)
	lastInit := "\x00"
	for _, segment := range segments {
		if segment.Discontinuity {
			playlist.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if segment.InitSegmentID != lastInit {
			if segment.InitSegmentID != "" {
				init := initByID[segment.InitSegmentID]
				local := localNames[init.StoragePath]
				fmt.Fprintf(&playlist, "#EXT-X-MAP:URI=\"%s\"\n", local)
			}
			lastInit = segment.InitSegmentID
		}
		fmt.Fprintf(&playlist, "#EXTINF:%s,\n%s\n", strconv.FormatFloat(segment.Duration, 'f', -1, 64), localNames[segment.StoragePath])
	}
	playlist.WriteString("#EXT-X-ENDLIST\n")
	return playlist.String(), nil
}
