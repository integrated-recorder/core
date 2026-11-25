package replication

import (
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
)

var ErrPlaybackIncomplete = errors.New("snapshot cannot be exposed as complete VOD playback")

// RestoredPlaylist describes a generated local HLS playlist. Paths are
// relative to the atomically published restore directory.
type RestoredPlaylist struct {
	TrackID string `json:"track_id"`
	Path    string `json:"path"`
}

type restoredInitObject struct {
	path   string
	size   int64
	digest string
}

// buildRestoredPlaylists creates local-only HLS views over the verified
// plaintext objects. The source archive is never modified. Snapshot v1 remains
// decryptable, but cannot be exposed as playable when it lacks the init-map
// relationship required to reconstruct an HLS timeline safely.
func buildRestoredPlaylists(snapshot archivesnapshot.Snapshot, objects []RestoredObject) (string, []RestoredPlaylist, map[string][]byte, error) {
	if archivesnapshot.Verify(snapshot) != nil {
		return "", nil, nil, ErrInvalid
	}
	if snapshot.SchemaVersion < 2 {
		return "", nil, nil, nil
	}
	for _, coverage := range snapshot.Content.Coverage {
		switch coverage.State {
		case archiveindex.CoverageKnownMissing, archiveindex.CoverageAcquisitionFailed, archiveindex.CoverageConflict:
			return "", nil, nil, ErrPlaybackIncomplete
		}
	}

	byCoordinate := make(map[archiveindex.Coordinate]RestoredObject, len(objects))
	for _, object := range objects {
		objectID := strings.TrimSuffix(path.Base(object.Path), ".payload")
		if object.Size <= 0 || object.SHA256 == "" || !validObjectID(objectID) || object.Path != path.Join("objects", objectID+".payload") {
			return "", nil, nil, ErrInvalid
		}
		if _, duplicate := byCoordinate[object.Coordinate]; duplicate {
			return "", nil, nil, ErrInvalid
		}
		byCoordinate[object.Coordinate] = object
	}
	if len(byCoordinate) != len(snapshot.Content.Objects) {
		return "", nil, nil, ErrPlaybackIncomplete
	}

	initPaths := make(map[string]restoredInitObject)
	mediaByTrack := make(map[string][]archivesnapshot.Object)
	for _, object := range snapshot.Content.Objects {
		if object.State != archiveindex.CoveragePresent {
			return "", nil, nil, ErrPlaybackIncomplete
		}
		restored, ok := byCoordinate[object.Coordinate]
		if !ok || restored.Size != object.PlaintextSize || restored.SHA256 != object.PlaintextSHA256 {
			return "", nil, nil, ErrPlaybackIncomplete
		}
		if object.Coordinate.Kind == archiveindex.ObjectInit {
			if object.InitIdentity == "" {
				continue
			}
			key := object.Coordinate.TrackID + "\x00" + object.InitIdentity
			candidate := restoredInitObject{path: restored.Path, size: restored.Size, digest: restored.SHA256}
			if prior, duplicate := initPaths[key]; duplicate {
				if prior.size != candidate.size || prior.digest != candidate.digest {
					return "", nil, nil, ErrPlaybackIncomplete
				}
				// Snapshot order is canonical, so retain the first identical map
				// occurrence as the deterministic playlist target.
				continue
			}
			initPaths[key] = candidate
		} else {
			mediaByTrack[object.Coordinate.TrackID] = append(mediaByTrack[object.Coordinate.TrackID], object)
		}
	}
	if len(mediaByTrack) == 0 {
		return "", nil, nil, ErrPlaybackIncomplete
	}

	tracks := make([]string, 0, len(mediaByTrack))
	for track := range mediaByTrack {
		tracks = append(tracks, track)
	}
	sort.Strings(tracks)
	files := make(map[string][]byte, len(tracks)+1)
	playlists := make([]RestoredPlaylist, 0, len(tracks))
	for index, track := range tracks {
		media := mediaByTrack[track]
		sort.Slice(media, func(i, j int) bool { return media[i].TimelineOrdinal < media[j].TimelineOrdinal })
		name := fmt.Sprintf("track-%03d.m3u8", index+1)
		playlist, err := buildRestoredMediaPlaylist(track, media, byCoordinate, initPaths)
		if err != nil {
			return "", nil, nil, err
		}
		files[path.Join("playlists", name)] = playlist
		playlists = append(playlists, RestoredPlaylist{TrackID: track, Path: path.Join("playlists", name)})
	}
	var master strings.Builder
	master.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n")
	for _, playlist := range playlists {
		master.WriteString("#EXT-X-STREAM-INF:BANDWIDTH=1000000\n")
		master.WriteString(playlist.Path)
		master.WriteByte('\n')
	}
	files["index.m3u8"] = []byte(master.String())
	return "index.m3u8", playlists, files, nil
}

func buildRestoredMediaPlaylist(track string, media []archivesnapshot.Object, byCoordinate map[archiveindex.Coordinate]RestoredObject, initPaths map[string]restoredInitObject) ([]byte, error) {
	if len(media) == 0 {
		return nil, ErrPlaybackIncomplete
	}
	maxDuration := 0.0
	for index, object := range media {
		if object.Coordinate.TrackID != track || object.Coordinate.Kind != archiveindex.ObjectMedia || object.TimelineOrdinal != uint64(index+1) || object.State != archiveindex.CoveragePresent {
			return nil, ErrPlaybackIncomplete
		}
		if object.Duration > maxDuration {
			maxDuration = object.Duration
		}
	}
	targetDuration := int64(math.Ceil(maxDuration))
	if targetDuration < 1 || targetDuration > 86400 {
		return nil, ErrPlaybackIncomplete
	}
	var output strings.Builder
	output.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n")
	output.WriteString("#EXT-X-TARGETDURATION:")
	output.WriteString(strconv.FormatInt(targetDuration, 10))
	output.WriteByte('\n')
	output.WriteString("#EXT-X-MEDIA-SEQUENCE:")
	output.WriteString(strconv.FormatUint(media[0].Coordinate.Sequence, 10))
	output.WriteByte('\n')
	lastInit := ""
	for _, object := range media {
		if object.Discontinuity {
			output.WriteString("#EXT-X-DISCONTINUITY\n")
		}
		if object.InitIdentity != lastInit {
			if lastInit != "" && object.InitIdentity == "" {
				return nil, ErrPlaybackIncomplete
			}
			if object.InitIdentity != "" {
				initPath, ok := initPaths[track+"\x00"+object.InitIdentity]
				if !ok {
					return nil, ErrPlaybackIncomplete
				}
				output.WriteString(`#EXT-X-MAP:URI="../`)
				output.WriteString(initPath.path)
				output.WriteString(`"`)
				output.WriteByte('\n')
			}
			lastInit = object.InitIdentity
		}
		if object.ProgramDateTime != "" {
			output.WriteString("#EXT-X-PROGRAM-DATE-TIME:")
			output.WriteString(object.ProgramDateTime)
			output.WriteByte('\n')
		}
		output.WriteString("#EXTINF:")
		output.WriteString(strconv.FormatFloat(object.Duration, 'f', -1, 64))
		output.WriteString(",\n../")
		output.WriteString(byCoordinate[object.Coordinate].Path)
		output.WriteByte('\n')
	}
	output.WriteString("#EXT-X-ENDLIST\n")
	return []byte(output.String()), nil
}
