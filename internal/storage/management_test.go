package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestArchiveIndexIncludesOnlyCanonicalReferences(t *testing.T) {
	store, recording := managementTestArchive(t)
	if err := os.WriteFile(filepath.Join(store.recordingDir(recording.ID), "unreferenced.log"), []byte("private stray"), 0600); err != nil {
		t.Fatal(err)
	}
	entries, err := store.ArchiveIndex(recording)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"manifests/main-1.m3u8",
		"manifests/main-1.m3u8.json",
		"recording.json",
		"tracks/main/init.m4s",
		"tracks/main/init.m4s.json",
		"tracks/main/segment.m4s",
		"tracks/main/segment.m4s.json",
	}
	if len(entries) != len(want) {
		t.Fatalf("ArchiveIndex returned %d entries: %#v", len(entries), entries)
	}
	for i, entry := range entries {
		if entry.Path != want[i] {
			t.Fatalf("entry[%d].Path = %q, want %q", i, entry.Path, want[i])
		}
		if filepath.IsAbs(entry.Path) || strings.Contains(entry.Path, "source.example") {
			t.Fatalf("unsafe archive path exposed: %#v", entry)
		}
	}
	if entries[5].SHA256 != managementDigest([]byte("media")) {
		t.Fatalf("canonical digest missing: %#v", entries[5])
	}
}

func TestRecordingDirectoryBytesIncludesPreservedUnreferencedFilesAndDegradedPayloads(t *testing.T) {
	store, recording := managementTestArchive(t)
	root := store.recordingDir(recording.ID)
	if err := os.Remove(filepath.Join(root, "tracks", "main", "segment.m4s")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "orphan.bin"), []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	want := int64(0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			want += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.RecordingDirectoryBytes(recording.ID)
	if err != nil || got != want {
		t.Fatalf("recording directory bytes=%d want=%d err=%v", got, want, err)
	}
	if _, err := store.ArchiveIndex(recording); err == nil {
		t.Fatal("archive index unexpectedly accepted the intentionally missing referenced payload")
	}
}

func TestArchiveIndexRejectsTraversalDuplicateAndSymlinkMetadataPaths(t *testing.T) {
	store, recording := managementTestArchive(t)
	t.Run("traversal", func(t *testing.T) {
		bad := cloneManagementRecording(recording)
		bad.Tracks["main"].Segments[0].StoragePath = "tracks/main/../../outside"
		if _, err := store.ArchiveIndex(bad); err == nil {
			t.Fatal("traversal metadata path was accepted")
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		bad := cloneManagementRecording(recording)
		bad.Tracks["main"].InitSegments[0].StoragePath = bad.Tracks["main"].Segments[0].StoragePath
		if _, err := store.ArchiveIndex(bad); err == nil {
			t.Fatal("duplicate canonical path was accepted")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "outside.m4s")
		if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(store.recordingDir(recording.ID), "tracks", "main", "linked.m4s")
		if err := os.Symlink(outside, link); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		bad := cloneManagementRecording(recording)
		bad.Tracks["main"].Segments[0].StoragePath = "tracks/main/linked.m4s"
		if _, err := store.ArchiveIndex(bad); err == nil {
			t.Fatal("symlink metadata path was accepted")
		}
	})
}

func TestVerifyRecordingReportsValidMissingAndCorruptPayloads(t *testing.T) {
	store, recording := managementTestArchive(t)
	root := store.recordingDir(recording.ID)
	if err := os.Remove(filepath.Join(root, "tracks", "main", "init.m4s")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifests", "main-1.m3u8"), []byte("altered"), 0600); err != nil {
		t.Fatal(err)
	}
	result := store.VerifyRecording(recording)
	if result.Status != IntegrityDegraded || result.ObjectsTotal != 3 || result.ObjectsVerified != 1 || result.ObjectsMissing != 1 || result.ObjectsCorrupt != 1 {
		t.Fatalf("unexpected integrity result: %#v", result)
	}
	codes := map[string]bool{}
	for _, issue := range result.Issues {
		codes[issue.Code] = true
		if strings.Contains(issue.Path, "source.example") || filepath.IsAbs(issue.Path) {
			t.Fatalf("unsafe issue path: %#v", issue)
		}
	}
	if !codes["missing_payload"] || !codes["payload_mismatch"] {
		t.Fatalf("missing/corrupt issues absent: %#v", result.Issues)
	}
}

func TestVerifyRecordingAcceptsAllCanonicalPayloads(t *testing.T) {
	store, recording := managementTestArchive(t)
	result := store.VerifyRecording(recording)
	if result.Status != IntegrityVerified || result.ObjectsTotal != 3 || result.ObjectsVerified != 3 || result.ObjectsMissing != 0 || result.ObjectsCorrupt != 0 || len(result.Issues) != 0 {
		t.Fatalf("valid archive failed verification: %#v", result)
	}
}

func TestVerifyRecordingRejectsUnsafePathWithoutEchoingIt(t *testing.T) {
	store, recording := managementTestArchive(t)
	bad := cloneManagementRecording(recording)
	bad.Tracks["main"].Segments[0].StoragePath = "../../secret-token"
	result := store.VerifyRecording(bad)
	if result.ObjectsTotal != 3 || result.ObjectsCorrupt != 1 {
		t.Fatalf("unsafe path not counted as corrupt: %#v", result)
	}
	for _, issue := range result.Issues {
		if strings.Contains(issue.Path, "secret-token") || filepath.IsAbs(issue.Path) {
			t.Fatalf("unsafe metadata path echoed: %#v", issue)
		}
	}
}

func TestStorageStatsCountsCanonicalObjectsAndDoesNotFollowSymlinks(t *testing.T) {
	store, recording := managementTestArchive(t)
	outside := filepath.Join(t.TempDir(), "large.bin")
	if err := os.WriteFile(outside, bytes.Repeat([]byte("x"), 32*1024), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(store.recordingDir(recording.ID), "tracks", "main", "outside-link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	stats, err := store.StorageStats()
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := uint64(0)
	if err = filepath.WalkDir(filepath.Join(store.Root(), "recordings"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == filepath.Join(store.Root(), "recordings") || entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			wantBytes += uint64(info.Size())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if stats.RecordingCount != 1 || stats.RecordingBytes != wantBytes || stats.SegmentCount != 1 || stats.InitSegmentCount != 1 || stats.ManifestCount != 1 {
		t.Fatalf("unexpected storage stats: %#v, want bytes %d", stats, wantBytes)
	}
	if stats.FilesystemTotalBytes == 0 || stats.FilesystemAvailableBytes > stats.FilesystemTotalBytes || stats.FilesystemUsedBytes > stats.FilesystemTotalBytes {
		t.Fatalf("invalid filesystem stats: %#v", stats)
	}
	if stats.ArchiveRoot != store.Root() {
		t.Fatalf("internal archive root = %q, want %q", stats.ArchiveRoot, store.Root())
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(store.Root())) {
		t.Fatalf("absolute archive root leaked in serialized storage stats: %s", encoded)
	}
}

func TestDeleteRecordingDataRenamesThenRemovesAndIsIdempotent(t *testing.T) {
	store, recording := managementTestArchive(t)
	other := cloneManagementRecording(recording)
	other.ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := store.CreateRecording(other); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteRecordingData(recording.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(store.recordingDir(recording.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical directory remains visible after delete: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(store.Root(), "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".deleting-"+recording.ID+"-") {
			t.Fatalf("tombstone remains after successful deletion: %q", entry.Name())
		}
	}
	if _, err = os.Stat(store.recordingDir(other.ID)); err != nil {
		t.Fatalf("unrelated recording was deleted: %v", err)
	}
	if err = store.DeleteRecordingData(recording.ID); err != nil {
		t.Fatalf("second delete was not idempotent: %v", err)
	}
}

func TestDeleteRecordingDataRejectsTraversalAndSymlink(t *testing.T) {
	store, _ := managementTestArchive(t)
	if err := store.DeleteRecordingData("../outside"); err == nil {
		t.Fatal("traversal recording ID was accepted")
	}
	external := t.TempDir()
	const id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	link := store.recordingDir(id)
	if err := os.Symlink(external, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := store.DeleteRecordingData(id); err == nil {
		t.Fatal("recording directory symlink was deleted")
	}
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("rejected symlink changed: info=%v err=%v", info, err)
	}
	if _, err := os.Stat(external); err != nil {
		t.Fatalf("symlink target was affected: %v", err)
	}
}

func TestPendingDeleteTombstoneIsCompletedOnRetryAndReload(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		store, recording := managementTestArchive(t)
		base := filepath.Join(store.Root(), "recordings")
		tombstone := filepath.Join(base, ".deleting-"+recording.ID+"-0123456789abcdef0123456789abcdef")
		if err := os.Rename(store.recordingDir(recording.ID), tombstone); err != nil {
			t.Fatal(err)
		}
		if err := store.DeleteRecordingData(recording.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(tombstone); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pending tombstone remains: %v", err)
		}
	})

	t.Run("reload", func(t *testing.T) {
		store, recording := managementTestArchive(t)
		base := filepath.Join(store.Root(), "recordings")
		tombstone := filepath.Join(base, ".deleting-"+recording.ID+"-fedcba9876543210fedcba9876543210")
		if err := os.Rename(store.recordingDir(recording.ID), tombstone); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.LoadAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 0 {
			t.Fatalf("deleted recording returned after reload: %#v", loaded)
		}
		if _, err := os.Lstat(tombstone); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pending tombstone remains after reload: %v", err)
		}
	})
}

func managementTestArchive(t *testing.T) (*Store, *domain.Recording) {
	t.Helper()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	now := time.Now().UTC()
	recording := &domain.Recording{FormatVersion: 1, ID: id, Title: "fixture", AdapterID: "opaque", State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	media := []byte("media")
	init := []byte("init")
	manifest := []byte("#EXTM3U\n")
	mediaResult, err := store.SavePayload(id, "tracks/main/segment.m4s", bytes.NewReader(media), 100)
	if err != nil {
		t.Fatal(err)
	}
	initResult, err := store.SavePayload(id, "tracks/main/init.m4s", bytes.NewReader(init), 100)
	if err != nil {
		t.Fatal(err)
	}
	manifestResult, err := store.SavePayload(id, "manifests/main-1.m3u8", bytes.NewReader(manifest), 100)
	if err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{ID: "segment-1", TrackID: "main", Sequence: 1, SourceURI: "https://source.example/segment?token=secret", StoragePath: "tracks/main/segment.m4s", PayloadSize: mediaResult.Size, SHA256: mediaResult.SHA256, Duration: 2}
	initSegment := domain.Segment{ID: "init-1", TrackID: "main", SourceURI: "https://source.example/init?token=secret", StoragePath: "tracks/main/init.m4s", PayloadSize: initResult.Size, SHA256: initResult.SHA256, IsInit: true}
	recording.Tracks["main"].Segments = []domain.Segment{segment}
	recording.Tracks["main"].InitSegments = []domain.Segment{initSegment}
	recording.Snapshots = []domain.ManifestSnapshot{{TrackID: "main", SourceURI: "https://source.example/live?token=secret", StoragePath: "manifests/main-1.m3u8", SHA256: manifestResult.SHA256, Size: manifestResult.Size}}
	for path, value := range map[string]any{segment.StoragePath: segment, initSegment.StoragePath: initSegment, recording.Snapshots[0].StoragePath: recording.Snapshots[0]} {
		if err = store.SaveSidecar(id, path, value); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	return store, recording
}

func cloneManagementRecording(recording *domain.Recording) *domain.Recording {
	clone := *recording
	clone.Tracks = make(map[string]*domain.Track, len(recording.Tracks))
	for id, track := range recording.Tracks {
		copyTrack := *track
		copyTrack.Segments = append([]domain.Segment(nil), track.Segments...)
		copyTrack.InitSegments = append([]domain.Segment(nil), track.InitSegments...)
		clone.Tracks[id] = &copyTrack
	}
	clone.Snapshots = append([]domain.ManifestSnapshot(nil), recording.Snapshots...)
	return &clone
}

func managementDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
