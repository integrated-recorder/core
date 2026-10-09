package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestCompareArchiveSnapshotsAllowingManifestAppend(t *testing.T) {
	const (
		payload   = "tracks/main/00000000000000000001.ts"
		manifest  = "manifests/main-123456-abcdef.m3u8"
		sidecar   = manifest + ".json"
		addedM3U8 = "manifests/main-123789-012345.m3u8"
		addedJSON = addedM3U8 + ".json"
	)
	before := map[string]string{
		payload:  "payload-hash",
		manifest: "manifest-hash",
		sidecar:  "sidecar-hash",
	}

	t.Run("allows complete paired manifest append", func(t *testing.T) {
		after := copySnapshot(before)
		after[addedM3U8] = "new-manifest-hash"
		after[addedJSON] = "new-sidecar-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err != nil {
			t.Fatalf("complete append rejected: %v", err)
		}
	})

	t.Run("allows an unreferenced manifest payload interrupted before its sidecar", func(t *testing.T) {
		after := copySnapshot(before)
		after[addedM3U8] = "new-manifest-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err != nil {
			t.Fatalf("atomic but unreferenced manifest payload rejected: %v", err)
		}
	})

	t.Run("accepts only complete manifest objects referenced by the archive root", func(t *testing.T) {
		recording := &domain.Recording{Snapshots: []domain.ManifestSnapshot{{StoragePath: manifest}}}
		if err := validateManifestSnapshotReferences(recording, before); err != nil {
			t.Fatalf("complete root-referenced snapshot rejected: %v", err)
		}
		recording.Snapshots = append(recording.Snapshots, domain.ManifestSnapshot{StoragePath: addedM3U8})
		if err := validateManifestSnapshotReferences(recording, map[string]string{
			addedM3U8: "new-manifest-hash",
		}); err == nil {
			t.Fatal("root reference to an orphan manifest payload accepted")
		}
	})

	t.Run("rejects changed preexisting object", func(t *testing.T) {
		after := copySnapshot(before)
		after[payload] = "changed-payload-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err == nil {
			t.Fatal("changed payload accepted")
		}
	})

	t.Run("rejects removed preexisting object", func(t *testing.T) {
		after := copySnapshot(before)
		delete(after, sidecar)
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err == nil {
			t.Fatal("removed sidecar accepted")
		}
	})

	t.Run("rejects nonmanifest addition", func(t *testing.T) {
		after := copySnapshot(before)
		after["tracks/main/00000000000000000002.ts"] = "unexpected-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err == nil {
			t.Fatal("unexpected payload addition accepted")
		}
	})

	t.Run("rejects an orphan manifest sidecar", func(t *testing.T) {
		after := copySnapshot(before)
		after[addedJSON] = "new-sidecar-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err == nil {
			t.Fatal("manifest sidecar without its payload accepted")
		}
	})

	t.Run("rejects nested manifest addition", func(t *testing.T) {
		after := copySnapshot(before)
		nested := "manifests/main-nested/added.m3u8"
		after[nested] = "new-manifest-hash"
		after[nested+".json"] = "new-sidecar-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err == nil {
			t.Fatal("nested manifest addition accepted")
		}
	})
}

func TestCompareArchiveSnapshotsAllowingV2ManifestAppend(t *testing.T) {
	root := t.TempDir()
	first := domain.ManifestSnapshot{TrackID: "main", SourceURI: "https://fixture.invalid/live.m3u8", StoragePath: "manifests/main-1-a.m3u8", FetchedAt: timeForCrashSnapshotTest(1), SHA256: "a"}
	second := domain.ManifestSnapshot{TrackID: "main", SourceURI: "https://fixture.invalid/live.m3u8", StoragePath: "manifests/main-2-b.m3u8", FetchedAt: timeForCrashSnapshotTest(2), SHA256: "b"}
	pagePath := "archive/v2/manifests/00000000000000000000.json"
	pageBytes := func(entries []domain.ManifestSnapshot) []byte {
		t.Helper()
		encoded, err := json.Marshal(struct {
			Version int                       `json:"version"`
			Number  uint64                    `json:"number"`
			Entries []domain.ManifestSnapshot `json:"entries"`
		}{Version: 1, Number: 0, Entries: entries})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	digest := func(data []byte) string {
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	writePage := func(data []byte) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(pagePath))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}

	beforePage := pageBytes([]domain.ManifestSnapshot{first})
	afterPage := pageBytes([]domain.ManifestSnapshot{first, second})
	writePage(afterPage)
	manifestObjects := map[string]string{
		first.StoragePath:            "payload-first",
		first.StoragePath + ".json":  "sidecar-first",
		second.StoragePath:           "payload-second",
		second.StoragePath + ".json": "sidecar-second",
	}
	before := map[string]string{pagePath: digest(beforePage), first.StoragePath: manifestObjects[first.StoragePath], first.StoragePath + ".json": manifestObjects[first.StoragePath+".json"]}
	after := map[string]string{pagePath: digest(afterPage)}
	for key, value := range manifestObjects {
		after[key] = value
	}
	if err := compareArchiveSnapshotsAllowingV2ManifestAppend(root, before, after, []domain.ManifestSnapshot{first}, []domain.ManifestSnapshot{first, second}); err != nil {
		t.Fatalf("append to partial final manifest page rejected: %v", err)
	}

	t.Run("rejects replacement of an existing manifest history row", func(t *testing.T) {
		changed := first
		changed.SHA256 = "changed"
		writePage(pageBytes([]domain.ManifestSnapshot{changed, second}))
		if err := compareArchiveSnapshotsAllowingV2ManifestAppend(root, before, after, []domain.ManifestSnapshot{first}, []domain.ManifestSnapshot{changed, second}); err == nil {
			t.Fatal("changed preexisting V2 manifest entry accepted")
		}
	})

	t.Run("rejects changed archive media", func(t *testing.T) {
		writePage(afterPage)
		beforeWithMedia := copySnapshot(before)
		afterWithMedia := copySnapshot(after)
		beforeWithMedia["archive/v2/media/main/00000000000000000000.json"] = "media-before"
		afterWithMedia["archive/v2/media/main/00000000000000000000.json"] = "media-after"
		if err := compareArchiveSnapshotsAllowingV2ManifestAppend(root, beforeWithMedia, afterWithMedia, []domain.ManifestSnapshot{first}, []domain.ManifestSnapshot{first, second}); err == nil {
			t.Fatal("changed media shard accepted")
		}
	})
}

func timeForCrashSnapshotTest(second int) time.Time {
	return time.Unix(int64(second), 0).UTC()
}

func copySnapshot(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}
