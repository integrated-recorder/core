package bootstrap

import "testing"

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

	t.Run("rejects incomplete pair", func(t *testing.T) {
		after := copySnapshot(before)
		after[addedM3U8] = "new-manifest-hash"
		if err := compareArchiveSnapshotsAllowingManifestAppend(before, after); err == nil {
			t.Fatal("unpaired manifest accepted")
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

func copySnapshot(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}
