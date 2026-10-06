package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestLoadAllMarksStaleRecordingInterrupted(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Add(-time.Minute)
	recording := &domain.Recording{FormatVersion: 1, ID: id, SourceURL: "https://source.example", State: domain.StateRecording, CreatedAt: started, StartedAt: started, Tracks: map[string]*domain.Track{"main": {ID: "main", PendingSequences: []uint64{3}, PendingSegments: []domain.PendingSequence{{SourceEpoch: 7, Sequence: 8}}, Segments: []domain.Segment{{ID: "s2", Sequence: 2}, {ID: "s1", Sequence: 1}}}}}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	recordings, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recordings) != 1 || recordings[0].State != domain.StateInterrupted {
		t.Fatalf("loaded recordings %#v", recordings)
	}
	if recordings[0].Tracks["main"].Segments[0].Sequence != 1 {
		t.Fatal("segments were not ordered on reload")
	}
	if recordings[0].StoppedAt == nil {
		t.Fatal("stopped_at was not set")
	}
	if len(recordings[0].Gaps) != 2 || recordings[0].Gaps[0].FromSequence != 3 || recordings[0].Gaps[0].SourceEpoch != 0 || recordings[0].Gaps[1].FromSequence != 8 || recordings[0].Gaps[1].SourceEpoch != 7 || len(recordings[0].Tracks["main"].PendingSequences) != 0 || len(recordings[0].Tracks["main"].PendingSegments) != 0 {
		t.Fatalf("pending segment was not converted to gap: %#v", recordings[0])
	}
	if _, err = os.Stat(filepath.Join(dir, "recordings", id, "recording.json")); err != nil {
		t.Fatal(err)
	}
}

func TestLocalCreateRecordingWithSidecarPublishesRootAndSidecarTogether(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0a23456789abcdef0123456789abcdef"
	type privateContext struct {
		SchemaVersion int    `json:"schema_version"`
		ManifestURL   string `json:"manifest_url"`
	}
	context := privateContext{SchemaVersion: 1, ManifestURL: "https://media.example/live.m3u8"}
	if err := store.CreateRecordingWithSidecar(stoppedRecording(id), "archive-index/acquisition-context", context); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "recordings", id, "recording.json")); err != nil {
		t.Fatalf("recording root was not published: %v", err)
	}
	var got privateContext
	if err := store.LoadSidecar(id, "archive-index/acquisition-context", 64<<10, &got); err != nil {
		t.Fatalf("private sidecar was not published with root: %v", err)
	}
	if got != context {
		t.Fatalf("sidecar=%#v, want %#v", got, context)
	}

	// A failed duplicate initialization must leave the existing root and
	// sidecar untouched rather than replacing either document.
	if err := store.CreateRecordingWithSidecar(stoppedRecording(id), "archive-index/acquisition-context", privateContext{SchemaVersion: 2}); err == nil {
		t.Fatal("duplicate recording initialization succeeded")
	}
	var after privateContext
	if err := store.LoadSidecar(id, "archive-index/acquisition-context", 64<<10, &after); err != nil || after != context {
		t.Fatalf("existing sidecar changed after failed initialization: %#v, %v", after, err)
	}
}

func TestLocalCreateRecordingStagingResidueIsNotPublished(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0c23456789abcdef0123456789abcdef"
	stage := filepath.Join(root, "recordings", ".incomplete-"+id+"-crash")
	if err := os.MkdirAll(filepath.Join(stage, "archive-index"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "archive-index", "acquisition-context.json"), []byte(`{"schema_version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("staging residue became a visible recording: %#v", loaded)
	}
	if _, err := os.Stat(filepath.Join(root, "recordings", id, "recording.json")); !os.IsNotExist(err) {
		t.Fatalf("staging residue published canonical root: %v", err)
	}
	found := false
	for _, issue := range store.RecoveryIssues() {
		if issue.ID == id && issue.Code == "incomplete_creation" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("crash residue was not reported: %#v", store.RecoveryIssues())
	}
}

func TestSavePayloadExactDoesNotPublishWrongLength(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "fedcba0987654321fedcba0987654321"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SavePayloadExact(id, "tracks/main/range.m4s", bytes.NewReader([]byte{1, 2, 3}), 100, 4); err == nil {
		t.Fatal("short payload was accepted")
	}
	if _, err = store.OpenPayload(id, "tracks/main/range.m4s"); err == nil {
		t.Fatal("wrong-length payload was published")
	}
}

func TestLoadAllReportsUnavailableCanonicalPayloadWithoutDeletingData(t *testing.T) {
	for _, tc := range []struct {
		name       string
		remove     bool
		wantCode   string
		wantExists bool
	}{
		{name: "missing", remove: true, wantCode: "canonical_payload_unavailable"},
		{name: "mismatched bytes", wantCode: "canonical_payload_mismatch", wantExists: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			const id = "0123456789abcdef0123456789abcdef"
			if err = store.CreateRecording(stoppedRecording(id)); err != nil {
				t.Fatal(err)
			}
			payload := []byte("original media bytes")
			result, err := store.SavePayload(id, "tracks/main/segment.ts", bytes.NewReader(payload), 1024)
			if err != nil {
				t.Fatal(err)
			}
			recording, err := os.ReadFile(filepath.Join(root, "recordings", id, "recording.json"))
			if err != nil {
				t.Fatal(err)
			}
			var doc domain.Recording
			if err = json.Unmarshal(recording, &doc); err != nil {
				t.Fatal(err)
			}
			doc.Tracks["main"].Segments = []domain.Segment{{ID: "segment-1", TrackID: "main", Sequence: 1, StoragePath: "tracks/main/segment.ts", PayloadSize: result.Size, SHA256: result.SHA256}}
			if err = store.SaveRecording(&doc); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "recordings", id, "tracks", "main", "segment.ts")
			if tc.remove {
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if err = os.WriteFile(path, []byte("different bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			loaded, err := store.LoadAll()
			if err != nil || len(loaded) != 1 {
				t.Fatalf("LoadAll = %d recordings, %v", len(loaded), err)
			}
			if len(loaded[0].Tracks["main"].Segments) != 1 {
				t.Fatal("canonical segment reference was silently removed")
			}
			found := false
			for _, issue := range store.RecoveryIssues() {
				if issue.ID == id && issue.Code == tc.wantCode {
					found = true
				}
			}
			if !found {
				t.Fatalf("recovery issue %q not recorded: %#v", tc.wantCode, store.RecoveryIssues())
			}
			_, statErr := os.Stat(path)
			if tc.wantExists && statErr != nil {
				t.Fatalf("mismatched source bytes were deleted: %v", statErr)
			}
			if !tc.wantExists && !os.IsNotExist(statErr) {
				t.Fatalf("missing payload unexpectedly changed: stat error %v", statErr)
			}
		})
	}
}

func TestRecordingPermissionsCanonicalURIsAndProvenanceSurviveReload(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	uri := "https://media.example/segment.m4s?token=opaque-value"
	recording := &domain.Recording{FormatVersion: 1, ID: id, AdapterID: "opaque-adapter", Adapter: &domain.AdapterProvenance{ID: "opaque-adapter", Version: "2.1", ProtocolVersion: 1, Fingerprint: "abcd"}, Resource: &domain.ResourceReference{Type: "beta", ID: "opaque-id", Parent: &domain.ResourceReference{Type: "alpha", ID: "parent-id"}}, SourceURIClassification: "sensitive", State: domain.StateStopped, CreatedAt: time.Now().UTC(), StartedAt: time.Now().UTC(), Tracks: map[string]*domain.Track{"main": {ID: "main", SourcePlaylistURL: "https://media.example/live.m3u8?sig=opaque", Segments: []domain.Segment{{ID: "segment-1", TrackID: "main", SourceURI: uri, StoragePath: "tracks/main/segment.m4s", PayloadSize: 3, SHA256: "hash"}}}}}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	if err = store.SaveSidecar(id, "tracks/main/segment.m4s", recording.Tracks["main"].Segments[0]); err != nil {
		t.Fatal(err)
	}
	manifest := []byte("#EXTM3U\nsegment.m4s?token=opaque-value\n")
	snapshot, err := store.SaveSnapshot(id, "main", "https://media.example/live.m3u8?auth=opaque", manifest, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	storedManifest, err := store.OpenPayload(id, snapshot.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, len(manifest))
	_, err = storedManifest.Read(data)
	storedManifest.Close()
	if err != nil || !bytes.Equal(data, manifest) {
		t.Fatalf("raw manifest changed: %q %v", data, err)
	}
	for path, want := range map[string]os.FileMode{filepath.Join(root, "recordings"): 0700, filepath.Join(root, "recordings", id): 0700, filepath.Join(root, "recordings", id, "manifests"): 0700, filepath.Join(root, "recordings", id, "recording.json"): 0600, filepath.Join(root, "recordings", id, "tracks", "main", "segment.m4s.json"): 0600} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != want {
			t.Errorf("mode %s=%o want %o", path, info.Mode().Perm(), want)
		}
	}
	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d recordings", len(loaded))
	}
	got := loaded[0]
	if got.Adapter == nil || got.Adapter.ID != "opaque-adapter" || got.Adapter.Version != "2.1" || got.Adapter.ProtocolVersion != 1 || got.Adapter.Fingerprint != "abcd" {
		t.Fatalf("provenance did not reload: %#v", got.Adapter)
	}
	if got.Resource == nil || got.Resource.Type != "beta" || got.Resource.Parent == nil || got.Resource.Parent.Type != "alpha" {
		t.Fatalf("resource reference did not reload: %#v", got.Resource)
	}
	if got.SourceURIClassification != "sensitive" || got.Tracks["main"].Segments[0].SourceURI != uri || got.Tracks["main"].SourcePlaylistURL != "https://media.example/live.m3u8?sig=opaque" {
		t.Fatalf("canonical source URI was altered: %#v", got)
	}
}

func TestCreateRecordingPublishesDurablePrivateRoot(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	recording := stoppedRecording(id)
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(root, "recordings")
	for path, want := range map[string]os.FileMode{
		base:                    0700,
		filepath.Join(base, id): 0700,
		filepath.Join(base, id, "tracks", "main"): 0700,
		filepath.Join(base, id, "manifests"):      0700,
		filepath.Join(base, id, "recording.json"): 0600,
	} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("mode %s = %o, want %o", path, got, want)
		}
	}
	data, err := os.ReadFile(filepath.Join(base, id, "recording.json"))
	if err != nil {
		t.Fatal(err)
	}
	var loaded domain.Recording
	if err = json.Unmarshal(data, &loaded); err != nil || loaded.ID != id {
		t.Fatalf("published root metadata = %#v, err %v", loaded, err)
	}
	if entries, readErr := os.ReadDir(base); readErr != nil || len(entries) != 1 || entries[0].Name() != id {
		t.Fatalf("unexpected staging residue after publish: entries=%v err=%v", entries, readErr)
	}
}

func TestLoadAllPreservesAndReportsIncompleteDirectories(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const healthy = "11111111111111111111111111111111"
	const incomplete = "22222222222222222222222222222222"
	const missingRoot = "33333333333333333333333333333333"
	const corruptRoot = "44444444444444444444444444444444"
	if err = store.CreateRecording(stoppedRecording(healthy)); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(root, "recordings", ".incomplete-"+incomplete+"-test")
	if err = os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(stage, "preserve.bin"), []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = store.NewRecordingDir(missingRoot); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "recordings", missingRoot, "payload.bin"), []byte("also preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = store.NewRecordingDir(corruptRoot); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "recordings", corruptRoot, "recording.json"), []byte("{bad json"), 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || loaded[0].ID != healthy {
		t.Fatalf("healthy recording load=%#v err=%v", loaded, err)
	}
	if _, err = os.Stat(filepath.Join(stage, "preserve.bin")); err != nil {
		t.Fatalf("incomplete staging bytes were removed: %v", err)
	}
	if _, err = os.Stat(filepath.Join(root, "recordings", missingRoot, "payload.bin")); err != nil {
		t.Fatalf("orphan directory bytes were removed: %v", err)
	}
	issues := store.RecoveryIssues()
	if !hasIssue(issues, incomplete, "incomplete_creation") || !hasIssue(issues, missingRoot, "metadata_missing") || !hasIssue(issues, corruptRoot, "metadata_invalid") {
		t.Fatalf("recovery issues=%#v", issues)
	}
}

func TestLoadAllReconcilesCommittedSidecarAfterRootWriteCrash(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "44444444444444444444444444444444"
	if err = store.CreateRecording(stoppedRecording(id)); err != nil {
		t.Fatal(err)
	}
	payload := []byte{0, 1, 2, 3, 0xfe, 0xff}
	result, err := store.SavePayload(id, "tracks/main/0001.m4s", bytes.NewReader(payload), 100)
	if err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{ID: "segment-1", TrackID: "main", Sequence: 12, SourceEpoch: 2, ArchiveOrdinal: 1, SourceURI: "https://media.example/seg?sig=secret", StoragePath: "tracks/main/0001.m4s", PayloadSize: result.Size, SHA256: result.SHA256, Duration: 2.5}
	if err = store.SaveSidecar(id, segment.StoragePath, segment); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load=%#v err=%v", loaded, err)
	}
	got := loaded[0].Tracks["main"].Segments
	if len(got) != 1 || got[0].ID != segment.ID || got[0].SourceEpoch != 2 || got[0].ArchiveOrdinal != 1 || got[0].SourceURI != segment.SourceURI {
		t.Fatalf("recovered segment=%#v", got)
	}
	rootDoc, err := os.ReadFile(filepath.Join(store.recordingDir(id), "recording.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted domain.Recording
	if err = json.Unmarshal(rootDoc, &persisted); err != nil || len(persisted.Tracks["main"].Segments) != 1 {
		t.Fatalf("reconciled root metadata=%#v err=%v", persisted.Tracks["main"], err)
	}
	stored, err := store.OpenPayload(id, segment.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := io.ReadAll(stored)
	stored.Close()
	if err != nil || !bytes.Equal(gotBytes, payload) {
		t.Fatalf("payload changed: %v %v", gotBytes, err)
	}
	if result.SHA256 != digest(payload) {
		t.Fatalf("payload hash mismatch: %s", result.SHA256)
	}
}

func TestLoadAllReportsButDoesNotAttachMismatchedSidecarPayload(t *testing.T) {
	for _, mismatch := range []string{"hash", "size"} {
		t.Run(mismatch, func(t *testing.T) {
			store, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			id := "55555555555555555555555555555555"
			if mismatch == "size" {
				id = "99999999999999999999999999999999"
			}
			if err = store.CreateRecording(stoppedRecording(id)); err != nil {
				t.Fatal(err)
			}
			payload := []byte("preserve despite bad sidecar")
			if _, err = store.SavePayload(id, "tracks/main/bad.m4s", bytes.NewReader(payload), 100); err != nil {
				t.Fatal(err)
			}
			segment := domain.Segment{ID: "bad", TrackID: "main", Sequence: 1, StoragePath: "tracks/main/bad.m4s", PayloadSize: int64(len(payload)), SHA256: digest(payload)}
			if mismatch == "hash" {
				segment.SHA256 = strings.Repeat("0", sha256.Size*2)
			} else {
				segment.PayloadSize++
			}
			if err = store.SaveSidecar(id, segment.StoragePath, segment); err != nil {
				t.Fatal(err)
			}
			loaded, err := store.LoadAll()
			if err != nil || len(loaded) != 1 || len(loaded[0].Tracks["main"].Segments) != 0 {
				t.Fatalf("mismatched segment was attached: %#v err=%v", loaded, err)
			}
			if !hasIssue(store.RecoveryIssues(), id, "sidecar_payload_mismatch") {
				t.Fatalf("missing mismatch issue: %#v", store.RecoveryIssues())
			}
			stored, err := store.OpenPayload(id, segment.StoragePath)
			if err != nil {
				t.Fatal(err)
			}
			gotBytes, err := io.ReadAll(stored)
			stored.Close()
			if err != nil || !bytes.Equal(gotBytes, payload) {
				t.Fatalf("mismatched bytes were not preserved: %v %v", gotBytes, err)
			}
		})
	}
}

func TestLoadAllReportsOrphanPayloadAndPreservesBytes(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "66666666666666666666666666666666"
	if err = store.CreateRecording(stoppedRecording(id)); err != nil {
		t.Fatal(err)
	}
	want := []byte("uncommitted payload bytes")
	result, err := store.SavePayload(id, "tracks/main/orphan.ts", bytes.NewReader(want), 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.SHA256 != digest(want) {
		t.Fatalf("save hash=%s want=%s", result.SHA256, digest(want))
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load=%#v err=%v", loaded, err)
	}
	if !hasIssue(store.RecoveryIssues(), id, "orphan_payload") {
		t.Fatalf("missing orphan report: %#v", store.RecoveryIssues())
	}
	file, err := store.OpenPayload(id, "tracks/main/orphan.ts")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(file)
	file.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("orphan payload changed: %v %v", got, err)
	}
}

func TestLegacyWireShapedDomainTypesAndArchiveOrderingReload(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "77777777777777777777777777777777"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"format_version":1,"id":"` + id + `","adapter":{"id":"opaque","version":"1.2","protocol_version":1,"descriptor_fingerprint":"fp"},"resource":{"resource_type":"beta","resource_id":"b","parent":{"resource_type":"alpha","resource_id":"a"}},"state":"stopped","created_at":"2020-01-01T00:00:00Z","started_at":"2020-01-01T00:00:00Z","tracks":{"main":{"id":"main","source_playlist_url":"https://example.invalid/live.m3u8","source_epoch":3,"next_archive_ordinal":3,"segments":[{"id":"later","track_id":"main","sequence":1,"source_epoch":3,"archive_ordinal":2,"source_uri":"https://example.invalid/1","storage_path":"tracks/main/2.m4s","payload_size":1,"sha256":"aa"},{"id":"earlier","track_id":"main","sequence":900,"source_epoch":2,"archive_ordinal":1,"source_uri":"https://example.invalid/2","storage_path":"tracks/main/1.m4s","payload_size":1,"sha256":"bb"}]}}}`)
	if err = os.WriteFile(filepath.Join(store.recordingDir(id), "recording.json"), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load=%#v err=%v", loaded, err)
	}
	r := loaded[0]
	if r.Adapter == nil || r.Adapter.Fingerprint != "fp" || r.Resource == nil || r.Resource.Parent == nil || r.Resource.Parent.ID != "a" {
		t.Fatalf("legacy protocol JSON failed to load into stable domain types: %#v %#v", r.Adapter, r.Resource)
	}
	track := r.Tracks["main"]
	if track.SourceEpoch != 3 || track.NextArchiveOrdinal != 3 || len(track.Segments) != 2 || track.Segments[0].ArchiveOrdinal != 1 || track.Segments[1].ArchiveOrdinal != 2 {
		t.Fatalf("archive ordering/metadata lost: %#v", track)
	}

	const noProvenanceID = "88888888888888888888888888888888"
	if err = store.NewRecordingDir(noProvenanceID); err != nil {
		t.Fatal(err)
	}
	noProvenance := []byte(`{"format_version":1,"id":"` + noProvenanceID + `","state":"stopped","created_at":"2020-01-01T00:00:00Z","started_at":"2020-01-01T00:00:00Z","tracks":{}}`)
	if err = os.WriteFile(filepath.Join(store.recordingDir(noProvenanceID), "recording.json"), noProvenance, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.LoadAll()
	if err != nil || len(loaded) != 2 {
		t.Fatalf("legacy recording without provenance failed: %#v %v", loaded, err)
	}
	for _, got := range loaded {
		if got.ID == noProvenanceID && (got.Adapter != nil || got.Resource != nil) {
			t.Fatalf("unexpected legacy provenance/resource: %#v %#v", got.Adapter, got.Resource)
		}
	}
}

func TestSyncDirectoryOnCurrentFilesystem(t *testing.T) {
	if err := syncDirectory(t.TempDir()); err != nil {
		t.Fatalf("sync directory: %v", err)
	}
	if err := syncDirectory(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing directory should return a real I/O error")
	}
}

func stoppedRecording(id string) *domain.Recording {
	now := time.Now().UTC()
	return &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
}

func hasIssue(issues []RecoveryIssue, id, code string) bool {
	for _, issue := range issues {
		if issue.ID == id && issue.Code == code {
			return true
		}
	}
	return false
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestManifestSnapshotSidecarRecoversRootMetadataUpdate(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err = store.CreateRecording(stoppedRecording(id)); err != nil {
		t.Fatal(err)
	}
	manifest := []byte("#EXTM3U\n#EXTINF:1,segment\nsegment.m4s\n")
	snapshot, err := store.SaveSnapshot(id, "main", "https://media.example/live.m3u8?sig=private", manifest, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after payload and sidecar publication, before the root
	// recording document references the snapshot.
	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || len(loaded[0].Snapshots) != 1 {
		t.Fatalf("snapshot was not recovered: %#v", loaded)
	}
	got := loaded[0].Snapshots[0]
	if got.StoragePath != snapshot.StoragePath || got.SHA256 != snapshot.SHA256 || got.SourceURI != snapshot.SourceURI {
		t.Fatalf("recovered snapshot = %#v, want %#v", got, snapshot)
	}
	if issues := store.RecoveryIssues(); len(issues) != 0 {
		t.Fatalf("recovered snapshot reported issues: %#v", issues)
	}
	loadedAgain, err := store.LoadAll()
	if err != nil || len(loadedAgain) != 1 || len(loadedAgain[0].Snapshots) != 1 {
		t.Fatalf("reloaded snapshot = %#v, err=%v", loadedAgain, err)
	}
	info, err := os.Stat(filepath.Join(root, "recordings", id, filepath.FromSlash(snapshot.StoragePath)+".json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot sidecar permissions = %v, err=%v", info, err)
	}
}

func TestUnreferencedManifestPayloadIsReportedAndPreserved(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err = store.CreateRecording(stoppedRecording(id)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SavePayload(id, "manifests/orphan.m3u8", bytes.NewReader([]byte("#EXTM3U\n")), 1024); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("LoadAll = %d recordings, err=%v", len(loaded), err)
	}
	if !hasIssue(store.RecoveryIssues(), id, "orphan_manifest") {
		t.Fatalf("orphan manifest was not reported: %#v", store.RecoveryIssues())
	}
	file, err := store.OpenPayload(id, "manifests/orphan.m3u8")
	if err != nil {
		t.Fatalf("orphan manifest was not preserved: %v", err)
	}
	file.Close()
}

func TestLegacyRecordingWithoutProvenanceStillLoads(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	if err = store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"format_version":1,"id":"` + id + `","state":"stopped","created_at":"2020-01-01T00:00:00Z","started_at":"2020-01-01T00:00:00Z","tracks":{}}`)
	if err = os.WriteFile(filepath.Join(store.recordingDir(id), "recording.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || loaded[0].Adapter != nil {
		t.Fatalf("legacy recording load=%#v %v", loaded, err)
	}
}
