package storageprocess

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dltkddnr04/integrated-recorder/internal/domain"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/storagecatalog"
	"github.com/dltkddnr04/integrated-recorder/internal/storage"
	"github.com/dltkddnr04/integrated-recorder/internal/storageproto"
)

func TestOpenStoreRejectsGenerationWithoutProviderSetInsteadOfUsingDirectLocalStorage(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	if store, provider, err := OpenStore(context.Background(), dataDir, "", ""); err == nil || store != nil || provider != nil {
		t.Fatalf("OpenStore silently accepted an unpinned generation: store=%v provider=%v err=%v", store, provider, err)
	}
	if _, err := os.Lstat(filepath.Join(dataDir, "recordings")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected unpinned startup mutated/created the legacy archive root: %v", err)
	}
}

// TestExternalProviderArchiveRoundTrip is an acceptance-level process-boundary
// test: Core's archive facade talks to an immutable catalog executable over
// Storage Provider Protocol v1. The provider receives only logical object keys
// and streamed bytes; it does not construct or interpret Recording models.
func TestExternalProviderArchiveRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	testRoot := t.TempDir()
	// Catalog artifacts and immutable manifests are intentionally read-only.
	// Restore private directory write permission before testing.T removes its
	// temporary tree, so cleanup remains deterministic on Unix filesystems.
	t.Cleanup(func() {
		if err := makeTestTreeRemovable(testRoot); err != nil {
			t.Errorf("prepare temporary acceptance-test tree cleanup: %v", err)
		}
	})
	dataDir := filepath.Join(testRoot, "data")
	objectDir := filepath.Join(testRoot, "physical-objects")
	catalogRoot := filepath.Join(dataDir, "runtime", "storage-providers")

	binary := buildExternalFixtureProvider(t, ctx)
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatalf("read provider executable: %v", err)
	}
	binaryDigest := sha256.Sum256(binaryBytes)
	expected := storagecatalog.Expected{
		ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version,
		SHA256: hex.EncodeToString(binaryDigest[:]), Size: int64(len(binaryBytes)),
	}

	catalog, err := storagecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatalf("open Host-owned provider catalog: %v", err)
	}
	artifact, err := catalog.Import(ctx, binary, expected)
	if err != nil {
		t.Fatalf("import and Protocol-probe fixture provider: %v", err)
	}
	config := storagecatalog.SetConfig{Values: map[string]json.RawMessage{
		"root": json.RawMessage(mustJSON(t, objectDir)),
	}}
	set, err := catalog.CreateSet(artifact.Digest, config)
	if err != nil {
		t.Fatalf("create immutable provider set: %v", err)
	}
	if err := catalog.Install(artifact); err != nil {
		t.Fatalf("install provider artifact in catalog: %v", err)
	}
	if err := catalog.SelectDesiredSet(artifact.ID, set.ID); err != nil {
		t.Fatalf("select immutable provider set: %v", err)
	}

	var closeProvider func() error
	t.Cleanup(func() {
		if closeProvider != nil {
			if err := closeProvider(); err != nil {
				t.Errorf("close storage provider process during cleanup: %v", err)
			}
		}
	})

	openCtx, openCancel := context.WithTimeout(ctx, 20*time.Second)
	store, providerRuntime, err := OpenStore(openCtx, dataDir, catalogRoot, set.ID)
	openCancel()
	if err != nil {
		t.Fatalf("open Core archive facade over provider process: %v", err)
	}
	if providerRuntime == nil {
		t.Fatal("selected external provider did not start a separate runtime process")
	}
	closeProvider = providerRuntime.Close

	const recordingID = "0123456789abcdef0123456789abcdef"
	const payloadPath = "tracks/main/00000042.m4s"
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	title, description := "External provider fixture", "Core archive semantics over streamed object I/O"
	recording := &domain.Recording{
		FormatVersion: 1,
		ID:            recordingID,
		Title:         title,
		State:         domain.StateStopped,
		CreatedAt:     now,
		StartedAt:     now,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
		MetadataTimeline: []domain.MetadataRevision{{ObservedAt: now, Title: &title, Description: &description}},
	}
	if err := store.NewRecordingDir(recordingID); err != nil {
		t.Fatalf("validate Core recording identity: %v", err)
	}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatalf("create canonical recording metadata through Core: %v", err)
	}

	// This payload is deliberately larger than a control frame. SavePayloadExact
	// and the provider protocol stream it as bytes rather than JSON/base64.
	payload := bytes.Repeat([]byte("storage-provider-v1-streamed-segment/"), 8192)
	result, err := store.SavePayloadExact(recordingID, payloadPath, bytes.NewReader(payload), int64(len(payload)), int64(len(payload)))
	if err != nil {
		t.Fatalf("stream exact payload through Core archive facade: %v", err)
	}
	if result.Size != int64(len(payload)) || result.SHA256 != payloadSHA256(payload) {
		t.Fatalf("Core payload result = %+v, want size %d and exact SHA-256", result, len(payload))
	}

	segment := domain.Segment{
		ID: "segment-42", TrackID: "main", Sequence: 42, ArchiveOrdinal: 42,
		SourceURI: "fixture://source/session/segment-42", Duration: 4,
		StoragePath: payloadPath, PayloadSize: result.Size, SHA256: result.SHA256,
	}
	if err := store.SaveSidecar(recordingID, payloadPath, segment); err != nil {
		t.Fatalf("save Core-owned segment sidecar: %v", err)
	}
	recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, segment)
	if err := store.SaveRecording(recording); err != nil {
		t.Fatalf("publish canonical recording model through Core: %v", err)
	}

	physicalKey := "recordings/" + recordingID + "/" + payloadPath
	physicalPath := filepath.Join(objectDir, filepath.FromSlash(physicalKey))
	physical, err := os.ReadFile(physicalPath)
	if err != nil {
		t.Fatalf("fixture provider did not publish logical object %q: %v", physicalKey, err)
	}
	if !bytes.Equal(physical, payload) || payloadSHA256(physical) != result.SHA256 {
		t.Fatal("provider object bytes differ from original payload")
	}
	if filepath.IsAbs(physicalKey) || physicalKey != "recordings/"+recordingID+"/tracks/main/00000042.m4s" {
		t.Fatalf("Core did not use the expected logical object key: %q", physicalKey)
	}
	localRecordingPath := filepath.Join(dataDir, "recordings", recordingID)
	if _, err := os.Lstat(localRecordingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Core materialized a local canonical recording directory: %v", err)
	}

	if err := closeProvider(); err != nil {
		t.Fatalf("close first provider process: %v", err)
	}
	closeProvider = nil

	// Reopening uses a new child process from the same immutable set. Read-only
	// loading reconstructs the domain Recording from Core's logical objects.
	reopenCtx, reopenCancel := context.WithTimeout(ctx, 20*time.Second)
	reopened, reopenedRuntime, err := OpenStore(reopenCtx, dataDir, catalogRoot, set.ID)
	reopenCancel()
	if err != nil {
		t.Fatalf("reopen Core archive facade in a new provider process: %v", err)
	}
	if reopenedRuntime == nil {
		t.Fatal("reopened storage provider process is missing")
	}
	closeProvider = reopenedRuntime.Close

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()
	loaded, err := reopened.LoadAllReadOnly()
	if err != nil {
		t.Fatalf("read-only Core archive discovery after provider restart: %v", err)
	}
	if len(loaded) != 1 || loaded[0].ID != recordingID || loaded[0].SegmentCount() != 1 {
		t.Fatalf("Core reconstructed recordings = %+v; want one recording with one segment", loaded)
	}
	gotRecording := loaded[0]
	gotSegment := gotRecording.Tracks["main"].Segments[0]
	if gotRecording.Title != title || gotRecording.MetadataTimeline[0].Title == nil || *gotRecording.MetadataTimeline[0].Title != title || gotRecording.MetadataTimeline[0].Description == nil || *gotRecording.MetadataTimeline[0].Description != description {
		t.Fatalf("Core metadata model was not reconstructed: %+v", gotRecording)
	}
	if gotSegment.StoragePath != payloadPath || gotSegment.ArchiveOrdinal != 42 || gotSegment.SHA256 != result.SHA256 {
		t.Fatalf("Core segment model was not reconstructed: %+v", gotSegment)
	}

	reader, err := reopened.OpenPayloadReader(recordingID, payloadPath)
	if err != nil {
		t.Fatalf("open full payload from restarted provider: %v", err)
	}
	gotPayload, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(gotPayload, payload) || payloadSHA256(gotPayload) != result.SHA256 {
		t.Fatalf("full payload read mismatch: read=%v close=%v", readErr, closeErr)
	}

	stat, err := reopened.StatPayload(recordingID, payloadPath)
	if err != nil || stat.Size != int64(len(payload)) || !stat.Regular {
		t.Fatalf("Core StatPayload() = %+v, %v", stat, err)
	}
	const rangeOffset, rangeLength = int64(731), int64(257)
	rangeReader, err := reopened.OpenPayloadRangeReaderContext(readCtx, recordingID, payloadPath, rangeOffset, rangeLength)
	if err != nil {
		t.Fatalf("open streamed payload range after restart: %v", err)
	}
	rangePayload, rangeReadErr := io.ReadAll(rangeReader)
	rangeCloseErr := rangeReader.Close()
	if rangeReadErr != nil || rangeCloseErr != nil || !bytes.Equal(rangePayload, payload[rangeOffset:rangeOffset+rangeLength]) {
		t.Fatalf("payload range mismatch: read=%v close=%v length=%d", rangeReadErr, rangeCloseErr, len(rangePayload))
	}

	integrity := reopened.VerifyRecording(gotRecording)
	if integrity.Status != storage.IntegrityVerified || integrity.ObjectsVerified != 1 || integrity.ObjectsCorrupt != 0 || integrity.ObjectsMissing != 0 {
		t.Fatalf("Core integrity verification after restart = %+v", integrity)
	}
	if _, err := os.Lstat(localRecordingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read/recovery materialized a local canonical recording: %v", err)
	}
}

// TestBundledLocalProviderArchiveRoundTrip builds the production storage.local
// executable and exercises its real Protocol v1 process boundary. The
// provider is pointed at the legacy recordings root so this also proves
// in-place adoption without rewriting the archive layout.
func TestBundledLocalProviderArchiveRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	testRoot := t.TempDir()
	t.Cleanup(func() {
		if err := makeTestTreeRemovable(testRoot); err != nil {
			t.Errorf("prepare local-provider acceptance tree cleanup: %v", err)
		}
	})
	dataDir := filepath.Join(testRoot, "data")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	// Seed the archive through the previous Core-owned filesystem backend
	// before storage.local exists. The plugin must adopt these exact objects in
	// place rather than making an existing archive appear empty.
	legacyStore, err := storage.New(dataDir)
	if err != nil {
		t.Fatalf("create legacy archive fixture: %v", err)
	}
	const recordingID = "abcdef0123456789abcdef0123456789"
	const legacyPayloadKey = "tracks/main/00000006.m4s"
	const payloadPath = "tracks/main/00000007.m4s"
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	title, description := "Legacy local archive", "Adopted without moving canonical objects"
	legacyRecording := &domain.Recording{
		FormatVersion: 1, ID: recordingID, Title: title, State: domain.StateStopped,
		CreatedAt: now, StartedAt: now,
		Tracks:           map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}},
		MetadataTimeline: []domain.MetadataRevision{{ObservedAt: now, Title: &title, Description: &description}},
	}
	if err := legacyStore.CreateRecording(legacyRecording); err != nil {
		t.Fatalf("create legacy recording metadata: %v", err)
	}
	legacyPayload := bytes.Repeat([]byte("legacy-local-source-segment/"), 8192)
	legacyResult, err := legacyStore.SavePayloadExact(recordingID, legacyPayloadKey, bytes.NewReader(legacyPayload), int64(len(legacyPayload)), int64(len(legacyPayload)))
	if err != nil {
		t.Fatalf("write legacy archive payload: %v", err)
	}
	legacySegment := domain.Segment{ID: "segment-6", TrackID: "main", Sequence: 6, ArchiveOrdinal: 6,
		SourceURI: "fixture://source/session/segment-6", Duration: 4, StoragePath: legacyPayloadKey,
		PayloadSize: legacyResult.Size, SHA256: legacyResult.SHA256}
	if err := legacyStore.SaveSidecar(recordingID, legacyPayloadKey, legacySegment); err != nil {
		t.Fatalf("write legacy archive sidecar: %v", err)
	}
	legacyRecording.Tracks["main"].Segments = append(legacyRecording.Tracks["main"].Segments, legacySegment)
	if err := legacyStore.SaveRecording(legacyRecording); err != nil {
		t.Fatalf("publish legacy recording metadata: %v", err)
	}
	legacyPhysicalPath := filepath.Join(dataDir, "recordings", recordingID, filepath.FromSlash(legacyPayloadKey))
	legacyPhysical, err := os.ReadFile(legacyPhysicalPath)
	if err != nil || !bytes.Equal(legacyPhysical, legacyPayload) || payloadSHA256(legacyPhysical) != legacyResult.SHA256 {
		t.Fatalf("legacy fixture bytes are not stable: size=%d err=%v", len(legacyPhysical), err)
	}
	binary := buildStorageLocalProvider(t, ctx)
	catalogRoot := filepath.Join(dataDir, "runtime", "storage-providers")
	catalog, err := storagecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.ImportBundled(ctx, binary, "local")
	if err != nil {
		t.Fatalf("import and Protocol-probe production storage.local: %v", err)
	}
	if err := catalog.Install(artifact); err != nil {
		t.Fatal(err)
	}
	archiveRoot := filepath.Join(dataDir, "recordings")
	rootJSON, err := json.Marshal(archiveRoot)
	if err != nil {
		t.Fatal(err)
	}
	set, err := catalog.CreateSet(artifact.Digest, storagecatalog.SetConfig{Values: map[string]json.RawMessage{"root": rootJSON}})
	if err != nil {
		t.Fatalf("create Host-controlled local provider set: %v", err)
	}
	if err := catalog.SelectDesiredSet("local", set.ID); err != nil {
		t.Fatal(err)
	}

	openStore := func() (*storage.Store, *Runtime) {
		t.Helper()
		openCtx, openCancel := context.WithTimeout(ctx, 20*time.Second)
		defer openCancel()
		store, runtime, openErr := OpenStore(openCtx, dataDir, catalogRoot, set.ID)
		if openErr != nil {
			t.Fatalf("open Core archive over production storage.local process: %v", openErr)
		}
		return store, runtime
	}

	store, provider := openStore()
	adopted, err := store.LoadAllReadOnly()
	if err != nil || len(adopted) != 1 || adopted[0].ID != recordingID || adopted[0].SegmentCount() != 1 || adopted[0].Title != title {
		t.Fatalf("storage.local failed to adopt existing legacy archive: recordings=%+v err=%v", adopted, err)
	}
	if segment := adopted[0].Tracks["main"].Segments[0]; segment.Sequence != 6 || segment.SHA256 != legacyResult.SHA256 || segment.StoragePath != legacyPayloadKey {
		t.Fatalf("legacy archive metadata changed during adoption: %+v", segment)
	}
	payload := bytes.Repeat([]byte("storage.local-streamed-source-segment/"), 32<<10)
	result, err := store.SavePayloadExact(recordingID, payloadPath, bytes.NewReader(payload), int64(len(payload)), int64(len(payload)))
	if err != nil {
		t.Fatalf("stream exact payload through storage.local: %v", err)
	}
	if result.Size != int64(len(payload)) || result.SHA256 != payloadSHA256(payload) {
		t.Fatalf("local provider result=%+v, want size=%d exact SHA-256", result, len(payload))
	}
	recording := adopted[0]
	segment := domain.Segment{ID: "segment-7", TrackID: "main", Sequence: 7, ArchiveOrdinal: 7,
		SourceURI: "fixture://source/session/segment-7", Duration: 4, StoragePath: payloadPath,
		PayloadSize: result.Size, SHA256: result.SHA256}
	if err := store.SaveSidecar(recordingID, payloadPath, segment); err != nil {
		t.Fatal(err)
	}
	recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, segment)
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	physicalPath := filepath.Join(dataDir, "recordings", recordingID, filepath.FromSlash(payloadPath))
	physical, err := os.ReadFile(physicalPath)
	if err != nil || !bytes.Equal(physical, payload) || payloadSHA256(physical) != result.SHA256 {
		t.Fatalf("storage.local did not preserve exact bytes at legacy archive path: size=%d err=%v", len(physical), err)
	}
	legacyPhysical, err = os.ReadFile(legacyPhysicalPath)
	if err != nil || !bytes.Equal(legacyPhysical, legacyPayload) || payloadSHA256(legacyPhysical) != legacyResult.SHA256 {
		t.Fatalf("storage.local adoption rewrote a previous canonical object: size=%d err=%v", len(legacyPhysical), err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("close production local provider process: %v", err)
	}

	store, provider = openStore()
	defer func() {
		if err := provider.Close(); err != nil {
			t.Errorf("close restarted local provider process: %v", err)
		}
	}()
	loaded, err := store.LoadAllReadOnly()
	if err != nil || len(loaded) != 1 || loaded[0].ID != recordingID || loaded[0].SegmentCount() != 2 || loaded[0].Title != title {
		t.Fatalf("legacy recording recovery through storage.local=%+v err=%v", loaded, err)
	}
	if segments := loaded[0].Tracks["main"].Segments; len(segments) != 2 || segments[0].SHA256 != legacyResult.SHA256 || segments[1].SHA256 != result.SHA256 {
		t.Fatalf("legacy/current payload hash metadata changed: %+v", segments)
	}
	if integrity := store.VerifyRecording(loaded[0]); integrity.Status != storage.IntegrityVerified || integrity.ObjectsVerified != 2 || integrity.ObjectsMissing != 0 || integrity.ObjectsCorrupt != 0 {
		t.Fatalf("local provider integrity verification=%+v", integrity)
	}
	reader, err := store.OpenPayloadRangeReaderContext(ctx, recordingID, payloadPath, 1024, 4096)
	if err != nil {
		t.Fatalf("open range through restarted local provider: %v", err)
	}
	ranged, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(ranged, payload[1024:1024+4096]) {
		t.Fatalf("local provider range mismatch: read=%v close=%v size=%d", readErr, closeErr, len(ranged))
	}
	if err := store.DeleteRecordingData(recordingID); err != nil {
		t.Fatalf("delete canonical recording through provider protocol: %v", err)
	}
	if _, err := os.Lstat(physicalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider-backed deletion left canonical payload object: %v", err)
	}
	if _, err := os.Lstat(legacyPhysicalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider-backed deletion left adopted legacy payload object: %v", err)
	}
	remaining, err := store.LoadAllReadOnly()
	if err != nil || len(remaining) != 0 {
		t.Fatalf("provider-backed deletion left canonical recordings: count=%d err=%v", len(remaining), err)
	}
}

func TestExternalProviderChildCrashRecoversThroughCoreOutcomeVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	binary := buildExternalFixtureProvider(t, ctx)
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatalf("read fixture executable: %v", err)
	}
	digest := sha256.Sum256(binaryBytes)
	expected := storagecatalog.Expected{
		ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version,
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(binaryBytes)),
	}

	for _, phase := range []string{"before_first_byte", "mid_stream", "after_atomic_publish_before_response"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			t.Cleanup(func() {
				if err := makeTestTreeRemovable(root); err != nil {
					t.Errorf("prepare crash-test cleanup: %v", err)
				}
			})
			dataDir := filepath.Join(root, "data")
			catalogRoot := filepath.Join(dataDir, "runtime", "storage-providers")
			physicalRoot := filepath.Join(root, "physical-objects")
			const recordingID = "abcdef0123456789abcdef0123456789"
			const relativePayloadPath = "tracks/main/00000001.m4s"
			const logicalKey = "recordings/" + recordingID + "/" + relativePayloadPath
			crashMarker := filepath.Join(root, "crash-fired")
			launchMarker := filepath.Join(root, "process-launches")
			attemptMarker := filepath.Join(root, "put-attempts")

			catalog, err := storagecatalog.Open(catalogRoot)
			if err != nil {
				t.Fatalf("open test provider catalog: %v", err)
			}
			artifact, err := catalog.Import(ctx, binary, expected)
			if err != nil {
				t.Fatalf("import fixture executable into immutable catalog: %v", err)
			}
			config := storagecatalog.SetConfig{Values: map[string]json.RawMessage{
				"root":           json.RawMessage(mustJSON(t, physicalRoot)),
				"crash_key":      json.RawMessage(mustJSON(t, logicalKey)),
				"crash_phase":    json.RawMessage(mustJSON(t, phase)),
				"crash_marker":   json.RawMessage(mustJSON(t, crashMarker)),
				"launch_marker":  json.RawMessage(mustJSON(t, launchMarker)),
				"attempt_marker": json.RawMessage(mustJSON(t, attemptMarker)),
			}}
			set, err := catalog.CreateSet(artifact.Digest, config)
			if err != nil {
				t.Fatalf("create immutable fixture provider set: %v", err)
			}
			if err := catalog.Install(artifact); err != nil {
				t.Fatalf("install fixture artifact: %v", err)
			}
			if err := catalog.SelectDesiredSet(artifact.ID, set.ID); err != nil {
				t.Fatalf("select fixture provider set: %v", err)
			}

			store, runtime, err := OpenStore(ctx, dataDir, catalogRoot, set.ID)
			if err != nil {
				t.Fatalf("open store on external provider process: %v", err)
			}
			if runtime == nil {
				t.Fatal("expected a process-backed provider runtime")
			}
			archive, ok := store.StorageBackend.(*storage.ObjectStoreArchiveBackend)
			if !ok {
				t.Fatal("Core did not use its object-store archive implementation")
			}
			t.Cleanup(func() {
				if err := runtime.Close(); err != nil {
					t.Errorf("close process-backed provider runtime: %v", err)
				}
			})

			now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
			title := "Crash recovery fixture"
			recording := &domain.Recording{
				FormatVersion: 1, ID: recordingID, Title: title, State: domain.StateRecording,
				CreatedAt: now, StartedAt: now,
				Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}},
			}
			if err := store.NewRecordingDir(recordingID); err != nil {
				t.Fatalf("validate canonical Recording ID: %v", err)
			}
			if err := store.CreateRecording(recording); err != nil {
				t.Fatalf("create canonical Recording before failpoint object: %v", err)
			}

			payload := bytes.Repeat([]byte("provider-child-crash-stream/"), 32768)
			writeCtx, writeCancel := context.WithTimeout(ctx, 45*time.Second)
			result, writeErr := archive.SavePayloadExactContext(writeCtx, recordingID, relativePayloadPath, bytes.NewReader(payload), int64(len(payload)), int64(len(payload)))
			writeCancel()
			if phase == "after_atomic_publish_before_response" {
				if writeErr != nil {
					t.Fatalf("Core did not resolve the exact committed bytes after ambiguous PUT: %v", writeErr)
				}
				if result.Size != int64(len(payload)) || result.SHA256 != payloadSHA256(payload) {
					t.Fatalf("ambiguous PUT result = %+v; want exact staged size and digest", result)
				}
			} else {
				if writeErr == nil {
					t.Fatal("Core accepted an object that was absent after the provider crashed before atomic publication")
				}
				physicalPath := filepath.Join(physicalRoot, filepath.FromSlash(logicalKey))
				if _, err := os.Lstat(physicalPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial bytes were exposed under the canonical object key: %v", err)
				}
				retryCtx, retryCancel := context.WithTimeout(ctx, 45*time.Second)
				result, err = archive.SavePayloadExactContext(retryCtx, recordingID, relativePayloadPath, bytes.NewReader(payload), int64(len(payload)), int64(len(payload)))
				retryCancel()
				if err != nil {
					t.Fatalf("Core retry did not publish complete object through restarted provider: %v", err)
				}
			}
			if result.Size != int64(len(payload)) || result.SHA256 != payloadSHA256(payload) {
				t.Fatalf("final payload result = %+v; want exact size and digest", result)
			}

			crashSignal, err := os.ReadFile(crashMarker)
			if err != nil {
				t.Fatalf("one-shot crash marker was not written: %v", err)
			}
			launched, err := os.ReadFile(launchMarker)
			if err != nil {
				t.Fatalf("provider launch marker was not written: %v", err)
			}
			launches := nonemptyLines(string(launched))
			if len(launches) != 2 {
				t.Fatalf("provider launch PIDs = %q; want initial child plus exactly one replacement", launches)
			}
			if string(crashSignal) != launches[0]+"\n" || launches[0] == launches[1] {
				t.Fatalf("crashing child marker %q and launch PIDs %q do not prove a replacement child", crashSignal, launches)
			}
			attempts, err := os.ReadFile(attemptMarker)
			if err != nil {
				t.Fatalf("provider did not record target PUT attempts: %v", err)
			}
			wantAttempts := 2
			if phase == "after_atomic_publish_before_response" {
				wantAttempts = 1 // Core accepts only after exact Stat/Open confirmation.
			}
			attemptLines := nonemptyLines(string(attempts))
			if len(attemptLines) != wantAttempts {
				t.Fatalf("target PUT attempts = %q; want %d (no blind replay after ambiguous completion)", attemptLines, wantAttempts)
			}
			for _, line := range attemptLines {
				if !strings.HasSuffix(line, " "+logicalKey) {
					t.Fatalf("fixture observed an unexpected canonical key in PUT attempts: %q", line)
				}
			}

			physicalPath := filepath.Join(physicalRoot, filepath.FromSlash(logicalKey))
			physical, err := os.ReadFile(physicalPath)
			if err != nil {
				t.Fatalf("final complete canonical object is missing: %v", err)
			}
			if !bytes.Equal(physical, payload) || payloadSHA256(physical) != result.SHA256 {
				t.Fatal("final physical object was partial or differed from the exact source payload")
			}

			segment := domain.Segment{
				ID: "segment-1", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1,
				SourceURI: "fixture://source/session/segment-1", Duration: 4,
				StoragePath: relativePayloadPath, PayloadSize: result.Size, SHA256: result.SHA256,
			}
			if err := store.SaveSidecar(recordingID, relativePayloadPath, segment); err != nil {
				t.Fatalf("save one canonical segment sidecar: %v", err)
			}
			recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, segment)
			if err := store.SaveRecording(recording); err != nil {
				t.Fatalf("save canonical Recording model: %v", err)
			}
			loaded, err := store.LoadAllReadOnly()
			if err != nil || len(loaded) != 1 || loaded[0].ID != recordingID || loaded[0].SegmentCount() != 1 || loaded[0].Tracks["main"].Segments[0].ArchiveOrdinal != 1 {
				t.Fatalf("canonical Recording after provider crash recovery = %+v, %v; want one Recording and one ordinal", loaded, err)
			}
			if _, err := os.Lstat(filepath.Join(dataDir, "recordings", recordingID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Core created a local canonical archive during provider recovery: %v", err)
			}

			// An outstanding streamed read must not hold Runtime's state lock and
			// prevent Close from canceling and terminating the exact child.
			stream, _, err := runtime.ObjectStore().Open(ctx, logicalKey)
			if err != nil {
				t.Fatalf("open stream before close-cancellation assertion: %v", err)
			}
			blockedCall := make(chan error, 1)
			go func() {
				_, err := runtime.ObjectStore().Stat(context.Background(), logicalKey)
				blockedCall <- err
			}()
			closeDone := make(chan error, 1)
			go func() { closeDone <- runtime.Close() }()
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("Runtime.Close failed while canceling streamed provider I/O: %v", err)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("Runtime.Close waited on an in-flight provider operation instead of terminating the child")
			}
			select {
			case err := <-blockedCall:
				if !errors.Is(err, storageproto.ErrClosed) && !errors.Is(err, context.Canceled) {
					t.Fatalf("operation queued behind streamed read after Close returned %v; want closed/canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("queued provider operation did not observe Runtime.Close cancellation")
			}
			_ = stream.Close()
		})
	}
}

func TestReadStreamsAreConcurrentBoundedAndReleasePermits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	runtime, key, payload := startFixtureRuntime(t, ctx)
	objects := runtime.ObjectStore()

	// Keep several response bodies open without consuming them. Every Open call
	// must return its headers while prior bodies still own read permits.
	const concurrent = 4
	type result struct {
		reader io.ReadCloser
		want   []byte
		err    error
	}
	opened := make(chan result, concurrent)
	for index := 0; index < concurrent; index++ {
		index := index
		go func() {
			var reader io.ReadCloser
			var err error
			want := payload
			if index%2 == 0 {
				reader, _, err = objects.Open(ctx, key)
			} else {
				reader, _, err = objects.OpenRange(ctx, key, 128, 4096)
				want = payload[128 : 128+4096]
			}
			opened <- result{reader: reader, want: want, err: err}
		}()
	}
	streams := make([]result, 0, concurrent)
	for index := 0; index < concurrent; index++ {
		select {
		case result := <-opened:
			if result.err != nil {
				t.Fatalf("concurrent stream open %d: %v", index, result.err)
			}
			streams = append(streams, result)
		case <-time.After(20 * time.Second):
			t.Fatalf("only %d of %d response bodies opened while prior streams remained held", len(streams), concurrent)
		}
	}
	if got := len(runtime.readGate); got != concurrent {
		t.Fatalf("active read permits=%d, want %d", got, concurrent)
	}
	for index, stream := range streams {
		got, err := io.ReadAll(stream.reader)
		if err != nil {
			t.Fatalf("consume held stream %d: %v", index, err)
		}
		if !bytes.Equal(got, stream.want) {
			t.Fatalf("stream %d returned %d bytes, want %d", index, len(got), len(stream.want))
		}
		if gotPermits := len(runtime.readGate); gotPermits != concurrent-index-1 {
			t.Fatalf("EOF did not release exactly one read permit: after stream %d got %d", index, gotPermits)
		}
		if err := stream.reader.Close(); err != nil {
			t.Fatalf("close consumed stream %d: %v", index, err)
		}
		if gotPermits := len(runtime.readGate); gotPermits != concurrent-index-1 {
			t.Fatalf("Close released a permit more than once after EOF: got %d", gotPermits)
		}
	}

	// Fill the entire bounded stream budget, then prove a seventeenth request
	// reaches the semaphore wait and is canceled without acquiring/leaking one.
	held := make([]io.ReadCloser, 0, maxConcurrentReadStreams)
	for index := 0; index < maxConcurrentReadStreams; index++ {
		reader, _, err := objects.Open(ctx, key)
		if err != nil {
			t.Fatalf("open bounded stream %d: %v", index, err)
		}
		held = append(held, reader)
	}
	if got := len(runtime.readGate); got != maxConcurrentReadStreams {
		t.Fatalf("read gate occupancy=%d, want limit %d", got, maxConcurrentReadStreams)
	}
	waitCtx, cancelWait := context.WithTimeout(ctx, time.Second)
	waitResult := make(chan error, 1)
	waitStarted := make(chan struct{})
	go func() {
		close(waitStarted)
		reader, _, err := objects.Open(waitCtx, key)
		if reader != nil {
			_ = reader.Close()
		}
		waitResult <- err
	}()
	<-waitStarted
	select {
	case err := <-waitResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("seventeenth Open wait error=%v, want context deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("seventeenth Open did not leave the read permit wait after cancellation")
	}
	cancelWait()
	if got := len(runtime.readGate); got != maxConcurrentReadStreams {
		t.Fatalf("canceled waiter changed held permit count to %d", got)
	}
	for _, reader := range held {
		if err := reader.Close(); err != nil {
			t.Errorf("close held stream: %v", err)
		}
	}
	if got := len(runtime.readGate); got != 0 {
		t.Fatalf("Close leaked read permits: %d remain", got)
	}

	// Parent-context cancellation aborts a live response and frees its permit
	// even if the caller has not yet read or closed the body.
	streamCtx, cancelStream := context.WithCancel(ctx)
	stream, _, err := objects.Open(streamCtx, key)
	if err != nil {
		t.Fatalf("open stream for cancellation: %v", err)
	}
	cancelStream()
	waitCtx, cancelWait = context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	for index := 0; index < maxConcurrentReadStreams; index++ {
		if err := runtime.acquireReadStream(waitCtx); err != nil {
			for released := 0; released < index; released++ {
				runtime.releaseReadStream()
			}
			_ = stream.Close()
			t.Fatalf("caller cancellation did not release stream permit %d: %v", index+1, err)
		}
	}
	for index := 0; index < maxConcurrentReadStreams; index++ {
		runtime.releaseReadStream()
	}
	_ = stream.Close()
	if got := len(runtime.readGate); got != 0 {
		t.Fatalf("cancellation followed by Close leaked/double-released permits: %d remain", got)
	}

	// Runtime lifetime cancellation also closes an outstanding body and returns
	// its permit without requiring the owner to call Close on that body.
	lifetimeStream, _, err := objects.Open(ctx, key)
	if err != nil {
		t.Fatalf("open stream for runtime shutdown: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("close runtime with open response stream: %v", err)
	}
	if got := len(runtime.readGate); got != 0 {
		t.Fatalf("runtime lifetime cancellation leaked an open read permit: %d remain", got)
	}
	if err := lifetimeStream.Close(); err != nil {
		t.Fatalf("close body after runtime shutdown: %v", err)
	}
	if got := len(runtime.readGate); got != 0 {
		t.Fatalf("body Close after runtime cancellation released a permit twice: %d remain", got)
	}
}

func TestNonReadOperationsWaitForOpenReadStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime, key, _ := startFixtureRuntime(t, ctx)
	objects := runtime.ObjectStore()
	reader, _, err := objects.Open(ctx, key)
	if err != nil {
		t.Fatalf("open stream before Stat: %v", err)
	}
	defer reader.Close()

	waitCtx, cancelWait := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = objects.Stat(waitCtx, key)
	cancelWait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stat overlapped an active object stream or returned the wrong error: %v", err)
	}
	if got := len(runtime.readGate); got != 1 {
		t.Fatalf("timed-out exclusive Stat disturbed active read stream permits: %d", got)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("close read stream before retrying Stat: %v", err)
	}
	if _, err := objects.Stat(ctx, key); err != nil {
		t.Fatalf("Stat did not proceed after read stream closed: %v", err)
	}
}

func TestPutOperationsRemainSerializedWhileReadsCanStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	markerPath := filepath.Join(t.TempDir(), "provider-put-attempts")
	runtime, _, _ := startFixtureRuntimeWithConfig(t, ctx, map[string]json.RawMessage{
		"crash_key":      mustJSON(t, "recordings/serial/second.bin"),
		"attempt_marker": mustJSON(t, markerPath),
	})
	objects := runtime.ObjectStore()
	firstBody := &blockingBody{started: make(chan struct{}), release: make(chan struct{})}
	firstResult := make(chan error, 1)
	go func() {
		_, err := objects.Put(ctx, "recordings/serial/first.bin", firstBody, 1)
		firstResult <- err
	}()
	select {
	case <-firstBody.started:
	case <-time.After(15 * time.Second):
		t.Fatal("first PUT did not enter the provider body stream")
	}
	if got := len(runtime.opGate); got != 1 {
		t.Fatalf("first PUT does not hold the existing exclusive operation gate: %d", got)
	}

	var secondReads atomic.Int32
	secondBody := &observedBody{reads: &secondReads}
	secondResult := make(chan error, 1)
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		_, err := objects.Put(ctx, "recordings/serial/second.bin", secondBody, 1)
		secondResult <- err
	}()
	<-secondStarted
	if got := secondReads.Load(); got != 0 {
		t.Fatalf("second PUT body was consumed before the first serialized PUT completed: reads=%d", got)
	}
	close(firstBody.release)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first serialized PUT: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("first serialized PUT did not complete")
	}
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("second serialized PUT: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("second serialized PUT did not run after the first completed")
	}
	if got := secondReads.Load(); got == 0 {
		t.Fatal("second PUT body was never consumed")
	}
}

type blockingBody struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	sent    bool
}

func (r *blockingBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	r.once.Do(func() { close(r.started) })
	<-r.release
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	buffer[0] = 'x'
	return 1, nil
}

type observedBody struct {
	reads *atomic.Int32
	sent  bool
}

func (r *observedBody) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	r.reads.Add(1)
	if r.sent {
		return 0, io.EOF
	}
	r.sent = true
	buffer[0] = 'y'
	return 1, nil
}

func startFixtureRuntime(t *testing.T, ctx context.Context) (*Runtime, string, []byte) {
	return startFixtureRuntimeWithConfig(t, ctx, nil)
}

func startFixtureRuntimeWithConfig(t *testing.T, ctx context.Context, extraValues map[string]json.RawMessage) (*Runtime, string, []byte) {
	t.Helper()
	testRoot := t.TempDir()
	t.Cleanup(func() {
		if err := makeTestTreeRemovable(testRoot); err != nil {
			t.Errorf("prepare fixture runtime tree cleanup: %v", err)
		}
	})
	binary := buildExternalFixtureProvider(t, ctx)
	binaryBytes, err := os.ReadFile(binary)
	if err != nil {
		t.Fatalf("read provider executable: %v", err)
	}
	digest := sha256.Sum256(binaryBytes)
	catalogRoot := filepath.Join(testRoot, "runtime", "storage-providers")
	catalog, err := storagecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Import(ctx, binary, storagecatalog.Expected{
		ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version,
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(binaryBytes)),
	})
	if err != nil {
		t.Fatalf("import real child provider: %v", err)
	}
	artifactPath, err := catalog.ArtifactPath(artifact.Digest)
	if err != nil {
		t.Fatal(err)
	}
	_, descriptor, err := catalog.DescribeArtifact(artifact.Digest)
	if err != nil {
		t.Fatal(err)
	}
	objectRoot := filepath.Join(testRoot, "objects")
	values := map[string]json.RawMessage{"root": mustJSON(t, objectRoot)}
	for key, value := range extraValues {
		values[key] = append(json.RawMessage(nil), value...)
	}
	config := storageproto.Config{Values: values}
	provider, err := Start(ctx, artifactPath, descriptor, config)
	if err != nil {
		t.Fatalf("start real child provider: %v", err)
	}
	t.Cleanup(func() {
		if err := provider.Close(); err != nil {
			t.Errorf("close real child provider: %v", err)
		}
	})
	key := "recordings/concurrent/stream.bin"
	payload := bytes.Repeat([]byte("concurrent-read-stream/"), 128<<10)
	if _, err := provider.ObjectStore().Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("seed provider object for concurrent streams: %v", err)
	}
	return provider, key, payload
}

func nonemptyLines(value string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(value), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func buildExternalFixtureProvider(t *testing.T, ctx context.Context) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "integrated-recorder-storage-fixture")
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, "../storageproto/testdata/provider")
	command.Stdout = &boundedTestOutput{}
	output := command.Stdout.(*boundedTestOutput)
	command.Stderr = output
	if err := command.Run(); err != nil {
		t.Fatalf("build external storage provider fixture: %v (%s)", err, output.String())
	}
	return binary
}

func buildStorageLocalProvider(t *testing.T, ctx context.Context) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "integrated-recorder-storage-local")
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, "../../cmd/storage-local")
	command.Stdout = &boundedTestOutput{}
	output := command.Stdout.(*boundedTestOutput)
	command.Stderr = output
	if err := command.Run(); err != nil {
		t.Fatalf("build production storage.local executable: %v (%s)", err, output.String())
	}
	return binary
}

type boundedTestOutput struct {
	data []byte
}

func (b *boundedTestOutput) Write(p []byte) (int, error) {
	const max = 64 << 10
	written := len(p)
	remaining := max - len(b.data)
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		b.data = append(b.data, p...)
	}
	return written, nil
}

func (b *boundedTestOutput) String() string {
	return string(b.data)
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func payloadSHA256(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func makeTestTreeRemovable(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return os.Chmod(path, 0700)
		}
		return nil
	})
}
