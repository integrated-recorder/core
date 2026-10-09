package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

type memoryPhysicalObjects struct {
	mu                 sync.Mutex
	objects            map[string][]byte
	modified           map[string]time.Time
	putKeys            []string
	listCalls          int
	statCalls          int
	openCalls          int
	maxPutRead         int
	failPutBefore      bool
	failPutAfterCommit bool
	wrongPutHash       bool
	failDeleteAfter    int
	deleteCalls        int
	blockPut           bool
	rangeCalls         []physicalRangeCall
}

type identifiedMemoryPhysicalObjects struct {
	*memoryPhysicalObjects
	id   string
	name string
}

type blockingListPhysicalObjects struct {
	*memoryPhysicalObjects
	listStarted chan context.Context
	releaseList chan struct{}
}

func (m *blockingListPhysicalObjects) List(ctx context.Context, prefix, cursor string, limit int) (PhysicalObjectPage, error) {
	m.listStarted <- ctx
	select {
	case <-ctx.Done():
		return PhysicalObjectPage{}, ctx.Err()
	case <-m.releaseList:
		return PhysicalObjectPage{}, errors.New("test list released")
	}
}

type recordingDirectoryRequestContextKey struct{}

func (m identifiedMemoryPhysicalObjects) StorageProviderIdentity() (string, string) {
	return m.id, m.name
}

type physicalRangeCall struct {
	key    string
	offset int64
	length int64
}

func newMemoryPhysicalObjects() *memoryPhysicalObjects {
	return &memoryPhysicalObjects{objects: map[string][]byte{}, modified: map[string]time.Time{}}
}

func (m *memoryPhysicalObjects) Put(ctx context.Context, key string, body io.Reader, size int64) (PhysicalObjectInfo, error) {
	if err := ValidateObjectKey(key); err != nil {
		return PhysicalObjectInfo{}, err
	}
	if size < 0 || size > MaxObjectBytes {
		return PhysicalObjectInfo{}, errors.New("invalid size")
	}
	buffer := make([]byte, 32<<10)
	var data bytes.Buffer
	h := sha256.New()
	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return PhysicalObjectInfo{}, err
		}
		n, err := body.Read(buffer)
		if n > 0 {
			if n > m.maxPutRead {
				m.maxPutRead = n
			}
			count += int64(n)
			if count > size {
				return PhysicalObjectInfo{}, errors.New("too many bytes")
			}
			_, _ = data.Write(buffer[:n])
			_, _ = h.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return PhysicalObjectInfo{}, err
		}
	}
	if count != size {
		return PhysicalObjectInfo{}, errors.New("short body")
	}
	m.mu.Lock()
	m.putKeys = append(m.putKeys, key)
	if m.blockPut {
		m.mu.Unlock()
		<-ctx.Done()
		return PhysicalObjectInfo{}, ctx.Err()
	}
	if m.failPutBefore {
		m.failPutBefore = false
		m.mu.Unlock()
		return PhysicalObjectInfo{}, errors.New("injected pre-publish failure")
	}
	value := append([]byte(nil), data.Bytes()...)
	m.objects[key] = value
	m.modified[key] = time.Now().UTC()
	info := m.infoLocked(key)
	if m.wrongPutHash {
		info.SHA256 = strings.Repeat("0", sha256.Size*2)
		m.wrongPutHash = false
	}
	failAfter := m.failPutAfterCommit
	m.failPutAfterCommit = false
	m.mu.Unlock()
	if failAfter {
		return PhysicalObjectInfo{}, errors.New("injected response loss after commit")
	}
	return info, nil
}

func (m *memoryPhysicalObjects) Open(ctx context.Context, key string) (io.ReadCloser, PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, PhysicalObjectInfo{}, err
	}
	if err := ValidateObjectKey(key); err != nil {
		return nil, PhysicalObjectInfo{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openCalls++
	value, ok := m.objects[key]
	if !ok {
		return nil, PhysicalObjectInfo{}, ErrObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), value...))), m.infoLocked(key), nil
}

func (m *memoryPhysicalObjects) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, PhysicalObjectInfo{}, err
	}
	if err := ValidateObjectKey(key); err != nil {
		return nil, PhysicalObjectInfo{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.objects[key]
	if !ok {
		return nil, PhysicalObjectInfo{}, ErrObjectNotFound
	}
	if offset < 0 || length <= 0 || offset > int64(len(value))-length {
		return nil, PhysicalObjectInfo{}, errors.New("invalid range")
	}
	m.rangeCalls = append(m.rangeCalls, physicalRangeCall{key: key, offset: offset, length: length})
	return io.NopCloser(bytes.NewReader(append([]byte(nil), value[offset:offset+length]...))), m.infoLocked(key), nil
}

func (m *memoryPhysicalObjects) Stat(ctx context.Context, key string) (PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return PhysicalObjectInfo{}, err
	}
	if err := ValidateObjectKey(key); err != nil {
		return PhysicalObjectInfo{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statCalls++
	if _, ok := m.objects[key]; !ok {
		return PhysicalObjectInfo{}, ErrObjectNotFound
	}
	return m.infoLocked(key), nil
}

func TestLoadSidecarUsesOneOpenAndValidatesReturnedDigest(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	id := "0123456789abcdef0123456789abcdef"
	if err := store.SaveSidecar(id, "archive/v2/test", map[string]string{"state": "ready"}); err != nil {
		t.Fatal(err)
	}
	objects.mu.Lock()
	objects.statCalls, objects.openCalls = 0, 0
	objects.mu.Unlock()
	var got map[string]string
	if err := store.LoadSidecar(id, "archive/v2/test", 1024, &got); err != nil {
		t.Fatal(err)
	}
	if got["state"] != "ready" {
		t.Fatalf("loaded sidecar = %#v", got)
	}
	objects.mu.Lock()
	stats, opens := objects.statCalls, objects.openCalls
	objects.mu.Unlock()
	if stats != 0 || opens != 1 {
		t.Fatalf("sidecar read operations = stat:%d open:%d, want stat:0 open:1", stats, opens)
	}
}

func TestPutBytesPublishesBoundedSidecarDirectlyAndConfirmsUncertainSuccess(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	backend := store.StorageBackend.(*ObjectStoreArchiveBackend)
	const id = "1123456789abcdef0123456789abcdef"
	value := map[string]string{"state": "ready"}

	if err := backend.SaveSidecar(id, "archive/v2/sidecar", value); err != nil {
		t.Fatal(err)
	}
	objects.mu.Lock()
	putsAfterDirect, statsAfterDirect, opensAfterDirect := len(objects.putKeys), objects.statCalls, objects.openCalls
	objects.failPutAfterCommit = true
	objects.mu.Unlock()
	if putsAfterDirect != 1 || statsAfterDirect != 0 || opensAfterDirect != 0 {
		t.Fatalf("successful bounded Put used unexpected follow-up operations: put=%d stat=%d open=%d", putsAfterDirect, statsAfterDirect, opensAfterDirect)
	}

	// The provider commits atomically but loses its response. Core may accept
	// this only after exact size+SHA confirmation, and must not issue a second
	// Put with a different object identity.
	value["state"] = "updated"
	if err := backend.SaveSidecar(id, "archive/v2/sidecar", value); err != nil {
		t.Fatalf("confirmed ambiguous Put failed: %v", err)
	}
	objects.mu.Lock()
	puts, stats, opens := len(objects.putKeys), objects.statCalls, objects.openCalls
	objects.mu.Unlock()
	if puts != 2 || stats != 1 || opens != 1 {
		t.Fatalf("uncertain publication verification calls: put=%d stat=%d open=%d, want 2/1/1", puts, stats, opens)
	}
	var got map[string]string
	if err := backend.LoadSidecar(id, "archive/v2/sidecar", 1024, &got); err != nil || got["state"] != "updated" {
		t.Fatalf("stored sidecar=%#v err=%v", got, err)
	}
}

func (m *memoryPhysicalObjects) List(ctx context.Context, prefix, cursor string, limit int) (PhysicalObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return PhysicalObjectPage{}, err
	}
	if validateObjectPrefix(prefix) != nil || limit < 1 || limit > maxObjectPageSize {
		return PhysicalObjectPage{}, errors.New("invalid list")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listCalls++
	keys := make([]string, 0)
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	more := len(keys) > limit
	if more {
		keys = keys[:limit]
	}
	page := PhysicalObjectPage{Items: make([]PhysicalObjectInfo, 0, len(keys))}
	for _, key := range keys {
		page.Items = append(page.Items, m.infoLocked(key))
	}
	if more && len(keys) > 0 {
		page.NextCursor = keys[len(keys)-1]
	}
	return page, nil
}

func (m *memoryPhysicalObjects) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCalls++
	if m.failDeleteAfter > 0 && m.deleteCalls >= m.failDeleteAfter {
		return errors.New("injected delete failure")
	}
	delete(m.objects, key)
	delete(m.modified, key)
	return nil
}

func (m *memoryPhysicalObjects) infoLocked(key string) PhysicalObjectInfo {
	value := m.objects[key]
	digest := sha256.Sum256(value)
	return PhysicalObjectInfo{Key: key, Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:]), ModifiedAt: m.modified[key]}
}

func (m *memoryPhysicalObjects) replace(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), value...)
	m.modified[key] = time.Now().UTC()
}

func (m *memoryPhysicalObjects) get(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.objects[key]...)
}

func TestNewWithObjectStoreArchiveContractAndCommitOrdering(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	id := "0123456789abcdef0123456789abcdef"
	recording := stoppedRecording(id)
	title, empty := "Source title", ""
	recording.MetadataTimeline = []domain.MetadataRevision{{ObservedAt: time.Now().UTC(), Title: &title, Description: &empty}}
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	rootKey := recordingObjectKey(id, "recording.json")
	if len(objects.get(rootKey)) == 0 {
		t.Fatal("initial root was not published")
	}
	payload := bytes.Repeat([]byte("streamed-object-"), 8192)
	objects.failPutAfterCommit = true
	result, err := store.SavePayload(id, "tracks/main/segment-1.m4s", bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatalf("ambiguous complete Put was not confirmed: %v", err)
	}
	if result.Size != int64(len(payload)) || result.SHA256 != digest(payload) {
		t.Fatalf("payload result = %#v", result)
	}
	if objects.maxPutRead == 0 || objects.maxPutRead > 64<<10 {
		t.Fatalf("provider received oversized read buffer %d", objects.maxPutRead)
	}
	segment := domain.Segment{ID: "segment-1", TrackID: "main", Sequence: 1, StoragePath: "tracks/main/segment-1.m4s", PayloadSize: int64(len(payload)), SHA256: digest(payload)}
	if err := store.SaveSidecar(id, segment.StoragePath, segment); err != nil {
		t.Fatal(err)
	}
	recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, segment)
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	objects.mu.Lock()
	puts := append([]string(nil), objects.putKeys...)
	objects.mu.Unlock()
	wantTail := []string{recordingObjectKey(id, segment.StoragePath), recordingObjectKey(id, segment.StoragePath+".json"), rootKey}
	if len(puts) < len(wantTail) {
		t.Fatalf("put order %v", puts)
	}
	for i, want := range wantTail {
		if puts[len(puts)-len(wantTail)+i] != want {
			t.Fatalf("put order tail %v, want %v", puts, wantTail)
		}
	}
	if !bytes.Equal(objects.get(recordingObjectKey(id, segment.StoragePath)), payload) {
		t.Fatal("provider did not preserve exact payload bytes")
	}
	reader, err := store.OpenPayloadReader(id, segment.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("open payload mismatch: %v", err)
	}
	stat, err := store.StatPayload(id, segment.StoragePath)
	if err != nil || stat.Size != int64(len(payload)) || !stat.Regular {
		t.Fatalf("stat=%#v err=%v", stat, err)
	}
	rangeReader, rangeInfo, err := store.StorageBackend.(*ObjectStoreArchiveBackend).OpenPayloadRange(id, segment.StoragePath, 7, 19)
	if err != nil {
		t.Fatal(err)
	}
	rangeBytes, err := io.ReadAll(rangeReader)
	_ = rangeReader.Close()
	if err != nil || !bytes.Equal(rangeBytes, payload[7:26]) || rangeInfo.Size != 19 {
		t.Fatalf("range %q info=%#v err=%v", rangeBytes, rangeInfo, err)
	}
	loaded, err := store.LoadRecordingReadOnly(id)
	if err != nil || len(loaded.Tracks["main"].Segments) != 1 {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	if loaded.MetadataTimeline[0].Description == nil || *loaded.MetadataTimeline[0].Description != "" {
		t.Fatal("known empty metadata was not preserved")
	}
	if verified := store.VerifyRecording(loaded); verified.Status != IntegrityVerified || verified.ObjectsVerified != 1 {
		t.Fatalf("integrity=%#v", verified)
	}
	index, err := store.ArchiveIndex(loaded)
	if err != nil || len(index) != 3 {
		t.Fatalf("index=%#v err=%v", index, err)
	}
	stats, err := store.StorageStats()
	if err != nil || stats.CapacityKnown || stats.RecordingCount != 1 || stats.SegmentCount != 1 {
		t.Fatalf("stats=%#v err=%v", stats, err)
	}
	if pool := store.PoolMetrics(); pool.CapacityKnown || pool.Kind != "remote" {
		t.Fatalf("pool metrics=%#v", pool)
	}
}

func TestObjectStoreCreateRecordingWithSidecarPublishesContextBeforeRoot(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0b23456789abcdef0123456789abcdef"
	rootKey := recordingObjectKey(id, "recording.json")
	sidecarKey := recordingObjectKey(id, "archive-index/acquisition-context") + ".json"
	type privateContext struct {
		SchemaVersion int    `json:"schema_version"`
		ManifestURL   string `json:"manifest_url"`
	}
	context := privateContext{SchemaVersion: 1, ManifestURL: "https://media.example/live.m3u8"}

	objects.failPutBefore = true
	if err := store.CreateRecordingWithSidecar(stoppedRecording(id), "archive-index/acquisition-context", context); err == nil {
		t.Fatal("injected sidecar PUT failure was ignored")
	}
	if len(objects.get(rootKey)) != 0 {
		t.Fatal("root was published after sidecar failure")
	}

	if err := store.CreateRecordingWithSidecar(stoppedRecording(id), "archive-index/acquisition-context", context); err != nil {
		t.Fatal(err)
	}
	if len(objects.get(rootKey)) == 0 || len(objects.get(sidecarKey)) == 0 {
		t.Fatal("successful initialization did not publish root and sidecar")
	}
	objects.mu.Lock()
	puts := append([]string(nil), objects.putKeys...)
	objects.mu.Unlock()
	if len(puts) < 2 || puts[len(puts)-2] != sidecarKey || puts[len(puts)-1] != rootKey {
		t.Fatalf("initial object publication order=%v, want sidecar before root", puts)
	}
}

func TestObjectStorePoolMetricsUsePinnedProviderIdentity(t *testing.T) {
	for _, test := range []struct {
		id, name, wantID, wantKind string
	}{
		{id: "local", name: "Local Storage", wantID: PoolIDLocalPrimary, wantKind: "local"},
		{id: "fixture-s3", name: "Fixture S3", wantID: "storage-fixture-s3", wantKind: "remote"},
	} {
		objects := identifiedMemoryPhysicalObjects{memoryPhysicalObjects: newMemoryPhysicalObjects(), id: test.id, name: test.name}
		store, err := NewWithObjectStore(t.TempDir(), objects)
		if err != nil {
			t.Fatal(err)
		}
		pool := store.PoolMetrics()
		if pool.ID != test.wantID || pool.DisplayName != test.name && test.id != "local" || pool.Kind != test.wantKind {
			t.Fatalf("provider %q pool = %+v, want id=%q kind=%q", test.id, pool, test.wantID, test.wantKind)
		}
	}
}

func TestNewWithObjectStoreDoesNotCleanAnotherGenerationStaging(t *testing.T) {
	root := t.TempDir()
	if _, err := NewWithObjectStore(root, newMemoryPhysicalObjects()); err != nil {
		t.Fatal(err)
	}
	otherStage := filepath.Join(root, "runtime", "storage-staging", "put-active-engine")
	if err := os.MkdirAll(otherStage, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(otherStage, "object")
	if err := os.WriteFile(marker, []byte("already admitted"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWithObjectStore(root, newMemoryPhysicalObjects()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("candidate generation open removed active Engine staging: %v", err)
	}
}

type boundedChunkReader struct {
	remaining int
	maxSeen   int
}

func (r *boundedChunkReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) > r.maxSeen {
		r.maxSeen = len(p)
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = byte(i)
	}
	r.remaining -= len(p)
	return len(p), nil
}

func TestObjectStoreStreamsAndAtomicReplacement(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	stream := &boundedChunkReader{remaining: 2 << 20}
	result, err := store.SavePayload(id, "tracks/main/stream.m4s", stream, 3<<20)
	if err != nil {
		t.Fatal(err)
	}
	if result.Size != 2<<20 || stream.maxSeen > 64<<10 || objects.maxPutRead > 64<<10 {
		t.Fatalf("not streamed: result=%#v sourceRead=%d providerRead=%d", result, stream.maxSeen, objects.maxPutRead)
	}
	key := recordingObjectKey(id, "tracks/main/stream.m4s")
	original := []byte("complete-original")
	if _, err := store.SavePayload(id, "tracks/main/replace.m4s", bytes.NewReader(original), 1024); err != nil {
		t.Fatal(err)
	}
	objects.failPutBefore = true
	if _, err := store.SavePayload(id, "tracks/main/replace.m4s", bytes.NewReader([]byte("replacement")), 1024); err == nil {
		t.Fatal("failed physical publication was accepted")
	}
	if got := objects.get(recordingObjectKey(id, "tracks/main/replace.m4s")); !bytes.Equal(got, original) {
		t.Fatalf("atomic overwrite lost old complete object: %q", got)
	}
	if objects.get(key) == nil {
		t.Fatal("streamed object missing")
	}
	objects.wrongPutHash = true
	if _, err := store.SavePayload(id, "tracks/main/bad-response.m4s", bytes.NewReader([]byte("verified source")), 1024); err == nil {
		t.Fatal("provider response with a false digest was accepted")
	}
	if _, err := store.SavePayloadExact(id, "tracks/main/short.m4s", bytes.NewReader([]byte("short")), 100, 8); err == nil {
		t.Fatal("wrong exact source size was accepted")
	}
	if _, err := objects.Stat(context.Background(), recordingObjectKey(id, "tracks/main/short.m4s")); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("size mismatch published an object: %v", err)
	}
}

func TestObjectStorePayloadRangeReaderUsesPhysicalRange(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	const path = "tracks/main/segment.m4s"
	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if _, err := store.SavePayload(id, path, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}

	reader, err := store.OpenPayloadRangeReaderContext(context.Background(), id, path, 8, 7)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("range read=%v close=%v", readErr, closeErr)
	}
	if want := payload[8:15]; !bytes.Equal(got, want) {
		t.Fatalf("range body=%q, want %q", got, want)
	}
	if len(got) != 7 {
		t.Fatalf("range body length=%d, want 7", len(got))
	}

	objects.mu.Lock()
	defer objects.mu.Unlock()
	if len(objects.rangeCalls) != 1 {
		t.Fatalf("physical range calls=%d, want 1", len(objects.rangeCalls))
	}
	call := objects.rangeCalls[0]
	if call.key != recordingObjectKey(id, path) || call.offset != 8 || call.length != 7 {
		t.Fatalf("physical range call=%#v", call)
	}
}

func TestObjectStoreListPaginationAndInvalidKeysAreRejectedBeforeProvider(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	backend := store.StorageBackend.(*ObjectStoreArchiveBackend)
	for i := 0; i < 1005; i++ {
		key := fmt.Sprintf("page/%04d", i)
		objects.replace(key, []byte("x"))
	}
	items, err := backend.listAll(context.Background(), "page/", 1100)
	if err != nil || len(items) != 1005 {
		t.Fatalf("items=%d err=%v", len(items), err)
	}
	objects.mu.Lock()
	listCalls := objects.listCalls
	before := len(objects.putKeys)
	objects.mu.Unlock()
	if listCalls < 2 {
		t.Fatalf("list calls=%d, want pagination", listCalls)
	}
	const id = "11111111111111111111111111111111"
	for _, path := range []string{"../escape", "tracks//x", "tracks/./x", `tracks\x`, "/absolute"} {
		if _, err := store.SavePayload(id, path, strings.NewReader("bad"), 100); err == nil {
			t.Fatalf("invalid key %q accepted", path)
		}
	}
	if _, err := store.StatPayload(id, "tracks/../escape"); err == nil {
		t.Fatal("unsafe stat key accepted")
	}
	objects.mu.Lock()
	after := len(objects.putKeys)
	objects.mu.Unlock()
	if after != before {
		t.Fatal("invalid key reached physical provider")
	}
	for _, key := range []string{"", "/absolute", "a//b", "a/../b", "a\\b", strings.Repeat("a", 1025)} {
		if ValidateObjectKey(key) == nil {
			t.Fatalf("invalid physical key %q accepted", key)
		}
	}
}

func TestObjectStoreRecoveryIntegrityDeleteResumeAndUnknownCapacity(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "22222222222222222222222222222222"
	recording := stoppedRecording(id)
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	data := []byte("canonical segment")
	path := "tracks/main/1.m4s"
	if _, err := store.SavePayload(id, path, bytes.NewReader(data), 1024); err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{ID: "s1", TrackID: "main", Sequence: 1, StoragePath: path, PayloadSize: int64(len(data)), SHA256: digest(data)}
	if err := store.SaveSidecar(id, path, segment); err != nil {
		t.Fatal(err)
	}
	recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, segment)
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	objects.replace(recordingObjectKey(id, path), []byte("corrupt"))
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || !store.StorageBackend.(*ObjectStoreArchiveBackend).HasCanonicalPayloadIssue(id) {
		t.Fatalf("recovery loaded=%#v err=%v issues=%#v", loaded, err, store.StorageBackend.RecoveryIssues())
	}
	if result := store.VerifyRecording(loaded[0]); result.Status != IntegrityDegraded || result.ObjectsCorrupt != 1 {
		t.Fatalf("integrity=%#v", result)
	}
	if err := objects.Delete(context.Background(), recordingObjectKey(id, path)); err != nil {
		t.Fatal(err)
	}
	if result := store.VerifyRecording(loaded[0]); result.Status != IntegrityDegraded || result.ObjectsMissing != 1 {
		t.Fatalf("missing object integrity=%#v", result)
	}
	if !store.StorageBackend.(*ObjectStoreArchiveBackend).HasCanonicalPayloadIssue(id) {
		t.Fatal("canonical issue missing")
	}
	objects.failDeleteAfter = 2
	if err := store.DeleteRecordingData(id); err == nil {
		t.Fatal("partial delete reported success")
	}
	// The archive is hidden even when only part of its object prefix was
	// removed. Startup retries cleanup, but preserves the tombstone on failure.
	loaded, err = store.LoadAll()
	if err != nil || len(loaded) != 0 {
		t.Fatalf("partially deleted recording visible: %d %v", len(loaded), err)
	}
	objects.failDeleteAfter = 0
	loaded, err = store.LoadAll()
	if err != nil || len(loaded) != 0 {
		t.Fatalf("tombstoned recording visible after recovery: %d %v", len(loaded), err)
	}
	if _, err := store.LoadRecordingReadOnly(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted root remains: %v", err)
	}
	markers, err := objects.List(context.Background(), deletionMarkerPrefix, "", 10)
	if err != nil || len(markers.Items) != 0 {
		t.Fatalf("deletion marker was not cleared: %#v %v", markers, err)
	}
	stats, err := store.StorageStats()
	if err != nil || stats.CapacityKnown {
		t.Fatalf("remote capacity was fabricated: %#v %v", stats, err)
	}
}

func TestObjectStoreContextCancellationAndDeadline(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	objects.blockPut = true
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "33333333333333333333333333333333"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = store.StorageBackend.(*ObjectStoreArchiveBackend).SavePayloadContext(ctx, id, "tracks/main/cancel.m4s", bytes.NewReader([]byte("payload")), 100)
	if err == nil {
		t.Fatal("deadline expiry was accepted")
	}
	if _, statErr := objects.Stat(context.Background(), recordingObjectKey(id, "tracks/main/cancel.m4s")); !errors.Is(statErr, ErrObjectNotFound) {
		t.Fatalf("cancelled put published an object: %v", statErr)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := store.StorageBackend.(*ObjectStoreArchiveBackend).SavePayloadContext(canceled, id, "tracks/main/canceled.m4s", bytes.NewReader([]byte("x")), 100); err == nil {
		t.Fatal("canceled call succeeded")
	}
}

func TestObjectStoreRecordingDirectoryBytesContextPropagatesCancellation(t *testing.T) {
	objects := &blockingListPhysicalObjects{
		memoryPhysicalObjects: newMemoryPhysicalObjects(),
		listStarted:           make(chan context.Context, 1),
		releaseList:           make(chan struct{}, 1),
	}
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	backend := store.StorageBackend.(*ObjectStoreArchiveBackend)
	const id = "55555555555555555555555555555555"
	marker := new(int)
	requestCtx, cancel := context.WithCancel(context.WithValue(context.Background(), recordingDirectoryRequestContextKey{}, marker))
	defer cancel()
	type result struct {
		bytes int64
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		byteCount, err := backend.RecordingDirectoryBytesContext(requestCtx, id)
		resultCh <- result{bytes: byteCount, err: err}
	}()
	completed := false
	defer func() {
		cancel()
		if completed {
			return
		}
		select {
		case objects.releaseList <- struct{}{}:
		default:
		}
		select {
		case <-resultCh:
		case <-time.After(time.Second):
			t.Error("recording byte listing did not exit")
		}
	}()

	var providerCtx context.Context
	select {
	case providerCtx = <-objects.listStarted:
	case <-time.After(time.Second):
		t.Fatal("provider List was not called")
	}
	if providerCtx.Value(recordingDirectoryRequestContextKey{}) != marker {
		t.Fatal("provider List did not receive request context")
	}
	cancel()
	if !errors.Is(providerCtx.Err(), context.Canceled) {
		t.Fatalf("provider context error = %v, want context.Canceled", providerCtx.Err())
	}
	select {
	case got := <-resultCh:
		completed = true
		if got.bytes != 0 || got.err == nil {
			t.Fatalf("canceled listing = %d, %v; want 0 bytes and error", got.bytes, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled recording byte listing did not exit")
	}
}

func TestObjectStoreRecoveryResumesStaleRecordingAndTimeline(t *testing.T) {
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "44444444444444444444444444444444"
	recording := stoppedRecording(id)
	recording.State = domain.StateRecording
	title := "live"
	recording.MetadataTimeline = []domain.MetadataRevision{{ObservedAt: time.Now().UTC(), Title: &title}}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || loaded[0].State != domain.StateInterrupted || len(loaded[0].MetadataTimeline) != 1 {
		t.Fatalf("generic recovery=%#v err=%v", loaded, err)
	}
}
