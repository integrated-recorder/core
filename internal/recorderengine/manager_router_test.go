package recorderengine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

type operationCounter struct {
	engine          *recorderengine.Engine
	mu              sync.Mutex
	counts          map[string]int
	shadow          bool
	beforeInventory func(context.Context) error
}

func (c *operationCounter) RuntimeInstanceID() string { return c.engine.RuntimeInstanceID() }
func (c *operationCounter) GenerationID() string      { return c.engine.GenerationID() }
func (c *operationCounter) Handle(ctx context.Context, operation string, payload json.RawMessage) (any, error) {
	c.mu.Lock()
	c.counts[operation]++
	beforeInventory := c.beforeInventory
	c.mu.Unlock()
	if operation == recorderengine.OperationInventory && beforeInventory != nil {
		if err := beforeInventory(ctx); err != nil {
			return nil, err
		}
	}
	result, err := c.engine.Handle(ctx, operation, payload)
	if err == nil && c.shadow && operation == recorderengine.OperationGet {
		if recording, ok := result.(*domain.Recording); ok {
			copy := *recording
			copy.Title = "read-only snapshot from generation b"
			return &copy, nil
		}
	}
	return result, err
}

func (c *operationCounter) count(operation string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[operation]
}

type runningTestEngine struct {
	engine *recorderengine.Engine
	count  *operationCounter
	client *recorderengine.ManagerClient
	cancel context.CancelFunc
	done   chan error
}

func startTestEngine(t *testing.T, root, generation string, store *storage.Store, shadow bool) runningTestEngine {
	t.Helper()
	manager, err := acquire.NewManagerWithMode(store, &http.Client{Transport: fixtureTransport{}, Timeout: time.Second}, nil, func(context.Context, string) error { return nil }, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, generation, generation+"-instance")
	if err != nil {
		t.Fatal(err)
	}
	counter := &operationCounter{engine: engine, counts: make(map[string]int), shadow: shadow}
	var token [32]byte
	for i := range token {
		token[i] = byte(i + 1)
	}
	socket := filepath.Join(root, generation, "engine.sock")
	server, err := runtimeipc.NewServer(socket, generation, token[:], counter)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	client, err := runtimeipc.NewClient(socket, generation, token[:], 5*time.Second)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	managerClient, err := recorderengine.NewManagerClient(client)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		var ready recorderengine.ReadyResult
		if err := client.Call(context.Background(), recorderengine.OperationReady, nil, &ready); err == nil && ready.Ready {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("test Engine did not become ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("IPC server shutdown: %v", err)
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := engine.Close(closeCtx); err != nil {
			t.Errorf("Engine shutdown: %v", err)
		}
	})
	return runningTestEngine{engine: engine, count: counter, client: managerClient, cancel: cancel, done: done}
}

func TestManagerRouterGenerationRoutingAndDetachAfterInventoryDrains(t *testing.T) {
	root := shortTempRoot(t)
	data := filepath.Join(root, "data")
	storeA, err := storage.New(data)
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := storage.New(data)
	if err != nil {
		t.Fatal(err)
	}
	engineA := startTestEngine(t, filepath.Join(root, "ipc"), "engine-a", storeA, false)
	engineB := startTestEngine(t, filepath.Join(root, "ipc"), "engine-b", storeB, true)
	router, err := recorderengine.NewManagerRouter("engine-a", map[string]*recorderengine.ManagerClient{
		"engine-a": engineA.client,
		"engine-b": engineB.client,
	}, storeA)
	if err != nil {
		t.Fatal(err)
	}
	if err := router.DetachGenerationContext(context.Background(), "engine-missing"); !errors.Is(err, recorderengine.ErrGenerationNotAttached) {
		t.Fatalf("missing generation detach error=%v", err)
	}
	if err := router.DetachGenerationContext(context.Background(), "engine-a"); !errors.Is(err, recorderengine.ErrCannotDetachActiveGeneration) {
		t.Fatalf("active generation detach error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := router.DetachGenerationContext(canceled, "engine-b"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled detach error=%v", err)
	}
	inventoryStarted := make(chan struct{})
	var inventoryStartOnce sync.Once
	engineB.count.mu.Lock()
	engineB.count.beforeInventory = func(ctx context.Context) error {
		inventoryStartOnce.Do(func() { close(inventoryStarted) })
		<-ctx.Done()
		return ctx.Err()
	}
	engineB.count.mu.Unlock()
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), 50*time.Millisecond)
	detachDone := make(chan error, 1)
	go func() { detachDone <- router.DetachGenerationContext(requestCtx, "engine-b") }()
	select {
	case <-inventoryStarted:
	case <-time.After(time.Second):
		cancelRequest()
		t.Fatal("detach did not request Engine inventory")
	}
	select {
	case err := <-detachDone:
		if err == nil {
			t.Fatal("inventory cancellation unexpectedly detached Engine")
		}
	case <-time.After(time.Second):
		t.Fatal("detach did not honor inventory cancellation")
	}
	cancelRequest()
	engineB.count.mu.Lock()
	engineB.count.beforeInventory = nil
	engineB.count.mu.Unlock()
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	const recordingAID = "1123456789abcdef0123456789abcdef"
	recordingA, err := router.StartResolvedWithID(context.Background(), recordingAID, "fixture", media, nil, "owned by generation a", nil)
	if err != nil || recordingA.ID != recordingAID {
		t.Fatalf("start on initial generation: recording=%#v err=%v", recordingA, err)
	}
	waitForSegment(t, router, recordingAID)
	liveView, err := router.LivePlaybackSnapshot(context.Background(), recordingAID)
	if err != nil || len(liveView.Segments) != 1 || liveView.Segments[0].SourceURI != "" {
		t.Fatalf("live playback snapshot=%#v err=%v", liveView, err)
	}
	if engineA.count.count(recorderengine.OperationLivePlaybackSnapshot) != 1 || engineB.count.count(recorderengine.OperationLivePlaybackSnapshot) != 0 {
		t.Fatalf("live snapshot did not route to the owning generation: A=%d B=%d", engineA.count.count(recorderengine.OperationLivePlaybackSnapshot), engineB.count.count(recorderengine.OperationLivePlaybackSnapshot))
	}
	if err := router.SetActive("engine-b"); err != nil {
		t.Fatal(err)
	}
	const recordingBID = "2123456789abcdef0123456789abcdef"
	recordingB, err := router.StartResolvedWithID(context.Background(), recordingBID, "fixture", media, nil, "owned by generation b", nil)
	if err != nil || recordingB.ID != recordingBID {
		t.Fatalf("start on switched generation: recording=%#v err=%v", recordingB, err)
	}
	if engineA.count.count(recorderengine.OperationStartResolved) != 1 || engineB.count.count(recorderengine.OperationStartResolved) != 1 {
		t.Fatalf("resolved starts were duplicated or misrouted: A=%d B=%d", engineA.count.count(recorderengine.OperationStartResolved), engineB.count.count(recorderengine.OperationStartResolved))
	}

	gotA, err := router.Get(recordingAID)
	if err != nil {
		t.Fatal(err)
	}
	if gotA.Title != "owned by generation a" {
		t.Fatalf("Get did not prefer live owner over read-only snapshot: title=%q", gotA.Title)
	}
	if err := router.DetachGenerationContext(context.Background(), "engine-a"); !errors.Is(err, recorderengine.ErrGenerationHasActiveRecords) {
		t.Fatalf("active Engine detach error=%v", err)
	}
	if _, err := router.Stop(recordingAID); err != nil {
		t.Fatal(err)
	}
	if engineA.count.count(recorderengine.OperationStop) != 1 || engineB.count.count(recorderengine.OperationStop) != 0 {
		t.Fatalf("Stop did not route once to live owner: A=%d B=%d", engineA.count.count(recorderengine.OperationStop), engineB.count.count(recorderengine.OperationStop))
	}
	if err := router.Delete(recordingAID); err != nil {
		t.Fatal(err)
	}
	if engineA.count.count(recorderengine.OperationDelete) != 1 || engineB.count.count(recorderengine.OperationDelete) != 0 {
		t.Fatalf("Delete was duplicated or misrouted: A=%d B=%d", engineA.count.count(recorderengine.OperationDelete), engineB.count.count(recorderengine.OperationDelete))
	}
	if err := router.DetachGenerationContext(context.Background(), "engine-a"); err != nil {
		t.Fatalf("detach after empty inventory: %v", err)
	}
	if err := router.SetActive("engine-a"); !errors.Is(err, recorderengine.ErrGenerationNotAttached) {
		t.Fatalf("detached generation became active: %v", err)
	}
	inventory, err := router.Inventory(context.Background())
	if err != nil || len(inventory) != 1 || inventory[0].GenerationID != "engine-b" || len(inventory[0].Active) != 1 || inventory[0].Active[0].RecordingID != recordingBID {
		t.Fatalf("post-detach inventory=%#v err=%v", inventory, err)
	}
	if _, err := router.Stop(recordingBID); err != nil {
		t.Fatal(err)
	}
	if engineB.count.count(recorderengine.OperationStop) != 1 {
		t.Fatalf("generation b Stop invocation count=%d", engineB.count.count(recorderengine.OperationStop))
	}
}

func TestEngineLifecycleOperationsKeepTransitionsSeparate(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	const id = "3123456789abcdef0123456789abcdef"
	if err := store.CreateRecording(&domain.Recording{
		FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now,
		StoppedAt: &now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}},
	}); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, "engine-lifecycle", "engine-lifecycle-instance")
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	request, err := json.Marshal(recorderengine.RecordingIDRequest{RecordingID: id})
	if err != nil {
		t.Fatal(err)
	}
	completedValue, err := engine.Handle(context.Background(), recorderengine.OperationComplete, request)
	completed, _ := completedValue.(*domain.Recording)
	if err != nil || completed.State != domain.StateCompleted || completed.ArchiveSealed {
		t.Fatalf("complete recording=%#v err=%v", completed, err)
	}
	snapshotValue, err := engine.Handle(context.Background(), recorderengine.OperationLifecycleSnapshot, request)
	snapshot, _ := snapshotValue.(acquire.LifecycleSnapshot)
	if err != nil || snapshot.CaptureState != domain.StateCompleted || snapshot.ArchiveSealed || !snapshot.Repairable || snapshot.EngineOwns {
		t.Fatalf("lifecycle snapshot=%#v err=%v", snapshot, err)
	}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationSealArchive, request); err != nil {
		t.Fatal(err)
	}
	sealed, err := manager.Get(id)
	if err != nil || sealed.State != domain.StateCompleted || !sealed.ArchiveSealed {
		t.Fatalf("seal changed capture state: recording=%#v err=%v", sealed, err)
	}
}

func TestManagerRouterTerminalDeleteFallbackRefusesActiveArchive(t *testing.T) {
	root := shortTempRoot(t)
	store, err := storage.New(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	engine := startTestEngine(t, filepath.Join(root, "ipc"), "engine-b", store, false)
	router, err := recorderengine.NewManagerRouter("engine-b", map[string]*recorderengine.ManagerClient{"engine-b": engine.client}, store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	const stoppedID = "3123456789abcdef0123456789abcdef"
	if err := store.CreateRecording(&domain.Recording{FormatVersion: 1, ID: stoppedID, State: domain.StateStopped, CreatedAt: now, StartedAt: now, StoppedAt: &now, Tracks: map[string]*domain.Track{"main": {ID: "main"}}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Delete(stoppedID); err != nil {
		t.Fatalf("terminal archive fallback delete: %v", err)
	}
	if _, err := store.LoadRecordingReadOnly(stoppedID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("terminal archive still exists after delete: %v", err)
	}

	const uncertainID = "4123456789abcdef0123456789abcdef"
	if err := store.CreateRecording(&domain.Recording{FormatVersion: 1, ID: uncertainID, State: domain.StateRecording, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main"}}}); err != nil {
		t.Fatal(err)
	}
	if err := router.Delete(uncertainID); !errors.Is(err, recorderengine.ErrRecordingMayBeActive) {
		t.Fatalf("unowned recording-state archive delete error=%v", err)
	}
	if _, err := store.LoadRecordingReadOnly(uncertainID); err != nil {
		t.Fatalf("ambiguous active archive was deleted: %v", err)
	}
}

func shortTempRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "ir-router-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func waitForSegment(t *testing.T, manager interface {
	Get(string) (*domain.Recording, error)
}, id string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		recording, err := manager.Get(id)
		if err == nil && recording.Tracks["main"] != nil && len(recording.Tracks["main"].Segments) != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("recording %s did not commit the deterministic fixture segment", id))
}
