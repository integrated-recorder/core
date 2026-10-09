package recorderengine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

type fixtureTransport struct{}

func (fixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body string
	switch request.URL.Path {
	case "/live.m3u8":
		body = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,fixture\nsegment.ts\n"
	case "/segment.ts":
		body = "fixture media bytes"
	default:
		return nil, io.EOF
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
}

func TestResolvedStartGetStopOverIPC(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "ir-engine-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	store, err := storage.New(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithMode(store, &http.Client{Transport: fixtureTransport{}, Timeout: time.Second}, nil, func(context.Context, string) error { return nil }, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, "generation-a", "engine-instance-a")
	if err != nil {
		t.Fatal(err)
	}
	token := []byte("0123456789abcdef0123456789abcdef")
	socket := filepath.Join(root, "ipc", "engine.sock")
	ipcServer, err := runtimeipc.NewServer(socket, "generation-a", token, engine)
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, cancelServer := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- ipcServer.Serve(serverCtx) }()
	client, err := runtimeipc.NewClient(socket, "generation-a", token, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	managerClient, err := recorderengine.NewManagerClient(client)
	if err != nil {
		t.Fatal(err)
	}
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	started, err := managerClient.StartResolved(context.Background(), "fixture", media, nil, "fixture recording", nil)
	if err != nil {
		t.Fatal(err)
	}
	if started.ID == "" || started.State != domain.StateRecording {
		t.Fatalf("resolved start result %#v", started)
	}
	deadline := time.Now().Add(4 * time.Second)
	var current *domain.Recording
	for {
		if current, err = managerClient.Get(started.ID); err != nil {
			t.Fatal(err)
		}
		if len(current.Tracks["main"].Segments) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deterministic HLS fixture segment was not committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if current.State != domain.StateRecording {
		t.Fatalf("fixture recording state before stop = %q", current.State)
	}
	if count, err := managerClient.ActiveRecordings(context.Background(), "generation-a"); err != nil || count != 1 {
		t.Fatalf("active recording count = %d, err=%v; want 1", count, err)
	}
	rows, err := managerClient.ListForManagement(context.Background(), 10)
	if err != nil || len(rows) != 1 || rows[0].ID != started.ID {
		t.Fatalf("manager client list=%#v err=%v", rows, err)
	}
	page, next, err := managerClient.ListForManagementPage(context.Background(), "", 1)
	if err != nil || len(page) != 1 || page[0].ID != started.ID || next != "" {
		t.Fatalf("manager client summary page=%#v next=%q err=%v", page, next, err)
	}
	header, err := managerClient.GetManagementHeader(context.Background(), started.ID)
	if err != nil || header.ID != started.ID || header.FormatVersion != storage.ShardedArchiveFormatVersion || header.SegmentCount() != 1 || len(header.Tracks["main"].Segments) != 0 {
		t.Fatalf("manager client management header=%#v err=%v", header, err)
	}
	const fixedID = "1123456789abcdef0123456789abcdef"
	fixed, err := managerClient.StartResolvedWithID(context.Background(), fixedID, "fixture", media, nil, "fixed id", nil)
	if err != nil || fixed.ID != fixedID {
		t.Fatalf("resolved start with caller ID = %#v, err=%v", fixed, err)
	}
	if _, err = managerClient.Stop(fixedID); err != nil {
		t.Fatal(err)
	}
	if err = managerClient.Delete(fixedID); err != nil {
		t.Fatalf("manager client delete: %v", err)
	}
	if _, err = managerClient.Get(fixedID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted recording Get error = %v, want not found", err)
	}
	if count, err := managerClient.ActiveRecordings(context.Background(), "generation-a"); err != nil || count != 1 {
		t.Fatalf("active recording count after second recording stopped = %d, err=%v; want 1", count, err)
	}
	if err := managerClient.BeginDrain(context.Background(), "generation-a"); err != nil {
		t.Fatalf("begin drain: %v", err)
	}
	if err := managerClient.BeginDrain(context.Background(), "generation-a"); err != nil {
		t.Fatalf("repeated begin drain: %v", err)
	}
	if err := managerClient.BeginDrain(context.Background(), "generation-b"); err == nil || !strings.Contains(err.Error(), "generation identity mismatch") {
		t.Fatalf("wrong-generation drain error = %v", err)
	}
	if _, err := managerClient.StartResolvedWithID(context.Background(), "2123456789abcdef0123456789abcdef", "fixture", media, nil, "after drain", nil); !errors.Is(err, recorderengine.ErrEngineDraining) {
		t.Fatalf("start with ID after drain error = %v, want ErrEngineDraining", err)
	}
	current, err = managerClient.Get(started.ID)
	if err != nil || current.State != domain.StateRecording {
		t.Fatalf("existing recording changed after drain: state=%q err=%v", currentState(current), err)
	}
	stopped, err := managerClient.Stop(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.State != domain.StateStopped || len(stopped.Tracks["main"].Segments) != 1 {
		t.Fatalf("stopped recording = %#v", stopped)
	}
	completed, err := managerClient.CompleteRecording(context.Background(), started.ID)
	if err != nil || completed.State != domain.StateCompleted || completed.ArchiveSealed {
		t.Fatalf("explicit completion over IPC = %#v err=%v", completed, err)
	}
	if err := managerClient.SealArchiveContext(context.Background(), started.ID); err != nil {
		t.Fatalf("explicit seal over IPC: %v", err)
	}
	if count, err := managerClient.ActiveRecordings(context.Background(), "generation-a"); err != nil || count != 0 {
		t.Fatalf("active recording count after stop = %d, err=%v; want 0", count, err)
	}
	var inventory recorderengine.InventoryResult
	if err := client.Call(context.Background(), recorderengine.OperationInventory, nil, &inventory); err != nil {
		t.Fatal(err)
	}
	if inventory.GenerationID != "generation-a" || len(inventory.Active) != 0 {
		t.Fatalf("engine inventory = %#v", inventory)
	}
	cancelServer()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	if err := engine.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	var persisted domain.Recording
	data, err := os.ReadFile(filepath.Join(root, "data", "recordings", started.ID, "recording.json"))
	if err != nil || json.Unmarshal(data, &persisted) != nil || persisted.State != domain.StateCompleted || !persisted.ArchiveSealed {
		t.Fatalf("canonical lifecycle was not durably published: err=%v state=%q", err, persisted.State)
	}
}

func TestEngineListInFreshGenerationIncludesReadOnlyPriorArchives(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "ir-engine-read-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	now := time.Now().UTC()
	if err := store.CreateRecording(&domain.Recording{FormatVersion: 1, ID: id, State: domain.StateRecording, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main"}}}); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithMode(store, nil, nil, func(context.Context, string) error { return nil }, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, "generation-b", "engine-instance-b")
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	result, err := engine.Handle(context.Background(), recorderengine.OperationList, json.RawMessage(`{"limit":10}`))
	if err != nil {
		t.Fatal(err)
	}
	rows, ok := result.([]*domain.Recording)
	if !ok || len(rows) != 1 || rows[0].ID != id || rows[0].State != domain.StateRecording {
		t.Fatalf("fresh engine listing %#v", result)
	}
	countResult, err := engine.Handle(context.Background(), recorderengine.OperationActiveCount, json.RawMessage(`{"generation_id":"generation-b"}`))
	if err != nil {
		t.Fatal(err)
	}
	count, ok := countResult.(recorderengine.ActiveRecordingCountResult)
	if !ok || count.Count != 0 {
		t.Fatalf("fresh engine counted read-only archive as owned active: %#v", countResult)
	}
}

func currentState(recording *domain.Recording) domain.RecordingState {
	if recording == nil {
		return "<nil>"
	}
	return recording.State
}

type mismatchedGenerationHandler struct{}

func (mismatchedGenerationHandler) Handle(_ context.Context, operation string, _ json.RawMessage) (any, error) {
	switch operation {
	case recorderengine.OperationBeginDrain:
		return recorderengine.DrainResult{GenerationID: "generation-b", InstanceID: "engine-b", Draining: true}, nil
	case recorderengine.OperationActiveCount:
		return recorderengine.ActiveRecordingCountResult{GenerationID: "generation-b", InstanceID: "engine-b", Count: 0}, nil
	default:
		return nil, errors.New("unsupported test operation")
	}
}

func TestManagerClientRejectsMismatchedGenerationResponses(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "ir-engine-mismatch-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	token := []byte("0123456789abcdef0123456789abcdef")
	socket := filepath.Join(root, "ipc", "engine.sock")
	ipcServer, err := runtimeipc.NewServer(socket, "generation-a", token, mismatchedGenerationHandler{})
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, cancelServer := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- ipcServer.Serve(serverCtx) }()
	client, err := runtimeipc.NewClient(socket, "generation-a", token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	managerClient, err := recorderengine.NewManagerClient(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := managerClient.BeginDrain(context.Background(), "generation-a"); err == nil || !strings.Contains(err.Error(), "response identity is invalid") {
		t.Fatalf("mismatched drain response error = %v", err)
	}
	if _, err := managerClient.ActiveRecordings(context.Background(), "generation-a"); err == nil || !strings.Contains(err.Error(), "response is invalid") {
		t.Fatalf("mismatched count response error = %v", err)
	}
	cancelServer()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}
