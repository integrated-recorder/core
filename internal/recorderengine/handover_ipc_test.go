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
	"syscall"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

func newTargetManagedEngine(t *testing.T, ownerClient *recordingOwnerFixtureClient, archive *storage.Store, generation, instance string) *recorderengine.Engine {
	t.Helper()
	manager, err := acquire.NewManagerWithMode(archive, &http.Client{Transport: handoverContinuationTransport{}, Timeout: time.Second}, nil, func(context.Context, string) error { return nil }, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(ownerClient.store); err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, generation, instance)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ConfigureRecordingOwnerClient(ownerClient); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := engine.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close target Engine: %v", err)
		}
	})
	return engine
}

type handoverContinuationTransport struct{}

func (handoverContinuationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/segment-2.ts" {
		body := "fixture media bytes 2"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
	}
	if request.URL.Path != "/target.m3u8" {
		if request.URL.Path == "/tail.m3u8" {
			body := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,fixture\nsegment.ts\n"
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
		}
		return fixtureTransport{}.RoundTrip(request)
	}
	body := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,fixture\nsegment.ts\n#EXTINF:1,fixture\nsegment-2.ts\n"
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
}

func callEngine[T any](t *testing.T, engine *recorderengine.Engine, operation string, request any) (T, error) {
	t.Helper()
	var zero T
	var payload json.RawMessage
	if request != nil {
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		payload = encoded
	}
	result, err := engine.Handle(context.Background(), operation, payload)
	if err != nil {
		return zero, err
	}
	value, ok := result.(T)
	if !ok {
		t.Fatalf("%s result type = %T", operation, result)
	}
	return value, nil
}

func waitForHandoverSnapshot(t *testing.T, engine *recorderengine.Engine, id string, owner recordingowner.Owner) recorderengine.HandoverSnapshotResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, err := callEngine[recorderengine.HandoverSnapshotResult](t, engine, recorderengine.OperationHandoverSnapshot, recorderengine.HandoverOwnerRequest{RecordingID: id, Owner: owner})
		if err == nil {
			return result
		}
		if !strings.Contains(err.Error(), "handover operation failed") {
			t.Fatalf("snapshot failed before worker became ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("source Engine did not publish a handover snapshot")
	return recorderengine.HandoverSnapshotResult{}
}

func assertPublicErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected IPC-safe error code %q", want)
	}
	public, ok := err.(interface{ PublicIPCError() (string, string) })
	if !ok {
		t.Fatalf("error %T is not an IPC-safe public error: %v", err, err)
	}
	code, message := public.PublicIPCError()
	if code != want || message == "" || len(message) > 256 {
		t.Fatalf("public error = (%q, %q); want code %q and bounded message", code, message, want)
	}
}

func TestManagedInventoryCarriesExactOwnerTupleAndRejectsStaleHandover(t *testing.T) {
	engine, _, ownerClient, _ := newManagedEngineFixture(t, fixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media)); err != nil {
		t.Fatal(err)
	}
	_, claims, _, _ := ownerClient.snapshot()
	if len(claims) != 1 {
		t.Fatalf("Host claims = %d; want 1", len(claims))
	}
	inventory, err := callEngine[recorderengine.InventoryResult](t, engine, recorderengine.OperationInventory, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Active) != 1 || inventory.Active[0].RecordingID != generatedRecordingID || inventory.Active[0].Owner == nil || *inventory.Active[0].Owner != claims[0] {
		t.Fatalf("managed Engine inventory did not expose the exact private owner tuple: %#v", inventory)
	}

	stale := claims[0]
	stale.Epoch++ // structurally valid, but not the current in-memory owner.
	_, err = callEngine[recorderengine.HandoverSnapshotResult](t, engine, recorderengine.OperationHandoverPause, recorderengine.HandoverOwnerRequest{RecordingID: generatedRecordingID, Owner: stale})
	assertPublicErrorCode(t, err, "stale_owner")
	inventory, err = callEngine[recorderengine.InventoryResult](t, engine, recorderengine.OperationInventory, nil)
	if err != nil || len(inventory.Active) != 1 || inventory.Active[0].Owner == nil || *inventory.Active[0].Owner != claims[0] {
		t.Fatalf("stale handover changed source inventory: inventory=%#v err=%v", inventory, err)
	}
}

func TestHandoverRequestStrictnessAndTargetIdentityValidation(t *testing.T) {
	engine, _, owners, _ := newManagedEngineFixture(t, fixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media)); err != nil {
		t.Fatal(err)
	}
	_, claims, _, _ := owners.snapshot()
	if len(claims) != 1 {
		t.Fatalf("Host claims = %d; want 1", len(claims))
	}
	raw := json.RawMessage(`{"recording_id":"` + generatedRecordingID + `","owner":{"recording_id":"` + generatedRecordingID + `","engine_generation":"` + fixtureGenerationID + `","worker_instance":"` + fixtureWorkerInstanceID + `","epoch":1},"unexpected":"field"}`)
	_, err := engine.Handle(context.Background(), recorderengine.OperationHandoverPause, raw)
	assertPublicErrorCode(t, err, "invalid_request")

	wrong := claims[0]
	wrong.EngineGeneration = "dddddddddddddddddddddddddddddddd"
	_, err = callEngine[recorderengine.HandoverSnapshotResult](t, engine, recorderengine.OperationHandoverSnapshot, recorderengine.HandoverOwnerRequest{RecordingID: generatedRecordingID, Owner: wrong})
	assertPublicErrorCode(t, err, "invalid_request")

	snapshot := acquire.HandoverSnapshot{RecordingID: generatedRecordingID, Owner: claims[0], AdapterID: "fixture", Media: media}
	badTarget := acquire.HandoverTargetIdentity{EngineGeneration: "dddddddddddddddddddddddddddddddd", WorkerInstance: fixtureWorkerInstanceID}
	_, err = callEngine[recorderengine.HandoverResult](t, engine, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: snapshot, Target: badTarget})
	assertPublicErrorCode(t, err, "invalid_request")
}

func TestHandoverResumeAcceptsUnchangedAbortAndNewerRollbackOwner(t *testing.T) {
	engine, _, owners, archive := newManagedEngineFixture(t, fixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media)); err != nil {
		t.Fatal(err)
	}
	_, claims, _, _ := owners.snapshot()
	if len(claims) != 1 {
		t.Fatalf("Host claims = %d; want 1", len(claims))
	}
	owner := claims[0]
	target := acquire.HandoverTargetIdentity{EngineGeneration: "dddddddddddddddddddddddddddddddd", WorkerInstance: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
	targetEngine := newTargetManagedEngine(t, owners, archive, target.EngineGeneration, target.WorkerInstance)

	initial := waitForHandoverSnapshot(t, engine, generatedRecordingID, owner)
	var err error
	paused, err := callEngine[recorderengine.HandoverSnapshotResult](t, engine, recorderengine.OperationHandoverPause, recorderengine.HandoverOwnerRequest{RecordingID: generatedRecordingID, Owner: owner})
	if err != nil || paused.Snapshot.RecordingID != initial.Snapshot.RecordingID {
		t.Fatalf("pause source: snapshot=%#v err=%v", paused, err)
	}
	if _, err := callEngine[recorderengine.HandoverResult](t, engine, recorderengine.OperationHandoverResume, recorderengine.HandoverResumeRequest{RecordingID: generatedRecordingID, Owner: owner, Snapshot: paused.Snapshot}); err != nil {
		t.Fatalf("resume before transfer with unchanged token: %v", err)
	}

	paused, err = callEngine[recorderengine.HandoverSnapshotResult](t, engine, recorderengine.OperationHandoverPause, recorderengine.HandoverOwnerRequest{RecordingID: generatedRecordingID, Owner: owner})
	if err != nil {
		t.Fatalf("pause source for rollback case: %v", err)
	}
	targetResult := acquire.HandoverTargetIdentity{EngineGeneration: target.EngineGeneration, WorkerInstance: target.WorkerInstance}
	targetSnapshot := paused.Snapshot
	targetSnapshot.Media.ManifestURL = "https://fixture.example/target.m3u8"
	if _, err := callEngine[recorderengine.HandoverResult](t, targetEngine, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: targetSnapshot, Target: targetResult, SourceDrained: true}); err != nil {
		t.Fatalf("prepare target: %v", err)
	}
	targetOwner, err := owners.store.Transfer(owner, target.EngineGeneration, target.WorkerInstance)
	if err != nil {
		t.Fatalf("Host transfer to candidate: %v", err)
	}
	rollbackOwner, err := owners.store.Transfer(targetOwner, owner.EngineGeneration, owner.WorkerInstance)
	if err != nil {
		t.Fatalf("Host rollback to source: %v", err)
	}
	if _, err := callEngine[recorderengine.HandoverResult](t, engine, recorderengine.OperationHandoverResume, recorderengine.HandoverResumeRequest{RecordingID: generatedRecordingID, Owner: rollbackOwner, Snapshot: paused.Snapshot}); err != nil {
		t.Fatalf("resume after Host-issued higher-epoch rollback token: %v", err)
	}
	if _, err := callEngine[recorderengine.HandoverResult](t, targetEngine, recorderengine.OperationHandoverDiscardTarget, recorderengine.HandoverTargetDiscardRequest{RecordingID: generatedRecordingID, Target: targetResult}); err != nil {
		t.Fatalf("discard prepared candidate after rollback: %v", err)
	}
	inventory, err := callEngine[recorderengine.InventoryResult](t, engine, recorderengine.OperationInventory, nil)
	if err != nil || len(inventory.Active) != 1 || inventory.Active[0].Owner == nil || *inventory.Active[0].Owner != rollbackOwner {
		t.Fatalf("source inventory after rollback = %#v err=%v", inventory, err)
	}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStop, rawPayload(t, recorderengine.RecordingIDRequest{RecordingID: generatedRecordingID})); err != nil {
		t.Fatalf("stop source-owned recording after rollback: %v", err)
	}
}

func TestHandoverTargetActivationRequiresTransferredHostOwner(t *testing.T) {
	source, _, owners, archive := newManagedEngineFixture(t, fixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	if _, err := source.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media)); err != nil {
		t.Fatal(err)
	}
	_, claims, _, _ := owners.snapshot()
	if len(claims) != 1 {
		t.Fatalf("Host claims = %d; want 1", len(claims))
	}
	oldOwner := claims[0]
	targetIdentity := acquire.HandoverTargetIdentity{EngineGeneration: "dddddddddddddddddddddddddddddddd", WorkerInstance: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
	target := newTargetManagedEngine(t, owners, archive, targetIdentity.EngineGeneration, targetIdentity.WorkerInstance)

	_ = waitForHandoverSnapshot(t, source, generatedRecordingID, oldOwner)
	snapshot, err := callEngine[recorderengine.HandoverSnapshotResult](t, source, recorderengine.OperationHandoverPause, recorderengine.HandoverOwnerRequest{RecordingID: generatedRecordingID, Owner: oldOwner})
	if err != nil {
		t.Fatalf("pause source: %v", err)
	}
	expiredSnapshot := snapshot.Snapshot
	expiredAt := time.Now().Add(-time.Minute).UTC()
	expiredSnapshot.Media.RefreshPolicy = &adapterproto.RefreshPolicy{ExpiresAt: &expiredAt, RefreshBeforeSeconds: 1}
	expiredSnapshot.Media.ManifestURL = "https://fixture.example/target.m3u8"
	_, err = callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: expiredSnapshot, Target: targetIdentity})
	assertPublicErrorCode(t, err, "source_refresh_required")
	_, err = callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: expiredSnapshot, Target: targetIdentity, SourceDrained: true})
	assertPublicErrorCode(t, err, "handover_failed")
	boundarySnapshot := snapshot.Snapshot
	boundarySnapshot.Media.ManifestURL = "https://fixture.example/tail.m3u8"
	_, err = callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: boundarySnapshot, Target: targetIdentity})
	assertPublicErrorCode(t, err, "source_boundary_required")
	targetSnapshot := snapshot.Snapshot
	targetSnapshot.Media.ManifestURL = "https://fixture.example/target.m3u8"
	if _, err := callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: targetSnapshot, Target: targetIdentity, SourceDrained: true}); err != nil {
		t.Fatalf("prepare target read-only: %v", err)
	}
	targetOwner := recordingowner.Owner{RecordingID: generatedRecordingID, EngineGeneration: targetIdentity.EngineGeneration, WorkerInstance: targetIdentity.WorkerInstance, Epoch: oldOwner.Epoch + 1}
	_, err = callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverActivateTarget, recorderengine.HandoverTargetOwnerRequest{RecordingID: generatedRecordingID, Owner: targetOwner, Target: targetIdentity})
	assertPublicErrorCode(t, err, "stale_owner")
	inventory, err := callEngine[recorderengine.InventoryResult](t, target, recorderengine.OperationInventory, nil)
	if err != nil || len(inventory.Active) != 0 {
		t.Fatalf("fence-rejected target appeared as owner: inventory=%#v err=%v", inventory, err)
	}
	if current, err := owners.store.Current(generatedRecordingID); err != nil || current != oldOwner {
		t.Fatalf("failed target activation changed Host owner: current=%+v err=%v", current, err)
	}
	// The deliberately stale activation attempt invalidates its prepared worker
	// state. Re-run read-only preflight before the Host issues a valid token.
	if _, err := callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverPrepareTarget, recorderengine.HandoverTargetRequest{Snapshot: targetSnapshot, Target: targetIdentity, SourceDrained: true}); err != nil {
		t.Fatalf("reprepare target after stale-token rejection: %v", err)
	}

	transferred, err := owners.store.Transfer(oldOwner, targetIdentity.EngineGeneration, targetIdentity.WorkerInstance)
	if err != nil {
		t.Fatalf("Host transfer: %v", err)
	}
	if _, err := callEngine[recorderengine.HandoverResult](t, target, recorderengine.OperationHandoverActivateTarget, recorderengine.HandoverTargetOwnerRequest{RecordingID: generatedRecordingID, Owner: transferred, Target: targetIdentity}); err != nil {
		t.Fatalf("activate target with Host-transferred owner: %v", err)
	}
	if _, err := callEngine[recorderengine.HandoverResult](t, source, recorderengine.OperationHandoverComplete, recorderengine.HandoverOwnerRequest{RecordingID: generatedRecordingID, Owner: oldOwner}); err != nil {
		t.Fatalf("complete old source: %v", err)
	}

	targetInventory, err := callEngine[recorderengine.InventoryResult](t, target, recorderengine.OperationInventory, nil)
	if err != nil || len(targetInventory.Active) != 1 || targetInventory.Active[0].RecordingID != generatedRecordingID || targetInventory.Active[0].Owner == nil || *targetInventory.Active[0].Owner != transferred {
		t.Fatalf("target inventory after ownership transfer = %#v err=%v", targetInventory, err)
	}
	sourceInventory, err := callEngine[recorderengine.InventoryResult](t, source, recorderengine.OperationInventory, nil)
	if err != nil || len(sourceInventory.Active) != 0 {
		t.Fatalf("source inventory after completion = %#v err=%v", sourceInventory, err)
	}
	if _, err := source.Handle(context.Background(), recorderengine.OperationStop, rawPayload(t, recorderengine.RecordingIDRequest{RecordingID: generatedRecordingID})); err == nil {
		t.Fatal("source Engine stopped a Recording after target ownership transfer")
	}
	if _, err := target.Handle(context.Background(), recorderengine.OperationStop, rawPayload(t, recorderengine.RecordingIDRequest{RecordingID: generatedRecordingID})); err != nil {
		t.Fatalf("stop target-owned recording: %v", err)
	}
}

func TestManagerClientHandoverUDSRoundTripWhenUnixSocketsAreAvailable(t *testing.T) {
	engine, _, owners, _ := newManagedEngineFixture(t, fixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media)); err != nil {
		t.Fatal(err)
	}
	_, claims, _, _ := owners.snapshot()
	if len(claims) != 1 {
		t.Fatalf("Host claims = %d; want 1", len(claims))
	}
	root, err := os.MkdirTemp("/private/tmp", "ir-handover-ipc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	token := []byte("0123456789abcdef0123456789abcdef")
	server, err := runtimeipc.NewServer(filepath.Join(root, "e.sock"), fixtureGenerationID, token, engine)
	if err != nil {
		if errors.Is(err, syscall.EPERM) {
			t.Skip("local test sandbox does not permit Unix domain sockets")
		}
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Engine IPC server stopped with error: %v", err)
		}
	}()
	transport, err := runtimeipc.NewClientForInstance(filepath.Join(root, "e.sock"), fixtureGenerationID, fixtureWorkerInstanceID, token, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client, err := recorderengine.NewManagerClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var snapshot acquire.HandoverSnapshot
	for {
		snapshot, err = client.HandoverSnapshot(context.Background(), generatedRecordingID, claims[0])
		if err == nil {
			if snapshot.Owner != claims[0] {
				t.Fatalf("IPC snapshot owner = %+v; want %+v", snapshot.Owner, claims[0])
			}
			break
		}
		if !errors.Is(err, recorderengine.ErrHandoverOperation) || time.Now().After(deadline) {
			t.Fatalf("IPC snapshot: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := client.PauseForHandover(context.Background(), generatedRecordingID, claims[0]); err != nil {
		t.Fatalf("IPC pause: %v", err)
	}
	stale := claims[0]
	stale.Epoch++
	if err := client.ResumeHandover(context.Background(), generatedRecordingID, stale, snapshot); !errors.Is(err, recorderengine.ErrHandoverStaleOwner) {
		t.Fatalf("IPC stale resume error=%v", err)
	}
	if err := client.ResumeHandover(context.Background(), generatedRecordingID, claims[0], snapshot); err != nil {
		t.Fatalf("IPC unchanged-owner resume: %v", err)
	}
	inventory, err := client.Inventory(context.Background())
	if err != nil || len(inventory.Active) != 1 || inventory.Active[0].Owner == nil || *inventory.Active[0].Owner != claims[0] {
		t.Fatalf("IPC inventory after resume=%#v err=%v", inventory, err)
	}
}

func rawPayload(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
