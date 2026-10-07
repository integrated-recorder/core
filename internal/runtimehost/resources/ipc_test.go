package resources

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimeipc"
)

func TestRuntimeResourceIPCOverPrivateUnixSocket(t *testing.T) {
	coordinator := testCoordinator(t, 32, 24, 2)
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("/tmp", "runtime-resources-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "resources.sock")
	server, err := NewIPCServer(socket, token[:], coordinator)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("resource IPC server shutdown: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("resource IPC server did not stop")
		}
	})

	client, err := NewRuntimeClient(socket, token[:], "engine-a")
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "0123456789abcdef0123456789abcdef"
	if err := client.SetReservation(context.Background(), recordingID, "payload-a", 12); err != nil {
		t.Fatal(err)
	}
	if err := client.AcquireQueue(context.Background(), "job-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.AcquireWriter(context.Background(), "writer-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.AcquireHistoricalScratch(context.Background(), "scratch-a", 2, 11); err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.UsedBytes != 12 || snapshot.QueueObjects != 1 || snapshot.ActiveWriters != 1 || snapshot.HistoricalScratchUsedObjects != 2 || snapshot.HistoricalScratchUsedBytes != 11 {
		t.Fatalf("aggregate snapshot = %+v", snapshot)
	}
	coordinator.mu.Lock()
	_, hasBoundScratchLease := coordinator.historicalScratch[leaseKey("engine-a", "scratch-a")]
	_, hasUnexpectedScratchLease := coordinator.historicalScratch[leaseKey("engine-b", "scratch-a")]
	coordinator.mu.Unlock()
	if !hasBoundScratchLease || hasUnexpectedScratchLease {
		t.Fatal("historical scratch lease was not bound to RuntimeClient owner")
	}
	if err := client.ReportProcessTelemetry(context.Background(), 100, 250, 2, 9, 1.25); err != nil {
		t.Fatalf("report telemetry: %v", err)
	}
	if err := client.ReportProcessTelemetry(context.Background(), 100, 250, 2, 9, 1.25); err != nil {
		t.Fatalf("repeat telemetry report: %v", err)
	}
	coordinator.mu.Lock()
	_, hasBoundOwner := coordinator.telemetryOwners["engine-a"]
	_, hasUnexpectedOwner := coordinator.telemetryOwners["engine-b"]
	coordinator.mu.Unlock()
	if !hasBoundOwner || hasUnexpectedOwner {
		t.Fatalf("telemetry report was not bound to RuntimeClient owner: owners=%v", coordinator.telemetryOwners)
	}
	globalSnapshot, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if globalSnapshot.QueueBytes != 9 || globalSnapshot.OldestAgeSeconds != 1.25 {
		t.Fatalf("IPC queue gauge projection = %+v", globalSnapshot)
	}
	telemetry, err := client.TelemetrySnapshot(context.Background())
	if err != nil {
		t.Fatalf("telemetry snapshot: %v", err)
	}
	if telemetry.Throughput.ReadBytesTotal != 100 || telemetry.Throughput.WriteBytesTotal != 250 || telemetry.ErrorsTotal != 2 {
		t.Fatalf("telemetry report was not bound/idempotent: %+v", telemetry)
	}
	if err := client.ReleaseWriter(context.Background(), "writer-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseHistoricalScratch(context.Background(), "scratch-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseQueue(context.Background(), "job-a"); err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseReservation(context.Background(), recordingID, "payload-a"); err != nil {
		t.Fatal(err)
	}
	if got := coordinator.Snapshot(); got.UsedBytes != 0 || got.QueueObjects != 0 || got.ActiveWriters != 0 || got.HistoricalScratchUsedObjects != 0 || got.HistoricalScratchUsedBytes != 0 {
		t.Fatalf("released leases remain: %+v", got)
	}
}

func TestRuntimeResourceIPCRejectsInvalidAndUnboundedOperations(t *testing.T) {
	handler, err := NewIPCHandler(testCoordinator(t, 32, 24, 2))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		operation string
		payload   string
	}{
		{operation: operationSetReservation, payload: `{"owner_id":"engine-a","recording_id":"../bad","reservation_id":"lease-a","desired_bytes":1}`},
		{operation: operationSetReservation, payload: `{"owner_id":"engine-a","recording_id":"0123456789abcdef0123456789abcdef","reservation_id":"lease-a","desired_bytes":0}`},
		{operation: operationAcquireQueue, payload: `{"owner_id":"engine-a","lease_id":"bad/id"}`},
		{operation: operationAcquireQueue, payload: `{"owner_id":"engine-a","lease_id":"lease-a","extra":true}`},
		{operation: operationAcquireHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"scratch-a","objects":0,"bytes":1}`},
		{operation: operationAcquireHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"scratch-a","objects":1,"bytes":-1}`},
		{operation: operationAcquireHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"scratch-a","objects":5,"bytes":1}`},
		{operation: operationAcquireHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"scratch-a","objects":1,"bytes":2147483649}`},
		{operation: operationAcquireHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"scratch-a","objects":1,"bytes":1,"extra":true}`},
		{operation: operationAcquireHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"scratch-a","objects":1,"bytes":1} {}`},
		{operation: operationReleaseHistoricalScratch, payload: `{"owner_id":"engine-a","lease_id":"bad/id"}`},
		{operation: operationReportTelemetry, payload: `{"read_bytes":1,"write_bytes":2,"errors":0}`},
		{operation: operationReportTelemetry, payload: `{"owner_id":"engine-a","read_bytes":1,"write_bytes":2,"errors":0,"recording_id":"0123456789abcdef0123456789abcdef"}`},
		{operation: operationReportTelemetry, payload: `{"owner_id":"engine-a","read_bytes":1,"write_bytes":2,"errors":0,"queue_bytes":-1}`},
		{operation: "resource_release_owner", payload: `{"owner_id":"engine-a"}`},
	}
	for _, test := range cases {
		t.Run(test.operation+test.payload, func(t *testing.T) {
			if _, err := handler.Handle(context.Background(), test.operation, []byte(test.payload)); err == nil {
				t.Fatal("invalid resource operation unexpectedly succeeded")
			}
		})
	}
	if _, err := handler.Handle(context.Background(), operationSnapshot, []byte(`{"recording_id":"0123456789abcdef0123456789abcdef"}`)); err == nil {
		t.Fatal("snapshot unexpectedly accepted an identity payload")
	}
}

func TestRuntimeResourceOwnerDeathCleanupReclaimsAllLeaseKinds(t *testing.T) {
	c := testCoordinator(t, 64, 48, 2)
	ctx := context.Background()
	if err := c.SetReservation(ctx, "engine-a", "0123456789abcdef0123456789abcdef", "payload-a", 16); err != nil {
		t.Fatal(err)
	}
	if err := c.AcquireQueue(ctx, "engine-a", "job-a"); err != nil {
		t.Fatal(err)
	}
	if err := c.AcquireWriter(ctx, "engine-a", "writer-a"); err != nil {
		t.Fatal(err)
	}
	if err := c.AcquireHistoricalScratch(ctx, "engine-a", "scratch-a", 2, 16); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseOwner("engine-a"); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(); got.UsedBytes != 0 || got.QueueObjects != 0 || got.ActiveWriters != 0 || got.HistoricalScratchUsedObjects != 0 || got.HistoricalScratchUsedBytes != 0 {
		t.Fatalf("owner death did not reclaim leases: %+v", got)
	}
	if err := c.ReleaseOwner("engine-a"); err != nil {
		t.Fatalf("owner cleanup must be idempotent: %v", err)
	}
	if err := c.ReleaseReservation("engine-a", "0123456789abcdef0123456789abcdef", "payload-a"); err != nil {
		t.Fatalf("already absent lease release error = %v", err)
	}
	if err := c.ReleaseHistoricalScratch("engine-a", "scratch-a"); err != nil {
		t.Fatalf("already absent scratch release error = %v", err)
	}
}

func TestHistoricalScratchIPCWaitHonorsCancellation(t *testing.T) {
	c := testCoordinatorWithScratch(t, 32, 24, 2, 1, 10)
	handler, err := NewIPCHandler(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcquireHistoricalScratch(context.Background(), "engine-a", "scratch-a", 1, 10); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	payload := []byte(`{"owner_id":"engine-b","lease_id":"scratch-b","objects":1,"bytes":1}`)
	if _, err := handler.Handle(ctx, operationAcquireHistoricalScratch, payload); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("scratch IPC admission error=%v, want deadline", err)
	}
	if got := c.Snapshot(); got.HistoricalScratchUsedObjects != 1 || got.HistoricalScratchUsedBytes != 10 {
		t.Fatalf("canceled IPC admission changed accounting: %+v", got)
	}
}

type observingHistoricalScratchHandler struct {
	*IPCHandler
	started chan struct{}
}

func (h *observingHistoricalScratchHandler) Handle(ctx context.Context, operation string, payload json.RawMessage) (any, error) {
	if operation == operationAcquireHistoricalScratch {
		select {
		case h.started <- struct{}{}:
		default:
		}
	}
	return h.IPCHandler.Handle(ctx, operation, payload)
}

func TestRuntimeClientHistoricalScratchCancellationDoesNotLeakAdmission(t *testing.T) {
	c := testCoordinatorWithScratch(t, 32, 24, 2, 1, 10)
	if err := c.AcquireHistoricalScratch(context.Background(), "engine-blocker", "scratch-held", 1, 10); err != nil {
		t.Fatal(err)
	}
	base, err := NewIPCHandler(c)
	if err != nil {
		t.Fatal(err)
	}
	handler := &observingHistoricalScratchHandler{IPCHandler: base, started: make(chan struct{}, 1)}
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("/tmp", "runtime-scratch-cancel-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	server, err := runtimeipc.NewServer(filepath.Join(dir, "resources.sock"), IPCIdentity, token[:], handler)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stopServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serveCtx) }()
	t.Cleanup(func() {
		stopServer()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("resource IPC server shutdown: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("resource IPC server did not stop")
		}
	})
	client, err := NewRuntimeClient(filepath.Join(dir, "resources.sock"), token[:], "engine-cancel")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.AcquireHistoricalScratch(ctx, "scratch-cancel", 1, 1) }()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("scratch IPC request did not reach Host")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled acquisition error=%v, want context cancellation", err)
		}
	case <-time.After(3 * historicalScratchAcquireCallTimeout):
		t.Fatal("canceled scratch acquisition did not finish after bounded server request deadline")
	}
	if err := c.ReleaseHistoricalScratch("engine-blocker", "scratch-held"); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(); got.HistoricalScratchUsedObjects != 0 || got.HistoricalScratchUsedBytes != 0 {
		t.Fatalf("canceled client left a late or leaked scratch lease: %+v", got)
	}
}

func TestLeaseReleaseRetriesUncertainOutcomeBoundedly(t *testing.T) {
	ctx := context.Background()
	attempts := 0
	leaseID := "scratch-stable"
	err := retryLeaseRelease(ctx, func() error {
		attempts++
		if leaseID != "scratch-stable" {
			t.Fatalf("lease identity changed between attempts: %q", leaseID)
		}
		if attempts < 3 {
			return &runtimeipc.RemoteError{Code: "deadline_exceeded", Message: "uncertain response"}
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("release retry attempts=%d error=%v, want success on third attempt", attempts, err)
	}

	attempts = 0
	permanent := &runtimeipc.RemoteError{Code: "resource_operation_failed", Message: "release rejected"}
	err = retryLeaseRelease(ctx, func() error {
		attempts++
		return permanent
	})
	if !errors.Is(err, permanent) || attempts != 1 {
		t.Fatalf("definitive release error retried: attempts=%d error=%v", attempts, err)
	}
}

func TestRuntimeResourceIPCRejectsWrongToken(t *testing.T) {
	c := testCoordinator(t, 32, 24, 2)
	var token, wrong [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(wrong[:]); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("/tmp", "runtime-resources-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "resources.sock")
	server, err := NewIPCServer(socket, token[:], c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	client, err := NewRuntimeClient(socket, wrong[:], "engine-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.AcquireQueue(context.Background(), "job-a"); err == nil {
		t.Fatal("client with wrong Host token was accepted")
	}
	if snapshot := c.Snapshot(); snapshot.QueueObjects != 0 {
		t.Fatalf("unauthorized client changed coordinator state: %+v", snapshot)
	}
}
