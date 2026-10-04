package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/resources"
)

type sharedRuntimeCoordinator struct {
	coordinator *resources.Coordinator
	owner       string
	onQueue     func()
	onWriter    func()
	failQueue   bool
	failWriter  bool
}

func (c *sharedRuntimeCoordinator) SetReservation(ctx context.Context, recordingID, reservationID string, desiredBytes int64) error {
	return c.coordinator.SetReservation(ctx, c.owner, recordingID, reservationID, desiredBytes)
}
func (c *sharedRuntimeCoordinator) ReleaseReservation(ctx context.Context, recordingID, reservationID string) error {
	return c.coordinator.ReleaseReservation(c.owner, recordingID, reservationID)
}
func (c *sharedRuntimeCoordinator) AcquireQueue(ctx context.Context, leaseID string) error {
	if c.failQueue {
		return errors.New("test coordinator unavailable")
	}
	err := c.coordinator.AcquireQueue(ctx, c.owner, leaseID)
	if err == nil && c.onQueue != nil {
		c.onQueue()
	}
	return err
}
func (c *sharedRuntimeCoordinator) ReleaseQueue(ctx context.Context, leaseID string) error {
	return c.coordinator.ReleaseQueue(c.owner, leaseID)
}
func (c *sharedRuntimeCoordinator) AcquireWriter(ctx context.Context, leaseID string) error {
	if c.failWriter {
		return errors.New("test coordinator unavailable")
	}
	err := c.coordinator.AcquireWriter(ctx, c.owner, leaseID)
	if err == nil && c.onWriter != nil {
		c.onWriter()
	}
	return err
}
func (c *sharedRuntimeCoordinator) ReleaseWriter(ctx context.Context, leaseID string) error {
	return c.coordinator.ReleaseWriter(c.owner, leaseID)
}

func newSharedRuntimeIngest(t *testing.T, options IngestOptions, coordinator *resources.Coordinator, owner string) *IngestService {
	t.Helper()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	client := &sharedRuntimeCoordinator{coordinator: coordinator, owner: owner}
	if err := store.ConfigureRuntimeIngestCoordinator(client); err != nil {
		t.Fatal(err)
	}
	service, err := store.IngestService()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("close ingest service: %v", err)
		}
	})
	return service
}

func smallSharedOptions() IngestOptions {
	options := DefaultIngestOptions()
	options.QueueObjects = 2
	options.GlobalBytes = 2 << 20
	options.PerRecordingBytes = 1536 << 10
	options.MaxPayloadBytes = 1 << 20
	options.Writers = 1
	options.PersistAttempts = 1
	return options
}

func sharedCoordinator(t *testing.T, global, perRecording int64, queue int) *resources.Coordinator {
	t.Helper()
	coordinator, err := resources.New(resources.Limits{GlobalBufferBytes: global, PerRecordingBufferBytes: perRecording, QueueObjects: queue, WriterConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func TestRuntimeCoordinatorEnforcesGlobalAndPerRecordingBytesAcrossServices(t *testing.T) {
	options := smallSharedOptions()
	t.Run("global budget", func(t *testing.T) {
		coordinator := sharedCoordinator(t, 1536<<10, 1536<<10, 4)
		first := newSharedRuntimeIngest(t, options, coordinator, "engine-a")
		second := newSharedRuntimeIngest(t, options, coordinator, "engine-b")
		payload, err := first.ReadPayload(context.Background(), "0123456789abcdef0123456789abcdef", bytes.NewReader(bytes.Repeat([]byte("a"), 900<<10)), 1<<20, 900<<10, 900<<10)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if _, err := second.ReadPayload(ctx, "1123456789abcdef0123456789abcdef", bytes.NewReader(bytes.Repeat([]byte("b"), 900<<10)), 1<<20, 900<<10, 900<<10); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cross-generation global budget error = %v, want deadline", err)
		}
		if got := second.Snapshot(); got.ReservedBytes != 0 || got.BufferUsedBytes != 0 {
			t.Fatalf("canceled global reservation leaked local accounting: %+v", got)
		}
		payload.Release()
		secondPayload, err := second.ReadPayload(context.Background(), "1123456789abcdef0123456789abcdef", bytes.NewReader(bytes.Repeat([]byte("b"), 900<<10)), 1<<20, 900<<10, 900<<10)
		if err != nil {
			t.Fatalf("global admission did not resume after release: %v", err)
		}
		secondPayload.Release()
		if got := coordinator.Snapshot().UsedBytes; got != 0 {
			t.Fatalf("global reservation leaked %d bytes", got)
		}
	})

	t.Run("per recording budget", func(t *testing.T) {
		coordinator := sharedCoordinator(t, 2<<20, 1536<<10, 4)
		first := newSharedRuntimeIngest(t, options, coordinator, "engine-a")
		second := newSharedRuntimeIngest(t, options, coordinator, "engine-b")
		const id = "2123456789abcdef0123456789abcdef"
		payload, err := first.ReadPayload(context.Background(), id, bytes.NewReader(bytes.Repeat([]byte("a"), 900<<10)), 1<<20, 900<<10, 900<<10)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if _, err := second.ReadPayload(ctx, id, bytes.NewReader(bytes.Repeat([]byte("b"), 900<<10)), 1<<20, 900<<10, 900<<10); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cross-generation per-recording budget error = %v, want deadline", err)
		}
		payload.Release()
		if got := coordinator.Snapshot().UsedBytes; got != 0 {
			t.Fatalf("per-recording reservation leaked %d bytes", got)
		}
	})
}

func TestRuntimeCoordinatorAccountsOldAndNewReallocationPeak(t *testing.T) {
	options := smallSharedOptions()
	coordinator := sharedCoordinator(t, 2<<20, 1536<<10, 4)
	service := newSharedRuntimeIngest(t, options, coordinator, "engine-a")
	started, release := make(chan struct{}), make(chan struct{})
	service.reallocationHook = func(oldCapacity, newCapacity int64) {
		if oldCapacity == 256<<10 && newCapacity == 512<<10 {
			if got := coordinator.Snapshot().UsedBytes; got != (256+512)<<10 {
				t.Errorf("shared reallocation peak=%d, want 768 KiB", got)
			}
			close(started)
			<-release
		}
	}
	done := make(chan resultPayload, 1)
	go func() {
		payload, err := service.ReadPayload(context.Background(), "3123456789abcdef0123456789abcdef", bytes.NewReader(bytes.Repeat([]byte("x"), 300<<10)), 1<<20, -1, 0)
		done <- resultPayload{payload: payload, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reallocation hook did not run")
	}
	close(release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if got := coordinator.Snapshot().UsedBytes; got != 512<<10 {
		t.Fatalf("post-reallocation shared reservation=%d, want 512 KiB", got)
	}
	result.payload.Release()
	if got := coordinator.Snapshot().UsedBytes; got != 0 {
		t.Fatalf("shared reservation after release=%d", got)
	}
}

func TestRuntimeCoordinatorReadErrorReleasesReservation(t *testing.T) {
	coordinator := sharedCoordinator(t, 2<<20, 1536<<10, 2)
	service := newSharedRuntimeIngest(t, smallSharedOptions(), coordinator, "engine-a")
	readFailure := errors.New("fixture read failure")
	reader := errorAfterBytes{Reader: bytes.NewReader([]byte("complete-prefix")), err: readFailure}
	if _, err := service.ReadPayload(context.Background(), "4123456789abcdef0123456789abcdef", reader, 1<<20, -1, 0); !errors.Is(err, readFailure) {
		t.Fatalf("ReadPayload error=%v, want fixture failure", err)
	}
	if got := coordinator.Snapshot().UsedBytes; got != 0 {
		t.Fatalf("failed read retained %d shared bytes", got)
	}
}

func TestRuntimeCoordinatorAdmissionFailuresDoNotLeakLeases(t *testing.T) {
	options := smallSharedOptions()
	coordinator := sharedCoordinator(t, 2<<20, 1536<<10, 2)
	queueClient := &sharedRuntimeCoordinator{coordinator: coordinator, owner: "engine-queue", failQueue: true}
	queueService := newSharedRuntimeIngest(t, options, coordinator, "engine-queue")
	queueService.global = queueClient
	queuePayload := mustIngestPayload(t, queueService, "8123456789abcdef0123456789abcdef")
	if err := queueService.Submit(context.Background(), queuePayload, func([]byte) (PayloadResult, error) {
		return PayloadResult{}, nil
	}, func(PayloadResult, error) {}); err == nil {
		t.Fatal("failed global queue admission unexpectedly succeeded")
	}
	queuePayload.Release()
	if got := coordinator.Snapshot(); got.UsedBytes != 0 || got.QueueObjects != 0 {
		t.Fatalf("queue admission failure leaked shared capacity: %+v", got)
	}

	writerClient := &sharedRuntimeCoordinator{coordinator: coordinator, owner: "engine-writer", failWriter: true}
	writerService := newSharedRuntimeIngest(t, options, coordinator, "engine-writer")
	writerService.global = writerClient
	writerPayload := mustIngestPayload(t, writerService, "9123456789abcdef0123456789abcdef")
	completed := make(chan error, 1)
	persisted := make(chan struct{}, 1)
	if err := writerService.Submit(context.Background(), writerPayload, func([]byte) (PayloadResult, error) {
		persisted <- struct{}{}
		return PayloadResult{}, nil
	}, func(_ PayloadResult, err error) { completed <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-completed:
		if err == nil {
			t.Fatal("writer admission failure did not fail the job")
		}
	case <-time.After(time.Second):
		t.Fatal("writer admission failure did not complete")
	}
	select {
	case <-persisted:
		t.Fatal("persistence ran without a global writer permit")
	default:
	}
	if got := coordinator.Snapshot(); got.UsedBytes != 0 || got.QueueObjects != 0 || got.ActiveWriters != 0 {
		t.Fatalf("writer admission failure leaked shared leases: %+v", got)
	}
}

type errorAfterBytes struct {
	*bytes.Reader
	err error
}

func (r errorAfterBytes) Read(p []byte) (int, error) {
	if r.Reader.Len() == 0 {
		return 0, r.err
	}
	return r.Reader.Read(p)
}

func TestRuntimeCoordinatorBoundsPendingQueueAndWritersAcrossServices(t *testing.T) {
	coordinator := sharedCoordinator(t, 2<<20, 1536<<10, 1)
	options := smallSharedOptions()
	queueAcquired := make(chan struct{})
	firstClient := &sharedRuntimeCoordinator{coordinator: coordinator, owner: "engine-a"}
	secondClient := &sharedRuntimeCoordinator{coordinator: coordinator, owner: "engine-b", onQueue: func() { close(queueAcquired) }}
	thirdClient := &sharedRuntimeCoordinator{coordinator: coordinator, owner: "engine-c"}
	first := newSharedRuntimeIngest(t, options, coordinator, "engine-a")
	second := newSharedRuntimeIngest(t, options, coordinator, "engine-b")
	third := newSharedRuntimeIngest(t, options, coordinator, "engine-c")
	// Use explicit wrappers with barriers for observing the global leases.
	first.global = firstClient
	second.global = secondClient
	third.global = thirdClient

	firstStarted, releaseFirst := make(chan struct{}), make(chan struct{})
	secondStarted, releaseSecond := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
		select {
		case <-releaseSecond:
		default:
			close(releaseSecond)
		}
	}()
	thirdStarted := make(chan struct{})
	firstDone, secondDone, thirdDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	firstPayload := mustIngestPayload(t, first, "5123456789abcdef0123456789abcdef")
	if err := first.Submit(context.Background(), firstPayload, func([]byte) (PayloadResult, error) {
		close(firstStarted)
		<-releaseFirst
		return PayloadResult{}, nil
	}, func(PayloadResult, error) { close(firstDone) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first global writer did not start")
	}

	secondPayload := mustIngestPayload(t, second, "6123456789abcdef0123456789abcdef")
	secondSubmitted := make(chan error, 1)
	go func() {
		secondSubmitted <- second.Submit(context.Background(), secondPayload, func([]byte) (PayloadResult, error) {
			close(secondStarted)
			<-releaseSecond
			return PayloadResult{}, nil
		}, func(PayloadResult, error) { close(secondDone) })
	}()
	select {
	case <-queueAcquired:
	case <-time.After(time.Second):
		t.Fatal("second pending queue lease was not acquired")
	}
	if err := <-secondSubmitted; err != nil {
		t.Fatal(err)
	}
	if got := coordinator.Snapshot().QueueObjects; got != 1 {
		t.Fatalf("pending global queue objects=%d, want 1", got)
	}

	thirdPayload := mustIngestPayload(t, third, "7123456789abcdef0123456789abcdef")
	thirdSubmitted := make(chan error, 1)
	go func() {
		thirdSubmitted <- third.Submit(context.Background(), thirdPayload, func([]byte) (PayloadResult, error) {
			close(thirdStarted)
			return PayloadResult{}, nil
		}, func(PayloadResult, error) { close(thirdDone) })
	}()
	select {
	case err := <-thirdSubmitted:
		t.Fatalf("third queue item exceeded global queue capacity: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := coordinator.Snapshot().ActiveWriters; got != 1 {
		t.Fatalf("global writer concurrency=%d, want 1", got)
	}

	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second writer did not resume after global permit release")
	}
	if err := <-thirdSubmitted; err != nil {
		t.Fatalf("third queue admission did not resume: %v", err)
	}
	if got := coordinator.Snapshot().ActiveWriters; got != 1 {
		t.Fatalf("writer permit multiplied across services: %d", got)
	}
	close(releaseSecond)
	select {
	case <-thirdStarted:
	case <-time.After(time.Second):
		t.Fatal("third writer did not resume after second release")
	}
	for name, done := range map[string]<-chan struct{}{"first": firstDone, "second": secondDone, "third": thirdDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s persist job did not complete", name)
		}
	}
	if got := coordinator.Snapshot(); got.QueueObjects != 0 || got.ActiveWriters != 0 {
		t.Fatalf("queue/writer leases leaked after jobs completed: %+v", got)
	}
}

func mustIngestPayload(t *testing.T, service *IngestService, recordingID string) *IngestPayload {
	t.Helper()
	payload, err := service.ReadPayload(context.Background(), recordingID, bytes.NewReader([]byte("payload")), 1<<20, 7, 7)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
