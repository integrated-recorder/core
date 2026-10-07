package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newSmallIngest(t *testing.T, attempts int) *IngestService {
	t.Helper()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultIngestOptions()
	options.QueueObjects = 2
	options.GlobalBytes = 16
	options.PerRecordingBytes = 8
	options.MaxPayloadBytes = 8
	options.PersistAttempts = attempts
	options.RetryBase = 10 * time.Millisecond
	if err := store.ConfigureIngestOptions(options); err != nil {
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

func TestIngestByteBudgetsGrowUnderHintAndResumeAfterRelease(t *testing.T) {
	service := newSmallIngest(t, 1)
	first, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("12345678")), 8, -1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 8 || got.ReservedBytes != 8 {
		t.Fatalf("underestimated hint did not grow its reservation: %#v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = service.ReadPayload(ctx, "recording-a", bytes.NewReader([]byte("x")), 8, -1, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-recording full budget error = %v", err)
	}
	first.Release()
	second, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("x")), 8, -1, 1)
	if err != nil {
		t.Fatalf("admission did not resume after release: %v", err)
	}
	second.Release()
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
		t.Fatalf("budget leaked after release: %#v", got)
	}
}

func TestIngestGrowthAccountsForOldAndNewBackingArrays(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const budget = 384 << 10 // 128 KiB + 256 KiB maximum growth peak.
	service, err := NewIngestService(store, IngestOptions{
		QueueObjects: 2, GlobalBytes: budget, PerRecordingBytes: budget,
		MaxPayloadBytes: 256 << 10, Writers: 1, PersistAttempts: 1,
	})
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
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var transientReserved int64
	service.reallocationHook = func(oldCapacity, newCapacity int64) {
		if oldCapacity == ingestAllocationChunk {
			snapshot := service.Snapshot()
			transientReserved = snapshot.ReservedBytes
			close(started)
			<-release
		}
	}
	reader := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("x"), int(ingestAllocationChunk))), bytes.NewReader([]byte("y")))
	readDone := make(chan resultPayload, 1)
	go func() {
		payload, readErr := service.ReadPayload(context.Background(), "growth", reader, 256<<10, -1, 0)
		readDone <- resultPayload{payload: payload, err: readErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("growth boundary hook was not reached")
	}
	if want := ingestAllocationChunk + 2*ingestAllocationChunk; transientReserved != want {
		t.Fatalf("transient old+new reservation=%d want=%d", transientReserved, want)
	}
	if transientReserved > budget || transientReserved > service.options.PerRecordingBytes {
		t.Fatalf("transient reservation exceeded a configured bound: %d", transientReserved)
	}
	close(release)
	got := <-readDone
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.payload == nil || len(got.payload.Bytes()) != int(ingestAllocationChunk)+1 {
		t.Fatalf("grown payload length = %v", got.payload)
	}
	if snapshot := service.Snapshot(); snapshot.ReservedBytes != 2*ingestAllocationChunk || snapshot.BufferUsedBytes != ingestAllocationChunk+1 {
		t.Fatalf("post-growth accounting = %#v", snapshot)
	}
	got.payload.Release()
	if snapshot := service.Snapshot(); snapshot.ReservedBytes != 0 || snapshot.BufferUsedBytes != 0 {
		t.Fatalf("released growth accounting leaked: %#v", snapshot)
	}
}

func TestIngestOptionsRequireMaximumReallocationPeak(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewIngestService(store, IngestOptions{
		QueueObjects: 2, GlobalBytes: 256 << 10, PerRecordingBytes: 256 << 10,
		MaxPayloadBytes: 256 << 10, Writers: 1, PersistAttempts: 1,
	})
	if err == nil {
		t.Fatal("configuration below the old+new growth peak was accepted")
	}
}

func TestValidateIngestOptionsRequiresReachableMetricsRetention(t *testing.T) {
	valid := []struct {
		name      string
		interval  time.Duration
		retention time.Duration
	}{
		{"1s/5m", time.Second, 5 * time.Minute},
		{"5s/5m", 5 * time.Second, 5 * time.Minute},
		{"7s/5m1s", 7 * time.Second, 5*time.Minute + time.Second},
		{"10s/5m", 10 * time.Second, 5 * time.Minute},
		{"1m/20m", time.Minute, 20 * time.Minute},
		{"1h/20h", time.Hour, 20 * time.Hour},
		{"1h/24h", time.Hour, 24 * time.Hour},
	}
	for _, test := range valid {
		t.Run(test.name+" valid", func(t *testing.T) {
			options := DefaultIngestOptions()
			options.SampleInterval, options.MetricsRetention = test.interval, test.retention
			if err := ValidateIngestOptions(options); err != nil {
				t.Fatalf("ValidateIngestOptions() = %v, want valid configuration", err)
			}
		})
	}

	invalid := []struct {
		name      string
		interval  time.Duration
		retention time.Duration
		want      string
	}{
		{"1s/1s", time.Second, time.Second, "at least 5 minutes"},
		{"7s/5m", 7 * time.Second, 5 * time.Minute, "at least 5 minutes 1 second"},
		{"7s/5m1s-minus-1ms", 7 * time.Second, 5*time.Minute + time.Second - time.Millisecond, "at least 5 minutes 1 second"},
		{"1s/5m-minus-1ms", time.Second, 5*time.Minute - time.Millisecond, "at least 5 minutes"},
		{"5s/5m-minus-1ms", 5 * time.Second, 5*time.Minute - time.Millisecond, "at least 5 minutes"},
		{"10s/5m-minus-1ms", 10 * time.Second, 5*time.Minute - time.Millisecond, "at least 5 minutes"},
		{"1m/5m", time.Minute, 5 * time.Minute, "at least 20 minutes"},
		{"1m/20m-minus-1ms", time.Minute, 20*time.Minute - time.Millisecond, "at least 20 minutes"},
		{"1h/1h", time.Hour, time.Hour, "at least 20 hours"},
		{"1m cadence message is singular", time.Minute, 5 * time.Minute, "for a sampling interval of 1 minute"},
		{"1m/19m59s", time.Minute, 19*time.Minute + 59*time.Second, "at least 20 minutes"},
		{"1h/20h-minus-1ms", time.Hour, 20*time.Hour - time.Millisecond, "at least 20 hours"},
		{"1h/19h59m59s", time.Hour, 19*time.Hour + 59*time.Minute + 59*time.Second, "at least 20 hours"},
	}
	for _, test := range invalid {
		t.Run(test.name+" invalid", func(t *testing.T) {
			options := DefaultIngestOptions()
			options.SampleInterval, options.MetricsRetention = test.interval, test.retention
			if err := ValidateIngestOptions(options); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateIngestOptions() error = %v, want message containing %q", err, test.want)
			}
		})
	}
}

func TestStoreIngestOptionsApplyBeforeConstructionAndCannotResizeService(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultIngestOptions()
	options.QueueObjects = 4
	options.GlobalBytes = 8 << 20
	options.PerRecordingBytes = 8 << 20
	options.MaxPayloadBytes = 4 << 20
	options.SampleInterval = time.Second
	options.MetricsRetention = 5 * time.Minute
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	service, err := store.IngestService()
	if err != nil {
		t.Fatal(err)
	}
	if got := service.Options(); got != options {
		t.Fatalf("service options = %#v, want %#v", got, options)
	}
	if cap(service.jobs) != options.QueueObjects {
		t.Fatalf("persist queue capacity = %d, want configured %d", cap(service.jobs), options.QueueObjects)
	}
	if err := store.ConfigureIngestOptions(DefaultIngestOptions()); err == nil {
		t.Fatal("reconfiguring after service construction succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIngestUsesConfiguredMaximumPayloadAndSaturatingRetryBackoff(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultIngestOptions()
	options.MaxPayloadBytes = 256 << 10
	options.GlobalBytes = 768 << 10
	options.PerRecordingBytes = 768 << 10
	options.RetryBase = 10 * time.Millisecond
	options.RetryMaxBackoff = 25 * time.Millisecond
	if err := store.ConfigureIngestOptions(options); err != nil {
		t.Fatal(err)
	}
	service, err := store.IngestService()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("close ingest service: %v", err)
		}
	}()
	if _, err := service.ReadPayload(context.Background(), "limited", bytes.NewReader([]byte("too large")), options.MaxPayloadBytes+1, -1, 0); !errors.Is(err, ErrIngestTooLarge) {
		t.Fatalf("ReadPayload above configured max = %v", err)
	}
	for _, test := range []struct {
		index int
		want  time.Duration
	}{{0, 10 * time.Millisecond}, {1, 20 * time.Millisecond}, {2, 25 * time.Millisecond}, {3, 25 * time.Millisecond}, {1000, 25 * time.Millisecond}} {
		if got := retryBackoff(options.RetryBase, options.RetryMaxBackoff, test.index); got != test.want {
			t.Errorf("retryBackoff(index=%d) = %s, want %s", test.index, got, test.want)
		}
	}
}

func TestCapacityFromBlocksRejectsInvalidAndOverflowingStatfs(t *testing.T) {
	got, ok := capacityFromBlocks(10, 3, 2, 4096)
	if !ok || got.TotalBytes != 40960 || got.UsedBytes != 28672 || got.AvailableBytes != 8192 {
		t.Fatalf("valid capacity = %#v, valid=%v", got, ok)
	}
	for name, input := range map[string][4]uint64{
		"free exceeds total":                {2, 3, 1, 4096},
		"available exceeds free":            {10, 3, 4, 4096},
		"zero block size":                   {2, 1, 1, 0},
		"total multiplication overflow":     {^uint64(0), 0, 0, 2},
		"used multiplication overflow":      {^uint64(0) / 2, ^uint64(0)/2 - 1, 0, 4},
		"available multiplication overflow": {^uint64(0) / 2, 0, ^uint64(0) / 2, 4},
	} {
		t.Run(name, func(t *testing.T) {
			capacity, valid := capacityFromBlocks(input[0], input[1], input[2], input[3])
			if valid || capacity != (PoolCapacity{}) {
				t.Fatalf("invalid capacity = %#v, valid=%v", capacity, valid)
			}
		})
	}
}

func TestKnownSmallHintsAllowFourSameRecordingCaptures(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewIngestService(store, IngestOptions{
		QueueObjects: 4, GlobalBytes: DefaultIngestGlobalBytes,
		PerRecordingBytes: DefaultIngestPerRecordingBytes,
		MaxPayloadBytes:   DefaultMaxIngestPayloadBytes, Writers: 1,
		PersistAttempts: 1, RetryBase: 10 * time.Millisecond,
	})
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

	start := make(chan struct{})
	type result struct {
		payload *IngestPayload
		err     error
	}
	results := make(chan result, 4)
	for i := 0; i < cap(results); i++ {
		go func() {
			<-start
			payload, readErr := service.ReadPayload(context.Background(), "same-recording", bytes.NewReader([]byte("data")), DefaultMaxIngestPayloadBytes, -1, 4)
			results <- result{payload: payload, err: readErr}
		}()
	}
	close(start)
	var payloads []*IngestPayload
	for i := 0; i < cap(results); i++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("concurrent known-length capture failed: %v", got.err)
			}
			payloads = append(payloads, got.payload)
		case <-time.After(time.Second):
			t.Fatal("four known-length bodies did not finish concurrently")
		}
	}
	if snapshot := service.Snapshot(); snapshot.BufferUsedBytes != 16 {
		t.Fatalf("buffered bytes = %d, want 16", snapshot.BufferUsedBytes)
	}
	for _, payload := range payloads {
		payload.Release()
	}
}

func TestUnknownLengthReservesIncrementallyAsBytesArrive(t *testing.T) {
	service := newSmallIngest(t, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	readCount := 0
	reader := readerFunc(func(p []byte) (int, error) {
		readCount++
		if readCount == 1 {
			return copy(p, []byte("data")), nil
		}
		if readCount > 2 {
			return 0, io.EOF
		}
		close(started)
		<-release
		return 0, io.EOF
	})
	firstDone := make(chan resultPayload, 1)
	go func() {
		payload, err := service.ReadPayload(context.Background(), "recording-a", reader, 8, -1, 0)
		firstDone <- resultPayload{payload: payload, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first unknown-length body did not start reading")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := service.ReadPayload(ctx, "recording-a", bytes.NewReader([]byte("x")), 8, -1, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second unknown body did not backpressure on full reservation: %v", err)
	}
	close(release)
	got := <-firstDone
	if got.err != nil {
		t.Fatal(got.err)
	}
	got.payload.Release()
}

type resultPayload struct {
	payload *IngestPayload
	err     error
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestIngestGlobalBudgetCancellationAndOversizeRelease(t *testing.T) {
	service := newSmallIngest(t, 1)
	first, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("12345678")), 8, -1, 8)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.ReadPayload(context.Background(), "recording-b", bytes.NewReader([]byte("abcdefgh")), 8, -1, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = service.ReadPayload(ctx, "recording-c", bytes.NewReader([]byte("z")), 8, -1, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("global full budget error = %v", err)
	}
	first.Release()
	second.Release()
	if _, err = service.ReadPayload(context.Background(), "recording-c", bytes.NewReader([]byte("123456789")), 8, -1, 1); !errors.Is(err, ErrIngestTooLarge) {
		t.Fatalf("oversized source error = %v", err)
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
		t.Fatalf("budget leaked after cancellation/error: %#v", got)
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestIngestReadErrorReleasesReservation(t *testing.T) {
	service := newSmallIngest(t, 1)
	want := errors.New("read failed")
	if _, err := service.ReadPayload(context.Background(), "recording-a", failingReader{err: want}, 8, -1, 8); !errors.Is(err, want) {
		t.Fatalf("read error = %v", err)
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
		t.Fatalf("reservation leaked after reader error: %#v", got)
	}
}

func TestSubmitCompletedPayloadIgnoresAcquisitionCancellationWhileQueueFull(t *testing.T) {
	service := newSmallIngest(t, 1)
	writerStarted, releaseWriter := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-releaseWriter:
		default:
			close(releaseWriter)
		}
	}()
	if err := service.SubmitCommit(context.Background(), "blocker", func() error {
		close(writerStarted)
		<-releaseWriter
		return nil
	}, func(error) {}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writerStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking commit did not start")
	}
	for i := 0; i < 2; i++ {
		if err := service.SubmitCommit(context.Background(), "queued", func() error { return nil }, func(error) {}); err != nil {
			t.Fatalf("fill queue %d: %v", i, err)
		}
	}
	if got := service.Snapshot().QueueObjects; got != 2 {
		t.Fatalf("queue objects=%d want=2", got)
	}
	payload, err := service.ReadPayload(context.Background(), "recording", bytes.NewReader([]byte("complete")), 8, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	completed := make(chan error, 1)
	var persistCalls atomic.Int32
	submitDone := make(chan error, 1)
	go func() {
		close(started)
		submitDone <- service.Submit(ctx, payload, func(data []byte) (PayloadResult, error) {
			persistCalls.Add(1)
			if !bytes.Equal(data, []byte("complete")) {
				return PayloadResult{}, errors.New("payload changed")
			}
			return PayloadResult{Size: int64(len(data))}, nil
		}, func(_ PayloadResult, persistErr error) { completed <- persistErr })
	}()
	<-started
	select {
	case err := <-submitDone:
		t.Fatalf("full queue unexpectedly accepted payload: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-submitDone:
		t.Fatalf("acquisition cancellation discarded complete payload: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := service.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		closeCancel()
		t.Fatalf("Close while a pre-existing payload waited returned %v, want deadline", err)
	}
	closeCancel()
	if err := service.SubmitCommit(context.Background(), "new-admission", func() error { return nil }, func(error) {}); !errors.Is(err, ErrIngestClosed) {
		t.Fatalf("post-close submission = %v, want ErrIngestClosed", err)
	}
	close(releaseWriter)
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatalf("submit after writer capacity returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("completed payload did not enter queue after capacity returned")
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("completed payload did not persist")
	}
	if persistCalls.Load() != 1 {
		t.Fatalf("completed payload persisted %d times, want exactly once", persistCalls.Load())
	}
	select {
	case <-service.Done():
	case <-time.After(time.Second):
		t.Fatal("close coordinator did not join after the accepted payload drained")
	}
}

func TestIngestRetriesSameVolatileBytesAndReleasesOnce(t *testing.T) {
	service := newSmallIngest(t, 3)
	payload, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("source")), 8, -1, 6)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	completed := make(chan error, 1)
	err = service.Submit(context.Background(), payload, func(data []byte) (PayloadResult, error) {
		if !bytes.Equal(data, []byte("source")) {
			t.Fatalf("retry bytes = %q", data)
		}
		if attempts.Add(1) == 1 {
			return PayloadResult{}, errors.New("transient storage error")
		}
		return PayloadResult{Size: int64(len(data)), SHA256: "digest"}, nil
	}, func(_ PayloadResult, persistErr error) { completed <- persistErr })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-completed:
	case <-time.After(time.Second):
		t.Fatal("persistence callback did not complete")
	}
	if err != nil || attempts.Load() != 2 {
		t.Fatalf("attempts=%d err=%v", attempts.Load(), err)
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 || got.StorageErrorsTotal != 0 {
		t.Fatalf("successful retry accounting = %#v", got)
	}
}

func TestDefaultPersistAttemptsMeansFiveTotalInvocations(t *testing.T) {
	service := newSmallIngest(t, DefaultPersistAttempts)
	payload, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("source")), 8, -1, 6)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	completed := make(chan error, 1)
	if err := service.Submit(context.Background(), payload, func([]byte) (PayloadResult, error) {
		attempts.Add(1)
		return PayloadResult{}, errors.New("permanent storage error")
	}, func(_ PayloadResult, persistErr error) { completed <- persistErr }); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-completed:
	case <-time.After(time.Second):
		t.Fatal("persistence callback did not complete")
	}
	if err == nil || attempts.Load() != int32(DefaultPersistAttempts) {
		t.Fatalf("permanent failure calls=%d err=%v, want exactly %d total calls", attempts.Load(), err, DefaultPersistAttempts)
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
		t.Fatalf("permanent failure did not release volatile payload: %#v", got)
	}
}

func TestIngestPermanentStorageFailureReleasesVolatileBytes(t *testing.T) {
	service := newSmallIngest(t, 2)
	payload, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("source")), 8, -1, 6)
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	firstCause := errors.New("synthetic first backend failure")
	finalCause := errors.New("synthetic final backend failure")
	var calls atomic.Int32
	err = service.Submit(context.Background(), payload, func([]byte) (PayloadResult, error) {
		if calls.Add(1) == 1 {
			return PayloadResult{}, firstCause
		}
		return PayloadResult{}, finalCause
	}, func(_ PayloadResult, persistErr error) { completed <- persistErr })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-completed:
	case <-time.After(time.Second):
		t.Fatal("failed persistence callback did not complete")
	}
	if !errors.Is(err, ErrCanonicalCommitFailed) {
		t.Fatalf("persistence classification = %v, want ErrCanonicalCommitFailed", err)
	}
	if !errors.Is(err, finalCause) || errors.Is(err, firstCause) {
		t.Fatalf("persistence error chain = %v, want only final cause", err)
	}
	var attemptErr attemptCountedError
	if !errors.As(err, &attemptErr) || attemptErr.Attempts() != 2 {
		t.Fatalf("persistence attempts = %v, want 2", err)
	}
	if calls.Load() != 2 || !strings.Contains(err.Error(), "after bounded retries (2 attempts)") || !strings.Contains(err.Error(), finalCause.Error()) || strings.Contains(err.Error(), firstCause.Error()) {
		t.Fatalf("persistence diagnostic = %q, calls=%d", err, calls.Load())
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 || got.StorageErrorsTotal != 1 {
		t.Fatalf("failed persistence accounting = %#v", got)
	}
}

type failingIngestCoordinator struct {
	queueErr      error
	writerErr     error
	queueRelease  atomic.Int32
	writerRelease atomic.Int32
}

func (c *failingIngestCoordinator) SetReservation(context.Context, string, string, int64) error {
	return nil
}
func (c *failingIngestCoordinator) ReleaseReservation(context.Context, string, string) error {
	return nil
}
func (c *failingIngestCoordinator) AcquireQueue(context.Context, string) error { return c.queueErr }
func (c *failingIngestCoordinator) ReleaseQueue(context.Context, string) error {
	c.queueRelease.Add(1)
	return nil
}
func (c *failingIngestCoordinator) AcquireWriter(context.Context, string) error { return c.writerErr }
func (c *failingIngestCoordinator) ReleaseWriter(context.Context, string) error {
	c.writerRelease.Add(1)
	return nil
}

func newSmallIngestWithCoordinator(t *testing.T, coordinator RuntimeIngestCoordinator) *IngestService {
	t.Helper()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultIngestOptions()
	options.QueueObjects = 2
	options.GlobalBytes = 16
	options.PerRecordingBytes = 8
	options.MaxPayloadBytes = 8
	options.RetryBase = 10 * time.Millisecond
	service, err := newIngestService(store, options, coordinator)
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

func TestIngestGlobalLeaseAcquisitionErrorsRetainCause(t *testing.T) {
	t.Run("queue", func(t *testing.T) {
		cause := errors.New("synthetic queue acquisition failure")
		coordinator := &failingIngestCoordinator{queueErr: cause}
		service := newSmallIngestWithCoordinator(t, coordinator)
		err := service.SubmitCommit(context.Background(), "queue-failure", func() error { return nil }, func(error) {})
		if !errors.Is(err, cause) || !errors.Is(err, ErrIngestCoordinatorUnavailable) || !strings.Contains(err.Error(), "storage coordinator acquire queue failed") {
			t.Fatalf("queue acquisition error = %v, want wrapped cause and classification", err)
		}
		if coordinator.queueRelease.Load() != 1 || service.Snapshot().QueueObjects != 0 {
			t.Fatalf("queue lease cleanup = releases:%d snapshot:%#v", coordinator.queueRelease.Load(), service.Snapshot())
		}
	})

	t.Run("writer", func(t *testing.T) {
		cause := errors.New("synthetic writer acquisition failure")
		coordinator := &failingIngestCoordinator{writerErr: cause}
		service := newSmallIngestWithCoordinator(t, coordinator)
		completed := make(chan error, 1)
		if err := service.SubmitCommit(context.Background(), "writer-failure", func() error { return nil }, func(err error) { completed <- err }); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-completed:
			if !errors.Is(err, cause) || !errors.Is(err, ErrIngestCoordinatorUnavailable) || errors.Is(err, ErrCanonicalCommitFailed) || !strings.Contains(err.Error(), "global storage writer is unavailable") {
				t.Fatalf("writer acquisition error = %v, want wrapped cause and classification", err)
			}
			var details IngestFailureDetails
			if !errors.As(err, &details) || details.CurrentJobKind() != IngestJobKindCanonicalPayload ||
				details.FirstFailureJobKind() != IngestJobKindCanonicalPayload || details.CurrentAttempts() != 0 || details.FirstFailureAttempts() != 0 {
				t.Fatalf("writer acquisition details = %#v, want canonical job with zero persistence attempts", details)
			}
		case <-time.After(time.Second):
			t.Fatal("writer acquisition failure did not complete job")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if coordinator.queueRelease.Load() != 1 || coordinator.writerRelease.Load() != 1 {
			t.Fatalf("writer lease cleanup = queue:%d writer:%d", coordinator.queueRelease.Load(), coordinator.writerRelease.Load())
		}
	})
}

func TestIngestCloseDeadlineCanBeRepeatedAfterWriterUnblocks(t *testing.T) {
	service := newSmallIngest(t, 1)
	payload, err := service.ReadPayload(context.Background(), "recording-a", bytes.NewReader([]byte("data")), 8, -1, 4)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	if err = service.Submit(context.Background(), payload, func(data []byte) (PayloadResult, error) {
		close(started)
		<-release
		return PayloadResult{Size: int64(len(data))}, nil
	}, func(PayloadResult, error) {}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = service.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first close error = %v", err)
	}
	close(release)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err = service.Close(ctx2); err != nil {
		t.Fatalf("repeated close error = %v", err)
	}
	if got := service.Snapshot(); got.BufferUsedBytes != 0 || got.ReservedBytes != 0 {
		t.Fatalf("closed service retained volatile bytes: %#v", got)
	}
}

var _ io.Reader = failingReader{}
