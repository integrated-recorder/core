package storageproto

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestProviderRequestGateDefaultIsFullySerialized(t *testing.T) {
	if !validConcurrentReadLimit(0) || !validConcurrentReadLimit(1) || !validConcurrentReadLimit(MaxConcurrentReadStreams) || validConcurrentReadLimit(-1) || validConcurrentReadLimit(MaxConcurrentReadStreams+1) {
		t.Fatal("Serve accepted a concurrent read limit outside its supported range")
	}
	gate := newProviderRequestGate(0)
	ctx := context.Background()
	readAsDefault := concurrentObjectRead(0, http.MethodGet, "/v1/objects")
	if readAsDefault {
		t.Fatal("default Serve configuration enabled concurrent object reads")
	}

	releaseFirst, err := gate.acquire(ctx, readAsDefault)
	if err != nil {
		t.Fatal(err)
	}
	waiting := gate.changed
	secondResult := make(chan error, 1)
	go func() {
		release, acquireErr := gate.acquire(ctx, false)
		if release != nil {
			release()
		}
		secondResult <- acquireErr
	}()
	<-waiting // the second request registered as a waiting exclusive operation
	select {
	case err := <-secondResult:
		t.Fatalf("default provider admitted a concurrent operation: %v", err)
	default:
	}
	releaseFirst()
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("serialized operation after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serialized operation did not proceed after release")
	}
}

func TestProviderRequestGateBoundsConcurrentReadsAndExcludesWriters(t *testing.T) {
	if !concurrentObjectRead(4, http.MethodGet, "/v1/objects") {
		t.Fatal("enabled object GET/range stream was not classified as concurrent read")
	}
	for _, input := range []struct {
		max    int
		method string
		path   string
	}{
		{max: 0, method: http.MethodGet, path: "/v1/objects"},
		{max: 4, method: http.MethodGet, path: "/v1/list"},
		{max: 4, method: http.MethodHead, path: "/v1/objects"},
		{max: 4, method: http.MethodPut, path: "/v1/objects"},
	} {
		if concurrentObjectRead(input.max, input.method, input.path) {
			t.Fatalf("unexpected concurrent-read classification: %+v", input)
		}
	}

	gate := newProviderRequestGate(2)
	readAsConcurrent := concurrentObjectRead(2, http.MethodGet, "/v1/objects")
	releaseFirst, err := gate.acquire(context.Background(), readAsConcurrent)
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond, err := gate.acquire(context.Background(), readAsConcurrent)
	if err != nil {
		releaseFirst()
		t.Fatal(err)
	}
	if got := gate.activeReaders(); got != 2 {
		t.Fatalf("active reads=%d, want configured limit 2", got)
	}
	thirdReadCtx, cancelThirdRead := context.WithTimeout(context.Background(), 25*time.Millisecond)
	thirdReadRelease, thirdReadErr := gate.acquire(thirdReadCtx, true)
	cancelThirdRead()
	if thirdReadRelease != nil {
		thirdReadRelease()
		t.Fatal("provider admitted a read beyond its configured stream limit")
	}
	if !errors.Is(thirdReadErr, context.DeadlineExceeded) {
		t.Fatalf("read beyond configured limit returned %v, want context deadline", thirdReadErr)
	}

	// Register an exclusive PUT/control operation while both streams are open.
	// The gate's notification makes this deterministic without timing sleeps.
	writerWaiting := gate.changed
	writerResult := make(chan func(), 1)
	writerErr := make(chan error, 1)
	go func() {
		release, acquireErr := gate.acquire(context.Background(), false)
		if acquireErr != nil {
			writerErr <- acquireErr
			return
		}
		writerResult <- release
	}()
	<-writerWaiting

	// Once an exclusive operation is queued, new reads wait behind it.
	readCtx, cancelRead := context.WithTimeout(context.Background(), 25*time.Millisecond)
	blockedReadRelease, blockedReadErr := gate.acquire(readCtx, true)
	cancelRead()
	if blockedReadRelease != nil {
		blockedReadRelease()
		t.Fatal("read admitted ahead of a queued exclusive operation")
	}
	if !errors.Is(blockedReadErr, context.DeadlineExceeded) {
		t.Fatalf("read waiting behind exclusive operation returned %v, want context deadline", blockedReadErr)
	}

	releaseFirst()
	gate.mu.Lock()
	writerStartedBeforeLastReaderClosed := gate.writer
	gate.mu.Unlock()
	if writerStartedBeforeLastReaderClosed {
		t.Fatal("exclusive operation began while one read stream remained active")
	}
	releaseSecond()
	var releaseWriter func()
	select {
	case err := <-writerErr:
		t.Fatalf("exclusive operation failed: %v", err)
	case releaseWriter = <-writerResult:
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive operation did not proceed after streams closed")
	}

	// Non-GET operations remain mutually serialized while the exclusive owner
	// is active.
	secondWriterWaiting := gate.changed
	secondWriterResult := make(chan error, 1)
	go func() {
		release, acquireErr := gate.acquire(context.Background(), false)
		if release != nil {
			release()
		}
		secondWriterResult <- acquireErr
	}()
	<-secondWriterWaiting
	select {
	case err := <-secondWriterResult:
		t.Fatalf("two exclusive operations overlapped: %v", err)
	default:
	}
	releaseWriter()
	select {
	case err := <-secondWriterResult:
		if err != nil {
			t.Fatalf("second exclusive operation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second exclusive operation did not proceed after release")
	}
}

func (g *providerRequestGate) activeReaders() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.readers
}
