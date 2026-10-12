package adapterhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

const defaultCallTimeout = 15 * time.Second

type process struct {
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	requests     chan struct{}
	responses    chan readResult
	done         chan struct{}
	killOnce     sync.Once
	shutdownOnce sync.Once
	sequence     atomic.Uint64
	unusable     atomic.Bool
	stderr       *boundedCapture
	startedAt    time.Time
	streamsMu    sync.Mutex
	streams      map[string]*eventSubscription
}

type readResult struct {
	response adapterproto.Response
	err      error
}

type eventSubscription struct {
	batches chan adapterproto.TimedEventBatch
	errors  chan error
}

// SafeProtocolError preserves only protocol error codes whose meaning is safe
// for Core to act on. Adapter-controlled messages and details are deliberately
// discarded at the process boundary.
type SafeProtocolError struct {
	Code string
}

func (e *SafeProtocolError) Error() string { return "adapter returned an error" }

func isSafeProtocolErrorCode(code string) bool {
	switch code {
	case "authentication_required", "interaction_required", "configuration_required":
		return true
	default:
		return false
	}
}

type boundedCapture struct {
	mu    sync.Mutex
	bytes []byte
	limit int
}

func (b *boundedCapture) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if len(b.bytes) < b.limit {
		remaining := b.limit - len(b.bytes)
		if len(p) > remaining {
			p = p[:remaining]
		}
		b.bytes = append(b.bytes, p...)
	}
	return n, nil
}

func startProcess(path string) (*process, error) {
	cmd := exec.Command(path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr := &boundedCapture{limit: 4096}
	cmd.Stderr = stderr
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	p := &process{cmd: cmd, stdin: stdin, requests: make(chan struct{}, 1), responses: make(chan readResult, 1), done: make(chan struct{}), stderr: stderr, startedAt: time.Now(), streams: make(map[string]*eventSubscription)}
	p.requests <- struct{}{}
	go func() {
		p.readLoop(bufio.NewReaderSize(stdout, 32<<10))
		// StdoutPipe requires its reader to finish before Wait reaps the child.
		// Waiting concurrently can close the pipe before a just-written frame
		// has been consumed, losing a valid response from a short-lived adapter.
		_ = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *process) readLoop(reader *bufio.Reader) {
	for {
		frame, err := adapterproto.ReadFrame(reader)
		if err != nil {
			// The caller may have canceled after writing a request and left a
			// complete response queued. Preserve that frame; EOF is also signaled
			// by readerDone/done, so a second queue item is not necessary.
			select {
			case p.responses <- readResult{err: err}:
			default:
			}
			if !errors.Is(err, io.EOF) {
				p.unusable.Store(true)
				p.kill()
				_, _ = io.Copy(io.Discard, reader)
			}
			return
		}
		if frame.Kind == adapterproto.FrameTypeNotification {
			p.routeNotification(*frame.Notification)
			continue
		}
		if frame.Kind != adapterproto.FrameTypeResponse || frame.Response == nil {
			p.unusable.Store(true)
			p.kill()
			_, _ = io.Copy(io.Discard, reader)
			return
		}
		select {
		case p.responses <- readResult{response: *frame.Response}:
		default:
			// More than one frame arrived without a request/response consumer.
			// The stream can no longer be synchronized, so discard this process.
			p.unusable.Store(true)
			p.kill()
			_, _ = io.Copy(io.Discard, reader)
			return
		}
	}
}

func (p *process) routeNotification(notification adapterproto.Notification) {
	if notification.Method != adapterproto.MethodEventsBatch && notification.Method != adapterproto.MethodEventsEnded {
		return
	}
	var identity struct {
		StreamID string `json:"stream_id"`
	}
	if json.Unmarshal(notification.Params, &identity) != nil || identity.StreamID == "" {
		return
	}
	p.streamsMu.Lock()
	subscription := p.streams[identity.StreamID]
	p.streamsMu.Unlock()
	if subscription == nil {
		return
	}
	if notification.Method == adapterproto.MethodEventsEnded {
		select {
		case subscription.errors <- errors.New("adapter timed event stream ended"):
		default:
		}
		return
	}
	var batch adapterproto.TimedEventBatch
	if json.Unmarshal(notification.Params, &batch) != nil || batch.Validate() != nil {
		select {
		case subscription.errors <- errors.New("adapter timed event batch is invalid"):
		default:
		}
		return
	}
	select {
	case subscription.batches <- batch:
	default:
		select {
		case subscription.errors <- errors.New("adapter timed event stream exceeded its bounded buffer"):
		default:
		}
	}
}

func (p *process) subscribeTimedEvents(streamID string) (<-chan adapterproto.TimedEventBatch, <-chan error, func(), error) {
	if streamID == "" || len(streamID) > 128 {
		return nil, nil, nil, errors.New("timed event stream identity is invalid")
	}
	p.streamsMu.Lock()
	defer p.streamsMu.Unlock()
	if len(p.streams) >= 16 {
		return nil, nil, nil, errors.New("adapter timed event stream limit reached")
	}
	if _, exists := p.streams[streamID]; exists {
		return nil, nil, nil, errors.New("timed event stream already exists")
	}
	subscription := &eventSubscription{batches: make(chan adapterproto.TimedEventBatch, 1), errors: make(chan error, 1)}
	p.streams[streamID] = subscription
	var once sync.Once
	cleanup := func() { once.Do(func() { p.streamsMu.Lock(); delete(p.streams, streamID); p.streamsMu.Unlock() }) }
	return subscription.batches, subscription.errors, cleanup, nil
}

func (p *process) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, errors.New("adapter process exited")
	case <-p.requests:
	}
	defer func() {
		select {
		case p.requests <- struct{}{}:
		default:
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("%d", p.sequence.Add(1))
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	request := adapterproto.Request{ProtocolVersion: adapterproto.Version, ID: id, Method: method, Params: paramsJSON}
	writeResult := make(chan error, 1)
	go func() { writeResult <- adapterproto.WriteRequest(p.stdin, request) }()
	select {
	case err = <-writeResult:
		if err != nil {
			p.kill()
			return nil, errors.New("adapter request failed")
		}
	case <-ctx.Done():
		// Writes to a child stdin pipe are not context-aware. Kill the process
		// to interrupt a blocked write, then join the writer before allowing a
		// later call to reuse this process slot.
		p.kill()
		<-writeResult
		return nil, fmt.Errorf("adapter request timed out or was canceled")
	case <-p.done:
		p.kill()
		<-writeResult
		return nil, errors.New("adapter process exited")
	}
	select {
	case <-ctx.Done():
		select {
		case result := <-p.responses:
			return p.consumeResponse(result, id)
		default:
		}
		p.kill()
		return nil, fmt.Errorf("adapter request timed out or was canceled")
	case <-p.done:
		// The process can write a complete response and exit immediately. The
		// read goroutine may have queued that frame just before Wait closed done;
		// prefer the complete frame over the process-exit notification.
		select {
		case result := <-p.responses:
			return p.consumeResponse(result, id)
		default:
			return nil, errors.New("adapter process exited")
		}
	case result := <-p.responses:
		return p.consumeResponse(result, id)
	}
}

func (p *process) consumeResponse(result readResult, id string) (json.RawMessage, error) {
	if result.err != nil {
		p.kill()
		if errors.Is(result.err, io.EOF) {
			return nil, errors.New("adapter closed protocol output")
		}
		if errors.Is(result.err, adapterproto.ErrUnsupportedProtocolVersion) {
			return nil, result.err
		}
		return nil, errors.New("adapter returned malformed protocol output")
	}
	if result.response.ID != id {
		p.kill()
		return nil, errors.New("adapter response id mismatch")
	}
	if result.response.ProtocolVersion != adapterproto.Version {
		p.kill()
		return nil, adapterproto.ErrUnsupportedProtocolVersion
	}
	if result.response.Error != nil {
		if isSafeProtocolErrorCode(result.response.Error.Code) {
			return nil, &SafeProtocolError{Code: result.response.Error.Code}
		}
		return nil, errors.New("adapter returned an error")
	}
	return result.response.Result, nil
}

func (p *process) isUnusable() bool {
	if p == nil || p.unusable.Load() {
		return true
	}
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *process) shutdown() {
	p.shutdownOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = p.call(ctx, adapterproto.MethodShutdown, map[string]any{})
		_ = p.stdin.Close()
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			p.kill()
			<-p.done
		}
	})
}

func (p *process) kill() {
	p.killOnce.Do(func() {
		p.unusable.Store(true)
		// Kill the child before closing its stdin. A request writer can be
		// blocked in a large pipe write while the child keeps the read end open
		// but does not consume input. On some platforms, closing that pipe from
		// another goroutine does not unblock the pending write until the reader
		// exits. Terminating the child first closes the read end and releases the
		// writer; closing stdin then prevents any further writes.
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		_ = p.stdin.Close()
	})
}
