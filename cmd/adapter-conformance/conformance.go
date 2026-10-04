package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
)

type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Error  string `json:"error,omitempty"`
}

type Report struct {
	Passed          bool    `json:"passed"`
	ProtocolVersion int     `json:"protocol_version"`
	AdapterID       string  `json:"adapter_id,omitempty"`
	Checks          []Check `json:"checks"`
}

func runConformance(binary string, timeout time.Duration) Report {
	report := Report{Passed: true, ProtocolVersion: adapterproto.Version, Checks: []Check{}}
	add := func(name string, err string) {
		check := Check{Name: name, Passed: err == "", Error: err}
		report.Checks = append(report.Checks, check)
		if !check.Passed {
			report.Passed = false
		}
	}

	if err := validateExecutable(binary); err != nil {
		add("executable", err.Error())
		return report
	}
	add("executable", "")

	first, err := startAdapter(binary)
	if err != nil {
		add("process_launch", "adapter process could not be started")
		return report
	}
	add("process_launch", "")

	firstDescriptor, err := requestDescriptor(first, timeout)
	if err != nil {
		add("describe", describeError(err))
		first.abort()
		return report
	}
	add("describe", "")
	report.AdapterID = firstDescriptor.ID
	if err = firstDescriptor.Validate(); err != nil {
		add("descriptor_validation", "descriptor is invalid")
		first.abort()
		return report
	}
	firstFingerprint := adapterhost.DescriptorFingerprint(firstDescriptor)
	add("descriptor_validation", "")

	if err = requestUnknownMethod(first, timeout); err != nil {
		add("structured_error", describeError(err))
		first.abort()
		return report
	}
	add("structured_error", "")

	if err = shutdown(first, timeout); err != nil {
		add("shutdown_acknowledgement", describeError(err))
		first.abort()
		add("shutdown_exit", "adapter did not exit cleanly after shutdown")
		return report
	}
	add("shutdown_acknowledgement", "")
	if err = first.waitForExit(timeout); err != nil {
		add("shutdown_exit", describeError(err))
		first.abort()
		return report
	}
	if err = first.waitForOutputEOF(timeout); err != nil {
		add("stdout_cleanliness", "adapter stdout did not close after shutdown")
		first.abort()
		return report
	}
	if first.hasUnexpectedOutput() {
		add("stdout_cleanliness", "adapter emitted an unsolicited stdout frame")
		return report
	}
	add("shutdown_exit", "")
	add("stdout_cleanliness", "")

	second, err := startAdapter(binary)
	if err != nil {
		add("restart_launch", "adapter process could not be restarted")
		return report
	}
	add("restart_launch", "")
	secondDescriptor, err := requestDescriptor(second, timeout)
	if err != nil {
		add("restart_stability", describeError(err))
		second.abort()
		return report
	}
	if secondDescriptor.Validate() != nil ||
		secondDescriptor.ID != firstDescriptor.ID ||
		secondDescriptor.Version != firstDescriptor.Version ||
		secondDescriptor.ProtocolVersion != firstDescriptor.ProtocolVersion ||
		adapterhost.DescriptorFingerprint(secondDescriptor) != firstFingerprint {
		add("restart_stability", "descriptor identity changed after process restart")
		second.abort()
		return report
	}
	add("restart_stability", "")
	if err = shutdown(second, timeout); err != nil {
		add("restart_shutdown_acknowledgement", describeError(err))
		second.abort()
		add("restart_shutdown_exit", "adapter did not exit cleanly after shutdown")
		return report
	}
	add("restart_shutdown_acknowledgement", "")
	if err = second.waitForExit(timeout); err != nil {
		add("restart_shutdown_exit", describeError(err))
		second.abort()
		return report
	}
	if err = second.waitForOutputEOF(timeout); err != nil {
		add("restart_stdout_cleanliness", "adapter stdout did not close after shutdown")
		second.abort()
		return report
	}
	if second.hasUnexpectedOutput() {
		add("restart_stdout_cleanliness", "adapter emitted an unsolicited stdout frame")
		return report
	}
	add("restart_shutdown_exit", "")
	add("restart_stdout_cleanliness", "")
	return report
}

func validateExecutable(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("binary must be a regular executable file")
	}
	return nil
}

func requestDescriptor(child *adapterProcess, timeout time.Duration) (adapterproto.Descriptor, error) {
	result, err := child.exchange(timeout, adapterproto.MethodDescribe, map[string]any{})
	if err != nil {
		return adapterproto.Descriptor{}, err
	}
	var descriptor adapterproto.Descriptor
	if json.Unmarshal(result, &descriptor) != nil {
		return adapterproto.Descriptor{}, errors.New("descriptor result is malformed")
	}
	if descriptor.ProtocolVersion != adapterproto.Version {
		return adapterproto.Descriptor{}, errors.New("descriptor protocol version is unsupported")
	}
	if err := descriptor.Validate(); err != nil {
		return adapterproto.Descriptor{}, errors.New("descriptor result is invalid")
	}
	return descriptor, nil
}

func requestUnknownMethod(child *adapterProcess, timeout time.Duration) error {
	response, err := child.exchangeResponse(timeout, "adapter-conformance.unknown_method", map[string]any{})
	if err != nil {
		return err
	}
	if response.Error == nil || response.Error.Code == "" {
		return errors.New("unknown method did not return a structured error")
	}
	return nil
}

func shutdown(child *adapterProcess, timeout time.Duration) error {
	response, err := child.exchangeResponse(timeout, adapterproto.MethodShutdown, map[string]any{})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return errors.New("shutdown returned an error")
	}
	child.closeInput()
	return nil
}

func describeError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "adapter operation timed out"
	}
	return err.Error()
}

type frameResult struct {
	response adapterproto.Response
	err      error
}

type adapterProcess struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stdout     io.ReadCloser
	responses  chan frameResult
	done       chan struct{}
	readerDone chan struct{}
	stopRead   chan struct{}
	stopOnce   sync.Once
	stdinMu    sync.Mutex
	stdoutMu   sync.Mutex
	waitErr    error
	waitMu     sync.Mutex
}

func startAdapter(path string) (*adapterProcess, error) {
	cmd := exec.Command(path)
	configureProcess(cmd)
	childStdin, stdin, err := os.Pipe()
	if err != nil {
		return nil, errors.New("adapter stdin is unavailable")
	}
	stdout, childStdout, err := os.Pipe()
	if err != nil {
		_ = childStdin.Close()
		_ = stdin.Close()
		return nil, errors.New("adapter stdout is unavailable")
	}
	stderr, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		_ = childStdin.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = childStdout.Close()
		return nil, errors.New("adapter stderr is unavailable")
	}
	cmd.Stdin = childStdin
	cmd.Stdout = childStdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = childStdin.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = childStdout.Close()
		_ = stderr.Close()
		return nil, errors.New("adapter process could not be started")
	}
	_ = childStdin.Close()
	_ = stderr.Close()
	// The parent must not keep the writer open: descendants inherit it and
	// would otherwise prevent EOF after the adapter process exits.
	_ = childStdout.Close()
	child := &adapterProcess{cmd: cmd, stdin: stdin, stdout: stdout, responses: make(chan frameResult, 1), done: make(chan struct{}), readerDone: make(chan struct{}), stopRead: make(chan struct{})}
	go child.readAndWait(stdout)
	go child.waitForProcess()
	return child, nil
}

func (p *adapterProcess) readAndWait(stdout io.ReadCloser) {
	defer close(p.readerDone)
	reader := bufio.NewReaderSize(stdout, 32<<10)
	for {
		response, err := adapterproto.ReadResponse(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.send(frameResult{err: errors.New("adapter stdout is not a valid response frame")})
				p.terminate()
			}
			break
		}
		if !p.send(frameResult{response: response}) {
			p.terminate()
			break
		}
	}
	p.closeOutput()
}

func (p *adapterProcess) waitForProcess() {
	err := p.cmd.Wait()
	p.waitMu.Lock()
	p.waitErr = err
	p.waitMu.Unlock()
	close(p.done)
}

func (p *adapterProcess) send(result frameResult) bool {
	select {
	case p.responses <- result:
		return true
	case <-p.stopRead:
		return false
	}
}

func (p *adapterProcess) exchange(timeout time.Duration, method string, params any) (json.RawMessage, error) {
	response, err := p.exchangeResponse(timeout, method, params)
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, errors.New("adapter returned a structured error")
	}
	return response.Result, nil
}

func (p *adapterProcess) exchangeResponse(timeout time.Duration, method string, params any) (adapterproto.Response, error) {
	stdin := p.input()
	if stdin == nil {
		return adapterproto.Response{}, errors.New("adapter stdin is closed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	id := method + "-1"
	request := adapterproto.Request{ProtocolVersion: adapterproto.Version, ID: id, Method: method}
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return adapterproto.Response{}, errors.New("request parameters are invalid")
	}
	request.Params = encodedParams
	written := make(chan error, 1)
	go func() { written <- adapterproto.WriteRequest(stdin, request) }()
	select {
	case err = <-written:
		if err != nil {
			p.terminate()
			return adapterproto.Response{}, errors.New("adapter request could not be written")
		}
	case <-ctx.Done():
		p.terminate()
		return adapterproto.Response{}, context.DeadlineExceeded
	case <-p.done:
		return adapterproto.Response{}, errors.New("adapter exited before request was written")
	}
	select {
	case frame := <-p.responses:
		if frame.err != nil {
			return adapterproto.Response{}, frame.err
		}
		if frame.response.ID != id {
			return adapterproto.Response{}, errors.New("adapter response ID does not match request")
		}
		if frame.response.ProtocolVersion != adapterproto.Version {
			return adapterproto.Response{}, errors.New("adapter response protocol version is unsupported")
		}
		return frame.response, nil
	case <-p.done:
		// Process exit can race with the reader goroutine decoding a final
		// complete frame. Wait for that read to finish before deciding there was
		// no response; if the process had descendants holding stdout, the caller's
		// deadline still bounds this wait and termination closes our read end.
		select {
		case frame := <-p.responses:
			return validateResponseFrame(frame, id)
		case <-p.readerDone:
			select {
			case frame := <-p.responses:
				return validateResponseFrame(frame, id)
			default:
				return adapterproto.Response{}, errors.New("adapter exited before responding")
			}
		case <-ctx.Done():
			p.terminate()
			return adapterproto.Response{}, context.DeadlineExceeded
		}
	case <-ctx.Done():
		p.terminate()
		return adapterproto.Response{}, context.DeadlineExceeded
	}
}

func validateResponseFrame(frame frameResult, id string) (adapterproto.Response, error) {
	if frame.err != nil {
		return adapterproto.Response{}, frame.err
	}
	if frame.response.ID != id || frame.response.ProtocolVersion != adapterproto.Version {
		return adapterproto.Response{}, errors.New("adapter response envelope does not match request")
	}
	return frame.response, nil
}

func (p *adapterProcess) waitForExit(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		p.waitMu.Lock()
		err := p.waitErr
		p.waitMu.Unlock()
		if err != nil {
			return errors.New("adapter exited unsuccessfully")
		}
		return nil
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func (p *adapterProcess) waitForOutputEOF(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.readerDone:
		return nil
	case <-timer.C:
		p.terminate()
		return context.DeadlineExceeded
	}
}

func (p *adapterProcess) hasUnexpectedOutput() bool {
	select {
	case frame := <-p.responses:
		return frame.err != nil || frame.response.ID != ""
	default:
		return false
	}
}

func (p *adapterProcess) terminate() {
	p.stopOnce.Do(func() { close(p.stopRead) })
	p.closeInput()
	killProcessTree(p.cmd)
	// A child may have inherited stdout and outlive the adapter process (for
	// example on platforms without process-group termination). Closing our
	// read handle ensures that this cannot hold the response reader or Wait path
	// open indefinitely.
	p.closeOutput()
}

func (p *adapterProcess) input() io.WriteCloser {
	p.stdinMu.Lock()
	defer p.stdinMu.Unlock()
	return p.stdin
}

func (p *adapterProcess) closeInput() {
	p.stdinMu.Lock()
	stdin := p.stdin
	p.stdin = nil
	p.stdinMu.Unlock()
	if stdin != nil {
		_ = stdin.Close()
	}
}

func (p *adapterProcess) closeOutput() {
	p.stdoutMu.Lock()
	stdout := p.stdout
	p.stdout = nil
	p.stdoutMu.Unlock()
	if stdout != nil {
		_ = stdout.Close()
	}
}

func (p *adapterProcess) abort() {
	p.terminate()
	select {
	case <-p.done:
	case <-time.After(time.Second):
		killProcessTree(p.cmd)
		// Process termination and output-reader cancellation are best effort,
		// but conformance must never hang if a broken child escapes supervision.
		select {
		case <-p.done:
		case <-time.After(time.Second):
		}
	}
}
