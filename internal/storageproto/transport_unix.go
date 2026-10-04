//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package storageproto

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os/exec"
	"syscall"
	"time"
)

func unixTransport(socketPath string) *http.Transport {
	return &http.Transport{DisableCompression: true, DisableKeepAlives: false, MaxConnsPerHost: MaxConcurrentReadStreams, MaxIdleConnsPerHost: MaxConcurrentReadStreams, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
}

func stopProcess(cmd *exec.Cmd, done <-chan struct{}, parentPipe interface{ Close() error }) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	if waitProcess(done, 2*time.Second) {
		return nil
	}
	// Closing the reserved liveness pipe tells a conforming provider to abort
	// active requests and exit if it did not honor the graceful signal.
	if parentPipe != nil {
		_ = parentPipe.Close()
	}
	if waitProcess(done, 500*time.Millisecond) {
		return nil
	}
	_ = cmd.Process.Kill()
	if waitProcess(done, 2*time.Second) {
		return nil
	}
	return errors.New("storage provider process did not exit")
}

func waitProcess(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
