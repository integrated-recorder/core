package storageproto

import (
	"strings"
	"sync"
)

const maxChildDiagnosticBytes = 4 << 10

// boundedDiagnosticCapture retains only a small amount of child stderr while
// startup is being classified. The bytes are never returned or logged; only a
// fixed safe category may be derived from a few recognized OS phrases.
type boundedDiagnosticCapture struct {
	mu       sync.Mutex
	data     []byte
	disabled bool
}

func newBoundedDiagnosticCapture() *boundedDiagnosticCapture {
	return &boundedDiagnosticCapture{}
}

func (c *boundedDiagnosticCapture) Write(p []byte) (int, error) {
	length := len(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disabled {
		return len(p), nil
	}
	remaining := maxChildDiagnosticBytes - len(c.data)
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		c.data = append(c.data, p...)
	}
	// Always consume the complete write so the os/exec pipe copier cannot
	// block even when a provider emits unbounded diagnostics.
	return length, nil
}

func (c *boundedDiagnosticCapture) failureCode() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	text := strings.ToLower(string(c.data))
	switch {
	case strings.Contains(text, "permission denied"), strings.Contains(text, "operation not permitted"):
		return "child_exit_permission_denied"
	case strings.Contains(text, "address already in use"):
		return "child_exit_address_in_use"
	case strings.Contains(text, "no such file or directory"):
		return "child_exit_resource_missing"
	default:
		return "child_exited"
	}
}

func (c *boundedDiagnosticCapture) disable() {
	c.mu.Lock()
	c.disabled = true
	c.mu.Unlock()
}

func (c *boundedDiagnosticCapture) clear() {
	c.mu.Lock()
	c.disabled = true
	for i := range c.data {
		c.data[i] = 0
	}
	c.data = nil
	c.mu.Unlock()
}
