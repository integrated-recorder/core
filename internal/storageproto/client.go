package storageproto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultStartupTimeout = 8 * time.Second
	maxSafeErrorBody      = 4 << 10
)

type StartOptions struct {
	Binary         string
	SocketPath     string
	TokenFile      string
	StartupTimeout time.Duration
}

type Client struct {
	http       *http.Client
	transport  *http.Transport
	tokenMu    sync.RWMutex
	token      []byte
	cmd        *exec.Cmd
	parentPipe io.WriteCloser
	done       chan struct{}
	closeOnce  sync.Once
	closeErr   error
}

type RemoteError struct{ Code string }

func (e *RemoteError) Error() string { return "storage provider request failed: " + e.Code }

// Start launches the provider without a shell or inherited environment. The
// child's stdin is reserved as a parent-liveness pipe; providers should watch
// it for EOF through Serve's default ParentLiveness behavior.
func Start(ctx context.Context, options StartOptions) (*Client, error) {
	if ctx == nil || !filepath.IsAbs(options.Binary) || !filepath.IsAbs(options.SocketPath) || !filepath.IsAbs(options.TokenFile) {
		return nil, errors.New("storage provider launch configuration is invalid")
	}
	info, err := os.Lstat(options.Binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0111 == 0 {
		return nil, errors.New("storage provider executable is invalid")
	}
	token, err := readToken(options.TokenFile)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(options.Binary, "--socket", options.SocketPath, "--token-file", options.TokenFile)
	cmd.Env = []string{}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	parentPipe, err := cmd.StdinPipe()
	if err != nil {
		for i := range token {
			token[i] = 0
		}
		return nil, errors.New("storage provider process could not start")
	}
	configureParentDeathSignal(cmd)
	if err = cmd.Start(); err != nil {
		_ = parentPipe.Close()
		for i := range token {
			token[i] = 0
		}
		return nil, errors.New("storage provider process could not start")
	}
	c := newClient(options.SocketPath, token)
	for i := range token {
		token[i] = 0
	}
	c.cmd = cmd
	c.parentPipe = parentPipe
	c.done = make(chan struct{})
	go func() { _ = cmd.Wait(); close(c.done) }()
	startup := options.StartupTimeout
	if startup <= 0 {
		startup = defaultStartupTimeout
	}
	deadline := time.NewTimer(startup)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		_, describeErr := c.Describe(probeCtx)
		cancel()
		if describeErr == nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			_ = c.Close()
			return nil, ctx.Err()
		case <-deadline.C:
			_ = c.Close()
			return nil, errors.New("storage provider did not become ready")
		case <-c.done:
			_ = c.Close()
			return nil, errors.New("storage provider exited before becoming ready")
		case <-ticker.C:
		}
	}
}

func NewClient(socketPath, tokenFile string) (*Client, error) {
	if !filepath.IsAbs(socketPath) || !filepath.IsAbs(tokenFile) {
		return nil, errors.New("storage provider socket path is invalid")
	}
	token, err := readToken(tokenFile)
	if err != nil {
		return nil, err
	}
	client := newClient(socketPath, token)
	for i := range token {
		token[i] = 0
	}
	return client, nil
}

func newClient(socketPath string, token []byte) *Client {
	transport := unixTransport(socketPath)
	return &Client{transport: transport, http: &http.Client{Transport: transport, Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, token: append([]byte(nil), token...)}
}

func (c *Client) Describe(ctx context.Context) (Descriptor, error) {
	var descriptor Descriptor
	err := c.jsonRequest(ctx, http.MethodGet, "/v1/descriptor", nil, MaxControlFrameBytes, http.StatusOK, &descriptor)
	if err != nil {
		return Descriptor{}, err
	}
	if err = ValidateDescriptor(descriptor); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func (c *Client) Configure(ctx context.Context, config Config) error {
	return c.jsonRequest(ctx, http.MethodPut, "/v1/config", config, MaxConfigBytes, http.StatusNoContent, nil)
}
func (c *Client) Probe(ctx context.Context) error {
	var result struct {
		Ready bool `json:"ready"`
	}
	if err := c.jsonRequest(ctx, http.MethodPost, "/v1/probe", struct{}{}, MaxControlFrameBytes, http.StatusOK, &result); err != nil {
		return err
	}
	if !result.Ready {
		return fmt.Errorf("%w: provider is not ready", ErrProtocol)
	}
	return nil
}

func (c *Client) Put(ctx context.Context, key string, body io.Reader, size int64) (ObjectInfo, error) {
	if err := ValidateKey(key); err != nil {
		return ObjectInfo{}, err
	}
	if size < 0 || size > MaxObjectBytes || body == nil {
		return ObjectInfo{}, ErrProtocol
	}
	u := "/v1/objects?key=" + url.QueryEscape(key)
	req, err := c.request(ctx, http.MethodPut, u, io.NopCloser(body))
	if err != nil {
		return ObjectInfo{}, err
	}
	req.ContentLength = size
	resp, err := c.http.Do(req)
	if err != nil {
		return ObjectInfo{}, normalizeTransportError(ctx, err)
	}
	defer resp.Body.Close()
	if err = checkStatus(resp, http.StatusOK); err != nil {
		return ObjectInfo{}, err
	}
	var info ObjectInfo
	if err = decodeResponse(resp, MaxControlFrameBytes, &info); err != nil {
		return ObjectInfo{}, err
	}
	if info.Size != size || !validDigest(info.SHA256) {
		return ObjectInfo{}, ErrProtocol
	}
	return info, nil
}

func (c *Client) Open(ctx context.Context, key string) (io.ReadCloser, ObjectInfo, error) {
	return c.open(ctx, key, "", 0)
}

func (c *Client) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, ObjectInfo, error) {
	if offset < 0 || length <= 0 || length > MaxObjectBytes || offset > MaxObjectBytes-length {
		return nil, ObjectInfo{}, ErrProtocol
	}
	return c.open(ctx, key, fmt.Sprintf("bytes=%d-%d", offset, offset+length-1), length)
}

func (c *Client) open(ctx context.Context, key, byteRange string, rangeLength int64) (io.ReadCloser, ObjectInfo, error) {
	if err := ValidateKey(key); err != nil {
		return nil, ObjectInfo{}, err
	}
	req, err := c.request(ctx, http.MethodGet, "/v1/objects?key="+url.QueryEscape(key), nil)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if byteRange != "" {
		req.Header.Set("Range", byteRange)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, ObjectInfo{}, normalizeTransportError(ctx, err)
	}
	wantStatus := http.StatusOK
	if byteRange != "" {
		wantStatus = http.StatusPartialContent
	}
	if err = checkStatus(resp, wantStatus); err != nil {
		resp.Body.Close()
		return nil, ObjectInfo{}, err
	}
	digest := resp.Header.Get("X-IR-Object-SHA256")
	if !validDigest(digest) || resp.ContentLength < 0 || resp.ContentLength > MaxObjectBytes {
		resp.Body.Close()
		return nil, ObjectInfo{}, ErrProtocol
	}
	info := ObjectInfo{Size: resp.ContentLength, SHA256: digest}
	if byteRange != "" {
		if resp.ContentLength != rangeLength {
			resp.Body.Close()
			return nil, ObjectInfo{}, ErrProtocol
		}
		start, _, _ := parseRange(byteRange)
		total, valid := parseContentRange(resp.Header.Get("Content-Range"), start, rangeLength)
		if !valid {
			resp.Body.Close()
			return nil, ObjectInfo{}, ErrProtocol
		}
		info.Size = total
	}
	return resp.Body, info, nil
}

func (c *Client) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	if err := ValidateKey(key); err != nil {
		return ObjectInfo{}, err
	}
	req, err := c.request(ctx, http.MethodHead, "/v1/objects?key="+url.QueryEscape(key), nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return ObjectInfo{}, normalizeTransportError(ctx, err)
	}
	defer resp.Body.Close()
	if err = checkStatus(resp, http.StatusOK); err != nil {
		return ObjectInfo{}, err
	}
	info := ObjectInfo{Size: resp.ContentLength, SHA256: resp.Header.Get("X-IR-Object-SHA256")}
	if info.Size < 0 || info.Size > MaxObjectBytes || !validDigest(info.SHA256) {
		return ObjectInfo{}, ErrProtocol
	}
	return info, nil
}

func (c *Client) List(ctx context.Context, prefix, cursor string, limit int) (ListPage, error) {
	if ValidatePrefix(prefix) != nil || len(cursor) > 1024 || limit < 1 || limit > MaxListLimit {
		return ListPage{}, ErrProtocol
	}
	query := url.Values{"prefix": []string{prefix}, "limit": []string{strconv.Itoa(limit)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var page ListPage
	err := c.jsonRequest(ctx, http.MethodGet, "/v1/list?"+query.Encode(), nil, MaxControlFrameBytes, http.StatusOK, &page)
	if err != nil {
		return ListPage{}, err
	}
	if err = validatePage(page, prefix, cursor, limit); err != nil {
		return ListPage{}, err
	}
	return page, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	return c.jsonRequest(ctx, http.MethodDelete, "/v1/objects?key="+url.QueryEscape(key), nil, 0, http.StatusNoContent, nil)
}

func (c *Client) jsonRequest(ctx context.Context, method, target string, value any, maximum int64, status int, output any) error {
	var body io.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil || int64(len(encoded)) > maximum {
			return ErrProtocol
		}
		defer func() {
			for i := range encoded {
				encoded[i] = 0
			}
		}()
		body = bytes.NewReader(encoded)
	}
	req, err := c.request(ctx, method, target, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return normalizeTransportError(ctx, err)
	}
	defer resp.Body.Close()
	if err = checkStatus(resp, status); err != nil {
		return err
	}
	if output == nil {
		return nil
	}
	return decodeResponse(resp, int(maximum), output)
}

func (c *Client) request(ctx context.Context, method, target string, body io.Reader) (*http.Request, error) {
	if c == nil || c.http == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, ErrProtocol
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://storage-provider"+target, body)
	if err != nil {
		return nil, ErrProtocol
	}
	c.tokenMu.RLock()
	authorization := "Bearer " + string(c.token)
	c.tokenMu.RUnlock()
	req.Header.Set("Authorization", authorization)
	return req, nil
}

func checkStatus(resp *http.Response, expected int) error {
	if resp.StatusCode == expected {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode == http.StatusBadGateway && strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/plain") {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxControlFrameBytes+1))
		return ErrUnframedResponse
	}
	var wire wireError
	if decodeResponse(resp, maxSafeErrorBody, &wire) == nil && validErrorCode(wire.Error.Code) {
		return &RemoteError{Code: wire.Error.Code}
	}
	return ErrProtocol
}

func decodeResponse(resp *http.Response, maximum int, output any) error {
	if maximum <= 0 || maximum > MaxConfigBytes || resp.ContentLength > int64(maximum) {
		return ErrProtocol
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, int64(maximum)+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return ErrProtocol
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrProtocol
	}
	return nil
}

func validErrorCode(code string) bool {
	switch code {
	case "invalid_request", "invalid_key", "invalid_prefix", "invalid_cursor", "invalid_limit", "invalid_size", "size_mismatch", "invalid_range", "not_found", "unauthorized", "method_not_allowed", "unsupported", "provider_error", "canceled", "invalid_provider_response":
		return true
	default:
		return false
	}
}

func normalizeTransportError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("%w: transport failed", ErrProtocol)
	}
	return nil
}

func parseContentRange(value string, offset, length int64) (int64, bool) {
	var start, end, total int64
	if _, err := fmt.Sscanf(value, "bytes %d-%d/%d", &start, &end, &total); err != nil {
		return 0, false
	}
	valid := start == offset && end == offset+length-1 && total > end && total <= MaxObjectBytes
	return total, valid && value == fmt.Sprintf("bytes %d-%d/%d", start, end, total)
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		defer func() {
			c.tokenMu.Lock()
			for i := range c.token {
				c.token[i] = 0
			}
			c.token = nil
			c.tokenMu.Unlock()
		}()
		if c.transport != nil {
			c.transport.CloseIdleConnections()
		}
		if c.cmd != nil && c.cmd.Process != nil {
			if c.done == nil {
				_ = c.cmd.Process.Kill()
				if c.parentPipe != nil {
					_ = c.parentPipe.Close()
				}
				return
			}
			if isProcessDone(c.done) {
				if c.parentPipe != nil {
					_ = c.parentPipe.Close()
				}
				return
			}
			c.closeErr = stopProcess(c.cmd, c.done, c.parentPipe)
		} else if c.parentPipe != nil {
			_ = c.parentPipe.Close()
		}
	})
	return c.closeErr
}

// Exited reports whether a process started by Start has been reaped. Clients
// created with NewClient are transport-only and have no child process.
func (c *Client) Exited() bool {
	return c == nil || c.done != nil && isProcessDone(c.done)
}

// Done returns the child-process completion signal when this client owns a
// process. It is nil for clients created with NewClient.
func (c *Client) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

func isProcessDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
