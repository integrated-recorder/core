package runtimeipc

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	defaultCallTimeout = 30 * time.Second
	maxCallTimeout     = 15 * time.Minute
	maxConnections     = 32
)

var identityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Handler performs one application operation. Errors are deliberately
// redacted by the transport unless they implement PublicError.
type Handler interface {
	Handle(context.Context, string, json.RawMessage) (any, error)
}

// PublicError marks a deliberately safe error that may cross the local IPC
// boundary. Implementation errors, source URIs, or adapter diagnostics must
// otherwise remain private.
type PublicError interface {
	error
	PublicIPCError() (code, message string)
}

type RemoteError struct {
	Code    string
	Message string
}

func (e *RemoteError) Error() string {
	if e == nil {
		return "runtime operation failed"
	}
	return e.Message
}

type Server struct {
	listener    net.Listener
	socketPath  string
	socketInfo  os.FileInfo
	generation  string
	instance    string
	token       []byte
	handler     Handler
	connections sync.Map
	closeOnce   sync.Once
}

func NewServer(socketPath, generationID string, token []byte, handler Handler) (*Server, error) {
	if !identityPattern.MatchString(generationID) {
		return nil, errors.New("runtime generation identity is invalid")
	}
	if len(token) != 32 {
		return nil, errors.New("runtime IPC token must contain 32 bytes")
	}
	if handler == nil {
		return nil, errors.New("runtime IPC handler is required")
	}
	if identified, ok := handler.(interface{ GenerationID() string }); ok && identified.GenerationID() != generationID {
		return nil, errors.New("runtime IPC handler generation identity mismatch")
	}
	instanceID := ""
	if identified, ok := handler.(interface{ RuntimeInstanceID() string }); ok {
		instanceID = identified.RuntimeInstanceID()
	}
	if instanceID == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, errors.New("runtime IPC instance identity unavailable")
		}
		instanceID = hex.EncodeToString(random[:])
	}
	if !identityPattern.MatchString(instanceID) {
		return nil, errors.New("runtime IPC instance identity is invalid")
	}
	if socketPath == "" || len(socketPath) > 100 {
		return nil, errors.New("runtime IPC socket path is invalid")
	}
	abs, err := filepath.Abs(socketPath)
	if err != nil {
		return nil, errors.New("runtime IPC socket path is invalid")
	}
	dir := filepath.Dir(abs)
	if err := ensurePrivateSocketDirectory(dir); err != nil {
		return nil, err
	}
	if info, lstatErr := os.Lstat(abs); lstatErr == nil {
		if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("runtime IPC path already exists and is not a socket")
		}
		if conn, dialErr := net.DialTimeout("unix", abs, 100*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			return nil, errors.New("runtime IPC socket is already in use")
		}
		if err := os.Remove(abs); err != nil {
			return nil, errors.New("stale runtime IPC socket could not be removed")
		}
	} else if !errors.Is(lstatErr, os.ErrNotExist) {
		return nil, errors.New("runtime IPC path could not be inspected")
	}
	listener, err := net.Listen("unix", abs)
	if err != nil {
		return nil, fmt.Errorf("listen on runtime IPC socket: %w", err)
	}
	if err := os.Chmod(abs, 0600); err != nil {
		_ = listener.Close()
		_ = os.Remove(abs)
		return nil, errors.New("runtime IPC socket permissions could not be secured")
	}
	info, err := os.Lstat(abs)
	if err != nil {
		_ = listener.Close()
		_ = os.Remove(abs)
		return nil, errors.New("runtime IPC socket could not be verified")
	}
	return &Server{listener: listener, socketPath: abs, socketInfo: info, generation: generationID, instance: instanceID, token: append([]byte(nil), token...), handler: handler}, nil
}

func ensurePrivateSocketDirectory(path string) error {
	info, err := os.Lstat(path)
	created := errors.Is(err, os.ErrNotExist)
	if created {
		if err := os.MkdirAll(path, 0700); err != nil {
			return errors.New("runtime IPC directory could not be created")
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("runtime IPC directory is not a safe directory")
	}
	if created {
		if err := os.Chmod(path, 0700); err != nil {
			return errors.New("runtime IPC directory permissions could not be secured")
		}
	} else if info.Mode().Perm() != 0700 {
		// Never chmod a caller-supplied existing location such as /tmp or /run.
		// The Runtime Host must provision a dedicated private IPC directory.
		return errors.New("runtime IPC directory must have mode 0700")
	}
	return nil
}

func (s *Server) Serve(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.listener.Close()
			s.connections.Range(func(key, _ any) bool {
				if conn, ok := key.(net.Conn); ok {
					_ = conn.Close()
				}
				return true
			})
		case <-stop:
		}
	}()
	defer close(stop)
	defer s.cleanupSocket()
	semaphore := make(chan struct{}, maxConnections)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept runtime IPC connection: %w", err)
		}
		select {
		case semaphore <- struct{}{}:
			s.connections.Store(conn, struct{}{})
			go func() {
				defer func() {
					<-semaphore
					s.connections.Delete(conn)
					_ = conn.Close()
				}()
				s.handleConn(ctx, conn)
			}()
		default:
			_ = conn.Close()
		}
	}
}

func (s *Server) cleanupSocket() {
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
		if current, err := os.Lstat(s.socketPath); err == nil && s.socketInfo != nil && os.SameFile(s.socketInfo, current) {
			_ = os.Remove(s.socketPath)
		}
	})
}

func (s *Server) handleConn(parent context.Context, conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var request Request
	if err := readFrame(conn, &request); err != nil {
		return
	}
	response := Response{ProtocolVersion: ProtocolVersion, RequestID: request.RequestID, GenerationID: s.generation, InstanceID: s.instance}
	if request.ProtocolVersion != ProtocolVersion {
		s.writeError(conn, response, "protocol_mismatch", "runtime protocol is not compatible")
		return
	}
	if !validRequestID(request.RequestID) || !identityPattern.MatchString(request.ClientInstanceID) {
		s.writeError(conn, response, "invalid_request", "request identity is invalid")
		return
	}
	if request.GenerationID != s.generation {
		s.writeError(conn, response, "generation_mismatch", "request targets another engine generation")
		return
	}
	if len(request.AuthToken) != len(s.token) || subtle.ConstantTimeCompare(request.AuthToken, s.token) != 1 {
		s.writeError(conn, response, "unauthorized", "runtime IPC authentication failed")
		return
	}
	deadline := time.Unix(0, request.Deadline)
	if request.Deadline <= 0 || !deadline.After(time.Now()) || deadline.After(time.Now().Add(maxCallTimeout)) {
		s.writeError(conn, response, "invalid_deadline", "request deadline is invalid")
		return
	}
	if len(request.Payload) > MaxFrameBytes/2 || (len(request.Payload) > 0 && !json.Valid(request.Payload)) {
		s.writeError(conn, response, "invalid_request", "request payload is invalid")
		return
	}
	_ = conn.SetDeadline(deadline)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	// A request owns its connection for one operation. Detect an early client
	// disconnect so a caller-cancelled bounded admission also cancels the Host
	// handler instead of leaving it blocked until the request deadline.
	go func() {
		var probe [1]byte
		_, _ = conn.Read(probe[:])
		cancel()
	}()
	var result any
	var opErr error
	func() {
		defer func() {
			if recover() != nil {
				result = nil
				opErr = errors.New("runtime operation failed")
			}
		}()
		result, opErr = s.handler.Handle(ctx, request.Operation, request.Payload)
	}()
	if ctx.Err() != nil {
		s.writeError(conn, response, "deadline_exceeded", "runtime operation deadline exceeded")
		return
	}
	if opErr != nil {
		var safe PublicError
		if errors.As(opErr, &safe) {
			code, message := safe.PublicIPCError()
			if !validErrorField(code, 64) || !validMessage(message, 256) {
				code, message = "operation_failed", "runtime operation failed"
			}
			s.writeError(conn, response, code, message)
			return
		}
		s.writeError(conn, response, "operation_failed", "runtime operation failed")
		return
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > MaxFrameBytes/2 {
		s.writeError(conn, response, "response_too_large", "runtime response exceeds size limit")
		return
	}
	response.OK = true
	response.Result = encoded
	_ = writeFrame(conn, response)
}

func (s *Server) writeError(conn net.Conn, response Response, code, message string) {
	response.OK = false
	response.Error = &ResponseError{Code: code, Message: message}
	_ = writeFrame(conn, response)
}

func validRequestID(value string) bool { return identityPattern.MatchString(value) }

func validErrorField(value string, max int) bool {
	return len(value) > 0 && len(value) <= max && identityPattern.MatchString(value)
}

func validMessage(value string, max int) bool {
	if len(value) == 0 || len(value) > max || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) && r != '\t' {
			return false
		}
	}
	return true
}

// Client is a stateless authenticated client; one request uses one UDS
// connection so a disconnected Control process cannot affect engine lifetime.
type Client struct {
	socketPath             string
	generation             string
	instance               string
	expectedRemoteInstance string
	token                  []byte
	timeout                time.Duration
}

func NewClient(socketPath, generationID string, token []byte, timeout time.Duration) (*Client, error) {
	return newClient(socketPath, generationID, token, timeout, "")
}

// NewClientForInstance additionally pins a known Engine process instance so
// stale socket replacement cannot be mistaken for the attached Engine.
func NewClientForInstance(socketPath, generationID, expectedInstanceID string, token []byte, timeout time.Duration) (*Client, error) {
	if !identityPattern.MatchString(expectedInstanceID) {
		return nil, errors.New("runtime IPC expected instance identity is invalid")
	}
	return newClient(socketPath, generationID, token, timeout, expectedInstanceID)
}

func newClient(socketPath, generationID string, token []byte, timeout time.Duration, expectedRemoteInstance string) (*Client, error) {
	if !identityPattern.MatchString(generationID) || socketPath == "" || len(token) != 32 {
		return nil, errors.New("runtime IPC client configuration is invalid")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, errors.New("runtime IPC client identity unavailable")
	}
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	if timeout > maxCallTimeout {
		timeout = maxCallTimeout
	}
	return &Client{socketPath: socketPath, generation: generationID, instance: hex.EncodeToString(random[:]), expectedRemoteInstance: expectedRemoteInstance, token: append([]byte(nil), token...), timeout: timeout}, nil
}

func (c *Client) Call(ctx context.Context, operation string, payload any, result any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !validErrorField(operation, 64) {
		return errors.New("runtime IPC operation is invalid")
	}
	var payloadJSON json.RawMessage
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil || len(encoded) > MaxFrameBytes/2 {
			return ErrFrameTooLarge
		}
		payloadJSON = encoded
	}
	now := time.Now()
	deadline := now.Add(c.timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if !deadline.After(now) {
		return context.DeadlineExceeded
	}
	var randomID [16]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		return errors.New("runtime IPC request identity unavailable")
	}
	request := Request{ProtocolVersion: ProtocolVersion, RequestID: hex.EncodeToString(randomID[:]), GenerationID: c.generation, ClientInstanceID: c.instance, AuthToken: c.token, Deadline: deadline.UnixNano(), Operation: operation, Payload: payloadJSON}
	conn, err := (&net.Dialer{Timeout: time.Until(deadline)}).DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return errors.New("runtime IPC connection unavailable")
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)
	stopCancel := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancel()
	if err := writeFrame(conn, request); err != nil {
		return err
	}
	var response Response
	if err := readFrame(conn, &response); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return context.DeadlineExceeded
		}
		return fmt.Errorf("runtime IPC response unavailable: %w", err)
	}
	if response.ProtocolVersion != ProtocolVersion || response.RequestID != request.RequestID || response.GenerationID != c.generation || !identityPattern.MatchString(response.InstanceID) || (c.expectedRemoteInstance != "" && response.InstanceID != c.expectedRemoteInstance) {
		return errors.New("runtime IPC response identity mismatch")
	}
	if !response.OK {
		if response.Error == nil || !validErrorField(response.Error.Code, 64) || !validMessage(response.Error.Message, 256) {
			return errors.New("runtime IPC operation failed")
		}
		return &RemoteError{Code: response.Error.Code, Message: response.Error.Message}
	}
	if result == nil {
		return nil
	}
	if len(response.Result) == 0 {
		return ErrMalformedFrame
	}
	if err := json.Unmarshal(response.Result, result); err != nil {
		return fmt.Errorf("runtime IPC result is malformed: %w", err)
	}
	return nil
}
