package runtimeipc

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const helperEnvironment = "IR_RUNTIMEIPC_HELPER"

func TestProcessServerHelper(t *testing.T) {
	switch os.Getenv(helperEnvironment) {
	case "server":
		runProcessServerHelper()
	case "client":
		runProcessClientHelper()
	default:
		return
	}
}

func runProcessServerHelper() {
	socket := os.Getenv("IR_RUNTIMEIPC_SOCKET")
	server, err := NewServer(socket, "generation-a", []byte("0123456789abcdef0123456789abcdef"), processTestHandler{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Serve(ctx) }()
	fmt.Fprintln(os.Stdout, "READY")
	for {
		time.Sleep(time.Hour)
	}
}

func runProcessClientHelper() {
	client, err := NewClient(os.Getenv("IR_RUNTIMEIPC_SOCKET"), "generation-a", []byte("0123456789abcdef0123456789abcdef"), time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var result map[string]any
	if err := client.Call(context.Background(), "ready", nil, &result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if err := os.WriteFile(os.Getenv("IR_RUNTIMEIPC_READY_FILE"), []byte("ready"), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	for {
		time.Sleep(time.Hour)
	}
}

type processTestHandler struct{}

func (processTestHandler) Handle(ctx context.Context, operation string, _ json.RawMessage) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch operation {
	case "ready", "inventory":
		return map[string]any{"ready": true, "active": []string{}}, nil
	case "block":
		<-ctx.Done()
		return nil, ctx.Err()
	default:
		return nil, errors.New("unsupported")
	}
}

type echoHandler struct{}

func (echoHandler) Handle(_ context.Context, operation string, payload json.RawMessage) (any, error) {
	if operation == "wait" {
		return nil, errors.New("unused")
	}
	return map[string]any{"operation": operation, "payload": json.RawMessage(payload)}, nil
}

type waitHandler struct{}

func (waitHandler) Handle(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type cancellationHandler struct {
	started  chan struct{}
	canceled chan struct{}
}

func (h cancellationHandler) Handle(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
	close(h.started)
	<-ctx.Done()
	close(h.canceled)
	return nil, ctx.Err()
}

func TestFramedClientServerRoundTripAndSocketPermissions(t *testing.T) {
	root := shortTempDir(t)
	socket := filepath.Join(root, "ipc", "engine.sock")
	token := []byte("0123456789abcdef0123456789abcdef")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := NewServer(socket, "generation-a", token, echoHandler{})
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	client, err := NewClient(socket, "generation-a", token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Operation string          `json:"operation"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := client.Call(context.Background(), "echo", map[string]string{"value": "ok"}, &result); err != nil {
		t.Fatal(err)
	}
	if result.Operation != "echo" || string(result.Payload) != `{"value":"ok"}` {
		t.Fatalf("round trip result %#v", result)
	}
	for path, want := range map[string]os.FileMode{filepath.Join(root, "ipc"): 0700, socket: 0600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want {
			t.Errorf("mode %s = %o, want %o", path, info.Mode().Perm(), want)
		}
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestProtocolAuthenticationGenerationAndMalformedFrames(t *testing.T) {
	for _, tc := range []struct {
		name           string
		protocol       int
		generation     string
		token          []byte
		frameBody      []byte
		frameSize      uint32
		wantCode       string
		wantNoResponse bool
	}{
		{name: "protocol mismatch", protocol: ProtocolVersion + 1, generation: "generation-a", token: []byte("0123456789abcdef0123456789abcdef"), wantCode: "protocol_mismatch"},
		{name: "wrong generation", protocol: ProtocolVersion, generation: "generation-b", token: []byte("0123456789abcdef0123456789abcdef"), wantCode: "generation_mismatch"},
		{name: "authentication failure", protocol: ProtocolVersion, generation: "generation-a", token: []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), wantCode: "unauthorized"},
		{name: "oversized frame", frameSize: MaxFrameBytes + 1, wantNoResponse: true},
		{name: "malformed JSON", frameBody: []byte("!"), wantNoResponse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, socket, _ := startTestServer(t, processTestHandler{})
			defer stopTestServer(t, server)
			conn, err := net.DialTimeout("unix", socket, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			if tc.frameSize > 0 {
				var header [4]byte
				binary.BigEndian.PutUint32(header[:], tc.frameSize)
				_, err = conn.Write(header[:])
			} else if tc.frameBody != nil {
				var header [4]byte
				binary.BigEndian.PutUint32(header[:], uint32(len(tc.frameBody)))
				if _, err = conn.Write(header[:]); err == nil {
					_, err = conn.Write(tc.frameBody)
				}
			} else {
				request := Request{ProtocolVersion: tc.protocol, RequestID: "request-1", GenerationID: tc.generation, ClientInstanceID: "client-a", AuthToken: tc.token, Deadline: time.Now().Add(time.Second).UnixNano(), Operation: "ready"}
				err = writeFrame(conn, request)
			}
			if err != nil {
				t.Fatal(err)
			}
			var response Response
			err = readFrame(conn, &response)
			if tc.wantNoResponse {
				if err == nil {
					t.Fatalf("expected malformed/oversized request to be closed, got %#v", response)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if response.Error == nil || response.Error.Code != tc.wantCode {
				t.Fatalf("response error %#v, want %q", response.Error, tc.wantCode)
			}
		})
	}
}

func TestClientRejectsWrongGenerationResponse(t *testing.T) {
	server, socket, token := startTestServer(t, processTestHandler{})
	defer stopTestServer(t, server)
	client, err := NewClient(socket, "generation-b", token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	err = client.Call(context.Background(), "ready", nil, &result)
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("Call error = %v, want generation identity rejection", err)
	}
}

func TestClientPinsExpectedEngineInstance(t *testing.T) {
	server, socket, token := startTestServer(t, processTestHandler{})
	defer stopTestServer(t, server)
	client, err := NewClientForInstance(socket, "generation-a", "another-instance", token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := client.Call(context.Background(), "ready", nil, &result); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("Call error = %v, want pinned instance rejection", err)
	}
}

func TestRequestDeadlineCancelsHandler(t *testing.T) {
	server, socket, token := startTestServer(t, waitHandler{})
	defer stopTestServer(t, server)
	client, err := NewClient(socket, "generation-a", token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err = client.Call(ctx, "wait", map[string]string{}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call error = %v, want deadline exceeded", err)
	}
}

func TestClientCancellationCancelsActiveHandler(t *testing.T) {
	handler := cancellationHandler{started: make(chan struct{}), canceled: make(chan struct{})}
	server, socket, token := startTestServer(t, handler)
	defer stopTestServer(t, server)
	client, err := NewClient(socket, "generation-a", token, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.Call(ctx, "cancel", nil, nil) }()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("client error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client call did not stop after cancellation")
	}
	select {
	case <-handler.canceled:
	case <-time.After(time.Second):
		t.Fatal("server handler did not observe client cancellation")
	}
}

func TestDisconnectedControlClientDoesNotStopEngineProcess(t *testing.T) {
	root := shortTempDir(t)
	socket := filepath.Join(root, "engine.sock")
	command := exec.Command(os.Args[0], "-test.run=^TestProcessServerHelper$")
	command.Env = append(os.Environ(), helperEnvironment+"=server", "IR_RUNTIMEIPC_SOCKET="+socket)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
	})
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if strings.TrimSpace(line) != "READY" {
			t.Fatalf("engine helper readiness output %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("engine helper did not become ready")
	}
	token := []byte("0123456789abcdef0123456789abcdef")
	client, err := NewClient(socket, "generation-a", token, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var readyResult map[string]any
	if err := client.Call(context.Background(), "ready", nil, &readyResult); err != nil {
		t.Fatal(err)
	}
	// Killing this client/control process is independent from the already
	// running engine service process and its listener.
	clientReadyPath := filepath.Join(root, "client.ready")
	clientProcess := exec.Command(os.Args[0], "-test.run=^TestProcessServerHelper$")
	clientProcess.Env = append(os.Environ(), helperEnvironment+"=client", "IR_RUNTIMEIPC_SOCKET="+socket, "IR_RUNTIMEIPC_READY_FILE="+clientReadyPath)
	if err := clientProcess.Start(); err != nil {
		t.Fatal(err)
	}
	clientReadyDeadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(clientReadyPath); err == nil {
			break
		}
		if time.Now().After(clientReadyDeadline) {
			_ = clientProcess.Process.Kill()
			_, _ = clientProcess.Process.Wait()
			t.Fatal("client process did not complete authenticated engine request")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := clientProcess.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = clientProcess.Process.Wait()
	var inventory map[string]any
	if err := client.Call(context.Background(), "inventory", nil, &inventory); err != nil {
		t.Fatalf("engine stopped after client process kill: %v", err)
	}
	if command.ProcessState != nil && command.ProcessState.Exited() {
		t.Fatal("engine process exited when client process was killed")
	}
}

func startTestServer(t *testing.T, handler Handler) (*Server, string, []byte) {
	t.Helper()
	root := shortTempDir(t)
	socket := filepath.Join(root, "engine.sock")
	token := []byte("0123456789abcdef0123456789abcdef")
	server, err := NewServer(socket, "generation-a", token, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-serveErr; err != nil {
			t.Errorf("runtime IPC server: %v", err)
		}
	})
	return server, socket, token
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "iripc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func stopTestServer(t *testing.T, server *Server) {
	t.Helper()
	if server != nil {
		server.cleanupSocket()
	}
}

func TestFrameDecoderRejectsTrailingAndUnknownFields(t *testing.T) {
	for _, body := range []string{`{"protocol_version":1,"request_id":"x","generation_id":"g","auth_token":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=","deadline_unix_nano":1,"operation":"ready","extra":true}`, `{} {}`} {
		reader := strings.NewReader(fmt.Sprintf("%c%c%c%c%s", byte(0), byte(0), byte(0), byte(len(body)), body))
		var request Request
		if err := readFrame(reader, &request); err == nil {
			t.Errorf("accepted invalid frame %q", body)
		}
	}
	_ = io.EOF
}
