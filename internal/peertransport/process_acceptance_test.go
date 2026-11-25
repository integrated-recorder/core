package peertransport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
	"github.com/pion/webrtc/v4"
)

// TestPeerTransportSeparateProcessAcceptance exercises the real encrypted
// signaling and authenticated Pion data-channel APIs across two OS processes.
// The Discovery endpoints are separate HTTPS servers with separate durable
// directories. This is same-host process-boundary coverage: it is not a
// Control Plane, independent-operator, cross-network, TURN, or revocation test.
func TestPeerTransportSeparateProcessAcceptance(t *testing.T) {
	if os.Getenv("IR_PEER_PROCESS_HELPER") == "1" {
		runPeerTransportProcessHelper(t)
		return
	}

	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("e", 64)
	firstDir, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "discovery-one"))
	if err != nil {
		t.Fatal(err)
	}
	secondDir, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "discovery-two"))
	if err != nil {
		t.Fatal(err)
	}
	_, ca, caPrivate := newTestDiscoveryCA(t)
	_, firstTarget := startTestDiscoveryTLSServer(t, "one.example", firstDir, ca, caPrivate)
	_, secondTarget := startTestDiscoveryTLSServer(t, "two.example", secondDir, ca, caPrivate)
	caPEM := encodeTestCertificate(ca.Raw)

	leftDir := filepath.Join(t.TempDir(), "node-left")
	rightDir := filepath.Join(t.TempDir(), "node-right")
	left := startPeerTransportProcess(t, leftDir)
	right := startPeerTransportProcess(t, rightDir)
	leftInfo := left.ready
	rightInfo := right.ready
	if leftInfo.NodeID == rightInfo.NodeID || leftInfo.PublicKey == rightInfo.PublicKey {
		t.Fatalf("separate durable node directories reused identity: left=%+v right=%+v", leftInfo, rightInfo)
	}

	keys := map[string]string{"identity:" + identityID: base64.RawURLEncoding.EncodeToString(rootPublic)}
	keys["node:"+leftInfo.NodeID] = leftInfo.PublicKey
	keys["node:"+rightInfo.NodeID] = rightInfo.PublicKey
	leftConfig := peerProcessConfig{
		IdentityID: identityID, IdentityHead: head, LocalNodeID: leftInfo.NodeID, RemoteNodeID: rightInfo.NodeID,
		CurrentKeys: keys, DiscoveryCA: caPEM, DiscoveryTargets: map[string]string{"one.example": firstTarget, "two.example": secondTarget},
	}
	rightConfig := peerProcessConfig{
		IdentityID: identityID, IdentityHead: head, LocalNodeID: rightInfo.NodeID, RemoteNodeID: leftInfo.NodeID,
		CurrentKeys: keys, DiscoveryCA: caPEM, DiscoveryTargets: map[string]string{"one.example": firstTarget, "two.example": secondTarget},
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if attempt == 2 {
			left = startPeerTransportProcess(t, leftDir)
			right = startPeerTransportProcess(t, rightDir)
			if left.ready != leftInfo || right.ready != rightInfo {
				t.Fatalf("node identity changed after process restart: left=%+v right=%+v", left.ready, right.ready)
			}
		}
		left.configure(t, leftConfig)
		right.configure(t, rightConfig)
		left.send(t, peerProcessCommand{Operation: "connect"})
		right.send(t, peerProcessCommand{Operation: "accept"})
		leftConnected := left.next(t, "connected", 30*time.Second)
		rightConnected := right.next(t, "connected", 30*time.Second)
		if leftConnected.RemoteNodeID != rightInfo.NodeID || rightConnected.RemoteNodeID != leftInfo.NodeID {
			t.Fatalf("handshake remote identity mismatch: left=%+v right=%+v", leftConnected, rightConnected)
		}

		payload := fmt.Sprintf("authenticated process data frame attempt=%d", attempt)
		left.send(t, peerProcessCommand{Operation: "send", Payload: payload})
		frame := right.next(t, "frame", 10*time.Second)
		if frame.Payload != payload {
			t.Fatalf("authenticated bounded channel payload=%q, want %q", frame.Payload, payload)
		}
		left.send(t, peerProcessCommand{Operation: "oversize"})
		if event := left.next(t, "oversize-result", 5*time.Second); !event.Bounded {
			t.Fatalf("oversized data frame was not rejected by channel bound: %+v", event)
		}
		stopPeerTransportProcess(t, left)
		stopPeerTransportProcess(t, right)
	}
}

type peerProcessNodeInfo struct {
	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
}

type peerProcessConfig struct {
	IdentityID       string            `json:"identity_id"`
	IdentityHead     string            `json:"identity_head"`
	LocalNodeID      string            `json:"local_node_id"`
	RemoteNodeID     string            `json:"remote_node_id"`
	CurrentKeys      map[string]string `json:"current_keys"`
	DiscoveryCA      string            `json:"discovery_ca"`
	DiscoveryTargets map[string]string `json:"discovery_targets"`
}

type peerProcessCommand struct {
	Operation string `json:"operation"`
	Payload   string `json:"payload,omitempty"`
}

type peerProcessEvent struct {
	Kind         string `json:"kind"`
	NodeID       string `json:"node_id,omitempty"`
	PublicKey    string `json:"public_key,omitempty"`
	RemoteNodeID string `json:"remote_node_id,omitempty"`
	Payload      string `json:"payload,omitempty"`
	Bounded      bool   `json:"bounded,omitempty"`
}

type peerTransportProcess struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	events chan peerProcessEvent
	ready  peerProcessNodeInfo
	stderr *synchronizedBuffer
	wait   chan error
	write  sync.Mutex
}

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(data)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func startPeerTransportProcess(t *testing.T, nodeDir string) *peerTransportProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPeerTransportSeparateProcessAcceptance$")
	cmd.Env = append(os.Environ(), "IR_PEER_PROCESS_HELPER=1", "IR_PEER_NODE_DIR="+nodeDir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &peerTransportProcess{cmd: cmd, stdin: bufio.NewWriter(stdin), events: make(chan peerProcessEvent, 16), stderr: &synchronizedBuffer{}, wait: make(chan error, 1)}
	cmd.Stderr = process.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start peer transport subprocess: %v", err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), MaxRemoteSDPBytes*2)
		for scanner.Scan() {
			var event peerProcessEvent
			if json.Unmarshal(scanner.Bytes(), &event) != nil {
				continue // Ignore the Go test runner's final PASS/FAIL banner.
			}
			process.events <- event
		}
	}()
	go func() { process.wait <- cmd.Wait() }()
	t.Cleanup(func() {
		select {
		case <-process.wait:
			return
		default:
		}
		_ = process.cmd.Process.Kill()
		select {
		case <-process.wait:
		case <-time.After(time.Second):
		}
	})
	ready := process.next(t, "ready", 10*time.Second)
	process.ready = peerProcessNodeInfo{NodeID: ready.NodeID, PublicKey: ready.PublicKey}
	return process
}

func (p *peerTransportProcess) configure(t *testing.T, config peerProcessConfig) {
	t.Helper()
	p.send(t, config)
}

func (p *peerTransportProcess) send(t *testing.T, value any) {
	t.Helper()
	p.write.Lock()
	defer p.write.Unlock()
	if err := json.NewEncoder(p.stdin).Encode(value); err != nil {
		t.Fatalf("write peer subprocess command: %v; stderr=%s", err, p.stderr.String())
	}
	if err := p.stdin.Flush(); err != nil {
		t.Fatalf("flush peer subprocess command: %v; stderr=%s", err, p.stderr.String())
	}
}

func (p *peerTransportProcess) next(t *testing.T, kind string, timeout time.Duration) peerProcessEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event := <-p.events:
			if event.Kind == kind {
				return event
			}
		case err := <-p.wait:
			p.wait <- err
			t.Fatalf("peer subprocess exited before %q (err=%v stderr=%s)", kind, err, p.stderr.String())
		case <-timer.C:
			t.Fatalf("timed out waiting for peer subprocess event %q (stderr=%s)", kind, p.stderr.String())
		}
	}
}

func stopPeerTransportProcess(t *testing.T, process *peerTransportProcess) {
	t.Helper()
	process.send(t, peerProcessCommand{Operation: "stop"})
	process.next(t, "stopped", 5*time.Second)
	select {
	case err := <-process.wait:
		process.wait <- err
		if err != nil {
			t.Fatalf("peer subprocess exit failed: %v stderr=%s", err, process.stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = process.cmd.Process.Kill()
		t.Fatal("peer subprocess did not exit after graceful stop")
	}
}

// Test helper runs only in an exec'd copy of this test binary. The resolver
// receives an explicit, test-created current-key projection; this isolates the
// transport process boundary and does not pretend to exercise Control Plane
// federation freshness, delegation policy, or RBAC.
func runPeerTransportProcessHelper(t *testing.T) {
	t.Helper()
	store, err := nodeidentity.Open(os.Getenv("IR_PEER_NODE_DIR"))
	if err != nil {
		t.Fatalf("open durable Node Key: %v", err)
	}
	info, err := store.Info()
	if errors.Is(err, nodeidentity.ErrNotCreated) {
		info, err = store.Create()
	}
	if err != nil {
		t.Fatalf("load durable Node Key: %v", err)
	}
	writePeerProcessEvent(peerProcessEvent{Kind: "ready", NodeID: info.NodeID, PublicKey: info.PublicKey})

	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 4096), 1<<20)
	if !input.Scan() {
		t.Fatalf("peer subprocess config was not provided: %v", input.Err())
	}
	var config peerProcessConfig
	if err := json.Unmarshal(input.Bytes(), &config); err != nil {
		t.Fatalf("decode peer subprocess config: %v", err)
	}
	if config.IdentityID == "" || config.IdentityHead == "" || config.LocalNodeID != info.NodeID || config.RemoteNodeID == "" || len(config.CurrentKeys) < 3 {
		t.Fatal("peer subprocess config is invalid")
	}
	currentKeys := make(map[string]ed25519.PublicKey, len(config.CurrentKeys))
	for id, encoded := range config.CurrentKeys {
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(key) != ed25519.PublicKeySize {
			t.Fatalf("decode current public key for %s", id)
		}
		currentKeys[id] = ed25519.PublicKey(key)
	}
	rootPEM := []byte(config.DiscoveryCA)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rootPEM) {
		t.Fatal("test Discovery CA could not be loaded")
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			target := config.DiscoveryTargets[host]
			if target == "" {
				return nil, errors.New("test Discovery target is unavailable")
			}
			return dialer.DialContext(ctx, network, target)
		},
	}
	defer transport.CloseIdleConnections()
	broker, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("create process FederationClient: %v", err)
	}
	now := time.Now().UTC().Add(-time.Second)
	resolveSignal := func(id, head string, _ time.Time) (signalingprotocol.IdentityState, error) {
		if id != config.IdentityID || head != "" && head != config.IdentityHead {
			return signalingprotocol.IdentityState{}, signalingprotocol.ErrStale
		}
		return signalingprotocol.IdentityState{IdentityID: config.IdentityID, HeadHash: config.IdentityHead, VerifiedAt: now, FreshUntil: now.Add(10 * time.Minute), CurrentKeys: currentKeys}, nil
	}
	resolvePeer := func(id, head string, _ time.Time) (authority.IdentityState, error) {
		if id != config.IdentityID || head != "" && head != config.IdentityHead {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return authority.IdentityState{IdentityID: config.IdentityID, HeadHash: config.IdentityHead, VerifiedAt: now, FreshUntil: now.Add(10 * time.Minute), CurrentKeys: currentKeys}, nil
	}
	options := SignalingOptions{
		Broker: broker, SignalSigner: store, SignalReplay: nodeidentity.PeerSignalReplayConsumer{NodeStore: store},
		SignalIdentityResolver: resolveSignal, HelloSigner: store, PeerIdentityResolver: resolvePeer,
		PeerReplay: nodeidentity.PeerReplayConsumer{NodeStore: store}, IdentityID: config.IdentityID,
		IdentityHead: config.IdentityHead, NodeID: info.NodeID, PollInterval: 20 * time.Millisecond,
		AuthorizePeer: func(identityID, nodeID string, _ time.Time) error {
			if identityID != config.IdentityID || nodeID != config.RemoteNodeID || len(currentKeys["node:"+nodeID]) != ed25519.PublicKeySize {
				return authority.ErrDenied
			}
			return nil
		},
		ICE: ICEConfig{},
	}

	if !input.Scan() {
		t.Fatalf("peer subprocess operation was not provided: %v", input.Err())
	}
	var operation peerProcessCommand
	if err := json.Unmarshal(input.Bytes(), &operation); err != nil || operation.Operation != "connect" && operation.Operation != "accept" {
		t.Fatal("peer subprocess operation is invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var peer *EstablishedPeer
	if operation.Operation == "connect" {
		peer, err = EstablishOutgoing(ctx, options, config.IdentityID, config.RemoteNodeID)
	} else {
		peer, err = AcceptIncoming(ctx, options)
	}
	if err != nil {
		t.Fatalf("establish peer transport: %v", err)
	}
	defer peer.Close()
	if peer.Data.Remote().ID != config.RemoteNodeID {
		t.Fatal("authenticated peer Node ID does not match configured remote")
	}
	pair, err := peer.PeerConnection.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
	if err != nil || pair == nil || pair.Local.Typ == webrtc.ICECandidateTypeRelay || pair.Remote.Typ == webrtc.ICECandidateTypeRelay ||
		pair.Local.Protocol != webrtc.ICEProtocolUDP || pair.Remote.Protocol != webrtc.ICEProtocolUDP {
		t.Fatalf("process acceptance selected a relayed or non-UDP path: pair=%+v err=%v", pair, err)
	}
	writePeerProcessEvent(peerProcessEvent{Kind: "connected", RemoteNodeID: peer.Data.Remote().ID})
	go func() {
		for {
			frame, err := peer.Data.Receive(context.Background())
			if err != nil {
				return
			}
			writePeerProcessEvent(peerProcessEvent{Kind: "frame", Payload: string(frame)})
		}
	}()

	for input.Scan() {
		var command peerProcessCommand
		if err := json.Unmarshal(input.Bytes(), &command); err != nil {
			t.Fatalf("decode peer subprocess command: %v", err)
		}
		switch command.Operation {
		case "send":
			if err := peer.Data.Send(context.Background(), []byte(command.Payload)); err != nil {
				t.Fatalf("send authenticated data frame: %v", err)
			}
		case "oversize":
			err := peer.Data.Send(context.Background(), bytes.Repeat([]byte("x"), MaxDataFrame+1))
			writePeerProcessEvent(peerProcessEvent{Kind: "oversize-result", Bounded: errors.Is(err, ErrFrameTooLarge)})
		case "stop":
			if err := peer.Close(); err != nil {
				t.Fatalf("close peer: %v", err)
			}
			writePeerProcessEvent(peerProcessEvent{Kind: "stopped"})
			return
		default:
			t.Fatalf("unsupported peer subprocess operation %q", command.Operation)
		}
	}
	if err := input.Err(); err != nil {
		t.Fatalf("read peer subprocess commands: %v", err)
	}
}

var peerProcessOutputMu sync.Mutex

func writePeerProcessEvent(event peerProcessEvent) {
	peerProcessOutputMu.Lock()
	defer peerProcessOutputMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(event)
}

func encodeTestCertificate(raw []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}))
}
