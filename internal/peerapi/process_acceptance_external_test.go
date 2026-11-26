package peerapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

// This acceptance test crosses two OS process boundaries. Each process opens
// its own durable Node Key and the peers signal through two independently
// persisted HTTPS Discovery stores. After mutual Node-Key authentication, the
// provider executes a real peerapi snapshot command against a durable signed
// OwnerSubject log and the requester verifies the immutable snapshot response.
// It is same-host coverage; it does not claim independent-node, cross-network,
// TURN, or production identity-history acceptance.
func TestPeerAPIMultiProcessSnapshotAcceptance(t *testing.T) {
	if os.Getenv("IR_PEER_API_PROCESS_HELPER") == "1" {
		runPeerAPIProcessHelper(t)
		return
	}

	root := t.TempDir()
	firstStore, err := discovery.OpenDirectory(filepath.Join(root, "discovery-one"))
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := discovery.OpenDirectory(filepath.Join(root, "discovery-two"))
	if err != nil {
		t.Fatal(err)
	}
	caPEM, firstTarget, secondTarget := startPeerAPIDiscovery(t, firstStore, secondStore)

	requester := startPeerAPIProcess(t, filepath.Join(root, "requester-node"))
	provider := startPeerAPIProcess(t, filepath.Join(root, "provider-node"))
	if requester.ready.NodeID == provider.ready.NodeID || requester.ready.PublicKey == provider.ready.PublicKey {
		t.Fatal("separate process Node Key stores reused a node identity")
	}

	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("b", 64)
	now := time.Now().UTC().Truncate(time.Second)
	verifiedAt := now.Add(-time.Second)
	identity := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"identity:" + identityID:         rootPublic,
			"node:" + requester.ready.NodeID: peerAPIProcessNodePublic(t, requester.ready),
			"node:" + provider.ready.NodeID:  peerAPIProcessNodePublic(t, provider.ready),
		},
	}
	snapshot := peerAPITestSnapshot(t)
	ownerSubject, err := authority.NewPersonOwnerSubject(identityID, "1", now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issuer := authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}
	createEvent, err := authority.NewAuthorityEventForSigner(ownerSubject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: ownerSubject, ArchiveIDs: []string{snapshot.Content.ArchiveID}}, issuer, identity, now)
	if err != nil {
		t.Fatalf("construct signed OwnerSubject event: %v", err)
	}
	signedCreate, err := authority.SignAuthorityEvent(createEvent, rootPrivate)
	if err != nil {
		t.Fatalf("sign OwnerSubject event: %v", err)
	}
	previousHash, err := authority.AuthorityEventHash(createEvent)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubject.ID, Issuer: issuer,
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: requester.ready.NodeID},
		ArchiveID: snapshot.Content.ArchiveID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x31}, 32)),
		PolicyEpoch: "1", IssuerKeyID: keyID,
	}
	signedGrant, err := authority.SignGrant(grant, rootPrivate)
	if err != nil {
		t.Fatalf("sign archive.view capability: %v", err)
	}
	deniedGrant := grant
	deniedGrant.Nonce = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x72}, 32))
	signedUncommittedGrant, err := authority.SignGrant(deniedGrant, rootPrivate)
	if err != nil {
		t.Fatalf("sign uncommitted archive.view capability: %v", err)
	}
	issueEvent, err := authority.NewAuthorityEventForSigner(ownerSubject.ID, "2", previousHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{signedGrant}}, issuer, identity, now)
	if err != nil {
		t.Fatalf("construct committed capability event: %v", err)
	}
	signedIssue, err := authority.SignAuthorityEvent(issueEvent, rootPrivate)
	if err != nil {
		t.Fatalf("sign capability event: %v", err)
	}

	encodedKeys := make(map[string]string, len(identity.CurrentKeys))
	for id, key := range identity.CurrentKeys {
		encodedKeys[id] = base64.RawURLEncoding.EncodeToString(key)
	}
	grantBytes, err := json.Marshal([]authority.SignedGrant{signedGrant})
	if err != nil {
		t.Fatal(err)
	}
	uncommittedGrantBytes, err := json.Marshal([]authority.SignedGrant{signedUncommittedGrant})
	if err != nil {
		t.Fatal(err)
	}
	eventsBytes, err := json.Marshal([]authority.SignedAuthorityEvent{signedCreate, signedIssue})
	if err != nil {
		t.Fatal(err)
	}
	snapshotBytes, err := archivesnapshot.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	common := peerAPIProcessConfig{
		IdentityID: identityID, IdentityHead: head, CurrentKeys: encodedKeys, DiscoveryCA: caPEM,
		DiscoveryTargets: map[string]string{"one.example": firstTarget, "two.example": secondTarget},
		GrantChain:       grantBytes, UncommittedGrantChain: uncommittedGrantBytes,
		AuthorityEvents: eventsBytes, Snapshot: base64.RawURLEncoding.EncodeToString(snapshotBytes),
		OwnerSubjectID: ownerSubject.ID, ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
	}
	requesterConfig := common
	requesterConfig.Role, requesterConfig.LocalNodeID, requesterConfig.RemoteNodeID = "requester", requester.ready.NodeID, provider.ready.NodeID
	providerConfig := common
	providerConfig.Role, providerConfig.LocalNodeID, providerConfig.RemoteNodeID = "provider", provider.ready.NodeID, requester.ready.NodeID
	requester.send(t, requesterConfig)
	provider.send(t, providerConfig)
	requester.send(t, peerAPIProcessCommand{Operation: "connect"})
	provider.send(t, peerAPIProcessCommand{Operation: "accept"})

	connected := requester.next(t, "connected", 35*time.Second)
	if connected.RemoteNodeID != provider.ready.NodeID {
		t.Fatalf("requester authenticated unexpected peer %q", connected.RemoteNodeID)
	}
	if got := requester.next(t, "snapshot", 15*time.Second); got.SnapshotID != snapshot.SnapshotID || got.ContentHash != snapshot.ContentHash {
		t.Fatalf("authenticated snapshot response mismatch: %+v", got)
	}
	provider.next(t, "command-denied", 8*time.Second)
	denied := requester.next(t, "uncommitted-capability-denied", 8*time.Second)
	if !denied.Denied {
		t.Fatalf("provider accepted an uncommitted signed capability: %+v", denied)
	}
	requester.send(t, peerAPIProcessCommand{Operation: "stop"})
	provider.send(t, peerAPIProcessCommand{Operation: "stop"})
	requester.waitForExit(t)
	provider.waitForExit(t)
}

type peerAPIProcessConfig struct {
	Role                  string            `json:"role"`
	IdentityID            string            `json:"identity_id"`
	IdentityHead          string            `json:"identity_head"`
	LocalNodeID           string            `json:"local_node_id"`
	RemoteNodeID          string            `json:"remote_node_id"`
	CurrentKeys           map[string]string `json:"current_keys"`
	DiscoveryCA           string            `json:"discovery_ca"`
	DiscoveryTargets      map[string]string `json:"discovery_targets"`
	GrantChain            json.RawMessage   `json:"grant_chain"`
	AuthorityEvents       json.RawMessage   `json:"authority_events"`
	UncommittedGrantChain json.RawMessage   `json:"uncommitted_grant_chain"`
	Snapshot              string            `json:"snapshot"`
	OwnerSubjectID        string            `json:"owner_subject_id"`
	ArchiveID             string            `json:"archive_id"`
	SnapshotID            string            `json:"snapshot_id"`
}

type peerAPIProcessCommand struct {
	Operation string `json:"operation"`
}

type peerAPIProcessEvent struct {
	Kind         string `json:"kind"`
	NodeID       string `json:"node_id,omitempty"`
	PublicKey    string `json:"public_key,omitempty"`
	RemoteNodeID string `json:"remote_node_id,omitempty"`
	SnapshotID   string `json:"snapshot_id,omitempty"`
	ContentHash  string `json:"content_hash,omitempty"`
	Denied       bool   `json:"denied,omitempty"`
}

type peerAPIProcess struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	events chan peerAPIProcessEvent
	ready  peerAPIProcessEvent
	stderr *peerAPIProcessBuffer
	wait   chan error
	write  sync.Mutex
}

func startPeerAPIProcess(t *testing.T, nodeDir string) *peerAPIProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPeerAPIMultiProcessSnapshotAcceptance$")
	cmd.Env = append(os.Environ(), "IR_PEER_API_PROCESS_HELPER=1", "IR_PEER_API_NODE_DIR="+nodeDir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &peerAPIProcess{cmd: cmd, stdin: bufio.NewWriter(stdin), events: make(chan peerAPIProcessEvent, 16), stderr: &peerAPIProcessBuffer{}, wait: make(chan error, 1)}
	cmd.Stderr = process.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start peer API process: %v", err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 2<<20)
		for scanner.Scan() {
			var event peerAPIProcessEvent
			if json.Unmarshal(scanner.Bytes(), &event) == nil {
				process.events <- event
			}
		}
	}()
	go func() { process.wait <- cmd.Wait() }()
	t.Cleanup(func() {
		select {
		case <-process.wait:
			return
		default:
			_ = process.cmd.Process.Kill()
			select {
			case <-process.wait:
			case <-time.After(2 * time.Second):
			}
		}
	})
	process.ready = process.next(t, "ready", 10*time.Second)
	return process
}

func (p *peerAPIProcess) send(t *testing.T, value any) {
	t.Helper()
	p.write.Lock()
	defer p.write.Unlock()
	if err := json.NewEncoder(p.stdin).Encode(value); err != nil {
		t.Fatalf("send peer API process input: %v (stderr=%s)", err, p.stderr.String())
	}
	if err := p.stdin.Flush(); err != nil {
		t.Fatalf("flush peer API process input: %v (stderr=%s)", err, p.stderr.String())
	}
}

func (p *peerAPIProcess) next(t *testing.T, kind string, timeout time.Duration) peerAPIProcessEvent {
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
			t.Fatalf("peer API process exited before %s: %v (stderr=%s)", kind, err, p.stderr.String())
		case <-timer.C:
			t.Fatalf("timed out waiting for peer API process event %s (stderr=%s)", kind, p.stderr.String())
		}
	}
}

func (p *peerAPIProcess) waitForExit(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.wait:
		if err != nil {
			t.Fatalf("peer API process exit: %v (stderr=%s)", err, p.stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatalf("peer API process did not stop (stderr=%s)", p.stderr.String())
	}
}

type peerAPIProcessBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (b *peerAPIProcessBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	const limit = 8 << 10
	if len(data) >= limit {
		b.b = append(b.b[:0], data[len(data)-limit:]...)
		return len(data), nil
	}
	if overflow := len(b.b) + len(data) - limit; overflow > 0 {
		copy(b.b, b.b[overflow:])
		b.b = b.b[:len(b.b)-overflow]
	}
	b.b = append(b.b, data...)
	return len(data), nil
}

func (b *peerAPIProcessBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(append([]byte(nil), b.b...))
}

func runPeerAPIProcessHelper(t *testing.T) {
	t.Helper()
	store, err := nodeidentity.Open(os.Getenv("IR_PEER_API_NODE_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Info()
	if errors.Is(err, nodeidentity.ErrNotCreated) {
		info, err = store.Create()
	}
	if err != nil {
		t.Fatalf("open durable process Node Key: %v", err)
	}
	writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "ready", NodeID: info.NodeID, PublicKey: info.PublicKey})
	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 4096), 2<<20)
	if !input.Scan() {
		t.Fatalf("read process configuration: %v", input.Err())
	}
	var config peerAPIProcessConfig
	if err := json.Unmarshal(input.Bytes(), &config); err != nil {
		t.Fatalf("decode process configuration: %v", err)
	}
	if config.Role == "" || config.LocalNodeID != info.NodeID || config.RemoteNodeID == "" || config.IdentityID == "" || config.IdentityHead == "" {
		t.Fatal("invalid peer API process configuration")
	}
	keys := make(map[string]ed25519.PublicKey, len(config.CurrentKeys))
	for id, encoded := range config.CurrentKeys {
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(key) != ed25519.PublicKeySize {
			t.Fatalf("invalid identity key fixture %s", id)
		}
		keys[id] = ed25519.PublicKey(key)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(config.DiscoveryCA)) {
		t.Fatal("failed to load HTTPS Discovery trust root")
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
				return nil, errors.New("unknown test Discovery endpoint")
			}
			return dialer.DialContext(ctx, network, target)
		},
	}
	defer transport.CloseIdleConnections()
	broker, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("create HTTPS Federation client: %v", err)
	}
	now := time.Now().UTC().Add(-time.Second)
	resolveSignal := func(id, head string, _ time.Time) (signalingprotocol.IdentityState, error) {
		if id != config.IdentityID || head != "" && head != config.IdentityHead {
			return signalingprotocol.IdentityState{}, signalingprotocol.ErrStale
		}
		return signalingprotocol.IdentityState{IdentityID: config.IdentityID, HeadHash: config.IdentityHead, VerifiedAt: now, FreshUntil: now.Add(4 * time.Minute), CurrentKeys: keys}, nil
	}
	resolvePeer := func(id, head string, _ time.Time) (authority.IdentityState, error) {
		if id != config.IdentityID || head != "" && head != config.IdentityHead {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return authority.IdentityState{IdentityID: config.IdentityID, HeadHash: config.IdentityHead, VerifiedAt: now, FreshUntil: now.Add(4 * time.Minute), CurrentKeys: keys}, nil
	}
	options := peertransport.SignalingOptions{
		Broker: broker, SignalSigner: store, SignalReplay: nodeidentity.PeerSignalReplayConsumer{NodeStore: store},
		SignalIdentityResolver: resolveSignal, HelloSigner: store, PeerIdentityResolver: resolvePeer,
		PeerReplay: nodeidentity.PeerReplayConsumer{NodeStore: store}, IdentityID: config.IdentityID,
		IdentityHead: config.IdentityHead, NodeID: info.NodeID, PollInterval: 20 * time.Millisecond,
		AuthorizePeer: func(identityID, nodeID string, _ time.Time) error {
			if identityID != config.IdentityID || nodeID != config.RemoteNodeID || len(keys["node:"+nodeID]) != ed25519.PublicKeySize {
				return authority.ErrDenied
			}
			return nil
		},
	}
	if !input.Scan() {
		t.Fatalf("read process role operation: %v", input.Err())
	}
	var operation peerAPIProcessCommand
	if err := json.Unmarshal(input.Bytes(), &operation); err != nil || operation.Operation != "connect" && operation.Operation != "accept" {
		t.Fatal("invalid process peer operation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	var peer *peertransport.EstablishedPeer
	if operation.Operation == "connect" {
		peer, err = peertransport.EstablishOutgoing(ctx, options, config.IdentityID, config.RemoteNodeID)
	} else {
		peer, err = peertransport.AcceptIncoming(ctx, options)
	}
	if err != nil {
		t.Fatalf("establish authenticated multi-process peer: %v", err)
	}
	defer peer.Close()
	writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "connected", RemoteNodeID: peer.Data.Remote().ID})

	if config.Role == "provider" {
		var events []authority.SignedAuthorityEvent
		if err := json.Unmarshal(config.AuthorityEvents, &events); err != nil || len(events) != 2 {
			t.Fatalf("decode signed authority fixture: events=%d err=%v", len(events), err)
		}
		stateStore, err := authority.Open(filepath.Join(os.Getenv("IR_PEER_API_NODE_DIR"), "authority"))
		if err != nil {
			t.Fatalf("open provider durable authority store: %v", err)
		}
		for _, event := range events {
			if err := stateStore.Apply(event, func(id, head string, at time.Time) (authority.IdentityState, error) {
				return resolvePeer(id, head, at)
			}, time.Now().UTC()); err != nil {
				t.Fatalf("apply owner-signed authority event: %v", err)
			}
		}
		snapshotBytes, err := base64.RawURLEncoding.DecodeString(config.Snapshot)
		if err != nil {
			t.Fatalf("decode immutable snapshot fixture: %v", err)
		}
		snapshot, err := archivesnapshot.Parse(snapshotBytes)
		if err != nil || archivesnapshot.Verify(snapshot) != nil {
			t.Fatalf("verify immutable snapshot fixture: %v", err)
		}
		service := Service{
			Catalog: snapshotCatalog{snapshot: snapshot}, Authorizer: lease.DurableAuthority{Store: stateStore, Resolve: resolvePeer},
			IdentityID: config.IdentityID, NodeID: info.NodeID, Resolve: resolvePeer,
			Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: store}, Signer: store,
		}
		serveCtx, stopServe := context.WithCancel(ctx)
		serveDone := make(chan error, 1)
		go func() {
			for {
				err := service.ServeOne(serveCtx, peer.Data)
				if err == nil {
					continue
				}
				if errors.Is(err, peerprotocol.ErrCommandDenied) {
					writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "command-denied"})
					continue
				}
				if serveCtx.Err() != nil {
					serveDone <- nil
				} else {
					serveDone <- err
				}
				return
			}
		}()
		if !input.Scan() {
			stopServe()
			t.Fatalf("read provider stop command: %v", input.Err())
		}
		if err := json.Unmarshal(input.Bytes(), &operation); err != nil || operation.Operation != "stop" {
			stopServe()
			t.Fatal("invalid provider stop command")
		}
		stopServe()
		_ = peer.Close()
		select {
		case <-serveDone:
		case <-time.After(3 * time.Second):
			t.Fatal("peer API provider did not stop")
		}
		writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "stopped"})
		return
	}

	var grants []authority.SignedGrant
	if err := json.Unmarshal(config.GrantChain, &grants); err != nil || len(grants) != 1 {
		t.Fatalf("decode requester capability fixture: grants=%d err=%v", len(grants), err)
	}
	resolve := func(id, head string, _ time.Time) (authority.IdentityState, error) {
		if id != config.IdentityID || head != "" && head != config.IdentityHead {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return authority.IdentityState{IdentityID: config.IdentityID, HeadHash: config.IdentityHead, VerifiedAt: now, FreshUntil: now.Add(4 * time.Minute), CurrentKeys: keys}, nil
	}
	nowFn := func() time.Time { return time.Now().UTC().Truncate(time.Second) }
	scope := RequestScope{
		IdentityID: config.IdentityID, IdentityHead: config.IdentityHead, NodeID: info.NodeID,
		TargetIdentityID: config.IdentityID, TargetNodeID: config.RemoteNodeID, OwnerSubjectID: config.OwnerSubjectID,
		ArchiveID: config.ArchiveID, SnapshotID: config.SnapshotID, Action: "archive.view", GrantChain: grants,
		Resolve: resolve, Signer: store, Now: nowFn,
	}
	client, err := NewScopedClient(peer.Data, MaxPeerResponseBytes, scope)
	if err != nil {
		t.Fatalf("construct scoped peer API client: %v", err)
	}
	ctxRequest, cancelRequest := context.WithTimeout(ctx, 10*time.Second)
	snapshot, err := client.GetSnapshot(ctxRequest, config.SnapshotID)
	cancelRequest()
	if err != nil {
		t.Fatalf("capability-checked cross-process snapshot read: %v", err)
	}
	writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "snapshot", SnapshotID: snapshot.SnapshotID, ContentHash: snapshot.ContentHash})

	// A second cryptographically valid but uncommitted capability must still be
	// rejected by the provider's durable authority log.
	var uncommittedGrants []authority.SignedGrant
	if err := json.Unmarshal(config.UncommittedGrantChain, &uncommittedGrants); err != nil || len(uncommittedGrants) != 1 {
		t.Fatalf("decode uncommitted capability fixture: grants=%d err=%v", len(uncommittedGrants), err)
	}
	deniedScope := scope
	deniedScope.GrantChain = uncommittedGrants
	deniedClient, err := NewScopedClient(peer.Data, MaxPeerResponseBytes, deniedScope)
	if err != nil {
		t.Fatalf("construct denied scoped client: %v", err)
	}
	ctxDenied, cancelDenied := context.WithTimeout(ctx, 4*time.Second)
	_, deniedErr := deniedClient.GetSnapshot(ctxDenied, config.SnapshotID)
	cancelDenied()
	if deniedErr == nil {
		t.Fatal("provider accepted a signed but uncommitted capability")
	}
	writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "uncommitted-capability-denied", Denied: true})
	if !input.Scan() {
		t.Fatalf("read requester stop command: %v", input.Err())
	}
	if err := json.Unmarshal(input.Bytes(), &operation); err != nil || operation.Operation != "stop" {
		t.Fatal("invalid requester stop command")
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("close requester peer: %v", err)
	}
	writePeerAPIProcessEvent(peerAPIProcessEvent{Kind: "stopped"})
}

func startPeerAPIDiscovery(t *testing.T, first, second *discovery.Directory) (string, string, string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "peerapi-acceptance-root"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	start := func(host string, directory *discovery.Directory) (*httptest.Server, string) {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(len(host) + 2)), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, caCert, key.Public(), caKey)
		if err != nil {
			t.Fatal(err)
		}
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		serverCert := tls.Certificate{Certificate: [][]byte{der, caDER}, PrivateKey: key, Leaf: certificate}
		server := httptest.NewUnstartedServer(discovery.HTTPHandler(directory))
		server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12}
		server.StartTLS()
		parsed, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(server.Close)
		return server, parsed.Host
	}
	_, firstTarget := start("one.example", first)
	_, secondTarget := start("two.example", second)
	return caPEM, firstTarget, secondTarget
}

func peerAPIProcessNodePublic(t *testing.T, ready peerAPIProcessEvent) ed25519.PublicKey {
	t.Helper()
	key, err := base64.RawURLEncoding.DecodeString(ready.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		t.Fatalf("invalid process Node Key public key (%d bytes): %v", len(key), err)
	}
	return ed25519.PublicKey(key)
}

func writePeerAPIProcessEvent(event peerAPIProcessEvent) {
	_, _ = fmt.Fprintf(os.Stdout, "%s\n", mustJSON(event))
}

func mustJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}
	return data
}
