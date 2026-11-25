package peerapi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
)

type managerTestSession struct {
	nodeID     string
	identityID string
	sessionID  string
	closed     chan struct{}
	one        sync.Once
}

func newManagerTestSession(nodeID, identityID, sessionID string) *managerTestSession {
	return &managerTestSession{nodeID: nodeID, identityID: identityID, sessionID: sessionID, closed: make(chan struct{})}
}

func (s *managerTestSession) Remote() authority.Principal {
	return authority.Principal{Kind: authority.PrincipalNode, ID: s.nodeID}
}
func (s *managerTestSession) SessionID() string                                       { return s.sessionID }
func (s *managerTestSession) RemoteIdentityID() string                                { return s.identityID }
func (s *managerTestSession) Send(context.Context, []byte) error                      { return nil }
func (s *managerTestSession) SendCommand(context.Context, peerprotocol.Command) error { return nil }
func (s *managerTestSession) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, peertransport.ErrChannelClosed
	}
}
func (s *managerTestSession) ReceiveAuthorizedCommand(ctx context.Context, _ peerprotocol.IdentityResolver, _ peerprotocol.CommandAuthorizer, _ peertransport.PeerCommandReplayConsumer, _, _ string) (peerprotocol.Command, error) {
	<-ctx.Done()
	return peerprotocol.Command{}, ctx.Err()
}
func (s *managerTestSession) Close() error {
	s.one.Do(func() { close(s.closed) })
	return nil
}

type managerTestTransport struct {
	accepted chan PeerSession
	dialed   PeerSession
}

func (t *managerTestTransport) Dial(ctx context.Context, _, _ string) (PeerSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return t.dialed, nil
}
func (t *managerTestTransport) Accept(ctx context.Context) (PeerSession, error) {
	select {
	case session := <-t.accepted:
		return session, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type managerTestCatalog struct{}

func (managerTestCatalog) Head(context.Context, string) (archivesnapshot.Snapshot, error) {
	return archivesnapshot.Snapshot{}, errors.New("unused")
}
func (managerTestCatalog) Snapshot(context.Context, string, string) (archivesnapshot.Snapshot, error) {
	return archivesnapshot.Snapshot{}, errors.New("unused")
}

type managerTestAuthorizer struct{}

func (managerTestAuthorizer) Authorize(context.Context, string, string, string, string, authority.Principal, []authority.SignedGrant, time.Time) error {
	return nil
}

type managerTestReplay struct{}

func (managerTestReplay) ConsumeCommand(peerprotocol.Command, peerprotocol.IdentityResolver, time.Time, string, authority.Principal, string, string) error {
	return nil
}

func testPeerService(nodeID string) *Service {
	return &Service{
		Catalog: managerTestCatalog{}, Authorizer: managerTestAuthorizer{}, IdentityID: "gi_test", NodeID: nodeID,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, errors.New("unused")
		},
		Replay: managerTestReplay{},
	}
}

func TestSessionManagerConnectsListsAndClosesPeerLifecycle(t *testing.T) {
	session := newManagerTestSession("nd_AAAAAAAAAAAAAAAAAAAAAA", "gi_remote", "session_A")
	transport := &managerTestTransport{accepted: make(chan PeerSession, 1), dialed: session}
	manager, err := NewSessionManager(transport, testPeerService("nd_local"))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Connect(t.Context(), "gi_remote", session.nodeID); !errors.Is(err, ErrSessionManagerClosed) {
		t.Fatalf("connect before Start=%v", err)
	}
	if err := manager.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Connect(t.Context(), "gi_remote", session.nodeID); err != nil {
		t.Fatalf("connect authenticated peer: %v", err)
	}
	listed := manager.List()
	if len(listed) != 1 || listed[0].IdentityID != "gi_remote" || listed[0].NodeID != session.nodeID || listed[0].SessionID != session.sessionID {
		t.Fatalf("session projection=%+v", listed)
	}
	if err := manager.Connect(t.Context(), "gi_remote", session.nodeID); !errors.Is(err, ErrPeerAlreadyConnected) {
		t.Fatalf("duplicate connect=%v", err)
	}
	if err := manager.Disconnect(session.nodeID); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if got := manager.List(); len(got) != 0 {
		t.Fatalf("sessions remain after disconnect: %+v", got)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(t.Context()); !errors.Is(err, ErrSessionManagerClosed) {
		t.Fatalf("Start after Close=%v", err)
	}
}

func TestSessionManagerAcceptsIncomingAndRejectsDuplicateNode(t *testing.T) {
	transport := &managerTestTransport{accepted: make(chan PeerSession, 2)}
	manager, err := NewSessionManager(transport, testPeerService("nd_local"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	first := newManagerTestSession("nd_BBBBBBBBBBBBBBBBBBBBBB", "gi_remote", "session_B")
	transport.accepted <- first
	if !waitForPeerSession(t, manager, first.nodeID) {
		t.Fatal("incoming authenticated peer was not registered")
	}
	duplicate := newManagerTestSession(first.nodeID, "gi_remote", "session_C")
	transport.accepted <- duplicate
	select {
	case <-duplicate.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("duplicate peer session was not closed")
	}
	if got := manager.List(); len(got) != 1 || got[0].SessionID != first.sessionID {
		t.Fatalf("duplicate replaced active peer session: %+v", got)
	}
	cancel()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("manager close did not close active peer")
	}
}

func TestSessionManagerStopsAndRestartsAcrossGenerationHandoff(t *testing.T) {
	transport := &managerTestTransport{accepted: make(chan PeerSession, 1)}
	manager, err := NewSessionManager(transport, testPeerService("nd_local"))
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	if err := manager.Start(firstCtx); err != nil {
		t.Fatal(err)
	}
	first := newManagerTestSession("nd_CCCCCCCCCCCCCCCCCCCCCC", "gi_test", "session_first")
	transport.accepted <- first
	if !waitForPeerSession(t, manager, first.nodeID) {
		t.Fatal("first generation did not accept the authenticated session")
	}
	cancelFirst()
	if err := manager.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("handoff Stop did not close the peer session")
	}
	if len(manager.List()) != 0 {
		t.Fatalf("session survived handoff Stop: %+v", manager.List())
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("restart manager after handoff: %v", err)
	}
	second := newManagerTestSession("nd_DDDDDDDDDDDDDDDDDDDDDD", "gi_test", "session_second")
	transport.accepted <- second
	if !waitForPeerSession(t, manager, second.nodeID) {
		t.Fatal("resumed generation did not accept a new authenticated session")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionManagerSlotLazilyRebuildsAndRetriesUnavailableSetup(t *testing.T) {
	var mu sync.Mutex
	available := false
	factories := 0
	slot, err := NewSessionManagerSlot(func(context.Context) (*SessionManager, error) {
		mu.Lock()
		defer mu.Unlock()
		factories++
		if !available {
			return nil, ErrSessionNetworkingUnavailable
		}
		transport := &managerTestTransport{accepted: make(chan PeerSession)}
		return NewSessionManager(transport, testPeerService("nd_local"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := slot.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if slot.Available() {
		t.Fatal("peer slot became available before identity prerequisites")
	}
	mu.Lock()
	available = true
	mu.Unlock()
	if !slot.Available() {
		t.Fatal("peer slot did not retry after identity setup became ready")
	}
	if err := slot.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := slot.Start(context.Background()); err != nil {
		t.Fatalf("slot did not restart after handoff: %v", err)
	}
	if !slot.Available() {
		t.Fatal("peer slot did not reconstruct manager after handoff")
	}
	mu.Lock()
	gotFactories := factories
	mu.Unlock()
	if gotFactories != 4 {
		t.Fatalf("manager factory calls=%d, want 4", gotFactories)
	}
	if err := slot.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitForPeerSession(t *testing.T, manager *SessionManager, nodeID string) bool {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		for _, info := range manager.List() {
			if info.NodeID == nodeID {
				return true
			}
		}
		select {
		case <-deadline:
			return false
		case <-ticker.C:
		}
	}
}
