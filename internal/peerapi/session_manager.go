package peerapi

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
)

var (
	ErrSessionManagerClosed = errors.New("peer session manager is closed")
	ErrPeerAlreadyConnected = errors.New("peer node already has an active session")
	ErrPeerNotConnected     = errors.New("peer node has no active session")
)

// PeerSession is a transport-authenticated session accepted by the manager.
// RemoteIdentityID is set only from the signed signaling exchange and
// Remote() is the Node-Key identity proven by the WebRTC handshake.
type PeerSession interface {
	Session
	SendCommand(context.Context, peerprotocol.Command) error
	RemoteIdentityID() string
	Close() error
}

// Transport establishes authenticated sessions. Production uses
// SignalingTransport; the interface also lets lifecycle tests control
// connection arrival and closure without weakening command authorization.
type Transport interface {
	Dial(context.Context, string, string) (PeerSession, error)
	Accept(context.Context) (PeerSession, error)
}

type SignalingTransport struct {
	Options peertransport.SignalingOptions
	// ResolveOptions refreshes the local identity head and other short-lived
	// signaling inputs for each handshake. Identity events can revoke a Node
	// Key while the Control generation remains running, so a head captured at
	// process startup must not be reused indefinitely.
	ResolveOptions func() (peertransport.SignalingOptions, error)
}

func (t SignalingTransport) Dial(ctx context.Context, identityID, nodeID string) (PeerSession, error) {
	options, err := t.options()
	if err != nil {
		return nil, err
	}
	peer, err := peertransport.EstablishOutgoing(ctx, options, identityID, nodeID)
	if err != nil {
		return nil, err
	}
	return establishedSession{EstablishedPeer: peer}, nil
}

func (t SignalingTransport) Accept(ctx context.Context) (PeerSession, error) {
	options, err := t.options()
	if err != nil {
		return nil, err
	}
	peer, err := peertransport.AcceptIncoming(ctx, options)
	if err != nil {
		return nil, err
	}
	return establishedSession{EstablishedPeer: peer}, nil
}

func (t SignalingTransport) options() (peertransport.SignalingOptions, error) {
	if t.ResolveOptions != nil {
		return t.ResolveOptions()
	}
	return t.Options, nil
}

type establishedSession struct{ *peertransport.EstablishedPeer }

func (s establishedSession) Remote() authority.Principal { return s.Data.Remote() }
func (s establishedSession) SessionID() string           { return s.Data.SessionID() }
func (s establishedSession) Send(ctx context.Context, data []byte) error {
	return s.Data.Send(ctx, data)
}
func (s establishedSession) Receive(ctx context.Context) ([]byte, error) {
	return s.Data.Receive(ctx)
}
func (s establishedSession) SendCommand(ctx context.Context, command peerprotocol.Command) error {
	return s.Data.SendCommand(ctx, command)
}
func (s establishedSession) ReceiveAuthorizedCommand(ctx context.Context, resolve peerprotocol.IdentityResolver, authorizer peerprotocol.CommandAuthorizer, replay peertransport.PeerCommandReplayConsumer, identityID, nodeID string) (peerprotocol.Command, error) {
	return s.Data.ReceiveAuthorizedCommand(ctx, resolve, authorizer, replay, identityID, nodeID)
}
func (s establishedSession) RemoteIdentityID() string { return s.EstablishedPeer.RemoteIdentityID }

type managedPeer struct {
	session   PeerSession
	identity  string
	node      string
	connected time.Time
	cancel    context.CancelFunc
}

type SessionInfo struct {
	IdentityID  string    `json:"identity_id"`
	NodeID      string    `json:"node_id"`
	SessionID   string    `json:"-"`
	ConnectedAt time.Time `json:"connected_at"`
}

// SessionManager owns the lifetime of authenticated peer sessions. Its
// service processes every command through the existing fresh identity,
// durable replay, and capability checks; the manager itself grants no
// archive permission.
type SessionManager struct {
	transport Transport
	service   *Service

	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	stopping bool
	closed   bool
	stopDone chan struct{}
	pending  map[string]struct{}
	peers    map[string]*managedPeer
	wg       sync.WaitGroup
}

func NewSessionManager(transport Transport, service *Service) (*SessionManager, error) {
	if transport == nil || service == nil {
		return nil, ErrInvalidRequest
	}
	return &SessionManager{transport: transport, service: service, pending: make(map[string]struct{}), peers: make(map[string]*managedPeer)}, nil
}

// Start begins the incoming signaling loop. The supplied context owns all
// accepted sessions; outgoing sessions established by Connect share this
// manager lifetime after the handshake completes.
func (m *SessionManager) Start(parent context.Context) error {
	if m == nil || parent == nil {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.started || m.stopping {
		return ErrSessionManagerClosed
	}
	m.ctx, m.cancel = context.WithCancel(parent)
	m.started = true
	m.wg.Add(1)
	go m.acceptLoop(m.ctx)
	return nil
}

// Connect establishes a signed signaling exchange and mutual Node-Key
// handshake. A successful session is retained independently of the request
// context and remains owned by the manager until disconnect or shutdown.
func (m *SessionManager) Connect(ctx context.Context, identityID, nodeID string) error {
	if m == nil || ctx == nil || identityID == "" || nodeID == "" {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	if m.closed || !m.started {
		m.mu.Unlock()
		return ErrSessionManagerClosed
	}
	if _, ok := m.peers[nodeID]; ok {
		m.mu.Unlock()
		return ErrPeerAlreadyConnected
	}
	if _, ok := m.pending[nodeID]; ok {
		m.mu.Unlock()
		return ErrPeerAlreadyConnected
	}
	m.pending[nodeID] = struct{}{}
	m.wg.Add(1)
	managerCtx := m.ctx
	m.mu.Unlock()
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.pending, nodeID)
		m.mu.Unlock()
	}()

	dialCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(managerCtx, cancel)
	defer func() { stopCancel(); cancel() }()
	peer, err := m.transport.Dial(dialCtx, identityID, nodeID)
	if err != nil {
		return err
	}
	if peer == nil || peer.Remote().Kind != authority.PrincipalNode || peer.Remote().ID != nodeID || peer.RemoteIdentityID() != identityID {
		if peer != nil {
			_ = peer.Close()
		}
		return peertransport.ErrMessageRejected
	}
	if err := m.attach(peer); err != nil {
		_ = peer.Close()
		return err
	}
	return nil
}

func (m *SessionManager) Disconnect(nodeID string) error {
	if m == nil || nodeID == "" {
		return ErrInvalidRequest
	}
	m.mu.Lock()
	peer := m.peers[nodeID]
	if peer != nil {
		delete(m.peers, nodeID)
	}
	m.mu.Unlock()
	if peer == nil {
		return ErrPeerNotConnected
	}
	peer.cancel()
	return peer.session.Close()
}

func (m *SessionManager) List() []SessionInfo {
	if m == nil {
		return []SessionInfo{}
	}
	m.mu.RLock()
	result := make([]SessionInfo, 0, len(m.peers))
	for _, peer := range m.peers {
		result = append(result, SessionInfo{IdentityID: peer.identity, NodeID: peer.node, SessionID: peer.session.SessionID(), ConnectedAt: peer.connected})
	}
	m.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result
}

// Client creates a scope-bound archive client for one active authenticated
// session. The manager fills target identity/node from the signed signaling
// exchange and rejects caller attempts to substitute another peer.
func (m *SessionManager) Client(nodeID string, maxResponseBytes int, scope RequestScope) (*Client, error) {
	if m == nil || nodeID == "" {
		return nil, ErrInvalidRequest
	}
	m.mu.RLock()
	peer := m.peers[nodeID]
	if peer == nil {
		m.mu.RUnlock()
		return nil, ErrPeerNotConnected
	}
	if scope.TargetIdentityID != "" && scope.TargetIdentityID != peer.identity || scope.TargetNodeID != "" && scope.TargetNodeID != peer.node {
		m.mu.RUnlock()
		return nil, peertransport.ErrMessageRejected
	}
	session, identity, targetNode := peer.session, peer.identity, peer.node
	m.mu.RUnlock()
	scope.TargetIdentityID, scope.TargetNodeID = identity, targetNode
	return NewScopedClient(session, maxResponseBytes, scope)
}

func (m *SessionManager) Close() error {
	if m == nil {
		return nil
	}
	return m.stop(true)
}

// Stop closes current connections and waits for in-flight handshakes and
// request loops. The manager can be started again after a generation handoff;
// Close permanently retires it.
func (m *SessionManager) Stop() error { return m.stop(false) }

func (m *SessionManager) stop(final bool) error {
	m.mu.Lock()
	if final {
		m.closed = true
	}
	if m.stopping {
		done := m.stopDone
		m.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	m.started = false
	m.stopping = true
	m.stopDone = make(chan struct{})
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.ctx = nil
	peers := make([]*managedPeer, 0, len(m.peers))
	for nodeID, peer := range m.peers {
		delete(m.peers, nodeID)
		peers = append(peers, peer)
	}
	m.mu.Unlock()
	var closeErr error
	for _, peer := range peers {
		peer.cancel()
		closeErr = errors.Join(closeErr, peer.session.Close())
	}
	m.wg.Wait()
	m.mu.Lock()
	m.stopping = false
	close(m.stopDone)
	m.stopDone = nil
	m.mu.Unlock()
	return closeErr
}

func (m *SessionManager) acceptLoop(ctx context.Context) {
	defer m.wg.Done()
	for {
		peer, err := m.transport.Accept(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			// A failing signaling provider or invalid exchange does not become
			// authorization. Retry after a bounded delay while keeping local
			// recording independent of peer availability.
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case <-timer.C:
			}
			continue
		}
		if peer == nil || peer.Remote().Kind != authority.PrincipalNode || peer.Remote().ID == "" || peer.RemoteIdentityID() == "" {
			if peer != nil {
				_ = peer.Close()
			}
			continue
		}
		if err := m.attach(peer); err != nil {
			_ = peer.Close()
		}
	}
}

func (m *SessionManager) attach(session PeerSession) error {
	remote := session.Remote()
	identityID, nodeID := session.RemoteIdentityID(), remote.ID
	if remote.Kind != authority.PrincipalNode || identityID == "" || nodeID == "" || session.SessionID() == "" {
		return peertransport.ErrMessageRejected
	}
	m.mu.Lock()
	if m.closed || !m.started || m.ctx == nil {
		m.mu.Unlock()
		return ErrSessionManagerClosed
	}
	if _, exists := m.peers[nodeID]; exists {
		m.mu.Unlock()
		return ErrPeerAlreadyConnected
	}
	ctx, cancel := context.WithCancel(m.ctx)
	managed := &managedPeer{session: session, identity: identityID, node: nodeID, connected: time.Now().UTC(), cancel: cancel}
	m.peers[nodeID] = managed
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		_ = m.service.Serve(ctx, session)
		cancel()
		_ = session.Close()
		m.mu.Lock()
		if m.peers[nodeID] == managed {
			delete(m.peers, nodeID)
		}
		m.mu.Unlock()
	}()
	return nil
}
