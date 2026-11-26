// Package peertransport provides the bounded WebRTC data-channel transport
// used by distributed Recorder peers. It transports opaque application
// frames; identity and capability authorization remain in peerprotocol and
// the archive/lease services.
package peertransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/pion/webrtc/v4"
)

const (
	ControlChannelLabel = "ir-control-v1"
	DataChannelLabel    = "ir-data-v1"
	MaxControlFrame     = 32 << 10
	MaxDataFrame        = 16 << 10
	MaxRemoteSDPBytes   = 256 << 10
	sctpReceiveBytes    = 4 << 20
	sendQueueHighWater  = 1 << 20
	sendQueueLowWater   = 256 << 10
)

var (
	ErrConfig              = errors.New("peer ICE configuration is invalid")
	ErrSDP                 = errors.New("peer session description is invalid")
	ErrFrameTooLarge       = errors.New("peer data-channel frame exceeds its bound")
	ErrChannelClosed       = errors.New("peer data channel is not open")
	ErrMessageRejected     = errors.New("peer data-channel message was rejected")
	ErrUnauthenticatedData = errors.New("peer data arrived before node authentication")
)

// ICEConfig contains short-lived ICE credentials. Callers must not persist or
// log TURN credentials. An empty server list is permitted for host-candidate
// tests and same-network operation; production deployments normally configure
// STUN and an authorized TURN server. RelayOnly requires at least one TURN URL.
type ICEConfig struct {
	Servers   []webrtc.ICEServer
	RelayOnly bool
}

type HandshakeRole string

const (
	RoleInitiator HandshakeRole = "initiator"
	RoleResponder HandshakeRole = "responder"
)

// NodeSigner is satisfied by nodeidentity.Store. Private Node Keys are never
// returned to the transport package.
type NodeSigner interface {
	SignPeerHello(string, string, time.Time) (peerprotocol.Hello, error)
	SignPeerHelloForSession(string, string, string, time.Time) (peerprotocol.Hello, error)
	SignPeerTranscriptProof(peerprotocol.Transcript) (string, error)
}

type handshakeFrame struct {
	Type      string              `json:"type"`
	Hello     *peerprotocol.Hello `json:"hello,omitempty"`
	Proof     string              `json:"proof,omitempty"`
	SessionID string              `json:"session_id,omitempty"`
}

// NewPeerConnection builds a Pion WebRTC peer with a strict SCTP message and
// receive-buffer limit. RelayOnly is an explicit route policy, never inferred
// from a published relay ID or an unreachable peer.
func NewPeerConnection(config ICEConfig) (*webrtc.PeerConnection, error) {
	if err := validateICEConfig(config); err != nil {
		return nil, err
	}
	setting := webrtc.SettingEngine{}
	setting.SetSCTPMaxMessageSize(MaxControlFrame)
	setting.SetSCTPMaxReceiveBufferSize(sctpReceiveBytes)
	api := webrtc.NewAPI(webrtc.WithSettingEngine(setting))
	servers := make([]webrtc.ICEServer, len(config.Servers))
	for i, server := range config.Servers {
		servers[i] = webrtc.ICEServer{
			URLs: append([]string(nil), server.URLs...), Username: server.Username,
			Credential: server.Credential, CredentialType: server.CredentialType,
		}
	}
	policy := webrtc.ICETransportPolicyAll
	if config.RelayOnly {
		policy = webrtc.ICETransportPolicyRelay
	}
	peer, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: servers, ICETransportPolicy: policy})
	if err != nil {
		return nil, ErrConfig
	}
	return peer, nil
}

func validateICEConfig(config ICEConfig) error {
	if len(config.Servers) > 16 {
		return ErrConfig
	}
	hasTURN := false
	for _, server := range config.Servers {
		if len(server.URLs) == 0 || len(server.URLs) > 8 {
			return ErrConfig
		}
		serverHasTURN := false
		for _, raw := range server.URLs {
			parsed, err := url.Parse(strings.TrimSpace(raw))
			if err != nil || parsed.User != nil || parsed.Fragment != "" || parsed.Opaque == "" && parsed.Host == "" {
				return ErrConfig
			}
			scheme := strings.ToLower(parsed.Scheme)
			if parsed.RawQuery != "" {
				query, queryErr := url.ParseQuery(parsed.RawQuery)
				if (scheme != "turn" && scheme != "turns") || queryErr != nil || len(query) != 1 || len(query["transport"]) != 1 || (query.Get("transport") != "udp" && query.Get("transport") != "tcp") {
					return ErrConfig
				}
			}
			switch scheme {
			case "stun", "stuns":
			case "turn", "turns":
				serverHasTURN, hasTURN = true, true
			default:
				return ErrConfig
			}
		}
		if serverHasTURN && (server.Username == "" || server.Credential == "") {
			return ErrConfig
		}
	}
	if config.RelayOnly && !hasTURN {
		return ErrConfig
	}
	return nil
}

// CreateChannels creates ordered control and data channels with distinct
// bounds. The receiver must still validate application frames and authorize
// each command after the peerprotocol handshake.
func CreateChannels(peer *webrtc.PeerConnection) (control, data *webrtc.DataChannel, err error) {
	if peer == nil {
		return nil, nil, ErrConfig
	}
	ordered := true
	control, err = peer.CreateDataChannel(ControlChannelLabel, &webrtc.DataChannelInit{Ordered: &ordered})
	if err != nil {
		return nil, nil, ErrConfig
	}
	data, err = peer.CreateDataChannel(DataChannelLabel, &webrtc.DataChannelInit{Ordered: &ordered})
	if err != nil {
		_ = control.Close()
		return nil, nil, ErrConfig
	}
	return control, data, nil
}

// CompleteOffer performs non-trickle ICE gathering and returns the complete
// offer for an authenticated signaling channel. Applications must encrypt or
// otherwise protect signaling descriptions when candidate addresses are
// sensitive; discovery is not an authorization boundary.
func CompleteOffer(ctx context.Context, peer *webrtc.PeerConnection) (webrtc.SessionDescription, error) {
	if ctx == nil || peer == nil {
		return webrtc.SessionDescription{}, ErrSDP
	}
	done := webrtc.GatheringCompletePromise(peer)
	offer, err := peer.CreateOffer(nil)
	if err != nil || len(offer.SDP) > MaxRemoteSDPBytes {
		return webrtc.SessionDescription{}, ErrSDP
	}
	if err := peer.SetLocalDescription(offer); err != nil {
		return webrtc.SessionDescription{}, ErrSDP
	}
	select {
	case <-done:
	case <-ctx.Done():
		return webrtc.SessionDescription{}, ctx.Err()
	}
	description := peer.LocalDescription()
	if description == nil || description.Type != webrtc.SDPTypeOffer || len(description.SDP) == 0 || len(description.SDP) > MaxRemoteSDPBytes {
		return webrtc.SessionDescription{}, ErrSDP
	}
	return *description, nil
}

// AcceptOffer validates and applies a bounded remote offer, gathers local ICE
// candidates and returns a complete answer. The signed peer handshake must be
// completed before any application command or archive object is accepted.
func AcceptOffer(ctx context.Context, peer *webrtc.PeerConnection, offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	if ctx == nil || peer == nil || offer.Type != webrtc.SDPTypeOffer || len(offer.SDP) == 0 || len(offer.SDP) > MaxRemoteSDPBytes {
		return webrtc.SessionDescription{}, ErrSDP
	}
	if err := peer.SetRemoteDescription(offer); err != nil {
		return webrtc.SessionDescription{}, ErrSDP
	}
	done := webrtc.GatheringCompletePromise(peer)
	answer, err := peer.CreateAnswer(nil)
	if err != nil || len(answer.SDP) > MaxRemoteSDPBytes {
		return webrtc.SessionDescription{}, ErrSDP
	}
	if err := peer.SetLocalDescription(answer); err != nil {
		return webrtc.SessionDescription{}, ErrSDP
	}
	select {
	case <-done:
	case <-ctx.Done():
		return webrtc.SessionDescription{}, ctx.Err()
	}
	description := peer.LocalDescription()
	if description == nil || description.Type != webrtc.SDPTypeAnswer || len(description.SDP) == 0 || len(description.SDP) > MaxRemoteSDPBytes {
		return webrtc.SessionDescription{}, ErrSDP
	}
	return *description, nil
}

func ApplyAnswer(peer *webrtc.PeerConnection, answer webrtc.SessionDescription) error {
	if peer == nil || answer.Type != webrtc.SDPTypeAnswer || len(answer.SDP) == 0 || len(answer.SDP) > MaxRemoteSDPBytes {
		return ErrSDP
	}
	if err := peer.SetRemoteDescription(answer); err != nil {
		return ErrSDP
	}
	return nil
}

// WaitOpen waits for an open channel and fails on cancellation or closure.
func WaitOpen(ctx context.Context, channel *webrtc.DataChannel) error {
	if ctx == nil || channel == nil {
		return ErrChannelClosed
	}
	if channel.ReadyState() == webrtc.DataChannelStateOpen {
		return nil
	}
	opened := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	channel.OnOpen(func() {
		select {
		case opened <- struct{}{}:
		default:
		}
	})
	channel.OnClose(func() {
		select {
		case closed <- struct{}{}:
		default:
		}
	})
	if channel.ReadyState() == webrtc.DataChannelStateOpen {
		return nil
	}
	if channel.ReadyState() == webrtc.DataChannelStateClosed {
		return ErrChannelClosed
	}
	select {
	case <-opened:
		return nil
	case <-closed:
		return ErrChannelClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetBoundedMessageHandler closes the channel on an oversized frame and
// passes a private copy to the handler. The handler should enqueue bounded
// work rather than block Pion's SCTP read loop.
func SetBoundedMessageHandler(channel *webrtc.DataChannel, maxBytes int, handler func([]byte) error) error {
	if channel == nil || maxBytes < 1 || maxBytes > channelFrameLimit(channel) || handler == nil {
		return ErrConfig
	}
	channel.OnMessage(func(message webrtc.DataChannelMessage) {
		if len(message.Data) > maxBytes {
			_ = channel.Close()
			return
		}
		if err := handler(append([]byte(nil), message.Data...)); err != nil {
			_ = channel.Close()
		}
	})
	return nil
}

// Sender enforces a per-channel frame limit and bounds the buffered outbound
// queue with Pion's bufferedAmount low-water notification.
type Sender struct {
	channel   *webrtc.DataChannel
	max       int
	low       chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
}

// AuthenticatedDataChannel is the application-data interface returned only
// after both Node Keys authenticate the handshake transcript. Callers must
// treat the raw Pion data channel as private to AuthenticateDataChannel.
type AuthenticatedDataChannel struct {
	remote        authority.Principal
	sessionID     string
	sender        *Sender
	inbound       chan []byte
	channel       *webrtc.DataChannel
	controlSender *Sender
	controlInbox  chan []byte
	control       *webrtc.DataChannel
}

// AuthenticateDataChannel rejects application data before the control-channel
// handshake succeeds, then exposes only bounded Send/Receive methods. On
// error, the caller remains responsible for closing the PeerConnection.
func AuthenticateDataChannel(ctx context.Context, role HandshakeRole, control, data *webrtc.DataChannel, signer NodeSigner, identityID, identityHead string, resolve peerprotocol.IdentityResolver, replay peerprotocol.ReplayConsumer) (*AuthenticatedDataChannel, error) {
	return authenticateDataChannelWithInbox(ctx, role, control, data, nil, signer, identityID, identityHead, resolve, replay)
}

// AuthenticateDataChannelWithInbox authenticates a channel whose bounded
// control-message handler was installed before this call. This is required by
// transports that publish a channel to a peer before authentication begins:
// Pion drops messages received before OnMessage is registered. The caller must
// route messages into controlInbox using SetBoundedMessageHandler.
func AuthenticateDataChannelWithInbox(ctx context.Context, role HandshakeRole, control, data *webrtc.DataChannel, controlInbox chan []byte, signer NodeSigner, identityID, identityHead string, resolve peerprotocol.IdentityResolver, replay peerprotocol.ReplayConsumer) (*AuthenticatedDataChannel, error) {
	if controlInbox == nil {
		return nil, ErrConfig
	}
	return authenticateDataChannelWithInbox(ctx, role, control, data, controlInbox, signer, identityID, identityHead, resolve, replay)
}

func authenticateDataChannelWithInbox(ctx context.Context, role HandshakeRole, control, data *webrtc.DataChannel, controlInbox chan []byte, signer NodeSigner, identityID, identityHead string, resolve peerprotocol.IdentityResolver, replay peerprotocol.ReplayConsumer) (*AuthenticatedDataChannel, error) {
	if ctx == nil || control == nil || data == nil || data.Label() != DataChannelLabel {
		return nil, ErrConfig
	}
	if controlInbox == nil {
		controlInbox = make(chan []byte, 32)
		if err := SetBoundedMessageHandler(control, MaxControlFrame, func(message []byte) error {
			select {
			case controlInbox <- message:
				return nil
			default:
				return ErrMessageRejected
			}
		}); err != nil {
			return nil, err
		}
	}
	inbound := make(chan []byte, 64)
	var dataAuthenticated atomic.Bool
	if err := SetBoundedMessageHandler(data, MaxDataFrame, func(message []byte) error {
		if !dataAuthenticated.Load() {
			return ErrUnauthenticatedData
		}
		select {
		case inbound <- message:
			return nil
		default:
			return ErrMessageRejected
		}
	}); err != nil {
		return nil, err
	}
	if err := WaitOpen(ctx, data); err != nil {
		return nil, err
	}
	remote, sessionID, err := authenticateWithInbox(ctx, role, control, controlInbox, signer, identityID, identityHead, resolve, replay)
	if err != nil {
		return nil, err
	}
	if data.ReadyState() != webrtc.DataChannelStateOpen {
		return nil, ErrUnauthenticatedData
	}
	dataAuthenticated.Store(true)
	sender, err := NewSender(data, MaxDataFrame)
	if err != nil {
		return nil, err
	}
	if control == nil || control.ReadyState() != webrtc.DataChannelStateOpen {
		return nil, ErrChannelClosed
	}
	controlSender, err := NewSender(control, MaxControlFrame)
	if err != nil {
		return nil, err
	}
	if err := completeApplicationReadiness(ctx, control, controlInbox, controlSender, sessionID); err != nil {
		return nil, err
	}
	return &AuthenticatedDataChannel{remote: remote, sessionID: sessionID, sender: sender, inbound: inbound, channel: data, controlSender: controlSender, controlInbox: controlInbox, control: control}, nil
}

// completeApplicationReadiness prevents either endpoint from exposing a
// session while the other endpoint is still switching from handshake-only
// handlers to bounded application handlers. A command/data frame sent after
// the peer wrapper becomes available is therefore never lost at that switch.
func completeApplicationReadiness(ctx context.Context, control *webrtc.DataChannel, inbox <-chan []byte, sender *Sender, sessionID string) error {
	if ctx == nil || control == nil || inbox == nil || sender == nil || sessionID == "" {
		return ErrConfig
	}
	ready, err := json.Marshal(handshakeFrame{Type: "ready", SessionID: sessionID})
	if err == nil {
		ready, err = identityprotocol.Canonicalize(ready)
	}
	if err != nil || len(ready) == 0 || len(ready) > MaxControlFrame {
		return ErrMessageRejected
	}
	if err := sender.Send(ctx, ready); err != nil {
		return err
	}
	frame, err := receiveHandshakeFrame(ctx, inbox)
	if err != nil {
		return err
	}
	if frame.Type != "ready" || frame.SessionID != sessionID {
		return ErrMessageRejected
	}
	return nil
}

// Remote returns the Node principal proven by the mutual handshake.
func (s *AuthenticatedDataChannel) Remote() authority.Principal {
	if s == nil {
		return authority.Principal{}
	}
	return s.remote
}

// SessionID returns the random session identifier authenticated by both Node
// Keys. It is included in signed application commands to prevent cross-session
// replay and must not be selected by callers.
func (s *AuthenticatedDataChannel) SessionID() string {
	if s == nil {
		return ""
	}
	return s.sessionID
}

// Send transmits one bounded opaque application frame after authentication.
func (s *AuthenticatedDataChannel) Send(ctx context.Context, payload []byte) error {
	if s == nil || s.sender == nil {
		return ErrChannelClosed
	}
	return s.sender.Send(ctx, payload)
}

// Receive waits for one bounded opaque frame or for closure/cancellation.
func (s *AuthenticatedDataChannel) Receive(ctx context.Context) ([]byte, error) {
	if s == nil || ctx == nil || s.sender == nil {
		return nil, ErrChannelClosed
	}
	select {
	case message := <-s.inbound:
		return append([]byte(nil), message...), nil
	case <-s.sender.closed:
		return nil, ErrChannelClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SendCommand writes one bounded control command over the control data channel
// only after mutual Node-Key authentication. Callers must create the signed
// command through nodeidentity.Store and provide a current Capability chain.
func (s *AuthenticatedDataChannel) SendCommand(ctx context.Context, command peerprotocol.Command) error {
	if s == nil || s.controlSender == nil {
		return ErrChannelClosed
	}
	encoded, err := json.Marshal(command)
	if err != nil || len(encoded) > MaxControlFrame {
		return ErrFrameTooLarge
	}
	return s.controlSender.Send(ctx, encoded)
}

// PeerCommandReplayConsumer atomically consumes a signed request identifier
// after current authority checks and before a caller performs side effects.
type PeerCommandReplayConsumer interface {
	ConsumeCommand(peerprotocol.Command, peerprotocol.IdentityResolver, time.Time, string, authority.Principal, string, string) error
}

// ReceiveAuthorizedCommand reads, parses, session-binds, capability-checks,
// and durably consumes one peer command. A command dispatcher must still
// reauthorize immediately before a later side effect if execution is deferred.
func (s *AuthenticatedDataChannel) ReceiveAuthorizedCommand(ctx context.Context, resolve peerprotocol.IdentityResolver, authorizer peerprotocol.CommandAuthorizer, replay PeerCommandReplayConsumer, localIdentityID, localNodeID string) (peerprotocol.Command, error) {
	if s == nil || ctx == nil || resolve == nil || authorizer == nil || replay == nil || s.control == nil || s.controlSender == nil || s.sessionID == "" {
		return peerprotocol.Command{}, ErrChannelClosed
	}
	var raw []byte
	select {
	case raw = <-s.controlInbox:
	case <-s.controlSender.closed:
		return peerprotocol.Command{}, ErrChannelClosed
	case <-ctx.Done():
		return peerprotocol.Command{}, ctx.Err()
	}
	command, err := peerprotocol.ParseCommand(raw)
	if err != nil {
		return peerprotocol.Command{}, ErrMessageRejected
	}
	// Measure expiry after dequeue so queueing or a slow caller cannot extend
	// the signed command's short lifetime.
	now := time.Now().UTC()
	if err := peerprotocol.VerifyAndAuthorizeCommand(ctx, command, resolve, authorizer, now, s.sessionID, s.remote, localIdentityID, localNodeID); err != nil {
		return peerprotocol.Command{}, err
	}
	if err := replay.ConsumeCommand(command, resolve, now, s.sessionID, s.remote, localIdentityID, localNodeID); err != nil {
		return peerprotocol.Command{}, err
	}
	return command, nil
}

// Close closes the application data channel. It does not close its peer.
func (s *AuthenticatedDataChannel) Close() error {
	if s == nil {
		return nil
	}
	var dataErr, controlErr error
	if s.channel != nil {
		dataErr = s.channel.Close()
	}
	if s.control != nil {
		controlErr = s.control.Close()
	}
	return errors.Join(dataErr, controlErr)
}

func NewSender(channel *webrtc.DataChannel, maxBytes int) (*Sender, error) {
	if channel == nil || maxBytes < 1 || maxBytes > channelFrameLimit(channel) {
		return nil, ErrConfig
	}
	sender := &Sender{channel: channel, max: maxBytes, low: make(chan struct{}, 1), closed: make(chan struct{})}
	channel.SetBufferedAmountLowThreshold(sendQueueLowWater)
	channel.OnBufferedAmountLow(func() {
		select {
		case sender.low <- struct{}{}:
		default:
		}
	})
	channel.OnClose(func() { sender.closeOnce.Do(func() { close(sender.closed) }) })
	return sender, nil
}

func channelFrameLimit(channel *webrtc.DataChannel) int {
	if channel == nil {
		return 0
	}
	switch channel.Label() {
	case ControlChannelLabel:
		return MaxControlFrame
	case DataChannelLabel:
		return MaxDataFrame
	default:
		return 0
	}
}

func (s *Sender) Send(ctx context.Context, payload []byte) error {
	if s == nil || s.channel == nil || ctx == nil || len(payload) == 0 || len(payload) > s.max {
		return ErrFrameTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel.ReadyState() != webrtc.DataChannelStateOpen {
		return ErrChannelClosed
	}
	for s.channel.BufferedAmount()+uint64(len(payload)) > sendQueueHighWater {
		select {
		case <-s.low:
		case <-s.closed:
			return ErrChannelClosed
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.channel.ReadyState() != webrtc.DataChannelStateOpen {
			return ErrChannelClosed
		}
	}
	if err := s.channel.Send(payload); err != nil {
		return ErrChannelClosed
	}
	return nil
}

// WaitConnection waits for ICE/DTLS to reach a usable state or terminate. It
// does not itself authenticate the remote peer.
func WaitConnection(ctx context.Context, peer *webrtc.PeerConnection) error {
	if ctx == nil || peer == nil {
		return ErrConfig
	}
	state := make(chan webrtc.PeerConnectionState, 4)
	peer.OnConnectionStateChange(func(next webrtc.PeerConnectionState) {
		select {
		case state <- next:
		default:
		}
	})
	current := peer.ConnectionState()
	if current == webrtc.PeerConnectionStateConnected {
		return nil
	}
	if current == webrtc.PeerConnectionStateFailed || current == webrtc.PeerConnectionStateClosed {
		return ErrChannelClosed
	}
	for {
		select {
		case next := <-state:
			switch next {
			case webrtc.PeerConnectionStateConnected:
				return nil
			case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
				return ErrChannelClosed
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Authenticate performs the mutual Node Key handshake over an open ordered
// control channel. No application data command should be accepted until this
// returns the verified remote Node principal. Both peers must have the exact
// current identity head available; stale or imported-only histories fail
// closed through resolver.
func Authenticate(ctx context.Context, role HandshakeRole, control *webrtc.DataChannel, signer NodeSigner, identityID, identityHead string, resolve peerprotocol.IdentityResolver, replay peerprotocol.ReplayConsumer) (authority.Principal, error) {
	remote, _, err := authenticateWithInbox(ctx, role, control, nil, signer, identityID, identityHead, resolve, replay)
	return remote, err
}

func authenticateWithInbox(ctx context.Context, role HandshakeRole, control *webrtc.DataChannel, preparedInbox chan []byte, signer NodeSigner, identityID, identityHead string, resolve peerprotocol.IdentityResolver, replay peerprotocol.ReplayConsumer) (authority.Principal, string, error) {
	if ctx == nil || control == nil || control.Label() != ControlChannelLabel || signer == nil || identityID == "" || identityHead == "" || resolve == nil || replay == nil || (role != RoleInitiator && role != RoleResponder) {
		return authority.Principal{}, "", ErrConfig
	}
	if err := WaitOpen(ctx, control); err != nil {
		return authority.Principal{}, "", err
	}
	inbound := preparedInbox
	if inbound == nil {
		inbound = make(chan []byte, 4)
		if err := SetBoundedMessageHandler(control, MaxControlFrame, func(message []byte) error {
			select {
			case inbound <- message:
				return nil
			default:
				return ErrMessageRejected
			}
		}); err != nil {
			return authority.Principal{}, "", err
		}
	}

	now := time.Now().UTC().Truncate(time.Second)
	var local, remote peerprotocol.Hello
	var transcript peerprotocol.Transcript
	if role == RoleInitiator {
		var err error
		local, err = signer.SignPeerHello(identityID, identityHead, now)
		if err != nil {
			return authority.Principal{}, "", err
		}
		if err := sendHandshakeFrame(control, handshakeFrame{Type: "hello", Hello: &local}); err != nil {
			return authority.Principal{}, "", err
		}
		frame, err := receiveHandshakeFrame(ctx, inbound)
		if err != nil {
			return authority.Principal{}, "", err
		}
		if frame.Type != "hello" || frame.Hello == nil {
			return authority.Principal{}, "", ErrMessageRejected
		}
		remote = *frame.Hello
		if _, _, err := peerprotocol.VerifyHello(remote, resolve, time.Now().UTC()); err != nil {
			return authority.Principal{}, "", err
		}
		transcript = peerprotocol.Transcript{Version: peerprotocol.HandshakeVersion, Initiator: local, Responder: remote}
		proof, err := signer.SignPeerTranscriptProof(transcript)
		if err != nil {
			return authority.Principal{}, "", err
		}
		transcript.InitiatorProof = proof
		if err := sendHandshakeFrame(control, handshakeFrame{Type: "proof", Proof: proof}); err != nil {
			return authority.Principal{}, "", err
		}
		frame, err = receiveHandshakeFrame(ctx, inbound)
		if err != nil {
			return authority.Principal{}, "", err
		}
		if frame.Type != "proof" || frame.Proof == "" {
			return authority.Principal{}, "", ErrMessageRejected
		}
		transcript.ResponderProof = frame.Proof
	} else {
		frame, err := receiveHandshakeFrame(ctx, inbound)
		if err != nil {
			return authority.Principal{}, "", err
		}
		if frame.Type != "hello" || frame.Hello == nil {
			return authority.Principal{}, "", ErrMessageRejected
		}
		remote = *frame.Hello
		if _, _, err := peerprotocol.VerifyHello(remote, resolve, time.Now().UTC()); err != nil {
			return authority.Principal{}, "", err
		}
		local, err = signer.SignPeerHelloForSession(identityID, identityHead, remote.SessionID, time.Now().UTC().Truncate(time.Second))
		if err != nil {
			return authority.Principal{}, "", err
		}
		if err := sendHandshakeFrame(control, handshakeFrame{Type: "hello", Hello: &local}); err != nil {
			return authority.Principal{}, "", err
		}
		frame, err = receiveHandshakeFrame(ctx, inbound)
		if err != nil {
			return authority.Principal{}, "", err
		}
		if frame.Type != "proof" || frame.Proof == "" {
			return authority.Principal{}, "", ErrMessageRejected
		}
		transcript = peerprotocol.Transcript{Version: peerprotocol.HandshakeVersion, Initiator: remote, Responder: local, InitiatorProof: frame.Proof}
		proof, err := signer.SignPeerTranscriptProof(transcript)
		if err != nil {
			return authority.Principal{}, "", err
		}
		transcript.ResponderProof = proof
		if err := sendHandshakeFrame(control, handshakeFrame{Type: "proof", Proof: proof}); err != nil {
			return authority.Principal{}, "", err
		}
	}
	initiator, responder, err := peerprotocol.VerifyTranscript(transcript, resolve, time.Now().UTC())
	if err != nil {
		return authority.Principal{}, "", err
	}
	if err := replay.Consume(transcript.Initiator, resolve, time.Now().UTC()); err != nil {
		return authority.Principal{}, "", err
	}
	if err := replay.Consume(transcript.Responder, resolve, time.Now().UTC()); err != nil {
		return authority.Principal{}, "", err
	}
	if role == RoleInitiator {
		return responder, transcript.Initiator.SessionID, nil
	}
	return initiator, transcript.Initiator.SessionID, nil
}

func sendHandshakeFrame(control *webrtc.DataChannel, frame handshakeFrame) error {
	encoded, err := json.Marshal(frame)
	if err == nil {
		encoded, err = identityprotocol.Canonicalize(encoded)
	}
	if err != nil || len(encoded) == 0 || len(encoded) > MaxControlFrame || control == nil || control.ReadyState() != webrtc.DataChannelStateOpen {
		return ErrMessageRejected
	}
	if err := control.Send(encoded); err != nil {
		return ErrChannelClosed
	}
	return nil
}

func receiveHandshakeFrame(ctx context.Context, inbound <-chan []byte) (handshakeFrame, error) {
	select {
	case <-ctx.Done():
		return handshakeFrame{}, ctx.Err()
	case encoded := <-inbound:
		if len(encoded) == 0 || len(encoded) > MaxControlFrame {
			return handshakeFrame{}, ErrMessageRejected
		}
		canonical, err := identityprotocol.Canonicalize(encoded)
		if err != nil || !bytes.Equal(canonical, encoded) {
			return handshakeFrame{}, ErrMessageRejected
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		var frame handshakeFrame
		if decoder.Decode(&frame) != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return handshakeFrame{}, ErrMessageRejected
		}
		switch frame.Type {
		case "hello":
			if frame.Hello == nil || frame.Proof != "" {
				return handshakeFrame{}, ErrMessageRejected
			}
		case "proof":
			if frame.Hello != nil || frame.Proof == "" {
				return handshakeFrame{}, ErrMessageRejected
			}
		case "ready":
			if frame.Hello != nil || frame.Proof != "" || frame.SessionID == "" {
				return handshakeFrame{}, ErrMessageRejected
			}
		default:
			return handshakeFrame{}, ErrMessageRejected
		}
		return frame, nil
	}
}
