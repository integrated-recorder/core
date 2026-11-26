package peertransport

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
	"github.com/pion/webrtc/v4"
)

var (
	ErrSignalingConfig = errors.New("peer signaling configuration is invalid")
	ErrSignalTimeout   = errors.New("peer signaling operation timed out")
)

// SignalingBroker is implemented by discovery.FederationClient. The broker
// moves opaque signed envelopes only; it cannot authorize a peer.
type SignalingBroker interface {
	PublishPeerSignal(context.Context, signalingprotocol.Message, time.Time) error
	LookupPeerSignals(context.Context, string) ([]signalingprotocol.Message, error)
}

type PeerSignalSigner interface {
	SignPeerSignal(signalingprotocol.Message, signalingprotocol.IdentityResolver, time.Time) (signalingprotocol.Message, error)
}

type PeerSignalReplayConsumer interface {
	Consume(signalingprotocol.Message, signalingprotocol.IdentityResolver, time.Time) error
}

// SignalingOptions supplies independent identity freshness, local Node Key,
// replay, transport and application handshake boundaries. AuthorizePeer must
// enforce the current node-level connection capability before SDP is shared;
// archive operations still require their own action/scope authorization.
type SignalingOptions struct {
	Broker                 SignalingBroker
	SignalSigner           PeerSignalSigner
	SignalReplay           PeerSignalReplayConsumer
	SignalIdentityResolver signalingprotocol.IdentityResolver
	HelloSigner            NodeSigner
	PeerIdentityResolver   peerprotocol.IdentityResolver
	PeerReplay             peerprotocol.ReplayConsumer
	IdentityID             string
	IdentityHead           string
	NodeID                 string
	ICE                    ICEConfig
	PollInterval           time.Duration
	AuthorizePeer          func(identityID, nodeID string, now time.Time) error
}

// EstablishedPeer is returned only after the WebRTC connection and mutual
// Node-Key transcript handshake succeed. Callers must still authorize every
// archive-specific request over the data channel.
type EstablishedPeer struct {
	PeerConnection   *webrtc.PeerConnection
	Data             *AuthenticatedDataChannel
	RemoteIdentityID string
}

// MonitorConnection watches an already authenticated peer connection. A brief
// ICE disconnect is recoverable; a persistent disconnect or terminal Pion
// state ends the session so the owner can perform a fresh authenticated dial.
func (p *EstablishedPeer) MonitorConnection(ctx context.Context, disconnectedGrace time.Duration) error {
	if p == nil || p.PeerConnection == nil || ctx == nil || disconnectedGrace <= 0 {
		return ErrSignalingConfig
	}
	return monitorConnectionStates(ctx, p.PeerConnection, disconnectedGrace)
}

func monitorConnectionStates(ctx context.Context, peer *webrtc.PeerConnection, disconnectedGrace time.Duration) error {
	states := make(chan webrtc.PeerConnectionState, 8)
	peer.OnConnectionStateChange(func(next webrtc.PeerConnectionState) {
		enqueueConnectionState(states, next)
	})
	return monitorConnectionStateStream(ctx, peer.ConnectionState(), states, disconnectedGrace)
}

func enqueueConnectionState(states chan webrtc.PeerConnectionState, next webrtc.PeerConnectionState) {
	select {
	case states <- next:
		return
	default:
	}
	// Connection state is a projection, so retain the newest transition when
	// the bounded queue fills. This preserves terminal states and prevents a
	// dropped Connected event from turning a recovered transient disconnect
	// into a false timeout.
	select {
	case <-states:
	default:
	}
	select {
	case states <- next:
	default:
	}
}

func monitorConnectionStateStream(ctx context.Context, current webrtc.PeerConnectionState, states <-chan webrtc.PeerConnectionState, disconnectedGrace time.Duration) error {
	var timer *time.Timer
	var timeout <-chan time.Time
	startGrace := func() {
		if timer == nil {
			timer = time.NewTimer(disconnectedGrace)
			timeout = timer.C
		}
	}
	stopGrace := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer = nil
			timeout = nil
		}
	}
	defer stopGrace()
	switch current {
	case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
		return ErrChannelClosed
	case webrtc.PeerConnectionStateDisconnected:
		startGrace()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return ErrChannelClosed
		case next := <-states:
			switch next {
			case webrtc.PeerConnectionStateConnected:
				stopGrace()
			case webrtc.PeerConnectionStateDisconnected:
				startGrace()
			case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
				return ErrChannelClosed
			}
		}
	}
}

func (p *EstablishedPeer) Close() error {
	if p == nil {
		return nil
	}
	var dataErr, peerErr error
	if p.Data != nil {
		dataErr = p.Data.Close()
	}
	if p.PeerConnection != nil {
		peerErr = p.PeerConnection.Close()
	}
	return errors.Join(dataErr, peerErr)
}

func (o SignalingOptions) validate() error {
	if o.Broker == nil || o.SignalSigner == nil || o.SignalReplay == nil || o.SignalIdentityResolver == nil ||
		o.HelloSigner == nil || o.PeerIdentityResolver == nil || o.PeerReplay == nil || o.IdentityID == "" || o.IdentityHead == "" ||
		o.NodeID == "" || o.AuthorizePeer == nil || validateICEConfig(o.ICE) != nil {
		return ErrSignalingConfig
	}
	return nil
}

// EstablishOutgoing performs a complete signed and encrypted signaling
// exchange, gathers ICE candidates, establishes Pion channels and mutually
// authenticates both Node Keys before returning application data access.
func EstablishOutgoing(ctx context.Context, options SignalingOptions, targetIdentityID, targetNodeID string) (*EstablishedPeer, error) {
	if ctx == nil || options.validate() != nil || targetIdentityID == "" || targetNodeID == "" {
		return nil, ErrSignalingConfig
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := options.AuthorizePeer(targetIdentityID, targetNodeID, now); err != nil {
		return nil, err
	}
	sessionID, err := signalingprotocol.NewSessionID(rand.Reader)
	if err != nil {
		return nil, err
	}
	ephemeral, err := signalingprotocol.NewEphemeralKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	init, err := signalingprotocol.NewMessage(signalingprotocol.KindInit, sessionID, options.IdentityID, options.IdentityHead, options.NodeID, targetIdentityID, targetNodeID, now, rand.Reader)
	if err != nil {
		return nil, err
	}
	init.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(ephemeral.PublicKey().Bytes())
	init, err = options.SignalSigner.SignPeerSignal(init, options.SignalIdentityResolver, now)
	if err != nil {
		return nil, err
	}
	if err := options.Broker.PublishPeerSignal(ctx, init, now); err != nil {
		return nil, err
	}

	accept, err := waitForSignal(ctx, options, func(message signalingprotocol.Message) bool {
		return message.Kind == signalingprotocol.KindAccept && message.SessionID == sessionID && message.SourceIdentityID == targetIdentityID && message.SourceNodeID == targetNodeID &&
			message.TargetIdentityID == options.IdentityID && message.TargetNodeID == options.NodeID
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for peer accept: %w", err)
	}
	if err := options.AuthorizePeer(targetIdentityID, targetNodeID, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("authorizing peer accept: %w", err)
	}
	if err := signalingprotocol.ValidateReply(init, accept); err != nil {
		return nil, err
	}
	key, err := signalingprotocol.DeriveSessionKey(ephemeral, init, accept)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	peer, err := NewPeerConnection(options.ICE)
	if err != nil {
		return nil, err
	}
	closePeer := true
	defer func() {
		if closePeer {
			_ = peer.Close()
		}
	}()
	control, data, err := CreateChannels(peer)
	if err != nil {
		return nil, err
	}
	if err := SetBoundedMessageHandler(data, MaxDataFrame, func([]byte) error { return ErrUnauthenticatedData }); err != nil {
		return nil, err
	}
	offerSDP, err := CompleteOffer(ctx, peer)
	if err != nil {
		return nil, err
	}
	now = time.Now().UTC().Truncate(time.Second)
	offer, err := signalingprotocol.NewMessage(signalingprotocol.KindOffer, sessionID, options.IdentityID, options.IdentityHead, options.NodeID, targetIdentityID, targetNodeID, now, rand.Reader)
	if err != nil {
		return nil, err
	}
	offer.ExpiresAt = init.ExpiresAt
	offer, err = signalingprotocol.SealPayload(offer, key, []byte(offerSDP.SDP), rand.Reader)
	if err != nil {
		return nil, err
	}
	offer, err = options.SignalSigner.SignPeerSignal(offer, options.SignalIdentityResolver, now)
	if err != nil {
		return nil, err
	}
	if err := options.Broker.PublishPeerSignal(ctx, offer, now); err != nil {
		return nil, err
	}
	answer, err := waitForSignal(ctx, options, func(message signalingprotocol.Message) bool {
		return message.Kind == signalingprotocol.KindAnswer && message.SessionID == sessionID && message.SourceIdentityID == targetIdentityID && message.SourceNodeID == targetNodeID &&
			message.TargetIdentityID == options.IdentityID && message.TargetNodeID == options.NodeID
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for peer answer: %w", err)
	}
	if err := options.AuthorizePeer(targetIdentityID, targetNodeID, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("authorizing peer answer: %w", err)
	}
	if err := signalingprotocol.ValidateSessionMessage(init, answer); err != nil {
		return nil, err
	}
	answerSDP, err := signalingprotocol.OpenPayload(answer, key)
	if err != nil {
		return nil, err
	}
	if err := ApplyAnswer(peer, webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answerSDP)}); err != nil {
		return nil, err
	}
	if err := WaitConnection(ctx, peer); err != nil {
		return nil, err
	}
	session, err := AuthenticateDataChannel(ctx, RoleInitiator, control, data, options.HelloSigner, options.IdentityID, options.IdentityHead, options.PeerIdentityResolver, options.PeerReplay)
	if err != nil {
		return nil, err
	}
	if session.Remote().Kind != authority.PrincipalNode || session.Remote().ID != targetNodeID {
		return nil, ErrMessageRejected
	}
	closePeer = false
	return &EstablishedPeer{PeerConnection: peer, Data: session, RemoteIdentityID: targetIdentityID}, nil
}

// AcceptIncoming waits for a valid init addressed to this Node, authorizes its
// sender, and completes the same encrypted signaling and mutual handshake.
func AcceptIncoming(ctx context.Context, options SignalingOptions) (*EstablishedPeer, error) {
	if ctx == nil || options.validate() != nil {
		return nil, ErrSignalingConfig
	}
	init, err := waitForSignal(ctx, options, func(message signalingprotocol.Message) bool {
		return message.Kind == signalingprotocol.KindInit && message.TargetIdentityID == options.IdentityID && message.TargetNodeID == options.NodeID
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for peer init: %w", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := options.AuthorizePeer(init.SourceIdentityID, init.SourceNodeID, now); err != nil {
		return nil, fmt.Errorf("authorizing peer init: %w", err)
	}
	ephemeral, err := signalingprotocol.NewEphemeralKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	accept, err := signalingprotocol.NewMessage(signalingprotocol.KindAccept, init.SessionID, options.IdentityID, options.IdentityHead, options.NodeID, init.SourceIdentityID, init.SourceNodeID, now, rand.Reader)
	if err != nil {
		return nil, err
	}
	accept.ExpiresAt = init.ExpiresAt
	accept.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(ephemeral.PublicKey().Bytes())
	accept, err = options.SignalSigner.SignPeerSignal(accept, options.SignalIdentityResolver, now)
	if err != nil {
		return nil, err
	}
	if err := signalingprotocol.ValidateReply(init, accept); err != nil {
		return nil, err
	}
	if err := options.Broker.PublishPeerSignal(ctx, accept, now); err != nil {
		return nil, err
	}
	key, err := signalingprotocol.DeriveSessionKey(ephemeral, init, accept)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	offer, err := waitForSignal(ctx, options, func(message signalingprotocol.Message) bool {
		return message.Kind == signalingprotocol.KindOffer && message.SessionID == init.SessionID && message.SourceIdentityID == init.SourceIdentityID && message.SourceNodeID == init.SourceNodeID &&
			message.TargetIdentityID == options.IdentityID && message.TargetNodeID == options.NodeID
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for peer offer: %w", err)
	}
	if err := options.AuthorizePeer(init.SourceIdentityID, init.SourceNodeID, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("authorizing peer offer: %w", err)
	}
	if err := signalingprotocol.ValidateSessionMessage(init, offer); err != nil {
		return nil, err
	}
	offerSDP, err := signalingprotocol.OpenPayload(offer, key)
	if err != nil {
		return nil, err
	}
	peer, err := NewPeerConnection(options.ICE)
	if err != nil {
		return nil, err
	}
	closePeer := true
	defer func() {
		if closePeer {
			_ = peer.Close()
		}
	}()
	incoming := make(chan *webrtc.DataChannel, 2)
	controlInbox := make(chan []byte, 4)
	peer.OnDataChannel(func(channel *webrtc.DataChannel) {
		if channel.Label() != ControlChannelLabel && channel.Label() != DataChannelLabel {
			_ = channel.Close()
			return
		}
		if channel.Label() == ControlChannelLabel {
			if err := SetBoundedMessageHandler(channel, MaxControlFrame, func(message []byte) error {
				select {
				case controlInbox <- message:
					return nil
				default:
					return ErrMessageRejected
				}
			}); err != nil {
				_ = channel.Close()
				return
			}
		} else if err := SetBoundedMessageHandler(channel, MaxDataFrame, func([]byte) error {
			return ErrUnauthenticatedData
		}); err != nil {
			_ = channel.Close()
			return
		}
		select {
		case incoming <- channel:
		default:
			_ = channel.Close()
		}
	})
	answerSDP, err := AcceptOffer(ctx, peer, webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offerSDP)})
	if err != nil {
		return nil, fmt.Errorf("building SDP answer: %w", err)
	}
	now = time.Now().UTC().Truncate(time.Second)
	answer, err := signalingprotocol.NewMessage(signalingprotocol.KindAnswer, init.SessionID, options.IdentityID, options.IdentityHead, options.NodeID, init.SourceIdentityID, init.SourceNodeID, now, rand.Reader)
	if err != nil {
		return nil, err
	}
	answer.ExpiresAt = init.ExpiresAt
	answer, err = signalingprotocol.SealPayload(answer, key, []byte(answerSDP.SDP), rand.Reader)
	if err != nil {
		return nil, err
	}
	answer, err = options.SignalSigner.SignPeerSignal(answer, options.SignalIdentityResolver, now)
	if err != nil {
		return nil, err
	}
	if err := options.Broker.PublishPeerSignal(ctx, answer, now); err != nil {
		return nil, fmt.Errorf("publishing encrypted answer: %w", err)
	}
	if err := WaitConnection(ctx, peer); err != nil {
		return nil, fmt.Errorf("waiting for ICE/DTLS connection: %w", err)
	}
	control, data, err := receiveChannels(ctx, incoming)
	if err != nil {
		return nil, fmt.Errorf("receiving negotiated data channels: %w", err)
	}
	authResult, err := AuthenticateDataChannelWithInbox(ctx, RoleResponder, control, data, controlInbox, options.HelloSigner, options.IdentityID, options.IdentityHead, options.PeerIdentityResolver, options.PeerReplay)
	if err != nil {
		return nil, fmt.Errorf("authenticating responder data channel: %w", err)
	}
	if authResult.Remote().Kind != authority.PrincipalNode || authResult.Remote().ID != init.SourceNodeID {
		return nil, ErrMessageRejected
	}
	closePeer = false
	return &EstablishedPeer{PeerConnection: peer, Data: authResult, RemoteIdentityID: init.SourceIdentityID}, nil
}

func waitForSignal(ctx context.Context, options SignalingOptions, match func(signalingprotocol.Message) bool) (signalingprotocol.Message, error) {
	interval := options.PollInterval
	if interval <= 0 || interval > 5*time.Second {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ignored := make(map[string]struct{}, 16)
	for {
		messages, err := options.Broker.LookupPeerSignals(ctx, options.NodeID)
		if err == nil {
			for _, message := range messages {
				if !match(message) {
					continue
				}
				if _, alreadyIgnored := ignored[message.MessageID]; alreadyIgnored {
					continue
				}
				now := time.Now().UTC()
				if err := options.SignalReplay.Consume(message, options.SignalIdentityResolver, now); err != nil {
					if errors.Is(err, signalingprotocol.ErrReplay) || errors.Is(err, signalingprotocol.ErrStale) || errors.Is(err, signalingprotocol.ErrDenied) || errors.Is(err, signalingprotocol.ErrInvalid) {
						if len(ignored) < 16 {
							ignored[message.MessageID] = struct{}{}
						}
						continue
					}
					return signalingprotocol.Message{}, err
				}
				return message, nil
			}
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return signalingprotocol.Message{}, errors.Join(ErrSignalTimeout, ctx.Err(), err)
			}
			return signalingprotocol.Message{}, errors.Join(ErrSignalTimeout, ctx.Err())
		case <-ticker.C:
		}
	}
}

func receiveChannels(ctx context.Context, incoming <-chan *webrtc.DataChannel) (*webrtc.DataChannel, *webrtc.DataChannel, error) {
	var control, data *webrtc.DataChannel
	for control == nil || data == nil {
		select {
		case channel := <-incoming:
			if channel == nil {
				return nil, nil, ErrChannelClosed
			}
			switch channel.Label() {
			case ControlChannelLabel:
				if control != nil {
					_ = channel.Close()
					return nil, nil, ErrMessageRejected
				}
				control = channel
			case DataChannelLabel:
				if data != nil {
					_ = channel.Close()
					return nil, nil, ErrMessageRejected
				}
				data = channel
			default:
				_ = channel.Close()
				return nil, nil, ErrMessageRejected
			}
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	return control, data, nil
}
