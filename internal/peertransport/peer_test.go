package peertransport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/pion/logging"
	"github.com/pion/turn/v5"
	"github.com/pion/webrtc/v4"
)

func TestICEConfigurationRequiresAuthorizedTURNForRelayOnly(t *testing.T) {
	if err := validateICEConfig(ICEConfig{RelayOnly: true, Servers: []webrtc.ICEServer{{URLs: []string{"stun:stun.example.test:3478"}}}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("relay-only without TURN error=%v", err)
	}
	if err := validateICEConfig(ICEConfig{Servers: []webrtc.ICEServer{{URLs: []string{"turn:turn.example.test:3478?transport=udp"}}}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("TURN without credentials error=%v", err)
	}
	if err := validateICEConfig(ICEConfig{RelayOnly: true, Servers: []webrtc.ICEServer{{
		URLs: []string{"turns:turn.example.test:5349?transport=tcp"}, Username: "short-lived-user", Credential: "short-lived-secret",
	}}}); err != nil {
		t.Fatalf("valid authorized TURN config rejected: %v", err)
	}
	if err := validateICEConfig(ICEConfig{Servers: []webrtc.ICEServer{{URLs: []string{"https://not-an-ice-server.example.test"}}}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("unrecognized ICE URL scheme error=%v", err)
	}
	if err := validateICEConfig(ICEConfig{Servers: []webrtc.ICEServer{{URLs: []string{"stun:stun.example.test:3478?transport=tcp"}}}}); !errors.Is(err, ErrConfig) {
		t.Fatalf("transport query accepted for STUN URL: %v", err)
	}
}

func TestChannelFrameBoundsFollowChannelClass(t *testing.T) {
	peer, err := NewPeerConnection(ICEConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	data, err := peer.CreateDataChannel(DataChannelLabel, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	if _, err := NewSender(data, MaxControlFrame); !errors.Is(err, ErrConfig) {
		t.Fatalf("data channel accepted control-plane bound: %v", err)
	}
	if err := SetBoundedMessageHandler(data, MaxControlFrame, func([]byte) error { return nil }); !errors.Is(err, ErrConfig) {
		t.Fatalf("data channel accepted oversized handler bound: %v", err)
	}
}

func TestLocalPeerConnectionUsesBoundedOrderedDataChannel(t *testing.T) {
	pair := newLocalPeerPair(t)
	for label, peer := range map[string]*webrtc.PeerConnection{"initiator": pair.initiator, "responder": pair.responder} {
		selected, err := peer.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
		if err != nil || selected == nil {
			t.Fatalf("%s did not establish a selected ICE path without TURN: pair=%+v err=%v", label, selected, err)
		}
		if selected.Local.Typ == webrtc.ICECandidateTypeRelay || selected.Remote.Typ == webrtc.ICECandidateTypeRelay {
			t.Fatalf("%s selected a relayed ICE path without TURN configured: pair=%+v", label, selected)
		}
		if selected.Local.Protocol != webrtc.ICEProtocolUDP || selected.Remote.Protocol != webrtc.ICEProtocolUDP {
			t.Fatalf("%s selected a non-UDP direct ICE path: pair=%+v", label, selected)
		}
	}
	control, data, peerControl, remote := pair.control, pair.data, pair.peerControl, pair.peerData
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var err error
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	localInfo, err := localNode.Create()
	if err != nil {
		t.Fatal(err)
	}
	remoteNode, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	remoteInfo, err := remoteNode.Create()
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("a", 64)
	verifiedAt := time.Now().UTC().Add(-time.Second)
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"identity:" + identityID:    rootPublic,
			"node:" + localInfo.NodeID:  decodeNodePublic(t, localInfo.PublicKey),
			"node:" + remoteInfo.NodeID: decodeNodePublic(t, remoteInfo.PublicKey),
		},
	}
	resolve := func(id, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return state, nil
	}
	type authResult struct {
		session *AuthenticatedDataChannel
		err     error
	}
	results := make(chan authResult, 2)
	go func() {
		session, err := AuthenticateDataChannel(ctx, RoleInitiator, control, data, localNode, identityID, head, resolve, nodeidentity.PeerReplayConsumer{NodeStore: localNode})
		if err == nil {
			err = session.Send(ctx, []byte("immediate post-auth frame"))
		}
		results <- authResult{session: session, err: err}
	}()
	go func() {
		session, err := AuthenticateDataChannel(ctx, RoleResponder, peerControl, remote, remoteNode, identityID, head, resolve, nodeidentity.PeerReplayConsumer{NodeStore: remoteNode})
		results <- authResult{session: session, err: err}
	}()
	var localSession, remoteSession *AuthenticatedDataChannel
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("authenticated data channel failed: %v", result.err)
		}
		if result.session == nil || result.session.Remote().Kind != authority.PrincipalNode {
			t.Fatalf("authenticated session/peer missing: %+v", result.session)
		}
		if result.session.Remote().ID == remoteInfo.NodeID {
			localSession = result.session
		} else if result.session.Remote().ID == localInfo.NodeID {
			remoteSession = result.session
		} else {
			t.Fatalf("authenticated peer principal=%+v", result.session.Remote())
		}
	}
	if localSession == nil || remoteSession == nil {
		t.Fatal("both authenticated data sessions were not established")
	}
	if localSession.SessionID() == "" || localSession.SessionID() != remoteSession.SessionID() {
		t.Fatalf("authenticated session IDs differ: local=%q remote=%q", localSession.SessionID(), remoteSession.SessionID())
	}
	defer localSession.Close()
	defer remoteSession.Close()
	immediate, err := remoteSession.Receive(ctx)
	if err != nil || string(immediate) != "immediate post-auth frame" {
		t.Fatalf("immediate post-auth frame was lost: payload=%q err=%v", immediate, err)
	}
	if err := localSession.Send(ctx, make([]byte, MaxDataFrame+1)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized data frame error=%v", err)
	}
	payload := []byte("authenticated application data is opaque to the transport")
	if err := localSession.Send(ctx, payload); err != nil {
		t.Fatalf("send data-channel frame: %v", err)
	}
	got, err := remoteSession.Receive(ctx)
	if err != nil {
		t.Fatalf("receive authenticated data frame: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("received frame=%q want=%q", got, payload)
	}

	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	const subjectID = "os_aaaaaaaaaaaaaaaaaaaaaa"
	const archiveID = "arc_bbbbbbbbbbbbbbbbbbbbbb"
	grantNonce := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x35}, 32))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localInfo.NodeID},
		ArchiveID: archiveID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: grantNonce, PolicyEpoch: "1", IssuerKeyID: keyID,
	}, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	requestID, nonce, err := peerprotocol.NewCommand(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	commandPayload := []byte(`{"archive_id":"` + archiveID + `","range":{"start":0,"end":3}}`)
	payloadHash, err := peerprotocol.PayloadDigest(commandPayload)
	if err != nil {
		t.Fatal(err)
	}
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: localSession.SessionID(),
		SourceIdentityID: identityID, SourceHeadHash: head, SourceNodeID: localInfo.NodeID,
		TargetIdentityID: identityID, TargetNodeID: remoteInfo.NodeID, OwnerSubjectID: subjectID,
		ArchiveID: archiveID, Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{grant}, Payload: commandPayload,
	}
	signedCommand, err := localNode.SignPeerCommand(command, resolve, now)
	if err != nil {
		t.Fatalf("sign peer command: %v", err)
	}
	if err := localSession.SendCommand(ctx, signedCommand); err != nil {
		t.Fatalf("send authenticated peer command: %v", err)
	}
	denyingAuthorizer := &transportCommandAuthorizer{deny: true}
	if _, err := remoteSession.ReceiveAuthorizedCommand(ctx, resolve, denyingAuthorizer,
		nodeidentity.PeerCommandReplayConsumer{NodeStore: remoteNode},
		identityID, remoteInfo.NodeID); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("command without current capability was accepted: %v", err)
	}
	if denyingAuthorizer.calls != 1 {
		t.Fatalf("denied authorization calls=%d, want 1", denyingAuthorizer.calls)
	}
	if err := localSession.SendCommand(ctx, signedCommand); err != nil {
		t.Fatalf("retry command after denied authorization: %v", err)
	}
	authorizer := &transportCommandAuthorizer{}
	accepted, err := remoteSession.ReceiveAuthorizedCommand(ctx, resolve, authorizer,
		nodeidentity.PeerCommandReplayConsumer{NodeStore: remoteNode},
		identityID, remoteInfo.NodeID)
	if err != nil || accepted.RequestID != requestID || authorizer.calls != 1 {
		t.Fatalf("receive authorized peer command: request=%q calls=%d err=%v", accepted.RequestID, authorizer.calls, err)
	}
	if err := localSession.SendCommand(ctx, signedCommand); err != nil {
		t.Fatalf("resend duplicate command: %v", err)
	}
	if _, err := remoteSession.ReceiveAuthorizedCommand(ctx, resolve, authorizer,
		nodeidentity.PeerCommandReplayConsumer{NodeStore: remoteNode},
		identityID, remoteInfo.NodeID); !errors.Is(err, peerprotocol.ErrReplay) {
		t.Fatalf("duplicate command was accepted: %v", err)
	}
}

func TestApplicationReadinessPreservesCancellationAfterLocalReady(t *testing.T) {
	pair := newLocalPeerPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inbox := make(chan []byte, 1)
	if err := SetBoundedMessageHandler(pair.control, MaxControlFrame, func(message []byte) error {
		select {
		case inbox <- message:
			return nil
		default:
			return ErrMessageRejected
		}
	}); err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(pair.control, MaxControlFrame)
	if err != nil {
		t.Fatal(err)
	}

	readyObserved := make(chan struct{})
	pair.peerControl.OnMessage(func(message webrtc.DataChannelMessage) {
		var frame handshakeFrame
		if json.Unmarshal(message.Data, &frame) == nil && frame.Type == "ready" {
			close(readyObserved)
			cancel()
		}
	})

	result := make(chan error, 1)
	go func() {
		result <- completeApplicationReadiness(ctx, pair.control, inbox, sender, "session-ready")
	}()

	select {
	case <-readyObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("remote did not observe the local readiness frame")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("readiness error=%v, want context.Canceled", err)
		}
		if errors.Is(err, ErrMessageRejected) {
			t.Fatalf("readiness cancellation was converted to ErrMessageRejected: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("readiness did not return after cancellation")
	}
}

func TestLocalPeerConnectionAuthenticatesNodesFromDifferentIdentities(t *testing.T) {
	pair := newLocalPeerPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	localNode, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	localInfo, err := localNode.Create()
	if err != nil {
		t.Fatal(err)
	}
	remoteNode, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	remoteInfo, err := remoteNode.Create()
	if err != nil {
		t.Fatal(err)
	}
	if localInfo.NodeID == remoteInfo.NodeID {
		t.Fatal("independent Node Keys unexpectedly share an ID")
	}

	type identityFixture struct {
		id    string
		head  string
		state authority.IdentityState
	}
	makeIdentity := func(node nodeidentity.Info, label string) identityFixture {
		t.Helper()
		public, _, keyErr := ed25519.GenerateKey(rand.Reader)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		id, idErr := identityprotocol.IdentityID(public)
		if idErr != nil {
			t.Fatal(idErr)
		}
		head := "sha256:" + strings.Repeat(label, 64)
		verifiedAt := time.Now().UTC().Add(-time.Second)
		return identityFixture{id: id, head: head, state: authority.IdentityState{
			IdentityID: id, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
			CurrentKeys: map[string]ed25519.PublicKey{
				"identity:" + id:      public,
				"node:" + node.NodeID: decodeNodePublic(t, node.PublicKey),
			},
		}}
	}
	localIdentity := makeIdentity(localInfo, "a")
	remoteIdentity := makeIdentity(remoteInfo, "b")
	if localIdentity.id == remoteIdentity.id {
		t.Fatal("independent identity roots unexpectedly share an ID")
	}
	states := map[string]identityFixture{
		localIdentity.id: localIdentity, remoteIdentity.id: remoteIdentity,
	}
	resolve := func(id, head string, _ time.Time) (authority.IdentityState, error) {
		fixture, ok := states[id]
		if !ok || head != "" && head != fixture.head {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return fixture.state, nil
	}
	type result struct {
		session *AuthenticatedDataChannel
		err     error
	}
	results := make(chan result, 2)
	go func() {
		session, authErr := AuthenticateDataChannel(ctx, RoleInitiator, pair.control, pair.data,
			localNode, localIdentity.id, localIdentity.head, resolve, nodeidentity.PeerReplayConsumer{NodeStore: localNode})
		results <- result{session: session, err: authErr}
	}()
	go func() {
		session, authErr := AuthenticateDataChannel(ctx, RoleResponder, pair.peerControl, pair.peerData,
			remoteNode, remoteIdentity.id, remoteIdentity.head, resolve, nodeidentity.PeerReplayConsumer{NodeStore: remoteNode})
		results <- result{session: session, err: authErr}
	}()

	var sessions []*AuthenticatedDataChannel
	for index := 0; index < 2; index++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("cross-identity Node-Key handshake failed: %v", got.err)
			}
			sessions = append(sessions, got.session)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for _, session := range sessions {
		if session == nil || session.SessionID() == "" || session.Remote().Kind != authority.PrincipalNode {
			t.Fatalf("cross-identity session was not mutually authenticated: %+v", session)
		}
	}
	if sessions[0].SessionID() != sessions[1].SessionID() {
		t.Fatalf("cross-identity peers disagree on session ID: %q != %q", sessions[0].SessionID(), sessions[1].SessionID())
	}
	if err := sessions[0].Send(ctx, []byte("cross-identity authenticated payload")); err != nil {
		t.Fatalf("send authenticated cross-identity payload: %v", err)
	}
	received, err := sessions[1].Receive(ctx)
	if err != nil || string(received) != "cross-identity authenticated payload" {
		t.Fatalf("received payload=%q err=%v", received, err)
	}
}

func TestRelayOnlyPeerConnectionUsesAuthorizedTURN(t *testing.T) {
	const sharedSecret = "isolated-test-turn-secret-at-least-32-bytes"
	listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	turnServer, err := turn.NewServer(turn.ServerConfig{
		AuthHandler: turn.LongTermTURNRESTAuthHandler(sharedSecret, nil),
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: listener,
			RelayAddressGenerator: &turn.RelayAddressGeneratorStatic{
				RelayAddress: net.ParseIP("127.0.0.1"),
				Address:      "127.0.0.1",
			},
		}},
		Realm:         "pion.ly",
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		_ = listener.Close()
		t.Fatalf("start isolated TURN server: %v", err)
	}
	defer func() {
		if closeErr := turnServer.Close(); closeErr != nil {
			t.Errorf("close isolated TURN server: %v", closeErr)
		}
	}()

	turnURL := "turn:" + listener.LocalAddr().String() + "?transport=udp"
	auth, err := NewTURNAuthConfig([]string{turnURL}, nil, []byte(sharedSecret), time.Minute, true)
	if err != nil {
		t.Fatalf("configure relay-only TURN: %v", err)
	}
	initiatorICE, err := auth.ICE("nd_aaaaaaaaaaaaaaaaaaaaaa", time.Now().UTC())
	if err != nil {
		t.Fatalf("derive initiator credentials: %v", err)
	}
	responderICE, err := auth.ICE("nd_bbbbbbbbbbbbbbbbbbbbbb", time.Now().UTC())
	if err != nil {
		t.Fatalf("derive responder credentials: %v", err)
	}
	pair := newPeerPair(t, initiatorICE, responderICE)
	for label, peer := range map[string]*webrtc.PeerConnection{"initiator": pair.initiator, "responder": pair.responder} {
		description := peer.LocalDescription()
		if description == nil || !strings.Contains(description.SDP, " typ relay ") {
			t.Fatalf("%s did not gather a TURN relay candidate", label)
		}
		for _, line := range strings.Split(description.SDP, "\n") {
			if strings.Contains(line, "a=candidate:") && !strings.Contains(line, " typ relay ") {
				t.Fatalf("%s gathered a non-relay candidate under relay-only policy: %q", label, line)
			}
		}
	}

	received := make(chan string, 1)
	pair.peerData.OnMessage(func(message webrtc.DataChannelMessage) { received <- string(message.Data) })
	if err := pair.data.Send([]byte("relay-only peer frame")); err != nil {
		t.Fatalf("send over TURN-relayed data channel: %v", err)
	}
	select {
	case value := <-received:
		if value != "relay-only peer frame" {
			t.Fatalf("relayed frame=%q", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TURN-relayed data channel did not deliver the frame")
	}
}

type transportCommandAuthorizer struct {
	calls int
	deny  bool
}

func (a *transportCommandAuthorizer) Authorize(_ context.Context, _, _, _, action string, principal authority.Principal, chain []authority.SignedGrant, _ time.Time) error {
	a.calls++
	if principal.Kind != authority.PrincipalNode || action != "archive.view" || len(chain) != 1 {
		return authority.ErrDenied
	}
	if a.deny {
		return authority.ErrDenied
	}
	return nil
}

func TestDataChannelRejectsFrameBeforeAuthentication(t *testing.T) {
	pair := newLocalPeerPair(t)
	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	node, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Create(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	dataClosed := make(chan struct{}, 1)
	pair.data.OnClose(func() { dataClosed <- struct{}{} })
	helloSeen := make(chan struct{}, 1)
	pair.peerControl.OnMessage(func(webrtc.DataChannelMessage) {
		select {
		case helloSeen <- struct{}{}:
		default:
		}
	})
	go func() {
		_, err := AuthenticateDataChannel(ctx, RoleInitiator, pair.control, pair.data, node, identityID, "sha256:"+strings.Repeat("b", 64), func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}, nodeidentity.PeerReplayConsumer{NodeStore: node})
		result <- err
	}()
	// Seeing the initiator's hello proves AuthenticateDataChannel installed
	// the data-channel rejection handler before starting the control handshake.
	select {
	case <-helloSeen:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("authentication hello was not sent")
	}
	if err := pair.peerData.Send([]byte("unauthenticated command")); err != nil {
		cancel()
		t.Fatalf("send pre-authentication data frame: %v", err)
	}
	select {
	case <-dataClosed:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("pre-authentication frame did not close the data channel")
	}
	cancel()
	if err := <-result; err == nil {
		t.Fatal("handshake succeeded after the data channel was closed")
	}
}

type connectedTestPair struct {
	initiator, responder  *webrtc.PeerConnection
	control, data         *webrtc.DataChannel
	peerControl, peerData *webrtc.DataChannel
}

func newLocalPeerPair(t *testing.T) connectedTestPair {
	t.Helper()
	return newPeerPair(t, ICEConfig{}, ICEConfig{})
}

func newPeerPair(t *testing.T, initiatorICE, responderICE ICEConfig) connectedTestPair {
	t.Helper()
	initiator, err := NewPeerConnection(initiatorICE)
	if err != nil {
		t.Fatal(err)
	}
	responder, err := NewPeerConnection(responderICE)
	if err != nil {
		_ = initiator.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = initiator.Close(); _ = responder.Close() })
	control, data, err := CreateChannels(initiator)
	if err != nil {
		t.Fatal(err)
	}
	if control.Label() != ControlChannelLabel || data.Label() != DataChannelLabel || !control.Ordered() || !data.Ordered() {
		t.Fatalf("unexpected channel contract: control=%q/%v data=%q/%v", control.Label(), control.Ordered(), data.Label(), data.Ordered())
	}
	peerData := make(chan *webrtc.DataChannel, 1)
	peerControl := make(chan *webrtc.DataChannel, 1)
	responder.OnDataChannel(func(channel *webrtc.DataChannel) {
		switch channel.Label() {
		case ControlChannelLabel:
			peerControl <- channel
		case DataChannelLabel:
			peerData <- channel
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	offer, err := CompleteOffer(ctx, initiator)
	if err != nil {
		t.Fatalf("create gathered offer: %v", err)
	}
	answer, err := AcceptOffer(ctx, responder, offer)
	if err != nil {
		t.Fatalf("accept offer: %v", err)
	}
	if err := ApplyAnswer(initiator, answer); err != nil {
		t.Fatalf("apply answer: %v", err)
	}
	if err := WaitConnection(ctx, initiator); err != nil {
		t.Fatalf("initiator connection: %v", err)
	}
	if err := WaitConnection(ctx, responder); err != nil {
		t.Fatalf("responder connection: %v", err)
	}
	if err := WaitOpen(ctx, data); err != nil {
		t.Fatalf("local data channel: %v", err)
	}
	var remoteData, remoteControl *webrtc.DataChannel
	select {
	case remoteData = <-peerData:
	case <-ctx.Done():
		t.Fatal("remote data channel was not created")
	}
	if err := WaitOpen(ctx, remoteData); err != nil {
		t.Fatalf("remote data channel: %v", err)
	}
	select {
	case remoteControl = <-peerControl:
	case <-ctx.Done():
		t.Fatal("remote control channel was not created")
	}
	return connectedTestPair{initiator: initiator, responder: responder, control: control, data: data, peerControl: remoteControl, peerData: remoteData}
}

func decodeNodePublic(t *testing.T, encoded string) ed25519.PublicKey {
	t.Helper()
	public, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(public) != ed25519.PublicKeySize {
		t.Fatalf("decode Node Key public value: bytes=%d err=%v", len(public), err)
	}
	return ed25519.PublicKey(public)
}

func TestRemoteDescriptionSizeIsBounded(t *testing.T) {
	peer, err := NewPeerConnection(ICEConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if err := ApplyAnswer(peer, webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(make([]byte, MaxRemoteSDPBytes+1))}); !errors.Is(err, ErrSDP) {
		t.Fatalf("oversized remote SDP error=%v", err)
	}
}
