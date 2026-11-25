package peertransport

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
	"github.com/pion/turn/v5"
	"github.com/pion/webrtc/v4"
)

func TestEncryptedSignalingEstablishesMutuallyAuthenticatedPeer(t *testing.T) {
	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("c", 64)
	initiatorStore, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	initiatorNode, err := initiatorStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	responderStore, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	responderNode, err := responderStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	verifiedAt := time.Now().UTC().Add(-time.Second)
	publicKeys := map[string]ed25519.PublicKey{
		"node:" + initiatorNode.NodeID: decodeNodePublic(t, initiatorNode.PublicKey),
		"node:" + responderNode.NodeID: decodeNodePublic(t, responderNode.PublicKey),
	}
	resolveSignal := func(id, requestedHead string, _ time.Time) (signalingprotocol.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return signalingprotocol.IdentityState{}, signalingprotocol.ErrStale
		}
		return signalingprotocol.IdentityState{IdentityID: identityID, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(10 * time.Minute), CurrentKeys: publicKeys}, nil
	}
	resolvePeer := func(id, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return authority.IdentityState{IdentityID: identityID, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(10 * time.Minute), CurrentKeys: publicKeys}, nil
	}
	firstDirectory, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "first-discovery"))
	if err != nil {
		t.Fatal(err)
	}
	secondDirectory, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "second-discovery"))
	if err != nil {
		t.Fatal(err)
	}
	roots, caCertificate, caPrivate := newTestDiscoveryCA(t)
	_, firstTarget := startTestDiscoveryTLSServer(t, "one.example", firstDirectory, caCertificate, caPrivate)
	_, secondTarget := startTestDiscoveryTLSServer(t, "two.example", secondDirectory, caCertificate, caPrivate)
	dialer := &net.Dialer{Timeout: time.Second}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			target := map[string]string{"one.example": firstTarget, "two.example": secondTarget}[host]
			if target == "" {
				return nil, errors.New("test discovery target is unavailable")
			}
			return dialer.DialContext(ctx, network, target)
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	broker, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	forgedSession, err := signalingprotocol.NewSessionID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := signalingprotocol.NewMessage(signalingprotocol.KindInit, forgedSession, identityID, head, initiatorNode.NodeID, identityID, responderNode.NodeID, time.Now().UTC(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	forgedEphemeral, err := signalingprotocol.NewEphemeralKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	forged.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(forgedEphemeral.PublicKey().Bytes())
	forged.MessageID = "pm_" + strings.Repeat("-", 22)
	issuedAt := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	forged.IssuedAt = issuedAt.Format(time.RFC3339)
	forged.ExpiresAt = issuedAt.Add(signalingprotocol.MaxMessageTTL).Format(time.RFC3339)
	forged.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	if _, err := firstDirectory.PublishSignal(forged, time.Now().UTC()); err != nil {
		t.Fatalf("publish test-only forged envelope: %v", err)
	}
	turnAuth := startTestTURN(t)
	initiatorICE, err := turnAuth.ICE(initiatorNode.NodeID, time.Now().UTC())
	if err != nil {
		t.Fatal("derive initiator TURN credential")
	}
	responderICE, err := turnAuth.ICE(responderNode.NodeID, time.Now().UTC())
	if err != nil {
		t.Fatal("derive responder TURN credential")
	}
	authorize := func(id, nodeID string, _ time.Time) error {
		if id != identityID || len(publicKeys["node:"+nodeID]) != ed25519.PublicKeySize {
			return authority.ErrDenied
		}
		return nil
	}
	initiator := SignalingOptions{
		Broker: broker, SignalSigner: initiatorStore,
		SignalReplay:           nodeidentity.PeerSignalReplayConsumer{NodeStore: initiatorStore},
		SignalIdentityResolver: resolveSignal, HelloSigner: initiatorStore,
		PeerIdentityResolver: resolvePeer, PeerReplay: nodeidentity.PeerReplayConsumer{NodeStore: initiatorStore},
		IdentityID: identityID, IdentityHead: head, NodeID: initiatorNode.NodeID,
		PollInterval: 10 * time.Millisecond, AuthorizePeer: authorize, ICE: initiatorICE,
	}
	responder := SignalingOptions{
		Broker: broker, SignalSigner: responderStore,
		SignalReplay:           nodeidentity.PeerSignalReplayConsumer{NodeStore: responderStore},
		SignalIdentityResolver: resolveSignal, HelloSigner: responderStore,
		PeerIdentityResolver: resolvePeer, PeerReplay: nodeidentity.PeerReplayConsumer{NodeStore: responderStore},
		IdentityID: identityID, IdentityHead: head, NodeID: responderNode.NodeID,
		PollInterval: 10 * time.Millisecond, AuthorizePeer: authorize, ICE: responderICE,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type peerResult struct {
		peer *EstablishedPeer
		err  error
	}
	responderResult := make(chan peerResult, 1)
	go func() {
		peer, err := AcceptIncoming(ctx, responder)
		responderResult <- peerResult{peer: peer, err: err}
	}()
	initiatorPeer, err := EstablishOutgoing(ctx, initiator, identityID, responderNode.NodeID)
	if err != nil {
		var responderErr error
		select {
		case result := <-responderResult:
			responderErr = result.err
		case <-time.After(time.Second):
		}
		t.Fatalf("outgoing peer establishment failed: %v; responder=%v", err, responderErr)
	}
	defer initiatorPeer.Close()
	var responderPeer *EstablishedPeer
	select {
	case result := <-responderResult:
		if result.err != nil {
			t.Fatalf("incoming peer establishment failed: %v", result.err)
		}
		responderPeer = result.peer
	case <-ctx.Done():
		t.Fatalf("responder timed out: %v", ctx.Err())
	}
	defer responderPeer.Close()
	if initiatorPeer.Data.Remote().ID != responderNode.NodeID || responderPeer.Data.Remote().ID != initiatorNode.NodeID {
		t.Fatalf("peer identities did not match the signed route: initiator=%+v responder=%+v", initiatorPeer.Data.Remote(), responderPeer.Data.Remote())
	}
	for _, connection := range []*EstablishedPeer{initiatorPeer, responderPeer} {
		pair, err := connection.PeerConnection.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
		if err != nil || pair == nil || pair.Local.Typ != webrtc.ICECandidateTypeRelay || pair.Remote.Typ != webrtc.ICECandidateTypeRelay {
			t.Fatalf("relay-only peer selected non-TURN candidate pair: pair=%+v err=%v", pair, err)
		}
	}
	frame := []byte("authenticated opaque command")
	if err := initiatorPeer.Data.Send(ctx, frame); err != nil {
		t.Fatalf("send authenticated command: %v", err)
	}
	got, err := responderPeer.Data.Receive(ctx)
	if err != nil || string(got) != string(frame) {
		t.Fatalf("receive authenticated command=%q err=%v", got, err)
	}

	for _, directory := range []*discovery.Directory{firstDirectory, secondDirectory} {
		messages, err := directory.LookupSignals(initiatorNode.NodeID, time.Now().UTC())
		if err != nil {
			t.Fatalf("lookup initiator relay messages: %v", err)
		}
		messages2, err := directory.LookupSignals(responderNode.NodeID, time.Now().UTC())
		if err != nil {
			t.Fatalf("lookup responder relay messages: %v", err)
		}
		messages = append(messages, messages2...)
		for _, message := range messages {
			canonical, err := signalingprotocol.CanonicalMessage(message)
			if err != nil {
				t.Fatalf("broker stored invalid envelope: %v", err)
			}
			if strings.Contains(string(canonical), "candidate:") || strings.Contains(string(canonical), "v=0\\r\\n") {
				t.Fatalf("SDP/ICE appeared in relay envelope: %s", canonical)
			}
		}
	}
}

func startTestTURN(t *testing.T) *TURNAuthConfig {
	t.Helper()
	const realm = "integrated-recorder.test"
	secret := []byte("loopback-test-coturn-shared-secret-32-bytes")
	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		AuthHandler: func(attributes *turn.RequestAttributes) (string, []byte, bool) {
			username := attributes.Username
			parts := strings.SplitN(username, ":", 2)
			if len(parts) != 2 || attributes.Realm != realm {
				return "", nil, false
			}
			expires, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil || time.Now().Unix() >= expires {
				return "", nil, false
			}
			mac := hmac.New(sha1.New, secret)
			_, _ = mac.Write([]byte(username))
			password := base64.StdEncoding.EncodeToString(mac.Sum(nil))
			return username, turn.GenerateAuthKey(username, realm, password), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn:            packetConn,
			RelayAddressGenerator: &turn.RelayAddressGeneratorNone{Address: "127.0.0.1"},
		}},
	})
	if err != nil {
		_ = packetConn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	config, err := NewTURNAuthConfig(
		[]string{"turn:" + packetConn.LocalAddr().String() + "?transport=udp"}, nil,
		secret, 4*time.Minute, true,
	)
	if err != nil {
		t.Fatal("initialize test TURN auth config")
	}
	return config
}

func newTestDiscoveryCA(t *testing.T) (*x509.CertPool, *x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Integrated Recorder test Discovery CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return roots, certificate, private
}

func startTestDiscoveryTLSServer(t *testing.T, hostname string, directory *discovery.Directory, ca *x509.Certificate, caPrivate ed25519.PrivateKey) (*httptest.Server, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(int64(len(hostname) + 2)), Subject: pkix.Name{CommonName: hostname}, DNSNames: []string{hostname},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(discovery.HTTPHandler(directory))
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der, ca.Raw}, PrivateKey: private}},
		MinVersion:   tls.VersionTLS12,
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, server.Listener.Addr().String()
}
