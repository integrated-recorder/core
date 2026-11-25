package signalingprotocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestSignedEncryptedSignalRoundTripAndCurrentDelegation(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	initiatorPublic, initiatorPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	responderPublic, responderPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	initiatorID, _ := identityprotocol.NodeID(initiatorPublic)
	responderID, _ := identityprotocol.NodeID(responderPublic)
	head := "sha256:" + strings.Repeat("a", 64)
	state := IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"identity:" + identityID: append(ed25519.PublicKey(nil), rootPublic...),
			"node:" + initiatorID:    append(ed25519.PublicKey(nil), initiatorPublic...),
			"node:" + responderID:    append(ed25519.PublicKey(nil), responderPublic...),
		},
	}
	resolve := func(id, requestedHead string, _ time.Time) (IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return IdentityState{}, ErrStale
		}
		return state, nil
	}

	sessionID, err := NewSessionID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	initiatorEphemeral, err := NewEphemeralKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	responderEphemeral, err := NewEphemeralKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	init, err := NewMessage(KindInit, sessionID, identityID, head, initiatorID, identityID, responderID, now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	init.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(initiatorEphemeral.PublicKey().Bytes())
	init, err = Sign(init, initiatorPrivate)
	if err != nil {
		t.Fatal(err)
	}
	accept, err := NewMessage(KindAccept, sessionID, identityID, head, responderID, identityID, initiatorID, now.Add(time.Second), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	accept.ExpiresAt = init.ExpiresAt
	accept.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(responderEphemeral.PublicKey().Bytes())
	accept, err = Sign(accept, responderPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(init, resolve, now.Add(2*time.Second)); err != nil {
		t.Fatalf("init did not verify: %v", err)
	}
	if err := Verify(accept, resolve, now.Add(2*time.Second)); err != nil {
		t.Fatalf("accept did not verify: %v", err)
	}
	if err := ValidateReply(init, accept); err != nil {
		t.Fatalf("accept not bound to init: %v", err)
	}
	initiatorKey, err := DeriveSessionKey(initiatorEphemeral, init, accept)
	if err != nil {
		t.Fatal(err)
	}
	responderKey, err := DeriveSessionKey(responderEphemeral, init, accept)
	if err != nil {
		t.Fatal(err)
	}
	if string(initiatorKey) != string(responderKey) {
		t.Fatal("peers derived different session keys")
	}

	plainSDP := []byte("v=0\r\no=- example ICE candidate 192.0.2.5\r\n")
	offer, err := NewMessage(KindOffer, sessionID, identityID, head, initiatorID, identityID, responderID, now.Add(2*time.Second), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	offer.ExpiresAt = init.ExpiresAt
	offer, err = SealPayload(offer, initiatorKey, plainSDP, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	offer, err = Sign(offer, initiatorPrivate)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := CanonicalMessage(offer)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), string(plainSDP)) || strings.Contains(string(encoded), "192.0.2.5") {
		t.Fatal("signaling provider can read plaintext SDP/candidate")
	}
	parsed, err := ParseCanonicalMessage(encoded)
	if err != nil || parsed.MessageID != offer.MessageID {
		t.Fatalf("canonical signal round trip=%+v err=%v", parsed, err)
	}
	if err := Verify(parsed, resolve, now.Add(3*time.Second)); err != nil {
		t.Fatalf("offer did not verify: %v", err)
	}
	if err := ValidateSessionMessage(init, parsed); err != nil {
		t.Fatalf("offer not bound to init: %v", err)
	}
	opened, err := OpenPayload(parsed, responderKey)
	if err != nil || string(opened) != string(plainSDP) {
		t.Fatalf("opened SDP=%q err=%v", opened, err)
	}
	answer, err := NewMessage(KindAnswer, sessionID, identityID, head, responderID, identityID, initiatorID, now.Add(3*time.Second), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	answer.ExpiresAt = init.ExpiresAt
	answer, err = SealPayload(answer, responderKey, []byte("v=0\\r\\no=- answer"), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	answer, err = Sign(answer, responderPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSessionMessage(init, answer); err != nil {
		t.Fatalf("answer not bound to init: %v", err)
	}
	if answerPlain, err := OpenPayload(answer, initiatorKey); err != nil || string(answerPlain) != "v=0\\r\\no=- answer" {
		t.Fatalf("opened answer=%q err=%v", answerPlain, err)
	}

	wrongRoute := parsed
	wrongRoute.TargetNodeID = "nd_" + strings.Repeat("B", 22)
	if _, err := OpenPayload(wrongRoute, responderKey); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("route substitution passed AEAD: %v", err)
	}
	if err := Verify(wrongRoute, resolve, now.Add(3*time.Second)); !errors.Is(err, ErrDenied) {
		t.Fatalf("route substitution passed signature: %v", err)
	}
	if err := Verify(parsed, resolve, now.Add(MaxMessageTTL+5*time.Second)); !errors.Is(err, ErrStale) {
		t.Fatalf("expired signal accepted: %v", err)
	}
	state.CurrentKeys = map[string]ed25519.PublicKey{"node:" + responderID: responderPublic}
	if err := Verify(parsed, resolve, now.Add(3*time.Second)); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked sender Node Key accepted: %v", err)
	}
}

func TestMessageBoundsAndCanonicalParsingFailClosed(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, _ := identityprotocol.NodeID(public)
	rootPublic, _, _ := ed25519.GenerateKey(rand.Reader)
	identityID, _ := identityprotocol.IdentityID(rootPublic)
	sessionID, _ := NewSessionID(rand.Reader)
	init, err := NewMessage(KindInit, sessionID, identityID, "sha256:"+strings.Repeat("b", 64), nodeID, identityID, "nd_"+strings.Repeat("A", 22), now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	init.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	if _, err := Sign(init, private); err != nil {
		t.Fatalf("valid 32-byte ephemeral field rejected: %v", err)
	}
	init.EphemeralPublicKey = "not-base64"
	if _, err := Sign(init, private); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed ephemeral key accepted: %v", err)
	}
	if _, err := NewMessage(KindInit, sessionID, identityID, "sha256:bad", nodeID, identityID, "nd_"+strings.Repeat("A", 22), now, rand.Reader); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed head hash accepted: %v", err)
	}

	cancel, err := NewMessage(KindCancel, sessionID, identityID, "sha256:"+strings.Repeat("b", 64), nodeID, identityID, "nd_"+strings.Repeat("A", 22), now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cancel, err = Sign(cancel, private)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := CanonicalMessage(cancel)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCanonicalMessage(append(canonical, ' ')); !errors.Is(err, ErrInvalid) {
		t.Fatalf("trailing noncanonical data accepted: %v", err)
	}
	withUnknown := strings.TrimSuffix(string(canonical), "}") + `,"unknown":true}`
	if _, err := ParseCanonicalMessage([]byte(withUnknown)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown field accepted: %v", err)
	}
}
