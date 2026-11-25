package peerprotocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestMutualTranscriptBindsCurrentDelegatedNodesAndFreshChallenges(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	initiatorPublic, initiatorKey, _ := ed25519.GenerateKey(rand.Reader)
	responderPublic, responderKey, _ := ed25519.GenerateKey(rand.Reader)
	initiatorID, _ := identityprotocol.NodeID(initiatorPublic)
	responderID, _ := identityprotocol.NodeID(responderPublic)
	head := "sha256:" + repeatHex('a', 64)
	state := authority.IdentityState{
		IdentityID: identity, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"identity:" + identity: append(ed25519.PublicKey(nil), rootPublic...),
			"node:" + initiatorID:  append(ed25519.PublicKey(nil), initiatorPublic...),
			"node:" + responderID:  append(ed25519.PublicKey(nil), responderPublic...),
		},
	}
	resolve := func(identityIDValue, headHash string, _ time.Time) (authority.IdentityState, error) {
		if identityIDValue != identity || (headHash != "" && headHash != head) {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}

	initiator, err := NewHello(identity, head, initiatorID, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	initiator, err = SignHello(initiator, initiatorKey)
	if err != nil {
		t.Fatal(err)
	}
	responder, err := NewHello(identity, head, responderID, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	responder.SessionID = initiator.SessionID
	responder, err = SignHello(responder, responderKey)
	if err != nil {
		t.Fatal(err)
	}
	transcript := Transcript{Version: HandshakeVersion, Initiator: initiator, Responder: responder}
	transcript.InitiatorProof, err = SignTranscriptProof(transcript, initiatorID, initiatorKey)
	if err != nil {
		t.Fatal(err)
	}
	transcript.ResponderProof, err = SignTranscriptProof(transcript, responderID, responderKey)
	if err != nil {
		t.Fatal(err)
	}
	gotInitiator, gotResponder, err := VerifyTranscript(transcript, resolve, now)
	if err != nil || gotInitiator.ID != initiatorID || gotResponder.ID != responderID {
		t.Fatalf("verified peers=(%+v,%+v), err=%v", gotInitiator, gotResponder, err)
	}

	changed := transcript
	changed.Responder.Nonce = initiator.Nonce
	if _, _, err := VerifyTranscript(changed, resolve, now); !errors.Is(err, ErrDenied) {
		t.Fatalf("nonce substitution accepted: %v", err)
	}
	changed = transcript
	changed.InitiatorProof = transcript.ResponderProof
	if _, _, err := VerifyTranscript(changed, resolve, now); !errors.Is(err, ErrDenied) {
		t.Fatalf("cross-role proof replay accepted: %v", err)
	}
	if _, _, err := VerifyTranscript(transcript, func(id, _ string, at time.Time) (authority.IdentityState, error) {
		stale := state
		stale.HeadHash = "sha256:" + repeatHex('b', 64)
		return stale, nil
	}, now); !errors.Is(err, ErrStale) {
		t.Fatalf("old identity head accepted: %v", err)
	}
}

func TestHelloRejectsExpiredMalformedAndRevokedNode(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	rootPublic, _, _ := ed25519.GenerateKey(rand.Reader)
	identity, _ := identityprotocol.IdentityID(rootPublic)
	public, key, _ := ed25519.GenerateKey(rand.Reader)
	node, _ := identityprotocol.NodeID(public)
	head := "sha256:" + repeatHex('c', 64)
	state := authority.IdentityState{
		IdentityID: identity, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{"node:" + node: public},
	}
	resolve := func(_, _ string, _ time.Time) (authority.IdentityState, error) { return state, nil }
	hello, err := NewHello(identity, head, node, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	hello, err = SignHello(hello, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyHello(hello, resolve, now.Add(MaxHandshakeTTL)); !errors.Is(err, ErrStale) {
		t.Fatalf("expired hello accepted: %v", err)
	}
	malformed := hello
	malformed.ExpiresAt = "2026-11-03T12:02:00+00:00"
	if _, _, err := VerifyHello(malformed, resolve, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("noncanonical time accepted: %v", err)
	}
	state.CurrentKeys = map[string]ed25519.PublicKey{}
	if _, _, err := VerifyHello(hello, resolve, now); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked node accepted: %v", err)
	}
}

func TestReplayCacheAllowsExactlyOneConcurrentConsumer(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	rootPublic, _, _ := ed25519.GenerateKey(rand.Reader)
	identity, _ := identityprotocol.IdentityID(rootPublic)
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	node, _ := identityprotocol.NodeID(public)
	head := "sha256:" + repeatHex('d', 64)
	hello, err := NewHello(identity, head, node, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	hello, err = SignHello(hello, private)
	if err != nil {
		t.Fatal(err)
	}
	state := authority.IdentityState{
		IdentityID: identity, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{"node:" + node: public},
	}
	resolve := func(id, headHash string, _ time.Time) (authority.IdentityState, error) {
		if id != identity || headHash != "" && headHash != head {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}
	cache, err := NewReplayCache(16)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- cache.Consume(hello, resolve, now)
		}()
	}
	wg.Wait()
	close(results)
	var success, replay int
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrReplay):
			replay++
		default:
			t.Fatalf("unexpected replay result: %v", err)
		}
	}
	if success != 1 || replay != 1 {
		t.Fatalf("successful consumers=%d replay rejections=%d", success, replay)
	}
	if err := cache.Consume(hello, resolve, now.Add(3*time.Minute)); !errors.Is(err, ErrStale) {
		t.Fatalf("expired challenge should not be consumed again: %v", err)
	}
}

func repeatHex(char byte, count int) string {
	result := make([]byte, count)
	for i := range result {
		result[i] = char
	}
	return string(result)
}

func ExampleTranscriptDigest() {
	_, _ = fmt.Println(HandshakeVersion)
	// Output: ir.peer-handshake/1
}
