package peerprotocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestSignedCommandBindsPeerSessionScopeAndCurrentNodeKeys(t *testing.T) {
	fixture := newCommandFixture(t)
	command := fixture.command(t)
	signed, err := SignCommand(command, fixture.sourcePrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCommand(signed, fixture.resolve, fixture.now, fixture.sessionID,
		authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); err != nil {
		t.Fatalf("valid command was rejected: %v", err)
	}
	wire, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseCommand(wire)
	if err != nil || parsed.RequestID != signed.RequestID {
		t.Fatalf("parse signed command: id=%q err=%v", parsed.RequestID, err)
	}

	t.Run("different authenticated session", func(t *testing.T) {
		if err := VerifyCommand(signed, fixture.resolve, fixture.now, "ps_"+strings.Repeat("x", 22),
			authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); !errors.Is(err, ErrCommandDenied) {
			t.Fatalf("session substitution error=%v", err)
		}
	})
	t.Run("different authenticated node", func(t *testing.T) {
		if err := VerifyCommand(signed, fixture.resolve, fixture.now, fixture.sessionID,
			authority.Principal{Kind: authority.PrincipalNode, ID: fixture.targetNodeID}, fixture.identityID, fixture.targetNodeID); !errors.Is(err, ErrCommandDenied) {
			t.Fatalf("peer substitution error=%v", err)
		}
	})
	t.Run("target node mismatch", func(t *testing.T) {
		if err := VerifyCommand(signed, fixture.resolve, fixture.now, fixture.sessionID,
			authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.sourceNodeID); !errors.Is(err, ErrCommandDenied) {
			t.Fatalf("target substitution error=%v", err)
		}
	})
	t.Run("payload mutation", func(t *testing.T) {
		changed := signed
		changed.Payload = json.RawMessage(`{ "type" : "archive.snapshot.get" }`)
		if err := VerifyCommand(changed, fixture.resolve, fixture.now, fixture.sessionID,
			authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); !errors.Is(err, ErrCommandInvalid) {
			t.Fatalf("equivalent JSON with different signed bytes error=%v", err)
		}
	})
	t.Run("expired command", func(t *testing.T) {
		if err := VerifyCommand(signed, fixture.resolve, fixture.now.Add(MaxCommandTTL), fixture.sessionID,
			authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); !errors.Is(err, ErrCommandStale) {
			t.Fatalf("expired command error=%v", err)
		}
	})
	t.Run("revoked sender key", func(t *testing.T) {
		state := fixture.state
		state.CurrentKeys = map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + fixture.identityID: fixture.rootPublic,
			authority.PrincipalNode + ":" + fixture.targetNodeID:   fixture.targetPublic,
		}
		resolve := func(id, head string, now time.Time) (authority.IdentityState, error) {
			if id != fixture.identityID || head != "" && head != fixture.head {
				return authority.IdentityState{}, ErrStale
			}
			return state, nil
		}
		if err := VerifyCommand(signed, resolve, fixture.now, fixture.sessionID,
			authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); !errors.Is(err, ErrCommandDenied) {
			t.Fatalf("revoked sender key error=%v", err)
		}
	})
}

func TestVerifyAndAuthorizeCommandRechecksCapabilityForEveryRequest(t *testing.T) {
	fixture := newCommandFixture(t)
	command, err := SignCommand(fixture.command(t), fixture.sourcePrivate)
	if err != nil {
		t.Fatal(err)
	}
	authorizer := &commandAuthorizerProbe{}
	err = VerifyAndAuthorizeCommand(context.Background(), command, fixture.resolve, authorizer, fixture.now, fixture.sessionID,
		authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID)
	if err != nil || authorizer.calls != 1 || authorizer.subjectID != fixture.subjectID || authorizer.archiveID != fixture.archiveID || authorizer.action != "archive.view" {
		t.Fatalf("authorization result err=%v calls=%d subject=%q archive=%q action=%q", err, authorizer.calls, authorizer.subjectID, authorizer.archiveID, authorizer.action)
	}
	authorizer.err = authority.ErrDenied
	if err := VerifyAndAuthorizeCommand(context.Background(), command, fixture.resolve, authorizer, fixture.now, fixture.sessionID,
		authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("denied capability error=%v", err)
	}
	if err := VerifyAndAuthorizeCommand(context.Background(), command, fixture.resolve, authorizer, fixture.now, fixture.sessionID,
		authority.Principal{Kind: authority.PrincipalNode, ID: fixture.sourceNodeID}, fixture.identityID, fixture.targetNodeID); authorizer.calls != 3 {
		t.Fatalf("authorizer was cached across calls: calls=%d err=%v", authorizer.calls, err)
	}
}

func TestCommandParserRejectsUnknownFieldsAndOversizedPayload(t *testing.T) {
	fixture := newCommandFixture(t)
	command, err := SignCommand(fixture.command(t), fixture.sourcePrivate)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	unknown := append(append([]byte(nil), wire[:len(wire)-1]...), []byte(`,"extra":true}`)...)
	if _, err := ParseCommand(unknown); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("unknown property error=%v", err)
	}
	duplicatePayload := json.RawMessage(`{"type":"archive.snapshot.get","type":"archive.delete"}`)
	if _, err := PayloadDigest(duplicatePayload); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("duplicate payload keys accepted while preparing command: %v", err)
	}
	duplicateWire, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	duplicateWire = bytes.Replace(duplicateWire, []byte(`"payload":{"type":"archive.snapshot.get"}`), []byte(`"payload":{"type":"archive.snapshot.get","type":"archive.delete"}`), 1)
	if _, err := ParseCommand(duplicateWire); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("duplicate nested payload properties accepted: %v", err)
	}
	tooLarge := command
	tooLarge.Payload = json.RawMessage(`{"data":"` + strings.Repeat("a", MaxCommandBody+8) + `"}`)
	tooLarge.PayloadHash, _ = PayloadDigest(tooLarge.Payload)
	if _, err := SignCommand(tooLarge, fixture.sourcePrivate); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("oversized payload error=%v", err)
	}
}

type commandFixture struct {
	now           time.Time
	identityID    string
	head          string
	sourceNodeID  string
	targetNodeID  string
	sessionID     string
	subjectID     string
	archiveID     string
	rootPublic    ed25519.PublicKey
	sourcePublic  ed25519.PublicKey
	targetPublic  ed25519.PublicKey
	sourcePrivate ed25519.PrivateKey
	state         authority.IdentityState
	resolve       IdentityResolver
	grantChain    []authority.SignedGrant
}

func newCommandFixture(t *testing.T) commandFixture {
	t.Helper()
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	sourcePublic, sourcePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	targetPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sourceNodeID, err := identityprotocol.NodeID(sourcePublic)
	if err != nil {
		t.Fatal(err)
	}
	targetNodeID, err := identityprotocol.NodeID(targetPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("c", 64)
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + identityID: rootPublic,
			authority.PrincipalNode + ":" + sourceNodeID:   sourcePublic,
			authority.PrincipalNode + ":" + targetNodeID:   targetPublic,
		},
	}
	resolve := func(id, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return authority.IdentityState{}, ErrStale
		}
		return state, nil
	}
	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	const subjectID = "os_aaaaaaaaaaaaaaaaaaaaaa"
	const archiveID = "arc_bbbbbbbbbbbbbbbbbbbbbb"
	grantNonce := base64.RawURLEncoding.EncodeToString(bytesRepeat(0x44, 32))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: sourceNodeID},
		ArchiveID: archiveID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: grantNonce, PolicyEpoch: "1", IssuerKeyID: keyID,
	}, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	return commandFixture{
		now: now, identityID: identityID, head: head, sourceNodeID: sourceNodeID, targetNodeID: targetNodeID,
		sessionID: "ps_" + strings.Repeat("d", 22), subjectID: subjectID, archiveID: archiveID,
		rootPublic: rootPublic, sourcePublic: sourcePublic, targetPublic: targetPublic, sourcePrivate: sourcePrivate,
		state: state, resolve: resolve, grantChain: []authority.SignedGrant{grant},
	}
}

func (f commandFixture) command(t *testing.T) Command {
	t.Helper()
	requestID, nonce, err := NewCommand(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"type":"archive.snapshot.get"}`)
	payloadHash, err := PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	return Command{
		Version: CommandVersion, RequestID: requestID, SessionID: f.sessionID,
		SourceIdentityID: f.identityID, SourceHeadHash: f.head, SourceNodeID: f.sourceNodeID,
		TargetIdentityID: f.identityID, TargetNodeID: f.targetNodeID, OwnerSubjectID: f.subjectID,
		ArchiveID: f.archiveID, Action: "archive.view", IssuedAt: f.now.Format(time.RFC3339), ExpiresAt: f.now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: f.grantChain, Payload: payload,
	}
}

type commandAuthorizerProbe struct {
	calls     int
	subjectID string
	archiveID string
	action    string
	err       error
}

func (a *commandAuthorizerProbe) Authorize(_ context.Context, subjectID, archiveID, _ string, action string, _ authority.Principal, chain []authority.SignedGrant, _ time.Time) error {
	a.calls++
	a.subjectID, a.archiveID, a.action = subjectID, archiveID, action
	if len(chain) != 1 {
		return authority.ErrDenied
	}
	return a.err
}

func bytesRepeat(value byte, count int) []byte {
	result := make([]byte, count)
	for i := range result {
		result[i] = value
	}
	return result
}

func TestPayloadDigestBindsExactJSONAndRejectsDuplicateProperties(t *testing.T) {
	left, err := PayloadDigest(json.RawMessage(`{"type":"archive.snapshot.get","v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := PayloadDigest(json.RawMessage("{ \"v\": 1, \"type\" : \"archive.snapshot.get\" }"))
	if err != nil || left == right {
		t.Fatalf("payload hash should bind exact wire bytes: %q == %q err=%v", left, right, err)
	}
	if _, err := PayloadDigest(json.RawMessage(`{"type":"a","type":"b"}`)); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("duplicate payload property error=%v", err)
	}
}
