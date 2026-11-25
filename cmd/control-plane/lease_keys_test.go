package main

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

type leaseKeyHistoryFixture struct {
	snapshot   globalidentity.Snapshot
	bundle     globalidentity.HistoryBundle
	projection globalidentity.AuthorityProjection
}

func (f leaseKeyHistoryFixture) Snapshot() (globalidentity.Snapshot, bool, error) {
	return f.snapshot, true, nil
}
func (f leaseKeyHistoryFixture) ExportHistory() (globalidentity.HistoryBundle, error) {
	return f.bundle, nil
}
func (f leaseKeyHistoryFixture) AuthorityProjectionAt(head string) (globalidentity.AuthorityProjection, error) {
	if head != f.projection.HeadHash {
		return globalidentity.AuthorityProjection{}, globalidentity.ErrHistoryInvalid
	}
	return f.projection, nil
}

func TestLocalLeaseKeyResolverUsesVerifiedHistoricalIdentityKey(t *testing.T) {
	fixture, private := newLeaseKeyHistoryFixture(t, time.Now().UTC().Add(-time.Minute))
	resolver := localLeaseKeyResolver{identity: fixture}
	key, err := resolver.VerifiedKey(context.Background(), authority.Principal{Kind: authority.PrincipalIdentity, ID: fixture.snapshot.IdentityID}, fixture.projection.VerifiedAt)
	if err != nil || !key.Equal(private.Public().(ed25519.PublicKey)) {
		t.Fatalf("historical identity key = %x, err = %v", key, err)
	}
	if _, err := resolver.VerifiedKey(context.Background(), authority.Principal{Kind: authority.PrincipalNode, ID: "nd_AAAAAAAAAAAAAAAAAAAAAA"}, fixture.projection.VerifiedAt); !errors.Is(err, errLeaseIdentityKeyUnavailable) {
		t.Fatalf("undelegated node key error = %v", err)
	}
}

func TestLocalLeaseKeyResolverRejectsAmbiguousAndUntrustedTime(t *testing.T) {
	issued := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	fixture, _ := newLeaseKeyHistoryFixture(t, issued)
	resolver := localLeaseKeyResolver{identity: fixture}
	principal := authority.Principal{Kind: authority.PrincipalIdentity, ID: fixture.snapshot.IdentityID}
	if _, err := resolver.VerifiedKey(context.Background(), principal, issued); !errors.Is(err, errLeaseIdentityKeyUnavailable) {
		t.Fatalf("equal-timestamp signature boundary accepted: %v", err)
	}
	if _, err := resolver.VerifiedKey(context.Background(), authority.Principal{Kind: "administrator", ID: "usr_123"}, issued.Add(time.Second)); !errors.Is(err, errLeaseIdentityKeyUnavailable) {
		t.Fatalf("unknown principal kind accepted: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.VerifiedKey(canceled, principal, issued.Add(time.Second)); !errors.Is(err, errLeaseIdentityKeyUnavailable) {
		t.Fatalf("canceled lookup error = %v", err)
	}
}

func TestLeaseKeyResolverFindsForeignNodeInVerifiedRegistryHistory(t *testing.T) {
	issued := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	local, _ := newLeaseKeyHistoryFixture(t, issued.Add(-time.Minute))
	registry, foreignIdentityID, foreignNodeID, foreignNodePublic, foreignIdentityPublic := newRegisteredLeaseNodeIdentity(t, issued)
	resolver := localLeaseKeyResolver{identity: local, registry: registry}

	for principal, expected := range map[authority.Principal]ed25519.PublicKey{
		{Kind: authority.PrincipalNode, ID: foreignNodeID}:         foreignNodePublic,
		{Kind: authority.PrincipalIdentity, ID: foreignIdentityID}: foreignIdentityPublic,
	} {
		key, err := resolver.VerifiedKey(context.Background(), principal, issued.Add(3*time.Second))
		if err != nil || !key.Equal(expected) {
			t.Fatalf("foreign principal %s/%s resolved to %x, err=%v", principal.Kind, principal.ID, key, err)
		}
	}
	if _, err := resolver.VerifiedKey(context.Background(), authority.Principal{Kind: authority.PrincipalNode, ID: "nd_AAAAAAAAAAAAAAAAAAAAAA"}, issued.Add(3*time.Second)); !errors.Is(err, errLeaseIdentityKeyUnavailable) {
		t.Fatalf("unregistered node unexpectedly resolved: %v", err)
	}
}

func newRegisteredLeaseNodeIdentity(t *testing.T, issued time.Time) (*globalidentity.HistoryRegistry, string, string, ed25519.PublicKey, ed25519.PublicKey) {
	t.Helper()
	identityPublic, identityPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	challengeBytes := make([]byte, 32)
	credentialBytes := make([]byte, 32)
	if _, err := rand.Read(challengeBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(credentialBytes); err != nil {
		t.Fatal(err)
	}
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes)
	genesis, genesisSignature := signLeaseIdentityEvent(t, identityID, "0", nil, "genesis", issued,
		identityPublic, identityPrivate, map[string]any{
			"identity_format": "global-identity/v1", "recovery_encryption_public_key": base64.RawURLEncoding.EncodeToString(recoveryPrivate.PublicKey().Bytes()),
			"webauthn_challenge": challenge, "credential_id": base64.RawURLEncoding.EncodeToString(credentialBytes),
		})
	genesisEvent, err := identityprotocol.VerifyEvent(genesis, genesisSignature)
	if err != nil {
		t.Fatal(err)
	}
	nodePublic, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identityprotocol.NodeID(nodePublic)
	if err != nil {
		t.Fatal(err)
	}
	nonceBytes := make([]byte, 32)
	if _, err := rand.Read(nonceBytes); err != nil {
		t.Fatal(err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	proofMessage, err := identityprotocol.NodeEnrollmentProofMessage(identityID, genesisEvent.EventHash, nonce)
	if err != nil {
		t.Fatal(err)
	}
	proof := ed25519.Sign(nodePrivate, proofMessage)
	previous := genesisEvent.EventHash
	delegation, delegationSignature := signLeaseIdentityEvent(t, identityID, "1", &previous, "node_delegate", issued.Add(time.Second),
		identityPublic, identityPrivate, map[string]any{
			"node_id": nodeID, "node_public_key": base64.RawURLEncoding.EncodeToString(nodePublic),
			"enrollment_nonce": nonce, "node_proof": base64.RawURLEncoding.EncodeToString(proof),
		})
	bundle := globalidentity.HistoryBundle{
		Format: globalidentity.HistoryBundleVersion, IdentityID: identityID,
		Events: []globalidentity.HistoryEvent{
			{CanonicalEvent: base64.RawURLEncoding.EncodeToString(genesis), Signature: base64.RawURLEncoding.EncodeToString(genesisSignature)},
			{CanonicalEvent: base64.RawURLEncoding.EncodeToString(delegation), Signature: base64.RawURLEncoding.EncodeToString(delegationSignature)},
		},
	}
	registry, err := globalidentity.OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(bundle); err != nil {
		t.Fatalf("import signed foreign identity history: %v", err)
	}
	return registry, identityID, nodeID, nodePublic, identityPublic
}

func signLeaseIdentityEvent(t *testing.T, identityID, sequence string, previous *string, operation string, issued time.Time,
	public ed25519.PublicKey, private ed25519.PrivateKey, payload map[string]any,
) ([]byte, []byte) {
	t.Helper()
	keyID, err := identityprotocol.KeyID(public)
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID, "sequence": sequence,
		"previous_event_hash": previous, "operation": operation, "issued_at": issued.UTC().Format(time.RFC3339),
		"signer_key_id": keyID, "signer_public_key": base64.RawURLEncoding.EncodeToString(public), "payload": payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, private)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func newLeaseKeyHistoryFixture(t *testing.T, issued time.Time) (leaseKeyHistoryFixture, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(public)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := identityprotocol.KeyID(public)
	if err != nil {
		t.Fatal(err)
	}
	issued = issued.UTC().Truncate(time.Second)
	input, err := json.Marshal(map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID, "sequence": "0",
		"previous_event_hash": nil, "operation": "genesis", "issued_at": issued.Format(time.RFC3339),
		"signer_key_id": keyID, "signer_public_key": base64.RawURLEncoding.EncodeToString(public), "payload": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, private)
	if err != nil {
		t.Fatal(err)
	}
	event, err := identityprotocol.VerifyEvent(canonical, signature)
	if err != nil {
		t.Fatal(err)
	}
	projection := globalidentity.AuthorityProjection{
		IdentityID: identityID, HeadHash: event.EventHash, RootPublicKey: public,
		VerifiedAt: issued.Add(time.Second),
	}
	return leaseKeyHistoryFixture{
		snapshot:   globalidentity.Snapshot{IdentityID: identityID},
		bundle:     globalidentity.HistoryBundle{IdentityID: identityID, Events: []globalidentity.HistoryEvent{{CanonicalEvent: base64.RawURLEncoding.EncodeToString(canonical), Signature: base64.RawURLEncoding.EncodeToString(signature)}}},
		projection: projection,
	}, private
}
