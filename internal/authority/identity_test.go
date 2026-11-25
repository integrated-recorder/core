package authority

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestIdentityStateFromStoreTracksActiveNodeKeys(t *testing.T) {
	rootSeed := make([]byte, ed25519.SeedSize)
	for i := range rootSeed {
		rootSeed[i] = byte(i + 1)
	}
	rootPrivate := ed25519.NewKeyFromSeed(rootSeed)
	rootPublic := rootPrivate.Public().(ed25519.PublicKey)
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	store, err := globalidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	credentialID := []byte("authority-projection-test")
	challenge := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	recoveryPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	genesis := signAuthorityHistoryEvent(t, rootPrivate, identityID, "0", nil, "genesis", map[string]any{
		"identity_format":                "global-identity/v1",
		"recovery_encryption_public_key": base64.RawURLEncoding.EncodeToString(recoveryPrivate.PublicKey().Bytes()),
		"webauthn_challenge":             challenge,
		"credential_id":                  base64.RawURLEncoding.EncodeToString(credentialID),
	}, now)
	if err := store.CreateGenesis(genesis.Canonical, genesis.Signature, make([]byte, 32), webauthn.Credential{ID: credentialID, PublicKey: []byte("public credential key")}, challenge); err != nil {
		t.Fatalf("create identity genesis: %v", err)
	}
	genesisHead := identityprotocol.EventHash(genesis.Canonical)
	state, err := IdentityStateFromStore(store, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.RecoveryEncryptionPublicKey, recoveryPrivate.PublicKey().Bytes()) {
		t.Fatal("identity state did not retain the recovery key from its verified genesis")
	}
	rootPrincipal := Principal{Kind: PrincipalIdentity, ID: identityID}
	if len(state.CurrentKeys) != 1 || len(state.CurrentKeys[rootPrincipal.key()]) != ed25519.PublicKeySize {
		t.Fatalf("initial keys = %v", state.CurrentKeys)
	}

	nodeSeed := make([]byte, ed25519.SeedSize)
	for i := range nodeSeed {
		nodeSeed[i] = byte(90 + i)
	}
	nodePrivate := ed25519.NewKeyFromSeed(nodeSeed)
	nodePublic := nodePrivate.Public().(ed25519.PublicKey)
	nodeID, err := identityprotocol.NodeID(nodePublic)
	if err != nil {
		t.Fatal(err)
	}
	proofNonce := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	message, err := identityprotocol.NodeEnrollmentProofMessage(identityID, identityprotocol.EventHash(genesis.Canonical), proofNonce)
	if err != nil {
		t.Fatal(err)
	}
	proof := ed25519.Sign(nodePrivate, message)
	delegation := signAuthorityHistoryEvent(t, rootPrivate, identityID, "1", hashPointer(identityprotocol.EventHash(genesis.Canonical)), "node_delegate", map[string]any{
		"node_id":          nodeID,
		"node_public_key":  base64.RawURLEncoding.EncodeToString(nodePublic),
		"enrollment_nonce": proofNonce,
		"node_proof":       base64.RawURLEncoding.EncodeToString(proof),
	}, now.Add(2*time.Second))
	if err := store.ApplyNodeEvent(delegation.Canonical, delegation.Signature); err != nil {
		t.Fatalf("delegate node: %v", err)
	}
	delegationHead := identityprotocol.EventHash(delegation.Canonical)
	registry, err := globalidentity.OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	delegatedHistory, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(delegatedHistory); err != nil {
		t.Fatalf("import delegated identity history: %v", err)
	}
	state, err = IdentityStateFromStore(store, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	nodePrincipal := Principal{Kind: PrincipalNode, ID: nodeID}
	if len(state.CurrentKeys) != 2 || len(state.CurrentKeys[nodePrincipal.key()]) != ed25519.PublicKeySize {
		t.Fatalf("delegated node key missing: %v", state.CurrentKeys)
	}

	delegationEvent, err := identityprotocol.VerifyEvent(delegation.Canonical, delegation.Signature)
	if err != nil {
		t.Fatal(err)
	}
	revocation := signAuthorityHistoryEvent(t, rootPrivate, identityID, "2", hashPointer(delegationEvent.EventHash), "node_revoke", map[string]any{"node_id": nodeID}, now.Add(4*time.Second))
	if err := store.ApplyNodeEvent(revocation.Canonical, revocation.Signature); err != nil {
		t.Fatalf("revoke node: %v", err)
	}
	state, err = IdentityStateFromStore(store, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(state.CurrentKeys) != 1 {
		t.Fatalf("revoked node key remained active: %v", state.CurrentKeys)
	}
	oldState, err := IdentityStateFromStoreAt(store, delegationHead, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatalf("read verified historical identity head: %v", err)
	}
	if len(oldState.CurrentKeys) != 2 || len(oldState.CurrentKeys[nodePrincipal.key()]) != ed25519.PublicKeySize {
		t.Fatalf("historical delegated key set = %v", oldState.CurrentKeys)
	}
	rootState, err := IdentityStateFromStoreAt(store, genesisHead, time.Now().UTC().Add(time.Minute))
	if err != nil || len(rootState.CurrentKeys) != 1 || !bytes.Equal(rootState.RecoveryEncryptionPublicKey, recoveryPrivate.PublicKey().Bytes()) {
		t.Fatalf("genesis key set=%v err=%v", rootState.CurrentKeys, err)
	}
	registeredProjection, err := registry.AuthorityProjection(identityID)
	if err != nil || !bytes.Equal(registeredProjection.RecoveryEncryptionPublicKey, recoveryPrivate.PublicKey().Bytes()) {
		t.Fatalf("registered identity projection recovery key mismatch: err=%v", err)
	}
	resolver := IdentityStateResolverFromRegistry(registry)
	if _, err := resolver(identityID, "", time.Now().UTC()); err != ErrStaleIdentity {
		t.Fatalf("manual remote-history import was treated as freshness proof: %v", err)
	}
	localResolver := IdentityStateResolverFromLocalAndRegistry(store, registry)
	localState, err := localResolver(identityID, "", time.Now().UTC())
	if err != nil || localState.HeadHash != identityprotocol.EventHash(revocation.Canonical) || len(localState.CurrentKeys) != 1 {
		t.Fatalf("stale imported identity overrode the authoritative local history: state=%+v err=%v", localState, err)
	}
	publicHistory, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(publicHistory); err != nil {
		t.Fatalf("advance registered history: %v", err)
	}
	if _, err := resolver(identityID, "", time.Now().UTC()); err != ErrStaleIdentity {
		t.Fatalf("manually advanced remote history was treated as freshness proof: %v", err)
	}
}

type signedHistoryFixture struct {
	Canonical []byte
	Signature []byte
}

func signAuthorityHistoryEvent(t *testing.T, private ed25519.PrivateKey, identityID, sequence string, previous *string, operation string, payload map[string]any, issued time.Time) signedHistoryFixture {
	t.Helper()
	public := private.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(public)
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         identityID,
		"sequence":            sequence,
		"previous_event_hash": previous,
		"operation":           operation,
		"issued_at":           issued.Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(public),
		"payload":             payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, private)
	if err != nil {
		t.Fatal(err)
	}
	return signedHistoryFixture{Canonical: canonical, Signature: signature}
}

func hashPointer(value string) *string { return &value }
