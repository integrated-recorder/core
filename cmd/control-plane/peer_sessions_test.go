package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
)

func clearPeerTURNEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"IR_PEER_ICE_CONFIGURATION_INVALID", "IR_PEER_TURN_URLS", "IR_PEER_STUN_URLS",
		"IR_PEER_TURN_SHARED_SECRET_FILE", "IR_PEER_TURN_CREDENTIAL_TTL", "IR_PEER_ICE_RELAY_ONLY",
	} {
		t.Setenv(key, "")
	}
}

func TestAuthorizePeerSessionPreservesSameIdentityNodePolicy(t *testing.T) {
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	localPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identityprotocol.NodeID(public)
	if err != nil {
		t.Fatal(err)
	}
	localNodeID, err := identityprotocol.NodeID(localPublic)
	if err != nil {
		t.Fatal(err)
	}
	const identityID = "gi_aaaaaaaaaaaaaaaaaaaaaa"
	state := authority.IdentityState{
		IdentityID: identityID,
		CurrentKeys: map[string]ed25519.PublicKey{
			"node:" + nodeID:      public,
			"node:" + localNodeID: localPublic,
		},
	}
	resolve := func(id, head string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || head != "" {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}
	now := time.Now().UTC()
	if err := authorizePeerSession(identityID, localNodeID, identityID, nodeID, resolve, nil, now); err != nil {
		t.Fatalf("currently delegated peer was rejected: %v", err)
	}
	if err := authorizePeerSession(identityID, localNodeID, "gi_bbbbbbbbbbbbbbbbbbbbbb", nodeID, resolve, nil, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("different identity was accepted: %v", err)
	}
	if err := authorizePeerSession(identityID, localNodeID, identityID, "nd_eeeeeeeeeeeeeeeeeeeeee", resolve, nil, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("undelegated node was accepted: %v", err)
	}
	state.Frozen = true
	if err := authorizePeerSession(identityID, localNodeID, identityID, nodeID, resolve, nil, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("frozen identity was accepted: %v", err)
	}
}

func TestAuthorizePeerSessionAllowsCrossIdentityOnlyWithCurrentCapability(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	rootAPublic, rootAPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootBPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeAPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeBPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	deniedNodePublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityA, err := identityprotocol.IdentityID(rootAPublic)
	if err != nil {
		t.Fatal(err)
	}
	identityB, err := identityprotocol.IdentityID(rootBPublic)
	if err != nil {
		t.Fatal(err)
	}
	nodeA, _ := identityprotocol.NodeID(nodeAPublic)
	nodeB, _ := identityprotocol.NodeID(nodeBPublic)
	deniedNode, _ := identityprotocol.NodeID(deniedNodePublic)
	stateA := authority.IdentityState{
		IdentityID: identityA, HeadHash: "sha256:" + strings.Repeat("a", 64), VerifiedAt: now, FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + identityA: rootAPublic,
			authority.PrincipalNode + ":" + nodeA:         nodeAPublic,
		},
	}
	stateB := authority.IdentityState{
		IdentityID: identityB, HeadHash: "sha256:" + strings.Repeat("b", 64), VerifiedAt: now, FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + identityB: rootBPublic,
			authority.PrincipalNode + ":" + nodeB:         nodeBPublic,
		},
	}
	resolve := func(id, head string, _ time.Time) (authority.IdentityState, error) {
		state, ok := map[string]authority.IdentityState{identityA: stateA, identityB: stateB}[id]
		if !ok || head != "" && head != state.HeadHash {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}
	store, err := authority.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	subject, err := authority.NewPersonOwnerSubject(identityA, "1", now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(sequence, previousHash, operation string, payload any) string {
		t.Helper()
		event, eventErr := authority.NewAuthorityEventForSigner(subject.ID, sequence, previousHash, operation, payload,
			authority.Principal{Kind: authority.PrincipalIdentity, ID: identityA}, stateA, now)
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		signed, signErr := authority.SignAuthorityEvent(event, rootAPrivate)
		if signErr != nil {
			t.Fatal(signErr)
		}
		if applyErr := store.Apply(signed, resolve, now); applyErr != nil {
			t.Fatal(applyErr)
		}
		hash, hashErr := authority.AuthorityEventHash(event)
		if hashErr != nil {
			t.Fatal(hashErr)
		}
		return hash
	}
	createHash := apply("1", "", authority.AuthorityCreateSubject, authority.SubjectCreatePayload{Subject: subject})
	archiveID := "arc_" + strings.Repeat("A", 22)
	claimHash := apply("2", createHash, authority.AuthorityClaimArchive, authority.ArchiveClaimPayload{ArchiveID: archiveID})
	keyID, err := identityprotocol.KeyID(rootAPublic)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:   authority.Principal{Kind: authority.PrincipalIdentity, ID: identityA},
		Audience: authority.Principal{Kind: authority.PrincipalNode, ID: nodeB}, ArchiveID: archiveID,
		Actions: []string{"archive.view"}, NotBefore: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce), PolicyEpoch: "1", IssuerKeyID: keyID,
	}
	signedGrant, err := authority.SignGrant(grant, rootAPrivate)
	if err != nil {
		t.Fatal(err)
	}
	grantHash, err := authority.GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}
	issueHash := apply("3", claimHash, authority.AuthorityIssueGrant, authority.GrantIssuePayload{Chain: []authority.SignedGrant{signedGrant}})

	if err := authorizePeerSession(identityA, nodeA, identityB, nodeB, resolve, store, now); err != nil {
		t.Fatalf("owner-authorized cross-identity connection was rejected: %v", err)
	}
	if err := authorizePeerSession(identityB, nodeB, identityA, nodeA, resolve, store, now); err != nil {
		t.Fatalf("recipient could not authorize the source using its synced subject grant: %v", err)
	}
	peers := &fakePeerConnectionManager{
		sessions:   []peerapi.SessionInfo{{IdentityID: identityB, NodeID: nodeB}},
		connectErr: map[string]error{},
	}
	reconciler := &peerConnectionReconciler{
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityA, NodeIDs: []string{nodeA}}, found: true},
		node:     fakeNodeInfoReader{info: nodeidentity.Info{NodeID: nodeA}},
		peers:    peers,
		resolve:  resolve,
		owners:   store,
	}
	if err := reconciler.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile authorized cross-identity session: %v", err)
	}
	_, disconnects := peers.results()
	if len(disconnects) != 0 {
		t.Fatalf("reconciler disconnected a currently authorized cross-identity session: %v", disconnects)
	}

	// This is unit/reconciler coverage of the authorization and desired-session
	// boundary. It does not exercise a real Pion transport or manager restart.
	delete(stateB.CurrentKeys, authority.PrincipalNode+":"+nodeB)
	if err := authorizePeerSession(identityA, nodeA, identityB, nodeB, resolve, store, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("cross-identity session remained authorized after remote Node revocation: %v", err)
	}
	if err := reconciler.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile revoked remote Node session: %v", err)
	}
	connects, disconnects := peers.results()
	if len(connects) != 0 || !reflect.DeepEqual(disconnects, []string{nodeB}) {
		t.Fatalf("revoked remote Node lifecycle connects=%v disconnects=%v", connects, disconnects)
	}
	// Model the production manager's successful disconnect by removing the
	// session from this fake's inventory before the next reconciliation pass.
	peers.mu.Lock()
	peers.sessions = nil
	peers.mu.Unlock()
	if err := reconciler.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile after revoked remote Node was removed: %v", err)
	}
	connects, disconnects = peers.results()
	if len(connects) != 0 || !reflect.DeepEqual(disconnects, []string{nodeB}) {
		t.Fatalf("revoked cross-identity Node was reconnected: connects=%v disconnects=%v", connects, disconnects)
	}

	if err := authorizePeerSession(identityA, nodeA, identityB, deniedNode, resolve, store, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("undelegated target Node was accepted: %v", err)
	}
	if err := authorizePeerSession(identityA, nodeA, identityB, nodeB, resolve, nil, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("cross-identity connection without an authority store was accepted: %v", err)
	}
	apply("4", issueHash, authority.AuthorityRevokeGrant, authority.GrantRevokePayload{GrantHash: grantHash})
	if err := authorizePeerSession(identityA, nodeA, identityB, nodeB, resolve, store, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("cross-identity connection retained a revoked capability: %v", err)
	}
	if err := authorizePeerSession(identityB, nodeB, identityA, nodeA, resolve, store, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("recipient retained access after capability revocation: %v", err)
	}
	if err := reconciler.Reconcile(t.Context()); err != nil {
		t.Fatalf("reconcile revoked cross-identity session: %v", err)
	}
	_, disconnects = peers.results()
	if len(disconnects) != 1 || disconnects[0] != nodeB {
		t.Fatalf("reconciler did not disconnect the revoked cross-identity session: %v", disconnects)
	}
	stateB.Frozen = true
	if err := authorizePeerSession(identityA, nodeA, identityB, nodeB, resolve, store, now); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("frozen target identity was accepted: %v", err)
	}
}

func TestNewPeerSessionSlotUnavailableWithoutDurableIdentityPrerequisites(t *testing.T) {
	slot := newPeerSessionSlot(t.TempDir(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if slot == nil {
		t.Fatal("slot was not constructed")
	}
	if err := slot.Start(t.Context()); err != nil {
		t.Fatalf("optional peer networking blocked Control startup: %v", err)
	}
	if slot.Available() {
		t.Fatal("peer networking became available without identity prerequisites")
	}
	if err := slot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadPeerTURNAuthConfigDefaultsToNoRemoteICEServers(t *testing.T) {
	clearPeerTURNEnvironment(t)
	config, err := loadPeerTURNAuthConfig()
	if err != nil || config != nil {
		t.Fatalf("unset peer ICE config=%+v err=%v", config, err)
	}
}

func TestLoadPeerTURNAuthConfigReadsOnlyPrivateSecretFile(t *testing.T) {
	clearPeerTURNEnvironment(t)
	secretPath := filepath.Join(t.TempDir(), "turn-secret")
	if err := os.WriteFile(secretPath, []byte("0123456789abcdef0123456789abcdef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IR_PEER_TURN_URLS", "turn:turn.example.test:3478?transport=udp, turns:turn.example.test:5349?transport=tcp")
	t.Setenv("IR_PEER_STUN_URLS", "stun:stun.example.test:3478")
	t.Setenv("IR_PEER_TURN_SHARED_SECRET_FILE", secretPath)
	t.Setenv("IR_PEER_TURN_CREDENTIAL_TTL", "4m")
	t.Setenv("IR_PEER_ICE_RELAY_ONLY", "true")
	config, err := loadPeerTURNAuthConfig()
	if err != nil {
		t.Fatal("valid private TURN config rejected")
	}
	if config == nil || !strings.Contains(config.String(), "turn_urls:2 stun_urls:1 relay_only:true credential_ttl:4m0s") {
		t.Fatal("loaded TURN config differs from expected safe shape")
	}
	if strings.Contains(config.String(), "0123456789abcdef") {
		t.Fatal("TURN config formatter exposed the shared secret")
	}
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	nodeID, err := identityprotocol.NodeID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	ice, err := config.ICE(nodeID, time.Now().UTC())
	if err != nil || len(ice.Servers) != 2 || !ice.RelayOnly || ice.Servers[1].Credential == "" {
		t.Fatal("loaded TURN config did not derive ICE credentials")
	}
	if err := os.Chmod(secretPath, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPeerTURNAuthConfig(); !errors.Is(err, peertransport.ErrConfig) {
		t.Fatal("group-readable TURN secret file was accepted")
	}
}

func TestLoadPeerTURNAuthConfigRejectsMalformedSettingsWithoutLeakingSecretPath(t *testing.T) {
	clearPeerTURNEnvironment(t)
	t.Setenv("IR_PEER_TURN_URLS", "turn:turn.example.test:3478,,turn:other.example.test:3478")
	_, err := loadPeerTURNAuthConfig()
	if !errors.Is(err, peertransport.ErrConfig) || strings.Contains(err.Error(), "turn.example") {
		t.Fatalf("malformed TURN settings error=%v", err)
	}
	clearPeerTURNEnvironment(t)
	t.Setenv("IR_PEER_ICE_CONFIGURATION_INVALID", "1")
	if _, err := loadPeerTURNAuthConfig(); !errors.Is(err, peertransport.ErrConfig) {
		t.Fatal("invalid optional Host configuration marker was ignored")
	}
}

func TestLoadPeerTURNAuthConfigRejectsSymlinkAndMissingSecret(t *testing.T) {
	clearPeerTURNEnvironment(t)
	secretPath := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secretPath, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(t.TempDir(), "secret-link")
	if err := os.Symlink(secretPath, linkPath); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IR_PEER_TURN_URLS", "turn:turn.example.test:3478")
	t.Setenv("IR_PEER_TURN_SHARED_SECRET_FILE", linkPath)
	if _, err := loadPeerTURNAuthConfig(); !errors.Is(err, peertransport.ErrConfig) {
		t.Fatal("TURN shared-secret symlink was accepted")
	}
	t.Setenv("IR_PEER_TURN_SHARED_SECRET_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := loadPeerTURNAuthConfig(); !errors.Is(err, peertransport.ErrConfig) {
		t.Fatal("missing TURN shared secret was accepted")
	}
}
