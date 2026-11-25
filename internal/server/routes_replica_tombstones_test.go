package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
)

type replicaTombstoneStoreFixture struct {
	*lease.Store
	providerLeases []lease.Lease
}

func (f replicaTombstoneStoreFixture) Snapshot(ctx context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	leases, tombstones, err := f.Store.Snapshot(ctx)
	return append(leases, f.providerLeases...), tombstones, err
}

type replicaTombstoneKeys struct {
	identityID string
	resolve    authority.AuthorityIdentityResolver
}

func (r replicaTombstoneKeys) VerifiedKey(_ context.Context, principal authority.Principal, at time.Time) (ed25519.PublicKey, error) {
	state, err := r.resolve(r.identityID, "", at)
	if err != nil {
		return nil, err
	}
	key := state.CurrentKeys[principal.Kind+":"+principal.ID]
	if len(key) != ed25519.PublicKeySize {
		return nil, lease.ErrDenied
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

func TestReplicaTombstoneRoutePersistsAndRetriesExactDecision(t *testing.T) {
	root := t.TempDir()
	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("tombstone-owner", "tombstone-owner-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := users.LoginAs("tombstone-owner", "tombstone-owner-test-password")
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(filepath.Join(root, "products"))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := globalidentity.Open(filepath.Join(root, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, signature, handle, credential, challenge, identityID := signedTestGenesis(t)
	if err := identity.CreateGenesis(genesis, signature, handle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	localNodeStore, err := nodeidentity.Open(filepath.Join(root, "local-node"))
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := localNodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	remoteNodeStore, err := nodeidentity.Open(filepath.Join(root, "remote-node"))
	if err != nil {
		t.Fatal(err)
	}
	remoteNode, err := remoteNodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	delegate := func(store *nodeidentity.Store, sequence string) {
		t.Helper()
		current, initialized, err := identity.Snapshot()
		if err != nil || !initialized {
			t.Fatalf("identity snapshot: initialized=%v err=%v", initialized, err)
		}
		proof, err := store.BeginEnrollmentProof(identityID, current.HeadHash, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		event, eventSignature := signedTestNodeDelegation(t, rootKey, proof, sequence, time.Now().UTC())
		if err := identity.ApplyNodeEvent(event, eventSignature); err != nil {
			t.Fatalf("delegate Node: %v", err)
		}
	}
	delegate(localNodeStore, "1")
	delegate(remoteNodeStore, "2")

	now := time.Now().UTC().Truncate(time.Second)
	identityState, err := authority.IdentityStateFromStore(identity, now)
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	subject, err := authority.NewPersonOwnerSubject(identityID, "1", now, bytes.NewReader([]byte("0123456789abcdef")))
	if err != nil {
		t.Fatal(err)
	}
	resolve := resolverForGlobalIdentity(identity)
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, identityState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(create, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(createSigned, resolve, now); err != nil {
		t.Fatal(err)
	}
	keyID, err := identityprotocol.KeyID(rootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID},
		ArchiveID: archiveID, Actions: []string{"archive.delete", "archive.replicate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: strings.Repeat("n", 43), PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: keyID,
	}, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	ownerHead, err := ownerStore.Snapshot(subject.ID, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := authority.NewAuthorityEvent(subject.ID, "2", ownerHead.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{grant}}, identityState, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := authority.SignAuthorityEvent(issue, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(issueSigned, resolve, now); err != nil {
		t.Fatal(err)
	}
	grantHash, err := authority.GrantHash(grant.Grant)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.Open(lease.Options{
		Root:       filepath.Join(root, "leases"),
		Authorizer: lease.DurableAuthority{Store: ownerStore, Resolve: resolve},
		Keys:       replicaTombstoneKeys{identityID: identityID, resolve: resolve},
	})
	if err != nil {
		t.Fatal(err)
	}
	tombstoneStore := replicaTombstoneStoreFixture{
		Store: leases,
		providerLeases: []lease.Lease{{Offer: lease.SignedOffer{Offer: lease.Offer{
			ArchiveID: archiveID, ProviderNodeID: remoteNode.NodeID,
		}}}},
	}
	peers := &replicaPeerSessionFixture{sessions: []peerapi.SessionInfo{{IdentityID: identityID, NodeID: remoteNode.NodeID}}}
	handler := NewWithOptions(nil, nil, nil, Options{
		Auth: users, Management: products, GlobalIdentity: identity, OwnerAuthority: ownerStore,
		NodeIdentity: localNodeStore, PeerSessions: peers, ReplicaLeases: tombstoneStore,
	})
	if permission := routePermission(http.MethodPost, "/api/replica-tombstones"); permission != authn.PermissionRecordingDelete {
		t.Fatalf("tombstone route permission=%q", permission)
	}
	body, err := json.Marshal(replicaTombstoneRequest{OwnerSubjectID: subject.ID, ArchiveID: archiveID, GrantHash: grantHash})
	if err != nil {
		t.Fatal(err)
	}
	request := func() *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/replica-tombstones", string(body), session))
		return recorder
	}
	invalidBody := replicaTombstoneRequest{OwnerSubjectID: subject.ID, ArchiveID: archiveID, GrantHash: "sha256:" + strings.Repeat("0", 64)}
	invalidJSON, _ := json.Marshal(invalidBody)
	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/replica-tombstones", string(invalidJSON), session))
	if invalid.Code != http.StatusForbidden {
		t.Fatalf("invalid grant status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	peers.err = errors.New("peer transport unavailable")
	first := request()
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"physical_deletion_performed":false`) || !strings.Contains(first.Body.String(), `"status":"pending"`) || strings.Contains(first.Body.String(), "peer transport unavailable") {
		t.Fatalf("first tombstone request status=%d body=%s", first.Code, first.Body.String())
	}
	_, firstTombstones, err := leases.Snapshot(context.Background())
	if err != nil || len(firstTombstones) != 1 || peers.calls != 1 || peers.requested != remoteNode.NodeID {
		t.Fatalf("first durable result tombstones=%d calls=%d target=%s err=%v", len(firstTombstones), peers.calls, peers.requested, err)
	}
	firstHash := firstTombstones[0].Hash
	firstSignature := peers.tombstone.Signature
	peers.err = nil
	second := request()
	_, secondTombstones, err := leases.Snapshot(context.Background())
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"status":"durable_receipt"`) || err != nil || len(secondTombstones) != 1 || secondTombstones[0].Hash != firstHash || len(secondTombstones[0].Deliveries) != 1 || secondTombstones[0].Deliveries[0].NodeID != remoteNode.NodeID || peers.calls != 2 || peers.tombstone.Signature != firstSignature {
		t.Fatalf("retry status=%d tombstones=%+v calls=%d err=%v body=%s", second.Code, secondTombstones, peers.calls, err, second.Body.String())
	}
	if strings.Contains(first.Body.String(), peers.tombstone.Signature) || peers.tombstone.Actor != (authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID}) {
		t.Fatal("API leaked the signature or signed as the wrong Node")
	}
}

func TestReplicaTombstoneTargetsOnlyCurrentReplicaProviders(t *testing.T) {
	local := "nd_AAAAAAAAAAAAAAAAAAAAAA"
	providerA := "nd_BBBBBBBBBBBBBBBBBBBBBB"
	providerB := "nd_CCCCCCCCCCCCCCCCCCCCCC"
	revoked := "nd_DDDDDDDDDDDDDDDDDDDDDD"
	archive := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	otherArchive := "arc_BBBBBBBBBBBBBBBBBBBBBB"
	leases := []lease.Lease{
		{Offer: lease.SignedOffer{Offer: lease.Offer{ArchiveID: archive, ProviderNodeID: providerB}}},
		{Offer: lease.SignedOffer{Offer: lease.Offer{ArchiveID: archive, ProviderNodeID: providerA}}},
		{Offer: lease.SignedOffer{Offer: lease.Offer{ArchiveID: archive, ProviderNodeID: providerA}}},
		{Offer: lease.SignedOffer{Offer: lease.Offer{ArchiveID: archive, ProviderNodeID: local}}},
		{Offer: lease.SignedOffer{Offer: lease.Offer{ArchiveID: archive, ProviderNodeID: revoked}}},
		{Offer: lease.SignedOffer{Offer: lease.Offer{ArchiveID: otherArchive, ProviderNodeID: providerB}}},
	}
	delegated := map[string]struct{}{providerA: {}, providerB: {}}
	got := replicaTombstoneTargets(leases, archive, local, delegated)
	want := []string{providerA, providerB}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tombstone targets=%v, want %v", got, want)
	}
}
