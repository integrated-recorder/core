package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
)

type replicaInboxPeerFixture struct {
	peerSessionManagerFixture
	called int
	scope  peerapi.RequestScope
	offers []lease.PendingOffer
}

func (m *replicaInboxPeerFixture) ListReplicaOffers(_ context.Context, nodeID string, scope peerapi.RequestScope) ([]lease.PendingOffer, error) {
	m.called++
	m.scope = scope
	if len(m.sessions) == 0 || nodeID != m.sessions[0].NodeID {
		return nil, peerapi.ErrPeerNotConnected
	}
	return append([]lease.PendingOffer(nil), m.offers...), nil
}

func TestReplicaInboxUsesCurrentNodeScopedReplicationAuthority(t *testing.T) {
	root := t.TempDir()
	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("inbox-owner", "inbox-owner-password"); err != nil {
		t.Fatal(err)
	}
	session, err := users.LoginAs("inbox-owner", "inbox-owner-password")
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
	sourceIdentity, err := globalidentity.Open(filepath.Join(root, "source-identity"))
	if err != nil {
		t.Fatal(err)
	}
	sourceRootKey, sourceGenesis, sourceSignature, sourceHandle, sourceCredential, sourceChallenge, sourceIdentityID := signedTestGenesis(t)
	if err := sourceIdentity.CreateGenesis(sourceGenesis, sourceSignature, sourceHandle, sourceCredential, sourceChallenge); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
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
	_, initialized, err := identity.Snapshot()
	if err != nil || !initialized {
		t.Fatalf("identity snapshot: initialized=%v err=%v", initialized, err)
	}
	for index, node := range []struct {
		store *nodeidentity.Store
	}{{localNodeStore}} {
		identityStateBefore, isInitialized, snapshotErr := identity.Snapshot()
		if snapshotErr != nil || !isInitialized {
			t.Fatalf("identity before Node enrollment: initialized=%v err=%v", isInitialized, snapshotErr)
		}
		proof, proofErr := node.store.BeginEnrollmentProof(identityID, identityStateBefore.HeadHash, now)
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		delegation, delegationSignature := signedTestNodeDelegation(t, rootKey, proof, fmt.Sprint(index+1), now)
		if err := identity.ApplyNodeEvent(delegation, delegationSignature); err != nil {
			t.Fatalf("delegate Node: %v", err)
		}
	}
	sourceBefore, _, err := sourceIdentity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sourceProof, err := remoteNodeStore.BeginEnrollmentProof(sourceIdentityID, sourceBefore.HeadHash, now)
	if err != nil {
		t.Fatal(err)
	}
	sourceDelegation, sourceDelegationSignature := signedTestNodeDelegation(t, sourceRootKey, sourceProof, "1", now)
	if err := remoteNodeStore.ConsumeEnrollmentEvent(sourceDelegation, sourceDelegationSignature, now); err != nil {
		t.Fatal(err)
	}
	if err := sourceIdentity.ApplyNodeEvent(sourceDelegation, sourceDelegationSignature); err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "identity-registry"))
	if err != nil {
		t.Fatal(err)
	}
	sourceHistory, err := sourceIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(sourceHistory); err != nil {
		t.Fatal(err)
	}
	directoryA, err := discovery.OpenDirectory(filepath.Join(root, "source-directory-a"))
	if err != nil {
		t.Fatal(err)
	}
	directoryB, err := discovery.OpenDirectory(filepath.Join(root, "source-directory-b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := directoryA.PublishIdentityHistory(sourceHistory); err != nil {
		t.Fatal(err)
	}
	if err := directoryB.PublishIdentityHistory(sourceHistory); err != nil {
		t.Fatal(err)
	}
	federation, err := discovery.NewFederationClient([]string{"https://source-a.example", "https://source-b.example"}, &http.Client{
		Transport: &serverHistoryFederationTransport{handlers: map[string]http.Handler{
			"source-a.example": discovery.HTTPHandler(directoryA),
			"source-b.example": discovery.HTTPHandler(directoryB),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolve := authority.IdentityStateResolverFromStores(map[string]*globalidentity.Store{
		identityID: identity, sourceIdentityID: sourceIdentity,
	})
	now = time.Now().UTC().Truncate(time.Second)
	ownerStore, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_" + strings.Repeat("A", 22)
	sourceIdentityState, err := authority.IdentityStateFromStore(sourceIdentity, now)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := authority.NewPersonOwnerSubject(sourceIdentityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x39}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, sourceIdentityState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(create, sourceRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(createSigned, resolve, now); err != nil {
		t.Fatal(err)
	}
	keyID, err := identityprotocol.KeyID(sourceRootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	nonce := sha256.Sum256([]byte("replica inbox capability"))
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:   authority.Principal{Kind: authority.PrincipalIdentity, ID: sourceIdentityID},
		Audience: authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID}, ArchiveID: archiveID,
		Actions: []string{"archive.replicate"}, NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce[:]), PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: keyID,
	}
	signedGrant, err := authority.SignGrant(grant, sourceRootKey)
	if err != nil {
		t.Fatal(err)
	}
	current, err := ownerStore.Snapshot(subject.ID, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := authority.NewAuthorityEvent(subject.ID, "2", current.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{signedGrant}}, sourceIdentityState, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := authority.SignAuthorityEvent(issue, sourceRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(issueSigned, resolve, now); err != nil {
		t.Fatal(err)
	}
	grantHash, err := authority.GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}
	snapshotID := "snap_" + strings.Repeat("d", 64)
	leaseID := "rl_" + strings.Repeat("E", 22)
	offerHash := "sha256:" + strings.Repeat("f", 64)
	remoteNodeID := remoteNode.NodeID
	peers := &replicaInboxPeerFixture{
		peerSessionManagerFixture: peerSessionManagerFixture{sessions: []peerapi.SessionInfo{{IdentityID: sourceIdentityID, NodeID: remoteNodeID}}},
		offers: []lease.PendingOffer{{LeaseID: leaseID, ArchiveID: archiveID, SnapshotID: snapshotID, OwnerSubject: subject.ID,
			OfferHash: offerHash, KeyEpoch: "1", ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), ReservedBytes: "1024", ObjectCount: 2}},
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: users, GlobalIdentity: identity, GlobalIdentityRegistry: registry,
		IdentityFederation: federation, NodeIdentity: localNodeStore, OwnerAuthority: ownerStore, PeerSessions: peers})
	target := "/api/peer-archives/" + remoteNodeID + "/replica-offers?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+target, "", session))
	if response.Code != http.StatusOK || peers.called != 1 || !strings.Contains(response.Body.String(), leaseID) || !strings.Contains(response.Body.String(), grantHash) {
		t.Fatalf("inbox status=%d calls=%d body=%s", response.Code, peers.called, response.Body.String())
	}
	scopeGrantHash, scopeHashErr := authority.GrantHash(peers.scope.GrantChain[0].Grant)
	if scopeHashErr != nil || strings.Contains(response.Body.String(), "signature") || strings.Contains(response.Body.String(), "encrypted_object_set") ||
		peers.scope.Action != "archive.replicate" || peers.scope.IdentityID != identityID || peers.scope.NodeID != localNode.NodeID ||
		peers.scope.TargetIdentityID != sourceIdentityID || peers.scope.TargetNodeID != remoteNodeID || peers.scope.OwnerSubjectID != subject.ID || peers.scope.ArchiveID != archiveID ||
		peers.scope.SnapshotID != "" || len(peers.scope.GrantChain) != 1 || scopeGrantHash != grantHash {
		t.Fatalf("inbox leaked offer internals or widened authority scope: body=%s scope=%+v", response.Body.String(), peers.scope)
	}
	httpResponse := httptest.NewRecorder()
	plainHTTPRequest := authenticatedIdentityRequest(http.MethodGet, "http://recorder.example.com"+target, "", session)
	plainHTTPRequest.Header.Del("X-IR-HTTPS")
	handler.ServeHTTP(httpResponse, plainHTTPRequest)
	if httpResponse.Code != http.StatusUpgradeRequired || peers.called != 1 {
		t.Fatalf("inbox served over HTTP or reached peer: status=%d calls=%d body=%s", httpResponse.Code, peers.called, httpResponse.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"source_identity_id":"`+sourceIdentityID+`"`) {
		t.Fatalf("cross-identity response omitted authenticated source identity: %s", response.Body.String())
	}
	server := &Server{
		globalIdentity: identity, globalIdentityRegistry: registry, ownerAuthority: ownerStore,
		federatedIdentityResolver: authority.NewFederatedIdentityResolver(registry, federation),
		nodeIdentity:              localNodeStore, peerSessions: peers,
	}
	importResponse := httptest.NewRecorder()
	importRequest := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/replica-imports", "", session)
	importScope, scopeOK := server.replicaImportScope(importResponse, importRequest, replicaImportRequest{
		SourceNodeID: remoteNodeID, OwnerSubjectID: subject.ID, GrantHash: grantHash,
		ArchiveID: archiveID, SnapshotID: snapshotID, LeaseID: leaseID,
	})
	if !scopeOK || importScope.IdentityID != identityID || importScope.NodeID != localNode.NodeID ||
		importScope.TargetIdentityID != sourceIdentityID || importScope.TargetNodeID != remoteNodeID ||
		importScope.OwnerSubjectID != subject.ID || importScope.Action != "archive.replicate" {
		t.Fatalf("cross-identity import scope was not bound to the authenticated source: ok=%v status=%d scope=%+v", scopeOK, importResponse.Code, importScope)
	}

	// A Node connected under two different authenticated identities is
	// ambiguous. Neither inbox discovery nor import may choose one identity.
	peers.sessions = append(peers.sessions, peerapi.SessionInfo{IdentityID: identityID, NodeID: remoteNodeID})
	ambiguousResponse := httptest.NewRecorder()
	server.replicaImportScope(ambiguousResponse, importRequest, replicaImportRequest{
		SourceNodeID: remoteNodeID, OwnerSubjectID: subject.ID, GrantHash: grantHash,
		ArchiveID: archiveID, SnapshotID: snapshotID, LeaseID: leaseID,
	})
	if ambiguousResponse.Code != http.StatusConflict {
		t.Fatalf("ambiguous source identity status=%d body=%s", ambiguousResponse.Code, ambiguousResponse.Body.String())
	}
	peers.sessions = peers.sessions[:1]

	// A source Node revoked in its signed history must stop being eligible even
	// though the peer session has not yet disconnected.
	sourceCurrent, _, err := sourceIdentity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	revokeEvent, revokeSignature := signedTestNodeRevocation(t, sourceRootKey, sourceIdentityID, "2", sourceCurrent.HeadHash, remoteNodeID, time.Now().UTC())
	if err := sourceIdentity.ApplyNodeEvent(revokeEvent, revokeSignature); err != nil {
		t.Fatal(err)
	}
	updatedSourceHistory, err := sourceIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(updatedSourceHistory); err != nil {
		t.Fatal(err)
	}
	if err := directoryA.PublishIdentityHistory(updatedSourceHistory); err != nil {
		t.Fatal(err)
	}
	if err := directoryB.PublishIdentityHistory(updatedSourceHistory); err != nil {
		t.Fatal(err)
	}
	revokedResponse := httptest.NewRecorder()
	handler.ServeHTTP(revokedResponse, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+target, "", session))
	if revokedResponse.Code != http.StatusForbidden || peers.called != 1 {
		t.Fatalf("revoked source was allowed or queried: status=%d calls=%d body=%s", revokedResponse.Code, peers.called, revokedResponse.Body.String())
	}
}

var _ peerReplicaOfferLister = (*replicaInboxPeerFixture)(nil)

func TestReplicaInboxRouteRequiresRecordingReadPermission(t *testing.T) {
	if got := routePermission(http.MethodGet, "/api/peer-archives/nd_AAAAAAAAAAAAAAAAAAAAAA/replica-offers"); got != authn.PermissionRecordingRead {
		t.Fatalf("replica inbox permission=%q", got)
	}
}
