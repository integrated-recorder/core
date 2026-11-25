package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/replication"
)

type peerArchiveHistoryTransport map[string]http.Handler

func (transport peerArchiveHistoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	handler := transport[request.URL.Host]
	if handler == nil {
		return nil, errors.New("unknown test discovery host")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(request.Method, request.URL.String(), request.Body))
	return response.Result(), nil
}

type peerArchiveSessionFixture struct {
	peerSessionManagerFixture
	snapshot archivesnapshot.Snapshot
	scope    peerapi.RequestScope
	called   int
	err      error
	manifest replication.ReplicaManifest
	epochs   []uint64
	envelope cryptoarchive.KeyEnvelope
	object   []byte
}

func (m *peerArchiveSessionFixture) HeadSnapshot(_ context.Context, nodeID string, scope peerapi.RequestScope) (archivesnapshot.Snapshot, error) {
	m.called++
	if nodeID != m.sessions[0].NodeID {
		return archivesnapshot.Snapshot{}, peerapi.ErrPeerNotConnected
	}
	m.scope = scope
	return m.snapshot, m.err
}

func (m *peerArchiveSessionFixture) Snapshot(_ context.Context, nodeID string, scope peerapi.RequestScope, snapshotID string) (archivesnapshot.Snapshot, error) {
	m.called++
	m.scope = scope
	if nodeID != m.sessions[0].NodeID || snapshotID != m.snapshot.SnapshotID {
		return archivesnapshot.Snapshot{}, peerapi.ErrPeerNotConnected
	}
	return m.snapshot, m.err
}

func (m *peerArchiveSessionFixture) ReplicaManifest(_ context.Context, nodeID string, scope peerapi.RequestScope, _ archivesnapshot.Snapshot, _ uint64) (replication.ReplicaManifest, error) {
	m.called++
	m.scope = scope
	if nodeID != m.sessions[0].NodeID {
		return replication.ReplicaManifest{}, peerapi.ErrPeerNotConnected
	}
	return m.manifest, m.err
}

func (m *peerArchiveSessionFixture) KeyEpochs(_ context.Context, nodeID string, scope peerapi.RequestScope, _ archivesnapshot.Snapshot, _ string) ([]uint64, error) {
	m.called++
	m.scope = scope
	if nodeID != m.sessions[0].NodeID {
		return nil, peerapi.ErrPeerNotConnected
	}
	return append([]uint64(nil), m.epochs...), m.err
}

func (m *peerArchiveSessionFixture) KeyEnvelope(_ context.Context, nodeID string, scope peerapi.RequestScope, _ archivesnapshot.Snapshot, _ uint64, recipientKeyID string) (cryptoarchive.KeyEnvelope, error) {
	m.called++
	m.scope = scope
	if nodeID != m.sessions[0].NodeID || recipientKeyID != m.envelope.RecipientKeyID {
		return cryptoarchive.KeyEnvelope{}, peerapi.ErrPeerNotConnected
	}
	return m.envelope, m.err
}

func (m *peerArchiveSessionFixture) OpenCiphertextRange(_ context.Context, nodeID string, scope peerapi.RequestScope, snapshotID, _ string, _ uint64, offset, length int64) (io.ReadCloser, error) {
	m.called++
	m.scope = scope
	if nodeID != m.sessions[0].NodeID || snapshotID != m.snapshot.SnapshotID || offset < 0 || length <= 0 || offset+length > int64(len(m.object)) {
		return nil, peerapi.ErrPeerNotConnected
	}
	return io.NopCloser(bytes.NewReader(m.object[offset : offset+length])), nil
}

func TestPeerArchiveHeadUsesCurrentCommittedArchiveCapability(t *testing.T) {
	root := t.TempDir()
	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("archive-owner", "remote-archive-owner-password"); err != nil {
		t.Fatal(err)
	}
	login, err := users.LoginAs("archive-owner", "remote-archive-owner-password")
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
	nodeStore, err := nodeidentity.Open(filepath.Join(root, "node"))
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := nodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	identityBefore, initialized, err := identity.Snapshot()
	if err != nil || !initialized {
		t.Fatalf("identity snapshot: initialized=%v err=%v", initialized, err)
	}
	proof, err := nodeStore.BeginEnrollmentProof(identityID, identityBefore.HeadHash, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	delegate, delegateSignature := signedTestNodeDelegation(t, rootKey, proof, "1", time.Now().UTC())
	if err := identity.ApplyNodeEvent(delegate, delegateSignature); err != nil {
		t.Fatalf("delegate local Node: %v", err)
	}

	now := time.Now().UTC()
	identityState, err := authority.IdentityStateFromStore(identity, now)
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	subject, err := authority.NewPersonOwnerSubject(identityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x72}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	createSubject, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, identityState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(createSubject, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	resolve := resolverForGlobalIdentity(identity)
	if err := ownerStore.Apply(createSigned, resolve, now); err != nil {
		t.Fatalf("create subject: %v", err)
	}
	rootKeyID, err := identityprotocol.KeyID(rootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	nonce := sha256.Sum256([]byte("peer archive head test nonce"))
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID},
		ArchiveID: archiveID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce[:]),
		PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: rootKeyID,
	}
	signedGrant, err := authority.SignGrant(grant, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	head, err := ownerStore.Snapshot(subject.ID, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	issueGrant, err := authority.NewAuthorityEvent(subject.ID, "2", head.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{signedGrant}}, identityState, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := authority.SignAuthorityEvent(issueGrant, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(issueSigned, resolve, now); err != nil {
		t.Fatalf("issue archive capability: %v", err)
	}
	grantHash, err := authority.GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}

	archiveSession, err := archiveindex.NewSessionIdentity("remote-recording", "hls", nil, "session")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("remote-recording", archiveSession)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ArchiveID = archiveID
	expectedSnapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	remoteNodeID := "nd_BBBBBBBBBBBBBBBBBBBBBB"
	peerManager := &peerArchiveSessionFixture{
		peerSessionManagerFixture: peerSessionManagerFixture{sessions: []peerapi.SessionInfo{{IdentityID: identityID, NodeID: remoteNodeID}}},
		snapshot:                  expectedSnapshot,
	}
	handler := NewWithOptions(nil, nil, nil, Options{
		Auth: users, GlobalIdentity: identity, OwnerAuthority: ownerStore,
		NodeIdentity: nodeStore, PeerSessions: peerManager,
	})
	capabilityList := httptest.NewRecorder()
	handler.ServeHTTP(capabilityList, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/peer-archives/capabilities", "", login))
	if capabilityList.Code != http.StatusOK || !strings.Contains(capabilityList.Body.String(), grantHash) || !strings.Contains(capabilityList.Body.String(), archiveID) || !strings.Contains(capabilityList.Body.String(), localNode.NodeID) {
		t.Fatalf("recording reader capability list status=%d body=%s", capabilityList.Code, capabilityList.Body.String())
	}
	path := "/api/peer-archives/" + remoteNodeID + "/head?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+path, "", login))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), expectedSnapshot.SnapshotID) || peerManager.called != 1 {
		t.Fatalf("peer archive head status=%d calls=%d body=%s", response.Code, peerManager.called, response.Body.String())
	}
	if peerManager.scope.IdentityID != identityID || peerManager.scope.NodeID != localNode.NodeID ||
		peerManager.scope.OwnerSubjectID != subject.ID || peerManager.scope.ArchiveID != archiveID ||
		peerManager.scope.SnapshotID != "" || peerManager.scope.Action != "archive.view" || len(peerManager.scope.GrantChain) != 1 {
		t.Fatalf("peer request scope did not preserve the committed archive capability: %+v", peerManager.scope)
	}
	previousPath := "/api/peer-archives/" + remoteNodeID + "/snapshots/" + expectedSnapshot.SnapshotID + "?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash
	previousResponse := httptest.NewRecorder()
	handler.ServeHTTP(previousResponse, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+previousPath, "", login))
	if previousResponse.Code != http.StatusOK || !strings.Contains(previousResponse.Body.String(), expectedSnapshot.SnapshotID) || peerManager.called != 2 {
		t.Fatalf("peer snapshot history item status=%d calls=%d body=%s", previousResponse.Code, peerManager.called, previousResponse.Body.String())
	}
	if peerManager.scope.SnapshotID != expectedSnapshot.SnapshotID || peerManager.scope.Action != "archive.view" {
		t.Fatalf("historical snapshot request was not bound to the selected immutable snapshot: %+v", peerManager.scope)
	}

	for _, attempt := range []struct {
		hash string
		code int
	}{{"sha256:" + strings.Repeat("0", 64), http.StatusForbidden}, {"not-a-hash", http.StatusBadRequest}} {
		denied := httptest.NewRecorder()
		invalidPath := "/api/peer-archives/" + remoteNodeID + "/head?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + attempt.hash
		handler.ServeHTTP(denied, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+invalidPath, "", login))
		if denied.Code != attempt.code || peerManager.called != 2 {
			t.Fatalf("uncommitted capability hash accepted: status=%d calls=%d body=%s", denied.Code, peerManager.called, denied.Body.String())
		}
	}

	peerManager.manifest = replication.ReplicaManifest{
		Version: replication.ReplicaManifestVersion, ArchiveID: archiveID, SnapshotID: expectedSnapshot.SnapshotID, KeyEpoch: "1",
	}
	manifestPath := "/api/peer-archives/" + remoteNodeID + "/snapshots/" + expectedSnapshot.SnapshotID + "/manifest?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash + "&key_epoch=1"
	manifestResponse := httptest.NewRecorder()
	handler.ServeHTTP(manifestResponse, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+manifestPath, "", login))
	if manifestResponse.Code != http.StatusOK || !strings.Contains(manifestResponse.Body.String(), expectedSnapshot.SnapshotID) || peerManager.called != 4 {
		t.Fatalf("peer manifest status=%d calls=%d body=%s", manifestResponse.Code, peerManager.called, manifestResponse.Body.String())
	}
	if peerManager.scope.SnapshotID != expectedSnapshot.SnapshotID || peerManager.scope.Action != "archive.view" {
		t.Fatalf("manifest scope widened or lost snapshot binding: %+v", peerManager.scope)
	}
	peerManager.epochs = []uint64{3, 1}
	epochsPath := "/api/peer-archives/" + remoteNodeID + "/snapshots/" + expectedSnapshot.SnapshotID + "/key-epochs?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash
	epochsResponse := httptest.NewRecorder()
	handler.ServeHTTP(epochsResponse, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+epochsPath, "", login))
	if epochsResponse.Code != http.StatusOK || !strings.Contains(epochsResponse.Body.String(), `"key_epochs":["3","1"]`) || strings.Contains(epochsResponse.Body.String(), "recovery_encryption_public_key") {
		t.Fatalf("peer key epoch discovery status=%d body=%s", epochsResponse.Code, epochsResponse.Body.String())
	}
	if peerManager.scope.SnapshotID != expectedSnapshot.SnapshotID || peerManager.scope.Action != "archive.view" {
		t.Fatalf("key epoch discovery scope widened or lost snapshot binding: %+v", peerManager.scope)
	}

	localRecoveryPublic, err := base64.RawURLEncoding.DecodeString(identityBefore.RecoveryEncryptionPublicKey)
	if err != nil || len(localRecoveryPublic) != 32 {
		t.Fatalf("decode local recovery public key: bytes=%d err=%v", len(localRecoveryPublic), err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	ownerKeyID, err := identityprotocol.KeyID(rootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	peerManager.envelope, err = cryptoarchive.WrapDEK(dek, archiveID, 1, localRecoveryPublic, ownerKeyID, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	envelopePath := "/api/peer-archives/" + remoteNodeID + "/snapshots/" + expectedSnapshot.SnapshotID + "/key-envelope?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash + "&key_epoch=1"
	envelopeResponse := httptest.NewRecorder()
	handler.ServeHTTP(envelopeResponse, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+envelopePath, "", login))
	if envelopeResponse.Code != http.StatusOK || !strings.Contains(envelopeResponse.Body.String(), peerManager.envelope.RecipientKeyID) || strings.Contains(envelopeResponse.Body.String(), "private_key") {
		t.Fatalf("peer key envelope status=%d body=%s", envelopeResponse.Code, envelopeResponse.Body.String())
	}

	peerManager.object = []byte("ciphertext")
	objectID := "ob_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x21}, 32))
	objectPath := "/api/peer-archives/" + remoteNodeID + "/snapshots/" + expectedSnapshot.SnapshotID + "/objects/" + objectID + "?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash + "&key_epoch=1&offset=2&length=4"
	objectResponse := httptest.NewRecorder()
	handler.ServeHTTP(objectResponse, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+objectPath, "", login))
	if objectResponse.Code != http.StatusOK || objectResponse.Body.String() != "pher" || objectResponse.Header().Get("Cache-Control") != "private, no-store" || objectResponse.Header().Get("Content-Length") != "4" {
		t.Fatalf("peer ciphertext range status=%d headers=%v body=%q", objectResponse.Code, objectResponse.Header(), objectResponse.Body.String())
	}
	callsBeforeInvalidRange := peerManager.called
	for _, invalidQuery := range []string{
		"owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash + "&key_epoch=1&offset=0&length=" + strconv.FormatInt(replication.MaxCiphertextChunkBytes+1, 10),
		"owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash + "&key_epoch=1&key_epoch=2&offset=0&length=1",
	} {
		invalid := httptest.NewRecorder()
		invalidPath := "/api/peer-archives/" + remoteNodeID + "/snapshots/" + expectedSnapshot.SnapshotID + "/objects/" + objectID + "?" + invalidQuery
		handler.ServeHTTP(invalid, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+invalidPath, "", login))
		if invalid.Code != http.StatusBadRequest || peerManager.called != callsBeforeInvalidRange {
			t.Fatalf("invalid ciphertext query reached peer: status=%d calls=%d body=%s", invalid.Code, peerManager.called, invalid.Body.String())
		}
	}
	invalidEnvelope := httptest.NewRecorder()
	invalidEnvelopePath := envelopePath + "&recipient_key_id=" + peerManager.envelope.RecipientKeyID
	handler.ServeHTTP(invalidEnvelope, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+invalidEnvelopePath, "", login))
	if invalidEnvelope.Code != http.StatusBadRequest || peerManager.called != callsBeforeInvalidRange {
		t.Fatalf("client-selected envelope recipient accepted: status=%d calls=%d body=%s", invalidEnvelope.Code, peerManager.called, invalidEnvelope.Body.String())
	}
}

func TestPeerArchiveHeadAllowsForeignOwnerOnlyForMatchingAuthenticatedIdentity(t *testing.T) {
	root := t.TempDir()
	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("archive-reader", "remote-archive-reader-password"); err != nil {
		t.Fatal(err)
	}
	login, err := users.LoginAs("archive-reader", "remote-archive-reader-password")
	if err != nil {
		t.Fatal(err)
	}

	openIdentityWithNode := func(name string) (*globalidentity.Store, *nodeidentity.Store, ed25519.PrivateKey, string, string) {
		t.Helper()
		identity, openErr := globalidentity.Open(filepath.Join(root, name+"-identity"))
		if openErr != nil {
			t.Fatal(openErr)
		}
		rootKey, genesis, signature, handle, credential, challenge, identityID := signedTestGenesis(t)
		if createErr := identity.CreateGenesis(genesis, signature, handle, credential, challenge); createErr != nil {
			t.Fatal(createErr)
		}
		nodes, openErr := nodeidentity.Open(filepath.Join(root, name+"-node"))
		if openErr != nil {
			t.Fatal(openErr)
		}
		node, createErr := nodes.Create()
		if createErr != nil {
			t.Fatal(createErr)
		}
		before, initialized, snapshotErr := identity.Snapshot()
		if snapshotErr != nil || !initialized {
			t.Fatalf("%s identity snapshot: initialized=%v err=%v", name, initialized, snapshotErr)
		}
		proof, proofErr := nodes.BeginEnrollmentProof(identityID, before.HeadHash, time.Now().UTC())
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		delegate, delegateSignature := signedTestNodeDelegation(t, rootKey, proof, "1", time.Now().UTC())
		if applyErr := identity.ApplyNodeEvent(delegate, delegateSignature); applyErr != nil {
			t.Fatalf("delegate %s Node: %v", name, applyErr)
		}
		return identity, nodes, rootKey, identityID, node.NodeID
	}

	localIdentity, localNodeStore, _, localIdentityID, localNodeID := openIdentityWithNode("local")
	remoteIdentity, _, remoteRootKey, remoteIdentityID, remoteNodeID := openIdentityWithNode("remote")
	registry, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "registry"))
	if err != nil {
		t.Fatal(err)
	}
	remoteHistory, err := remoteIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(remoteHistory); err != nil {
		t.Fatalf("import verified remote identity history: %v", err)
	}
	remoteDirectoryA, err := discovery.OpenDirectory(filepath.Join(root, "directory-a"))
	if err != nil {
		t.Fatal(err)
	}
	remoteDirectoryB, err := discovery.OpenDirectory(filepath.Join(root, "directory-b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := remoteDirectoryA.PublishIdentityHistory(remoteHistory); err != nil {
		t.Fatal(err)
	}
	if err := remoteDirectoryB.PublishIdentityHistory(remoteHistory); err != nil {
		t.Fatal(err)
	}
	federation, err := discovery.NewFederationClient([]string{"https://authority-one.example", "https://authority-two.example"}, &http.Client{
		Transport: peerArchiveHistoryTransport{
			"authority-one.example": discovery.HTTPHandler(remoteDirectoryA),
			"authority-two.example": discovery.HTTPHandler(remoteDirectoryB),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolve := authority.IdentityStateResolverFromStores(map[string]*globalidentity.Store{
		localIdentityID: localIdentity, remoteIdentityID: remoteIdentity,
	})
	now := time.Now().UTC().Truncate(time.Second)
	remoteState, err := resolve(remoteIdentityID, "", now)
	if err != nil || remoteState.Frozen || !remoteState.FreshUntil.After(now) {
		t.Fatalf("resolve fresh remote controller state: state=%+v err=%v", remoteState, err)
	}
	ownerStore, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	subject, err := authority.NewPersonOwnerSubject(remoteIdentityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x4a}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, remoteState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(create, remoteRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(createSigned, resolve, now); err != nil {
		t.Fatalf("create foreign OwnerSubject: %v", err)
	}
	keyID, err := identityprotocol.KeyID(remoteRootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:   authority.Principal{Kind: authority.PrincipalIdentity, ID: remoteIdentityID},
		Audience: authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}, ArchiveID: archiveID,
		Actions: []string{"archive.view"}, NotBefore: now.Add(-time.Minute).Format(time.RFC3339),
		ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x6b}, 32)),
		PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: keyID,
	}
	signedGrant, err := authority.SignGrant(grant, remoteRootKey)
	if err != nil {
		t.Fatal(err)
	}
	grantHash, err := authority.GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}
	ownerSnapshot, err := ownerStore.Snapshot(subject.ID, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := authority.NewAuthorityEvent(subject.ID, "2", ownerSnapshot.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{signedGrant}}, remoteState, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := authority.SignAuthorityEvent(issue, remoteRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(issueSigned, resolve, now); err != nil {
		t.Fatalf("issue foreign archive capability: %v", err)
	}

	archiveSession, err := archiveindex.NewSessionIdentity("foreign-recording", "hls", nil, "session")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("foreign-recording", archiveSession)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ArchiveID = archiveID
	expectedSnapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	peerManager := &peerArchiveSessionFixture{
		peerSessionManagerFixture: peerSessionManagerFixture{sessions: []peerapi.SessionInfo{{IdentityID: remoteIdentityID, NodeID: remoteNodeID}}},
		snapshot:                  expectedSnapshot,
	}
	handler := NewWithOptions(nil, nil, nil, Options{
		Auth: users, GlobalIdentity: localIdentity, GlobalIdentityRegistry: registry, IdentityFederation: federation,
		OwnerAuthority: ownerStore, NodeIdentity: localNodeStore, PeerSessions: peerManager,
	})
	capabilities := httptest.NewRecorder()
	handler.ServeHTTP(capabilities, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/peer-archives/capabilities", "", login))
	if capabilities.Code != http.StatusOK || !strings.Contains(capabilities.Body.String(), grantHash) || !strings.Contains(capabilities.Body.String(), remoteIdentityID) {
		t.Fatalf("foreign capability projection status=%d body=%s", capabilities.Code, capabilities.Body.String())
	}
	path := "/api/peer-archives/" + remoteNodeID + "/head?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID + "&grant_hash=" + grantHash
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+path, "", login))
	if response.Code != http.StatusOK || peerManager.called != 1 || !strings.Contains(response.Body.String(), expectedSnapshot.SnapshotID) {
		t.Fatalf("foreign OwnerSubject peer read status=%d calls=%d body=%s", response.Code, peerManager.called, response.Body.String())
	}
	if peerManager.scope.IdentityID != localIdentityID || peerManager.scope.NodeID != localNodeID ||
		peerManager.scope.TargetIdentityID != remoteIdentityID || peerManager.scope.TargetNodeID != remoteNodeID ||
		peerManager.scope.OwnerSubjectID != subject.ID || peerManager.scope.Action != "archive.view" {
		t.Fatalf("cross-identity peer command scope is incorrect: %+v", peerManager.scope)
	}
	peerManager.sessions[0].IdentityID = localIdentityID
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com"+path, "", login))
	if denied.Code != http.StatusConflict || peerManager.called != 1 {
		t.Fatalf("foreign OwnerSubject was sent to a peer claiming another identity: status=%d calls=%d body=%s", denied.Code, peerManager.called, denied.Body.String())
	}
}
