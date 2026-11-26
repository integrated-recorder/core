package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/storage"
)

type archiveRekeyCaptureManager struct {
	recordingManager
	request recorderengine.RekeyArchiveHistoryRequest
	calls   int
}

// This route-level manager double captures the public HTTP-to-Engine request
// boundary. It returns a successful Engine result without performing archive
// re-encryption; that cryptographic behavior is covered by recorderengine tests.
func (m *archiveRekeyCaptureManager) RekeyArchiveHistory(_ context.Context, request recorderengine.RekeyArchiveHistoryRequest) (recorderengine.RekeyArchiveHistoryResult, error) {
	m.calls++
	m.request = request
	return recorderengine.RekeyArchiveHistoryResult{}, nil
}

type countingHistoryFederationTransport struct {
	mu       sync.Mutex
	handlers map[string]http.Handler
	calls    map[string]int
}

func (transport *countingHistoryFederationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	handler := transport.handlers[request.URL.Hostname()]
	if handler == nil {
		return nil, errors.New("test discovery provider unavailable")
	}
	transport.mu.Lock()
	transport.calls[request.URL.Hostname()]++
	transport.mu.Unlock()
	copy := request.Clone(request.Context())
	copy.RemoteAddr = "192.0.2.73:443"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, copy)
	response := recorder.Result()
	response.Request = request
	return response, nil
}

func (transport *countingHistoryFederationTransport) callsFor(host string) int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.calls[host]
}

func TestArchiveReplicaRekeyResolvesFederatedRecipientAndAuditsSuccess(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(filepath.Join(root, "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	writeProductRecording(t, store, recordingID, domain.StateStopped)
	baseManager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer baseManager.Close(context.Background())
	if err := baseManager.SealArchiveContext(context.Background(), recordingID); err != nil {
		t.Fatalf("seal real archive fixture: %v", err)
	}
	catalog, err := archivecatalog.Open(filepath.Join(root, "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := baseManager.PersistArchiveSnapshot(context.Background(), catalog, recordingID)
	if err != nil || snapshot.SnapshotID == "" || snapshot.Content.Archive == "" {
		t.Fatalf("persist sealed snapshot: snapshot=%+v err=%v", snapshot, err)
	}
	sealed, err := baseManager.Get(recordingID)
	if err != nil || !sealed.ArchiveSealed {
		t.Fatalf("archive fixture is not sealed: recording=%+v err=%v", sealed, err)
	}

	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("rekey-owner", "rekey-owner-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := users.LoginAs("rekey-owner", "rekey-owner-test-password")
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(filepath.Join(root, "management"))
	if err != nil {
		t.Fatal(err)
	}

	localIdentity, err := globalidentity.Open(filepath.Join(root, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	localRootKey, localGenesis, localSignature, localHandle, localCredential, localChallenge, localIdentityID := signedTestGenesis(t)
	if err := localIdentity.CreateGenesis(localGenesis, localSignature, localHandle, localCredential, localChallenge); err != nil {
		t.Fatal(err)
	}
	localNodeStore, err := nodeidentity.Open(filepath.Join(root, "node"))
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := localNodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	localBefore, initialized, err := localIdentity.Snapshot()
	if err != nil || !initialized {
		t.Fatalf("local identity snapshot: initialized=%v err=%v", initialized, err)
	}
	proof, err := localNodeStore.BeginEnrollmentProof(localIdentityID, localBefore.HeadHash, now)
	if err != nil {
		t.Fatal(err)
	}
	delegation, delegationSignature := signedTestNodeDelegation(t, localRootKey, proof, "1", now)
	if err := localIdentity.ApplyNodeEvent(delegation, delegationSignature); err != nil {
		t.Fatalf("delegate local Node: %v", err)
	}

	_, recipientGenesis, recipientSignature, recipientHandle, recipientCredential, recipientChallenge, recipientIdentityID := signedTestGenesis(t)
	if recipientIdentityID == localIdentityID {
		t.Fatal("generated recipient identity unexpectedly equals local identity")
	}
	recipientIdentity, err := globalidentity.Open(filepath.Join(root, "recipient-identity"))
	if err != nil {
		t.Fatal(err)
	}
	if err := recipientIdentity.CreateGenesis(recipientGenesis, recipientSignature, recipientHandle, recipientCredential, recipientChallenge); err != nil {
		t.Fatal(err)
	}
	recipientHistory, err := recipientIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	firstDirectory, err := discovery.OpenDirectory(filepath.Join(root, "discovery-a"))
	if err != nil {
		t.Fatal(err)
	}
	secondDirectory, err := discovery.OpenDirectory(filepath.Join(root, "discovery-b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := firstDirectory.PublishIdentityHistory(recipientHistory); err != nil {
		t.Fatal(err)
	}
	if err := secondDirectory.PublishIdentityHistory(recipientHistory); err != nil {
		t.Fatal(err)
	}
	transport := &countingHistoryFederationTransport{
		handlers: map[string]http.Handler{
			"identity-a.example": discovery.HTTPHandler(firstDirectory),
			"identity-b.example": discovery.HTTPHandler(secondDirectory),
		},
		calls: map[string]int{},
	}
	federation, err := discovery.NewFederationClient([]string{"https://identity-a.example", "https://identity-b.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "identity-registry"))
	if err != nil {
		t.Fatal(err)
	}
	ownerAuthority, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	localState, err := authority.IdentityStateFromStore(localIdentity, now)
	if err != nil {
		t.Fatal(err)
	}
	resolveLocal := authority.IdentityStateResolverFromStores(map[string]*globalidentity.Store{localIdentityID: localIdentity})
	archiveID := snapshot.Content.ArchiveID
	subject, err := authority.NewPersonOwnerSubject(localIdentityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x39}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, localState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(create, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerAuthority.Apply(createSigned, resolveLocal, now); err != nil {
		t.Fatalf("claim sealed archive: %v", err)
	}
	keyID, err := identityprotocol.KeyID(localRootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	nonce := sha256.Sum256([]byte("positive replica rekey route test"))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: localIdentityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshot.SnapshotID,
		Actions:   []string{"archive.replicate", "key.rotate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce[:]),
		PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: keyID,
	}, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	ownerSnapshot, err := ownerAuthority.Snapshot(subject.ID, resolveLocal, now)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := authority.NewAuthorityEvent(subject.ID, "2", ownerSnapshot.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{grant}}, localState, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := authority.SignAuthorityEvent(issue, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerAuthority.Apply(issueSigned, resolveLocal, now); err != nil {
		t.Fatalf("issue rotation and replication capability: %v", err)
	}
	grantHash, err := authority.GrantHash(grant.Grant)
	if err != nil {
		t.Fatal(err)
	}
	recipientFixtureState, err := authority.IdentityStateFromStore(recipientIdentity, now)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	envelope, err := cryptoarchive.WrapDEK(dek, archiveID, 2, recipientFixtureState.RecoveryEncryptionPublicKey, keyID, localRootKey)
	if err != nil {
		t.Fatalf("construct recipient-bound envelope fixture: %v", err)
	}

	manager := &archiveRekeyCaptureManager{recordingManager: baseManager}
	handler := NewWithOptions(manager, nil, nil, Options{
		Auth: users, Management: products, GlobalIdentity: localIdentity,
		GlobalIdentityRegistry: registry, IdentityFederation: federation,
		OwnerAuthority: ownerAuthority, ArchiveCatalog: catalog, NodeIdentity: localNodeStore,
	})
	body, err := json.Marshal(archiveReplicaRekeyRequest{
		OwnerSubjectID: subject.ID, GrantHash: grantHash, RecipientIdentityID: recipientIdentityID,
		PreviousKeyEpoch: 1, KeyEpoch: 2, KeyEnvelope: envelope,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodPost,
		"https://recorder.example.com/api/recordings/"+recordingID+"/replicas/rekey", string(body), session))
	if response.Code != http.StatusOK {
		t.Fatalf("rekey route status=%d body=%s", response.Code, response.Body.String())
	}
	if manager.calls != 1 {
		t.Fatalf("manager calls=%d, want exactly one", manager.calls)
	}
	if manager.request.RecordingID != recordingID || manager.request.SnapshotID != snapshot.SnapshotID ||
		manager.request.SubjectID != subject.ID || manager.request.KeyEpoch != 2 || manager.request.PreviousKeyEpoch != 1 ||
		!manager.request.UseStoredKey || manager.request.Recipient.IdentityID != recipientIdentityID ||
		manager.request.KeyEnvelope.ArchiveID != archiveID || manager.request.KeyEnvelope.KeyEpoch != "2" {
		t.Fatalf("manager received incomplete rekey request: %+v", manager.request)
	}
	gotRecipient := manager.request.Recipient
	wantRecipient, err := authority.NewFederatedIdentityResolver(registry, federation).Resolve(recipientIdentityID, "", gotRecipient.VerifiedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotRecipient, wantRecipient) || gotRecipient.IdentityID != recipientIdentityID || gotRecipient.Frozen ||
		!bytes.Equal(gotRecipient.RecoveryEncryptionPublicKey, wantRecipient.RecoveryEncryptionPublicKey) ||
		!gotRecipient.FreshUntil.After(gotRecipient.VerifiedAt) || time.Since(gotRecipient.VerifiedAt) > time.Minute {
		t.Fatalf("manager recipient was not the current verified federated identity state: got=%+v want=%+v", gotRecipient, wantRecipient)
	}
	if manager.request.KeyEnvelope.RecipientKeyID != cryptoarchive.RecipientKeyID(gotRecipient.RecoveryEncryptionPublicKey) ||
		manager.request.KeyEnvelope.RecipientPublicKeySHA256 != cryptoarchive.RecipientPublicKeyHash(gotRecipient.RecoveryEncryptionPublicKey) {
		t.Fatalf("manager envelope is not bound to the resolved federated recipient key: envelope=%+v recipient_identity=%s", manager.request.KeyEnvelope, gotRecipient.IdentityID)
	}
	if transport.callsFor("identity-a.example") == 0 || transport.callsFor("identity-b.example") == 0 {
		t.Fatalf("recipient was not resolved through all configured federation mirrors: calls=%d,%d", transport.callsFor("identity-a.example"), transport.callsFor("identity-b.example"))
	}
	if !strings.Contains(response.Body.String(), `"verification"`) {
		t.Fatalf("unexpected rekey response body: %s", response.Body.String())
	}
	audit := products.Audit(10)
	if len(audit) != 1 || audit[0].Type != "archive_history_rekey_prepared" || audit[0].ObjectID != archiveID {
		t.Fatalf("successful rekey audit=%+v", audit)
	}
}
