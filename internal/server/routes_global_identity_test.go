package server

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/nodeidentity"
)

func TestNodeIdentityAPIIsAuthenticatedCSRFProtectedAndNeverExposesPrivateKey(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "node-identity-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "node-identity-test-password")
	if err != nil {
		t.Fatal(err)
	}
	store, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, NodeIdentity: store, Management: products})

	status := httptest.NewRecorder()
	handler.ServeHTTP(status, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/node-identity/status", "", session))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"created":false`) {
		t.Fatalf("initial status=%d body=%s", status.Code, status.Body.String())
	}

	noCSRF := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/create", "", session)
	noCSRF.Header.Del("X-CSRF-Token")
	noCSRFResponse := httptest.NewRecorder()
	handler.ServeHTTP(noCSRFResponse, noCSRF)
	if noCSRFResponse.Code != http.StatusForbidden {
		t.Fatalf("Node Key creation without CSRF status=%d body=%s", noCSRFResponse.Code, noCSRFResponse.Body.String())
	}

	created := httptest.NewRecorder()
	handler.ServeHTTP(created, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/create", "", session))
	if created.Code != http.StatusCreated || created.Header().Get("X-Audit-Status") != "recorded" || strings.Contains(created.Body.String(), "private") || !strings.Contains(created.Body.String(), `"node_id":"nd_`) {
		t.Fatalf("Node Key creation status=%d body=%s", created.Code, created.Body.String())
	}
	audit := products.Audit(10)
	if len(audit) != 1 || audit[0].Type != "node_identity_created" || audit[0].ObjectID == "" || audit[0].Actor == nil || audit[0].Actor.Type != management.AuditActorUser {
		t.Fatalf("Node Key creation audit=%+v", audit)
	}
	duplicate := httptest.NewRecorder()
	handler.ServeHTTP(duplicate, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/create", "", session))
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate Node Key create status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	statusAfter := httptest.NewRecorder()
	handler.ServeHTTP(statusAfter, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/node-identity/status", "", session))
	if statusAfter.Code != http.StatusOK || strings.Contains(statusAfter.Body.String(), "private") || !strings.Contains(statusAfter.Body.String(), `"created":true`) {
		t.Fatalf("created status=%d body=%s", statusAfter.Code, statusAfter.Body.String())
	}
}

func TestGlobalIdentityForkRoutesUseRBACAndCSRF(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "fork-route-owner-password"); err != nil {
		t.Fatal(err)
	}
	ownerSession, err := auth.LoginAs("local-admin", "fork-route-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := auth.CreateUser("fork-viewer", "fork-route-viewer-password", authn.RoleViewer)
	if err != nil {
		t.Fatal(err)
	}
	viewerSession, err := auth.LoginAs(viewer.Login, "fork-route-viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	store, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, genesis, signature, handle, credential, challenge, _ := signedTestGenesis(t)
	if err := store.CreateGenesis(genesis, signature, handle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: store, Management: products})

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/forks", nil))
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated fork list status=%d body=%s", unauthenticated.Code, unauthenticated.Body.String())
	}
	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/forks", "", viewerSession))
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"forks":[]`) {
		t.Fatalf("read-only fork evidence status=%d body=%s", listed.Code, listed.Body.String())
	}
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/forks/resolve", `{}`, viewerSession))
	if denied.Code != http.StatusForbidden {
		t.Fatalf("viewer fork resolution status=%d body=%s", denied.Code, denied.Body.String())
	}
	noCSRFRequest := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/forks/resolve", `{}`, ownerSession)
	noCSRFRequest.Header.Del("X-CSRF-Token")
	noCSRF := httptest.NewRecorder()
	handler.ServeHTTP(noCSRF, noCSRFRequest)
	if noCSRF.Code != http.StatusForbidden {
		t.Fatalf("fork resolution without CSRF status=%d body=%s", noCSRF.Code, noCSRF.Body.String())
	}
}

func TestGlobalIdentityForkResolutionRouteAppliesRootSignedChoice(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "fork-resolution-route-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "fork-resolution-route-password")
	if err != nil {
		t.Fatal(err)
	}
	store, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, signature, handle, credential, challenge, identityID := signedTestGenesis(t)
	if err := store.CreateGenesis(genesis, signature, handle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	firstEvent, firstSignature, firstNode := signedRouteNodeDelegation(t, rootKey, identityID, initial.HeadHash, 0x61)
	secondEvent, secondSignature, secondNode := signedRouteNodeDelegation(t, rootKey, identityID, initial.HeadHash, 0x62)
	if err := store.ApplyNodeEvent(firstEvent, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyNodeEvent(secondEvent, secondSignature); !errors.Is(err, globalidentity.ErrIdentityFork) {
		t.Fatalf("competing valid successor did not freeze identity: %v", err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: store, Management: products})

	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/forks", "", session))
	var response struct {
		Forks []globalidentity.ForkChoice `json:"forks"`
	}
	if listed.Code != http.StatusOK || json.Unmarshal(listed.Body.Bytes(), &response) != nil || len(response.Forks) != 1 {
		t.Fatalf("fork choices status=%d body=%s", listed.Code, listed.Body.String())
	}
	choice := response.Forks[0]
	firstSigned, err := identityprotocol.VerifyEvent(firstEvent, firstSignature)
	if err != nil {
		t.Fatal(err)
	}
	secondSigned, err := identityprotocol.VerifyEvent(secondEvent, secondSignature)
	if err != nil {
		t.Fatal(err)
	}
	selectedNode := ""
	selectedHead := choice.SecondHeadHash
	switch selectedHead {
	case firstSigned.EventHash:
		selectedNode = firstNode
	case secondSigned.EventHash:
		selectedNode = secondNode
	default:
		t.Fatalf("API returned heads outside retained signed branch evidence: %+v", choice)
	}
	if selectedHead == firstSigned.EventHash {
		selectedHead = choice.FirstHeadHash
	} else {
		selectedHead = choice.SecondHeadHash
	}
	canonical, resolutionSignature := signRouteIdentityEvent(t, rootKey, identityID, "2", selectedHead, "fork_resolve", map[string]any{
		"fork_id": choice.ForkID, "previous_event_hash": choice.PreviousHash,
		"first_head_hash": choice.FirstHeadHash, "second_head_hash": choice.SecondHeadHash,
		"selected_head_hash": selectedHead,
	})
	body, err := json.Marshal(identityEventEnvelope{
		CanonicalEvent: base64.RawURLEncoding.EncodeToString(canonical),
		Signature:      base64.RawURLEncoding.EncodeToString(resolutionSignature),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved := httptest.NewRecorder()
	handler.ServeHTTP(resolved, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/forks/resolve", string(body), session))
	if resolved.Code != http.StatusOK || resolved.Header().Get("X-Audit-Status") != "recorded" {
		t.Fatalf("root-signed route resolution status=%d body=%s", resolved.Code, resolved.Body.String())
	}
	snapshot, initialized, err := store.Snapshot()
	if err != nil || !initialized || snapshot.Frozen || snapshot.Sequence != "2" || len(snapshot.NodeIDs) != 1 || snapshot.NodeIDs[0] != selectedNode || snapshot.ForkCount != 1 {
		t.Fatalf("route did not apply the exact root-authorized branch: snapshot=%+v initialized=%v err=%v", snapshot, initialized, err)
	}
	choices, err := store.ForkChoices()
	if err != nil || len(choices) != 0 {
		t.Fatalf("resolved branch remains actionable: choices=%+v err=%v", choices, err)
	}
	audit := products.Audit(10)
	if len(audit) != 1 || audit[0].Type != "global_identity_fork_resolved" || audit[0].ObjectID != choice.ForkID {
		t.Fatalf("fork resolution was not safely audited: %+v", audit)
	}
}

func signedRouteNodeDelegation(t *testing.T, rootKey ed25519.PrivateKey, identityID, parentHash string, seed byte) ([]byte, []byte, string) {
	t.Helper()
	seedBytes := make([]byte, ed25519.SeedSize)
	for i := range seedBytes {
		seedBytes[i] = seed
	}
	nodeKey := ed25519.NewKeyFromSeed(seedBytes)
	nodePublic := nodeKey.Public().(ed25519.PublicKey)
	nodeID, err := identityprotocol.NodeID(nodePublic)
	if err != nil {
		t.Fatal(err)
	}
	publicText := base64.RawURLEncoding.EncodeToString(nodePublic)
	nonceBytes := make([]byte, 32)
	for i := range nonceBytes {
		nonceBytes[i] = seed + byte(i)
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	proofMessage, err := identityprotocol.NodeEnrollmentProofMessage(identityID, parentHash, nonce)
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"node_id": nodeID, "node_public_key": publicText, "enrollment_nonce": nonce,
		"node_proof": base64.RawURLEncoding.EncodeToString(ed25519.Sign(nodeKey, proofMessage)),
	}
	canonical, signature := signRouteIdentityEvent(t, rootKey, identityID, "1", parentHash, "node_delegate", payload)
	return canonical, signature, nodeID
}

func signRouteIdentityEvent(t *testing.T, privateKey ed25519.PrivateKey, identityID, sequence, previousHash, operation string, payload map[string]any) ([]byte, []byte) {
	t.Helper()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID,
		"sequence": sequence, "previous_event_hash": previousHash, "operation": operation,
		"issued_at":     time.Now().UTC().Truncate(time.Second).Format(time.RFC3339),
		"signer_key_id": keyID, "signer_public_key": base64.RawURLEncoding.EncodeToString(publicKey), "payload": payload,
	}
	canonicalInput, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(canonicalInput, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func TestRemoteIdentityRegistryAPIRequiresRBACAndStoresOnlyVerifiedPublicHistory(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "identity-registry-test-password"); err != nil {
		t.Fatal(err)
	}
	ownerSession, err := auth.LoginAs("local-admin", "identity-registry-test-password")
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := auth.CreateUser("registry-viewer", "registry-viewer-password", authn.RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	viewerSession, err := auth.LoginAs(viewer.Login, "registry-viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	local, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, localGenesis, localSignature, localHandle, localCredential, localChallenge, _ := signedTestGenesis(t)
	if err := local.CreateGenesis(localGenesis, localSignature, localHandle, localCredential, localChallenge); err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: local, GlobalIdentityRegistry: registry, Management: products})

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/registry", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated list status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/registry", "", viewerSession))
	if denied.Code != http.StatusForbidden {
		t.Fatalf("viewer registry list status=%d body=%s", denied.Code, denied.Body.String())
	}

	remote, err := globalidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, remoteGenesis, remoteSignature, remoteHandle, remoteCredential, remoteChallenge, remoteID := signedTestGenesis(t)
	if err := remote.CreateGenesis(remoteGenesis, remoteSignature, remoteHandle, remoteCredential, remoteChallenge); err != nil {
		t.Fatal(err)
	}
	bundle, err := remote.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	imported := httptest.NewRecorder()
	handler.ServeHTTP(imported, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/registry/import", string(body), ownerSession))
	if imported.Code != http.StatusOK || !strings.Contains(imported.Body.String(), remoteID) || imported.Header().Get("X-Audit-Status") != "recorded" {
		t.Fatalf("remote history import status=%d body=%s", imported.Code, imported.Body.String())
	}
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/registry", "", ownerSession))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), remoteID) || strings.Contains(list.Body.String(), "private_key") || strings.Contains(list.Body.String(), "test-public-key") {
		t.Fatalf("remote history list status=%d body=%s", list.Code, list.Body.String())
	}
	if audit := products.Audit(10); len(audit) != 1 || audit[0].Type != "remote_identity_history_imported" || audit[0].ObjectID != remoteID {
		t.Fatalf("remote import audit=%+v", audit)
	}

	badBundle := bundle
	badBundle.Events = append([]globalidentity.HistoryEvent(nil), bundle.Events...)
	badSignature, err := base64.RawURLEncoding.DecodeString(badBundle.Events[0].Signature)
	if err != nil {
		t.Fatal(err)
	}
	badSignature[0] ^= 1
	badBundle.Events[0].Signature = base64.RawURLEncoding.EncodeToString(badSignature)
	badBody, err := json.Marshal(badBundle)
	if err != nil {
		t.Fatal(err)
	}
	badImport := httptest.NewRecorder()
	handler.ServeHTTP(badImport, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/registry/import", string(badBody), ownerSession))
	if badImport.Code != http.StatusBadRequest {
		t.Fatalf("invalid signature import status=%d body=%s", badImport.Code, badImport.Body.String())
	}
}

func TestGlobalIdentityHistoryPublishRequiresConfiguredFederationAndPublishesSignedHistory(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "identity-publish-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "identity-publish-test-password")
	if err != nil {
		t.Fatal(err)
	}
	local, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, canonical, signature, handle, credential, challenge, identityID := signedTestGenesis(t)
	if err := local.CreateGenesis(canonical, signature, handle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	noFederation := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: local, GlobalIdentityRegistry: registry, Management: products})
	missing := httptest.NewRecorder()
	noFederation.ServeHTTP(missing, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/history/publish", "", session))
	if missing.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured history federation status=%d body=%s", missing.Code, missing.Body.String())
	}
	first, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "second"))
	if err != nil {
		t.Fatal(err)
	}
	transport := &serverHistoryFederationTransport{handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)}}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: local, GlobalIdentityRegistry: registry, IdentityFederation: federation, Management: products})
	csrf := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/history/publish", "", session)
	csrf.Header.Del("X-CSRF-Token")
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, csrf)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("history publish without CSRF status=%d body=%s", denied.Code, denied.Body.String())
	}
	published := httptest.NewRecorder()
	handler.ServeHTTP(published, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/history/publish", "", session))
	if published.Code != http.StatusOK || !strings.Contains(published.Body.String(), identityID) {
		t.Fatalf("history publish status=%d body=%s", published.Code, published.Body.String())
	}
	for _, directory := range []*discovery.Directory{first, second} {
		bundle, found, err := directory.LookupIdentityHistory(identityID)
		if err != nil || !found || bundle.IdentityID != identityID {
			t.Fatalf("published identity history missing: found=%v err=%v", found, err)
		}
	}
	verified := httptest.NewRecorder()
	handler.ServeHTTP(verified, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/verified/"+identityID, "", session))
	if verified.Code != http.StatusOK || !strings.Contains(verified.Body.String(), `"identity_signing_public_key":"`+base64.RawURLEncoding.EncodeToString(rootKey.Public().(ed25519.PublicKey))+`"`) ||
		strings.Contains(verified.Body.String(), "private_key") || strings.Contains(verified.Body.String(), "signing_seed") || strings.Contains(verified.Body.String(), "encryption_private") {
		t.Fatalf("verified identity key projection status=%d body=%s", verified.Code, verified.Body.String())
	}

	// Verify the bounded automatic mirror helper reports complete publication
	// and a partial-provider failure without exposing raw transport details.
	synced := httptest.NewRecorder()
	handler.publishCurrentIdentityHistory(synced)
	if synced.Header().Get("X-IR-Identity-History-Sync") != "published" {
		t.Fatalf("automatic identity history synchronization status=%q", synced.Header().Get("X-IR-Identity-History-Sync"))
	}
	partialFederation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: &serverHistoryFederationTransport{handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first)}}})
	if err != nil {
		t.Fatal(err)
	}
	pendingServer := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: local, IdentityFederation: partialFederation, Management: products})
	pending := httptest.NewRecorder()
	pendingServer.publishCurrentIdentityHistory(pending)
	if pending.Header().Get("X-IR-Identity-History-Sync") != "pending" || strings.Contains(pending.Body.String(), "unavailable") {
		t.Fatalf("partial publication was not safely reported as pending: header=%q body=%s", pending.Header().Get("X-IR-Identity-History-Sync"), pending.Body.String())
	}
}

func TestIdentityMutationRepublishesCurrentHistoryToEveryConfiguredProvider(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "identity-sync-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "identity-sync-test-password")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	node, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := node.Create()
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, genesisSignature, userHandle, credential, challenge, identityID := signedTestGenesis(t)
	if err := identity.CreateGenesis(genesis, genesisSignature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	identitySnapshot, _, err := identity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := node.BeginEnrollmentProof(identityID, identitySnapshot.HeadHash, now)
	if err != nil {
		t.Fatal(err)
	}
	delegation, delegationSignature := signedTestNodeDelegation(t, rootKey, proof, "1", now)
	if err := node.ConsumeEnrollmentEvent(delegation, delegationSignature, now); err != nil {
		t.Fatal(err)
	}
	if err := identity.ApplyNodeEvent(delegation, delegationSignature); err != nil {
		t.Fatal(err)
	}
	first, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "second"))
	if err != nil {
		t.Fatal(err)
	}
	transport := &serverHistoryFederationTransport{handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)}}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := identity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := federation.PublishIdentityHistory(t.Context(), bundle, now); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: identity, IdentityFederation: federation, Management: products})
	identitySnapshot, _, err = identity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	revoke, revokeSignature := signedTestNodeRevocation(t, rootKey, identityID, "2", identitySnapshot.HeadHash, localNode.NodeID, now.Add(time.Second))
	body, err := json.Marshal(identityEventEnvelope{CanonicalEvent: base64.RawURLEncoding.EncodeToString(revoke), Signature: base64.RawURLEncoding.EncodeToString(revokeSignature)})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	request := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/nodes/"+localNode.NodeID+"/revoke", string(body), session)
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-IR-Identity-History-Sync") != "published" {
		t.Fatalf("committed identity mutation was not federated: status=%d sync=%q body=%s", response.Code, response.Header().Get("X-IR-Identity-History-Sync"), response.Body.String())
	}
	current, _, err := identity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []*discovery.Directory{first, second} {
		mirrored, found, err := directory.LookupIdentityHistory(identityID)
		if err != nil || !found {
			t.Fatalf("provider did not retain updated identity history: found=%v err=%v", found, err)
		}
		summary, err := globalidentity.SummarizeHistoryBundle(mirrored)
		if err != nil || summary.HeadHash != current.HeadHash {
			t.Fatalf("provider history head=%q current=%q err=%v", summary.HeadHash, current.HeadHash, err)
		}
	}
}

type serverHistoryFederationTransport struct{ handlers map[string]http.Handler }

func (transport *serverHistoryFederationTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	handler := transport.handlers[request.URL.Hostname()]
	if handler == nil {
		return nil, errors.New("test discovery provider unavailable")
	}
	copy := request.Clone(request.Context())
	copy.RemoteAddr = "192.0.2.73:443"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, copy)
	response := recorder.Result()
	response.Request = request
	return response, nil
}

func TestNodeAdvertisementSigningRequiresCurrentDelegationAndUsesProtectedKey(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "node-advertisement-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "node-advertisement-test-password")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	node, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := node.Create()
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, genesisSignature, userHandle, credential, challenge, identityID := signedTestGenesis(t)
	if err := identity.CreateGenesis(genesis, genesisSignature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := identity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	proof, err := node.BeginEnrollmentProof(identityID, snapshot.HeadHash, now)
	if err != nil {
		t.Fatal(err)
	}
	delegation, delegationSignature := signedTestNodeDelegation(t, rootKey, proof, "1", now)
	if err := node.ConsumeEnrollmentEvent(delegation, delegationSignature, now); err != nil {
		t.Fatal(err)
	}
	if err := identity.ApplyNodeEvent(delegation, delegationSignature); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err = identity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	identityHint, err := discoveryprotocol.GlobalIdentityHint(identityID)
	if err != nil {
		t.Fatal(err)
	}
	addressRecipient, err := base64.RawURLEncoding.DecodeString(snapshot.RecoveryEncryptionPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	addressEnvelope, err := discoveryprotocol.SealAddressEnvelope(addressRecipient, discoveryprotocol.AddressContext{
		GlobalIdentityHint: identityHint, IdentityHeadHash: snapshot.HeadHash, NodeID: localNode.NodeID,
	}, []byte(`{"addresses":["wss://recording-node.example/signal"]}`))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: identity, NodeIdentity: node, Management: products})
	requestBody, err := json.Marshal(map[string]any{
		"address_envelope": addressEnvelope,
		"capabilities":     []string{"webrtc-datachannel/1"},
		"relay_ids":        []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	noCSRFRequest := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/advertisement/sign", string(requestBody), session)
	noCSRFRequest.Header.Del("X-CSRF-Token")
	noCSRFResponse := httptest.NewRecorder()
	handler.ServeHTTP(noCSRFResponse, noCSRFRequest)
	if noCSRFResponse.Code != http.StatusForbidden {
		t.Fatalf("advertisement signing without CSRF status=%d body=%s", noCSRFResponse.Code, noCSRFResponse.Body.String())
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/advertisement/sign", string(requestBody), session))
	if response.Code != http.StatusCreated || response.Header().Get("X-Audit-Status") != "recorded" || strings.Contains(response.Body.String(), "private_key") {
		t.Fatalf("advertisement signing status=%d audit=%q body=%s", response.Code, response.Header().Get("X-Audit-Status"), response.Body.String())
	}
	wrongRecipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongEnvelope, err := discoveryprotocol.SealAddressEnvelope(wrongRecipient.PublicKey().Bytes(), discoveryprotocol.AddressContext{
		GlobalIdentityHint: identityHint, IdentityHeadHash: snapshot.HeadHash, NodeID: localNode.NodeID,
	}, []byte(`{"addresses":["wss://attacker.example/signal"]}`))
	if err != nil {
		t.Fatal(err)
	}
	wrongRequestBody, err := json.Marshal(map[string]any{"address_envelope": wrongEnvelope, "capabilities": []string{"webrtc-datachannel/1"}, "relay_ids": []string{}})
	if err != nil {
		t.Fatal(err)
	}
	wrongKeyResponse := httptest.NewRecorder()
	handler.ServeHTTP(wrongKeyResponse, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/advertisement/sign", string(wrongRequestBody), session))
	if wrongKeyResponse.Code != http.StatusBadRequest {
		t.Fatalf("advertisement encrypted to a non-identity key status=%d body=%s", wrongKeyResponse.Code, wrongKeyResponse.Body.String())
	}
	var signed struct {
		Canonical string `json:"canonical_advertisement"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &signed); err != nil {
		t.Fatal(err)
	}
	canonical, err := base64.RawURLEncoding.DecodeString(signed.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(signed.Signature)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := discoveryprotocol.Verify(canonical, signature, now.Add(time.Minute))
	if err != nil || verified.Advertisement.NodeID != localNode.NodeID || verified.Advertisement.IdentityHeadHash != snapshot.HeadHash || verified.DelegationValid {
		t.Fatalf("API returned invalid or overtrusted advertisement: %+v err=%v", verified, err)
	}
	audit := products.Audit(10)
	foundAudit := false
	for _, event := range audit {
		if event.Type == "node_advertisement_signed" && event.ObjectID == localNode.NodeID {
			foundAudit = true
		}
	}
	if !foundAudit {
		t.Fatalf("signed advertisement audit does not identify stable Node ID: %+v", audit)
	}
	revoke, revokeSignature := signedTestNodeRevocation(t, rootKey, identityID, "2", verified.Advertisement.IdentityHeadHash, localNode.NodeID, now.Add(time.Second))
	if err := identity.ApplyNodeEvent(revoke, revokeSignature); err != nil {
		t.Fatal(err)
	}
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/advertisement/sign", string(requestBody), session))
	if denied.Code != http.StatusForbidden || !strings.Contains(denied.Body.String(), "node_not_authorized") {
		t.Fatalf("revoked Node Key advertisement status=%d body=%s", denied.Code, denied.Body.String())
	}
}

func TestNodeEnrollmentChallengeAndSignedHistoryImportAcrossIndependentStores(t *testing.T) {
	targetRoot := t.TempDir()
	auth, err := authn.Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "node-enrollment-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "node-enrollment-test-password")
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := globalidentity.Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	targetNode, err := nodeidentity.Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: targetIdentity, NodeIdentity: targetNode, Management: products})

	created := httptest.NewRecorder()
	handler.ServeHTTP(created, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/create", "", session))
	if created.Code != http.StatusCreated {
		t.Fatalf("target Node Key creation status=%d body=%s", created.Code, created.Body.String())
	}
	var nodeResult struct {
		Node nodeidentity.Info `json:"node"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &nodeResult); err != nil {
		t.Fatal(err)
	}

	ownerRoot := t.TempDir()
	ownerAuth, err := authn.Open(ownerRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerAuth.Bootstrap("identity-owner", "node-enrollment-owner-test-password"); err != nil {
		t.Fatal(err)
	}
	ownerSession, err := ownerAuth.LoginAs("identity-owner", "node-enrollment-owner-test-password")
	if err != nil {
		t.Fatal(err)
	}
	ownerIdentity, err := globalidentity.Open(ownerRoot)
	if err != nil {
		t.Fatal(err)
	}
	ownerManagement, err := management.Open(ownerRoot)
	if err != nil {
		t.Fatal(err)
	}
	ownerHandler := NewWithOptions(nil, nil, nil, Options{Auth: ownerAuth, GlobalIdentity: ownerIdentity, Management: ownerManagement})
	rootKey, genesis, genesisSignature, userHandle, credential, challenge, identityID := signedTestGenesis(t)
	if err := ownerIdentity.CreateGenesis(genesis, genesisSignature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	ownerSnapshot, _, err := ownerIdentity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	challengeRequestBody, _ := json.Marshal(map[string]string{"identity_id": identityID, "previous_event_hash": ownerSnapshot.HeadHash})
	challengeResponse := httptest.NewRecorder()
	handler.ServeHTTP(challengeResponse, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/node-identity/enrollment/challenge", string(challengeRequestBody), session))
	if challengeResponse.Code != http.StatusCreated {
		t.Fatalf("one-use Node enrollment challenge status=%d body=%s", challengeResponse.Code, challengeResponse.Body.String())
	}
	var proof nodeidentity.EnrollmentProof
	if err := json.Unmarshal(challengeResponse.Body.Bytes(), &proof); err != nil {
		t.Fatal(err)
	}
	if proof.NodeID != nodeResult.Node.NodeID || proof.PublicKey != nodeResult.Node.PublicKey || proof.IdentityID != identityID || proof.PreviousHash != ownerSnapshot.HeadHash || proof.Nonce == "" || proof.Signature == "" {
		t.Fatalf("enrollment proof did not bind the target Node Key and identity head: %+v", proof)
	}

	delegation, delegationSignature := signedTestNodeDelegation(t, rootKey, proof, "1", time.Now().UTC())
	delegationEnvelope, err := json.Marshal(identityEventEnvelope{CanonicalEvent: base64.RawURLEncoding.EncodeToString(delegation), Signature: base64.RawURLEncoding.EncodeToString(delegationSignature)})
	if err != nil {
		t.Fatal(err)
	}
	delegateResponse := httptest.NewRecorder()
	ownerHandler.ServeHTTP(delegateResponse, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/nodes/delegate", string(delegationEnvelope), ownerSession))
	if delegateResponse.Code != http.StatusOK {
		t.Fatalf("owner API rejected the valid root-signed node delegation: status=%d body=%s", delegateResponse.Code, delegateResponse.Body.String())
	}
	delegationEvent, err := identityprotocol.VerifyEvent(delegation, delegationSignature)
	if err != nil {
		t.Fatal(err)
	}
	delegatedState, delegated, err := ownerIdentity.Snapshot()
	if err != nil || !delegated || delegatedState.HeadHash != delegationEvent.EventHash || !containsIdentityNode(delegatedState.NodeIDs, proof.NodeID) {
		t.Fatalf("owner API did not persist the delegated Node identity: snapshot=%+v initialized=%v err=%v", delegatedState, delegated, err)
	}
	bundle, err := ownerIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	bundleJSON, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a process stop after the one-use proof has been durably consumed
	// but before the imported identity history was written. The exact event
	// must remain retryable without making the nonce available to another event.
	if err := targetNode.ConsumeEnrollmentEvent(delegation, delegationSignature, time.Now().UTC()); err != nil {
		t.Fatalf("pre-consume enrollment event to simulate interrupted import: %v", err)
	}
	importResponse := httptest.NewRecorder()
	handler.ServeHTTP(importResponse, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/history/import", string(bundleJSON), session))
	if importResponse.Code != http.StatusOK || !strings.Contains(importResponse.Body.String(), `"identity_id":"`+identityID+`"`) || !strings.Contains(importResponse.Body.String(), `"node_ids":["`+proof.NodeID+`"]`) {
		t.Fatalf("signed history enrollment import status=%d body=%s", importResponse.Code, importResponse.Body.String())
	}
	if strings.Contains(importResponse.Body.String(), "private_key") || strings.Contains(importResponse.Body.String(), "encryption_private") || strings.Contains(importResponse.Body.String(), "signing_seed") {
		t.Fatalf("private Node Key material leaked in import response: %s", importResponse.Body.String())
	}
	retryImport := httptest.NewRecorder()
	handler.ServeHTTP(retryImport, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/history/import", string(bundleJSON), session))
	if retryImport.Code != http.StatusOK {
		t.Fatalf("idempotent history import retry status=%d body=%s", retryImport.Code, retryImport.Body.String())
	}
	ownerJoined, _, err := ownerIdentity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	revoke, revokeSignature := signedTestNodeRevocation(t, rootKey, identityID, "2", ownerJoined.HeadHash, proof.NodeID, time.Now().UTC())
	revokedEvent, err := identityprotocol.VerifyEvent(revoke, revokeSignature)
	if err != nil {
		t.Fatal(err)
	}
	revokeEnvelope, err := json.Marshal(identityEventEnvelope{CanonicalEvent: base64.RawURLEncoding.EncodeToString(revoke), Signature: base64.RawURLEncoding.EncodeToString(revokeSignature)})
	if err != nil {
		t.Fatal(err)
	}
	revokeResponse := httptest.NewRecorder()
	ownerHandler.ServeHTTP(revokeResponse, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/nodes/"+proof.NodeID+"/revoke", string(revokeEnvelope), ownerSession))
	if revokeResponse.Code != http.StatusOK {
		t.Fatalf("owner API rejected the valid root-signed Node Key revocation: status=%d body=%s", revokeResponse.Code, revokeResponse.Body.String())
	}
	ownerRevoked, initialized, err := ownerIdentity.Snapshot()
	if err != nil || !initialized || ownerRevoked.HeadHash != revokedEvent.EventHash || containsIdentityNode(ownerRevoked.NodeIDs, proof.NodeID) {
		t.Fatalf("owner API did not persist the Node revocation: snapshot=%+v initialized=%v err=%v", ownerRevoked, initialized, err)
	}
	revokedBundle, err := ownerIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	revokedJSON, err := json.Marshal(revokedBundle)
	if err != nil {
		t.Fatal(err)
	}
	revokeSync := httptest.NewRecorder()
	handler.ServeHTTP(revokeSync, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/history/import", string(revokedJSON), session))
	if revokeSync.Code != http.StatusOK {
		t.Fatalf("authorized Node Key revocation did not sync to the joined node: status=%d body=%s", revokeSync.Code, revokeSync.Body.String())
	}
	revokedTarget, found, err := targetIdentity.Snapshot()
	if err != nil || !found || revokedTarget.Frozen || len(revokedTarget.NodeIDs) != 0 || revokedTarget.HeadHash != revokedEvent.EventHash || revokedTarget.Sequence != "2" {
		t.Fatalf("revoked node state did not propagate safely: snapshot=%+v found=%v err=%v", revokedTarget, found, err)
	}
	changedDelegation, changedSignature := signedTestNodeDelegation(t, rootKey, proof, "1", time.Now().UTC().Add(time.Second))
	if _, err := targetNode.ValidateEnrollmentEvent(changedDelegation, changedSignature, time.Now().UTC()); !errors.Is(err, nodeidentity.ErrEnrollmentChallengeConsumed) {
		t.Fatalf("accepted enrollment nonce was reused for a different event: %v", err)
	}
	status := httptest.NewRecorder()
	handler.ServeHTTP(status, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/status", "", session))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"frozen":false`) {
		t.Fatalf("joined identity status=%d body=%s", status.Code, status.Body.String())
	}
	if len(products.Audit(20)) < 4 {
		t.Fatalf("Node Key, challenge and history import were not audited: %+v", products.Audit(20))
	}
	var sawDelegationAudit, sawRevocationAudit bool
	for _, entry := range ownerManagement.Audit(20) {
		if entry.ObjectID != proof.NodeID {
			continue
		}
		sawDelegationAudit = sawDelegationAudit || entry.Type == "node_identity_delegated"
		sawRevocationAudit = sawRevocationAudit || entry.Type == "node_identity_revoked"
	}
	if !sawDelegationAudit || !sawRevocationAudit {
		t.Fatalf("owner Node delegation/revocation mutations were not audited: %+v", ownerManagement.Audit(20))
	}
}

func signedTestGenesis(t *testing.T) (ed25519.PrivateKey, []byte, []byte, []byte, webauthn.Credential, string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyID, _ := identityprotocol.KeyID(publicKey)
	challengeBytes := make([]byte, 32)
	if _, err := rand.Read(challengeBytes); err != nil {
		t.Fatal(err)
	}
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes)
	credential := webauthn.Credential{ID: []byte("target-rp-test-credential"), PublicKey: []byte("test-public-key")}
	encryptionPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encryptionPublic := encryptionPrivate.PublicKey().Bytes()
	event := map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         identityID,
		"sequence":            "0",
		"previous_event_hash": nil,
		"operation":           "genesis",
		"issued_at":           time.Now().UTC().Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(publicKey),
		"payload": map[string]any{
			"identity_format":                "global-identity/v1",
			"recovery_encryption_public_key": base64.RawURLEncoding.EncodeToString(encryptionPublic),
			"webauthn_challenge":             challenge,
			"credential_id":                  base64.RawURLEncoding.EncodeToString(credential.ID),
		},
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	userHandle := make([]byte, 32)
	if _, err := rand.Read(userHandle); err != nil {
		t.Fatal(err)
	}
	return privateKey, canonical, signature, userHandle, credential, challenge, identityID
}

func signedTestNodeDelegation(t *testing.T, rootKey ed25519.PrivateKey, proof nodeidentity.EnrollmentProof, sequence string, issuedAt time.Time) ([]byte, []byte) {
	t.Helper()
	publicKey := rootKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         proof.IdentityID,
		"sequence":            sequence,
		"previous_event_hash": proof.PreviousHash,
		"operation":           "node_delegate",
		"issued_at":           issuedAt.UTC().Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(publicKey),
		"payload": map[string]any{
			"enrollment_nonce": proof.Nonce,
			"node_id":          proof.NodeID,
			"node_proof":       proof.Signature,
			"node_public_key":  proof.PublicKey,
		},
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func signedTestNodeRevocation(t *testing.T, rootKey ed25519.PrivateKey, identityID, sequence, previousHash, nodeID string, issuedAt time.Time) ([]byte, []byte) {
	t.Helper()
	publicKey := rootKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         identityID,
		"sequence":            sequence,
		"previous_event_hash": previousHash,
		"operation":           "node_revoke",
		"issued_at":           issuedAt.UTC().Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(publicKey),
		"payload":             map[string]any{"node_id": nodeID},
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func TestGlobalIdentityStatusAndPasskeyOptionsAreScopedAndOriginBound(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "global-identity-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "global-identity-test-password")
	if err != nil {
		t.Fatal(err)
	}
	store, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: store})

	statusRequest := authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/global-identity/status", "", session)
	statusResponse := httptest.NewRecorder()
	handler.ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK || !strings.Contains(statusResponse.Body.String(), `"initialized":false`) {
		t.Fatalf("global identity status=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}

	unauthorized := httptest.NewRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/registration/options", strings.NewReader(`{"identity_id":"gi_FZHpkMERS415hhqLqd6caA"}`))
	unauthorized.Header.Set("Origin", "https://recorder.example.com")
	unauthorized.Header.Set("X-IR-HTTPS", "1")
	unauthorizedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedResponse, unauthorized)
	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated identity registration status=%d body=%s", unauthorizedResponse.Code, unauthorizedResponse.Body.String())
	}

	badOrigin := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/registration/options", `{"identity_id":"gi_FZHpkMERS415hhqLqd6caA"}`, session)
	badOrigin.Header.Set("Origin", "https://attacker.example")
	badOriginResponse := httptest.NewRecorder()
	handler.ServeHTTP(badOriginResponse, badOrigin)
	if badOriginResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-origin identity registration status=%d body=%s", badOriginResponse.Code, badOriginResponse.Body.String())
	}

	insecureOrigin := authenticatedIdentityRequest(http.MethodPost, "http://recorder.example.com/api/global-identity/registration/options", `{"identity_id":"gi_FZHpkMERS415hhqLqd6caA"}`, session)
	insecureOrigin.Header.Del("X-IR-HTTPS")
	insecureResponse := httptest.NewRecorder()
	handler.ServeHTTP(insecureResponse, insecureOrigin)
	if insecureResponse.Code != http.StatusForbidden {
		t.Fatalf("non-local HTTP WebAuthn registration status=%d body=%s", insecureResponse.Code, insecureResponse.Body.String())
	}

	optionsRequest := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/registration/options", `{"identity_id":"gi_FZHpkMERS415hhqLqd6caA"}`, session)
	optionsResponse := httptest.NewRecorder()
	handler.ServeHTTP(optionsResponse, optionsRequest)
	if optionsResponse.Code != http.StatusOK {
		t.Fatalf("registration options status=%d body=%s", optionsResponse.Code, optionsResponse.Body.String())
	}
	var options struct {
		PublicKey struct {
			Challenge string `json:"challenge"`
			RP        struct {
				ID string `json:"id"`
			} `json:"rp"`
			User struct {
				ID string `json:"id"`
			} `json:"user"`
			AuthenticatorSelection struct {
				ResidentKey      string `json:"residentKey"`
				UserVerification string `json:"userVerification"`
			} `json:"authenticatorSelection"`
		} `json:"publicKey"`
	}
	if err := json.Unmarshal(optionsResponse.Body.Bytes(), &options); err != nil {
		t.Fatal(err)
	}
	if options.PublicKey.Challenge == "" || options.PublicKey.RP.ID != "recorder.example.com" || options.PublicKey.User.ID == "" || options.PublicKey.AuthenticatorSelection.ResidentKey != "required" || options.PublicKey.AuthenticatorSelection.UserVerification != "required" {
		t.Fatalf("unsafe or incomplete passkey options: %+v", options.PublicKey)
	}
	var challengeCookie *http.Cookie
	for _, cookie := range optionsResponse.Result().Cookies() {
		if cookie.Name == globalIdentityCeremonyCookie {
			challengeCookie = cookie
		}
	}
	if challengeCookie == nil || !challengeCookie.HttpOnly || !challengeCookie.Secure || challengeCookie.SameSite != http.SameSiteStrictMode || challengeCookie.Path != "/api/global-identity" {
		t.Fatalf("passkey ceremony cookie is missing secure attributes: %+v", challengeCookie)
	}
	if strings.Contains(optionsResponse.Body.String(), "recovery") || strings.Contains(optionsResponse.Body.String(), "private") {
		t.Fatalf("passkey options exposed recovery material: %s", optionsResponse.Body.String())
	}
}

func TestGlobalIdentityPasskeyRequiresRecoverableRPOriginAndSessionRoutesAreUnauthenticated(t *testing.T) {
	handler := NewWithOptions(nil, nil, nil, Options{})
	for _, item := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/api/global-identity/session", http.StatusOK},
		{http.MethodPost, "/api/global-identity/login/options", http.StatusServiceUnavailable},
	} {
		request := httptest.NewRequest(item.method, "http://localhost"+item.path, strings.NewReader(`{}`))
		request.Header.Set("Origin", "http://localhost")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != item.status {
			t.Errorf("%s %s status=%d body=%s want %d", item.method, item.path, response.Code, response.Body.String(), item.status)
		}
	}
	if _, _, ok := globalIdentityRequestContext(httptest.NewRequest(http.MethodPost, "http://100.64.0.10/api/global-identity/login/options", nil)); ok {
		t.Fatal("WebAuthn accepted a non-local cleartext IP origin")
	}
	if _, _, ok := globalIdentityRequestContext(httptest.NewRequest(http.MethodPost, "https://100.64.0.10/api/global-identity/login/options", nil)); ok {
		t.Fatal("WebAuthn accepted an IP literal RP ID")
	}
}

func TestGlobalIdentityLoginOptionsSupportsOrdinaryAndPlaybackBoundPasskeyLogin(t *testing.T) {
	root := t.TempDir()
	store, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	_, canonical, signature, userHandle, credential, challenge, _ := signedTestGenesis(t)
	if err := store.CreateGenesis(canonical, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{GlobalIdentity: store})

	ordinary := httptest.NewRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/login/options", strings.NewReader(`{}`))
	ordinary.Header.Set("Origin", "https://recorder.example.com")
	ordinaryResponse := httptest.NewRecorder()
	handler.ServeHTTP(ordinaryResponse, ordinary)
	if ordinaryResponse.Code != http.StatusOK {
		t.Fatalf("ordinary login options status=%d body=%s", ordinaryResponse.Code, ordinaryResponse.Body.String())
	}
	var ordinaryOptions struct {
		PublicKey struct {
			Challenge string `json:"challenge"`
		} `json:"publicKey"`
	}
	if err := json.Unmarshal(ordinaryResponse.Body.Bytes(), &ordinaryOptions); err != nil {
		t.Fatal(err)
	}
	if ordinaryOptions.PublicKey.Challenge == "" {
		t.Fatalf("ordinary login options omitted the WebAuthn challenge: %s", ordinaryResponse.Body.String())
	}

	playbackPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	playbackRequestBody, err := json.Marshal(map[string]string{
		"recipient_public_key": base64.RawURLEncoding.EncodeToString(playbackPrivate.PublicKey().Bytes()),
	})
	if err != nil {
		t.Fatal(err)
	}
	playback := httptest.NewRequest(http.MethodPost, "https://recorder.example.com/api/global-identity/login/options", strings.NewReader(string(playbackRequestBody)))
	playback.Header.Set("Origin", "https://recorder.example.com")
	playbackResponse := httptest.NewRecorder()
	handler.ServeHTTP(playbackResponse, playback)
	if playbackResponse.Code != http.StatusOK {
		t.Fatalf("playback-bound login options status=%d body=%s", playbackResponse.Code, playbackResponse.Body.String())
	}
	var playbackOptions struct {
		PublicKey struct {
			Challenge string `json:"challenge"`
		} `json:"publicKey"`
	}
	if err := json.Unmarshal(playbackResponse.Body.Bytes(), &playbackOptions); err != nil {
		t.Fatal(err)
	}
	if playbackOptions.PublicKey.Challenge == "" || playbackOptions.PublicKey.Challenge == ordinaryOptions.PublicKey.Challenge {
		t.Fatalf("playback login did not receive a distinct bound challenge: ordinary=%q playback=%q", ordinaryOptions.PublicKey.Challenge, playbackOptions.PublicKey.Challenge)
	}
}

func authenticatedIdentityRequest(method, target, body string, session authn.Session) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Header.Set("Origin", "https://recorder.example.com")
	request.Header.Set("X-IR-HTTPS", "1")
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	if method != http.MethodGet {
		request.Header.Set("X-CSRF-Token", session.CSRFToken)
	}
	return request
}
