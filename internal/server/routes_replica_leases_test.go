package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerapi"
)

type replicaLeaseSnapshotFixture struct {
	leases     []lease.Lease
	tombstones []lease.TombstoneRecord
	err        error
}

func (f replicaLeaseSnapshotFixture) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return f.leases, f.tombstones, f.err
}

func TestReplicaLeaseListReturnsOnlySanitizedProjection(t *testing.T) {
	ownerSubjectID := "os_AAAAAAAAAAAAAAAAAAAAAA"
	identityID := "gi_BBBBBBBBBBBBBBBBBBBBBB"
	providerNodeID := "nd_AAAAAAAAAAAAAAAAAAAAAA"
	archiveID := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	snapshotID := "snap_" + strings.Repeat("a", 64)
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: providerNodeID},
		ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{"archive.replicate"},
		NotBefore: "2026-11-03T00:00:00Z", ExpiresAt: "2026-11-05T00:00:00Z",
		DelegationDepth: "0", Nonce: strings.Repeat("n", 43), PolicyEpoch: "1", IssuerKeyID: "ik_" + strings.Repeat("k", 43),
	}
	grantHash, err := authority.GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{replicaLeases: replicaLeaseSnapshotFixture{
		leases: []lease.Lease{{
			Offer: lease.SignedOffer{
				Offer: lease.Offer{LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA", ArchiveID: archiveID, SnapshotID: snapshotID, ProviderNodeID: providerNodeID, OwnerSubjectID: ownerSubjectID, ReservedBytes: "4096", ExpiresAt: "2026-11-04T00:00:00Z", EncryptedObjectSet: []lease.EncryptedObject{{ObjectID: "ob_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", CiphertextSize: "100"}}},
				Actor: leaseActorFixture(), GrantChain: []authority.SignedGrant{{Grant: grant, Signature: []byte("sensitive-grant-signature")}}, Signature: "sensitive-signature",
			}, Status: lease.StatusOffered, Revision: "1", Progress: []lease.Progress{{ObjectID: "ob_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", CommittedOffset: "100"}},
		}},
		tombstones: []lease.TombstoneRecord{{Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: "arc_BBBBBBBBBBBBBBBBBBBBBB", DeletionEpoch: "2", IssuedAt: "2026-11-03T00:00:00Z", OwnerSubjectID: "os_BBBBBBBBBBBBBBBBBBBBBB", ReasonCode: "owner_requested", AuthorizedEvidence: "sha256:" + strings.Repeat("b", 64)}, Signature: "sensitive-tombstone-signature"}, Frozen: true}},
	}}
	request := httptest.NewRequest("GET", "/api/replica-leases", nil)
	response := httptest.NewRecorder()
	server.replicaLeasesList(response, request)
	if response.Code != 200 {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, expected := range []string{"\"status\":\"offered\"", "\"completed_objects\":1", "\"frozen\":true", "\"owner_subject_id\":\"" + ownerSubjectID + "\"", "\"grant_hash\":\"" + grantHash + "\""} {
		if !strings.Contains(body, expected) {
			t.Fatalf("response missing %s: %s", expected, body)
		}
	}
	for _, forbidden := range []string{"\"signature\"", "grant_chain", "sensitive-signature", "sensitive-tombstone-signature"} {
		if strings.Contains(strings.ToLower(body), forbidden) {
			t.Fatalf("response exposed %q: %s", forbidden, body)
		}
	}
}

func TestReplicaLeaseListFailsClosedWhenStoreUnavailable(t *testing.T) {
	serverWithoutStore := &Server{}
	response := httptest.NewRecorder()
	serverWithoutStore.replicaLeasesList(response, httptest.NewRequest("GET", "/api/replica-leases", nil))
	if response.Code != 503 {
		t.Fatalf("nil store status=%d body=%s", response.Code, response.Body.String())
	}
	for _, fixture := range []replicaLeaseSnapshotFixture{{err: errors.New("private filesystem path")}} {
		server := &Server{replicaLeases: fixture}
		response := httptest.NewRecorder()
		server.replicaLeasesList(response, httptest.NewRequest("GET", "/api/replica-leases", nil))
		if response.Code != 503 || strings.Contains(response.Body.String(), "private filesystem path") {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestReplicaOfferRetryRequiresExactLeaseScopeAndKeyEpoch(t *testing.T) {
	request := archiveReplicaEncryptRequest{
		OwnerSubjectID: "os_AAAAAAAAAAAAAAAAAAAAAA", GrantHash: "sha256:" + strings.Repeat("a", 64),
		KeyEpoch: 1710000000000, LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA", ProviderNodeID: "nd_BBBBBBBBBBBBBBBBBBBBBB",
	}
	offer := lease.Offer{
		LeaseID: request.LeaseID, OwnerSubjectID: request.OwnerSubjectID, ArchiveID: "arc_CCCCCCCCCCCCCCCCCCCCCC",
		SnapshotID: "snap_" + strings.Repeat("d", 64), KeyEpoch: "1710000000000", ProviderNodeID: request.ProviderNodeID,
	}
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: "nd_EEEEEEEEEEEEEEEEEEEEEE"}
	if !replicaOfferMatchesRetry(offer, actor, request, offer.ArchiveID, offer.SnapshotID, actor.ID) {
		t.Fatal("exact retry scope was rejected")
	}
	request.KeyEpoch++
	if replicaOfferMatchesRetry(offer, actor, request, offer.ArchiveID, offer.SnapshotID, actor.ID) {
		t.Fatal("retry with a different key epoch reused the durable lease")
	}
	request.KeyEpoch--
	request.ProviderNodeID = "nd_FFFFFFFFFFFFFFFFFFFFFF"
	if replicaOfferMatchesRetry(offer, actor, request, offer.ArchiveID, offer.SnapshotID, actor.ID) {
		t.Fatal("retry with a different provider reused the durable lease")
	}
}

func TestReplicaOfferRouteRequiresAuthenticationAndHTTPS(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("replica-owner", "replica-owner-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("replica-owner", "replica-owner-test-password")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth})
	target := "https://recorder.example.com/api/recordings/rec_test/replicas/offers"

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{}`)))
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated request status=%d body=%s", unauthenticated.Code, unauthenticated.Body.String())
	}

	cleartext := authenticatedIdentityRequest(http.MethodPost, "http://recorder.example.com/api/recordings/rec_test/replicas/offers", `{}`, session)
	cleartext.Header.Del("X-IR-HTTPS")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, cleartext)
	if response.Code != http.StatusUpgradeRequired || !strings.Contains(response.Body.String(), `"error_code":"https_required"`) {
		t.Fatalf("authenticated cleartext request status=%d body=%s", response.Code, response.Body.String())
	}
}

type replicaLeaseVerificationFixture struct {
	current  lease.Lease
	verified bool
}

func (f *replicaLeaseVerificationFixture) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return []lease.Lease{f.current}, nil, nil
}

func (f *replicaLeaseVerificationFixture) Get(_ context.Context, leaseID string) (lease.Lease, error) {
	if leaseID != f.current.Offer.Offer.LeaseID {
		return lease.Lease{}, lease.ErrNotFound
	}
	return f.current, nil
}

func (f *replicaLeaseVerificationFixture) VerifyReplica(_ context.Context, leaseID string, _ lease.ObjectReader) (lease.Lease, error) {
	if leaseID != f.current.Offer.Offer.LeaseID || f.current.Status != lease.StatusVerifying {
		return lease.Lease{}, lease.ErrTransition
	}
	f.verified = true
	f.current.Status = lease.StatusActive
	return f.current, nil
}

type replicaPeerSessionFixture struct {
	sessions  []peerapi.SessionInfo
	tombstone lease.SignedTombstone
	requested string
	calls     int
	err       error
}

func (f *replicaPeerSessionFixture) List() []peerapi.SessionInfo {
	return append([]peerapi.SessionInfo(nil), f.sessions...)
}
func (f *replicaPeerSessionFixture) Connect(context.Context, string, string) error { return nil }
func (f *replicaPeerSessionFixture) Disconnect(string) error                       { return nil }
func (f *replicaPeerSessionFixture) SubmitTombstone(_ context.Context, nodeID string, scope peerapi.RequestScope, signed lease.SignedTombstone) error {
	f.calls++
	f.requested = nodeID
	f.tombstone = signed
	if scope.TargetNodeID != nodeID || scope.Action != "archive.replicate" || scope.SnapshotID != "" {
		return peerapi.ErrInvalidRequest
	}
	return f.err
}
func (f *replicaPeerSessionFixture) OpenCiphertextRange(context.Context, string, peerapi.RequestScope, string, string, uint64, int64, int64) (io.ReadCloser, error) {
	return nil, errors.New("unexpected peer read")
}

func TestReplicaLeaseVerifyIsClassifiedAndFailsClosed(t *testing.T) {
	leaseID := "rl_AAAAAAAAAAAAAAAAAAAAAA"
	if permission := routePermission(http.MethodPost, "/api/replica-leases/"+leaseID+"/verify"); permission != authn.PermissionRecordingExport {
		t.Fatalf("verify route permission=%q", permission)
	}
	auth, err := authn.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("replica-owner", "replica-owner-test-password"); err != nil {
		t.Fatal(err)
	}
	ownerSession, err := auth.LoginAs("replica-owner", "replica-owner-test-password")
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := auth.CreateUserWithRoles("replica-viewer", "replica-viewer-test-password", []string{authn.RoleViewer})
	if err != nil {
		t.Fatal(err)
	}
	viewerSession, err := auth.LoginAs(viewer.Login, "replica-viewer-test-password")
	if err != nil {
		t.Fatal(err)
	}
	requestBody := `{"owner_subject_id":"os_AAAAAAAAAAAAAAAAAAAAAA","grant_hash":"sha256:` + strings.Repeat("a", 64) + `"}`

	for _, test := range []struct {
		name       string
		status     lease.Status
		session    authn.Session
		secure     bool
		wantStatus int
	}{
		{name: "viewer denied", status: lease.StatusVerifying, session: viewerSession, secure: true, wantStatus: http.StatusForbidden},
		{name: "http denied", status: lease.StatusVerifying, session: ownerSession, secure: false, wantStatus: http.StatusUpgradeRequired},
		{name: "not verifying", status: lease.StatusOffered, session: ownerSession, secure: true, wantStatus: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &replicaLeaseVerificationFixture{current: lease.Lease{
				Offer: lease.SignedOffer{Offer: lease.Offer{LeaseID: leaseID}}, Status: test.status,
			}}
			handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, ReplicaLeases: store, PeerSessions: &replicaPeerSessionFixture{}})
			request := authenticatedIdentityRequest(http.MethodPost, "http://recorder.example.com/api/replica-leases/"+leaseID+"/verify", requestBody, test.session)
			if !test.secure {
				request.Header.Del("X-IR-HTTPS")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s want=%d", response.Code, response.Body.String(), test.wantStatus)
			}
			if store.verified {
				t.Fatal("verification ran before request gates passed")
			}
		})
	}
}

func TestReplicaImportRequiresOwnerPermissionHTTPSAndConfiguredState(t *testing.T) {
	if permission := routePermission(http.MethodPost, "/api/replica-imports"); permission != authn.PermissionRecordingExport {
		t.Fatalf("import route permission=%q", permission)
	}
	auth, err := authn.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("replica-owner", "replica-owner-test-password"); err != nil {
		t.Fatal(err)
	}
	ownerSession, err := auth.LoginAs("replica-owner", "replica-owner-test-password")
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := auth.CreateUserWithRoles("replica-viewer", "replica-viewer-test-password", []string{authn.RoleViewer})
	if err != nil {
		t.Fatal(err)
	}
	viewerSession, err := auth.LoginAs(viewer.Login, "replica-viewer-test-password")
	if err != nil {
		t.Fatal(err)
	}
	body := `{"source_node_id":"nd_AAAAAAAAAAAAAAAAAAAAAA","owner_subject_id":"os_AAAAAAAAAAAAAAAAAAAAAA","grant_hash":"sha256:` + strings.Repeat("a", 64) + `","archive_id":"arc_AAAAAAAAAAAAAAAAAAAAAA","snapshot_id":"snap_` + strings.Repeat("b", 64) + `","lease_id":"rl_AAAAAAAAAAAAAAAAAAAAAA"}`
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth})
	tests := []struct {
		name       string
		session    *authn.Session
		secure     bool
		wantStatus int
	}{
		{name: "unauthenticated", wantStatus: http.StatusUnauthorized},
		{name: "viewer denied", session: &viewerSession, secure: true, wantStatus: http.StatusForbidden},
		{name: "cleartext denied", session: &ownerSession, wantStatus: http.StatusUpgradeRequired},
		{name: "missing runtime state fails closed", session: &ownerSession, secure: true, wantStatus: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "http://recorder.example.com/api/replica-imports", strings.NewReader(body))
			if test.session != nil {
				request = authenticatedIdentityRequest(http.MethodPost, request.URL.String(), body, *test.session)
			}
			if !test.secure {
				request.Header.Del("X-IR-HTTPS")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s want=%d", response.Code, response.Body.String(), test.wantStatus)
			}
		})
	}
}

func leaseActorFixture() authority.Principal {
	return authority.Principal{Kind: authority.PrincipalNode, ID: "nd_CCCCCCCCCCCCCCCCCCCCCC"}
}
