package server

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/management"
)

func TestOwnerAuthorityViewReportsSortedRekeyRequiredArchives(t *testing.T) {
	view := ownerAuthorityView(authority.AuthoritySnapshot{
		ArchiveIDs: []string{}, ReplicationPolicy: authority.ReplicationPolicy{MinimumReplicas: "2", GracePeriodSeconds: "60", CapacityLimitBytes: "4096", RelocateAfterGrace: true}, RekeyRequiredSinceHash: "sha256:" + strings.Repeat("1", 64), RekeyRequiredArchives: map[string]struct{}{
			"arc_ZZZZZZZZZZZZZZZZZZZZZZ": {},
			"arc_AAAAAAAAAAAAAAAAAAAAAA": {},
		},
	})
	want := []string{"arc_AAAAAAAAAAAAAAAAAAAAAA", "arc_ZZZZZZZZZZZZZZZZZZZZZZ"}
	if !reflect.DeepEqual(view.RekeyRequiredArchives, want) {
		t.Fatalf("rekey-required archives=%v, want %v", view.RekeyRequiredArchives, want)
	}
	encoded, err := json.Marshal(view)
	if err != nil || !strings.Contains(string(encoded), `"rekey_required_archives":["arc_AAAAAAAAAAAAAAAAAAAAAA","arc_ZZZZZZZZZZZZZZZZZZZZZZ"]`) {
		t.Fatalf("rekey status missing from public OwnerSubject projection: %s err=%v", encoded, err)
	}
	if !strings.Contains(string(encoded), `"rekey_required_since_event_hash":"sha256:`+strings.Repeat("1", 64)+`"`) {
		t.Fatalf("transfer binding missing from public OwnerSubject projection: %s", encoded)
	}
	if !strings.Contains(string(encoded), `"replication_policy":{"minimum_replicas":"2","grace_period_seconds":"60","capacity_limit_bytes":"4096","relocate_after_grace":true}`) {
		t.Fatalf("replication policy missing from public OwnerSubject projection: %s", encoded)
	}
}

func TestOwnerAuthorityRekeyGateHasStableConflictCode(t *testing.T) {
	response := httptest.NewRecorder()
	writeOwnerAuthorityError(response, authority.ErrRekeyRequired)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"error_code":"archive_rekey_required"`) {
		t.Fatalf("rekey-gated authority response status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestOwnerAuthoritySyncImportsAgreedSignedHistoryWithRBACAndCSRF(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "authority-sync-owner-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "authority-sync-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := auth.CreateUser("authority-sync-viewer", "authority-sync-viewer-password", authn.RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	viewerSession, err := auth.LoginAs(viewer.Login, "authority-sync-viewer-password")
	if err != nil {
		t.Fatal(err)
	}

	global, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, signature, userHandle, credential, challenge, identityID := signedTestGenesis(t)
	if err := global.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Now().UTC()
	identityState, err := authority.IdentityStateFromStore(global, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	resolver := resolverForGlobalIdentity(global)
	first, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "second"))
	if err != nil {
		t.Fatal(err)
	}
	transport := &serverAuthorityDirectoryTransport{
		handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)},
		failures: map[string]bool{},
	}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}

	subject, err := authority.NewPersonOwnerSubject(identityID, "1", issuedAt, strings.NewReader("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject, authority.SubjectCreatePayload{Subject: subject}, identityState, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := authority.SignAuthorityEvent(event, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	sourceStore, err := authority.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceStore.Apply(signed, resolver, issuedAt); err != nil {
		t.Fatal(err)
	}
	bundle, err := sourceStore.ExportFederatedHistory(subject.ID, resolver, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := federation.PublishOwnerAuthorityHistory(context.Background(), subject.ID, bundle); err != nil {
		t.Fatal(err)
	}

	recipientStore, err := authority.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: global, OwnerAuthority: recipientStore, IdentityFederation: federation, Management: products})
	path := "https://recorder.example.com/api/owner-authority/subjects/" + subject.ID + "/sync"

	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, authenticatedIdentityRequest(http.MethodPost, path, "", viewerSession))
	if denied.Code != http.StatusForbidden {
		t.Fatalf("viewer authority sync status=%d body=%s", denied.Code, denied.Body.String())
	}
	noCSRF := authenticatedIdentityRequest(http.MethodPost, path, "", session)
	noCSRF.Header.Del("X-CSRF-Token")
	csrfDenied := httptest.NewRecorder()
	handler.ServeHTTP(csrfDenied, noCSRF)
	if csrfDenied.Code != http.StatusForbidden {
		t.Fatalf("authority sync without CSRF status=%d body=%s", csrfDenied.Code, csrfDenied.Body.String())
	}

	synced := httptest.NewRecorder()
	syncRequest := authenticatedIdentityRequest(http.MethodPost, path, "", session)
	handler.ServeHTTP(synced, syncRequest)
	if synced.Code != http.StatusOK || !strings.Contains(synced.Body.String(), `"rekey_required_archives":[]`) || !strings.Contains(synced.Body.String(), `"subject_id":"`+subject.ID+`"`) {
		t.Fatalf("authority sync request=%s %s status=%d headers=%v body=%s", syncRequest.Method, syncRequest.URL.Path, synced.Code, synced.Header(), synced.Body.String())
	}
	repeated := httptest.NewRecorder()
	handler.ServeHTTP(repeated, authenticatedIdentityRequest(http.MethodPost, path, "", session))
	if repeated.Code != http.StatusOK || !strings.Contains(repeated.Body.String(), `"sequence":"1"`) {
		t.Fatalf("repeated authority sync was not idempotent: status=%d body=%s", repeated.Code, repeated.Body.String())
	}
	withoutAudit := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: global, OwnerAuthority: recipientStore, IdentityFederation: federation})
	auditFailure := httptest.NewRecorder()
	withoutAudit.ServeHTTP(auditFailure, authenticatedIdentityRequest(http.MethodPost, path, "", session))
	if auditFailure.Code != http.StatusServiceUnavailable || !strings.Contains(auditFailure.Body.String(), `"error_code":"audit_unavailable"`) {
		t.Fatalf("authority sync audit failure status=%d body=%s", auditFailure.Code, auditFailure.Body.String())
	}
	imported, err := recipientStore.Snapshot(subject.ID, resolver, time.Now().UTC())
	if err != nil || imported.HeadHash == "" || imported.Sequence != "1" {
		t.Fatalf("synced authority history=%+v err=%v", imported, err)
	}
	if audit := products.Audit(10); len(audit) != 2 || audit[0].Type != "owner_subject_history_sync" || audit[0].ObjectID != subject.ID || audit[1].Type != "owner_subject_history_sync" || audit[1].ObjectID != subject.ID {
		t.Fatalf("authority sync audit=%+v", audit)
	}
}

func TestOwnerAuthorityAPIRequiresLocalRBACAndCSRFAndPersistsSignedSubject(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "owner-authority-api-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "owner-authority-api-password")
	if err != nil {
		t.Fatal(err)
	}
	member, err := auth.CreateUser("authority-viewer", "authority-viewer-password", authn.RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	memberSession, err := auth.LoginAs(member.Login, "authority-viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	global, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, signature, userHandle, credential, challenge, identityID := signedTestGenesis(t)
	if err := global.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	identityState, err := authority.IdentityStateFromStore(global, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := authority.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: global, OwnerAuthority: ownerStore, Management: products})
	memberRead := httptest.NewRecorder()
	handler.ServeHTTP(memberRead, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/owner-authority/subjects", "", memberSession))
	if memberRead.Code != http.StatusForbidden {
		t.Fatalf("member OwnerSubject read status=%d body=%s, want forbidden", memberRead.Code, memberRead.Body.String())
	}
	now := time.Now().UTC()
	subject, err := authority.NewPersonOwnerSubject(identityID, "1", now, strings.NewReader("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject, authority.SubjectCreatePayload{Subject: subject}, identityState, now)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := authority.SignAuthorityEvent(event, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(authority.AuthorityEventWire{Event: signed.Event, Signature: base64.RawURLEncoding.EncodeToString(signed.Signature)})
	if err != nil {
		t.Fatal(err)
	}
	withoutCSRF := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(body), session)
	withoutCSRF.Header.Del("X-CSRF-Token")
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, withoutCSRF)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF status=%d body=%s", denied.Code, denied.Body.String())
	}

	created := httptest.NewRecorder()
	handler.ServeHTTP(created, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(body), session))
	if created.Code != http.StatusOK || !strings.Contains(created.Body.String(), `"subject_id":"`+subject.ID+`"`) || !strings.Contains(created.Body.String(), `"archive_ids":[]`) || strings.Contains(created.Body.String(), "private_key") {
		t.Fatalf("signed subject create status=%d body=%s", created.Code, created.Body.String())
	}
	archiveID := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	current, err := ownerStore.Snapshot(subject.ID, resolverForGlobalIdentity(global), now)
	if err != nil {
		t.Fatal(err)
	}
	claimEvent, err := authority.NewAuthorityEvent(subject.ID, "2", current.HeadHash, authority.AuthorityClaimArchive, authority.ArchiveClaimPayload{ArchiveID: archiveID}, identityState, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	claimSigned, err := authority.SignAuthorityEvent(claimEvent, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	claimBody, err := json.Marshal(authority.AuthorityEventWire{Event: claimSigned.Event, Signature: base64.RawURLEncoding.EncodeToString(claimSigned.Signature)})
	if err != nil {
		t.Fatal(err)
	}
	claimed := httptest.NewRecorder()
	handler.ServeHTTP(claimed, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(claimBody), session))
	if claimed.Code != http.StatusOK || !strings.Contains(claimed.Body.String(), `"archive_ids":["`+archiveID+`"]`) {
		t.Fatalf("archive claim status=%d body=%s", claimed.Code, claimed.Body.String())
	}
	otherSubject, err := authority.NewPersonOwnerSubject(identityID, "1", now.Add(2*time.Second), strings.NewReader("fedcba9876543210"))
	if err != nil {
		t.Fatal(err)
	}
	otherCreate, err := authority.NewAuthorityEvent(otherSubject.ID, "1", "", authority.AuthorityCreateSubject, authority.SubjectCreatePayload{Subject: otherSubject, ArchiveIDs: []string{archiveID}}, identityState, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	otherSigned, err := authority.SignAuthorityEvent(otherCreate, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	otherBody, err := json.Marshal(authority.AuthorityEventWire{Event: otherSigned.Event, Signature: base64.RawURLEncoding.EncodeToString(otherSigned.Signature)})
	if err != nil {
		t.Fatal(err)
	}
	duplicateClaim := httptest.NewRecorder()
	handler.ServeHTTP(duplicateClaim, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(otherBody), session))
	if duplicateClaim.Code != http.StatusConflict || !strings.Contains(duplicateClaim.Body.String(), "archive_already_claimed") {
		t.Fatalf("duplicate archive claim status=%d body=%s", duplicateClaim.Code, duplicateClaim.Body.String())
	}
	competingSubject := subject
	competingSubject.CreatedAt = competingSubject.CreatedAt.Add(time.Second)
	competingEvent, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject, authority.SubjectCreatePayload{Subject: competingSubject}, identityState, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	competingSigned, err := authority.SignAuthorityEvent(competingEvent, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	competingBody, err := json.Marshal(authority.AuthorityEventWire{Event: competingSigned.Event, Signature: base64.RawURLEncoding.EncodeToString(competingSigned.Signature)})
	if err != nil {
		t.Fatal(err)
	}
	competingResponse := httptest.NewRecorder()
	handler.ServeHTTP(competingResponse, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(competingBody), session))
	if competingResponse.Code != http.StatusConflict || !strings.Contains(competingResponse.Body.String(), "owner_authority_fork") {
		t.Fatalf("competing valid initial subject status=%d body=%s", competingResponse.Code, competingResponse.Body.String())
	}
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/owner-authority/subjects/"+subject.ID, "", session))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), identityID) {
		t.Fatalf("subject read status=%d body=%s", get.Code, get.Body.String())
	}
	history := httptest.NewRecorder()
	handler.ServeHTTP(history, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/owner-authority/subjects/"+subject.ID+"/events", "", session))
	if history.Code != http.StatusOK || !strings.Contains(history.Body.String(), `"events":[`) || !strings.Contains(history.Body.String(), `"forks":[{"first_event"`) || strings.Contains(history.Body.String(), string(ed25519.PrivateKey(rootKey))) {
		t.Fatalf("subject history status=%d body=%s", history.Code, history.Body.String())
	}
	audit := products.Audit(5)
	if len(audit) != 2 || audit[0].Type != "owner_subject_event_applied" || audit[0].ObjectID != subject.ID || audit[1].Type != "owner_subject_event_applied" || audit[1].ObjectID != subject.ID {
		t.Fatalf("OwnerSubject event audit=%+v", audit)
	}
	parsed, err := identityprotocol.IdentityID(rootKey.Public().(ed25519.PublicKey))
	if err != nil || parsed != identityID {
		t.Fatalf("test identity fixture invalid: %s %v", parsed, err)
	}
}

func TestOwnerAuthorityMutationPublishesHistoryToEveryConfiguredMirror(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("local-admin", "owner-authority-federation-password"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("local-admin", "owner-authority-federation-password")
	if err != nil {
		t.Fatal(err)
	}
	global, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey, genesis, signature, userHandle, credential, challenge, identityID := signedTestGenesis(t)
	if err := global.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	identityState, err := authority.IdentityStateFromStore(global, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := authority.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
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
	transport := &serverAuthorityDirectoryTransport{
		handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)},
		failures: map[string]bool{"two.example": true},
	}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, GlobalIdentity: global, OwnerAuthority: ownerStore, IdentityFederation: federation, Management: products})
	now := time.Now().UTC().Add(time.Second).Truncate(time.Second)
	subject, err := authority.NewPersonOwnerSubject(identityID, "1", now, strings.NewReader("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject, authority.SubjectCreatePayload{Subject: subject}, identityState, now)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := authority.SignAuthorityEvent(event, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(authority.AuthorityEventWire{Event: signed.Event, Signature: base64.RawURLEncoding.EncodeToString(signed.Signature)})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(body), session))
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "owner_authority_federation_incomplete") {
		t.Fatalf("partial authority publication status=%d body=%s", response.Code, response.Body.String())
	}
	// Retry the exact signed event after the failed publication. The server
	// first converges the already committed local head, then treats the event
	// as an idempotent retry instead of creating a competing branch.
	transport.failures["two.example"] = false
	retry := httptest.NewRecorder()
	handler.ServeHTTP(retry, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/owner-authority/events", string(body), session))
	if retry.Code != http.StatusOK {
		t.Fatalf("exact authority event retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	for label, directory := range map[string]*discovery.Directory{"first": first, "second": second} {
		histories, err := directory.LookupOwnerAuthorityHistories(subject.ID)
		if err != nil || len(histories) != 1 {
			t.Fatalf("%s mirror histories=%d err=%v", label, len(histories), err)
		}
		var bundle discovery.OwnerAuthorityHistoryBundle
		if err := json.Unmarshal(histories[0], &bundle); err != nil || bundle.Identity != identityID || bundle.Subject != subject.ID || len(bundle.Events) != 1 {
			t.Fatalf("%s mirrored bundle=%+v err=%v", label, bundle, err)
		}
	}
}

type serverAuthorityDirectoryTransport struct {
	handlers map[string]http.Handler
	failures map[string]bool
}

func (transport serverAuthorityDirectoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.failures[request.URL.Hostname()] {
		return nil, context.DeadlineExceeded
	}
	handler := transport.handlers[request.URL.Hostname()]
	if handler == nil {
		return nil, context.DeadlineExceeded
	}
	copy := request.Clone(request.Context())
	copy.RemoteAddr = "192.0.2.53:443"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, copy)
	response := recorder.Result()
	response.Request = request
	return response, nil
}

func resolverForGlobalIdentity(store *globalidentity.Store) authority.AuthorityIdentityResolver {
	return authority.IdentityStateResolverFromLocalAndRegistry(store, nil)
}
