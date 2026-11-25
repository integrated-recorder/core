package authority

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestAuthorityStorePersistsGrantRevocationAndRevalidatesOnRestart(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubject(t, store, f, resolve)

	grant := f.rootGrant()
	chain := []SignedGrant{mustSign(t, grant, f.rootPrivate)}
	issue := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, store, f.subject.ID, resolve, f.now), AuthorityIssueGrant, GrantIssuePayload{Chain: chain}, f.now.Add(time.Minute))
	if err := store.Apply(issue, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatalf("record grant: %v", err)
	}
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(time.Minute), f.delegate, f.archiveID, "", "archive.view"); err != nil {
		t.Fatalf("fresh delegated grant denied: %v", err)
	}
	if err := store.AuthorizeChain(f.subject.ID, chain, resolve, f.now.Add(time.Minute), f.delegate, f.archiveID, "", "archive.view"); err != nil {
		t.Fatalf("exact committed grant chain denied: %v", err)
	}
	modifiedChain := cloneGrantChain(chain)
	modifiedChain[0].Grant.Actions = []string{"archive.delete"}
	if err := store.AuthorizeChain(f.subject.ID, modifiedChain, resolve, f.now.Add(time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrDenied) {
		t.Fatalf("uncommitted grant chain accepted: %v", err)
	}

	hash, err := GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}
	revoke := signedAuthorityEvent(t, f, f.subject.ID, "3", mustHead(t, store, f.subject.ID, resolve, f.now.Add(time.Minute)), AuthorityRevokeGrant, GrantRevokePayload{GrantHash: hash}, f.now.Add(2*time.Minute))
	if err := store.Apply(revoke, resolve, f.now.Add(2*time.Minute)); err != nil {
		t.Fatalf("record revocation: %v", err)
	}
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(2*time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrDenied) {
		t.Fatalf("revoked grant authorization = %v, want denied", err)
	}
	if err := store.AuthorizeChain(f.subject.ID, chain, resolve, f.now.Add(2*time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("revoked command grant authorization = %v, want revoked", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reopened.Snapshot(f.subject.ID, resolve, f.now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("read persisted authority after restart: %v", err)
	}
	if snapshot.Subject.ID != f.subject.ID || snapshot.Sequence != "3" || snapshot.HeadHash == "" || len(snapshot.RevokedHashes) != 1 {
		t.Fatalf("unexpected persisted authority state: %+v", snapshot)
	}
	info, err := os.Stat(filepath.Join(root, "security", "owner-authority-v1.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("authority state permissions: info=%v err=%v", info, err)
	}
}

func TestAuthorityStoreRequiresArchiveClaimBeforeGrantAndPersistsClaim(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubjectWithArchives(t, store, f, resolve, nil)

	grant := f.rootGrant()
	unclaimed := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, store, f.subject.ID, resolve, f.now), AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{mustSign(t, grant, f.rootPrivate)}}, f.now.Add(time.Minute))
	if err := store.Apply(unclaimed, resolve, f.now.Add(time.Minute)); !errors.Is(err, ErrArchiveUnclaimed) {
		t.Fatalf("unclaimed archive grant error=%v, want ErrArchiveUnclaimed", err)
	}

	claim := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, store, f.subject.ID, resolve, f.now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, f.now.Add(time.Minute))
	if err := store.Apply(claim, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatalf("claim archive: %v", err)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reopened.Snapshot(f.subject.ID, resolve, f.now.Add(time.Minute))
	if err != nil || snapshot.Sequence != "2" || !containsArchiveID(snapshot.ArchiveIDs, f.archiveID) {
		t.Fatalf("persisted archive claim snapshot=%+v err=%v", snapshot, err)
	}
	issue := signedAuthorityEvent(t, f, f.subject.ID, "3", snapshot.HeadHash, AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{mustSign(t, grant, f.rootPrivate)}}, f.now.Add(2*time.Minute))
	if err := reopened.Apply(issue, resolve, f.now.Add(2*time.Minute)); err != nil {
		t.Fatalf("grant after archive claim: %v", err)
	}
}

func TestOwnerSubjectReplicationPolicyIsOwnerSignedBoundedAndDurable(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubject(t, store, f, resolve)

	initial, err := store.Snapshot(f.subject.ID, resolve, f.now)
	if err != nil || initial.ReplicationPolicy != DefaultReplicationPolicy() {
		t.Fatalf("new OwnerSubject default replication policy=%+v err=%v", initial.ReplicationPolicy, err)
	}
	policy := ReplicationPolicy{
		MinimumReplicas: "2", GracePeriodSeconds: "86400", CapacityLimitBytes: "1073741824", RelocateAfterGrace: true,
	}
	event := signedAuthorityEvent(t, f, f.subject.ID, "2", initial.HeadHash, AuthoritySetReplicationPolicy, ReplicationPolicyPayload{Policy: policy}, f.now.Add(time.Minute))
	secondStore, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var writers sync.WaitGroup
	for _, writer := range []*Store{store, secondStore} {
		writers.Add(1)
		go func(writer *Store) {
			defer writers.Done()
			<-start
			results <- writer.Apply(event, resolve, f.now.Add(time.Minute))
		}(writer)
	}
	close(start)
	writers.Wait()
	close(results)
	for applyErr := range results {
		if applyErr != nil {
			t.Fatalf("concurrent exact policy event retry was not idempotent: %v", applyErr)
		}
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	current, err := reopened.Snapshot(f.subject.ID, resolve, f.now.Add(2*time.Minute))
	if err != nil || current.Sequence != "2" || current.ReplicationPolicy != policy {
		t.Fatalf("replication policy did not survive signed-history replay: state=%+v err=%v", current, err)
	}

	invalid := policy
	invalid.MinimumReplicas = "17"
	invalidEvent := signedAuthorityEvent(t, f, f.subject.ID, "3", current.HeadHash, AuthoritySetReplicationPolicy, ReplicationPolicyPayload{Policy: invalid}, f.now.Add(2*time.Minute))
	if err := reopened.Apply(invalidEvent, resolve, f.now.Add(2*time.Minute)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("out-of-range replica policy error=%v, want ErrInvalid", err)
	}

	nonOwnerEvent, err := NewAuthorityEventForSigner(f.subject.ID, "3", current.HeadHash, AuthoritySetReplicationPolicy,
		ReplicationPolicyPayload{Policy: DefaultReplicationPolicy()}, f.delegate, f.context().Identity, f.now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	nonOwnerSigned, err := SignAuthorityEvent(nonOwnerEvent, f.delegatePrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Apply(nonOwnerSigned, resolve, f.now.Add(2*time.Minute)); !errors.Is(err, ErrDenied) {
		t.Fatalf("delegated Node changed OwnerSubject policy: %v", err)
	}
	unchanged, err := reopened.Snapshot(f.subject.ID, resolve, f.now.Add(2*time.Minute))
	if err != nil || unchanged.Sequence != "2" || unchanged.ReplicationPolicy != policy {
		t.Fatalf("rejected policy updates changed durable state: %+v err=%v", unchanged, err)
	}
}

func TestAuthorityStoreSerializesArchiveClaimsAcrossSubjectsAndStores(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	first, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	firstSubject := f.subject
	secondSubject, err := NewPersonOwnerSubject(f.identityID, "1", f.now, bytes.NewReader(bytes.Repeat([]byte{0x52}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	if firstSubject.ID == secondSubject.ID {
		t.Fatal("test subject IDs unexpectedly match")
	}
	createSubjectFor(t, first, f, resolve, firstSubject, nil)
	createSubjectFor(t, second, f, resolve, secondSubject, nil)

	firstEvent := signedAuthorityEvent(t, f, firstSubject.ID, "2", mustHead(t, first, firstSubject.ID, resolve, f.now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, f.now.Add(time.Minute))
	secondEvent := signedAuthorityEvent(t, f, secondSubject.ID, "2", mustHead(t, second, secondSubject.ID, resolve, f.now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, f.now.Add(time.Minute))
	start := make(chan struct{})
	errorsOut := make(chan error, 2)
	var wg sync.WaitGroup
	for _, attempt := range []struct {
		store *Store
		event SignedAuthorityEvent
	}{{first, firstEvent}, {second, secondEvent}} {
		wg.Add(1)
		go func(store *Store, event SignedAuthorityEvent) {
			defer wg.Done()
			<-start
			errorsOut <- store.Apply(event, resolve, f.now.Add(time.Minute))
		}(attempt.store, attempt.event)
	}
	close(start)
	wg.Wait()
	close(errorsOut)
	successes, claimed := 0, 0
	for applyErr := range errorsOut {
		switch {
		case applyErr == nil:
			successes++
		case errors.Is(applyErr, ErrArchiveClaimed):
			claimed++
		default:
			t.Fatalf("concurrent archive claim error=%v", applyErr)
		}
	}
	if successes != 1 || claimed != 1 {
		t.Fatalf("concurrent claims: accepted=%d conflict=%d, want one each", successes, claimed)
	}
}

func TestAuthorityStoreReservesArchiveIDsInFrozenClaimForks(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubjectWithArchives(t, store, f, resolve, nil)
	otherArchive := "arc_BBBBBBBBBBBBBBBBBBBBBB"
	parent := mustHead(t, store, f.subject.ID, resolve, f.now)
	first := signedAuthorityEvent(t, f, f.subject.ID, "2", parent, AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, f.now.Add(time.Minute))
	competing := signedAuthorityEvent(t, f, f.subject.ID, "2", parent, AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: otherArchive}, f.now.Add(time.Minute))
	if err := store.Apply(first, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(competing, resolve, f.now.Add(time.Minute)); !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("competing claim error=%v, want a frozen fork", err)
	}
	otherSubject, err := NewPersonOwnerSubject(f.identityID, "1", f.now, bytes.NewReader(bytes.Repeat([]byte{0x61}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	create := signedAuthorityEvent(t, f, otherSubject.ID, "1", "", AuthorityCreateSubject, SubjectCreatePayload{Subject: otherSubject, ArchiveIDs: []string{otherArchive}}, f.now)
	if err := store.Apply(create, resolve, f.now); !errors.Is(err, ErrArchiveClaimed) {
		t.Fatalf("claim from frozen fork was not reserved: %v", err)
	}
}

func TestAuthorityStoreRejectsUnsortedOrDuplicateGenesisArchiveClaims(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	first := "arc_AAAAAAAAAAAAAAAAAAAAAA"
	second := "arc_BBBBBBBBBBBBBBBBBBBBBB"
	for _, ids := range [][]string{{second, first}, {first, first}} {
		subject, subjectErr := NewPersonOwnerSubject(f.identityID, "1", f.now, bytes.NewReader(bytes.Repeat([]byte{byte(len(ids) + int(ids[0][4]))}, 16)))
		if subjectErr != nil {
			t.Fatal(subjectErr)
		}
		event := signedAuthorityEvent(t, f, subject.ID, "1", "", AuthorityCreateSubject, SubjectCreatePayload{Subject: subject, ArchiveIDs: ids}, f.now)
		if err := store.Apply(event, resolve, f.now); !errors.Is(err, ErrInvalid) {
			t.Fatalf("genesis Archive IDs %v error=%v, want ErrInvalid", ids, err)
		}
	}
}

func TestParseBrowserOwnerSubjectEventWire(t *testing.T) {
	// This mirrors the JSON shape produced by web/src/identity/authority.ts and
	// the first OwnerSubject event assembled by the settings UI. The parser is
	// deliberately tested separately from identity verification: HTTP clients
	// must be able to submit the signed event before authorization is evaluated.
	const browserEvent = `{"event":{"event_version":"ir.owner-authority.event/1","identity_id":"gi_79nODdjLD8IwHkh18VACHA","identity_head_hash":"sha256:b67bbeb0c0c4eb38dc9fe17a638568a7a1d47b7e2b9c85eec86f6ad708bf0124","subject_id":"os_AmY27vZTpM63uZAbevX5cg","sequence":"1","operation":"subject_create","issued_at":"2026-11-02T14:00:04Z","signer":{"kind":"identity","id":"gi_79nODdjLD8IwHkh18VACHA"},"signer_key_id":"ik_ivZtaioFqTk7S4riVjMzbWxfSetiR1PdHlQMMusDs5E","payload":{"subject":{"subject_id":"os_AmY27vZTpM63uZAbevX5cg","subject_type":"person","controller":{"kind":"identity","id":"gi_79nODdjLD8IwHkh18VACHA"},"controller_policy":"global_identity_recovery_root/1","policy_epoch":"1","created_at":"2026-11-02T14:00:04Z"}}},"signature":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}`
	if _, err := ParseAuthorityEvent([]byte(browserEvent)); err != nil {
		t.Fatalf("parse browser OwnerSubject event: %v", err)
	}
}

func TestBrowserURLCapabilitySignatureSurvivesAuthorityEventApplication(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubject(t, store, f, resolve)

	grant := f.rootGrant()
	signedGrant, err := SignGrant(grant, f.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	// This round trip mirrors browser JSON: inner signatures are unpadded
	// base64url strings embedded in the signed authority-event payload.
	event := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, store, f.subject.ID, resolve, f.now), AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{signedGrant}}, f.now.Add(time.Minute))
	eventSignature := base64.RawURLEncoding.EncodeToString(event.Signature)
	wire, err := json.Marshal(struct {
		Event     AuthorityEvent `json:"event"`
		Signature string         `json:"signature"`
	}{Event: event.Event, Signature: eventSignature})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseAuthorityEvent(wire)
	if err != nil {
		t.Fatalf("parse browser event: %v", err)
	}
	if err := store.Apply(parsed, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatalf("apply browser-signed grant event: %v", err)
	}
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(time.Minute), f.delegate, f.archiveID, "", "archive.view"); err != nil {
		t.Fatalf("browser-issued grant did not authorize its exact scope: %v", err)
	}

	encoded, err := json.Marshal(signedGrant)
	var grantWire signedGrantWire
	decodeErr := json.Unmarshal(encoded, &grantWire)
	if err != nil || decodeErr != nil || grantWire.Signature != base64.RawURLEncoding.EncodeToString(signedGrant.Signature) {
		t.Fatalf("signed grant did not use canonical base64url JSON: %s marshal=%v decode=%v", encoded, err, decodeErr)
	}
}

func TestAuthorityStoreFreezesOnlyForTwoValidEventsAtSameParent(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubject(t, store, f, resolve)
	parent := mustHead(t, store, f.subject.ID, resolve, f.now)

	firstGrant := f.rootGrant()
	first := signedAuthorityEvent(t, f, f.subject.ID, "2", parent, AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{mustSign(t, firstGrant, f.rootPrivate)}}, f.now.Add(time.Minute))
	if err := store.Apply(first, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatalf("first branch: %v", err)
	}

	secondGrant := f.rootGrant()
	secondGrant.Nonce = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x44}, 32))
	second := signedAuthorityEvent(t, f, f.subject.ID, "2", parent, AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{mustSign(t, secondGrant, f.rootPrivate)}}, f.now.Add(time.Minute))
	if err := store.Apply(second, resolve, f.now.Add(time.Minute)); !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("valid competing event result = %v, want frozen fork", err)
	}
	snapshot, err := store.Snapshot(f.subject.ID, resolve, f.now.Add(time.Minute))
	if err != nil || !snapshot.Frozen || snapshot.ForkCount != 1 {
		t.Fatalf("fork snapshot=%+v err=%v", snapshot, err)
	}
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("authorization while frozen = %v, want fork", err)
	}
	if err := store.Apply(second, resolve, f.now.Add(time.Minute)); !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("append while frozen = %v, want fork", err)
	}
}

func TestAuthorityStoreRejectsMalformedCompetingEventWithoutFreezing(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	createAuthoritySubject(t, store, f, resolve)
	parent := mustHead(t, store, f.subject.ID, resolve, f.now)
	valid := signedAuthorityEvent(t, f, f.subject.ID, "2", parent, AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{mustSign(t, f.rootGrant(), f.rootPrivate)}}, f.now.Add(time.Minute))
	if err := store.Apply(valid, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	malformed := signedAuthorityEvent(t, f, f.subject.ID, "2", parent, AuthorityIssueGrant, GrantIssuePayload{}, f.now.Add(time.Minute))
	if err := store.Apply(malformed, resolve, f.now.Add(time.Minute)); !errors.Is(err, ErrAuthorityStale) {
		t.Fatalf("malformed competing event result=%v, want rejected", err)
	}
	snapshot, err := store.Snapshot(f.subject.ID, resolve, f.now.Add(time.Minute))
	if err != nil || snapshot.Frozen {
		t.Fatalf("malformed event froze state: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestAuthorityStoreConcurrentInitialCreationFreezesFork(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	firstStore, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve

	first := signedAuthorityEvent(t, f, f.subject.ID, "1", "", AuthorityCreateSubject, SubjectCreatePayload{Subject: f.subject}, f.now)
	initialIdentity := f.context().Identity
	if err := validateAuthorityEvent(first.Event); err != nil {
		t.Fatalf("initial event shape: %v", err)
	}
	if err := verifyEventSignature(first, initialIdentity); err != nil {
		t.Fatalf("initial event signature: %v", err)
	}
	if _, err := verifyAndApplyFirst(first, initialIdentity, f.now); err != nil {
		t.Fatalf("initial subject semantics: %v", err)
	}
	competingSubject := f.subject
	competingSubject.CreatedAt = competingSubject.CreatedAt.Add(time.Second)
	competing := signedAuthorityEvent(t, f, f.subject.ID, "1", "", AuthorityCreateSubject, SubjectCreatePayload{Subject: competingSubject}, f.now.Add(time.Second))
	start := make(chan struct{})
	type concurrentResult struct {
		name string
		err  error
	}
	results := make(chan concurrentResult, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		results <- concurrentResult{name: "first", err: firstStore.Apply(first, resolve, f.now)}
	}()
	go func() {
		defer workers.Done()
		<-start
		results <- concurrentResult{name: "second", err: secondStore.Apply(competing, resolve, f.now.Add(time.Second))}
	}()
	close(start)
	workers.Wait()
	close(results)
	successes, forksSeen := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			successes++
		case errors.Is(result.err, ErrAuthorityFork):
			forksSeen++
		default:
			t.Fatalf("concurrent valid initial event %s result=%v", result.name, result.err)
		}
	}
	if successes != 1 || forksSeen != 1 {
		t.Fatalf("concurrent initial event results success=%d fork=%d", successes, forksSeen)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reopened.Snapshot(f.subject.ID, resolve, f.now.Add(2*time.Second))
	if err != nil || !snapshot.Frozen || snapshot.ForkCount != 1 {
		t.Fatalf("reopened initial fork snapshot=%+v err=%v", snapshot, err)
	}
	events, forks, err := reopened.ExportEvidence(f.subject.ID)
	if err != nil || len(events) != 1 || len(forks) != 1 || forks[0].PreviousHash != "" {
		t.Fatalf("reopened fork evidence events=%d forks=%+v err=%v", len(events), forks, err)
	}
	firstHash, _ := AuthorityEventHash(forks[0].First.Event)
	secondHash, _ := AuthorityEventHash(forks[0].Second.Event)
	if firstHash == secondHash {
		t.Fatal("fork evidence collapsed to a single branch")
	}
	if err := reopened.Authorize(f.subject.ID, resolve, f.now.Add(2*time.Second), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("authorization with competing initial subject events = %v, want frozen", err)
	}
}

func TestAuthorityStoreExactInitialEventRetryIsIdempotent(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	event := signedAuthorityEvent(t, f, f.subject.ID, "1", "", AuthorityCreateSubject, SubjectCreatePayload{Subject: f.subject}, f.now)
	for attempt := 0; attempt < 2; attempt++ {
		if err := store.Apply(event, resolve, f.now); err != nil {
			t.Fatalf("exact initial event retry %d: %v", attempt+1, err)
		}
	}
	snapshot, err := store.Snapshot(f.subject.ID, resolve, f.now)
	if err != nil || snapshot.Frozen || snapshot.Sequence != "1" {
		t.Fatalf("idempotent subject snapshot=%+v err=%v", snapshot, err)
	}
}

func TestAuthorityStoreAcceptsDelegatedNodeGrantAndRevocation(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := f.context().Identity
	resolve := staticAuthorityResolver{f.identityID: identity}.resolve
	createAuthoritySubject(t, store, f, resolve)

	rootGrant := f.rootGrant()
	rootSigned := mustSign(t, rootGrant, f.rootPrivate)
	rootHead := mustHead(t, store, f.subject.ID, resolve, f.now)
	rootIssue := signedAuthorityEvent(t, f, f.subject.ID, "2", rootHead, AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{rootSigned}}, f.now.Add(time.Minute))
	if err := store.Apply(rootIssue, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatalf("record root delegation: %v", err)
	}
	rootGrantHash, err := GrantHash(rootGrant)
	if err != nil {
		t.Fatal(err)
	}

	childGrant := f.grant(f.delegate, f.recipient, []string{"archive.view"}, "0")
	childGrant.ParentGrantHash = rootGrantHash
	childSigned := mustSign(t, childGrant, f.delegatePrivate)
	identityHead := mustHead(t, store, f.subject.ID, resolve, f.now.Add(time.Minute))
	childEvent, err := NewAuthorityEventForSigner(f.subject.ID, "3", identityHead, AuthorityIssueGrant, GrantIssuePayload{Chain: []SignedGrant{rootSigned, childSigned}}, f.delegate, identity, f.now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	childEventSigned, err := SignAuthorityEvent(childEvent, f.delegatePrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(childEventSigned, resolve, f.now.Add(2*time.Minute)); err != nil {
		t.Fatalf("record delegated Node grant: %v", err)
	}
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(2*time.Minute), f.recipient, f.archiveID, "", "archive.view"); err != nil {
		t.Fatalf("recipient grant denied before revocation: %v", err)
	}

	childHash, err := GrantHash(childGrant)
	if err != nil {
		t.Fatal(err)
	}
	revokeHead := mustHead(t, store, f.subject.ID, resolve, f.now.Add(2*time.Minute))
	revokeEvent, err := NewAuthorityEventForSigner(f.subject.ID, "4", revokeHead, AuthorityRevokeGrant, GrantRevokePayload{GrantHash: childHash}, f.delegate, identity, f.now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	revokeSigned, err := SignAuthorityEvent(revokeEvent, f.delegatePrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(revokeSigned, resolve, f.now.Add(3*time.Minute)); err != nil {
		t.Fatalf("Node revoke its delegated grant: %v", err)
	}
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(3*time.Minute), f.recipient, f.archiveID, "", "archive.view"); !errors.Is(err, ErrDenied) {
		t.Fatalf("recipient grant remains active after revocation: %v", err)
	}
}

func TestAuthorityStoreTransferPersistsEpochAndInvalidatesOldAuthority(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	oldState := f.context().Identity
	recipientState := IdentityState{
		IdentityID: f.recipient.ID, HeadHash: "sha256:" + string(bytes.Repeat([]byte{'1'}, 64)),
		VerifiedAt: f.now, FreshUntil: f.now.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{f.recipient.key(): f.recipientPrivate.Public().(ed25519.PublicKey)},
	}
	resolve := staticAuthorityResolver{f.identityID: oldState, f.recipient.ID: recipientState}.resolve
	createAuthoritySubject(t, store, f, resolve)

	offer, err := NewTransferOffer(f.context(), f.recipient, f.now.Add(time.Hour), bytes.NewReader(append([]byte("transfer-id-1234"), bytes.Repeat([]byte{0x77}, 32)...)))
	if err != nil {
		t.Fatal(err)
	}
	offer, err = BindRecipientHead(offer, recipientState, f.now)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignTransferOffer(offer, f.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := AcceptTransferOffer(signed, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	transfer := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, store, f.subject.ID, resolve, f.now), AuthorityTransfer, TransferPayload{Accepted: accepted}, f.now.Add(time.Minute))
	if err := store.Apply(transfer, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatalf("apply dual-consent transfer: %v", err)
	}
	store, err = Open(root)
	if err != nil {
		t.Fatalf("reopen transferred authority store: %v", err)
	}
	snapshot, err := store.Snapshot(f.subject.ID, resolve, f.now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Subject.Controller != f.recipient || snapshot.Subject.PolicyEpoch != "2" || snapshot.Sequence != "2" {
		t.Fatalf("transfer state was not persisted: %+v", snapshot)
	}
	if _, pending := snapshot.RekeyRequiredArchives[f.archiveID]; !pending {
		t.Fatalf("transfer did not durably derive rekey-required state for the previously claimed archive: %+v", snapshot.RekeyRequiredArchives)
	}

	newOwnerGrant := func(archiveID string, nonce byte) SignedGrant {
		t.Helper()
		publicKey := f.recipientPrivate.Public().(ed25519.PublicKey)
		keyID, err := identityprotocol.KeyID(publicKey)
		if err != nil {
			t.Fatal(err)
		}
		issuedAt := f.now.Add(3 * time.Minute)
		grant := Grant{
			Version: ProfileVersion, SubjectID: snapshot.Subject.ID, Issuer: f.recipient,
			Audience: f.delegate, ArchiveID: archiveID, Actions: []string{"archive.view"},
			NotBefore: issuedAt.UTC().Format(time.RFC3339), ExpiresAt: issuedAt.Add(time.Hour).UTC().Format(time.RFC3339),
			DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{nonce}, 32)),
			PolicyEpoch: snapshot.Subject.PolicyEpoch, IssuerKeyID: keyID,
		}
		signed, err := SignGrant(grant, f.recipientPrivate)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	oldArchiveGrant := newOwnerGrant(f.archiveID, 0x51)
	if err := store.Authorize(f.subject.ID, resolve, f.now.Add(2*time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrRekeyRequired) {
		t.Fatalf("archive authorization did not preserve the transfer rekey gate: %v", err)
	}
	if err := store.AuthorizeChain(f.subject.ID, []SignedGrant{oldArchiveGrant}, resolve, f.now.Add(2*time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrRekeyRequired) {
		t.Fatalf("remote command authorization did not preserve the transfer rekey gate: %v", err)
	}
	blockedEvent, err := NewAuthorityEventForSigner(f.subject.ID, "3", snapshot.HeadHash, AuthorityIssueGrant,
		GrantIssuePayload{Chain: []SignedGrant{oldArchiveGrant}}, f.recipient, recipientState, f.now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	blockedSigned, err := SignAuthorityEvent(blockedEvent, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(blockedSigned, resolve, f.now.Add(3*time.Minute)); !errors.Is(err, ErrRekeyRequired) {
		t.Fatalf("new controller reissued a grant before archive keys were rekeyed: %v", err)
	}

	claimNewArchive, err := NewAuthorityEventForSigner(f.subject.ID, "3", snapshot.HeadHash, AuthorityClaimArchive,
		ArchiveClaimPayload{ArchiveID: f.otherArchive}, f.recipient, recipientState, f.now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	claimNewArchiveSigned, err := SignAuthorityEvent(claimNewArchive, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(claimNewArchiveSigned, resolve, f.now.Add(3*time.Minute)); err != nil {
		t.Fatalf("new controller could not claim a new, unrelated archive: %v", err)
	}
	newArchiveSnapshot, err := store.Snapshot(f.subject.ID, resolve, f.now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	newArchiveGrant := newOwnerGrant(f.otherArchive, 0x52)
	grantEvent, err := NewAuthorityEventForSigner(f.subject.ID, "4", newArchiveSnapshot.HeadHash, AuthorityIssueGrant,
		GrantIssuePayload{Chain: []SignedGrant{newArchiveGrant}}, f.recipient, recipientState, f.now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	grantEventSigned, err := SignAuthorityEvent(grantEvent, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(grantEventSigned, resolve, f.now.Add(4*time.Minute)); err != nil {
		t.Fatalf("rekey pending for one archive blocked a grant on a newly claimed archive: %v", err)
	}
	policy := ReplicationPolicy{MinimumReplicas: "2", GracePeriodSeconds: "7200", CapacityLimitBytes: "0", RelocateAfterGrace: true}
	previousHash := mustHead(t, store, f.subject.ID, resolve, f.now.Add(4*time.Minute))
	policyTime := f.now.Add(4*time.Minute + 30*time.Second)
	policyEvent, err := NewAuthorityEventForSigner(f.subject.ID, "5", previousHash, AuthoritySetReplicationPolicy,
		ReplicationPolicyPayload{Policy: policy}, f.recipient, recipientState, policyTime)
	if err != nil {
		t.Fatal(err)
	}
	policySigned, err := SignAuthorityEvent(policyEvent, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(policySigned, resolve, policyTime); err != nil {
		t.Fatalf("new controller could not update the retained subject policy: %v", err)
	}
	transferred, err := store.Snapshot(f.subject.ID, resolve, policyTime)
	if err != nil || transferred.ReplicationPolicy != policy || transferred.Subject.Controller != f.recipient {
		t.Fatalf("transferred subject policy=%+v controller=%+v err=%v", transferred.ReplicationPolicy, transferred.Subject.Controller, err)
	}
}

func TestNewControllerMustAcknowledgeCompleteRekeyForEveryTransferredArchive(t *testing.T) {
	f := newAuthorityFixture(t)
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	oldState := f.context().Identity
	recipientState := IdentityState{
		IdentityID: f.recipient.ID, HeadHash: "sha256:" + strings.Repeat("1", 64),
		VerifiedAt: f.now, FreshUntil: f.now.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{f.recipient.key(): f.recipientPrivate.Public().(ed25519.PublicKey)},
	}
	recipientEncryptionPrivate, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{0x71}, 32))
	if err != nil {
		t.Fatal(err)
	}
	recipientState.RecoveryEncryptionPublicKey = recipientEncryptionPrivate.PublicKey().Bytes()
	resolve := staticAuthorityResolver{f.identityID: oldState, f.recipient.ID: recipientState}.resolve
	archiveIDs := []string{f.archiveID, f.otherArchive}
	sort.Strings(archiveIDs)
	createAuthoritySubjectWithArchives(t, store, f, resolve, archiveIDs)
	offer, err := NewTransferOffer(f.context(), f.recipient, f.now.Add(time.Hour), bytes.NewReader(append([]byte("rekey-complete-test"), bytes.Repeat([]byte{0x61}, 32)...)))
	if err != nil {
		t.Fatal(err)
	}
	offer, err = BindRecipientHead(offer, recipientState, f.now)
	if err != nil {
		t.Fatal(err)
	}
	signedOffer, err := SignTransferOffer(offer, f.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := AcceptTransferOffer(signedOffer, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	transfer := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, store, f.subject.ID, resolve, f.now), AuthorityTransfer, TransferPayload{Accepted: accepted}, f.now.Add(time.Minute))
	if err := store.Apply(transfer, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	transferHash, err := AuthorityEventHash(transfer.Event)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(f.subject.ID, resolve, f.now.Add(2*time.Minute))
	if err != nil || snapshot.RekeyRequiredSinceHash != transferHash {
		t.Fatalf("pending rekey transfer hash=%q want=%q err=%v", snapshot.RekeyRequiredSinceHash, transferHash, err)
	}

	recipientKeyID := cryptoarchive.RecipientKeyID(recipientState.RecoveryEncryptionPublicKey)
	evidence := func(archiveID string) ArchiveRekeyEvidence {
		return ArchiveRekeyEvidence{
			ArchiveID: archiveID, PreviousKeyEpoch: "4", NewKeyEpoch: "1700000000001", SnapshotCount: "2",
			HeadSnapshotID:      "snap_" + strings.Repeat("a", 64),
			BundleSetCommitment: "sha256:" + strings.Repeat("b", 64),
			EnvelopeSHA256:      "sha256:" + strings.Repeat("c", 64), RecipientKeyID: recipientKeyID,
		}
	}
	newEvent := func(payload CompleteRekeyPayload) SignedAuthorityEvent {
		t.Helper()
		event, err := NewAuthorityEventForSigner(f.subject.ID, "3", snapshot.HeadHash, AuthorityCompleteRekey,
			payload, f.recipient, recipientState, f.now.Add(3*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		signed, err := SignAuthorityEvent(event, f.recipientPrivate)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}

	partial := newEvent(CompleteRekeyPayload{TransferEventHash: transferHash, Archives: []ArchiveRekeyEvidence{evidence(archiveIDs[0])}})
	if err := store.Apply(partial, resolve, f.now.Add(3*time.Minute)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("partial archive rekey acknowledgement accepted: %v", err)
	}
	wrongTransfer := newEvent(CompleteRekeyPayload{TransferEventHash: "sha256:" + strings.Repeat("d", 64), Archives: []ArchiveRekeyEvidence{evidence(archiveIDs[0]), evidence(archiveIDs[1])}})
	if err := store.Apply(wrongTransfer, resolve, f.now.Add(3*time.Minute)); !errors.Is(err, ErrAuthorityStale) {
		t.Fatalf("rekey acknowledgement for another transfer accepted: %v", err)
	}
	wrongRecipient := evidence(archiveIDs[0])
	wrongRecipient.RecipientKeyID = cryptoarchive.RecipientKeyID(bytes.Repeat([]byte{0x72}, 32))
	wrongRecipientEvent := newEvent(CompleteRekeyPayload{TransferEventHash: transferHash, Archives: []ArchiveRekeyEvidence{wrongRecipient, evidence(archiveIDs[1])}})
	if err := store.Apply(wrongRecipientEvent, resolve, f.now.Add(3*time.Minute)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("rekey acknowledgement for an unrelated recovery key accepted: %v", err)
	}
	missingRecoveryKey := recipientState
	missingRecoveryKey.RecoveryEncryptionPublicKey = nil
	missingRecoveryKeyResolver := staticAuthorityResolver{f.identityID: oldState, f.recipient.ID: missingRecoveryKey}.resolve
	missingKeyEvent := newEvent(CompleteRekeyPayload{TransferEventHash: transferHash, Archives: []ArchiveRekeyEvidence{evidence(archiveIDs[0]), evidence(archiveIDs[1])}})
	if err := store.Apply(missingKeyEvent, missingRecoveryKeyResolver, f.now.Add(3*time.Minute)); !errors.Is(err, ErrDenied) {
		t.Fatalf("rekey acknowledgement accepted without a verified recipient recovery key: %v", err)
	}
	valid := newEvent(CompleteRekeyPayload{TransferEventHash: transferHash, Archives: []ArchiveRekeyEvidence{evidence(archiveIDs[0]), evidence(archiveIDs[1])}})
	if err := store.Apply(valid, resolve, f.now.Add(3*time.Minute)); err != nil {
		t.Fatalf("new controller acknowledgement failed: %v", err)
	}
	if err := store.Apply(valid, resolve, f.now.Add(3*time.Minute)); err != nil {
		t.Fatalf("exact rekey completion retry was not idempotent: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := reopened.Snapshot(f.subject.ID, resolve, f.now.Add(4*time.Minute))
	if err != nil || len(completed.RekeyRequiredArchives) != 0 || completed.RekeyRequiredSinceHash != "" {
		t.Fatalf("rekey acknowledgement did not survive replay: pending=%v transferHash=%q err=%v", completed.RekeyRequiredArchives, completed.RekeyRequiredSinceHash, err)
	}
	if err := reopened.Authorize(f.subject.ID, resolve, f.now.Add(4*time.Minute), f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrDenied) {
		t.Fatalf("cleared rekey gate unexpectedly authorized an unissued grant: %v", err)
	}

	rootKeyID, err := identityprotocol.KeyID(f.recipientPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := f.now.Add(4 * time.Minute)
	grant := Grant{
		Version: ProfileVersion, SubjectID: completed.Subject.ID, Issuer: f.recipient, Audience: f.delegate,
		ArchiveID: f.archiveID, Actions: []string{"archive.view"},
		NotBefore: issuedAt.UTC().Format(time.RFC3339), ExpiresAt: issuedAt.Add(time.Hour).UTC().Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x39}, 32)),
		PolicyEpoch: completed.Subject.PolicyEpoch, IssuerKeyID: rootKeyID,
	}
	signedGrant, err := SignGrant(grant, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := NewAuthorityEventForSigner(f.subject.ID, "4", completed.HeadHash, AuthorityIssueGrant,
		GrantIssuePayload{Chain: []SignedGrant{signedGrant}}, f.recipient, recipientState, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := SignAuthorityEvent(issue, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Apply(issueSigned, resolve, issuedAt); err != nil {
		t.Fatalf("new controller could not issue a grant after its acknowledgement: %v", err)
	}
	if err := reopened.Authorize(f.subject.ID, resolve, issuedAt, f.delegate, f.archiveID, "", "archive.view"); err != nil {
		t.Fatalf("archive authorization remained blocked after rekey acknowledgement: %v", err)
	}
}

func TestCompleteRekeyPayloadIsCanonicalAndBounded(t *testing.T) {
	valid := ArchiveRekeyEvidence{
		ArchiveID: "arc_0123456789abcdefghijkl", PreviousKeyEpoch: "4", NewKeyEpoch: "1700000000001", SnapshotCount: "2",
		HeadSnapshotID:      "snap_" + strings.Repeat("a", 64),
		BundleSetCommitment: "sha256:" + strings.Repeat("b", 64),
		EnvelopeSHA256:      "sha256:" + strings.Repeat("c", 64),
		RecipientKeyID:      "rk_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x71}, 32)),
	}
	base := CompleteRekeyPayload{TransferEventHash: "sha256:" + strings.Repeat("d", 64), Archives: []ArchiveRekeyEvidence{valid}}
	if !validCompleteRekeyPayload(base) {
		t.Fatal("valid canonical completion evidence was rejected")
	}
	cases := map[string]func(*CompleteRekeyPayload){
		"missing archive evidence": func(payload *CompleteRekeyPayload) { payload.Archives = nil },
		"non-canonical epoch":      func(payload *CompleteRekeyPayload) { payload.Archives[0].NewKeyEpoch = "017" },
		"non-increasing epoch":     func(payload *CompleteRekeyPayload) { payload.Archives[0].NewKeyEpoch = "4" },
		"zero snapshots":           func(payload *CompleteRekeyPayload) { payload.Archives[0].SnapshotCount = "0" },
		"bad snapshot id":          func(payload *CompleteRekeyPayload) { payload.Archives[0].HeadSnapshotID = "snap_invalid" },
		"bad bundle commitment":    func(payload *CompleteRekeyPayload) { payload.Archives[0].BundleSetCommitment = "sha256:short" },
		"bad envelope hash":        func(payload *CompleteRekeyPayload) { payload.Archives[0].EnvelopeSHA256 = "sha256:short" },
		"bad recipient id":         func(payload *CompleteRekeyPayload) { payload.Archives[0].RecipientKeyID = "rk_invalid" },
		"bad transfer hash":        func(payload *CompleteRekeyPayload) { payload.TransferEventHash = "sha256:short" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			payload := CompleteRekeyPayload{
				TransferEventHash: base.TransferEventHash,
				Archives:          append([]ArchiveRekeyEvidence(nil), base.Archives...),
			}
			mutate(&payload)
			if validCompleteRekeyPayload(payload) {
				t.Fatalf("invalid completion payload accepted: %+v", payload)
			}
		})
	}
}

type staticAuthorityResolver map[string]IdentityState

func (resolver staticAuthorityResolver) resolve(identityID, headHash string, _ time.Time) (IdentityState, error) {
	state, ok := resolver[identityID]
	if !ok || (headHash != "" && state.HeadHash != headHash) {
		return IdentityState{}, ErrStaleIdentity
	}
	return state, nil
}

func createAuthoritySubject(t *testing.T, store *Store, f *authorityFixture, resolve AuthorityIdentityResolver) {
	t.Helper()
	createSubjectFor(t, store, f, resolve, f.subject, []string{f.archiveID})
}

func createAuthoritySubjectWithArchives(t *testing.T, store *Store, f *authorityFixture, resolve AuthorityIdentityResolver, archiveIDs []string) {
	t.Helper()
	createSubjectFor(t, store, f, resolve, f.subject, archiveIDs)
}

func createSubjectFor(t *testing.T, store *Store, f *authorityFixture, resolve AuthorityIdentityResolver, subject OwnerSubject, archiveIDs []string) {
	t.Helper()
	event := signedAuthorityEvent(t, f, subject.ID, "1", "", AuthorityCreateSubject, SubjectCreatePayload{Subject: subject, ArchiveIDs: archiveIDs}, f.now)
	if err := store.Apply(event, resolve, f.now); err != nil {
		t.Fatalf("create OwnerSubject: %v", err)
	}
}

func signedAuthorityEvent(t *testing.T, f *authorityFixture, subjectID, sequence, previousHash, operation string, payload any, issuedAt time.Time) SignedAuthorityEvent {
	t.Helper()
	event, err := NewAuthorityEvent(subjectID, sequence, previousHash, operation, payload, f.context().Identity, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignAuthorityEvent(event, f.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func mustHead(t *testing.T, store *Store, subjectID string, resolve AuthorityIdentityResolver, now time.Time) string {
	t.Helper()
	snapshot, err := store.Snapshot(subjectID, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.HeadHash
}
