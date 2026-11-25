package authority

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/discovery"
)

func TestSynchronizeFederatedAuthorityImportsCurrentHistoryAndFailsOnStaleMirror(t *testing.T) {
	f := newAuthorityFixture(t)
	now := f.now.Add(time.Minute)
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	source, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	target, err := Open(filepath.Join(t.TempDir(), "target"))
	if err != nil {
		t.Fatal(err)
	}
	createAuthoritySubjectWithArchives(t, source, f, resolve, nil)
	first, second := newAuthorityDirectory(t), newAuthorityDirectory(t)
	federation := newAuthorityFederation(t, first, second, nil)
	bundle, err := source.ExportFederatedHistory(f.subject.ID, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := federation.PublishOwnerAuthorityHistory(context.Background(), f.subject.ID, bundle); err != nil {
		t.Fatal(err)
	}
	if err := target.SynchronizeFederatedHistory(context.Background(), f.subject.ID, federation, resolve, now); err != nil {
		t.Fatalf("sync initial history: %v", err)
	}
	if snapshot, err := target.Snapshot(f.subject.ID, resolve, now); err != nil || snapshot.Sequence != "1" {
		t.Fatalf("target initial snapshot=%+v err=%v", snapshot, err)
	}
	claim := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, source, f.subject.ID, resolve, now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, now.Add(time.Minute))
	if err := source.Apply(claim, resolve, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	updated, err := source.ExportFederatedHistory(f.subject.ID, resolve, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	// A single stale mirror is not sufficient to advance authorization.
	if err := first.PublishOwnerAuthorityHistory(f.subject.ID, mustAuthorityBundleJSON(t, updated)); err != nil {
		t.Fatal(err)
	}
	if err := target.SynchronizeFederatedHistory(context.Background(), f.subject.ID, federation, resolve, now.Add(time.Minute)); !errors.Is(err, ErrAuthorityStale) {
		t.Fatalf("one-provider update was accepted: %v", err)
	}
	if _, err := federation.PublishOwnerAuthorityHistory(context.Background(), f.subject.ID, updated); err != nil {
		t.Fatal(err)
	}
	if err := target.SynchronizeFederatedHistory(context.Background(), f.subject.ID, federation, resolve, now.Add(time.Minute)); err != nil {
		t.Fatalf("sync agreed updated history: %v", err)
	}
	if snapshot, err := target.Snapshot(f.subject.ID, resolve, now.Add(time.Minute)); err != nil || snapshot.Sequence != "2" || !containsArchiveID(snapshot.ArchiveIDs, f.archiveID) {
		t.Fatalf("target updated snapshot=%+v err=%v", snapshot, err)
	}
}

func TestSynchronizeFederatedAuthorityFreezesValidCompetingSuccessors(t *testing.T) {
	f := newAuthorityFixture(t)
	resolve := staticAuthorityResolver{f.identityID: f.context().Identity}.resolve
	firstSource, err := Open(filepath.Join(t.TempDir(), "first-source"))
	if err != nil {
		t.Fatal(err)
	}
	secondSource, err := Open(filepath.Join(t.TempDir(), "second-source"))
	if err != nil {
		t.Fatal(err)
	}
	target, err := Open(filepath.Join(t.TempDir(), "target"))
	if err != nil {
		t.Fatal(err)
	}
	createSubjectFor(t, firstSource, f, resolve, f.subject, nil)
	createSubjectFor(t, secondSource, f, resolve, f.subject, nil)
	firstBase, err := firstSource.ExportFederatedHistory(f.subject.ID, resolve, f.now)
	if err != nil {
		t.Fatal(err)
	}
	secondBase, err := secondSource.ExportFederatedHistory(f.subject.ID, resolve, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if mustAuthorityBundleJSON(t, firstBase) == nil || mustAuthorityBundleJSON(t, secondBase) == nil {
		t.Fatal("failed to encode base histories")
	}
	firstClaim := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, firstSource, f.subject.ID, resolve, f.now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, f.now.Add(time.Minute))
	secondArchive := "arc_CCCCCCCCCCCCCCCCCCCCCC"
	secondClaim := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, secondSource, f.subject.ID, resolve, f.now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: secondArchive}, f.now.Add(time.Minute))
	if err := firstSource.Apply(firstClaim, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := secondSource.Apply(secondClaim, resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	first, second := newAuthorityDirectory(t), newAuthorityDirectory(t)
	federation := newAuthorityFederation(t, first, second, nil)
	for _, store := range []*Store{firstSource, secondSource} {
		bundle, err := store.ExportFederatedHistory(f.subject.ID, resolve, f.now.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := federation.PublishOwnerAuthorityHistory(context.Background(), f.subject.ID, bundle); err != nil {
			t.Fatal(err)
		}
	}
	err = target.SynchronizeFederatedHistory(context.Background(), f.subject.ID, federation, resolve, f.now.Add(time.Minute))
	if !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("competing valid histories error=%v, want frozen fork", err)
	}
	snapshot, err := target.Snapshot(f.subject.ID, resolve, f.now.Add(2*time.Minute))
	if err != nil || !snapshot.Frozen || snapshot.ForkCount != 1 {
		t.Fatalf("target fork snapshot=%+v err=%v", snapshot, err)
	}
}

func TestImportFederatedAuthorityAcceptsValidHistoricalIdentityHeads(t *testing.T) {
	f := newAuthorityFixture(t)
	oldIdentity := f.context().Identity
	now := f.now.Add(2 * time.Minute)
	currentIdentity := oldIdentity
	currentIdentity.HeadHash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	resolve := func(identityID, headHash string, at time.Time) (IdentityState, error) {
		if identityID != f.identityID {
			return IdentityState{}, ErrStaleIdentity
		}
		if headHash == "" {
			return currentIdentity, nil
		}
		if headHash == oldIdentity.HeadHash {
			return oldIdentity, nil
		}
		return IdentityState{}, ErrStaleIdentity
	}
	source, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	createAuthoritySubjectWithArchives(t, source, f, staticAuthorityResolver{f.identityID: oldIdentity}.resolve, nil)
	claim := signedAuthorityEvent(t, f, f.subject.ID, "2", mustHead(t, source, f.subject.ID, staticAuthorityResolver{f.identityID: oldIdentity}.resolve, f.now), AuthorityClaimArchive, ArchiveClaimPayload{ArchiveID: f.archiveID}, f.now.Add(time.Minute))
	if err := source.Apply(claim, staticAuthorityResolver{f.identityID: oldIdentity}.resolve, f.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	bundle, err := source.ExportFederatedHistory(f.subject.ID, staticAuthorityResolver{f.identityID: oldIdentity}.resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	target, err := Open(filepath.Join(t.TempDir(), "target"))
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ImportFederatedHistory(bundle, resolve, now); err != nil {
		t.Fatalf("import valid historical event heads: %v", err)
	}
	if snapshot, err := target.Snapshot(f.subject.ID, resolve, now); err != nil || snapshot.Sequence != "2" || !containsArchiveID(snapshot.ArchiveIDs, f.archiveID) {
		t.Fatalf("imported snapshot=%+v err=%v", snapshot, err)
	}
	if err := target.Apply(claim, resolve, now); !errors.Is(err, ErrStaleIdentity) {
		t.Fatalf("new submission at obsolete identity head error=%v, want stale", err)
	}
	badBundle := bundle
	badBundle.Events = append([]json.RawMessage(nil), bundle.Events...)
	var tampered SignedAuthorityEvent
	if err := json.Unmarshal(badBundle.Events[0], &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.Signature[0] ^= 0xff
	badBundle.Events[0], err = json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	invalidTarget, err := Open(filepath.Join(t.TempDir(), "invalid-target"))
	if err != nil {
		t.Fatal(err)
	}
	if err := invalidTarget.ImportFederatedHistory(badBundle, resolve, now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("tampered imported signature error=%v, want ErrInvalid", err)
	}
	if _, err := invalidTarget.Snapshot(f.subject.ID, resolve, now); !errors.Is(err, ErrAuthorityStore) {
		t.Fatalf("invalid history left durable authorization state: %v", err)
	}
}

func newAuthorityDirectory(t *testing.T) *discovery.Directory {
	t.Helper()
	directory, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "directory"))
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func newAuthorityFederation(t *testing.T, first, second *discovery.Directory, failures map[string]bool) *discovery.FederationClient {
	t.Helper()
	transport := &authorityDirectoryTransport{
		handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)},
		failures: failures,
	}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	return federation
}

func mustAuthorityBundleJSON(t *testing.T, bundle discovery.OwnerAuthorityHistoryBundle) []byte {
	t.Helper()
	body, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
