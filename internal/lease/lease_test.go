package lease

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

type testKeys map[string]ed25519.PublicKey

func (keys testKeys) VerifiedKey(_ context.Context, principal authority.Principal, _ time.Time) (ed25519.PublicKey, error) {
	key, ok := keys[principal.Kind+":"+principal.ID]
	if !ok {
		return nil, ErrDenied
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

type allowAuthority struct{ denied bool }

func (a allowAuthority) Authorize(_ context.Context, _, archiveID, snapshotID, action string, actor authority.Principal, chain []authority.SignedGrant, _ time.Time) error {
	if a.denied || !archiveIDPattern.MatchString(archiveID) || snapshotID != "" && !snapshotIDPattern.MatchString(snapshotID) || !actorValid(actor) || len(chain) == 0 {
		return ErrDenied
	}
	if action != "archive.replicate" && action != "archive.delete" {
		return ErrDenied
	}
	return nil
}

type objectMap map[string][]byte

func (objects objectMap) OpenCiphertext(_ context.Context, _, _, objectID string) (io.ReadCloser, error) {
	value, ok := objects[objectID]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(value)), nil
}

type leaseFixture struct {
	store           *Store
	root            string
	now             time.Time
	owner           authority.Principal
	ownerPrivate    ed25519.PrivateKey
	providerID      string
	providerPrivate ed25519.PrivateKey
	chain           []authority.SignedGrant
	offer           Offer
	offerHash       string
	ciphertext      []byte
}

func newLeaseFixture(t *testing.T) leaseFixture {
	t.Helper()
	_, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := identityprotocol.IdentityID(ownerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	_, providerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	providerID, err := identityprotocol.NodeID(providerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	objects := []byte("encrypted-object-payload")
	digest := sha256.Sum256(objects)
	archiveHash := sha256.Sum256([]byte("manifest"))
	owner := authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID}
	archiveID := "arc_aaaaaaaaaaaaaaaaaaaaaa"
	snapshotID := "snap_" + strings.Repeat("a", 64)
	subjectID := "os_aaaaaaaaaaaaaaaaaaaaaa"
	ownerKeyID, err := identityprotocol.KeyID(ownerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subjectID, Issuer: owner,
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: providerID},
		ArchiveID: archiveID, SnapshotID: snapshotID,
		Actions:   []string{"archive.delete", "archive.replicate"},
		NotBefore: now.Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: strings.Repeat("n", 43), PolicyEpoch: "1", IssuerKeyID: ownerKeyID,
	}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	chain := []authority.SignedGrant{grant}
	evidence, err := grantEvidenceHash(chain)
	if err != nil {
		t.Fatal(err)
	}
	offer := Offer{
		Version: ProtocolVersion, LeaseID: "rl_aaaaaaaaaaaaaaaaaaaaaa", ArchiveID: archiveID,
		SnapshotID: snapshotID, KeyEpoch: "1", PlaintextManifestHash: "sha256:" + hex.EncodeToString(archiveHash[:]),
		ReplicaManifestHash: "sha256:" + hex.EncodeToString(archiveHash[:]),
		EncryptedObjectSet:  []EncryptedObject{{ObjectID: "ob_aaaaaaaaaaaaaaaaaaaaaa", CiphertextSize: strconvItoa(len(objects)), CiphertextSHA256: "sha256:" + hex.EncodeToString(digest[:])}},
		ProviderNodeID:      providerID, OwnerSubjectID: subjectID, AuthorizedEvidence: evidence, ReservedBytes: strconvItoa(len(objects)),
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339), RenewalPolicy: "owner_approval",
	}
	keys := testKeys{owner.Kind + ":" + owner.ID: ownerPrivate.Public().(ed25519.PublicKey), authority.PrincipalNode + ":" + providerID: providerPrivate.Public().(ed25519.PublicKey)}
	root := filepath.Join(t.TempDir(), "lease-state")
	store, err := Open(Options{Root: root, Authorizer: allowAuthority{}, Keys: keys, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := OfferHash(offer)
	if err != nil {
		t.Fatal(err)
	}
	return leaseFixture{store: store, root: root, now: now, owner: owner, ownerPrivate: ownerPrivate, providerID: providerID, providerPrivate: providerPrivate, chain: chain, offer: offer, offerHash: hash, ciphertext: objects}
}

func (f leaseFixture) signedOffer(t *testing.T, leaseID string) SignedOffer {
	t.Helper()
	offer := f.offer
	offer.LeaseID = leaseID
	signed, err := SignOffer(offer, f.owner, f.chain, f.ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func (f leaseFixture) signedAcceptance(t *testing.T, signed SignedOffer) SignedAcceptance {
	t.Helper()
	hash, err := OfferHash(signed.Offer)
	if err != nil {
		t.Fatal(err)
	}
	acceptance := Acceptance{Version: ProtocolVersion, LeaseID: signed.Offer.LeaseID, OfferHash: hash, ProviderNodeID: f.providerID, ReservedBytes: signed.Offer.ReservedBytes, AcceptedAt: f.now.Add(time.Second).Format(time.RFC3339)}
	result, err := SignAcceptance(acceptance, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestReplicaLeaseTransferReadbackRestartRenewAndExpiry(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signedOffer := f.signedOffer(t, f.offer.LeaseID)
	lease, err := f.store.IssueOffer(ctx, signedOffer)
	if err != nil || lease.Status != StatusOffered {
		t.Fatalf("offer=%+v err=%v", lease, err)
	}
	accepted, err := f.store.Accept(ctx, f.signedAcceptance(t, signedOffer))
	if err != nil || accepted.Status != StatusAccepted {
		t.Fatalf("accept=%+v err=%v", accepted, err)
	}
	if _, err := f.store.BeginTransfer(ctx, accepted.Offer.Offer.LeaseID); err != nil {
		t.Fatal(err)
	}
	object := f.offer.EncryptedObjectSet[0]
	checkpoint := func(offset string, at time.Time) SignedCheckpoint {
		value := Checkpoint{Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash, ProviderNodeID: f.providerID, ObjectID: object.ObjectID, CommittedOffset: offset, RecordedAt: at.Format(time.RFC3339)}
		signed, err := SignCheckpoint(value, f.providerPrivate)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	partial := len(f.ciphertext) / 2
	partialLease, err := f.store.Checkpoint(ctx, checkpoint("1", f.now.Add(2*time.Second)))
	if err != nil || partialLease.Status != StatusTransferring {
		t.Fatalf("first partial checkpoint=%+v err=%v", partialLease, err)
	}
	// Offsets are strictly monotonic; multiple fsync checkpoints in the same
	// RFC3339 second are valid and do not need artificial delays.
	partialLease, err = f.store.Checkpoint(ctx, checkpoint(strconvItoa(partial), f.now.Add(2*time.Second)))
	if err != nil || partialLease.Status != StatusTransferring {
		t.Fatalf("partial checkpoint=%+v err=%v", partialLease, err)
	}
	if _, err := f.store.Checkpoint(ctx, checkpoint(strconvItoa(partial-1), f.now.Add(4*time.Second))); !errors.Is(err, ErrConflict) {
		t.Fatalf("offset rollback accepted: %v", err)
	}
	finalCheckpoint := checkpoint(object.CiphertextSize, f.now.Add(5*time.Second))
	verifying, err := f.store.Checkpoint(ctx, finalCheckpoint)
	if err != nil || verifying.Status != StatusVerifying {
		t.Fatalf("final checkpoint=%+v err=%v", verifying, err)
	}
	if replayed, replayErr := f.store.Checkpoint(ctx, finalCheckpoint); replayErr != nil || replayed.Status != StatusVerifying {
		t.Fatalf("exact checkpoint retry after verification was not idempotent: status=%q err=%v", replayed.Status, replayErr)
	}
	if _, err := f.store.Checkpoint(ctx, checkpoint(object.CiphertextSize, f.now.Add(6*time.Second))); !errors.Is(err, ErrTransition) {
		t.Fatalf("new checkpoint advanced a verifying lease: %v", err)
	}
	if _, err := f.store.VerifyReplica(ctx, f.offer.LeaseID, objectMap{object.ObjectID: []byte("corrupt")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("corrupt readback accepted: %v", err)
	}
	active, err := f.store.VerifyReplica(ctx, f.offer.LeaseID, objectMap{object.ObjectID: f.ciphertext})
	if err != nil || active.Status != StatusActive {
		t.Fatalf("verified lease=%+v err=%v", active, err)
	}
	if replayed, replayErr := f.store.Checkpoint(ctx, finalCheckpoint); replayErr != nil || replayed.Status != StatusActive {
		t.Fatalf("exact checkpoint retry after activation was not idempotent: status=%q err=%v", replayed.Status, replayErr)
	}
	if active.Offer.Offer.ProviderNodeID != f.providerID || active.Offer.Offer.OwnerSubjectID == "" {
		t.Fatalf("lease pin/owner lost: %+v", active.Offer.Offer)
	}

	// A second Store instance exercises on-disk verification and process-safe transaction reload.
	reopened, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: testKeys{f.owner.Kind + ":" + f.owner.ID: f.ownerPrivate.Public().(ed25519.PublicKey), authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey)}, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	restored, err := reopened.Get(ctx, f.offer.LeaseID)
	if err != nil || restored.Status != StatusActive || len(restored.Checkpoints) != 1 || restored.Checkpoints[0].Checkpoint.CommittedOffset != object.CiphertextSize {
		t.Fatalf("restored lease=%+v err=%v", restored, err)
	}

	newExpiry := f.now.Add(48 * time.Hour).Format(time.RFC3339)
	acceptedAt := parseTime(active.Acceptance.Acceptance.AcceptedAt)
	equalSecondRequest, err := SignRenewalRequest(Renewal{
		Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash,
		OwnerSubjectID: f.offer.OwnerSubjectID, IssuedAt: acceptedAt.Format(time.RFC3339), ExpiresAt: newExpiry,
	}, f.owner, f.chain, f.ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.BeginRenewal(ctx, equalSecondRequest); !errors.Is(err, ErrDenied) {
		t.Fatalf("renewal with same-second acceptance time was accepted: %v", err)
	}
	renewal := Renewal{Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash, OwnerSubjectID: f.offer.OwnerSubjectID, IssuedAt: f.now.Add(3 * time.Second).Format(time.RFC3339), ExpiresAt: newExpiry}
	signedRenewal, err := SignRenewal(renewal, f.owner, f.chain, f.ownerPrivate, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	request := SignedRenewalRequest{Renewal: signedRenewal.Renewal, Actor: signedRenewal.Actor, GrantChain: signedRenewal.GrantChain, OwnerSignature: signedRenewal.OwnerSignature}
	pending, err := reopened.BeginRenewal(ctx, request)
	if err != nil || pending.Status != StatusRenewing || pending.PendingRenewal == nil {
		t.Fatalf("pending renewal=%+v err=%v", pending, err)
	}
	deliveredRequest, deliveredHash, err := reopened.RenewalForProvider(ctx, f.offer.LeaseID, f.providerID)
	if err != nil || deliveredHash != f.offerHash || !reflect.DeepEqual(deliveredRequest, request) {
		t.Fatalf("provider renewal request=%+v hash=%q err=%v", deliveredRequest, deliveredHash, err)
	}
	if _, _, err := reopened.RenewalForProvider(ctx, f.offer.LeaseID, "nd_aaaaaaaaaaaaaaaaaaaaaa"); !errors.Is(err, ErrDenied) {
		t.Fatalf("renewal request was exposed to a different provider: %v", err)
	}
	if expired, expireErr := reopened.Expire(ctx, f.now.Add(25*time.Hour)); expireErr != nil || len(expired) != 0 {
		t.Fatalf("lease expired at its previous deadline while an authorized renewal was pending: expired=%v err=%v", expired, expireErr)
	}
	reopenedAgain, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: testKeys{
		f.owner.Kind + ":" + f.owner.ID:              f.ownerPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey),
	}, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatalf("reopen pending renewal: %v", err)
	}
	storedPending, err := reopenedAgain.Get(ctx, f.offer.LeaseID)
	if err != nil || storedPending.PendingRenewal == nil || !renewalRequestMatches(*storedPending.PendingRenewal, signedRenewal) {
		t.Fatalf("stored renewal request mismatch: stored=%+v err=%v", storedPending.PendingRenewal, err)
	}
	// A pending renewal protects the old lease deadline, but only through the
	// signed request's own expiry. A separate copy lets the expiry path be
	// tested without consuming the live fixture's pending transition.
	pendingState, err := os.ReadFile(filepath.Join(f.root, stateName))
	if err != nil {
		t.Fatal(err)
	}
	expiryRoot := filepath.Join(t.TempDir(), "pending-expiry")
	if err := os.MkdirAll(expiryRoot, privateDirMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(expiryRoot, stateName), pendingState, privateFileMode); err != nil {
		t.Fatal(err)
	}
	expiryStore, err := Open(Options{Root: expiryRoot, Authorizer: allowAuthority{}, Keys: testKeys{
		f.owner.Kind + ":" + f.owner.ID:              f.ownerPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey),
	}, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatalf("open isolated pending-expiry store: %v", err)
	}
	if expired, expireErr := expiryStore.Expire(ctx, parseTime(newExpiry).Add(time.Second)); expireErr != nil || len(expired) != 1 || expired[0] != f.offer.LeaseID {
		t.Fatalf("pending renewal did not expire at its own signed deadline: expired=%v err=%v", expired, expireErr)
	}
	renewed, err := reopenedAgain.CompleteRenewal(ctx, signedRenewal)
	if err != nil || renewed.Status != StatusActive || len(renewed.Renewals) != 1 {
		t.Fatalf("renewed=%+v err=%v", renewed, err)
	}
	retried, err := reopenedAgain.CompleteRenewal(ctx, signedRenewal)
	if err != nil || retried.Revision != renewed.Revision || retried.Status != StatusActive {
		t.Fatalf("completed renewal retry was not idempotent: %+v err=%v", retried, err)
	}
	completedRetry, err := reopenedAgain.BeginRenewal(ctx, request)
	if err != nil || completedRetry.Revision != renewed.Revision || completedRetry.Status != StatusActive {
		t.Fatalf("replayed owner request after provider completion changed state: %+v err=%v", completedRetry, err)
	}
	expired, err := reopened.Expire(ctx, f.now.Add(25*time.Hour))
	if err != nil || len(expired) != 0 {
		t.Fatalf("lease expired before renewed deadline: %v %v", expired, err)
	}
	expired, err = reopened.Expire(ctx, f.now.Add(49*time.Hour))
	if err != nil || len(expired) != 1 || expired[0] != f.offer.LeaseID {
		t.Fatalf("expiry=%v err=%v", expired, err)
	}
	final, err := reopened.Get(ctx, f.offer.LeaseID)
	if err != nil || final.Status != StatusExpired {
		t.Fatalf("final lease=%+v err=%v", final, err)
	}
}

func TestOfferForProviderOnlyReturnsUnexpiredUndeliveredOfferToNamedProvider(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signed); err != nil {
		t.Fatal(err)
	}
	got, hash, err := f.store.OfferForProvider(ctx, f.offer.LeaseID, f.providerID)
	if err != nil || got.Signature != signed.Signature || hash != f.offerHash {
		t.Fatalf("offer=%+v hash=%q err=%v", got, hash, err)
	}
	if _, _, err := f.store.OfferForProvider(ctx, f.offer.LeaseID, "nd_bbbbbbbbbbbbbbbbbbbbbb"); !errors.Is(err, ErrDenied) {
		t.Fatalf("offer disclosed to a different provider: %v", err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signed)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.OfferForProvider(ctx, f.offer.LeaseID, f.providerID); !errors.Is(err, ErrDenied) {
		t.Fatalf("accepted offer remained retrievable: %v", err)
	}

	expiredFixture := newLeaseFixture(t)
	if _, err := expiredFixture.store.IssueOffer(ctx, expiredFixture.signedOffer(t, expiredFixture.offer.LeaseID)); err != nil {
		t.Fatal(err)
	}
	expiredFixture.store.now = func() time.Time { return expiredFixture.now.Add(25 * time.Hour) }
	if _, _, err := expiredFixture.store.OfferForProvider(ctx, expiredFixture.offer.LeaseID, expiredFixture.providerID); !errors.Is(err, ErrDenied) {
		t.Fatalf("expired offer remained retrievable: %v", err)
	}
}

func TestPendingOffersForProviderExposesOnlyScopedUnacceptedProjections(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signed); err != nil {
		t.Fatal(err)
	}

	items, total, err := f.store.PendingOffersForProvider(ctx, f.providerID, f.offer.OwnerSubjectID, f.offer.ArchiveID, "", 0, 16)
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("pending offers=%+v total=%d err=%v", items, total, err)
	}
	item := items[0]
	if item.LeaseID != f.offer.LeaseID || item.ArchiveID != f.offer.ArchiveID || item.SnapshotID != f.offer.SnapshotID ||
		item.OwnerSubject != f.offer.OwnerSubjectID || item.OfferHash != f.offerHash || item.ObjectCount != len(f.offer.EncryptedObjectSet) {
		t.Fatalf("unexpected pending offer projection: %+v", item)
	}
	if got, count, err := f.store.PendingOffersForProvider(ctx, f.providerID, f.offer.OwnerSubjectID, f.offer.ArchiveID, "snap_"+strings.Repeat("b", 64), 0, 16); err != nil || count != 0 || len(got) != 0 {
		t.Fatalf("snapshot-mismatched offers=%+v total=%d err=%v", got, count, err)
	}
	if got, count, err := f.store.PendingOffersForProvider(ctx, "nd_bbbbbbbbbbbbbbbbbbbbbb", f.offer.OwnerSubjectID, f.offer.ArchiveID, "", 0, 16); err != nil || count != 0 || len(got) != 0 {
		t.Fatalf("offers disclosed to a different provider: %+v total=%d err=%v", got, count, err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signed)); err != nil {
		t.Fatal(err)
	}
	if got, count, err := f.store.PendingOffersForProvider(ctx, f.providerID, f.offer.OwnerSubjectID, f.offer.ArchiveID, "", 0, 16); err != nil || count != 0 || len(got) != 0 {
		t.Fatalf("accepted offer remained in inbox: %+v total=%d err=%v", got, count, err)
	}
}

func TestExpiredVerifyingLeaseCannotBecomeActiveDuringReadback(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signedOffer := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signedOffer)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BeginTransfer(ctx, f.offer.LeaseID); err != nil {
		t.Fatal(err)
	}
	checkpoint := Checkpoint{
		Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash,
		ProviderNodeID: f.providerID, ObjectID: f.offer.EncryptedObjectSet[0].ObjectID,
		CommittedOffset: f.offer.EncryptedObjectSet[0].CiphertextSize, RecordedAt: f.now.Add(2 * time.Second).Format(time.RFC3339),
	}
	signedCheckpoint, err := SignCheckpoint(checkpoint, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := f.store.Checkpoint(ctx, signedCheckpoint); err != nil || result.Status != StatusVerifying {
		t.Fatalf("transfer did not reach verifying: status=%s err=%v", result.Status, err)
	}
	future := f.now.Add(25 * time.Hour)
	reopened, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Now: func() time.Time { return future },
		Keys: testKeys{
			f.owner.Kind + ":" + f.owner.ID:              f.ownerPrivate.Public().(ed25519.PublicKey),
			authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.VerifyReplica(ctx, f.offer.LeaseID, objectMap{f.offer.EncryptedObjectSet[0].ObjectID: f.ciphertext}); !errors.Is(err, ErrTransition) {
		t.Fatalf("expired lease became active after readback: %v", err)
	}
	current, err := reopened.Get(ctx, f.offer.LeaseID)
	if err != nil || current.Status != StatusVerifying {
		t.Fatalf("expired lease state changed unexpectedly: status=%s err=%v", current.Status, err)
	}
}

func TestOfferRequiresReplicaManifestBindingHash(t *testing.T) {
	f := newLeaseFixture(t)
	withoutMapping := f.offer
	withoutMapping.ReplicaManifestHash = ""
	if _, err := CanonicalOffer(withoutMapping); !errors.Is(err, ErrInvalid) {
		t.Fatalf("offer without signed ciphertext mapping hash accepted: %v", err)
	}
}

func TestTombstoneForkFreezesAndDeletionReportBindsExactDecision(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signedOffer := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signedOffer)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BeginTransfer(ctx, f.offer.LeaseID); err != nil {
		t.Fatal(err)
	}
	evidence, err := grantEvidenceHash(f.chain)
	if err != nil {
		t.Fatal(err)
	}
	makeTombstone := func(reason string) SignedTombstone {
		tomb := Tombstone{Version: TombstoneVersion, ArchiveID: f.offer.ArchiveID, DeletionEpoch: "1", ReasonCode: reason, IssuedAt: f.now.Add(5 * time.Second).Format(time.RFC3339), AuthorizedEvidence: evidence}
		signed, err := SignTombstone(tomb, f.offer.OwnerSubjectID, f.owner, f.chain, f.ownerPrivate)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	first := makeTombstone("owner_requested")
	stored, err := f.store.AddTombstone(ctx, first)
	if err != nil || stored.Frozen {
		t.Fatalf("tombstone=%+v err=%v", stored, err)
	}
	terminating, err := f.store.Get(ctx, f.offer.LeaseID)
	if err != nil || terminating.Status != StatusTerminating {
		t.Fatalf("lease after deletion decision=%+v err=%v", terminating, err)
	}
	report := DeletionReport{Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash, ProviderNodeID: f.providerID, Status: string(StatusDeleted), ReportedAt: f.now.Add(6 * time.Second).Format(time.RFC3339), TombstoneHash: stored.Hash}
	signedReport, err := SignDeletionReport(report, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	wrongTombstoneReport := report
	wrongTombstoneReport.TombstoneHash = "sha256:" + strings.Repeat("0", 64)
	wrongSignedReport, err := SignDeletionReport(wrongTombstoneReport, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ReportDeletion(ctx, wrongSignedReport); !errors.Is(err, ErrConflict) {
		t.Fatalf("report for another tombstone was accepted: %v", err)
	}
	forgedReport := signedReport
	forgedReport.Signature = strings.Repeat("A", 86)
	if _, err := f.store.ReportDeletion(ctx, forgedReport); !errors.Is(err, ErrDenied) {
		t.Fatalf("unsigned/forged provider deletion acknowledgement was accepted: %v", err)
	}
	stillTerminating, err := f.store.Get(ctx, f.offer.LeaseID)
	if err != nil || stillTerminating.Status != StatusTerminating || stillTerminating.DeletionReport != nil {
		t.Fatalf("forged provider acknowledgement changed durable lease state: status=%q report=%+v err=%v", stillTerminating.Status, stillTerminating.DeletionReport, err)
	}
	deleted, err := f.store.ReportDeletion(ctx, signedReport)
	if err != nil || deleted.Status != StatusDeleted {
		t.Fatalf("reported deletion=%+v err=%v", deleted, err)
	}
	// A second valid same-epoch event freezes the archive; it is never auto-selected.
	if tomb, err := f.store.AddTombstone(ctx, makeTombstone("policy_retention")); !errors.Is(err, ErrFrozen) || !tomb.Frozen {
		t.Fatalf("fork was not frozen: record=%+v err=%v", tomb, err)
	}
	blockedOffer := f.signedOffer(t, "rl_bbbbbbbbbbbbbbbbbbbbbb")
	if _, err := f.store.IssueOffer(ctx, blockedOffer); !errors.Is(err, ErrFrozen) {
		t.Fatalf("frozen archive accepted new replica: %v", err)
	}
	if err := os.Chmod(filepath.Join(f.root, stateName), privateFileMode); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: testKeys{f.owner.Kind + ":" + f.owner.ID: f.ownerPrivate.Public().(ed25519.PublicKey), authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey)}})
	if err != nil {
		t.Fatalf("fork evidence did not survive restart: %v", err)
	}
	_, tombstones, err := reopened.Snapshot(ctx)
	if err != nil || len(tombstones) != 1 || !tombstones[0].Frozen || len(tombstones[0].ForkEvidence) != 1 {
		t.Fatalf("reopened tombstones=%+v err=%v", tombstones, err)
	}
}

func TestCreateTombstoneConcurrentRetriesPersistOneDecision(t *testing.T) {
	f := newLeaseFixture(t)
	rootKeyID, err := identityprotocol.KeyID(f.ownerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: f.offer.OwnerSubjectID, Issuer: f.owner,
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: f.providerID},
		ArchiveID: f.offer.ArchiveID, Actions: []string{"archive.delete", "archive.replicate"},
		NotBefore: f.now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: f.now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: strings.Repeat("z", 43), PolicyEpoch: "1", IssuerKeyID: rootKeyID,
	}, f.ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	chain := []authority.SignedGrant{grant}
	const attempts = 16
	results := make(chan TombstoneRecord, attempts)
	errorsFound := make(chan error, attempts)
	var workers sync.WaitGroup
	for range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			record, createErr := f.store.CreateTombstone(context.Background(), f.offer.OwnerSubjectID, f.offer.ArchiveID, "owner_requested", f.now.Add(time.Minute), func(tombstone Tombstone) (SignedTombstone, error) {
				return SignTombstone(tombstone, f.offer.OwnerSubjectID, authority.Principal{Kind: authority.PrincipalNode, ID: f.providerID}, chain, f.providerPrivate)
			})
			if createErr != nil {
				errorsFound <- createErr
				return
			}
			results <- record
		}()
	}
	workers.Wait()
	close(results)
	close(errorsFound)
	for createErr := range errorsFound {
		t.Errorf("concurrent tombstone creation failed: %v", createErr)
	}
	var hash string
	for record := range results {
		if record.Frozen || record.Signed.Tombstone.DeletionEpoch != "1" {
			t.Fatalf("unexpected created tombstone: %+v", record)
		}
		if hash == "" {
			hash = record.Hash
		} else if record.Hash != hash {
			t.Fatalf("concurrent retries produced different decisions: %q != %q", record.Hash, hash)
		}
	}
	_, tombstones, err := f.store.Snapshot(context.Background())
	if err != nil || len(tombstones) != 1 || tombstones[0].Frozen || tombstones[0].Hash != hash {
		t.Fatalf("durable tombstones=%+v err=%v", tombstones, err)
	}
}

func TestTombstoneDeliveryReceiptIsDurableAndIdempotent(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	at := f.now.Add(time.Minute)
	record, err := f.store.CreateTombstone(ctx, f.offer.OwnerSubjectID, f.offer.ArchiveID, "owner_requested", at, func(tombstone Tombstone) (SignedTombstone, error) {
		return SignTombstone(tombstone, f.offer.OwnerSubjectID, authority.Principal{Kind: authority.PrincipalNode, ID: f.providerID}, f.chain, f.providerPrivate)
	})
	if err != nil {
		t.Fatal(err)
	}
	acknowledgedAt := at.Add(time.Second)
	if err := f.store.MarkTombstoneDelivered(ctx, f.offer.ArchiveID, f.providerID, record.Hash, acknowledgedAt); err != nil {
		t.Fatal(err)
	}
	// Replaying the peer's same receipt must not create another checkpoint or
	// change the first acknowledgement timestamp.
	if err := f.store.MarkTombstoneDelivered(ctx, f.offer.ArchiveID, f.providerID, record.Hash, acknowledgedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := f.store.MarkTombstoneDelivered(ctx, f.offer.ArchiveID, f.providerID, "sha256:"+strings.Repeat("0", 64), acknowledgedAt); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong tombstone hash error=%v, want conflict", err)
	}
	reopened, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: testKeys{
		"identity:" + f.owner.ID: f.ownerPrivate.Public().(ed25519.PublicKey),
		"node:" + f.providerID:   f.providerPrivate.Public().(ed25519.PublicKey),
	}, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	_, tombstones, err := reopened.Snapshot(ctx)
	if err != nil || len(tombstones) != 1 || len(tombstones[0].Deliveries) != 1 {
		t.Fatalf("durable delivery projection=%+v err=%v", tombstones, err)
	}
	delivery := tombstones[0].Deliveries[0]
	if delivery.NodeID != f.providerID || delivery.TombstoneHash != record.Hash || delivery.AcknowledgedAt != acknowledgedAt.UTC().Format(time.RFC3339) {
		t.Fatalf("delivery checkpoint=%+v", delivery)
	}
}

func TestLeaseAuthorityAndReadbackFailClosed(t *testing.T) {
	f := newLeaseFixture(t)
	signed := f.signedOffer(t, f.offer.LeaseID)
	unsignedStore, err := Open(Options{Root: filepath.Join(t.TempDir(), "readonly"), Keys: testKeys{f.owner.Kind + ":" + f.owner.ID: f.ownerPrivate.Public().(ed25519.PublicKey)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unsignedStore.IssueOffer(context.Background(), signed); !errors.Is(err, ErrDenied) {
		t.Fatalf("missing authority accepted offer: %v", err)
	}
	bad := signed
	bad.Offer.ReservedBytes = "999"
	if _, err := f.store.IssueOffer(context.Background(), bad); err == nil {
		t.Fatal("tampered offer passed signature validation")
	}
	if _, err := os.Stat(filepath.Join(f.root, stateName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected requests wrote state: %v", err)
	}
}

func TestCheckpointFromUntrustedProviderKeyDoesNotAdvanceLease(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signedOffer := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatal(err)
	}
	accepted, err := f.store.Accept(ctx, f.signedAcceptance(t, signedOffer))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BeginTransfer(ctx, accepted.Offer.Offer.LeaseID); err != nil {
		t.Fatal(err)
	}
	wrongPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	object := f.offer.EncryptedObjectSet[0]
	checkpoint, err := SignCheckpoint(Checkpoint{
		Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash,
		ProviderNodeID: f.providerID, ObjectID: object.ObjectID, CommittedOffset: "1",
		RecordedAt: f.now.Add(2 * time.Second).Format(time.RFC3339),
	}, wrongPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Checkpoint(ctx, checkpoint); !errors.Is(err, ErrDenied) {
		t.Fatalf("checkpoint signed by an untrusted key was accepted: %v", err)
	}
	current, err := f.store.Get(ctx, f.offer.LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != StatusTransferring || len(current.Checkpoints) != 0 || len(current.Progress) != 1 || current.Progress[0].CommittedOffset != "0" {
		t.Fatalf("untrusted checkpoint advanced durable lease: status=%s checkpoints=%d progress=%+v", current.Status, len(current.Checkpoints), current.Progress)
	}
}

func TestAcceptanceRequiresCurrentOwnerAuthorization(t *testing.T) {
	f := newLeaseFixture(t)
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(context.Background(), signed); err != nil {
		t.Fatal(err)
	}
	keys := testKeys{
		f.owner.Kind + ":" + f.owner.ID:              f.ownerPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey),
	}
	deniedStore, err := Open(Options{Root: f.root, Authorizer: allowAuthority{denied: true}, Keys: keys, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deniedStore.Accept(context.Background(), f.signedAcceptance(t, signed)); !errors.Is(err, ErrDenied) {
		t.Fatalf("provider accepted after owner authorization was revoked: %v", err)
	}
	accepted, err := f.store.Accept(context.Background(), f.signedAcceptance(t, signed))
	if err != nil || accepted.Status != StatusAccepted {
		t.Fatalf("currently authorized acceptance=%+v err=%v", accepted, err)
	}
}

func TestProviderCapacityReservationAcceptsExactBoundaryAndReopensWithNewLimit(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	reserved, err := strconv.ParseUint(f.offer.ReservedBytes, 10, 64)
	if err != nil || reserved == 0 {
		t.Fatalf("fixture reservation=%q err=%v", f.offer.ReservedBytes, err)
	}
	providerStore, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys,
		Now: f.store.now, ProviderCapacityBytes: reserved, LocalProviderNodeID: f.providerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := providerStore.IssueOffer(ctx, signed); err != nil {
		t.Fatal(err)
	}
	acceptance := f.signedAcceptance(t, signed)
	accepted, err := providerStore.AcceptProvider(ctx, acceptance)
	if err != nil || accepted.Status != StatusAccepted {
		t.Fatalf("exact-boundary provider acceptance=%+v err=%v", accepted, err)
	}
	if retry, retryErr := providerStore.AcceptProvider(ctx, acceptance); retryErr != nil || retry.Revision != accepted.Revision {
		t.Fatalf("exact provider acceptance retry changed durable state: accepted=%+v retry=%+v err=%v", accepted, retry, retryErr)
	}

	second := f.signedOffer(t, "rl_bbbbbbbbbbbbbbbbbbbbbb")
	if _, err := providerStore.IssueOffer(ctx, second); err != nil {
		t.Fatal(err)
	}
	before, err := providerStore.Get(ctx, second.Offer.LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	tooSmall, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys,
		Now: f.store.now, ProviderCapacityBytes: reserved - 1, LocalProviderNodeID: f.providerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	secondAcceptance := f.signedAcceptance(t, second)
	if _, err := tooSmall.AcceptProvider(ctx, secondAcceptance); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("reconfigured over-capacity acceptance error=%v", err)
	}
	after, err := tooSmall.Get(ctx, second.Offer.LeaseID)
	if err != nil || !reflect.DeepEqual(before, after) || after.Status != StatusOffered {
		t.Fatalf("rejected provider acceptance changed lease state: before=%+v after=%+v err=%v", before, after, err)
	}

	raised, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys,
		Now: f.store.now, ProviderCapacityBytes: reserved * 2, LocalProviderNodeID: f.providerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptedSecond, err := raised.AcceptProvider(ctx, secondAcceptance)
	if err != nil || acceptedSecond.Status != StatusAccepted {
		t.Fatalf("raised-capacity provider acceptance=%+v err=%v", acceptedSecond, err)
	}
}

func TestProviderCapacityReservationUnlimitedByDefault(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signed); err != nil {
		t.Fatal(err)
	}
	accepted, err := f.store.AcceptProvider(ctx, f.signedAcceptance(t, signed))
	if err != nil || accepted.Status != StatusAccepted {
		t.Fatalf("default unlimited provider acceptance=%+v err=%v", accepted, err)
	}
}

func TestProviderCapacityOptionIsBoundedByProtocolMaximum(t *testing.T) {
	keys := testKeys{}
	for _, capacity := range []uint64{0, MaxReservedBytes} {
		if _, err := Open(Options{
			Root: filepath.Join(t.TempDir(), "valid-capacity"), Keys: keys, ProviderCapacityBytes: capacity,
		}); err != nil {
			t.Fatalf("valid provider capacity %d rejected: %v", capacity, err)
		}
	}
	if _, err := Open(Options{
		Root: filepath.Join(t.TempDir(), "invalid-capacity"), Keys: keys, ProviderCapacityBytes: MaxReservedBytes + 1,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("provider capacity above protocol maximum accepted: %v", err)
	}
}

func TestSourceAcceptanceDoesNotUseLocalProviderCapacity(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	providerStore, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys,
		Now: f.store.now, ProviderCapacityBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := providerStore.IssueOffer(ctx, signed); err != nil {
		t.Fatal(err)
	}
	accepted, err := providerStore.Accept(ctx, f.signedAcceptance(t, signed))
	if err != nil || accepted.Status != StatusAccepted {
		t.Fatalf("source-side acceptance was incorrectly capacity-limited: lease=%+v err=%v", accepted, err)
	}
}

func TestConcurrentProviderAcceptancesCannotExceedCapacity(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	reserved, err := strconv.ParseUint(f.offer.ReservedBytes, 10, 64)
	if err != nil || reserved == 0 {
		t.Fatalf("fixture reservation=%q err=%v", f.offer.ReservedBytes, err)
	}
	providerStore, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys,
		Now: f.store.now, ProviderCapacityBytes: reserved, LocalProviderNodeID: f.providerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := f.signedOffer(t, f.offer.LeaseID)
	second := f.signedOffer(t, "rl_bbbbbbbbbbbbbbbbbbbbbb")
	for _, signed := range []SignedOffer{first, second} {
		if _, err := providerStore.IssueOffer(ctx, signed); err != nil {
			t.Fatal(err)
		}
	}
	acceptances := []SignedAcceptance{f.signedAcceptance(t, first), f.signedAcceptance(t, second)}
	start := make(chan struct{})
	type result struct{ err error }
	results := make(chan result, len(acceptances))
	var workers sync.WaitGroup
	for _, acceptance := range acceptances {
		acceptance := acceptance
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := providerStore.AcceptProvider(ctx, acceptance)
			results <- result{err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	accepted, capacityRejected := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			accepted++
		case errors.Is(result.err, ErrCapacityExceeded):
			capacityRejected++
		default:
			t.Fatalf("unexpected concurrent provider acceptance error: %v", result.err)
		}
	}
	if accepted != 1 || capacityRejected != 1 {
		t.Fatalf("capacity race accepted=%d rejected=%d, want exactly one of each", accepted, capacityRejected)
	}
}

func TestProviderCapacityCountsPossiblyRetainedLeaseStates(t *testing.T) {
	for _, status := range []Status{
		StatusAccepted, StatusTransferring, StatusVerifying, StatusActive, StatusRenewing,
		StatusExpired, StatusFailed, StatusTerminating, StatusUnconfirmed,
	} {
		leases := []Lease{{Status: status, Offer: SignedOffer{Offer: Offer{ProviderNodeID: "nd_aaaaaaaaaaaaaaaaaaaaaa", ReservedBytes: "7"}}}}
		used, err := providerReservedBytes(leases, "nd_aaaaaaaaaaaaaaaaaaaaaa")
		if err != nil || used != 7 {
			t.Errorf("status %q used=%d err=%v, want 7", status, used, err)
		}
	}
	used, err := providerReservedBytes([]Lease{{Status: StatusOffered, Offer: SignedOffer{Offer: Offer{ProviderNodeID: "nd_aaaaaaaaaaaaaaaaaaaaaa", ReservedBytes: "7"}}}}, "nd_aaaaaaaaaaaaaaaaaaaaaa")
	if err != nil || used != 0 {
		t.Fatalf("never-accepted offered lease used=%d err=%v, want 0", used, err)
	}
	deleted := Lease{
		Status: StatusDeleted, OfferHash: "sha256:" + strings.Repeat("a", 64),
		Offer: SignedOffer{Offer: Offer{LeaseID: "rl_aaaaaaaaaaaaaaaaaaaaaa", ProviderNodeID: "nd_aaaaaaaaaaaaaaaaaaaaaa", ReservedBytes: "7"}},
		DeletionReport: &SignedDeletionReport{Report: DeletionReport{
			Status: string(StatusDeleted), LeaseID: "rl_aaaaaaaaaaaaaaaaaaaaaa",
			OfferHash: "sha256:" + strings.Repeat("a", 64), ProviderNodeID: "nd_aaaaaaaaaaaaaaaaaaaaaa",
		}},
	}
	if used, err := providerReservedBytes([]Lease{deleted}, "nd_aaaaaaaaaaaaaaaaaaaaaa"); err != nil || used != 0 {
		t.Fatalf("confirmed deleted lease used=%d err=%v, want 0", used, err)
	}
	deleted.DeletionReport = nil
	if used, err := providerReservedBytes([]Lease{deleted}, "nd_aaaaaaaaaaaaaaaaaaaaaa"); err != nil || used != 7 {
		t.Fatalf("deleted lease without receipt used=%d err=%v, want 7", used, err)
	}
}

func TestProviderCapacityAccountingRejectsOverflow(t *testing.T) {
	leases := make([]Lease, 16385)
	for i := range leases {
		leases[i] = Lease{Status: StatusActive, Offer: SignedOffer{Offer: Offer{ProviderNodeID: "nd_aaaaaaaaaaaaaaaaaaaaaa", ReservedBytes: strconv.FormatUint(MaxReservedBytes, 10)}}}
	}
	if _, err := providerReservedBytes(leases, "nd_aaaaaaaaaaaaaaaaaaaaaa"); !errors.Is(err, ErrCapacityOverflow) {
		t.Fatalf("overflowing provider capacity accounting error=%v", err)
	}
}

func TestProviderCapacityIgnoresOutboundLeasesOnSameNode(t *testing.T) {
	localNodeID := "nd_aaaaaaaaaaaaaaaaaaaaaa"
	remoteProviderID := "nd_bbbbbbbbbbbbbbbbbbbbbb"
	leases := []Lease{
		{Status: StatusActive, Offer: SignedOffer{Offer: Offer{ProviderNodeID: remoteProviderID, ReservedBytes: "900"}}},
		{Status: StatusTransferring, Offer: SignedOffer{Offer: Offer{ProviderNodeID: localNodeID, ReservedBytes: "40"}}},
	}
	used, err := providerReservedBytes(leases, localNodeID)
	if err != nil || used != 40 {
		t.Fatalf("provider capacity included nonlocal outbound lease: used=%d err=%v, want 40", used, err)
	}
}

func TestProviderCapacityDoesNotChargeAcceptedOutboundLease(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	_, remotePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	remoteNodeID, err := identityprotocol.NodeID(remotePrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := strconv.ParseUint(f.offer.ReservedBytes, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	keys := testKeys{
		f.owner.Kind + ":" + f.owner.ID:              f.ownerPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + remoteNodeID: remotePrivate.Public().(ed25519.PublicKey),
	}
	store, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: keys, Now: f.store.now,
		ProviderCapacityBytes: reserved, LocalProviderNodeID: f.providerID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// This is a source-side lease for bytes that belong on another Node.
	outboundOffer := f.offer
	outboundOffer.LeaseID = "rl_cccccccccccccccccccccc"
	outboundOffer.ProviderNodeID = remoteNodeID
	outbound, err := SignOffer(outboundOffer, f.owner, f.chain, f.ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.IssueOffer(ctx, outbound); err != nil {
		t.Fatal(err)
	}
	outboundHash, err := OfferHash(outbound.Offer)
	if err != nil {
		t.Fatal(err)
	}
	outboundReceipt, err := SignAcceptance(Acceptance{
		Version: ProtocolVersion, LeaseID: outbound.Offer.LeaseID, OfferHash: outboundHash,
		ProviderNodeID: remoteNodeID, ReservedBytes: outbound.Offer.ReservedBytes,
		AcceptedAt: f.now.Add(time.Second).Format(time.RFC3339),
	}, remotePrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Accept(ctx, outboundReceipt); err != nil {
		t.Fatalf("outbound provider receipt failed: %v", err)
	}

	// A local incoming offer should still fit at its exact capacity because the
	// outbound reservation above is charged to remoteNodeID, not this Node.
	inbound := f.signedOffer(t, f.offer.LeaseID)
	if _, err := store.IssueOffer(ctx, inbound); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcceptProvider(ctx, f.signedAcceptance(t, inbound)); err != nil {
		t.Fatalf("outbound lease consumed local provider capacity: %v", err)
	}
}

func TestProviderCapacityFailsClosedWithoutLocalNodeIdentity(t *testing.T) {
	f := newLeaseFixture(t)
	providerStore, err := Open(Options{
		Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys,
		Now: f.store.now, ProviderCapacityBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := providerStore.IssueOffer(context.Background(), signed); err != nil {
		t.Fatal(err)
	}
	if _, err := providerStore.AcceptProvider(context.Background(), f.signedAcceptance(t, signed)); !errors.Is(err, ErrProviderIdentity) {
		t.Fatalf("provider acceptance without local Node identity error=%v", err)
	}
}

func activeOutboundLease(t *testing.T) (leaseFixture, Lease) {
	t.Helper()
	f := newLeaseFixture(t)
	ctx := context.Background()
	signed := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signed); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signed)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BeginTransfer(ctx, f.offer.LeaseID); err != nil {
		t.Fatal(err)
	}
	object := f.offer.EncryptedObjectSet[0]
	checkpoint, err := SignCheckpoint(Checkpoint{
		Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: f.offerHash,
		ProviderNodeID: f.providerID, ObjectID: object.ObjectID,
		CommittedOffset: object.CiphertextSize, RecordedAt: f.now.Add(2 * time.Second).Format(time.RFC3339),
	}, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Checkpoint(ctx, checkpoint); err != nil {
		t.Fatal(err)
	}
	active, err := f.store.VerifyReplica(ctx, f.offer.LeaseID, objectMap{object.ObjectID: f.ciphertext})
	if err != nil || active.Status != StatusActive {
		t.Fatalf("activate outbound fixture: lease=%+v err=%v", active, err)
	}
	f.store.localProviderNodeID = "nd_zzzzzzzzzzzzzzzzzzzzzz"
	return f, active
}

func TestMarkOutboundCiphertextReadbackMismatch(t *testing.T) {
	t.Run("success and retry idempotency", func(t *testing.T) {
		f, active := activeOutboundLease(t)
		failed, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, active.OfferHash, active.Revision, f.now.Add(3*time.Second))
		if err != nil || failed.Status != StatusFailed || failed.FailureCode != "ciphertext_readback_mismatch" || failed.FailureAt == "" {
			t.Fatalf("failed=%+v err=%v", failed, err)
		}
		if failed.Revision == active.Revision || !reflect.DeepEqual(failed.Acceptance, active.Acceptance) || !reflect.DeepEqual(failed.Checkpoints, active.Checkpoints) || !reflect.DeepEqual(failed.Renewals, active.Renewals) {
			t.Fatal("failure transition did not preserve lease evidence or advance revision")
		}
		retried, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, failed.OfferHash, failed.Revision, f.now.Add(3*time.Second))
		if err != nil || !reflect.DeepEqual(retried, failed) {
			t.Fatalf("same-revision retry=%+v err=%v", retried, err)
		}
		if _, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, active.OfferHash, active.Revision, f.now.Add(3*time.Second)); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale revision retry error=%v", err)
		}
	})

	t.Run("stale revision or offer hash", func(t *testing.T) {
		f, active := activeOutboundLease(t)
		if _, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, active.OfferHash, "999", f.now.Add(3*time.Second)); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale revision error=%v", err)
		}
		wrongHash := "sha256:" + strings.Repeat("f", 64)
		if _, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, wrongHash, active.Revision, f.now.Add(3*time.Second)); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale offer hash error=%v", err)
		}
	})

	t.Run("non-active lease", func(t *testing.T) {
		f := newLeaseFixture(t)
		f.store.localProviderNodeID = "nd_zzzzzzzzzzzzzzzzzzzzzz"
		offer, err := f.store.IssueOffer(context.Background(), f.signedOffer(t, f.offer.LeaseID))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, offer.OfferHash, offer.Revision, f.now.Add(3*time.Second)); !errors.Is(err, ErrTransition) {
			t.Fatalf("non-active transition error=%v", err)
		}
	})

	t.Run("tombstoned archive", func(t *testing.T) {
		f, active := activeOutboundLease(t)
		if _, err := f.store.CreateTombstone(context.Background(), f.offer.OwnerSubjectID, f.offer.ArchiveID, "owner_requested", f.now.Add(time.Minute), func(tombstone Tombstone) (SignedTombstone, error) {
			return SignTombstone(tombstone, f.offer.OwnerSubjectID, f.owner, f.chain, f.ownerPrivate)
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, active.OfferHash, active.Revision, f.now.Add(3*time.Second)); !errors.Is(err, ErrDenied) {
			t.Fatalf("tombstoned transition error=%v", err)
		}
	})

	t.Run("concurrent stale observation", func(t *testing.T) {
		f, active := activeOutboundLease(t)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var succeeded, conflicted int
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, active.OfferHash, active.Revision, f.now.Add(3*time.Second))
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					succeeded++
				} else if errors.Is(err, ErrConflict) {
					conflicted++
				} else {
					t.Errorf("race result: %v", err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("success=%d conflict=%d, want one each", succeeded, conflicted)
		}
	})
}

func TestReadbackFailureMetadataSurvivesAuthorizedTombstoneDeletion(t *testing.T) {
	f, active := activeOutboundLease(t)
	failed, err := f.store.FailReadbackMismatch(context.Background(), f.offer.LeaseID, active.OfferHash, active.Revision, f.now.Add(3*time.Second))
	if err != nil || failed.Status != StatusFailed {
		t.Fatalf("mark verified mismatch: status=%s err=%v", failed.Status, err)
	}
	tombstone, err := f.store.CreateTombstone(context.Background(), f.offer.OwnerSubjectID, f.offer.ArchiveID,
		"owner_requested", f.now.Add(time.Minute), func(value Tombstone) (SignedTombstone, error) {
			return SignTombstone(value, f.offer.OwnerSubjectID, f.owner, f.chain, f.ownerPrivate)
		})
	if err != nil {
		t.Fatal(err)
	}
	report := DeletionReport{
		Version: ProtocolVersion, LeaseID: f.offer.LeaseID, OfferHash: failed.OfferHash,
		ProviderNodeID: f.providerID, Status: string(StatusDeleted),
		ReportedAt: f.now.Add(2 * time.Minute).Format(time.RFC3339), TombstoneHash: tombstone.Hash,
	}
	signed, err := SignDeletionReport(report, f.providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := f.store.ReportDeletion(context.Background(), signed)
	if err != nil || deleted.Status != StatusDeleted || deleted.FailureCode != "ciphertext_readback_mismatch" || deleted.FailureAt != failed.FailureAt {
		t.Fatalf("authorized deletion did not preserve failure history: lease=%+v err=%v", deleted, err)
	}
	if _, err := Open(Options{Root: f.store.root, Keys: f.store.keys, Authorizer: f.store.authorizer, Now: f.store.now}); err != nil {
		t.Fatalf("reopen after deletion rejected preserved failure history: %v", err)
	}
}

func TestWrongTombstonePredecessorConflictsWithoutFreezing(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	evidence, err := grantEvidenceHash(f.chain)
	if err != nil {
		t.Fatal(err)
	}
	makeTombstone := func(epoch, previous, reason string, at time.Time) SignedTombstone {
		tomb := Tombstone{
			Version: TombstoneVersion, ArchiveID: f.offer.ArchiveID,
			DeletionEpoch: epoch, PreviousHead: previous, ReasonCode: reason,
			IssuedAt: at.Format(time.RFC3339), AuthorizedEvidence: evidence,
		}
		signed, err := SignTombstone(tomb, f.offer.OwnerSubjectID, f.owner, f.chain, f.ownerPrivate)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	first, err := f.store.AddTombstone(ctx, makeTombstone("1", "", "owner_requested", f.now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.store.AddTombstone(ctx, makeTombstone("2", first.Hash, "policy_retention", f.now.Add(2*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	wrongHead := "sha256:" + strings.Repeat("f", 64)
	if _, err := f.store.AddTombstone(ctx, makeTombstone("2", wrongHead, "owner_requested", f.now.Add(3*time.Second))); !errors.Is(err, ErrConflict) {
		t.Fatalf("non-successor event was treated as a fork: %v", err)
	}
	_, tombstones, err := f.store.Snapshot(ctx)
	if err != nil || len(tombstones) != 1 || tombstones[0].Frozen || tombstones[0].Hash != second.Hash {
		t.Fatalf("invalid predecessor changed tombstone state: %+v err=%v", tombstones, err)
	}
}

func TestDeletionTimeoutIsUnconfirmedNotProofOfDeletion(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signedOffer := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signedOffer)); err != nil {
		t.Fatal(err)
	}
	evidence, err := grantEvidenceHash(f.chain)
	if err != nil {
		t.Fatal(err)
	}
	tombstone := Tombstone{Version: TombstoneVersion, ArchiveID: f.offer.ArchiveID, DeletionEpoch: "1", ReasonCode: "owner_requested", IssuedAt: f.now.Add(2 * time.Second).Format(time.RFC3339), AuthorizedEvidence: evidence}
	signedTombstone, err := SignTombstone(tombstone, f.offer.OwnerSubjectID, f.owner, f.chain, f.ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AddTombstone(ctx, signedTombstone); err != nil {
		t.Fatal(err)
	}
	advanced, err := f.store.Expire(ctx, f.now.Add(25*time.Hour))
	if err != nil || len(advanced) != 1 || advanced[0] != f.offer.LeaseID {
		t.Fatalf("termination timeout advance=%v err=%v", advanced, err)
	}
	lease, err := f.store.Get(ctx, f.offer.LeaseID)
	if err != nil || lease.Status != StatusUnconfirmed || lease.DeletionReport != nil {
		t.Fatalf("timeout claimed remote deletion: lease=%+v err=%v", lease, err)
	}
	reopened, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: testKeys{
		f.owner.Kind + ":" + f.owner.ID:              f.ownerPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + f.providerID: f.providerPrivate.Public().(ed25519.PublicKey),
	}})
	if err != nil {
		t.Fatalf("unconfirmed state did not survive restart: %v", err)
	}
	lease, err = reopened.Get(ctx, f.offer.LeaseID)
	if err != nil || lease.Status != StatusUnconfirmed || lease.DeletionReport != nil {
		t.Fatalf("restored timeout state=%+v err=%v", lease, err)
	}
}

func TestConcurrentDuplicateOfferIsIdempotent(t *testing.T) {
	f := newLeaseFixture(t)
	signed := f.signedOffer(t, f.offer.LeaseID)
	const writers = 16
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := f.store.IssueOffer(context.Background(), signed); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	leases, _, err := f.store.Snapshot(context.Background())
	if err != nil || len(leases) != 1 || leases[0].Status != StatusOffered {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
}

func TestStateDirectoryAndStoredStateMustBePrivateAndValid(t *testing.T) {
	f := newLeaseFixture(t)
	if info, err := os.Stat(f.root); err != nil || info.Mode().Perm() != privateDirMode {
		t.Fatalf("root mode=%v err=%v", info, err)
	}
	if err := os.Chmod(f.root, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Root: f.root, Keys: testKeys{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("permissive state directory accepted: %v", err)
	}
	_ = os.Chmod(f.root, privateDirMode)
	if err := os.WriteFile(filepath.Join(f.root, stateName), []byte(`{"state_version":"1","leases":[],"tombstones":[],"extra":true}`), privateFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Root: f.root, Keys: testKeys{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown durable fields accepted: %v", err)
	}
}

func TestLeaseStateV1MigratesAndPersistsTombstoneDeliveryCursorAcrossReopen(t *testing.T) {
	f := newLeaseFixture(t)
	ctx := context.Background()
	signedOffer := f.signedOffer(t, f.offer.LeaseID)
	if _, err := f.store.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Accept(ctx, f.signedAcceptance(t, signedOffer)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BeginTransfer(ctx, f.offer.LeaseID); err != nil {
		t.Fatal(err)
	}
	evidence, err := grantEvidenceHash(f.chain)
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := SignTombstone(Tombstone{
		Version: TombstoneVersion, ArchiveID: f.offer.ArchiveID, OwnerSubjectID: f.offer.OwnerSubjectID,
		DeletionEpoch: "1", ReasonCode: "owner_requested", IssuedAt: f.now.Add(5 * time.Second).Format(time.RFC3339),
		AuthorizedEvidence: evidence,
	}, f.offer.OwnerSubjectID, f.owner, f.chain, f.ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	storedTombstone, err := f.store.AddTombstone(ctx, tombstone)
	if err != nil {
		t.Fatal(err)
	}

	// Materialize the exact existing lease and tombstone payloads as the old
	// strict v1 shape. This exercises preservation, not only an empty migration.
	path := filepath.Join(f.root, stateName)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var seeded snapshot
	if err := strictDecode(contents, &seeded); err != nil {
		t.Fatalf("decode seeded state: %v", err)
	}
	legacy, err := json.Marshal(snapshotV1{Version: stateVersionV1, Leases: seeded.Leases, Tombstones: seeded.Tombstones})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, legacy, privateFileMode); err != nil {
		t.Fatal(err)
	}

	store, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys, Now: f.store.now})
	if err != nil {
		t.Fatalf("open/migrate v1 state: %v", err)
	}
	contents, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var migrated map[string]json.RawMessage
	if err := json.Unmarshal(contents, &migrated); err != nil || string(migrated["state_version"]) != `"2"` {
		t.Fatalf("v1 state was not atomically migrated to v2: version=%s err=%v", migrated["state_version"], err)
	}
	leases, tombstones, err := store.Snapshot(ctx)
	if err != nil || len(leases) != 1 || leases[0].Status != StatusTerminating || len(tombstones) != 1 || tombstones[0].Hash != storedTombstone.Hash {
		t.Fatalf("migration did not preserve existing data: leases=%+v tombstones=%+v err=%v", leases, tombstones, err)
	}
	want := TombstoneDeliveryCursor{TombstoneHash: "sha256:" + strings.Repeat("a", 64), NodeID: "nd_" + strings.Repeat("b", 22)}
	if err := store.SetTombstoneDeliveryCursor(ctx, want); err != nil {
		t.Fatalf("persist cursor: %v", err)
	}
	reopened, err := Open(Options{Root: f.root, Authorizer: allowAuthority{}, Keys: f.store.keys, Now: f.store.now})
	if err != nil {
		t.Fatalf("reopen migrated state: %v", err)
	}
	got, err := reopened.GetTombstoneDeliveryCursor(context.Background())
	if err != nil || got != want {
		t.Fatalf("reopened cursor=%+v err=%v, want %+v", got, err, want)
	}
}

func TestLeaseStateV2RejectsMalformedTombstoneDeliveryCursor(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{name: "invalid hash", cursor: `{"tombstone_hash":"not-a-hash","node_id":"nd_bbbbbbbbbbbbbbbbbbbbbb"}`},
		{name: "invalid node", cursor: `{"tombstone_hash":"sha256:` + strings.Repeat("a", 64) + `","node_id":"node"}`},
		{name: "unknown cursor field", cursor: `{"tombstone_hash":"sha256:` + strings.Repeat("a", 64) + `","node_id":"nd_bbbbbbbbbbbbbbbbbbbbbb","extra":true}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "lease-state")
			if err := os.Mkdir(root, privateDirMode); err != nil {
				t.Fatal(err)
			}
			state := `{"state_version":"2","leases":[],"tombstones":[],"tombstone_delivery_cursor":` + test.cursor + `}`
			if err := os.WriteFile(filepath.Join(root, stateName), []byte(state), privateFileMode); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Options{Root: root, Keys: testKeys{}}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("malformed v2 cursor accepted: %v", err)
			}
		})
	}
}

func TestCiphertextHashBounded(t *testing.T) {
	data := []byte("ciphertext")
	want := sha256.Sum256(data)
	got, err := hashBounded(context.Background(), bytes.NewReader(data), strconvItoa(len(data)))
	if err != nil || got != "sha256:"+hex.EncodeToString(want[:]) {
		t.Fatalf("hash=%s err=%v", got, err)
	}
}

func strconvItoa(v int) string { return strconv.Itoa(v) }
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
