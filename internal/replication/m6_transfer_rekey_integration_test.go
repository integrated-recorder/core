package replication

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
)

// TestOwnerSubjectTransferRequiresRecipientVerifiedRekeyHistory joins the
// durable authority gate to actual immutable archive re-encryption. It is a
// package integration test, not a substitute for independent-node acceptance.
func TestOwnerSubjectTransferRequiresRecipientVerifiedRekeyHistory(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	oldPublic, oldPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	newPublic, newPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentityID, err := identityprotocol.IdentityID(oldPublic)
	if err != nil {
		t.Fatal(err)
	}
	newIdentityID, err := identityprotocol.IdentityID(newPublic)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	newNodePublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	newNodeID, err := identityprotocol.NodeID(newNodePublic)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity := integrationIdentityState(oldIdentityID, oldPublic, nil, now)
	newIdentity := integrationIdentityState(newIdentityID, newPublic, recoveryPrivate.PublicKey().Bytes(), now)
	resolver := integrationIdentityResolver{oldIdentityID: oldIdentity, newIdentityID: newIdentity}.resolve

	archiveRoot := t.TempDir()
	replicationRoot := filepath.Join(archiveRoot, "replicas")
	catalogRoot := filepath.Join(archiveRoot, "catalog")
	archiveStore, err := Open(replicationRoot)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := archivecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	snapshots, canonical := integrationArchiveHistory(t, ctx, catalog)
	archiveID := snapshots[len(snapshots)-1].Content.ArchiveID

	authorityRoot := filepath.Join(archiveRoot, "authority")
	authorityStore, err := authority.Open(authorityRoot)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := authority.NewPersonOwnerSubject(oldIdentityID, "1", now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	created := integrationSignedAuthorityEvent(t, subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, oldIdentity, oldPrivate, now)
	if err := authorityStore.Apply(created, resolver, now); err != nil {
		t.Fatalf("create and claim OwnerSubject: %v", err)
	}
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: newNodeID}
	oldGrant := integrationGrant(t, subject.ID, oldIdentityID, newNodeID, archiveID, "1", []string{"archive.replicate", "key.rotate"}, now, oldPrivate, oldPublic)
	state, err := authorityStore.Snapshot(subject.ID, resolver, now)
	if err != nil {
		t.Fatal(err)
	}
	grantEvent := integrationSignedAuthorityEvent(t, subject.ID, "2", state.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{oldGrant}}, oldIdentity, oldPrivate, now.Add(time.Second))
	if err := authorityStore.Apply(grantEvent, resolver, now.Add(time.Second)); err != nil {
		t.Fatalf("issue old-controller rekey capability: %v", err)
	}
	verifier := transferRekeyAuthorizer{store: authorityStore, resolve: resolver}
	oldDEK, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer oldDEK.Destroy()
	newDEK, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer newDEK.Destroy()
	auth := Authorization{SubjectID: subject.ID, Actor: actor, Chain: []authority.SignedGrant{oldGrant}, Verifier: verifier}
	for _, snapshot := range snapshots {
		if _, err := archiveStore.EncryptSnapshot(ctx, snapshot, canonical, oldDEK, 4, auth); err != nil {
			t.Fatalf("encrypt original epoch for snapshot %s: %v", snapshot.SnapshotID, err)
		}
	}
	if err := authorityStore.AuthorizeChain(subject.ID, []authority.SignedGrant{oldGrant}, resolver, now.Add(2*time.Second), actor,
		archiveID, snapshots[1].SnapshotID, "key.rotate"); err != nil {
		t.Fatalf("old controller key.rotate grant denied before transfer: %v", err)
	}

	newEnvelope, err := cryptoarchive.WrapDEK(newDEK, archiveID, 5, recoveryPrivate.PublicKey().Bytes(),
		mustIntegrationKeyID(t, oldPublic), oldPrivate)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := archiveStore.RekeyArchiveHistory(ctx, catalog, archiveID,
		func(context.Context, archivesnapshot.Snapshot) (CanonicalSource, error) { return canonical, nil },
		4, 5, oldDEK, newDEK, newEnvelope, oldPublic, recoveryPrivate.PublicKey().Bytes(), auth)
	if err != nil {
		t.Fatalf("prepare actual multi-snapshot history rekey: %v", err)
	}
	openedDEK, err := cryptoarchive.OpenDEK(newEnvelope, cryptoarchive.EnvelopeExpectation{
		ArchiveID: archiveID, KeyEpoch: 5, RecipientKeyID: cryptoarchive.RecipientKeyID(recoveryPrivate.PublicKey().Bytes()),
		SignerKeyID: mustIntegrationKeyID(t, oldPublic), SignerPublicKey: oldPublic, RecipientPrivate: recoveryPrivate.Bytes(),
	})
	if err != nil {
		t.Fatalf("new controller could not open its recovery envelope: %v", err)
	}
	defer openedDEK.Destroy()
	recipientEvidence, err := archiveStore.VerifyRekeyHistory(ctx, catalog, archiveID, 4, 5, oldDEK, openedDEK, oldPublic, recoveryPrivate.PublicKey().Bytes())
	if err != nil {
		t.Fatalf("recipient did not verify the actual encrypted history: %v", err)
	}
	if recipientEvidence != prepared {
		t.Fatalf("recipient verification differs from prepared history: recipient=%+v prepared=%+v", recipientEvidence, prepared)
	}

	// Dual consent advances the controller/epoch, clears old grants and gates
	// every previously claimed archive until the new controller acknowledges
	// actual recipient-side history verification.
	transferContext := authority.VerificationContext{Subject: subject, SubjectPolicyEpoch: "1", Identity: oldIdentity, Now: now.Add(3 * time.Second)}
	offer, err := authority.NewTransferOffer(transferContext, authority.Principal{Kind: authority.PrincipalIdentity, ID: newIdentityID},
		now.Add(time.Hour), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	offer, err = authority.BindRecipientHead(offer, newIdentity, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	signedOffer, err := authority.SignTransferOffer(offer, oldPrivate)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := authority.AcceptTransferOffer(signedOffer, newPrivate)
	if err != nil {
		t.Fatal(err)
	}
	state, err = authorityStore.Snapshot(subject.ID, resolver, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	transferEvent := integrationSignedAuthorityEvent(t, subject.ID, "3", state.HeadHash, authority.AuthorityTransfer,
		authority.TransferPayload{Accepted: accepted}, oldIdentity, oldPrivate, now.Add(3*time.Second))
	if err := authorityStore.Apply(transferEvent, resolver, now.Add(3*time.Second)); err != nil {
		t.Fatalf("apply dual-consent controller transfer: %v", err)
	}
	transferHash, err := authority.AuthorityEventHash(transferEvent.Event)
	if err != nil {
		t.Fatal(err)
	}
	state, err = authorityStore.Snapshot(subject.ID, resolver, now.Add(4*time.Second))
	if err != nil || state.Subject.Controller.ID != newIdentityID || state.Subject.PolicyEpoch != "2" || len(state.RekeyRequiredArchives) != 1 {
		t.Fatalf("transfer did not persist recipient ownership and rekey gate: state=%+v err=%v", state, err)
	}
	if err := authorityStore.AuthorizeChain(subject.ID, []authority.SignedGrant{oldGrant}, resolver, now.Add(4*time.Second), actor,
		archiveID, snapshots[1].SnapshotID, "archive.replicate"); !errors.Is(err, authority.ErrRekeyRequired) {
		t.Fatalf("archive was not fail-closed during transfer rekey gate: %v", err)
	}

	evidence := authority.ArchiveRekeyEvidence{
		ArchiveID: recipientEvidence.ArchiveID, PreviousKeyEpoch: recipientEvidence.PreviousKeyEpoch,
		NewKeyEpoch: recipientEvidence.NewKeyEpoch, SnapshotCount: recipientEvidence.SnapshotCount,
		HeadSnapshotID: recipientEvidence.HeadSnapshotID, BundleSetCommitment: recipientEvidence.BundleSetCommitment,
		EnvelopeSHA256: recipientEvidence.EnvelopeSHA256, RecipientKeyID: recipientEvidence.RecipientKeyID,
	}
	completeEvent := func(archives []authority.ArchiveRekeyEvidence) authority.SignedAuthorityEvent {
		t.Helper()
		current, snapshotErr := authorityStore.Snapshot(subject.ID, resolver, now.Add(5*time.Second))
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		return integrationSignedAuthorityEvent(t, subject.ID, "4", current.HeadHash, authority.AuthorityCompleteRekey,
			authority.CompleteRekeyPayload{TransferEventHash: transferHash, Archives: archives}, newIdentity, newPrivate, now.Add(5*time.Second))
	}
	if err := authorityStore.Apply(completeEvent(nil), resolver, now.Add(5*time.Second)); !errors.Is(err, authority.ErrInvalid) {
		t.Fatalf("incomplete rekey evidence cleared transfer gate: %v", err)
	}
	wrongRecipientEvidence := evidence
	wrongRecipientEvidence.RecipientKeyID = cryptoarchive.RecipientKeyID(bytes.Repeat([]byte{0x99}, 32))
	if err := authorityStore.Apply(completeEvent([]authority.ArchiveRekeyEvidence{wrongRecipientEvidence}), resolver, now.Add(5*time.Second)); !errors.Is(err, authority.ErrInvalid) {
		t.Fatalf("wrong-recipient rekey evidence cleared transfer gate: %v", err)
	}
	validCompletion := completeEvent([]authority.ArchiveRekeyEvidence{evidence})
	if err := authorityStore.Apply(validCompletion, resolver, now.Add(5*time.Second)); err != nil {
		t.Fatalf("recipient verified complete rekey event rejected: %v", err)
	}

	// Durable replay must preserve both the transfer and the exact recipient
	// verification outcome. The previous controller's grant remains unusable.
	authorityStore, err = authority.Open(authorityRoot)
	if err != nil {
		t.Fatal(err)
	}
	state, err = authorityStore.Snapshot(subject.ID, resolver, now.Add(6*time.Second))
	if err != nil || state.Subject.Controller.ID != newIdentityID || state.Subject.PolicyEpoch != "2" || len(state.RekeyRequiredArchives) != 0 {
		t.Fatalf("completed transfer/rekey did not survive authority reopen: state=%+v err=%v", state, err)
	}
	if err := authorityStore.AuthorizeChain(subject.ID, []authority.SignedGrant{oldGrant}, resolver, now.Add(6*time.Second), actor,
		archiveID, snapshots[1].SnapshotID, "archive.replicate"); !errors.Is(err, authority.ErrDenied) {
		t.Fatalf("old controller retained archive authority after transfer: %v", err)
	}
	newGrant := integrationGrant(t, subject.ID, newIdentityID, newNodeID, archiveID, "2", []string{"archive.view"}, now.Add(7*time.Second), newPrivate, newPublic)
	state, err = authorityStore.Snapshot(subject.ID, resolver, now.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	newGrantEvent := integrationSignedAuthorityEvent(t, subject.ID, "5", state.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{newGrant}}, newIdentity, newPrivate, now.Add(7*time.Second))
	if err := authorityStore.Apply(newGrantEvent, resolver, now.Add(7*time.Second)); err != nil {
		t.Fatalf("new controller could not issue fresh post-rekey grant: %v", err)
	}
	if err := authorityStore.Authorize(subject.ID, resolver, now.Add(8*time.Second), actor, archiveID, snapshots[1].SnapshotID, "archive.view"); err != nil {
		t.Fatalf("new controller grant did not authorize the archive: %v", err)
	}

	archiveStore, err = Open(replicationRoot)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err = archivecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	verifiedAfterRestart, err := archiveStore.VerifyRekeyHistory(ctx, catalog, archiveID, 4, 5, oldDEK, openedDEK, oldPublic, recoveryPrivate.PublicKey().Bytes())
	if err != nil || verifiedAfterRestart != recipientEvidence {
		t.Fatalf("rekeyed ciphertext/snapshot history changed after restart: result=%+v err=%v", verifiedAfterRestart, err)
	}
	for _, snapshot := range snapshots {
		bundle, err := archiveStore.LoadBundle(snapshot, 5)
		if err != nil || len(bundle.Objects) != len(snapshot.Content.Objects) {
			t.Fatalf("new epoch bundle for snapshot %s is incomplete: bundle=%+v err=%v", snapshot.SnapshotID, bundle, err)
		}
	}
}

type integrationIdentityResolver map[string]authority.IdentityState

func (r integrationIdentityResolver) resolve(identityID, headHash string, now time.Time) (authority.IdentityState, error) {
	state, ok := r[identityID]
	if !ok || state.Frozen || !state.FreshUntil.After(now) || headHash != "" && headHash != state.HeadHash {
		return authority.IdentityState{}, authority.ErrStaleIdentity
	}
	return state, nil
}

func integrationIdentityState(identityID string, publicKey ed25519.PublicKey, recoveryPublic []byte, now time.Time) authority.IdentityState {
	return authority.IdentityState{
		IdentityID: identityID, HeadHash: "sha256:" + strings.Repeat("a", 64),
		VerifiedAt: now.Add(-time.Minute), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys:                 map[string]ed25519.PublicKey{"identity:" + identityID: append(ed25519.PublicKey(nil), publicKey...)},
		RecoveryEncryptionPublicKey: append([]byte(nil), recoveryPublic...),
	}
}

func integrationArchiveHistory(t *testing.T, ctx context.Context, catalog *archivecatalog.Store) ([]archivesnapshot.Snapshot, coordinateMapSource) {
	t.Helper()
	const recordingID = "m6-owner-transfer-rekey"
	session, err := archiveindex.NewSessionIdentity(recordingID, "hls", nil, "broadcast-m6-transfer")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory(recordingID, session)
	if err != nil {
		t.Fatal(err)
	}
	source := coordinateMapSource{}
	snapshots := make([]archivesnapshot.Snapshot, 0, 2)
	previous := ""
	for sequence := uint64(1); sequence <= 2; sequence++ {
		coordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 3, DiscontinuitySequence: 2, Sequence: sequence, Kind: archiveindex.ObjectMedia}
		payload := []byte("m6-canonical-segment-" + strconv.FormatUint(sequence, 10))
		digest := sha256.Sum256(payload)
		if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: "m6-transfer-" + strconv.FormatUint(sequence, 10), Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt: time.Date(2026, 11, 26, 0, 0, int(sequence), 0, time.UTC), PayloadPath: "recordings/m6-transfer/" + strconv.FormatUint(sequence, 10) + ".ts",
			Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]), Verification: archiveindex.VerificationVerified, Duration: 2,
		}); err != nil {
			t.Fatal(err)
		}
		snapshot, err := archivesnapshot.Build(inventory, sequence, previous)
		if err != nil {
			t.Fatal(err)
		}
		if err := catalog.Append(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		source[coordinate] = append([]byte(nil), payload...)
		snapshots = append(snapshots, snapshot)
		previous = snapshot.SnapshotID
	}
	return snapshots, source
}

func integrationSignedAuthorityEvent(t *testing.T, subjectID, sequence, previousHash, operation string, payload any,
	identity authority.IdentityState, privateKey ed25519.PrivateKey, now time.Time) authority.SignedAuthorityEvent {
	t.Helper()
	event, err := authority.NewAuthorityEvent(subjectID, sequence, previousHash, operation, payload, identity, now)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := authority.SignAuthorityEvent(event, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func integrationGrant(t *testing.T, subjectID, issuerID, audienceNodeID, archiveID, policyEpoch string, actions []string,
	now time.Time, privateKey ed25519.PrivateKey, publicKey ed25519.PublicKey) authority.SignedGrant {
	t.Helper()
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: issuerID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: audienceNodeID},
		ArchiveID: archiveID, Actions: actions,
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce), PolicyEpoch: policyEpoch, IssuerKeyID: keyID,
	}
	signed, err := authority.SignGrant(grant, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func mustIntegrationKeyID(t *testing.T, publicKey ed25519.PublicKey) string {
	t.Helper()
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return keyID
}

type transferRekeyAuthorizer struct {
	store   *authority.Store
	resolve authority.AuthorityIdentityResolver
}

func (v transferRekeyAuthorizer) Authorize(_ context.Context, subjectID, archiveID, snapshotID, action string,
	actor authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	return v.store.AuthorizeChain(subjectID, chain, v.resolve, now, actor, archiveID, snapshotID, action)
}

var _ lease.Authorizer = transferRekeyAuthorizer{}
