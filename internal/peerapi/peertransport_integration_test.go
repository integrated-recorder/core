package peerapi

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
	"github.com/integrated-recorder/core/internal/replication"
	"github.com/pion/webrtc/v4"
)

func TestAuthenticatedPionSessionServesCapabilityCheckedSnapshot(t *testing.T) {
	t.Run("same_identity", func(t *testing.T) {
		runAuthenticatedPionLeaseLifecycle(t, false)
	})
	t.Run("cross_identity", func(t *testing.T) {
		runAuthenticatedPionLeaseLifecycle(t, true)
	})
}

func runAuthenticatedPionLeaseLifecycle(t *testing.T, crossIdentity bool) {
	t.Helper()
	identityPublic, identityPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + string(bytes.Repeat([]byte{'a'}, 64))
	providerIdentityPublic := identityPublic
	providerIdentityID := identityID
	providerHead := head
	if crossIdentity {
		providerIdentityPublic, _, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		providerIdentityID, err = identityprotocol.IdentityID(providerIdentityPublic)
		if err != nil {
			t.Fatal(err)
		}
		providerHead = "sha256:" + strings.Repeat("d", 64)
	}
	sourceNode, sourceInfo := newPeerAPINode(t)
	targetNode, targetInfo := newPeerAPINode(t)
	verifiedAt := time.Now().UTC().Add(-time.Second)
	sourceState := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"identity:" + identityID:    identityPublic,
			"node:" + sourceInfo.NodeID: nodePublicFromInfo(t, sourceInfo),
		},
	}
	if !crossIdentity {
		sourceState.CurrentKeys["node:"+targetInfo.NodeID] = nodePublicFromInfo(t, targetInfo)
	}
	providerState := authority.IdentityState{
		IdentityID: providerIdentityID, HeadHash: providerHead, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"identity:" + providerIdentityID: providerIdentityPublic,
			"node:" + targetInfo.NodeID:      nodePublicFromInfo(t, targetInfo),
		},
	}
	states := map[string]authority.IdentityState{identityID: sourceState}
	if crossIdentity {
		states[providerIdentityID] = providerState
	}
	resolve := func(requestedIdentity, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		state, ok := states[requestedIdentity]
		if !ok || requestedHead != "" && requestedHead != state.HeadHash {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return state, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sourceSession, targetSession := authenticatedPionPair(t, ctx, sourceNode, targetNode, identityID, head, providerIdentityID, providerHead, resolve)
	defer sourceSession.Close()
	defer targetSession.Close()

	snapshot := peerAPITestSnapshot(t)
	keyID, err := identityprotocol.KeyID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	ownerSubjectID := "os_aaaaaaaaaaaaaaaaaaaaaa"
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: sourceInfo.NodeID},
		ArchiveID: snapshot.Content.ArchiveID, Actions: []string{"archive.replicate", "archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32)),
		PolicyEpoch: "1", IssuerKeyID: keyID,
	}, identityPrivate)
	if err != nil {
		t.Fatal(err)
	}
	targetGrant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: targetInfo.NodeID},
		ArchiveID: snapshot.Content.ArchiveID, Actions: []string{"archive.replicate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5b}, 32)),
		PolicyEpoch: "1", IssuerKeyID: keyID,
	}, identityPrivate)
	if err != nil {
		t.Fatal(err)
	}
	deleteGrant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: sourceInfo.NodeID},
		ArchiveID: snapshot.Content.ArchiveID, Actions: []string{"archive.delete"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5c}, 32)),
		PolicyEpoch: "1", IssuerKeyID: keyID,
	}, identityPrivate)
	if err != nil {
		t.Fatal(err)
	}
	grantVerification := authority.VerificationContext{
		Subject: authority.OwnerSubject{
			ID: ownerSubjectID, Type: authority.SubjectPerson,
			Controller:       authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
			ControllerPolicy: authority.ControllerRecoveryRoot, PolicyEpoch: "1", CreatedAt: now,
		},
		SubjectPolicyEpoch: "1", Identity: sourceState, Now: now,
	}
	requestID, nonce, err := peerprotocol.NewCommand(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"operation":"snapshot.get"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: sourceSession.SessionID(),
		SourceIdentityID: identityID, SourceHeadHash: head, SourceNodeID: sourceInfo.NodeID,
		TargetIdentityID: providerIdentityID, TargetNodeID: targetInfo.NodeID, OwnerSubjectID: grant.Grant.SubjectID,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.view",
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{grant}, Payload: payload,
	}
	signed, err := sourceNode.SignPeerCommand(command, resolve, now)
	if err != nil {
		t.Fatalf("sign current, session-scoped peer command: %v", err)
	}
	client, err := NewClient(sourceSession, MaxPeerResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	authorizer := &peerAPIScopeProbe{verification: grantVerification}
	keyResolver := peerAPITestKeys{
		"identity:" + identityID:         identityPublic,
		"identity:" + providerIdentityID: providerIdentityPublic,
		"node:" + sourceInfo.NodeID:      nodePublicFromInfo(t, sourceInfo),
		"node:" + targetInfo.NodeID:      nodePublicFromInfo(t, targetInfo),
	}
	localLeaseRoot := filepath.Join(t.TempDir(), "replica-leases")
	localLeases, err := lease.Open(lease.Options{Root: localLeaseRoot, Keys: keyResolver, Authorizer: authorizer})
	if err != nil {
		t.Fatalf("open receiving node durable lease store: %v", err)
	}
	sourceLeaseRoot := filepath.Join(t.TempDir(), "source-leases")
	sourceLeases, err := lease.Open(lease.Options{Root: sourceLeaseRoot, Keys: keyResolver, Authorizer: authorizer})
	if err != nil {
		t.Fatalf("open source node durable lease store: %v", err)
	}
	canonicalBytes := []byte("canonical media bytes")
	coordinate := snapshot.Content.Objects[0].Coordinate
	canonicalSource := peerAPICanonicalSource{coordinate: coordinate, data: canonicalBytes}
	ciphertextStore, err := replication.Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	archiveAuthorization := replication.Authorization{
		SubjectID: grant.Grant.SubjectID, Actor: authority.Principal{Kind: authority.PrincipalNode, ID: sourceInfo.NodeID},
		Chain: []authority.SignedGrant{grant}, Verifier: authorizer,
	}
	bundle, err := ciphertextStore.EncryptSnapshot(ctx, snapshot, canonicalSource, dek, 1, archiveAuthorization)
	if err != nil || len(bundle.Objects) != 1 {
		t.Fatalf("encrypt canonical archive for peer transfer: objects=%d err=%v", len(bundle.Objects), err)
	}
	recoveryPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 1, recoveryPrivate.PublicKey().Bytes(), ownerKeyID, identityPrivate)
	if err != nil {
		t.Fatalf("wrap archive DEK for target recovery key: %v", err)
	}
	if err := ciphertextStore.PersistKeyEnvelope(snapshot, envelope, identityPublic); err != nil {
		t.Fatalf("persist owner-signed recipient key envelope: %v", err)
	}
	switchableCiphertexts := &peerAPISwitchableCiphertextStore{store: ciphertextStore}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: switchableCiphertexts, Leases: localLeases, Authorizer: authorizer,
		IdentityID: providerIdentityID, NodeID: targetInfo.NodeID,
		Resolve: resolve, Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: targetNode}, Signer: targetNode,
		Now: func() time.Time { return time.Now().UTC() },
	}
	serveCtx, stopServe := context.WithCancel(ctx)
	defer stopServe()
	serveResult := make(chan error, 1)
	go func() { serveResult <- service.Serve(serveCtx, targetSession) }()
	response, err := client.Do(ctx, signed)
	if err != nil || response.Kind != responseKindSnapshot {
		t.Fatalf("peer API response kind=%d body=%x err=%v", response.Kind, response.Body, err)
	}
	returned, err := archivesnapshot.Parse(response.Body)
	if err != nil || archivesnapshot.Verify(returned) != nil || returned.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("peer API returned wrong snapshot=%q err=%v", returned.SnapshotID, err)
	}
	if authorizer.calls.Load() != 4 {
		t.Fatalf("current authority was checked %d times, want encryption admission and command receipt/pre-read", authorizer.calls.Load())
	}
	if err := (nodeidentity.PeerCommandReplayConsumer{NodeStore: targetNode}).ConsumeCommand(signed, resolve, time.Now().UTC(), targetSession.SessionID(), targetSession.Remote(), providerIdentityID, targetInfo.NodeID); !errors.Is(err, peerprotocol.ErrReplay) {
		t.Fatalf("durable replay journal did not consume delivered command: %v", err)
	}

	replicationClient, err := NewScopedClient(sourceSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: head, NodeID: sourceInfo.NodeID,
		TargetIdentityID: providerIdentityID, TargetNodeID: targetInfo.NodeID, OwnerSubjectID: grant.Grant.SubjectID,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.replicate",
		GrantChain: []authority.SignedGrant{grant}, Resolve: resolve, Signer: sourceNode,
	})
	if err != nil {
		t.Fatalf("create capability-scoped replication client: %v", err)
	}
	manifest, err := replicationClient.ReplicaManifest(ctx, returned, 1)
	if err != nil {
		t.Fatalf("read public replica manifest over authenticated peer session: %v", err)
	}
	keyEpochs, err := replicationClient.KeyEpochs(ctx, returned, cryptoarchive.RecipientKeyID(recoveryPrivate.PublicKey().Bytes()))
	if err != nil || len(keyEpochs) != 1 || keyEpochs[0] != 1 {
		t.Fatalf("discover recipient-bound key epochs over authenticated peer session: epochs=%v err=%v", keyEpochs, err)
	}
	transferBundle, err := replication.BundleFromPublicReplicaManifest(returned, manifest)
	if err != nil || transferBundle.SnapshotID != snapshot.SnapshotID || len(transferBundle.Objects) != len(bundle.Objects) {
		t.Fatalf("peer replica manifest did not bind to returned snapshot: objects=%d err=%v", len(transferBundle.Objects), err)
	}
	if authorizer.calls.Load() != 8 {
		t.Fatalf("current authority was checked %d times after manifest and key epoch discovery, want 8", authorizer.calls.Load())
	}
	object := transferBundle.Objects[0]
	var receivedCiphertext bytes.Buffer
	for offset := int64(0); offset < object.CiphertextSize; {
		length := object.CiphertextSize - offset
		if length > replication.MaxCiphertextChunkBytes {
			length = replication.MaxCiphertextChunkBytes
		}
		rangeReader, openErr := replicationClient.OpenCiphertextRange(ctx, snapshot.Content.ArchiveID, snapshot.SnapshotID, object.ObjectID, 1, offset, length)
		if openErr != nil {
			t.Fatalf("request scoped ciphertext range at %d: %v", offset, openErr)
		}
		written, copyErr := io.CopyN(&receivedCiphertext, rangeReader, length)
		closeErr := rangeReader.Close()
		if copyErr != nil || closeErr != nil || written != length {
			t.Fatalf("ciphertext range at %d returned %d/%d bytes: copy=%v close=%v", offset, written, length, copyErr, closeErr)
		}
		offset += length
	}
	if receivedCiphertext.Len() != int(object.CiphertextSize) {
		t.Fatalf("ciphertext length=%d, want %d", receivedCiphertext.Len(), object.CiphertextSize)
	}
	var restored bytes.Buffer
	if err := cryptoarchive.DecryptObject(ctx, dek, cryptoarchive.ObjectDescriptor{
		ArchiveID: bundle.ArchiveID, Coordinate: object.Coordinate, ObjectType: string(object.Coordinate.Kind),
		PlaintextSize: object.PlaintextSize, PlaintextSHA256: object.PlaintextSHA256,
		CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256, KeyEpoch: 1,
	}, bytes.NewReader(receivedCiphertext.Bytes()), &restored); err != nil {
		t.Fatalf("verify and decrypt ciphertext returned through the authenticated peer session: %v", err)
	}
	if !bytes.Equal(restored.Bytes(), canonicalBytes) {
		t.Fatalf("P2P restored object=%q, want exact canonical bytes %q", restored.Bytes(), canonicalBytes)
	}
	wantRangeCalls := int((object.CiphertextSize + replication.MaxCiphertextChunkBytes - 1) / replication.MaxCiphertextChunkBytes)
	if authorizer.calls.Load() != int32(8+3*wantRangeCalls) || wantRangeCalls < 1 {
		t.Fatalf("authority calls=%d, want encryption, snapshot/manifest commands, and ciphertext-store checks for %d range(s)", authorizer.calls.Load(), wantRangeCalls)
	}

	// Exercise the other direction as a complete replica lifecycle: the local
	// target Node Key accepts a source-signed offer, durably records the lease,
	// imports bounded ciphertext ranges over its authenticated Pion session,
	// checkpoints each durable prefix, and activates only after local readback.
	publicManifest, err := replication.PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := replication.ReplicaManifestHash(snapshot, publicManifest)
	if err != nil {
		t.Fatal(err)
	}
	refs := bundle.CiphertextRefs()
	var reservedBytes int64
	objects := make([]lease.EncryptedObject, 0, len(refs))
	for _, ref := range refs {
		size, parseErr := strconv.ParseInt(ref.CiphertextSize, 10, 64)
		if parseErr != nil || size <= 0 {
			t.Fatalf("invalid ciphertext reference size %q: %v", ref.CiphertextSize, parseErr)
		}
		reservedBytes += size
		objects = append(objects, lease.EncryptedObject{
			ObjectID: ref.ObjectID, CiphertextSize: ref.CiphertextSize, CiphertextSHA256: ref.CiphertextSHA256,
		})
	}
	leaseNow := time.Now().UTC().Truncate(time.Second)
	offer, err := sourceNode.SignReplicaOffer(lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: "rl_" + strings.Repeat("A", 22),
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: "1",
		PlaintextManifestHash: snapshot.ContentHash, ReplicaManifestHash: manifestHash,
		EncryptedObjectSet: objects, ProviderNodeID: targetInfo.NodeID, OwnerSubjectID: grant.Grant.SubjectID,
		ReservedBytes: strconv.FormatInt(reservedBytes, 10), IssuedAt: leaseNow.Format(time.RFC3339),
		ExpiresAt: leaseNow.Add(time.Hour).Format(time.RFC3339), RenewalPolicy: "manual",
	}, []authority.SignedGrant{grant})
	if err != nil {
		t.Fatalf("sign replica offer with source Node Key: %v", err)
	}
	offerHash, err := lease.OfferHash(offer.Offer)
	if err != nil {
		t.Fatalf("hash signed replica offer: %v", err)
	}
	issued, err := sourceLeases.IssueOffer(ctx, offer)
	if err != nil || issued.Status != lease.StatusOffered {
		t.Fatalf("persist source lease offer before delivery: status=%s err=%v", issued.Status, err)
	}

	sourceService := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: ciphertextStore, Leases: sourceLeases, Authorizer: authorizer,
		IdentityID: identityID, NodeID: sourceInfo.NodeID,
		Resolve: resolve, Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: sourceNode}, Signer: sourceNode,
		Now: func() time.Time { return time.Now().UTC() },
	}
	sourceServeCtx, stopSourceServe := context.WithCancel(ctx)
	sourceServeResult := make(chan error, 1)
	go func() { sourceServeResult <- sourceService.Serve(sourceServeCtx, sourceSession) }()
	providerClient, err := NewScopedClient(targetSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: providerIdentityID, IdentityHead: providerHead, NodeID: targetInfo.NodeID,
		TargetIdentityID: identityID, TargetNodeID: sourceInfo.NodeID, OwnerSubjectID: grant.Grant.SubjectID,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.replicate",
		GrantChain: []authority.SignedGrant{targetGrant}, Resolve: resolve, Signer: targetNode,
	})
	if err != nil {
		stopSourceServe()
		t.Fatalf("create receiving-node scoped peer client: %v", err)
	}
	providerManifest, err := providerClient.ReplicaManifest(ctx, snapshot, 1)
	if err != nil {
		stopSourceServe()
		t.Fatalf("fetch replica manifest over authenticated source session: %v", err)
	}
	authorizerCallsBeforeEnvelope := authorizer.calls.Load()
	providerEnvelope, err := providerClient.KeyEnvelope(ctx, snapshot, 1, cryptoarchive.RecipientKeyID(recoveryPrivate.PublicKey().Bytes()))
	if err != nil {
		stopSourceServe()
		t.Fatalf("fetch recipient-bound key envelope over authenticated source session: %v", err)
	}
	if got := authorizer.calls.Load() - authorizerCallsBeforeEnvelope; got != 2 {
		stopSourceServe()
		t.Fatalf("key-envelope command authority checks=%d, want 2", got)
	}
	recoveredDEK, err := cryptoarchive.OpenDEK(providerEnvelope, cryptoarchive.EnvelopeExpectation{
		ArchiveID: snapshot.Content.ArchiveID, KeyEpoch: 1, RecipientKeyID: envelope.RecipientKeyID,
		SignerKeyID: ownerKeyID, SignerPublicKey: identityPublic, RecipientPrivate: recoveryPrivate.Bytes(),
	})
	if err != nil {
		stopSourceServe()
		t.Fatalf("open received key envelope using receiving recovery key: %v", err)
	}
	defer recoveredDEK.Destroy()
	providerReplicaRoot := filepath.Join(t.TempDir(), "provider-replica")
	providerReplicaStore, err := replication.Open(providerReplicaRoot)
	if err != nil {
		stopSourceServe()
		t.Fatal(err)
	}
	switchableCiphertexts.Set(providerReplicaStore)
	imported, err := providerClient.ImportReplicaFromPeer(ctx, localLeases, providerReplicaStore, targetNode, targetNode,
		snapshot.Content.ArchiveID, snapshot.SnapshotID, offer.Offer.LeaseID)
	importedManifestHash, importedHashErr := replication.ReplicaManifestHash(snapshot, imported.Manifest)
	expectedManifestHash, expectedHashErr := replication.ReplicaManifestHash(snapshot, providerManifest)
	if err != nil || importedHashErr != nil || expectedHashErr != nil || importedManifestHash != expectedManifestHash || len(imported.Bundle.Objects) != 1 ||
		imported.Snapshot.ContentHash != snapshot.ContentHash || imported.Lease.Status != lease.StatusActive || len(imported.Lease.Checkpoints) != 1 {
		t.Fatalf("authenticated offer acceptance, resumable import and source readback: status=%s objects=%d err=%v", imported.Lease.Status, len(imported.Bundle.Objects), err)
	}
	providerBundle := imported.Bundle
	providerSnapshot := imported.Snapshot
	sourceActive, err := sourceLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || sourceActive.Status != lease.StatusActive {
		t.Fatalf("source readback verification did not activate its lease projection: status=%q err=%v", sourceActive.Status, err)
	}
	sourceAccepted := sourceActive
	if sourceAccepted.Acceptance.Acceptance.ProviderNodeID != targetInfo.NodeID {
		t.Fatalf("source did not durably receive provider acceptance: provider=%q", sourceAccepted.Acceptance.Acceptance.ProviderNodeID)
	}
	renewalNow := time.Now().UTC().Truncate(time.Second)
	acceptedAt, err := time.Parse(time.RFC3339, sourceActive.Acceptance.Acceptance.AcceptedAt)
	if err != nil {
		t.Fatalf("parse source acceptance time: %v", err)
	}
	if !renewalNow.After(acceptedAt) {
		renewalNow = acceptedAt.Add(time.Second)
	}
	renewalRequest, err := sourceNode.SignReplicaRenewalRequest(lease.Renewal{
		Version: lease.ProtocolVersion, LeaseID: offer.Offer.LeaseID, OfferHash: offerHash,
		OwnerSubjectID: grant.Grant.SubjectID, IssuedAt: renewalNow.Format(time.RFC3339),
		ExpiresAt: renewalNow.Add(2 * time.Hour).Format(time.RFC3339),
	}, []authority.SignedGrant{grant})
	if err != nil {
		t.Fatalf("sign owner-authorized renewal request: %v", err)
	}
	if _, err := sourceLeases.BeginRenewal(ctx, renewalRequest); err != nil {
		t.Fatalf("persist owner renewal at source: %v", err)
	}
	// Model a provider process stopping after the source durably accepted the
	// provider-signed receipt, but before the provider commits its own lease.
	// The provider must remain in StatusRenewing and resume by replaying the
	// exact deterministic receipt after reopening its durable Store.
	requestAtProvider, requestHash, err := providerClient.ReplicaRenewalRequest(ctx, snapshot.Content.ArchiveID, snapshot.SnapshotID, offer.Offer.LeaseID)
	if err != nil || requestHash != offerHash {
		t.Fatalf("fetch pending renewal before provider crash: hash=%q err=%v", requestHash, err)
	}
	if _, err := localLeases.BeginRenewal(ctx, requestAtProvider); err != nil {
		t.Fatalf("persist provider pending renewal before crash: %v", err)
	}
	providerReceipt, err := targetNode.SignReplicaRenewalReceipt(requestAtProvider)
	if err != nil {
		t.Fatalf("sign provider renewal receipt before crash: %v", err)
	}
	if err := providerClient.SubmitReplicaRenewal(ctx, snapshot.Content.ArchiveID, snapshot.SnapshotID, offer.Offer.LeaseID, providerReceipt); err != nil {
		t.Fatalf("source did not durably accept provider receipt before crash: %v", err)
	}
	localLeases, err = lease.Open(lease.Options{Root: localLeaseRoot, Keys: keyResolver, Authorizer: authorizer})
	if err != nil {
		t.Fatalf("reopen provider lease store after simulated crash: %v", err)
	}
	providerPending, err := localLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || providerPending.Status != lease.StatusRenewing || providerPending.PendingRenewal == nil {
		t.Fatalf("provider renewal was not left restartable after source acknowledgment: status=%q pending=%t err=%v", providerPending.Status, providerPending.PendingRenewal != nil, err)
	}
	renewedProvider, err := providerClient.RenewReplicaLease(ctx, localLeases, targetNode, snapshot.Content.ArchiveID, snapshot.SnapshotID, offer.Offer.LeaseID)
	if err != nil || renewedProvider.Status != lease.StatusActive || len(renewedProvider.Renewals) != 1 {
		t.Fatalf("peer renewal transport at provider: status=%q renewals=%d err=%v", renewedProvider.Status, len(renewedProvider.Renewals), err)
	}
	renewedSource, err := sourceLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || renewedSource.Status != lease.StatusActive || len(renewedSource.Renewals) != 1 ||
		renewedSource.Renewals[0].Renewal.ExpiresAt != renewalRequest.Renewal.ExpiresAt {
		t.Fatalf("peer renewal was not committed at source: status=%q renewals=%d err=%v", renewedSource.Status, len(renewedSource.Renewals), err)
	}
	// Replaying the completed exchange models a lost final response. It must
	// return the same lease revision instead of creating another renewal.
	renewalRetry, err := providerClient.RenewReplicaLease(ctx, localLeases, targetNode, snapshot.Content.ArchiveID, snapshot.SnapshotID, offer.Offer.LeaseID)
	if err != nil || renewalRetry.Revision != renewedProvider.Revision || len(renewalRetry.Renewals) != 1 {
		t.Fatalf("completed peer renewal retry changed durable state: revision=%s renewals=%d err=%v", renewalRetry.Revision, len(renewalRetry.Renewals), err)
	}
	renewedSourceRetry, err := sourceLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || renewedSourceRetry.Revision != renewedSource.Revision || len(renewedSourceRetry.Renewals) != 1 {
		t.Fatalf("completed source renewal retry changed durable state: revision=%s renewals=%d err=%v", renewedSourceRetry.Revision, len(renewedSourceRetry.Renewals), err)
	}
	stopSourceServe()
	select {
	case serveErr := <-sourceServeResult:
		if !errors.Is(serveErr, context.Canceled) {
			t.Fatalf("source peer service cancellation=%v", serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("source peer service did not stop after context cancellation")
	}
	if providerBundle.Objects[0].CiphertextSHA256 != object.CiphertextSHA256 || providerBundle.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("activated replica binding differs from signed snapshot: %+v", providerBundle.Objects[0])
	}
	reopenedProviderStore, err := replication.Open(providerReplicaRoot)
	if err != nil {
		t.Fatal(err)
	}
	persistedSnapshot, err := reopenedProviderStore.LoadSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID)
	if err != nil || persistedSnapshot.ContentHash != providerSnapshot.ContentHash {
		t.Fatalf("reopen imported immutable snapshot descriptor: content hash=%q err=%v", persistedSnapshot.ContentHash, err)
	}
	persistedBundle, err := reopenedProviderStore.LoadBundle(snapshot, 1)
	if err != nil || len(persistedBundle.Objects) != 1 {
		t.Fatalf("reopen activated replica binding: objects=%d err=%v", len(persistedBundle.Objects), err)
	}
	persistedCiphertext, err := reopenedProviderStore.OpenCiphertextForPeer(ctx, snapshot, persistedBundle, replication.Authorization{
		SubjectID: grant.Grant.SubjectID,
		Actor:     authority.Principal{Kind: authority.PrincipalNode, ID: targetInfo.NodeID},
		Chain:     []authority.SignedGrant{targetGrant}, Verifier: authorizer,
	}, "archive.replicate", object.ObjectID)
	if err != nil {
		t.Fatalf("read back activated ciphertext under exact replication scope: %v", err)
	}
	var durableCiphertext bytes.Buffer
	_, copyErr := io.Copy(&durableCiphertext, persistedCiphertext)
	closeErr := persistedCiphertext.Close()
	if copyErr != nil || closeErr != nil || !bytes.Equal(durableCiphertext.Bytes(), receivedCiphertext.Bytes()) {
		t.Fatalf("reopened replica ciphertext differs from authenticated transfer: copy=%v close=%v", copyErr, closeErr)
	}
	var durableRestore bytes.Buffer
	if err := cryptoarchive.DecryptObject(ctx, recoveredDEK, cryptoarchive.ObjectDescriptor{
		ArchiveID: bundle.ArchiveID, Coordinate: object.Coordinate, ObjectType: string(object.Coordinate.Kind),
		PlaintextSize: object.PlaintextSize, PlaintextSHA256: object.PlaintextSHA256,
		CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256, KeyEpoch: 1,
	}, bytes.NewReader(durableCiphertext.Bytes()), &durableRestore); err != nil || !bytes.Equal(durableRestore.Bytes(), canonicalBytes) {
		t.Fatalf("durable replica failed exact source-side recovery check: restored=%q err=%v", durableRestore.Bytes(), err)
	}
	// Exercise the complete deletion path over the authenticated peer session.
	// The owner Node signs a durable source tombstone, the provider records it,
	// erases only its replica files, and the source persists the provider's
	// tombstone-bound deletion report before acknowledging delivery.
	tombstoneRecord, err := sourceLeases.CreateTombstone(ctx, ownerSubjectID, snapshot.Content.ArchiveID, "owner_requested", time.Now().UTC(), func(tombstone lease.Tombstone) (lease.SignedTombstone, error) {
		return sourceNode.SignReplicaTombstone(tombstone, []authority.SignedGrant{deleteGrant})
	})
	if err != nil || tombstoneRecord.Hash == "" {
		t.Fatalf("persist owner-signed source tombstone: hash=%q err=%v", tombstoneRecord.Hash, err)
	}
	sourceTombstoneClient, err := NewScopedClient(sourceSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: head, NodeID: sourceInfo.NodeID,
		TargetIdentityID: providerIdentityID, TargetNodeID: targetInfo.NodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: snapshot.Content.ArchiveID, Action: "archive.replicate",
		GrantChain: []authority.SignedGrant{grant}, Resolve: resolve, Signer: sourceNode,
	})
	if err != nil {
		t.Fatalf("create scoped tombstone delivery client: %v", err)
	}
	tombstoneReceipt, err := sourceTombstoneClient.SubmitTombstoneWithReceipt(ctx, tombstoneRecord.Signed)
	if err != nil {
		select {
		case serviceErr := <-serveResult:
			t.Fatalf("deliver signed tombstone and receive provider erasure report: %v (provider service exited: %v)", err, serviceErr)
		default:
			t.Fatalf("deliver signed tombstone and receive provider erasure report: %v", err)
		}
	}
	if tombstoneReceipt.TombstoneHash != tombstoneRecord.Hash || tombstoneReceipt.Erasure == nil ||
		tombstoneReceipt.Erasure.ArchiveID != snapshot.Content.ArchiveID || tombstoneReceipt.Erasure.TombstoneHash != tombstoneRecord.Hash ||
		tombstoneReceipt.Erasure.BundleFilesRemoved == 0 || tombstoneReceipt.Erasure.PrivateFilesRemoved == 0 || tombstoneReceipt.Erasure.CiphertextRemoved == 0 ||
		len(tombstoneReceipt.DeletionReports) != 1 {
		t.Fatalf("provider tombstone receipt did not report completed scoped erasure: %+v", tombstoneReceipt)
	}
	for _, report := range tombstoneReceipt.DeletionReports {
		if _, err := sourceLeases.ReportDeletion(ctx, report); err != nil {
			t.Fatalf("persist provider-signed deletion report at source: %v", err)
		}
	}
	if err := sourceLeases.MarkTombstoneDelivered(ctx, snapshot.Content.ArchiveID, targetInfo.NodeID, tombstoneRecord.Hash, time.Now().UTC()); err != nil {
		t.Fatalf("persist exact provider tombstone delivery acknowledgement: %v", err)
	}
	providerDeleted, err := localLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || providerDeleted.Status != lease.StatusDeleted || providerDeleted.DeletionReport == nil || providerDeleted.DeletionReport.Report.TombstoneHash != tombstoneRecord.Hash {
		t.Fatalf("provider lease did not durably record deletion: status=%q report=%+v err=%v", providerDeleted.Status, providerDeleted.DeletionReport, err)
	}
	sourceDeleted, err := sourceLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || sourceDeleted.Status != lease.StatusDeleted || sourceDeleted.DeletionReport == nil || sourceDeleted.DeletionReport.Report.TombstoneHash != tombstoneRecord.Hash {
		t.Fatalf("source did not persist the matching provider deletion report: status=%q report=%+v err=%v", sourceDeleted.Status, sourceDeleted.DeletionReport, err)
	}
	if _, err := providerReplicaStore.LoadBundle(snapshot, 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider replica bundle remains after accepted tombstone: %v", err)
	}
	if _, err := providerReplicaStore.LoadSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID); !errors.Is(err, replication.ErrSnapshotNotFound) {
		t.Fatalf("provider replica snapshot remains after accepted tombstone: %v", err)
	}
	if _, err := ciphertextStore.LoadBundle(snapshot, 1); err != nil {
		t.Fatalf("tombstone incorrectly removed the source's canonical encrypted archive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(providerReplicaRoot, "objects", object.ObjectID+".irac")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider ciphertext object remains after accepted tombstone: %v", err)
	}
	// A lost response or controller retry reuses the same immutable tombstone;
	// deletion remains idempotent and the source acknowledgement is unchanged.
	retryReceipt, err := sourceTombstoneClient.SubmitTombstoneWithReceipt(ctx, tombstoneRecord.Signed)
	if err != nil || retryReceipt.TombstoneHash != tombstoneRecord.Hash || retryReceipt.Erasure == nil ||
		retryReceipt.Erasure.BundleFilesRemoved != 0 || retryReceipt.Erasure.PrivateFilesRemoved != 0 || retryReceipt.Erasure.CiphertextRemoved != 0 {
		t.Fatalf("same tombstone retry was not idempotent: erasure=%+v err=%v", retryReceipt.Erasure, err)
	}
	for _, report := range retryReceipt.DeletionReports {
		if _, err := sourceLeases.ReportDeletion(ctx, report); err != nil {
			t.Fatalf("idempotently persist retried provider report: %v", err)
		}
	}
	if err := sourceLeases.MarkTombstoneDelivered(ctx, snapshot.Content.ArchiveID, targetInfo.NodeID, tombstoneRecord.Hash, time.Now().UTC()); err != nil {
		t.Fatalf("idempotently persist repeated tombstone delivery acknowledgement: %v", err)
	}
	reopenedSourceLeases, err := lease.Open(lease.Options{Root: sourceLeaseRoot, Keys: keyResolver, Authorizer: authorizer})
	if err != nil {
		t.Fatalf("reopen source lease store after tombstone acknowledgement: %v", err)
	}
	reopenedSourceLease, err := reopenedSourceLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || reopenedSourceLease.Status != lease.StatusDeleted || reopenedSourceLease.DeletionReport == nil ||
		reopenedSourceLease.DeletionReport.Report.TombstoneHash != tombstoneRecord.Hash {
		t.Fatalf("source deletion report did not survive durable reopen: status=%q report=%+v err=%v", reopenedSourceLease.Status, reopenedSourceLease.DeletionReport, err)
	}
	_, reopenedSourceTombstones, err := reopenedSourceLeases.Snapshot(ctx)
	if err != nil || len(reopenedSourceTombstones) != 1 || reopenedSourceTombstones[0].Hash != tombstoneRecord.Hash ||
		len(reopenedSourceTombstones[0].Deliveries) != 1 || reopenedSourceTombstones[0].Deliveries[0].NodeID != targetInfo.NodeID {
		t.Fatalf("source tombstone/delivery checkpoint did not survive durable reopen: tombstones=%+v err=%v", reopenedSourceTombstones, err)
	}
	reopenedProviderLeases, err := lease.Open(lease.Options{Root: localLeaseRoot, Keys: keyResolver, Authorizer: authorizer})
	if err != nil {
		t.Fatalf("reopen provider lease store after erasure: %v", err)
	}
	reopenedProviderLease, err := reopenedProviderLeases.Get(ctx, offer.Offer.LeaseID)
	if err != nil || reopenedProviderLease.Status != lease.StatusDeleted || reopenedProviderLease.DeletionReport == nil ||
		reopenedProviderLease.DeletionReport.Report.TombstoneHash != tombstoneRecord.Hash {
		t.Fatalf("provider deletion state did not survive durable reopen: status=%q report=%+v err=%v", reopenedProviderLease.Status, reopenedProviderLease.DeletionReport, err)
	}
	reopenedErasedStore, err := replication.Open(providerReplicaRoot)
	if err != nil {
		t.Fatalf("reopen provider replica store after erasure: %v", err)
	}
	if _, err := reopenedErasedStore.LoadBundle(snapshot, 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("erased provider bundle returned after durable reopen: %v", err)
	}
	if _, err := reopenedErasedStore.LoadSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID); !errors.Is(err, replication.ErrSnapshotNotFound) {
		t.Fatalf("erased provider snapshot returned after durable reopen: %v", err)
	}
}

func TestAuthenticatedPionSnapshotReadAcrossGlobalIdentities(t *testing.T) {
	ownerPublic, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerIdentityID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	requesterPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	requesterIdentityID, err := identityprotocol.IdentityID(requesterPublic)
	if err != nil {
		t.Fatal(err)
	}
	if requesterIdentityID == ownerIdentityID {
		t.Fatal("independent identity roots unexpectedly share an ID")
	}

	requesterNode, requesterInfo := newPeerAPINode(t)
	ownerNode, ownerInfo := newPeerAPINode(t)
	requesterHead := "sha256:" + strings.Repeat("b", 64)
	ownerHead := "sha256:" + strings.Repeat("c", 64)
	verifiedAt := time.Now().UTC().Add(-time.Second)
	states := map[string]authority.IdentityState{
		requesterIdentityID: {
			IdentityID: requesterIdentityID, HeadHash: requesterHead, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
			CurrentKeys: map[string]ed25519.PublicKey{
				authority.PrincipalIdentity + ":" + requesterIdentityID: requesterPublic,
				authority.PrincipalNode + ":" + requesterInfo.NodeID:    nodePublicFromInfo(t, requesterInfo),
			},
		},
		ownerIdentityID: {
			IdentityID: ownerIdentityID, HeadHash: ownerHead, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
			CurrentKeys: map[string]ed25519.PublicKey{
				authority.PrincipalIdentity + ":" + ownerIdentityID: ownerPublic,
				authority.PrincipalNode + ":" + ownerInfo.NodeID:    nodePublicFromInfo(t, ownerInfo),
			},
		},
	}
	resolve := func(identityID, head string, _ time.Time) (authority.IdentityState, error) {
		state, ok := states[identityID]
		if !ok || head != "" && head != state.HeadHash {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return state, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	requesterSession, ownerSession := authenticatedPionPair(
		t, ctx, requesterNode, ownerNode,
		requesterIdentityID, requesterHead, ownerIdentityID, ownerHead, resolve,
	)
	defer requesterSession.Close()
	defer ownerSession.Close()

	snapshot := peerAPITestSnapshot(t)
	now := time.Now().UTC().Truncate(time.Second)
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	ownerSubjectID := "os_bbbbbbbbbbbbbbbbbbbbbb"
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
		Issuer: authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerIdentityID},
		// This grant crosses the identity boundary by naming the requester's
		// globally derived Node ID. The peer session proves possession of that
		// Node Key; the signed OwnerSubject grant remains the access authority.
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: requesterInfo.NodeID},
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x6d}, 32)),
		PolicyEpoch: "1", IssuerKeyID: ownerKeyID,
	}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	verification := authority.VerificationContext{
		Subject: authority.OwnerSubject{
			ID: ownerSubjectID, Type: authority.SubjectPerson,
			Controller:       authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerIdentityID},
			ControllerPolicy: authority.ControllerRecoveryRoot, PolicyEpoch: "1", CreatedAt: now,
		},
		SubjectPolicyEpoch: "1", Identity: states[ownerIdentityID], Now: now,
	}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Authorizer: &peerAPIScopeProbe{verification: verification},
		IdentityID: ownerIdentityID, NodeID: ownerInfo.NodeID, Resolve: resolve,
		Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: ownerNode}, Signer: ownerNode,
		Now: func() time.Time { return time.Now().UTC() },
	}
	serveCtx, stopServe := context.WithCancel(ctx)
	serveResult := make(chan error, 1)
	go func() { serveResult <- service.Serve(serveCtx, ownerSession) }()
	defer func() {
		stopServe()
		select {
		case <-serveResult:
		case <-time.After(time.Second):
			t.Error("cross-identity peer service did not stop")
		}
	}()

	client, err := NewScopedClient(requesterSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: requesterIdentityID, IdentityHead: requesterHead, NodeID: requesterInfo.NodeID,
		TargetIdentityID: ownerIdentityID, TargetNodeID: ownerInfo.NodeID,
		OwnerSubjectID: ownerSubjectID, ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", GrantChain: []authority.SignedGrant{grant},
		Resolve: resolve, Signer: requesterNode,
	})
	if err != nil {
		t.Fatalf("create cross-identity scoped client: %v", err)
	}
	got, err := client.GetSnapshot(ctx, snapshot.SnapshotID)
	if err != nil || archivesnapshot.Verify(got) != nil || got.SnapshotID != snapshot.SnapshotID || got.Content.ArchiveID != snapshot.Content.ArchiveID {
		t.Fatalf("cross-identity capability read returned snapshot=%q archive=%q err=%v", got.SnapshotID, got.Content.ArchiveID, err)
	}
	grantHash, err := authority.GrantHash(grant.Grant)
	if err != nil {
		t.Fatal(err)
	}
	authorizer := service.Authorizer.(*peerAPIScopeProbe)
	authorizer.revokedGrantHash.Store(&grantHash)
	revokedCtx, cancelRevoked := context.WithTimeout(ctx, time.Second)
	defer cancelRevoked()
	if _, err := client.GetSnapshot(revokedCtx, snapshot.SnapshotID); err == nil {
		t.Fatal("cross-identity capability remained usable after the OwnerSubject grant was revoked")
	}
}

func TestAuthenticatedPionServesPersistedReplicaSnapshotAndCiphertext(t *testing.T) {
	identityPublic, identityPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	providerIdentityPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	providerIdentityID, err := identityprotocol.IdentityID(providerIdentityPublic)
	if err != nil {
		t.Fatal(err)
	}
	if providerIdentityID == identityID {
		t.Fatal("provider and archive owner unexpectedly share a Global Identity")
	}
	head := "sha256:" + strings.Repeat("b", 64)
	providerHead := "sha256:" + strings.Repeat("d", 64)
	sourceNode, sourceInfo := newPeerAPINode(t)
	targetNode, targetInfo := newPeerAPINode(t)
	verifiedAt := time.Now().UTC().Add(-time.Second)
	states := map[string]authority.IdentityState{
		identityID: {
			IdentityID: identityID, HeadHash: head, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
			CurrentKeys: map[string]ed25519.PublicKey{
				"identity:" + identityID:    identityPublic,
				"node:" + sourceInfo.NodeID: nodePublicFromInfo(t, sourceInfo),
			},
		},
		providerIdentityID: {
			IdentityID: providerIdentityID, HeadHash: providerHead, VerifiedAt: verifiedAt, FreshUntil: verifiedAt.Add(5 * time.Minute),
			CurrentKeys: map[string]ed25519.PublicKey{
				"identity:" + providerIdentityID: providerIdentityPublic,
				"node:" + targetInfo.NodeID:      nodePublicFromInfo(t, targetInfo),
			},
		},
	}
	resolve := func(requestedIdentity, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		state, ok := states[requestedIdentity]
		if !ok || requestedHead != "" && requestedHead != state.HeadHash {
			return authority.IdentityState{}, peerprotocol.ErrStale
		}
		return state, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sourceSession, targetSession := authenticatedPionPair(t, ctx, sourceNode, targetNode, identityID, head, providerIdentityID, providerHead, resolve)
	defer sourceSession.Close()
	defer targetSession.Close()

	snapshot := peerAPITestSnapshot(t)
	now := time.Now().UTC().Truncate(time.Second)
	ownerSubjectID := "os_bbbbbbbbbbbbbbbbbbbbbb"
	keyID, err := identityprotocol.KeyID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: sourceInfo.NodeID},
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Actions: []string{"archive.replicate", "archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x6b}, 32)),
		PolicyEpoch: "1", IssuerKeyID: keyID,
	}, identityPrivate)
	if err != nil {
		t.Fatal(err)
	}
	verification := authority.VerificationContext{
		Subject: authority.OwnerSubject{
			ID: ownerSubjectID, Type: authority.SubjectPerson,
			Controller:       authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
			ControllerPolicy: authority.ControllerRecoveryRoot, PolicyEpoch: "1", CreatedAt: now,
		},
		SubjectPolicyEpoch: "1", Identity: states[identityID], Now: now,
	}
	authorizer := &peerAPIScopeProbe{verification: verification}
	providerStore, err := replication.Open(filepath.Join(t.TempDir(), "provider-replica"))
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	canonicalBytes := []byte("canonical media bytes")
	coordinate := snapshot.Content.Objects[0].Coordinate
	bundle, err := providerStore.EncryptSnapshot(ctx, snapshot, peerAPICanonicalSource{coordinate: coordinate, data: canonicalBytes}, dek, 1, replication.Authorization{
		SubjectID: ownerSubjectID, Actor: authority.Principal{Kind: authority.PrincipalNode, ID: sourceInfo.NodeID},
		Chain: []authority.SignedGrant{grant}, Verifier: authorizer,
	})
	if err != nil || len(bundle.Objects) != 1 {
		t.Fatalf("prepare provider ciphertext fixture: objects=%d err=%v", len(bundle.Objects), err)
	}
	if err := providerStore.PersistSnapshot(snapshot); err != nil {
		t.Fatalf("persist replica descriptor: %v", err)
	}
	service := Service{
		ReplicaSnapshots: providerStore, Ciphertexts: providerStore, Authorizer: authorizer,
		IdentityID: providerIdentityID, NodeID: targetInfo.NodeID, Resolve: resolve,
		Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: targetNode},
		Now:    func() time.Time { return time.Now().UTC() },
	}
	serveCtx, stopServe := context.WithCancel(ctx)
	serveResult := make(chan error, 1)
	go func() { serveResult <- service.Serve(serveCtx, targetSession) }()
	client, err := NewScopedClient(sourceSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: head, NodeID: sourceInfo.NodeID,
		TargetIdentityID: providerIdentityID, TargetNodeID: targetInfo.NodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.view",
		GrantChain: []authority.SignedGrant{grant}, Resolve: resolve, Signer: sourceNode,
	})
	if err != nil {
		stopServe()
		t.Fatal(err)
	}
	returned, err := client.GetSnapshot(ctx, snapshot.SnapshotID)
	if err != nil || returned.SnapshotID != snapshot.SnapshotID || returned.ContentHash != snapshot.ContentHash {
		stopServe()
		t.Fatalf("read persisted replica descriptor over Pion: snapshot=%q err=%v", returned.SnapshotID, err)
	}
	manifest, err := client.ReplicaManifest(ctx, returned, 1)
	if err != nil {
		stopServe()
		t.Fatalf("read persisted replica manifest over Pion: %v", err)
	}
	transferBundle, err := replication.BundleFromPublicReplicaManifest(returned, manifest)
	if err != nil || len(transferBundle.Objects) != 1 {
		stopServe()
		t.Fatalf("bind persisted replica manifest: objects=%d err=%v", len(transferBundle.Objects), err)
	}
	remoteObject := transferBundle.Objects[0]
	issuedAt := time.Now().UTC().Truncate(time.Second)
	readbackOffer, err := replication.BuildLeaseOffer(returned, manifest, replication.LeaseOfferSpec{
		LeaseID: "rl_" + strings.Repeat("C", 22), ProviderNodeID: targetInfo.NodeID,
		OwnerSubjectID: ownerSubjectID, KeyEpoch: 1, IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(time.Hour),
	})
	if err != nil {
		stopServe()
		t.Fatalf("bind source readback to replica manifest: %v", err)
	}
	signedReadbackOffer, err := sourceNode.SignReplicaOffer(readbackOffer, []authority.SignedGrant{grant})
	if err != nil {
		stopServe()
		t.Fatalf("sign source readback lease: %v", err)
	}
	readbackClient, err := NewScopedClient(sourceSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: head, NodeID: sourceInfo.NodeID,
		TargetIdentityID: providerIdentityID, TargetNodeID: targetInfo.NodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.replicate",
		GrantChain: []authority.SignedGrant{grant}, Resolve: resolve, Signer: sourceNode,
	})
	if err != nil {
		stopServe()
		t.Fatalf("create replication-scoped source readback client: %v", err)
	}
	readbackLease := lease.Lease{Status: lease.StatusVerifying, Offer: signedReadbackOffer}
	stream, err := (replication.PeerLeaseObjectReader{Lease: readbackLease, Source: readbackClient}).OpenCiphertext(
		ctx, targetInfo.NodeID, readbackOffer.LeaseID, remoteObject.ObjectID,
	)
	if err != nil {
		stopServe()
		t.Fatalf("open source-side readback over authenticated Pion session: %v", err)
	}
	var ciphertext bytes.Buffer
	written, copyErr := io.Copy(&ciphertext, stream)
	closeErr := stream.Close()
	if copyErr != nil || closeErr != nil || written != remoteObject.CiphertextSize {
		stopServe()
		t.Fatalf("persisted provider readback stream: wrote=%d/%d copy=%v close=%v", written, remoteObject.CiphertextSize, copyErr, closeErr)
	}
	var restored bytes.Buffer
	if err := cryptoarchive.DecryptObject(ctx, dek, cryptoarchive.ObjectDescriptor{
		ArchiveID: bundle.ArchiveID, Coordinate: remoteObject.Coordinate, ObjectType: string(remoteObject.Coordinate.Kind),
		PlaintextSize: remoteObject.PlaintextSize, PlaintextSHA256: remoteObject.PlaintextSHA256,
		CiphertextSize: remoteObject.CiphertextSize, CiphertextSHA256: remoteObject.CiphertextSHA256, KeyEpoch: 1,
	}, bytes.NewReader(ciphertext.Bytes()), &restored); err != nil || !bytes.Equal(restored.Bytes(), canonicalBytes) {
		stopServe()
		t.Fatalf("restore provider ciphertext readback: bytes=%d err=%v", restored.Len(), err)
	}
	stopServe()
	if err := <-serveResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("provider readback service cancellation=%v", err)
	}
}

type peerAPISwitchableCiphertextStore struct {
	mu    sync.RWMutex
	store CiphertextStore
}

func (s *peerAPISwitchableCiphertextStore) Set(store CiphertextStore) {
	s.mu.Lock()
	s.store = store
	s.mu.Unlock()
}

func (s *peerAPISwitchableCiphertextStore) current() CiphertextStore {
	s.mu.RLock()
	store := s.store
	s.mu.RUnlock()
	return store
}

func (s *peerAPISwitchableCiphertextStore) LoadBundle(snapshot archivesnapshot.Snapshot, keyEpoch uint64) (replication.Bundle, error) {
	store := s.current()
	if store == nil {
		return replication.Bundle{}, replication.ErrObjectUnavailable
	}
	return store.LoadBundle(snapshot, keyEpoch)
}

func (s *peerAPISwitchableCiphertextStore) KeyEpochs(snapshot archivesnapshot.Snapshot) ([]uint64, error) {
	store, ok := s.current().(KeyEpochStore)
	if !ok {
		return nil, replication.ErrObjectUnavailable
	}
	return store.KeyEpochs(snapshot)
}

func (s *peerAPISwitchableCiphertextStore) LoadKeyEnvelope(snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientKeyID string) (cryptoarchive.KeyEnvelope, error) {
	store, ok := s.current().(KeyEnvelopeStore)
	if !ok {
		return cryptoarchive.KeyEnvelope{}, replication.ErrObjectUnavailable
	}
	return store.LoadKeyEnvelope(snapshot, keyEpoch, recipientKeyID)
}

func (s *peerAPISwitchableCiphertextStore) OpenCiphertextRangeForPeer(ctx context.Context, snapshot archivesnapshot.Snapshot, bundle replication.Bundle, authorization replication.Authorization, action, objectID string, offset, length int64) (io.ReadCloser, error) {
	store := s.current()
	if store == nil {
		return nil, replication.ErrObjectUnavailable
	}
	return store.OpenCiphertextRangeForPeer(ctx, snapshot, bundle, authorization, action, objectID, offset, length)
}

func (s *peerAPISwitchableCiphertextStore) EraseTombstonedArchive(ctx context.Context, archiveID, tombstoneHash string, state replication.TombstoneLeaseState) (replication.ReplicaErasure, error) {
	store, ok := s.current().(interface {
		EraseTombstonedArchive(context.Context, string, string, replication.TombstoneLeaseState) (replication.ReplicaErasure, error)
	})
	if !ok {
		return replication.ReplicaErasure{}, replication.ErrObjectUnavailable
	}
	return store.EraseTombstonedArchive(ctx, archiveID, tombstoneHash, state)
}

type peerAPITestKeys map[string]ed25519.PublicKey

func (keys peerAPITestKeys) VerifiedKey(_ context.Context, principal authority.Principal, _ time.Time) (ed25519.PublicKey, error) {
	key, ok := keys[principal.Kind+":"+principal.ID]
	if !ok {
		return nil, lease.ErrDenied
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

type peerAPIScopeProbe struct {
	calls            atomic.Int32
	revokedGrantHash atomic.Pointer[string]
	verification     authority.VerificationContext
}

func (a *peerAPIScopeProbe) Authorize(_ context.Context, subjectID, archiveID, snapshotID, action string, audience authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	a.calls.Add(1)
	if subjectID != a.verification.Subject.ID || now.IsZero() {
		return authority.ErrDenied
	}
	verification := a.verification
	verification.Now = now
	if revoked := a.revokedGrantHash.Load(); revoked != nil {
		verification.RevokedGrantHashes = map[string]struct{}{*revoked: {}}
	}
	return authority.VerifyGrantChain(verification, chain, audience, archiveID, snapshotID, action)
}

type peerAPILeaseBytesReader struct {
	providerNodeID string
	leaseID        string
	objects        map[string][]byte
}

func (r peerAPILeaseBytesReader) OpenCiphertext(_ context.Context, providerNodeID, leaseID, objectID string) (io.ReadCloser, error) {
	if providerNodeID != r.providerNodeID || leaseID != r.leaseID {
		return nil, lease.ErrDenied
	}
	data, ok := r.objects[objectID]
	if !ok {
		return nil, lease.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type peerAPICanonicalSource struct {
	coordinate archiveindex.Coordinate
	data       []byte
}

func (s peerAPICanonicalSource) OpenCanonical(_ context.Context, coordinate archiveindex.Coordinate) (io.ReadCloser, error) {
	if coordinate != s.coordinate {
		return nil, errors.New("canonical coordinate does not match fixture")
	}
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func newPeerAPINode(t *testing.T) (*nodeidentity.Store, nodeidentity.Info) {
	t.Helper()
	store, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	return store, info
}

func nodePublicFromInfo(t *testing.T, info nodeidentity.Info) ed25519.PublicKey {
	t.Helper()
	public, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
	if err != nil || len(public) != ed25519.PublicKeySize {
		t.Fatalf("invalid test Node Key public bytes=%d err=%v", len(public), err)
	}
	return ed25519.PublicKey(public)
}

func authenticatedPionPair(
	t *testing.T,
	ctx context.Context,
	sourceNode, targetNode *nodeidentity.Store,
	sourceIdentityID, sourceHead, targetIdentityID, targetHead string,
	resolve peerprotocol.IdentityResolver,
) (*peertransport.AuthenticatedDataChannel, *peertransport.AuthenticatedDataChannel) {
	t.Helper()
	sourcePeer, err := peertransport.NewPeerConnection(peertransport.ICEConfig{})
	if err != nil {
		t.Fatal(err)
	}
	targetPeer, err := peertransport.NewPeerConnection(peertransport.ICEConfig{})
	if err != nil {
		_ = sourcePeer.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourcePeer.Close(); _ = targetPeer.Close() })
	control, data, err := peertransport.CreateChannels(sourcePeer)
	if err != nil {
		t.Fatal(err)
	}
	controlChannels := make(chan *webrtc.DataChannel, 1)
	dataChannels := make(chan *webrtc.DataChannel, 1)
	targetPeer.OnDataChannel(func(channel *webrtc.DataChannel) {
		switch channel.Label() {
		case peertransport.ControlChannelLabel:
			controlChannels <- channel
		case peertransport.DataChannelLabel:
			dataChannels <- channel
		}
	})
	offer, err := peertransport.CompleteOffer(ctx, sourcePeer)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	answer, err := peertransport.AcceptOffer(ctx, targetPeer, offer)
	if err != nil {
		t.Fatalf("accept offer: %v", err)
	}
	if err := peertransport.ApplyAnswer(sourcePeer, answer); err != nil {
		t.Fatal(err)
	}
	if err := peertransport.WaitConnection(ctx, sourcePeer); err != nil {
		t.Fatalf("source ICE: %v", err)
	}
	if err := peertransport.WaitConnection(ctx, targetPeer); err != nil {
		t.Fatalf("target ICE: %v", err)
	}
	var targetControl, targetData *webrtc.DataChannel
	select {
	case targetControl = <-controlChannels:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case targetData = <-dataChannels:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, channel := range []*webrtc.DataChannel{control, data, targetControl, targetData} {
		if err := peertransport.WaitOpen(ctx, channel); err != nil {
			t.Fatalf("open %s: %v", channel.Label(), err)
		}
	}
	type result struct {
		session *peertransport.AuthenticatedDataChannel
		err     error
	}
	// Match the production signaling responder: install its bounded control
	// inbox as soon as the channel is available, before the initiator can send
	// its first authentication frame.
	targetInbox := make(chan []byte, 4)
	if err := peertransport.SetBoundedMessageHandler(targetControl, peertransport.MaxControlFrame, func(message []byte) error {
		select {
		case targetInbox <- message:
			return nil
		default:
			return peertransport.ErrMessageRejected
		}
	}); err != nil {
		t.Fatalf("install responder control inbox: %v", err)
	}
	results := make(chan result, 2)
	go func() {
		session, err := peertransport.AuthenticateDataChannel(ctx, peertransport.RoleInitiator, control, data,
			sourceNode, sourceIdentityID, sourceHead, resolve, nodeidentity.PeerReplayConsumer{NodeStore: sourceNode})
		results <- result{session: session, err: err}
	}()
	go func() {
		session, err := peertransport.AuthenticateDataChannelWithInbox(ctx, peertransport.RoleResponder, targetControl, targetData, targetInbox,
			targetNode, targetIdentityID, targetHead, resolve, nodeidentity.PeerReplayConsumer{NodeStore: targetNode})
		results <- result{session: session, err: err}
	}()
	var sourceSession, targetSession *peertransport.AuthenticatedDataChannel
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("Node-Key authentication: %v", result.err)
		}
		if result.session.Remote().ID == targetNodeInfo(t, targetNode).NodeID {
			sourceSession = result.session
		} else {
			targetSession = result.session
		}
	}
	if sourceSession == nil || targetSession == nil || sourceSession.SessionID() != targetSession.SessionID() {
		t.Fatal("mutually authenticated peer sessions did not share the session ID")
	}
	return sourceSession, targetSession
}

func targetNodeInfo(t *testing.T, store *nodeidentity.Store) nodeidentity.Info {
	t.Helper()
	info, err := store.Info()
	if err != nil {
		t.Fatal(err)
	}
	return info
}
