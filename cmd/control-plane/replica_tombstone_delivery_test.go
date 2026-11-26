package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/replication"
)

type tombstoneRetryKeys map[string]ed25519.PublicKey

func requireTombstoneRetry(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (keys tombstoneRetryKeys) VerifiedKey(_ context.Context, principal authority.Principal, _ time.Time) (ed25519.PublicKey, error) {
	key, ok := keys[principal.Kind+":"+principal.ID]
	if !ok {
		return nil, lease.ErrDenied
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

type tombstoneRetryPeer struct {
	identityID string
	nodeID     string
	provider   *lease.Store
	report     lease.SignedDeletionReport
	seen       []lease.SignedTombstone
}

func (p *tombstoneRetryPeer) List() []peerapi.SessionInfo {
	return []peerapi.SessionInfo{{IdentityID: p.identityID, NodeID: p.nodeID}}
}

func (p *tombstoneRetryPeer) Connect(context.Context, string, string) error {
	return errors.New("unexpected peer connect for an already connected session")
}

func (p *tombstoneRetryPeer) SubmitTombstoneWithReceipt(ctx context.Context, nodeID string, _ peerapi.RequestScope, signed lease.SignedTombstone) (peerapi.TombstoneReceipt, error) {
	if nodeID != p.nodeID {
		return peerapi.TombstoneReceipt{}, errors.New("wrong tombstone recipient")
	}
	p.seen = append(p.seen, signed)
	accepted, err := p.provider.AddTombstone(ctx, signed)
	if err != nil {
		return peerapi.TombstoneReceipt{}, err
	}
	if accepted.Hash == "" || accepted.Signed.Tombstone != signed.Tombstone {
		return peerapi.TombstoneReceipt{}, errors.New("provider did not persist exact tombstone")
	}
	return peerapi.TombstoneReceipt{
		Version: "ir.archive-tombstone-receipt/1", ArchiveID: signed.Tombstone.ArchiveID,
		OwnerSubjectID: signed.Tombstone.OwnerSubjectID, DeletionEpoch: signed.Tombstone.DeletionEpoch,
		TombstoneHash:   accepted.Hash,
		Erasure:         &replication.ReplicaErasure{ArchiveID: accepted.Signed.Tombstone.ArchiveID, TombstoneHash: accepted.Hash},
		DeletionReports: []lease.SignedDeletionReport{p.report},
	}, nil
}

type failFirstTombstoneAckStore struct {
	*lease.Store
	failed bool
}

func (s *failFirstTombstoneAckStore) MarkTombstoneDelivered(ctx context.Context, archiveID, nodeID, hash string, at time.Time) error {
	if !s.failed {
		s.failed = true
		items, tombstones, err := s.Store.Snapshot(ctx)
		if err != nil || len(tombstones) != 1 || len(tombstones[0].Deliveries) != 0 || len(items) != 1 || items[0].DeletionReport == nil || items[0].Status != lease.StatusDeleted {
			return errors.New("injected ack failure reached before report persistence")
		}
		return errors.New("injected durable acknowledgement write failure")
	}
	return s.Store.MarkTombstoneDelivered(ctx, archiveID, nodeID, hash, at)
}

func TestReplicaTombstoneProviderTargetsResolveCrossIdentityLease(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	localIdentityID := "gi_localidentity000000000"
	providerIdentityID := "gi_provideridentity000000"
	localState, localNode, _ := replicaTransferIdentityFixture(t, localIdentityID, now)
	providerState, _, providerNodeID := replicaTransferIdentityFixture(t, providerIdentityID, now)
	archiveID := "arc_crossidentity00000000"
	item := replicaTransferLeaseFixtureItem("rl_"+"aabbccddeeff0011223344", lease.StatusActive,
		providerNodeID, localNode.NodeID, "os_crossidentity00000000", archiveID,
		"snap_"+"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", now.Add(time.Hour))
	// Registry fork evidence may be retained after a valid explicit resolution;
	// the current refreshed projection, rather than the historical evidence
	// count, determines whether the identity is frozen.
	registered := []globalidentity.RegisteredIdentity{{IdentityID: providerIdentityID, ForkCount: 1, NodeIDs: []string{providerNodeID}}}
	resolved := 0
	resolve := func(identityID, _ string, _ time.Time) (authority.IdentityState, error) {
		resolved++
		if identityID != providerIdentityID {
			return authority.IdentityState{}, errors.New("unexpected identity lookup")
		}
		return providerState, nil
	}
	byArchive, unresolved := replicaTombstoneProviderTargets(
		[]lease.Lease{item}, localIdentityID, localNode.NodeID, localState, registered, resolve, now,
	)
	if got := byArchive[archiveID][providerNodeID]; got != providerIdentityID {
		t.Fatalf("provider identity = %q, want %q; targets=%v", got, providerIdentityID, byArchive)
	}
	if unresolved[archiveID] {
		t.Fatal("verified cross-identity provider was left unresolved")
	}
	if resolved != 1 {
		t.Fatalf("remote identity refresh count=%d, want 1", resolved)
	}
}

func TestTombstoneDeliveryBatchRotatesAfterFailedBoundedPass(t *testing.T) {
	work := make([]replicaTombstoneDeliveryTarget, 20)
	for i := range work {
		work[i].key = fmt.Sprintf("tombstone-%02d/provider", i)
	}
	cursor := ""
	first := tombstoneDeliveryBatch(append([]replicaTombstoneDeliveryTarget(nil), work...), cursor, maxTombstoneDeliveryAttemptsPerPass)
	if len(first) != maxTombstoneDeliveryAttemptsPerPass {
		t.Fatalf("first batch size=%d, want %d", len(first), maxTombstoneDeliveryAttemptsPerPass)
	}
	// Reconcile persists the planned batch endpoint before the first network
	// call. Model the entire bounded batch remaining unacknowledged after a
	// failed pass: future work must still move past these providers.
	cursor = first[len(first)-1].key
	second := tombstoneDeliveryBatch(append([]replicaTombstoneDeliveryTarget(nil), work...), cursor, maxTombstoneDeliveryAttemptsPerPass)
	if len(second) != maxTombstoneDeliveryAttemptsPerPass {
		t.Fatalf("second batch size=%d, want %d", len(second), maxTombstoneDeliveryAttemptsPerPass)
	}
	for i := range second {
		var want replicaTombstoneDeliveryTarget
		if i < len(work)-maxTombstoneDeliveryAttemptsPerPass {
			want = work[maxTombstoneDeliveryAttemptsPerPass+i]
		} else {
			want = work[i-(len(work)-maxTombstoneDeliveryAttemptsPerPass)]
		}
		if second[i].key != want.key {
			t.Fatalf("second batch[%d]=%q, want %q", i, second[i].key, want.key)
		}
	}
}

type tombstoneCursorTestKeys struct{}

func (tombstoneCursorTestKeys) VerifiedKey(context.Context, authority.Principal, time.Time) (ed25519.PublicKey, error) {
	return nil, nil
}

func TestTombstoneDeliveryBatchResumesAfterStoreReopen(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "lease-state")
	store, err := lease.Open(lease.Options{Root: root, Keys: tombstoneCursorTestKeys{}})
	if err != nil {
		t.Fatal(err)
	}
	hash := "sha256:" + strings.Repeat("a", 64)
	work := make([]replicaTombstoneDeliveryTarget, 20)
	for i := range work {
		nodeID := fmt.Sprintf("nd_%022d", i+1)
		work[i] = replicaTombstoneDeliveryTarget{
			key:          hash + "\x00" + nodeID,
			tombstone:    lease.TombstoneRecord{Hash: hash},
			targetNodeID: nodeID,
		}
	}

	first := tombstoneDeliveryBatch(append([]replicaTombstoneDeliveryTarget(nil), work...), "", maxTombstoneDeliveryAttemptsPerPass)
	if len(first) == 0 {
		t.Fatal("first batch is empty")
	}
	// The reconciler persists the batch endpoint once before the first network
	// attempt. Simulate a crash after that durable write.
	if err := store.SetTombstoneDeliveryCursor(ctx, lease.TombstoneDeliveryCursor{
		TombstoneHash: hash, NodeID: first[len(first)-1].targetNodeID,
	}); err != nil {
		t.Fatalf("persist planned batch cursor: %v", err)
	}

	// A new Store simulates a Control Plane restart. The next pass must select
	// the remaining targets before wrapping back to the unavailable prefix.
	reopened, err := lease.Open(lease.Options{Root: root, Keys: tombstoneCursorTestKeys{}})
	if err != nil {
		t.Fatalf("reopen lease state: %v", err)
	}
	cursor, err := reopened.GetTombstoneDeliveryCursor(ctx)
	if err != nil {
		t.Fatalf("read persisted cursor: %v", err)
	}
	second := tombstoneDeliveryBatch(append([]replicaTombstoneDeliveryTarget(nil), work...), tombstoneDeliveryCursorKey(cursor), maxTombstoneDeliveryAttemptsPerPass)
	if len(second) != maxTombstoneDeliveryAttemptsPerPass {
		t.Fatalf("second batch size=%d, want %d", len(second), maxTombstoneDeliveryAttemptsPerPass)
	}
	for i := maxTombstoneDeliveryAttemptsPerPass; i < len(work); i++ {
		want := work[i].targetNodeID
		found := false
		for _, got := range second {
			if got.targetNodeID == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("later provider %q was not selected after reopen: %+v", want, second)
		}
	}
}

func TestReplicaTombstoneProviderTargetsFailClosedForStaleOrAmbiguousIdentity(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	localIdentityID := "gi_localidentity000000000"
	providerIdentityID := "gi_provideridentity000000"
	otherIdentityID := "gi_otheridentity00000000"
	localState, localNode, _ := replicaTransferIdentityFixture(t, localIdentityID, now)
	providerState, _, providerNodeID := replicaTransferIdentityFixture(t, providerIdentityID, now)
	otherState, _, otherNodeID := replicaTransferIdentityFixture(t, otherIdentityID, now)
	archiveID := "arc_crossidentity00000000"
	item := replicaTransferLeaseFixtureItem("rl_"+"112233445566778899aabb", lease.StatusActive,
		providerNodeID, localNode.NodeID, "os_crossidentity00000000", archiveID,
		"snap_"+"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", now.Add(time.Hour))

	t.Run("stale target stays pending", func(t *testing.T) {
		stale := providerState
		stale.FreshUntil = now
		byArchive, unresolved := replicaTombstoneProviderTargets(
			[]lease.Lease{item}, localIdentityID, localNode.NodeID, localState,
			[]globalidentity.RegisteredIdentity{{IdentityID: providerIdentityID, NodeIDs: []string{providerNodeID}}},
			func(string, string, time.Time) (authority.IdentityState, error) { return stale, nil }, now,
		)
		if len(byArchive[archiveID]) != 0 || !unresolved[archiveID] {
			t.Fatalf("stale identity was used: targets=%v unresolved=%v", byArchive, unresolved)
		}
	})

	t.Run("ambiguous node stays pending", func(t *testing.T) {
		// A second verified identity projection claims the same Node ID. The
		// derived key will conflict with its advertised ID and is discarded; a
		// cryptographically valid duplicate is covered by the map's ambiguity
		// handling in production when the same Node public key is delegated twice.
		stateByID := map[string]authority.IdentityState{
			providerIdentityID: providerState,
			otherIdentityID:    otherState,
		}
		registrations := []globalidentity.RegisteredIdentity{
			{IdentityID: providerIdentityID, NodeIDs: []string{providerNodeID}},
			{IdentityID: otherIdentityID, NodeIDs: []string{providerNodeID, otherNodeID}},
		}
		// Duplicate the same currently delegated public key under the second
		// verified identity to model an ambiguous Node principal.
		for key, publicKey := range providerState.CurrentKeys {
			if strings.HasPrefix(key, authority.PrincipalNode+":") && strings.HasSuffix(key, providerNodeID) {
				otherState.CurrentKeys[key] = publicKey
			}
		}
		stateByID[otherIdentityID] = otherState
		byArchive, unresolved := replicaTombstoneProviderTargets(
			[]lease.Lease{item}, localIdentityID, localNode.NodeID, localState, registrations,
			func(identityID, _ string, _ time.Time) (authority.IdentityState, error) {
				state, ok := stateByID[identityID]
				if !ok {
					return authority.IdentityState{}, errors.New("unknown identity")
				}
				return state, nil
			}, now,
		)
		if len(byArchive[archiveID]) != 0 || !unresolved[archiveID] {
			t.Fatalf("ambiguous Node identity was used: targets=%v unresolved=%v", byArchive, unresolved)
		}
	})
}

func TestReplicaTombstoneDeliveryRetriesAfterAckPersistenceFailure(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	identity, rootKey, localNode, providerNode := tombstoneRetryIdentityFixture(t, root, now)
	identityID := localNode.IdentityID
	localState, err := authority.IdentityStateFromStore(identity, now)
	requireTombstoneRetry(t, err)
	resolve := authority.IdentityStateResolverFromStores(map[string]*globalidentity.Store{identityID: identity})
	registry, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "registry"))
	requireTombstoneRetry(t, err)
	ownerStore, err := authority.Open(filepath.Join(root, "authority"))
	requireTombstoneRetry(t, err)
	const archiveID = "arc_aaaaaaaaaaaaaaaaaaaaaa"
	subject, err := authority.NewPersonOwnerSubject(identityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x51}, 16)))
	requireTombstoneRetry(t, err)
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, localState, now)
	requireTombstoneRetry(t, err)
	createSigned, err := authority.SignAuthorityEvent(create, rootKey)
	requireTombstoneRetry(t, err)
	if err := ownerStore.Apply(createSigned, resolve, now); err != nil {
		t.Fatalf("create OwnerSubject: %v", err)
	}
	ownerSnapshot, err := ownerStore.Snapshot(subject.ID, resolve, now)
	requireTombstoneRetry(t, err)
	rootKeyID, err := identityprotocol.KeyID(rootKey.Public().(ed25519.PublicKey))
	requireTombstoneRetry(t, err)
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID},
		ArchiveID: archiveID, Actions: []string{"archive.delete", "archive.replicate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: strings.Repeat("z", 43), PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: rootKeyID,
	}, rootKey)
	requireTombstoneRetry(t, err)
	issue, err := authority.NewAuthorityEvent(subject.ID, "2", ownerSnapshot.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{grant}}, localState, now)
	requireTombstoneRetry(t, err)
	issueSigned, err := authority.SignAuthorityEvent(issue, rootKey)
	requireTombstoneRetry(t, err)
	if err := ownerStore.Apply(issueSigned, resolve, now); err != nil {
		t.Fatalf("issue deletion/replication capability: %v", err)
	}

	keyResolver := tombstoneRetryKeys{
		authority.PrincipalIdentity + ":" + identityID:      rootKey.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + localNode.NodeID:    localNode.PublicKey,
		authority.PrincipalNode + ":" + providerNode.NodeID: providerNode.PublicKey,
	}
	leaseOptions := func(path string) lease.Options {
		return lease.Options{
			Root: path, Authorizer: lease.DurableAuthority{Store: ownerStore, Resolve: resolve},
			Keys: keyResolver, Now: func() time.Time { return now.Add(3 * time.Second) },
		}
	}
	sourcePath := filepath.Join(root, "source-leases")
	sourceStore, err := lease.Open(leaseOptions(sourcePath))
	requireTombstoneRetry(t, err)
	providerStore, err := lease.Open(leaseOptions(filepath.Join(root, "provider-leases")))
	requireTombstoneRetry(t, err)
	leaseID := "rl_aaaaaaaaaaaaaaaaaaaaaa"
	snapshotID := "snap_" + strings.Repeat("b", 64)
	manifestHash := "sha256:" + strings.Repeat("c", 64)
	offer := lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: leaseID, ArchiveID: archiveID, SnapshotID: snapshotID,
		KeyEpoch: "1", PlaintextManifestHash: manifestHash, ReplicaManifestHash: manifestHash,
		EncryptedObjectSet: []lease.EncryptedObject{{ObjectID: "ob_" + strings.Repeat("d", 22), CiphertextSize: "1", CiphertextSHA256: manifestHash}},
		ProviderNodeID:     providerNode.NodeID, OwnerSubjectID: subject.ID, ReservedBytes: "1",
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), RenewalPolicy: "owner_approval",
	}
	signedOffer, err := localNode.Store.SignReplicaOffer(offer, []authority.SignedGrant{grant})
	requireTombstoneRetry(t, err)
	if _, err := sourceStore.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatalf("persist source lease offer: %v", err)
	}
	offerHash, err := lease.OfferHash(signedOffer.Offer)
	requireTombstoneRetry(t, err)
	acceptance, err := lease.SignAcceptance(lease.Acceptance{
		Version: lease.ProtocolVersion, LeaseID: leaseID, OfferHash: offerHash, ProviderNodeID: providerNode.NodeID,
		ReservedBytes: "1", AcceptedAt: now.Add(time.Second).Format(time.RFC3339),
	}, providerNode.PrivateKey)
	requireTombstoneRetry(t, err)
	if _, err := sourceStore.Accept(ctx, acceptance); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceStore.BeginTransfer(ctx, leaseID); err != nil {
		t.Fatal(err)
	}
	tombstone, err := sourceStore.CreateTombstone(ctx, subject.ID, archiveID, "owner_requested", now.Add(time.Second), func(value lease.Tombstone) (lease.SignedTombstone, error) {
		return localNode.Store.SignReplicaTombstone(value, []authority.SignedGrant{grant})
	})
	requireTombstoneRetry(t, err)
	report, err := lease.SignDeletionReport(lease.DeletionReport{
		Version: lease.ProtocolVersion, LeaseID: leaseID, OfferHash: offerHash, ProviderNodeID: providerNode.NodeID,
		Status: string(lease.StatusDeleted), ReportedAt: now.Add(2 * time.Second).Format(time.RFC3339), TombstoneHash: tombstone.Hash,
	}, providerNode.PrivateKey)
	requireTombstoneRetry(t, err)
	peers := &tombstoneRetryPeer{identityID: identityID, nodeID: providerNode.NodeID, provider: providerStore, report: report}
	failingAcks := &failFirstTombstoneAckStore{Store: sourceStore}
	reconciler := &replicaTombstoneDeliveryReconciler{
		leases: failingAcks, identity: identity, registry: registry, authority: ownerStore,
		node: localNode.Store, peers: peers,
	}
	if err := reconciler.Reconcile(ctx, now.Add(3*time.Second)); err == nil {
		t.Fatal("first pass succeeded despite injected durable ACK write failure")
	}
	if !failingAcks.failed || len(peers.seen) != 1 || !reflect.DeepEqual(peers.seen[0], tombstone.Signed) {
		t.Fatalf("first pass did not persist report then fail ACK: failed=%v deliveries=%d", failingAcks.failed, len(peers.seen))
	}
	_, storedTombstones, err := sourceStore.Snapshot(ctx)
	if err != nil || len(storedTombstones) != 1 || len(storedTombstones[0].Deliveries) != 0 {
		t.Fatalf("ACK unexpectedly durable after injected failure: tombstones=%+v err=%v", storedTombstones, err)
	}
	items, _, err := sourceStore.Snapshot(ctx)
	if err != nil || len(items) != 1 || items[0].DeletionReport == nil || items[0].Status != lease.StatusDeleted {
		t.Fatalf("signed provider report was not durable before ACK failure: leases=%+v err=%v", items, err)
	}

	reopened, err := lease.Open(leaseOptions(sourcePath))
	requireTombstoneRetry(t, err)
	failingAcks.Store = reopened
	if err := reconciler.Reconcile(ctx, now.Add(4*time.Second)); err != nil {
		t.Fatalf("retry after source store reopen: %v", err)
	}
	if len(peers.seen) != 2 || !reflect.DeepEqual(peers.seen[0], peers.seen[1]) || !reflect.DeepEqual(peers.seen[1], tombstone.Signed) {
		t.Fatalf("retry did not resend the exact persisted tombstone: %+v", peers.seen)
	}
	_, providerTombstones, err := providerStore.Snapshot(ctx)
	if err != nil || len(providerTombstones) != 1 || providerTombstones[0].Hash != tombstone.Hash || providerTombstones[0].Frozen {
		t.Fatalf("provider replay was not idempotent: tombstones=%+v err=%v", providerTombstones, err)
	}
	_, finalTombstones, err := reopened.Snapshot(ctx)
	if err != nil || len(finalTombstones) != 1 || len(finalTombstones[0].Deliveries) != 1 || finalTombstones[0].Deliveries[0].TombstoneHash != tombstone.Hash {
		t.Fatalf("source ACK did not settle after retry: tombstones=%+v err=%v", finalTombstones, err)
	}
	// The signed report is an authenticated provider observation, not proof
	// that every physical copy was erased.
}

type tombstoneRetryNode struct {
	Store      *nodeidentity.Store
	NodeID     string
	IdentityID string
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
}

func tombstoneRetryIdentityFixture(t *testing.T, root string, now time.Time) (*globalidentity.Store, ed25519.PrivateKey, tombstoneRetryNode, tombstoneRetryNode) {
	t.Helper()
	public, rootKey, err := ed25519.GenerateKey(rand.Reader)
	requireTombstoneRetry(t, err)
	identityID, err := identityprotocol.IdentityID(public)
	requireTombstoneRetry(t, err)
	keyID, err := identityprotocol.KeyID(public)
	requireTombstoneRetry(t, err)
	recoveryKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	requireTombstoneRetry(t, err)
	challenge := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))
	credentialID := []byte("tombstone-test-credential")
	genesis, err := json.Marshal(map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID, "sequence": "0",
		"previous_event_hash": nil, "operation": "genesis", "issued_at": now.Format(time.RFC3339),
		"signer_key_id": keyID, "signer_public_key": base64.RawURLEncoding.EncodeToString(public),
		"payload": map[string]any{
			"identity_format": "global-identity/v1", "recovery_encryption_public_key": base64.RawURLEncoding.EncodeToString(recoveryKey.PublicKey().Bytes()),
			"webauthn_challenge": challenge, "credential_id": base64.RawURLEncoding.EncodeToString(credentialID),
		},
	})
	requireTombstoneRetry(t, err)
	canonicalGenesis, signature, err := identityprotocol.SignEvent(genesis, rootKey)
	requireTombstoneRetry(t, err)
	identity, err := globalidentity.Open(filepath.Join(root, "identity"))
	requireTombstoneRetry(t, err)
	if err := identity.CreateGenesis(canonicalGenesis, signature, bytes.Repeat([]byte{0x42}, 32), webauthn.Credential{ID: credentialID, PublicKey: []byte("credential-public-key")}, challenge); err != nil {
		t.Fatalf("create Global Identity fixture: %v", err)
	}
	local := enrollTombstoneRetryNode(t, root, identity, identityID, rootKey, "local-node", now)
	provider := enrollTombstoneRetryProvider(t, identity, identityID, rootKey, now)
	return identity, rootKey, local, provider
}

func enrollTombstoneRetryNode(t *testing.T, root string, identity *globalidentity.Store, identityID string, rootKey ed25519.PrivateKey, name string, now time.Time) tombstoneRetryNode {
	t.Helper()
	store, err := nodeidentity.Open(filepath.Join(root, name))
	requireTombstoneRetry(t, err)
	info, err := store.Create()
	requireTombstoneRetry(t, err)
	snapshot, _, err := identity.Snapshot()
	requireTombstoneRetry(t, err)
	proof, err := store.BeginEnrollmentProof(identityID, snapshot.HeadHash, now)
	requireTombstoneRetry(t, err)
	sequence, err := strconv.Atoi(snapshot.Sequence)
	requireTombstoneRetry(t, err)
	rootPublic := rootKey.Public().(ed25519.PublicKey)
	rootKeyID, err := identityprotocol.KeyID(rootPublic)
	requireTombstoneRetry(t, err)
	event, err := json.Marshal(map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID,
		"sequence": strconv.Itoa(sequence + 1), "previous_event_hash": snapshot.HeadHash,
		"operation": "node_delegate", "issued_at": now.Format(time.RFC3339), "signer_key_id": rootKeyID,
		"signer_public_key": base64.RawURLEncoding.EncodeToString(rootPublic),
		"payload":           map[string]any{"node_id": proof.NodeID, "node_public_key": proof.PublicKey, "enrollment_nonce": proof.Nonce, "node_proof": proof.Signature},
	})
	requireTombstoneRetry(t, err)
	canonical, signature, err := identityprotocol.SignEvent(event, rootKey)
	requireTombstoneRetry(t, err)
	if _, err := store.ValidateEnrollmentEvent(canonical, signature, now); err != nil {
		t.Fatalf("validate %s enrollment: %v", name, err)
	}
	if err := identity.ApplyNodeEvent(canonical, signature); err != nil {
		t.Fatalf("apply %s delegation: %v", name, err)
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
	requireTombstoneRetry(t, err)
	return tombstoneRetryNode{Store: store, NodeID: info.NodeID, IdentityID: identityID, PublicKey: ed25519.PublicKey(publicKey)}
}

func enrollTombstoneRetryProvider(t *testing.T, identity *globalidentity.Store, identityID string, rootKey ed25519.PrivateKey, now time.Time) tombstoneRetryNode {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	requireTombstoneRetry(t, err)
	nodeID, err := identityprotocol.NodeID(publicKey)
	requireTombstoneRetry(t, err)
	snapshot, _, err := identity.Snapshot()
	requireTombstoneRetry(t, err)
	sequence, err := strconv.Atoi(snapshot.Sequence)
	requireTombstoneRetry(t, err)
	nonce := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x63}, 32))
	proofMessage, err := identityprotocol.NodeEnrollmentProofMessage(identityID, snapshot.HeadHash, nonce)
	requireTombstoneRetry(t, err)
	proof := ed25519.Sign(privateKey, proofMessage)
	rootPublic := rootKey.Public().(ed25519.PublicKey)
	rootKeyID, err := identityprotocol.KeyID(rootPublic)
	requireTombstoneRetry(t, err)
	event, err := json.Marshal(map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID,
		"sequence": strconv.Itoa(sequence + 1), "previous_event_hash": snapshot.HeadHash,
		"operation": "node_delegate", "issued_at": now.Format(time.RFC3339), "signer_key_id": rootKeyID,
		"signer_public_key": base64.RawURLEncoding.EncodeToString(rootPublic),
		"payload": map[string]any{"node_id": nodeID, "node_public_key": base64.RawURLEncoding.EncodeToString(publicKey),
			"enrollment_nonce": nonce, "node_proof": base64.RawURLEncoding.EncodeToString(proof)},
	})
	requireTombstoneRetry(t, err)
	canonical, signature, err := identityprotocol.SignEvent(event, rootKey)
	requireTombstoneRetry(t, err)
	if err := identity.ApplyNodeEvent(canonical, signature); err != nil {
		t.Fatalf("apply provider node delegation: %v", err)
	}
	return tombstoneRetryNode{NodeID: nodeID, IdentityID: identityID, PublicKey: publicKey, PrivateKey: privateKey}
}
