package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/replication"
)

type replicaIntegrityRangeFixture struct {
	data      []byte
	responses [][]byte
	err       error
	calls     int
	sessions  []peerapi.SessionInfo
	nodeID    string
	sourceID  string
	clock     *time.Time
	advance   time.Duration
	issued    []time.Time
}

func (f *replicaIntegrityRangeFixture) List() []peerapi.SessionInfo {
	return append([]peerapi.SessionInfo(nil), f.sessions...)
}

type replicaIntegritySignerFixture struct{}

func (replicaIntegritySignerFixture) SignPeerCommand(command peerprotocol.Command, _ peerprotocol.IdentityResolver, _ time.Time) (peerprotocol.Command, error) {
	return command, nil
}

type replicaIntegrityIdentityFixture struct{ snapshot globalidentity.Snapshot }

func (f replicaIntegrityIdentityFixture) Snapshot() (globalidentity.Snapshot, bool, error) {
	return f.snapshot, true, nil
}

type replicaIntegrityOwnerFixture struct{ snapshot authority.AuthoritySnapshot }

func (f replicaIntegrityOwnerFixture) Snapshot(string, authority.AuthorityIdentityResolver, time.Time) (authority.AuthoritySnapshot, error) {
	return f.snapshot, nil
}
func (replicaIntegrityOwnerFixture) AuthorizeChain(string, []authority.SignedGrant, authority.AuthorityIdentityResolver, time.Time, authority.Principal, string, string, string) error {
	return nil
}

type replicaIntegrityLeaseFixture struct {
	items    []lease.Lease
	failures int
}

func (f *replicaIntegrityLeaseFixture) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return append([]lease.Lease(nil), f.items...), nil, nil
}
func (f *replicaIntegrityLeaseFixture) FailReadbackMismatch(_ context.Context, leaseID, offerHash, revision string, at time.Time) (lease.Lease, error) {
	for i := range f.items {
		if f.items[i].Offer.Offer.LeaseID == leaseID && f.items[i].OfferHash == offerHash && f.items[i].Revision == revision && f.items[i].Status == lease.StatusActive {
			f.failures++
			f.items[i].Status = lease.StatusFailed
			f.items[i].FailureCode = "ciphertext_readback_mismatch"
			f.items[i].FailureAt = at.UTC().Format(time.RFC3339)
			return f.items[i], nil
		}
	}
	return lease.Lease{}, lease.ErrConflict
}

func (f *replicaIntegrityRangeFixture) OpenCiphertextRange(_ context.Context, nodeID string, scope peerapi.RequestScope,
	snapshotID, objectID string, keyEpoch uint64, offset, length int64) (io.ReadCloser, error) {
	f.calls++
	if scope.Now != nil {
		f.issued = append(f.issued, scope.Now())
	}
	if f.clock != nil && f.advance > 0 {
		*f.clock = f.clock.Add(f.advance)
	}
	if f.err != nil {
		return nil, f.err
	}
	data := f.data
	if f.calls <= len(f.responses) {
		data = f.responses[f.calls-1]
	}
	expectedNode := f.nodeID
	if expectedNode == "" {
		expectedNode = "nd_provider"
	}
	expectedSource := f.sourceID
	if expectedSource == "" {
		expectedSource = "nd_source"
	}
	if nodeID != expectedNode || scope.Action != "archive.replicate" || scope.NodeID != expectedSource ||
		snapshotID != "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		objectID != "ob_aaaaaaaaaaaaaaaa" || keyEpoch != 1 || offset < 0 || length <= 0 || offset+length > int64(len(data)) {
		return nil, peerapi.ErrInvalidRequest
	}
	return io.NopCloser(bytes.NewReader(data[offset : offset+length])), nil
}

func TestVerifyRemoteReplicaObjectsReadsBoundedExactRanges(t *testing.T) {
	data := bytes.Repeat([]byte("replica-integrity"), int(replication.MaxCiphertextChunkBytes/17)+3)
	digest := sha256.Sum256(data)
	provider := &peerapi.SessionInfo{
		IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", NodeID: "nd_provider",
	}
	peers := &replicaIntegrityRangeFixture{data: data}
	offer := lease.Offer{
		ArchiveID:  "arc_aaaaaaaaaaaaaaaaaaaaaa",
		SnapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		KeyEpoch:   "1", ProviderNodeID: provider.NodeID,
		OwnerSubjectID:     "os_aaaaaaaaaaaaaaaaaaaaaa",
		EncryptedObjectSet: []lease.EncryptedObject{{ObjectID: "ob_aaaaaaaaaaaaaaaa", CiphertextSize: strconv.FormatInt(int64(len(data)), 10), CiphertextSHA256: "sha256:" + hex.EncodeToString(digest[:])}},
	}
	matched, err := verifyRemoteReplicaObjects(context.Background(), peers, provider, nil, replicaIntegritySignerFixture{},
		"gi_bbbbbbbbbbbbbbbbbbbbbb", "head", "nd_source", offer, []authority.SignedGrant{{}}, func() time.Time { return time.Now() })
	if err != nil || !matched {
		t.Fatalf("matching remote objects: matched=%t err=%v", matched, err)
	}
	if peers.calls < 2 {
		t.Fatalf("large ciphertext was not read in bounded ranges: calls=%d", peers.calls)
	}
}

func TestVerifyRemoteReplicaObjectsDistinguishesMismatchFromUnavailable(t *testing.T) {
	provider := &peerapi.SessionInfo{IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", NodeID: "nd_provider"}
	data := []byte("ciphertext")
	digest := sha256.Sum256([]byte("other-bytes"))
	offer := lease.Offer{
		ArchiveID:  "arc_aaaaaaaaaaaaaaaaaaaaaa",
		SnapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		KeyEpoch:   "1", ProviderNodeID: provider.NodeID, OwnerSubjectID: "os_aaaaaaaaaaaaaaaaaaaaaa",
		EncryptedObjectSet: []lease.EncryptedObject{{ObjectID: "ob_aaaaaaaaaaaaaaaa", CiphertextSize: strconv.FormatInt(int64(len(data)), 10), CiphertextSHA256: "sha256:" + hex.EncodeToString(digest[:])}},
	}
	peers := &replicaIntegrityRangeFixture{data: data}
	matched, err := verifyRemoteReplicaObjects(context.Background(), peers, provider, nil, replicaIntegritySignerFixture{},
		"gi_bbbbbbbbbbbbbbbbbbbbbb", "head", "nd_source", offer, nil, func() time.Time { return time.Now() })
	if err != nil || matched {
		t.Fatalf("digest mismatch should be explicit: matched=%t err=%v", matched, err)
	}
	peers.err = io.ErrUnexpectedEOF
	matched, err = verifyRemoteReplicaObjects(context.Background(), peers, provider, nil, replicaIntegritySignerFixture{},
		"gi_bbbbbbbbbbbbbbbbbbbbbb", "head", "nd_source", offer, nil, func() time.Time { return time.Now() })
	if err == nil || matched {
		t.Fatalf("incomplete readback must remain unknown, not corruption: matched=%t err=%v", matched, err)
	}
}

func TestVerifyRemoteReplicaObjectsUsesFreshClockAcrossLongScan(t *testing.T) {
	data := bytes.Repeat([]byte("fresh-command-time"), int(replication.MaxCiphertextChunkBytes/18)+2)
	digest := sha256.Sum256(data)
	provider := &peerapi.SessionInfo{IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", NodeID: "nd_provider"}
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	peers := &replicaIntegrityRangeFixture{data: data, clock: &now, advance: peerprotocol.MaxCommandTTL + time.Second}
	offer := lease.Offer{
		ArchiveID: "arc_aaaaaaaaaaaaaaaaaaaaaa", SnapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		KeyEpoch: "1", ProviderNodeID: provider.NodeID, OwnerSubjectID: "os_aaaaaaaaaaaaaaaaaaaaaa",
		EncryptedObjectSet: []lease.EncryptedObject{{ObjectID: "ob_aaaaaaaaaaaaaaaa", CiphertextSize: strconv.Itoa(len(data)), CiphertextSHA256: "sha256:" + hex.EncodeToString(digest[:])}},
	}
	matched, err := verifyRemoteReplicaObjects(context.Background(), peers, provider, nil, replicaIntegritySignerFixture{},
		"gi_bbbbbbbbbbbbbbbbbbbbbb", "head", "nd_source", offer, nil, func() time.Time { return *peers.clock })
	if err != nil || !matched {
		t.Fatalf("long readback with refreshed command time: matched=%t err=%v", matched, err)
	}
	if len(peers.issued) < 2 || !peers.issued[1].After(peers.issued[0].Add(peerprotocol.MaxCommandTTL)) {
		t.Fatalf("range commands did not receive fresh issued times: %v", peers.issued)
	}
}

func TestReplicaIntegrityReconcilerFailsOnlyVerifiedOutboundMismatch(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	localRootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	localIdentityID, err := identityprotocol.IdentityID(localRootPublic)
	if err != nil {
		t.Fatal(err)
	}
	providerRootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	providerIdentityID, err := identityprotocol.IdentityID(providerRootPublic)
	if err != nil {
		t.Fatal(err)
	}
	nodeStore, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := nodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	localNodePublic, err := base64.RawURLEncoding.DecodeString(localNode.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	providerNodePublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	providerNodeID, err := identityprotocol.NodeID(providerNodePublic)
	if err != nil {
		t.Fatal(err)
	}
	localState := authority.IdentityState{
		IdentityID: localIdentityID, HeadHash: "sha256:" + stringsRepeat("a", 64), VerifiedAt: now.Add(-time.Minute), FreshUntil: now.Add(10 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + localIdentityID: localRootPublic,
			authority.PrincipalNode + ":" + localNode.NodeID:    localNodePublic,
		},
	}
	providerState := authority.IdentityState{
		IdentityID: providerIdentityID, HeadHash: "sha256:" + stringsRepeat("b", 64), VerifiedAt: now.Add(-time.Minute), FreshUntil: now.Add(10 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + providerIdentityID: providerRootPublic,
			authority.PrincipalNode + ":" + providerNodeID:         providerNodePublic,
		},
	}
	resolver := func(identityID, _ string, at time.Time) (authority.IdentityState, error) {
		switch identityID {
		case localIdentityID:
			localState.VerifiedAt = at.Add(-time.Minute)
			localState.FreshUntil = at.Add(4 * time.Minute)
			return localState, nil
		case providerIdentityID:
			providerState.VerifiedAt = at.Add(-time.Minute)
			providerState.FreshUntil = at.Add(4 * time.Minute)
			return providerState, nil
		default:
			return authority.IdentityState{}, authority.ErrDenied
		}
	}
	archiveID := "arc_aaaaaaaaaaaaaaaaaaaaaa"
	snapshotID := "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ownerID := "os_aaaaaaaaaaaaaaaaaaaaaa"
	data := []byte("locally verified ciphertext differs from signed manifest")
	expected := bytes.Repeat([]byte("x"), len(data))
	want := sha256.Sum256(expected)
	object := lease.EncryptedObject{ObjectID: "ob_aaaaaaaaaaaaaaaa", CiphertextSize: strconv.FormatInt(int64(len(data)), 10), CiphertextSHA256: "sha256:" + hex.EncodeToString(want[:])}
	item := lease.Lease{
		Offer: lease.SignedOffer{Offer: lease.Offer{
			LeaseID: "rl_aaaaaaaaaaaaaaaaaaaaaa", ArchiveID: archiveID, SnapshotID: snapshotID, KeyEpoch: "1",
			ProviderNodeID: providerNodeID, OwnerSubjectID: ownerID, ExpiresAt: now.Add(-time.Hour).Format(time.RFC3339),
			EncryptedObjectSet: []lease.EncryptedObject{object},
		}, Actor: authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID}, GrantChain: []authority.SignedGrant{{}}},
		Renewals:  []lease.SignedRenewal{{Renewal: lease.Renewal{ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339)}}},
		OfferHash: "sha256:" + stringsRepeat("c", 64), Status: lease.StatusActive, Revision: "1",
	}
	leaseStore := &replicaIntegrityLeaseFixture{items: []lease.Lease{item}}
	peers := &replicaIntegrityRangeFixture{data: data, responses: [][]byte{data, expected}, nodeID: providerNodeID, sourceID: localNode.NodeID,
		sessions: []peerapi.SessionInfo{{IdentityID: providerIdentityID, NodeID: providerNodeID}}}
	buildReconciler := func() *replicaIntegrityReconciler {
		return &replicaIntegrityReconciler{
			leases: leaseStore, identity: replicaIntegrityIdentityFixture{snapshot: globalidentity.Snapshot{IdentityID: localIdentityID}}, resolve: resolver,
			owners: replicaIntegrityOwnerFixture{snapshot: authority.AuthoritySnapshot{
				Subject:    authority.OwnerSubject{ID: ownerID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: localIdentityID}},
				ArchiveIDs: []string{archiveID},
			}}, node: nodeStore, peers: peers, interval: time.Hour, clock: func() time.Time { return now },
		}
	}
	if err := buildReconciler().Reconcile(context.Background(), now); err != nil {
		if diagnostic, ok := err.(replicaIntegrityReconcileError); ok {
			t.Fatalf("integrity reconcile stage=%s: %v", diagnostic.stage, err)
		}
		t.Fatal(err)
	}
	if leaseStore.failures != 0 || leaseStore.items[0].Status != lease.StatusActive {
		t.Fatalf("single inconsistent read across atomic replacement was treated as damage: failures=%d lease=%+v", leaseStore.failures, leaseStore.items[0])
	}
	peers.calls = 0
	peers.responses = [][]byte{data, data}
	if err := buildReconciler().Reconcile(context.Background(), now); err != nil {
		if diagnostic, ok := err.(replicaIntegrityReconcileError); ok {
			t.Fatalf("integrity reconcile stage=%s: %v", diagnostic.stage, err)
		}
		t.Fatal(err)
	}
	if leaseStore.failures != 1 || leaseStore.items[0].Status != lease.StatusFailed || leaseStore.items[0].FailureCode != "ciphertext_readback_mismatch" {
		t.Fatalf("complete mismatch did not fail exact source lease: failures=%d lease=%+v", leaseStore.failures, leaseStore.items[0])
	}
	if peers.calls != 2 {
		t.Fatalf("expected the mismatching object to be read twice before marking failed, got %d", peers.calls)
	}
	// A later transport failure is uncertainty and must leave a fresh active
	// lease unchanged; only two complete mismatching reads may fail it.
	leaseStore.items[0] = item
	leaseStore.failures = 0
	peers.calls = 0
	peers.responses = nil
	peers.clock = nil
	peers.advance = 0
	peers.issued = nil
	peers.err = io.ErrUnexpectedEOF
	unavailableReconciler := buildReconciler()
	if err := unavailableReconciler.Reconcile(context.Background(), now.Add(2*time.Minute)); err == nil {
		t.Fatal("unavailable peer should produce a sanitized reconcile error")
	}
	if leaseStore.failures != 0 || leaseStore.items[0].Status != lease.StatusActive {
		t.Fatalf("unavailable peer was treated as damage: failures=%d lease=%+v", leaseStore.failures, leaseStore.items[0])
	}
	if calls := peers.calls; calls != 1 {
		t.Fatalf("unavailable readback calls=%d, want one range request", calls)
	}
	if err := unavailableReconciler.Reconcile(context.Background(), now.Add(6*time.Minute)); err != nil || peers.calls != 1 {
		t.Fatalf("availability retry was not bounded by backoff: calls=%d err=%v", peers.calls, err)
	}
	if err := unavailableReconciler.Reconcile(context.Background(), now.Add(8*time.Minute)); err == nil || peers.calls != 2 {
		t.Fatalf("availability retry did not resume after backoff: calls=%d err=%v", peers.calls, err)
	}
}
