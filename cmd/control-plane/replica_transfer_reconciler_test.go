package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
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

type replicaTransferLeaseFixture struct {
	items      []lease.Lease
	tombstones []lease.TombstoneRecord
	err        error
}

func (f replicaTransferLeaseFixture) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return append([]lease.Lease(nil), f.items...), append([]lease.TombstoneRecord(nil), f.tombstones...), f.err
}

func TestReplicaTransferReconcilerResumesOnlyExplicitlyAcceptedLiveLeases(t *testing.T) {
	identityID := "gi_aaaaaaaaaaaaaaaaaaaaaa"
	ownerID := "os_aaaaaaaaaaaaaaaaaaaaaa"
	archiveID := "arc_aaaaaaaaaaaaaaaaaaaaaa"
	blockedArchiveID := "arc_bbbbbbbbbbbbbbbbbbbbbb"
	snapshotID := "snap_" + strings.Repeat("a", 64)
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	identityState, localInfo, sourceID := replicaTransferIdentityFixture(t, identityID, now)
	chain := []authority.SignedGrant{{Grant: authority.Grant{
		SubjectID: ownerID, Audience: authority.Principal{Kind: authority.PrincipalNode, ID: localInfo.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{"archive.replicate"},
	}}}

	items := []lease.Lease{
		replicaTransferLeaseFixtureItem("rl_0000000000000000000001", lease.StatusOffered, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000002", lease.StatusAccepted, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000003", lease.StatusTransferring, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000004", lease.StatusVerifying, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000005", lease.StatusActive, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000006", lease.StatusAccepted, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(-time.Second)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000007", lease.StatusAccepted, "nd_zzzzzzzzzzzzzzzzzzzzzz", sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000008", lease.StatusAccepted, localInfo.NodeID, sourceID, ownerID, blockedArchiveID, snapshotID, now.Add(time.Hour)),
	}
	tombstone := lease.TombstoneRecord{Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: blockedArchiveID}}}
	peers := &fakePeerConnectionManager{sessions: []peerapi.SessionInfo{{IdentityID: identityID, NodeID: sourceID}}, connectErr: map[string]error{}}
	gate := replication.NewImportGate()
	gate.MarkSourceVerified(items[4].Offer.Offer.LeaseID, items[4].OfferHash)
	var imported []string
	reconciler := &replicaTransferReconciler{
		leases:   replicaTransferLeaseFixture{items: items, tombstones: []lease.TombstoneRecord{tombstone}},
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID}, found: true},
		resolve: func(candidateIdentityID, _ string, _ time.Time) (authority.IdentityState, error) {
			if candidateIdentityID != identityID {
				return authority.IdentityState{}, errors.New("unexpected identity")
			}
			return identityState, nil
		},
		ownerSnapshot: func(subjectID string, _ time.Time) (authority.AuthoritySnapshot, error) {
			if subjectID != ownerID {
				return authority.AuthoritySnapshot{}, errors.New("unexpected OwnerSubject")
			}
			return authority.AuthoritySnapshot{
				Subject:    authority.OwnerSubject{ID: ownerID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}},
				ArchiveIDs: []string{archiveID, blockedArchiveID}, Grants: map[string][]authority.SignedGrant{"grant": chain},
			}, nil
		},
		authorize: func(subjectID string, candidate []authority.SignedGrant, at time.Time, audience authority.Principal, candidateArchiveID, candidateSnapshotID, action string) error {
			if subjectID != ownerID || !reflect.DeepEqual(candidate, chain) || !at.Equal(now) ||
				audience != (authority.Principal{Kind: authority.PrincipalNode, ID: localInfo.NodeID}) ||
				candidateArchiveID != archiveID || candidateSnapshotID != snapshotID || action != "archive.replicate" {
				return errors.New("wrong capability scope")
			}
			return nil
		},
		node:       fakeNodeInfoReader{info: localInfo},
		signer:     fakePeerCommandSigner{},
		peers:      peers,
		importGate: gate,
		maxPerPass: 8, // The reconciler still enforces its production cap of two.
		importReplica: func(_ context.Context, sourceNodeID string, scope peerapi.RequestScope, gotArchiveID, gotSnapshotID, leaseID string) (lease.Lease, error) {
			if sourceNodeID != sourceID || scope.TargetNodeID != sourceID || scope.NodeID != localInfo.NodeID ||
				scope.OwnerSubjectID != ownerID || gotArchiveID != archiveID || gotSnapshotID != snapshotID {
				return lease.Lease{}, errors.New("wrong import scope")
			}
			imported = append(imported, leaseID)
			for _, item := range items {
				if item.Offer.Offer.LeaseID == leaseID {
					item.Status = lease.StatusActive
					return item, nil
				}
			}
			return lease.Lease{}, errors.New("fixture lease disappeared")
		},
	}
	if err := reconciler.Reconcile(context.Background(), now); err == nil {
		t.Fatal("bounded pass with remaining accepted leases was reported complete")
	}
	if len(imported) != 2 || imported[0] != "rl_0000000000000000000002" || imported[1] != "rl_0000000000000000000003" {
		t.Fatalf("first bounded pass resumed %v, want the first two eligible leases", imported)
	}
	// Model the durable store reflecting each successful import before the next
	// maintenance pass. A restart pass should then reach the remaining lease.
	items[1].Status = lease.StatusActive
	items[2].Status = lease.StatusActive
	if err := reconciler.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("second bounded pass: %v", err)
	}
	want := []string{"rl_0000000000000000000002", "rl_0000000000000000000003", "rl_0000000000000000000004"}
	if !reflect.DeepEqual(imported, want) {
		t.Fatalf("resumed leases=%v, want only accepted non-tombstoned live leases %v", imported, want)
	}
	if connects, _ := peers.results(); len(connects) != 0 {
		t.Fatalf("already connected source was dialed again: %v", connects)
	}
}

func TestReplicaTransferReconcilerResumesCrossIdentityLeaseUnderOwnerController(t *testing.T) {
	providerIdentityID := "gi_provideridentity000000"
	sourceIdentityID := "gi_sourceidentity00000000"
	ownerID := "os_crossidentity00000000"
	archiveID := "arc_crossidentity00000000"
	snapshotID := "snap_" + strings.Repeat("f", 64)
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	providerState, providerInfo, _ := replicaTransferIdentityFixture(t, providerIdentityID, now)
	sourceState, _, sourceNodeID := replicaTransferIdentityFixture(t, sourceIdentityID, now)
	delete(providerState.CurrentKeys, authority.PrincipalNode+":"+sourceNodeID)
	item := replicaTransferLeaseFixtureItem("rl_"+strings.Repeat("x", 22), lease.StatusAccepted, providerInfo.NodeID, sourceNodeID, ownerID, archiveID, snapshotID, now.Add(time.Hour))
	chain := []authority.SignedGrant{{Grant: authority.Grant{
		SubjectID: ownerID, Audience: authority.Principal{Kind: authority.PrincipalNode, ID: providerInfo.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{"archive.replicate"},
	}}}
	peers := &fakePeerConnectionManager{sessions: []peerapi.SessionInfo{{IdentityID: sourceIdentityID, NodeID: sourceNodeID}}, connectErr: map[string]error{}}
	var imported bool
	reconciler := &replicaTransferReconciler{
		leases:   replicaTransferLeaseFixture{items: []lease.Lease{item}},
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: providerIdentityID}, found: true},
		resolve: func(identityID, _ string, _ time.Time) (authority.IdentityState, error) {
			switch identityID {
			case providerIdentityID:
				return providerState, nil
			case sourceIdentityID:
				return sourceState, nil
			default:
				return authority.IdentityState{}, errors.New("unknown identity")
			}
		},
		ownerSnapshot: func(subjectID string, _ time.Time) (authority.AuthoritySnapshot, error) {
			return authority.AuthoritySnapshot{
				Subject:    authority.OwnerSubject{ID: subjectID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: sourceIdentityID}},
				ArchiveIDs: []string{archiveID}, Grants: map[string][]authority.SignedGrant{"grant": chain},
			}, nil
		},
		authorize: func(subjectID string, candidate []authority.SignedGrant, _ time.Time, audience authority.Principal, gotArchiveID, gotSnapshotID, action string) error {
			if subjectID != ownerID || !reflect.DeepEqual(candidate, chain) || audience != (authority.Principal{Kind: authority.PrincipalNode, ID: providerInfo.NodeID}) ||
				gotArchiveID != archiveID || gotSnapshotID != snapshotID || action != "archive.replicate" {
				return errors.New("wrong cross-identity capability scope")
			}
			return nil
		},
		node: fakeNodeInfoReader{info: providerInfo}, signer: fakePeerCommandSigner{}, peers: peers,
		importGate: replication.NewImportGate(),
		importReplica: func(_ context.Context, sourceNode string, scope peerapi.RequestScope, gotArchive, gotSnapshot, leaseID string) (lease.Lease, error) {
			if sourceNode != sourceNodeID || scope.IdentityID != providerIdentityID || scope.TargetIdentityID != sourceIdentityID ||
				scope.TargetNodeID != sourceNodeID || gotArchive != archiveID || gotSnapshot != snapshotID || leaseID != item.Offer.Offer.LeaseID {
				return lease.Lease{}, errors.New("cross-identity peer scope was not preserved")
			}
			imported = true
			item.Status = lease.StatusActive
			return item, nil
		},
	}
	if err := reconciler.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("cross-identity accepted lease did not resume: %v", err)
	}
	if !imported {
		t.Fatal("cross-identity accepted lease was not imported")
	}
}

func TestReplicaTransferReconcilerRetriesActiveLeaseSourceVerificationAfterRestart(t *testing.T) {
	identityID := "gi_cccccccccccccccccccccc"
	ownerID := "os_cccccccccccccccccccccc"
	archiveID := "arc_cccccccccccccccccccccc"
	snapshotID := "snap_" + strings.Repeat("c", 64)
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	identityState, localInfo, sourceID := replicaTransferIdentityFixture(t, identityID, now)
	item := replicaTransferLeaseFixtureItem("rl_0000000000000000000021", lease.StatusActive, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(-time.Hour))
	item.Renewals = []lease.SignedRenewal{{Renewal: lease.Renewal{ExpiresAt: now.Add(time.Hour).Format(time.RFC3339)}}}
	store := replicaTransferLeaseFixture{items: []lease.Lease{item}}
	peers := &fakePeerConnectionManager{sessions: []peerapi.SessionInfo{{IdentityID: identityID, NodeID: sourceID}}, connectErr: map[string]error{}}
	chain := []authority.SignedGrant{{Grant: authority.Grant{
		SubjectID: ownerID, Audience: authority.Principal{Kind: authority.PrincipalNode, ID: localInfo.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{"archive.replicate"},
	}}}
	build := func(gate *replication.ImportGate, importer func(context.Context, string, peerapi.RequestScope, string, string, string) (lease.Lease, error)) *replicaTransferReconciler {
		return &replicaTransferReconciler{
			leases: store, identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID}, found: true},
			resolve: func(string, string, time.Time) (authority.IdentityState, error) { return identityState, nil },
			ownerSnapshot: func(subjectID string, _ time.Time) (authority.AuthoritySnapshot, error) {
				return authority.AuthoritySnapshot{
					Subject:    authority.OwnerSubject{ID: subjectID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}},
					ArchiveIDs: []string{archiveID}, Grants: map[string][]authority.SignedGrant{"grant": chain},
				}, nil
			},
			authorize: func(string, []authority.SignedGrant, time.Time, authority.Principal, string, string, string) error {
				return nil
			},
			node: fakeNodeInfoReader{info: localInfo}, signer: fakePeerCommandSigner{}, peers: peers,
			importGate: gate, importReplica: importer, maxPerPass: 1,
		}
	}
	calls := 0
	first := build(replication.NewImportGate(), func(context.Context, string, peerapi.RequestScope, string, string, string) (lease.Lease, error) {
		calls++
		return item, errors.New("lost source verification response")
	})
	if err := first.Reconcile(context.Background(), now); err == nil {
		t.Fatal("lost source verification response was reported as complete")
	}
	if calls != 1 {
		t.Fatalf("first process import attempts=%d, want 1", calls)
	}

	// A new ImportGate models a Control Plane restart: the durable provider
	// lease remains active, but process-local source confirmation is unknown.
	second := build(replication.NewImportGate(), func(context.Context, string, peerapi.RequestScope, string, string, string) (lease.Lease, error) {
		calls++
		return item, nil
	})
	if err := second.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("restart confirmation pass: %v", err)
	}
	if calls != 2 {
		t.Fatalf("restart did not retry source verification: attempts=%d", calls)
	}
	if err := second.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("confirmed active lease pass: %v", err)
	}
	if calls != 2 {
		t.Fatalf("confirmed active lease was needlessly rechecked: attempts=%d", calls)
	}
}

func TestReplicaTransferReconcilerRotatesBoundedRetriesAndRequiresCurrentCapability(t *testing.T) {
	identityID := "gi_bbbbbbbbbbbbbbbbbbbbbb"
	ownerID := "os_bbbbbbbbbbbbbbbbbbbbbb"
	archiveID := "arc_bbbbbbbbbbbbbbbbbbbbbb"
	snapshotID := "snap_" + strings.Repeat("b", 64)
	now := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	identityState, localInfo, sourceID := replicaTransferIdentityFixture(t, identityID, now)
	chain := []authority.SignedGrant{{Grant: authority.Grant{
		SubjectID: ownerID, Audience: authority.Principal{Kind: authority.PrincipalNode, ID: localInfo.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{"archive.replicate"},
	}}}
	items := []lease.Lease{
		replicaTransferLeaseFixtureItem("rl_0000000000000000000011", lease.StatusAccepted, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000012", lease.StatusAccepted, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
		replicaTransferLeaseFixtureItem("rl_0000000000000000000013", lease.StatusAccepted, localInfo.NodeID, sourceID, ownerID, archiveID, snapshotID, now.Add(time.Hour)),
	}
	peers := &fakePeerConnectionManager{sessions: []peerapi.SessionInfo{{IdentityID: identityID, NodeID: sourceID}}, connectErr: map[string]error{}}
	var imported []string
	allowCapability := true
	reconciler := &replicaTransferReconciler{
		leases:   replicaTransferLeaseFixture{items: items},
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID}, found: true},
		resolve:  func(string, string, time.Time) (authority.IdentityState, error) { return identityState, nil },
		ownerSnapshot: func(subjectID string, _ time.Time) (authority.AuthoritySnapshot, error) {
			return authority.AuthoritySnapshot{
				Subject:    authority.OwnerSubject{ID: subjectID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}},
				ArchiveIDs: []string{archiveID}, Grants: map[string][]authority.SignedGrant{"grant": chain},
			}, nil
		},
		authorize: func(string, []authority.SignedGrant, time.Time, authority.Principal, string, string, string) error {
			if !allowCapability {
				return authority.ErrDenied
			}
			return nil
		},
		node: fakeNodeInfoReader{info: localInfo}, signer: fakePeerCommandSigner{}, peers: peers,
		importGate: replication.NewImportGate(), maxPerPass: 2,
		importReplica: func(_ context.Context, _ string, _ peerapi.RequestScope, _ string, _ string, leaseID string) (lease.Lease, error) {
			imported = append(imported, leaseID)
			return lease.Lease{}, errors.New("injected transient transfer failure")
		},
	}
	if err := reconciler.Reconcile(context.Background(), now); err == nil {
		t.Fatal("transient failures were reported as a successful full pass")
	}
	if !reflect.DeepEqual(imported, []string{"rl_0000000000000000000011", "rl_0000000000000000000012"}) {
		t.Fatalf("first bounded pass=%v", imported)
	}
	if err := reconciler.Reconcile(context.Background(), now); err == nil {
		t.Fatal("second transient pass was reported as success")
	}
	if !reflect.DeepEqual(imported, []string{"rl_0000000000000000000011", "rl_0000000000000000000012", "rl_0000000000000000000013", "rl_0000000000000000000011"}) {
		t.Fatalf("retry rotation=%v", imported)
	}
	allowCapability = false
	before := len(imported)
	if err := reconciler.Reconcile(context.Background(), now); err == nil {
		t.Fatal("revoked/missing capability was reported as success")
	}
	if len(imported) != before {
		t.Fatalf("import ran without current capability: before=%d after=%d", before, len(imported))
	}
}

type fakePeerCommandSigner struct{}

func (fakePeerCommandSigner) SignPeerCommand(command peerprotocol.Command, _ peerprotocol.IdentityResolver, _ time.Time) (peerprotocol.Command, error) {
	return command, nil
}

func replicaTransferLeaseFixtureItem(leaseID string, status lease.Status, providerNodeID, sourceNodeID, ownerID, archiveID, snapshotID string, expires time.Time) lease.Lease {
	return lease.Lease{
		Offer: lease.SignedOffer{
			Actor: authority.Principal{Kind: authority.PrincipalNode, ID: sourceNodeID},
			Offer: lease.Offer{
				LeaseID: leaseID, ProviderNodeID: providerNodeID, OwnerSubjectID: ownerID,
				ArchiveID: archiveID, SnapshotID: snapshotID, ExpiresAt: expires.UTC().Format(time.RFC3339),
			},
		},
		OfferHash: "sha256:" + strings.Repeat("a", 64),
		Status:    status,
	}
}

func replicaTransferIdentityFixture(t *testing.T, identityID string, now time.Time) (authority.IdentityState, nodeidentity.Info, string) {
	t.Helper()
	_, localPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, sourcePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	localPublic := localPrivate.Public().(ed25519.PublicKey)
	sourcePublic := sourcePrivate.Public().(ed25519.PublicKey)
	localNodeID, err := identityprotocol.NodeID(localPublic)
	if err != nil {
		t.Fatal(err)
	}
	sourceNodeID, err := identityprotocol.NodeID(sourcePublic)
	if err != nil {
		t.Fatal(err)
	}
	return authority.IdentityState{
		IdentityID: identityID, HeadHash: "sha256:" + strings.Repeat("a", 64),
		VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(time.Hour),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalNode + ":" + localNodeID:  localPublic,
			authority.PrincipalNode + ":" + sourceNodeID: sourcePublic,
		},
	}, nodeidentity.Info{NodeID: localNodeID, PublicKey: base64.RawURLEncoding.EncodeToString(localPublic)}, sourceNodeID
}
