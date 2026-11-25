package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
)

type renewalTestLeaseStore struct {
	items    []lease.Lease
	request  lease.SignedRenewalRequest
	beginErr error
}

func (s *renewalTestLeaseStore) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return append([]lease.Lease(nil), s.items...), nil, nil
}

func (s *renewalTestLeaseStore) BeginRenewal(_ context.Context, request lease.SignedRenewalRequest) (lease.Lease, error) {
	s.request = request
	return lease.Lease{}, s.beginErr
}

type renewalTestNode struct {
	store *nodeidentity.Store
	info  nodeidentity.Info
}

func (n renewalTestNode) Info() (nodeidentity.Info, error) { return n.info, nil }
func (n renewalTestNode) SignReplicaRenewalRequest(value lease.Renewal, chain []authority.SignedGrant) (lease.SignedRenewalRequest, error) {
	return n.store.SignReplicaRenewalRequest(value, chain)
}
func (n renewalTestNode) SignReplicaRenewalReceipt(value lease.SignedRenewalRequest) (lease.SignedRenewal, error) {
	return n.store.SignReplicaRenewalReceipt(value)
}
func (n renewalTestNode) SignPeerCommand(command peerprotocol.Command, _ peerprotocol.IdentityResolver, _ time.Time) (peerprotocol.Command, error) {
	return command, nil
}

type renewalTestPeers struct{ sessions []peerapi.SessionInfo }

func (p renewalTestPeers) List() []peerapi.SessionInfo {
	return append([]peerapi.SessionInfo(nil), p.sessions...)
}
func (renewalTestPeers) Connect(context.Context, string, string) error {
	return errors.New("unexpected peer connect")
}
func (renewalTestPeers) RenewReplicaLease(context.Context, string, peerapi.RequestScope, *lease.Store, peerapi.ReplicaRenewalSigner, string, string, string) (lease.Lease, error) {
	return lease.Lease{}, errors.New("unexpected provider renewal")
}

func TestReplicaRenewalReconcilerBeginsOnlyOwnerApprovedDueRenewals(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	identityPublic, identityPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	nodeStore, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := nodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	nodePublic, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	ownerID := "os_" + stringsRepeat("a", 22)
	archiveID := "arc_" + stringsRepeat("b", 22)
	snapshotID := "snap_" + stringsRepeat("a", 64)
	providerID := "nd_" + stringsRepeat("c", 22)
	identityKeyID, err := identityprotocol.KeyID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{"archive.replicate"},
		NotBefore: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(30 * 24 * time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		PolicyEpoch: "1", IssuerKeyID: identityKeyID,
	}, identityPrivate)
	if err != nil {
		t.Fatal(err)
	}
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: "sha256:" + stringsRepeat("b", 64),
		VerifiedAt: now.Add(-time.Minute), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + identityID: identityPublic,
			authority.PrincipalNode + ":" + info.NodeID:    nodePublic,
		},
	}
	owner := authority.AuthoritySnapshot{
		Subject:           authority.OwnerSubject{ID: ownerID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}},
		ReplicationPolicy: authority.ReplicationPolicy{MinimumReplicas: "1", GracePeriodSeconds: "0", CapacityLimitBytes: "0", RelocateAfterGrace: true},
		ArchiveIDs:        []string{archiveID}, Grants: map[string][]authority.SignedGrant{"renew": {grant}},
	}
	item := lease.Lease{
		Offer: lease.SignedOffer{
			Actor: authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID},
			Offer: lease.Offer{LeaseID: "rl_" + stringsRepeat("d", 22), OwnerSubjectID: ownerID,
				ArchiveID: archiveID, SnapshotID: snapshotID, ProviderNodeID: providerID,
				RenewalPolicy: "owner_approval", ExpiresAt: now.Add(6 * time.Hour).Format(time.RFC3339)},
		},
		OfferHash: "sha256:" + stringsRepeat("c", 64), Status: lease.StatusActive,
	}
	if _, err := nodeStore.SignReplicaRenewalRequest(lease.Renewal{
		Version: lease.ProtocolVersion, LeaseID: item.Offer.Offer.LeaseID, OfferHash: item.OfferHash,
		OwnerSubjectID: ownerID, IssuedAt: now.Format(time.RFC3339),
		ExpiresAt: now.Add(automaticReplicaOfferTTL).Format(time.RFC3339),
	}, owner.Grants["renew"]); err != nil {
		t.Fatalf("fixture renewal signing: %v", err)
	}
	store := &renewalTestLeaseStore{items: []lease.Lease{item}}
	reconciler := &replicaRenewalReconciler{
		leases:   store,
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID}, found: true},
		resolve: func(candidate, head string, at time.Time) (authority.IdentityState, error) {
			if candidate != identityID || head != "" || at.Before(now) {
				return authority.IdentityState{}, errors.New("unexpected identity resolution")
			}
			return state, nil
		},
		owners: renewalTestOwner{owner: owner},
		node:   renewalTestNode{store: nodeStore, info: info}, peers: renewalTestPeers{},
		store: &lease.Store{},
	}
	if err := reconciler.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("owner-approved renewal pass: %v", err)
	}
	request := store.request
	if request.Renewal.LeaseID != item.Offer.Offer.LeaseID || request.Renewal.OfferHash != item.OfferHash ||
		request.Renewal.OwnerSubjectID != ownerID || request.Actor != (authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID}) ||
		request.OwnerSignature == "" || request.Renewal.ExpiresAt != now.Add(automaticReplicaOfferTTL).Format(time.RFC3339) {
		t.Fatalf("invalid durable renewal request: lease=%t hash=%t owner=%t actor=%t signature=%t expiry=%t request=%+v", request.Renewal.LeaseID == item.Offer.Offer.LeaseID, request.Renewal.OfferHash == item.OfferHash, request.Renewal.OwnerSubjectID == ownerID, request.Actor == (authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID}), request.OwnerSignature != "", request.Renewal.ExpiresAt == now.Add(automaticReplicaOfferTTL).Format(time.RFC3339), request)
	}
	canonical, err := lease.CanonicalRenewal(request.Renewal)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(nodePublic), append([]byte("IR/replica-lease/v1/renew\x00"), canonical...), mustDecodeSignature(t, request.OwnerSignature)) {
		t.Fatalf("renewal request signature is invalid: %v", err)
	}

	store.request = lease.SignedRenewalRequest{}
	store.items[0].Offer.Offer.RenewalPolicy = "manual"
	if err := reconciler.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("manual renewal policy pass: %v", err)
	}
	if store.request.OwnerSignature != "" {
		t.Fatal("manual renewal policy was automatically renewed")
	}
}

func TestReplicaRenewalDueUsesBoundedLeadWindow(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	base := lease.Lease{Offer: lease.SignedOffer{Offer: lease.Offer{
		ExpiresAt: now.Add(48 * time.Hour).Format(time.RFC3339),
	}}}
	tests := []struct {
		name    string
		expires time.Time
		want    bool
	}{
		{name: "outside lead window", expires: now.Add(replicaRenewalLeadTime + time.Second), want: false},
		{name: "at lead window", expires: now.Add(replicaRenewalLeadTime), want: true},
		{name: "inside lead window", expires: now.Add(time.Hour), want: true},
		{name: "expired", expires: now, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := base
			item.Offer.Offer.ExpiresAt = test.expires.Format(time.RFC3339)
			if got := replicaRenewalDue(item, now); got != test.want {
				t.Fatalf("replicaRenewalDue(%s) = %t, want %t", test.expires, got, test.want)
			}
		})
	}
}

func TestReplicaRenewalSelectionResumesProviderOutboxStates(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	localNode := "nd_" + stringsRepeat("a", 22)
	sourceNode := "nd_" + stringsRepeat("b", 22)
	base := lease.Lease{Offer: lease.SignedOffer{
		Actor: authority.Principal{Kind: authority.PrincipalNode, ID: sourceNode},
		Offer: lease.Offer{ProviderNodeID: localNode, RenewalPolicy: "owner_approval", ExpiresAt: now.Add(72 * time.Hour).Format(time.RFC3339)},
	}, Status: lease.StatusActive}
	tests := []struct {
		name string
		item lease.Lease
		want bool
	}{
		{name: "provider interrupted before receipt completion", item: func() lease.Lease {
			v := base
			v.Status = lease.StatusRenewing
			v.PendingRenewal = &lease.SignedRenewalRequest{}
			return v
		}(), want: true},
		{name: "provider not due", item: base, want: false},
		{name: "source renewing request is not provider work", item: func() lease.Lease {
			v := base
			v.Offer.Offer.ProviderNodeID = sourceNode
			v.Offer.Actor = authority.Principal{Kind: authority.PrincipalNode, ID: localNode}
			v.Status = lease.StatusRenewing
			v.PendingRenewal = &lease.SignedRenewalRequest{}
			return v
		}(), want: false},
		{name: "manual provider lease", item: func() lease.Lease {
			v := base
			v.Offer.Offer.RenewalPolicy = "manual"
			v.Status = lease.StatusRenewing
			v.PendingRenewal = &lease.SignedRenewalRequest{}
			return v
		}(), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := replicaRenewalEligibleForNode(test.item, localNode, now); got != test.want {
				t.Fatalf("replica renewal selection = %t, want %t", got, test.want)
			}
		})
	}
}

func TestReplicaLeaseEffectiveExpiryHonorsPendingRenewal(t *testing.T) {
	oldExpiry := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	pendingExpiry := oldExpiry.Add(7 * 24 * time.Hour)
	item := lease.Lease{
		Offer:  lease.SignedOffer{Offer: lease.Offer{ExpiresAt: oldExpiry.Format(time.RFC3339)}},
		Status: lease.StatusRenewing,
		PendingRenewal: &lease.SignedRenewalRequest{Renewal: lease.Renewal{
			ExpiresAt: pendingExpiry.Format(time.RFC3339),
		}},
	}
	if got := replicaLeaseEffectiveExpiry(item); !got.Equal(pendingExpiry) {
		t.Fatalf("pending renewal effective expiry=%s, want %s", got, pendingExpiry)
	}
}

type renewalTestOwner struct{ owner authority.AuthoritySnapshot }

func (o renewalTestOwner) Snapshot(subjectID string, _ authority.AuthorityIdentityResolver, _ time.Time) (authority.AuthoritySnapshot, error) {
	if subjectID != o.owner.Subject.ID {
		return authority.AuthoritySnapshot{}, errors.New("unexpected owner subject")
	}
	return o.owner, nil
}
func (renewalTestOwner) AuthorizeChain(string, []authority.SignedGrant, authority.AuthorityIdentityResolver, time.Time, authority.Principal, string, string, string) error {
	return nil
}

func stringsRepeat(value string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += value
	}
	return result
}

func mustDecodeSignature(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
