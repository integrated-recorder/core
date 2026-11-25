package main

import (
	"context"
	"encoding/base64"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
)

const (
	replicaRenewalLeadTime = 24 * time.Hour
	maxReplicaRenewalsPass = 2
	replicaRenewalTimeout  = 30 * time.Second
)

type replicaRenewalLeaseStore interface {
	Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error)
	BeginRenewal(context.Context, lease.SignedRenewalRequest) (lease.Lease, error)
}

type replicaRenewalNode interface {
	Info() (nodeidentity.Info, error)
	SignReplicaRenewalRequest(lease.Renewal, []authority.SignedGrant) (lease.SignedRenewalRequest, error)
	SignReplicaRenewalReceipt(lease.SignedRenewalRequest) (lease.SignedRenewal, error)
	peerapi.PeerCommandSigner
}

type replicaRenewalPeers interface {
	List() []peerapi.SessionInfo
	Connect(context.Context, string, string) error
	RenewReplicaLease(context.Context, string, peerapi.RequestScope, *lease.Store, peerapi.ReplicaRenewalSigner, string, string, string) (lease.Lease, error)
}

type replicaRenewalReconciler struct {
	leases   replicaRenewalLeaseStore
	identity interface {
		Snapshot() (globalidentity.Snapshot, bool, error)
	}
	resolve authority.AuthorityIdentityResolver
	owners  interface {
		Snapshot(string, authority.AuthorityIdentityResolver, time.Time) (authority.AuthoritySnapshot, error)
		AuthorizeChain(string, []authority.SignedGrant, authority.AuthorityIdentityResolver, time.Time, authority.Principal, string, string, string) error
	}
	node       replicaRenewalNode
	peers      replicaRenewalPeers
	store      *lease.Store
	maxPerPass int

	mu       sync.Mutex
	cursorID string
}

// Reconcile begins owner-authorized renewals on source leases and completes
// pending renewals on provider leases. RenewalPolicy is a consent boundary:
// automatic renewal is limited to the explicit owner_approval profile.
func (r *replicaRenewalReconciler) Reconcile(ctx context.Context, at time.Time) error {
	if r == nil || ctx == nil || ctx.Err() != nil || at.IsZero() || r.leases == nil || r.identity == nil ||
		r.resolve == nil || r.owners == nil || r.node == nil || r.peers == nil || r.store == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := at.UTC().Truncate(time.Second)
	localIdentity, initialized, err := r.identity.Snapshot()
	if err != nil || !initialized || localIdentity.IdentityID == "" || localIdentity.Frozen {
		return errors.New("replica renewal identity is unavailable")
	}
	identityState, err := r.resolve(localIdentity.IdentityID, "", now)
	if err != nil || !replicaOfferIdentityUsable(identityState, localIdentity.IdentityID, now) {
		return errors.New("replica renewal identity is stale or frozen")
	}
	info, err := r.node.Info()
	if err != nil || info.NodeID == "" {
		return errors.New("replica renewal Node identity is unavailable")
	}
	localPublic, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
	localID, idErr := identityprotocol.NodeID(localPublic)
	if err != nil || idErr != nil || localID != info.NodeID ||
		!equalPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+info.NodeID], localPublic) {
		return errors.New("replica renewal Node is not currently delegated")
	}
	items, tombstones, err := r.leases.Snapshot(ctx)
	if err != nil {
		return errors.New("replica renewal lease state is unavailable")
	}
	blocked := make(map[string]struct{}, len(tombstones))
	for _, item := range tombstones {
		blocked[item.Signed.Tombstone.ArchiveID] = struct{}{}
	}
	eligible := make([]lease.Lease, 0)
	for _, item := range items {
		offer := item.Offer.Offer
		if !replicaRenewalEligibleForNode(item, info.NodeID, now) {
			continue
		}
		if _, tombstoned := blocked[offer.ArchiveID]; tombstoned {
			continue
		}
		if item.Offer.Actor == (authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID}) ||
			offer.ProviderNodeID == info.NodeID {
			eligible = append(eligible, item)
		}
	}
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].Offer.Offer.LeaseID < eligible[j].Offer.Offer.LeaseID
	})
	if len(eligible) == 0 {
		r.cursorID = ""
		return nil
	}
	start := sort.Search(len(eligible), func(i int) bool { return eligible[i].Offer.Offer.LeaseID > r.cursorID })
	if start == len(eligible) {
		start = 0
	}
	limit := r.maxPerPass
	if limit < 1 || limit > maxReplicaRenewalsPass {
		limit = maxReplicaRenewalsPass
	}
	if limit > len(eligible) {
		limit = len(eligible)
	}
	var firstErr error
	for offset := 0; offset < limit; offset++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := eligible[(start+offset)%len(eligible)]
		var renewErr error
		if item.Offer.Offer.ProviderNodeID == info.NodeID {
			renewErr = r.completeProviderRenewal(ctx, now, localIdentity.IdentityID, identityState, info.NodeID, item)
		} else {
			renewErr = r.beginSourceRenewal(ctx, now, localIdentity.IdentityID, info.NodeID, item)
		}
		if renewErr != nil && firstErr == nil {
			firstErr = renewErr
		}
		r.cursorID = item.Offer.Offer.LeaseID
	}
	if len(eligible) > limit && firstErr == nil {
		return errors.New("replica renewals remain pending")
	}
	return firstErr
}

func replicaRenewalDue(item lease.Lease, now time.Time) bool {
	expires := replicaLeaseEffectiveExpiry(item)
	return !expires.IsZero() && expires.After(now) && !expires.After(now.Add(replicaRenewalLeadTime))
}

func replicaRenewalEligibleForNode(item lease.Lease, localNodeID string, now time.Time) bool {
	if localNodeID == "" || item.Offer.Offer.RenewalPolicy != "owner_approval" {
		return false
	}
	offer := item.Offer.Offer
	if offer.ProviderNodeID == localNodeID {
		if item.Status == lease.StatusRenewing && item.PendingRenewal != nil {
			return true
		}
		return item.Status == lease.StatusActive && replicaRenewalDue(item, now)
	}
	return item.Offer.Actor == (authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}) &&
		item.Status == lease.StatusActive && replicaRenewalDue(item, now)
}

func (r *replicaRenewalReconciler) beginSourceRenewal(ctx context.Context, now time.Time, identityID, nodeID string, item lease.Lease) error {
	offer := item.Offer.Offer
	owner, err := r.owners.Snapshot(offer.OwnerSubjectID, r.resolve, now)
	if err != nil || owner.Frozen || owner.Subject.ID != offer.OwnerSubjectID ||
		owner.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}) ||
		!containsArchiveID(owner.ArchiveIDs, offer.ArchiveID) || !authority.ValidReplicationPolicy(owner.ReplicationPolicy) ||
		owner.ReplicationPolicy.MinimumReplicas == "0" {
		return nil
	}
	if _, rekeyPending := owner.RekeyRequiredArchives[offer.ArchiveID]; rekeyPending {
		return nil
	}
	chain := replicaRenewalGrant(owner, nodeID, offer.ArchiveID, offer.SnapshotID,
		func(candidate []authority.SignedGrant) error {
			return r.owners.AuthorizeChain(offer.OwnerSubjectID, candidate, r.resolve, now,
				authority.Principal{Kind: authority.PrincipalNode, ID: nodeID}, offer.ArchiveID, offer.SnapshotID, "archive.replicate")
		})
	if len(chain) == 0 {
		return nil
	}
	expiresAt := now.Add(automaticReplicaOfferTTL)
	if maximum := now.Add(lease.MaxLeaseDuration); expiresAt.After(maximum) {
		expiresAt = maximum
	}
	for _, signed := range chain {
		grantExpiry := parseReplicaExpiry(signed.Grant.ExpiresAt)
		if !grantExpiry.IsZero() && grantExpiry.Before(expiresAt) {
			expiresAt = grantExpiry
		}
	}
	if !expiresAt.After(replicaLeaseEffectiveExpiry(item)) {
		return nil
	}
	renewal := lease.Renewal{
		Version: lease.ProtocolVersion, LeaseID: offer.LeaseID, OfferHash: item.OfferHash,
		OwnerSubjectID: offer.OwnerSubjectID, IssuedAt: now.Format(time.RFC3339), ExpiresAt: expiresAt.Format(time.RFC3339),
	}
	request, err := r.node.SignReplicaRenewalRequest(renewal, chain)
	if err != nil {
		return errors.New("replica renewal request signing failed")
	}
	_, err = r.leases.BeginRenewal(ctx, request)
	if errors.Is(err, lease.ErrConflict) {
		return nil
	}
	if err != nil {
		return errors.New("replica renewal request persistence failed")
	}
	return nil
}

func replicaRenewalGrant(owner authority.AuthoritySnapshot, nodeID, archiveID, snapshotID string,
	authorize func([]authority.SignedGrant) error) []authority.SignedGrant {
	keys := make([]string, 0, len(owner.Grants))
	for key := range owner.Grants {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		chain := owner.Grants[key]
		if replicaGrantTargets(chain, authority.Principal{Kind: authority.PrincipalNode, ID: nodeID}, archiveID, snapshotID) && authorize(chain) == nil {
			return chain
		}
	}
	return nil
}

func (r *replicaRenewalReconciler) completeProviderRenewal(ctx context.Context, now time.Time, identityID string,
	localState authority.IdentityState, localNodeID string, item lease.Lease) error {
	offer := item.Offer.Offer
	owner, err := r.owners.Snapshot(offer.OwnerSubjectID, r.resolve, now)
	if err != nil || owner.Frozen || owner.Subject.ID != offer.OwnerSubjectID ||
		!containsArchiveID(owner.ArchiveIDs, offer.ArchiveID) {
		return nil
	}
	sourceIdentityID := owner.Subject.Controller.ID
	if owner.Subject.Controller.Kind != authority.PrincipalIdentity || sourceIdentityID == "" {
		return nil
	}
	sourceState, err := r.resolve(sourceIdentityID, "", now)
	if err != nil || !replicaOfferIdentityUsable(sourceState, sourceIdentityID, now) {
		return nil
	}
	sourceKey := sourceState.CurrentKeys[authority.PrincipalNode+":"+offerActorID(item)]
	if len(sourceKey) == 0 {
		return nil
	}
	derivedSource, err := identityprotocol.NodeID(sourceKey)
	if err != nil || derivedSource != offerActorID(item) {
		return nil
	}
	if !replicaOfferIdentityUsable(localState, identityID, now) {
		return nil
	}
	ownerGrant := replicaRenewalGrant(owner, localNodeID, offer.ArchiveID, offer.SnapshotID,
		func(chain []authority.SignedGrant) error {
			return r.owners.AuthorizeChain(offer.OwnerSubjectID, chain, r.resolve, now,
				authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}, offer.ArchiveID, offer.SnapshotID, "archive.replicate")
		})
	if len(ownerGrant) == 0 {
		return nil
	}
	sourceNodeID := offerActorID(item)
	if !peerSessionConnected(r.peers.List(), sourceIdentityID, sourceNodeID) {
		connectCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		connectErr := r.peers.Connect(connectCtx, sourceIdentityID, sourceNodeID)
		cancel()
		if connectErr != nil && !peerSessionConnected(r.peers.List(), sourceIdentityID, sourceNodeID) {
			return errors.New("replica renewal source is unavailable")
		}
	}
	scope := peerapi.RequestScope{
		IdentityID: identityID, IdentityHead: localState.HeadHash, NodeID: localNodeID,
		TargetIdentityID: sourceIdentityID, TargetNodeID: sourceNodeID,
		OwnerSubjectID: offer.OwnerSubjectID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
		Action: "archive.replicate", GrantChain: ownerGrant,
		Resolve: peerprotocol.IdentityResolver(r.resolve), Signer: r.node,
	}
	callCtx, cancel := context.WithTimeout(ctx, replicaRenewalTimeout)
	defer cancel()
	_, err = r.peers.RenewReplicaLease(callCtx, sourceNodeID, scope, r.store, r.node, offer.ArchiveID, offer.SnapshotID, offer.LeaseID)
	if err != nil {
		var remote peerapi.RemoteError
		if errors.As(err, &remote) && remote.Code == "not_found" {
			return nil // Source has not durably started its renewal request yet.
		}
		return errors.New("replica renewal exchange failed")
	}
	return nil
}

func offerActorID(item lease.Lease) string { return item.Offer.Actor.ID }
