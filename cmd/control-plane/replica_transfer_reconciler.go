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
	"github.com/integrated-recorder/core/internal/replication"
)

const (
	maxReplicaTransferAttemptsPerPass = 2
	replicaTransferTimeout            = 15 * time.Minute
)

type replicaTransferLeaseReader interface {
	Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error)
}

type replicaTransferPeerSessions interface {
	List() []peerapi.SessionInfo
	Connect(context.Context, string, string) error
}

type replicaTransferReconciler struct {
	leases   replicaTransferLeaseReader
	identity interface {
		Snapshot() (globalidentity.Snapshot, bool, error)
	}
	resolve       authority.AuthorityIdentityResolver
	ownerSnapshot func(string, time.Time) (authority.AuthoritySnapshot, error)
	authorize     func(string, []authority.SignedGrant, time.Time, authority.Principal, string, string, string) error
	node          interface {
		Info() (nodeidentity.Info, error)
	}
	signer        peerapi.PeerCommandSigner
	peers         replicaTransferPeerSessions
	importGate    *replication.ImportGate
	importReplica func(context.Context, string, peerapi.RequestScope, string, string, string) (lease.Lease, error)

	mu          sync.Mutex
	nextLeaseID string
	maxPerPass  int
}

type replicaTransferCandidate struct {
	lease            lease.Lease
	sourceIdentityID string
}

// Reconcile resumes only durable leases that were already accepted by this
// Node. Offered work is never accepted here: a local operator's explicit
// acceptance remains the storage-consent boundary. Transfer checkpoints and
// source readback remain owned by the existing peer API and lease stores.
func (r *replicaTransferReconciler) Reconcile(ctx context.Context, now time.Time) error {
	if r == nil || ctx == nil || ctx.Err() != nil || now.IsZero() || r.leases == nil || r.identity == nil ||
		r.resolve == nil || r.ownerSnapshot == nil || r.authorize == nil || r.node == nil || r.peers == nil ||
		r.signer == nil || r.importGate == nil || r.importReplica == nil {
		return nil
	}
	localIdentity, initialized, err := r.identity.Snapshot()
	if err != nil || !initialized || localIdentity.IdentityID == "" || localIdentity.Frozen {
		return errors.New("replica transfer identity is unavailable")
	}
	identityState, err := r.resolve(localIdentity.IdentityID, "", now)
	if err != nil || identityState.IdentityID != localIdentity.IdentityID || identityState.Frozen ||
		identityState.HeadHash == "" || identityState.VerifiedAt.IsZero() || !identityState.FreshUntil.After(now) {
		return errors.New("replica transfer identity is stale or frozen")
	}
	nodeInfo, err := r.node.Info()
	if err != nil || nodeInfo.NodeID == "" {
		return errors.New("replica transfer Node identity is unavailable")
	}
	localPublic, err := base64.RawURLEncoding.DecodeString(nodeInfo.PublicKey)
	derivedLocalNode, deriveErr := identityprotocol.NodeID(localPublic)
	if err != nil || deriveErr != nil || derivedLocalNode != nodeInfo.NodeID ||
		!equalPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+nodeInfo.NodeID], localPublic) {
		return errors.New("replica transfer Node is not currently delegated")
	}

	leases, tombstones, err := r.leases.Snapshot(ctx)
	if err != nil {
		return errors.New("replica transfer lease state is unavailable")
	}
	blockedArchives := make(map[string]struct{}, len(tombstones))
	for _, item := range tombstones {
		if archiveID := item.Signed.Tombstone.ArchiveID; archiveID != "" {
			blockedArchives[archiveID] = struct{}{}
		}
	}
	activeOffers := make(map[string]string)
	for _, current := range leases {
		if current.Status == lease.StatusActive && current.OfferHash != "" {
			// Source verification survives restarts only while a provider-receipted
			// renewal (or the original Offer) is committed. A pending renewal is
			// not durable proof that the other side accepted the extended lease.
			if _, tombstoned := blockedArchives[current.Offer.Offer.ArchiveID]; !tombstoned &&
				replicaLeaseCommittedExpiry(current).After(now) {
				activeOffers[current.Offer.Offer.LeaseID] = current.OfferHash
			}
		}
	}
	r.importGate.RetainSourceVerified(activeOffers)
	candidates := make([]replicaTransferCandidate, 0)
	for _, current := range leases {
		offer := current.Offer
		if current.Status == lease.StatusActive && r.importGate.SourceVerified(offer.Offer.LeaseID, current.OfferHash) {
			continue
		}
		if current.Status != lease.StatusAccepted && current.Status != lease.StatusTransferring &&
			current.Status != lease.StatusVerifying && current.Status != lease.StatusActive {
			continue
		}
		if offer.Actor.Kind != authority.PrincipalNode || offer.Actor.ID == "" || offer.Actor.ID == nodeInfo.NodeID ||
			offer.Offer.ProviderNodeID != nodeInfo.NodeID || !replicaLeaseCommittedExpiry(current).After(now) {
			continue
		}
		if _, blocked := blockedArchives[offer.Offer.ArchiveID]; blocked {
			continue
		}
		owner, ownerErr := r.ownerSnapshot(offer.Offer.OwnerSubjectID, now)
		if ownerErr != nil || owner.Frozen || owner.Subject.ID != offer.Offer.OwnerSubjectID ||
			owner.Subject.Controller.Kind != authority.PrincipalIdentity || owner.Subject.Controller.ID == "" ||
			!containsArchiveID(owner.ArchiveIDs, offer.Offer.ArchiveID) {
			continue
		}
		sourceState, resolveErr := r.resolve(owner.Subject.Controller.ID, "", now)
		if resolveErr != nil || !replicaTransferIdentityUsable(sourceState, owner.Subject.Controller.ID, now) {
			continue
		}
		if sourceKey := sourceState.CurrentKeys[authority.PrincipalNode+":"+offer.Actor.ID]; len(sourceKey) == 0 {
			continue
		} else if sourceID, keyErr := identityprotocol.NodeID(sourceKey); keyErr != nil || sourceID != offer.Actor.ID {
			continue
		}
		candidates = append(candidates, replicaTransferCandidate{lease: current, sourceIdentityID: owner.Subject.Controller.ID})
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lease.Offer.Offer.LeaseID < candidates[j].lease.Offer.Offer.LeaseID
	})
	start := r.startIndex(candidates)
	maxPerPass := r.maxPerPass
	if maxPerPass < 1 || maxPerPass > maxReplicaTransferAttemptsPerPass {
		maxPerPass = maxReplicaTransferAttemptsPerPass
	}
	limit := len(candidates)
	if limit > maxPerPass {
		limit = maxPerPass
	}
	pending := false
	for offset := 0; offset < limit; offset++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidate := candidates[(start+offset)%len(candidates)]
		current := candidate.lease
		if err := r.resumeOne(ctx, now, localIdentity.IdentityID, identityState, nodeInfo.NodeID, current, candidate.sourceIdentityID); err != nil {
			pending = true
		} else {
			r.importGate.MarkSourceVerified(current.Offer.Offer.LeaseID, current.OfferHash)
		}
		r.setNextLeaseID(current.Offer.Offer.LeaseID)
	}
	if pending || len(candidates) > limit {
		return errors.New("replica transfers remain pending")
	}
	return nil
}

func (r *replicaTransferReconciler) resumeOne(
	ctx context.Context,
	now time.Time,
	identityID string,
	identityState authority.IdentityState,
	localNodeID string,
	current lease.Lease,
	sourceIdentityID string,
) error {
	offer := current.Offer.Offer
	owner, err := r.ownerSnapshot(offer.OwnerSubjectID, now)
	if err != nil || owner.Frozen || owner.Subject.ID != offer.OwnerSubjectID ||
		owner.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: sourceIdentityID}) ||
		!containsArchiveID(owner.ArchiveIDs, offer.ArchiveID) {
		return errors.New("replica transfer OwnerSubject is unavailable")
	}
	sourceState, err := r.resolve(sourceIdentityID, "", now)
	if !replicaTransferIdentityUsable(sourceState, sourceIdentityID, now) {
		return errors.New("replica source identity is stale or frozen")
	}
	sourceKey := sourceState.CurrentKeys[authority.PrincipalNode+":"+current.Offer.Actor.ID]
	if len(sourceKey) == 0 {
		return errors.New("replica source Node is no longer delegated")
	}
	if derived, deriveErr := identityprotocol.NodeID(sourceKey); deriveErr != nil || derived != current.Offer.Actor.ID {
		return errors.New("replica source Node identity is invalid")
	}
	audience := authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}
	grantHashes := make([]string, 0, len(owner.Grants))
	for hash := range owner.Grants {
		grantHashes = append(grantHashes, hash)
	}
	sort.Strings(grantHashes)
	var chain []authority.SignedGrant
	for _, hash := range grantHashes {
		candidate := owner.Grants[hash]
		if !replicaTransferGrantCouldMatch(candidate, offer, audience) {
			continue
		}
		if r.authorize(offer.OwnerSubjectID, candidate, now, audience, offer.ArchiveID, offer.SnapshotID, "archive.replicate") == nil {
			chain = candidate
			break
		}
	}
	if len(chain) == 0 {
		return errors.New("replica transfer has no current local capability")
	}
	sourceNodeID := current.Offer.Actor.ID
	if !peerSessionConnected(r.peers.List(), sourceIdentityID, sourceNodeID) {
		connectCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		connectErr := r.peers.Connect(connectCtx, sourceIdentityID, sourceNodeID)
		cancel()
		if connectErr != nil && !peerSessionConnected(r.peers.List(), sourceIdentityID, sourceNodeID) {
			return errors.New("replica source peer is unavailable")
		}
	}
	scope := peerapi.RequestScope{
		IdentityID: identityID, IdentityHead: identityState.HeadHash, NodeID: localNodeID,
		TargetIdentityID: sourceIdentityID, TargetNodeID: sourceNodeID,
		OwnerSubjectID: offer.OwnerSubjectID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
		Action: "archive.replicate", GrantChain: chain,
		Resolve: peerprotocol.IdentityResolver(r.resolve), Signer: r.signer,
	}
	importCtx, cancel := context.WithTimeout(ctx, replicaTransferTimeout)
	defer cancel()
	release, err := r.importGate.Acquire(importCtx, offer.LeaseID)
	if err != nil {
		return err
	}
	defer release()
	if current.Status == lease.StatusActive && r.importGate.SourceVerified(offer.LeaseID, current.OfferHash) {
		return nil
	}
	result, err := r.importReplica(importCtx, sourceNodeID, scope, offer.ArchiveID, offer.SnapshotID, offer.LeaseID)
	if err != nil {
		return err
	}
	resultOffer := result.Offer.Offer
	if result.Status != lease.StatusActive || result.OfferHash == "" || result.OfferHash != current.OfferHash ||
		resultOffer.LeaseID != offer.LeaseID || resultOffer.ArchiveID != offer.ArchiveID ||
		resultOffer.SnapshotID != offer.SnapshotID || resultOffer.ProviderNodeID != localNodeID {
		return errors.New("replica transfer did not reach verified active state")
	}
	return nil
}

func replicaTransferIdentityUsable(state authority.IdentityState, identityID string, now time.Time) bool {
	return identityID != "" && state.IdentityID == identityID && !state.Frozen && state.HeadHash != "" &&
		!state.VerifiedAt.IsZero() && state.FreshUntil.After(now)
}

func replicaTransferGrantCouldMatch(chain []authority.SignedGrant, offer lease.Offer, audience authority.Principal) bool {
	if len(chain) == 0 {
		return false
	}
	leaf := chain[len(chain)-1].Grant
	if leaf.SubjectID != offer.OwnerSubjectID || leaf.Audience != audience || leaf.ArchiveID != offer.ArchiveID ||
		(leaf.SnapshotID != "" && leaf.SnapshotID != offer.SnapshotID) {
		return false
	}
	for _, action := range leaf.Actions {
		if action == "archive.replicate" {
			return true
		}
	}
	return false
}

func parseReplicaExpiry(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339, value)
	return parsed
}

func (r *replicaTransferReconciler) startIndex(candidates []replicaTransferCandidate) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextLeaseID == "" {
		return 0
	}
	for i, candidate := range candidates {
		if candidate.lease.Offer.Offer.LeaseID > r.nextLeaseID {
			return i
		}
	}
	return 0
}

func (r *replicaTransferReconciler) setNextLeaseID(leaseID string) {
	r.mu.Lock()
	r.nextLeaseID = leaseID
	r.mu.Unlock()
}
