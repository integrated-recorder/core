package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
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
	maxReplicaIntegrityChecksPerPass = 1
	replicaIntegrityCheckInterval    = 24 * time.Hour
	replicaIntegrityRetryInterval    = 5 * time.Minute
	replicaIntegrityCheckTimeout     = 30 * time.Minute
)

type replicaIntegrityLeaseStore interface {
	Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error)
	FailReadbackMismatch(context.Context, string, string, string, time.Time) (lease.Lease, error)
}

type replicaIntegrityPeerSessions interface {
	List() []peerapi.SessionInfo
	OpenCiphertextRange(context.Context, string, peerapi.RequestScope, string, string, uint64, int64, int64) (io.ReadCloser, error)
}

type replicaIntegrityCheck struct {
	offerHash  string
	attempted  time.Time
	retryAfter time.Duration
}

// replicaIntegrityReconciler performs bounded, source-initiated readback of
// active remote replicas. Only a complete object read whose bytes disagree
// with the immutable signed offer changes source lease state. Transport,
// authorization, and availability errors are observations of uncertainty and
// never count as corruption.
type replicaIntegrityReconciler struct {
	leases   replicaIntegrityLeaseStore
	identity interface {
		Snapshot() (globalidentity.Snapshot, bool, error)
	}
	resolve authority.AuthorityIdentityResolver
	owners  interface {
		Snapshot(string, authority.AuthorityIdentityResolver, time.Time) (authority.AuthoritySnapshot, error)
		AuthorizeChain(string, []authority.SignedGrant, authority.AuthorityIdentityResolver, time.Time, authority.Principal, string, string, string) error
	}
	node interface {
		Info() (nodeidentity.Info, error)
		peerapi.PeerCommandSigner
	}
	peers      replicaIntegrityPeerSessions
	maxPerPass int
	interval   time.Duration
	clock      func() time.Time

	mu       sync.Mutex
	last     map[string]replicaIntegrityCheck
	cursorID string
}

type replicaIntegrityReconcileError struct{ stage string }

func (e replicaIntegrityReconcileError) Error() string {
	return "replica integrity reconciliation failed"
}
func (e replicaIntegrityReconcileError) DiagnosticStage() string {
	switch e.stage {
	case "identity", "node", "lease_state", "authority", "readback", "failure_persist":
		return e.stage
	default:
		return "unknown"
	}
}

func (r *replicaIntegrityReconciler) Reconcile(ctx context.Context, at time.Time) error {
	if r == nil || ctx == nil || ctx.Err() != nil || at.IsZero() || r.leases == nil || r.identity == nil ||
		r.resolve == nil || r.owners == nil || r.node == nil || r.peers == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := at.UTC().Truncate(time.Second)
	identity, initialized, err := r.identity.Snapshot()
	if err != nil || !initialized || identity.IdentityID == "" || identity.Frozen {
		return replicaIntegrityReconcileError{stage: "identity"}
	}
	identityState, err := r.resolve(identity.IdentityID, "", now)
	if err != nil || !replicaOfferIdentityUsable(identityState, identity.IdentityID, now) {
		return replicaIntegrityReconcileError{stage: "identity"}
	}
	info, err := r.node.Info()
	if err != nil || info.NodeID == "" {
		return replicaIntegrityReconcileError{stage: "node"}
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
	derivedNodeID, deriveErr := identityprotocol.NodeID(publicKey)
	if err != nil || deriveErr != nil || derivedNodeID != info.NodeID ||
		!equalPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+info.NodeID], publicKey) {
		return replicaIntegrityReconcileError{stage: "node"}
	}
	items, tombstones, err := r.leases.Snapshot(ctx)
	if err != nil {
		return replicaIntegrityReconcileError{stage: "lease_state"}
	}
	blocked := make(map[string]struct{}, len(tombstones))
	for _, item := range tombstones {
		if item.Signed.Tombstone.ArchiveID != "" {
			blocked[item.Signed.Tombstone.ArchiveID] = struct{}{}
		}
	}
	interval := r.interval
	if interval <= 0 {
		interval = replicaIntegrityCheckInterval
	}
	limit := r.maxPerPass
	if limit <= 0 || limit > maxReplicaIntegrityChecksPerPass {
		limit = maxReplicaIntegrityChecksPerPass
	}
	if r.last == nil {
		r.last = make(map[string]replicaIntegrityCheck)
	}
	clock := r.clock
	if clock == nil {
		clock = time.Now
	}
	sessions := r.peers.List()
	candidates := make([]lease.Lease, 0)
	currentIDs := make(map[string]struct{})
	for _, item := range items {
		offer := item.Offer.Offer
		if item.Status == lease.StatusActive && item.Offer.Actor == (authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID}) &&
			offer.ProviderNodeID != "" && offer.ProviderNodeID != info.NodeID {
			currentIDs[offer.LeaseID] = struct{}{}
			candidates = append(candidates, item)
		}
	}
	for leaseID := range r.last {
		if _, exists := currentIDs[leaseID]; !exists {
			delete(r.last, leaseID)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Offer.Offer.LeaseID < candidates[j].Offer.Offer.LeaseID
	})
	checked := 0
	var firstErr error
	start := sort.Search(len(candidates), func(i int) bool { return candidates[i].Offer.Offer.LeaseID > r.cursorID })
	if start == len(candidates) {
		start = 0
	}
	for offset := 0; offset < len(candidates); offset++ {
		item := candidates[(start+offset)%len(candidates)]
		if ctx.Err() != nil {
			return ctx.Err()
		}
		offer := item.Offer.Offer
		if !replicaLeaseCommittedExpiry(item).After(now) {
			continue
		}
		if _, tombstoned := blocked[offer.ArchiveID]; tombstoned {
			continue
		}
		if previous, ok := r.last[offer.LeaseID]; ok && previous.offerHash == item.OfferHash && now.Before(previous.attempted.Add(previous.retryAfter)) {
			continue
		}
		owner, ownerErr := r.owners.Snapshot(offer.OwnerSubjectID, r.resolve, now)
		if ownerErr != nil || owner.Frozen || owner.Subject.ID != offer.OwnerSubjectID ||
			owner.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: identity.IdentityID}) ||
			!containsArchiveID(owner.ArchiveIDs, offer.ArchiveID) || len(item.Offer.GrantChain) == 0 ||
			r.owners.AuthorizeChain(offer.OwnerSubjectID, item.Offer.GrantChain, r.resolve, now,
				authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID}, offer.ArchiveID, offer.SnapshotID, "archive.replicate") != nil {
			continue
		}
		session := replicaIntegritySession(sessions, offer.ProviderNodeID, r.resolve, now)
		if session == nil {
			continue
		}
		if !peerSessionConnected(sessions, session.IdentityID, session.NodeID) {
			continue
		}
		r.last[offer.LeaseID] = replicaIntegrityCheck{offerHash: item.OfferHash, attempted: now, retryAfter: replicaIntegrityRetryInterval}
		r.cursorID = offer.LeaseID
		checkCtx, cancel := context.WithTimeout(ctx, replicaIntegrityCheckTimeout)
		matches, checkErr := verifyRemoteReplicaObjects(checkCtx, r.peers, session, r.resolve, r.node, identity.IdentityID,
			identityState.HeadHash, info.NodeID, offer, item.Offer.GrantChain, clock)
		checked++
		if checkErr != nil {
			// A failed or incomplete readback is not evidence that bytes are bad.
			if firstErr == nil {
				firstErr = replicaIntegrityReconcileError{stage: "readback"}
			}
		} else if !matches {
			// A content-addressed object is published atomically, but a large
			// read spans multiple range requests. A second complete read reduces
			// false positives during replacement, but is not a stable-inode proof.
			rechecked, recheckErr := verifyRemoteReplicaObjects(checkCtx, r.peers, session, r.resolve, r.node, identity.IdentityID,
				identityState.HeadHash, info.NodeID, offer, item.Offer.GrantChain, clock)
			if recheckErr != nil {
				if firstErr == nil {
					firstErr = replicaIntegrityReconcileError{stage: "readback"}
				}
			} else if !rechecked {
				if _, failErr := r.leases.FailReadbackMismatch(ctx, offer.LeaseID, item.OfferHash, item.Revision, now); failErr != nil &&
					!errors.Is(failErr, lease.ErrConflict) && !errors.Is(failErr, lease.ErrTransition) && firstErr == nil {
					firstErr = replicaIntegrityReconcileError{stage: "failure_persist"}
				}
			} else {
				// A second complete, matching read indicates the first mismatch may
				// have crossed an atomic replacement. Do not fail the lease; schedule
				// the next routine confirmation on the normal interval.
				r.last[offer.LeaseID] = replicaIntegrityCheck{offerHash: item.OfferHash, attempted: now, retryAfter: interval}
			}
		} else {
			r.last[offer.LeaseID] = replicaIntegrityCheck{offerHash: item.OfferHash, attempted: now, retryAfter: interval}
		}
		cancel()
		if checked >= limit {
			break
		}
	}
	return firstErr
}

func replicaIntegritySession(sessions []peerapi.SessionInfo, providerNodeID string,
	resolve authority.AuthorityIdentityResolver, now time.Time) *peerapi.SessionInfo {
	var selected *peerapi.SessionInfo
	for _, session := range sessions {
		if session.NodeID != providerNodeID || session.IdentityID == "" {
			continue
		}
		state, err := resolve(session.IdentityID, "", now)
		if err != nil || !replicaOfferIdentityUsable(state, session.IdentityID, now) {
			continue
		}
		publicKey := state.CurrentKeys[authority.PrincipalNode+":"+providerNodeID]
		derived, keyErr := identityprotocol.NodeID(publicKey)
		if keyErr != nil || derived != providerNodeID {
			continue
		}
		if selected != nil && selected.IdentityID != session.IdentityID {
			return nil
		}
		copy := session
		selected = &copy
	}
	return selected
}

func verifyRemoteReplicaObjects(ctx context.Context, peers replicaIntegrityPeerSessions, session *peerapi.SessionInfo,
	resolve authority.AuthorityIdentityResolver, signer peerapi.PeerCommandSigner, identityID, identityHead, sourceNodeID string,
	offer lease.Offer, grantChain []authority.SignedGrant, clock func() time.Time) (bool, error) {
	if ctx == nil || peers == nil || session == nil || signer == nil || identityID == "" || identityHead == "" || sourceNodeID == "" ||
		offer.ProviderNodeID != session.NodeID || offer.ArchiveID == "" || offer.SnapshotID == "" || len(offer.EncryptedObjectSet) == 0 || clock == nil {
		return false, errors.New("replica readback scope is invalid")
	}
	keyEpoch, err := strconv.ParseUint(offer.KeyEpoch, 10, 64)
	if err != nil || keyEpoch == 0 {
		return false, errors.New("replica readback epoch is invalid")
	}
	scope := peerapi.RequestScope{
		IdentityID: identityID, IdentityHead: identityHead, NodeID: sourceNodeID,
		TargetIdentityID: session.IdentityID, TargetNodeID: session.NodeID,
		OwnerSubjectID: offer.OwnerSubjectID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
		Action: "archive.replicate", GrantChain: append([]authority.SignedGrant(nil), grantChain...),
		Resolve: peerprotocol.IdentityResolver(resolve), Signer: signer,
		Now: func() time.Time { return clock().UTC() },
	}
	for _, object := range offer.EncryptedObjectSet {
		size, parseErr := strconv.ParseInt(object.CiphertextSize, 10, 64)
		if parseErr != nil || size <= 0 || size > lease.MaxCiphertextObjectBytes || !strings.HasPrefix(object.CiphertextSHA256, "sha256:") {
			return false, errors.New("replica object descriptor is invalid")
		}
		expectedDigest, decodeErr := hex.DecodeString(strings.TrimPrefix(object.CiphertextSHA256, "sha256:"))
		if decodeErr != nil || len(expectedDigest) != sha256.Size {
			return false, errors.New("replica object digest is invalid")
		}
		hasher := sha256.New()
		for offset := int64(0); offset < size; {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			length := int64(replication.MaxCiphertextChunkBytes)
			if remaining := size - offset; remaining < length {
				length = remaining
			}
			reader, openErr := peers.OpenCiphertextRange(ctx, session.NodeID, scope, offer.SnapshotID, object.ObjectID,
				keyEpoch, offset, length)
			if openErr != nil {
				return false, openErr
			}
			written, copyErr := io.CopyN(hasher, reader, length)
			closeErr := reader.Close()
			if copyErr != nil || closeErr != nil || written != length {
				return false, errors.New("replica object readback incomplete")
			}
			offset += length
		}
		if !equalDigest(hasher.Sum(nil), expectedDigest) {
			return false, nil
		}
	}
	return true, nil
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var diff byte
	for i := range left {
		diff |= left[i] ^ right[i]
	}
	return diff == 0
}
