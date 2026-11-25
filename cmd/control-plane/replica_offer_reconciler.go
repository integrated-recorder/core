package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/replication"
)

const (
	automaticReplicaOfferTTL      = 7 * 24 * time.Hour
	maxAutomaticReplicaOffersPass = 2
	maxAutomaticReplicaProviders  = 128
	maxReplicaOfferIdentityAge    = 5 * time.Minute
)

type replicaOfferReconcileError struct{ stage string }

func (e replicaOfferReconcileError) Error() string { return "replica offer reconciliation failed" }

// DiagnosticStage is a bounded internal label. It intentionally excludes raw
// child, storage, filesystem, and authority errors.
func (e replicaOfferReconcileError) DiagnosticStage() string {
	switch e.stage {
	case "snapshot_lookup", "policy_plan", "snapshot_encryption", "manifest_validation", "lease_id", "offer_build", "offer_sign", "lease_persist":
		return e.stage
	default:
		return "unknown"
	}
}

type replicaOfferLeaseStore interface {
	Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error)
	IssueOffer(context.Context, lease.SignedOffer) (lease.Lease, error)
}

type replicaOfferAuthorityStore interface {
	List(authority.AuthorityIdentityResolver, time.Time) ([]authority.AuthoritySnapshot, error)
	AuthorizeChain(string, []authority.SignedGrant, authority.AuthorityIdentityResolver, time.Time, authority.Principal, string, string, string) error
}

type replicaOfferIdentityStore interface {
	Snapshot() (globalidentity.Snapshot, bool, error)
}

type replicaOfferNodeStore interface {
	Info() (nodeidentity.Info, error)
	SignReplicaOffer(lease.Offer, []authority.SignedGrant) (lease.SignedOffer, error)
}

type replicaOfferPeerSessions interface {
	List() []peerapi.SessionInfo
}

type replicaOfferArchiveCatalog interface {
	Head(context.Context, string) (archivesnapshot.Snapshot, error)
}

type replicaOfferCiphertextStore interface {
	KeyEpochs(archivesnapshot.Snapshot) ([]uint64, error)
	LoadKeyEnvelope(archivesnapshot.Snapshot, uint64, string) (cryptoarchive.KeyEnvelope, error)
}

type replicaOfferManager interface {
	List() []*domain.Recording
	EncryptArchiveSnapshot(context.Context, recorderengine.EncryptArchiveSnapshotRequest) (replication.ReplicaManifest, error)
}

// replicaOfferReconciler applies an OwnerSubject's minimum-replica target by
// creating bounded, signed offers for already encrypted or locally prepared
// sealed snapshots. It never accepts an offer or begins a transfer: the
// provider must explicitly accept the storage obligation.
//
// Initial encryption still requires an Owner-signed recovery envelope and a
// locally protected DEK prepared through the existing explicit flow. The
// reconciler will not generate or replace either authority artifact.
type replicaOfferReconciler struct {
	leases     replicaOfferLeaseStore
	identity   replicaOfferIdentityStore
	resolve    authority.AuthorityIdentityResolver
	authority  replicaOfferAuthorityStore
	node       replicaOfferNodeStore
	peers      replicaOfferPeerSessions
	catalog    replicaOfferArchiveCatalog
	ciphertext replicaOfferCiphertextStore
	manager    replicaOfferManager
	maxPerPass int

	mu sync.Mutex
}

func (r *replicaOfferReconciler) Reconcile(ctx context.Context, at time.Time) error {
	if r == nil || ctx == nil || ctx.Err() != nil || at.IsZero() || r.leases == nil || r.identity == nil ||
		r.resolve == nil || r.authority == nil || r.node == nil || r.peers == nil || r.catalog == nil ||
		r.ciphertext == nil || r.manager == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := at.UTC().Truncate(time.Second)

	localIdentity, initialized, err := r.identity.Snapshot()
	if err != nil || !initialized || localIdentity.IdentityID == "" || localIdentity.Frozen {
		return errors.New("replica offer identity is unavailable")
	}
	identityState, err := r.resolve(localIdentity.IdentityID, "", now)
	if err != nil || !replicaOfferIdentityUsable(identityState, localIdentity.IdentityID, now) {
		return errors.New("replica offer identity is stale or frozen")
	}
	localNode, err := r.node.Info()
	if err != nil || localNode.NodeID == "" {
		return errors.New("replica offer Node identity is unavailable")
	}
	localNodePublic, err := base64.RawURLEncoding.DecodeString(localNode.PublicKey)
	localNodeID, nodeIDErr := identityprotocol.NodeID(localNodePublic)
	if err != nil || nodeIDErr != nil || localNodeID != localNode.NodeID ||
		!equalPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+localNode.NodeID], localNodePublic) {
		return errors.New("replica offer Node is not currently delegated")
	}

	leaseItems, tombstones, err := r.leases.Snapshot(ctx)
	if err != nil {
		return errors.New("replica offer lease state is unavailable")
	}
	ownerSnapshots, err := r.authority.List(r.resolve, now)
	if err != nil {
		return errors.New("replica offer authority state is unavailable")
	}
	providers := automaticReplicaProviders(localNode.NodeID, identityState, r.resolve, r.peers.List(), now)
	if len(providers) == 0 {
		return nil
	}
	recordingsByArchive := make(map[string]string)
	for _, recording := range r.manager.List() {
		if recording == nil || !recording.ArchiveSealed ||
			(recording.State != domain.StateStopped && recording.State != domain.StateCompleted && recording.State != domain.StateInterrupted) {
			continue
		}
		archiveID := recording.ArchiveID
		if archiveID == "" {
			archiveID, err = archivesnapshot.ArchiveIDForRecording(recording.ID)
			if err != nil {
				continue
			}
		}
		if _, exists := recordingsByArchive[archiveID]; !exists {
			recordingsByArchive[archiveID] = recording.ID
		}
	}

	maxPerPass := r.maxPerPass
	if maxPerPass <= 0 || maxPerPass > maxAutomaticReplicaOffersPass {
		maxPerPass = maxAutomaticReplicaOffersPass
	}
	created := 0
	failed := false
	failureStage := ""
	markFailed := func(stage string) {
		if !failed {
			failed = true
			failureStage = stage
		}
	}
	for _, owner := range ownerSnapshots {
		if err := ctx.Err(); err != nil {
			return err
		}
		if owner.Frozen || owner.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: localIdentity.IdentityID}) ||
			!authority.ValidReplicationPolicy(owner.ReplicationPolicy) {
			continue
		}
		minimum, _ := strconv.ParseUint(owner.ReplicationPolicy.MinimumReplicas, 10, 64)
		if minimum == 0 {
			continue
		}
		for _, archiveID := range owner.ArchiveIDs {
			if created >= maxPerPass {
				return nil
			}
			if _, rekeyPending := owner.RekeyRequiredArchives[archiveID]; rekeyPending {
				continue
			}
			if replicaArchiveTombstoned(archiveID, tombstones) {
				continue
			}
			recordingID := recordingsByArchive[archiveID]
			if recordingID == "" {
				continue
			}
			snapshot, snapshotErr := r.catalog.Head(ctx, archiveID)
			if snapshotErr != nil || archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != archiveID ||
				snapshot.Content.Archive != archiveindex.ArchiveSealed {
				if snapshotErr != nil && !errors.Is(snapshotErr, archivecatalog.ErrNotFound) {
					markFailed("snapshot_lookup")
				}
				continue
			}

			chain := r.authorizedReplicaOfferChain(owner, now, localNode.NodeID, archiveID, snapshot.SnapshotID)
			if len(chain) == 0 {
				continue
			}
			// A connected peer is only a transport candidate. It must also be an
			// explicit audience of a current OwnerSubject archive.replicate grant
			// before the scheduler discloses even an offer for this snapshot.
			eligibleProviders := r.authorizedReplicaProviders(owner, now, archiveID, snapshot.SnapshotID, providers)
			if len(eligibleProviders) == 0 {
				continue
			}
			// Preflight with one byte prevents expensive encryption when the target
			// is already met, all providers have pending offers, or no capacity is
			// available. The exact manifest size is checked again before signing.
			if plan, planErr := planAutomaticReplicaOffers(owner.ReplicationPolicy, owner.Subject.ID, archiveID,
				snapshot.SnapshotID, localNode.NodeID, leaseItems, tombstones, eligibleProviders, now, 1, 1); planErr != nil {
				markFailed("policy_plan")
				continue
			} else if len(plan) == 0 {
				continue
			}

			epoch, envelope, ok := r.preparedRecoveryEnvelope(snapshot, identityState, localIdentity, leaseItems, owner.Subject.ID)
			if !ok {
				// A Node cannot manufacture Owner recovery authority. First-time
				// archive encryption remains an explicit Owner browser operation.
				continue
			}
			manifest, encryptErr := r.manager.EncryptArchiveSnapshot(ctx, recorderengine.EncryptArchiveSnapshotRequest{
				RecordingID: recordingID, SnapshotID: snapshot.SnapshotID, SubjectID: owner.Subject.ID,
				GrantChain: chain, KeyEpoch: epoch, UseStoredKey: true, KeyEnvelope: envelope,
			})
			if encryptErr != nil {
				markFailed("snapshot_encryption")
				continue
			}
			reservedBytes, sizeErr := replicaManifestReservedBytes(manifest)
			if sizeErr != nil {
				markFailed("manifest_validation")
				continue
			}
			plan, planErr := planAutomaticReplicaOffers(owner.ReplicationPolicy, owner.Subject.ID, archiveID,
				snapshot.SnapshotID, localNode.NodeID, leaseItems, tombstones, eligibleProviders, now,
				maxPerPass-created, reservedBytes)
			if planErr != nil {
				markFailed("policy_plan")
				continue
			}
			for _, providerID := range plan {
				if created >= maxPerPass || ctx.Err() != nil {
					break
				}
				leaseID, idErr := newAutomaticReplicaLeaseID()
				if idErr != nil {
					markFailed("lease_id")
					break
				}
				expiresAt := now.Add(automaticReplicaOfferTTL)
				if maximum := now.Add(lease.MaxLeaseDuration); expiresAt.After(maximum) {
					expiresAt = maximum
				}
				offer, offerErr := replication.BuildLeaseOffer(snapshot, manifest, replication.LeaseOfferSpec{
					LeaseID: leaseID, ProviderNodeID: providerID, OwnerSubjectID: owner.Subject.ID,
					KeyEpoch: epoch, IssuedAt: now, ExpiresAt: expiresAt,
				})
				if offerErr != nil {
					markFailed("offer_build")
					break
				}
				signed, signErr := r.node.SignReplicaOffer(offer, chain)
				if signErr != nil {
					markFailed("offer_sign")
					break
				}
				issued, issueErr := r.leases.IssueOffer(ctx, signed)
				if issueErr != nil {
					markFailed("lease_persist")
					break
				}
				leaseItems = append(leaseItems, issued)
				created++
			}
		}
	}
	if failed {
		return replicaOfferReconcileError{stage: failureStage}
	}
	return nil
}

func replicaOfferIdentityUsable(state authority.IdentityState, identityID string, now time.Time) bool {
	if state.IdentityID != identityID || state.Frozen || state.HeadHash == "" || state.VerifiedAt.IsZero() ||
		state.FreshUntil.IsZero() || now.Before(state.VerifiedAt) || now.Sub(state.VerifiedAt) > maxReplicaOfferIdentityAge ||
		!state.FreshUntil.After(now) || state.FreshUntil.After(state.VerifiedAt.Add(maxReplicaOfferIdentityAge)) {
		return false
	}
	rootPublic := state.CurrentKeys[authority.PrincipalIdentity+":"+identityID]
	derivedIdentity, err := identityprotocol.IdentityID(rootPublic)
	return err == nil && derivedIdentity == identityID
}

func replicaArchiveTombstoned(archiveID string, tombstones []lease.TombstoneRecord) bool {
	for _, record := range tombstones {
		if record.Signed.Tombstone.ArchiveID == archiveID {
			return true
		}
	}
	return false
}

func (r *replicaOfferReconciler) authorizedReplicaOfferChain(owner authority.AuthoritySnapshot, now time.Time, localNodeID, archiveID, snapshotID string) []authority.SignedGrant {
	hashes := make([]string, 0, len(owner.Grants))
	for hash := range owner.Grants {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		chain := owner.Grants[hash]
		if len(chain) == 0 {
			continue
		}
		if r.authority.AuthorizeChain(owner.Subject.ID, chain, r.resolve, now,
			authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}, archiveID, snapshotID, "archive.replicate") == nil {
			return chain
		}
	}
	return nil
}

func (r *replicaOfferReconciler) authorizedReplicaProviders(owner authority.AuthoritySnapshot, now time.Time,
	archiveID, snapshotID string, candidates []string) []string {
	hashes := make([]string, 0, len(owner.Grants))
	for hash := range owner.Grants {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	result := make([]string, 0, len(candidates))
	for _, providerID := range candidates {
		if providerID == "" {
			continue
		}
		principal := authority.Principal{Kind: authority.PrincipalNode, ID: providerID}
		for _, hash := range hashes {
			chain := owner.Grants[hash]
			if !replicaGrantTargets(chain, principal, archiveID, snapshotID) {
				continue
			}
			if r.authority.AuthorizeChain(owner.Subject.ID, chain, r.resolve, now, principal,
				archiveID, snapshotID, "archive.replicate") == nil {
				result = append(result, providerID)
				break
			}
		}
	}
	sort.Strings(result)
	return result
}

func replicaGrantTargets(chain []authority.SignedGrant, audience authority.Principal, archiveID, snapshotID string) bool {
	if len(chain) == 0 {
		return false
	}
	grant := chain[len(chain)-1].Grant
	if grant.Audience != audience || grant.ArchiveID != archiveID ||
		(grant.SnapshotID != "" && grant.SnapshotID != snapshotID) {
		return false
	}
	for _, action := range grant.Actions {
		if action == "archive.replicate" {
			return true
		}
	}
	return false
}

func (r *replicaOfferReconciler) preparedRecoveryEnvelope(snapshot archivesnapshot.Snapshot, state authority.IdentityState,
	identity globalidentity.Snapshot, leases []lease.Lease, ownerSubjectID string) (uint64, cryptoarchive.KeyEnvelope, bool) {
	recipientPublic, err := base64.RawURLEncoding.DecodeString(identity.RecoveryEncryptionPublicKey)
	rootPublic := state.CurrentKeys[authority.PrincipalIdentity+":"+identity.IdentityID]
	rootKeyID, keyErr := identityprotocol.KeyID(rootPublic)
	if err != nil || len(recipientPublic) != 32 || keyErr != nil {
		return 0, cryptoarchive.KeyEnvelope{}, false
	}
	epochSet := make(map[uint64]struct{})
	if epochs, epochErr := r.ciphertext.KeyEpochs(snapshot); epochErr == nil {
		for _, epoch := range epochs {
			if epoch > 0 {
				epochSet[epoch] = struct{}{}
			}
		}
	}
	for _, item := range leases {
		offer := item.Offer.Offer
		if offer.OwnerSubjectID != ownerSubjectID || offer.ArchiveID != snapshot.Content.ArchiveID {
			continue
		}
		epoch, parseErr := strconv.ParseUint(offer.KeyEpoch, 10, 64)
		if parseErr == nil && epoch > 0 && strconv.FormatUint(epoch, 10) == offer.KeyEpoch {
			epochSet[epoch] = struct{}{}
		}
	}
	epochs := make([]uint64, 0, len(epochSet))
	for epoch := range epochSet {
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] > epochs[j] })
	for _, epoch := range epochs {
		envelope, loadErr := r.ciphertext.LoadKeyEnvelope(snapshot, epoch, cryptoarchive.RecipientKeyID(recipientPublic))
		if loadErr != nil || envelope.ArchiveID != snapshot.Content.ArchiveID || envelope.KeyEpoch != strconv.FormatUint(epoch, 10) ||
			envelope.RecipientPublicKeySHA256 != cryptoarchive.RecipientPublicKeyHash(recipientPublic) ||
			envelope.SignerKeyID != rootKeyID || cryptoarchive.VerifyKeyEnvelopeSignature(envelope, rootPublic) != nil {
			continue
		}
		return epoch, envelope, true
	}
	return 0, cryptoarchive.KeyEnvelope{}, false
}

func automaticReplicaProviders(localNodeID string, sourceState authority.IdentityState, resolve authority.AuthorityIdentityResolver,
	sessions []peerapi.SessionInfo, now time.Time) []string {
	set := make(map[string]struct{})
	for _, session := range sessions {
		if session.NodeID == "" || session.NodeID == localNodeID || session.IdentityID == "" {
			continue
		}
		state := sourceState
		if session.IdentityID != sourceState.IdentityID {
			resolved, err := resolve(session.IdentityID, "", now)
			if err != nil || !replicaOfferIdentityUsable(resolved, session.IdentityID, now) {
				continue
			}
			state = resolved
		}
		publicKey := state.CurrentKeys[authority.PrincipalNode+":"+session.NodeID]
		if derived, err := identityprotocol.NodeID(publicKey); err == nil && derived == session.NodeID {
			set[session.NodeID] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Strings(result)
	if len(result) > maxAutomaticReplicaProviders {
		result = result[:maxAutomaticReplicaProviders]
	}
	return result
}

func planAutomaticReplicaOffers(policy authority.ReplicationPolicy, ownerSubjectID, archiveID, snapshotID, sourceNodeID string,
	leases []lease.Lease, tombstones []lease.TombstoneRecord, providers []string, now time.Time, limit int, newOfferBytes uint64) ([]string, error) {
	if !authority.ValidReplicationPolicy(policy) || ownerSubjectID == "" || archiveID == "" || snapshotID == "" || sourceNodeID == "" ||
		now.IsZero() || limit <= 0 || newOfferBytes == 0 || replicaArchiveTombstoned(archiveID, tombstones) {
		return nil, nil
	}
	minimum, _ := strconv.ParseUint(policy.MinimumReplicas, 10, 64)
	capacityLimit, _ := strconv.ParseUint(policy.CapacityLimitBytes, 10, 64)
	graceSeconds, _ := strconv.ParseUint(policy.GracePeriodSeconds, 10, 64)
	if minimum == 0 {
		return nil, nil
	}
	if graceSeconds > uint64((1<<63-1)/int64(time.Second)) {
		return nil, errors.New("replica policy grace period is invalid")
	}
	grace := time.Duration(graceSeconds) * time.Second
	activeProviders := make(map[string]struct{})
	pendingProviders := make(map[string]struct{})
	occupiedProviders := make(map[string]struct{})
	var capacityUsed uint64
	var latestTerminal time.Time
	hasTerminal := false
	for _, item := range leases {
		offer := item.Offer.Offer
		if offer.OwnerSubjectID != ownerSubjectID {
			continue
		}
		if replicaLeaseReservesCapacity(item) {
			reserved, err := strconv.ParseUint(offer.ReservedBytes, 10, 64)
			if err != nil || reserved > math.MaxUint64-capacityUsed {
				return nil, errors.New("replica lease capacity is invalid")
			}
			capacityUsed += reserved
		}
		if offer.ArchiveID != archiveID {
			continue
		}
		if replicaLeaseTerminal(item.Status) {
			if terminalAt := replicaLeaseTerminalTime(item); terminalAt.After(latestTerminal) {
				latestTerminal = terminalAt
				hasTerminal = true
			}
			continue
		}
		if offer.SnapshotID != snapshotID {
			continue
		}
		provider := offer.ProviderNodeID
		if item.Status == lease.StatusTerminating {
			occupiedProviders[provider] = struct{}{}
			continue
		}
		if !replicaLeaseHoldsCapacity(item, now) {
			continue
		}
		occupiedProviders[provider] = struct{}{}
		switch item.Status {
		case lease.StatusActive, lease.StatusRenewing:
			activeProviders[provider] = struct{}{}
		case lease.StatusOffered, lease.StatusAccepted, lease.StatusTransferring, lease.StatusVerifying:
			pendingProviders[provider] = struct{}{}
		}
	}
	coveredOrPending := make(map[string]struct{}, len(activeProviders)+len(pendingProviders))
	for provider := range activeProviders {
		coveredOrPending[provider] = struct{}{}
	}
	for provider := range pendingProviders {
		coveredOrPending[provider] = struct{}{}
	}
	if uint64(len(coveredOrPending)) >= minimum {
		return nil, nil
	}
	if hasTerminal {
		if !policy.RelocateAfterGrace {
			return nil, nil
		}
		if now.Before(latestTerminal.Add(grace)) {
			return nil, nil
		}
	}
	if capacityLimit > 0 {
		if capacityUsed >= capacityLimit {
			return nil, nil
		}
		remainingCopies := (capacityLimit - capacityUsed) / newOfferBytes
		if remainingCopies == 0 {
			return nil, nil
		}
		if uint64(limit) > remainingCopies {
			limit = int(remainingCopies)
		}
	}

	needed := int(minimum - uint64(len(coveredOrPending)))
	if needed > limit {
		needed = limit
	}
	result := make([]string, 0, needed)
	for _, provider := range providers {
		if provider == "" || provider == sourceNodeID {
			continue
		}
		if _, exists := occupiedProviders[provider]; exists {
			continue
		}
		result = append(result, provider)
		if len(result) == needed {
			break
		}
	}
	return result, nil
}

func replicaLeaseReservesCapacity(item lease.Lease) bool {
	switch item.Status {
	case lease.StatusOffered, lease.StatusAccepted, lease.StatusTransferring, lease.StatusVerifying,
		lease.StatusActive, lease.StatusRenewing, lease.StatusTerminating, lease.StatusUnconfirmed:
		return true
	default:
		return false
	}
}

func replicaLeaseHoldsCapacity(item lease.Lease, now time.Time) bool {
	switch item.Status {
	case lease.StatusOffered, lease.StatusAccepted, lease.StatusTransferring, lease.StatusVerifying,
		lease.StatusActive, lease.StatusRenewing, lease.StatusTerminating:
		return replicaLeaseEffectiveExpiry(item).After(now)
	default:
		return false
	}
}

func replicaLeaseTerminal(status lease.Status) bool {
	switch status {
	case lease.StatusExpired, lease.StatusFailed, lease.StatusDeleted, lease.StatusUnconfirmed:
		return true
	default:
		return false
	}
}

func replicaLeaseTerminalTime(item lease.Lease) time.Time {
	latest := replicaLeaseEffectiveExpiry(item)
	if item.DeletionReport != nil {
		if reported := parseReplicaExpiry(item.DeletionReport.Report.ReportedAt); reported.After(latest) {
			latest = reported
		}
	}
	if requested := parseReplicaExpiry(item.TerminationRequestedAt); requested.After(latest) {
		latest = requested
	}
	return latest
}

func replicaLeaseEffectiveExpiry(item lease.Lease) time.Time {
	if item.PendingRenewal != nil {
		if pending := parseReplicaExpiry(item.PendingRenewal.Renewal.ExpiresAt); pending.After(replicaLeaseCommittedExpiry(item)) {
			return pending
		}
	}
	return replicaLeaseCommittedExpiry(item)
}

func replicaLeaseCommittedExpiry(item lease.Lease) time.Time {
	if len(item.Renewals) > 0 {
		if renewed := parseReplicaExpiry(item.Renewals[len(item.Renewals)-1].Renewal.ExpiresAt); !renewed.IsZero() {
			return renewed
		}
	}
	return parseReplicaExpiry(item.Offer.Offer.ExpiresAt)
}

func replicaManifestReservedBytes(manifest replication.ReplicaManifest) (uint64, error) {
	if manifest.Version != replication.ReplicaManifestVersion || len(manifest.Objects) == 0 {
		return 0, replication.ErrInvalid
	}
	var total uint64
	for _, object := range manifest.Objects {
		size, err := strconv.ParseUint(object.CiphertextSize, 10, 64)
		if err != nil || size == 0 || strconv.FormatUint(size, 10) != object.CiphertextSize || size > math.MaxUint64-total {
			return 0, replication.ErrInvalid
		}
		total += size
	}
	return total, nil
}

func newAutomaticReplicaLeaseID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "rl_" + base64.RawURLEncoding.EncodeToString(entropy[:]), nil
}
