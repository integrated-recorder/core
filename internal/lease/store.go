package lease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
)

const (
	stateVersion    = "1"
	stateName       = "replica-leases-v1.json"
	lockName        = ".replica-leases.lock"
	privateDirMode  = 0700
	privateFileMode = 0600
)

// Store is the durable authority boundary for local replica retention state.
// A nil authorizer is allowed for read-only recovery, but all mutations fail
// closed. Key resolution must use verified identity history, not Discovery.
type Store struct {
	root       string
	authorizer Authorizer
	keys       KeyResolver
	now        func() time.Time
}

func Open(options Options) (*Store, error) {
	if !filepath.IsAbs(options.Root) || filepath.Clean(options.Root) != options.Root || options.Keys == nil {
		return nil, ErrInvalid
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if err := ensurePrivateDirectory(options.Root); err != nil {
		return nil, err
	}
	store := &Store{root: options.Root, authorizer: options.Authorizer, keys: options.Keys, now: options.Now}
	if err := store.withLocked(context.Background(), func(state *snapshot) (bool, error) { return false, nil }); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Snapshot(ctx context.Context) ([]Lease, []TombstoneRecord, error) {
	var leases []Lease
	var tombstones []TombstoneRecord
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		leases = make([]Lease, len(state.Leases))
		for i, lease := range state.Leases {
			leases[i] = cloneLease(lease)
		}
		tombstones = cloneTombstones(state.Tombstones)
		return false, nil
	})
	return leases, tombstones, err
}

func (s *Store) Get(ctx context.Context, leaseID string) (Lease, error) {
	if !leaseIDPattern.MatchString(leaseID) {
		return Lease{}, ErrInvalid
	}
	var result Lease
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		for _, lease := range state.Leases {
			if lease.Offer.Offer.LeaseID == leaseID {
				result = cloneLease(lease)
				return false, nil
			}
		}
		return false, ErrNotFound
	})
	return result, err
}

// OfferForProvider returns the original source-Node-signed offer only while it is
// still awaiting delivery to its designated provider. It does not authorize
// acceptance; the receiving Store must pass the returned value through
// IssueOffer, which rechecks the current authority chain, historical signer,
// expiry, and local tombstones before persisting it.
func (s *Store) OfferForProvider(ctx context.Context, leaseID, providerNodeID string) (SignedOffer, string, error) {
	if !leaseIDPattern.MatchString(leaseID) || !nodeIDPattern.MatchString(providerNodeID) {
		return SignedOffer{}, "", ErrInvalid
	}
	var offer SignedOffer
	var hash string
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		for _, item := range state.Leases {
			if item.Offer.Offer.LeaseID != leaseID {
				continue
			}
			if item.Status != StatusOffered || item.Offer.Offer.ProviderNodeID != providerNodeID ||
				!parseTime(item.Offer.Offer.ExpiresAt).After(s.now().UTC()) {
				return false, ErrDenied
			}
			if tomb := findTombstone(state.Tombstones, item.Offer.Offer.ArchiveID); tomb != nil {
				if tomb.Frozen {
					return false, ErrFrozen
				}
				return false, ErrDenied
			}
			offer, hash = cloneSignedOffer(item.Offer), item.OfferHash
			return false, nil
		}
		return false, ErrNotFound
	})
	return offer, hash, err
}

// PendingOffersForProvider returns only unexpired offers awaiting acceptance
// by the named provider and scoped to the caller's current OwnerSubject and
// archive capability. Pagination is stable because leases are persisted in
// lease-ID order. The returned projection contains no signed offer or object
// identifiers; callers must fetch the offer through OfferForProvider and the
// receiver must validate it again through IssueOffer.
func (s *Store) PendingOffersForProvider(ctx context.Context, providerNodeID, ownerSubjectID, archiveID, snapshotID string, offset, limit int) ([]PendingOffer, int, error) {
	if !nodeIDPattern.MatchString(providerNodeID) || !ownerSubjectPattern.MatchString(ownerSubjectID) ||
		!archiveIDPattern.MatchString(archiveID) || snapshotID != "" && !snapshotIDPattern.MatchString(snapshotID) ||
		offset < 0 || offset > MaxPendingOffers || limit < 1 || limit > MaxPendingOfferPage {
		return nil, 0, ErrInvalid
	}
	items := make([]PendingOffer, 0)
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		if tomb := findTombstone(state.Tombstones, archiveID); tomb != nil {
			if tomb.Frozen {
				return false, ErrFrozen
			}
			return false, ErrDenied
		}
		now := s.now().UTC()
		for _, current := range state.Leases {
			offer := current.Offer.Offer
			if current.Status != StatusOffered || offer.ProviderNodeID != providerNodeID || offer.OwnerSubjectID != ownerSubjectID ||
				offer.ArchiveID != archiveID || snapshotID != "" && offer.SnapshotID != snapshotID || !parseTime(offer.ExpiresAt).After(now) {
				continue
			}
			items = append(items, PendingOffer{
				LeaseID: offer.LeaseID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
				OwnerSubject: offer.OwnerSubjectID, OfferHash: current.OfferHash, KeyEpoch: offer.KeyEpoch,
				ExpiresAt: offer.ExpiresAt, ReservedBytes: offer.ReservedBytes, ObjectCount: len(offer.EncryptedObjectSet),
			})
			if len(items) > MaxPendingOffers {
				return false, ErrConflict
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, 0, err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].LeaseID < items[j].LeaseID })
	if offset > len(items) {
		return []PendingOffer{}, len(items), nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return append([]PendingOffer(nil), items[offset:end]...), len(items), nil
}

// IssueOffer persists one owner-authorized offer. Duplicate delivery of the
// byte-identical offer is idempotent; a lease ID cannot be rebound.
func (s *Store) IssueOffer(ctx context.Context, signed SignedOffer) (Lease, error) {
	canonical, err := signed.Offer.canonical()
	if err != nil {
		return Lease{}, err
	}
	evidence, err := grantEvidenceHash(signed.GrantChain)
	if err != nil || evidence != signed.Offer.AuthorizedEvidence {
		return Lease{}, ErrDenied
	}
	if err := s.authorize(ctx, signed.Offer.OwnerSubjectID, signed.Offer.ArchiveID, signed.Offer.SnapshotID, "archive.replicate", signed.Actor, signed.GrantChain); err != nil {
		return Lease{}, err
	}
	if err := s.verifyPrincipalSignature(ctx, signed.Actor, parseTime(signed.Offer.IssuedAt), signed.Signature, offerDomain, canonical); err != nil {
		return Lease{}, err
	}
	now := s.now().UTC()
	issued, expires := parseTime(signed.Offer.IssuedAt), parseTime(signed.Offer.ExpiresAt)
	if issued.After(now.Add(2*time.Minute)) || !expires.After(now) {
		return Lease{}, ErrDenied
	}
	hash, err := OfferHash(signed.Offer)
	if err != nil {
		return Lease{}, err
	}
	var result Lease
	err = s.withLocked(ctx, func(state *snapshot) (bool, error) {
		if tomb := findTombstone(state.Tombstones, signed.Offer.ArchiveID); tomb != nil {
			if tomb.Frozen {
				return false, ErrFrozen
			}
			return false, ErrDenied
		}
		for _, existing := range state.Leases {
			if existing.Offer.Offer.LeaseID == signed.Offer.LeaseID {
				if existing.OfferHash == hash {
					result = cloneLease(existing)
					return false, nil
				}
				return false, ErrConflict
			}
		}
		lease := Lease{Offer: cloneSignedOffer(signed), OfferHash: hash, Status: StatusOffered, Revision: "1", Progress: initialProgress(signed.Offer)}
		state.Leases = append(state.Leases, lease)
		sort.Slice(state.Leases, func(i, j int) bool { return state.Leases[i].Offer.Offer.LeaseID < state.Leases[j].Offer.Offer.LeaseID })
		result = cloneLease(lease)
		return true, nil
	})
	return result, err
}

func (s *Store) Accept(ctx context.Context, signed SignedAcceptance) (Lease, error) {
	a := signed.Acceptance
	canonical, err := CanonicalAcceptance(a)
	if err != nil {
		return Lease{}, err
	}
	if err := s.verifyNodeSignature(ctx, a.ProviderNodeID, parseTime(a.AcceptedAt), signed.Signature, acceptDomain, canonical); err != nil {
		return Lease{}, err
	}
	proposed, err := s.Get(ctx, a.LeaseID)
	if err != nil {
		return Lease{}, err
	}
	if proposed.OfferHash != a.OfferHash || proposed.Offer.Offer.ProviderNodeID != a.ProviderNodeID {
		return Lease{}, ErrDenied
	}
	offer := proposed.Offer
	if err := s.authorize(ctx, offer.Offer.OwnerSubjectID, offer.Offer.ArchiveID, offer.Offer.SnapshotID, "archive.replicate", offer.Actor, offer.GrantChain); err != nil {
		return Lease{}, err
	}
	var result Lease
	err = s.withLocked(ctx, func(state *snapshot) (bool, error) {
		lease, index := findLease(state.Leases, a.LeaseID)
		if index < 0 {
			return false, ErrNotFound
		}
		if acceptanceCanBeConfirmed(lease.Status) && lease.Acceptance.Acceptance == a && lease.Acceptance.Signature == signed.Signature {
			result = cloneLease(lease)
			return false, nil
		}
		if lease.Status != StatusOffered || a.OfferHash != lease.OfferHash || a.ProviderNodeID != lease.Offer.Offer.ProviderNodeID || a.ReservedBytes != lease.Offer.Offer.ReservedBytes {
			return false, ErrTransition
		}
		acceptedAt := parseTime(a.AcceptedAt)
		if acceptedAt.Before(parseTime(lease.Offer.Offer.IssuedAt)) || acceptedAt.After(s.now().UTC().Add(2*time.Minute)) || !acceptedAt.Before(parseTime(lease.Offer.Offer.ExpiresAt)) {
			return false, ErrDenied
		}
		lease.Acceptance = cloneAcceptance(signed)
		lease.Status = StatusAccepted
		if err := bumpLease(&lease); err != nil {
			return false, err
		}
		state.Leases[index] = lease
		result = cloneLease(lease)
		return true, nil
	})
	return result, err
}

// acceptanceCanBeConfirmed reports the nonterminal states in which an exact,
// already persisted provider receipt may be acknowledged again. This makes
// lost-response retries idempotent without allowing an old acceptance to
// resurrect expired, failed, or terminating leases.
func acceptanceCanBeConfirmed(status Status) bool {
	switch status {
	case StatusAccepted, StatusTransferring, StatusVerifying, StatusActive, StatusRenewing:
		return true
	default:
		return false
	}
}

func (s *Store) BeginTransfer(ctx context.Context, leaseID string) (Lease, error) {
	return s.mutateLease(ctx, leaseID, func(lease *Lease) error {
		if lease.Status == StatusTransferring {
			return nil
		}
		if lease.Status != StatusAccepted {
			return ErrTransition
		}
		if !parseTime(lease.Offer.Offer.ExpiresAt).After(s.now().UTC()) {
			lease.Status = StatusExpired
			return nil
		}
		lease.Status = StatusTransferring
		return nil
	})
}

// Checkpoint accepts a provider-signed durable prefix offset. Offset rollback
// is rejected. Full ciphertext readback is required before activation.
func (s *Store) Checkpoint(ctx context.Context, signed SignedCheckpoint) (Lease, error) {
	c := signed.Checkpoint
	canonical, err := CanonicalCheckpoint(c)
	if err != nil {
		return Lease{}, err
	}
	if err := s.verifyNodeSignature(ctx, c.ProviderNodeID, parseTime(c.RecordedAt), signed.Signature, checkpointDomain, canonical); err != nil {
		return Lease{}, err
	}
	return s.mutateLease(ctx, c.LeaseID, func(lease *Lease) error {
		if c.OfferHash != lease.OfferHash || c.ProviderNodeID != lease.Offer.Offer.ProviderNodeID {
			return ErrTransition
		}
		if !parseTime(lease.Offer.Offer.ExpiresAt).After(s.now().UTC()) {
			return ErrTransition
		}
		if lease.Status != StatusTransferring {
			// After a receiver has durably reached verification, a lost peer
			// response may cause it to replay its last checkpoint. Accept only the
			// byte-for-byte same signed checkpoint as an idempotent no-op; never
			// let a new offset or signature advance a lease beyond transfer.
			if lease.Status == StatusVerifying || lease.Status == StatusActive {
				for _, existing := range lease.Checkpoints {
					if existing.Checkpoint.ObjectID == c.ObjectID && existing == signed {
						return nil
					}
				}
			}
			return ErrTransition
		}
		object, ok := findObject(lease.Offer.Offer, c.ObjectID)
		if !ok {
			return ErrInvalid
		}
		limit, _ := strconv.ParseUint(object.CiphertextSize, 10, 64)
		offset, _ := strconv.ParseUint(c.CommittedOffset, 10, 64)
		if offset > limit || offset == 0 || parseTime(c.RecordedAt).After(s.now().UTC().Add(2*time.Minute)) {
			return ErrInvalid
		}
		for i := range lease.Progress {
			if lease.Progress[i].ObjectID != c.ObjectID {
				continue
			}
			previous, _ := strconv.ParseUint(lease.Progress[i].CommittedOffset, 10, 64)
			if offset < previous {
				return ErrConflict
			}
			if offset == previous {
				return nil
			}
			// The offset is the monotonic value. Several durable chunks may be
			// committed during one UTC second, so equal timestamps are valid; an
			// older timestamp still indicates a clock rollback or replay.
			if previous != 0 && parseTime(c.RecordedAt).Before(parseTime(lease.Progress[i].UpdatedAt)) {
				return ErrConflict
			}
			lease.Progress[i].CommittedOffset, lease.Progress[i].UpdatedAt = c.CommittedOffset, c.RecordedAt
			checkpointIndex := sort.Search(len(lease.Checkpoints), func(index int) bool {
				return lease.Checkpoints[index].Checkpoint.ObjectID >= c.ObjectID
			})
			if checkpointIndex < len(lease.Checkpoints) && lease.Checkpoints[checkpointIndex].Checkpoint.ObjectID == c.ObjectID {
				lease.Checkpoints[checkpointIndex] = cloneCheckpoint(signed)
			} else {
				lease.Checkpoints = append(lease.Checkpoints, SignedCheckpoint{})
				copy(lease.Checkpoints[checkpointIndex+1:], lease.Checkpoints[checkpointIndex:])
				lease.Checkpoints[checkpointIndex] = cloneCheckpoint(signed)
			}
			if allTransferred(*lease) {
				lease.Status = StatusVerifying
			}
			return nil
		}
		return ErrInvalid
	})
}

// VerifyReplica streams every ciphertext object back from the provider and
// checks the committed size and digest before making the retention lease active.
// The readback is not a claim of future storage durability.
func (s *Store) VerifyReplica(ctx context.Context, leaseID string, reader ObjectReader) (Lease, error) {
	if reader == nil {
		return Lease{}, ErrInvalid
	}
	lease, err := s.Get(ctx, leaseID)
	if err != nil {
		return Lease{}, err
	}
	if lease.Status != StatusVerifying || !allTransferred(lease) || !leaseExpiry(lease).After(s.now().UTC()) {
		return Lease{}, ErrTransition
	}
	for _, object := range lease.Offer.Offer.EncryptedObjectSet {
		stream, err := reader.OpenCiphertext(ctx, lease.Offer.Offer.ProviderNodeID, leaseID, object.ObjectID)
		if err != nil {
			return Lease{}, fmt.Errorf("%w: provider object readback", ErrDenied)
		}
		actual, readErr := hashBounded(ctx, stream, object.CiphertextSize)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || actual != object.CiphertextSHA256 {
			return Lease{}, ErrConflict
		}
	}
	var result Lease
	err = s.withLocked(ctx, func(state *snapshot) (bool, error) {
		current, index := findLease(state.Leases, leaseID)
		if index < 0 {
			return false, ErrNotFound
		}
		if current.Revision != lease.Revision || current.Status != StatusVerifying || !allTransferred(current) || !leaseExpiry(current).After(s.now().UTC()) {
			return false, ErrConflict
		}
		if findTombstone(state.Tombstones, current.Offer.Offer.ArchiveID) != nil {
			return false, ErrDenied
		}
		current.Status = StatusActive
		if err := bumpLease(&current); err != nil {
			return false, err
		}
		state.Leases[index] = current
		result = cloneLease(current)
		return true, nil
	})
	return result, err
}

func (s *Store) Renew(ctx context.Context, signed SignedRenewal) (Lease, error) {
	request := SignedRenewalRequest{Renewal: signed.Renewal, Actor: signed.Actor, GrantChain: cloneChain(signed.GrantChain), OwnerSignature: signed.OwnerSignature}
	canonical, err := CanonicalRenewal(request.Renewal)
	if err != nil {
		return Lease{}, err
	}
	lease, err := s.Get(ctx, signed.Renewal.LeaseID)
	if err != nil {
		return Lease{}, err
	}
	if err := s.verifyNodeSignature(ctx, lease.Offer.Offer.ProviderNodeID, parseTime(signed.Renewal.IssuedAt), signed.ProviderSignature, renewDomain, canonical); err != nil {
		return Lease{}, err
	}
	if lease.Status == StatusActive && len(lease.Renewals) > 0 && reflect.DeepEqual(lease.Renewals[len(lease.Renewals)-1], signed) {
		if err := s.verifyPrincipalSignature(ctx, signed.Actor, parseTime(signed.Renewal.IssuedAt), signed.OwnerSignature, renewDomain, canonical); err != nil {
			return Lease{}, err
		}
		return lease, nil
	}
	if _, err := s.BeginRenewal(ctx, request); err != nil {
		return Lease{}, err
	}
	return s.CompleteRenewal(ctx, signed)
}

// BeginRenewal durably records the current-authorized owner request before a
// provider receipt is available. A process restart can resume at CompleteRenewal.
func (s *Store) BeginRenewal(ctx context.Context, request SignedRenewalRequest) (Lease, error) {
	r := request.Renewal
	canonical, err := CanonicalRenewal(r)
	if err != nil {
		return Lease{}, err
	}
	evidence, err := grantEvidenceHash(request.GrantChain)
	if err != nil || evidence != r.AuthorizedEvidence {
		return Lease{}, ErrDenied
	}
	if err := s.verifyPrincipalSignature(ctx, request.Actor, parseTime(r.IssuedAt), request.OwnerSignature, renewDomain, canonical); err != nil {
		return Lease{}, err
	}
	lease, err := s.Get(ctx, r.LeaseID)
	if err != nil {
		return Lease{}, err
	}
	if lease.Status == StatusActive && len(lease.Renewals) > 0 && renewalRequestMatches(request, lease.Renewals[len(lease.Renewals)-1]) {
		if err := s.authorize(ctx, r.OwnerSubjectID, lease.Offer.Offer.ArchiveID, lease.Offer.Offer.SnapshotID, "archive.replicate", request.Actor, request.GrantChain); err != nil {
			return Lease{}, err
		}
		return lease, nil
	}
	if err := validateRenewalForLease(lease, request, s.now().UTC()); err != nil {
		return Lease{}, err
	}
	if err := s.authorize(ctx, r.OwnerSubjectID, lease.Offer.Offer.ArchiveID, lease.Offer.Offer.SnapshotID, "archive.replicate", request.Actor, request.GrantChain); err != nil {
		return Lease{}, err
	}
	return s.mutateLease(ctx, r.LeaseID, func(current *Lease) error {
		if current.Status == StatusRenewing && current.PendingRenewal != nil && reflect.DeepEqual(*current.PendingRenewal, request) {
			return nil
		}
		if current.Status == StatusActive && len(current.Renewals) > 0 && renewalRequestMatches(request, current.Renewals[len(current.Renewals)-1]) {
			return nil
		}
		if current.Status != StatusActive || current.Revision != lease.Revision {
			return ErrConflict
		}
		if err := validateRenewalForLease(*current, request, s.now().UTC()); err != nil {
			return err
		}
		current.Status = StatusRenewing
		current.PendingRenewal = cloneRenewalRequest(&request)
		return nil
	})
}

// CompleteRenewal commits the provider's independent receipt signature. The
// signed request must exactly match the durable pending owner request.
func (s *Store) CompleteRenewal(ctx context.Context, signed SignedRenewal) (Lease, error) {
	r := signed.Renewal
	canonical, err := CanonicalRenewal(r)
	if err != nil {
		return Lease{}, err
	}
	if err := s.verifyPrincipalSignature(ctx, signed.Actor, parseTime(r.IssuedAt), signed.OwnerSignature, renewDomain, canonical); err != nil {
		return Lease{}, err
	}
	lease, err := s.Get(ctx, r.LeaseID)
	if err != nil {
		return Lease{}, err
	}
	if lease.Status == StatusActive && len(lease.Renewals) > 0 && reflect.DeepEqual(lease.Renewals[len(lease.Renewals)-1], signed) {
		if err := s.verifyNodeSignature(ctx, lease.Offer.Offer.ProviderNodeID, parseTime(r.IssuedAt), signed.ProviderSignature, renewDomain, canonical); err != nil {
			return Lease{}, err
		}
		return lease, nil
	}
	if lease.Status != StatusRenewing || lease.PendingRenewal == nil || lease.Offer.Offer.ProviderNodeID == "" {
		return Lease{}, ErrTransition
	}
	if err := validateRenewalForLease(lease, *lease.PendingRenewal, s.now().UTC()); err != nil {
		return Lease{}, err
	}
	if !renewalRequestMatches(*lease.PendingRenewal, signed) {
		return Lease{}, ErrConflict
	}
	if err := s.verifyNodeSignature(ctx, lease.Offer.Offer.ProviderNodeID, parseTime(r.IssuedAt), signed.ProviderSignature, renewDomain, canonical); err != nil {
		return Lease{}, err
	}
	if err := s.authorize(ctx, r.OwnerSubjectID, lease.Offer.Offer.ArchiveID, lease.Offer.Offer.SnapshotID, "archive.replicate", signed.Actor, signed.GrantChain); err != nil {
		return Lease{}, err
	}
	return s.mutateLease(ctx, r.LeaseID, func(current *Lease) error {
		if current.Status != StatusRenewing || current.Revision != lease.Revision || current.PendingRenewal == nil || !renewalRequestMatches(*current.PendingRenewal, signed) {
			return ErrConflict
		}
		current.Renewals = append(current.Renewals, cloneRenewal(signed))
		current.PendingRenewal = nil
		current.Status = StatusActive
		return nil
	})
}

func validateRenewalForLease(lease Lease, request SignedRenewalRequest, now time.Time) error {
	r := request.Renewal
	issued, expires := parseTime(r.IssuedAt), parseTime(r.ExpiresAt)
	if lease.Status != StatusActive && lease.Status != StatusRenewing ||
		lease.OfferHash != r.OfferHash || lease.Offer.Offer.OwnerSubjectID != r.OwnerSubjectID ||
		request.Actor.ID == "" || issued.After(now.Add(2*time.Minute)) ||
		!issued.After(parseTime(lease.Acceptance.Acceptance.AcceptedAt)) || !expires.After(now) || expires.Sub(issued) > MaxLeaseDuration ||
		!expires.After(leaseCommittedExpiry(lease)) {
		return ErrDenied
	}
	if len(lease.Renewals) > 0 && !issued.After(parseTime(lease.Renewals[len(lease.Renewals)-1].Renewal.IssuedAt)) {
		return ErrConflict
	}
	return nil
}

// RenewalForProvider returns the durable owner request only to the provider
// named by the original offer. The requesting peer must still pass command
// authorization, and the provider Store revalidates the signed request before
// it signs a receipt.
func (s *Store) RenewalForProvider(ctx context.Context, leaseID, providerNodeID string) (SignedRenewalRequest, string, error) {
	if !leaseIDPattern.MatchString(leaseID) || !nodeIDPattern.MatchString(providerNodeID) {
		return SignedRenewalRequest{}, "", ErrInvalid
	}
	var request SignedRenewalRequest
	var offerHash string
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		item, index := findLease(state.Leases, leaseID)
		if index < 0 {
			return false, ErrNotFound
		}
		if item.Offer.Offer.ProviderNodeID != providerNodeID {
			return false, ErrDenied
		}
		switch item.Status {
		case StatusRenewing:
			if item.PendingRenewal == nil {
				return false, ErrInvalid
			}
			if err := validateRenewalForLease(item, *item.PendingRenewal, s.now().UTC()); err != nil {
				return false, err
			}
			request = *cloneRenewalRequest(item.PendingRenewal)
		case StatusActive:
			if len(item.Renewals) == 0 {
				return false, ErrDenied
			}
			latest := item.Renewals[len(item.Renewals)-1]
			request = SignedRenewalRequest{
				Renewal: latest.Renewal, Actor: latest.Actor, GrantChain: cloneChain(latest.GrantChain),
				OwnerSignature: latest.OwnerSignature,
			}
		default:
			return false, ErrDenied
		}
		offerHash = item.OfferHash
		return false, nil
	})
	return request, offerHash, err
}

func renewalRequestMatches(request SignedRenewalRequest, signed SignedRenewal) bool {
	return request.Renewal == signed.Renewal && request.Actor == signed.Actor && request.OwnerSignature == signed.OwnerSignature && equalGrantChains(request.GrantChain, signed.GrantChain)
}

// Expire advances leases whose retention deadline has passed. It never deletes
// remote objects and never changes canonical local archive state.
func (s *Store) Expire(ctx context.Context, now time.Time) ([]string, error) {
	if now.IsZero() {
		return nil, ErrInvalid
	}
	now = now.UTC()
	var expired []string
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		changed := false
		for i := range state.Leases {
			lease := &state.Leases[i]
			if terminal(lease.Status) {
				continue
			}
			if lease.Status == StatusTerminating {
				requested := parseTime(lease.TerminationRequestedAt)
				if !requested.IsZero() && !now.Before(requested.Add(24*time.Hour)) {
					lease.Status = StatusUnconfirmed
					lease.TerminationRequestedAt = ""
					if err := bumpLease(lease); err != nil {
						return false, err
					}
					expired = append(expired, lease.Offer.Offer.LeaseID)
					changed = true
				}
				continue
			}
			deadline := leaseExpiry(*lease)
			if !deadline.IsZero() && !now.Before(deadline) {
				lease.Status = StatusExpired
				lease.PendingRenewal = nil
				if err := bumpLease(lease); err != nil {
					return false, err
				}
				expired = append(expired, lease.Offer.Offer.LeaseID)
				changed = true
			}
		}
		return changed, nil
	})
	sort.Strings(expired)
	return expired, err
}

// AddTombstone records an owner-authorized deletion decision. It blocks new
// replication and moves existing leases to terminating, but does not delete
// original local archives or assert that a remote peer erased its copy.
func (s *Store) AddTombstone(ctx context.Context, signed SignedTombstone) (TombstoneRecord, error) {
	hash, err := s.validateSignedTombstone(ctx, signed)
	if err != nil {
		return TombstoneRecord{}, err
	}
	t := signed.Tombstone
	var result TombstoneRecord
	err = s.withLocked(ctx, func(state *snapshot) (bool, error) {
		current := findTombstone(state.Tombstones, t.ArchiveID)
		if current != nil {
			if current.Frozen {
				return false, ErrFrozen
			}
			old := current.Signed.Tombstone
			if t.DeletionEpoch == old.DeletionEpoch && hash == current.Hash {
				result = cloneTombstone(*current)
				return false, nil
			}
			oldEpoch, _ := strconv.ParseUint(old.DeletionEpoch, 10, 64)
			newEpoch, _ := strconv.ParseUint(t.DeletionEpoch, 10, 64)
			if newEpoch == oldEpoch && hash != current.Hash {
				if t.OwnerSubjectID != old.OwnerSubjectID || t.PreviousHead != old.PreviousHead {
					return false, ErrConflict
				}
				current.Frozen = true
				current.ForkHashes = sortedUnique(append(current.ForkHashes, current.Hash, hash))
				current.ForkEvidence = append(current.ForkEvidence, cloneSignedTombstone(signed))
				result = cloneTombstone(*current)
				return true, ErrFrozen
			}
			if newEpoch != oldEpoch+1 || t.PreviousHead != current.Hash || t.OwnerSubjectID != old.OwnerSubjectID {
				return false, ErrConflict
			}
		} else if t.DeletionEpoch != "1" || t.PreviousHead != "" {
			return false, ErrConflict
		}
		record := TombstoneRecord{Signed: cloneSignedTombstone(signed), Hash: hash}
		if current == nil {
			state.Tombstones = append(state.Tombstones, record)
		} else {
			*current = record
		}
		for i := range state.Leases {
			lease := &state.Leases[i]
			if lease.Offer.Offer.ArchiveID == t.ArchiveID && !terminal(lease.Status) {
				if lease.Status == StatusOffered {
					lease.Status = StatusFailed
					lease.FailureCode = "tombstoned_before_acceptance"
				} else {
					lease.Status = StatusTerminating
					lease.TerminationRequestedAt = t.IssuedAt
					lease.PendingRenewal = nil
				}
				if err := bumpLease(lease); err != nil {
					return false, err
				}
			}
		}
		sort.Slice(state.Tombstones, func(i, j int) bool {
			return state.Tombstones[i].Signed.Tombstone.ArchiveID < state.Tombstones[j].Signed.Tombstone.ArchiveID
		})
		result = cloneTombstone(record)
		return true, nil
	})
	return result, err
}

// CreateTombstone atomically creates the first archive tombstone or returns
// the already persisted decision. Signing occurs while holding the store's
// cross-process mutation lock, so concurrent retries cannot create two
// different epoch-1 tombstones and freeze an otherwise valid archive.
func (s *Store) CreateTombstone(ctx context.Context, ownerSubjectID, archiveID, reasonCode string, now time.Time, sign func(Tombstone) (SignedTombstone, error)) (TombstoneRecord, error) {
	if s == nil || ctx == nil || sign == nil || !ownerSubjectPattern.MatchString(ownerSubjectID) ||
		!archiveIDPattern.MatchString(archiveID) || !validReason(reasonCode) || now.IsZero() {
		return TombstoneRecord{}, ErrInvalid
	}
	issuedAt := now.UTC().Truncate(time.Second).Format(time.RFC3339)
	var result TombstoneRecord
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		if current := findTombstone(state.Tombstones, archiveID); current != nil {
			if current.Frozen {
				return false, ErrFrozen
			}
			if current.Signed.Tombstone.OwnerSubjectID != ownerSubjectID {
				return false, ErrConflict
			}
			if _, err := s.validateSignedTombstone(ctx, current.Signed); err != nil {
				return false, err
			}
			result = cloneTombstone(*current)
			return false, nil
		}
		tombstone := Tombstone{
			Version: TombstoneVersion, ArchiveID: archiveID, OwnerSubjectID: ownerSubjectID,
			DeletionEpoch: "1", ReasonCode: reasonCode, IssuedAt: issuedAt,
		}
		signed, err := sign(tombstone)
		if err != nil {
			return false, err
		}
		unsigned := signed.Tombstone
		unsigned.AuthorizedEvidence = ""
		if unsigned != tombstone {
			return false, ErrInvalid
		}
		hash, err := s.validateSignedTombstone(ctx, signed)
		if err != nil {
			return false, err
		}
		record := TombstoneRecord{Signed: cloneSignedTombstone(signed), Hash: hash}
		state.Tombstones = append(state.Tombstones, record)
		sort.Slice(state.Tombstones, func(i, j int) bool {
			return state.Tombstones[i].Signed.Tombstone.ArchiveID < state.Tombstones[j].Signed.Tombstone.ArchiveID
		})
		for i := range state.Leases {
			current := &state.Leases[i]
			if current.Offer.Offer.ArchiveID != archiveID || terminal(current.Status) {
				continue
			}
			if current.Status == StatusOffered {
				current.Status = StatusFailed
				current.FailureCode = "tombstoned_before_acceptance"
			} else {
				current.Status = StatusTerminating
				current.TerminationRequestedAt = issuedAt
				current.PendingRenewal = nil
			}
			if err := bumpLease(current); err != nil {
				return false, err
			}
		}
		result = cloneTombstone(record)
		return true, nil
	})
	return result, err
}

func (s *Store) validateSignedTombstone(ctx context.Context, signed SignedTombstone) (string, error) {
	tombstone := signed.Tombstone
	canonical, err := CanonicalTombstone(tombstone)
	if err != nil {
		return "", err
	}
	if err := s.authorize(ctx, tombstone.OwnerSubjectID, tombstone.ArchiveID, "", "archive.delete", signed.Actor, signed.GrantChain); err != nil {
		return "", err
	}
	evidence, err := grantEvidenceHash(signed.GrantChain)
	if err != nil || evidence != tombstone.AuthorizedEvidence {
		return "", ErrDenied
	}
	if err := s.verifyPrincipalSignature(ctx, signed.Actor, parseTime(tombstone.IssuedAt), signed.Signature, tombstoneDomain, canonical); err != nil {
		return "", err
	}
	return digest("IR/archive-tombstone/v1/hash\x00", canonical), nil
}

// MarkTombstoneDelivered persists that a delegated peer returned the exact
// tombstone receipt over an authenticated session. Replaying an acknowledgement
// is idempotent; a receipt for another tombstone hash cannot advance state.
func (s *Store) MarkTombstoneDelivered(ctx context.Context, archiveID, nodeID, tombstoneHash string, acknowledgedAt time.Time) error {
	if s == nil || ctx == nil || !archiveIDPattern.MatchString(archiveID) || !nodeIDPattern.MatchString(nodeID) ||
		!hashPattern.MatchString(tombstoneHash) || acknowledgedAt.IsZero() {
		return ErrInvalid
	}
	at := acknowledgedAt.UTC().Truncate(time.Second).Format(time.RFC3339)
	var opErr error
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		record := findTombstone(state.Tombstones, archiveID)
		if record == nil {
			return false, ErrNotFound
		}
		if record.Frozen {
			return false, ErrFrozen
		}
		if record.Hash != tombstoneHash || parseTime(at).Before(parseTime(record.Signed.Tombstone.IssuedAt)) {
			return false, ErrConflict
		}
		for _, delivery := range record.Deliveries {
			if delivery.NodeID != nodeID {
				continue
			}
			if delivery.TombstoneHash != tombstoneHash {
				return false, ErrConflict
			}
			return false, nil
		}
		record.Deliveries = append(record.Deliveries, TombstoneDelivery{
			NodeID: nodeID, TombstoneHash: tombstoneHash, AcknowledgedAt: at,
		})
		sort.Slice(record.Deliveries, func(i, j int) bool { return record.Deliveries[i].NodeID < record.Deliveries[j].NodeID })
		return true, nil
	})
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrFrozen) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalid) {
		return err
	}
	if err != nil {
		opErr = errors.New("replica tombstone delivery acknowledgement could not be persisted")
	}
	return opErr
}

// ReportDeletion stores the provider's signed report as an observation only.
// A "deleted" report is not cryptographic proof that all copies were erased.
func (s *Store) ReportDeletion(ctx context.Context, signed SignedDeletionReport) (Lease, error) {
	r := signed.Report
	canonical, err := CanonicalDeletionReport(r)
	if err != nil {
		return Lease{}, err
	}
	if err := s.verifyNodeSignature(ctx, r.ProviderNodeID, parseTime(r.ReportedAt), signed.Signature, deletionReportDomain, canonical); err != nil {
		return Lease{}, err
	}
	if parseTime(r.ReportedAt).After(s.now().UTC().Add(2 * time.Minute)) {
		return Lease{}, ErrDenied
	}
	var result Lease
	err = s.withLocked(ctx, func(state *snapshot) (bool, error) {
		lease, index := findLease(state.Leases, r.LeaseID)
		if index < 0 {
			return false, ErrNotFound
		}
		if r.TombstoneHash != "" {
			tombstone := findTombstone(state.Tombstones, lease.Offer.Offer.ArchiveID)
			if tombstone == nil || tombstone.Frozen || tombstone.Hash != r.TombstoneHash {
				return false, ErrConflict
			}
		}
		if lease.DeletionReport != nil && reflect.DeepEqual(*lease.DeletionReport, signed) && lease.Status == Status(r.Status) {
			result = cloneLease(lease)
			return false, nil
		}
		if lease.OfferHash != r.OfferHash || lease.Offer.Offer.ProviderNodeID != r.ProviderNodeID {
			return false, ErrTransition
		}
		requested := parseTime(lease.TerminationRequestedAt)
		if r.TombstoneHash != "" && (lease.Status == StatusExpired || lease.Status == StatusFailed || lease.Status == StatusUnconfirmed) && r.Status == string(StatusDeleted) {
			if tombstone := findTombstone(state.Tombstones, lease.Offer.Offer.ArchiveID); tombstone != nil && !tombstone.Frozen && tombstone.Hash == r.TombstoneHash {
				requested = parseTime(tombstone.Signed.Tombstone.IssuedAt)
			}
		} else if lease.Status != StatusTerminating {
			return false, ErrTransition
		}
		if requested.IsZero() || parseTime(r.ReportedAt).Before(requested) {
			return false, ErrDenied
		}
		copyReport := cloneDeletionReport(signed)
		lease.DeletionReport = &copyReport
		lease.TerminationRequestedAt = ""
		lease.Status = Status(r.Status)
		if err := bumpLease(&lease); err != nil {
			return false, err
		}
		state.Leases[index] = lease
		result = cloneLease(lease)
		return true, nil
	})
	return result, err
}

func (s *Store) mutateLease(ctx context.Context, leaseID string, mutate func(*Lease) error) (Lease, error) {
	if !leaseIDPattern.MatchString(leaseID) {
		return Lease{}, ErrInvalid
	}
	var result Lease
	err := s.withLocked(ctx, func(state *snapshot) (bool, error) {
		lease, index := findLease(state.Leases, leaseID)
		if index < 0 {
			return false, ErrNotFound
		}
		before := cloneLease(lease)
		if err := mutate(&lease); err != nil {
			return false, err
		}
		if reflect.DeepEqual(lease, before) {
			result = cloneLease(lease)
			return false, nil
		}
		if err := bumpLease(&lease); err != nil {
			return false, err
		}
		state.Leases[index] = lease
		result = cloneLease(lease)
		return true, nil
	})
	return result, err
}

func (s *Store) authorize(ctx context.Context, subjectID, archiveID, snapshotID, action string, actor authority.Principal, chain []authority.SignedGrant) error {
	if s.authorizer == nil || !ownerSubjectPattern.MatchString(subjectID) || !actorValid(actor) || len(chain) == 0 {
		return ErrDenied
	}
	if err := s.authorizer.Authorize(ctx, subjectID, archiveID, snapshotID, action, actor, cloneChain(chain), s.now().UTC()); err != nil {
		return fmt.Errorf("%w: capability verification", ErrDenied)
	}
	return nil
}

func (s *Store) verifyPrincipalSignature(ctx context.Context, actor authority.Principal, at time.Time, signature, domain string, canonical []byte) error {
	if !actorValid(actor) || s.keys == nil {
		return ErrDenied
	}
	key, err := s.keys.VerifiedKey(ctx, actor, at)
	if err != nil {
		return ErrDenied
	}
	return verifySignature(key, signature, domain, canonical)
}

func (s *Store) verifyNodeSignature(ctx context.Context, nodeID string, at time.Time, signature, domain string, canonical []byte) error {
	if !nodeIDPattern.MatchString(nodeID) {
		return ErrDenied
	}
	key, err := s.keys.VerifiedKey(ctx, authority.Principal{Kind: authority.PrincipalNode, ID: nodeID}, at)
	if err != nil {
		return ErrDenied
	}
	return verifySignature(key, signature, domain, canonical)
}

func (s *Store) withLocked(ctx context.Context, fn func(*snapshot) (bool, error)) error {
	if s == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	lockPath := filepath.Join(s.root, lockName)
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, privateFileMode)
	if err != nil {
		return errors.New("replica lease state is unavailable")
	}
	defer lock.Close()
	info, statErr := os.Lstat(lockPath)
	opened, fileErr := lock.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) || info.Mode().Perm() != privateFileMode {
		return ErrInvalid
	}
	if err := leaseLock(lock); err != nil {
		return errors.New("replica lease lock is unavailable")
	}
	defer leaseUnlock(lock)
	state, err := s.readState()
	if err != nil {
		return err
	}
	changed, opErr := fn(&state)
	if opErr != nil {
		if changed {
			if writeErr := s.writeState(state); writeErr != nil {
				return writeErr
			}
		}
		return opErr
	}
	if changed {
		return s.writeState(state)
	}
	return nil
}

func (s *Store) readState() (snapshot, error) {
	path := filepath.Join(s.root, stateName)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return snapshot{Version: stateVersion, Leases: []Lease{}, Tombstones: []TombstoneRecord{}}, nil
	}
	if err != nil {
		return snapshot{}, ErrInvalid
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != privateFileMode || info.Size() <= 0 || info.Size() > maxStateBytes {
		return snapshot{}, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, maxStateBytes+1))
	if err != nil || len(data) > maxStateBytes {
		return snapshot{}, ErrInvalid
	}
	var state snapshot
	if err := strictDecode(data, &state); err != nil || state.Version != stateVersion || state.Leases == nil || state.Tombstones == nil {
		return snapshot{}, ErrInvalid
	}
	if err := s.validateState(context.Background(), state); err != nil {
		return snapshot{}, err
	}
	return state, nil
}

func (s *Store) validateState(ctx context.Context, state snapshot) error {
	if len(state.Leases) > 100000 || len(state.Tombstones) > 100000 {
		return ErrInvalid
	}
	lastLease, lastArchive := "", ""
	for _, lease := range state.Leases {
		if lease.Offer.Offer.LeaseID <= lastLease || !validStatus(lease.Status) || !decimal(lease.Revision) || lease.OfferHash == "" {
			return ErrInvalid
		}
		lastLease = lease.Offer.Offer.LeaseID
		offerBytes, err := lease.Offer.Offer.canonical()
		if err != nil {
			return ErrInvalid
		}
		if digest("IR/replica-lease/v1/hash\x00", offerBytes) != lease.OfferHash {
			return ErrInvalid
		}
		if evidence, err := grantEvidenceHash(lease.Offer.GrantChain); err != nil || evidence != lease.Offer.Offer.AuthorizedEvidence {
			return ErrInvalid
		}
		if s.verifyPrincipalSignature(ctx, lease.Offer.Actor, parseTime(lease.Offer.Offer.IssuedAt), lease.Offer.Signature, offerDomain, offerBytes) != nil {
			return ErrInvalid
		}
		hasAcceptance := lease.Acceptance.Acceptance.LeaseID != ""
		if !hasAcceptance {
			if lease.Status != StatusOffered && lease.Status != StatusExpired && !(lease.Status == StatusFailed && lease.FailureCode == "tombstoned_before_acceptance") {
				return ErrInvalid
			}
			if lease.Acceptance.Signature != "" || lease.Acceptance.Acceptance != (Acceptance{}) {
				return ErrInvalid
			}
		} else {
			a := lease.Acceptance.Acceptance
			canonical, err := CanonicalAcceptance(a)
			if err != nil || a.LeaseID != lease.Offer.Offer.LeaseID || a.OfferHash != lease.OfferHash || a.ProviderNodeID != lease.Offer.Offer.ProviderNodeID || a.ReservedBytes != lease.Offer.Offer.ReservedBytes || s.verifyNodeSignature(ctx, lease.Offer.Offer.ProviderNodeID, parseTime(a.AcceptedAt), lease.Acceptance.Signature, acceptDomain, canonical) != nil {
				return ErrInvalid
			}
		}
		if !validateProgress(lease) {
			return ErrInvalid
		}
		if (lease.Status == StatusTransferring && allTransferred(lease)) || ((lease.Status == StatusVerifying || lease.Status == StatusActive || lease.Status == StatusRenewing) && !allTransferred(lease)) {
			return ErrInvalid
		}
		if (lease.Status == StatusRenewing) != (lease.PendingRenewal != nil) {
			return ErrInvalid
		}
		if lease.PendingRenewal != nil {
			request := *lease.PendingRenewal
			r := request.Renewal
			canonical, err := CanonicalRenewal(r)
			evidence, evidenceErr := grantEvidenceHash(request.GrantChain)
			if err != nil || evidenceErr != nil || evidence != r.AuthorizedEvidence || r.LeaseID != lease.Offer.Offer.LeaseID || r.OfferHash != lease.OfferHash || r.OwnerSubjectID != lease.Offer.Offer.OwnerSubjectID || !parseTime(r.IssuedAt).After(parseTime(lease.Acceptance.Acceptance.AcceptedAt)) || !parseTime(r.ExpiresAt).After(leaseCommittedExpiry(lease)) || s.verifyPrincipalSignature(ctx, request.Actor, parseTime(r.IssuedAt), request.OwnerSignature, renewDomain, canonical) != nil {
				return ErrInvalid
			}
			if len(lease.Renewals) > 0 && !parseTime(r.IssuedAt).After(parseTime(lease.Renewals[len(lease.Renewals)-1].Renewal.IssuedAt)) {
				return ErrInvalid
			}
		}
		if lease.Status == StatusTerminating && (parseTime(lease.TerminationRequestedAt).IsZero() || !hasAcceptance) {
			return ErrInvalid
		}
		if lease.Status != StatusTerminating && lease.TerminationRequestedAt != "" {
			return ErrInvalid
		}
		if lease.Status == StatusDeleted && (lease.DeletionReport == nil || lease.DeletionReport.Report.Status != string(StatusDeleted)) {
			return ErrInvalid
		}
		if lease.FailureCode != "" && lease.FailureCode != "tombstoned_before_acceptance" {
			return ErrInvalid
		}
		if len(lease.Checkpoints) > len(lease.Offer.Offer.EncryptedObjectSet) {
			return ErrInvalid
		}
		expectedOffsets := make(map[string]uint64, len(lease.Progress))
		progressByObject := make(map[string]Progress, len(lease.Progress))
		for _, progress := range lease.Progress {
			expectedOffsets[progress.ObjectID] = 0
			progressByObject[progress.ObjectID] = progress
		}
		lastCheckpointObject := ""
		for _, checkpoint := range lease.Checkpoints {
			c := checkpoint.Checkpoint
			canonical, err := CanonicalCheckpoint(c)
			progress, hasProgress := progressByObject[c.ObjectID]
			if err != nil || c.ObjectID <= lastCheckpointObject || !hasProgress || c.LeaseID != lease.Offer.Offer.LeaseID || c.OfferHash != lease.OfferHash || c.ProviderNodeID != lease.Offer.Offer.ProviderNodeID || c.CommittedOffset != progress.CommittedOffset || c.RecordedAt != progress.UpdatedAt || parseTime(c.RecordedAt).Before(parseTime(lease.Acceptance.Acceptance.AcceptedAt)) || s.verifyNodeSignature(ctx, c.ProviderNodeID, parseTime(c.RecordedAt), checkpoint.Signature, checkpointDomain, canonical) != nil {
				return ErrInvalid
			}
			lastCheckpointObject = c.ObjectID
			object, ok := findObject(lease.Offer.Offer, c.ObjectID)
			if !ok {
				return ErrInvalid
			}
			limit, _ := strconv.ParseUint(object.CiphertextSize, 10, 64)
			offset, _ := strconv.ParseUint(c.CommittedOffset, 10, 64)
			if offset == 0 || offset > limit || offset < expectedOffsets[c.ObjectID] {
				return ErrInvalid
			}
			expectedOffsets[c.ObjectID] = offset
		}
		for _, progress := range lease.Progress {
			offset, _ := strconv.ParseUint(progress.CommittedOffset, 10, 64)
			if offset != expectedOffsets[progress.ObjectID] {
				return ErrInvalid
			}
		}
		lastRenewalIssue, lastRenewalExpiry := parseTime(lease.Acceptance.Acceptance.AcceptedAt), leaseExpiry(Lease{Offer: lease.Offer})
		for _, renewal := range lease.Renewals {
			canonical, err := CanonicalRenewal(renewal.Renewal)
			r := renewal.Renewal
			evidence, evidenceErr := grantEvidenceHash(renewal.GrantChain)
			issued, expires := parseTime(r.IssuedAt), parseTime(r.ExpiresAt)
			if err != nil || evidenceErr != nil || evidence != r.AuthorizedEvidence || r.LeaseID != lease.Offer.Offer.LeaseID || r.OfferHash != lease.OfferHash || r.OwnerSubjectID != lease.Offer.Offer.OwnerSubjectID || !issued.After(lastRenewalIssue) || expires.Sub(issued) > MaxLeaseDuration || !expires.After(lastRenewalExpiry) || s.verifyPrincipalSignature(ctx, renewal.Actor, issued, renewal.OwnerSignature, renewDomain, canonical) != nil || s.verifyNodeSignature(ctx, lease.Offer.Offer.ProviderNodeID, issued, renewal.ProviderSignature, renewDomain, canonical) != nil {
				return ErrInvalid
			}
			lastRenewalIssue, lastRenewalExpiry = issued, expires
		}
		if lease.DeletionReport != nil {
			report := lease.DeletionReport.Report
			canonical, err := CanonicalDeletionReport(report)
			if err != nil || report.LeaseID != lease.Offer.Offer.LeaseID || report.OfferHash != lease.OfferHash || report.ProviderNodeID != lease.Offer.Offer.ProviderNodeID || Status(report.Status) != lease.Status || s.verifyNodeSignature(ctx, lease.Offer.Offer.ProviderNodeID, parseTime(report.ReportedAt), lease.DeletionReport.Signature, deletionReportDomain, canonical) != nil {
				return ErrInvalid
			}
		}
	}
	for _, tomb := range state.Tombstones {
		if tomb.Signed.Tombstone.ArchiveID <= lastArchive {
			return ErrInvalid
		}
		lastArchive = tomb.Signed.Tombstone.ArchiveID
		canonical, err := CanonicalTombstone(tomb.Signed.Tombstone)
		if err != nil || digest("IR/archive-tombstone/v1/hash\x00", canonical) != tomb.Hash || s.verifyPrincipalSignature(ctx, tomb.Signed.Actor, parseTime(tomb.Signed.Tombstone.IssuedAt), tomb.Signed.Signature, tombstoneDomain, canonical) != nil {
			return ErrInvalid
		}
		if evidence, err := grantEvidenceHash(tomb.Signed.GrantChain); err != nil || evidence != tomb.Signed.Tombstone.AuthorizedEvidence {
			return ErrInvalid
		}
		if tomb.Frozen {
			if len(tomb.ForkHashes) < 2 || len(tomb.ForkEvidence) == 0 {
				return ErrInvalid
			}
			hashes := []string{tomb.Hash}
			for _, evidence := range tomb.ForkEvidence {
				fork := evidence.Tombstone
				forkCanonical, err := CanonicalTombstone(fork)
				if err != nil || fork.ArchiveID != tomb.Signed.Tombstone.ArchiveID || fork.DeletionEpoch != tomb.Signed.Tombstone.DeletionEpoch || digest("IR/archive-tombstone/v1/hash\x00", forkCanonical) == tomb.Hash || s.verifyPrincipalSignature(ctx, evidence.Actor, parseTime(fork.IssuedAt), evidence.Signature, tombstoneDomain, forkCanonical) != nil {
					return ErrInvalid
				}
				if grantHash, err := grantEvidenceHash(evidence.GrantChain); err != nil || grantHash != fork.AuthorizedEvidence {
					return ErrInvalid
				}
				hashes = append(hashes, digest("IR/archive-tombstone/v1/hash\x00", forkCanonical))
			}
			if strings.Join(sortedUnique(hashes), "\x00") != strings.Join(sortedUnique(append([]string(nil), tomb.ForkHashes...)), "\x00") {
				return ErrInvalid
			}
		}
		lastDeliveryNode := ""
		for _, delivery := range tomb.Deliveries {
			acknowledgedAt := parseTime(delivery.AcknowledgedAt)
			if !nodeIDPattern.MatchString(delivery.NodeID) || delivery.NodeID <= lastDeliveryNode ||
				delivery.TombstoneHash != tomb.Hash || acknowledgedAt.IsZero() || acknowledgedAt.Before(parseTime(tomb.Signed.Tombstone.IssuedAt)) {
				return ErrInvalid
			}
			lastDeliveryNode = delivery.NodeID
		}
	}
	return nil
}

func (s *Store) writeState(state snapshot) error {
	sort.Slice(state.Leases, func(i, j int) bool { return state.Leases[i].Offer.Offer.LeaseID < state.Leases[j].Offer.Offer.LeaseID })
	sort.Slice(state.Tombstones, func(i, j int) bool {
		return state.Tombstones[i].Signed.Tombstone.ArchiveID < state.Tombstones[j].Signed.Tombstone.ArchiveID
	})
	data, err := json.Marshal(state)
	if err != nil || len(data) > maxStateBytes {
		return ErrInvalid
	}
	tmp, err := os.CreateTemp(s.root, ".replica-leases-*.tmp")
	if err != nil {
		return errors.New("replica lease state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(privateFileMode); err != nil {
		_ = tmp.Close()
		return errors.New("replica lease state permissions could not be set")
	}
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("replica lease state could not be written")
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("replica lease state could not be synchronized")
	}
	if err = tmp.Close(); err != nil {
		return errors.New("replica lease state could not be closed")
	}
	if err = os.Rename(tmpPath, filepath.Join(s.root, stateName)); err != nil {
		return errors.New("replica lease state could not be committed")
	}
	dir, err := os.Open(s.root)
	if err != nil {
		return errors.New("replica lease directory could not be opened")
	}
	syncErr, closeErr := dir.Sync(), dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("replica lease directory could not be synchronized")
	}
	return nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, privateDirMode); err != nil {
		return errors.New("replica lease directory is unavailable")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != privateDirMode {
		return ErrInvalid
	}
	return nil
}

func hashBounded(ctx context.Context, reader io.Reader, sizeText string) (string, error) {
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil || size < 0 || size > MaxCiphertextObjectBytes {
		return "", ErrInvalid
	}
	h := sha256.New()
	limited := io.LimitReader(&contextReader{ctx: ctx, reader: reader}, size+1)
	n, err := io.Copy(h, limited)
	if err != nil || n != size {
		return "", ErrConflict
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func findLease(leases []Lease, id string) (Lease, int) {
	i := sort.Search(len(leases), func(i int) bool { return leases[i].Offer.Offer.LeaseID >= id })
	if i < len(leases) && leases[i].Offer.Offer.LeaseID == id {
		return cloneLease(leases[i]), i
	}
	return Lease{}, -1
}
func findObject(offer Offer, id string) (EncryptedObject, bool) {
	i := sort.Search(len(offer.EncryptedObjectSet), func(i int) bool { return offer.EncryptedObjectSet[i].ObjectID >= id })
	if i < len(offer.EncryptedObjectSet) && offer.EncryptedObjectSet[i].ObjectID == id {
		return offer.EncryptedObjectSet[i], true
	}
	return EncryptedObject{}, false
}
func findTombstone(records []TombstoneRecord, archive string) *TombstoneRecord {
	i := sort.Search(len(records), func(i int) bool { return records[i].Signed.Tombstone.ArchiveID >= archive })
	if i < len(records) && records[i].Signed.Tombstone.ArchiveID == archive {
		return &records[i]
	}
	return nil
}
func initialProgress(offer Offer) []Progress {
	out := make([]Progress, len(offer.EncryptedObjectSet))
	for i, o := range offer.EncryptedObjectSet {
		out[i] = Progress{ObjectID: o.ObjectID, CommittedOffset: "0"}
	}
	return out
}
func allTransferred(lease Lease) bool {
	if len(lease.Progress) != len(lease.Offer.Offer.EncryptedObjectSet) {
		return false
	}
	for i, p := range lease.Progress {
		if p.ObjectID != lease.Offer.Offer.EncryptedObjectSet[i].ObjectID || p.CommittedOffset != lease.Offer.Offer.EncryptedObjectSet[i].CiphertextSize {
			return false
		}
	}
	return true
}
func validateProgress(lease Lease) bool {
	if len(lease.Progress) != len(lease.Offer.Offer.EncryptedObjectSet) {
		return false
	}
	for i, p := range lease.Progress {
		object := lease.Offer.Offer.EncryptedObjectSet[i]
		if p.ObjectID != object.ObjectID || !decimal(p.CommittedOffset) {
			return false
		}
		offset, _ := strconv.ParseUint(p.CommittedOffset, 10, 64)
		limit, _ := strconv.ParseUint(object.CiphertextSize, 10, 64)
		if offset > limit {
			return false
		}
		if p.CommittedOffset != "0" && parseTime(p.UpdatedAt).IsZero() {
			return false
		}
	}
	return true
}
func bumpLease(lease *Lease) error {
	v, err := nextRevision(lease.Revision)
	if err != nil {
		return err
	}
	lease.Revision = v
	return nil
}
func validStatus(s Status) bool {
	switch s {
	case StatusOffered, StatusAccepted, StatusTransferring, StatusVerifying, StatusActive, StatusRenewing, StatusExpired, StatusFailed, StatusTerminating, StatusDeleted, StatusUnconfirmed:
		return true
	}
	return false
}
func terminal(s Status) bool {
	return s == StatusExpired || s == StatusFailed || s == StatusDeleted || s == StatusUnconfirmed
}
func leaseExpiry(l Lease) time.Time {
	if l.PendingRenewal != nil {
		if pendingExpiry := parseTime(l.PendingRenewal.Renewal.ExpiresAt); pendingExpiry.After(leaseCommittedExpiry(l)) {
			return pendingExpiry
		}
	}
	return leaseCommittedExpiry(l)
}

func leaseCommittedExpiry(l Lease) time.Time {
	if len(l.Renewals) > 0 {
		return parseTime(l.Renewals[len(l.Renewals)-1].Renewal.ExpiresAt)
	}
	return parseTime(l.Offer.Offer.ExpiresAt)
}
func actorValid(p authority.Principal) bool {
	return p.Kind == authority.PrincipalIdentity && identityIDPattern.MatchString(p.ID) || p.Kind == authority.PrincipalNode && nodeIDPattern.MatchString(p.ID)
}
func digest(domain string, b []byte) string {
	h := sha256.Sum256(append([]byte(domain), b...))
	return "sha256:" + hex.EncodeToString(h[:])
}
func grantEvidenceHash(chain []authority.SignedGrant) (string, error) {
	if len(chain) == 0 {
		return "", ErrDenied
	}
	b, err := canonicalRecord(chain)
	if err != nil {
		return "", err
	}
	return digest("IR/replica-lease/v1/authorization\x00", b), nil
}
func sortedUnique(values []string) []string {
	sort.Strings(values)
	out := values[:0]
	for _, v := range values {
		if len(out) == 0 || out[len(out)-1] != v {
			out = append(out, v)
		}
	}
	return out
}

func cloneSignedOffer(v SignedOffer) SignedOffer {
	v.Offer = cloneOffer(v.Offer)
	v.GrantChain = cloneChain(v.GrantChain)
	return v
}
func cloneAcceptance(v SignedAcceptance) SignedAcceptance { return v }
func cloneRenewal(v SignedRenewal) SignedRenewal          { v.GrantChain = cloneChain(v.GrantChain); return v }
func cloneSignedTombstone(v SignedTombstone) SignedTombstone {
	v.GrantChain = cloneChain(v.GrantChain)
	return v
}
func cloneDeletionReport(v SignedDeletionReport) SignedDeletionReport { return v }
func cloneTombstone(v TombstoneRecord) TombstoneRecord {
	v.Signed = cloneSignedTombstone(v.Signed)
	v.ForkHashes = append([]string(nil), v.ForkHashes...)
	v.ForkEvidence = append([]SignedTombstone(nil), v.ForkEvidence...)
	for i := range v.ForkEvidence {
		v.ForkEvidence[i] = cloneSignedTombstone(v.ForkEvidence[i])
	}
	v.Deliveries = append([]TombstoneDelivery(nil), v.Deliveries...)
	return v
}
func cloneTombstones(in []TombstoneRecord) []TombstoneRecord {
	out := make([]TombstoneRecord, len(in))
	for i, v := range in {
		out[i] = cloneTombstone(v)
	}
	return out
}

func stateError() error { return fmt.Errorf("%w: persisted state is invalid", ErrInvalid) }
