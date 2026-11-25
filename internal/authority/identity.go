package authority

import (
	"crypto/ed25519"
	"time"

	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

// IdentityStateFromStore builds the verifier's active-key and freshness view
// from the local Global Identity store. The store validates its entire durable
// event history under lock before producing this public-only projection.
// Revocation state for capabilities remains a separate verified authority-log
// input because OwnerSubject policy is not the Global Identity key history.
func IdentityStateFromStore(store *globalidentity.Store, now time.Time) (IdentityState, error) {
	if store == nil || now.IsZero() {
		return IdentityState{}, ErrInvalid
	}
	projection, err := store.AuthorityProjection()
	if err != nil {
		return IdentityState{}, err
	}
	// AuthorityProjection records its verification timestamp after acquiring
	// and validating the durable store. Callers commonly capture now before
	// invoking this function; clamp that small read-side clock skew to the
	// caller's evaluation instant instead of rejecting a freshly verified
	// identity as being "from the future". This is conservative for freshness.
	if projection.VerifiedAt.After(now) {
		projection.VerifiedAt = now
	}
	return identityStateFromProjection(projection, now)
}

// IdentityStateFromStoreAt constructs a historical verification key set from
// a known identity event head. It must only be used to verify that event-time
// signatures were valid; current authorization still requires
// IdentityStateFromStore and current revocation/fork checks.
func IdentityStateFromStoreAt(store *globalidentity.Store, headHash string, now time.Time) (IdentityState, error) {
	if store == nil || now.IsZero() || headHash == "" {
		return IdentityState{}, ErrInvalid
	}
	projection, err := store.AuthorityProjectionAt(headHash)
	if err != nil {
		return IdentityState{}, err
	}
	if projection.VerifiedAt.After(now) {
		projection.VerifiedAt = now
	}
	return identityStateFromProjection(projection, now)
}

// IdentityStateResolverFromStores creates a resolver for the supplied verified
// local identity histories. A request for an unknown identity or unknown head
// fails closed; network Discovery results are not treated as identity trust.
func IdentityStateResolverFromStores(stores map[string]*globalidentity.Store) AuthorityIdentityResolver {
	return func(identityID, headHash string, now time.Time) (IdentityState, error) {
		store := stores[identityID]
		if store == nil {
			return IdentityState{}, ErrStaleIdentity
		}
		if headHash == "" {
			return IdentityStateFromStore(store, now)
		}
		return IdentityStateFromStoreAt(store, headHash, now)
	}
}

// IdentityStateResolverFromRegistry intentionally fails closed. A locally
// imported signed history proves that its events were signed, not that the
// imported head is current. The durable import timestamp is not independent
// freshness evidence, so it must never authorize remote capabilities. This
// resolver can be enabled only after authenticated multi-source freshness
// synchronization is implemented.
func IdentityStateResolverFromRegistry(registry *globalidentity.HistoryRegistry) AuthorityIdentityResolver {
	return func(identityID, headHash string, now time.Time) (IdentityState, error) {
		if registry == nil {
			return IdentityState{}, ErrStaleIdentity
		}
		return IdentityState{}, ErrStaleIdentity
	}
}

// IdentityStateResolverFromLocalAndRegistry gives the local Global Identity
// store authority over its own identity. Other IDs remain fail-closed until a
// freshness proof from authenticated independent history sources is available.
func IdentityStateResolverFromLocalAndRegistry(local *globalidentity.Store, registry *globalidentity.HistoryRegistry) AuthorityIdentityResolver {
	registryResolver := IdentityStateResolverFromRegistry(registry)
	return func(identityID, headHash string, now time.Time) (IdentityState, error) {
		if local != nil {
			snapshot, initialized, err := local.Snapshot()
			if err != nil {
				return IdentityState{}, ErrStaleIdentity
			}
			if initialized && snapshot.IdentityID == identityID {
				if headHash == "" {
					return IdentityStateFromStore(local, now)
				}
				return IdentityStateFromStoreAt(local, headHash, now)
			}
		}
		return registryResolver(identityID, headHash, now)
	}
}

func identityStateFromProjection(projection globalidentity.AuthorityProjection, now time.Time) (IdentityState, error) {
	if projection.VerifiedAt.IsZero() || now.Before(projection.VerifiedAt) || now.Sub(projection.VerifiedAt) > maxIdentityFreshness {
		return IdentityState{}, ErrStaleIdentity
	}
	identityID, err := identityprotocol.IdentityID(projection.RootPublicKey)
	if err != nil || identityID != projection.IdentityID || !identityprotocolHash(projection.HeadHash) {
		return IdentityState{}, ErrInvalid
	}
	keys := make(map[string]ed25519.PublicKey, len(projection.ActiveNodes)+1)
	rootPrincipal := Principal{Kind: PrincipalIdentity, ID: projection.IdentityID}
	keys[rootPrincipal.key()] = append(ed25519.PublicKey(nil), projection.RootPublicKey...)
	for _, node := range projection.ActiveNodes {
		derived, err := identityprotocol.NodeID(node.PublicKey)
		if err != nil || derived != node.NodeID {
			return IdentityState{}, ErrInvalid
		}
		principal := Principal{Kind: PrincipalNode, ID: node.NodeID}
		keys[principal.key()] = append(ed25519.PublicKey(nil), node.PublicKey...)
	}
	return IdentityState{
		IdentityID: projection.IdentityID, HeadHash: projection.HeadHash,
		VerifiedAt: projection.VerifiedAt, FreshUntil: projection.VerifiedAt.Add(maxIdentityFreshness),
		Frozen: projection.Frozen, CurrentKeys: keys,
		RecoveryEncryptionPublicKey: append([]byte(nil), projection.RecoveryEncryptionPublicKey...),
	}, nil
}
