package authority

import (
	"context"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
)

const identityFederationRefreshTimeout = 10 * time.Second

// FederatedIdentityResolver obtains remote signed history from every
// configured HTTPS mirror for each authorization decision and accepts only an
// exact agreement. Discovery remains a transport mirror, never a signer.
type FederatedIdentityResolver struct {
	registry   *globalidentity.HistoryRegistry
	federation *discovery.FederationClient
}

func NewFederatedIdentityResolver(registry *globalidentity.HistoryRegistry, federation *discovery.FederationClient) *FederatedIdentityResolver {
	return &FederatedIdentityResolver{registry: registry, federation: federation}
}

// Refresh fetches from all configured providers, imports each valid history
// before deciding whether to authorize, and leaves divergent or forked state
// frozen in the durable local registry. It never selects one branch on a
// mismatch. Provider agreement is operational freshness evidence only and
// assumes the configured operators are not all colluding to replay stale data.
func (r *FederatedIdentityResolver) Refresh(ctx context.Context, identityID string, now time.Time) error {
	if r == nil || r.registry == nil || r.federation == nil || ctx == nil || identityID == "" || now.IsZero() {
		return ErrStaleIdentity
	}
	lookup, lookupErr := r.federation.LookupIdentityHistory(ctx, identityID, now)
	var importErr error
	for _, bundle := range lookup.Bundles {
		if err := r.registry.Import(bundle); err != nil && !errors.Is(err, globalidentity.ErrHistoryStale) {
			importErr = errors.Join(importErr, err)
		}
	}
	if importErr != nil {
		if projection, err := r.registry.AuthorityProjection(identityID); err == nil && projection.Frozen {
			return ErrFrozenIdentity
		}
	}
	if lookupErr != nil || importErr != nil || !lookup.Complete || !lookup.Agreed || lookup.Summary.IdentityID != identityID {
		return ErrStaleIdentity
	}
	projection, err := r.registry.AuthorityProjection(identityID)
	if err != nil {
		return ErrStaleIdentity
	}
	if projection.Frozen {
		return ErrFrozenIdentity
	}
	if projection.HeadHash != lookup.Summary.HeadHash {
		return ErrStaleIdentity
	}
	return nil
}

// Resolve implements AuthorityIdentityResolver for remote identities. Each
// authorization decision refreshes every configured provider; a previous
// successful lookup is not a lease and cannot mask a newly published revoke,
// fork, or provider outage. Local recording remains independent of this path.
func (r *FederatedIdentityResolver) Resolve(identityID, headHash string, now time.Time) (IdentityState, error) {
	if r == nil || r.registry == nil || r.federation == nil || identityID == "" || now.IsZero() {
		return IdentityState{}, ErrStaleIdentity
	}
	ctx, cancel := context.WithTimeout(context.Background(), identityFederationRefreshTimeout)
	defer cancel()
	if err := r.Refresh(ctx, identityID, now); err != nil {
		return IdentityState{}, err
	}
	current, err := r.registry.AuthorityProjection(identityID)
	if err != nil {
		return IdentityState{}, ErrStaleIdentity
	}
	if current.Frozen {
		return IdentityState{}, ErrFrozenIdentity
	}
	projection := current
	if headHash != "" && headHash != current.HeadHash {
		projection, err = r.registry.AuthorityProjectionAt(identityID, headHash)
		if err != nil {
			return IdentityState{}, ErrStaleIdentity
		}
	}
	if projection.Frozen {
		return IdentityState{}, ErrFrozenIdentity
	}
	projection.VerifiedAt = now.UTC()
	return identityStateFromProjection(projection, now)
}

func IdentityStateResolverFromFederated(local *globalidentity.Store, remote *FederatedIdentityResolver) AuthorityIdentityResolver {
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
		return remote.Resolve(identityID, headHash, now)
	}
}
