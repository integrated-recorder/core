package lease

import (
	"context"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
)

// FederatedAuthority refreshes the signed OwnerSubject event history from all
// configured mirrors at every external command boundary before consulting
// the local durable authority store. Mirror agreement is an operational
// freshness signal only; signatures and forks remain verified by authority.Store.
type FederatedAuthority struct {
	Store      *authority.Store
	Federation *discovery.FederationClient
	Resolve    authority.AuthorityIdentityResolver
}

func (a FederatedAuthority) Authorize(ctx context.Context, subjectID, archiveID, snapshotID, action string, audience authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	if ctx == nil || ctx.Err() != nil || a.Store == nil || a.Federation == nil || a.Resolve == nil || now.IsZero() || len(chain) == 0 {
		return ErrDenied
	}
	if err := a.Store.SynchronizeFederatedHistory(ctx, subjectID, a.Federation, a.Resolve, now); err != nil {
		return ErrDenied
	}
	return (DurableAuthority{Store: a.Store, Resolve: a.Resolve}).Authorize(ctx, subjectID, archiveID, snapshotID, action, audience, chain, now)
}
