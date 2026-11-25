package peerprotocol

import (
	"context"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
)

// DurableCommandAuthorizer adapts the local signed OwnerSubject log to the
// command receive boundary. It accepts only a grant chain committed in that
// durable log and re-reads current authority for every command. Until remote
// OwnerSubject log synchronization is available, an absent subject fails
// closed through authority.Store.
type DurableCommandAuthorizer struct {
	Store   *authority.Store
	Resolve authority.AuthorityIdentityResolver
}

func (a DurableCommandAuthorizer) Authorize(ctx context.Context, subjectID, archiveID, snapshotID, action string, audience authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	if ctx == nil || a.Store == nil || a.Resolve == nil || now.IsZero() {
		return ErrCommandDenied
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.Store.AuthorizeChain(subjectID, chain, a.Resolve, now, audience, archiveID, snapshotID, action)
}
