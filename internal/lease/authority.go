package lease

import (
	"bytes"
	"context"
	"reflect"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
)

// DurableAuthority binds replica management commands to the current
// OwnerSubject log and an exact grant chain already present in that log.
// Peer-supplied grants are never trusted merely because their signatures are
// valid; the chain must also be committed in the durable, unfrozen subject log.
type DurableAuthority struct {
	Store   *authority.Store
	Resolve authority.AuthorityIdentityResolver
}

func (a DurableAuthority) Authorize(ctx context.Context, subjectID, archiveID, snapshotID, action string, audience authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.Store == nil || a.Resolve == nil || now.IsZero() || len(chain) == 0 {
		return ErrDenied
	}
	subject, err := a.Store.Snapshot(subjectID, a.Resolve, now)
	if err != nil || subject.Frozen {
		return ErrDenied
	}
	identity, err := a.Resolve(subject.Subject.Controller.ID, "", now)
	if err != nil || identity.Frozen {
		return ErrDenied
	}
	verification := authority.VerificationContext{
		Subject: subject.Subject, SubjectPolicyEpoch: subject.Subject.PolicyEpoch,
		RevokedGrantHashes: subject.RevokedHashes, Identity: identity, Now: now,
	}
	if err := authority.VerifyGrantChain(verification, chain, audience, archiveID, snapshotID, action); err != nil {
		return ErrDenied
	}
	lastHash, err := authority.GrantHash(chain[len(chain)-1].Grant)
	if err != nil {
		return ErrDenied
	}
	committed, ok := subject.Grants[lastHash]
	if !ok {
		return ErrDenied
	}
	if equalGrantChains(committed, chain) {
		return nil
	}
	return ErrDenied
}

func equalGrantChains(a, b []authority.SignedGrant) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i].Grant, b[i].Grant) || !bytes.Equal(a[i].Signature, b[i].Signature) {
			return false
		}
	}
	return true
}
