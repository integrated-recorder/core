package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

var errLeaseIdentityKeyUnavailable = errors.New("replica lease identity key is unavailable")

type leaseIdentityHistory interface {
	Snapshot() (globalidentity.Snapshot, bool, error)
	ExportHistory() (globalidentity.HistoryBundle, error)
	AuthorityProjectionAt(string) (globalidentity.AuthorityProjection, error)
}

type leasePublicHistoryRegistry interface {
	List() ([]globalidentity.RegisteredIdentity, error)
	Get(string) (globalidentity.HistoryBundle, bool, error)
	AuthorityProjectionAt(string, string) (globalidentity.AuthorityProjection, error)
}

// localLeaseKeyResolver verifies lease signatures against the local identity's
// history or another identity's already verified public registry history.
// Lease signatures are historical evidence: later revocation does not
// invalidate a committed signature. Current authorization and fork checks
// remain the Authorizer's job.
type localLeaseKeyResolver struct {
	identity leaseIdentityHistory
	registry leasePublicHistoryRegistry
}

func (r localLeaseKeyResolver) VerifiedKey(ctx context.Context, principal authority.Principal, at time.Time) (ed25519.PublicKey, error) {
	if ctx == nil || ctx.Err() != nil || r.identity == nil || at.IsZero() || at.Location() != time.UTC || principal.ID == "" {
		return nil, errLeaseIdentityKeyUnavailable
	}
	snapshot, initialized, err := r.identity.Snapshot()
	if err != nil || !initialized || snapshot.IdentityID == "" {
		return nil, errLeaseIdentityKeyUnavailable
	}
	localBundle, err := r.identity.ExportHistory()
	if err != nil || localBundle.IdentityID != snapshot.IdentityID || len(localBundle.Events) == 0 {
		return nil, errLeaseIdentityKeyUnavailable
	}

	identityIDs := []string{snapshot.IdentityID}
	if principal.Kind == authority.PrincipalIdentity {
		identityIDs = []string{principal.ID}
	} else if r.registry != nil {
		registered, listErr := r.registry.List()
		if listErr != nil {
			return nil, errLeaseIdentityKeyUnavailable
		}
		for _, item := range registered {
			if item.IdentityID != "" && item.IdentityID != snapshot.IdentityID {
				identityIDs = append(identityIDs, item.IdentityID)
			}
		}
	}

	var selected ed25519.PublicKey
	for _, identityID := range identityIDs {
		if err := ctx.Err(); err != nil {
			return nil, errLeaseIdentityKeyUnavailable
		}
		bundle := localBundle
		local := identityID == snapshot.IdentityID
		if !local {
			if principal.Kind == authority.PrincipalIdentity && identityID != principal.ID {
				continue
			}
			if r.registry == nil {
				continue
			}
			var found bool
			bundle, found, err = r.registry.Get(identityID)
			if err != nil || !found || bundle.IdentityID != identityID {
				return nil, errLeaseIdentityKeyUnavailable
			}
		}
		projection, projectionErr := leaseHistoryProjectionAt(ctx, bundle, at, func(head string) (globalidentity.AuthorityProjection, error) {
			if local {
				return r.identity.AuthorityProjectionAt(head)
			}
			return r.registry.AuthorityProjectionAt(identityID, head)
		})
		if projectionErr != nil {
			continue
		}
		var candidate ed25519.PublicKey
		switch principal.Kind {
		case authority.PrincipalIdentity:
			if identityID == principal.ID && projection.IdentityID == principal.ID && len(projection.RootPublicKey) == ed25519.PublicKeySize {
				candidate = projection.RootPublicKey
			}
		case authority.PrincipalNode:
			for _, node := range projection.ActiveNodes {
				if node.NodeID == principal.ID && len(node.PublicKey) == ed25519.PublicKeySize {
					derived, deriveErr := identityprotocol.NodeID(node.PublicKey)
					if deriveErr == nil && derived == principal.ID {
						candidate = node.PublicKey
					}
					break
				}
			}
		}
		if len(candidate) == 0 {
			continue
		}
		if len(selected) > 0 && !selected.Equal(candidate) {
			return nil, errLeaseIdentityKeyUnavailable
		}
		selected = append(ed25519.PublicKey(nil), candidate...)
	}
	if len(selected) != ed25519.PublicKeySize {
		return nil, errLeaseIdentityKeyUnavailable
	}
	return selected, nil
}

func leaseHistoryProjectionAt(
	ctx context.Context,
	bundle globalidentity.HistoryBundle,
	at time.Time,
	projectionAt func(string) (globalidentity.AuthorityProjection, error),
) (globalidentity.AuthorityProjection, error) {
	if ctx == nil || at.IsZero() || at.Location() != time.UTC || bundle.IdentityID == "" || len(bundle.Events) == 0 || projectionAt == nil {
		return globalidentity.AuthorityProjection{}, errLeaseIdentityKeyUnavailable
	}
	var selectedHash string
	var previous time.Time
	for _, item := range bundle.Events {
		if err := ctx.Err(); err != nil {
			return globalidentity.AuthorityProjection{}, errLeaseIdentityKeyUnavailable
		}
		canonical, decodeErr := base64.RawURLEncoding.DecodeString(item.CanonicalEvent)
		signature, signatureErr := base64.RawURLEncoding.DecodeString(item.Signature)
		event, verifyErr := identityprotocol.VerifyEvent(canonical, signature)
		if decodeErr != nil || signatureErr != nil || base64.RawURLEncoding.EncodeToString(canonical) != item.CanonicalEvent ||
			base64.RawURLEncoding.EncodeToString(signature) != item.Signature || verifyErr != nil || event.IdentityID != bundle.IdentityID ||
			(!previous.IsZero() && event.IssuedAt.Before(previous)) {
			return globalidentity.AuthorityProjection{}, errLeaseIdentityKeyUnavailable
		}
		if event.IssuedAt.Equal(at) {
			// Event ordering cannot prove which key was authoritative when an
			// identity event and lease signature share the same second.
			return globalidentity.AuthorityProjection{}, errLeaseIdentityKeyUnavailable
		}
		if event.IssuedAt.Before(at) {
			selectedHash = event.EventHash
		}
		previous = event.IssuedAt
	}
	if selectedHash == "" {
		return globalidentity.AuthorityProjection{}, errLeaseIdentityKeyUnavailable
	}
	projection, err := projectionAt(selectedHash)
	if err != nil || projection.IdentityID != bundle.IdentityID {
		return globalidentity.AuthorityProjection{}, errLeaseIdentityKeyUnavailable
	}
	return projection, nil
}
