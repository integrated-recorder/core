package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/lease"
)

func TestReplicaTombstoneProviderTargetsResolveCrossIdentityLease(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	localIdentityID := "gi_localidentity000000000"
	providerIdentityID := "gi_provideridentity000000"
	localState, localNode, _ := replicaTransferIdentityFixture(t, localIdentityID, now)
	providerState, _, providerNodeID := replicaTransferIdentityFixture(t, providerIdentityID, now)
	archiveID := "arc_crossidentity00000000"
	item := replicaTransferLeaseFixtureItem("rl_"+"aabbccddeeff0011223344", lease.StatusActive,
		providerNodeID, localNode.NodeID, "os_crossidentity00000000", archiveID,
		"snap_"+"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", now.Add(time.Hour))
	// Registry fork evidence may be retained after a valid explicit resolution;
	// the current refreshed projection, rather than the historical evidence
	// count, determines whether the identity is frozen.
	registered := []globalidentity.RegisteredIdentity{{IdentityID: providerIdentityID, ForkCount: 1, NodeIDs: []string{providerNodeID}}}
	resolved := 0
	resolve := func(identityID, _ string, _ time.Time) (authority.IdentityState, error) {
		resolved++
		if identityID != providerIdentityID {
			return authority.IdentityState{}, errors.New("unexpected identity lookup")
		}
		return providerState, nil
	}
	byArchive, unresolved := replicaTombstoneProviderTargets(
		[]lease.Lease{item}, localIdentityID, localNode.NodeID, localState, registered, resolve, now,
	)
	if got := byArchive[archiveID][providerNodeID]; got != providerIdentityID {
		t.Fatalf("provider identity = %q, want %q; targets=%v", got, providerIdentityID, byArchive)
	}
	if unresolved[archiveID] {
		t.Fatal("verified cross-identity provider was left unresolved")
	}
	if resolved != 1 {
		t.Fatalf("remote identity refresh count=%d, want 1", resolved)
	}
}

func TestReplicaTombstoneProviderTargetsFailClosedForStaleOrAmbiguousIdentity(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	localIdentityID := "gi_localidentity000000000"
	providerIdentityID := "gi_provideridentity000000"
	otherIdentityID := "gi_otheridentity00000000"
	localState, localNode, _ := replicaTransferIdentityFixture(t, localIdentityID, now)
	providerState, _, providerNodeID := replicaTransferIdentityFixture(t, providerIdentityID, now)
	otherState, _, otherNodeID := replicaTransferIdentityFixture(t, otherIdentityID, now)
	archiveID := "arc_crossidentity00000000"
	item := replicaTransferLeaseFixtureItem("rl_"+"112233445566778899aabb", lease.StatusActive,
		providerNodeID, localNode.NodeID, "os_crossidentity00000000", archiveID,
		"snap_"+"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", now.Add(time.Hour))

	t.Run("stale target stays pending", func(t *testing.T) {
		stale := providerState
		stale.FreshUntil = now
		byArchive, unresolved := replicaTombstoneProviderTargets(
			[]lease.Lease{item}, localIdentityID, localNode.NodeID, localState,
			[]globalidentity.RegisteredIdentity{{IdentityID: providerIdentityID, NodeIDs: []string{providerNodeID}}},
			func(string, string, time.Time) (authority.IdentityState, error) { return stale, nil }, now,
		)
		if len(byArchive[archiveID]) != 0 || !unresolved[archiveID] {
			t.Fatalf("stale identity was used: targets=%v unresolved=%v", byArchive, unresolved)
		}
	})

	t.Run("ambiguous node stays pending", func(t *testing.T) {
		// A second verified identity projection claims the same Node ID. The
		// derived key will conflict with its advertised ID and is discarded; a
		// cryptographically valid duplicate is covered by the map's ambiguity
		// handling in production when the same Node public key is delegated twice.
		stateByID := map[string]authority.IdentityState{
			providerIdentityID: providerState,
			otherIdentityID:    otherState,
		}
		registrations := []globalidentity.RegisteredIdentity{
			{IdentityID: providerIdentityID, NodeIDs: []string{providerNodeID}},
			{IdentityID: otherIdentityID, NodeIDs: []string{providerNodeID, otherNodeID}},
		}
		// Duplicate the same currently delegated public key under the second
		// verified identity to model an ambiguous Node principal.
		for key, publicKey := range providerState.CurrentKeys {
			if strings.HasPrefix(key, authority.PrincipalNode+":") && strings.HasSuffix(key, providerNodeID) {
				otherState.CurrentKeys[key] = publicKey
			}
		}
		stateByID[otherIdentityID] = otherState
		byArchive, unresolved := replicaTombstoneProviderTargets(
			[]lease.Lease{item}, localIdentityID, localNode.NodeID, localState, registrations,
			func(identityID, _ string, _ time.Time) (authority.IdentityState, error) {
				state, ok := stateByID[identityID]
				if !ok {
					return authority.IdentityState{}, errors.New("unknown identity")
				}
				return state, nil
			}, now,
		)
		if len(byArchive[archiveID]) != 0 || !unresolved[archiveID] {
			t.Fatalf("ambiguous Node identity was used: targets=%v unresolved=%v", byArchive, unresolved)
		}
	})
}
