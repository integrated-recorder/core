package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
)

const (
	maxTombstoneDeliveryAttemptsPerPass = 16
	tombstoneDeliveryTimeout            = 20 * time.Second
)

type replicaTombstoneDeliveryReconciler struct {
	leases    *lease.Store
	identity  *globalidentity.Store
	registry  *globalidentity.HistoryRegistry
	federated *discovery.FederationClient
	authority *authority.Store
	node      *nodeidentity.Store
	peers     *peerapi.SessionManagerSlot
}

// Reconcile retries the exact persisted tombstone to Nodes with a durable
// replica lease. It never broadcasts archive metadata to unrelated identity
// members and never changes archive bytes. A failed attempt leaves no receipt
// checkpoint, so the next joined maintenance pass can safely retry it.
func (r *replicaTombstoneDeliveryReconciler) Reconcile(ctx context.Context, now time.Time) error {
	if r == nil || ctx == nil || now.IsZero() || r.leases == nil {
		return nil
	}
	leases, tombstones, err := r.leases.Snapshot(ctx)
	if err != nil {
		return errors.New("replica tombstone state is unavailable")
	}
	if len(tombstones) == 0 {
		return nil
	}
	if r.identity == nil || r.registry == nil || r.authority == nil || r.node == nil || r.peers == nil {
		return errors.New("replica tombstone delivery dependencies are unavailable")
	}
	local, initialized, err := r.identity.Snapshot()
	if err != nil || !initialized || local.IdentityID == "" || local.Frozen {
		return errors.New("replica tombstone identity is unavailable")
	}
	remote := authority.NewFederatedIdentityResolver(r.registry, r.federated)
	resolve := authority.IdentityStateResolverFromFederated(r.identity, remote)
	identityState, err := resolve(local.IdentityID, "", now)
	if err != nil || identityState.Frozen || identityState.IdentityID != local.IdentityID || identityState.HeadHash == "" || !identityState.FreshUntil.After(now) {
		return errors.New("replica tombstone identity is stale or frozen")
	}
	nodeInfo, err := r.node.Info()
	if err != nil || nodeInfo.NodeID == "" {
		return errors.New("replica tombstone signer is unavailable")
	}
	localPublicBytes, err := base64.RawURLEncoding.DecodeString(nodeInfo.PublicKey)
	localPublic := ed25519.PublicKey(localPublicBytes)
	if err != nil || len(localPublic) != ed25519.PublicKeySize || !equalPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+nodeInfo.NodeID], localPublic) {
		return errors.New("replica tombstone signer is not currently delegated")
	}
	registered, err := r.registry.List()
	if err != nil {
		return errors.New("replica tombstone identity registry is unavailable")
	}
	providersByArchive, unresolvedProviders := replicaTombstoneProviderTargets(
		leases, local.IdentityID, nodeInfo.NodeID, identityState, registered, resolve, now,
	)
	acknowledged := make(map[string]map[string]struct{}, len(tombstones))
	for _, tombstone := range tombstones {
		for _, delivery := range tombstone.Deliveries {
			if acknowledged[tombstone.Hash] == nil {
				acknowledged[tombstone.Hash] = make(map[string]struct{})
			}
			acknowledged[tombstone.Hash][delivery.NodeID] = struct{}{}
		}
	}
	attempted := 0
	pending := false
	for _, tombstone := range tombstones {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if tombstone.Frozen {
			continue
		}
		decision := tombstone.Signed.Tombstone
		owner, err := r.authority.Snapshot(decision.OwnerSubjectID, resolve, now)
		if err != nil || owner.Frozen || owner.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: local.IdentityID}) || !containsArchiveID(owner.ArchiveIDs, decision.ArchiveID) {
			pending = true
			continue
		}
		chain := tombstone.Signed.GrantChain
		audience := authority.Principal{Kind: authority.PrincipalNode, ID: nodeInfo.NodeID}
		if r.authority.AuthorizeChain(decision.OwnerSubjectID, chain, resolve, now, audience, decision.ArchiveID, "", "archive.delete") != nil ||
			r.authority.AuthorizeChain(decision.OwnerSubjectID, chain, resolve, now, audience, decision.ArchiveID, "", "archive.replicate") != nil {
			pending = true
			continue
		}
		providerSet := providersByArchive[decision.ArchiveID]
		if unresolvedProviders[decision.ArchiveID] {
			pending = true
		}
		targets := make([]string, 0, len(providerSet))
		for target := range providerSet {
			targets = append(targets, target)
		}
		sort.Strings(targets)
		for _, target := range targets {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if _, done := acknowledged[tombstone.Hash][target]; done {
				continue
			}
			if attempted >= maxTombstoneDeliveryAttemptsPerPass {
				return errors.New("replica tombstone delivery pass reached its bounded work limit")
			}
			attempted++
			passCtx, cancel := context.WithTimeout(ctx, tombstoneDeliveryTimeout)
			targetIdentityID := providerSet[target]
			if !peerSessionConnected(r.peers.List(), targetIdentityID, target) {
				connectErr := r.peers.Connect(passCtx, targetIdentityID, target)
				if connectErr != nil && !peerSessionConnected(r.peers.List(), targetIdentityID, target) {
					cancel()
					pending = true
					continue
				}
			}
			scope := peerapi.RequestScope{
				IdentityID: local.IdentityID, IdentityHead: identityState.HeadHash, NodeID: nodeInfo.NodeID,
				TargetIdentityID: targetIdentityID, TargetNodeID: target,
				OwnerSubjectID: decision.OwnerSubjectID, ArchiveID: decision.ArchiveID,
				Action: "archive.replicate", GrantChain: chain,
				Resolve: peerprotocol.IdentityResolver(resolve), Signer: r.node,
			}
			receipt, deliveryErr := r.peers.SubmitTombstoneWithReceipt(passCtx, target, scope, tombstone.Signed)
			cancel()
			if deliveryErr != nil || receipt.Erasure == nil || receipt.Erasure.ArchiveID != decision.ArchiveID || receipt.Erasure.TombstoneHash != tombstone.Hash {
				pending = true
				continue
			}
			reported := make(map[string]struct{}, len(receipt.DeletionReports))
			receiptPending := false
			for _, report := range receipt.DeletionReports {
				if report.Report.ProviderNodeID != target || report.Report.TombstoneHash != tombstone.Hash || report.Report.Status != string(lease.StatusDeleted) {
					receiptPending = true
					break
				}
				if _, err := r.leases.ReportDeletion(ctx, report); err != nil {
					receiptPending = true
					break
				}
				reported[report.Report.LeaseID] = struct{}{}
			}
			if receiptPending {
				pending = true
				continue
			}
			completeReports := true
			for _, item := range leases {
				if item.Offer.Offer.ArchiveID != decision.ArchiveID || item.Offer.Offer.ProviderNodeID != target ||
					(item.Status != lease.StatusTerminating && item.Status != lease.StatusExpired && item.Status != lease.StatusUnconfirmed &&
						!(item.Status == lease.StatusFailed && item.FailureCode != "tombstoned_before_acceptance")) {
					continue
				}
				if _, ok := reported[item.Offer.Offer.LeaseID]; !ok {
					completeReports = false
					break
				}
			}
			if !completeReports {
				pending = true
				continue
			}
			if err := r.leases.MarkTombstoneDelivered(ctx, decision.ArchiveID, target, tombstone.Hash, now); err != nil {
				pending = true
				continue
			}
			if acknowledged[tombstone.Hash] == nil {
				acknowledged[tombstone.Hash] = make(map[string]struct{})
			}
			acknowledged[tombstone.Hash][target] = struct{}{}
		}
	}
	if pending {
		return errors.New("one or more replica tombstone deliveries remain pending")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// replicaTombstoneProviderTargets resolves each durable provider Node to one
// current, verified identity. Registry projections are only candidate indexes:
// every remote identity is refreshed through the federated resolver before it
// can be used as a peer target. Ambiguous, stale, revoked, or unavailable
// identities remain pending instead of receiving a deletion decision.
func replicaTombstoneProviderTargets(
	leases []lease.Lease,
	localIdentityID, localNodeID string,
	localState authority.IdentityState,
	registered []globalidentity.RegisteredIdentity,
	resolve authority.AuthorityIdentityResolver,
	now time.Time,
) (map[string]map[string]string, map[string]bool) {
	providers := make(map[string]map[string]struct{})
	wantedNodes := make(map[string]struct{})
	for _, item := range leases {
		offer := item.Offer.Offer
		if offer.ArchiveID == "" || offer.ProviderNodeID == "" || offer.ProviderNodeID == localNodeID {
			continue
		}
		if providers[offer.ArchiveID] == nil {
			providers[offer.ArchiveID] = make(map[string]struct{})
		}
		providers[offer.ArchiveID][offer.ProviderNodeID] = struct{}{}
		wantedNodes[offer.ProviderNodeID] = struct{}{}
	}

	identityByNode := make(map[string]string)
	ambiguous := make(map[string]struct{})
	addCurrentNodes := func(identityID string, state authority.IdentityState) {
		if !replicaTransferIdentityUsable(state, identityID, now) {
			return
		}
		for keyID, publicKey := range state.CurrentKeys {
			const prefix = authority.PrincipalNode + ":"
			if !strings.HasPrefix(keyID, prefix) {
				continue
			}
			nodeID := strings.TrimPrefix(keyID, prefix)
			if _, wanted := wantedNodes[nodeID]; !wanted {
				continue
			}
			derived, err := identityprotocol.NodeID(publicKey)
			if err != nil || derived != nodeID {
				continue
			}
			if existing := identityByNode[nodeID]; existing != "" && existing != identityID {
				delete(identityByNode, nodeID)
				ambiguous[nodeID] = struct{}{}
				continue
			}
			if _, conflict := ambiguous[nodeID]; !conflict {
				identityByNode[nodeID] = identityID
			}
		}
	}
	addCurrentNodes(localIdentityID, localState)
	for _, item := range registered {
		if item.IdentityID == "" || item.IdentityID == localIdentityID || item.Frozen {
			continue
		}
		candidate := false
		for _, nodeID := range item.NodeIDs {
			if _, wanted := wantedNodes[nodeID]; wanted {
				candidate = true
				break
			}
		}
		if !candidate || resolve == nil {
			continue
		}
		state, err := resolve(item.IdentityID, "", now)
		if err == nil {
			addCurrentNodes(item.IdentityID, state)
		}
	}

	byArchive := make(map[string]map[string]string, len(providers))
	unresolved := make(map[string]bool)
	for archiveID, nodes := range providers {
		for nodeID := range nodes {
			identityID := identityByNode[nodeID]
			if identityID == "" {
				unresolved[archiveID] = true
				continue
			}
			if byArchive[archiveID] == nil {
				byArchive[archiveID] = make(map[string]string)
			}
			byArchive[archiveID][nodeID] = identityID
		}
	}
	return byArchive, unresolved
}

func peerSessionConnected(sessions []peerapi.SessionInfo, identityID, nodeID string) bool {
	for _, session := range sessions {
		if session.IdentityID == identityID && session.NodeID == nodeID {
			return true
		}
	}
	return false
}

func containsArchiveID(archives []string, archiveID string) bool {
	for _, current := range archives {
		if current == archiveID {
			return true
		}
	}
	return false
}
