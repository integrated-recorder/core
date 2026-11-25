package server

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/replication"
)

var replicaLeaseIDPattern = regexp.MustCompile(`^rl_[A-Za-z0-9_-]{22}$`)
var replicaNodeIDPattern = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
var replicaArchiveIDPattern = regexp.MustCompile(`^arc_[A-Za-z0-9_-]{22}$`)
var replicaSnapshotIDPattern = regexp.MustCompile(`^snap_[a-f0-9]{64}$`)

func validReplicaLeaseID(value string) bool { return replicaLeaseIDPattern.MatchString(value) }

type replicaLeaseSummary struct {
	LeaseID         string       `json:"lease_id"`
	ArchiveID       string       `json:"archive_id"`
	SnapshotID      string       `json:"snapshot_id"`
	KeyEpoch        string       `json:"key_epoch"`
	OwnerSubjectID  string       `json:"owner_subject_id"`
	GrantHash       string       `json:"grant_hash"`
	ProviderNodeID  string       `json:"provider_node_id"`
	Status          lease.Status `json:"status"`
	Revision        string       `json:"revision"`
	ExpiresAt       string       `json:"expires_at"`
	ReservedBytes   string       `json:"reserved_bytes"`
	ObjectCount     int          `json:"object_count"`
	CompletedObject int          `json:"completed_objects"`
}

type replicaTombstoneSummary struct {
	ArchiveID         string   `json:"archive_id"`
	DeletionEpoch     string   `json:"deletion_epoch"`
	IssuedAt          string   `json:"issued_at"`
	Frozen            bool     `json:"frozen"`
	AcknowledgedNodes []string `json:"acknowledged_nodes"`
}

func (s *Server) registerReplicaLeaseRoutes() {
	s.mux.HandleFunc("GET /api/replica-leases", s.replicaLeasesList)
	s.mux.HandleFunc("POST /api/replica-tombstones", s.replicaTombstoneSubmit)
	s.mux.HandleFunc("POST /api/replica-leases/{leaseID}/verify", s.replicaLeaseVerify)
	s.mux.HandleFunc("POST /api/replica-imports", s.replicaImport)
}

type replicaTombstoneRequest struct {
	OwnerSubjectID string `json:"owner_subject_id"`
	ArchiveID      string `json:"archive_id"`
	GrantHash      string `json:"grant_hash"`
	ReasonCode     string `json:"reason_code,omitempty"`
}

type replicaTombstonePeerResult struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"` // durable_receipt or pending; never proof of physical erasure
}

type replicaTombstoneResponse struct {
	ArchiveID        string                       `json:"archive_id"`
	DeletionEpoch    string                       `json:"deletion_epoch"`
	TombstoneHash    string                       `json:"tombstone_hash"`
	Frozen           bool                         `json:"frozen"`
	LocalDecision    string                       `json:"local_decision"`
	PhysicalDeletion bool                         `json:"physical_deletion_performed"`
	Peers            []replicaTombstonePeerResult `json:"peers"`
}

type replicaTombstoneSubmitter interface {
	SubmitTombstone(context.Context, string, peerapi.RequestScope, lease.SignedTombstone) error
}

type replicaTombstoneStore interface {
	replicaLeaseStore
	CreateTombstone(context.Context, string, string, string, time.Time, func(lease.Tombstone) (lease.SignedTombstone, error)) (lease.TombstoneRecord, error)
	MarkTombstoneDelivered(context.Context, string, string, string, time.Time) error
}

// replicaTombstoneSubmit records an Owner-authorized archive deletion
// decision locally, then attempts delivery of that exact durable event only to
// currently delegated Nodes recorded as replica providers for this archive.
// The operation never deletes media bytes.
func (s *Server) replicaTombstoneSubmit(w http.ResponseWriter, r *http.Request) {
	principal, authenticated := authn.PrincipalFromContext(r.Context())
	if !authenticated || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if !authn.HasPermission(principal, authn.PermissionRecordingDelete) {
		writeCodedError(w, http.StatusForbidden, "permission_denied", "Permission denied.")
		return
	}
	if !secureBrowserRequest(r) {
		writeCodedError(w, http.StatusUpgradeRequired, "https_required", "Archive deletion decisions require HTTPS.")
		return
	}
	var request replicaTombstoneRequest
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil || request.OwnerSubjectID == "" ||
		!replicaArchiveIDPattern.MatchString(request.ArchiveID) || !validReplicaGrantHash(request.GrantHash) {
		writeCodedError(w, http.StatusBadRequest, "replica_tombstone_invalid", "A claimed archive, OwnerSubject, and current delete-and-replicate capability are required.")
		return
	}
	if request.ReasonCode == "" {
		request.ReasonCode = "owner_requested"
	}
	if request.ReasonCode != "owner_requested" && request.ReasonCode != "policy_retention" {
		writeCodedError(w, http.StatusBadRequest, "replica_tombstone_invalid", "The archive deletion reason is not supported.")
		return
	}
	store, storeOK := s.replicaLeases.(replicaTombstoneStore)
	if !storeOK || s.nodeIdentity == nil || s.globalIdentity == nil || s.ownerAuthority == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_tombstone_unavailable", "Durable archive authority is not configured on this Control Plane.")
		return
	}
	identityID, identityHead, nodeID, chain, resolver, targets, ok := s.replicaTombstoneAuthority(w, r, request)
	if !ok {
		return
	}
	now := time.Now().UTC()
	record, err := store.CreateTombstone(r.Context(), request.OwnerSubjectID, request.ArchiveID, request.ReasonCode, now,
		func(tombstone lease.Tombstone) (lease.SignedTombstone, error) {
			return s.nodeIdentity.SignReplicaTombstone(tombstone, chain)
		})
	if err != nil {
		switch {
		case errors.Is(err, lease.ErrFrozen):
			writeCodedError(w, http.StatusConflict, "replica_tombstone_frozen", "Conflicting deletion decisions froze this archive; manual authority review is required.")
		case errors.Is(err, lease.ErrConflict), errors.Is(err, lease.ErrTransition):
			writeCodedError(w, http.StatusConflict, "replica_tombstone_conflict", "The archive deletion state changed; refresh authority and retry.")
		case errors.Is(err, lease.ErrDenied):
			writeCodedError(w, http.StatusForbidden, "replica_tombstone_denied", "The current archive capability does not authorize this action.")
		default:
			writeCodedError(w, http.StatusServiceUnavailable, "replica_tombstone_store_failed", "The archive deletion decision could not be persisted safely.")
		}
		return
	}
	if !s.auditMutation(w, r, "replica_tombstone_recorded", request.ArchiveID) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "The deletion decision is durable locally, but audit recording failed; retry the same request to resume delivery.")
		return
	}
	results := make([]replicaTombstonePeerResult, 0, len(targets))
	submitter, canSubmit := s.peerSessions.(replicaTombstoneSubmitter)
	for _, target := range targets {
		result := replicaTombstonePeerResult{NodeID: target, Status: "pending"}
		if canSubmit && s.peerSessionsAvailable() {
			scope := peerapi.RequestScope{
				IdentityID: identityID, IdentityHead: identityHead, NodeID: nodeID,
				TargetIdentityID: identityID, TargetNodeID: target,
				OwnerSubjectID: request.OwnerSubjectID, ArchiveID: request.ArchiveID,
				Action: "archive.replicate", GrantChain: chain,
				Resolve: peerprotocol.IdentityResolver(resolver), Signer: s.nodeIdentity,
			}
			ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
			err := submitter.SubmitTombstone(ctx, target, scope, record.Signed)
			cancel()
			if err == nil && store.MarkTombstoneDelivered(r.Context(), request.ArchiveID, target, record.Hash, time.Now().UTC()) == nil {
				result.Status = "durable_receipt"
			}
		}
		results = append(results, result)
	}
	writeJSON(w, http.StatusOK, replicaTombstoneResponse{
		ArchiveID: request.ArchiveID, DeletionEpoch: record.Signed.Tombstone.DeletionEpoch,
		TombstoneHash: record.Hash, Frozen: record.Frozen, LocalDecision: "durable",
		PhysicalDeletion: false, Peers: results,
	})
}

func (s *Server) replicaTombstoneAuthority(w http.ResponseWriter, r *http.Request, request replicaTombstoneRequest) (string, string, string, []authority.SignedGrant, authority.AuthorityIdentityResolver, []string, bool) {
	localIdentity, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || localIdentity.Frozen || localIdentity.IdentityID == "" {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_tombstone_identity_unavailable", "Current identity authority is unavailable or frozen.")
		return "", "", "", nil, nil, nil, false
	}
	resolver := authority.IdentityStateResolverFromLocalAndRegistry(s.globalIdentity, s.globalIdentityRegistry)
	if s.federatedIdentityResolver != nil {
		resolver = authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	}
	now := time.Now().UTC()
	identityState, err := resolver(localIdentity.IdentityID, "", now)
	if err != nil || identityState.IdentityID != localIdentity.IdentityID || identityState.Frozen || identityState.HeadHash == "" || !identityState.FreshUntil.After(now) {
		writeCodedError(w, http.StatusForbidden, "replica_tombstone_identity_stale", "Current identity authority is not fresh enough to authorize this action.")
		return "", "", "", nil, nil, nil, false
	}
	node, err := s.nodeIdentity.Info()
	localPublic, decodeErr := base64.RawURLEncoding.DecodeString(node.PublicKey)
	if err != nil || decodeErr != nil || len(localPublic) != 32 || node.NodeID == "" ||
		!equalPeerPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], localPublic) {
		writeCodedError(w, http.StatusForbidden, "replica_tombstone_node_unauthorized", "The local Node is not a current member of this identity.")
		return "", "", "", nil, nil, nil, false
	}
	ownerState, err := s.ownerAuthority.Snapshot(request.OwnerSubjectID, resolver, now)
	if err != nil || ownerState.Frozen || ownerState.Subject.Controller.Kind != authority.PrincipalIdentity ||
		ownerState.Subject.Controller.ID != localIdentity.IdentityID || !archiveClaimed(ownerState.ArchiveIDs, request.ArchiveID) {
		writeCodedError(w, http.StatusForbidden, "replica_tombstone_owner_unavailable", "The OwnerSubject does not currently control this claimed archive.")
		return "", "", "", nil, nil, nil, false
	}
	chain, exists := ownerState.Grants[request.GrantHash]
	if !exists || len(chain) == 0 {
		writeCodedError(w, http.StatusForbidden, "replica_tombstone_capability_invalid", "The current delete-and-replicate capability is not present in authority history.")
		return "", "", "", nil, nil, nil, false
	}
	audience := authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}
	leaf := chain[len(chain)-1].Grant
	if leaf.SubjectID != request.OwnerSubjectID || leaf.Audience != audience || leaf.ArchiveID != request.ArchiveID || leaf.SnapshotID != "" ||
		!containsPeerAction(leaf.Actions, "archive.delete") || !containsPeerAction(leaf.Actions, "archive.replicate") ||
		s.ownerAuthority.AuthorizeChain(request.OwnerSubjectID, chain, resolver, now, audience, request.ArchiveID, "", "archive.delete") != nil ||
		s.ownerAuthority.AuthorizeChain(request.OwnerSubjectID, chain, resolver, now, audience, request.ArchiveID, "", "archive.replicate") != nil {
		writeCodedError(w, http.StatusForbidden, "replica_tombstone_capability_invalid", "The capability is expired, revoked, stale, or does not authorize archive deletion and replication.")
		return "", "", "", nil, nil, nil, false
	}
	// A deletion decision contains archive identity and authority metadata.
	// Deliver only to currently delegated Nodes that this durable store knows
	// hold or were offered a replica, rather than disclosing it to every Node
	// in the Global Identity.
	leases, _, err := s.replicaLeases.Snapshot(r.Context())
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_tombstone_store_failed", "Replica delivery targets could not be read safely.")
		return "", "", "", nil, nil, nil, false
	}
	delegated := make(map[string]struct{})
	for keyID, publicKey := range identityState.CurrentKeys {
		prefix := authority.PrincipalNode + ":"
		if !strings.HasPrefix(keyID, prefix) {
			continue
		}
		delegatedNodeID := strings.TrimPrefix(keyID, prefix)
		verifiedNodeID, keyErr := identityprotocol.NodeID(publicKey)
		if keyErr == nil && verifiedNodeID == delegatedNodeID && delegatedNodeID != node.NodeID {
			delegated[delegatedNodeID] = struct{}{}
		}
	}
	targets := replicaTombstoneTargets(leases, request.ArchiveID, node.NodeID, delegated)
	return localIdentity.IdentityID, identityState.HeadHash, node.NodeID, append([]authority.SignedGrant(nil), chain...), resolver, targets, true
}

func replicaTombstoneTargets(leases []lease.Lease, archiveID, localNodeID string, delegated map[string]struct{}) []string {
	targetSet := make(map[string]struct{})
	for _, current := range leases {
		offer := current.Offer.Offer
		if offer.ArchiveID != archiveID || offer.ProviderNodeID == localNodeID || !replicaNodeIDPattern.MatchString(offer.ProviderNodeID) {
			continue
		}
		if _, isDelegated := delegated[offer.ProviderNodeID]; isDelegated {
			targetSet[offer.ProviderNodeID] = struct{}{}
		}
	}
	targets := make([]string, 0, len(targetSet))
	for target := range targetSet {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	return targets
}

func archiveClaimed(archives []string, archiveID string) bool {
	for _, current := range archives {
		if current == archiveID {
			return true
		}
	}
	return false
}

type replicaImportRequest struct {
	SourceNodeID   string `json:"source_node_id"`
	OwnerSubjectID string `json:"owner_subject_id"`
	GrantHash      string `json:"grant_hash"`
	ArchiveID      string `json:"archive_id"`
	SnapshotID     string `json:"snapshot_id"`
	LeaseID        string `json:"lease_id"`
}

type peerReplicaImporter interface {
	ImportReplicaFromPeer(context.Context, string, peerapi.RequestScope, *lease.Store, *replication.Store,
		peerapi.ReplicaAcceptanceSigner, replication.ReplicaCheckpointSigner, string, string, string) (peerapi.ReplicaImportResult, error)
}

type replicaImportResponse struct {
	Lease      replicaLeaseSummary `json:"lease"`
	SnapshotID string              `json:"snapshot_id"`
	Objects    int                 `json:"objects"`
}

// replicaImport is an explicit owner-authorized provider action. It accepts
// only the named source offer and uses durable lease/checkpoint state to resume
// after interruptions; offers are never accepted automatically on arrival.
func (s *Server) replicaImport(w http.ResponseWriter, r *http.Request) {
	principal, authenticated := authn.PrincipalFromContext(r.Context())
	if !authenticated || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if !authn.HasPermission(principal, authn.PermissionRecordingExport) {
		writeCodedError(w, http.StatusForbidden, "permission_denied", "Permission denied.")
		return
	}
	if !secureBrowserRequest(r) {
		writeCodedError(w, http.StatusUpgradeRequired, "https_required", "Replica import requires HTTPS.")
		return
	}
	var request replicaImportRequest
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil ||
		!replicaNodeIDPattern.MatchString(request.SourceNodeID) || request.OwnerSubjectID == "" ||
		!validReplicaGrantHash(request.GrantHash) || !replicaArchiveIDPattern.MatchString(request.ArchiveID) ||
		!replicaSnapshotIDPattern.MatchString(request.SnapshotID) || !validReplicaLeaseID(request.LeaseID) {
		writeCodedError(w, http.StatusBadRequest, "replica_import_invalid", "A source, current capability, archive, snapshot, and lease are required.")
		return
	}
	leases, leasesOK := s.replicaLeases.(*lease.Store)
	importer, importerOK := s.peerSessions.(peerReplicaImporter)
	if !leasesOK || importer == nil || !importerOK || s.replicaCiphertexts == nil || s.nodeIdentity == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_import_unavailable", "Replica import is not configured on this Control Plane.")
		return
	}
	scope, ok := s.replicaImportScope(w, r, request)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	releaseImport, gateErr := s.replicaImportGate.Acquire(ctx, request.LeaseID)
	if gateErr != nil {
		writeCodedError(w, http.StatusGatewayTimeout, "replica_import_interrupted", "Replica import did not start; retry the same lease to resume from durable progress.")
		return
	}
	defer releaseImport()
	result, err := importer.ImportReplicaFromPeer(ctx, request.SourceNodeID, scope, leases, s.replicaCiphertexts,
		s.nodeIdentity, s.nodeIdentity, request.ArchiveID, request.SnapshotID, request.LeaseID)
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			writeCodedError(w, http.StatusGatewayTimeout, "replica_import_interrupted", "Replica import did not finish; retry the same lease to resume from durable progress.")
		case errors.Is(err, lease.ErrCapacityExceeded):
			writeCodedError(w, http.StatusInsufficientStorage, "replica_provider_capacity_exceeded", "This Node's configured replica storage capacity does not have room for this offer.")
		case errors.Is(err, lease.ErrProviderIdentity):
			writeCodedError(w, http.StatusServiceUnavailable, "replica_provider_identity_unavailable", "This Node cannot safely reserve replica capacity until its Node identity is available.")
		case errors.Is(err, lease.ErrDenied), errors.Is(err, lease.ErrFrozen), errors.Is(err, peerprotocol.ErrCommandDenied):
			writeCodedError(w, http.StatusForbidden, "replica_import_denied", "Current archive authority does not authorize this import.")
		case errors.Is(err, lease.ErrConflict), errors.Is(err, lease.ErrTransition), errors.Is(err, replication.ErrUnauthorized):
			writeCodedError(w, http.StatusConflict, "replica_import_conflict", "The lease or archive state is not ready for this import.")
		default:
			writeCodedError(w, http.StatusBadGateway, "replica_import_failed", "The encrypted replica could not be imported safely; the durable lease can be retried.")
		}
		return
	}
	if result.Lease.Status != lease.StatusActive || result.Lease.OfferHash == "" {
		writeCodedError(w, http.StatusBadGateway, "replica_import_incomplete", "The encrypted replica did not reach verified active state; retry the same lease to resume safely.")
		return
	}
	s.replicaImportGate.MarkSourceVerified(request.LeaseID, result.Lease.OfferHash)
	if !s.auditMutation(w, r, "replica_import_completed", request.LeaseID) {
		return
	}
	writeJSON(w, http.StatusOK, replicaImportResponse{
		Lease: summarizeReplicaLease(result.Lease), SnapshotID: result.Snapshot.SnapshotID, Objects: len(result.Bundle.Objects),
	})
}

func validReplicaGrantHash(value string) bool {
	if len(value) != len("sha256:")+64 || value[:len("sha256:")] != "sha256:" {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func (s *Server) replicaImportScope(w http.ResponseWriter, r *http.Request, request replicaImportRequest) (peerapi.RequestScope, bool) {
	if s.ownerAuthority == nil || s.globalIdentity == nil || s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_import_authority_unavailable", "Current archive authority is unavailable.")
		return peerapi.RequestScope{}, false
	}
	localIdentity, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || localIdentity.Frozen || localIdentity.IdentityID == "" {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_import_identity_unavailable", "Current identity authority is unavailable or frozen.")
		return peerapi.RequestScope{}, false
	}
	node, err := s.nodeIdentity.Info()
	if err != nil || node.NodeID == "" || request.SourceNodeID == node.NodeID {
		writeCodedError(w, http.StatusBadRequest, "replica_import_peer_invalid", "The source must be a different currently delegated Node.")
		return peerapi.RequestScope{}, false
	}
	resolve := authority.IdentityStateResolverFromLocalAndRegistry(s.globalIdentity, s.globalIdentityRegistry)
	if s.federatedIdentityResolver != nil {
		resolve = authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	}
	now := time.Now().UTC()
	identityState, err := resolve(localIdentity.IdentityID, "", now)
	if err != nil || identityState.IdentityID != localIdentity.IdentityID || identityState.Frozen || identityState.HeadHash == "" {
		writeCodedError(w, http.StatusForbidden, "replica_import_node_unauthorized", "Current identity authority is not fresh enough to authorize this import.")
		return peerapi.RequestScope{}, false
	}
	localPublic, localDecodeErr := base64.RawURLEncoding.DecodeString(node.PublicKey)
	sourcePublic := identityState.CurrentKeys[authority.PrincipalNode+":"+request.SourceNodeID]
	sourceNodeID, sourceKeyErr := identityprotocol.NodeID(sourcePublic)
	if err != nil || localDecodeErr != nil || sourceKeyErr != nil || sourceNodeID != request.SourceNodeID ||
		identityState.IdentityID != localIdentity.IdentityID || identityState.Frozen || identityState.HeadHash == "" ||
		!equalPeerPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], localPublic) {
		writeCodedError(w, http.StatusForbidden, "replica_import_node_unauthorized", "Both Nodes must be current members of the same unfrozen identity.")
		return peerapi.RequestScope{}, false
	}
	if !connectedPeerForIdentity(s.peerSessions.List(), request.SourceNodeID, localIdentity.IdentityID) {
		writeCodedError(w, http.StatusConflict, "peer_session_not_connected", "The replica source has no active authenticated peer session.")
		return peerapi.RequestScope{}, false
	}
	ownerState, err := s.ownerAuthority.Snapshot(request.OwnerSubjectID, resolve, now)
	if err != nil || ownerState.Frozen || ownerState.Subject.Controller.Kind != authority.PrincipalIdentity || ownerState.Subject.Controller.ID != localIdentity.IdentityID {
		writeCodedError(w, http.StatusForbidden, "replica_import_denied", "The OwnerSubject is unavailable, frozen, or not controlled by this identity.")
		return peerapi.RequestScope{}, false
	}
	chain, exists := ownerState.Grants[request.GrantHash]
	if !exists || len(chain) == 0 {
		writeCodedError(w, http.StatusForbidden, "replica_import_denied", "The current replication capability is not present in authority history.")
		return peerapi.RequestScope{}, false
	}
	audience := authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}
	leaf := chain[len(chain)-1].Grant
	if leaf.SubjectID != request.OwnerSubjectID || leaf.Audience != audience || leaf.ArchiveID != request.ArchiveID ||
		(leaf.SnapshotID != "" && leaf.SnapshotID != request.SnapshotID) || !containsPeerAction(leaf.Actions, "archive.replicate") ||
		s.ownerAuthority.AuthorizeChain(request.OwnerSubjectID, chain, resolve, now, audience, request.ArchiveID, request.SnapshotID, "archive.replicate") != nil {
		writeCodedError(w, http.StatusForbidden, "replica_import_denied", "The capability is expired, revoked, stale, or does not authorize this archive.")
		return peerapi.RequestScope{}, false
	}
	return peerapi.RequestScope{
		IdentityID: localIdentity.IdentityID, IdentityHead: identityState.HeadHash, NodeID: node.NodeID,
		TargetIdentityID: localIdentity.IdentityID, TargetNodeID: request.SourceNodeID,
		OwnerSubjectID: request.OwnerSubjectID, ArchiveID: request.ArchiveID, SnapshotID: request.SnapshotID,
		Action: "archive.replicate", GrantChain: chain, Resolve: peerprotocol.IdentityResolver(resolve), Signer: s.nodeIdentity,
	}, true
}

type replicaReadbackRequest struct {
	OwnerSubjectID string `json:"owner_subject_id"`
	GrantHash      string `json:"grant_hash"`
}

type replicaLeaseVerifier interface {
	VerifyReplica(context.Context, string, lease.ObjectReader) (lease.Lease, error)
}

type replicaLeaseReader interface {
	Get(context.Context, string) (lease.Lease, error)
}

type replicaPeerRangeReader interface {
	OpenCiphertextRange(context.Context, string, peerapi.RequestScope, string, string, uint64, int64, int64) (io.ReadCloser, error)
}

type peerReplicaRangeSource struct {
	peer   replicaPeerRangeReader
	nodeID string
	scope  peerapi.RequestScope
}

func (r peerReplicaRangeSource) OpenCiphertextRange(ctx context.Context, archiveID, snapshotID, objectID string, keyEpoch uint64, offset, length int64) (io.ReadCloser, error) {
	if r.peer == nil || r.nodeID == "" || archiveID != r.scope.ArchiveID || snapshotID != r.scope.SnapshotID || r.scope.Action != "archive.replicate" {
		return nil, replication.ErrUnauthorized
	}
	return r.peer.OpenCiphertextRange(ctx, r.nodeID, r.scope, snapshotID, objectID, keyEpoch, offset, length)
}

func (s *Server) replicaLeaseVerify(w http.ResponseWriter, r *http.Request) {
	principal, authenticated := authn.PrincipalFromContext(r.Context())
	if !authenticated || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if !authn.HasPermission(principal, authn.PermissionRecordingExport) {
		writeCodedError(w, http.StatusForbidden, "permission_denied", "Permission denied.")
		return
	}
	if !secureBrowserRequest(r) {
		writeCodedError(w, http.StatusUpgradeRequired, "https_required", "Replica readback requires HTTPS.")
		return
	}
	leaseID := r.PathValue("leaseID")
	if !validReplicaLeaseID(leaseID) {
		writeCodedError(w, http.StatusBadRequest, "replica_lease_invalid", "The replica lease identifier is invalid.")
		return
	}
	var request replicaReadbackRequest
	if err := decodeJSONBody(w, r, 8<<10, &request); err != nil || request.OwnerSubjectID == "" || len(request.GrantHash) != len("sha256:")+64 || request.GrantHash[:len("sha256:")] != "sha256:" {
		writeCodedError(w, http.StatusBadRequest, "replica_readback_invalid", "A current OwnerSubject capability reference is required.")
		return
	}
	for _, char := range request.GrantHash[len("sha256:"):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			writeCodedError(w, http.StatusBadRequest, "replica_readback_invalid", "The capability reference is invalid.")
			return
		}
	}
	reader, ok := s.replicaLeases.(replicaLeaseReader)
	verifier, canVerify := s.replicaLeases.(replicaLeaseVerifier)
	if !ok || !canVerify || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_readback_unavailable", "Replica readback is not configured on this Control Plane.")
		return
	}
	current, err := reader.Get(r.Context(), leaseID)
	if err != nil {
		writeCodedError(w, http.StatusNotFound, "replica_lease_not_found", "The replica lease is unavailable.")
		return
	}
	if current.Status != lease.StatusVerifying || current.Offer.Offer.LeaseID != leaseID {
		writeCodedError(w, http.StatusConflict, "replica_readback_not_ready", "The replica has not reached source-side readback verification.")
		return
	}
	source, scope, ok := s.replicaReadbackScope(w, r, request, current)
	if !ok {
		return
	}
	peer, ok := s.peerSessions.(replicaPeerRangeReader)
	if !ok {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Authenticated peer range reads are unavailable.")
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	readback := replication.PeerLeaseObjectReader{
		Lease:  current,
		Source: peerReplicaRangeSource{peer: peer, nodeID: source, scope: scope},
	}
	verified, err := verifier.VerifyReplica(ctx, leaseID, readback)
	if err != nil {
		if errors.Is(err, lease.ErrConflict) {
			writeCodedError(w, http.StatusBadGateway, "replica_readback_mismatch", "The provider bytes do not match the signed replica manifest.")
			return
		}
		writeCodedError(w, http.StatusBadGateway, "replica_readback_failed", "The provider ciphertext could not be independently verified.")
		return
	}
	if !s.auditMutation(w, r, "replica_readback_verified", leaseID) {
		return
	}
	writeJSON(w, http.StatusOK, summarizeReplicaLease(verified))
}

func (s *Server) replicaReadbackScope(w http.ResponseWriter, r *http.Request, request replicaReadbackRequest, current lease.Lease) (string, peerapi.RequestScope, bool) {
	if s.ownerAuthority == nil || s.globalIdentity == nil || s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_authority_unavailable", "Current archive authority is unavailable.")
		return "", peerapi.RequestScope{}, false
	}
	localIdentity, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || localIdentity.Frozen || localIdentity.IdentityID == "" {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_unavailable", "Current identity authority is unavailable or frozen.")
		return "", peerapi.RequestScope{}, false
	}
	node, err := s.nodeIdentity.Info()
	if err != nil || node.NodeID == "" {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_node_unavailable", "The local Node identity is unavailable.")
		return "", peerapi.RequestScope{}, false
	}
	now := time.Now().UTC()
	resolve := authority.IdentityStateResolverFromLocalAndRegistry(s.globalIdentity, s.globalIdentityRegistry)
	if s.federatedIdentityResolver != nil {
		resolve = authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	}
	identityState, err := resolve(localIdentity.IdentityID, "", now)
	publicKey, decodeErr := base64.RawURLEncoding.DecodeString(node.PublicKey)
	if err != nil || decodeErr != nil || identityState.Frozen || identityState.HeadHash == "" ||
		!equalPeerPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], publicKey) {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_stale", "Current identity authority is not fresh enough to authorize peer access.")
		return "", peerapi.RequestScope{}, false
	}
	providerNodeID := current.Offer.Offer.ProviderNodeID
	if providerNodeID == "" || providerNodeID == node.NodeID || !connectedPeerForIdentity(s.peerSessions.List(), providerNodeID, localIdentity.IdentityID) {
		writeCodedError(w, http.StatusConflict, "peer_session_not_connected", "The replica provider has no active authenticated session.")
		return "", peerapi.RequestScope{}, false
	}
	ownerState, err := s.ownerAuthority.Snapshot(request.OwnerSubjectID, resolve, now)
	if err != nil || ownerState.Frozen || ownerState.Subject.Controller.ID != localIdentity.IdentityID {
		writeCodedError(w, http.StatusForbidden, "replica_readback_denied", "The OwnerSubject is unavailable, frozen, or not controlled by this identity.")
		return "", peerapi.RequestScope{}, false
	}
	chain, exists := ownerState.Grants[request.GrantHash]
	if !exists || len(chain) == 0 {
		writeCodedError(w, http.StatusForbidden, "replica_readback_denied", "The current replication capability is not present in authority history.")
		return "", peerapi.RequestScope{}, false
	}
	offer := current.Offer.Offer
	// The local source Node is the signed peer-command actor. The provider is
	// the remote target of the readback request, so the capability audience is
	// the local Node, matching the peer API's command authorization semantics.
	audience := authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}
	leaf := chain[len(chain)-1].Grant
	if leaf.SubjectID != request.OwnerSubjectID || leaf.Audience != audience || leaf.ArchiveID != offer.ArchiveID ||
		(leaf.SnapshotID != "" && leaf.SnapshotID != offer.SnapshotID) || !containsPeerAction(leaf.Actions, "archive.replicate") {
		writeCodedError(w, http.StatusForbidden, "replica_readback_denied", "The current capability does not authorize readback from this provider.")
		return "", peerapi.RequestScope{}, false
	}
	if err := s.ownerAuthority.AuthorizeChain(request.OwnerSubjectID, chain, resolve, now, audience, offer.ArchiveID, offer.SnapshotID, "archive.replicate"); err != nil {
		writeCodedError(w, http.StatusForbidden, "replica_readback_denied", "The capability is expired, revoked, stale, or otherwise unauthorized.")
		return "", peerapi.RequestScope{}, false
	}
	return providerNodeID, peerapi.RequestScope{
		IdentityID: localIdentity.IdentityID, IdentityHead: identityState.HeadHash, NodeID: node.NodeID,
		TargetIdentityID: localIdentity.IdentityID, TargetNodeID: providerNodeID,
		OwnerSubjectID: request.OwnerSubjectID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
		Action: "archive.replicate", GrantChain: chain, Resolve: peerprotocol.IdentityResolver(resolve), Signer: s.nodeIdentity,
	}, true
}

// replicaLeasesList exposes only an operational projection. Signed grants,
// signatures, and owner-subject details remain in the private durable store.
func (s *Server) replicaLeasesList(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.replicaLeases == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_leases_unavailable", "Replica lease state is not available on this Control Plane.")
		return
	}
	leases, tombstones, err := s.replicaLeases.Snapshot(r.Context())
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_leases_unavailable", "Replica lease state could not be read safely.")
		return
	}
	items := make([]replicaLeaseSummary, 0, len(leases))
	for _, current := range leases {
		items = append(items, summarizeReplicaLease(current))
	}
	deletionRecords := make([]replicaTombstoneSummary, 0, len(tombstones))
	for _, current := range tombstones {
		item := current.Signed.Tombstone
		acknowledgedNodes := make([]string, 0, len(current.Deliveries))
		for _, delivery := range current.Deliveries {
			acknowledgedNodes = append(acknowledgedNodes, delivery.NodeID)
		}
		deletionRecords = append(deletionRecords, replicaTombstoneSummary{
			ArchiveID: item.ArchiveID, DeletionEpoch: item.DeletionEpoch, IssuedAt: item.IssuedAt,
			Frozen: current.Frozen, AcknowledgedNodes: acknowledgedNodes,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"leases": items, "tombstones": deletionRecords})
}

func summarizeReplicaLease(current lease.Lease) replicaLeaseSummary {
	offer := current.Offer.Offer
	grantHash := ""
	if grants := current.Offer.GrantChain; len(grants) > 0 {
		// This public projection contains only the leaf grant identifier, not the
		// signed capability chain or its signatures. The UI uses it to resume the
		// exact persisted offer after a browser reload.
		grantHash, _ = authority.GrantHash(grants[len(grants)-1].Grant)
	}
	completed := 0
	for _, object := range offer.EncryptedObjectSet {
		for _, progress := range current.Progress {
			if progress.ObjectID == object.ObjectID && progress.CommittedOffset == object.CiphertextSize {
				completed++
				break
			}
		}
	}
	return replicaLeaseSummary{
		LeaseID: offer.LeaseID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
		KeyEpoch: offer.KeyEpoch, OwnerSubjectID: offer.OwnerSubjectID, GrantHash: grantHash,
		ProviderNodeID: offer.ProviderNodeID, Status: current.Status, Revision: current.Revision,
		ExpiresAt: offer.ExpiresAt, ReservedBytes: offer.ReservedBytes,
		ObjectCount: len(offer.EncryptedObjectSet), CompletedObject: completed,
	}
}
