package server

import (
	"context"
	"encoding/base64"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
)

const (
	maxReplicaInboxScopes = 32
	maxReplicaInboxOffers = 256
)

type peerReplicaOfferLister interface {
	ListReplicaOffers(context.Context, string, peerapi.RequestScope) ([]lease.PendingOffer, error)
}

type replicaInboxOfferView struct {
	lease.PendingOffer
	GrantHash string `json:"grant_hash"`
}

type replicaInboxResponse struct {
	SourceNodeID string                  `json:"source_node_id"`
	ArchiveID    string                  `json:"archive_id"`
	Offers       []replicaInboxOfferView `json:"offers"`
}

type replicaInboxScope struct {
	snapshotID string
	grantHash  string
	chain      []authority.SignedGrant
}

func (s *Server) replicaInbox(w http.ResponseWriter, r *http.Request) {
	principal, authenticated := authn.PrincipalFromContext(r.Context())
	if !authenticated || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return
	}
	if !authn.HasPermission(principal, authn.PermissionRecordingRead) {
		writeCodedError(w, http.StatusForbidden, "permission_denied", "Permission denied.")
		return
	}
	if !exactPeerArchiveQuery(r.URL.Query(), "owner_subject_id", "archive_id") || !validReplicaOwnerSubjectID(r.URL.Query().Get("owner_subject_id")) ||
		!replicaArchiveIDPattern.MatchString(r.URL.Query().Get("archive_id")) {
		writeCodedError(w, http.StatusBadRequest, "replica_inbox_request_invalid", "A valid OwnerSubject and claimed archive are required.")
		return
	}
	if !secureBrowserRequest(r) {
		writeCodedError(w, http.StatusUpgradeRequired, "https_required", "Replica inbox access requires HTTPS.")
		return
	}
	lister, ok := s.peerSessions.(peerReplicaOfferLister)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() || s.ownerAuthority == nil || s.globalIdentity == nil || s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_inbox_unavailable", "Replica inbox access is not configured.")
		return
	}
	localIdentity, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || localIdentity.Frozen || localIdentity.IdentityID == "" {
		writeCodedError(w, http.StatusServiceUnavailable, "replica_inbox_identity_unavailable", "Current identity authority is unavailable or frozen.")
		return
	}
	sourceNodeID := r.PathValue("nodeID")
	if !connectedPeerForIdentity(s.peerSessions.List(), sourceNodeID, localIdentity.IdentityID) {
		writeCodedError(w, http.StatusConflict, "peer_session_not_connected", "The selected source Node has no active authenticated session.")
		return
	}
	now := time.Now().UTC()
	resolve := authority.IdentityStateResolverFromLocalAndRegistry(s.globalIdentity, s.globalIdentityRegistry)
	if s.federatedIdentityResolver != nil {
		resolve = authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	}
	identityState, err := resolve(localIdentity.IdentityID, "", now)
	if err != nil || identityState.IdentityID != localIdentity.IdentityID || identityState.Frozen || identityState.HeadHash == "" || !identityState.FreshUntil.After(now) {
		writeCodedError(w, http.StatusForbidden, "replica_inbox_identity_stale", "Current identity authority is stale or frozen.")
		return
	}
	node, err := s.nodeIdentity.Info()
	localPublic, decodeErr := base64.RawURLEncoding.DecodeString(node.PublicKey)
	if err != nil || decodeErr != nil || len(localPublic) != 32 || node.NodeID == "" ||
		!equalPeerPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], localPublic) {
		writeCodedError(w, http.StatusForbidden, "replica_inbox_node_not_delegated", "The local Node is not currently delegated by this identity.")
		return
	}
	if sourceNodeID == node.NodeID {
		writeCodedError(w, http.StatusBadRequest, "replica_inbox_request_invalid", "The source must be a different delegated Node.")
		return
	}
	sourcePublic := identityState.CurrentKeys[authority.PrincipalNode+":"+sourceNodeID]
	verifiedSource, sourceErr := identityprotocol.NodeID(sourcePublic)
	if sourceErr != nil || verifiedSource != sourceNodeID {
		writeCodedError(w, http.StatusForbidden, "replica_inbox_source_not_delegated", "The source Node is not currently delegated by this identity.")
		return
	}
	subjectID, archiveID := r.URL.Query().Get("owner_subject_id"), r.URL.Query().Get("archive_id")
	owner, err := s.ownerAuthority.Snapshot(subjectID, resolve, now)
	if err != nil || owner.Frozen || owner.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: localIdentity.IdentityID}) ||
		!archiveClaimed(owner.ArchiveIDs, archiveID) {
		writeCodedError(w, http.StatusForbidden, "replica_inbox_authority_denied", "The OwnerSubject does not currently control this claimed archive.")
		return
	}
	audience := authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}
	scopes := make(map[string]replicaInboxScope)
	for hash, chain := range owner.Grants {
		if len(chain) == 0 {
			continue
		}
		leaf := chain[len(chain)-1].Grant
		if leaf.SubjectID != subjectID || leaf.Audience != audience || leaf.ArchiveID != archiveID || !containsPeerAction(leaf.Actions, "archive.replicate") ||
			s.ownerAuthority.AuthorizeChain(subjectID, chain, resolve, now, audience, archiveID, leaf.SnapshotID, "archive.replicate") != nil {
			continue
		}
		current, exists := scopes[leaf.SnapshotID]
		if !exists || hash < current.grantHash {
			scopes[leaf.SnapshotID] = replicaInboxScope{snapshotID: leaf.SnapshotID, grantHash: hash, chain: append([]authority.SignedGrant(nil), chain...)}
		}
	}
	if broad, exists := scopes[""]; exists {
		scopes = map[string]replicaInboxScope{"": broad}
	}
	if len(scopes) == 0 {
		writeCodedError(w, http.StatusForbidden, "replica_inbox_capability_missing", "No current archive replication capability is assigned to this Node.")
		return
	}
	if len(scopes) > maxReplicaInboxScopes {
		writeCodedError(w, http.StatusUnprocessableEntity, "replica_inbox_scope_limit", "Too many snapshot-specific capabilities are available for one inbox query.")
		return
	}
	snapshotIDs := make([]string, 0, len(scopes))
	for snapshotID := range scopes {
		snapshotIDs = append(snapshotIDs, snapshotID)
	}
	sort.Strings(snapshotIDs)
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	byLease := make(map[string]replicaInboxOfferView)
	for _, snapshotID := range snapshotIDs {
		scope := scopes[snapshotID]
		requestScope := peerapi.RequestScope{
			IdentityID: localIdentity.IdentityID, IdentityHead: identityState.HeadHash, NodeID: node.NodeID,
			TargetIdentityID: localIdentity.IdentityID, TargetNodeID: sourceNodeID,
			OwnerSubjectID: subjectID, ArchiveID: archiveID, SnapshotID: scope.snapshotID,
			Action: "archive.replicate", GrantChain: scope.chain, Resolve: peerprotocol.IdentityResolver(resolve), Signer: s.nodeIdentity,
		}
		offers, listErr := lister.ListReplicaOffers(ctx, sourceNodeID, requestScope)
		if listErr != nil {
			writePeerArchiveReadError(w, listErr, "Pending replica offers could not be retrieved from the source Node.")
			return
		}
		for _, offer := range offers {
			if offer.ArchiveID != archiveID || offer.OwnerSubject != subjectID || scope.snapshotID != "" && offer.SnapshotID != scope.snapshotID {
				writeCodedError(w, http.StatusBadGateway, "replica_inbox_response_invalid", "The source returned an offer outside the requested authority scope.")
				return
			}
			if prior, exists := byLease[offer.LeaseID]; exists {
				if prior.OfferHash != offer.OfferHash || prior.SnapshotID != offer.SnapshotID {
					writeCodedError(w, http.StatusBadGateway, "replica_inbox_response_conflict", "The source returned conflicting views of one pending offer.")
					return
				}
				continue
			}
			byLease[offer.LeaseID] = replicaInboxOfferView{PendingOffer: offer, GrantHash: scope.grantHash}
			if len(byLease) > maxReplicaInboxOffers {
				writeCodedError(w, http.StatusUnprocessableEntity, "replica_inbox_result_limit", "The pending offer list exceeds the bounded inbox limit.")
				return
			}
		}
	}
	result := make([]replicaInboxOfferView, 0, len(byLease))
	for _, offer := range byLease {
		result = append(result, offer)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LeaseID < result[j].LeaseID })
	w.Header().Set("Cache-Control", "private, no-store")
	writeJSON(w, http.StatusOK, replicaInboxResponse{SourceNodeID: sourceNodeID, ArchiveID: archiveID, Offers: result})
}

func validReplicaOwnerSubjectID(value string) bool {
	if len(value) != len("os_")+22 || !strings.HasPrefix(value, "os_") {
		return false
	}
	for _, char := range value[3:] {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
