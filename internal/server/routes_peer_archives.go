package server

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/replication"
)

const peerArchiveRequestTimeout = 20 * time.Second

// peerArchiveHeadReader is implemented by the live session slot. Keeping this
// capability separate from session management lets embedded Control Planes
// without P2P archive access continue to start and fail closed at the route.
type peerArchiveHeadReader interface {
	HeadSnapshot(context.Context, string, peerapi.RequestScope) (archivesnapshot.Snapshot, error)
}

type peerArchiveDataReader interface {
	Snapshot(context.Context, string, peerapi.RequestScope, string) (archivesnapshot.Snapshot, error)
	ReplicaManifest(context.Context, string, peerapi.RequestScope, archivesnapshot.Snapshot, uint64) (replication.ReplicaManifest, error)
	KeyEpochs(context.Context, string, peerapi.RequestScope, archivesnapshot.Snapshot, string) ([]uint64, error)
	KeyEnvelope(context.Context, string, peerapi.RequestScope, archivesnapshot.Snapshot, uint64, string) (cryptoarchive.KeyEnvelope, error)
	OpenCiphertextRange(context.Context, string, peerapi.RequestScope, string, string, uint64, int64, int64) (io.ReadCloser, error)
}

type peerArchiveSessionKeyReader interface {
	SessionKeyEpochs(context.Context, string, peerapi.RequestScope, archivesnapshot.Snapshot) ([]uint64, error)
	SessionDEKEnvelope(context.Context, string, peerapi.RequestScope, archivesnapshot.Snapshot, uint64, []byte) (cryptoarchive.SessionDEKEnvelope, error)
}

type peerArchiveHeadResponse struct {
	NodeID   string                   `json:"node_id"`
	Snapshot archivesnapshot.Snapshot `json:"snapshot"`
}

type peerArchiveKeyEpochsResponse struct {
	NodeID     string   `json:"node_id"`
	ArchiveID  string   `json:"archive_id"`
	SnapshotID string   `json:"snapshot_id"`
	KeyEpochs  []string `json:"key_epochs"`
}

func (s *Server) registerPeerArchiveRoutes() {
	s.mux.HandleFunc("GET /api/peer-archives/capabilities", s.peerArchiveCapabilities)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/replica-offers", s.replicaInbox)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/head", s.peerArchiveHead)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/snapshots/{snapshotID}", s.peerArchiveSnapshot)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/snapshots/{snapshotID}/manifest", s.peerArchiveManifest)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/snapshots/{snapshotID}/key-epochs", s.peerArchiveKeyEpochs)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/snapshots/{snapshotID}/key-envelope", s.peerArchiveKeyEnvelope)
	s.mux.HandleFunc("GET /api/peer-archives/{nodeID}/snapshots/{snapshotID}/objects/{objectID}", s.peerArchiveCiphertextRange)
}

type peerArchiveCapabilityView struct {
	OwnerSubjectID string `json:"owner_subject_id"`
	ArchiveID      string `json:"archive_id"`
	GrantHash      string `json:"grant_hash"`
	ExpiresAt      string `json:"expires_at"`
}

type peerArchiveCapabilitiesResponse struct {
	LocalNodeID  string                      `json:"local_node_id"`
	Sessions     []peerapi.SessionInfo       `json:"sessions"`
	Capabilities []peerArchiveCapabilityView `json:"capabilities"`
}

// peerArchiveCapabilities returns only current archive-wide read capabilities
// addressed to this installation's delegated Node Key. It contains no signed
// chains or signing material; every actual snapshot request rechecks the
// durable chain independently in peerArchiveHead.
func (s *Server) peerArchiveCapabilities(w http.ResponseWriter, _ *http.Request) {
	response := peerArchiveCapabilitiesResponse{Sessions: []peerapi.SessionInfo{}, Capabilities: []peerArchiveCapabilityView{}}
	if s.globalIdentity == nil || s.nodeIdentity == nil || s.ownerAuthority == nil {
		writeJSON(w, http.StatusOK, response)
		return
	}
	localIdentity, initialized, err := s.globalIdentity.Snapshot()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_authority_unavailable", "Current archive authority is unavailable.")
		return
	}
	if !initialized || localIdentity.Frozen || localIdentity.IdentityID == "" {
		writeJSON(w, http.StatusOK, response)
		return
	}
	node, err := s.nodeIdentity.Info()
	if err != nil || node.NodeID == "" {
		writeJSON(w, http.StatusOK, response)
		return
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
		writeJSON(w, http.StatusOK, response)
		return
	}
	response.LocalNodeID = node.NodeID
	if s.peerSessionsAvailable() {
		response.Sessions = append(response.Sessions, s.peerSessions.List()...)
	}
	subjects, err := s.ownerAuthority.List(resolve, now)
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_authority_unavailable", "Current archive authority is unavailable.")
		return
	}
	audience := authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}
	for _, subject := range subjects {
		if subject.Frozen || subject.Subject.Controller.Kind != authority.PrincipalIdentity {
			continue
		}
		controller, controllerErr := resolve(subject.Subject.Controller.ID, "", now)
		if controllerErr != nil || controller.Frozen || controller.IdentityID != subject.Subject.Controller.ID ||
			controller.HeadHash == "" || !controller.FreshUntil.After(now) {
			continue
		}
		verification := authority.VerificationContext{
			Subject: subject.Subject, SubjectPolicyEpoch: subject.Subject.PolicyEpoch,
			RevokedGrantHashes: subject.RevokedHashes, Identity: controller, Now: now,
		}
		for hash, chain := range subject.Grants {
			if len(chain) == 0 {
				continue
			}
			grant := chain[len(chain)-1].Grant
			if grant.Audience != audience || grant.SnapshotID != "" || !containsPeerAction(grant.Actions, "archive.view") ||
				authority.VerifyGrantChain(verification, chain, audience, grant.ArchiveID, "", "archive.view") != nil {
				continue
			}
			response.Capabilities = append(response.Capabilities, peerArchiveCapabilityView{
				OwnerSubjectID: subject.Subject.ID, ArchiveID: grant.ArchiveID, GrantHash: hash, ExpiresAt: grant.ExpiresAt,
			})
		}
	}
	sort.Slice(response.Sessions, func(i, j int) bool { return response.Sessions[i].NodeID < response.Sessions[j].NodeID })
	sort.Slice(response.Capabilities, func(i, j int) bool {
		if response.Capabilities[i].ArchiveID != response.Capabilities[j].ArchiveID {
			return response.Capabilities[i].ArchiveID < response.Capabilities[j].ArchiveID
		}
		return response.Capabilities[i].OwnerSubjectID < response.Capabilities[j].OwnerSubjectID
	})
	writeJSON(w, http.StatusOK, response)
}

// peerArchiveHead reads one immutable snapshot descriptor over an
// authenticated Node-Key session. The browser may name a committed grant by
// hash, but cannot submit or replace a signed grant chain. The chain is read
// from the local durable authority log and revalidated at request time.
func (s *Server) peerArchiveHead(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.peerSessions.(peerArchiveHeadReader)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer archive access is not configured.")
		return
	}
	query := r.URL.Query()
	if !exactPeerArchiveQuery(query, "owner_subject_id", "archive_id", "grant_hash") {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "A subject, archive, and committed capability reference are required.")
		return
	}
	nodeID, scope, ok := s.peerArchiveRequestScope(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	snapshot, err := reader.HeadSnapshot(ctx, nodeID, scope)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive snapshot could not be retrieved or verified.")
		return
	}
	if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != scope.ArchiveID {
		writeCodedError(w, http.StatusBadGateway, "peer_archive_snapshot_invalid", "The remote archive snapshot failed integrity validation.")
		return
	}
	writeJSON(w, http.StatusOK, peerArchiveHeadResponse{NodeID: nodeID, Snapshot: snapshot})
}

// peerArchiveSnapshot retrieves one exact immutable descriptor in the archive
// snapshot chain. The archive-wide capability is revalidated by
// peerArchiveRequestScope on every request; selecting an old snapshot never
// reuses authorization from the head request.
func (s *Server) peerArchiveSnapshot(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer archive access is not configured.")
		return
	}
	if !exactPeerArchiveQuery(r.URL.Query(), "owner_subject_id", "archive_id", "grant_hash") {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "A subject, archive, and committed capability reference are required.")
		return
	}
	nodeID, scope, ok := s.peerArchiveRequestScope(w, r)
	if !ok {
		return
	}
	snapshotID := r.PathValue("snapshotID")
	if !validSnapshotID(snapshotID) {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot identifier is invalid.")
		return
	}
	scope.SnapshotID = snapshotID
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	snapshot, err := reader.Snapshot(ctx, nodeID, scope, snapshotID)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive snapshot could not be retrieved or verified.")
		return
	}
	if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != scope.ArchiveID || snapshot.SnapshotID != snapshotID {
		writeCodedError(w, http.StatusBadGateway, "peer_archive_snapshot_invalid", "The remote archive snapshot failed integrity validation.")
		return
	}
	writePeerArchiveJSON(w, peerArchiveHeadResponse{NodeID: nodeID, Snapshot: snapshot})
}

func (s *Server) peerArchiveManifest(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer archive access is not configured.")
		return
	}
	if !exactPeerArchiveQuery(r.URL.Query(), "owner_subject_id", "archive_id", "grant_hash", "key_epoch") {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot and committed capability parameters are invalid.")
		return
	}
	nodeID, scope, ok := s.peerArchiveRequestScope(w, r)
	if !ok {
		return
	}
	snapshotID := r.PathValue("snapshotID")
	keyEpoch, ok := parsePeerArchiveUint(r.URL.Query().Get("key_epoch"), true)
	if !validSnapshotID(snapshotID) || !ok {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot or key epoch is invalid.")
		return
	}
	scope.SnapshotID = snapshotID
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	snapshot, err := reader.Snapshot(ctx, nodeID, scope, snapshotID)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive snapshot could not be retrieved or verified.")
		return
	}
	manifest, err := reader.ReplicaManifest(ctx, nodeID, scope, snapshot, keyEpoch)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote encrypted archive manifest could not be retrieved or verified.")
		return
	}
	writePeerArchiveJSON(w, manifest)
}

func (s *Server) peerArchiveKeyEpochs(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer archive access is not configured.")
		return
	}
	if !exactPeerArchiveQuery(r.URL.Query(), "owner_subject_id", "archive_id", "grant_hash") {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot and committed capability parameters are invalid.")
		return
	}
	nodeID, scope, ok := s.peerArchiveRequestScope(w, r)
	if !ok {
		return
	}
	snapshotID := r.PathValue("snapshotID")
	if !validSnapshotID(snapshotID) {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot identifier is invalid.")
		return
	}
	local, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || local.Frozen {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_unavailable", "The local recovery public key is unavailable.")
		return
	}
	recoveryPublic, err := base64.RawURLEncoding.DecodeString(local.RecoveryEncryptionPublicKey)
	if err != nil || len(recoveryPublic) != 32 {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_unavailable", "The local recovery public key is invalid.")
		return
	}
	recipientKeyID := cryptoarchive.RecipientKeyID(recoveryPublic)
	scope.SnapshotID = snapshotID
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	snapshot, err := reader.Snapshot(ctx, nodeID, scope, snapshotID)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive snapshot could not be retrieved or verified.")
		return
	}
	epochs, err := reader.KeyEpochs(ctx, nodeID, scope, snapshot, recipientKeyID)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive key metadata could not be retrieved or verified.")
		return
	}
	if len(epochs) > replication.MaxSnapshotKeyEpochs {
		writeCodedError(w, http.StatusBadGateway, "peer_archive_key_epochs_invalid", "The remote archive key metadata is invalid.")
		return
	}
	response := peerArchiveKeyEpochsResponse{NodeID: nodeID, ArchiveID: scope.ArchiveID, SnapshotID: snapshotID, KeyEpochs: make([]string, 0, len(epochs))}
	var previous uint64
	for index, epoch := range epochs {
		if epoch == 0 || index > 0 && epoch >= previous {
			writeCodedError(w, http.StatusBadGateway, "peer_archive_key_epochs_invalid", "The remote archive key metadata is invalid.")
			return
		}
		response.KeyEpochs = append(response.KeyEpochs, strconv.FormatUint(epoch, 10))
		previous = epoch
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) peerArchiveKeyEnvelope(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer archive access is not configured.")
		return
	}
	if !exactPeerArchiveQuery(r.URL.Query(), "owner_subject_id", "archive_id", "grant_hash", "key_epoch") {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot and committed capability parameters are invalid.")
		return
	}
	nodeID, scope, ok := s.peerArchiveRequestScope(w, r)
	if !ok {
		return
	}
	snapshotID := r.PathValue("snapshotID")
	keyEpoch, ok := parsePeerArchiveUint(r.URL.Query().Get("key_epoch"), true)
	if !validSnapshotID(snapshotID) || !ok {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot or key epoch is invalid.")
		return
	}
	local, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || local.Frozen {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_unavailable", "The local recovery public key is unavailable.")
		return
	}
	recoveryPublic, err := base64.RawURLEncoding.DecodeString(local.RecoveryEncryptionPublicKey)
	if err != nil || len(recoveryPublic) != 32 {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_unavailable", "The local recovery public key is invalid.")
		return
	}
	recipientKeyID := cryptoarchive.RecipientKeyID(recoveryPublic)
	scope.SnapshotID = snapshotID
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	snapshot, err := reader.Snapshot(ctx, nodeID, scope, snapshotID)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive snapshot could not be retrieved or verified.")
		return
	}
	envelope, err := reader.KeyEnvelope(ctx, nodeID, scope, snapshot, keyEpoch, recipientKeyID)
	if err != nil {
		writePeerArchiveReadError(w, err, "The recipient-bound archive key envelope could not be retrieved or verified.")
		return
	}
	writePeerArchiveJSON(w, envelope)
}

func (s *Server) peerArchiveCiphertextRange(w http.ResponseWriter, r *http.Request) {
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	if !ok || s.peerSessions == nil || !s.peerSessionsAvailable() {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_sessions_unavailable", "Peer archive access is not configured.")
		return
	}
	if !exactPeerArchiveQuery(r.URL.Query(), "owner_subject_id", "archive_id", "grant_hash", "key_epoch", "offset", "length") {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The ciphertext range and committed capability parameters are invalid.")
		return
	}
	nodeID, scope, ok := s.peerArchiveRequestScope(w, r)
	if !ok {
		return
	}
	snapshotID, objectID := r.PathValue("snapshotID"), r.PathValue("objectID")
	keyEpoch, epochOK := parsePeerArchiveUint(r.URL.Query().Get("key_epoch"), true)
	offset, offsetOK := parsePeerArchiveUint(r.URL.Query().Get("offset"), false)
	length, lengthOK := parsePeerArchiveUint(r.URL.Query().Get("length"), true)
	if !validSnapshotID(snapshotID) || !epochOK || !offsetOK || !lengthOK || length > uint64(replication.MaxCiphertextChunkBytes) || offset > uint64(^uint64(0)>>1) {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The snapshot or ciphertext range is invalid.")
		return
	}
	scope.SnapshotID = snapshotID
	ctx, cancel := context.WithTimeout(r.Context(), peerArchiveRequestTimeout)
	defer cancel()
	body, err := reader.OpenCiphertextRange(ctx, nodeID, scope, snapshotID, objectID, keyEpoch, int64(offset), int64(length))
	if err != nil {
		writePeerArchiveReadError(w, err, "The encrypted archive object range could not be retrieved or verified.")
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	if _, err := io.CopyN(w, body, int64(length)); err != nil {
		// The peer client already checked exact length before returning this
		// reader; an HTTP write failure is logged only as a stage, never raw.
		return
	}
}

func (s *Server) peerArchiveRequestScope(w http.ResponseWriter, r *http.Request) (string, peerapi.RequestScope, bool) {
	query := r.URL.Query()
	subjectID, archiveID, grantHash := query.Get("owner_subject_id"), query.Get("archive_id"), query.Get("grant_hash")
	if len(subjectID) != 25 || len(archiveID) != 26 || !strings.HasPrefix(grantHash, "sha256:") || len(grantHash) != len("sha256:")+64 {
		writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The subject, archive, or capability reference is invalid.")
		return "", peerapi.RequestScope{}, false
	}
	for _, char := range grantHash[len("sha256:"):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			writeCodedError(w, http.StatusBadRequest, "peer_archive_request_invalid", "The capability reference is invalid.")
			return "", peerapi.RequestScope{}, false
		}
	}
	if s.ownerAuthority == nil || s.globalIdentity == nil || s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_authority_unavailable", "Current archive authority is unavailable.")
		return "", peerapi.RequestScope{}, false
	}
	localIdentity, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || localIdentity.Frozen || localIdentity.IdentityID == "" {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_unavailable", "Current identity authority is unavailable or frozen.")
		return "", peerapi.RequestScope{}, false
	}
	now := time.Now().UTC()
	resolve := authority.IdentityStateResolverFromLocalAndRegistry(s.globalIdentity, s.globalIdentityRegistry)
	if s.federatedIdentityResolver != nil {
		resolve = authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	}
	identityState, err := resolve(localIdentity.IdentityID, "", now)
	if err != nil || identityState.Frozen || identityState.HeadHash == "" || !identityState.FreshUntil.After(now) {
		writeCodedError(w, http.StatusServiceUnavailable, "peer_archive_identity_stale", "Current identity authority is not fresh enough to authorize peer access.")
		return "", peerapi.RequestScope{}, false
	}
	node, err := s.nodeIdentity.Info()
	publicKey, decodeErr := base64.RawURLEncoding.DecodeString(node.PublicKey)
	if err != nil || decodeErr != nil || node.NodeID == "" || len(publicKey) == 0 ||
		!equalPeerPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], publicKey) {
		writeCodedError(w, http.StatusForbidden, "peer_archive_node_not_delegated", "This Node Key is not delegated by the current identity history.")
		return "", peerapi.RequestScope{}, false
	}
	nodeID := r.PathValue("nodeID")
	ownerState, err := s.ownerAuthority.Snapshot(subjectID, resolve, now)
	if err != nil || ownerState.Frozen || ownerState.Subject.Controller.Kind != authority.PrincipalIdentity {
		writeCodedError(w, http.StatusForbidden, "peer_archive_authority_denied", "The OwnerSubject is unavailable, frozen, or does not have a verified identity controller.")
		return "", peerapi.RequestScope{}, false
	}
	controllerID := ownerState.Subject.Controller.ID
	controller, err := resolve(controllerID, "", now)
	if err != nil || controller.Frozen || controller.IdentityID != controllerID || controller.HeadHash == "" || !controller.FreshUntil.After(now) {
		writeCodedError(w, http.StatusForbidden, "peer_archive_authority_denied", "The OwnerSubject controller identity is stale or frozen.")
		return "", peerapi.RequestScope{}, false
	}
	connectedIdentity, connected := connectedPeerIdentityForNode(s.peerSessions.List(), nodeID)
	if !connected || connectedIdentity != controllerID {
		writeCodedError(w, http.StatusConflict, "peer_session_not_connected", "The requested Recorder Node has no active authenticated session for this OwnerSubject controller.")
		return "", peerapi.RequestScope{}, false
	}
	chain, exists := ownerState.Grants[grantHash]
	if !exists || len(chain) == 0 {
		writeCodedError(w, http.StatusForbidden, "peer_archive_authority_denied", "The requested capability is not present in current authority history.")
		return "", peerapi.RequestScope{}, false
	}
	leaf := chain[len(chain)-1].Grant
	if leaf.SubjectID != subjectID || leaf.Audience != (authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}) ||
		leaf.ArchiveID != archiveID || leaf.SnapshotID != "" || !containsPeerAction(leaf.Actions, "archive.view") {
		writeCodedError(w, http.StatusForbidden, "peer_archive_authority_denied", "The current capability does not authorize archive-wide viewing by this Node.")
		return "", peerapi.RequestScope{}, false
	}
	if err := s.ownerAuthority.AuthorizeChain(subjectID, chain, resolve, now, authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}, archiveID, "", "archive.view"); err != nil {
		writeCodedError(w, http.StatusForbidden, "peer_archive_authority_denied", "The current capability is expired, revoked, stale, or otherwise unauthorized.")
		return "", peerapi.RequestScope{}, false
	}
	return nodeID, peerapi.RequestScope{
		IdentityID: localIdentity.IdentityID, IdentityHead: identityState.HeadHash, NodeID: node.NodeID,
		TargetIdentityID: controllerID, TargetNodeID: nodeID,
		OwnerSubjectID: subjectID, ArchiveID: archiveID, Action: "archive.view",
		GrantChain: chain, Resolve: peerprotocol.IdentityResolver(resolve), Signer: s.nodeIdentity,
	}, true
}

func exactPeerArchiveQuery(query map[string][]string, names ...string) bool {
	if len(query) != len(names) {
		return false
	}
	for _, name := range names {
		if !singleQueryValue(query, name) {
			return false
		}
	}
	return true
}

func parsePeerArchiveUint(value string, positive bool) (uint64, bool) {
	parsed, err := strconv.ParseUint(value, 10, 63)
	if err != nil || strconv.FormatUint(parsed, 10) != value || positive && parsed == 0 {
		return 0, false
	}
	return parsed, true
}

func validSnapshotID(value string) bool {
	if len(value) != len("snap_")+64 || !strings.HasPrefix(value, "snap_") {
		return false
	}
	for _, char := range value[len("snap_"):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func writePeerArchiveReadError(w http.ResponseWriter, err error, message string) {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeCodedError(w, http.StatusGatewayTimeout, "peer_archive_timeout", "The peer archive request did not complete in time.")
	case errors.Is(err, peerapi.ErrPeerNotConnected):
		writeCodedError(w, http.StatusConflict, "peer_session_not_connected", "The authenticated Recorder Node session closed before the request completed.")
	default:
		writeCodedError(w, http.StatusBadGateway, "peer_archive_unavailable", message)
	}
}

func writePeerArchiveJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	writeJSON(w, http.StatusOK, value)
}

func singleQueryValue(query map[string][]string, key string) bool {
	values, exists := query[key]
	return exists && len(values) == 1 && values[0] != ""
}

func connectedPeerForIdentity(sessions []peerapi.SessionInfo, nodeID, identityID string) bool {
	if nodeID == "" || identityID == "" {
		return false
	}
	for _, session := range sessions {
		if session.NodeID == nodeID && session.IdentityID == identityID {
			return true
		}
	}
	return false
}

func connectedPeerIdentityForNode(sessions []peerapi.SessionInfo, nodeID string) (string, bool) {
	if nodeID == "" {
		return "", false
	}
	identityID := ""
	for _, session := range sessions {
		if session.NodeID != nodeID {
			continue
		}
		if identityID != "" && identityID != session.IdentityID {
			return "", false
		}
		identityID = session.IdentityID
	}
	return identityID, identityID != ""
}

func equalPeerPublicKey(expected, actual []byte) bool {
	if len(expected) == 0 || len(expected) != len(actual) {
		return false
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false
		}
	}
	return true
}

func containsPeerAction(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
