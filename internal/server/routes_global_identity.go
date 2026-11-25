package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
)

const (
	globalIdentityCeremonyCookie = "ir_global_identity_ceremony"
	globalIdentitySessionCookie  = "ir_global_identity_session"
	globalIdentityCSRFCookie     = "ir_global_identity_csrf"
)

type identityEventEnvelope struct {
	CanonicalEvent string `json:"canonical_event"`
	Signature      string `json:"signature"`
}

type identityRegistrationEnvelope struct {
	Credential     json.RawMessage `json:"credential"`
	CanonicalEvent string          `json:"canonical_event"`
	Signature      string          `json:"signature"`
}

func (s *Server) registerGlobalIdentityRoutes() {
	s.registerDiscoveryRoutes()
	s.registerOwnerAuthorityRoutes()
	s.mux.HandleFunc("GET /api/global-identity/status", s.globalIdentityStatus)
	s.mux.HandleFunc("GET /api/global-identity/history", s.globalIdentityHistoryExport)
	s.mux.HandleFunc("GET /api/global-identity/forks", s.globalIdentityForkChoices)
	s.mux.HandleFunc("POST /api/global-identity/forks/resolve", s.globalIdentityForkResolve)
	s.mux.HandleFunc("POST /api/global-identity/history/publish", s.globalIdentityHistoryPublish)
	s.mux.HandleFunc("POST /api/global-identity/history/import", s.globalIdentityHistoryImport)
	s.mux.HandleFunc("GET /api/global-identity/registry", s.globalIdentityRegistryList)
	s.mux.HandleFunc("GET /api/global-identity/verified/{identityID}", s.globalIdentityVerifiedRecipient)
	s.mux.HandleFunc("POST /api/global-identity/registry/import", s.globalIdentityRegistryImport)
	s.mux.HandleFunc("POST /api/global-identity/nodes/delegate", s.globalIdentityNodeDelegate)
	s.mux.HandleFunc("POST /api/global-identity/nodes/{nodeID}/revoke", s.globalIdentityNodeRevoke)
	s.mux.HandleFunc("GET /api/node-identity/status", s.nodeIdentityStatus)
	s.mux.HandleFunc("POST /api/node-identity/create", s.nodeIdentityCreate)
	s.mux.HandleFunc("POST /api/node-identity/enrollment/challenge", s.nodeIdentityEnrollmentChallenge)
	s.mux.HandleFunc("POST /api/global-identity/registration/options", s.globalIdentityRegistrationOptions)
	s.mux.HandleFunc("POST /api/global-identity/registration/finish", s.globalIdentityRegistrationFinish)
	s.mux.HandleFunc("POST /api/global-identity/credentials/{credentialID}/revoke", s.globalIdentityRevokeCredential)
	s.mux.HandleFunc("POST /api/global-identity/login/options", s.globalIdentityLoginOptions)
	s.mux.HandleFunc("POST /api/global-identity/login/finish", s.globalIdentityLoginFinish)
	s.mux.HandleFunc("GET /api/global-identity/session", s.globalIdentitySession)
	s.mux.HandleFunc("POST /api/global-identity/logout", s.globalIdentityLogout)
	s.mux.HandleFunc("GET /api/global-identity/peer-archives/{nodeID}/snapshots/{snapshotID}/session-key-epochs", s.globalIdentitySessionKeyEpochs)
	s.mux.HandleFunc("POST /api/global-identity/peer-archives/{nodeID}/snapshots/{snapshotID}/session-key-envelope", s.globalIdentitySessionKeyEnvelope)
}

type verifiedIdentityRecipientView struct {
	IdentityID                  string    `json:"identity_id"`
	HeadHash                    string    `json:"head_hash"`
	VerifiedAt                  time.Time `json:"verified_at"`
	FreshUntil                  time.Time `json:"fresh_until"`
	Frozen                      bool      `json:"frozen"`
	IdentitySigningPublicKey    string    `json:"identity_signing_public_key"`
	RecoveryEncryptionPublicKey string    `json:"recovery_encryption_public_key"`
}

// globalIdentityVerifiedRecipient returns only a public, currently verified
// identity projection needed to construct a recipient-bound recovery
// envelope. It never returns credentials or private key material.
func (s *Server) globalIdentityVerifiedRecipient(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	if s.globalIdentity == nil || s.federatedIdentityResolver == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "recipient_identity_unavailable", "A current verified recipient identity is unavailable.")
		return
	}
	id := r.PathValue("identityID")
	if id == "" {
		writeCodedError(w, http.StatusBadRequest, "recipient_identity_invalid", "The recipient identity is invalid.")
		return
	}
	resolve := authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	state, err := resolve(id, "", time.Now().UTC())
	identitySigningKey := state.CurrentKeys["identity:"+id]
	if err != nil || state.Frozen || state.IdentityID != id || len(identitySigningKey) != ed25519.PublicKeySize || len(state.RecoveryEncryptionPublicKey) != 32 {
		writeCodedError(w, http.StatusConflict, "recipient_identity_unavailable", "The recipient identity is stale, frozen, or unavailable.")
		return
	}
	writeJSON(w, http.StatusOK, verifiedIdentityRecipientView{
		IdentityID: state.IdentityID, HeadHash: state.HeadHash, VerifiedAt: state.VerifiedAt,
		FreshUntil: state.FreshUntil, Frozen: state.Frozen,
		IdentitySigningPublicKey:    base64.RawURLEncoding.EncodeToString(identitySigningKey),
		RecoveryEncryptionPublicKey: base64.RawURLEncoding.EncodeToString(state.RecoveryEncryptionPublicKey),
	})
}

func (s *Server) globalIdentityHistoryPublish(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentity == nil || s.identityFederation == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "identity_federation_unavailable", "Identity history federation is not configured.")
		return
	}
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized || snapshot.Frozen {
		writeCodedError(w, http.StatusConflict, "global_identity_unavailable", "The local identity history cannot be published in its current state.")
		return
	}
	bundle, err := s.globalIdentity.ExportHistory()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "The signed public identity history is unavailable.")
		return
	}
	providers, err := s.identityFederation.PublishIdentityHistory(r.Context(), bundle, time.Now().UTC())
	if err != nil {
		writeCodedError(w, http.StatusBadGateway, "identity_history_publish_incomplete", "The signed identity history did not reach every configured Discovery provider.")
		return
	}
	if !s.auditMutation(w, r, "global_identity_history_published", snapshot.IdentityID) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "Identity history was published but its audit record could not be written.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"identity_id": snapshot.IdentityID, "head_hash": snapshot.HeadHash, "providers": providers})
}

// publishCurrentIdentityHistory mirrors a committed local identity mutation
// after it has already been durably accepted. Federation outages never roll
// back local identity changes or interrupt local recording; they remain
// visible as a bounded, sanitized status for an operator to retry.
func (s *Server) publishCurrentIdentityHistory(w http.ResponseWriter) {
	if s.identityFederation == nil {
		w.Header().Set("X-IR-Identity-History-Sync", "disabled")
		return
	}
	status := "pending"
	if s.globalIdentity != nil {
		if snapshot, initialized, err := s.globalIdentity.Snapshot(); err == nil && initialized && !snapshot.Frozen {
			if bundle, err := s.globalIdentity.ExportHistory(); err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, publishErr := s.identityFederation.PublishIdentityHistory(ctx, bundle, time.Now().UTC())
				cancel()
				if publishErr == nil {
					status = "published"
				}
			}
		}
	}
	w.Header().Set("X-IR-Identity-History-Sync", status)
	if status == "pending" && s.logs != nil {
		// Do not expose provider URLs, raw errors, identity secrets, or signed
		// event data in the operational log.
		s.logs.Add("warn", "identity-federation", "identity history publication is pending; remote authorization may fail closed until retry succeeds")
	}
}

func (s *Server) globalIdentityRegistryList(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	if s.globalIdentityRegistry == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_registry_unavailable", "Verified remote identity history is unavailable.")
		return
	}
	identities, err := s.globalIdentityRegistry.List()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_registry_unavailable", "Verified remote identity history is unavailable.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"identities": identities})
}

func (s *Server) globalIdentityRegistryImport(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentity == nil || s.globalIdentityRegistry == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_registry_unavailable", "Verified remote identity history is unavailable.")
		return
	}
	var bundle globalidentity.HistoryBundle
	if err := decodeJSONBody(w, r, 6<<20, &bundle); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_history_invalid", "The signed identity history is invalid.")
		return
	}
	local, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "The local Global Identity is unavailable.")
		return
	}
	if bundle.IdentityID == local.IdentityID {
		writeCodedError(w, http.StatusConflict, "identity_history_local_conflict", "The local identity history must use the node enrollment synchronization path.")
		return
	}
	err = s.globalIdentityRegistry.Import(bundle)
	if err != nil && !errors.Is(err, globalidentity.ErrIdentityFork) {
		switch {
		case errors.Is(err, globalidentity.ErrIdentityFrozen):
			writeCodedError(w, http.StatusConflict, "identity_frozen", "This identity history is frozen and cannot be advanced.")
		case errors.Is(err, globalidentity.ErrHistoryStale):
			writeCodedError(w, http.StatusConflict, "identity_history_stale", "The imported identity history is older than the registered history.")
		case errors.Is(err, globalidentity.ErrRegistryFull):
			writeCodedError(w, http.StatusConflict, "identity_registry_full", "The verified identity history registry reached its safe limit.")
		case errors.Is(err, globalidentity.ErrHistoryInvalid), errors.Is(err, globalidentity.ErrInvalidEvent), errors.Is(err, globalidentity.ErrHistoryDifferentID):
			writeCodedError(w, http.StatusBadRequest, "identity_history_invalid", "The signed identity history is invalid.")
		default:
			writeCodedError(w, http.StatusServiceUnavailable, "global_identity_registry_unavailable", "Verified remote identity history could not be stored safely.")
		}
		return
	}
	auditType := "remote_identity_history_imported"
	if errors.Is(err, globalidentity.ErrIdentityFork) {
		auditType = "remote_identity_history_fork_detected"
	}
	if !s.auditMutation(w, r, auditType, bundle.IdentityID) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "Identity history was stored but its audit record could not be written.")
		return
	}
	if errors.Is(err, globalidentity.ErrIdentityFork) {
		writeCodedError(w, http.StatusConflict, "identity_fork", "Competing valid identity histories were retained and the identity was frozen.")
		return
	}
	identities, err := s.globalIdentityRegistry.List()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_registry_unavailable", "Verified remote identity history could not be read safely.")
		return
	}
	for _, identity := range identities {
		if identity.IdentityID == bundle.IdentityID {
			writeJSON(w, http.StatusOK, identity)
			return
		}
	}
	writeCodedError(w, http.StatusServiceUnavailable, "global_identity_registry_unavailable", "The imported identity history could not be projected safely.")
}

func (s *Server) globalIdentityHistoryExport(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	if s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	bundle, err := s.globalIdentity.ExportHistory()
	if err != nil {
		writeCodedError(w, http.StatusConflict, "global_identity_history_unavailable", "Global identity history could not be exported safely.")
		return
	}
	writeJSON(w, http.StatusOK, bundle)
}

func (s *Server) globalIdentityForkChoices(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	if s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	choices, err := s.globalIdentity.ForkChoices()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_history_unavailable", "Verified fork evidence is unavailable.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"forks": choices})
}

func (s *Server) globalIdentityForkResolve(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	var envelope identityEventEnvelope
	if err := decodeJSONBody(w, r, 2<<20, &envelope); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_fork_resolution_invalid", "The recovery-root-signed fork resolution is invalid.")
		return
	}
	canonical, signature, err := decodeIdentityEventEnvelope(envelope.CanonicalEvent, envelope.Signature)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_fork_resolution_invalid", "The recovery-root-signed fork resolution is invalid.")
		return
	}
	event, err := identityprotocol.VerifyEvent(canonical, signature)
	if err != nil || event.Operation != "fork_resolve" {
		writeCodedError(w, http.StatusUnprocessableEntity, "identity_fork_resolution_invalid", "The signed event is not a valid fork resolution.")
		return
	}
	if err := s.globalIdentity.ApplyForkResolution(canonical, signature); err != nil {
		writeCodedError(w, http.StatusUnprocessableEntity, "identity_fork_resolution_rejected", "The recovery-root-signed selection does not match the retained fork evidence.")
		return
	}
	s.publishCurrentIdentityHistory(w)
	if !s.auditMutation(w, r, "global_identity_fork_resolved", eventPayloadString(event.Payload, "fork_id")) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "The fork resolution was stored but audit recording failed.")
		return
	}
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Resolved identity state could not be read safely.")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) globalIdentityHistoryImport(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentity == nil || s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Identity history import is unavailable.")
		return
	}
	var bundle globalidentity.HistoryBundle
	if err := decodeJSONBody(w, r, 6<<20, &bundle); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_history_invalid", "The identity history bundle is invalid.")
		return
	}
	current, initialized, err := s.globalIdentity.Snapshot()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity state could not be read safely.")
		return
	}
	local, infoErr := s.nodeIdentity.Info()
	if errors.Is(infoErr, nodeidentity.ErrNotCreated) {
		writeCodedError(w, http.StatusConflict, "node_identity_required", "Create this installation's Node Key before importing identity history.")
		return
	}
	if infoErr != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "The local Node Key could not be read safely.")
		return
	}
	localNodeID := local.NodeID
	localNodeActive := initialized && containsIdentityNode(current.NodeIDs, localNodeID)
	var delegationCanonical, delegationSignature []byte
	candidateCanonical, candidateSignature, delegationErr := findNodeDelegation(bundle, localNodeID)
	if delegationErr == nil {
		_, proofErr := s.nodeIdentity.ValidateEnrollmentEvent(candidateCanonical, candidateSignature, time.Now().UTC())
		if proofErr == nil {
			delegationCanonical, delegationSignature = candidateCanonical, candidateSignature
		} else if !localNodeActive || !errors.Is(proofErr, nodeidentity.ErrEnrollmentChallengeExpired) && !errors.Is(proofErr, nodeidentity.ErrEnrollmentChallengeConsumed) {
			writeCodedError(w, http.StatusForbidden, "node_enrollment_proof_invalid", "The Node Key proof is invalid, expired, or already used.")
			return
		}
	} else if !localNodeActive {
		writeCodedError(w, http.StatusForbidden, "node_enrollment_proof_required", "The bundle does not contain this installation's Node Key delegation.")
		return
	}
	if !initialized && len(delegationCanonical) == 0 {
		writeCodedError(w, http.StatusForbidden, "node_enrollment_proof_required", "A valid one-use Node Key enrollment proof is required to join this identity.")
		return
	}
	if len(delegationCanonical) > 0 {
		// Consume before publishing imported identity state. A crash between
		// these durable writes can retry the exact same event idempotently, but
		// cannot allow a second event to take over the enrollment nonce.
		if err := s.nodeIdentity.ConsumeEnrollmentEvent(delegationCanonical, delegationSignature, time.Now().UTC()); err != nil {
			writeCodedError(w, http.StatusConflict, "node_enrollment_state_unavailable", "The verified enrollment challenge could not be durably consumed.")
			return
		}
	}
	err = s.globalIdentity.ImportHistory(bundle, localNodeID)
	if errors.Is(err, globalidentity.ErrIdentityFork) {
		s.auditMutation(w, r, "identity_history_fork_detected", bundle.IdentityID)
		writeCodedError(w, http.StatusConflict, "identity_fork_detected", "Conflicting valid identity histories were preserved and frozen.")
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, globalidentity.ErrHistoryNodeNotActive):
			writeCodedError(w, http.StatusForbidden, "node_not_authorized", "This Node Key is not delegated by the imported identity.")
		case errors.Is(err, globalidentity.ErrHistoryDifferentID):
			writeCodedError(w, http.StatusConflict, "identity_history_conflict", "The imported identity differs from this installation's pinned identity.")
		case errors.Is(err, globalidentity.ErrHistoryInvalid):
			writeCodedError(w, http.StatusBadRequest, "identity_history_invalid", "The signed identity history could not be verified.")
		default:
			writeCodedError(w, http.StatusServiceUnavailable, "identity_history_unavailable", "Identity history could not be imported safely.")
		}
		return
	}
	s.publishCurrentIdentityHistory(w)
	s.auditMutation(w, r, "identity_history_imported", bundle.IdentityID)
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Imported identity state could not be read safely.")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func containsIdentityNode(nodeIDs []string, nodeID string) bool {
	for _, candidate := range nodeIDs {
		if candidate == nodeID {
			return true
		}
	}
	return false
}

func (s *Server) nodeIdentityEnrollmentChallenge(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "Node identity is unavailable.")
		return
	}
	var request struct {
		IdentityID   string `json:"identity_id"`
		PreviousHash string `json:"previous_event_hash"`
	}
	if err := decodeJSONBody(w, r, 2<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "node_enrollment_invalid", "The enrollment context is invalid.")
		return
	}
	if s.globalIdentity != nil {
		current, initialized, err := s.globalIdentity.Snapshot()
		if err != nil {
			writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Local identity state could not be read safely.")
			return
		}
		if initialized && (current.Frozen || current.IdentityID != request.IdentityID || current.HeadHash != request.PreviousHash) {
			writeCodedError(w, http.StatusConflict, "identity_head_stale", "Enrollment must use the current unfrozen identity head.")
			return
		}
	}
	proof, err := s.nodeIdentity.BeginEnrollmentProof(request.IdentityID, request.PreviousHash, time.Now().UTC())
	if errors.Is(err, nodeidentity.ErrEnrollmentChallengeActive) {
		writeCodedError(w, http.StatusConflict, "node_enrollment_pending", "A valid enrollment challenge is already pending.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "node_enrollment_invalid", "A bounded Node Key enrollment proof could not be created.")
		return
	}
	s.auditMutation(w, r, "node_enrollment_challenge_issued", proof.NodeID)
	writeJSON(w, http.StatusCreated, proof)
}

func (s *Server) globalIdentityNodeDelegate(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	s.applyGlobalIdentityNodeEvent(w, r, "node_delegate", "node_identity_delegated")
}

func (s *Server) globalIdentityNodeRevoke(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	var envelope identityEventEnvelope
	if err := decodeJSONBody(w, r, 2<<20, &envelope); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	canonical, signature, err := decodeIdentityEventEnvelope(envelope.CanonicalEvent, envelope.Signature)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	event, err := identityprotocol.VerifyEvent(canonical, signature)
	if err != nil || event.Operation != "node_revoke" || eventPayloadString(event.Payload, "node_id") != r.PathValue("nodeID") {
		writeCodedError(w, http.StatusUnprocessableEntity, "node_revoke_rejected", "The signed Node Key revocation does not match this request.")
		return
	}
	s.applyVerifiedNodeEvent(w, r, event, canonical, signature, "node_identity_revoked")
}

func (s *Server) applyGlobalIdentityNodeEvent(w http.ResponseWriter, r *http.Request, operation, auditType string) {
	if s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	var envelope identityEventEnvelope
	if err := decodeJSONBody(w, r, 2<<20, &envelope); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	canonical, signature, err := decodeIdentityEventEnvelope(envelope.CanonicalEvent, envelope.Signature)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	event, err := identityprotocol.VerifyEvent(canonical, signature)
	if err != nil || event.Operation != operation {
		writeCodedError(w, http.StatusUnprocessableEntity, "identity_event_invalid", "The signed identity event has the wrong operation.")
		return
	}
	s.applyVerifiedNodeEvent(w, r, event, canonical, signature, auditType)
}

func (s *Server) applyVerifiedNodeEvent(w http.ResponseWriter, r *http.Request, event identityprotocol.Event, canonical, signature []byte, auditType string) {
	if s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	err := s.globalIdentity.ApplyNodeEvent(canonical, signature)
	if errors.Is(err, globalidentity.ErrIdentityFork) {
		s.auditMutation(w, r, "identity_history_fork_detected", event.IdentityID)
		writeCodedError(w, http.StatusConflict, "identity_fork_detected", "Conflicting valid identity histories were preserved and frozen.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusUnprocessableEntity, "identity_node_event_rejected", "The signed Node Key event was rejected.")
		return
	}
	s.publishCurrentIdentityHistory(w)
	objectID := eventPayloadString(event.Payload, "node_id")
	if !s.auditMutation(w, r, auditType, objectID) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "The identity mutation was stored but audit recording failed.")
		return
	}
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Updated identity state could not be read safely.")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func findNodeDelegation(bundle globalidentity.HistoryBundle, nodeID string) ([]byte, []byte, error) {
	var selectedCanonical, selectedSignature []byte
	for _, item := range bundle.Events {
		canonical, signature, err := decodeIdentityEventEnvelope(item.CanonicalEvent, item.Signature)
		if err != nil {
			return nil, nil, err
		}
		event, err := identityprotocol.VerifyEvent(canonical, signature)
		if err != nil {
			return nil, nil, err
		}
		if event.Operation == "node_delegate" && eventPayloadString(event.Payload, "node_id") == nodeID {
			selectedCanonical, selectedSignature = canonical, signature
		}
	}
	if len(selectedCanonical) == 0 {
		return nil, nil, errors.New("delegation event not found")
	}
	return selectedCanonical, selectedSignature, nil
}

func eventPayloadString(payload map[string]any, name string) string {
	value, _ := payload[name].(string)
	return value
}

func (s *Server) nodeIdentityStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	if s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "Node identity is unavailable.")
		return
	}
	info, err := s.nodeIdentity.Info()
	if errors.Is(err, nodeidentity.ErrNotCreated) {
		writeJSON(w, http.StatusOK, map[string]any{"created": false, "node": nil})
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "Node identity state could not be read safely.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"created": true, "node": info})
}

func (s *Server) nodeIdentityCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "Node identity is unavailable.")
		return
	}
	info, err := s.nodeIdentity.Create()
	if errors.Is(err, nodeidentity.ErrExists) {
		writeCodedError(w, http.StatusConflict, "node_identity_exists", "This installation already has a Node Key.")
		return
	}
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "A Node Key could not be created safely.")
		return
	}
	auditRecorded := s.auditMutation(w, r, "node_identity_created", info.NodeID)
	writeJSON(w, http.StatusCreated, map[string]any{"created": true, "node": info, "audit_recorded": auditRecorded})
}

func (s *Server) globalIdentityStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	if s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity state could not be read safely.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"initialized": initialized, "identity": func() any {
		if initialized {
			return snapshot
		}
		return nil
	}()})
}

func (s *Server) globalIdentityRegistrationOptions(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentityCeremonies == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	var request struct {
		IdentityID string `json:"identity_id"`
	}
	if err := decodeJSONBody(w, r, 2<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_request_invalid", "The identity request is invalid.")
		return
	}
	rpID, origin, ok := globalIdentityRequestContext(r)
	if !ok {
		writeCodedError(w, http.StatusForbidden, "webauthn_origin_invalid", "Passkey setup requires this application's secure origin. Use HTTPS or localhost.")
		return
	}
	token, options, err := s.globalIdentityCeremonies.BeginRegistration(rpID, origin, request.IdentityID)
	if err != nil {
		writeCodedError(w, http.StatusConflict, "identity_registration_unavailable", "Identity registration could not be started.")
		return
	}
	setIdentityChallengeCookie(w, r, token)
	writeJSON(w, http.StatusOK, options)
}

func (s *Server) globalIdentityRegistrationFinish(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentityCeremonies == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	rpID, origin, ok := globalIdentityRequestContext(r)
	if !ok {
		clearIdentityChallengeCookie(w, r)
		writeCodedError(w, http.StatusForbidden, "webauthn_origin_invalid", "Passkey setup requires this application's secure origin. Use HTTPS or localhost.")
		return
	}
	_ = rpID
	var envelope identityRegistrationEnvelope
	if err := decodeJSONBody(w, r, 2<<20, &envelope); err != nil || len(envelope.Credential) == 0 || len(envelope.Credential) > 512<<10 {
		clearIdentityChallengeCookie(w, r)
		writeCodedError(w, http.StatusBadRequest, "identity_registration_invalid", "Passkey registration data is invalid.")
		return
	}
	canonical, signature, err := decodeIdentityEventEnvelope(envelope.CanonicalEvent, envelope.Signature)
	if err != nil {
		clearIdentityChallengeCookie(w, r)
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	challenge, err := r.Cookie(globalIdentityCeremonyCookie)
	if err != nil || challenge.Value == "" {
		clearIdentityChallengeCookie(w, r)
		writeCodedError(w, http.StatusBadRequest, "webauthn_challenge_missing", "The passkey challenge expired. Start again.")
		return
	}
	request := r.Clone(r.Context())
	request.Body = io.NopCloser(bytes.NewReader(envelope.Credential))
	request.ContentLength = int64(len(envelope.Credential))
	request.Header = r.Header.Clone()
	request.Header.Set("Content-Type", "application/json")
	snapshot, err := s.globalIdentityCeremonies.FinishRegistration(challenge.Value, origin, request, canonical, signature)
	clearIdentityChallengeCookie(w, r)
	if err != nil {
		writeCodedError(w, http.StatusUnprocessableEntity, "identity_registration_rejected", "Passkey registration or signed identity verification failed.")
		return
	}
	s.publishCurrentIdentityHistory(w)
	writeJSON(w, http.StatusOK, map[string]any{"initialized": true, "identity": snapshot})
}

func (s *Server) globalIdentityRevokeCredential(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentityCeremonies == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	var envelope identityEventEnvelope
	if err := decodeJSONBody(w, r, 2<<20, &envelope); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	canonical, signature, err := decodeIdentityEventEnvelope(envelope.CanonicalEvent, envelope.Signature)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_event_invalid", "The signed identity event is invalid.")
		return
	}
	if err := s.globalIdentityCeremonies.RevokeCredential(canonical, signature, r.PathValue("credentialID")); err != nil {
		writeCodedError(w, http.StatusUnprocessableEntity, "identity_credential_revoke_rejected", "Credential revocation was rejected.")
		return
	}
	s.publishCurrentIdentityHistory(w)
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity state could not be read safely.")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) globalIdentityLoginOptions(w http.ResponseWriter, r *http.Request) {
	if s.globalIdentityCeremonies == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	rpID, origin, ok := globalIdentityRequestContext(r)
	if !ok {
		writeCodedError(w, http.StatusForbidden, "webauthn_origin_invalid", "Passkey sign-in requires this application's secure origin. Use HTTPS or localhost.")
		return
	}
	var request struct {
		RecipientPublicKey string `json:"recipient_public_key,omitempty"`
	}
	if err := decodeJSONBody(w, r, 2<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "identity_request_invalid", "The passkey request is invalid.")
		return
	}
	var playbackPublicKey []byte
	if request.RecipientPublicKey != "" {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(request.RecipientPublicKey)
		if decodeErr != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != request.RecipientPublicKey {
			writeCodedError(w, http.StatusBadRequest, "identity_request_invalid", "The passkey request is invalid.")
			return
		}
		playbackPublicKey = decoded
	}
	var token string
	var options *protocol.CredentialAssertion
	var err error
	if len(playbackPublicKey) == 0 {
		token, options, err = s.globalIdentityCeremonies.BeginLogin(rpID, origin)
	} else {
		token, options, err = s.globalIdentityCeremonies.BeginLogin(rpID, origin, playbackPublicKey)
	}
	if err != nil {
		writeCodedError(w, http.StatusConflict, "identity_login_unavailable", "Passkey sign-in could not be started.")
		clear(playbackPublicKey)
		return
	}
	clear(playbackPublicKey)
	setIdentityChallengeCookie(w, r, token)
	writeJSON(w, http.StatusOK, options)
}

func (s *Server) globalIdentityLoginFinish(w http.ResponseWriter, r *http.Request) {
	if s.globalIdentityCeremonies == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity is unavailable.")
		return
	}
	rpID, origin, ok := globalIdentityRequestContext(r)
	if !ok {
		clearIdentityChallengeCookie(w, r)
		writeCodedError(w, http.StatusForbidden, "webauthn_origin_invalid", "Passkey sign-in requires this application's secure origin. Use HTTPS or localhost.")
		return
	}
	_ = rpID
	r.Body = http.MaxBytesReader(w, r.Body, 512<<10)
	challenge, err := r.Cookie(globalIdentityCeremonyCookie)
	if err != nil || challenge.Value == "" {
		clearIdentityChallengeCookie(w, r)
		writeCodedError(w, http.StatusBadRequest, "webauthn_challenge_missing", "The passkey challenge expired. Start again.")
		return
	}
	identityID, sessionToken, csrfToken, err := s.globalIdentityCeremonies.FinishLogin(challenge.Value, origin, r)
	clearIdentityChallengeCookie(w, r)
	if err != nil {
		writeCodedError(w, http.StatusUnauthorized, "identity_authentication_failed", "Passkey authentication failed.")
		return
	}
	secure := secureBrowserRequest(r) || s.forceSecureCookie
	http.SetCookie(w, &http.Cookie{Name: globalIdentitySessionCookie, Value: sessionToken, Path: "/api/global-identity", Expires: time.Now().Add(12 * time.Hour), MaxAge: int((12 * time.Hour) / time.Second), HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode})
	http.SetCookie(w, &http.Cookie{Name: globalIdentityCSRFCookie, Value: csrfToken, Path: "/api/global-identity", Expires: time.Now().Add(12 * time.Hour), MaxAge: int((12 * time.Hour) / time.Second), Secure: secure, SameSite: http.SameSiteStrictMode})
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "identity_id": identityID})
}

func (s *Server) globalIdentitySession(w http.ResponseWriter, r *http.Request) {
	if s.globalIdentityCeremonies == nil {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	cookie, err := r.Cookie(globalIdentitySessionCookie)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	identityID, csrfToken, authenticated := s.globalIdentityCeremonies.Session(cookie.Value)
	if !authenticated {
		writeJSON(w, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	// Keep this token name distinct from the local account CSRF token. The
	// frontend API client intentionally learns only the local token from
	// authenticated account/session responses.
	_, playbackKeyBound := s.globalIdentityCeremonies.SessionPlaybackPublicKey(cookie.Value)
	writeJSON(w, http.StatusOK, map[string]any{"authenticated": true, "identity_id": identityID, "global_csrf_token": csrfToken, "playback_key_bound": playbackKeyBound})
}

func (s *Server) globalIdentitySessionKeyEpochs(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionRecordingRead) {
		return
	}
	identityID, _, ok := s.globalPlaybackSession(w, r, false)
	if !ok {
		return
	}
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	keyReader, keyOK := s.peerSessions.(peerArchiveSessionKeyReader)
	if !ok || !keyOK || !s.peerSessionsAvailable() {
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
	if identityID != scope.IdentityID {
		writeCodedError(w, http.StatusForbidden, "passkey_identity_mismatch", "Sign in with this Recorder's Global Identity to unlock the archive key.")
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
	epochs, err := keyReader.SessionKeyEpochs(ctx, nodeID, scope, snapshot)
	if err != nil {
		writePeerArchiveReadError(w, err, "The remote archive key availability could not be verified.")
		return
	}
	values := make([]string, 0, len(epochs))
	for _, epoch := range epochs {
		if epoch == 0 {
			writeCodedError(w, http.StatusBadGateway, "peer_archive_key_epochs_invalid", "The remote archive key metadata is invalid.")
			return
		}
		values = append(values, strconv.FormatUint(epoch, 10))
	}
	writePeerArchiveJSON(w, map[string]any{"node_id": nodeID, "archive_id": scope.ArchiveID, "snapshot_id": snapshotID, "key_epochs": values})
}

func (s *Server) globalIdentitySessionKeyEnvelope(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionRecordingRead) {
		return
	}
	if !sameOriginBootstrapRequest(r) {
		writeCodedError(w, http.StatusForbidden, "csrf_validation_failed", "The request origin could not be verified.")
		return
	}
	identityID, sessionToken, ok := s.globalPlaybackSession(w, r, true)
	if !ok {
		return
	}
	reader, ok := s.peerSessions.(peerArchiveDataReader)
	keyReader, keyOK := s.peerSessions.(peerArchiveSessionKeyReader)
	if !ok || !keyOK || !s.peerSessionsAvailable() {
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
	if identityID != scope.IdentityID {
		writeCodedError(w, http.StatusForbidden, "passkey_identity_mismatch", "Sign in with this Recorder's Global Identity to unlock the archive key.")
		return
	}
	snapshotID := r.PathValue("snapshotID")
	keyEpoch, epochOK := parsePeerArchiveUint(r.URL.Query().Get("key_epoch"), true)
	if !validSnapshotID(snapshotID) || !epochOK {
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
	// Consume the Passkey-bound recipient only after request, CSRF, local
	// permission, live capability, and snapshot checks have succeeded. This
	// prevents malformed or unauthorized requests from burning the one-use key.
	recipientPublicKey, keyBound := s.globalIdentityCeremonies.TakeSessionPlaybackPublicKey(sessionToken)
	if !keyBound {
		writeCodedError(w, http.StatusUnauthorized, "passkey_playback_key_required", "Start a new Passkey playback session to unlock this archive.")
		return
	}
	defer clear(recipientPublicKey)
	envelope, err := keyReader.SessionDEKEnvelope(ctx, nodeID, scope, snapshot, keyEpoch, recipientPublicKey)
	if err != nil {
		writePeerArchiveReadError(w, err, "The session-bound archive key could not be retrieved or verified.")
		return
	}
	if envelope.ArchiveID != snapshot.Content.ArchiveID || envelope.SnapshotID != snapshot.SnapshotID ||
		envelope.KeyEpoch != strconv.FormatUint(keyEpoch, 10) || envelope.RecipientKeyID != cryptoarchive.RecipientKeyID(recipientPublicKey) ||
		envelope.SupplierNodeID != nodeID || envelope.OwnerSubjectID != scope.OwnerSubjectID {
		writeCodedError(w, http.StatusBadGateway, "peer_archive_key_envelope_invalid", "The remote session key envelope is invalid.")
		return
	}
	writePeerArchiveJSON(w, envelope)
}

func (s *Server) globalPlaybackSession(w http.ResponseWriter, r *http.Request, requireCSRF bool) (string, string, bool) {
	if s.globalIdentityCeremonies == nil {
		writeCodedError(w, http.StatusUnauthorized, "global_identity_authentication_required", "Authenticate with a Passkey before online archive playback.")
		return "", "", false
	}
	cookie, err := r.Cookie(globalIdentitySessionCookie)
	if err != nil || cookie.Value == "" {
		writeCodedError(w, http.StatusUnauthorized, "global_identity_authentication_required", "Authenticate with a Passkey before online archive playback.")
		return "", "", false
	}
	if requireCSRF {
		csrfCookie, cookieErr := r.Cookie(globalIdentityCSRFCookie)
		if cookieErr != nil || !s.globalIdentityCeremonies.ValidCSRF(cookie.Value, csrfCookie.Value) ||
			!s.globalIdentityCeremonies.ValidCSRF(cookie.Value, r.Header.Get("X-IR-Global-CSRF")) {
			writeCodedError(w, http.StatusForbidden, "csrf_validation_failed", "The request could not be verified.")
			return "", "", false
		}
	}
	identityID, _, authenticated := s.globalIdentityCeremonies.Session(cookie.Value)
	publicKey, keyBound := s.globalIdentityCeremonies.SessionPlaybackPublicKey(cookie.Value)
	clear(publicKey)
	if !authenticated || !keyBound {
		writeCodedError(w, http.StatusUnauthorized, "passkey_playback_key_required", "Sign in with a Passkey for this playback session.")
		return "", "", false
	}
	return identityID, cookie.Value, true
}

func (s *Server) globalIdentityLogout(w http.ResponseWriter, r *http.Request) {
	if s.globalIdentityCeremonies == nil || !sameOriginBootstrapRequest(r) {
		writeCodedError(w, http.StatusForbidden, "identity_logout_rejected", "The sign-out request was rejected.")
		return
	}
	session, err := r.Cookie(globalIdentitySessionCookie)
	csrf, csrfErr := r.Cookie(globalIdentityCSRFCookie)
	if err == nil && csrfErr == nil && s.globalIdentityCeremonies.ValidCSRF(session.Value, csrf.Value) && s.globalIdentityCeremonies.ValidCSRF(session.Value, r.Header.Get("X-IR-Global-CSRF")) {
		s.globalIdentityCeremonies.Logout(session.Value)
	}
	secure := secureBrowserRequest(r) || s.forceSecureCookie
	http.SetCookie(w, expiredIdentityCookie(globalIdentitySessionCookie, true, secure))
	http.SetCookie(w, expiredIdentityCookie(globalIdentityCSRFCookie, false, secure))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) requireGlobalIdentityManagement(w http.ResponseWriter, r *http.Request, permission authn.Permission) bool {
	principal, ok := authn.PrincipalFromContext(r.Context())
	if !ok || s.auth == nil {
		writeCodedError(w, http.StatusUnauthorized, "authentication_required", "Authentication required.")
		return false
	}
	if !authn.HasPermission(principal, permission) {
		writeCodedError(w, http.StatusForbidden, "permission_denied", "Permission denied.")
		return false
	}
	return true
}

func globalIdentityRequestContext(r *http.Request) (rpID, origin string, ok bool) {
	if r == nil || !sameOriginBootstrapRequest(r) {
		return "", "", false
	}
	parsed, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || parsed.Host == "" {
		return "", "", false
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "" || net.ParseIP(host) != nil {
		return "", "", false
	}
	local := host == "localhost"
	if parsed.Scheme == "https" {
		if !secureBrowserRequest(r) {
			return "", "", false
		}
	} else if parsed.Scheme != "http" || !local || secureBrowserRequest(r) {
		return "", "", false
	}
	return host, parsed.Scheme + "://" + parsed.Host, true
}

func decodeIdentityEventEnvelope(canonicalText, signatureText string) ([]byte, []byte, error) {
	if canonicalText == "" || len(canonicalText) > (1<<20)*2 || signatureText == "" || len(signatureText) > 128 {
		return nil, nil, errors.New("identity event envelope is invalid")
	}
	canonical, err := base64.RawURLEncoding.DecodeString(canonicalText)
	if err != nil || len(canonical) == 0 || len(canonical) > 1<<20 || base64.RawURLEncoding.EncodeToString(canonical) != canonicalText {
		return nil, nil, errors.New("identity event encoding is invalid")
	}
	signature, err := base64.RawURLEncoding.DecodeString(signatureText)
	if err != nil || len(signature) != 64 || base64.RawURLEncoding.EncodeToString(signature) != signatureText {
		return nil, nil, errors.New("identity signature encoding is invalid")
	}
	return canonical, signature, nil
}

func setIdentityChallengeCookie(w http.ResponseWriter, r *http.Request, value string) {
	secure := secureBrowserRequest(r) || r.TLS != nil
	http.SetCookie(w, &http.Cookie{Name: globalIdentityCeremonyCookie, Value: value, Path: "/api/global-identity", MaxAge: int(globalidentityCeremonyCookieLifetime().Seconds()), Expires: time.Now().Add(globalidentityCeremonyCookieLifetime()), HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode})
}

func clearIdentityChallengeCookie(w http.ResponseWriter, r *http.Request) {
	secure := secureBrowserRequest(r) || r.TLS != nil
	http.SetCookie(w, expiredIdentityCookie(globalIdentityCeremonyCookie, true, secure))
}

func expiredIdentityCookie(name string, httpOnly, secure bool) *http.Cookie {
	return &http.Cookie{Name: name, Value: "", Path: "/api/global-identity", MaxAge: -1, Expires: time.Unix(1, 0), HttpOnly: httpOnly, Secure: secure, SameSite: http.SameSiteStrictMode}
}

func globalidentityCeremonyCookieLifetime() time.Duration { return 5 * time.Minute }
