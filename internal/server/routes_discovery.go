package server

import (
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/nodeidentity"
)

type signNodeAdvertisementRequest struct {
	AddressEnvelope string   `json:"address_envelope"`
	Capabilities    []string `json:"capabilities"`
	RelayIDs        []string `json:"relay_ids"`
}

type signedNodeAdvertisementResponse struct {
	CanonicalAdvertisement string `json:"canonical_advertisement"`
	Signature              string `json:"signature"`
}

func (s *Server) registerDiscoveryRoutes() {
	s.mux.HandleFunc("POST /api/node-identity/advertisement/sign", s.signNodeAdvertisement)
}

// signNodeAdvertisement signs only a short-lived advertisement for the
// current, unfrozen identity head and an actively delegated local Node Key.
// The bounded address envelope is never decrypted or logged here; the
// recipient must verify its HPKE context and tag. The signature is not a
// remote authorization.
func (s *Server) signNodeAdvertisement(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	if s.globalIdentity == nil || s.nodeIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "discovery_unavailable", "Node discovery is unavailable.")
		return
	}
	var request signNodeAdvertisementRequest
	if err := decodeJSONBody(w, r, 24<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "node_advertisement_invalid", "The node advertisement is invalid.")
		return
	}
	snapshot, initialized, err := s.globalIdentity.Snapshot()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Global identity state could not be read safely.")
		return
	}
	if !initialized {
		writeCodedError(w, http.StatusConflict, "global_identity_required", "Create or import a Global Identity before advertising this node.")
		return
	}
	if snapshot.Frozen {
		writeCodedError(w, http.StatusConflict, "identity_frozen", "A frozen identity cannot publish a node advertisement.")
		return
	}
	localNode, err := s.nodeIdentity.Info()
	if err != nil {
		if err == nodeidentity.ErrNotCreated {
			writeCodedError(w, http.StatusConflict, "node_identity_required", "Create this installation's Node Key before advertising it.")
		} else {
			writeCodedError(w, http.StatusServiceUnavailable, "node_identity_unavailable", "The local Node Key could not be read safely.")
		}
		return
	}
	if !containsIdentityNode(snapshot.NodeIDs, localNode.NodeID) {
		writeCodedError(w, http.StatusForbidden, "node_not_authorized", "The local Node Key is not delegated by the current identity history.")
		return
	}
	addressEnvelope, err := discoveryprotocol.ParseAddressEnvelope(request.AddressEnvelope)
	recoveryPublicKey, decodeErr := base64.RawURLEncoding.DecodeString(snapshot.RecoveryEncryptionPublicKey)
	if err != nil || decodeErr != nil || discoveryprotocol.AddressRecipientPublicKeyHash(recoveryPublicKey) != addressEnvelope.RecipientPublicKeySHA256 {
		writeCodedError(w, http.StatusBadRequest, "node_advertisement_invalid", "The address envelope is not encrypted for this Global Identity recovery key.")
		return
	}
	identityHint, err := discoveryprotocol.GlobalIdentityHint(snapshot.IdentityID)
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "node_advertisement_unavailable", "A bounded node advertisement could not be prepared.")
		return
	}
	now := time.Now().UTC().Truncate(time.Second)
	advertisement := discoveryprotocol.Advertisement{
		AdvertisementVersion: discoveryprotocol.Version,
		GlobalIdentityHint:   identityHint,
		IdentityHeadHash:     snapshot.HeadHash,
		IssuedAt:             now.Format(time.RFC3339),
		ExpiresAt:            now.Add(5 * time.Minute).Format(time.RFC3339),
		AddressEnvelope:      request.AddressEnvelope,
		Capabilities:         request.Capabilities,
		RelayIDs:             request.RelayIDs,
	}
	canonical, signature, err := s.nodeIdentity.SignAdvertisement(advertisement)
	if err != nil {
		if errors.Is(err, discoveryprotocol.ErrInvalid) {
			writeCodedError(w, http.StatusBadRequest, "node_advertisement_invalid", "The node advertisement could not be signed safely.")
		} else {
			writeCodedError(w, http.StatusServiceUnavailable, "node_advertisement_unavailable", "The node advertisement could not be signed safely.")
		}
		return
	}
	if !s.auditMutation(w, r, "node_advertisement_signed", localNode.NodeID) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "The node advertisement was not returned because its audit record could not be written.")
		return
	}
	writeJSON(w, http.StatusCreated, signedNodeAdvertisementResponse{
		CanonicalAdvertisement: base64.RawURLEncoding.EncodeToString(canonical),
		Signature:              base64.RawURLEncoding.EncodeToString(signature),
	})
}
