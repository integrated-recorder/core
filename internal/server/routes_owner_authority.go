package server

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
)

const ownerAuthorityPublishTimeout = 10 * time.Second

type ownerAuthorityGrantView struct {
	GrantHash  string              `json:"grant_hash"`
	Issuer     authority.Principal `json:"issuer"`
	Audience   authority.Principal `json:"audience"`
	ArchiveID  string              `json:"archive_id"`
	SnapshotID string              `json:"snapshot_id,omitempty"`
	Actions    []string            `json:"actions"`
	ExpiresAt  string              `json:"expires_at"`
	Revoked    bool                `json:"revoked"`
	Depth      string              `json:"delegation_depth"`
}

type ownerAuthoritySnapshotView struct {
	Subject                authority.OwnerSubject      `json:"subject"`
	ReplicationPolicy      authority.ReplicationPolicy `json:"replication_policy"`
	ArchiveIDs             []string                    `json:"archive_ids"`
	RekeyRequiredArchives  []string                    `json:"rekey_required_archives"`
	RekeyRequiredSinceHash string                      `json:"rekey_required_since_event_hash,omitempty"`
	Sequence               string                      `json:"sequence"`
	HeadHash               string                      `json:"head_hash"`
	Frozen                 bool                        `json:"frozen"`
	ForkCount              int                         `json:"fork_count"`
	Grants                 []ownerAuthorityGrantView   `json:"grants"`
}

type ownerAuthorityForkView struct {
	PreviousHash string                       `json:"previous_event_hash,omitempty"`
	First        authority.AuthorityEventWire `json:"first_event"`
	Second       authority.AuthorityEventWire `json:"second_event"`
}

func (s *Server) registerOwnerAuthorityRoutes() {
	s.mux.HandleFunc("GET /api/owner-authority/subjects", s.ownerAuthorityList)
	s.mux.HandleFunc("GET /api/owner-authority/subjects/{subjectID}", s.ownerAuthorityGet)
	s.mux.HandleFunc("GET /api/owner-authority/subjects/{subjectID}/events", s.ownerAuthorityHistory)
	s.mux.HandleFunc("POST /api/owner-authority/subjects/{subjectID}/sync", s.ownerAuthoritySync)
	s.mux.HandleFunc("POST /api/owner-authority/events", s.ownerAuthorityApplyEvent)
}

func (s *Server) ownerAuthorityList(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	store, resolver, ok := s.ownerAuthorityDependencies(w)
	if !ok {
		return
	}
	subjects, err := store.List(resolver, time.Now().UTC())
	if err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	views := make([]ownerAuthoritySnapshotView, 0, len(subjects))
	for _, subject := range subjects {
		views = append(views, ownerAuthorityView(subject))
	}
	writeJSON(w, http.StatusOK, map[string]any{"subjects": views})
}

func (s *Server) ownerAuthorityGet(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	store, resolver, ok := s.ownerAuthorityDependencies(w)
	if !ok {
		return
	}
	snapshot, err := store.Snapshot(r.PathValue("subjectID"), resolver, time.Now().UTC())
	if err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ownerAuthorityView(snapshot))
}

func (s *Server) ownerAuthorityHistory(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemRead) {
		return
	}
	store, resolver, ok := s.ownerAuthorityDependencies(w)
	if !ok {
		return
	}
	if _, err := store.Snapshot(r.PathValue("subjectID"), resolver, time.Now().UTC()); err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	events, forks, err := store.ExportEvidence(r.PathValue("subjectID"))
	if err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	wire := make([]authority.AuthorityEventWire, 0, len(events))
	for _, event := range events {
		wire = append(wire, authorityEventWire(event))
	}
	forkWire := make([]ownerAuthorityForkView, 0, len(forks))
	for _, fork := range forks {
		forkWire = append(forkWire, ownerAuthorityForkView{
			PreviousHash: fork.PreviousHash,
			First:        authorityEventWire(fork.First),
			Second:       authorityEventWire(fork.Second),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": wire, "forks": forkWire})
}

// ownerAuthoritySync imports a signed OwnerSubject history only when every
// configured Discovery mirror returns the same current candidate set. The
// authority Store still verifies all historical identity signatures and
// durably freezes any valid fork; Discovery is a transport, not a trust root.
func (s *Server) ownerAuthoritySync(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	store, resolver, ok := s.ownerAuthorityDependencies(w)
	if !ok {
		return
	}
	if s.identityFederation == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "owner_authority_sync_unavailable", "No verified authority mirrors are configured.")
		return
	}
	subjectID := r.PathValue("subjectID")
	if !validReplicaOwnerSubjectID(subjectID) {
		writeCodedError(w, http.StatusBadRequest, "owner_authority_sync_invalid", "A valid OwnerSubject identifier is required.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), ownerAuthorityPublishTimeout)
	defer cancel()
	err := store.SynchronizeFederatedHistory(ctx, subjectID, s.identityFederation, resolver, time.Now().UTC())
	if !s.auditMutation(w, r, "owner_subject_history_sync", subjectID) {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "Authority history synchronization could not be recorded in the audit log.")
		return
	}
	if err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	snapshot, err := store.Snapshot(subjectID, resolver, time.Now().UTC())
	if err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ownerAuthorityView(snapshot))
}

func authorityEventWire(event authority.SignedAuthorityEvent) authority.AuthorityEventWire {
	return authority.AuthorityEventWire{Event: event.Event, Signature: base64.RawURLEncoding.EncodeToString(event.Signature)}
}

func (s *Server) ownerAuthorityApplyEvent(w http.ResponseWriter, r *http.Request) {
	if !s.requireGlobalIdentityManagement(w, r, authn.PermissionSystemSettings) {
		return
	}
	store, resolver, ok := s.ownerAuthorityDependencies(w)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, (64<<10)+1))
	if err != nil || len(body) == 0 || len(body) > 64<<10 {
		writeCodedError(w, http.StatusBadRequest, "owner_authority_event_invalid", "The signed OwnerSubject event is invalid.")
		return
	}
	event, err := authority.ParseAuthorityEvent(body)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "owner_authority_event_invalid", "The signed OwnerSubject event is invalid.")
		return
	}
	if err := s.prepareOwnerAuthorityMutation(r, store, resolver, event.Event.SubjectID); err != nil {
		if errors.Is(err, authority.ErrAuthorityFork) || errors.Is(err, authority.ErrAuthorityStale) {
			writeOwnerAuthorityError(w, err)
		} else {
			writeCodedError(w, http.StatusServiceUnavailable, "owner_authority_federation_incomplete", "The current signed authority history is not synchronized across every configured mirror. Retry after synchronization recovers.")
		}
		return
	}
	if err := store.Apply(event, resolver, time.Now().UTC()); err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	publishErr := s.publishOwnerAuthorityHistory(r, store, resolver, event.Event.SubjectID)
	auditOK := s.auditMutation(w, r, "owner_subject_event_applied", event.Event.SubjectID)
	if publishErr != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "owner_authority_federation_incomplete", "The signed change is stored locally but has not reached every configured authority mirror. Retry the same signed event to complete synchronization.")
		return
	}
	if !auditOK {
		writeCodedError(w, http.StatusServiceUnavailable, "audit_unavailable", "The authority event was stored but audit recording failed.")
		return
	}
	snapshot, err := store.Snapshot(event.Event.SubjectID, resolver, time.Now().UTC())
	if err != nil {
		writeOwnerAuthorityError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ownerAuthorityView(snapshot))
}

func (s *Server) prepareOwnerAuthorityMutation(r *http.Request, store *authority.Store, resolver authority.AuthorityIdentityResolver, subjectID string) error {
	if s.identityFederation == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), ownerAuthorityPublishTimeout)
	defer cancel()
	bundle, err := store.ExportFederatedHistory(subjectID, resolver, time.Now().UTC())
	if errors.Is(err, authority.ErrAuthorityStore) {
		// A subject-create event has no prior durable history to synchronize.
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := s.identityFederation.PublishOwnerAuthorityHistory(ctx, subjectID, bundle); err != nil {
		return err
	}
	return store.SynchronizeFederatedHistory(ctx, subjectID, s.identityFederation, resolver, time.Now().UTC())
}

func (s *Server) publishOwnerAuthorityHistory(r *http.Request, store *authority.Store, resolver authority.AuthorityIdentityResolver, subjectID string) error {
	if s.identityFederation == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), ownerAuthorityPublishTimeout)
	defer cancel()
	bundle, err := store.ExportFederatedHistory(subjectID, resolver, time.Now().UTC())
	if err != nil {
		return err
	}
	_, err = s.identityFederation.PublishOwnerAuthorityHistory(ctx, subjectID, bundle)
	return err
}

func (s *Server) ownerAuthorityDependencies(w http.ResponseWriter) (*authority.Store, authority.AuthorityIdentityResolver, bool) {
	if s.ownerAuthority == nil || s.globalIdentity == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "owner_authority_unavailable", "OwnerSubject authority is unavailable.")
		return nil, nil, false
	}
	_, initialized, err := s.globalIdentity.Snapshot()
	if err != nil || !initialized {
		writeCodedError(w, http.StatusServiceUnavailable, "global_identity_unavailable", "Current identity history is unavailable.")
		return nil, nil, false
	}
	resolver := authority.IdentityStateResolverFromLocalAndRegistry(s.globalIdentity, s.globalIdentityRegistry)
	if s.federatedIdentityResolver != nil {
		resolver = authority.IdentityStateResolverFromFederated(s.globalIdentity, s.federatedIdentityResolver)
	}
	return s.ownerAuthority, resolver, true
}

func ownerAuthorityView(snapshot authority.AuthoritySnapshot) ownerAuthoritySnapshotView {
	view := ownerAuthoritySnapshotView{Subject: snapshot.Subject, ReplicationPolicy: snapshot.ReplicationPolicy, ArchiveIDs: append([]string(nil), snapshot.ArchiveIDs...), RekeyRequiredSinceHash: snapshot.RekeyRequiredSinceHash, Sequence: snapshot.Sequence, HeadHash: snapshot.HeadHash, Frozen: snapshot.Frozen, ForkCount: snapshot.ForkCount}
	if view.ArchiveIDs == nil {
		view.ArchiveIDs = []string{}
	}
	for archiveID := range snapshot.RekeyRequiredArchives {
		view.RekeyRequiredArchives = append(view.RekeyRequiredArchives, archiveID)
	}
	sort.Strings(view.RekeyRequiredArchives)
	if view.RekeyRequiredArchives == nil {
		view.RekeyRequiredArchives = []string{}
	}
	hashes := make([]string, 0, len(snapshot.Grants))
	for hash := range snapshot.Grants {
		hashes = append(hashes, hash)
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		chain := snapshot.Grants[hash]
		if len(chain) == 0 {
			continue
		}
		grant := chain[len(chain)-1].Grant
		_, revoked := snapshot.RevokedHashes[hash]
		view.Grants = append(view.Grants, ownerAuthorityGrantView{
			GrantHash: hash, Issuer: grant.Issuer, Audience: grant.Audience,
			ArchiveID: grant.ArchiveID, SnapshotID: grant.SnapshotID,
			Actions: append([]string(nil), grant.Actions...), ExpiresAt: grant.ExpiresAt,
			Revoked: revoked, Depth: grant.DelegationDepth,
		})
	}
	if view.Grants == nil {
		view.Grants = []ownerAuthorityGrantView{}
	}
	return view
}

func writeOwnerAuthorityError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, authority.ErrAuthorityFork):
		writeCodedError(w, http.StatusConflict, "owner_authority_fork", "Conflicting valid OwnerSubject histories are frozen.")
	case errors.Is(err, authority.ErrStaleIdentity), errors.Is(err, authority.ErrAuthorityStale):
		writeCodedError(w, http.StatusConflict, "owner_authority_stale", "The identity or OwnerSubject head changed; refresh and retry.")
	case errors.Is(err, authority.ErrDenied), errors.Is(err, authority.ErrRevoked):
		writeCodedError(w, http.StatusForbidden, "permission_denied", "The signed OwnerSubject operation is not authorized.")
	case errors.Is(err, authority.ErrInvalid):
		writeCodedError(w, http.StatusUnprocessableEntity, "owner_authority_event_invalid", "The signed OwnerSubject event is invalid.")
	case errors.Is(err, authority.ErrAuthorityLimit):
		writeCodedError(w, http.StatusConflict, "owner_authority_limit", "The OwnerSubject history reached its safe size limit.")
	case errors.Is(err, authority.ErrArchiveClaimed):
		writeCodedError(w, http.StatusConflict, "archive_already_claimed", "This archive is already claimed by an OwnerSubject.")
	case errors.Is(err, authority.ErrArchiveUnclaimed):
		writeCodedError(w, http.StatusConflict, "archive_claim_required", "Claim this archive for the OwnerSubject before issuing capabilities.")
	case errors.Is(err, authority.ErrRekeyRequired):
		writeCodedError(w, http.StatusConflict, "archive_rekey_required", "This archive remains locked until the ownership-transfer key rotation is verified.")
	case errors.Is(err, authority.ErrAuthorityStore):
		writeCodedError(w, http.StatusServiceUnavailable, "owner_authority_unavailable", "OwnerSubject authority state could not be read safely.")
	default:
		writeCodedError(w, http.StatusServiceUnavailable, "owner_authority_unavailable", "OwnerSubject authority state could not be read safely.")
	}
}
