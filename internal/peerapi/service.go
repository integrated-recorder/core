// Package peerapi serves a bounded archive-data and replica-lease API over an
// already authenticated Node-Key WebRTC session. It exposes immutable
// snapshots, public ciphertext manifests, recipient-bound encrypted DEK
// envelopes, ciphertext ranges, and signed offer/acceptance/renewal transport;
// it never opens canonical plaintext or exports local filesystem paths.
package peerapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
	"github.com/integrated-recorder/core/internal/replication"
)

const (
	ResponseVersion          = "ir.peer-response/1"
	MaxPeerResponseBytes     = 16 << 20
	MaxOfferPageObjects      = 256
	offerPageVersion         = "ir.replica-offer-page/1"
	responseHeaderBytes      = 4 + 16 + 1 + 4 + 4 + 8 + sha256.Size
	responseKindSnapshot     = byte(1)
	responseKindCiphertext   = byte(2)
	responseKindError        = byte(3)
	responseKindManifest     = byte(4)
	responseKindKeyEnvelope  = byte(5)
	responseKindOfferPage    = byte(6)
	responseKindLeaseReceipt = byte(7)
	responseKindRenewal      = byte(8)
	responseKindTombstone    = byte(9)
	responseKindKeyEpochs    = byte(10)
	responseKindOfferInbox   = byte(11)
	responseKindSessionDEK   = byte(12)
	responseMagic            = "IRPR"
	offerInboxVersion        = "ir.replica-offer-inbox/1"
)

var (
	ErrInvalidRequest  = errors.New("peer request is invalid")
	ErrResponseInvalid = errors.New("peer response is invalid")
	ErrRemoteFailure   = errors.New("remote peer request failed")
)

type Catalog interface {
	Head(context.Context, string) (archivesnapshot.Snapshot, error)
	Snapshot(context.Context, string, string) (archivesnapshot.Snapshot, error)
}

type ReplicaSnapshotReader interface {
	LoadSnapshot(string, string) (archivesnapshot.Snapshot, error)
}

type CiphertextStore interface {
	LoadBundle(archivesnapshot.Snapshot, uint64) (replication.Bundle, error)
	OpenCiphertextRangeForPeer(context.Context, archivesnapshot.Snapshot, replication.Bundle, replication.Authorization, string, string, int64, int64) (io.ReadCloser, error)
}

type KeyEnvelopeStore interface {
	LoadKeyEnvelope(archivesnapshot.Snapshot, uint64, string) (cryptoarchive.KeyEnvelope, error)
}

type KeyEpochStore interface {
	KeyEpochs(archivesnapshot.Snapshot) ([]uint64, error)
}

// SessionDEKProvider may expose an archive key only for an already validated
// snapshot and a one-time browser recipient. Implementations must unwrap the
// DEK from local Node-Key protection and clear it after sealing.
type SessionDEKProvider interface {
	HasSessionDEK(archivesnapshot.Snapshot, uint64) bool
	SealSessionDEK(archivesnapshot.Snapshot, uint64, []byte, cryptoarchive.SessionDEKBinding) (cryptoarchive.SessionDEKEnvelope, error)
}

// OfferReader exposes only an owner-authorized offer awaiting delivery to one
// designated provider. The receiver still revalidates the signed offer in its
// durable lease Store before accepting it.
type OfferReader interface {
	OfferForProvider(context.Context, string, string) (lease.SignedOffer, string, error)
}

// OfferInboxReader returns a non-secret projection of unaccepted offers for
// the authenticated provider. The command's current capability scope is
// checked separately before this method is called.
type OfferInboxReader interface {
	PendingOffersForProvider(context.Context, string, string, string, string, int, int) ([]lease.PendingOffer, int, error)
}

// TombstoneRecorder applies an Owner-signed archive deletion decision to the
// receiver's replica lease state. Implementations must independently verify
// the delete capability, signer history, signature, epoch, and fork state.
// Recording a tombstone never claims that remote ciphertext was erased.
type TombstoneRecorder interface {
	AddTombstone(context.Context, lease.SignedTombstone) (lease.TombstoneRecord, error)
}

// CommandAuthorizer must re-resolve the current durable OwnerSubject chain on
// each call. The same verifier guards the lower-level ciphertext read.
type CommandAuthorizer interface {
	peerprotocol.CommandAuthorizer
	lease.Authorizer
}

type Session interface {
	Remote() authority.Principal
	SessionID() string
	SendCommand(context.Context, peerprotocol.Command) error
	Send(context.Context, []byte) error
	Receive(context.Context) ([]byte, error)
	ReceiveAuthorizedCommand(context.Context, peerprotocol.IdentityResolver, peerprotocol.CommandAuthorizer, peertransport.PeerCommandReplayConsumer, string, string) (peerprotocol.Command, error)
}

type Service struct {
	Catalog          Catalog
	ReplicaSnapshots ReplicaSnapshotReader
	Ciphertexts      CiphertextStore
	SessionKeys      SessionDEKProvider
	Leases           OfferReader
	Authorizer       CommandAuthorizer
	Signer           PeerCommandSigner
	IdentityID       string
	NodeID           string
	Resolve          peerprotocol.IdentityResolver
	Replay           peertransport.PeerCommandReplayConsumer
	Now              func() time.Time
}

func (s *Service) snapshot(ctx context.Context, archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	if s == nil {
		return archivesnapshot.Snapshot{}, archivecatalog.ErrNotFound
	}
	if s.Catalog != nil {
		snapshot, err := s.Catalog.Snapshot(ctx, archiveID, snapshotID)
		if err == nil || !errors.Is(err, archivecatalog.ErrNotFound) {
			return snapshot, err
		}
	}
	if s.ReplicaSnapshots == nil {
		return archivesnapshot.Snapshot{}, archivecatalog.ErrNotFound
	}
	return s.ReplicaSnapshots.LoadSnapshot(archiveID, snapshotID)
}

// Serve processes authorized peer commands until the session closes or the
// caller cancels its lifecycle context. Each command is still independently
// signature-, freshness-, replay-, and capability-checked by ServeOne.
// Session owners should run one Serve loop per authenticated peer session and
// close the session when the context is cancelled.
func (s *Service) Serve(ctx context.Context, session Session) error {
	if ctx == nil || session == nil {
		return ErrInvalidRequest
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ServeOne(ctx, session); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

type requestPayload struct {
	Operation          string                  `json:"operation"`
	ObjectID           string                  `json:"object_id,omitempty"`
	LeaseID            string                  `json:"lease_id,omitempty"`
	KeyEpoch           string                  `json:"key_epoch,omitempty"`
	RecipientKeyID     string                  `json:"recipient_key_id,omitempty"`
	RecipientPublicKey string                  `json:"recipient_public_key,omitempty"`
	Offset             string                  `json:"offset,omitempty"`
	Length             string                  `json:"length,omitempty"`
	Limit              string                  `json:"limit,omitempty"`
	Acceptance         *lease.SignedAcceptance `json:"acceptance,omitempty"`
	Checkpoint         *lease.SignedCheckpoint `json:"checkpoint,omitempty"`
	Renewal            *lease.SignedRenewal    `json:"renewal,omitempty"`
	Tombstone          *lease.SignedTombstone  `json:"tombstone,omitempty"`
}

type replicaOfferPage struct {
	Version   string                  `json:"page_version"`
	LeaseID   string                  `json:"lease_id"`
	OfferHash string                  `json:"offer_hash"`
	Offset    string                  `json:"offset"`
	Total     string                  `json:"total_objects"`
	Signed    lease.SignedOffer       `json:"signed_offer"`
	Objects   []lease.EncryptedObject `json:"objects"`
}

type replicaOfferInboxPage struct {
	Version       string               `json:"page_version"`
	OwnerSubject  string               `json:"owner_subject_id"`
	ArchiveID     string               `json:"archive_id"`
	SnapshotScope string               `json:"snapshot_scope,omitempty"`
	Offset        string               `json:"offset"`
	Total         string               `json:"total_offers"`
	Offers        []lease.PendingOffer `json:"offers"`
}

type errorPayload struct {
	Version   string `json:"response_version"`
	RequestID string `json:"request_id"`
	Code      string `json:"error_code"`
}

type keyEpochsPayload struct {
	Version    string   `json:"response_version"`
	ArchiveID  string   `json:"archive_id"`
	SnapshotID string   `json:"snapshot_id"`
	KeyEpochs  []string `json:"key_epochs"`
}

type sessionKeyEpochsPayload struct {
	Version    string   `json:"response_version"`
	ArchiveID  string   `json:"archive_id"`
	SnapshotID string   `json:"snapshot_id"`
	KeyEpochs  []string `json:"key_epochs"`
}

// ServeOne receives one session-bound, capability-authorized request, checks
// authority again immediately before reading the archive, and sends one
// bounded response. The caller owns the session lifecycle and should invoke
// this in a bounded per-peer request loop.
func (s *Service) ServeOne(ctx context.Context, session Session) error {
	if s == nil || ctx == nil || session == nil || s.Catalog == nil && s.ReplicaSnapshots == nil || s.Authorizer == nil || s.Resolve == nil || s.Replay == nil || s.IdentityID == "" || s.NodeID == "" {
		return ErrInvalidRequest
	}
	command, err := session.ReceiveAuthorizedCommand(ctx, s.Resolve, s.Authorizer, s.Replay, s.IdentityID, s.NodeID)
	if err != nil {
		return err
	}
	if command.SessionID != session.SessionID() {
		return peerprotocol.ErrCommandDenied
	}
	kind, body, err := s.executeOnSession(ctx, command, session.Remote(), session)
	if err != nil {
		kind = responseKindError
		body, _ = json.Marshal(errorPayload{Version: ResponseVersion, RequestID: command.RequestID, Code: publicErrorCode(err)})
	}
	frames, err := EncodeResponse(command.RequestID, kind, body)
	if err != nil {
		return err
	}
	for _, frame := range frames {
		if err := session.Send(ctx, frame); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) execute(ctx context.Context, command peerprotocol.Command, actor authority.Principal) (byte, []byte, error) {
	return s.executeOnSession(ctx, command, actor, nil)
}

func (s *Service) executeOnSession(ctx context.Context, command peerprotocol.Command, actor authority.Principal, session Session) (byte, []byte, error) {
	if actor.Kind != authority.PrincipalNode || actor.ID == "" || actor.ID != command.SourceNodeID ||
		command.SessionID == "" || command.TargetIdentityID != s.IdentityID || command.TargetNodeID != s.NodeID {
		return 0, nil, peerprotocol.ErrCommandDenied
	}
	request, err := decodeRequest(command.Payload)
	if err != nil {
		return 0, nil, err
	}
	if !operationAllowed(request.Operation, command) {
		return 0, nil, ErrInvalidRequest
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	if err := s.Authorizer.Authorize(ctx, command.OwnerSubjectID, command.ArchiveID, command.SnapshotID, command.Action, actor, command.GrantChain, now); err != nil {
		return 0, nil, peerprotocol.ErrCommandDenied
	}
	switch request.Operation {
	case "snapshot.head":
		if s.Catalog == nil {
			return 0, nil, archivecatalog.ErrNotFound
		}
		snapshot, err := s.Catalog.Head(ctx, command.ArchiveID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		body, err := archivesnapshot.Marshal(snapshot)
		return responseKindSnapshot, body, err
	case "snapshot.get":
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		body, err := archivesnapshot.Marshal(snapshot)
		return responseKindSnapshot, body, err
	case "replica.manifest":
		if s.Ciphertexts == nil {
			return 0, nil, errors.New("ciphertext service is unavailable")
		}
		keyEpoch, err := canonicalUint(request.KeyEpoch)
		if err != nil || keyEpoch == 0 {
			return 0, nil, ErrInvalidRequest
		}
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		bundle, err := s.Ciphertexts.LoadBundle(snapshot, keyEpoch)
		if err != nil {
			return 0, nil, err
		}
		manifest, err := replication.PublicReplicaManifest(snapshot, bundle)
		if err != nil {
			return 0, nil, err
		}
		body, err := json.Marshal(manifest)
		return responseKindManifest, body, err
	case "replica.key-epochs":
		store, hasEpochs := s.Ciphertexts.(KeyEpochStore)
		envelopes, hasEnvelopes := s.Ciphertexts.(KeyEnvelopeStore)
		if s.Ciphertexts == nil || !hasEpochs || !hasEnvelopes || !validRecipientKeyID(request.RecipientKeyID) || len(command.GrantChain) == 0 {
			return 0, nil, ErrInvalidRequest
		}
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		issuer := command.GrantChain[0].Grant.Issuer
		if issuer.Kind != authority.PrincipalIdentity || issuer.ID == "" {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		state, err := s.Resolve(issuer.ID, "", now)
		if err != nil || state.IdentityID != issuer.ID || state.Frozen || state.VerifiedAt.IsZero() || state.FreshUntil.IsZero() || now.Before(state.VerifiedAt) || now.After(state.FreshUntil) {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		signerPublic := state.CurrentKeys[authority.PrincipalIdentity+":"+issuer.ID]
		signerKeyID, err := identityprotocol.KeyID(signerPublic)
		if err != nil {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		epochs, err := store.KeyEpochs(snapshot)
		if err != nil {
			return 0, nil, err
		}
		if len(epochs) > replication.MaxSnapshotKeyEpochs {
			return 0, nil, ErrResponseInvalid
		}
		response := keyEpochsPayload{Version: "ir.peer-key-epochs/1", ArchiveID: command.ArchiveID, SnapshotID: command.SnapshotID, KeyEpochs: []string{}}
		for _, epoch := range epochs {
			if epoch == 0 {
				continue
			}
			bundle, loadErr := s.Ciphertexts.LoadBundle(snapshot, epoch)
			if loadErr != nil {
				continue
			}
			if _, manifestErr := replication.PublicReplicaManifest(snapshot, bundle); manifestErr != nil {
				continue
			}
			envelope, loadErr := envelopes.LoadKeyEnvelope(snapshot, epoch, request.RecipientKeyID)
			if loadErr != nil || envelope.ArchiveID != command.ArchiveID || envelope.KeyEpoch != strconv.FormatUint(epoch, 10) ||
				envelope.RecipientKeyID != request.RecipientKeyID || envelope.SignerKeyID != signerKeyID || cryptoarchive.VerifyKeyEnvelopeSignature(envelope, signerPublic) != nil {
				continue
			}
			response.KeyEpochs = append(response.KeyEpochs, strconv.FormatUint(epoch, 10))
		}
		body, err := json.Marshal(response)
		return responseKindKeyEpochs, body, err
	case "replica.key-envelope":
		store, ok := s.Ciphertexts.(KeyEnvelopeStore)
		if !ok {
			return 0, nil, errors.New("key envelope service is unavailable")
		}
		keyEpoch, err := canonicalUint(request.KeyEpoch)
		if err != nil || keyEpoch == 0 || !validRecipientKeyID(request.RecipientKeyID) || len(command.GrantChain) == 0 {
			return 0, nil, ErrInvalidRequest
		}
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		envelope, err := store.LoadKeyEnvelope(snapshot, keyEpoch, request.RecipientKeyID)
		if err != nil {
			return 0, nil, err
		}
		if envelope.ArchiveID != command.ArchiveID || envelope.KeyEpoch != strconv.FormatUint(keyEpoch, 10) || envelope.RecipientKeyID != request.RecipientKeyID {
			return 0, nil, cryptoarchive.ErrEnvelopeInvalid
		}
		issuer := command.GrantChain[0].Grant.Issuer
		if issuer.Kind != authority.PrincipalIdentity || issuer.ID == "" {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		state, err := s.Resolve(issuer.ID, "", now)
		if err != nil || state.IdentityID != issuer.ID || state.Frozen || state.VerifiedAt.IsZero() || state.FreshUntil.IsZero() || now.Before(state.VerifiedAt) || now.After(state.FreshUntil) {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		signerPublic := state.CurrentKeys[authority.PrincipalIdentity+":"+issuer.ID]
		signerKeyID, err := identityprotocol.KeyID(signerPublic)
		if err != nil || signerKeyID != envelope.SignerKeyID || cryptoarchive.VerifyKeyEnvelopeSignature(envelope, signerPublic) != nil {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		body, err := cryptoarchive.MarshalKeyEnvelope(envelope)
		return responseKindKeyEnvelope, body, err
	case "replica.session-key-epochs":
		store, hasEpochs := s.Ciphertexts.(KeyEpochStore)
		if !hasEpochs || s.SessionKeys == nil || len(command.GrantChain) == 0 || command.Action != "archive.view" {
			return 0, nil, ErrInvalidRequest
		}
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		if _, err := s.currentCapabilityIssuer(command, now); err != nil {
			return 0, nil, err
		}
		epochs, err := store.KeyEpochs(snapshot)
		if err != nil {
			return 0, nil, err
		}
		if len(epochs) > replication.MaxSnapshotKeyEpochs {
			return 0, nil, ErrResponseInvalid
		}
		response := sessionKeyEpochsPayload{Version: "ir.peer-session-key-epochs/1", ArchiveID: command.ArchiveID, SnapshotID: command.SnapshotID, KeyEpochs: []string{}}
		var previous uint64
		for index, epoch := range epochs {
			if epoch == 0 || index > 0 && epoch >= previous {
				return 0, nil, ErrResponseInvalid
			}
			previous = epoch
			if s.SessionKeys.HasSessionDEK(snapshot, epoch) {
				response.KeyEpochs = append(response.KeyEpochs, strconv.FormatUint(epoch, 10))
			}
		}
		body, err := json.Marshal(response)
		return responseKindKeyEpochs, body, err
	case "replica.session-key-envelope":
		if s.SessionKeys == nil || command.Action != "archive.view" || len(command.GrantChain) == 0 {
			return 0, nil, ErrInvalidRequest
		}
		keyEpoch, err := canonicalUint(request.KeyEpoch)
		if err != nil || keyEpoch == 0 {
			return 0, nil, ErrInvalidRequest
		}
		recipientPublicKey, err := base64.RawURLEncoding.DecodeString(request.RecipientPublicKey)
		if err != nil || len(recipientPublicKey) != 32 || base64.RawURLEncoding.EncodeToString(recipientPublicKey) != request.RecipientPublicKey {
			return 0, nil, ErrInvalidRequest
		}
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		if _, err := s.currentCapabilityIssuer(command, now); err != nil {
			return 0, nil, err
		}
		grantHash, err := authority.GrantHash(command.GrantChain[len(command.GrantChain)-1].Grant)
		if err != nil {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		binding := cryptoarchive.SessionDEKBinding{
			ArchiveID: command.ArchiveID, SnapshotID: command.SnapshotID, KeyEpoch: keyEpoch,
			OwnerSubjectID: command.OwnerSubjectID, GrantHash: grantHash, SupplierNodeID: s.NodeID,
		}
		envelope, err := s.SessionKeys.SealSessionDEK(snapshot, keyEpoch, recipientPublicKey, binding)
		if err != nil || envelope.RecipientKeyID != cryptoarchive.RecipientKeyID(recipientPublicKey) ||
			envelope.SupplierNodeID != s.NodeID || envelope.OwnerSubjectID != command.OwnerSubjectID || envelope.GrantHash != grantHash {
			return 0, nil, ErrResponseInvalid
		}
		body, err := cryptoarchive.MarshalSessionDEKEnvelope(envelope)
		return responseKindSessionDEK, body, err
	case "replica.offer.page":
		if s.Leases == nil {
			return 0, nil, errors.New("lease offer service is unavailable")
		}
		if !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		offset, err := canonicalUint(request.Offset)
		if err != nil || offset > lease.MaxObjects {
			return 0, nil, ErrInvalidRequest
		}
		limit, err := canonicalUint(request.Limit)
		if err != nil || limit == 0 || limit > MaxOfferPageObjects {
			return 0, nil, ErrInvalidRequest
		}
		signed, offerHash, err := s.Leases.OfferForProvider(ctx, request.LeaseID, actor.ID)
		if err != nil {
			return 0, nil, err
		}
		offer := signed.Offer
		computedHash, err := lease.OfferHash(offer)
		if err != nil || computedHash != offerHash {
			return 0, nil, lease.ErrInvalid
		}
		if offer.ArchiveID != command.ArchiveID || offer.SnapshotID != command.SnapshotID || offer.OwnerSubjectID != command.OwnerSubjectID ||
			offer.ProviderNodeID != actor.ID || len(offer.EncryptedObjectSet) == 0 || len(offer.EncryptedObjectSet) > lease.MaxObjects {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		if offset >= uint64(len(offer.EncryptedObjectSet)) {
			return 0, nil, ErrInvalidRequest
		}
		end := offset + limit
		if end > uint64(len(offer.EncryptedObjectSet)) {
			end = uint64(len(offer.EncryptedObjectSet))
		}
		pageSigned := signed
		pageSigned.Offer.EncryptedObjectSet = nil
		page := replicaOfferPage{
			Version: offerPageVersion, LeaseID: request.LeaseID, OfferHash: offerHash,
			Offset: strconv.FormatUint(offset, 10), Total: strconv.FormatUint(uint64(len(offer.EncryptedObjectSet)), 10),
			Signed: pageSigned, Objects: append([]lease.EncryptedObject(nil), offer.EncryptedObjectSet[offset:end]...),
		}
		body, err := json.Marshal(page)
		return responseKindOfferPage, body, err
	case "replica.offers":
		store, ok := s.Leases.(OfferInboxReader)
		if !ok || !validPeerInboxPage(request.Offset, request.Limit) {
			return 0, nil, ErrInvalidRequest
		}
		offset, _ := strconv.Atoi(request.Offset)
		limit, _ := strconv.Atoi(request.Limit)
		offers, total, err := store.PendingOffersForProvider(ctx, actor.ID, command.OwnerSubjectID, command.ArchiveID, command.SnapshotID, offset, limit)
		if err != nil {
			return 0, nil, err
		}
		if total < 0 || total > lease.MaxPendingOffers || offset > total || len(offers) > limit || offset+len(offers) > total {
			return 0, nil, ErrResponseInvalid
		}
		for _, offer := range offers {
			if offer.LeaseID == "" || offer.ArchiveID != command.ArchiveID || offer.OwnerSubject != command.OwnerSubjectID ||
				offer.SnapshotID == "" || command.SnapshotID != "" && offer.SnapshotID != command.SnapshotID ||
				offer.OfferHash == "" || offer.ObjectCount < 1 {
				return 0, nil, ErrResponseInvalid
			}
		}
		page := replicaOfferInboxPage{
			Version: offerInboxVersion, OwnerSubject: command.OwnerSubjectID, ArchiveID: command.ArchiveID,
			SnapshotScope: command.SnapshotID, Offset: strconv.Itoa(offset), Total: strconv.Itoa(total), Offers: offers,
		}
		body, err := json.Marshal(page)
		return responseKindOfferInbox, body, err
	case "replica.acceptance":
		store, ok := s.Leases.(interface {
			Get(context.Context, string) (lease.Lease, error)
			Accept(context.Context, lease.SignedAcceptance) (lease.Lease, error)
		})
		if !ok || request.Acceptance == nil || !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		current, err := store.Get(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		offer := current.Offer.Offer
		acceptance := request.Acceptance.Acceptance
		if offer.LeaseID != request.LeaseID || offer.ArchiveID != command.ArchiveID || offer.SnapshotID != command.SnapshotID ||
			offer.OwnerSubjectID != command.OwnerSubjectID || offer.ProviderNodeID != actor.ID ||
			(current.Status != lease.StatusOffered && !acceptanceMayBeRetried(current.Status)) ||
			acceptance.LeaseID != request.LeaseID || acceptance.OfferHash != current.OfferHash || acceptance.ProviderNodeID != actor.ID {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		accepted, err := store.Accept(ctx, *request.Acceptance)
		if err != nil {
			return 0, nil, err
		}
		body, err := json.Marshal(leaseReceiptPayload{
			Version: "ir.replica-lease-receipt/1", LeaseID: request.LeaseID,
			OfferHash: accepted.OfferHash, Status: string(lease.StatusAccepted),
		})
		return responseKindLeaseReceipt, body, err
	case "replica.tombstone":
		recorder, ok := s.Leases.(TombstoneRecorder)
		if !ok || request.Tombstone == nil {
			return 0, nil, ErrInvalidRequest
		}
		tombstone := request.Tombstone.Tombstone
		if tombstone.ArchiveID != command.ArchiveID || tombstone.OwnerSubjectID != command.OwnerSubjectID {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		stored, err := recorder.AddTombstone(ctx, *request.Tombstone)
		if err != nil {
			return 0, nil, err
		}
		if stored.Hash == "" || stored.Signed.Tombstone.ArchiveID != tombstone.ArchiveID ||
			stored.Signed.Tombstone.OwnerSubjectID != tombstone.OwnerSubjectID ||
			stored.Signed.Tombstone.DeletionEpoch != tombstone.DeletionEpoch {
			return 0, nil, lease.ErrInvalid
		}
		var erasure *replication.ReplicaErasure
		var deletionReports []lease.SignedDeletionReport
		if s.Ciphertexts != nil {
			eraser, ok := s.Ciphertexts.(interface {
				EraseTombstonedArchive(context.Context, string, string, replication.TombstoneLeaseState) (replication.ReplicaErasure, error)
			})
			state, stateOK := s.Leases.(replication.TombstoneLeaseState)
			if !ok || !stateOK {
				return 0, nil, lease.ErrInvalid
			}
			result, err := eraser.EraseTombstonedArchive(ctx, tombstone.ArchiveID, stored.Hash, state)
			if err != nil {
				return 0, nil, err
			}
			erasure = &result
			leaseState, _, snapshotErr := state.Snapshot(ctx)
			if snapshotErr != nil {
				return 0, nil, lease.ErrInvalid
			}
			now := time.Now().UTC()
			if s.Now != nil {
				now = s.Now().UTC()
			}
			for _, item := range leaseState {
				if item.Offer.Offer.ArchiveID != tombstone.ArchiveID {
					continue
				}
				if item.DeletionReport != nil && item.DeletionReport.Report.TombstoneHash == stored.Hash {
					deletionReports = append(deletionReports, *item.DeletionReport)
					continue
				}
				if item.Status != lease.StatusTerminating && item.Status != lease.StatusExpired && item.Status != lease.StatusUnconfirmed &&
					!(item.Status == lease.StatusFailed && item.FailureCode != "tombstoned_before_acceptance") {
					continue
				}
				reporter, reportStoreOK := s.Leases.(interface {
					ReportDeletion(context.Context, lease.SignedDeletionReport) (lease.Lease, error)
				})
				signer, signerOK := s.Signer.(interface {
					SignReplicaDeletionReport(lease.DeletionReport) (lease.SignedDeletionReport, error)
				})
				if !reportStoreOK || !signerOK {
					return 0, nil, lease.ErrInvalid
				}
				report, err := signer.SignReplicaDeletionReport(lease.DeletionReport{
					Version: lease.ProtocolVersion, LeaseID: item.Offer.Offer.LeaseID,
					OfferHash: item.OfferHash, Status: string(lease.StatusDeleted),
					ReportedAt: now.Truncate(time.Second).Format(time.RFC3339), TombstoneHash: stored.Hash,
				})
				if err != nil {
					return 0, nil, err
				}
				if _, err := reporter.ReportDeletion(ctx, report); err != nil {
					return 0, nil, err
				}
				deletionReports = append(deletionReports, report)
			}
		}
		body, err := json.Marshal(tombstoneReceiptPayload{
			Version: "ir.archive-tombstone-receipt/1", ArchiveID: tombstone.ArchiveID,
			OwnerSubjectID: tombstone.OwnerSubjectID, DeletionEpoch: tombstone.DeletionEpoch,
			TombstoneHash: stored.Hash, Erasure: erasure, DeletionReports: deletionReports,
		})
		return responseKindTombstone, body, err
	case "replica.transfer.begin":
		store, ok := s.Leases.(interface {
			Get(context.Context, string) (lease.Lease, error)
			BeginTransfer(context.Context, string) (lease.Lease, error)
		})
		if !ok || !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		current, err := store.Get(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		offer := current.Offer.Offer
		if offer.LeaseID != request.LeaseID || offer.ArchiveID != command.ArchiveID ||
			offer.SnapshotID != command.SnapshotID || offer.OwnerSubjectID != command.OwnerSubjectID ||
			offer.ProviderNodeID != actor.ID || current.Acceptance.Acceptance.ProviderNodeID != actor.ID ||
			current.Acceptance.Signature == "" {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		transferring, err := store.BeginTransfer(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		body, err := json.Marshal(leaseReceiptPayload{
			Version: "ir.replica-lease-receipt/1", LeaseID: request.LeaseID,
			OfferHash: transferring.OfferHash, Status: string(transferring.Status),
		})
		return responseKindLeaseReceipt, body, err
	case "replica.checkpoint":
		store, ok := s.Leases.(interface {
			Get(context.Context, string) (lease.Lease, error)
			Checkpoint(context.Context, lease.SignedCheckpoint) (lease.Lease, error)
		})
		if !ok || request.Checkpoint == nil || !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		current, err := store.Get(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		offer := current.Offer.Offer
		checkpoint := request.Checkpoint.Checkpoint
		if offer.LeaseID != request.LeaseID || offer.ArchiveID != command.ArchiveID ||
			offer.SnapshotID != command.SnapshotID || offer.OwnerSubjectID != command.OwnerSubjectID ||
			offer.ProviderNodeID != actor.ID || checkpoint.LeaseID != request.LeaseID ||
			checkpoint.OfferHash != current.OfferHash || checkpoint.ProviderNodeID != actor.ID {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		updated, err := store.Checkpoint(ctx, *request.Checkpoint)
		if err != nil {
			return 0, nil, err
		}
		body, err := json.Marshal(leaseReceiptPayload{
			Version: "ir.replica-lease-receipt/1", LeaseID: request.LeaseID,
			OfferHash: updated.OfferHash, Status: string(updated.Status),
		})
		return responseKindLeaseReceipt, body, err
	case "replica.renewal.request":
		store, ok := s.Leases.(interface {
			Get(context.Context, string) (lease.Lease, error)
			RenewalForProvider(context.Context, string, string) (lease.SignedRenewalRequest, string, error)
		})
		if !ok || !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		current, err := store.Get(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		offer := current.Offer.Offer
		if offer.LeaseID != request.LeaseID || offer.ArchiveID != command.ArchiveID || offer.SnapshotID != command.SnapshotID ||
			offer.OwnerSubjectID != command.OwnerSubjectID || offer.ProviderNodeID != actor.ID {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		renewalRequest, offerHash, err := store.RenewalForProvider(ctx, request.LeaseID, actor.ID)
		if err != nil {
			return 0, nil, err
		}
		if renewalRequest.Renewal.LeaseID != request.LeaseID || renewalRequest.Renewal.OfferHash != offerHash || offerHash != current.OfferHash ||
			renewalRequest.Renewal.OwnerSubjectID != offer.OwnerSubjectID {
			return 0, nil, lease.ErrInvalid
		}
		body, err := json.Marshal(replicaRenewalPayload{
			Version: "ir.replica-renewal-request/1", LeaseID: request.LeaseID,
			OfferHash: offerHash, Request: renewalRequest,
		})
		return responseKindRenewal, body, err
	case "replica.renewal.receipt":
		store, ok := s.Leases.(interface {
			Get(context.Context, string) (lease.Lease, error)
			CompleteRenewal(context.Context, lease.SignedRenewal) (lease.Lease, error)
		})
		if !ok || request.Renewal == nil || !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		current, err := store.Get(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		offer, renewal := current.Offer.Offer, request.Renewal.Renewal
		if offer.LeaseID != request.LeaseID || offer.ArchiveID != command.ArchiveID || offer.SnapshotID != command.SnapshotID ||
			offer.OwnerSubjectID != command.OwnerSubjectID || offer.ProviderNodeID != actor.ID ||
			!renewalMayBeCompleted(current.Status) || renewal.LeaseID != request.LeaseID ||
			renewal.OfferHash != current.OfferHash || renewal.OwnerSubjectID != offer.OwnerSubjectID {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		renewed, err := store.CompleteRenewal(ctx, *request.Renewal)
		if err != nil {
			return 0, nil, err
		}
		body, err := json.Marshal(leaseReceiptPayload{
			Version: "ir.replica-lease-receipt/1", LeaseID: request.LeaseID,
			OfferHash: renewed.OfferHash, Status: string(lease.StatusActive),
		})
		return responseKindLeaseReceipt, body, err
	case "replica.verify":
		store, ok := s.Leases.(interface {
			Get(context.Context, string) (lease.Lease, error)
			VerifyReplica(context.Context, string, lease.ObjectReader) (lease.Lease, error)
		})
		if !ok || session == nil || s.Signer == nil || !validPeerLeaseID(request.LeaseID) {
			return 0, nil, ErrInvalidRequest
		}
		current, err := store.Get(ctx, request.LeaseID)
		if err != nil {
			return 0, nil, err
		}
		offer := current.Offer.Offer
		if offer.LeaseID != request.LeaseID || offer.ArchiveID != command.ArchiveID ||
			offer.SnapshotID != command.SnapshotID || offer.OwnerSubjectID != command.OwnerSubjectID ||
			offer.ProviderNodeID != actor.ID {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		if current.Status == lease.StatusActive {
			body, err := json.Marshal(leaseReceiptPayload{
				Version: "ir.replica-lease-receipt/1", LeaseID: request.LeaseID,
				OfferHash: current.OfferHash, Status: string(lease.StatusActive),
			})
			return responseKindLeaseReceipt, body, err
		}
		if current.Status != lease.StatusVerifying || len(current.Offer.GrantChain) == 0 {
			return 0, nil, lease.ErrTransition
		}
		// The provider-facing command proves the connected peer is authorized.
		// Readback uses the original source-audience grant from the immutable
		// offer and sends each bounded ciphertext range on this same session.
		identityState, err := s.Resolve(s.IdentityID, "", now)
		if err != nil || identityState.IdentityID != s.IdentityID || identityState.Frozen || identityState.HeadHash == "" {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		readback, err := NewScopedClient(session, MaxPeerResponseBytes, RequestScope{
			IdentityID: s.IdentityID, IdentityHead: identityState.HeadHash, NodeID: s.NodeID,
			TargetIdentityID: command.SourceIdentityID, TargetNodeID: actor.ID,
			OwnerSubjectID: offer.OwnerSubjectID, ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID,
			Action: "archive.replicate", GrantChain: current.Offer.GrantChain, Resolve: s.Resolve, Signer: s.Signer,
			Now: s.Now,
		})
		if err != nil {
			return 0, nil, peerprotocol.ErrCommandDenied
		}
		verified, err := store.VerifyReplica(ctx, request.LeaseID, replication.PeerLeaseObjectReader{
			Lease: current, Source: readback,
		})
		if err != nil {
			return 0, nil, err
		}
		body, err := json.Marshal(leaseReceiptPayload{
			Version: "ir.replica-lease-receipt/1", LeaseID: request.LeaseID,
			OfferHash: verified.OfferHash, Status: string(verified.Status),
		})
		return responseKindLeaseReceipt, body, err
	case "ciphertext.range":
		if s.Ciphertexts == nil {
			return 0, nil, errors.New("ciphertext service is unavailable")
		}
		keyEpoch, err := canonicalUint(request.KeyEpoch)
		if err != nil || keyEpoch == 0 {
			return 0, nil, ErrInvalidRequest
		}
		offset, err := canonicalUint(request.Offset)
		if err != nil || offset > uint64(^uint64(0)>>1) {
			return 0, nil, ErrInvalidRequest
		}
		length, err := canonicalUint(request.Length)
		if err != nil || length == 0 || length > uint64(replication.MaxCiphertextChunkBytes) {
			return 0, nil, ErrInvalidRequest
		}
		snapshot, err := s.snapshot(ctx, command.ArchiveID, command.SnapshotID)
		if err != nil {
			return 0, nil, err
		}
		if archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != command.ArchiveID || snapshot.SnapshotID != command.SnapshotID {
			return 0, nil, archivecatalog.ErrInvalid
		}
		bundle, err := s.Ciphertexts.LoadBundle(snapshot, keyEpoch)
		if err != nil {
			return 0, nil, err
		}
		auth := replication.Authorization{SubjectID: command.OwnerSubjectID, Actor: actor, Chain: command.GrantChain, Verifier: s.Authorizer}
		reader, err := s.Ciphertexts.OpenCiphertextRangeForPeer(ctx, snapshot, bundle, auth, command.Action, request.ObjectID, int64(offset), int64(length))
		if err != nil {
			return 0, nil, err
		}
		defer reader.Close()
		body, err := io.ReadAll(io.LimitReader(reader, int64(length)+1))
		if err != nil || uint64(len(body)) != length {
			return 0, nil, replication.ErrObjectUnavailable
		}
		return responseKindCiphertext, body, nil
	default:
		return 0, nil, ErrInvalidRequest
	}
}

func acceptanceMayBeRetried(status lease.Status) bool {
	switch status {
	case lease.StatusAccepted, lease.StatusTransferring, lease.StatusVerifying, lease.StatusActive, lease.StatusRenewing:
		return true
	default:
		return false
	}
}

func renewalMayBeCompleted(status lease.Status) bool {
	return status == lease.StatusRenewing || status == lease.StatusActive
}

func (s *Service) currentCapabilityIssuer(command peerprotocol.Command, now time.Time) (authority.IdentityState, error) {
	if s == nil || s.Resolve == nil || len(command.GrantChain) == 0 || now.IsZero() {
		return authority.IdentityState{}, peerprotocol.ErrCommandDenied
	}
	issuer := command.GrantChain[0].Grant.Issuer
	if issuer.Kind != authority.PrincipalIdentity || issuer.ID == "" {
		return authority.IdentityState{}, peerprotocol.ErrCommandDenied
	}
	state, err := s.Resolve(issuer.ID, "", now)
	if err != nil || state.IdentityID != issuer.ID || state.Frozen || state.VerifiedAt.IsZero() ||
		state.FreshUntil.IsZero() || now.Before(state.VerifiedAt) || now.After(state.FreshUntil) {
		return authority.IdentityState{}, peerprotocol.ErrCommandDenied
	}
	return state, nil
}

func operationAllowed(operation string, command peerprotocol.Command) bool {
	switch operation {
	case "snapshot.head":
		return command.Action == "archive.view" && command.SnapshotID == ""
	case "snapshot.get":
		return (command.Action == "archive.view" || command.Action == "archive.replicate") && command.SnapshotID != ""
	case "replica.manifest":
		return (command.Action == "archive.view" || command.Action == "archive.replicate") && command.SnapshotID != ""
	case "replica.key-epochs":
		return (command.Action == "archive.view" || command.Action == "archive.replicate") && command.SnapshotID != ""
	case "replica.key-envelope":
		return (command.Action == "archive.view" || command.Action == "archive.replicate") && command.SnapshotID != ""
	case "replica.session-key-epochs", "replica.session-key-envelope":
		return command.Action == "archive.view" && command.SnapshotID != ""
	case "replica.offer.page":
		return command.Action == "archive.replicate" && command.SnapshotID != ""
	case "replica.offers":
		return command.Action == "archive.replicate"
	case "replica.acceptance":
		return command.Action == "archive.replicate" && command.SnapshotID != ""
	case "replica.transfer.begin", "replica.checkpoint":
		return command.Action == "archive.replicate" && command.SnapshotID != ""
	case "replica.renewal.request", "replica.renewal.receipt":
		return command.Action == "archive.replicate" && command.SnapshotID != ""
	case "replica.tombstone":
		// A deletion decision fences replication for the whole archive, so it
		// uses an archive-wide capability and deliberately carries no snapshot.
		return command.Action == "archive.replicate" && command.SnapshotID == ""
	case "replica.verify":
		return command.Action == "archive.replicate" && command.SnapshotID != ""
	case "ciphertext.range":
		return command.SnapshotID != "" && (command.Action == "archive.view" || command.Action == "archive.export" || command.Action == "archive.replicate")
	default:
		return false
	}
}

func validRecipientKeyID(value string) bool {
	if len(value) != 46 || !strings.HasPrefix(value, "rk_") {
		return false
	}
	for _, char := range value[3:] {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func validPeerLeaseID(value string) bool {
	if len(value) != 25 || !strings.HasPrefix(value, "rl_") {
		return false
	}
	for _, char := range value[3:] {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func validPeerSnapshotID(value string) bool {
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

func validPeerSHA256(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func validPeerInboxPage(offset, limit string) bool {
	parsedOffset, offsetErr := canonicalUint(offset)
	parsedLimit, limitErr := canonicalUint(limit)
	return offsetErr == nil && parsedOffset <= lease.MaxPendingOffers && limitErr == nil && parsedLimit > 0 && parsedLimit <= lease.MaxPendingOfferPage
}

func validLeaseObjectRef(object lease.EncryptedObject) bool {
	if len(object.ObjectID) < 19 || len(object.ObjectID) > 67 || !strings.HasPrefix(object.ObjectID, "ob_") ||
		len(object.CiphertextSHA256) != 71 || !strings.HasPrefix(object.CiphertextSHA256, "sha256:") ||
		len(object.CiphertextSize) == 0 || len(object.CiphertextSize) > 20 {
		return false
	}
	for _, char := range object.ObjectID[3:] {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	digest, err := hex.DecodeString(object.CiphertextSHA256[7:])
	if err != nil || len(digest) != sha256.Size {
		return false
	}
	size, err := strconv.ParseUint(object.CiphertextSize, 10, 64)
	return err == nil && size > 0 && strconv.FormatUint(size, 10) == object.CiphertextSize
}

func decodeRequest(payload json.RawMessage) (requestPayload, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request requestPayload
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return requestPayload{}, ErrInvalidRequest
	}
	return request, nil
}

func canonicalUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 63)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, ErrInvalidRequest
	}
	return parsed, nil
}

func publicErrorCode(err error) string {
	switch {
	case errors.Is(err, peerprotocol.ErrCommandDenied), errors.Is(err, authority.ErrDenied), errors.Is(err, authority.ErrRevoked), errors.Is(err, lease.ErrDenied), errors.Is(err, lease.ErrFrozen):
		return "permission_denied"
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, peerprotocol.ErrCommandInvalid), errors.Is(err, lease.ErrInvalid):
		return "request_invalid"
	case errors.Is(err, archivecatalog.ErrNotFound), errors.Is(err, replication.ErrObjectUnavailable), errors.Is(err, lease.ErrNotFound):
		return "not_found"
	default:
		return "unavailable"
	}
}

type Response struct {
	RequestID string
	Kind      byte
	Body      []byte
}

// ReplicaImportResult is the durable provider-side outcome of one accepted
// peer offer. Partial fields remain available on error so callers can report
// the persisted lease state and safely retry the same operation.
type ReplicaImportResult struct {
	Lease    lease.Lease
	Snapshot archivesnapshot.Snapshot
	Manifest replication.ReplicaManifest
	Bundle   replication.Bundle
}

type leaseReceiptPayload struct {
	Version   string `json:"receipt_version"`
	LeaseID   string `json:"lease_id"`
	OfferHash string `json:"offer_hash"`
	Status    string `json:"status"`
}

type replicaRenewalPayload struct {
	Version   string                     `json:"renewal_payload_version"`
	LeaseID   string                     `json:"lease_id"`
	OfferHash string                     `json:"offer_hash"`
	Request   lease.SignedRenewalRequest `json:"request"`
}

type tombstoneReceiptPayload struct {
	Version         string                       `json:"receipt_version"`
	ArchiveID       string                       `json:"archive_id"`
	OwnerSubjectID  string                       `json:"owner_subject_id"`
	DeletionEpoch   string                       `json:"deletion_epoch"`
	TombstoneHash   string                       `json:"tombstone_hash"`
	Erasure         *replication.ReplicaErasure  `json:"erasure,omitempty"`
	DeletionReports []lease.SignedDeletionReport `json:"deletion_reports,omitempty"`
}

// TombstoneReceipt confirms durable tombstone acceptance. When the receiving
// node has the encrypted replica store configured, Erasure records its local
// deletion operation; neither field proves secure media overwriting.
type TombstoneReceipt = tombstoneReceiptPayload

// RequestSession is implemented by peertransport.AuthenticatedDataChannel.
// Requests on one session are serialized because the ordered data channel has
// one receive stream and each response is correlated to one command ID.
type RequestSession interface {
	Remote() authority.Principal
	SessionID() string
	SendCommand(context.Context, peerprotocol.Command) error
	Receive(context.Context) ([]byte, error)
}

type Client struct {
	session RequestSession
	maxBody int
	gate    chan struct{}
	scope   *RequestScope
}

// PeerCommandSigner is normally nodeidentity.Store. It keeps the Node Key
// private and rechecks current identity/key delegation before signing.
type PeerCommandSigner interface {
	SignPeerCommand(peerprotocol.Command, peerprotocol.IdentityResolver, time.Time) (peerprotocol.Command, error)
}

// RequestScope fixes the identity, archive and capability context for a
// client bound to one authenticated peer session. Individual range requests
// may select only an object and byte range inside this fixed scope.
type RequestScope struct {
	IdentityID       string
	IdentityHead     string
	NodeID           string
	TargetIdentityID string
	TargetNodeID     string
	OwnerSubjectID   string
	ArchiveID        string
	SnapshotID       string
	Action           string
	GrantChain       []authority.SignedGrant
	Resolve          peerprotocol.IdentityResolver
	Signer           PeerCommandSigner
	Now              func() time.Time
}

func NewClient(session RequestSession, maxResponseBytes int) (*Client, error) {
	if session == nil || session.SessionID() == "" || session.Remote().Kind != authority.PrincipalNode || session.Remote().ID == "" || maxResponseBytes < 1 || maxResponseBytes > MaxPeerResponseBytes {
		return nil, ErrInvalidRequest
	}
	return &Client{session: session, maxBody: maxResponseBytes, gate: make(chan struct{}, 1)}, nil
}

// NewScopedClient creates a client that can request ciphertext ranges using
// fresh Node-Key-signed commands. The peer session must already be mutually
// authenticated; command signing still validates both current identity
// histories and delegated Node Keys for each request.
func NewScopedClient(session RequestSession, maxResponseBytes int, scope RequestScope) (*Client, error) {
	client, err := NewClient(session, maxResponseBytes)
	if err != nil || scope.IdentityID == "" || scope.IdentityHead == "" || scope.NodeID == "" ||
		scope.TargetIdentityID == "" || scope.TargetNodeID == "" || scope.OwnerSubjectID == "" ||
		scope.ArchiveID == "" || scope.Action == "" || len(scope.GrantChain) == 0 ||
		scope.Resolve == nil || scope.Signer == nil || scope.TargetNodeID != session.Remote().ID {
		return nil, ErrInvalidRequest
	}
	scope.GrantChain = append([]authority.SignedGrant(nil), scope.GrantChain...)
	client.scope = &scope
	return client, nil
}

// HeadSnapshot fetches the latest immutable snapshot descriptor for the
// archive fixed in this client's capability scope. The grant must be
// archive-wide because a snapshot-restricted grant cannot discover another
// snapshot.
func (c *Client) HeadSnapshot(ctx context.Context) (archivesnapshot.Snapshot, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.view" || c.scope.SnapshotID != "" {
		return archivesnapshot.Snapshot{}, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "snapshot.head"}, "", "archive.view")
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return parseSnapshotResponse(response, c.scope.ArchiveID, "")
}

// GetSnapshot retrieves an immutable snapshot by ID. A client pinned to one
// snapshot cannot request another; an archive-wide grant remains subject to
// the peer's per-command authority check.
func (c *Client) GetSnapshot(ctx context.Context, snapshotID string) (archivesnapshot.Snapshot, error) {
	if c == nil || c.scope == nil || ctx == nil || (c.scope.Action != "archive.view" && c.scope.Action != "archive.replicate") || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) {
		return archivesnapshot.Snapshot{}, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "snapshot.get"}, snapshotID, c.scope.Action)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return parseSnapshotResponse(response, c.scope.ArchiveID, snapshotID)
}

// ReplicaManifest retrieves only the public ciphertext-to-coordinate mapping
// for a snapshot. The command must carry archive.view or archive.replicate authority; the
// manifest itself contains neither plaintext digests nor key material and is
// checked against the independently verified snapshot before it is returned.
func (c *Client) ReplicaManifest(ctx context.Context, snapshot archivesnapshot.Snapshot, keyEpoch uint64) (replication.ReplicaManifest, error) {
	if c == nil || c.scope == nil || ctx == nil || (c.scope.Action != "archive.view" && c.scope.Action != "archive.replicate") ||
		archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != c.scope.ArchiveID ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshot.SnapshotID) || keyEpoch == 0 {
		return replication.ReplicaManifest{}, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.manifest", KeyEpoch: strconv.FormatUint(keyEpoch, 10)}, snapshot.SnapshotID, c.scope.Action)
	if err != nil {
		return replication.ReplicaManifest{}, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return replication.ReplicaManifest{}, err
	}
	if response.Kind != responseKindManifest {
		return replication.ReplicaManifest{}, ErrResponseInvalid
	}
	var manifest replication.ReplicaManifest
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return replication.ReplicaManifest{}, ErrResponseInvalid
	}
	if _, err := replication.BundleFromPublicReplicaManifest(snapshot, manifest); err != nil || manifest.KeyEpoch != strconv.FormatUint(keyEpoch, 10) {
		return replication.ReplicaManifest{}, ErrResponseInvalid
	}
	return manifest, nil
}

// KeyEpochs returns the bounded set of validated encrypted bundle epochs for
// which this peer has a current, signed envelope addressed to recipientKeyID.
// Epochs are public metadata; the response never includes envelope or key
// material.
func (c *Client) KeyEpochs(ctx context.Context, snapshot archivesnapshot.Snapshot, recipientKeyID string) ([]uint64, error) {
	if c == nil || c.scope == nil || ctx == nil || (c.scope.Action != "archive.view" && c.scope.Action != "archive.replicate") ||
		archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != c.scope.ArchiveID ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshot.SnapshotID) || !validRecipientKeyID(recipientKeyID) || len(c.scope.GrantChain) == 0 {
		return nil, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.key-epochs", RecipientKeyID: recipientKeyID}, snapshot.SnapshotID, c.scope.Action)
	if err != nil {
		return nil, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return nil, err
	}
	if response.Kind != responseKindKeyEpochs {
		return nil, ErrResponseInvalid
	}
	return parseKeyEpochsPayload(response.Body, snapshot)
}

// SessionKeyEpochs returns only epochs for which the remote node can unlock a
// locally protected source key. It carries no recipient material or DEK.
func (c *Client) SessionKeyEpochs(ctx context.Context, snapshot archivesnapshot.Snapshot) ([]uint64, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.view" ||
		archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != c.scope.ArchiveID ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshot.SnapshotID) || len(c.scope.GrantChain) == 0 {
		return nil, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.session-key-epochs"}, snapshot.SnapshotID, "archive.view")
	if err != nil {
		return nil, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return nil, err
	}
	if response.Kind != responseKindKeyEpochs {
		return nil, ErrResponseInvalid
	}
	var payload sessionKeyEpochsPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || payload.Version != "ir.peer-session-key-epochs/1" ||
		payload.ArchiveID != snapshot.Content.ArchiveID || payload.SnapshotID != snapshot.SnapshotID || len(payload.KeyEpochs) > replication.MaxSnapshotKeyEpochs {
		return nil, ErrResponseInvalid
	}
	return parseCanonicalEpochs(payload.KeyEpochs)
}

// SessionDEKEnvelope requests a one-time envelope for an ephemeral browser
// recipient over the authenticated Node-Key session and current archive.view
// capability. The returned key remains encrypted to that recipient.
func (c *Client) SessionDEKEnvelope(ctx context.Context, snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientPublicKey []byte) (cryptoarchive.SessionDEKEnvelope, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.view" ||
		archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != c.scope.ArchiveID ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshot.SnapshotID) || keyEpoch == 0 || len(recipientPublicKey) != 32 || len(c.scope.GrantChain) == 0 {
		return cryptoarchive.SessionDEKEnvelope{}, ErrInvalidRequest
	}
	grantHash, err := authority.GrantHash(c.scope.GrantChain[len(c.scope.GrantChain)-1].Grant)
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{
		Operation: "replica.session-key-envelope", KeyEpoch: strconv.FormatUint(keyEpoch, 10),
		RecipientPublicKey: base64.RawURLEncoding.EncodeToString(recipientPublicKey),
	}, snapshot.SnapshotID, "archive.view")
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	if response.Kind != responseKindSessionDEK {
		return cryptoarchive.SessionDEKEnvelope{}, ErrResponseInvalid
	}
	envelope, err := cryptoarchive.ParseSessionDEKEnvelope(response.Body)
	if err != nil || envelope.ArchiveID != snapshot.Content.ArchiveID || envelope.SnapshotID != snapshot.SnapshotID ||
		envelope.KeyEpoch != strconv.FormatUint(keyEpoch, 10) || envelope.OwnerSubjectID != c.scope.OwnerSubjectID ||
		envelope.GrantHash != grantHash || envelope.SupplierNodeID != c.session.Remote().ID ||
		envelope.RecipientKeyID != cryptoarchive.RecipientKeyID(recipientPublicKey) ||
		envelope.RecipientPublicKeySHA256 != cryptoarchive.RecipientPublicKeyHash(recipientPublicKey) {
		return cryptoarchive.SessionDEKEnvelope{}, ErrResponseInvalid
	}
	return envelope, nil
}

func parseCanonicalEpochs(values []string) ([]uint64, error) {
	if len(values) > replication.MaxSnapshotKeyEpochs {
		return nil, ErrResponseInvalid
	}
	epochs := make([]uint64, 0, len(values))
	var previous uint64
	for index, value := range values {
		epoch, err := canonicalUint(value)
		if err != nil || epoch == 0 || index > 0 && epoch >= previous {
			return nil, ErrResponseInvalid
		}
		epochs = append(epochs, epoch)
		previous = epoch
	}
	return epochs, nil
}

func parseKeyEpochsPayload(body []byte, snapshot archivesnapshot.Snapshot) ([]uint64, error) {
	var payload keyEpochsPayload
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || payload.Version != "ir.peer-key-epochs/1" ||
		payload.ArchiveID != snapshot.Content.ArchiveID || payload.SnapshotID != snapshot.SnapshotID || len(payload.KeyEpochs) > replication.MaxSnapshotKeyEpochs {
		return nil, ErrResponseInvalid
	}
	epochs := make([]uint64, 0, len(payload.KeyEpochs))
	var previous uint64
	for index, value := range payload.KeyEpochs {
		epoch, parseErr := canonicalUint(value)
		if parseErr != nil || epoch == 0 || index > 0 && epoch >= previous {
			return nil, ErrResponseInvalid
		}
		epochs = append(epochs, epoch)
		previous = epoch
	}
	return epochs, nil
}

// ReplicaOffer fetches a provider-targeted owner-signed lease offer in bounded
// pages. The returned signature is not itself an acceptance: callers must
// persist it through the receiving lease Store's IssueOffer method, which
// independently rechecks current authority, signer history, expiry, and
// tombstones.
func (c *Client) ReplicaOffer(ctx context.Context, archiveID, snapshotID, leaseID string) (lease.SignedOffer, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) {
		return lease.SignedOffer{}, ErrInvalidRequest
	}
	const pageLimit = MaxOfferPageObjects
	var header lease.SignedOffer
	var headerBytes []byte
	var offerHash string
	var total uint64
	objects := make([]lease.EncryptedObject, 0)
	for offset := uint64(0); ; {
		command, err := c.newCommand(ctx, requestPayload{
			Operation: "replica.offer.page", LeaseID: leaseID,
			Offset: strconv.FormatUint(offset, 10), Limit: strconv.Itoa(pageLimit),
		}, snapshotID, "archive.replicate")
		if err != nil {
			return lease.SignedOffer{}, err
		}
		response, err := c.Do(ctx, command)
		if err != nil {
			return lease.SignedOffer{}, err
		}
		if response.Kind != responseKindOfferPage {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
		var page replicaOfferPage
		decoder := json.NewDecoder(bytes.NewReader(response.Body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&page) != nil || decoder.Decode(&struct{}{}) != io.EOF || page.Version != offerPageVersion ||
			page.LeaseID != leaseID || page.OfferHash == "" || page.Signed.Offer.EncryptedObjectSet != nil || len(page.Objects) == 0 || len(page.Objects) > pageLimit {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
		pageOffset, offsetErr := canonicalUint(page.Offset)
		pageTotal, totalErr := canonicalUint(page.Total)
		if offsetErr != nil || totalErr != nil || pageOffset != offset || pageTotal == 0 || pageTotal > lease.MaxObjects || pageOffset >= pageTotal {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
		expectedPageLength := pageTotal - pageOffset
		if expectedPageLength > pageLimit {
			expectedPageLength = pageLimit
		}
		if uint64(len(page.Objects)) != expectedPageLength || page.Signed.Offer.LeaseID != leaseID ||
			page.Signed.Offer.ArchiveID != archiveID || page.Signed.Offer.SnapshotID != snapshotID ||
			page.Signed.Offer.OwnerSubjectID != c.scope.OwnerSubjectID || page.Signed.Offer.ProviderNodeID != c.scope.NodeID {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
		currentHeader, err := json.Marshal(page.Signed)
		if err != nil {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
		if offset == 0 {
			header, headerBytes, offerHash, total = page.Signed, currentHeader, page.OfferHash, pageTotal
			objects = make([]lease.EncryptedObject, 0, int(total))
		} else if pageTotal != total || page.OfferHash != offerHash || !bytes.Equal(currentHeader, headerBytes) {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
		for _, object := range page.Objects {
			if !validLeaseObjectRef(object) || len(objects) > 0 && object.ObjectID <= objects[len(objects)-1].ObjectID {
				return lease.SignedOffer{}, ErrResponseInvalid
			}
			objects = append(objects, object)
		}
		offset += uint64(len(page.Objects))
		if offset == total {
			break
		}
		if offset > total {
			return lease.SignedOffer{}, ErrResponseInvalid
		}
	}
	header.Offer.EncryptedObjectSet = objects
	computedHash, err := lease.OfferHash(header.Offer)
	if err != nil || computedHash != offerHash {
		return lease.SignedOffer{}, ErrResponseInvalid
	}
	return header, nil
}

// ReplicaOffers lists bounded metadata for unaccepted offers that the source
// Node has explicitly addressed to this Node. It returns no signed grants,
// signatures, object identifiers, ciphertext, or key material. Import still
// fetches the original offer and crosses the receiver Store's full validation
// boundary, and acceptance remains a separate explicit operation.
func (c *Client) ReplicaOffers(ctx context.Context) ([]lease.PendingOffer, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		c.scope.ArchiveID == "" || c.scope.OwnerSubjectID == "" {
		return nil, ErrInvalidRequest
	}
	const pageLimit = lease.MaxPendingOfferPage
	result := make([]lease.PendingOffer, 0)
	var total int
	for offset := 0; ; {
		command, err := c.newCommand(ctx, requestPayload{
			Operation: "replica.offers", Offset: strconv.Itoa(offset), Limit: strconv.Itoa(pageLimit),
		}, c.scope.SnapshotID, "archive.replicate")
		if err != nil {
			return nil, err
		}
		response, err := c.Do(ctx, command)
		if err != nil {
			return nil, err
		}
		if response.Kind != responseKindOfferInbox {
			return nil, ErrResponseInvalid
		}
		var page replicaOfferInboxPage
		decoder := json.NewDecoder(bytes.NewReader(response.Body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&page) != nil || decoder.Decode(&struct{}{}) != io.EOF || page.Version != offerInboxVersion ||
			page.OwnerSubject != c.scope.OwnerSubjectID || page.ArchiveID != c.scope.ArchiveID || page.SnapshotScope != c.scope.SnapshotID {
			return nil, ErrResponseInvalid
		}
		pageOffset, offsetErr := canonicalUint(page.Offset)
		pageTotal, totalErr := canonicalUint(page.Total)
		if offsetErr != nil || totalErr != nil || pageOffset != uint64(offset) || pageTotal > lease.MaxPendingOffers || pageOffset > pageTotal {
			return nil, ErrResponseInvalid
		}
		if offset == 0 {
			total = int(pageTotal)
		} else if int(pageTotal) != total {
			// A mutation between pages invalidates the inventory. Do not return a
			// partial or potentially shifted inbox; the caller can retry.
			return nil, ErrResponseInvalid
		}
		want := pageLimit
		if remaining := total - offset; remaining < want {
			want = remaining
		}
		if len(page.Offers) != want {
			return nil, ErrResponseInvalid
		}
		for _, offer := range page.Offers {
			if !validPeerLeaseID(offer.LeaseID) || offer.ArchiveID != c.scope.ArchiveID || offer.OwnerSubject != c.scope.OwnerSubjectID ||
				!validPeerSnapshotID(offer.SnapshotID) || c.scope.SnapshotID != "" && offer.SnapshotID != c.scope.SnapshotID ||
				!validPeerSHA256(offer.OfferHash) || offer.ObjectCount < 1 || offer.ObjectCount > lease.MaxObjects {
				return nil, ErrResponseInvalid
			}
			keyEpoch, keyErr := canonicalUint(offer.KeyEpoch)
			reserved, reservedErr := canonicalUint(offer.ReservedBytes)
			if keyErr != nil || keyEpoch == 0 || reservedErr != nil || reserved == 0 {
				return nil, ErrResponseInvalid
			}
			if _, timeErr := time.Parse(time.RFC3339, offer.ExpiresAt); timeErr != nil {
				return nil, ErrResponseInvalid
			}
			if len(result) > 0 && offer.LeaseID <= result[len(result)-1].LeaseID {
				return nil, ErrResponseInvalid
			}
			result = append(result, offer)
		}
		offset += len(page.Offers)
		if offset == total {
			return result, nil
		}
		if len(page.Offers) == 0 || offset > total {
			return nil, ErrResponseInvalid
		}
	}
}

// ReceiveReplicaOffer fetches an offer and crosses the receiver's durable
// authorization boundary before returning. It cannot mark an offer accepted;
// that requires a separate provider-signed acceptance receipt.
func (c *Client) ReceiveReplicaOffer(ctx context.Context, destination *lease.Store, archiveID, snapshotID, leaseID string) (lease.Lease, error) {
	if destination == nil {
		return lease.Lease{}, ErrInvalidRequest
	}
	signed, err := c.ReplicaOffer(ctx, archiveID, snapshotID, leaseID)
	if err != nil {
		return lease.Lease{}, err
	}
	return destination.IssueOffer(ctx, signed)
}

// ReplicaAcceptanceSigner is normally nodeidentity.Store. The private Node
// Key remains behind that store and signs only the validated acceptance body.
type ReplicaAcceptanceSigner interface {
	SignReplicaAcceptance(lease.Acceptance) (lease.SignedAcceptance, error)
}

// ReplicaRenewalSigner is normally nodeidentity.Store. The provider signs
// only after its local lease Store accepts the owner-signed request.
type ReplicaRenewalSigner interface {
	SignReplicaRenewalReceipt(lease.SignedRenewalRequest) (lease.SignedRenewal, error)
}

// AcceptReplicaOffer persists the offer locally, records a provider-signed
// acceptance, and forwards that receipt to the source. If the network send
// fails after local persistence, the accepted lease is returned with the
// transport error so the caller can retry from its durable receipt.
func (c *Client) AcceptReplicaOffer(ctx context.Context, destination *lease.Store, signer ReplicaAcceptanceSigner, archiveID, snapshotID, leaseID string) (lease.Lease, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) || signer == nil || destination == nil {
		return lease.Lease{}, ErrInvalidRequest
	}
	current, err := destination.Get(ctx, leaseID)
	if errors.Is(err, lease.ErrNotFound) {
		current, err = c.ReceiveReplicaOffer(ctx, destination, archiveID, snapshotID, leaseID)
	} else if err == nil {
		offer := current.Offer.Offer
		if offer.ArchiveID != archiveID || offer.SnapshotID != snapshotID || offer.ProviderNodeID != c.scope.NodeID ||
			offer.OwnerSubjectID != c.scope.OwnerSubjectID || current.OfferHash == "" {
			return lease.Lease{}, ErrInvalidRequest
		}
	}
	if err != nil {
		return lease.Lease{}, err
	}
	var signed lease.SignedAcceptance
	if current.Status == lease.StatusOffered {
		acceptedAt := time.Now().UTC().Truncate(time.Second)
		if c.scope != nil && c.scope.Now != nil {
			acceptedAt = c.scope.Now().UTC().Truncate(time.Second)
		}
		value := lease.Acceptance{
			Version: lease.ProtocolVersion, LeaseID: leaseID, OfferHash: current.OfferHash,
			ProviderNodeID: current.Offer.Offer.ProviderNodeID, ReservedBytes: current.Offer.Offer.ReservedBytes,
			AcceptedAt: acceptedAt.Format(time.RFC3339),
		}
		signed, err = signer.SignReplicaAcceptance(value)
		if err != nil {
			return current, err
		}
		// The destination is the receiving provider's local durable store. Its
		// explicit consent must reserve capacity atomically with acceptance.
		accepted, acceptErr := destination.AcceptProvider(ctx, signed)
		if acceptErr != nil {
			// Another caller may have durably accepted the same offer between
			// our read and mutation. Reuse that exact signed receipt; never mint
			// a second acceptance for the same local lease.
			latest, getErr := destination.Get(ctx, leaseID)
			if getErr != nil || latest.OfferHash != current.OfferHash || latest.Acceptance.Signature == "" ||
				latest.Acceptance.Acceptance.LeaseID != leaseID || latest.Acceptance.Acceptance.OfferHash != latest.OfferHash ||
				latest.Acceptance.Acceptance.ProviderNodeID != c.scope.NodeID || latest.Offer.Offer.ArchiveID != archiveID ||
				latest.Offer.Offer.SnapshotID != snapshotID || latest.Offer.Offer.OwnerSubjectID != c.scope.OwnerSubjectID ||
				latest.Offer.Offer.ProviderNodeID != c.scope.NodeID {
				return current, acceptErr
			}
			switch latest.Status {
			case lease.StatusAccepted, lease.StatusTransferring, lease.StatusVerifying, lease.StatusActive, lease.StatusRenewing:
			default:
				return current, acceptErr
			}
			current, signed = latest, latest.Acceptance
		} else {
			current = accepted
		}
	} else {
		current, err = destination.Get(ctx, leaseID)
		if err != nil {
			return lease.Lease{}, err
		}
		switch current.Status {
		case lease.StatusAccepted, lease.StatusTransferring, lease.StatusVerifying, lease.StatusActive, lease.StatusRenewing:
		default:
			return lease.Lease{}, ErrInvalidRequest
		}
		if current.Acceptance.Acceptance.LeaseID != leaseID || current.Acceptance.Signature == "" || current.OfferHash == "" {
			return lease.Lease{}, ErrInvalidRequest
		}
		signed = current.Acceptance
	}
	if err := c.SubmitReplicaAcceptance(ctx, archiveID, snapshotID, leaseID, signed); err != nil {
		return current, err
	}
	// Transition both durable lease records into transfer only after the source
	// has acknowledged the provider's exact persisted acceptance. Both calls are
	// idempotent, so a lost receipt response can safely retry from local state.
	current, err = destination.BeginTransfer(ctx, leaseID)
	if errors.Is(err, lease.ErrTransition) {
		// A repeated acceptance request may arrive after this provider already
		// advanced the same durable lease beyond transfer. Do not rewind it.
		latest, getErr := destination.Get(ctx, leaseID)
		if getErr != nil || latest.OfferHash != current.OfferHash ||
			(latest.Status != lease.StatusVerifying && latest.Status != lease.StatusActive && latest.Status != lease.StatusRenewing) {
			return current, err
		}
		return latest, nil
	}
	if err != nil {
		return current, err
	}
	if err := c.BeginReplicaTransfer(ctx, archiveID, snapshotID, leaseID); err != nil {
		return current, err
	}
	return current, nil
}

// SubmitReplicaAcceptance sends the already durable provider receipt to its
// source over a fresh archive.replicate command. Repeating the same signed
// acceptance is idempotent at the source Store.
func (c *Client) SubmitReplicaAcceptance(ctx context.Context, archiveID, snapshotID, leaseID string, signed lease.SignedAcceptance) error {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) ||
		signed.Acceptance.LeaseID != leaseID || signed.Acceptance.ProviderNodeID != c.scope.NodeID {
		return ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.acceptance", LeaseID: leaseID, Acceptance: &signed}, snapshotID, "archive.replicate")
	if err != nil {
		return err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return err
	}
	if response.Kind != responseKindLeaseReceipt {
		return ErrResponseInvalid
	}
	var receipt leaseReceiptPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		receipt.Version != "ir.replica-lease-receipt/1" || receipt.LeaseID != leaseID ||
		receipt.OfferHash != signed.Acceptance.OfferHash || receipt.Status != string(lease.StatusAccepted) {
		return ErrResponseInvalid
	}
	return nil
}

// BeginReplicaTransfer tells the source to enter its durable transferring
// state after the provider has persisted and delivered its signed acceptance.
// The source's transition is idempotent so retries do not reopen an expired or
// terminal lease.
func (c *Client) BeginReplicaTransfer(ctx context.Context, archiveID, snapshotID, leaseID string) error {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) {
		return ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.transfer.begin", LeaseID: leaseID}, snapshotID, "archive.replicate")
	if err != nil {
		return err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return err
	}
	if response.Kind != responseKindLeaseReceipt {
		return ErrResponseInvalid
	}
	var receipt leaseReceiptPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		receipt.Version != "ir.replica-lease-receipt/1" || receipt.LeaseID != leaseID ||
		receipt.Status != string(lease.StatusTransferring) || receipt.OfferHash == "" {
		return ErrResponseInvalid
	}
	return nil
}

// SubmitReplicaCheckpoint forwards the provider's already durable, Node-Key
// signed ciphertext offset to the source lease. The source independently
// verifies the signature and monotonic offset before advancing its projection.
func (c *Client) SubmitReplicaCheckpoint(ctx context.Context, archiveID, snapshotID, leaseID string, signed lease.SignedCheckpoint) error {
	checkpoint := signed.Checkpoint
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) ||
		checkpoint.LeaseID != leaseID || checkpoint.ProviderNodeID != c.scope.NodeID || checkpoint.OfferHash == "" || signed.Signature == "" {
		return ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.checkpoint", LeaseID: leaseID, Checkpoint: &signed}, snapshotID, "archive.replicate")
	if err != nil {
		return err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return err
	}
	if response.Kind != responseKindLeaseReceipt {
		return ErrResponseInvalid
	}
	var receipt leaseReceiptPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		receipt.Version != "ir.replica-lease-receipt/1" || receipt.LeaseID != leaseID ||
		receipt.OfferHash != checkpoint.OfferHash ||
		(receipt.Status != string(lease.StatusTransferring) && receipt.Status != string(lease.StatusVerifying)) {
		return ErrResponseInvalid
	}
	return nil
}

// VerifyReplica asks the source to read every immutable ciphertext object back
// from this provider over the authenticated session. The source activates its
// lease only after size and digest verification succeeds. Repeating the call
// after a lost response is safe: the source returns the already-active receipt.
func (c *Client) VerifyReplica(ctx context.Context, destination *lease.Store, archiveID, snapshotID, leaseID string) error {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) || destination == nil {
		return ErrInvalidRequest
	}
	local, err := destination.Get(ctx, leaseID)
	if err != nil {
		return err
	}
	offer := local.Offer.Offer
	if local.Status != lease.StatusActive || offer.LeaseID != leaseID || offer.ArchiveID != archiveID ||
		offer.SnapshotID != snapshotID || offer.OwnerSubjectID != c.scope.OwnerSubjectID ||
		offer.ProviderNodeID != c.scope.NodeID || local.OfferHash == "" {
		return lease.ErrTransition
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.verify", LeaseID: leaseID}, snapshotID, "archive.replicate")
	if err != nil {
		return err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return err
	}
	if response.Kind != responseKindLeaseReceipt {
		return ErrResponseInvalid
	}
	var receipt leaseReceiptPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		receipt.Version != "ir.replica-lease-receipt/1" || receipt.LeaseID != leaseID ||
		receipt.OfferHash != local.OfferHash || receipt.Status != string(lease.StatusActive) {
		return ErrResponseInvalid
	}
	return nil
}

// ImportReplicaFromPeer accepts or resumes a previously offered replica,
// pulls only the offered encrypted snapshot, finalizes the provider's durable
// ciphertext binding, then requests source-side readback before returning
// success. The caller must explicitly invoke this operation; merely receiving
// an offer never grants storage or acceptance automatically.
func (c *Client) ImportReplicaFromPeer(
	ctx context.Context,
	leases *lease.Store,
	store *replication.Store,
	acceptanceSigner ReplicaAcceptanceSigner,
	checkpointSigner replication.ReplicaCheckpointSigner,
	archiveID, snapshotID, leaseID string,
) (ReplicaImportResult, error) {
	result := ReplicaImportResult{}
	if c == nil || c.scope == nil || ctx == nil || leases == nil || store == nil || acceptanceSigner == nil || checkpointSigner == nil ||
		c.scope.Action != "archive.replicate" || archiveID == "" || archiveID != c.scope.ArchiveID ||
		snapshotID == "" || c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID || !validPeerLeaseID(leaseID) {
		return result, ErrInvalidRequest
	}
	current, err := c.AcceptReplicaOffer(ctx, leases, acceptanceSigner, archiveID, snapshotID, leaseID)
	result.Lease = current
	if err != nil {
		return result, err
	}
	snapshot, err := c.GetSnapshot(ctx, snapshotID)
	if err != nil {
		return result, err
	}
	result.Snapshot = snapshot
	keyEpoch, err := canonicalUint(current.Offer.Offer.KeyEpoch)
	if err != nil || keyEpoch == 0 {
		return result, lease.ErrInvalid
	}
	manifest, err := c.ReplicaManifest(ctx, snapshot, keyEpoch)
	if err != nil {
		return result, err
	}
	result.Manifest = manifest
	importAuthorization := replication.LeaseImportAuthorizer{
		LeaseID: leaseID, ProviderNodeID: c.scope.NodeID, Leases: leases,
		ForwardCheckpoint: func(checkpointCtx context.Context, checkpoint lease.SignedCheckpoint) error {
			return c.SubmitReplicaCheckpoint(checkpointCtx, archiveID, snapshotID, leaseID, checkpoint)
		},
	}
	bundle, err := store.ImportReplicaFromPeer(ctx, importAuthorization, snapshot, manifest, c, checkpointSigner)
	if err != nil {
		if latest, getErr := leases.Get(ctx, leaseID); getErr == nil {
			result.Lease = latest
		}
		return result, err
	}
	result.Bundle = bundle
	if err := c.VerifyReplica(ctx, leases, archiveID, snapshotID, leaseID); err != nil {
		if latest, getErr := leases.Get(ctx, leaseID); getErr == nil {
			result.Lease = latest
		}
		return result, err
	}
	result.Lease, err = leases.Get(ctx, leaseID)
	if err != nil {
		return result, err
	}
	if result.Lease.Status != lease.StatusActive {
		return result, lease.ErrTransition
	}
	return result, nil
}

// RenewReplicaLease applies a source's pending owner-authorized renewal at the
// provider, obtains the provider's durable Node-Key receipt, and submits that
// exact receipt back to the source. Both stores remain resumable if transport
// fails after either local durable transition.
func (c *Client) RenewReplicaLease(ctx context.Context, destination *lease.Store, signer ReplicaRenewalSigner, archiveID, snapshotID, leaseID string) (lease.Lease, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) || signer == nil || destination == nil {
		return lease.Lease{}, ErrInvalidRequest
	}
	current, err := destination.Get(ctx, leaseID)
	if err != nil {
		return lease.Lease{}, err
	}
	offer := current.Offer.Offer
	if offer.LeaseID != leaseID || offer.ArchiveID != archiveID || offer.SnapshotID != snapshotID ||
		offer.OwnerSubjectID != c.scope.OwnerSubjectID || offer.ProviderNodeID != c.scope.NodeID || current.OfferHash == "" {
		return lease.Lease{}, ErrInvalidRequest
	}
	request, offerHash, err := c.ReplicaRenewalRequest(ctx, archiveID, snapshotID, leaseID)
	if err != nil {
		return current, err
	}
	if offerHash != current.OfferHash || request.Renewal.LeaseID != leaseID || request.Renewal.OfferHash != current.OfferHash ||
		request.Renewal.OwnerSubjectID != offer.OwnerSubjectID {
		return current, ErrResponseInvalid
	}
	if _, err := destination.BeginRenewal(ctx, request); err != nil {
		return current, err
	}
	signed, err := signer.SignReplicaRenewalReceipt(request)
	if err != nil {
		return current, err
	}
	if err := c.SubmitReplicaRenewal(ctx, archiveID, snapshotID, leaseID, signed); err != nil {
		return current, err
	}
	// Keep the provider's durable lease in StatusRenewing until the source has
	// acknowledged the receipt. A retry can re-sign the same deterministic
	// Ed25519 receipt and replay it after any process restart.
	return destination.CompleteRenewal(ctx, signed)
}

// ReplicaRenewalRequest retrieves a pending owner request or the source's
// latest completed request. The latter permits exact receipt replay after a
// lost response without creating a new renewal or extending its expiry.
func (c *Client) ReplicaRenewalRequest(ctx context.Context, archiveID, snapshotID, leaseID string) (lease.SignedRenewalRequest, string, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) {
		return lease.SignedRenewalRequest{}, "", ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.renewal.request", LeaseID: leaseID}, snapshotID, "archive.replicate")
	if err != nil {
		return lease.SignedRenewalRequest{}, "", err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return lease.SignedRenewalRequest{}, "", err
	}
	if response.Kind != responseKindRenewal {
		return lease.SignedRenewalRequest{}, "", ErrResponseInvalid
	}
	var payload replicaRenewalPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		payload.Version != "ir.replica-renewal-request/1" || payload.LeaseID != leaseID ||
		payload.OfferHash == "" || payload.Request.Renewal.LeaseID != leaseID || payload.Request.Renewal.OfferHash != payload.OfferHash ||
		payload.Request.Renewal.OwnerSubjectID != c.scope.OwnerSubjectID || payload.Request.OwnerSignature == "" {
		return lease.SignedRenewalRequest{}, "", ErrResponseInvalid
	}
	if _, err := lease.CanonicalRenewal(payload.Request.Renewal); err != nil {
		return lease.SignedRenewalRequest{}, "", ErrResponseInvalid
	}
	return payload.Request, payload.OfferHash, nil
}

// SubmitReplicaRenewal forwards a provider-signed renewal receipt to the
// source's durable Store. It is safe to retry the exact same signed value.
func (c *Client) SubmitReplicaRenewal(ctx context.Context, archiveID, snapshotID, leaseID string, signed lease.SignedRenewal) error {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		archiveID == "" || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshotID) || !validPeerLeaseID(leaseID) ||
		signed.Renewal.LeaseID != leaseID || signed.Actor.ID == "" {
		return ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.renewal.receipt", LeaseID: leaseID, Renewal: &signed}, snapshotID, "archive.replicate")
	if err != nil {
		return err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return err
	}
	if response.Kind != responseKindLeaseReceipt {
		return ErrResponseInvalid
	}
	var receipt leaseReceiptPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		receipt.Version != "ir.replica-lease-receipt/1" || receipt.LeaseID != leaseID ||
		receipt.OfferHash != signed.Renewal.OfferHash || receipt.Status != string(lease.StatusActive) {
		return ErrResponseInvalid
	}
	return nil
}

// SubmitTombstone delivers an Owner-signed deletion decision over the
// authenticated peer command channel. The receiver revalidates the embedded
// archive.delete capability and signature before blocking new replication.
// Its receipt confirms durable tombstone storage only, never physical erasure.
func (c *Client) SubmitTombstone(ctx context.Context, signed lease.SignedTombstone) error {
	_, err := c.SubmitTombstoneWithReceipt(ctx, signed)
	return err
}

// SubmitTombstoneWithReceipt delivers the exact owner-signed decision and
// returns the authenticated provider's durable result.
func (c *Client) SubmitTombstoneWithReceipt(ctx context.Context, signed lease.SignedTombstone) (TombstoneReceipt, error) {
	if c == nil || c.scope == nil || ctx == nil || c.scope.Action != "archive.replicate" ||
		c.scope.SnapshotID != "" ||
		signed.Tombstone.ArchiveID != c.scope.ArchiveID || signed.Tombstone.OwnerSubjectID != c.scope.OwnerSubjectID {
		return TombstoneReceipt{}, ErrInvalidRequest
	}
	canonical, err := lease.CanonicalTombstone(signed.Tombstone)
	if err != nil {
		return TombstoneReceipt{}, ErrInvalidRequest
	}
	expectedDigest := sha256.Sum256(append([]byte("IR/archive-tombstone/v1/hash\x00"), canonical...))
	expectedHash := "sha256:" + hex.EncodeToString(expectedDigest[:])
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.tombstone", Tombstone: &signed}, "", "archive.replicate")
	if err != nil {
		return TombstoneReceipt{}, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return TombstoneReceipt{}, err
	}
	if response.Kind != responseKindTombstone {
		return TombstoneReceipt{}, ErrResponseInvalid
	}
	var receipt tombstoneReceiptPayload
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		receipt.Version != "ir.archive-tombstone-receipt/1" || receipt.ArchiveID != signed.Tombstone.ArchiveID ||
		receipt.OwnerSubjectID != signed.Tombstone.OwnerSubjectID || receipt.DeletionEpoch != signed.Tombstone.DeletionEpoch ||
		receipt.TombstoneHash != expectedHash {
		return TombstoneReceipt{}, ErrResponseInvalid
	}
	if receipt.Erasure != nil && (receipt.Erasure.ArchiveID != receipt.ArchiveID || receipt.Erasure.TombstoneHash != receipt.TombstoneHash) {
		return TombstoneReceipt{}, ErrResponseInvalid
	}
	for _, report := range receipt.DeletionReports {
		if _, err := lease.CanonicalDeletionReport(report.Report); err != nil ||
			report.Report.ProviderNodeID != c.scope.TargetNodeID || report.Report.Status != string(lease.StatusDeleted) ||
			report.Report.TombstoneHash != receipt.TombstoneHash {
			return TombstoneReceipt{}, ErrResponseInvalid
		}
	}
	return receipt, nil
}

// KeyEnvelope retrieves a signed, recipient-bound encrypted DEK envelope. It
// never returns a plaintext key; the receiver must hold the matching private
// recovery key and independently open the envelope.
func (c *Client) KeyEnvelope(ctx context.Context, snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientKeyID string) (cryptoarchive.KeyEnvelope, error) {
	if c == nil || c.scope == nil || ctx == nil || (c.scope.Action != "archive.view" && c.scope.Action != "archive.replicate") ||
		archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != c.scope.ArchiveID ||
		(c.scope.SnapshotID != "" && c.scope.SnapshotID != snapshot.SnapshotID) || keyEpoch == 0 || !validRecipientKeyID(recipientKeyID) || len(c.scope.GrantChain) == 0 {
		return cryptoarchive.KeyEnvelope{}, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{Operation: "replica.key-envelope", KeyEpoch: strconv.FormatUint(keyEpoch, 10), RecipientKeyID: recipientKeyID}, snapshot.SnapshotID, c.scope.Action)
	if err != nil {
		return cryptoarchive.KeyEnvelope{}, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return cryptoarchive.KeyEnvelope{}, err
	}
	if response.Kind != responseKindKeyEnvelope {
		return cryptoarchive.KeyEnvelope{}, ErrResponseInvalid
	}
	envelope, err := cryptoarchive.ParseKeyEnvelope(response.Body)
	if err != nil || envelope.ArchiveID != snapshot.Content.ArchiveID || envelope.KeyEpoch != strconv.FormatUint(keyEpoch, 10) || envelope.RecipientKeyID != recipientKeyID {
		return cryptoarchive.KeyEnvelope{}, ErrResponseInvalid
	}
	issuer := c.scope.GrantChain[0].Grant.Issuer
	if issuer.Kind != authority.PrincipalIdentity || issuer.ID == "" {
		return cryptoarchive.KeyEnvelope{}, ErrResponseInvalid
	}
	now := time.Now().UTC()
	if c.scope.Now != nil {
		now = c.scope.Now().UTC()
	}
	state, err := c.scope.Resolve(issuer.ID, "", now)
	if err != nil || state.IdentityID != issuer.ID || state.Frozen || state.VerifiedAt.IsZero() || state.FreshUntil.IsZero() || now.Before(state.VerifiedAt) || now.After(state.FreshUntil) {
		return cryptoarchive.KeyEnvelope{}, ErrResponseInvalid
	}
	signerPublic := state.CurrentKeys[authority.PrincipalIdentity+":"+issuer.ID]
	signerKeyID, err := identityprotocol.KeyID(signerPublic)
	if err != nil || signerKeyID != envelope.SignerKeyID || cryptoarchive.VerifyKeyEnvelopeSignature(envelope, signerPublic) != nil {
		return cryptoarchive.KeyEnvelope{}, ErrResponseInvalid
	}
	return envelope, nil
}

func parseSnapshotResponse(response Response, archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	if response.Kind != responseKindSnapshot {
		return archivesnapshot.Snapshot{}, ErrResponseInvalid
	}
	snapshot, err := archivesnapshot.Parse(response.Body)
	if err != nil || archivesnapshot.Verify(snapshot) != nil || snapshot.Content.ArchiveID != archiveID ||
		(snapshotID != "" && snapshot.SnapshotID != snapshotID) {
		return archivesnapshot.Snapshot{}, ErrResponseInvalid
	}
	return snapshot, nil
}

// OpenCiphertextRange implements replication.CiphertextRangeSource. Each
// range is a new short-lived signed command and the complete response is
// bounded, request-correlated, digest-checked, and required to match the
// exact requested length before any bytes are returned to the importer.
func (c *Client) OpenCiphertextRange(ctx context.Context, archiveID, snapshotID, objectID string, keyEpoch uint64, offset, length int64) (io.ReadCloser, error) {
	if c == nil || c.scope == nil || ctx == nil || archiveID != c.scope.ArchiveID || snapshotID == "" ||
		(c.scope.SnapshotID != "" && snapshotID != c.scope.SnapshotID) ||
		!validPeerObjectID(objectID) || keyEpoch == 0 || offset < 0 || length < 1 || length > replication.MaxCiphertextChunkBytes {
		return nil, ErrInvalidRequest
	}
	command, err := c.newCommand(ctx, requestPayload{
		Operation: "ciphertext.range", ObjectID: objectID, KeyEpoch: strconv.FormatUint(keyEpoch, 10),
		Offset: strconv.FormatInt(offset, 10), Length: strconv.FormatInt(length, 10),
	}, snapshotID, c.scope.Action)
	if err != nil {
		return nil, err
	}
	response, err := c.Do(ctx, command)
	if err != nil {
		return nil, err
	}
	if response.Kind != responseKindCiphertext || int64(len(response.Body)) != length {
		return nil, ErrResponseInvalid
	}
	return io.NopCloser(bytes.NewReader(response.Body)), nil
}

func (c *Client) newCommand(ctx context.Context, payload requestPayload, snapshotID, action string) (peerprotocol.Command, error) {
	if c == nil || c.scope == nil || ctx == nil || action == "" {
		return peerprotocol.Command{}, ErrInvalidRequest
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return peerprotocol.Command{}, ErrInvalidRequest
	}
	requestID, nonce, err := peerprotocol.NewCommand(nil)
	if err != nil {
		return peerprotocol.Command{}, err
	}
	now := time.Now().UTC()
	if c.scope.Now != nil {
		now = c.scope.Now().UTC()
	}
	if now.IsZero() {
		return peerprotocol.Command{}, ErrInvalidRequest
	}
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: c.session.SessionID(),
		SourceIdentityID: c.scope.IdentityID, SourceHeadHash: c.scope.IdentityHead, SourceNodeID: c.scope.NodeID,
		TargetIdentityID: c.scope.TargetIdentityID, TargetNodeID: c.scope.TargetNodeID,
		OwnerSubjectID: c.scope.OwnerSubjectID, ArchiveID: c.scope.ArchiveID, SnapshotID: snapshotID,
		Action: action, IssuedAt: now.Truncate(time.Second).Format(time.RFC3339),
		ExpiresAt: now.Truncate(time.Second).Add(peerprotocol.MaxCommandTTL).Format(time.RFC3339),
		Nonce:     nonce, GrantChain: append([]authority.SignedGrant(nil), c.scope.GrantChain...), Payload: payloadBytes,
	}
	command.PayloadHash, err = peerprotocol.PayloadDigest(command.Payload)
	if err != nil {
		return peerprotocol.Command{}, ErrInvalidRequest
	}
	return c.scope.Signer.SignPeerCommand(command, c.scope.Resolve, now)
}

func validPeerObjectID(value string) bool {
	if !strings.HasPrefix(value, "ob_") || len(value) != 46 {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "ob_"))
	return err == nil && len(decoded) == sha256.Size && base64.RawURLEncoding.EncodeToString(decoded) == value[3:]
}

// Do sends one already signed command and receives its complete bounded
// response. It validates command framing locally, binds it to this live
// session, and checks safe remote error envelopes without exposing peer
// implementation details.
func (c *Client) Do(ctx context.Context, command peerprotocol.Command) (Response, error) {
	if c == nil || c.session == nil || c.gate == nil || ctx == nil || command.SessionID != c.session.SessionID() ||
		c.session.Remote().Kind != authority.PrincipalNode || command.TargetNodeID != c.session.Remote().ID {
		return Response{}, peerprotocol.ErrCommandDenied
	}
	select {
	case c.gate <- struct{}{}:
		defer func() { <-c.gate }()
	case <-ctx.Done():
		return Response{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	wire, err := json.Marshal(command)
	if err != nil || len(wire) > peertransport.MaxControlFrame {
		return Response{}, peerprotocol.ErrCommandInvalid
	}
	if _, err := peerprotocol.ParseCommand(wire); err != nil {
		return Response{}, peerprotocol.ErrCommandInvalid
	}
	if err := c.session.SendCommand(ctx, command); err != nil {
		return Response{}, err
	}
	response, err := ReceiveResponse(ctx, c.session, command.RequestID, c.maxBody)
	if err != nil {
		return Response{}, err
	}
	if response.Kind != responseKindError {
		return response, nil
	}
	var remoteError errorPayload
	if json.Unmarshal(response.Body, &remoteError) != nil || remoteError.Version != ResponseVersion || remoteError.RequestID != command.RequestID || !validPublicErrorCode(remoteError.Code) {
		return Response{}, ErrResponseInvalid
	}
	return response, RemoteError{Code: remoteError.Code}
}

type RemoteError struct{ Code string }

func (e RemoteError) Error() string        { return "remote peer request failed: " + e.Code }
func (e RemoteError) Is(target error) bool { return target == ErrRemoteFailure }

func validPublicErrorCode(code string) bool {
	switch code {
	case "permission_denied", "request_invalid", "not_found", "unavailable":
		return true
	default:
		return false
	}
}

// EncodeResponse splits one response into ordered, bounded WebRTC frames.
// Each frame repeats the total size and content hash so truncation, duplicate
// chunks, cross-request mixing, and payload corruption fail closed.
func EncodeResponse(requestID string, kind byte, body []byte) ([][]byte, error) {
	id, err := decodeRequestID(requestID)
	if err != nil || kind < responseKindSnapshot || kind > responseKindOfferInbox || len(body) == 0 || len(body) > MaxPeerResponseBytes {
		return nil, ErrResponseInvalid
	}
	chunkSize := peertransport.MaxDataFrame - responseHeaderBytes
	count := (len(body) + chunkSize - 1) / chunkSize
	if count == 0 || count > int(^uint32(0)) {
		return nil, ErrResponseInvalid
	}
	digest := sha256.Sum256(body)
	frames := make([][]byte, 0, count)
	for index, offset := 0, 0; offset < len(body); index++ {
		end := offset + chunkSize
		if end > len(body) {
			end = len(body)
		}
		frame := make([]byte, responseHeaderBytes+end-offset)
		copy(frame[:4], responseMagic)
		copy(frame[4:20], id)
		frame[20] = kind
		binary.BigEndian.PutUint32(frame[21:25], uint32(index))
		binary.BigEndian.PutUint32(frame[25:29], uint32(count))
		binary.BigEndian.PutUint64(frame[29:37], uint64(len(body)))
		copy(frame[37:responseHeaderBytes], digest[:])
		copy(frame[responseHeaderBytes:], body[offset:end])
		frames = append(frames, frame)
		offset = end
	}
	return frames, nil
}

// ReceiveResponse assembles one ordered response and verifies its exact
// request ID, bounds, chunk sequence, and whole-body SHA-256.
type ResponseSession interface {
	Receive(context.Context) ([]byte, error)
}

func ReceiveResponse(ctx context.Context, session ResponseSession, requestID string, maxBytes int) (Response, error) {
	wantID, err := decodeRequestID(requestID)
	if err != nil || session == nil || ctx == nil || maxBytes < 1 || maxBytes > MaxPeerResponseBytes {
		return Response{}, ErrResponseInvalid
	}
	first, err := session.Receive(ctx)
	if err != nil {
		return Response{}, err
	}
	meta, chunk, err := decodeResponseFrame(first, wantID, 0, maxBytes)
	if err != nil {
		return Response{}, err
	}
	body := make([]byte, 0, int(meta.totalBytes))
	body = append(body, chunk...)
	for index := uint32(1); index < meta.count; index++ {
		frame, err := session.Receive(ctx)
		if err != nil {
			return Response{}, err
		}
		next, chunk, err := decodeResponseFrame(frame, wantID, index, maxBytes)
		if err != nil || next != meta {
			return Response{}, ErrResponseInvalid
		}
		body = append(body, chunk...)
	}
	digest := sha256.Sum256(body)
	if uint64(len(body)) != meta.totalBytes || !bytes.Equal(digest[:], meta.digest[:]) {
		return Response{}, ErrResponseInvalid
	}
	return Response{RequestID: requestID, Kind: meta.kind, Body: body}, nil
}

type responseMeta struct {
	kind       byte
	count      uint32
	totalBytes uint64
	digest     [sha256.Size]byte
}

func decodeResponseFrame(frame, expectedID []byte, expectedIndex uint32, maxBytes int) (responseMeta, []byte, error) {
	if len(frame) < responseHeaderBytes || len(frame) > peertransport.MaxDataFrame || string(frame[:4]) != responseMagic || !bytes.Equal(frame[4:20], expectedID) {
		return responseMeta{}, nil, ErrResponseInvalid
	}
	meta := responseMeta{kind: frame[20], count: binary.BigEndian.Uint32(frame[25:29]), totalBytes: binary.BigEndian.Uint64(frame[29:37])}
	index := binary.BigEndian.Uint32(frame[21:25])
	copy(meta.digest[:], frame[37:responseHeaderBytes])
	if meta.kind < responseKindSnapshot || meta.kind > responseKindOfferInbox || meta.count == 0 || index != expectedIndex || meta.totalBytes == 0 || meta.totalBytes > uint64(maxBytes) {
		return responseMeta{}, nil, ErrResponseInvalid
	}
	chunk := frame[responseHeaderBytes:]
	chunkSize := uint64(peertransport.MaxDataFrame - responseHeaderBytes)
	wantCount := (meta.totalBytes + chunkSize - 1) / chunkSize
	wantChunk := chunkSize
	if index == meta.count-1 {
		wantChunk = meta.totalBytes - uint64(index)*chunkSize
	}
	if wantCount != uint64(meta.count) || uint64(len(chunk)) != wantChunk {
		return responseMeta{}, nil, ErrResponseInvalid
	}
	return meta, chunk, nil
}

func decodeRequestID(requestID string) ([]byte, error) {
	if !strings.HasPrefix(requestID, "pc_") || len(requestID) != 25 {
		return nil, ErrResponseInvalid
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(requestID, "pc_"))
	if err != nil || len(decoded) != 16 || base64.RawURLEncoding.EncodeToString(decoded) != requestID[3:] {
		return nil, ErrResponseInvalid
	}
	return decoded, nil
}
