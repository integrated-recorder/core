// Package lease implements durable replica-lease and tombstone state.
// A lease is evidence of a peer's bounded retention responsibility, not proof
// that a remote peer will retain or delete bytes. Ciphertext is verified by
// reading it back before a lease becomes active.
package lease

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	ProtocolVersion                 = "ir.replica-lease/1"
	TombstoneVersion                = "ir.archive-tombstone/1"
	MaxObjects                      = 100000
	MaxPendingOffers                = 4096
	MaxPendingOfferPage             = 128
	MaxLeaseDuration                = 90 * 24 * time.Hour
	MaxReservedBytes         uint64 = 1 << 50
	MaxCiphertextObjectBytes int64  = 2 << 30
	maxStateBytes                   = 64 << 20
	offerDomain                     = "IR/replica-lease/v1/offer\x00"
	acceptDomain                    = "IR/replica-lease/v1/accept\x00"
	checkpointDomain                = "IR/replica-lease/v1/checkpoint\x00"
	renewDomain                     = "IR/replica-lease/v1/renew\x00"
	tombstoneDomain                 = "IR/archive-tombstone/v1/sign\x00"
	deletionReportDomain            = "IR/replica-lease/v1/deletion-report\x00"
)

var (
	ErrInvalid          = errors.New("replica lease data is invalid")
	ErrDenied           = errors.New("replica lease action is not authorized")
	ErrConflict         = errors.New("replica lease state conflicts")
	ErrNotFound         = errors.New("replica lease not found")
	ErrTransition       = errors.New("replica lease transition is not allowed")
	ErrFrozen           = errors.New("archive replication is frozen by conflicting tombstones")
	ErrCapacityExceeded = errors.New("replica provider capacity exceeded")
	ErrCapacityOverflow = errors.New("replica provider capacity accounting overflow")
	ErrProviderIdentity = errors.New("local replica provider identity is unavailable")
	archiveIDPattern    = regexp.MustCompile(`^arc_[A-Za-z0-9_-]{22}$`)
	snapshotIDPattern   = regexp.MustCompile(`^snap_[a-f0-9]{64}$`)
	leaseIDPattern      = regexp.MustCompile(`^rl_[A-Za-z0-9_-]{22}$`)
	objectIDPattern     = regexp.MustCompile(`^ob_[A-Za-z0-9_-]{16,64}$`)
	ownerSubjectPattern = regexp.MustCompile(`^os_[A-Za-z0-9_-]{22}$`)
	identityIDPattern   = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	nodeIDPattern       = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	hashPattern         = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	decimalPattern      = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
)

type Status string

const (
	StatusOffered      Status = "offered"
	StatusAccepted     Status = "accepted"
	StatusTransferring Status = "transferring"
	StatusVerifying    Status = "verifying"
	StatusActive       Status = "active"
	StatusRenewing     Status = "renewing"
	StatusExpired      Status = "expired"
	StatusFailed       Status = "failed"
	StatusTerminating  Status = "terminating"
	StatusDeleted      Status = "deleted"
	StatusUnconfirmed  Status = "unconfirmed"
)

// EncryptedObject names immutable ciphertext. Plaintext hashes are purposely
// absent from the peer lease protocol; a replica receives no decryption key.
type EncryptedObject struct {
	ObjectID         string `json:"object_id"`
	CiphertextSize   string `json:"ciphertext_size"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
}

type Offer struct {
	Version               string            `json:"lease_version"`
	LeaseID               string            `json:"lease_id"`
	ArchiveID             string            `json:"archive_id"`
	SnapshotID            string            `json:"snapshot_id"`
	KeyEpoch              string            `json:"key_epoch"`
	PlaintextManifestHash string            `json:"plaintext_manifest_hash"`
	ReplicaManifestHash   string            `json:"replica_manifest_hash"`
	EncryptedObjectSet    []EncryptedObject `json:"encrypted_object_set"`
	ProviderNodeID        string            `json:"provider_node_id"`
	OwnerSubjectID        string            `json:"owner_subject_id"`
	AuthorizedEvidence    string            `json:"authorized_evidence"`
	ReservedBytes         string            `json:"reserved_bytes"`
	IssuedAt              string            `json:"issued_at"`
	ExpiresAt             string            `json:"expires_at"`
	RenewalPolicy         string            `json:"renewal_policy"`
}

type SignedOffer struct {
	Offer      Offer                   `json:"offer"`
	Actor      authority.Principal     `json:"actor"`
	GrantChain []authority.SignedGrant `json:"grant_chain"`
	Signature  string                  `json:"signature"`
}

// PendingOffer is a bounded, non-secret inbox projection for the Node named
// by an unaccepted offer. It intentionally omits signatures, capability
// chains, encrypted object identifiers, and all key material; the receiver
// must still fetch and validate the original signed offer before accepting it.
type PendingOffer struct {
	LeaseID       string `json:"lease_id"`
	ArchiveID     string `json:"archive_id"`
	SnapshotID    string `json:"snapshot_id"`
	OwnerSubject  string `json:"owner_subject_id"`
	OfferHash     string `json:"offer_hash"`
	KeyEpoch      string `json:"key_epoch"`
	ExpiresAt     string `json:"expires_at"`
	ReservedBytes string `json:"reserved_bytes"`
	ObjectCount   int    `json:"object_count"`
}

type Acceptance struct {
	Version        string `json:"receipt_version"`
	LeaseID        string `json:"lease_id"`
	OfferHash      string `json:"offer_hash"`
	ProviderNodeID string `json:"provider_node_id"`
	ReservedBytes  string `json:"reserved_bytes"`
	AcceptedAt     string `json:"accepted_at"`
}

type SignedAcceptance struct {
	Acceptance Acceptance `json:"acceptance"`
	Signature  string     `json:"signature"`
}

// Checkpoint advances only after the provider has durably committed the stated
// ciphertext prefix. The final full-object readback is still mandatory.
type Checkpoint struct {
	Version         string `json:"receipt_version"`
	LeaseID         string `json:"lease_id"`
	OfferHash       string `json:"offer_hash"`
	ProviderNodeID  string `json:"provider_node_id"`
	ObjectID        string `json:"object_id"`
	CommittedOffset string `json:"committed_offset"`
	RecordedAt      string `json:"recorded_at"`
}

type SignedCheckpoint struct {
	Checkpoint Checkpoint `json:"checkpoint"`
	Signature  string     `json:"signature"`
}

type Renewal struct {
	Version            string `json:"renewal_version"`
	LeaseID            string `json:"lease_id"`
	OfferHash          string `json:"offer_hash"`
	OwnerSubjectID     string `json:"owner_subject_id"`
	AuthorizedEvidence string `json:"authorized_evidence"`
	ExpiresAt          string `json:"expires_at"`
	IssuedAt           string `json:"issued_at"`
}

type SignedRenewal struct {
	Renewal           Renewal                 `json:"renewal"`
	Actor             authority.Principal     `json:"actor"`
	GrantChain        []authority.SignedGrant `json:"grant_chain"`
	OwnerSignature    string                  `json:"owner_signature"`
	ProviderSignature string                  `json:"provider_signature"`
}

// SignedRenewalRequest is the durable owner-authorized request awaiting the
// provider's independent receipt signature.
type SignedRenewalRequest struct {
	Renewal        Renewal                 `json:"renewal"`
	Actor          authority.Principal     `json:"actor"`
	GrantChain     []authority.SignedGrant `json:"grant_chain"`
	OwnerSignature string                  `json:"owner_signature"`
}

type Tombstone struct {
	Version            string `json:"tombstone_version"`
	ArchiveID          string `json:"archive_id"`
	OwnerSubjectID     string `json:"owner_subject_id"`
	DeletionEpoch      string `json:"deletion_epoch"`
	PreviousHead       string `json:"previous_head"`
	ReasonCode         string `json:"reason_code"`
	IssuedAt           string `json:"issued_at"`
	AuthorizedEvidence string `json:"authorized_evidence"`
}

type SignedTombstone struct {
	Tombstone  Tombstone               `json:"tombstone"`
	Actor      authority.Principal     `json:"actor"`
	GrantChain []authority.SignedGrant `json:"grant_chain"`
	Signature  string                  `json:"signature"`
}

type DeletionReport struct {
	Version        string `json:"report_version"`
	LeaseID        string `json:"lease_id"`
	OfferHash      string `json:"offer_hash"`
	ProviderNodeID string `json:"provider_node_id"`
	Status         string `json:"status"` // deleted or unconfirmed; never proof of physical erasure
	ReportedAt     string `json:"reported_at"`
	TombstoneHash  string `json:"tombstone_hash,omitempty"`
}

type SignedDeletionReport struct {
	Report    DeletionReport `json:"report"`
	Signature string         `json:"signature"`
}

type Progress struct {
	ObjectID        string `json:"object_id"`
	CommittedOffset string `json:"committed_offset"`
	UpdatedAt       string `json:"updated_at"`
}

type Lease struct {
	Offer                  SignedOffer           `json:"offer"`
	OfferHash              string                `json:"offer_hash"`
	Acceptance             SignedAcceptance      `json:"acceptance"`
	PendingRenewal         *SignedRenewalRequest `json:"pending_renewal,omitempty"`
	Status                 Status                `json:"status"`
	Progress               []Progress            `json:"progress"`
	Checkpoints            []SignedCheckpoint    `json:"checkpoints"`
	Renewals               []SignedRenewal       `json:"renewals"`
	DeletionReport         *SignedDeletionReport `json:"deletion_report,omitempty"`
	TerminationRequestedAt string                `json:"termination_requested_at,omitempty"`
	FailureCode            string                `json:"failure_code,omitempty"`
	FailureAt              string                `json:"failure_at,omitempty"`
	Revision               string                `json:"revision"`
}

type TombstoneRecord struct {
	Signed       SignedTombstone     `json:"signed"`
	Hash         string              `json:"hash"`
	Frozen       bool                `json:"frozen"`
	ForkHashes   []string            `json:"fork_hashes,omitempty"`
	ForkEvidence []SignedTombstone   `json:"fork_evidence,omitempty"`
	Deliveries   []TombstoneDelivery `json:"deliveries,omitempty"`
}

// TombstoneDelivery records a peer receipt accepted over an authenticated
// session for this exact immutable tombstone. It is an operational checkpoint,
// not proof that the peer physically erased any media.
type TombstoneDelivery struct {
	NodeID         string `json:"node_id"`
	TombstoneHash  string `json:"tombstone_hash"`
	AcknowledgedAt string `json:"acknowledged_at"`
}

// TombstoneDeliveryCursor is bounded scheduling metadata for retry fairness.
// It is not an acknowledgement and never changes deletion authority.
type TombstoneDeliveryCursor struct {
	TombstoneHash string `json:"tombstone_hash"`
	NodeID        string `json:"node_id"`
}

type Authorizer interface {
	Authorize(context.Context, string, string, string, string, authority.Principal, []authority.SignedGrant, time.Time) error
}

type KeyResolver interface {
	// VerifiedKey resolves a key against validated history at the signed event
	// time. Current authorization and revocation checks happen independently.
	VerifiedKey(context.Context, authority.Principal, time.Time) (ed25519.PublicKey, error)
}

// ObjectReader opens the exact provider-side ciphertext object for readback.
type ObjectReader interface {
	OpenCiphertext(context.Context, string, string, string) (io.ReadCloser, error)
}

type Options struct {
	Root                  string
	Authorizer            Authorizer
	Keys                  KeyResolver
	Now                   func() time.Time
	ProviderCapacityBytes uint64
	LocalProviderNodeID   string
}

type snapshot struct {
	Version        string                   `json:"state_version"`
	Leases         []Lease                  `json:"leases"`
	Tombstones     []TombstoneRecord        `json:"tombstones"`
	DeliveryCursor *TombstoneDeliveryCursor `json:"tombstone_delivery_cursor,omitempty"`
	Migrated       bool                     `json:"-"`
}

type snapshotV1 struct {
	Version    string            `json:"state_version"`
	Leases     []Lease           `json:"leases"`
	Tombstones []TombstoneRecord `json:"tombstones"`
}

func (o Offer) canonical() ([]byte, error) {
	if err := validateOffer(o); err != nil {
		return nil, err
	}
	b, err := json.Marshal(o)
	if err != nil {
		return nil, ErrInvalid
	}
	return identityprotocol.Canonicalize(b)
}

func CanonicalOffer(o Offer) ([]byte, error) { return o.canonical() }

func SignOffer(o Offer, actor authority.Principal, chain []authority.SignedGrant, key ed25519.PrivateKey) (SignedOffer, error) {
	if len(key) != ed25519.PrivateKeySize || !actorValid(actor) {
		return SignedOffer{}, ErrInvalid
	}
	evidence, err := grantEvidenceHash(chain)
	if err != nil {
		return SignedOffer{}, err
	}
	if o.AuthorizedEvidence != "" && o.AuthorizedEvidence != evidence {
		return SignedOffer{}, ErrDenied
	}
	o.AuthorizedEvidence = evidence
	b, err := o.canonical()
	if err != nil {
		return SignedOffer{}, err
	}
	return SignedOffer{Offer: cloneOffer(o), Actor: actor, GrantChain: cloneChain(chain), Signature: encodeSignature(ed25519.Sign(key, append([]byte(offerDomain), b...)))}, nil
}

func CanonicalAcceptance(a Acceptance) ([]byte, error) {
	if a.Version != ProtocolVersion || !leaseIDPattern.MatchString(a.LeaseID) || !hashPattern.MatchString(a.OfferHash) || !nodeIDPattern.MatchString(a.ProviderNodeID) || !decimal(a.ReservedBytes) || parseTime(a.AcceptedAt) == (time.Time{}) {
		return nil, ErrInvalid
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, ErrInvalid
	}
	return identityprotocol.Canonicalize(b)
}

func SignAcceptance(a Acceptance, key ed25519.PrivateKey) (SignedAcceptance, error) {
	if len(key) != ed25519.PrivateKeySize {
		return SignedAcceptance{}, ErrInvalid
	}
	b, err := CanonicalAcceptance(a)
	if err != nil {
		return SignedAcceptance{}, err
	}
	return SignedAcceptance{Acceptance: a, Signature: encodeSignature(ed25519.Sign(key, append([]byte(acceptDomain), b...)))}, nil
}

func CanonicalCheckpoint(c Checkpoint) ([]byte, error) {
	if c.Version != ProtocolVersion || !leaseIDPattern.MatchString(c.LeaseID) || !hashPattern.MatchString(c.OfferHash) || !nodeIDPattern.MatchString(c.ProviderNodeID) || !objectIDPattern.MatchString(c.ObjectID) || !decimal(c.CommittedOffset) || parseTime(c.RecordedAt) == (time.Time{}) {
		return nil, ErrInvalid
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, ErrInvalid
	}
	return identityprotocol.Canonicalize(b)
}

func SignCheckpoint(c Checkpoint, key ed25519.PrivateKey) (SignedCheckpoint, error) {
	if len(key) != ed25519.PrivateKeySize {
		return SignedCheckpoint{}, ErrInvalid
	}
	b, err := CanonicalCheckpoint(c)
	if err != nil {
		return SignedCheckpoint{}, err
	}
	return SignedCheckpoint{Checkpoint: c, Signature: encodeSignature(ed25519.Sign(key, append([]byte(checkpointDomain), b...)))}, nil
}

func CanonicalTombstone(t Tombstone) ([]byte, error) {
	if t.Version != TombstoneVersion || !archiveIDPattern.MatchString(t.ArchiveID) || !ownerSubjectPattern.MatchString(t.OwnerSubjectID) || !positiveDecimal(t.DeletionEpoch) || (t.DeletionEpoch == "1" && t.PreviousHead != "") || (t.DeletionEpoch != "1" && !hashPattern.MatchString(t.PreviousHead)) || !validReason(t.ReasonCode) || parseTime(t.IssuedAt) == (time.Time{}) || !hashPattern.MatchString(t.AuthorizedEvidence) {
		return nil, ErrInvalid
	}
	b, err := json.Marshal(t)
	if err != nil {
		return nil, ErrInvalid
	}
	return identityprotocol.Canonicalize(b)
}

func SignTombstone(t Tombstone, ownerSubjectID string, actor authority.Principal, chain []authority.SignedGrant, key ed25519.PrivateKey) (SignedTombstone, error) {
	if len(key) != ed25519.PrivateKeySize || !ownerSubjectPattern.MatchString(ownerSubjectID) || !actorValid(actor) {
		return SignedTombstone{}, ErrInvalid
	}
	if t.OwnerSubjectID != "" && t.OwnerSubjectID != ownerSubjectID {
		return SignedTombstone{}, ErrDenied
	}
	t.OwnerSubjectID = ownerSubjectID
	evidence, err := grantEvidenceHash(chain)
	if err != nil || (t.AuthorizedEvidence != "" && evidence != t.AuthorizedEvidence) {
		return SignedTombstone{}, ErrDenied
	}
	t.AuthorizedEvidence = evidence
	b, err := CanonicalTombstone(t)
	if err != nil {
		return SignedTombstone{}, err
	}
	return SignedTombstone{Tombstone: t, Actor: actor, GrantChain: cloneChain(chain), Signature: encodeSignature(ed25519.Sign(key, append([]byte(tombstoneDomain), b...)))}, nil
}

func CanonicalRenewal(r Renewal) ([]byte, error) {
	if r.Version != ProtocolVersion || !leaseIDPattern.MatchString(r.LeaseID) || !hashPattern.MatchString(r.OfferHash) || !ownerSubjectPattern.MatchString(r.OwnerSubjectID) || !hashPattern.MatchString(r.AuthorizedEvidence) || parseTime(r.IssuedAt) == (time.Time{}) || parseTime(r.ExpiresAt) == (time.Time{}) {
		return nil, ErrInvalid
	}
	if !parseTime(r.IssuedAt).Before(parseTime(r.ExpiresAt)) {
		return nil, ErrInvalid
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, ErrInvalid
	}
	return identityprotocol.Canonicalize(b)
}

func SignRenewal(r Renewal, actor authority.Principal, chain []authority.SignedGrant, ownerKey, providerKey ed25519.PrivateKey) (SignedRenewal, error) {
	request, err := SignRenewalRequest(r, actor, chain, ownerKey)
	if err != nil {
		return SignedRenewal{}, err
	}
	return SignRenewalReceipt(request, providerKey)
}

// SignRenewalReceipt attaches the provider's independent Node-Key receipt to
// an owner-signed renewal request that has already crossed the provider Store's
// current-authority validation boundary.
func SignRenewalReceipt(request SignedRenewalRequest, providerKey ed25519.PrivateKey) (SignedRenewal, error) {
	if len(providerKey) != ed25519.PrivateKeySize || !actorValid(request.Actor) || request.OwnerSignature == "" {
		return SignedRenewal{}, ErrInvalid
	}
	evidence, err := grantEvidenceHash(request.GrantChain)
	if err != nil || evidence != request.Renewal.AuthorizedEvidence {
		return SignedRenewal{}, ErrDenied
	}
	b, err := CanonicalRenewal(request.Renewal)
	if err != nil {
		return SignedRenewal{}, err
	}
	message := append([]byte(renewDomain), b...)
	return SignedRenewal{
		Renewal: request.Renewal, Actor: request.Actor, GrantChain: cloneChain(request.GrantChain),
		OwnerSignature: request.OwnerSignature, ProviderSignature: encodeSignature(ed25519.Sign(providerKey, message)),
	}, nil
}

func SignRenewalRequest(r Renewal, actor authority.Principal, chain []authority.SignedGrant, ownerKey ed25519.PrivateKey) (SignedRenewalRequest, error) {
	if len(ownerKey) != ed25519.PrivateKeySize || !actorValid(actor) {
		return SignedRenewalRequest{}, ErrInvalid
	}
	evidence, err := grantEvidenceHash(chain)
	if err != nil {
		return SignedRenewalRequest{}, err
	}
	if r.AuthorizedEvidence != "" && r.AuthorizedEvidence != evidence {
		return SignedRenewalRequest{}, ErrDenied
	}
	r.AuthorizedEvidence = evidence
	b, err := CanonicalRenewal(r)
	if err != nil {
		return SignedRenewalRequest{}, err
	}
	return SignedRenewalRequest{Renewal: r, Actor: actor, GrantChain: cloneChain(chain), OwnerSignature: encodeSignature(ed25519.Sign(ownerKey, append([]byte(renewDomain), b...)))}, nil
}

func CanonicalDeletionReport(r DeletionReport) ([]byte, error) {
	if r.Version != ProtocolVersion || !leaseIDPattern.MatchString(r.LeaseID) || !hashPattern.MatchString(r.OfferHash) || !nodeIDPattern.MatchString(r.ProviderNodeID) || (r.Status != string(StatusDeleted) && r.Status != string(StatusUnconfirmed)) || parseTime(r.ReportedAt) == (time.Time{}) || (r.TombstoneHash != "" && !hashPattern.MatchString(r.TombstoneHash)) {
		return nil, ErrInvalid
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, ErrInvalid
	}
	return identityprotocol.Canonicalize(b)
}

func SignDeletionReport(r DeletionReport, key ed25519.PrivateKey) (SignedDeletionReport, error) {
	if len(key) != ed25519.PrivateKeySize {
		return SignedDeletionReport{}, ErrInvalid
	}
	b, err := CanonicalDeletionReport(r)
	if err != nil {
		return SignedDeletionReport{}, err
	}
	return SignedDeletionReport{Report: r, Signature: encodeSignature(ed25519.Sign(key, append([]byte(deletionReportDomain), b...)))}, nil
}

func validateOffer(o Offer) error {
	if o.Version != ProtocolVersion || !leaseIDPattern.MatchString(o.LeaseID) || !archiveIDPattern.MatchString(o.ArchiveID) || !snapshotIDPattern.MatchString(o.SnapshotID) || !positiveDecimal(o.KeyEpoch) || !hashPattern.MatchString(o.PlaintextManifestHash) || !hashPattern.MatchString(o.ReplicaManifestHash) || !nodeIDPattern.MatchString(o.ProviderNodeID) || !ownerSubjectPattern.MatchString(o.OwnerSubjectID) || !hashPattern.MatchString(o.AuthorizedEvidence) || !positiveDecimal(o.ReservedBytes) {
		return ErrInvalid
	}
	issued, expires := parseTime(o.IssuedAt), parseTime(o.ExpiresAt)
	if issued.IsZero() || expires.IsZero() || !expires.After(issued) || expires.Sub(issued) > MaxLeaseDuration {
		return ErrInvalid
	}
	if o.RenewalPolicy != "owner_approval" && o.RenewalPolicy != "manual" {
		return ErrInvalid
	}
	if len(o.EncryptedObjectSet) == 0 || len(o.EncryptedObjectSet) > MaxObjects {
		return ErrInvalid
	}
	var total uint64
	last := ""
	for _, object := range o.EncryptedObjectSet {
		if !objectIDPattern.MatchString(object.ObjectID) || object.ObjectID <= last || !positiveDecimal(object.CiphertextSize) || !hashPattern.MatchString(object.CiphertextSHA256) {
			return ErrInvalid
		}
		size, err := strconv.ParseUint(object.CiphertextSize, 10, 64)
		if err != nil || size > MaxReservedBytes-total {
			return ErrInvalid
		}
		total += size
		last = object.ObjectID
	}
	reserved, err := strconv.ParseUint(o.ReservedBytes, 10, 64)
	if err != nil || reserved != total || reserved > MaxReservedBytes {
		return ErrInvalid
	}
	return nil
}

func OfferHash(o Offer) (string, error) {
	b, err := o.canonical()
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(append([]byte("IR/replica-lease/v1/hash\x00"), b...))
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil || t.Location() != time.UTC || t.Format(time.RFC3339) != s {
		return time.Time{}
	}
	return t
}

func decimal(s string) bool         { return decimalPattern.MatchString(s) }
func positiveDecimal(s string) bool { return decimal(s) && s != "0" }
func validReason(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func encodeSignature(b []byte) string { return strings.TrimRight(base64URL(b), "=") }
func base64URL(b []byte) string       { return base64.RawURLEncoding.EncodeToString(b) }

func verifySignature(public ed25519.PublicKey, signature, domain string, canonical []byte) error {
	sig, err := ed25519Signature(signature)
	if err != nil || len(public) != ed25519.PublicKeySize || !ed25519.Verify(public, append([]byte(domain), canonical...), sig) {
		return ErrDenied
	}
	return nil
}

func ed25519Signature(value string) ([]byte, error) {
	if value == "" || strings.Contains(value, "=") {
		return nil, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(raw) != value {
		return nil, ErrInvalid
	}
	return raw, nil
}

func cloneOffer(o Offer) Offer {
	o.EncryptedObjectSet = append([]EncryptedObject(nil), o.EncryptedObjectSet...)
	return o
}

func cloneCheckpoint(v SignedCheckpoint) SignedCheckpoint { return v }
func cloneChain(in []authority.SignedGrant) []authority.SignedGrant {
	out := make([]authority.SignedGrant, len(in))
	for i, g := range in {
		out[i] = g
		out[i].Signature = append([]byte(nil), g.Signature...)
		out[i].Grant.Actions = append([]string(nil), g.Grant.Actions...)
	}
	return out
}
func cloneLease(l Lease) Lease {
	l.Offer.Offer = cloneOffer(l.Offer.Offer)
	l.Offer.GrantChain = cloneChain(l.Offer.GrantChain)
	l.PendingRenewal = cloneRenewalRequest(l.PendingRenewal)
	l.Progress = append([]Progress(nil), l.Progress...)
	l.Checkpoints = append([]SignedCheckpoint(nil), l.Checkpoints...)
	l.Renewals = append([]SignedRenewal(nil), l.Renewals...)
	for i := range l.Renewals {
		l.Renewals[i] = cloneRenewal(l.Renewals[i])
	}
	if l.DeletionReport != nil {
		report := *l.DeletionReport
		l.DeletionReport = &report
	}
	return l
}

func cloneRenewalRequest(v *SignedRenewalRequest) *SignedRenewalRequest {
	if v == nil {
		return nil
	}
	copy := *v
	copy.GrantChain = cloneChain(v.GrantChain)
	return &copy
}

func strictDecode(data []byte, dst any) error {
	if len(data) == 0 || len(data) > maxStateBytes {
		return ErrInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(dst) != nil || dec.Decode(&struct{}{}) != io.EOF {
		return ErrInvalid
	}
	return nil
}

func sortedLeases(leas []Lease) {
	sort.Slice(leas, func(i, j int) bool { return leas[i].Offer.Offer.LeaseID < leas[j].Offer.Offer.LeaseID })
}
func nextRevision(s string) (string, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == ^uint64(0) {
		return "", ErrInvalid
	}
	return strconv.FormatUint(n+1, 10), nil
}

func canonicalRecord(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, ErrInvalid
	}
	out, err := identityprotocol.Canonicalize(b)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical record", ErrInvalid)
	}
	return out, nil
}
