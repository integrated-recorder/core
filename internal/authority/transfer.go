package authority

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	transferVersion      = "ir.owner-subject.transfer/1"
	maxTransferWindow    = 24 * time.Hour
	transferOfferDomain  = "IR/owner-subject-transfer/v1/offer\x00"
	transferAcceptDomain = "IR/owner-subject-transfer/v1/accept\x00"
)

// TransferOffer requires explicit consent from the current controller and
// the receiving Global Identity. A successful signature pair changes the
// subject controller and epoch, but leaves all archive DEKs in a separate
// rekey-required state until new envelopes are durably verified.
type TransferOffer struct {
	Version                   string    `json:"transfer_version"`
	TransferID                string    `json:"transfer_id"`
	SubjectID                 string    `json:"subject_id"`
	From                      Principal `json:"from"`
	To                        Principal `json:"to"`
	OldPolicyEpoch            string    `json:"old_policy_epoch"`
	NewPolicyEpoch            string    `json:"new_policy_epoch"`
	OldIdentityHeadHash       string    `json:"old_identity_head_hash"`
	RecipientIdentityHeadHash string    `json:"recipient_identity_head_hash"`
	Nonce                     string    `json:"nonce"`
	IssuedAt                  string    `json:"issued_at"`
	ExpiresAt                 string    `json:"expires_at"`
}

type SignedTransferOffer struct {
	Offer     TransferOffer `json:"offer"`
	Signature []byte        `json:"signature"`
}

type AcceptedTransfer struct {
	Offer                  TransferOffer `json:"offer"`
	CurrentControllerProof string        `json:"current_controller_proof"`
	RecipientProof         string        `json:"recipient_proof"`
}

type TransferResult struct {
	Subject                OwnerSubject `json:"subject"`
	State                  string       `json:"state"`
	InvalidatedPolicyEpoch string       `json:"invalidated_policy_epoch"`
	RekeyRequired          bool         `json:"rekey_required"`
}

// NewTransferOffer binds both parties' current identity heads and requires an
// expiry no more than 24 hours in the future.
func NewTransferOffer(ctx VerificationContext, recipient Principal, expiresAt time.Time, entropy io.Reader) (TransferOffer, error) {
	if err := validateContext(ctx); err != nil {
		return TransferOffer{}, err
	}
	if recipient.Kind != PrincipalIdentity || !recipient.valid() || recipient.ID == ctx.Identity.IdentityID {
		return TransferOffer{}, ErrInvalid
	}
	if !expiresAt.After(ctx.Now) || expiresAt.Sub(ctx.Now) > maxTransferWindow {
		return TransferOffer{}, ErrInvalid
	}
	if entropy == nil {
		entropy = rand.Reader
	}
	transferRaw, err := randomBytes(entropy, 16)
	if err != nil {
		return TransferOffer{}, err
	}
	nonceRaw, err := randomBytes(entropy, 32)
	if err != nil {
		return TransferOffer{}, err
	}
	nextEpoch, err := nextDecimal(ctx.Subject.PolicyEpoch)
	if err != nil {
		return TransferOffer{}, err
	}
	return TransferOffer{
		Version:    transferVersion,
		TransferID: "tr_" + base64.RawURLEncoding.EncodeToString(transferRaw),
		SubjectID:  ctx.Subject.ID, From: ctx.Subject.Controller, To: recipient,
		OldPolicyEpoch: ctx.Subject.PolicyEpoch, NewPolicyEpoch: nextEpoch,
		OldIdentityHeadHash:       ctx.Identity.HeadHash,
		RecipientIdentityHeadHash: "", // Filled from the recipient's verified state by BindRecipientHead.
		Nonce:                     base64.RawURLEncoding.EncodeToString(nonceRaw),
		IssuedAt:                  ctx.Now.UTC().Format(time.RFC3339), ExpiresAt: expiresAt.UTC().Format(time.RFC3339),
	}, nil
}

// BindRecipientHead completes the offer with the current, fresh recipient
// identity head. The returned value must be signed after binding.
func BindRecipientHead(offer TransferOffer, recipientState IdentityState, now time.Time) (TransferOffer, error) {
	if !offer.To.valid() || offer.To.Kind != PrincipalIdentity || validateIdentityState(recipientState, now) != nil || recipientState.IdentityID != offer.To.ID {
		return TransferOffer{}, ErrDenied
	}
	offer.RecipientIdentityHeadHash = recipientState.HeadHash
	if err := validateTransferOffer(offer); err != nil {
		return TransferOffer{}, err
	}
	return offer, nil
}

func SignTransferOffer(offer TransferOffer, privateKey ed25519.PrivateKey) (SignedTransferOffer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return SignedTransferOffer{}, ErrInvalid
	}
	canonical, err := canonicalTransferOffer(offer)
	if err != nil {
		return SignedTransferOffer{}, err
	}
	return SignedTransferOffer{Offer: offer, Signature: ed25519.Sign(privateKey, append([]byte(transferOfferDomain), canonical...))}, nil
}

// AcceptTransferOffer creates recipient consent over the exact offer. The old
// controller's signature is carried alongside it and independently checked by
// VerifyTransferAcceptance.
func AcceptTransferOffer(signed SignedTransferOffer, recipientPrivateKey ed25519.PrivateKey) (AcceptedTransfer, error) {
	if len(recipientPrivateKey) != ed25519.PrivateKeySize {
		return AcceptedTransfer{}, ErrInvalid
	}
	canonical, err := canonicalTransferOffer(signed.Offer)
	if err != nil || len(signed.Signature) != ed25519.SignatureSize {
		return AcceptedTransfer{}, ErrInvalid
	}
	recipientSignature := ed25519.Sign(recipientPrivateKey, append([]byte(transferAcceptDomain), canonical...))
	return AcceptedTransfer{
		Offer:                  signed.Offer,
		CurrentControllerProof: base64.RawURLEncoding.EncodeToString(signed.Signature),
		RecipientProof:         base64.RawURLEncoding.EncodeToString(recipientSignature),
	}, nil
}

// VerifyTransferAcceptance consumes the source OwnerSubject context and the
// receiving identity's independently verified key projection. Success returns
// a new subject epoch; callers must persist it atomically with the accepted
// transfer and keep archive keys unavailable until rekey completes.
func VerifyTransferAcceptance(ctx VerificationContext, recipientState IdentityState, accepted AcceptedTransfer) (TransferResult, error) {
	if err := validateContext(ctx); err != nil {
		return TransferResult{}, err
	}
	if err := validateIdentityState(recipientState, ctx.Now); err != nil {
		return TransferResult{}, err
	}
	offer := accepted.Offer
	if err := validateTransferOffer(offer); err != nil {
		return TransferResult{}, err
	}
	expectedEpoch, epochErr := nextDecimal(ctx.Subject.PolicyEpoch)
	if epochErr != nil || offer.SubjectID != ctx.Subject.ID || offer.From != ctx.Subject.Controller || offer.To.Kind != PrincipalIdentity || offer.To.ID != recipientState.IdentityID ||
		offer.OldPolicyEpoch != ctx.SubjectPolicyEpoch || offer.NewPolicyEpoch != expectedEpoch ||
		offer.OldIdentityHeadHash != ctx.Identity.HeadHash || offer.RecipientIdentityHeadHash != recipientState.HeadHash {
		return TransferResult{}, ErrStaleIdentity
	}
	if !grantTimeValid(Grant{NotBefore: offer.IssuedAt, ExpiresAt: offer.ExpiresAt}, ctx.Now) {
		return TransferResult{}, ErrDenied
	}
	canonical, err := canonicalTransferOffer(offer)
	if err != nil {
		return TransferResult{}, err
	}
	oldSignature, oldErr := base64.RawURLEncoding.DecodeString(accepted.CurrentControllerProof)
	newSignature, newErr := base64.RawURLEncoding.DecodeString(accepted.RecipientProof)
	oldKey := ctx.Identity.CurrentKeys[offer.From.key()]
	newPrincipal := Principal{Kind: PrincipalIdentity, ID: offer.To.ID}
	newKey := recipientState.CurrentKeys[newPrincipal.key()]
	if oldErr != nil || newErr != nil || len(oldSignature) != ed25519.SignatureSize || len(newSignature) != ed25519.SignatureSize ||
		base64.RawURLEncoding.EncodeToString(oldSignature) != accepted.CurrentControllerProof || base64.RawURLEncoding.EncodeToString(newSignature) != accepted.RecipientProof ||
		!ed25519.Verify(oldKey, append([]byte(transferOfferDomain), canonical...), oldSignature) || !ed25519.Verify(newKey, append([]byte(transferAcceptDomain), canonical...), newSignature) {
		return TransferResult{}, ErrInvalid
	}
	next := ctx.Subject
	next.Controller = newPrincipal
	next.PolicyEpoch = offer.NewPolicyEpoch
	return TransferResult{Subject: next, State: "rekey_required", InvalidatedPolicyEpoch: offer.OldPolicyEpoch, RekeyRequired: true}, nil
}

func canonicalTransferOffer(offer TransferOffer) ([]byte, error) {
	if err := validateTransferOffer(offer); err != nil {
		return nil, err
	}
	data, err := json.Marshal(offer)
	if err != nil {
		return nil, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil {
		return nil, ErrInvalid
	}
	return canonical, nil
}

func validateTransferOffer(offer TransferOffer) error {
	if offer.Version != transferVersion || !transferIDPattern.MatchString(offer.TransferID) || !validRawURL32(offer.Nonce) ||
		!subjectIDPattern.MatchString(offer.SubjectID) || offer.From.Kind != PrincipalIdentity || !offer.From.valid() || offer.To.Kind != PrincipalIdentity || !offer.To.valid() || offer.From == offer.To ||
		!validDecimal(offer.OldPolicyEpoch) || !validDecimal(offer.NewPolicyEpoch) || offer.NewPolicyEpoch != mustNextDecimal(offer.OldPolicyEpoch) ||
		!identityprotocolHash(offer.OldIdentityHeadHash) || !identityprotocolHash(offer.RecipientIdentityHeadHash) {
		return ErrInvalid
	}
	issuedAt, issuedErr := parseTime(offer.IssuedAt)
	expiresAt, expiresErr := parseTime(offer.ExpiresAt)
	if issuedErr != nil || expiresErr != nil || !issuedAt.Before(expiresAt) || expiresAt.Sub(issuedAt) > maxTransferWindow {
		return ErrInvalid
	}
	return nil
}

func validateIdentityState(state IdentityState, now time.Time) error {
	if state.Frozen {
		return ErrFrozenIdentity
	}
	if !identityIDPattern.MatchString(state.IdentityID) || !identityprotocolHash(state.HeadHash) || now.IsZero() || state.VerifiedAt.IsZero() || state.FreshUntil.IsZero() || now.Before(state.VerifiedAt) || now.Sub(state.VerifiedAt) > maxIdentityFreshness || !state.FreshUntil.After(now) || state.FreshUntil.After(state.VerifiedAt.Add(maxIdentityFreshness)) {
		return ErrStaleIdentity
	}
	root := Principal{Kind: PrincipalIdentity, ID: state.IdentityID}
	rootKey := state.CurrentKeys[root.key()]
	derived, err := identityprotocol.IdentityID(rootKey)
	if err != nil || derived != state.IdentityID {
		return ErrDenied
	}
	for key, public := range state.CurrentKeys {
		if len(public) != ed25519.PublicKeySize {
			return ErrDenied
		}
		kind, id, found := strings.Cut(key, ":")
		if !found {
			return ErrDenied
		}
		switch kind {
		case PrincipalIdentity:
			if id != state.IdentityID || key != root.key() {
				return ErrDenied
			}
		case PrincipalNode:
			nodeID, nodeErr := identityprotocol.NodeID(public)
			if nodeErr != nil || nodeID != id {
				return ErrDenied
			}
		default:
			return ErrDenied
		}
	}
	return nil
}

// ValidateIdentityState applies the same freshness, fork, and authorized-key
// checks used by OwnerSubject verification to a state assembled by a trusted
// local identity verifier. Callers must still establish where the state came
// from; this function does not fetch or independently prove remote freshness.
func ValidateIdentityState(state IdentityState, now time.Time) error {
	return validateIdentityState(state, now)
}

func randomBytes(source io.Reader, size int) ([]byte, error) {
	if size <= 0 || source == nil {
		return nil, ErrInvalid
	}
	out := make([]byte, size)
	if _, err := io.ReadFull(source, out); err != nil {
		return nil, fmt.Errorf("%w: transfer nonce generation", ErrInvalid)
	}
	return out, nil
}

func validRawURL32(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func nextDecimal(value string) (string, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed >= 999_999_999 {
		return "", ErrInvalid
	}
	return strconv.FormatUint(parsed+1, 10), nil
}

func mustNextDecimal(value string) string {
	next, err := nextDecimal(value)
	if err != nil {
		return ""
	}
	return next
}
