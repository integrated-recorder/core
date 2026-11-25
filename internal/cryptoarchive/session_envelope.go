package cryptoarchive

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hpke"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const SessionDEKEnvelopeType = "ir.session-dek-envelope/1"

var (
	ErrSessionEnvelopeInvalid      = errors.New("session archive key envelope is invalid")
	ErrSessionEnvelopeUnauthorized = errors.New("session archive key envelope binding is unauthorized")
	sessionSnapshotIDPattern       = regexp.MustCompile(`^snap_[0-9a-f]{64}$`)
	sessionOwnerSubjectPattern     = regexp.MustCompile(`^os_[A-Za-z0-9_-]{22}$`)
)

// SessionDEKBinding scopes a short-lived DEK delivery to one already
// authorized peer response. The envelope is intentionally not a durable key
// grant: the peer session authenticates SupplierNodeID, while the caller must
// revalidate the exact OwnerSubject capability before requesting it.
type SessionDEKBinding struct {
	ArchiveID      string
	SnapshotID     string
	KeyEpoch       uint64
	OwnerSubjectID string
	GrantHash      string
	SupplierNodeID string
}

type SessionDEKEnvelope struct {
	Version                  string `json:"envelope_version"`
	ArchiveID                string `json:"archive_id"`
	SnapshotID               string `json:"snapshot_id"`
	KeyEpoch                 string `json:"key_epoch"`
	OwnerSubjectID           string `json:"owner_subject_id"`
	GrantHash                string `json:"grant_hash"`
	SupplierNodeID           string `json:"supplier_node_id"`
	RecipientKeyID           string `json:"recipient_key_id"`
	RecipientPublicKeySHA256 string `json:"recipient_public_key_sha256"`
	KEM                      string `json:"kem"`
	KDF                      string `json:"kdf"`
	AEAD                     string `json:"aead"`
	Encapsulation            string `json:"encapsulation"`
	Ciphertext               string `json:"ciphertext"`
}

type sessionDEKContext struct {
	Version        string `json:"envelope_version"`
	ArchiveID      string `json:"archive_id"`
	SnapshotID     string `json:"snapshot_id"`
	KeyEpoch       string `json:"key_epoch"`
	OwnerSubjectID string `json:"owner_subject_id"`
	GrantHash      string `json:"grant_hash"`
	SupplierNodeID string `json:"supplier_node_id"`
	RecipientKeyID string `json:"recipient_key_id"`
	RecipientHash  string `json:"recipient_public_key_sha256"`
}

// SealSessionDEK creates a one-recipient HPKE envelope for a browser key held
// only in memory. It does not sign the envelope: authenticity is provided by
// the authenticated Node-Key peer session, and authority by the current
// OwnerSubject capability checked before the operation. The context is
// authenticated as both HPKE info and AEAD associated data.
func SealSessionDEK(dataKey, recipientPublicKey []byte, binding SessionDEKBinding) (SessionDEKEnvelope, error) {
	if len(dataKey) != keyBytes || len(recipientPublicKey) != 32 || !validSessionBinding(binding) {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	publicKey, err := hpke.DHKEM(ecdh.X25519()).NewPublicKey(recipientPublicKey)
	if err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	context := sessionDEKContext{
		Version: SessionDEKEnvelopeType, ArchiveID: binding.ArchiveID, SnapshotID: binding.SnapshotID,
		KeyEpoch: strconv.FormatUint(binding.KeyEpoch, 10), OwnerSubjectID: binding.OwnerSubjectID,
		GrantHash: binding.GrantHash, SupplierNodeID: binding.SupplierNodeID,
		RecipientKeyID: RecipientKeyID(recipientPublicKey), RecipientHash: RecipientPublicKeyHash(recipientPublicKey),
	}
	contextBytes, err := canonicalStruct(context)
	if err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	info := append([]byte("IR/session-dek-envelope/v1/hpke-info\x00"), contextBytes...)
	aad := append([]byte("IR/session-dek-envelope/v1/hpke-aad\x00"), contextBytes...)
	enc, sender, err := hpke.NewSender(publicKey, hpke.HKDFSHA256(), hpke.AES256GCM(), info)
	if err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	ciphertext, err := sender.Seal(aad, dataKey)
	if err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	envelope := SessionDEKEnvelope{
		Version: context.Version, ArchiveID: context.ArchiveID, SnapshotID: context.SnapshotID,
		KeyEpoch: context.KeyEpoch, OwnerSubjectID: context.OwnerSubjectID, GrantHash: context.GrantHash,
		SupplierNodeID: context.SupplierNodeID, RecipientKeyID: context.RecipientKeyID,
		RecipientPublicKeySHA256: context.RecipientHash, KEM: kemID, KDF: kdfID, AEAD: aeadID,
		Encapsulation: base64.RawURLEncoding.EncodeToString(enc), Ciphertext: base64.RawURLEncoding.EncodeToString(ciphertext),
	}
	if err := validateSessionDEKEnvelope(envelope); err != nil {
		return SessionDEKEnvelope{}, err
	}
	return envelope, nil
}

// OpenSessionDEK checks every expected authority/snapshot/recipient binding
// before decrypting. The caller must clear the returned key after use.
func OpenSessionDEK(envelope SessionDEKEnvelope, recipientPrivateKey []byte, expected SessionDEKBinding) ([]byte, error) {
	if err := validateSessionDEKEnvelope(envelope); err != nil || len(recipientPrivateKey) != 32 || !validSessionBinding(expected) {
		return nil, ErrSessionEnvelopeInvalid
	}
	if envelope.ArchiveID != expected.ArchiveID || envelope.SnapshotID != expected.SnapshotID ||
		envelope.KeyEpoch != strconv.FormatUint(expected.KeyEpoch, 10) || envelope.OwnerSubjectID != expected.OwnerSubjectID ||
		envelope.GrantHash != expected.GrantHash || envelope.SupplierNodeID != expected.SupplierNodeID {
		return nil, ErrSessionEnvelopeUnauthorized
	}
	privateKey, err := ecdh.X25519().NewPrivateKey(recipientPrivateKey)
	if err != nil {
		return nil, ErrSessionEnvelopeUnauthorized
	}
	publicKey := privateKey.PublicKey().Bytes()
	if envelope.RecipientKeyID != RecipientKeyID(publicKey) || envelope.RecipientPublicKeySHA256 != RecipientPublicKeyHash(publicKey) {
		return nil, ErrSessionEnvelopeUnauthorized
	}
	context := sessionDEKContext{
		Version: envelope.Version, ArchiveID: envelope.ArchiveID, SnapshotID: envelope.SnapshotID,
		KeyEpoch: envelope.KeyEpoch, OwnerSubjectID: envelope.OwnerSubjectID, GrantHash: envelope.GrantHash,
		SupplierNodeID: envelope.SupplierNodeID, RecipientKeyID: envelope.RecipientKeyID,
		RecipientHash: envelope.RecipientPublicKeySHA256,
	}
	contextBytes, err := canonicalStruct(context)
	if err != nil {
		return nil, ErrSessionEnvelopeInvalid
	}
	enc, err := base64.RawURLEncoding.DecodeString(envelope.Encapsulation)
	if err != nil || len(enc) != 32 {
		return nil, ErrSessionEnvelopeInvalid
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) != keyBytes+16 {
		return nil, ErrSessionEnvelopeInvalid
	}
	info := append([]byte("IR/session-dek-envelope/v1/hpke-info\x00"), contextBytes...)
	aad := append([]byte("IR/session-dek-envelope/v1/hpke-aad\x00"), contextBytes...)
	recipient, err := hpke.DHKEM(ecdh.X25519()).NewPrivateKey(recipientPrivateKey)
	if err != nil {
		return nil, ErrSessionEnvelopeUnauthorized
	}
	opened, err := hpke.NewRecipient(enc, recipient, hpke.HKDFSHA256(), hpke.AES256GCM(), info)
	if err != nil {
		return nil, ErrSessionEnvelopeInvalid
	}
	plain, err := opened.Open(aad, ciphertext)
	if err != nil || len(plain) != keyBytes {
		clear(plain)
		return nil, ErrSessionEnvelopeInvalid
	}
	return plain, nil
}

func MarshalSessionDEKEnvelope(envelope SessionDEKEnvelope) ([]byte, error) {
	if err := validateSessionDEKEnvelope(envelope); err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil || len(data) > 16<<10 {
		return nil, ErrSessionEnvelopeInvalid
	}
	return identityprotocol.Canonicalize(data)
}

func ParseSessionDEKEnvelope(data []byte) (SessionDEKEnvelope, error) {
	if len(data) == 0 || len(data) > 16<<10 {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	var envelope SessionDEKEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	reencoded, err := json.Marshal(envelope)
	if err != nil {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	reencoded, err = identityprotocol.Canonicalize(reencoded)
	if err != nil || !bytes.Equal(canonical, reencoded) {
		return SessionDEKEnvelope{}, ErrSessionEnvelopeInvalid
	}
	if err := validateSessionDEKEnvelope(envelope); err != nil {
		return SessionDEKEnvelope{}, err
	}
	return envelope, nil
}

func validSessionBinding(binding SessionDEKBinding) bool {
	return archiveIDPattern.MatchString(binding.ArchiveID) && sessionSnapshotIDPattern.MatchString(binding.SnapshotID) &&
		binding.KeyEpoch > 0 && sessionOwnerSubjectPattern.MatchString(binding.OwnerSubjectID) &&
		sha256Pattern.MatchString(binding.GrantHash) && identityprotocol.IsNodeID(binding.SupplierNodeID)
}

func validateSessionDEKEnvelope(envelope SessionDEKEnvelope) error {
	if envelope.Version != SessionDEKEnvelopeType || !archiveIDPattern.MatchString(envelope.ArchiveID) ||
		!sessionSnapshotIDPattern.MatchString(envelope.SnapshotID) || !keyEpochPattern.MatchString(envelope.KeyEpoch) ||
		!sessionOwnerSubjectPattern.MatchString(envelope.OwnerSubjectID) || !sha256Pattern.MatchString(envelope.GrantHash) ||
		!identityprotocol.IsNodeID(envelope.SupplierNodeID) || !recipientKeyIDPattern.MatchString(envelope.RecipientKeyID) ||
		!sha256Pattern.MatchString(envelope.RecipientPublicKeySHA256) || envelope.KEM != kemID || envelope.KDF != kdfID || envelope.AEAD != aeadID ||
		len(envelope.Encapsulation) > 128 || len(envelope.Ciphertext) > 256 {
		return ErrSessionEnvelopeInvalid
	}
	if _, err := strconv.ParseUint(envelope.KeyEpoch, 10, 64); err != nil {
		return ErrSessionEnvelopeInvalid
	}
	enc, err := base64.RawURLEncoding.DecodeString(envelope.Encapsulation)
	if err != nil || len(enc) != 32 || base64.RawURLEncoding.EncodeToString(enc) != envelope.Encapsulation {
		return ErrSessionEnvelopeInvalid
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) != keyBytes+16 || base64.RawURLEncoding.EncodeToString(ciphertext) != envelope.Ciphertext {
		return ErrSessionEnvelopeInvalid
	}
	// The recipient key ID and digest are checked against the actual key before
	// decryption; here ensure the digest syntax is canonical and bounded.
	if _, err := hex.DecodeString(envelope.RecipientPublicKeySHA256[len("sha256:"):]); err != nil {
		return ErrSessionEnvelopeInvalid
	}
	return nil
}
