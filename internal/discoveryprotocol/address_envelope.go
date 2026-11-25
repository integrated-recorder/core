package discoveryprotocol

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	AddressEnvelopeVersion = "ir.node-address-envelope/1"
	addressPayloadMax      = 8 << 10
	addressEncapsulation   = 32
	addressTagBytes        = 16
	addressKEM             = "DHKEM(X25519,HKDF-SHA256)"
	addressKDF             = "HKDF-SHA256"
	addressAEAD            = "AES-256-GCM"
	addressKeyHashDomain   = "IR/node-address-envelope/v1/recipient-key\x00"
	addressInfoDomain      = "IR/node-address-envelope/v1/hpke-info\x00"
	addressAADDomain       = "IR/node-address-envelope/v1/hpke-aad\x00"
)

var (
	addressNodeIDRE = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	addressHashRE   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// AddressContext binds a private connection document to one identity history
// head and one Node Key. The same envelope cannot be replayed for another
// identity, history head, or node.
type AddressContext struct {
	GlobalIdentityHint string
	IdentityHeadHash   string
	NodeID             string
}

// AddressEnvelope is a recipient-encrypted canonical JSON document. The
// outer NodeAdvertisement supplies the Node Key signature; HPKE Base mode
// itself does not authenticate the sender.
type AddressEnvelope struct {
	EnvelopeVersion          string `json:"envelope_version"`
	RecipientPublicKeySHA256 string `json:"recipient_public_key_sha256"`
	KEM                      string `json:"kem"`
	KDF                      string `json:"kdf"`
	AEAD                     string `json:"aead"`
	Encapsulation            string `json:"encapsulation"`
	Ciphertext               string `json:"ciphertext"`
}

type addressHPKEContext struct {
	EnvelopeVersion          string `json:"envelope_version"`
	GlobalIdentityHint       string `json:"global_identity_hint"`
	IdentityHeadHash         string `json:"identity_head_hash"`
	NodeID                   string `json:"node_id"`
	RecipientPublicKeySHA256 string `json:"recipient_public_key_sha256"`
}

var ErrAddressEnvelope = errors.New("node address envelope is invalid")

// SealAddressEnvelope canonicalizes a bounded JSON connection document and
// encrypts it for the Global Identity recovery-encryption key. The recovery
// private key is never needed by this server-side protocol package.
func SealAddressEnvelope(recipientPublicKey []byte, context AddressContext, plaintextJSON []byte) (string, error) {
	if len(recipientPublicKey) != 32 || !validAddressContext(context) || len(plaintextJSON) == 0 || len(plaintextJSON) > addressPayloadMax {
		return "", ErrAddressEnvelope
	}
	canonicalPlaintext, err := identityprotocol.Canonicalize(plaintextJSON)
	if err != nil || len(canonicalPlaintext) == 0 || len(canonicalPlaintext) > addressPayloadMax {
		return "", ErrAddressEnvelope
	}
	defer clear(canonicalPlaintext)
	kem := hpke.DHKEM(ecdh.X25519())
	recipient, err := kem.NewPublicKey(recipientPublicKey)
	if err != nil {
		return "", ErrAddressEnvelope
	}
	keyHash := AddressRecipientPublicKeyHash(recipientPublicKey)
	if !addressHashRE.MatchString(keyHash) {
		return "", ErrAddressEnvelope
	}
	ctxBytes, err := canonicalAddressContext(addressHPKEContext{
		EnvelopeVersion: AddressEnvelopeVersion, GlobalIdentityHint: context.GlobalIdentityHint,
		IdentityHeadHash: context.IdentityHeadHash, NodeID: context.NodeID,
		RecipientPublicKeySHA256: keyHash,
	})
	if err != nil {
		return "", ErrAddressEnvelope
	}
	info := append([]byte(addressInfoDomain), ctxBytes...)
	aad := append([]byte(addressAADDomain), ctxBytes...)
	enc, sender, err := hpke.NewSender(recipient, hpke.HKDFSHA256(), hpke.AES256GCM(), info)
	if err != nil {
		return "", ErrAddressEnvelope
	}
	ciphertext, err := sender.Seal(aad, canonicalPlaintext)
	if err != nil {
		return "", ErrAddressEnvelope
	}
	defer clear(ciphertext)
	envelope := AddressEnvelope{
		EnvelopeVersion: AddressEnvelopeVersion, RecipientPublicKeySHA256: keyHash,
		KEM: addressKEM, KDF: addressKDF, AEAD: addressAEAD,
		Encapsulation: base64.RawURLEncoding.EncodeToString(enc),
		Ciphertext:    base64.RawURLEncoding.EncodeToString(ciphertext),
	}
	encoded, err := marshalAddressEnvelope(envelope)
	if err != nil || len(encoded) > maxAddressEnvelope {
		return "", ErrAddressEnvelope
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// OpenAddressEnvelope authenticates the bound context and returns canonical
// JSON only after HPKE authentication succeeds. Callers must separately
// verify the advertisement signature and current identity delegation.
func OpenAddressEnvelope(value string, recipientPrivateKey []byte, context AddressContext) ([]byte, error) {
	envelope, err := ParseAddressEnvelope(value)
	if err != nil || len(recipientPrivateKey) != 32 || !validAddressContext(context) {
		return nil, ErrAddressEnvelope
	}
	privateKey, err := ecdh.X25519().NewPrivateKey(recipientPrivateKey)
	if err != nil {
		return nil, ErrAddressEnvelope
	}
	publicKey := privateKey.PublicKey().Bytes()
	keyHash := AddressRecipientPublicKeyHash(publicKey)
	if keyHash != envelope.RecipientPublicKeySHA256 {
		return nil, ErrAddressEnvelope
	}
	ctxBytes, err := canonicalAddressContext(addressHPKEContext{
		EnvelopeVersion: envelope.EnvelopeVersion, GlobalIdentityHint: context.GlobalIdentityHint,
		IdentityHeadHash: context.IdentityHeadHash, NodeID: context.NodeID,
		RecipientPublicKeySHA256: keyHash,
	})
	if err != nil {
		return nil, ErrAddressEnvelope
	}
	enc, _ := base64.RawURLEncoding.DecodeString(envelope.Encapsulation)
	ciphertext, _ := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	info := append([]byte(addressInfoDomain), ctxBytes...)
	aad := append([]byte(addressAADDomain), ctxBytes...)
	recipient, err := hpke.DHKEM(ecdh.X25519()).NewPrivateKey(recipientPrivateKey)
	if err != nil {
		return nil, ErrAddressEnvelope
	}
	opened, err := hpke.NewRecipient(enc, recipient, hpke.HKDFSHA256(), hpke.AES256GCM(), info)
	if err != nil {
		return nil, ErrAddressEnvelope
	}
	plaintext, err := opened.Open(aad, ciphertext)
	if err != nil || len(plaintext) == 0 || len(plaintext) > addressPayloadMax {
		clear(plaintext)
		return nil, ErrAddressEnvelope
	}
	canonical, err := identityprotocol.Canonicalize(plaintext)
	if err != nil || !bytes.Equal(canonical, plaintext) {
		clear(plaintext)
		return nil, ErrAddressEnvelope
	}
	return plaintext, nil
}

// ParseAddressEnvelope verifies the canonical bounded wire representation.
// It cannot prove that the ciphertext is valid or decryptable without the
// recipient private key.
func ParseAddressEnvelope(value string) (AddressEnvelope, error) {
	if value == "" || len(value) > maxAddressEnvelope*2 {
		return AddressEnvelope{}, ErrAddressEnvelope
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(encoded) == 0 || len(encoded) > maxAddressEnvelope || base64.RawURLEncoding.EncodeToString(encoded) != value {
		return AddressEnvelope{}, ErrAddressEnvelope
	}
	var envelope AddressEnvelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return AddressEnvelope{}, ErrAddressEnvelope
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil || !bytes.Equal(canonical, encoded) || validateAddressEnvelope(envelope) != nil {
		return AddressEnvelope{}, ErrAddressEnvelope
	}
	return envelope, nil
}

func AddressRecipientPublicKeyHash(publicKey []byte) string {
	if len(publicKey) != 32 {
		return ""
	}
	digest := sha256.Sum256(append([]byte(addressKeyHashDomain), publicKey...))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validAddressEnvelope(value string) bool {
	_, err := ParseAddressEnvelope(value)
	return err == nil
}

func validAddressContext(context AddressContext) bool {
	return identityHintRE.MatchString(context.GlobalIdentityHint) && addressHashRE.MatchString(context.IdentityHeadHash) && addressNodeIDRE.MatchString(context.NodeID)
}

func validateAddressEnvelope(envelope AddressEnvelope) error {
	if envelope.EnvelopeVersion != AddressEnvelopeVersion || !addressHashRE.MatchString(envelope.RecipientPublicKeySHA256) || envelope.KEM != addressKEM || envelope.KDF != addressKDF || envelope.AEAD != addressAEAD {
		return ErrAddressEnvelope
	}
	enc, err := base64.RawURLEncoding.DecodeString(envelope.Encapsulation)
	if err != nil || len(enc) != addressEncapsulation || base64.RawURLEncoding.EncodeToString(enc) != envelope.Encapsulation {
		return ErrAddressEnvelope
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) < addressTagBytes+1 || len(ciphertext) > addressPayloadMax+addressTagBytes || base64.RawURLEncoding.EncodeToString(ciphertext) != envelope.Ciphertext {
		return ErrAddressEnvelope
	}
	return nil
}

func canonicalAddressContext(context addressHPKEContext) ([]byte, error) {
	encoded, err := json.Marshal(context)
	if err != nil {
		return nil, fmt.Errorf("%w: context", ErrAddressEnvelope)
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil {
		return nil, ErrAddressEnvelope
	}
	return canonical, nil
}

func marshalAddressEnvelope(envelope AddressEnvelope) ([]byte, error) {
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, ErrAddressEnvelope
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil || strings.TrimSpace(string(canonical)) == "" {
		return nil, ErrAddressEnvelope
	}
	return canonical, nil
}
