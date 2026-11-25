package identityprotocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	EventVersion    = "ir.identity.event/1"
	signDomain      = "IR/identity-event/v1/sign\x00"
	hashDomain      = "IR/identity-event/v1/hash\x00"
	keyDomain       = "IR/identity-key/v1\x00"
	idDomain        = "IR/global-identity-id/v1\x00"
	nodeIDDomain    = "IR/node-id/v1\x00"
	nodeProofDomain = "IR/node-enrollment/v1/pop\x00"
)

var (
	ErrInvalidEvent = errors.New("identity event is invalid")
	sequenceRE      = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
	identityIDRE    = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	keyIDRE         = regexp.MustCompile(`^ik_[A-Za-z0-9_-]{43}$`)
	eventHashRE     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	nodeIDRE        = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	knownOperations = map[string]struct{}{
		"genesis": {}, "credential_add": {}, "credential_revoke": {},
		"node_delegate": {}, "node_revoke": {}, "subject_policy_update": {},
		"recovery_rotate": {}, "identity_freeze": {}, "identity_unfreeze": {},
		"fork_resolve": {},
	}
)

// Event is a structurally and cryptographically verified ir.identity.event/1.
// Verification does not establish that the signer was authorized by the
// history head; callers must apply the chain and authority rules separately.
type Event struct {
	IdentityID      string
	Sequence        string
	PreviousHash    *string
	Operation       string
	IssuedAt        time.Time
	SignerKeyID     string
	SignerPublicKey ed25519.PublicKey
	Payload         map[string]any
	Canonical       []byte
	Signature       []byte
	EventHash       string
}

// IdentityID derives the stable, recovery-reproducible identity ID from the
// genesis recovery signing public key. It is independent of passkeys, RP IDs,
// installation IDs, nodes, and OwnerSubject IDs.
func IdentityID(recoverySigningPublicKey ed25519.PublicKey) (string, error) {
	if len(recoverySigningPublicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%w: recovery signing key length", ErrInvalidEvent)
	}
	digest := sha256.Sum256(append([]byte(idDomain), recoverySigningPublicKey...))
	return "gi_" + base64.RawURLEncoding.EncodeToString(digest[:16]), nil
}

func KeyID(publicKey ed25519.PublicKey) (string, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%w: signing key length", ErrInvalidEvent)
	}
	digest := sha256.Sum256(append([]byte(keyDomain), publicKey...))
	return "ik_" + base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func EventHash(canonical []byte) string {
	digest := sha256.Sum256(append([]byte(hashDomain), canonical...))
	return "sha256:" + hex.EncodeToString(digest[:])
}

// NodeID derives a stable per-installation node identifier from its Ed25519
// public key. It is deliberately independent of the Global Identity ID.
func NodeID(publicKey ed25519.PublicKey) (string, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%w: node public key length", ErrInvalidEvent)
	}
	digest := sha256.Sum256(append([]byte(nodeIDDomain), publicKey...))
	return "nd_" + base64.RawURLEncoding.EncodeToString(digest[:16]), nil
}

// NodeEnrollmentProofMessage creates the domain-separated message a target
// node signs to prove possession of its Node Key during enrollment. The
// verifier-generated nonce is bound to the identity and exact current head.
func NodeEnrollmentProofMessage(identityID, previousHash, nonce string) ([]byte, error) {
	if !identityIDRE.MatchString(identityID) || !eventHashRE.MatchString(previousHash) || !validRawURLNonce(nonce) {
		return nil, fmt.Errorf("%w: node enrollment proof context", ErrInvalidEvent)
	}
	return []byte(nodeProofDomain + identityID + "\x00" + previousHash + "\x00" + nonce), nil
}

func IsNodeID(value string) bool { return nodeIDRE.MatchString(value) }

func validRawURLNonce(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == value
}

// SignEvent canonicalizes one event and signs the profile's domain-separated
// bytes. The caller remains responsible for constructing a valid event and
// securely holding the private key.
func SignEvent(input []byte, privateKey ed25519.PrivateKey) ([]byte, []byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, nil, fmt.Errorf("%w: private key length", ErrInvalidEvent)
	}
	canonical, err := Canonicalize(input)
	if err != nil {
		return nil, nil, err
	}
	signature := ed25519.Sign(privateKey, append([]byte(signDomain), canonical...))
	return canonical, signature, nil
}

// VerifyEvent requires canonical wire bytes and a signature over the exact
// event profile. It validates schema, encoding, signer-derived IDs, and
// genesis structure, but deliberately does not infer successor authority.
func VerifyEvent(canonical, signature []byte) (Event, error) {
	if len(canonical) == 0 || len(canonical) > maxCanonicalDocumentBytes || len(signature) != ed25519.SignatureSize {
		return Event{}, fmt.Errorf("%w: size or signature length", ErrInvalidEvent)
	}
	decoded, err := decodeStrictJSON(canonical)
	if err != nil {
		return Event{}, fmt.Errorf("%w: JSON profile: %v", ErrInvalidEvent, err)
	}
	reencoded, err := canonicalJSON(decoded)
	if err != nil || string(reencoded) != string(canonical) {
		return Event{}, fmt.Errorf("%w: event bytes are not canonical", ErrInvalidEvent)
	}
	object, ok := decoded.(map[string]any)
	if !ok || len(object) != 9 {
		return Event{}, fmt.Errorf("%w: event object fields", ErrInvalidEvent)
	}
	for _, field := range []string{"event_version", "identity_id", "sequence", "previous_event_hash", "operation", "issued_at", "signer_key_id", "signer_public_key", "payload"} {
		if _, ok := object[field]; !ok {
			return Event{}, fmt.Errorf("%w: missing event field", ErrInvalidEvent)
		}
	}
	stringField := func(name string) (string, bool) {
		value, ok := object[name].(string)
		return value, ok
	}
	version, okVersion := stringField("event_version")
	identityID, okIdentity := stringField("identity_id")
	sequence, okSequence := stringField("sequence")
	operation, okOperation := stringField("operation")
	issuedAtText, okIssuedAt := stringField("issued_at")
	keyID, okKeyID := stringField("signer_key_id")
	publicText, okPublic := stringField("signer_public_key")
	if !okVersion || version != EventVersion || !okIdentity || !identityIDRE.MatchString(identityID) || !okSequence || !sequenceRE.MatchString(sequence) || !okOperation {
		return Event{}, fmt.Errorf("%w: event identity/version/sequence/operation", ErrInvalidEvent)
	}
	if _, known := knownOperations[operation]; !known {
		return Event{}, fmt.Errorf("%w: unknown operation", ErrInvalidEvent)
	}
	if !okIssuedAt || !okKeyID || !keyIDRE.MatchString(keyID) || !okPublic {
		return Event{}, fmt.Errorf("%w: signer or timestamp fields", ErrInvalidEvent)
	}
	issuedAt, err := time.Parse(time.RFC3339, issuedAtText)
	if err != nil {
		return Event{}, fmt.Errorf("%w: timestamp", ErrInvalidEvent)
	}
	_, offset := issuedAt.Zone()
	if offset != 0 {
		return Event{}, fmt.Errorf("%w: timestamp is not UTC", ErrInvalidEvent)
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(publicText)
	if err != nil || len(publicKey) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(publicKey) != publicText {
		return Event{}, fmt.Errorf("%w: public key encoding", ErrInvalidEvent)
	}
	derivedKeyID, _ := KeyID(ed25519.PublicKey(publicKey))
	if keyID != derivedKeyID {
		return Event{}, fmt.Errorf("%w: signer key ID mismatch", ErrInvalidEvent)
	}
	previousValue := object["previous_event_hash"]
	isNull := previousValue == nil
	var previous *string
	if isNull {
		if operation != "genesis" || sequence != "0" {
			return Event{}, fmt.Errorf("%w: only genesis may omit previous hash", ErrInvalidEvent)
		}
		identityFromKey, _ := IdentityID(ed25519.PublicKey(publicKey))
		if identityID != identityFromKey {
			return Event{}, fmt.Errorf("%w: genesis identity ID mismatch", ErrInvalidEvent)
		}
	} else {
		value, ok := object["previous_event_hash"].(string)
		if !ok || !eventHashRE.MatchString(value) || sequence == "0" || operation == "genesis" {
			return Event{}, fmt.Errorf("%w: previous hash/sequence relationship", ErrInvalidEvent)
		}
		previous = &value
	}
	payload, ok := object["payload"].(map[string]any)
	if !ok {
		return Event{}, fmt.Errorf("%w: payload must be an object", ErrInvalidEvent)
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), append([]byte(signDomain), canonical...), signature) {
		return Event{}, fmt.Errorf("%w: signature verification", ErrInvalidEvent)
	}
	return Event{
		IdentityID: identityID, Sequence: sequence, PreviousHash: previous,
		Operation: operation, IssuedAt: issuedAt.UTC(), SignerKeyID: keyID,
		SignerPublicKey: ed25519.PublicKey(append([]byte(nil), publicKey...)),
		Payload:         payload, Canonical: append([]byte(nil), canonical...),
		Signature: append([]byte(nil), signature...), EventHash: EventHash(canonical),
	}, nil
}
