// Package signalingprotocol defines signed, short-lived, end-to-end encrypted
// envelopes for WebRTC signaling. Rendezvous providers route messages but do
// not authorize peers; current identity state is checked by each recipient.
package signalingprotocol

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	Version          = "ir.peer-signaling/1"
	MaxMessageTTL    = 2 * time.Minute
	MaxClockSkew     = 30 * time.Second
	MaxSDPBytes      = 256 << 10
	MaxCiphertext    = MaxSDPBytes + 64
	MaxEnvelopeBytes = 384 << 10
	messageDomain    = "IR/peer-signaling/v1/message\x00"
	keyDomain        = "IR/peer-signaling/v1/session-key\x00"
)

var (
	ErrInvalid        = errors.New("peer signaling message is invalid")
	ErrDenied         = errors.New("peer signaling message is not authorized")
	ErrStale          = errors.New("peer signaling message is stale")
	ErrDecrypt        = errors.New("peer signaling payload could not be authenticated")
	ErrReplay         = errors.New("peer signaling message was already consumed")
	ErrReplayCapacity = errors.New("peer signaling replay store is full")
	identityRE        = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	nodeRE            = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	sessionRE         = regexp.MustCompile(`^ps_[A-Za-z0-9_-]{22}$`)
	messageRE         = regexp.MustCompile(`^pm_[A-Za-z0-9_-]{22}$`)
	headRE            = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Kind string

const (
	KindInit   Kind = "init"
	KindAccept Kind = "accept"
	KindOffer  Kind = "offer"
	KindAnswer Kind = "answer"
	KindCancel Kind = "cancel"
)

// Message is a Node-Key-signed, recipient-bound rendezvous envelope. Init and
// accept carry one-time X25519 public keys; offer and answer carry AEAD
// ciphertext. The signature binds every routing, lifetime and payload field.
type Message struct {
	Version            string `json:"version"`
	MessageID          string `json:"message_id"`
	SessionID          string `json:"session_id"`
	SourceIdentityID   string `json:"source_identity_id"`
	SourceHeadHash     string `json:"source_head_hash"`
	SourceNodeID       string `json:"source_node_id"`
	TargetIdentityID   string `json:"target_identity_id"`
	TargetNodeID       string `json:"target_node_id"`
	Kind               Kind   `json:"kind"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	EphemeralPublicKey string `json:"ephemeral_public_key,omitempty"`
	Ciphertext         string `json:"ciphertext,omitempty"`
	Signature          string `json:"signature"`
}

type unsignedMessage struct {
	Version            string `json:"version"`
	MessageID          string `json:"message_id"`
	SessionID          string `json:"session_id"`
	SourceIdentityID   string `json:"source_identity_id"`
	SourceHeadHash     string `json:"source_head_hash"`
	SourceNodeID       string `json:"source_node_id"`
	TargetIdentityID   string `json:"target_identity_id"`
	TargetNodeID       string `json:"target_node_id"`
	Kind               Kind   `json:"kind"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	EphemeralPublicKey string `json:"ephemeral_public_key,omitempty"`
	Ciphertext         string `json:"ciphertext,omitempty"`
}

// IdentityState is the minimal current public authority projection needed to
// validate a signaling sender or recipient. It intentionally avoids coupling
// the wire package to a particular authority store implementation.
type IdentityState struct {
	IdentityID  string
	HeadHash    string
	Frozen      bool
	VerifiedAt  time.Time
	FreshUntil  time.Time
	CurrentKeys map[string]ed25519.PublicKey
}

type IdentityResolver func(identityID, headHash string, now time.Time) (IdentityState, error)

// NewSessionID creates an unpredictable identifier, not an authorization token.
func NewSessionID(entropy io.Reader) (string, error) {
	if entropy == nil {
		entropy = rand.Reader
	}
	raw := make([]byte, 16)
	if _, err := io.ReadFull(entropy, raw); err != nil {
		return "", fmt.Errorf("%w: session randomness", ErrInvalid)
	}
	return "ps_" + base64.RawURLEncoding.EncodeToString(raw), nil
}

// NewEphemeralKey creates an X25519 key that must be discarded after the
// signaling exchange completes.
func NewEphemeralKey(entropy io.Reader) (*ecdh.PrivateKey, error) {
	if entropy == nil {
		entropy = rand.Reader
	}
	key, err := ecdh.X25519().GenerateKey(entropy)
	if err != nil {
		return nil, fmt.Errorf("%w: ephemeral key generation", ErrInvalid)
	}
	return key, nil
}

// NewMessage creates common bounded metadata. Init/accept callers set the
// ephemeral public key before signing. Offer/answer callers seal payload bytes.
func NewMessage(kind Kind, sessionID, sourceIdentityID, sourceHeadHash, sourceNodeID, targetIdentityID, targetNodeID string, now time.Time, entropy io.Reader) (Message, error) {
	if entropy == nil {
		entropy = rand.Reader
	}
	if now.IsZero() {
		return Message{}, ErrInvalid
	}
	raw := make([]byte, 16)
	if _, err := io.ReadFull(entropy, raw); err != nil {
		return Message{}, fmt.Errorf("%w: message randomness", ErrInvalid)
	}
	now = now.UTC().Truncate(time.Second)
	m := Message{
		Version: Version, MessageID: "pm_" + base64.RawURLEncoding.EncodeToString(raw), SessionID: sessionID,
		SourceIdentityID: sourceIdentityID, SourceHeadHash: sourceHeadHash, SourceNodeID: sourceNodeID,
		TargetIdentityID: targetIdentityID, TargetNodeID: targetNodeID, Kind: kind,
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(MaxMessageTTL).Format(time.RFC3339),
	}
	if err := validateBase(m); err != nil {
		return Message{}, err
	}
	return m, nil
}

// Sign signs an envelope using its source Node Key. Production code should
// call nodeidentity.Store.SignPeerSignal to keep the key encapsulated.
func Sign(m Message, privateKey ed25519.PrivateKey) (Message, error) {
	if len(privateKey) != ed25519.PrivateKeySize || m.Signature != "" || validateShape(m) != nil {
		return Message{}, ErrInvalid
	}
	derived, err := identityprotocol.NodeID(privateKey.Public().(ed25519.PublicKey))
	if err != nil || derived != m.SourceNodeID {
		return Message{}, ErrDenied
	}
	canonical, err := canonical(unsignedFrom(m))
	if err != nil || len(canonical) > MaxEnvelopeBytes {
		return Message{}, ErrInvalid
	}
	m.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, append([]byte(messageDomain), canonical...)))
	return m, nil
}

// Verify checks signature, current active Node Key delegation and freshness.
// The resolver must refresh state rather than reuse an advertisement cache.
func Verify(m Message, resolve IdentityResolver, now time.Time) error {
	if resolve == nil || now.IsZero() || validateShape(m) != nil || m.Signature == "" {
		return ErrInvalid
	}
	issued, _ := parseTime(m.IssuedAt)
	expires, _ := parseTime(m.ExpiresAt)
	if now.Before(issued.Add(-MaxClockSkew)) || !now.Before(expires) {
		return ErrStale
	}
	historical, err := resolve(m.SourceIdentityID, m.SourceHeadHash, now)
	if err != nil || !fresh(historical, m.SourceIdentityID, m.SourceHeadHash, now) {
		return ErrStale
	}
	current, err := resolve(m.SourceIdentityID, "", now)
	if err != nil || !fresh(current, m.SourceIdentityID, m.SourceHeadHash, now) {
		return ErrStale
	}
	target, err := resolve(m.TargetIdentityID, "", now)
	if err != nil || target.Frozen || target.IdentityID != m.TargetIdentityID || target.VerifiedAt.IsZero() || now.Before(target.VerifiedAt) || target.FreshUntil.IsZero() || !now.Before(target.FreshUntil) {
		return ErrStale
	}
	if len(target.CurrentKeys["node:"+m.TargetNodeID]) != ed25519.PublicKeySize {
		return ErrDenied
	}
	key, ok := historical.CurrentKeys["node:"+m.SourceNodeID]
	currentKey, currentOK := current.CurrentKeys["node:"+m.SourceNodeID]
	if !ok || !currentOK || len(key) != ed25519.PublicKeySize || !ed25519.PublicKey(key).Equal(currentKey) {
		return ErrDenied
	}
	derived, err := identityprotocol.NodeID(key)
	if err != nil || derived != m.SourceNodeID {
		return ErrDenied
	}
	signature, err := base64.RawURLEncoding.DecodeString(m.Signature)
	canonical, canonicalErr := canonical(unsignedFrom(m))
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != m.Signature || canonicalErr != nil || !ed25519.Verify(key, append([]byte(messageDomain), canonical...), signature) {
		return ErrDenied
	}
	return nil
}

// ValidateSigningAuthority checks the current, unfrozen identity heads and
// active Node Key delegation for both endpoints before the local Node Key
// signs a message. It intentionally does not require a signature yet.
func ValidateSigningAuthority(m Message, resolve IdentityResolver, now time.Time) error {
	if resolve == nil || now.IsZero() || validateShape(m) != nil || m.Signature != "" {
		return ErrInvalid
	}
	issued, _ := parseTime(m.IssuedAt)
	expires, _ := parseTime(m.ExpiresAt)
	if now.Before(issued.Add(-MaxClockSkew)) || !now.Before(expires) || expires.After(now.Add(MaxMessageTTL+MaxClockSkew)) {
		return ErrStale
	}
	sourceHistorical, err := resolve(m.SourceIdentityID, m.SourceHeadHash, now)
	if err != nil || !fresh(sourceHistorical, m.SourceIdentityID, m.SourceHeadHash, now) {
		return ErrStale
	}
	sourceCurrent, err := resolve(m.SourceIdentityID, "", now)
	if err != nil || !fresh(sourceCurrent, m.SourceIdentityID, m.SourceHeadHash, now) {
		return ErrStale
	}
	targetCurrent, err := resolve(m.TargetIdentityID, "", now)
	if err != nil || targetCurrent.Frozen || targetCurrent.IdentityID != m.TargetIdentityID || targetCurrent.VerifiedAt.IsZero() || now.Before(targetCurrent.VerifiedAt) || targetCurrent.FreshUntil.IsZero() || !now.Before(targetCurrent.FreshUntil) {
		return ErrStale
	}
	sourceKey := sourceCurrent.CurrentKeys["node:"+m.SourceNodeID]
	historicalKey := sourceHistorical.CurrentKeys["node:"+m.SourceNodeID]
	if len(sourceKey) != ed25519.PublicKeySize || len(historicalKey) != ed25519.PublicKeySize || !ed25519.PublicKey(sourceKey).Equal(historicalKey) {
		return ErrDenied
	}
	if len(targetCurrent.CurrentKeys["node:"+m.TargetNodeID]) != ed25519.PublicKeySize {
		return ErrDenied
	}
	return nil
}

// ValidateReply binds a signed accept to the exact peers/session from init.
// Both signatures must be independently checked with Verify first.
func ValidateReply(init, accept Message) error {
	if validateShape(init) != nil || validateShape(accept) != nil || init.Signature == "" || accept.Signature == "" ||
		init.Kind != KindInit || accept.Kind != KindAccept || init.SessionID != accept.SessionID ||
		init.EphemeralPublicKey == accept.EphemeralPublicKey ||
		init.SourceIdentityID != accept.TargetIdentityID || init.SourceNodeID != accept.TargetNodeID ||
		init.TargetIdentityID != accept.SourceIdentityID || init.TargetNodeID != accept.SourceNodeID {
		return ErrDenied
	}
	initIssued, _ := parseTime(init.IssuedAt)
	acceptIssued, _ := parseTime(accept.IssuedAt)
	initExpires, _ := parseTime(init.ExpiresAt)
	acceptExpires, _ := parseTime(accept.ExpiresAt)
	if acceptIssued.Before(initIssued) || acceptIssued.Sub(initIssued) > MaxMessageTTL || acceptExpires.After(initExpires) {
		return ErrStale
	}
	return nil
}

// ValidateSessionMessage binds offer, answer, and cancel messages to the
// original init, including participant direction and the original expiry.
// Call Verify on both messages before accepting them.
func ValidateSessionMessage(init, message Message) error {
	if validateShape(init) != nil || validateShape(message) != nil || init.Signature == "" || message.Signature == "" ||
		init.Kind != KindInit || init.SessionID != message.SessionID {
		return ErrDenied
	}
	initIssued, _ := parseTime(init.IssuedAt)
	messageIssued, _ := parseTime(message.IssuedAt)
	initExpires, _ := parseTime(init.ExpiresAt)
	messageExpires, _ := parseTime(message.ExpiresAt)
	if messageIssued.Before(initIssued) || messageIssued.After(initExpires) || messageExpires.After(initExpires) {
		return ErrStale
	}
	sameDirection := init.SourceIdentityID == message.SourceIdentityID && init.SourceNodeID == message.SourceNodeID &&
		init.TargetIdentityID == message.TargetIdentityID && init.TargetNodeID == message.TargetNodeID
	reverseDirection := init.SourceIdentityID == message.TargetIdentityID && init.SourceNodeID == message.TargetNodeID &&
		init.TargetIdentityID == message.SourceIdentityID && init.TargetNodeID == message.SourceNodeID
	switch message.Kind {
	case KindOffer:
		if !sameDirection {
			return ErrDenied
		}
	case KindAnswer:
		if !reverseDirection {
			return ErrDenied
		}
	case KindCancel:
		if !sameDirection && !reverseDirection {
			return ErrDenied
		}
	default:
		return ErrDenied
	}
	return nil
}

// DeriveSessionKey derives a shared key from the two one-time X25519 keys.
// The init/accept messages must first have passed Verify and ValidateReply.
func DeriveSessionKey(localPrivate *ecdh.PrivateKey, init, accept Message) ([]byte, error) {
	if localPrivate == nil || ValidateReply(init, accept) != nil {
		return nil, ErrInvalid
	}
	localPublic := base64.RawURLEncoding.EncodeToString(localPrivate.PublicKey().Bytes())
	var remoteEncoded string
	switch localPublic {
	case init.EphemeralPublicKey:
		remoteEncoded = accept.EphemeralPublicKey
	case accept.EphemeralPublicKey:
		remoteEncoded = init.EphemeralPublicKey
	default:
		return nil, ErrDenied
	}
	remoteBytes, err := base64.RawURLEncoding.DecodeString(remoteEncoded)
	if err != nil || len(remoteBytes) != 32 || base64.RawURLEncoding.EncodeToString(remoteBytes) != remoteEncoded {
		return nil, ErrInvalid
	}
	remote, err := ecdh.X25519().NewPublicKey(remoteBytes)
	if err != nil {
		return nil, ErrInvalid
	}
	shared, err := localPrivate.ECDH(remote)
	if err != nil {
		return nil, ErrDenied
	}
	salt := sha256.Sum256(append([]byte(keyDomain), []byte(init.SessionID)...))
	info, err := canonical([]string{init.SourceIdentityID, init.SourceNodeID, init.TargetIdentityID, init.TargetNodeID})
	if err != nil {
		clear(shared)
		return nil, ErrInvalid
	}
	key, err := hkdf.Key(sha256.New, shared, salt[:], string(append([]byte(keyDomain), info...)), 32)
	clear(shared)
	if err != nil {
		return nil, ErrInvalid
	}
	return key, nil
}

// SealPayload encrypts one bounded SDP document. The returned envelope must
// be signed before it is published to a rendezvous provider.
func SealPayload(m Message, key, plaintext []byte, entropy io.Reader) (Message, error) {
	if (m.Kind != KindOffer && m.Kind != KindAnswer) || len(key) != 32 || len(plaintext) == 0 || len(plaintext) > MaxSDPBytes || m.Ciphertext != "" || m.EphemeralPublicKey != "" || validateBase(m) != nil {
		return Message{}, ErrInvalid
	}
	if entropy == nil {
		entropy = rand.Reader
	}
	aead, err := newAEAD(key)
	if err != nil {
		return Message{}, ErrInvalid
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(entropy, nonce); err != nil {
		return Message{}, ErrInvalid
	}
	sealed := aead.Seal(nonce, nonce, plaintext, associatedData(m))
	m.Ciphertext = base64.RawURLEncoding.EncodeToString(sealed)
	if validateShape(m) != nil {
		return Message{}, ErrInvalid
	}
	return m, nil
}

// OpenPayload authenticates routing, session, operation and lifetime metadata
// as AEAD associated data before returning plaintext SDP.
func OpenPayload(m Message, key []byte) ([]byte, error) {
	if (m.Kind != KindOffer && m.Kind != KindAnswer) || len(key) != 32 || validateShape(m) != nil {
		return nil, ErrInvalid
	}
	sealed, err := base64.RawURLEncoding.DecodeString(m.Ciphertext)
	aead, aeadErr := newAEAD(key)
	if err != nil || aeadErr != nil || len(sealed) < aead.NonceSize()+aead.Overhead() {
		return nil, ErrDecrypt
	}
	plaintext, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], associatedData(m))
	if err != nil || len(plaintext) == 0 || len(plaintext) > MaxSDPBytes {
		return nil, ErrDecrypt
	}
	return plaintext, nil
}

// CanonicalMessage returns the bounded canonical wire representation.
func CanonicalMessage(m Message) ([]byte, error) {
	if validateShape(m) != nil || m.Signature == "" {
		return nil, ErrInvalid
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return nil, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil || len(canonical) == 0 || len(canonical) > MaxEnvelopeBytes {
		return nil, ErrInvalid
	}
	return canonical, nil
}

// ParseCanonicalMessage rejects unknown fields, noncanonical bytes and
// trailing values. identityprotocol canonicalization rejects duplicate keys.
func ParseCanonicalMessage(encoded []byte) (Message, error) {
	if len(encoded) == 0 || len(encoded) > MaxEnvelopeBytes {
		return Message{}, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil || string(canonical) != string(encoded) {
		return Message{}, ErrInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var m Message
	if decoder.Decode(&m) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateShape(m) != nil || m.Signature == "" {
		return Message{}, ErrInvalid
	}
	return m, nil
}

func validateBase(m Message) error {
	if m.Version != Version || !messageRE.MatchString(m.MessageID) || !sessionRE.MatchString(m.SessionID) ||
		!identityRE.MatchString(m.SourceIdentityID) || !headRE.MatchString(m.SourceHeadHash) || !nodeRE.MatchString(m.SourceNodeID) ||
		!identityRE.MatchString(m.TargetIdentityID) || !nodeRE.MatchString(m.TargetNodeID) || m.SourceNodeID == m.TargetNodeID {
		return ErrInvalid
	}
	switch m.Kind {
	case KindInit, KindAccept, KindOffer, KindAnswer, KindCancel:
	default:
		return ErrInvalid
	}
	issued, err := parseTime(m.IssuedAt)
	if err != nil {
		return ErrInvalid
	}
	expires, err := parseTime(m.ExpiresAt)
	if err != nil || !expires.After(issued) || expires.Sub(issued) > MaxMessageTTL {
		return ErrInvalid
	}
	return nil
}

func validateShape(m Message) error {
	if err := validateBase(m); err != nil {
		return err
	}
	switch m.Kind {
	case KindInit, KindAccept:
		key, err := base64.RawURLEncoding.DecodeString(m.EphemeralPublicKey)
		if err != nil || len(key) != 32 || base64.RawURLEncoding.EncodeToString(key) != m.EphemeralPublicKey || m.Ciphertext != "" {
			return ErrInvalid
		}
	case KindOffer, KindAnswer:
		ciphertext, err := base64.RawURLEncoding.DecodeString(m.Ciphertext)
		if err != nil || len(ciphertext) < 12+16 || len(ciphertext) > MaxCiphertext || base64.RawURLEncoding.EncodeToString(ciphertext) != m.Ciphertext || m.EphemeralPublicKey != "" {
			return ErrInvalid
		}
	case KindCancel:
		if m.EphemeralPublicKey != "" || m.Ciphertext != "" {
			return ErrInvalid
		}
	}
	if m.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(m.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != m.Signature {
			return ErrInvalid
		}
	}
	return nil
}

func fresh(state IdentityState, identityID, headHash string, now time.Time) bool {
	return !state.Frozen && state.IdentityID == identityID && state.HeadHash == headHash && !state.VerifiedAt.IsZero() &&
		!now.Before(state.VerifiedAt) && !state.FreshUntil.IsZero() && now.Before(state.FreshUntil)
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || !strings.HasSuffix(value, "Z") || parsed.Location() != time.UTC || parsed.Format(time.RFC3339) != value {
		return time.Time{}, ErrInvalid
	}
	return parsed, nil
}

func unsignedFrom(m Message) unsignedMessage {
	return unsignedMessage{
		Version: m.Version, MessageID: m.MessageID, SessionID: m.SessionID,
		SourceIdentityID: m.SourceIdentityID, SourceHeadHash: m.SourceHeadHash, SourceNodeID: m.SourceNodeID,
		TargetIdentityID: m.TargetIdentityID, TargetNodeID: m.TargetNodeID, Kind: m.Kind,
		IssuedAt: m.IssuedAt, ExpiresAt: m.ExpiresAt, EphemeralPublicKey: m.EphemeralPublicKey,
		Ciphertext: m.Ciphertext,
	}
}

func associatedData(m Message) []byte {
	unsigned := unsignedFrom(m)
	unsigned.Ciphertext = ""
	data, _ := canonical(unsigned)
	return append([]byte(messageDomain), data...)
}

func canonical(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil || len(canonical) == 0 || len(canonical) > MaxEnvelopeBytes {
		return nil, ErrInvalid
	}
	return canonical, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
