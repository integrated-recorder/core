// Package peerprotocol defines the node-authenticated application handshake
// carried inside an untrusted WebRTC data channel. Signaling and ICE only
// establish a candidate transport; this transcript binds the channel to two
// currently delegated Node Keys and fresh session challenges.
package peerprotocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	HandshakeVersion  = "ir.peer-handshake/1"
	MaxHandshakeTTL   = 2 * time.Minute
	MaxHandshakeBytes = 32 << 10
	helloDomain       = "IR/peer-handshake/v1/hello\x00"
	transcriptDomain  = "IR/peer-handshake/v1/transcript\x00"
	proofDomain       = "IR/peer-handshake/v1/proof\x00"
)

var (
	ErrInvalid        = errors.New("peer handshake is invalid")
	ErrDenied         = errors.New("peer handshake is not authorized")
	ErrStale          = errors.New("peer identity state is stale")
	ErrReplay         = errors.New("peer handshake session was already consumed")
	ErrReplayCapacity = errors.New("peer handshake replay cache is full")
	identityID        = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	nodeID            = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	sessionID         = regexp.MustCompile(`^ps_[A-Za-z0-9_-]{22}$`)
	hashValue         = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Hello is a one-use challenge signed by a delegated Node Key. Public keys
// are intentionally omitted: verifiers resolve them from validated identity
// history and require the same key to remain active at the current head.
type Hello struct {
	Version    string `json:"handshake_version"`
	IdentityID string `json:"identity_id"`
	HeadHash   string `json:"identity_head_hash"`
	NodeID     string `json:"node_id"`
	SessionID  string `json:"session_id"`
	Nonce      string `json:"nonce"`
	IssuedAt   string `json:"issued_at"`
	ExpiresAt  string `json:"expires_at"`
	Signature  string `json:"signature"`
}

type unsignedHello struct {
	Version    string `json:"handshake_version"`
	IdentityID string `json:"identity_id"`
	HeadHash   string `json:"identity_head_hash"`
	NodeID     string `json:"node_id"`
	SessionID  string `json:"session_id"`
	Nonce      string `json:"nonce"`
	IssuedAt   string `json:"issued_at"`
	ExpiresAt  string `json:"expires_at"`
}

// Transcript contains both signed challenges and each party's signature over
// their exact ordered pair. Reordering peers, replacing either challenge, or
// replaying a proof in another session changes the transcript digest.
type Transcript struct {
	Version        string `json:"handshake_version"`
	Initiator      Hello  `json:"initiator"`
	Responder      Hello  `json:"responder"`
	InitiatorProof string `json:"initiator_proof"`
	ResponderProof string `json:"responder_proof"`
}

type transcriptUnsigned struct {
	Version   string `json:"handshake_version"`
	Initiator Hello  `json:"initiator"`
	Responder Hello  `json:"responder"`
}

type IdentityResolver func(identityID, headHash string, now time.Time) (authority.IdentityState, error)

// ReplayConsumer atomically consumes a verified one-use Hello. Production
// callers should use a durable implementation so process restart does not
// reopen the short replay window; ReplayCache is suitable for isolated tests
// and single-process deployments that intentionally accept that limitation.
type ReplayConsumer interface {
	Consume(Hello, IdentityResolver, time.Time) error
}

// NewHello makes a bounded challenge. The caller must sign it with the
// protected key belonging to nodeID; nonce and session randomness are
// generated here and never accepted from a signaling peer.
func NewHello(identityIDValue, headHash, nodeIDValue string, now time.Time, entropy io.Reader) (Hello, error) {
	if entropy == nil {
		entropy = rand.Reader
	}
	if !identityID.MatchString(identityIDValue) || !hashValue.MatchString(headHash) || !nodeID.MatchString(nodeIDValue) || now.IsZero() {
		return Hello{}, ErrInvalid
	}
	random := make([]byte, 16+32)
	if _, err := io.ReadFull(entropy, random); err != nil {
		return Hello{}, fmt.Errorf("%w: challenge generation", ErrInvalid)
	}
	now = now.UTC().Truncate(time.Second)
	return Hello{
		Version: HandshakeVersion, IdentityID: identityIDValue, HeadHash: headHash,
		NodeID: nodeIDValue, SessionID: "ps_" + base64.RawURLEncoding.EncodeToString(random[:16]),
		Nonce:    base64.RawURLEncoding.EncodeToString(random[16:]),
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(MaxHandshakeTTL).Format(time.RFC3339),
	}, nil
}

// SignHello signs the exact bounded challenge with the local Node Key. It
// verifies that the public key derives the advertised Node ID before signing.
func SignHello(hello Hello, key ed25519.PrivateKey) (Hello, error) {
	if len(key) != ed25519.PrivateKeySize {
		return Hello{}, ErrInvalid
	}
	if err := validateHelloShape(hello); err != nil {
		return Hello{}, err
	}
	derived, err := identityprotocol.NodeID(key.Public().(ed25519.PublicKey))
	if err != nil || derived != hello.NodeID {
		return Hello{}, ErrDenied
	}
	canonical, err := canonical(unsignedHelloFrom(hello))
	if err != nil {
		return Hello{}, err
	}
	hello.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, append([]byte(helloDomain), canonical...)))
	return hello, nil
}

// VerifyHello requires that the challenged node key is active both at the
// advertised history head and at the resolver's current head. An old signed
// advertisement cannot authorize a session after node revocation or identity
// advancement. The resolver must provide independently refreshed current
// state; imported history timestamps alone are insufficient.
func VerifyHello(hello Hello, resolve IdentityResolver, now time.Time) (authority.Principal, ed25519.PublicKey, error) {
	if resolve == nil || now.IsZero() || validateHelloShape(hello) != nil {
		return authority.Principal{}, nil, ErrInvalid
	}
	issued, _ := time.Parse(time.RFC3339, hello.IssuedAt)
	expires, _ := time.Parse(time.RFC3339, hello.ExpiresAt)
	if now.Before(issued.Add(-30*time.Second)) || !now.Before(expires) {
		return authority.Principal{}, nil, ErrStale
	}
	historical, err := resolve(hello.IdentityID, hello.HeadHash, now)
	if err != nil || !resolvedIdentityFresh(historical, hello.IdentityID, hello.HeadHash, now) {
		return authority.Principal{}, nil, ErrStale
	}
	current, err := resolve(hello.IdentityID, "", now)
	if err != nil || !resolvedIdentityFresh(current, hello.IdentityID, hello.HeadHash, now) {
		return authority.Principal{}, nil, ErrStale
	}
	principal := authority.Principal{Kind: authority.PrincipalNode, ID: hello.NodeID}
	key, ok := historical.CurrentKeys[principal.Kind+":"+principal.ID]
	currentKey, currentOK := current.CurrentKeys[principal.Kind+":"+principal.ID]
	if !ok || !currentOK || len(key) != ed25519.PublicKeySize || !ed25519.PublicKey(key).Equal(currentKey) {
		return authority.Principal{}, nil, ErrDenied
	}
	derived, err := identityprotocol.NodeID(key)
	if err != nil || derived != hello.NodeID {
		return authority.Principal{}, nil, ErrDenied
	}
	signature, err := base64.RawURLEncoding.DecodeString(hello.Signature)
	canonical, canonicalErr := canonical(unsignedHelloFrom(hello))
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != hello.Signature || canonicalErr != nil || !ed25519.Verify(key, append([]byte(helloDomain), canonical...), signature) {
		return authority.Principal{}, nil, ErrDenied
	}
	return principal, append(ed25519.PublicKey(nil), key...), nil
}

func resolvedIdentityFresh(state authority.IdentityState, identityIDValue, headHash string, now time.Time) bool {
	return !state.Frozen && state.IdentityID == identityIDValue && state.HeadHash == headHash &&
		!state.VerifiedAt.IsZero() && !now.Before(state.VerifiedAt) &&
		!state.FreshUntil.IsZero() && now.Before(state.FreshUntil)
}

// TranscriptDigest returns the domain-separated hash both nodes sign after
// exchanging valid Hello values. Hello order is role-significant.
func TranscriptDigest(transcript Transcript) (string, error) {
	if transcript.Version != HandshakeVersion || validateHelloShape(transcript.Initiator) != nil || validateHelloShape(transcript.Responder) != nil || transcript.Initiator.Signature == "" || transcript.Responder.Signature == "" || transcript.Initiator.SessionID != transcript.Responder.SessionID || transcript.Initiator.NodeID == transcript.Responder.NodeID || transcript.Initiator.Nonce == transcript.Responder.Nonce {
		return "", ErrInvalid
	}
	data, err := canonical(transcriptUnsigned{Version: transcript.Version, Initiator: transcript.Initiator, Responder: transcript.Responder})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte(transcriptDomain), data...))
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// SignTranscriptProof signs a role-independent digest of the two challenges.
// Each Node Key must match the corresponding Hello principal.
func SignTranscriptProof(transcript Transcript, nodeIDValue string, key ed25519.PrivateKey) (string, error) {
	digest, err := TranscriptDigest(transcript)
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return "", ErrInvalid
	}
	derived, err := identityprotocol.NodeID(key.Public().(ed25519.PublicKey))
	if err != nil || derived != nodeIDValue || (nodeIDValue != transcript.Initiator.NodeID && nodeIDValue != transcript.Responder.NodeID) {
		return "", ErrDenied
	}
	return base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, append([]byte(proofDomain), []byte(digest)...))), nil
}

// VerifyTranscript verifies both fresh identity-bound challenges and both
// signatures over the same transcript. Callers must also consume the two
// session IDs using ReplayCache before attaching application commands.
func VerifyTranscript(transcript Transcript, resolve IdentityResolver, now time.Time) (authority.Principal, authority.Principal, error) {
	if transcript.Version != HandshakeVersion || transcript.Initiator.SessionID != transcript.Responder.SessionID {
		return authority.Principal{}, authority.Principal{}, ErrInvalid
	}
	initiator, initiatorKey, err := VerifyHello(transcript.Initiator, resolve, now)
	if err != nil {
		return authority.Principal{}, authority.Principal{}, err
	}
	responder, responderKey, err := VerifyHello(transcript.Responder, resolve, now)
	if err != nil || initiator.ID == responder.ID {
		return authority.Principal{}, authority.Principal{}, ErrDenied
	}
	digest, err := TranscriptDigest(transcript)
	if err != nil || !verifyProof(transcript.InitiatorProof, initiatorKey, digest) || !verifyProof(transcript.ResponderProof, responderKey, digest) {
		return authority.Principal{}, authority.Principal{}, ErrDenied
	}
	return initiator, responder, nil
}

func verifyProof(encoded string, key ed25519.PublicKey, digest string) bool {
	signature, err := base64.RawURLEncoding.DecodeString(encoded)
	return err == nil && len(signature) == ed25519.SignatureSize && base64.RawURLEncoding.EncodeToString(signature) == encoded && ed25519.Verify(key, append([]byte(proofDomain), []byte(digest)...), signature)
}

// ReplayCache is a bounded in-memory implementation of ReplayConsumer.
// Production stores should persist the bounded TTL entries across restart.
type ReplayCache struct {
	mu      sync.Mutex
	seen    map[string]time.Time
	maxSeen int
}

func NewReplayCache(maxEntries int) (*ReplayCache, error) {
	if maxEntries < 1 || maxEntries > 1_000_000 {
		return nil, ErrInvalid
	}
	return &ReplayCache{seen: make(map[string]time.Time), maxSeen: maxEntries}, nil
}

func (c *ReplayCache) Consume(hello Hello, resolve IdentityResolver, now time.Time) error {
	if c == nil || now.IsZero() {
		return ErrInvalid
	}
	if _, _, err := VerifyHello(hello, resolve, now); err != nil {
		return err
	}
	key := hello.IdentityID + ":" + hello.SessionID + ":" + hello.NodeID
	issued, _ := time.Parse(time.RFC3339, hello.IssuedAt)
	expires, _ := time.Parse(time.RFC3339, hello.ExpiresAt)
	if now.Before(issued.Add(-30*time.Second)) || !now.Before(expires) {
		return ErrStale
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for seenKey, until := range c.seen {
		if !now.Before(until) {
			delete(c.seen, seenKey)
		}
	}
	if _, exists := c.seen[key]; exists {
		return ErrReplay
	}
	if len(c.seen) >= c.maxSeen {
		return ErrReplayCapacity
	}
	c.seen[key] = expires
	return nil
}

func validateHelloShape(hello Hello) error {
	if hello.Version != HandshakeVersion || !identityID.MatchString(hello.IdentityID) || !hashValue.MatchString(hello.HeadHash) || !nodeID.MatchString(hello.NodeID) || !sessionID.MatchString(hello.SessionID) || !validBase64URL(hello.Nonce, 32) || hello.Signature != "" && !validBase64URL(hello.Signature, ed25519.SignatureSize) {
		return ErrInvalid
	}
	issued, errA := time.Parse(time.RFC3339, hello.IssuedAt)
	expires, errB := time.Parse(time.RFC3339, hello.ExpiresAt)
	if errA != nil || errB != nil || issued.Location() != time.UTC || expires.Location() != time.UTC || !strings.HasSuffix(hello.IssuedAt, "Z") || !strings.HasSuffix(hello.ExpiresAt, "Z") || issued.Format(time.RFC3339) != hello.IssuedAt || expires.Format(time.RFC3339) != hello.ExpiresAt || !expires.After(issued) || expires.Sub(issued) > MaxHandshakeTTL {
		return ErrInvalid
	}
	return nil
}

func validBase64URL(value string, size int) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == size && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func unsignedHelloFrom(hello Hello) unsignedHello {
	return unsignedHello{Version: hello.Version, IdentityID: hello.IdentityID, HeadHash: hello.HeadHash, NodeID: hello.NodeID, SessionID: hello.SessionID, Nonce: hello.Nonce, IssuedAt: hello.IssuedAt, ExpiresAt: hello.ExpiresAt}
}

func canonical(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalid
	}
	result, err := identityprotocol.Canonicalize(data)
	if err != nil || len(result) > MaxHandshakeBytes {
		return nil, ErrInvalid
	}
	return result, nil
}

// CanonicalHello serializes the signed Hello wire shape for signaling/data
// channel framing. It rejects non-canonical or oversized metadata.
func CanonicalHello(hello Hello) ([]byte, error) {
	if validateHelloShape(hello) != nil || hello.Signature == "" {
		return nil, ErrInvalid
	}
	return canonical(hello)
}
