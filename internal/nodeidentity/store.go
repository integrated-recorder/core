// Package nodeidentity stores the private key that identifies one Recorder
// installation. The key is deliberately separate from Global Identity
// recovery material and is never returned by this package.
package nodeidentity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

const (
	fileName                  = "node-key-v1.json"
	advertisementSequenceFile = "node-advertisement-sequence-v1.json"
	peerReplayFile            = "peer-handshake-replay-v1.json"
	maxFile                   = 4 << 10
	maxPeerReplayFile         = 1 << 20
	maxPeerReplayEntries      = 8192
	fileMode                  = 0600
	directoryMode             = 0700
)

var (
	ErrNotCreated = errors.New("node identity has not been created")
	ErrExists     = errors.New("node identity already exists")
	ErrCorrupt    = errors.New("node identity state is invalid")
	ErrInvalid    = errors.New("node identity input is invalid")
)

type storedKey struct {
	SchemaVersion int    `json:"schema_version"`
	NodeID        string `json:"node_id"`
	PublicKey     string `json:"public_key"`
	PrivateKey    string `json:"private_key"`
}

type advertisementSequenceState struct {
	SchemaVersion int    `json:"schema_version"`
	Sequence      string `json:"sequence"`
}

type peerReplayState struct {
	SchemaVersion int               `json:"schema_version"`
	Entries       []peerReplayEntry `json:"entries"`
}

type peerReplayEntry struct {
	Key       string `json:"key"`
	ExpiresAt string `json:"expires_at"`
}

// Info contains only public Node Key information.
type Info struct {
	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
}

// EnrollmentProof is safe to send to the identity owner for approval. It does
// not confer authority: a recovery-root-signed history event is still needed.
type EnrollmentProof struct {
	IdentityID   string    `json:"identity_id"`
	PreviousHash string    `json:"previous_event_hash"`
	NodeID       string    `json:"node_id"`
	PublicKey    string    `json:"node_public_key"`
	Nonce        string    `json:"enrollment_nonce"`
	Signature    string    `json:"node_proof"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// Store provides explicit create-once access to this installation's Node Key.
// Open never silently replaces missing or malformed state.
type Store struct {
	mu   sync.Mutex
	root string
}

// PeerReplayConsumer adapts the durable Node Store replay journal to the
// peerprotocol interface without naming the operation generically on Store.
type PeerReplayConsumer struct{ NodeStore *Store }

func (c PeerReplayConsumer) Consume(hello peerprotocol.Hello, resolve peerprotocol.IdentityResolver, now time.Time) error {
	if c.NodeStore == nil {
		return ErrInvalid
	}
	return c.NodeStore.ConsumePeerHello(hello, resolve, now)
}

// PeerSignalReplayConsumer adapts the Node Key store's durable replay journal
// to the signaling protocol's receive path.
type PeerSignalReplayConsumer struct{ NodeStore *Store }

func (c PeerSignalReplayConsumer) Consume(message signalingprotocol.Message, resolve signalingprotocol.IdentityResolver, now time.Time) error {
	if c.NodeStore == nil {
		return ErrInvalid
	}
	return c.NodeStore.ConsumePeerSignal(message, resolve, now)
}

// PeerCommandReplayConsumer adapts the Node Key store's durable replay journal
// to the authenticated peer-command receive path.
type PeerCommandReplayConsumer struct{ NodeStore *Store }

func (c PeerCommandReplayConsumer) ConsumeCommand(command peerprotocol.Command, resolve peerprotocol.IdentityResolver, now time.Time, sessionID string, remote authority.Principal, localIdentityID, localNodeID string) error {
	if c.NodeStore == nil {
		return ErrInvalid
	}
	return c.NodeStore.ConsumePeerCommand(command, resolve, now, sessionID, remote, localIdentityID, localNodeID)
}

func Open(dataRoot string) (*Store, error) {
	if strings.TrimSpace(dataRoot) == "" || !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot {
		return nil, ErrInvalid
	}
	securityDir := filepath.Join(dataRoot, "security")
	if err := os.MkdirAll(securityDir, directoryMode); err != nil {
		return nil, errors.New("node identity storage is unavailable")
	}
	info, err := os.Lstat(securityDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, ErrCorrupt
	}
	store := &Store{root: dataRoot}
	unlock, err := lockNodeIdentity(dataRoot)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, err := store.readLocked(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return store, nil
}

// Create generates and durably stores the one Node Key for this installation.
// It refuses to rotate or replace an existing key, including after corruption.
func (s *Store) Create() (Info, error) {
	if s == nil {
		return Info{}, ErrInvalid
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Info{}, errors.New("node identity key generation failed")
	}
	defer clear(privateKey)
	nodeID, err := identityprotocol.NodeID(publicKey)
	if err != nil {
		return Info{}, ErrInvalid
	}
	encoded := storedKey{
		SchemaVersion: 1,
		NodeID:        nodeID,
		PublicKey:     base64.RawURLEncoding.EncodeToString(publicKey),
		PrivateKey:    base64.RawURLEncoding.EncodeToString(privateKey),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return Info{}, err
	}
	defer unlock()
	if _, err := s.readLocked(); err == nil {
		return Info{}, ErrExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return Info{}, err
	}
	if err := s.writeLocked(encoded); err != nil {
		return Info{}, err
	}
	return Info{NodeID: nodeID, PublicKey: encoded.PublicKey}, nil
}

func (s *Store) Info() (Info, error) {
	key, err := s.read()
	if errors.Is(err, os.ErrNotExist) {
		return Info{}, ErrNotCreated
	}
	if err != nil {
		return Info{}, err
	}
	return Info{NodeID: key.NodeID, PublicKey: key.PublicKey}, nil
}

// SignReplicaCheckpoint signs a durable ciphertext-prefix receipt with this
// installation's Node Key. The caller cannot select another provider Node ID
// and the private key never leaves this package.
func (s *Store) SignReplicaCheckpoint(checkpoint lease.Checkpoint) (lease.SignedCheckpoint, error) {
	if s == nil {
		return lease.SignedCheckpoint{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedCheckpoint{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedCheckpoint{}, ErrNotCreated
		}
		return lease.SignedCheckpoint{}, err
	}
	checkpoint.ProviderNodeID = key.NodeID
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedCheckpoint{}, ErrCorrupt
	}
	defer clear(privateBytes)
	return lease.SignCheckpoint(checkpoint, ed25519.PrivateKey(privateBytes))
}

// SignReplicaOffer signs an owner-authorized replication offer with this
// installation's Node Key. The actor identity is always taken from protected
// local key state; callers cannot make this Node sign for another principal.
// The receiving lease store still validates the current grant chain and offer
// scope independently before persisting it.
func (s *Store) SignReplicaOffer(offer lease.Offer, chain []authority.SignedGrant) (lease.SignedOffer, error) {
	if s == nil {
		return lease.SignedOffer{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedOffer{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedOffer{}, ErrNotCreated
		}
		return lease.SignedOffer{}, err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedOffer{}, ErrCorrupt
	}
	defer clear(privateBytes)
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: key.NodeID}
	return lease.SignOffer(offer, actor, chain, ed25519.PrivateKey(privateBytes))
}

// SignReplicaAcceptance signs a provider's lease acceptance with this
// installation's Node Key. The provider Node ID always comes from protected
// local key state, never from a caller-supplied value.
func (s *Store) SignReplicaAcceptance(acceptance lease.Acceptance) (lease.SignedAcceptance, error) {
	if s == nil {
		return lease.SignedAcceptance{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedAcceptance{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedAcceptance{}, ErrNotCreated
		}
		return lease.SignedAcceptance{}, err
	}
	acceptance.ProviderNodeID = key.NodeID
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedAcceptance{}, ErrCorrupt
	}
	defer clear(privateBytes)
	return lease.SignAcceptance(acceptance, ed25519.PrivateKey(privateBytes))
}

// SignReplicaRenewalRequest signs an owner-authorized renewal request with
// this installation's Node Key. The lease Stores on both peers still verify
// current authority, grant evidence, expiry and the provider's independent
// receipt before committing the renewal.
func (s *Store) SignReplicaRenewalRequest(renewal lease.Renewal, chain []authority.SignedGrant) (lease.SignedRenewalRequest, error) {
	if s == nil {
		return lease.SignedRenewalRequest{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedRenewalRequest{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedRenewalRequest{}, ErrNotCreated
		}
		return lease.SignedRenewalRequest{}, err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedRenewalRequest{}, ErrCorrupt
	}
	defer clear(privateBytes)
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: key.NodeID}
	return lease.SignRenewalRequest(renewal, actor, chain, ed25519.PrivateKey(privateBytes))
}

// SignReplicaTombstone signs an archive-level deletion decision with this
// installation's Node Key. The caller must validate the current committed
// archive.delete capability before asking the Node to sign. The private key
// remains in protected local state.
func (s *Store) SignReplicaTombstone(tombstone lease.Tombstone, chain []authority.SignedGrant) (lease.SignedTombstone, error) {
	if s == nil {
		return lease.SignedTombstone{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedTombstone{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedTombstone{}, ErrNotCreated
		}
		return lease.SignedTombstone{}, err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedTombstone{}, ErrCorrupt
	}
	defer clear(privateBytes)
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: key.NodeID}
	return lease.SignTombstone(tombstone, tombstone.OwnerSubjectID, actor, chain, ed25519.PrivateKey(privateBytes))
}

// SignReplicaDeletionReport signs this provider's observed completion of one
// exact archive tombstone. It is a provider assertion, not proof of secure
// overwriting; the replica store performs the erasure before this is called.
func (s *Store) SignReplicaDeletionReport(report lease.DeletionReport) (lease.SignedDeletionReport, error) {
	if s == nil {
		return lease.SignedDeletionReport{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedDeletionReport{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedDeletionReport{}, ErrNotCreated
		}
		return lease.SignedDeletionReport{}, err
	}
	report.ProviderNodeID = key.NodeID
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedDeletionReport{}, ErrCorrupt
	}
	defer clear(privateBytes)
	return lease.SignDeletionReport(report, ed25519.PrivateKey(privateBytes))
}

// SignReplicaRenewalReceipt adds this installation's Node-Key signature to an
// owner-signed renewal request. The caller must first validate and persist the
// request through its durable lease Store; source and provider Stores verify
// both signatures and current authority independently.
func (s *Store) SignReplicaRenewalReceipt(request lease.SignedRenewalRequest) (lease.SignedRenewal, error) {
	if s == nil {
		return lease.SignedRenewal{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return lease.SignedRenewal{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return lease.SignedRenewal{}, ErrNotCreated
		}
		return lease.SignedRenewal{}, err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return lease.SignedRenewal{}, ErrCorrupt
	}
	defer clear(privateBytes)
	return lease.SignRenewalReceipt(request, ed25519.PrivateKey(privateBytes))
}

// SignPeerHello creates a short-lived challenge for an authenticated peer
// session using this installation's protected Node Key. The caller supplies
// the current identity history head obtained from its validated local store;
// this method never accepts a caller-selected Node ID or public key.
func (s *Store) SignPeerHello(identityID, identityHead string, now time.Time) (peerprotocol.Hello, error) {
	return s.signPeerHello(identityID, identityHead, "", now)
}

// SignPeerHelloForSession creates the responder challenge for the initiator's
// session identifier. The response still carries independent entropy and is
// signed only by this store's Node Key.
func (s *Store) SignPeerHelloForSession(identityID, identityHead, sessionID string, now time.Time) (peerprotocol.Hello, error) {
	if sessionID == "" {
		return peerprotocol.Hello{}, ErrInvalid
	}
	return s.signPeerHello(identityID, identityHead, sessionID, now)
}

func (s *Store) signPeerHello(identityID, identityHead, sessionID string, now time.Time) (peerprotocol.Hello, error) {
	if s == nil || identityID == "" || identityHead == "" || now.IsZero() {
		return peerprotocol.Hello{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return peerprotocol.Hello{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return peerprotocol.Hello{}, ErrNotCreated
		}
		return peerprotocol.Hello{}, err
	}
	hello, err := peerprotocol.NewHello(identityID, identityHead, key.NodeID, now, nil)
	if err != nil {
		return peerprotocol.Hello{}, err
	}
	if sessionID != "" {
		hello.SessionID = sessionID
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return peerprotocol.Hello{}, ErrCorrupt
	}
	defer clear(privateBytes)
	hello, err = peerprotocol.SignHello(hello, ed25519.PrivateKey(privateBytes))
	if err != nil {
		return peerprotocol.Hello{}, err
	}
	return hello, nil
}

// SignPeerTranscriptProof signs the exact mutual handshake transcript with
// this installation's Node Key. It does not reveal the key and refuses a
// transcript that does not contain this Node ID in either role.
func (s *Store) SignPeerTranscriptProof(transcript peerprotocol.Transcript) (string, error) {
	if s == nil {
		return "", ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return "", err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrNotCreated
		}
		return "", err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return "", ErrCorrupt
	}
	defer clear(privateBytes)
	return peerprotocol.SignTranscriptProof(transcript, key.NodeID, ed25519.PrivateKey(privateBytes))
}

// ConsumePeerHello durably consumes a previously signature- and freshness-
// verified peer challenge. The state contains only a domain-separated hash of
// public identity/session identifiers and the challenge expiry. It is
// atomically persisted under the Node Key lock so a Runtime Host restart
// cannot reopen the short Hello replay window.
func (s *Store) ConsumePeerHello(hello peerprotocol.Hello, resolve peerprotocol.IdentityResolver, now time.Time) error {
	if s == nil || now.IsZero() {
		return ErrInvalid
	}
	if _, _, err := peerprotocol.VerifyHello(hello, resolve, now); err != nil {
		return err
	}
	expires, err := time.Parse(time.RFC3339, hello.ExpiresAt)
	if err != nil || expires.Format(time.RFC3339) != hello.ExpiresAt || !expires.After(now) {
		return peerprotocol.ErrStale
	}
	digest := sha256.Sum256([]byte("IR/peer-handshake/replay/v1\x00" + hello.IdentityID + "\x00" + hello.SessionID + "\x00" + hello.NodeID))
	entry := peerReplayEntry{Key: hex.EncodeToString(digest[:]), ExpiresAt: hello.ExpiresAt}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := s.readLocked(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotCreated
		}
		return err
	}
	state, err := s.readPeerReplayLocked()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = peerReplayState{SchemaVersion: 1, Entries: []peerReplayEntry{}}
	}
	kept := make([]peerReplayEntry, 0, len(state.Entries)+1)
	for _, prior := range state.Entries {
		priorExpiry, parseErr := time.Parse(time.RFC3339, prior.ExpiresAt)
		if parseErr != nil || priorExpiry.After(now.Add(peerprotocol.MaxHandshakeTTL+30*time.Second)) {
			return ErrCorrupt
		}
		if now.Before(priorExpiry) {
			if prior.Key == entry.Key {
				return peerprotocol.ErrReplay
			}
			kept = append(kept, prior)
		}
	}
	if len(kept) >= maxPeerReplayEntries {
		return peerprotocol.ErrReplayCapacity
	}
	kept = append(kept, entry)
	sort.Slice(kept, func(i, j int) bool { return kept[i].Key < kept[j].Key })
	state.Entries = kept
	return s.writePeerReplayLocked(state)
}

// SignPeerSignal signs a rendezvous message with this installation's Node Key
// only after the resolver confirms fresh, unfrozen identity heads and active
// Node Key delegation for both source and target.
func (s *Store) SignPeerSignal(message signalingprotocol.Message, resolve signalingprotocol.IdentityResolver, now time.Time) (signalingprotocol.Message, error) {
	if s == nil || message.Signature != "" || now.IsZero() {
		return signalingprotocol.Message{}, ErrInvalid
	}
	if err := signalingprotocol.ValidateSigningAuthority(message, resolve, now); err != nil {
		return signalingprotocol.Message{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return signalingprotocol.Message{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return signalingprotocol.Message{}, ErrNotCreated
		}
		return signalingprotocol.Message{}, err
	}
	if message.SourceNodeID != key.NodeID {
		return signalingprotocol.Message{}, signalingprotocol.ErrDenied
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return signalingprotocol.Message{}, ErrCorrupt
	}
	defer clear(privateBytes)
	return signalingprotocol.Sign(message, ed25519.PrivateKey(privateBytes))
}

// SignPeerCommand signs one scope-bound command with this installation's Node
// Key only after both identity heads and source/target Node delegations are
// fresh. The private key remains inside the Node Store.
func (s *Store) SignPeerCommand(command peerprotocol.Command, resolve peerprotocol.IdentityResolver, now time.Time) (peerprotocol.Command, error) {
	if s == nil || now.IsZero() || command.Signature != "" {
		return peerprotocol.Command{}, ErrInvalid
	}
	if err := peerprotocol.ValidateCommandSigningAuthority(command, resolve, now); err != nil {
		return peerprotocol.Command{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return peerprotocol.Command{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return peerprotocol.Command{}, ErrNotCreated
		}
		return peerprotocol.Command{}, err
	}
	if command.SourceNodeID != key.NodeID {
		return peerprotocol.Command{}, peerprotocol.ErrCommandDenied
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return peerprotocol.Command{}, ErrCorrupt
	}
	defer clear(privateBytes)
	return peerprotocol.SignCommand(command, ed25519.PrivateKey(privateBytes))
}

// ConsumePeerSignal verifies current source and target Node Key authorization,
// checks this store is the addressed recipient, then durably consumes the
// one-use message ID. This shares the private replay journal with peer Hello
// challenges so restart cannot reopen either replay window.
func (s *Store) ConsumePeerSignal(message signalingprotocol.Message, resolve signalingprotocol.IdentityResolver, now time.Time) error {
	if s == nil || now.IsZero() {
		return ErrInvalid
	}
	if err := signalingprotocol.Verify(message, resolve, now); err != nil {
		return err
	}
	expires, err := time.Parse(time.RFC3339, message.ExpiresAt)
	if err != nil || expires.Format(time.RFC3339) != message.ExpiresAt || !expires.After(now) || expires.After(now.Add(signalingprotocol.MaxMessageTTL+signalingprotocol.MaxClockSkew)) {
		return signalingprotocol.ErrStale
	}
	digest := sha256.Sum256([]byte("IR/peer-signaling/replay/v1\x00" + message.SourceIdentityID + "\x00" + message.SourceNodeID + "\x00" + message.SessionID + "\x00" + message.MessageID))
	entry := peerReplayEntry{Key: hex.EncodeToString(digest[:]), ExpiresAt: message.ExpiresAt}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotCreated
		}
		return err
	}
	if message.TargetNodeID != key.NodeID {
		return signalingprotocol.ErrDenied
	}
	state, err := s.readPeerReplayLocked()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = peerReplayState{SchemaVersion: 1, Entries: []peerReplayEntry{}}
	}
	kept := make([]peerReplayEntry, 0, len(state.Entries)+1)
	for _, prior := range state.Entries {
		priorExpiry, parseErr := time.Parse(time.RFC3339, prior.ExpiresAt)
		if parseErr != nil || priorExpiry.After(now.Add(signalingprotocol.MaxMessageTTL+signalingprotocol.MaxClockSkew)) {
			return ErrCorrupt
		}
		if now.Before(priorExpiry) {
			if prior.Key == entry.Key {
				return signalingprotocol.ErrReplay
			}
			kept = append(kept, prior)
		}
	}
	if len(kept) >= maxPeerReplayEntries {
		return signalingprotocol.ErrReplayCapacity
	}
	kept = append(kept, entry)
	sort.Slice(kept, func(i, j int) bool { return kept[i].Key < kept[j].Key })
	state.Entries = kept
	return s.writePeerReplayLocked(state)
}

// ConsumePeerCommand verifies and durably consumes one signed, session-bound
// command. Only a hash of its public request/session identifiers is stored;
// payloads and capability contents are not copied into the replay journal.
func (s *Store) ConsumePeerCommand(command peerprotocol.Command, resolve peerprotocol.IdentityResolver, now time.Time, sessionID string, remote authority.Principal, localIdentityID, localNodeID string) error {
	if s == nil || now.IsZero() {
		return ErrInvalid
	}
	if err := peerprotocol.VerifyCommand(command, resolve, now, sessionID, remote, localIdentityID, localNodeID); err != nil {
		return err
	}
	expires, err := time.Parse(time.RFC3339, command.ExpiresAt)
	if err != nil || expires.Format(time.RFC3339) != command.ExpiresAt || !expires.After(now) || expires.After(now.Add(peerprotocol.MaxCommandTTL+30*time.Second)) {
		return peerprotocol.ErrCommandStale
	}
	digest := sha256.Sum256([]byte("IR/peer-command/replay/v1\x00" + command.SourceIdentityID + "\x00" + command.SourceNodeID + "\x00" + command.SessionID + "\x00" + command.RequestID + "\x00" + command.Nonce))
	entry := peerReplayEntry{Key: hex.EncodeToString(digest[:]), ExpiresAt: command.ExpiresAt}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotCreated
		}
		return err
	}
	if command.TargetNodeID != key.NodeID || command.TargetNodeID != localNodeID {
		return peerprotocol.ErrCommandDenied
	}
	state, err := s.readPeerReplayLocked()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		state = peerReplayState{SchemaVersion: 1, Entries: []peerReplayEntry{}}
	}
	kept := make([]peerReplayEntry, 0, len(state.Entries)+1)
	for _, prior := range state.Entries {
		priorExpiry, parseErr := time.Parse(time.RFC3339, prior.ExpiresAt)
		if parseErr != nil || priorExpiry.After(now.Add(signalingprotocol.MaxMessageTTL+signalingprotocol.MaxClockSkew)) {
			return ErrCorrupt
		}
		if now.Before(priorExpiry) {
			if prior.Key == entry.Key {
				return peerprotocol.ErrReplay
			}
			kept = append(kept, prior)
		}
	}
	if len(kept) >= maxPeerReplayEntries {
		return peerprotocol.ErrReplayCapacity
	}
	kept = append(kept, entry)
	sort.Slice(kept, func(i, j int) bool { return kept[i].Key < kept[j].Key })
	state.Entries = kept
	return s.writePeerReplayLocked(state)
}

func (s *Store) readPeerReplayLocked() (peerReplayState, error) {
	path := filepath.Join(s.root, "security", peerReplayFile)
	info, err := os.Lstat(path)
	if err != nil {
		return peerReplayState{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != fileMode || info.Size() <= 0 || info.Size() > maxPeerReplayFile {
		return peerReplayState{}, ErrCorrupt
	}
	f, err := os.Open(path)
	if err != nil {
		return peerReplayState{}, ErrCorrupt
	}
	opened, statErr := f.Stat()
	data, readErr := io.ReadAll(io.LimitReader(f, maxPeerReplayFile+1))
	closeErr := f.Close()
	if statErr != nil || !os.SameFile(info, opened) || readErr != nil || closeErr != nil || len(data) > maxPeerReplayFile {
		return peerReplayState{}, ErrCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state peerReplayState
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF || state.SchemaVersion != 1 || state.Entries == nil || len(state.Entries) > maxPeerReplayEntries {
		return peerReplayState{}, ErrCorrupt
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(canonical, data) {
		return peerReplayState{}, ErrCorrupt
	}
	previous := ""
	for _, entry := range state.Entries {
		if len(entry.Key) != sha256.Size*2 || entry.Key <= previous {
			return peerReplayState{}, ErrCorrupt
		}
		if _, err := hex.DecodeString(entry.Key); err != nil {
			return peerReplayState{}, ErrCorrupt
		}
		expires, err := time.Parse(time.RFC3339, entry.ExpiresAt)
		if err != nil || expires.Location() != time.UTC || expires.Format(time.RFC3339) != entry.ExpiresAt || !strings.HasSuffix(entry.ExpiresAt, "Z") {
			return peerReplayState{}, ErrCorrupt
		}
		previous = entry.Key
	}
	return state, nil
}

func (s *Store) writePeerReplayLocked(state peerReplayState) error {
	if state.SchemaVersion != 1 || state.Entries == nil || len(state.Entries) > maxPeerReplayEntries {
		return ErrCorrupt
	}
	previous := ""
	for _, entry := range state.Entries {
		if len(entry.Key) != sha256.Size*2 || entry.Key <= previous {
			return ErrCorrupt
		}
		if _, err := hex.DecodeString(entry.Key); err != nil {
			return ErrCorrupt
		}
		expires, err := time.Parse(time.RFC3339, entry.ExpiresAt)
		if err != nil || expires.Location() != time.UTC || expires.Format(time.RFC3339) != entry.ExpiresAt || !strings.HasSuffix(entry.ExpiresAt, "Z") {
			return ErrCorrupt
		}
		previous = entry.Key
	}
	data, err := json.Marshal(state)
	if err != nil || len(data) > maxPeerReplayFile {
		return ErrCorrupt
	}
	path := filepath.Join(s.root, "security", peerReplayFile)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".peer-handshake-replay-*.tmp")
	if err != nil {
		return errors.New("peer handshake replay state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return errors.New("peer handshake replay permissions could not be set")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("peer handshake replay state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("peer handshake replay state could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("peer handshake replay state could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("peer handshake replay state could not be committed")
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("peer handshake replay directory could not be opened")
	}
	syncErr, closeErr := directory.Sync(), directory.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("peer handshake replay directory could not be synchronized")
	}
	return nil
}

// SignAdvertisement signs an advertisement for this installation and
// durably advances its sequence before returning the signature. Callers cannot
// choose another Node ID or public key. A sequence gap after a crash is safe;
// sequence reuse is not.
func (s *Store) SignAdvertisement(ad discoveryprotocol.Advertisement) ([]byte, []byte, error) {
	if s == nil {
		return nil, nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrNotCreated
		}
		return nil, nil, err
	}
	sequence, err := s.readAdvertisementSequenceLocked()
	if err != nil {
		return nil, nil, err
	}
	next, err := incrementDecimal(sequence)
	if err != nil {
		return nil, nil, err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return nil, nil, ErrCorrupt
	}
	defer clear(privateBytes)
	ad.NodeID = key.NodeID
	ad.NodePublicKey = key.PublicKey
	ad.Sequence = next
	canonical, signature, err := discoveryprotocol.Sign(ad, ed25519.PrivateKey(privateBytes))
	if err != nil {
		return nil, nil, err
	}
	if err := s.writeAdvertisementSequenceLocked(advertisementSequenceState{SchemaVersion: 1, Sequence: next}); err != nil {
		return nil, nil, err
	}
	return canonical, signature, nil
}

func (s *Store) readAdvertisementSequenceLocked() (string, error) {
	path := filepath.Join(s.root, "security", advertisementSequenceFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "0", nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != fileMode {
		return "", ErrCorrupt
	}
	f, err := os.Open(path)
	if err != nil {
		return "", ErrCorrupt
	}
	opened, statErr := f.Stat()
	data, readErr := io.ReadAll(io.LimitReader(f, 129))
	closeErr := f.Close()
	if statErr != nil || !os.SameFile(info, opened) || readErr != nil || closeErr != nil || len(data) > 128 {
		return "", ErrCorrupt
	}
	var state advertisementSequenceState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF || state.SchemaVersion != 1 || !validDecimal(state.Sequence) {
		return "", ErrCorrupt
	}
	return state.Sequence, nil
}

func (s *Store) writeAdvertisementSequenceLocked(state advertisementSequenceState) error {
	if state.SchemaVersion != 1 || !validDecimal(state.Sequence) {
		return ErrCorrupt
	}
	data, err := json.Marshal(state)
	if err != nil {
		return ErrCorrupt
	}
	path := filepath.Join(s.root, "security", advertisementSequenceFile)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".node-advertisement-sequence-*.tmp")
	if err != nil {
		return errors.New("node advertisement state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return errors.New("node advertisement permissions could not be set")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("node advertisement state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("node advertisement state could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("node advertisement state could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("node advertisement state could not be committed")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("node advertisement directory could not be opened")
	}
	syncErr, closeErr := dir.Sync(), dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("node advertisement directory could not be synchronized")
	}
	return nil
}

func incrementDecimal(value string) (string, error) {
	if !validDecimal(value) || len(value) >= 20 && value == "99999999999999999999" {
		return "", ErrCorrupt
	}
	carry := byte(1)
	bytes := []byte(value)
	for i := len(bytes) - 1; i >= 0 && carry == 1; i-- {
		if bytes[i] == '9' {
			bytes[i] = '0'
		} else {
			bytes[i]++
			carry = 0
		}
	}
	if carry == 1 {
		bytes = append([]byte{'1'}, bytes...)
	}
	return string(bytes), nil
}

func validDecimal(value string) bool {
	if value == "" || len(value) > 20 || len(value) > 1 && value[0] == '0' {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *Store) read() (storedKey, error) {
	if s == nil {
		return storedKey{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return storedKey{}, err
	}
	defer unlock()
	return s.readLocked()
}

func (s *Store) readLocked() (storedKey, error) {
	path := s.path()
	info, err := os.Lstat(path)
	if err != nil {
		return storedKey{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != fileMode {
		return storedKey{}, ErrCorrupt
	}
	f, err := os.Open(path)
	if err != nil {
		return storedKey{}, ErrCorrupt
	}
	opened, statErr := f.Stat()
	data, readErr := io.ReadAll(io.LimitReader(f, maxFile+1))
	closeErr := f.Close()
	if statErr != nil || !os.SameFile(info, opened) || readErr != nil || closeErr != nil || len(data) > maxFile {
		return storedKey{}, ErrCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var key storedKey
	if decoder.Decode(&key) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateKey(key) != nil {
		return storedKey{}, ErrCorrupt
	}
	return key, nil
}

func validateKey(key storedKey) error {
	if key.SchemaVersion != 1 || !identityprotocol.IsNodeID(key.NodeID) {
		return ErrCorrupt
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(key.PublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(publicKey) != key.PublicKey {
		return ErrCorrupt
	}
	privateKey, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(privateKey) != ed25519.PrivateKeySize || base64.RawURLEncoding.EncodeToString(privateKey) != key.PrivateKey {
		return ErrCorrupt
	}
	derivedID, err := identityprotocol.NodeID(ed25519.PublicKey(publicKey))
	if err != nil || derivedID != key.NodeID || !bytes.Equal(ed25519.PrivateKey(privateKey).Public().(ed25519.PublicKey), publicKey) {
		return ErrCorrupt
	}
	return nil
}

func (s *Store) writeLocked(key storedKey) error {
	if validateKey(key) != nil {
		return ErrCorrupt
	}
	data, err := json.Marshal(key)
	if err != nil || len(data) > maxFile {
		return errors.New("node identity state could not be encoded")
	}
	defer clear(data)
	path := s.path()
	if _, err := os.Lstat(path); err == nil {
		return ErrExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("node identity state could not be inspected")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".node-key-*.tmp")
	if err != nil {
		return errors.New("node identity state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return errors.New("node identity permissions could not be set")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("node identity state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("node identity state could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("node identity state could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("node identity state could not be committed")
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("node identity directory could not be opened")
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("node identity directory could not be synchronized")
	}
	return nil
}

func (s *Store) path() string { return filepath.Join(s.root, "security", fileName) }
