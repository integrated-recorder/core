// Package globalidentity stores public global-identity history and WebAuthn
// credentials separately from RC12 local accounts. Recovery secrets and
// private keys are never accepted by this package or persisted here.
package globalidentity

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	SchemaVersion   = 2
	storeFilename   = "global-identity-v1.json"
	maxStoreBytes   = 4 << 20
	maxForkEvidence = 16
)

var (
	ErrAlreadyInitialized = errors.New("global identity is already initialized")
	ErrNotInitialized     = errors.New("global identity is not initialized")
	ErrCorruptStore       = errors.New("global identity store is invalid")
	ErrInvalidGenesis     = errors.New("global identity genesis is invalid")
	ErrInvalidEvent       = errors.New("global identity event is invalid")
	ErrCredentialNotFound = errors.New("global identity credential was not found")
	ErrIdentityFrozen     = errors.New("global identity is frozen pending fork resolution")
	ErrIdentityFork       = errors.New("global identity fork detected and frozen")
	ErrForkEvidenceLimit  = errors.New("global identity fork evidence limit reached")
)

type signedEvent struct {
	Canonical []byte `json:"canonical_event"`
	Signature []byte `json:"signature"`
}

type forkEvidence struct {
	PreviousHash string      `json:"previous_event_hash"`
	First        signedEvent `json:"first_event"`
	Second       signedEvent `json:"second_event"`
	// Branches preserve the complete signed suffix observed on each side when
	// the fork was detected. First/Second remain the direct competing events
	// for v1 compatibility and stable fork identity.
	FirstBranch  []signedEvent `json:"first_branch,omitempty"`
	SecondBranch []signedEvent `json:"second_branch,omitempty"`
}

type record struct {
	SchemaVersion           int                   `json:"schema_version"`
	CredentialInventoryMode string                `json:"credential_inventory_mode,omitempty"`
	IdentityID              string                `json:"identity_id"`
	UserHandle              []byte                `json:"user_handle"`
	History                 []signedEvent         `json:"history"`
	Credentials             []webauthn.Credential `json:"credentials"`
	Frozen                  bool                  `json:"frozen,omitempty"`
	ForkEvidence            []forkEvidence        `json:"fork_evidence,omitempty"`
	CreatedAt               time.Time             `json:"created_at"`
}

// Snapshot is the intentionally limited public projection of persisted
// identity state. RecoveryEncryptionPublicKey is public genesis material;
// private recovery keys, user handles, credential public keys, and attestation
// records are never included.
type Snapshot struct {
	IdentityID                  string    `json:"identity_id"`
	Sequence                    string    `json:"sequence"`
	HeadHash                    string    `json:"head_hash"`
	RecoveryEncryptionPublicKey string    `json:"recovery_encryption_public_key"`
	CreatedAt                   time.Time `json:"created_at"`
	CredentialCount             int       `json:"credential_count"`
	CredentialIDs               []string  `json:"credential_ids"`
	Frozen                      bool      `json:"frozen"`
	ForkCount                   int       `json:"fork_count"`
	NodeIDs                     []string  `json:"node_ids"`
}

// ActiveNodeKey is a public Node Key projection for local authorization
// consumers. It contains no private key, credential, or recovery material.
type ActiveNodeKey struct {
	NodeID    string
	PublicKey ed25519.PublicKey
}

// AuthorityProjection is derived from the fully verified durable history.
// It is an in-process trust input, not a wire object to accept from a peer.
type AuthorityProjection struct {
	IdentityID                  string
	HeadHash                    string
	RootPublicKey               ed25519.PublicKey
	RecoveryEncryptionPublicKey []byte
	ActiveNodes                 []ActiveNodeKey
	Frozen                      bool
	VerifiedAt                  time.Time
}

type Store struct {
	mu   sync.Mutex
	root string
}

func validHistoryHash(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func Open(dataRoot string) (*Store, error) {
	if strings.TrimSpace(dataRoot) == "" || !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot {
		return nil, errors.New("global identity data root is invalid")
	}
	securityDir := filepath.Join(dataRoot, "security")
	if err := os.MkdirAll(securityDir, 0700); err != nil {
		return nil, errors.New("global identity storage is unavailable")
	}
	info, err := os.Lstat(securityDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, ErrCorruptStore
	}
	store := &Store{root: dataRoot}
	unlock, err := lockGlobalIdentity(dataRoot)
	if err != nil {
		return nil, err
	}
	defer unlock()
	stored, err := store.readLocked()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil && stored.SchemaVersion == 1 {
		stored.SchemaVersion = SchemaVersion
		stored.CredentialInventoryMode = "complete"
		if err := store.writeLocked(stored); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *Store) Snapshot() (Snapshot, bool, error) {
	if s == nil {
		return Snapshot{}, false, ErrCorruptStore
	}
	var snapshot Snapshot
	var exists bool
	if err := s.withSnapshot(func(value Snapshot, found bool) {
		snapshot = value
		exists = found
	}); err != nil {
		return Snapshot{}, false, err
	}
	return snapshot, exists, nil
}

// AuthorityProjection returns the public root and currently delegated Node
// Keys from one locked, fully validated history read. Recovery private keys,
// WebAuthn credential records and user handles are deliberately excluded.
func (s *Store) AuthorityProjection() (AuthorityProjection, error) {
	if s == nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return AuthorityProjection{}, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return AuthorityProjection{}, err
	}
	genesis, err := identityprotocol.VerifyEvent(stored.History[0].Canonical, stored.History[0].Signature)
	if err != nil || genesis.Operation != "genesis" {
		return AuthorityProjection{}, ErrCorruptStore
	}
	head, err := identityprotocol.VerifyEvent(stored.History[len(stored.History)-1].Canonical, stored.History[len(stored.History)-1].Signature)
	if err != nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	activeNodes, err := activeNodePublicKeysAt(stored.History)
	if err != nil {
		return AuthorityProjection{}, err
	}
	recoveryKey, err := recoveryEncryptionPublicKey(genesis.Payload)
	if err != nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	nodes := make([]ActiveNodeKey, 0, len(activeNodes))
	for nodeID, publicKey := range activeNodes {
		nodes = append(nodes, ActiveNodeKey{NodeID: nodeID, PublicKey: append(ed25519.PublicKey(nil), publicKey...)})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	return AuthorityProjection{
		IdentityID: stored.IdentityID, HeadHash: head.EventHash,
		RootPublicKey:               append(ed25519.PublicKey(nil), genesis.SignerPublicKey...),
		RecoveryEncryptionPublicKey: append([]byte(nil), recoveryKey...),
		ActiveNodes:                 nodes, Frozen: stored.Frozen, VerifiedAt: time.Now().UTC(),
	}, nil
}

// AuthorityProjectionAt returns the key set that was authorized at a known
// event head. The whole durable history is validated before the prefix is
// projected. It is intended to verify historical signatures in another
// append-only log; callers still need the current projection to decide
// whether a sensitive operation is presently allowed.
func (s *Store) AuthorityProjectionAt(headHash string) (AuthorityProjection, error) {
	if s == nil || !validHistoryHash(headHash) {
		return AuthorityProjection{}, ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return AuthorityProjection{}, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return AuthorityProjection{}, err
	}
	position := -1
	for i := range stored.History {
		if identityprotocol.EventHash(stored.History[i].Canonical) == headHash {
			position = i
			break
		}
	}
	if position < 0 {
		return AuthorityProjection{}, ErrHistoryInvalid
	}
	prefix := stored.History[:position+1]
	genesis, err := identityprotocol.VerifyEvent(prefix[0].Canonical, prefix[0].Signature)
	if err != nil || genesis.Operation != "genesis" {
		return AuthorityProjection{}, ErrCorruptStore
	}
	head, err := identityprotocol.VerifyEvent(prefix[len(prefix)-1].Canonical, prefix[len(prefix)-1].Signature)
	if err != nil || head.EventHash != headHash {
		return AuthorityProjection{}, ErrCorruptStore
	}
	activeNodes, err := activeNodePublicKeysAt(prefix)
	if err != nil {
		return AuthorityProjection{}, err
	}
	recoveryKey, err := recoveryEncryptionPublicKey(genesis.Payload)
	if err != nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	nodes := make([]ActiveNodeKey, 0, len(activeNodes))
	for nodeID, publicKey := range activeNodes {
		nodes = append(nodes, ActiveNodeKey{NodeID: nodeID, PublicKey: append(ed25519.PublicKey(nil), publicKey...)})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	return AuthorityProjection{
		IdentityID: stored.IdentityID, HeadHash: head.EventHash,
		RootPublicKey:               append(ed25519.PublicKey(nil), genesis.SignerPublicKey...),
		RecoveryEncryptionPublicKey: append([]byte(nil), recoveryKey...),
		ActiveNodes:                 nodes, Frozen: stored.Frozen, VerifiedAt: time.Now().UTC(),
	}, nil
}

// withSnapshot runs a short read-only callback while the durable identity
// lock is held. Authorization consumers use this to make a decision at one
// linearization point with respect to history imports and local mutations.
// The callback must not call back into Store.
func (s *Store) withSnapshot(callback func(Snapshot, bool)) error {
	if s == nil || callback == nil {
		return ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		callback(Snapshot{}, false)
		return nil
	}
	if err != nil {
		return err
	}
	head, _ := identityprotocol.VerifyEvent(stored.History[len(stored.History)-1].Canonical, stored.History[len(stored.History)-1].Signature)
	genesis, _ := identityprotocol.VerifyEvent(stored.History[0].Canonical, stored.History[0].Signature)
	recoveryEncryptionPublicKey, _ := genesis.Payload["recovery_encryption_public_key"].(string)
	credentialIDs := make([]string, 0, len(stored.Credentials))
	for _, credential := range stored.Credentials {
		credentialIDs = append(credentialIDs, base64.RawURLEncoding.EncodeToString(credential.ID))
	}
	nodeIDs, err := activeNodeIDs(stored)
	if err != nil {
		return err
	}
	callback(Snapshot{IdentityID: stored.IdentityID, Sequence: head.Sequence, HeadHash: head.EventHash, RecoveryEncryptionPublicKey: recoveryEncryptionPublicKey, CreatedAt: stored.CreatedAt, CredentialCount: len(stored.Credentials), CredentialIDs: credentialIDs, Frozen: stored.Frozen, ForkCount: len(stored.ForkEvidence), NodeIDs: nodeIDs}, true)
	return nil
}

func (s *Store) CreateGenesis(canonicalEvent, signature, userHandle []byte, credential webauthn.Credential, expectedChallenge string) error {
	if s == nil {
		return ErrCorruptStore
	}
	verified, err := identityprotocol.VerifyEvent(canonicalEvent, signature)
	if err != nil || verified.Operation != "genesis" || verified.Sequence != "0" || verified.PreviousHash != nil {
		return ErrInvalidGenesis
	}
	if len(userHandle) != 32 || len(credential.ID) == 0 || len(credential.PublicKey) == 0 || expectedChallenge == "" {
		return ErrInvalidGenesis
	}
	credentialID := base64.RawURLEncoding.EncodeToString(credential.ID)
	if !validGenesisPayload(verified.Payload, expectedChallenge, credentialID) {
		return ErrInvalidGenesis
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	if _, err := s.readLocked(); err == nil {
		return ErrAlreadyInitialized
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	stored := record{
		SchemaVersion:           SchemaVersion,
		CredentialInventoryMode: "complete",
		IdentityID:              verified.IdentityID,
		UserHandle:              append([]byte(nil), userHandle...),
		History:                 []signedEvent{{Canonical: append([]byte(nil), canonicalEvent...), Signature: append([]byte(nil), signature...)}},
		Credentials:             []webauthn.Credential{credential},
		CreatedAt:               time.Now().UTC(),
	}
	return s.writeLocked(stored)
}

func (s *Store) LoadForWebAuthn() (string, []byte, []webauthn.Credential, error) {
	if s == nil {
		return "", nil, nil, ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return "", nil, nil, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return "", nil, nil, ErrNotInitialized
	}
	if err != nil {
		return "", nil, nil, err
	}
	return stored.IdentityID, append([]byte(nil), stored.UserHandle...), cloneCredentials(stored.Credentials), nil
}

func (s *Store) WebAuthnUser(identityID string) (webauthn.User, error) {
	if s == nil {
		return nil, ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return nil, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotInitialized
	}
	if err != nil {
		return nil, err
	}
	if identityID != "" && stored.IdentityID != identityID {
		return nil, ErrNotInitialized
	}
	return stored.webAuthnUser(), nil
}

func (s *Store) ReplaceCredential(updated webauthn.Credential) error {
	if s == nil || len(updated.ID) == 0 || len(updated.PublicKey) == 0 {
		return ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return err
	}
	for index, existing := range stored.Credentials {
		if bytes.Equal(existing.ID, updated.ID) {
			stored.Credentials[index] = updated
			return s.writeLocked(stored)
		}
	}
	return ErrCredentialNotFound
}

// AddCredential appends a recovery-root-signed credential_add event only
// after the registration ceremony has verified the new WebAuthn credential.
func (s *Store) AddCredential(canonicalEvent, signature []byte, expectedChallenge string, credential webauthn.Credential) error {
	if s == nil || len(credential.ID) == 0 || len(credential.PublicKey) == 0 {
		return ErrInvalidEvent
	}
	verified, err := identityprotocol.VerifyEvent(canonicalEvent, signature)
	if err != nil || verified.Operation != "credential_add" || stringPayload(verified.Payload, "webauthn_challenge") != expectedChallenge || stringPayload(verified.Payload, "credential_id") != base64.RawURLEncoding.EncodeToString(credential.ID) {
		return ErrInvalidEvent
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return err
	}
	if stored.Frozen {
		return ErrIdentityFrozen
	}
	if err := s.validateAndAppendOrFreeze(&stored, verified); err != nil {
		return err
	}
	for _, existing := range stored.Credentials {
		if bytes.Equal(existing.ID, credential.ID) {
			return ErrInvalidEvent
		}
	}
	stored.Credentials = append(stored.Credentials, credential)
	return s.writeLocked(stored)
}

// RevokeCredential appends a recovery-root-signed credential_revoke event.
// It deliberately does not require a WebAuthn session: a lost authenticator
// is exactly when offline recovery must be able to revoke it.
func (s *Store) RevokeCredential(canonicalEvent, signature []byte, expectedCredentialID string) error {
	if s == nil {
		return ErrInvalidEvent
	}
	verified, err := identityprotocol.VerifyEvent(canonicalEvent, signature)
	if err != nil || verified.Operation != "credential_revoke" || stringPayload(verified.Payload, "credential_id") != expectedCredentialID {
		return ErrInvalidEvent
	}
	credentialID, err := base64.RawURLEncoding.DecodeString(stringPayload(verified.Payload, "credential_id"))
	if err != nil || len(credentialID) == 0 || base64.RawURLEncoding.EncodeToString(credentialID) != stringPayload(verified.Payload, "credential_id") {
		return ErrInvalidEvent
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return err
	}
	if stored.Frozen {
		return ErrIdentityFrozen
	}
	if err := s.validateAndAppendOrFreeze(&stored, verified); err != nil {
		return err
	}
	for index, existing := range stored.Credentials {
		if bytes.Equal(existing.ID, credentialID) {
			stored.Credentials = append(stored.Credentials[:index], stored.Credentials[index+1:]...)
			break
		}
	}
	return s.writeLocked(stored)
}

// ApplyNodeEvent commits a recovery-root-authorized node_delegate or
// node_revoke event. If a second valid successor is observed for an already
// accepted parent, both signed events are durably retained and the identity
// is frozen. It never chooses a branch by timestamp or arrival order.
func (s *Store) ApplyNodeEvent(canonicalEvent, signature []byte) error {
	if s == nil {
		return ErrInvalidEvent
	}
	verified, err := identityprotocol.VerifyEvent(canonicalEvent, signature)
	if err != nil || (verified.Operation != "node_delegate" && verified.Operation != "node_revoke") {
		return ErrInvalidEvent
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return err
	}
	if verified.IdentityID != stored.IdentityID {
		return ErrInvalidEvent
	}
	if eventKnown(stored, verified.EventHash) {
		return nil
	}
	if err := validateNodeSuccessor(stored, verified); err != nil {
		return err
	}
	parent := *verified.PreviousHash
	head, _ := identityprotocol.VerifyEvent(stored.History[len(stored.History)-1].Canonical, stored.History[len(stored.History)-1].Signature)
	if parent == head.EventHash {
		if stored.Frozen {
			return ErrIdentityFrozen
		}
		stored.History = append(stored.History, signedEvent{Canonical: append([]byte(nil), canonicalEvent...), Signature: append([]byte(nil), signature...)})
		return s.writeLocked(stored)
	}

	// A known non-head parent must already have one accepted child on the
	// linear history. A different valid child proves a fork.
	first := childForParent(stored.History, parent)
	if first == nil {
		return ErrInvalidEvent
	}
	if len(stored.ForkEvidence) >= maxForkEvidence {
		stored.Frozen = true
		if err := s.writeLocked(stored); err != nil {
			return err
		}
		return ErrForkEvidenceLimit
	}
	stored.ForkEvidence = append(stored.ForkEvidence, forkEvidence{
		PreviousHash: parent,
		First:        cloneSignedEvent(*first),
		Second:       signedEvent{Canonical: append([]byte(nil), canonicalEvent...), Signature: append([]byte(nil), signature...)},
		FirstBranch:  branchAfterParent(stored.History, parent),
		SecondBranch: []signedEvent{{Canonical: append([]byte(nil), canonicalEvent...), Signature: append([]byte(nil), signature...)}},
	})
	stored.Frozen = true
	if err := s.writeLocked(stored); err != nil {
		return err
	}
	return ErrIdentityFork
}

func eventKnown(stored record, eventHash string) bool {
	for _, entry := range stored.History {
		event, err := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
		if err == nil && event.EventHash == eventHash {
			return true
		}
	}
	for _, fork := range stored.ForkEvidence {
		for _, entry := range []signedEvent{fork.First, fork.Second} {
			event, err := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
			if err == nil && event.EventHash == eventHash {
				return true
			}
		}
	}
	return false
}

func childForParent(history []signedEvent, parent string) *signedEvent {
	for index := range history {
		event, err := identityprotocol.VerifyEvent(history[index].Canonical, history[index].Signature)
		if err == nil && event.PreviousHash != nil && *event.PreviousHash == parent {
			return &history[index]
		}
	}
	return nil
}

func validateNodeSuccessor(stored record, event identityprotocol.Event) error {
	if event.PreviousHash == nil || event.IdentityID != stored.IdentityID {
		return ErrInvalidEvent
	}
	parentIndex := -1
	for index, entry := range stored.History {
		parent, err := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
		if err == nil && parent.EventHash == *event.PreviousHash {
			parentIndex = index
			if err := validateEventSuccessor(parent, event); err != nil {
				return err
			}
			break
		}
	}
	if parentIndex < 0 {
		return ErrInvalidEvent
	}
	root, err := identityprotocol.VerifyEvent(stored.History[0].Canonical, stored.History[0].Signature)
	if err != nil || !bytes.Equal(event.SignerPublicKey, root.SignerPublicKey) {
		return ErrInvalidEvent
	}
	active, err := activeNodesAt(stored.History[:parentIndex+1])
	if err != nil {
		return ErrCorruptStore
	}
	activeSet := make(map[string]struct{}, len(active))
	for _, nodeID := range active {
		activeSet[nodeID] = struct{}{}
	}
	switch event.Operation {
	case "node_delegate":
		if len(event.Payload) != 4 {
			return ErrInvalidEvent
		}
		nodeID := stringPayload(event.Payload, "node_id")
		publicText := stringPayload(event.Payload, "node_public_key")
		nonce := stringPayload(event.Payload, "enrollment_nonce")
		proofText := stringPayload(event.Payload, "node_proof")
		publicKey, decodeErr := base64.RawURLEncoding.DecodeString(publicText)
		proof, proofErr := base64.RawURLEncoding.DecodeString(proofText)
		derivedNodeID, nodeErr := identityprotocol.NodeID(ed25519.PublicKey(publicKey))
		message, messageErr := identityprotocol.NodeEnrollmentProofMessage(event.IdentityID, *event.PreviousHash, nonce)
		if decodeErr != nil || proofErr != nil || nodeErr != nil || messageErr != nil || len(proof) != ed25519.SignatureSize || len(publicKey) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(publicKey) != publicText || base64.RawURLEncoding.EncodeToString(proof) != proofText || derivedNodeID != nodeID || !ed25519.Verify(ed25519.PublicKey(publicKey), message, proof) {
			return ErrInvalidEvent
		}
		if _, exists := activeSet[nodeID]; exists {
			return ErrInvalidEvent
		}
	case "node_revoke":
		if len(event.Payload) != 1 {
			return ErrInvalidEvent
		}
		nodeID := stringPayload(event.Payload, "node_id")
		if !identityprotocol.IsNodeID(nodeID) {
			return ErrInvalidEvent
		}
		if _, exists := activeSet[nodeID]; !exists {
			return ErrInvalidEvent
		}
	default:
		return ErrInvalidEvent
	}
	return nil
}

func activeNodeIDs(stored record) ([]string, error) {
	return activeNodesAt(stored.History)
}

func activeNodesAt(history []signedEvent) ([]string, error) {
	keys, err := activeNodePublicKeysAt(history)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func activeNodePublicKeysAt(history []signedEvent) (map[string]ed25519.PublicKey, error) {
	nodes := make(map[string]ed25519.PublicKey)
	for index, entry := range history {
		if index == 0 {
			continue
		}
		event, err := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
		if err != nil {
			return nil, ErrCorruptStore
		}
		switch event.Operation {
		case "node_delegate":
			id := stringPayload(event.Payload, "node_id")
			publicText := stringPayload(event.Payload, "node_public_key")
			publicKey, decodeErr := base64.RawURLEncoding.DecodeString(publicText)
			derivedID, idErr := identityprotocol.NodeID(ed25519.PublicKey(publicKey))
			if decodeErr != nil || idErr != nil || derivedID != id || len(publicKey) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(publicKey) != publicText {
				return nil, ErrCorruptStore
			}
			if _, exists := nodes[id]; exists {
				return nil, ErrCorruptStore
			}
			nodes[id] = ed25519.PublicKey(append([]byte(nil), publicKey...))
		case "node_revoke":
			id := stringPayload(event.Payload, "node_id")
			if _, exists := nodes[id]; !exists {
				return nil, ErrCorruptStore
			}
			delete(nodes, id)
		}
	}
	return nodes, nil
}

func cloneSignedEvent(event signedEvent) signedEvent {
	return signedEvent{Canonical: append([]byte(nil), event.Canonical...), Signature: append([]byte(nil), event.Signature...)}
}

func (s *Store) readLocked() (record, error) {
	path := s.storePath()
	info, err := os.Lstat(path)
	if err != nil {
		return record{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return record{}, ErrCorruptStore
	}
	f, err := os.Open(path)
	if err != nil {
		return record{}, ErrCorruptStore
	}
	opened, statErr := f.Stat()
	if statErr != nil || !os.SameFile(info, opened) {
		_ = f.Close()
		return record{}, ErrCorruptStore
	}
	data, readErr := io.ReadAll(io.LimitReader(f, maxStoreBytes+1))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || len(data) > maxStoreBytes {
		return record{}, ErrCorruptStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var stored record
	if err := decoder.Decode(&stored); err != nil {
		return record{}, ErrCorruptStore
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return record{}, ErrCorruptStore
	}
	if err := validateRecord(stored); err != nil {
		return record{}, err
	}
	return stored, nil
}

func validateRecord(stored record) error {
	if (stored.SchemaVersion != 1 && stored.SchemaVersion != SchemaVersion) || len(stored.UserHandle) != 32 || len(stored.History) == 0 || stored.CreatedAt.IsZero() {
		return ErrCorruptStore
	}
	inventoryMode := stored.CredentialInventoryMode
	if stored.SchemaVersion == 1 {
		if inventoryMode != "" {
			return ErrCorruptStore
		}
		inventoryMode = "complete"
	} else if inventoryMode != "complete" && inventoryMode != "local" {
		return ErrCorruptStore
	}
	genesis, err := identityprotocol.VerifyEvent(stored.History[0].Canonical, stored.History[0].Signature)
	if err != nil || genesis.Operation != "genesis" || genesis.IdentityID != stored.IdentityID || !validGenesisPayload(genesis.Payload, "", "") {
		return ErrCorruptStore
	}
	if len(stored.ForkEvidence) > maxForkEvidence {
		return ErrCorruptStore
	}
	for _, fork := range stored.ForkEvidence {
		if err := validateStoredFork(stored, fork); err != nil {
			return ErrCorruptStore
		}
	}
	previous := genesis
	for index, signed := range stored.History[1:] {
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err != nil || validateEventSuccessor(previous, event) != nil || !bytes.Equal(event.SignerPublicKey, genesis.SignerPublicKey) {
			return ErrCorruptStore
		}
		if validateSuccessorInRecord(stored, stored.History[:index+1], event) != nil {
			return ErrCorruptStore
		}
		previous = event
	}
	if stored.Frozen != hasUnresolvedForks(stored) {
		return ErrCorruptStore
	}
	active, err := activeCredentialIDs(stored.History)
	if err != nil {
		return ErrCorruptStore
	}
	seen := make(map[string]struct{}, len(stored.Credentials))
	for _, credential := range stored.Credentials {
		if len(credential.ID) == 0 || len(credential.PublicKey) == 0 {
			return ErrCorruptStore
		}
		id := base64.RawURLEncoding.EncodeToString(credential.ID)
		if _, expected := active[id]; !expected {
			return ErrCorruptStore
		}
		if _, exists := seen[id]; exists {
			return ErrCorruptStore
		}
		seen[id] = struct{}{}
	}
	if inventoryMode == "complete" && len(seen) != len(active) {
		return ErrCorruptStore
	}
	return nil
}

func validateStoredFork(stored record, fork forkEvidence) error {
	firstBranch := fork.FirstBranch
	secondBranch := fork.SecondBranch
	if len(firstBranch) == 0 {
		firstBranch = []signedEvent{fork.First}
	}
	if len(secondBranch) == 0 {
		secondBranch = []signedEvent{fork.Second}
	}
	if len(firstBranch) > 10000 || len(secondBranch) > 10000 || len(firstBranch) == 0 || len(secondBranch) == 0 ||
		!sameSignedEvent(firstBranch[0], fork.First) || !sameSignedEvent(secondBranch[0], fork.Second) {
		return ErrCorruptStore
	}
	if fork.PreviousHash == "" {
		first, firstHead, firstRoot, firstErr := validateForkBranch(stored, nil, firstBranch, true)
		second, secondHead, secondRoot, secondErr := validateForkBranch(stored, nil, secondBranch, true)
		if firstErr != nil || secondErr != nil || first.Operation != "genesis" || second.Operation != "genesis" || first.EventHash == second.EventHash || firstHead.EventHash == secondHead.EventHash || !bytes.Equal(firstRoot.SignerPublicKey, secondRoot.SignerPublicKey) {
			return ErrCorruptStore
		}
		firstInHistory := branchPresent(stored.History, firstBranch, true)
		secondInHistory := branchPresent(stored.History, secondBranch, true)
		if firstInHistory == secondInHistory {
			return ErrCorruptStore
		}
		return nil
	}
	prefix, found := historyPrefixForHash(stored, fork.PreviousHash)
	if !found {
		return ErrCorruptStore
	}
	first, firstHead, _, firstErr := validateForkBranch(stored, prefix, firstBranch, false)
	second, secondHead, _, secondErr := validateForkBranch(stored, prefix, secondBranch, false)
	if firstErr != nil || secondErr != nil || first.PreviousHash == nil || second.PreviousHash == nil || *first.PreviousHash != fork.PreviousHash || *second.PreviousHash != fork.PreviousHash || first.Sequence != second.Sequence || first.EventHash == second.EventHash || firstHead.EventHash == secondHead.EventHash {
		return ErrCorruptStore
	}
	firstInHistory, secondInHistory := branchPresent(stored.History, firstBranch, false), branchPresent(stored.History, secondBranch, false)
	if firstInHistory && secondInHistory || !firstInHistory && !secondInHistory && !forkOrphanedByResolution(stored, fork) {
		return ErrCorruptStore
	}
	return nil
}

func validateForkBranch(stored record, prefix, branch []signedEvent, genesis bool) (identityprotocol.Event, identityprotocol.Event, identityprotocol.Event, error) {
	if len(branch) == 0 {
		return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
	}
	path := cloneSignedEvents(prefix)
	if genesis {
		if len(path) != 0 {
			return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
		}
		root, err := identityprotocol.VerifyEvent(branch[0].Canonical, branch[0].Signature)
		if err != nil || root.Operation != "genesis" || root.IdentityID != stored.IdentityID || root.Sequence != "0" || root.PreviousHash != nil || !validGenesisPayload(root.Payload, "", "") {
			return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
		}
		path = append(path, cloneSignedEvent(branch[0]))
	} else {
		if len(path) == 0 {
			return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
		}
		first, err := identityprotocol.VerifyEvent(branch[0].Canonical, branch[0].Signature)
		if err != nil || validateSuccessorInRecord(stored, path, first) != nil {
			return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
		}
		path = append(path, cloneSignedEvent(branch[0]))
	}
	for _, signed := range branch[1:] {
		previous, err := identityprotocol.VerifyEvent(path[len(path)-1].Canonical, path[len(path)-1].Signature)
		if err != nil {
			return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
		}
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err != nil || validateSuccessorInRecord(stored, path, event) != nil || validateEventSuccessor(previous, event) != nil {
			return identityprotocol.Event{}, identityprotocol.Event{}, identityprotocol.Event{}, ErrCorruptStore
		}
		path = append(path, cloneSignedEvent(signed))
	}
	first, _ := identityprotocol.VerifyEvent(branch[0].Canonical, branch[0].Signature)
	head, _ := identityprotocol.VerifyEvent(path[len(path)-1].Canonical, path[len(path)-1].Signature)
	root, _ := identityprotocol.VerifyEvent(path[0].Canonical, path[0].Signature)
	return first, head, root, nil
}

func branchPresent(history, branch []signedEvent, genesis bool) bool {
	start := 0
	if !genesis {
		if len(branch) == 0 {
			return false
		}
		first, err := identityprotocol.VerifyEvent(branch[0].Canonical, branch[0].Signature)
		if err != nil || first.PreviousHash == nil {
			return false
		}
		foundParent := false
		for i, signed := range history {
			parent, verifyErr := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
			if verifyErr == nil && parent.EventHash == *first.PreviousHash {
				start, foundParent = i+1, true
				break
			}
		}
		if !foundParent {
			return false
		}
	}
	if len(history)-start < len(branch) {
		return false
	}
	for i := range branch {
		if !sameSignedEvent(history[start+i], branch[i]) {
			return false
		}
	}
	return true
}

func branchAfterParent(history []signedEvent, parentHash string) []signedEvent {
	for index, signed := range history {
		parent, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err == nil && parent.EventHash == parentHash && index+1 < len(history) {
			return cloneSignedEvents(history[index+1:])
		}
	}
	return nil
}

func validGenesisPayload(payload map[string]any, expectedChallenge, expectedCredentialID string) bool {
	if len(payload) != 4 {
		return false
	}
	format, _ := payload["identity_format"].(string)
	challenge, _ := payload["webauthn_challenge"].(string)
	encryptionPublicText, _ := payload["recovery_encryption_public_key"].(string)
	encryptionPublic, err := base64.RawURLEncoding.DecodeString(encryptionPublicText)
	credentialID := stringPayload(payload, "credential_id")
	return format == "global-identity/v1" && validChallenge(challenge) && (expectedChallenge == "" || challenge == expectedChallenge) && (expectedCredentialID == "" || credentialID == expectedCredentialID) && validBase64URL(credentialID) && err == nil && validX25519PublicKey(encryptionPublic) && base64.RawURLEncoding.EncodeToString(encryptionPublic) == encryptionPublicText
}

func recoveryEncryptionPublicKey(payload map[string]any) ([]byte, error) {
	encoded, ok := payload["recovery_encryption_public_key"].(string)
	if !ok {
		return nil, ErrInvalidGenesis
	}
	key, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || !validX25519PublicKey(key) || base64.RawURLEncoding.EncodeToString(key) != encoded {
		return nil, ErrInvalidGenesis
	}
	return key, nil
}

func validX25519PublicKey(value []byte) bool {
	if len(value) != 32 {
		return false
	}
	publicKey, err := ecdh.X25519().NewPublicKey(value)
	if err != nil {
		return false
	}
	// X25519's public-key constructor validates only the encoding length. An
	// ECDH probe rejects low-order points before they become durable identity
	// recovery recipients that can never open an HPKE envelope.
	privateBytes := [32]byte{1}
	privateKey, err := ecdh.X25519().NewPrivateKey(privateBytes[:])
	if err != nil {
		return false
	}
	_, err = privateKey.ECDH(publicKey)
	return err == nil
}

func validChallenge(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validBase64URL(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) > 0 && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func stringPayload(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func (s *Store) validateAndAppendOrFreeze(stored *record, event identityprotocol.Event) error {
	if stored == nil || len(stored.History) == 0 || event.PreviousHash == nil {
		return ErrInvalidEvent
	}
	parentIndex := -1
	for index, entry := range stored.History {
		parent, err := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
		if err == nil && parent.EventHash == *event.PreviousHash {
			parentIndex = index
			break
		}
	}
	if parentIndex < 0 || validateSuccessorInRecord(*stored, stored.History[:parentIndex+1], event) != nil {
		return ErrInvalidEvent
	}
	head, err := identityprotocol.VerifyEvent(stored.History[len(stored.History)-1].Canonical, stored.History[len(stored.History)-1].Signature)
	if err != nil {
		return ErrCorruptStore
	}
	if *event.PreviousHash == head.EventHash {
		if stored.Frozen {
			return ErrIdentityFrozen
		}
		stored.History = append(stored.History, signedEvent{Canonical: append([]byte(nil), event.Canonical...), Signature: append([]byte(nil), event.Signature...)})
		return nil
	}
	first := childForParent(stored.History, *event.PreviousHash)
	if first == nil {
		return ErrInvalidEvent
	}
	if len(stored.ForkEvidence) >= maxForkEvidence {
		stored.Frozen = true
		if err := s.writeLocked(*stored); err != nil {
			return err
		}
		return ErrForkEvidenceLimit
	}
	stored.ForkEvidence = append(stored.ForkEvidence, forkEvidence{
		PreviousHash: *event.PreviousHash,
		First:        cloneSignedEvent(*first),
		Second:       signedEvent{Canonical: append([]byte(nil), event.Canonical...), Signature: append([]byte(nil), event.Signature...)},
		FirstBranch:  branchAfterParent(stored.History, *event.PreviousHash),
		SecondBranch: []signedEvent{{Canonical: append([]byte(nil), event.Canonical...), Signature: append([]byte(nil), event.Signature...)}},
	})
	stored.Frozen = true
	if err := s.writeLocked(*stored); err != nil {
		return err
	}
	return ErrIdentityFork
}

func validateSuccessorInHistory(identityID string, prefix []signedEvent, event identityprotocol.Event) error {
	if len(prefix) == 0 || event.IdentityID != identityID || event.PreviousHash == nil {
		return ErrInvalidEvent
	}
	previous, err := identityprotocol.VerifyEvent(prefix[len(prefix)-1].Canonical, prefix[len(prefix)-1].Signature)
	if err != nil || validateEventSuccessor(previous, event) != nil {
		return ErrInvalidEvent
	}
	root, err := identityprotocol.VerifyEvent(prefix[0].Canonical, prefix[0].Signature)
	if err != nil || !bytes.Equal(event.SignerPublicKey, root.SignerPublicKey) {
		return ErrInvalidEvent
	}
	switch event.Operation {
	case "credential_add":
		if len(event.Payload) != 2 {
			return ErrInvalidEvent
		}
		id := stringPayload(event.Payload, "credential_id")
		if !validBase64URL(id) || !validChallenge(stringPayload(event.Payload, "webauthn_challenge")) {
			return ErrInvalidEvent
		}
		active, err := activeCredentialIDs(prefix)
		if err != nil {
			return err
		}
		if _, exists := active[id]; exists {
			return ErrInvalidEvent
		}
	case "credential_revoke":
		if len(event.Payload) != 1 {
			return ErrInvalidEvent
		}
		id := stringPayload(event.Payload, "credential_id")
		if !validBase64URL(id) {
			return ErrInvalidEvent
		}
		active, err := activeCredentialIDs(prefix)
		if err != nil {
			return err
		}
		if _, exists := active[id]; !exists {
			return ErrInvalidEvent
		}
	case "node_delegate", "node_revoke":
		stored := record{IdentityID: identityID, History: prefix}
		return validateNodeSuccessor(stored, event)
	case "fork_resolve":
		if _, err := parseForkResolution(event); err != nil {
			return ErrInvalidEvent
		}
	default:
		return ErrInvalidEvent
	}
	return nil
}

func activeCredentialIDs(history []signedEvent) (map[string]struct{}, error) {
	if len(history) == 0 {
		return nil, ErrCorruptStore
	}
	genesis, err := identityprotocol.VerifyEvent(history[0].Canonical, history[0].Signature)
	if err != nil || genesis.Operation != "genesis" || !validGenesisPayload(genesis.Payload, "", "") {
		return nil, ErrCorruptStore
	}
	active := map[string]struct{}{stringPayload(genesis.Payload, "credential_id"): {}}
	for _, signed := range history[1:] {
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err != nil {
			return nil, ErrCorruptStore
		}
		switch event.Operation {
		case "credential_add":
			id := stringPayload(event.Payload, "credential_id")
			if _, exists := active[id]; exists || !validBase64URL(id) || !validChallenge(stringPayload(event.Payload, "webauthn_challenge")) || len(event.Payload) != 2 {
				return nil, ErrCorruptStore
			}
			active[id] = struct{}{}
		case "credential_revoke":
			id := stringPayload(event.Payload, "credential_id")
			if _, exists := active[id]; !exists || !validBase64URL(id) || len(event.Payload) != 1 {
				return nil, ErrCorruptStore
			}
			delete(active, id)
		}
	}
	return active, nil
}

func validateEventSuccessor(previous, next identityprotocol.Event) error {
	previousSequence, err := strconv.ParseUint(previous.Sequence, 10, 64)
	if err != nil || previousSequence == ^uint64(0) || next.Sequence != strconv.FormatUint(previousSequence+1, 10) || next.IdentityID != previous.IdentityID || next.PreviousHash == nil || *next.PreviousHash != previous.EventHash {
		return ErrInvalidEvent
	}
	return nil
}

func (s *Store) writeLocked(stored record) error {
	if stored.SchemaVersion == 1 {
		stored.CredentialInventoryMode = "complete"
	}
	stored.SchemaVersion = SchemaVersion
	if stored.CredentialInventoryMode == "" {
		stored.CredentialInventoryMode = "complete"
	}
	if err := validateRecord(stored); err != nil {
		return err
	}
	data, err := json.Marshal(stored)
	if err != nil || len(data) > maxStoreBytes {
		return errors.New("global identity state could not be encoded")
	}
	path := s.storePath()
	if info, statErr := os.Lstat(path); statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return ErrCorruptStore
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("global identity state could not be inspected")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".global-identity-*.tmp")
	if err != nil {
		return errors.New("global identity state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return errors.New("global identity state permissions could not be secured")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("global identity state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("global identity state could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("global identity state could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("global identity state could not be published")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("global identity directory could not be synchronized")
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("global identity directory could not be synchronized")
	}
	return nil
}

func (s *Store) storePath() string {
	return filepath.Join(s.root, "security", storeFilename)
}

func cloneCredentials(input []webauthn.Credential) []webauthn.Credential {
	output := make([]webauthn.Credential, len(input))
	for index, credential := range input {
		data, err := json.Marshal(credential)
		if err != nil || json.Unmarshal(data, &output[index]) != nil {
			return nil
		}
	}
	return output
}

func (r record) webAuthnUser() webauthnUser {
	return webauthnUser{handle: append([]byte(nil), r.UserHandle...), id: r.IdentityID, credentials: cloneCredentials(r.Credentials)}
}

type webauthnUser struct {
	handle      []byte
	id          string
	credentials []webauthn.Credential
}

func (u webauthnUser) WebAuthnID() []byte          { return append([]byte(nil), u.handle...) }
func (u webauthnUser) WebAuthnName() string        { return u.id }
func (u webauthnUser) WebAuthnDisplayName() string { return "Integrated Recorder Global Identity" }
func (u webauthnUser) WebAuthnCredentials() []webauthn.Credential {
	return cloneCredentials(u.credentials)
}

func (s *Store) String() string {
	return fmt.Sprintf("global-identity-store(schema=%d)", SchemaVersion)
}
