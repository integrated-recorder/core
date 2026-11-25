package nodeidentity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	enrollmentChallengeFile = "node-enrollment-challenge-v1.json"
	enrollmentChallengeTTL  = 5 * time.Minute
)

var (
	ErrEnrollmentChallengeActive   = errors.New("a node enrollment challenge is already active")
	ErrEnrollmentChallengeExpired  = errors.New("node enrollment challenge expired or is unavailable")
	ErrEnrollmentChallengeConsumed = errors.New("node enrollment challenge was already consumed")
)

type enrollmentChallengeState struct {
	SchemaVersion int       `json:"schema_version"`
	IdentityID    string    `json:"identity_id"`
	PreviousHash  string    `json:"previous_event_hash"`
	Nonce         string    `json:"nonce"`
	ExpiresAt     time.Time `json:"expires_at"`
	EventHash     string    `json:"event_hash,omitempty"`
}

// BeginEnrollmentProof creates a verifier-side, one-use nonce and binds the
// local Node Key proof to the exact identity log head that an owner will sign.
func (s *Store) BeginEnrollmentProof(identityID, previousHash string, now time.Time) (EnrollmentProof, error) {
	if s == nil || now.IsZero() {
		return EnrollmentProof{}, ErrInvalid
	}
	nonceBytes := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, nonceBytes); err != nil {
		return EnrollmentProof{}, ErrInvalid
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes)
	message, err := identityprotocol.NodeEnrollmentProofMessage(identityID, previousHash, nonce)
	if err != nil {
		clear(nonceBytes)
		return EnrollmentProof{}, ErrInvalid
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		clear(nonceBytes)
		return EnrollmentProof{}, err
	}
	defer unlock()
	key, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		clear(nonceBytes)
		return EnrollmentProof{}, ErrNotCreated
	}
	if err != nil {
		clear(nonceBytes)
		return EnrollmentProof{}, err
	}
	existing, err := s.readEnrollmentChallengeLocked()
	if err == nil && existing.EventHash == "" && existing.ExpiresAt.After(now) {
		clear(nonceBytes)
		return EnrollmentProof{}, ErrEnrollmentChallengeActive
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		clear(nonceBytes)
		return EnrollmentProof{}, err
	}
	expiresAt := now.UTC().Add(enrollmentChallengeTTL)
	state := enrollmentChallengeState{SchemaVersion: 1, IdentityID: identityID, PreviousHash: previousHash, Nonce: nonce, ExpiresAt: expiresAt}
	if err := s.writeEnrollmentChallengeLocked(state); err != nil {
		clear(nonceBytes)
		return EnrollmentProof{}, err
	}
	privateBytes, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	clear(nonceBytes)
	if err != nil || len(privateBytes) != ed25519.PrivateKeySize {
		clear(privateBytes)
		return EnrollmentProof{}, ErrCorrupt
	}
	signature := ed25519.Sign(ed25519.PrivateKey(privateBytes), message)
	clear(privateBytes)
	return EnrollmentProof{
		IdentityID: identityID, PreviousHash: previousHash, NodeID: key.NodeID,
		PublicKey: key.PublicKey, Nonce: nonce,
		Signature: base64.RawURLEncoding.EncodeToString(signature), ExpiresAt: expiresAt,
	}, nil
}

// ValidateEnrollmentEvent checks that an imported, recovery-root-signed node
// delegation carries the active one-use proof issued by this installation.
func (s *Store) ValidateEnrollmentEvent(canonical, signature []byte, now time.Time) (string, error) {
	if s == nil || now.IsZero() {
		return "", ErrInvalid
	}
	verified, err := identityprotocol.VerifyEvent(canonical, signature)
	if err != nil || verified.Operation != "node_delegate" || verified.PreviousHash == nil {
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
		return "", err
	}
	state, err := s.readEnrollmentChallengeLocked()
	if err != nil {
		return "", ErrEnrollmentChallengeExpired
	}
	if err := validateEnrollmentState(key, state, verified, now); err != nil {
		return "", err
	}
	return verified.EventHash, nil
}

// ConsumeEnrollmentEvent durably binds the challenge to one accepted event.
// Replaying the same event is idempotent; another event cannot reuse the nonce.
func (s *Store) ConsumeEnrollmentEvent(canonical, signature []byte, now time.Time) error {
	if s == nil || now.IsZero() {
		return ErrInvalid
	}
	verified, err := identityprotocol.VerifyEvent(canonical, signature)
	if err != nil || verified.Operation != "node_delegate" || verified.PreviousHash == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockNodeIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	key, err := s.readLocked()
	if err != nil {
		return err
	}
	state, err := s.readEnrollmentChallengeLocked()
	if err != nil {
		return ErrEnrollmentChallengeExpired
	}
	if err := validateEnrollmentState(key, state, verified, now); err != nil {
		return err
	}
	if state.EventHash == verified.EventHash {
		return nil
	}
	state.EventHash = verified.EventHash
	return s.writeEnrollmentChallengeLocked(state)
}

func validateEnrollmentState(key storedKey, state enrollmentChallengeState, event identityprotocol.Event, now time.Time) error {
	if state.EventHash != "" {
		if state.EventHash == event.EventHash {
			return nil
		}
		return ErrEnrollmentChallengeConsumed
	}
	if !state.ExpiresAt.After(now) {
		return ErrEnrollmentChallengeExpired
	}
	return matchEnrollmentEvent(key, state, event)
}

func matchEnrollmentEvent(key storedKey, state enrollmentChallengeState, event identityprotocol.Event) error {
	if event.IdentityID != state.IdentityID || event.PreviousHash == nil || *event.PreviousHash != state.PreviousHash || len(event.Payload) != 4 {
		return ErrEnrollmentChallengeExpired
	}
	publicText := stringPayload(event.Payload, "node_public_key")
	nodeID := stringPayload(event.Payload, "node_id")
	nonce := stringPayload(event.Payload, "enrollment_nonce")
	proofText := stringPayload(event.Payload, "node_proof")
	if key.NodeID != nodeID || key.PublicKey != publicText || nonce != state.Nonce {
		return ErrEnrollmentChallengeExpired
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(publicText)
	if err != nil || len(publicKey) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(publicKey) != publicText {
		return ErrEnrollmentChallengeExpired
	}
	proof, err := base64.RawURLEncoding.DecodeString(proofText)
	if err != nil || len(proof) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(proof) != proofText {
		return ErrEnrollmentChallengeExpired
	}
	message, err := identityprotocol.NodeEnrollmentProofMessage(state.IdentityID, state.PreviousHash, state.Nonce)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(publicKey), message, proof) {
		return ErrEnrollmentChallengeExpired
	}
	return nil
}

func stringPayload(payload map[string]any, name string) string {
	value, _ := payload[name].(string)
	return value
}

func (s *Store) readEnrollmentChallengeLocked() (enrollmentChallengeState, error) {
	path := s.enrollmentChallengePath()
	info, err := os.Lstat(path)
	if err != nil {
		return enrollmentChallengeState{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != fileMode {
		return enrollmentChallengeState{}, ErrCorrupt
	}
	f, err := os.Open(path)
	if err != nil {
		return enrollmentChallengeState{}, ErrCorrupt
	}
	opened, statErr := f.Stat()
	data, readErr := io.ReadAll(io.LimitReader(f, maxFile+1))
	closeErr := f.Close()
	if statErr != nil || !os.SameFile(info, opened) || readErr != nil || closeErr != nil || len(data) > maxFile {
		return enrollmentChallengeState{}, ErrCorrupt
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state enrollmentChallengeState
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF || state.SchemaVersion != 1 || state.ExpiresAt.IsZero() {
		return enrollmentChallengeState{}, ErrCorrupt
	}
	if _, err := identityprotocol.NodeEnrollmentProofMessage(state.IdentityID, state.PreviousHash, state.Nonce); err != nil {
		return enrollmentChallengeState{}, ErrCorrupt
	}
	if state.EventHash != "" {
		if len(state.EventHash) != len("sha256:")+64 || state.EventHash[:7] != "sha256:" {
			return enrollmentChallengeState{}, ErrCorrupt
		}
		decoded, err := hex.DecodeString(state.EventHash[7:])
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != state.EventHash[7:] {
			return enrollmentChallengeState{}, ErrCorrupt
		}
	}
	return state, nil
}

func (s *Store) writeEnrollmentChallengeLocked(state enrollmentChallengeState) error {
	if _, err := identityprotocol.NodeEnrollmentProofMessage(state.IdentityID, state.PreviousHash, state.Nonce); err != nil || state.SchemaVersion != 1 || state.ExpiresAt.IsZero() {
		return ErrInvalid
	}
	data, err := json.Marshal(state)
	if err != nil || len(data) > maxFile {
		return ErrInvalid
	}
	path := s.enrollmentChallengePath()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".node-enrollment-*.tmp")
	if err != nil {
		return errors.New("node enrollment state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(fileMode); err != nil {
		_ = tmp.Close()
		return errors.New("node enrollment permissions could not be set")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("node enrollment state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("node enrollment state could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("node enrollment state could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("node enrollment state could not be published")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("node identity directory could not be synchronized")
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("node identity directory could not be synchronized")
	}
	return nil
}

func (s *Store) enrollmentChallengePath() string {
	return filepath.Join(s.root, "security", enrollmentChallengeFile)
}
