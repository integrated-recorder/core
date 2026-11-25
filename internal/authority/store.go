package authority

import (
	"bytes"
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

	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	AuthorityEventVersion  = "ir.owner-authority.event/1"
	maxAuthorityStoreBytes = 16 << 20
	maxAuthorityEvents     = 4096
	maxAuthorityForks      = 16
	maxAuthorityEventBytes = 64 << 10
	authorityEventDomain   = "IR/owner-authority/v1/sign\x00"
	authorityHashDomain    = "IR/owner-authority/v1/hash\x00"
)

var (
	ErrAuthorityStore = errors.New("owner authority store is unavailable or invalid")
	ErrAuthorityFork  = errors.New("owner authority history is frozen by a fork")
	ErrAuthorityStale = errors.New("owner authority event is stale")
	ErrAuthorityLimit = errors.New("owner authority history limit reached")
	ErrRekeyRequired  = errors.New("archive key rekey is required before authorization")
)

const (
	AuthorityCreateSubject        = "subject_create"
	AuthorityClaimArchive         = "archive_claim"
	AuthorityIssueGrant           = "grant_issue"
	AuthorityRevokeGrant          = "grant_revoke"
	AuthorityTransfer             = "subject_transfer"
	AuthorityCompleteRekey        = "archive_rekey_complete"
	AuthoritySetReplicationPolicy = "replication_policy_set"
)

// AuthorityEvent is a bounded, append-only OwnerSubject policy event. The
// identity head is explicit so a signature cannot be reinterpreted under a
// different node-delegation history. Sequence and hashes are decimal/string
// encoded to keep the canonical protocol free of JSON-number ambiguity.
type AuthorityEvent struct {
	Version      string          `json:"event_version"`
	IdentityID   string          `json:"identity_id"`
	IdentityHead string          `json:"identity_head_hash"`
	SubjectID    string          `json:"subject_id"`
	Sequence     string          `json:"sequence"`
	PreviousHash string          `json:"previous_event_hash,omitempty"`
	Operation    string          `json:"operation"`
	IssuedAt     string          `json:"issued_at"`
	Signer       Principal       `json:"signer"`
	SignerKeyID  string          `json:"signer_key_id"`
	Payload      json.RawMessage `json:"payload"`
}

type SignedAuthorityEvent struct {
	Event     AuthorityEvent `json:"event"`
	Signature []byte         `json:"signature"`
}

type AuthorityEventWire struct {
	Event     AuthorityEvent `json:"event"`
	Signature string         `json:"signature"`
}

type SubjectCreatePayload struct {
	Subject    OwnerSubject `json:"subject"`
	ArchiveIDs []string     `json:"archive_ids,omitempty"`
}

// ArchiveClaimPayload records an explicit, recovery-root-signed association
// between an OwnerSubject and one stable logical archive. The archive bytes
// and RC12 recording root remain untouched.
type ArchiveClaimPayload struct {
	ArchiveID string `json:"archive_id"`
}

type GrantIssuePayload struct {
	Chain []SignedGrant `json:"chain"`
}

type GrantRevokePayload struct {
	GrantHash string `json:"grant_hash"`
}

// ReplicationPolicy is an owner-controlled retention target for one
// OwnerSubject. Numeric values use canonical decimal strings. A zero replica
// target disables automatic replication; a zero capacity cap is unlimited.
type ReplicationPolicy struct {
	MinimumReplicas    string `json:"minimum_replicas"`
	GracePeriodSeconds string `json:"grace_period_seconds"`
	CapacityLimitBytes string `json:"capacity_limit_bytes"`
	RelocateAfterGrace bool   `json:"relocate_after_grace"`
}

type ReplicationPolicyPayload struct {
	Policy ReplicationPolicy `json:"policy"`
}

type TransferPayload struct {
	Accepted AcceptedTransfer `json:"accepted_transfer"`
}

// ArchiveRekeyEvidence records the new controller's signed acknowledgement
// for one previously claimed archive. The values are commitments produced by
// the client after it opens the recovery envelope and verifies the complete
// immutable snapshot chain. Authority validates the shape and exact archive
// coverage; it cannot independently verify local ciphertext without receiving
// the recovery secret, which must never be sent to this server.
type ArchiveRekeyEvidence struct {
	ArchiveID           string `json:"archive_id"`
	PreviousKeyEpoch    string `json:"previous_key_epoch"`
	NewKeyEpoch         string `json:"new_key_epoch"`
	SnapshotCount       string `json:"snapshot_count"`
	HeadSnapshotID      string `json:"head_snapshot_id"`
	BundleSetCommitment string `json:"bundle_set_commitment"`
	EnvelopeSHA256      string `json:"envelope_sha256"`
	RecipientKeyID      string `json:"recipient_key_id"`
}

type CompleteRekeyPayload struct {
	TransferEventHash string                 `json:"transfer_event_hash"`
	Archives          []ArchiveRekeyEvidence `json:"archives"`
}

type AuthorityIdentityResolver func(identityID, headHash string, now time.Time) (IdentityState, error)

type AuthoritySnapshot struct {
	Subject           OwnerSubject
	ReplicationPolicy ReplicationPolicy
	ArchiveIDs        []string
	Sequence          string
	HeadHash          string
	Frozen            bool
	ForkCount         int
	Grants            map[string][]SignedGrant
	RevokedHashes     map[string]struct{}
	// RekeyRequiredArchives is derived from accepted transfer events and
	// rebuilt during replay. It is intentionally not an independently mutable
	// durable flag; only a future verified rekey-complete event may clear it.
	RekeyRequiredArchives map[string]struct{} `json:"-"`
	// RekeyRequiredSinceHash binds the pending set to the accepted transfer
	// event that invalidated the prior controller's policy epoch.
	RekeyRequiredSinceHash string `json:"-"`
	usedNonces             map[string]struct{}
}

type AuthorityForkEvidence struct {
	PreviousHash string
	First        SignedAuthorityEvent
	Second       SignedAuthorityEvent
}

type authorityDisk struct {
	Version  int                    `json:"version"`
	Subjects []authoritySubjectDisk `json:"subjects"`
}

type authoritySubjectDisk struct {
	SubjectID string                 `json:"subject_id"`
	Events    []SignedAuthorityEvent `json:"events"`
	Forks     []authorityFork        `json:"forks,omitempty"`
}

type authorityFork struct {
	PreviousHash string               `json:"previous_event_hash"`
	First        SignedAuthorityEvent `json:"first_event"`
	Second       SignedAuthorityEvent `json:"second_event"`
}

type Store struct {
	mu   sync.Mutex
	root string
}

func Open(dataRoot string) (*Store, error) {
	if strings.TrimSpace(dataRoot) == "" || !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot {
		return nil, ErrAuthorityStore
	}
	securityDir := filepath.Join(dataRoot, "security")
	if err := os.MkdirAll(securityDir, 0700); err != nil {
		return nil, ErrAuthorityStore
	}
	info, err := os.Lstat(securityDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, ErrAuthorityStore
	}
	s := &Store{root: dataRoot}
	unlock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	_, err = s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

func NewAuthorityEvent(subjectID string, sequence string, previousHash string, operation string, payload any, identity IdentityState, now time.Time) (AuthorityEvent, error) {
	root := Principal{Kind: PrincipalIdentity, ID: identity.IdentityID}
	return NewAuthorityEventForSigner(subjectID, sequence, previousHash, operation, payload, root, identity, now)
}

// NewAuthorityEventForSigner binds an authority-log event to a currently
// authorized identity root or delegated Node key. Operation-specific policy
// is still checked by Store.Apply; possession of an active key alone is not
// authorization to issue or revoke a capability.
func NewAuthorityEventForSigner(subjectID string, sequence string, previousHash string, operation string, payload any, signer Principal, identity IdentityState, now time.Time) (AuthorityEvent, error) {
	if now.IsZero() || validateIdentityState(identity, now) != nil || !subjectIDPattern.MatchString(subjectID) || !validDecimal(sequence) || !signer.valid() {
		return AuthorityEvent{}, ErrInvalid
	}
	if (sequence == "1") != (previousHash == "") || (previousHash != "" && !identityprotocolHash(previousHash)) {
		return AuthorityEvent{}, ErrInvalid
	}
	root := Principal{Kind: PrincipalIdentity, ID: identity.IdentityID}
	key, ok := identity.CurrentKeys[signer.key()]
	if !ok || !principalKeyMatches(signer, key) || (signer.Kind != PrincipalIdentity && signer.Kind != PrincipalNode) || (signer.Kind == PrincipalIdentity && signer != root) {
		return AuthorityEvent{}, ErrDenied
	}
	keyID, err := identityprotocol.KeyID(key)
	if err != nil {
		return AuthorityEvent{}, ErrInvalid
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil || len(payloadBytes) == 0 || len(payloadBytes) > maxAuthorityEventBytes/2 {
		return AuthorityEvent{}, ErrInvalid
	}
	return AuthorityEvent{
		Version: AuthorityEventVersion, IdentityID: identity.IdentityID, IdentityHead: identity.HeadHash,
		SubjectID: subjectID, Sequence: sequence, PreviousHash: previousHash,
		Operation: operation, IssuedAt: now.UTC().Format(time.RFC3339), Signer: signer,
		SignerKeyID: keyID, Payload: payloadBytes,
	}, nil
}

func SignAuthorityEvent(event AuthorityEvent, privateKey ed25519.PrivateKey) (SignedAuthorityEvent, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	canonical, err := CanonicalAuthorityEvent(event)
	if err != nil {
		return SignedAuthorityEvent{}, err
	}
	return SignedAuthorityEvent{Event: event, Signature: ed25519.Sign(privateKey, append([]byte(authorityEventDomain), canonical...))}, nil
}

func CanonicalAuthorityEvent(event AuthorityEvent) ([]byte, error) {
	if err := validateAuthorityEvent(event); err != nil {
		return nil, err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil {
		return nil, ErrInvalid
	}
	return canonical, nil
}

func AuthorityEventHash(event AuthorityEvent) (string, error) {
	canonical, err := CanonicalAuthorityEvent(event)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte(authorityHashDomain), canonical...))
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

func ParseAuthorityEvent(data []byte) (SignedAuthorityEvent, error) {
	if len(data) == 0 || len(data) > maxAuthorityEventBytes {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	if _, err := identityprotocol.Canonicalize(data); err != nil {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var wire AuthorityEventWire
	if decoder.Decode(&wire) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(wire.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != wire.Signature {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	if _, err := CanonicalAuthorityEvent(wire.Event); err != nil {
		return SignedAuthorityEvent{}, err
	}
	return SignedAuthorityEvent{Event: wire.Event, Signature: signature}, nil
}

func (s *Store) Apply(event SignedAuthorityEvent, resolve AuthorityIdentityResolver, now time.Time) error {
	if s == nil || resolve == nil || now.IsZero() || len(event.Signature) != ed25519.SignatureSize {
		return ErrInvalid
	}
	canonical, err := CanonicalAuthorityEvent(event.Event)
	if err != nil {
		return err
	}
	stateAtEvent, err := resolve(event.Event.IdentityID, event.Event.IdentityHead, now)
	if err != nil || stateAtEvent.IdentityID != event.Event.IdentityID || stateAtEvent.HeadHash != event.Event.IdentityHead || stateAtEvent.Frozen {
		return ErrStaleIdentity
	}
	currentIdentity, err := resolve(event.Event.IdentityID, "", now)
	if err != nil || currentIdentity.Frozen || currentIdentity.IdentityID != event.Event.IdentityID || currentIdentity.HeadHash != event.Event.IdentityHead || validateIdentityState(currentIdentity, now) != nil {
		return ErrStaleIdentity
	}
	key, ok := stateAtEvent.CurrentKeys[event.Event.Signer.key()]
	if !ok || !principalKeyMatches(event.Event.Signer, key) {
		return ErrDenied
	}
	keyID, err := identityprotocol.KeyID(key)
	if err != nil || keyID != event.Event.SignerKeyID || !ed25519.Verify(key, append([]byte(authorityEventDomain), canonical...), event.Signature) {
		return ErrInvalid
	}
	if err := validateEventTime(event.Event, now); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		stored = authorityDisk{Version: 1}
	} else if err != nil {
		return err
	}
	index := subjectRecordIndex(stored.Subjects, event.Event.SubjectID)
	if index < 0 {
		if event.Event.Operation != AuthorityCreateSubject || event.Event.Sequence != "1" || event.Event.PreviousHash != "" {
			return ErrAuthorityStale
		}
		record, err := verifyAndApplyFirst(event, stateAtEvent, now)
		if err != nil {
			return err
		}
		var createPayload SubjectCreatePayload
		if err := decodeAuthorityPayload(event.Event.Payload, &createPayload); err != nil {
			return ErrInvalid
		}
		for _, archiveID := range createPayload.ArchiveIDs {
			if err := ensureArchiveUnclaimed(stored.Subjects, archiveID, resolve, now); err != nil {
				return err
			}
		}
		stored.Subjects = append(stored.Subjects, record)
	} else {
		record := stored.Subjects[index]
		if len(record.Forks) > 0 {
			return ErrAuthorityFork
		}
		state, err := validateSubjectLog(record, resolve, now)
		if err != nil {
			return err
		}
		eventHash, err := AuthorityEventHash(event.Event)
		if err != nil {
			return err
		}
		for i, existing := range record.Events {
			if existing.Event.Sequence == event.Event.Sequence && existing.Event.PreviousHash == event.Event.PreviousHash {
				existingHash, hashErr := AuthorityEventHash(existing.Event)
				if hashErr == nil && existingHash == eventHash {
					return nil // Idempotent exact retry.
				}
				if len(record.Forks) >= maxAuthorityForks {
					return ErrAuthorityLimit
				}
				// Concurrent first creation is also a real history fork. There
				// is no accepted prefix to replay in this case, so validate both
				// root events independently and persist the evidence while all
				// OwnerSubject authorization remains frozen.
				if i == 0 && event.Event.Sequence == "1" && event.Event.PreviousHash == "" {
					if _, createErr := verifyAndApplyFirst(event, stateAtEvent, now); createErr != nil {
						return ErrAuthorityStale
					}
					record.Forks = append(record.Forks, authorityFork{PreviousHash: "", First: existing, Second: event})
					stored.Subjects[index] = record
					if err := s.writeLocked(stored); err != nil {
						return err
					}
					return ErrAuthorityFork
				}
				prefix, replayErr := replayAuthorityEvents(record.Events[:i], resolve, now)
				if replayErr != nil || event.Event.Sequence != mustNextDecimal(prefix.Sequence) || event.Event.PreviousHash != prefix.HeadHash || applyNextEvent(&prefix, event, resolve, now) != nil {
					return ErrAuthorityStale
				}
				record.Forks = append(record.Forks, authorityFork{PreviousHash: event.Event.PreviousHash, First: existing, Second: event})
				stored.Subjects[index] = record
				if err := s.writeLocked(stored); err != nil {
					return err
				}
				return ErrAuthorityFork
			}
		}
		if event.Event.SubjectID != state.Subject.ID || event.Event.Sequence != mustNextDecimal(state.Sequence) || event.Event.PreviousHash != state.HeadHash {
			return ErrAuthorityStale
		}
		if event.Event.Operation == AuthorityClaimArchive {
			var payload ArchiveClaimPayload
			if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil {
				return ErrInvalid
			}
			if err := ensureArchiveUnclaimed(stored.Subjects, payload.ArchiveID, resolve, now); err != nil {
				return err
			}
		}
		if err := applyNextEvent(&state, event, resolve, now); err != nil {
			return err
		}
		if len(record.Events) >= maxAuthorityEvents {
			return ErrAuthorityLimit
		}
		record.Events = append(record.Events, event)
		stored.Subjects[index] = record
	}
	sort.Slice(stored.Subjects, func(i, j int) bool { return stored.Subjects[i].SubjectID < stored.Subjects[j].SubjectID })
	return s.writeLocked(stored)
}

func (s *Store) Snapshot(subjectID string, resolve AuthorityIdentityResolver, now time.Time) (AuthoritySnapshot, error) {
	if s == nil || resolve == nil || !subjectIDPattern.MatchString(subjectID) || now.IsZero() {
		return AuthoritySnapshot{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return AuthoritySnapshot{}, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return AuthoritySnapshot{}, ErrAuthorityStore
	}
	if err != nil {
		return AuthoritySnapshot{}, err
	}
	index := subjectRecordIndex(stored.Subjects, subjectID)
	if index < 0 {
		return AuthoritySnapshot{}, ErrAuthorityStore
	}
	state, err := validateSubjectLog(stored.Subjects[index], resolve, now)
	if err != nil {
		return AuthoritySnapshot{}, err
	}
	state.Frozen = len(stored.Subjects[index].Forks) > 0
	state.ForkCount = len(stored.Subjects[index].Forks)
	return state, nil
}

func (s *Store) List(resolve AuthorityIdentityResolver, now time.Time) ([]AuthoritySnapshot, error) {
	if s == nil || resolve == nil || now.IsZero() {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return []AuthoritySnapshot{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]AuthoritySnapshot, 0, len(stored.Subjects))
	for _, record := range stored.Subjects {
		snapshot, err := validateSubjectLog(record, resolve, now)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	return result, nil
}

// Authorize checks the current durable OwnerSubject epoch, frozen state,
// current identity history and every signed grant in a chain. Callers should
// use this at each external command boundary rather than cache a successful
// result across identity or authority updates.
func (s *Store) Authorize(subjectID string, resolve AuthorityIdentityResolver, now time.Time, audience Principal, archiveID, snapshotID, action string) error {
	state, err := s.Snapshot(subjectID, resolve, now)
	if err != nil {
		return err
	}
	if state.Frozen {
		return ErrAuthorityFork
	}
	if _, pending := state.RekeyRequiredArchives[archiveID]; pending {
		return ErrRekeyRequired
	}
	identity, err := resolve(state.Subject.Controller.ID, "", now)
	if err != nil || identity.Frozen || validateIdentityState(identity, now) != nil {
		return ErrStaleIdentity
	}
	context := VerificationContext{Subject: state.Subject, SubjectPolicyEpoch: state.Subject.PolicyEpoch, RevokedGrantHashes: state.RevokedHashes, Identity: identity, Now: now}
	for _, chain := range state.Grants {
		if err := VerifyGrantChain(context, chain, audience, archiveID, snapshotID, action); err == nil {
			return nil
		}
	}
	return ErrDenied
}

// AuthorizeChain verifies the exact signed grant chain submitted by a remote
// command against the current durable OwnerSubject history. A valid but
// unrecorded chain is not accepted, and revocation, policy changes, stale
// identity state, or a subject fork take effect on the next call.
func (s *Store) AuthorizeChain(subjectID string, chain []SignedGrant, resolve AuthorityIdentityResolver, now time.Time, audience Principal, archiveID, snapshotID, action string) error {
	if len(chain) == 0 {
		return ErrDenied
	}
	state, err := s.Snapshot(subjectID, resolve, now)
	if err != nil {
		return err
	}
	if state.Frozen {
		return ErrAuthorityFork
	}
	if _, pending := state.RekeyRequiredArchives[archiveID]; pending {
		return ErrRekeyRequired
	}
	leafHash, err := GrantHash(chain[len(chain)-1].Grant)
	if err != nil {
		return ErrDenied
	}
	committed, exists := state.Grants[leafHash]
	if !exists || !sameGrantChain(committed, chain) {
		return ErrDenied
	}
	identity, err := resolve(state.Subject.Controller.ID, "", now)
	if err != nil || identity.Frozen || validateIdentityState(identity, now) != nil {
		return ErrStaleIdentity
	}
	context := VerificationContext{Subject: state.Subject, SubjectPolicyEpoch: state.Subject.PolicyEpoch, RevokedGrantHashes: state.RevokedHashes, Identity: identity, Now: now}
	return VerifyGrantChain(context, chain, audience, archiveID, snapshotID, action)
}

func (s *Store) Export(subjectID string) ([]SignedAuthorityEvent, error) {
	events, _, err := s.ExportEvidence(subjectID)
	return events, err
}

// ExportEvidence returns the accepted event sequence and every retained
// competing successor. Fork evidence is public signed history, not secret
// material; consumers must still treat the subject as frozen until a future
// explicit resolution protocol exists.
func (s *Store) ExportEvidence(subjectID string) ([]SignedAuthorityEvent, []AuthorityForkEvidence, error) {
	if s == nil || !subjectIDPattern.MatchString(subjectID) {
		return nil, nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return nil, nil, err
	}
	index := subjectRecordIndex(stored.Subjects, subjectID)
	if index < 0 {
		return nil, nil, ErrAuthorityStore
	}
	record := stored.Subjects[index]
	events := append([]SignedAuthorityEvent(nil), record.Events...)
	forks := make([]AuthorityForkEvidence, 0, len(record.Forks))
	for _, fork := range record.Forks {
		forks = append(forks, AuthorityForkEvidence{PreviousHash: fork.PreviousHash, First: fork.First, Second: fork.Second})
	}
	return events, forks, nil
}

func verifyAndApplyFirst(event SignedAuthorityEvent, identity IdentityState, now time.Time) (authoritySubjectDisk, error) {
	var payload SubjectCreatePayload
	if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil {
		return authoritySubjectDisk{}, err
	}
	subject := payload.Subject
	if subject.ID != event.Event.SubjectID || subject.Type != SubjectPerson || subject.Controller != event.Event.Signer || subject.ControllerPolicy != ControllerRecoveryRoot || subject.PolicyEpoch != "1" || subject.CreatedAt.IsZero() || subject.CreatedAt.After(now.Add(2*time.Minute)) {
		return authoritySubjectDisk{}, ErrInvalid
	}
	if err := validateIdentityState(identity, now); err != nil || identity.IdentityID != subject.Controller.ID {
		return authoritySubjectDisk{}, ErrDenied
	}
	if !validArchiveIDs(payload.ArchiveIDs) {
		return authoritySubjectDisk{}, ErrInvalid
	}
	return authoritySubjectDisk{SubjectID: subject.ID, Events: []SignedAuthorityEvent{event}}, nil
}

// ensureArchiveUnclaimed serializes Archive ownership claims across all local
// OwnerSubject logs. Every existing log is revalidated before it can block or
// permit a claim; corrupt, stale, or unverifiable authority state fails closed.
func ensureArchiveUnclaimed(subjects []authoritySubjectDisk, archiveID string, resolve AuthorityIdentityResolver, now time.Time) error {
	if !archiveIDPattern.MatchString(archiveID) {
		return ErrInvalid
	}
	for _, record := range subjects {
		state, err := validateSubjectLog(record, resolve, now)
		if err != nil {
			return err
		}
		if containsArchiveID(state.ArchiveIDs, archiveID) {
			return ErrArchiveClaimed
		}
		// A frozen branch is still authenticated evidence that the archive may
		// be claimed. Reserve archive IDs mentioned by either valid successor;
		// otherwise a competing subject could claim the losing branch's scope.
		for _, fork := range record.Forks {
			for _, candidate := range []SignedAuthorityEvent{fork.First, fork.Second} {
				if eventClaimsArchive(candidate.Event, archiveID) {
					return ErrArchiveClaimed
				}
			}
		}
	}
	return nil
}

func eventClaimsArchive(event AuthorityEvent, archiveID string) bool {
	switch event.Operation {
	case AuthorityCreateSubject:
		var payload SubjectCreatePayload
		if decodeAuthorityPayload(event.Payload, &payload) != nil {
			return false // The enclosing log validation rejects malformed evidence.
		}
		return containsArchiveID(payload.ArchiveIDs, archiveID)
	case AuthorityClaimArchive:
		var payload ArchiveClaimPayload
		return decodeAuthorityPayload(event.Payload, &payload) == nil && payload.ArchiveID == archiveID
	default:
		return false
	}
}

func validArchiveIDs(ids []string) bool {
	previous := ""
	for _, id := range ids {
		if !archiveIDPattern.MatchString(id) || (previous != "" && id <= previous) {
			return false
		}
		previous = id
	}
	return true
}

func containsArchiveID(ids []string, archiveID string) bool {
	index := sort.SearchStrings(ids, archiveID)
	return index < len(ids) && ids[index] == archiveID
}

func applyNextEvent(state *AuthoritySnapshot, event SignedAuthorityEvent, resolve AuthorityIdentityResolver, now time.Time) error {
	eventIdentity, err := resolve(event.Event.IdentityID, event.Event.IdentityHead, now)
	if err != nil || eventIdentity.Frozen || eventIdentity.IdentityID != event.Event.IdentityID || eventIdentity.HeadHash != event.Event.IdentityHead {
		return ErrStaleIdentity
	}
	if err := validateIdentityState(eventIdentity, now); err != nil {
		return err
	}
	if event.Event.IdentityID != state.Subject.Controller.ID {
		return ErrStaleIdentity
	}
	eventHash, err := AuthorityEventHash(event.Event)
	if err != nil {
		return err
	}
	ctx := VerificationContext{Subject: state.Subject, SubjectPolicyEpoch: state.Subject.PolicyEpoch, RevokedGrantHashes: state.RevokedHashes, Identity: eventIdentity, Now: now}
	switch event.Event.Operation {
	case AuthorityClaimArchive:
		var payload ArchiveClaimPayload
		if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil || !archiveIDPattern.MatchString(payload.ArchiveID) {
			return ErrInvalid
		}
		if event.Event.Signer != state.Subject.Controller {
			return ErrDenied
		}
		for _, archiveID := range state.ArchiveIDs {
			if archiveID == payload.ArchiveID {
				return ErrArchiveClaimed
			}
		}
		state.ArchiveIDs = append(state.ArchiveIDs, payload.ArchiveID)
		sort.Strings(state.ArchiveIDs)
	case AuthorityIssueGrant:
		var payload GrantIssuePayload
		if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil || len(payload.Chain) == 0 {
			return ErrInvalid
		}
		leaf := payload.Chain[len(payload.Chain)-1].Grant
		if leaf.Issuer != event.Event.Signer {
			return ErrDenied
		}
		if !containsArchiveID(state.ArchiveIDs, leaf.ArchiveID) {
			return ErrArchiveUnclaimed
		}
		if _, pending := state.RekeyRequiredArchives[leaf.ArchiveID]; pending {
			return ErrRekeyRequired
		}
		if err := VerifyGrantChain(ctx, payload.Chain, leaf.Audience, leaf.ArchiveID, leaf.SnapshotID, leaf.Actions[0]); err != nil {
			return err
		}
		if state.usedNonces == nil {
			state.usedNonces = map[string]struct{}{}
		}
		for i, entry := range payload.Chain {
			hash, err := GrantHash(entry.Grant)
			if err != nil {
				return err
			}
			prefix := payload.Chain[:i+1]
			if existing, exists := state.Grants[hash]; exists {
				if !sameGrantChain(existing, prefix) {
					return ErrAuthorityStale
				}
				continue
			}
			if _, exists := state.usedNonces[entry.Grant.Nonce]; exists {
				return ErrAuthorityStale
			}
			state.Grants[hash] = cloneGrantChain(prefix)
			state.usedNonces[entry.Grant.Nonce] = struct{}{}
		}
	case AuthorityRevokeGrant:
		var payload GrantRevokePayload
		if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil || !identityprotocolHash(payload.GrantHash) {
			return ErrInvalid
		}
		chain, exists := state.Grants[payload.GrantHash]
		if !exists || len(chain) == 0 {
			return ErrDenied
		}
		if event.Event.Signer != state.Subject.Controller && event.Event.Signer != chain[len(chain)-1].Grant.Issuer {
			return ErrDenied
		}
		state.RevokedHashes[payload.GrantHash] = struct{}{}
	case AuthorityTransfer:
		if len(state.RekeyRequiredArchives) > 0 {
			return ErrRekeyRequired
		}
		if event.Event.Signer != state.Subject.Controller {
			return ErrDenied
		}
		var payload TransferPayload
		if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil {
			return ErrInvalid
		}
		accepted := payload.Accepted
		recipientState, err := resolve(accepted.Offer.To.ID, accepted.Offer.RecipientIdentityHeadHash, now)
		if err != nil {
			return ErrStaleIdentity
		}
		result, err := VerifyTransferAcceptance(ctx, recipientState, accepted)
		if err != nil {
			return err
		}
		state.Subject = result.Subject
		state.Grants = map[string][]SignedGrant{}
		state.RevokedHashes = map[string]struct{}{}
		state.RekeyRequiredArchives = make(map[string]struct{}, len(state.ArchiveIDs))
		for _, archiveID := range state.ArchiveIDs {
			state.RekeyRequiredArchives[archiveID] = struct{}{}
		}
		state.RekeyRequiredSinceHash = eventHash
	case AuthorityCompleteRekey:
		if len(state.RekeyRequiredArchives) == 0 {
			return ErrAuthorityStale
		}
		if event.Event.Signer != state.Subject.Controller {
			return ErrDenied
		}
		var payload CompleteRekeyPayload
		if err := decodeAuthorityPayload(event.Event.Payload, &payload); err != nil || !validCompleteRekeyPayload(payload) {
			return ErrInvalid
		}
		if payload.TransferEventHash != state.RekeyRequiredSinceHash {
			return ErrAuthorityStale
		}
		// Bind every acknowledgement to the recovery encryption key in the
		// signer's verified identity history. A syntactically valid recipient
		// key ID is not sufficient: otherwise a signed event could clear the
		// transfer gate while the new envelopes target an unrelated key.
		recipientPublicKey := ctx.Identity.RecoveryEncryptionPublicKey
		if len(recipientPublicKey) != 32 {
			return ErrDenied
		}
		expectedRecipientKeyID := cryptoarchive.RecipientKeyID(recipientPublicKey)
		pending := make([]string, 0, len(state.RekeyRequiredArchives))
		for archiveID := range state.RekeyRequiredArchives {
			pending = append(pending, archiveID)
		}
		sort.Strings(pending)
		if len(payload.Archives) != len(pending) {
			return ErrInvalid
		}
		for index, evidence := range payload.Archives {
			if evidence.ArchiveID != pending[index] || evidence.RecipientKeyID != expectedRecipientKeyID {
				return ErrInvalid
			}
		}
		state.RekeyRequiredArchives = map[string]struct{}{}
		state.RekeyRequiredSinceHash = ""
	case AuthoritySetReplicationPolicy:
		var payload ReplicationPolicyPayload
		if decodeAuthorityPayload(event.Event.Payload, &payload) != nil || !ValidReplicationPolicy(payload.Policy) {
			return ErrInvalid
		}
		if event.Event.Signer != state.Subject.Controller {
			return ErrDenied
		}
		state.ReplicationPolicy = payload.Policy
	default:
		return ErrInvalid
	}
	state.Sequence = event.Event.Sequence
	state.HeadHash = eventHash
	return nil
}

func validCompleteRekeyPayload(payload CompleteRekeyPayload) bool {
	if !identityprotocolHash(payload.TransferEventHash) || len(payload.Archives) == 0 || len(payload.Archives) > 4096 {
		return false
	}
	previousArchiveID := ""
	for _, evidence := range payload.Archives {
		if !archiveIDPattern.MatchString(evidence.ArchiveID) || evidence.ArchiveID <= previousArchiveID ||
			!validPositiveUintString(evidence.PreviousKeyEpoch) || !validPositiveUintString(evidence.NewKeyEpoch) ||
			!validPositiveUintString(evidence.SnapshotCount) ||
			!snapshotIDPattern.MatchString(evidence.HeadSnapshotID) ||
			!identityprotocolHash(evidence.BundleSetCommitment) || !identityprotocolHash(evidence.EnvelopeSHA256) ||
			!validRecipientKeyID(evidence.RecipientKeyID) {
			return false
		}
		previousEpoch, _ := strconv.ParseUint(evidence.PreviousKeyEpoch, 10, 64)
		newEpoch, _ := strconv.ParseUint(evidence.NewKeyEpoch, 10, 64)
		if newEpoch <= previousEpoch {
			return false
		}
		previousArchiveID = evidence.ArchiveID
	}
	return true
}

func validPositiveUintString(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed > 0 && strconv.FormatUint(parsed, 10) == value
}

func validRecipientKeyID(value string) bool {
	if !strings.HasPrefix(value, "rk_") || len(value) != 46 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "rk_"))
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == strings.TrimPrefix(value, "rk_")
}

func validateSubjectLog(record authoritySubjectDisk, resolve AuthorityIdentityResolver, now time.Time) (AuthoritySnapshot, error) {
	if len(record.Events) == 0 || len(record.Events) > maxAuthorityEvents || record.SubjectID == "" {
		return AuthoritySnapshot{}, ErrAuthorityStore
	}
	first := record.Events[0]
	identity, err := resolve(first.Event.IdentityID, first.Event.IdentityHead, now)
	if err != nil || identity.IdentityID != first.Event.IdentityID || identity.HeadHash != first.Event.IdentityHead {
		return AuthoritySnapshot{}, ErrStaleIdentity
	}
	if err := verifyEventSignature(first, identity); err != nil {
		return AuthoritySnapshot{}, err
	}
	state, err := replayAuthorityEvents(record.Events, resolve, now)
	if err != nil {
		return AuthoritySnapshot{}, err
	}
	currentIdentity, err := resolve(state.Subject.Controller.ID, "", now)
	if err != nil || currentIdentity.IdentityID != state.Subject.Controller.ID || currentIdentity.Frozen {
		return AuthoritySnapshot{}, ErrStaleIdentity
	}
	if err := validateIdentityState(currentIdentity, now); err != nil {
		return AuthoritySnapshot{}, err
	}
	state.Frozen = len(record.Forks) > 0
	state.ForkCount = len(record.Forks)
	for _, fork := range record.Forks {
		for _, candidate := range []SignedAuthorityEvent{fork.First, fork.Second} {
			identity, resolveErr := resolve(candidate.Event.IdentityID, candidate.Event.IdentityHead, now)
			if resolveErr != nil || identity.IdentityID != candidate.Event.IdentityID || identity.HeadHash != candidate.Event.IdentityHead || verifyEventSignature(candidate, identity) != nil {
				return AuthoritySnapshot{}, ErrAuthorityStore
			}
		}
		firstHash, firstErr := AuthorityEventHash(fork.First.Event)
		secondHash, secondErr := AuthorityEventHash(fork.Second.Event)
		if fork.First.Event.SubjectID != record.SubjectID || fork.Second.Event.SubjectID != record.SubjectID || fork.First.Event.Sequence != fork.Second.Event.Sequence || fork.First.Event.PreviousHash != fork.PreviousHash || fork.Second.Event.PreviousHash != fork.PreviousHash || firstErr != nil || secondErr != nil || firstHash == secondHash {
			return AuthoritySnapshot{}, ErrAuthorityStore
		}
		branchIndex := -1
		for i, accepted := range record.Events {
			acceptedHash, hashErr := AuthorityEventHash(accepted.Event)
			if hashErr == nil && acceptedHash == firstHash {
				branchIndex = i
				break
			}
		}
		if branchIndex < 0 || validateForkBranch(record.Events[:branchIndex], fork.First, resolve, now) != nil || validateForkBranch(record.Events[:branchIndex], fork.Second, resolve, now) != nil {
			return AuthoritySnapshot{}, ErrAuthorityStore
		}
	}
	return state, nil
}

func validateForkBranch(prefix []SignedAuthorityEvent, branch SignedAuthorityEvent, resolve AuthorityIdentityResolver, now time.Time) error {
	identity, err := resolve(branch.Event.IdentityID, branch.Event.IdentityHead, now)
	if err != nil || identity.IdentityID != branch.Event.IdentityID || identity.HeadHash != branch.Event.IdentityHead || verifyEventSignature(branch, identity) != nil {
		return ErrAuthorityStore
	}
	if len(prefix) == 0 {
		_, err := verifyAndApplyFirst(branch, identity, now)
		return err
	}
	state, err := replayAuthorityEvents(prefix, resolve, now)
	if err != nil || branch.Event.Sequence != mustNextDecimal(state.Sequence) || branch.Event.PreviousHash != state.HeadHash {
		return ErrAuthorityStore
	}
	return applyNextEvent(&state, branch, resolve, now)
}

func replayAuthorityEvents(events []SignedAuthorityEvent, resolve AuthorityIdentityResolver, now time.Time) (AuthoritySnapshot, error) {
	if len(events) == 0 || len(events) > maxAuthorityEvents {
		return AuthoritySnapshot{}, ErrAuthorityStore
	}
	first := events[0]
	identity, err := resolve(first.Event.IdentityID, first.Event.IdentityHead, now)
	if err != nil || identity.IdentityID != first.Event.IdentityID || identity.HeadHash != first.Event.IdentityHead {
		return AuthoritySnapshot{}, ErrStaleIdentity
	}
	if err := verifyEventSignature(first, identity); err != nil {
		return AuthoritySnapshot{}, err
	}
	if _, err := verifyAndApplyFirst(first, identity, now); err != nil {
		return AuthoritySnapshot{}, err
	}
	var payload SubjectCreatePayload
	if err := decodeAuthorityPayload(first.Event.Payload, &payload); err != nil {
		return AuthoritySnapshot{}, err
	}
	if !validArchiveIDs(payload.ArchiveIDs) {
		return AuthoritySnapshot{}, ErrAuthorityStore
	}
	state := AuthoritySnapshot{
		Subject: payload.Subject, ArchiveIDs: append([]string(nil), payload.ArchiveIDs...),
		ReplicationPolicy: DefaultReplicationPolicy(),
		Sequence:          first.Event.Sequence, Grants: map[string][]SignedGrant{},
		RevokedHashes: map[string]struct{}{}, RekeyRequiredArchives: map[string]struct{}{},
		usedNonces: map[string]struct{}{},
	}
	state.HeadHash, err = AuthorityEventHash(first.Event)
	if err != nil {
		return AuthoritySnapshot{}, err
	}
	for _, event := range events[1:] {
		identity, err := resolve(event.Event.IdentityID, event.Event.IdentityHead, now)
		if err != nil || identity.IdentityID != event.Event.IdentityID || identity.HeadHash != event.Event.IdentityHead {
			return AuthoritySnapshot{}, ErrStaleIdentity
		}
		if err := verifyEventSignature(event, identity); err != nil {
			return AuthoritySnapshot{}, err
		}
		if event.Event.Sequence != mustNextDecimal(state.Sequence) || event.Event.PreviousHash != state.HeadHash || event.Event.SubjectID != state.Subject.ID {
			return AuthoritySnapshot{}, ErrAuthorityStore
		}
		if err := applyNextEvent(&state, event, resolve, now); err != nil {
			return AuthoritySnapshot{}, err
		}
	}
	return state, nil
}

func verifyEventSignature(event SignedAuthorityEvent, identity IdentityState) error {
	canonical, err := CanonicalAuthorityEvent(event.Event)
	if err != nil || len(event.Signature) != ed25519.SignatureSize {
		return ErrInvalid
	}
	key := identity.CurrentKeys[event.Event.Signer.key()]
	keyID, keyErr := identityprotocol.KeyID(key)
	if keyErr != nil || keyID != event.Event.SignerKeyID || !principalKeyMatches(event.Event.Signer, key) || !ed25519.Verify(key, append([]byte(authorityEventDomain), canonical...), event.Signature) {
		return ErrInvalid
	}
	return nil
}

func validateAuthorityEvent(event AuthorityEvent) error {
	if event.Version != AuthorityEventVersion || !identityIDPattern.MatchString(event.IdentityID) || !identityprotocolHash(event.IdentityHead) || !subjectIDPattern.MatchString(event.SubjectID) || !validDecimal(event.Sequence) || !event.Signer.valid() || !identityKeyID(event.SignerKeyID) || len(event.Payload) == 0 || len(event.Payload) > maxAuthorityEventBytes/2 {
		return ErrInvalid
	}
	if (event.Sequence == "1") != (event.PreviousHash == "") || (event.PreviousHash != "" && !identityprotocolHash(event.PreviousHash)) {
		return ErrInvalid
	}
	if event.Sequence == "1" && event.Operation != AuthorityCreateSubject || event.Sequence != "1" && event.Operation == AuthorityCreateSubject {
		return ErrInvalid
	}
	switch event.Operation {
	case AuthorityCreateSubject, AuthorityClaimArchive, AuthorityIssueGrant, AuthorityRevokeGrant, AuthorityTransfer, AuthorityCompleteRekey, AuthoritySetReplicationPolicy:
	default:
		return ErrInvalid
	}
	if _, err := parseTime(event.IssuedAt); err != nil {
		return ErrInvalid
	}
	if _, err := identityprotocol.Canonicalize(event.Payload); err != nil {
		return ErrInvalid
	}
	return nil
}

func validateEventTime(event AuthorityEvent, now time.Time) error {
	issued, err := parseTime(event.IssuedAt)
	if err != nil || issued.After(now.Add(2*time.Minute)) || now.Sub(issued) > 24*time.Hour {
		return ErrStaleIdentity
	}
	return nil
}

func decodeAuthorityPayload(data []byte, target any) error {
	if len(data) == 0 || len(data) > maxAuthorityEventBytes/2 {
		return ErrInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return ErrInvalid
	}
	return nil
}

func cloneGrantChain(chain []SignedGrant) []SignedGrant {
	result := make([]SignedGrant, len(chain))
	for i, entry := range chain {
		result[i] = SignedGrant{Grant: cloneGrant(entry.Grant), Signature: append([]byte(nil), entry.Signature...)}
	}
	return result
}

func sameGrantChain(existing, candidate []SignedGrant) bool {
	if len(existing) != len(candidate) {
		return false
	}
	for i := range existing {
		existingHash, existingErr := GrantHash(existing[i].Grant)
		candidateHash, candidateErr := GrantHash(candidate[i].Grant)
		if existingErr != nil || candidateErr != nil || existingHash != candidateHash || !bytes.Equal(existing[i].Signature, candidate[i].Signature) {
			return false
		}
	}
	return true
}

func subjectRecordIndex(records []authoritySubjectDisk, subjectID string) int {
	for i := range records {
		if records[i].SubjectID == subjectID {
			return i
		}
	}
	return -1
}

func (s *Store) readLocked() (authorityDisk, error) {
	path := filepath.Join(s.root, "security", "owner-authority-v1.json")
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return authorityDisk{}, os.ErrNotExist
		}
		return authorityDisk{}, ErrAuthorityStore
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() > maxAuthorityStoreBytes {
		return authorityDisk{}, ErrAuthorityStore
	}
	file, err := os.Open(path)
	if err != nil {
		return authorityDisk{}, ErrAuthorityStore
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return authorityDisk{}, ErrAuthorityStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxAuthorityStoreBytes+1))
	if err != nil || len(data) > maxAuthorityStoreBytes {
		return authorityDisk{}, ErrAuthorityStore
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var stored authorityDisk
	if decoder.Decode(&stored) != nil || decoder.Decode(&struct{}{}) != io.EOF || stored.Version != 1 || len(stored.Subjects) > 4096 {
		return authorityDisk{}, ErrAuthorityStore
	}
	seen := make(map[string]struct{}, len(stored.Subjects))
	for _, record := range stored.Subjects {
		if !subjectIDPattern.MatchString(record.SubjectID) || len(record.Forks) > maxAuthorityForks || len(record.Events) == 0 || len(record.Events) > maxAuthorityEvents {
			return authorityDisk{}, ErrAuthorityStore
		}
		if _, duplicate := seen[record.SubjectID]; duplicate {
			return authorityDisk{}, ErrAuthorityStore
		}
		seen[record.SubjectID] = struct{}{}
		for _, event := range record.Events {
			if event.Event.SubjectID != record.SubjectID || len(event.Signature) != ed25519.SignatureSize || validateAuthorityEvent(event.Event) != nil {
				return authorityDisk{}, ErrAuthorityStore
			}
		}
		for _, fork := range record.Forks {
			if (fork.PreviousHash != "" && !identityprotocolHash(fork.PreviousHash)) || len(fork.First.Signature) != ed25519.SignatureSize || len(fork.Second.Signature) != ed25519.SignatureSize || validateAuthorityEvent(fork.First.Event) != nil || validateAuthorityEvent(fork.Second.Event) != nil {
				return authorityDisk{}, ErrAuthorityStore
			}
		}
	}
	return stored, nil
}

func (s *Store) writeLocked(stored authorityDisk) error {
	data, err := json.Marshal(stored)
	if err != nil || len(data) > maxAuthorityStoreBytes {
		return ErrAuthorityLimit
	}
	dir := filepath.Join(s.root, "security")
	file, err := os.CreateTemp(dir, ".owner-authority-*.tmp")
	if err != nil {
		return ErrAuthorityStore
	}
	path := file.Name()
	defer os.Remove(path)
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return ErrAuthorityStore
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return ErrAuthorityStore
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return ErrAuthorityStore
	}
	if err := file.Close(); err != nil {
		return ErrAuthorityStore
	}
	if err := os.Rename(path, filepath.Join(dir, "owner-authority-v1.json")); err != nil {
		return ErrAuthorityStore
	}
	directory, err := os.Open(dir)
	if err != nil {
		return ErrAuthorityStore
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return ErrAuthorityStore
	}
	return nil
}

func (s *Store) lock() (func(), error) {
	path := filepath.Join(s.root, "security", "owner-authority.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrAuthorityStore
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, ErrAuthorityStore
	}
	if err := authorityFlock(file); err != nil {
		_ = file.Close()
		return nil, ErrAuthorityStore
	}
	return func() { authorityFunlock(file); _ = file.Close() }, nil
}

func parseSequence(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%w: invalid authority sequence", ErrInvalid)
	}
	return parsed, nil
}
