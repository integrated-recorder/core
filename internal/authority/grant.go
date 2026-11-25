// Package authority verifies OwnerSubject-scoped, attenuating capability
// grants. It is a verification kernel: callers must provide an identity state
// projection that has already been checked against the current signed identity
// history. Discovery, transport, and a grant signature alone are never an
// authority source.
package authority

import (
	"bytes"
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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	ProfileVersion         = "ir.capability.grant/1"
	SubjectPerson          = "person"
	SubjectGroup           = "group"
	PrincipalIdentity      = "identity"
	PrincipalNode          = "node"
	ControllerRecoveryRoot = "global_identity_recovery_root/1"
	maxGrantChain          = 16
	maxDelegationDepth     = 8
	maxIdentityFreshness   = 5 * time.Minute
	maxSignedGrantBytes    = 32 << 10
	grantSignDomain        = "IR/capability-grant/v1/sign\x00"
	grantHashDomain        = "IR/capability-grant/v1/hash\x00"
)

var (
	ErrInvalid            = errors.New("capability authority data is invalid")
	ErrDenied             = errors.New("capability authorization denied")
	ErrStaleIdentity      = errors.New("identity authority state is stale")
	ErrFrozenIdentity     = errors.New("identity is frozen")
	ErrRevoked            = errors.New("capability is revoked")
	ErrUnsupportedSubject = errors.New("OwnerSubject type is not supported")
	ErrArchiveClaimed     = errors.New("archive is already claimed by an OwnerSubject")
	ErrArchiveUnclaimed   = errors.New("archive must be explicitly claimed before grants are issued")
	identityIDPattern     = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	nodeIDPattern         = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	subjectIDPattern      = regexp.MustCompile(`^os_[A-Za-z0-9_-]{22}$`)
	transferIDPattern     = regexp.MustCompile(`^tr_[A-Za-z0-9_-]{22}$`)
	archiveIDPattern      = regexp.MustCompile(`^arc_[A-Za-z0-9_-]{22}$`)
	snapshotIDPattern     = regexp.MustCompile(`^snap_[a-f0-9]{64}$`)
	decimalPattern        = regexp.MustCompile(`^(0|[1-9][0-9]{0,8})$`)
	noncePattern          = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
)

// Principal identifies a person or node in an OwnerSubject policy. It is not
// an account login, session ID, or Discovery address.
type Principal struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func (p Principal) valid() bool {
	switch p.Kind {
	case PrincipalIdentity:
		return identityIDPattern.MatchString(p.ID)
	case PrincipalNode:
		return nodeIDPattern.MatchString(p.ID)
	default:
		return false
	}
}

func (p Principal) key() string { return p.Kind + ":" + p.ID }

// OwnerSubject is the stable owner of archive authority. v0.2.0 implements
// person subjects; group policy is reserved in the schema but fails closed.
type OwnerSubject struct {
	ID               string    `json:"subject_id"`
	Type             string    `json:"subject_type"`
	Controller       Principal `json:"controller"`
	ControllerPolicy string    `json:"controller_policy"`
	PolicyEpoch      string    `json:"policy_epoch"`
	CreatedAt        time.Time `json:"created_at"`
}

// NewPersonOwnerSubject creates a random stable subject ID controlled by the
// supplied Global Identity. Randomness is provided by crypto/rand by default.
func NewPersonOwnerSubject(identityID, policyEpoch string, now time.Time, entropy io.Reader) (OwnerSubject, error) {
	controller := Principal{Kind: PrincipalIdentity, ID: identityID}
	if !controller.valid() || !validDecimal(policyEpoch) || now.IsZero() {
		return OwnerSubject{}, ErrInvalid
	}
	if entropy == nil {
		entropy = rand.Reader
	}
	raw := make([]byte, 16)
	if _, err := io.ReadFull(entropy, raw); err != nil {
		return OwnerSubject{}, fmt.Errorf("%w: subject identifier generation", ErrInvalid)
	}
	return OwnerSubject{
		ID: "os_" + base64.RawURLEncoding.EncodeToString(raw), Type: SubjectPerson,
		Controller: controller, ControllerPolicy: ControllerRecoveryRoot,
		PolicyEpoch: policyEpoch, CreatedAt: now.UTC(),
	}, nil
}

// Grant describes one capability in a delegation chain. Resources are exact
// archive IDs with an optional snapshot restriction. Empty SnapshotID means
// the archive as a whole; it cannot be widened by a child grant.
type Grant struct {
	Version         string    `json:"grant_version"`
	SubjectID       string    `json:"subject_id"`
	Issuer          Principal `json:"issuer"`
	Audience        Principal `json:"audience"`
	ArchiveID       string    `json:"archive_id"`
	SnapshotID      string    `json:"snapshot_id,omitempty"`
	Actions         []string  `json:"actions"`
	NotBefore       string    `json:"not_before"`
	ExpiresAt       string    `json:"expires_at"`
	DelegationDepth string    `json:"delegation_depth"`
	Nonce           string    `json:"nonce"`
	ParentGrantHash string    `json:"parent_grant_hash,omitempty"`
	PolicyEpoch     string    `json:"policy_epoch"`
	IssuerKeyID     string    `json:"issuer_key_id"`
}

// SignedGrant keeps signature bytes separate from the canonical grant body.
type SignedGrant struct {
	Grant     Grant  `json:"grant"`
	Signature []byte `json:"signature"`
}

type signedGrantWire struct {
	Grant     Grant  `json:"grant"`
	Signature string `json:"signature"`
}

// MarshalJSON uses the protocol's canonical unpadded base64url signature
// representation. Go's default []byte JSON encoding is padded standard
// base64, which does not match the browser signer or the signed test vectors.
func (s SignedGrant) MarshalJSON() ([]byte, error) {
	if len(s.Signature) != ed25519.SignatureSize {
		return nil, ErrInvalid
	}
	if _, err := CanonicalGrant(s.Grant); err != nil {
		return nil, err
	}
	return json.Marshal(signedGrantWire{Grant: cloneGrant(s.Grant), Signature: base64.RawURLEncoding.EncodeToString(s.Signature)})
}

// UnmarshalJSON accepts only the protocol's canonical unpadded base64url
// signature form and rejects unknown fields before the grant can enter a
// durable authority event.
func (s *SignedGrant) UnmarshalJSON(data []byte) error {
	if s == nil || len(data) == 0 || len(data) > maxSignedGrantBytes {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire signedGrantWire
	if decoder.Decode(&wire) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return ErrInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(wire.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != wire.Signature {
		return ErrInvalid
	}
	if _, err := CanonicalGrant(wire.Grant); err != nil {
		return err
	}
	s.Grant = cloneGrant(wire.Grant)
	s.Signature = signature
	return nil
}

// IdentityState must be constructed from a verified, sufficiently current
// Global Identity history. CurrentKeys must contain only keys that history
// currently authorizes. It intentionally does not contain OwnerSubject policy
// epoch or capability revocations; those are separate authority state.
type IdentityState struct {
	IdentityID                  string
	HeadHash                    string
	VerifiedAt                  time.Time
	FreshUntil                  time.Time
	Frozen                      bool
	CurrentKeys                 map[string]ed25519.PublicKey
	RecoveryEncryptionPublicKey []byte
}

type VerificationContext struct {
	// SubjectPolicyEpoch and RevokedGrantHashes come from the current,
	// independently verified OwnerSubject authority history.
	Subject            OwnerSubject
	SubjectPolicyEpoch string
	RevokedGrantHashes map[string]struct{}
	Identity           IdentityState
	Now                time.Time
}

// CanonicalGrant returns the exact signed bytes for the v1 profile. Action
// order is significant on the wire and must be strictly sorted; signatures do
// not normalize potentially ambiguous inputs.
func CanonicalGrant(grant Grant) ([]byte, error) {
	if err := validateGrantShape(grant); err != nil {
		return nil, err
	}
	data, err := json.Marshal(grant)
	if err != nil {
		return nil, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil {
		return nil, fmt.Errorf("%w: canonical grant", ErrInvalid)
	}
	return canonical, nil
}

func GrantHash(grant Grant) (string, error) {
	canonical, err := CanonicalGrant(grant)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte(grantHashDomain), canonical...))
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

// SignGrant is intended for the browser/client holding an authorized
// controller key and for deterministic protocol tests. Server code should
// verify grants; it must never accept a caller-selected key as trusted.
func SignGrant(grant Grant, privateKey ed25519.PrivateKey) (SignedGrant, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return SignedGrant{}, ErrInvalid
	}
	canonical, err := CanonicalGrant(grant)
	if err != nil {
		return SignedGrant{}, err
	}
	return SignedGrant{Grant: cloneGrant(grant), Signature: ed25519.Sign(privateKey, append([]byte(grantSignDomain), canonical...))}, nil
}

// ParseSignedGrant parses the bounded API representation. It rejects
// duplicate JSON properties, unknown fields, trailing values, padded or
// noncanonical signature encodings, and protocol numbers before authorization.
func ParseSignedGrant(data []byte) (SignedGrant, error) {
	if len(data) == 0 || len(data) > maxSignedGrantBytes {
		return SignedGrant{}, ErrInvalid
	}
	if _, err := identityprotocol.Canonicalize(data); err != nil {
		return SignedGrant{}, ErrInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var wire signedGrantWire
	if decoder.Decode(&wire) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return SignedGrant{}, ErrInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(wire.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != wire.Signature {
		return SignedGrant{}, ErrInvalid
	}
	if _, err := CanonicalGrant(wire.Grant); err != nil {
		return SignedGrant{}, err
	}
	return SignedGrant{Grant: cloneGrant(wire.Grant), Signature: signature}, nil
}

// VerifyGrantChain validates the complete chain and current authorization
// context. It refuses stale/frozen identities, known revocations, widened
// scope, expired grants, unknown actions, and any untrusted issuer key.
func VerifyGrantChain(ctx VerificationContext, chain []SignedGrant, audience Principal, archiveID, snapshotID, action string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if !audience.valid() || !archiveIDPattern.MatchString(archiveID) || (snapshotID != "" && !snapshotIDPattern.MatchString(snapshotID)) || !knownAction(action) {
		return ErrDenied
	}
	if len(chain) == 0 || len(chain) > maxGrantChain {
		return ErrDenied
	}

	var previous *Grant
	var previousHash string
	seenNonces := make(map[string]struct{}, len(chain))
	for i := range chain {
		entry := chain[i]
		if err := verifySignedGrant(ctx, entry); err != nil {
			return err
		}
		grant := entry.Grant
		if _, duplicate := seenNonces[grant.Nonce]; duplicate {
			return ErrInvalid
		}
		seenNonces[grant.Nonce] = struct{}{}
		if grant.SubjectID != ctx.Subject.ID || grant.PolicyEpoch != ctx.SubjectPolicyEpoch {
			return ErrStaleIdentity
		}
		if _, revoked := ctx.RevokedGrantHashes[grantHashUnchecked(grant)]; revoked {
			return ErrRevoked
		}
		if i == 0 {
			if grant.ParentGrantHash != "" || grant.Issuer != ctx.Subject.Controller {
				return ErrDenied
			}
		} else {
			if grant.ParentGrantHash != previousHash || !attenuates(*previous, grant) {
				return ErrDenied
			}
		}
		if !grantTimeValid(grant, ctx.Now) {
			return ErrDenied
		}
		previous = &grant
		previousHash = grantHashUnchecked(grant)
	}

	last := chain[len(chain)-1].Grant
	if last.Audience != audience || last.ArchiveID != archiveID || (last.SnapshotID != "" && last.SnapshotID != snapshotID) || !containsAction(last.Actions, action) {
		return ErrDenied
	}
	if action == "archive.delegate" {
		depth, err := strconv.Atoi(last.DelegationDepth)
		if err != nil || depth == 0 {
			return ErrDenied
		}
	}
	return nil
}

func validateContext(ctx VerificationContext) error {
	if ctx.Subject.Type == SubjectGroup {
		return ErrUnsupportedSubject
	}
	if ctx.Subject.Type != SubjectPerson || !subjectIDPattern.MatchString(ctx.Subject.ID) || !ctx.Subject.Controller.valid() || ctx.Subject.ControllerPolicy != ControllerRecoveryRoot || !validDecimal(ctx.Subject.PolicyEpoch) || !validDecimal(ctx.SubjectPolicyEpoch) || ctx.Now.IsZero() || (ctx.Subject.Controller.Kind == PrincipalIdentity && ctx.Identity.IdentityID != ctx.Subject.Controller.ID) {
		return ErrInvalid
	}
	if ctx.SubjectPolicyEpoch != ctx.Subject.PolicyEpoch {
		return ErrStaleIdentity
	}
	if err := validateIdentityState(ctx.Identity, ctx.Now); err != nil {
		return err
	}
	rootKey, ok := ctx.Identity.CurrentKeys[ctx.Subject.Controller.key()]
	if !ok || len(rootKey) != ed25519.PublicKeySize {
		return ErrDenied
	}
	if !principalKeyMatches(ctx.Subject.Controller, rootKey) {
		return ErrDenied
	}
	return nil
}

func verifySignedGrant(ctx VerificationContext, entry SignedGrant) error {
	canonical, err := CanonicalGrant(entry.Grant)
	if err != nil || len(entry.Signature) != ed25519.SignatureSize {
		return ErrInvalid
	}
	key, ok := ctx.Identity.CurrentKeys[entry.Grant.Issuer.key()]
	if !ok || len(key) != ed25519.PublicKeySize || !principalKeyMatches(entry.Grant.Issuer, key) {
		return ErrDenied
	}
	keyID, err := identityprotocol.KeyID(key)
	if err != nil || keyID != entry.Grant.IssuerKeyID || !ed25519.Verify(key, append([]byte(grantSignDomain), canonical...), entry.Signature) {
		return ErrInvalid
	}
	return nil
}

func validateGrantShape(g Grant) error {
	if g.Version != ProfileVersion || !subjectIDPattern.MatchString(g.SubjectID) || !g.Issuer.valid() || !g.Audience.valid() || !archiveIDPattern.MatchString(g.ArchiveID) || (g.SnapshotID != "" && !snapshotIDPattern.MatchString(g.SnapshotID)) || !validDecimal(g.PolicyEpoch) || !decimalPattern.MatchString(g.DelegationDepth) || !noncePattern.MatchString(g.Nonce) {
		return ErrInvalid
	}
	depth, _ := strconv.Atoi(g.DelegationDepth)
	if depth > maxDelegationDepth || !identityKeyID(g.IssuerKeyID) {
		return ErrInvalid
	}
	if g.ParentGrantHash != "" && !identityprotocolHash(g.ParentGrantHash) {
		return ErrInvalid
	}
	if len(g.Actions) == 0 || len(g.Actions) > 16 {
		return ErrInvalid
	}
	for i, action := range g.Actions {
		if !knownAction(action) || (i > 0 && g.Actions[i-1] >= action) {
			return ErrInvalid
		}
	}
	start, startErr := parseTime(g.NotBefore)
	end, endErr := parseTime(g.ExpiresAt)
	if startErr != nil || endErr != nil || !start.Before(end) || end.Sub(start) > 366*24*time.Hour {
		return ErrInvalid
	}
	return nil
}

func attenuates(parent, child Grant) bool {
	if child.Issuer != parent.Audience || parent.SubjectID != child.SubjectID || parent.PolicyEpoch != child.PolicyEpoch || parent.ArchiveID != child.ArchiveID {
		return false
	}
	if parent.SnapshotID != "" && child.SnapshotID != parent.SnapshotID {
		return false
	}
	parentDepth, errA := strconv.Atoi(parent.DelegationDepth)
	childDepth, errB := strconv.Atoi(child.DelegationDepth)
	if errA != nil || errB != nil || parentDepth <= 0 || childDepth >= parentDepth {
		return false
	}
	if !containsAction(parent.Actions, "archive.delegate") {
		return false
	}
	for _, action := range child.Actions {
		if !containsAction(parent.Actions, action) {
			return false
		}
	}
	parentStart, errA := parseTime(parent.NotBefore)
	childStart, errB := parseTime(child.NotBefore)
	parentEnd, errC := parseTime(parent.ExpiresAt)
	childEnd, errD := parseTime(child.ExpiresAt)
	return errA == nil && errB == nil && errC == nil && errD == nil && !childStart.Before(parentStart) && !childEnd.After(parentEnd)
}

func grantTimeValid(g Grant, now time.Time) bool {
	start, errA := parseTime(g.NotBefore)
	end, errB := parseTime(g.ExpiresAt)
	return errA == nil && errB == nil && !now.Before(start) && now.Before(end)
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339) != value {
		return time.Time{}, ErrInvalid
	}
	return parsed, nil
}

func knownAction(action string) bool {
	switch action {
	case "archive.view", "archive.export", "archive.replicate", "archive.annotate", "archive.manage", "archive.delegate", "archive.transfer", "archive.delete", "node.manage", "node.enroll", "identity.manage", "key.rotate":
		return true
	default:
		return false
	}
}

func containsAction(actions []string, wanted string) bool {
	i := sort.SearchStrings(actions, wanted)
	return i < len(actions) && actions[i] == wanted
}

func principalKeyMatches(principal Principal, key ed25519.PublicKey) bool {
	switch principal.Kind {
	case PrincipalIdentity:
		id, err := identityprotocol.IdentityID(key)
		return err == nil && id == principal.ID
	case PrincipalNode:
		id, err := identityprotocol.NodeID(key)
		return err == nil && id == principal.ID
	default:
		return false
	}
}

func grantHashUnchecked(grant Grant) string {
	canonical, _ := CanonicalGrant(grant)
	hash := sha256.Sum256(append([]byte(grantHashDomain), canonical...))
	return "sha256:" + hex.EncodeToString(hash[:])
}

func cloneGrant(g Grant) Grant {
	g.Actions = append([]string(nil), g.Actions...)
	return g
}

func validDecimal(value string) bool { return decimalPattern.MatchString(value) }

func identityKeyID(value string) bool {
	if !strings.HasPrefix(value, "ik_") || len(value) != 3+43 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "ik_"))
	return err == nil && len(raw) == 32 && base64.RawURLEncoding.EncodeToString(raw) == strings.TrimPrefix(value, "ik_")
}

func identityprotocolHash(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}
