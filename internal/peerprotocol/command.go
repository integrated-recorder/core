package peerprotocol

import (
	"bytes"
	"context"
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
	"time"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	CommandVersion    = "ir.peer-command/1"
	MaxCommandTTL     = 2 * time.Minute
	MaxCommandBytes   = 32 << 10
	MaxCommandBody    = 12 << 10
	commandDomain     = "IR/peer-command/v1/sign\x00"
	commandHashDomain = "IR/peer-command/v1/payload\x00"
)

var (
	ErrCommandInvalid  = errors.New("peer command is invalid")
	ErrCommandDenied   = errors.New("peer command is not authorized")
	ErrCommandStale    = errors.New("peer command is stale")
	commandID          = regexp.MustCompile(`^pc_[A-Za-z0-9_-]{22}$`)
	commandAction      = regexp.MustCompile(`^[a-z][a-z0-9.]{2,63}$`)
	commandPayloadHash = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Command is a short-lived, Node-Key-signed request sent only over an already
// mutually authenticated peer session. The command binds its capability,
// resource scope, payload digest, and exact transport session. A receiver
// must still resolve current identity history and authorize the grant chain
// at execution time; the signature alone is never authorization.
type Command struct {
	Version          string                  `json:"command_version"`
	RequestID        string                  `json:"request_id"`
	SessionID        string                  `json:"session_id"`
	SourceIdentityID string                  `json:"source_identity_id"`
	SourceHeadHash   string                  `json:"source_head_hash"`
	SourceNodeID     string                  `json:"source_node_id"`
	TargetIdentityID string                  `json:"target_identity_id"`
	TargetNodeID     string                  `json:"target_node_id"`
	OwnerSubjectID   string                  `json:"owner_subject_id"`
	ArchiveID        string                  `json:"archive_id"`
	SnapshotID       string                  `json:"snapshot_id,omitempty"`
	Action           string                  `json:"action"`
	IssuedAt         string                  `json:"issued_at"`
	ExpiresAt        string                  `json:"expires_at"`
	Nonce            string                  `json:"nonce"`
	PayloadHash      string                  `json:"payload_hash"`
	GrantChain       []authority.SignedGrant `json:"grant_chain"`
	Payload          json.RawMessage         `json:"payload"`
	Signature        string                  `json:"signature"`
}

type unsignedCommand struct {
	Version          string                  `json:"command_version"`
	RequestID        string                  `json:"request_id"`
	SessionID        string                  `json:"session_id"`
	SourceIdentityID string                  `json:"source_identity_id"`
	SourceHeadHash   string                  `json:"source_head_hash"`
	SourceNodeID     string                  `json:"source_node_id"`
	TargetIdentityID string                  `json:"target_identity_id"`
	TargetNodeID     string                  `json:"target_node_id"`
	OwnerSubjectID   string                  `json:"owner_subject_id"`
	ArchiveID        string                  `json:"archive_id"`
	SnapshotID       string                  `json:"snapshot_id,omitempty"`
	Action           string                  `json:"action"`
	IssuedAt         string                  `json:"issued_at"`
	ExpiresAt        string                  `json:"expires_at"`
	Nonce            string                  `json:"nonce"`
	PayloadHash      string                  `json:"payload_hash"`
	GrantChain       []authority.SignedGrant `json:"grant_chain"`
}

// CommandAuthorizer is implemented by the durable OwnerSubject authority
// boundary. Implementations must check the exact committed grant chain,
// policy epoch, revocation state, and current identity freshness on every
// command; do not cache successful decisions between requests.
type CommandAuthorizer interface {
	Authorize(context.Context, string, string, string, string, authority.Principal, []authority.SignedGrant, time.Time) error
}

// NewCommand creates the random request identifier and nonce used by a
// single peer command. It does not grant authority and must not be reused.
func NewCommand(entropy io.Reader) (requestID, nonce string, err error) {
	if entropy == nil {
		entropy = rand.Reader
	}
	raw := make([]byte, 16+32)
	if _, err := io.ReadFull(entropy, raw); err != nil {
		return "", "", fmt.Errorf("%w: random command identity", ErrCommandInvalid)
	}
	return "pc_" + base64.RawURLEncoding.EncodeToString(raw[:16]), base64.RawURLEncoding.EncodeToString(raw[16:]), nil
}

// PayloadDigest returns the hash of the exact bounded JSON payload bytes.
// The exact bytes are signed by their digest, avoiding interpretation changes
// between JSON implementations while rejecting duplicate object properties.
func PayloadDigest(payload json.RawMessage) (string, error) {
	if err := validateJSON(payload, MaxCommandBody); err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte(commandHashDomain), payload...))
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

// SignCommand signs the bounded command using the Node Key matching its
// declared source. Production code should keep private keys behind
// nodeidentity.Store and call its signing method after authority validation.
func SignCommand(command Command, privateKey ed25519.PrivateKey) (Command, error) {
	if len(privateKey) != ed25519.PrivateKeySize || command.Signature != "" {
		return Command{}, ErrCommandInvalid
	}
	if err := validateCommandShape(command); err != nil {
		return Command{}, err
	}
	derived, err := identityprotocol.NodeID(privateKey.Public().(ed25519.PublicKey))
	if err != nil || derived != command.SourceNodeID {
		return Command{}, ErrCommandDenied
	}
	canonical, err := CanonicalCommand(command)
	if err != nil {
		return Command{}, err
	}
	command.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, append([]byte(commandDomain), canonical...)))
	return command, nil
}

// ValidateCommandSigningAuthority verifies both fresh identity heads and the
// currently delegated source and target Node Keys before a node signs. The
// receiver repeats these checks after transport establishment.
func ValidateCommandSigningAuthority(command Command, resolve IdentityResolver, now time.Time) error {
	if resolve == nil || now.IsZero() || validateCommandShape(command) != nil || command.Signature != "" {
		return ErrCommandInvalid
	}
	issued, _ := time.Parse(time.RFC3339, command.IssuedAt)
	expires, _ := time.Parse(time.RFC3339, command.ExpiresAt)
	if now.Before(issued.Add(-30*time.Second)) || !now.Before(expires) || expires.After(now.Add(MaxCommandTTL+30*time.Second)) {
		return ErrCommandStale
	}
	sourceHistory, err := resolve(command.SourceIdentityID, command.SourceHeadHash, now)
	if err != nil || !resolvedIdentityFresh(sourceHistory, command.SourceIdentityID, command.SourceHeadHash, now) {
		return ErrStale
	}
	sourceCurrent, err := resolve(command.SourceIdentityID, "", now)
	if err != nil || !resolvedIdentityFresh(sourceCurrent, command.SourceIdentityID, command.SourceHeadHash, now) {
		return ErrStale
	}
	targetCurrent, err := resolve(command.TargetIdentityID, "", now)
	if err != nil || targetCurrent.Frozen || targetCurrent.IdentityID != command.TargetIdentityID || targetCurrent.VerifiedAt.IsZero() || now.Before(targetCurrent.VerifiedAt) || targetCurrent.FreshUntil.IsZero() || !now.Before(targetCurrent.FreshUntil) {
		return ErrStale
	}
	if !authorizedNodeKey(sourceHistory, command.SourceNodeID) || !authorizedNodeKey(sourceCurrent, command.SourceNodeID) || !authorizedNodeKey(targetCurrent, command.TargetNodeID) {
		return ErrCommandDenied
	}
	return nil
}

// CanonicalCommand returns the exact signed representation without signature.
func CanonicalCommand(command Command) ([]byte, error) {
	if err := validateCommandShape(command); err != nil {
		return nil, err
	}
	data, err := json.Marshal(unsignedFromCommand(command))
	if err != nil {
		return nil, ErrCommandInvalid
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil || len(canonical) > MaxCommandBytes {
		return nil, ErrCommandInvalid
	}
	return canonical, nil
}

// VerifyCommand checks the signed sender against fresh current identity
// history and binds the command to the authenticated WebRTC session and this
// exact receiving Node. It does not execute or authorize the requested action.
func VerifyCommand(command Command, resolve IdentityResolver, now time.Time, sessionID string, remote authority.Principal, localIdentityID, localNodeID string) error {
	if resolve == nil || now.IsZero() || remote.Kind != authority.PrincipalNode ||
		remote.ID != command.SourceNodeID || sessionID == "" || command.SessionID != sessionID ||
		command.TargetIdentityID != localIdentityID || command.TargetNodeID != localNodeID {
		return ErrCommandDenied
	}
	if err := validateCommandShape(command); err != nil || command.Signature == "" {
		return ErrCommandInvalid
	}
	issued, _ := time.Parse(time.RFC3339, command.IssuedAt)
	expires, _ := time.Parse(time.RFC3339, command.ExpiresAt)
	if now.Before(issued.Add(-30*time.Second)) || !now.Before(expires) || expires.Sub(issued) > MaxCommandTTL || expires.After(now.Add(MaxCommandTTL+30*time.Second)) {
		return ErrCommandStale
	}
	sourceHistory, err := resolve(command.SourceIdentityID, command.SourceHeadHash, now)
	if err != nil || !resolvedIdentityFresh(sourceHistory, command.SourceIdentityID, command.SourceHeadHash, now) {
		return ErrStale
	}
	sourceCurrent, err := resolve(command.SourceIdentityID, "", now)
	if err != nil || !resolvedIdentityFresh(sourceCurrent, command.SourceIdentityID, command.SourceHeadHash, now) {
		return ErrStale
	}
	targetCurrent, err := resolve(localIdentityID, "", now)
	if err != nil || targetCurrent.Frozen || targetCurrent.IdentityID != localIdentityID || targetCurrent.VerifiedAt.IsZero() || now.Before(targetCurrent.VerifiedAt) || targetCurrent.FreshUntil.IsZero() || !now.Before(targetCurrent.FreshUntil) {
		return ErrStale
	}
	sourceKey := sourceHistory.CurrentKeys[authority.PrincipalNode+":"+command.SourceNodeID]
	currentSourceKey := sourceCurrent.CurrentKeys[authority.PrincipalNode+":"+command.SourceNodeID]
	targetKey := targetCurrent.CurrentKeys[authority.PrincipalNode+":"+localNodeID]
	if len(sourceKey) != ed25519.PublicKeySize || len(currentSourceKey) != ed25519.PublicKeySize || len(targetKey) != ed25519.PublicKeySize ||
		!ed25519.PublicKey(sourceKey).Equal(currentSourceKey) || !authorizedNodeKey(sourceHistory, command.SourceNodeID) ||
		!authorizedNodeKey(sourceCurrent, command.SourceNodeID) || !authorizedNodeKey(targetCurrent, localNodeID) {
		return ErrCommandDenied
	}
	canonical, err := CanonicalCommand(command)
	signature, decodeErr := base64.RawURLEncoding.DecodeString(command.Signature)
	if err != nil || decodeErr != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != command.Signature ||
		!ed25519.Verify(sourceKey, append([]byte(commandDomain), canonical...), signature) {
		return ErrCommandDenied
	}
	return nil
}

func authorizedNodeKey(state authority.IdentityState, expectedNodeID string) bool {
	key := state.CurrentKeys[authority.PrincipalNode+":"+expectedNodeID]
	derived, err := identityprotocol.NodeID(key)
	if err != nil || derived != expectedNodeID {
		return false
	}
	root := state.CurrentKeys[authority.PrincipalIdentity+":"+state.IdentityID]
	identityIDValue, err := identityprotocol.IdentityID(root)
	return err == nil && identityIDValue == state.IdentityID
}

// VerifyAndAuthorizeCommand verifies the session-bound Node Key request and
// then rechecks its complete signed grant against current durable authority.
// Receivers should call this immediately before each side effect.
func VerifyAndAuthorizeCommand(ctx context.Context, command Command, resolve IdentityResolver, authorizer CommandAuthorizer, now time.Time, sessionID string, remote authority.Principal, localIdentityID, localNodeID string) error {
	if ctx == nil || authorizer == nil {
		return ErrCommandDenied
	}
	if err := VerifyCommand(command, resolve, now, sessionID, remote, localIdentityID, localNodeID); err != nil {
		return err
	}
	if err := authorizer.Authorize(ctx, command.OwnerSubjectID, command.ArchiveID, command.SnapshotID, command.Action,
		remote, command.GrantChain, now); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return ErrCommandDenied
	}
	return nil
}

// ParseCommand is the bounded wire parser for untrusted data-channel frames.
func ParseCommand(data []byte) (Command, error) {
	if len(data) == 0 || len(data) > MaxCommandBytes {
		return Command{}, ErrCommandInvalid
	}
	if err := validateJSON(data, MaxCommandBytes); err != nil {
		return Command{}, ErrCommandInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var command Command
	if decoder.Decode(&command) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateCommandShape(command) != nil {
		return Command{}, ErrCommandInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(command.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != command.Signature {
		return Command{}, ErrCommandInvalid
	}
	return command, nil
}

func validateCommandShape(command Command) error {
	if command.Version != CommandVersion || !commandID.MatchString(command.RequestID) || !sessionID.MatchString(command.SessionID) ||
		!identityID.MatchString(command.SourceIdentityID) || !headID.MatchString(command.SourceHeadHash) || !nodeID.MatchString(command.SourceNodeID) ||
		!identityID.MatchString(command.TargetIdentityID) || !nodeID.MatchString(command.TargetNodeID) || command.SourceNodeID == command.TargetNodeID ||
		!ownerSubjectID.MatchString(command.OwnerSubjectID) || !archiveID.MatchString(command.ArchiveID) || command.SnapshotID != "" && !snapshotID.MatchString(command.SnapshotID) ||
		!commandAction.MatchString(command.Action) || !commandPayloadHash.MatchString(command.PayloadHash) || len(command.GrantChain) == 0 || len(command.GrantChain) > 16 {
		return ErrCommandInvalid
	}
	issued, errA := time.Parse(time.RFC3339, command.IssuedAt)
	expires, errB := time.Parse(time.RFC3339, command.ExpiresAt)
	if errA != nil || errB != nil || issued.Location() != time.UTC || expires.Location() != time.UTC || !strings.HasSuffix(command.IssuedAt, "Z") || !strings.HasSuffix(command.ExpiresAt, "Z") ||
		issued.Format(time.RFC3339) != command.IssuedAt || expires.Format(time.RFC3339) != command.ExpiresAt || !expires.After(issued) || expires.Sub(issued) > MaxCommandTTL ||
		!validBase64URL(command.Nonce, 32) {
		return ErrCommandInvalid
	}
	payloadHash, err := PayloadDigest(command.Payload)
	if err != nil || payloadHash != command.PayloadHash {
		return ErrCommandInvalid
	}
	if command.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(command.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != command.Signature {
			return ErrCommandInvalid
		}
	} else {
		command.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	}
	wire, err := json.Marshal(command)
	if err != nil || len(wire) > MaxCommandBytes {
		return ErrCommandInvalid
	}
	data, err := json.Marshal(unsignedFromCommand(command))
	if err != nil || len(data) > MaxCommandBytes {
		return ErrCommandInvalid
	}
	return nil
}

// validateJSON accepts the application's general JSON payload values while
// rejecting invalid UTF-8, duplicate object keys, excessive nesting, and
// trailing data. Identity events use a deliberately narrower profile that is
// unsuitable for arbitrary command payloads.
func validateJSON(data []byte, maxBytes int) error {
	if len(data) == 0 || len(data) > maxBytes || !utf8.Valid(data) || !json.Valid(data) {
		return ErrCommandInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, 0); err != nil {
		return ErrCommandInvalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrCommandInvalid
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return ErrCommandInvalid
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrCommandInvalid
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return ErrCommandInvalid
				}
				key, ok := keyToken.(string)
				if !ok {
					return ErrCommandInvalid
				}
				if _, exists := seen[key]; exists {
					return ErrCommandInvalid
				}
				seen[key] = struct{}{}
				if err := scanJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
		case '[':
			for decoder.More() {
				if err := scanJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
		default:
			return ErrCommandInvalid
		}
		if err != nil {
			return ErrCommandInvalid
		}
	}
	return nil
}

func unsignedFromCommand(command Command) unsignedCommand {
	return unsignedCommand{
		Version: command.Version, RequestID: command.RequestID, SessionID: command.SessionID,
		SourceIdentityID: command.SourceIdentityID, SourceHeadHash: command.SourceHeadHash, SourceNodeID: command.SourceNodeID,
		TargetIdentityID: command.TargetIdentityID, TargetNodeID: command.TargetNodeID, OwnerSubjectID: command.OwnerSubjectID,
		ArchiveID: command.ArchiveID, SnapshotID: command.SnapshotID, Action: command.Action,
		IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt, Nonce: command.Nonce, PayloadHash: command.PayloadHash,
		GrantChain: command.GrantChain,
	}
}

var (
	ownerSubjectID = regexp.MustCompile(`^os_[A-Za-z0-9_-]{22}$`)
	archiveID      = regexp.MustCompile(`^arc_[A-Za-z0-9_-]{22}$`)
	snapshotID     = regexp.MustCompile(`^snap_[a-f0-9]{64}$`)
	headID         = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)
