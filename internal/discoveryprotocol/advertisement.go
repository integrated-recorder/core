// Package discoveryprotocol defines the signed, deliberately untrusted
// NodeAdvertisement wire contract. It authenticates the advertising Node Key;
// it does not prove current Global Identity delegation or freshness.
package discoveryprotocol

import (
	"crypto/ed25519"
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
	Version             = "ir.node.advertisement/1"
	maxAdvertisement    = 32 << 10
	maxAddressEnvelope  = 16 << 10
	maxRelayIDs         = 8
	maxCapabilities     = 8
	maxAdvertisementTTL = 10 * time.Minute
	maxClockSkew        = 2 * time.Minute
	signDomain          = "IR/node-advertisement/v1/sign\x00"
)

var (
	ErrInvalid        = errors.New("node advertisement is invalid")
	identityHintRE    = regexp.MustCompile(`^gih_[A-Za-z0-9_-]{22}$`)
	eventHashRE       = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	relayIDRE         = regexp.MustCompile(`^rly_[A-Za-z0-9_-]{16,64}$`)
	knownCapabilities = map[string]struct{}{
		"webrtc-datachannel/1": {},
		"turn-udp/1":           {},
		"turn-tcp/1":           {},
		"turn-tls/1":           {},
	}
)

// Advertisement is signed by the installation Node Key. Network addresses
// are kept in an opaque, recipient-encrypted envelope and are never plain
// directory fields. IdentityHeadHash is only a hint: callers must compare the
// signed identity history and current delegation independently.
type Advertisement struct {
	AdvertisementVersion string   `json:"advertisement_version"`
	GlobalIdentityHint   string   `json:"global_identity_hint"`
	IdentityHeadHash     string   `json:"identity_head_hash"`
	NodeID               string   `json:"node_id"`
	NodePublicKey        string   `json:"node_public_key"`
	Sequence             string   `json:"sequence"`
	IssuedAt             string   `json:"issued_at"`
	ExpiresAt            string   `json:"expires_at"`
	AddressEnvelope      string   `json:"address_envelope"`
	Capabilities         []string `json:"capabilities"`
	RelayIDs             []string `json:"relay_ids"`
}

type VerifiedAdvertisement struct {
	Advertisement Advertisement
	Canonical     []byte
	Signature     []byte
	NodeKeyValid  bool
	// DelegationValid is intentionally false: only a separately verified,
	// sufficiently fresh identity history can establish current authority.
	DelegationValid bool
}

// GlobalIdentityHint gives directory clients a bounded lookup token without
// placing the Global Identity ID itself in the directory record. It is a
// deterministic pseudonym, not an unlinkability or secrecy guarantee.
func GlobalIdentityHint(identityID string) (string, error) {
	if len(identityID) != len("gi_")+22 || !strings.HasPrefix(identityID, "gi_") {
		return "", ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(identityID, "gi_"))
	if err != nil || len(raw) != 16 || base64.RawURLEncoding.EncodeToString(raw) != strings.TrimPrefix(identityID, "gi_") {
		return "", ErrInvalid
	}
	digest := sha256.Sum256(append([]byte("IR/discovery/global-identity-hint/v1\x00"), []byte(identityID)...))
	return "gih_" + base64.RawURLEncoding.EncodeToString(digest[:16]), nil
}

// Canonicalize validates and returns the exact signed representation. The
// signing key is supplied by the protected Node Identity store, never by an
// advertisement producer.
func Canonicalize(ad Advertisement) ([]byte, error) {
	if err := validate(ad, time.Time{}, false); err != nil {
		return nil, err
	}
	data, err := json.Marshal(ad)
	if err != nil || len(data) > maxAdvertisement {
		return nil, ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil || len(canonical) > maxAdvertisement {
		return nil, fmt.Errorf("%w: canonical encoding", ErrInvalid)
	}
	return canonical, nil
}

// Sign creates the domain-separated Node Key signature. It is suitable for
// protocol tests and trusted callers; production signing should use
// nodeidentity.Store.SignAdvertisement so the private key remains encapsulated.
func Sign(ad Advertisement, privateKey ed25519.PrivateKey) ([]byte, []byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, nil, ErrInvalid
	}
	canonical, err := Canonicalize(ad)
	if err != nil {
		return nil, nil, err
	}
	return canonical, ed25519.Sign(privateKey, append([]byte(signDomain), canonical...)), nil
}

// Verify validates encoding, NodeID/public-key binding, signature, bounded
// lifetime and clock skew. It does not establish Global Identity authority.
func Verify(canonical, signature []byte, now time.Time) (VerifiedAdvertisement, error) {
	if len(canonical) == 0 || len(canonical) > maxAdvertisement || len(signature) != ed25519.SignatureSize || now.IsZero() {
		return VerifiedAdvertisement{}, ErrInvalid
	}
	canonicalAgain, err := identityprotocol.Canonicalize(canonical)
	if err != nil || string(canonicalAgain) != string(canonical) {
		return VerifiedAdvertisement{}, fmt.Errorf("%w: noncanonical document", ErrInvalid)
	}
	var ad Advertisement
	decoder := json.NewDecoder(strings.NewReader(string(canonical)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&ad) != nil {
		return VerifiedAdvertisement{}, fmt.Errorf("%w: schema", ErrInvalid)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return VerifiedAdvertisement{}, fmt.Errorf("%w: trailing data", ErrInvalid)
	}
	if err := validate(ad, now, true); err != nil {
		return VerifiedAdvertisement{}, err
	}
	publicKey, _ := base64.RawURLEncoding.DecodeString(ad.NodePublicKey)
	if !ed25519.Verify(ed25519.PublicKey(publicKey), append([]byte(signDomain), canonical...), signature) {
		return VerifiedAdvertisement{}, fmt.Errorf("%w: signature", ErrInvalid)
	}
	return VerifiedAdvertisement{
		Advertisement: ad, Canonical: append([]byte(nil), canonical...),
		Signature: append([]byte(nil), signature...), NodeKeyValid: true,
		DelegationValid: false,
	}, nil
}

func validate(ad Advertisement, now time.Time, checkTime bool) error {
	if ad.AdvertisementVersion != Version || !identityHintRE.MatchString(ad.GlobalIdentityHint) || !eventHashRE.MatchString(ad.IdentityHeadHash) {
		return ErrInvalid
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(ad.NodePublicKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize || base64.RawURLEncoding.EncodeToString(publicKey) != ad.NodePublicKey {
		return ErrInvalid
	}
	nodeID, err := identityprotocol.NodeID(ed25519.PublicKey(publicKey))
	if err != nil || nodeID != ad.NodeID {
		return ErrInvalid
	}
	if !decimalSequence(ad.Sequence) || !validAddressEnvelope(ad.AddressEnvelope) || !validSet(ad.Capabilities, maxCapabilities, knownCapabilities) || ad.RelayIDs == nil || len(ad.RelayIDs) > maxRelayIDs {
		return ErrInvalid
	}
	for i, relayID := range ad.RelayIDs {
		if !relayIDRE.MatchString(relayID) || i > 0 && ad.RelayIDs[i-1] >= relayID {
			return ErrInvalid
		}
	}
	issued, err := time.Parse(time.RFC3339, ad.IssuedAt)
	if err != nil || !strings.HasSuffix(ad.IssuedAt, "Z") || issued.Location() != time.UTC || issued.Format(time.RFC3339) != ad.IssuedAt {
		return ErrInvalid
	}
	expires, err := time.Parse(time.RFC3339, ad.ExpiresAt)
	if err != nil || !strings.HasSuffix(ad.ExpiresAt, "Z") || expires.Location() != time.UTC || expires.Format(time.RFC3339) != ad.ExpiresAt || !expires.After(issued) || expires.Sub(issued) > maxAdvertisementTTL {
		return ErrInvalid
	}
	if checkTime && (issued.After(now.UTC().Add(maxClockSkew)) || !expires.After(now.UTC().Add(-maxClockSkew))) {
		return ErrInvalid
	}
	return nil
}

func decimalSequence(value string) bool {
	if value == "" || len(value) > 20 || value[0] == '0' && value != "0" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func validSet(values []string, limit int, known map[string]struct{}) bool {
	if len(values) == 0 || len(values) > limit {
		return false
	}
	for i, value := range values {
		if _, ok := known[value]; !ok || i > 0 && values[i-1] >= value {
			return false
		}
	}
	return true
}
