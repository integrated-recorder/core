package discoveryprotocol

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestAdvertisementSignVerifyAndTrustBoundary(t *testing.T) {
	privateKey := advertisementTestKey()
	ad := advertisementFixture(t, privateKey)
	canonical, signature, err := Sign(ad, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(canonical, signature, time.Date(2026, 11, 2, 12, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if !verified.NodeKeyValid || verified.DelegationValid {
		t.Fatalf("unexpected trust projection: %+v", verified)
	}
	if verified.Advertisement.NodeID != ad.NodeID || verified.Advertisement.Sequence != "7" {
		t.Fatalf("unexpected verified advertisement: %+v", verified.Advertisement)
	}
	if _, err := Verify(canonical, signature, time.Date(2026, 11, 2, 12, 12, 0, 0, time.UTC)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expired advertisement accepted: %v", err)
	}
	tampered := append([]byte(nil), signature...)
	tampered[0] ^= 0x80
	if _, err := Verify(canonical, tampered, time.Date(2026, 11, 2, 12, 1, 0, 0, time.UTC)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("tampered signature accepted: %v", err)
	}
}

func TestAdvertisementRejectsMalformedOrAmbiguousFields(t *testing.T) {
	privateKey := advertisementTestKey()
	now := time.Date(2026, 11, 2, 12, 1, 0, 0, time.UTC)
	base := advertisementFixture(t, privateKey)
	tests := []struct {
		name   string
		mutate func(*Advertisement)
	}{
		{"wrong node identity", func(ad *Advertisement) { ad.NodeID = "nd_AAAAAAAAAAAAAAAAAAAAAA" }},
		{"sequence leading zero", func(ad *Advertisement) { ad.Sequence = "07" }},
		{"unsupported capability", func(ad *Advertisement) { ad.Capabilities = []string{"webrtc-datachannel/1", "unknown/9"} }},
		{"unsorted capabilities", func(ad *Advertisement) { ad.Capabilities = []string{"webrtc-datachannel/1", "turn-udp/1"} }},
		{"malformed address envelope", func(ad *Advertisement) { ad.AddressEnvelope = "not-private-address-data" }},
		{"excessive lifetime", func(ad *Advertisement) { ad.ExpiresAt = "2026-11-02T12:11:00Z" }},
		{"non-UTC timestamp", func(ad *Advertisement) { ad.IssuedAt = "2026-11-02T12:00:00+00:00" }},
		{"duplicate relay id", func(ad *Advertisement) { ad.RelayIDs = []string{"rly_abcdefghijklmnop", "rly_abcdefghijklmnop"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ad := base
			ad.Capabilities = append([]string(nil), base.Capabilities...)
			ad.RelayIDs = append([]string(nil), base.RelayIDs...)
			test.mutate(&ad)
			canonical, signature, err := Sign(ad, privateKey)
			if err == nil {
				if _, err := Verify(canonical, signature, now); err == nil {
					t.Fatal("invalid advertisement accepted")
				}
			}
		})
	}
	canonical, signature, err := Sign(base, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{
		append(append([]byte(nil), canonical...), []byte(` {}`)...),
		[]byte(strings.Replace(string(canonical), `"sequence":"7"`, `"sequence":"7","sequence":"8"`, 1)),
	} {
		if _, err := Verify(input, signature, now); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ambiguous wire document accepted: %v", err)
		}
	}
}

func TestGlobalIdentityHintIsStablePseudonym(t *testing.T) {
	identityID := "gi_AAAAAAAAAAAAAAAAAAAAAA"
	first, err := GlobalIdentityHint(identityID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GlobalIdentityHint(identityID)
	if err != nil || first != second || len(strings.TrimPrefix(first, "gih_")) != 22 {
		t.Fatalf("unstable hint: first=%q second=%q err=%v", first, second, err)
	}
	if _, err := GlobalIdentityHint("gi_invalid"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid identity accepted: %v", err)
	}
}

func advertisementFixture(t *testing.T, privateKey ed25519.PrivateKey) Advertisement {
	t.Helper()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	nodeID, err := identityprotocol.NodeID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	identityHint, err := GlobalIdentityHint("gi_AAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	headHash := "sha256:" + strings.Repeat("a", 64)
	recipient, err := ecdh.X25519().NewPrivateKey(bytesOf(32, 0x52))
	if err != nil {
		t.Fatal(err)
	}
	addressEnvelope, err := SealAddressEnvelope(recipient.PublicKey().Bytes(), AddressContext{
		GlobalIdentityHint: identityHint, IdentityHeadHash: headHash, NodeID: nodeID,
	}, []byte(`{"addresses":["wss://node.example.test/signal"]}`))
	if err != nil {
		t.Fatal(err)
	}
	return Advertisement{
		AdvertisementVersion: Version,
		GlobalIdentityHint:   identityHint,
		IdentityHeadHash:     headHash,
		NodeID:               nodeID,
		NodePublicKey:        base64.RawURLEncoding.EncodeToString(publicKey),
		Sequence:             "7",
		IssuedAt:             "2026-11-02T12:00:00Z",
		ExpiresAt:            "2026-11-02T12:10:00Z",
		AddressEnvelope:      addressEnvelope,
		Capabilities:         []string{"webrtc-datachannel/1"},
		RelayIDs:             []string{"rly_abcdefghijklmnop"},
	}
}

func advertisementTestKey() ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	return ed25519.NewKeyFromSeed(seed)
}

func bytesOf(count int, value byte) []byte {
	data := make([]byte, count)
	for i := range data {
		data[i] = value
	}
	return data
}

func TestAdvertisementKnownAnswerSigningDomain(t *testing.T) {
	privateKey := advertisementTestKey()
	ad := advertisementFixture(t, privateKey)
	canonical, signature, err := Sign(ad, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(privateKey.Public().(ed25519.PublicKey), append([]byte(signDomain), canonical...), signature) {
		t.Fatal("domain-separated signature did not verify")
	}
	digest := sha256.Sum256(canonical)
	if len(digest) != sha256.Size {
		t.Fatal("unexpected digest size")
	}
}

func TestSignedAdvertisementSharedVector(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "discovery-v1", "test-vector.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Advertisement Advertisement `json:"advertisement"`
		Canonical     string        `json:"canonical_json"`
		Signature     string        `json:"signature"`
		VerifiedAt    string        `json:"verified_at"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	canonical, err := Canonicalize(vector.Advertisement)
	if err != nil || string(canonical) != vector.Canonical {
		t.Fatalf("canonical vector mismatch: err=%v", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(vector.Signature)
	if err != nil {
		t.Fatal(err)
	}
	now, err := time.Parse(time.RFC3339, vector.VerifiedAt)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(canonical, signature, now)
	if err != nil || verified.Advertisement.NodeID != vector.Advertisement.NodeID {
		t.Fatalf("shared vector rejected: node=%q err=%v", verified.Advertisement.NodeID, err)
	}
}
