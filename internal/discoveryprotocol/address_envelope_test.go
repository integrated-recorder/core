package discoveryprotocol

import (
	"crypto/ecdh"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAddressEnvelopeRoundTripAndContextBinding(t *testing.T) {
	privateBytes := bytesOf(32, 0x39)
	privateKey, err := ecdh.X25519().NewPrivateKey(privateBytes)
	if err != nil {
		t.Fatal(err)
	}
	context := addressContextFixture(t)
	plaintext := []byte(`{"addresses":["wss://node.example.test/signal"]}`)
	envelope, err := SealAddressEnvelope(privateKey.PublicKey().Bytes(), context, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseAddressEnvelope(envelope)
	if err != nil || parsed.RecipientPublicKeySHA256 != AddressRecipientPublicKeyHash(privateKey.PublicKey().Bytes()) {
		t.Fatalf("address envelope parse=%+v err=%v", parsed, err)
	}
	opened, err := OpenAddressEnvelope(envelope, privateBytes, context)
	if err != nil || string(opened) != string(plaintext) {
		t.Fatalf("opened address payload=%s err=%v", opened, err)
	}
	clear(opened)

	wrongContext := context
	wrongContext.IdentityHeadHash = "sha256:" + strings.Repeat("b", 64)
	if _, err := OpenAddressEnvelope(envelope, privateBytes, wrongContext); !errors.Is(err, ErrAddressEnvelope) {
		t.Fatalf("envelope opened for another identity head: %v", err)
	}
	wrongPrivate := bytesOf(32, 0x3a)
	if _, err := OpenAddressEnvelope(envelope, wrongPrivate, context); !errors.Is(err, ErrAddressEnvelope) {
		t.Fatalf("envelope opened with another recovery key: %v", err)
	}
}

func TestAddressEnvelopeRejectsMalformedAndOversizedInputs(t *testing.T) {
	privateKey, err := ecdh.X25519().NewPrivateKey(bytesOf(32, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	context := addressContextFixture(t)
	envelope, err := SealAddressEnvelope(privateKey.PublicKey().Bytes(), context, []byte(`{"addresses":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseAddressEnvelope(envelope + "="); !errors.Is(err, ErrAddressEnvelope) {
		t.Fatalf("noncanonical base64 accepted: %v", err)
	}
	if _, err := SealAddressEnvelope(privateKey.PublicKey().Bytes(), context, []byte(`{"addresses":[],"addresses":[]}`)); !errors.Is(err, ErrAddressEnvelope) {
		t.Fatalf("ambiguous JSON accepted: %v", err)
	}
	if _, err := SealAddressEnvelope(privateKey.PublicKey().Bytes(), context, bytesOf(addressPayloadMax+1, 'x')); !errors.Is(err, ErrAddressEnvelope) {
		t.Fatalf("oversized payload accepted: %v", err)
	}
	if _, err := SealAddressEnvelope(make([]byte, 32), context, []byte(`{"addresses":[]}`)); !errors.Is(err, ErrAddressEnvelope) {
		t.Fatalf("low-order recipient key accepted: %v", err)
	}
}

func TestAddressEnvelopeSharedVector(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "discovery-v1", "address-envelope-test-vector.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		TestOnly bool `json:"test_only"`
		Context  struct {
			GlobalIdentityHint string `json:"global_identity_hint"`
			IdentityHeadHash   string `json:"identity_head_hash"`
			NodeID             string `json:"node_id"`
		} `json:"context"`
		PrivateKeyHex   string `json:"recipient_private_key_hex"`
		Plaintext       string `json:"plaintext_json"`
		GoEnvelope      string `json:"address_envelope"`
		BrowserEnvelope string `json:"browser_address_envelope"`
	}
	if err := json.Unmarshal(data, &vector); err != nil || !vector.TestOnly {
		t.Fatalf("test vector is invalid: err=%v", err)
	}
	privateKey, err := hex.DecodeString(vector.PrivateKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	context := AddressContext{
		GlobalIdentityHint: vector.Context.GlobalIdentityHint,
		IdentityHeadHash:   vector.Context.IdentityHeadHash,
		NodeID:             vector.Context.NodeID,
	}
	for name, envelope := range map[string]string{"Go": vector.GoEnvelope, "browser": vector.BrowserEnvelope} {
		plaintext, err := OpenAddressEnvelope(envelope, privateKey, context)
		if err != nil || string(plaintext) != vector.Plaintext {
			t.Fatalf("shared %s address vector plaintext=%s err=%v", name, plaintext, err)
		}
		clear(plaintext)
	}
}

func addressContextFixture(t *testing.T) AddressContext {
	t.Helper()
	hint, err := GlobalIdentityHint("gi_AAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	return AddressContext{
		GlobalIdentityHint: hint,
		IdentityHeadHash:   "sha256:" + strings.Repeat("a", 64),
		NodeID:             "nd_AAAAAAAAAAAAAAAAAAAAAA",
	}
}
