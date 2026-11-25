package authority

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGoAuthorityVector(t *testing.T) {
	path := filepath.Join("..", "..", "protocol", "authority-v1", "test-vector.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Format            string `json:"format"`
		RootSeed          string `json:"root_seed"`
		Grant             Grant  `json:"grant"`
		Signature         string `json:"signature"`
		GrantHash         string `json:"grant_hash"`
		ArchiveClaimEvent struct {
			Event     AuthorityEvent `json:"event"`
			Signature string         `json:"signature"`
			EventHash string         `json:"event_hash"`
		} `json:"archive_claim_event"`
	}
	if err := json.Unmarshal(data, &vector); err != nil || vector.Format != "ir.capability.test-vector/1" {
		t.Fatalf("decode authority vector: err=%v format=%q", err, vector.Format)
	}
	seed, err := base64.RawURLEncoding.DecodeString(vector.RootSeed)
	if err != nil || len(seed) != ed25519.SeedSize {
		t.Fatal("authority vector seed is invalid")
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	signed, err := SignGrant(vector.Grant, privateKey)
	if err != nil {
		t.Fatalf("sign authority vector: %v", err)
	}
	if base64.RawURLEncoding.EncodeToString(signed.Signature) != vector.Signature {
		t.Fatal("Go signature does not match the shared authority vector")
	}
	hash, err := GrantHash(vector.Grant)
	if err != nil || hash != vector.GrantHash {
		t.Fatalf("grant hash = %q err=%v, want %q", hash, err, vector.GrantHash)
	}
	claim, err := SignAuthorityEvent(vector.ArchiveClaimEvent.Event, privateKey)
	if err != nil || base64.RawURLEncoding.EncodeToString(claim.Signature) != vector.ArchiveClaimEvent.Signature {
		t.Fatalf("archive claim event signature does not match shared vector: err=%v", err)
	}
	eventHash, err := AuthorityEventHash(vector.ArchiveClaimEvent.Event)
	if err != nil || eventHash != vector.ArchiveClaimEvent.EventHash {
		t.Fatalf("archive claim event hash = %q err=%v, want %q", eventHash, err, vector.ArchiveClaimEvent.EventHash)
	}
	wire, err := json.Marshal(struct {
		Grant     Grant  `json:"grant"`
		Signature string `json:"signature"`
	}{Grant: vector.Grant, Signature: vector.Signature})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseSignedGrant(wire)
	if err != nil || base64.RawURLEncoding.EncodeToString(parsed.Signature) != vector.Signature {
		t.Fatalf("parse shared signed grant: err=%v", err)
	}
}
