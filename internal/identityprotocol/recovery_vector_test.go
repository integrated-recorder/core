package identityprotocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

type recoveryVector struct {
	EntropyHex           string `json:"entropy_hex"`
	Mnemonic             string `json:"mnemonic_english"`
	HKDFSalt             string `json:"hkdf_salt_utf8"`
	SigningInfo          string `json:"signing_info_utf8"`
	SigningSeedHex       string `json:"signing_seed_hex"`
	SigningPublicKey     string `json:"signing_public_key_base64url"`
	EncryptionInfo       string `json:"encryption_info_utf8"`
	EncryptionPrivateHex string `json:"encryption_private_key_hex"`
	EncryptionPublicKey  string `json:"encryption_public_key_base64url"`
	IdentityID           string `json:"identity_id"`
	SignerKeyID          string `json:"signer_key_id"`
	RecoveryFileHex      string `json:"recovery_file_hex"`
}

func TestRecoveryDerivationVectorCrossLanguage(t *testing.T) {
	path := filepath.Join("..", "..", "protocol", "recovery-v1", "test-vector.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v recoveryVector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	entropy, err := hex.DecodeString(v.EntropyHex)
	if err != nil || len(entropy) != 32 {
		t.Fatalf("invalid public test entropy: len=%d err=%v", len(entropy), err)
	}
	derive := func(info string) []byte {
		reader := hkdf.New(sha256.New, entropy, []byte(v.HKDFSalt), []byte(info))
		out := make([]byte, 32)
		if _, err := io.ReadFull(reader, out); err != nil {
			t.Fatalf("HKDF derivation: %v", err)
		}
		return out
	}
	signingSeed := derive(v.SigningInfo)
	if hex.EncodeToString(signingSeed) != v.SigningSeedHex {
		t.Fatal("Ed25519 seed differs from the shared vector")
	}
	signingKey := ed25519.NewKeyFromSeed(signingSeed)
	publicKey := signingKey.Public().(ed25519.PublicKey)
	if base64.RawURLEncoding.EncodeToString(publicKey) != v.SigningPublicKey {
		t.Fatal("Ed25519 public key differs from the shared vector")
	}
	idDigest := sha256.Sum256(append([]byte("IR/global-identity-id/v1\x00"), publicKey...))
	if got := "gi_" + base64.RawURLEncoding.EncodeToString(idDigest[:16]); got != v.IdentityID {
		t.Fatalf("identity ID=%s, want %s", got, v.IdentityID)
	}
	keyDigest := sha256.Sum256(append([]byte(keyDomain), publicKey...))
	if got := "ik_" + base64.RawURLEncoding.EncodeToString(keyDigest[:]); got != v.SignerKeyID {
		t.Fatalf("signer key ID=%s, want %s", got, v.SignerKeyID)
	}
	encryptionPrivate := derive(v.EncryptionInfo)
	if hex.EncodeToString(encryptionPrivate) != v.EncryptionPrivateHex {
		t.Fatal("X25519 private scalar differs from the shared vector")
	}
	encryptionPublic, err := curve25519.X25519(encryptionPrivate, curve25519.Basepoint)
	if err != nil {
		t.Fatalf("derive X25519 public key: %v", err)
	}
	if base64.RawURLEncoding.EncodeToString(encryptionPublic) != v.EncryptionPublicKey {
		t.Fatal("X25519 public key differs from the shared vector")
	}
	body := append([]byte("IRRC\x01"), entropy...)
	checksum := sha256.Sum256(append([]byte("IR/recovery-file/v1/check\x00"), body...))
	recoveryFile := append(body, checksum[:16]...)
	if hex.EncodeToString(recoveryFile) != v.RecoveryFileHex {
		t.Fatal("versioned recovery file differs from the shared vector")
	}
}
