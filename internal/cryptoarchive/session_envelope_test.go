package cryptoarchive

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestSessionDEKEnvelopeBindsPasskeyRecipientAndArchiveAuthority(t *testing.T) {
	_, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identityprotocol.NodeID(nodePrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	binding := SessionDEKBinding{
		ArchiveID: "arc_aaaaaaaaaaaaaaaaaaaaaa", SnapshotID: "snap_" + strings.Repeat("1", 64), KeyEpoch: 7,
		OwnerSubjectID: "os_aaaaaaaaaaaaaaaaaaaaaa", GrantHash: "sha256:" + strings.Repeat("b", 64), SupplierNodeID: nodeID,
	}
	private, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dataKey := bytes.Repeat([]byte{0x5a}, 32)
	envelope, err := SealSessionDEK(dataKey, private.PublicKey().Bytes(), binding)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalSessionDEKEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseSessionDEKEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenSessionDEK(parsed, private.Bytes(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, dataKey) {
		t.Fatal("opened DEK does not match source key")
	}
	clear(opened)

	wrongBinding := binding
	wrongBinding.GrantHash = "sha256:" + strings.Repeat("c", 64)
	if _, err := OpenSessionDEK(parsed, private.Bytes(), wrongBinding); !errors.Is(err, ErrSessionEnvelopeUnauthorized) {
		t.Fatalf("wrong capability binding error = %v", err)
	}
	otherPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSessionDEK(parsed, otherPrivate.Bytes(), binding); !errors.Is(err, ErrSessionEnvelopeUnauthorized) {
		t.Fatalf("wrong ephemeral recipient error = %v", err)
	}

	parsed.SupplierNodeID = "nd_aaaaaaaaaaaaaaaaaaaaaa"
	if _, err := OpenSessionDEK(parsed, private.Bytes(), binding); !errors.Is(err, ErrSessionEnvelopeUnauthorized) {
		t.Fatalf("wrong supplier binding error = %v", err)
	}
}

func TestParseSessionDEKEnvelopeRejectsUnknownAndNonCanonicalFields(t *testing.T) {
	_, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identityprotocol.NodeID(nodePrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	private, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	binding := SessionDEKBinding{
		ArchiveID: "arc_aaaaaaaaaaaaaaaaaaaaaa", SnapshotID: "snap_" + strings.Repeat("1", 64), KeyEpoch: 1,
		OwnerSubjectID: "os_aaaaaaaaaaaaaaaaaaaaaa", GrantHash: "sha256:" + strings.Repeat("b", 64), SupplierNodeID: nodeID,
	}
	envelope, err := SealSessionDEK(bytes.Repeat([]byte{9}, 32), private.PublicKey().Bytes(), binding)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalSessionDEKEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	withUnknown := bytes.TrimSuffix(encoded, []byte("}"))
	withUnknown = append(withUnknown, []byte(`,"private_key":"must-not-parse"}`)...)
	if _, err := ParseSessionDEKEnvelope(withUnknown); !errors.Is(err, ErrSessionEnvelopeInvalid) {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := ParseSessionDEKEnvelope(append(encoded, []byte(" ")...)); !errors.Is(err, ErrSessionEnvelopeInvalid) {
		t.Fatalf("non-canonical encoding error = %v", err)
	}
}

func TestSessionDEKEnvelopeOpensSharedBrowserProtocolVector(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "cryptoarchive-v1", "session-key-envelope-test-vector.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		TestOnlyNote           string `json:"test_only_note"`
		RecipientPrivateKeyHex string `json:"recipient_private_key_hex"`
		DEKHex                 string `json:"dek_hex"`
		Binding                struct {
			ArchiveID      string `json:"archive_id"`
			SnapshotID     string `json:"snapshot_id"`
			KeyEpoch       string `json:"key_epoch"`
			OwnerSubjectID string `json:"owner_subject_id"`
			GrantHash      string `json:"grant_hash"`
			SupplierNodeID string `json:"supplier_node_id"`
		} `json:"binding"`
		Envelope SessionDEKEnvelope `json:"envelope"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(vector.TestOnlyNote, "never be used") {
		t.Fatal("shared fixture is not marked as test-only key material")
	}
	privateKey, err := hex.DecodeString(vector.RecipientPrivateKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(privateKey)
	wantDEK, err := hex.DecodeString(vector.DEKHex)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(wantDEK)
	epoch, err := strconv.ParseUint(vector.Binding.KeyEpoch, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	binding := SessionDEKBinding{
		ArchiveID: vector.Binding.ArchiveID, SnapshotID: vector.Binding.SnapshotID, KeyEpoch: epoch,
		OwnerSubjectID: vector.Binding.OwnerSubjectID, GrantHash: vector.Binding.GrantHash,
		SupplierNodeID: vector.Binding.SupplierNodeID,
	}
	opened, err := OpenSessionDEK(vector.Envelope, privateKey, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(opened)
	if !bytes.Equal(opened, wantDEK) {
		t.Fatal("Go did not open the shared browser-compatible HPKE vector")
	}
}
