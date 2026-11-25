package identityprotocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type vector struct {
	Event        json.RawMessage `json:"event"`
	Canonical    string          `json:"canonical_json"`
	EventHash    string          `json:"event_hash"`
	Signature    string          `json:"signature"`
	TestOnlyNote string          `json:"test_only_key_note"`
}

func TestSignedIdentityEventVector(t *testing.T) {
	v := loadVector(t)
	event, err := decodeStrictJSON(v.Event)
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	canonical, err := canonicalJSON(event)
	if err != nil {
		t.Fatalf("canonicalize event: %v", err)
	}
	if string(canonical) != v.Canonical {
		t.Fatalf("canonical bytes differ\n got: %s\nwant: %s", canonical, v.Canonical)
	}

	message := append([]byte(signDomain), canonical...)
	signature, err := base64.RawURLEncoding.DecodeString(v.Signature)
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	publicKeyText := event.(map[string]any)["signer_public_key"].(string)
	publicKey, err := base64.RawURLEncoding.DecodeString(publicKeyText)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		t.Fatalf("decode public key: length=%d err=%v", len(publicKey), err)
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), message, signature) {
		t.Fatal("fixture signature did not verify")
	}

	hash := sha256.Sum256(append([]byte(hashDomain), canonical...))
	if got := "sha256:" + hex.EncodeToString(hash[:]); got != v.EventHash {
		t.Fatalf("event hash=%s, want %s", got, v.EventHash)
	}
	keyDigest := sha256.Sum256(append([]byte(keyDomain), publicKey...))
	keyID := "ik_" + base64.RawURLEncoding.EncodeToString(keyDigest[:])
	if got := event.(map[string]any)["signer_key_id"]; got != keyID {
		t.Fatalf("signer key id=%v, want %s", got, keyID)
	}
	identityDigest := sha256.Sum256(append([]byte("IR/global-identity-id/v1\x00"), publicKey...))
	identityID := "gi_" + base64.RawURLEncoding.EncodeToString(identityDigest[:16])
	if got := event.(map[string]any)["identity_id"]; got != identityID {
		t.Fatalf("identity id=%v, want %s", got, identityID)
	}
	verified, err := VerifyEvent(canonical, signature)
	if err != nil {
		t.Fatalf("production event verifier rejected the vector: %v", err)
	}
	if verified.EventHash != v.EventHash || verified.IdentityID != identityID || verified.Sequence != "0" || verified.Operation != "genesis" {
		t.Fatalf("verified event projection is inconsistent: %+v", verified)
	}
}

func TestNodeIDAndEnrollmentProofAreDomainSeparatedAndStrict(t *testing.T) {
	seed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	nodeID, err := NodeID(publicKey)
	if err != nil || !IsNodeID(nodeID) {
		t.Fatalf("node ID=%q err=%v", nodeID, err)
	}
	repeated, err := NodeID(publicKey)
	if err != nil || repeated != nodeID {
		t.Fatalf("node ID is not stable: first=%q second=%q err=%v", nodeID, repeated, err)
	}
	identityID, err := IdentityID(publicKey)
	if err != nil || identityID == nodeID {
		t.Fatalf("node and global identity identifiers are not separated: identity=%q node=%q err=%v", identityID, nodeID, err)
	}
	previous := EventHash([]byte("test-parent"))
	nonce := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x41}, 32))
	message, err := NodeEnrollmentProofMessage(identityID, previous, nonce)
	if err != nil || !ed25519.Verify(publicKey, message, ed25519.Sign(privateKey, message)) {
		t.Fatalf("valid enrollment proof message failed: err=%v", err)
	}
	if _, err := NodeEnrollmentProofMessage(identityID, previous, nonce+"="); err == nil {
		t.Fatal("padded enrollment nonce was accepted")
	}
	if _, err := NodeEnrollmentProofMessage(identityID, previous, base64.RawURLEncoding.EncodeToString([]byte("short"))); err == nil {
		t.Fatal("short enrollment nonce was accepted")
	}
	if _, err := NodeID(ed25519.PublicKey(publicKey[:len(publicKey)-1])); err == nil {
		t.Fatal("short Node Key public key was accepted")
	}
}

func TestSignedIdentityEventVectorRejectsMutationAndWrongAuthority(t *testing.T) {
	v := loadVector(t)
	event, err := decodeStrictJSON(v.Event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalJSON(event)
	if err != nil {
		t.Fatal(err)
	}
	signature, _ := base64.RawURLEncoding.DecodeString(v.Signature)
	message := append([]byte(signDomain), canonical...)
	publicKeyBytes, _ := base64.RawURLEncoding.DecodeString(event.(map[string]any)["signer_public_key"].(string))
	publicKey := ed25519.PublicKey(publicKeyBytes)

	mutated, err := decodeStrictJSON(v.Event)
	if err != nil {
		t.Fatal(err)
	}
	mutated.(map[string]any)["identity_id"] = "gi_AQECAwQFBgcICQoLDA0ODw"
	mutatedBytes, err := canonicalJSON(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if ed25519.Verify(publicKey, append([]byte(signDomain), mutatedBytes...), signature) {
		t.Fatal("signature accepted a changed identity")
	}
	otherKey := append(ed25519.PublicKey(nil), publicKey...)
	otherKey[0] ^= 0x80
	if ed25519.Verify(otherKey, message, signature) {
		t.Fatal("signature accepted an untrusted public key")
	}
	if _, err := VerifyEvent(mutatedBytes, signature); err == nil {
		t.Fatal("production event verifier accepted a mutated identity")
	}
	if _, err := VerifyEvent(append([]byte(" "), canonical...), signature); err == nil {
		t.Fatal("production verifier accepted non-canonical wire bytes")
	}
}

func TestStrictIdentityEventJSONRejectsAmbiguousOrOutOfProfileInput(t *testing.T) {
	for name, input := range map[string]string{
		"duplicate property":  `{"a":"first","a":"second"}`,
		"numeric field":       `{"sequence":1}`,
		"trailing value":      `{"a":"b"} {"c":"d"}`,
		"lone high surrogate": `{"a":"\ud800"}`,
		"lone low surrogate":  `{"a":"\udc00"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeStrictJSON([]byte(input)); err == nil {
				t.Fatalf("accepted invalid input %s", input)
			}
		})
	}
	if _, err := canonicalJSON(map[string]any{"invalid key": "x"}); err == nil {
		t.Fatal("accepted a non-schema object key")
	}
	if _, err := canonicalJSON(map[string]any{"sequence": uint64(1)}); err == nil {
		t.Fatal("accepted a numeric value; protocol integers are decimal strings")
	}
	if _, err := canonicalJSON(map[string]any{"text": string([]byte{0xff})}); err == nil {
		t.Fatal("accepted invalid UTF-8")
	}
	if _, err := decodeStrictJSON([]byte(`{"text":"\ud83d\ude80"}`)); err != nil {
		t.Fatalf("rejected valid surrogate pair: %v", err)
	}
}

func TestCanonicalizeEnforcesDocumentSizeAndNestingBounds(t *testing.T) {
	// The protocol accepts a document at its byte limit and rejects the next
	// byte, independently of the caller's character count.
	content := strings.Repeat("a", maxCanonicalDocumentBytes-8)
	input := []byte(`{"a":"` + content + `"}`)
	if len(input) != maxCanonicalDocumentBytes {
		t.Fatalf("boundary fixture length=%d, want %d", len(input), maxCanonicalDocumentBytes)
	}
	if _, err := Canonicalize(input); err != nil {
		t.Fatalf("rejected document at byte limit: %v", err)
	}
	if _, err := Canonicalize(append(input, ' ')); err == nil {
		t.Fatal("accepted document over byte limit")
	}

	depth64 := strings.Repeat("[", 64) + `null` + strings.Repeat("]", 64)
	if _, err := Canonicalize([]byte(depth64)); err != nil {
		t.Fatalf("rejected nesting at depth limit: %v", err)
	}
	depth65 := "[" + depth64 + "]"
	if _, err := Canonicalize([]byte(depth65)); err == nil {
		t.Fatal("accepted nesting beyond depth limit")
	}
}

func TestJCSStringProfileEscapesControlsWithoutHTMLOrUnicodeRewriting(t *testing.T) {
	value := map[string]any{"text": "<>&\u2028/\n녹화"}
	got, err := canonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"text\":\"<>&\u2028/\\n녹화\"}"
	if string(got) != want {
		t.Fatalf("canonical string=%q, want %q", got, want)
	}
}

func TestTwoNodeIdentityHarnessFreezesOnValidFork(t *testing.T) {
	// This deterministic in-memory harness tests the contract only. It does not
	// claim that production identity replication or network transport exists.
	v := loadVector(t)
	event, err := decodeStrictJSON(v.Event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalJSON(event)
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	genesisSig := ed25519.Sign(privateKey, append([]byte(signDomain), canonical...))

	nodeA := newHarnessNode(publicKey)
	nodeB := newHarnessNode(publicKey)
	genesisHash := eventDigest(canonical)
	if err := nodeA.apply(canonical, genesisSig); err != nil {
		t.Fatalf("node A rejected genesis: %v", err)
	}
	if err := nodeB.apply(canonical, genesisSig); err != nil {
		t.Fatalf("node B rejected genesis: %v", err)
	}

	parent := fmt.Sprintf("sha256:%x", genesisHash)
	publicKeyText := base64.RawURLEncoding.EncodeToString(publicKey)
	keyDigest := sha256.Sum256(append([]byte(keyDomain), publicKey...))
	keyID := "ik_" + base64.RawURLEncoding.EncodeToString(keyDigest[:])
	forkEvent := func(label string) ([]byte, []byte) {
		event := map[string]any{
			"event_version":       "ir.identity.event/1",
			"identity_id":         "gi_NgLJOCrMiSllxIuMKTBZYw",
			"sequence":            "1",
			"previous_event_hash": parent,
			"operation":           "credential_add",
			"issued_at":           "2026-10-25T00:00:01Z",
			"signer_key_id":       keyID,
			"signer_public_key":   publicKeyText,
			"payload":             map[string]any{"credential_label": label},
		}
		canonicalEvent, err := canonicalJSON(event)
		if err != nil {
			t.Fatalf("canonicalize fork event: %v", err)
		}
		signature := ed25519.Sign(privateKey, append([]byte(signDomain), canonicalEvent...))
		return canonicalEvent, signature
	}
	forkOne, sigOne := forkEvent("first-successor")
	forkTwo, sigTwo := forkEvent("second-successor")
	if err := nodeA.apply(forkOne, sigOne); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.apply(forkTwo, sigTwo); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.observeFork(nodeB.head); !errors.Is(err, errForkDetected) || !nodeA.frozen {
		t.Fatalf("fork was not frozen: err=%v frozen=%v", err, nodeA.frozen)
	}
	if err := nodeB.observeFork(nodeA.head); !errors.Is(err, errForkDetected) || !nodeB.frozen {
		t.Fatalf("second node did not freeze on the fork: err=%v frozen=%v", err, nodeB.frozen)
	}
}

var errForkDetected = errors.New("identity fork detected")

type harnessNode struct {
	key    ed25519.PublicKey
	head   string
	frozen bool
}

func newHarnessNode(key ed25519.PublicKey) *harnessNode {
	return &harnessNode{key: append(ed25519.PublicKey(nil), key...)}
}

func (n *harnessNode) apply(signedEvent []byte, signature []byte) error {
	if n.frozen {
		return errForkDetected
	}
	decoded, err := decodeStrictJSON(signedEvent)
	if err != nil {
		return err
	}
	canonical, err := canonicalJSON(decoded)
	if err != nil || !bytes.Equal(canonical, signedEvent) {
		return errors.New("event bytes are not canonical")
	}
	event, ok := decoded.(map[string]any)
	if !ok {
		return errors.New("event is not an object")
	}
	previous := ""
	if rawPrevious, ok := event["previous_event_hash"]; ok && rawPrevious != nil {
		previous, ok = rawPrevious.(string)
		if !ok {
			return errors.New("previous hash is not a string or null")
		}
	}
	if n.head == "" {
		if previous != "" {
			return errors.New("genesis has previous event")
		}
	} else if previous != n.head {
		return errors.New("event does not extend local head")
	}
	input := append([]byte(signDomain), canonical...)
	if !ed25519.Verify(n.key, input, signature) {
		return errors.New("invalid event signature")
	}
	digest := eventDigest(canonical)
	n.head = fmt.Sprintf("sha256:%x", digest)
	return nil
}

func (n *harnessNode) observeFork(remoteHead string) error {
	if n.head != "" && remoteHead != "" && n.head != remoteHead {
		n.frozen = true
		return errForkDetected
	}
	return nil
}

func loadVector(t testing.TB) vector {
	t.Helper()
	path := filepath.Join("..", "..", "protocol", "identity-v1", "test-vector.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v vector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	if v.EventHash == "" || v.Signature == "" || v.TestOnlyNote == "" {
		t.Fatal("incomplete identity event vector")
	}
	return v
}

func eventDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(hashDomain), canonical...))
}
