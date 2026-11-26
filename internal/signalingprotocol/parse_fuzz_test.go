package signalingprotocol

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func FuzzParseCanonicalMessageCanonicalRoundTrip(f *testing.F) {
	valid, err := canonicalMessageFuzzSeed()
	if err != nil {
		f.Fatalf("build canonical signaling seed: %v", err)
	}
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"version":"ir.peer-signaling/1","version":"ir.peer-signaling/1"}`))
	f.Add(append(bytes.Clone(valid), ' '))
	f.Add(append(bytes.Clone(valid), []byte(` {}`)...))

	f.Fuzz(func(t *testing.T, input []byte) {
		message, err := ParseCanonicalMessage(input)
		if err != nil {
			return
		}
		encoded, err := CanonicalMessage(message)
		if err != nil {
			t.Fatalf("parser accepted a message that canonical encoder rejects: %v", err)
		}
		if !bytes.Equal(encoded, input) {
			t.Fatal("accepted signaling message did not round-trip to identical canonical bytes")
		}
	})
}

func canonicalMessageFuzzSeed() ([]byte, error) {
	rootPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	sourcePrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x32}, ed25519.SeedSize))
	targetPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x33}, ed25519.SeedSize))
	identityID, err := identityprotocol.IdentityID(rootPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		return nil, err
	}
	sourceNodeID, err := identityprotocol.NodeID(sourcePrivate.Public().(ed25519.PublicKey))
	if err != nil {
		return nil, err
	}
	targetNodeID, err := identityprotocol.NodeID(targetPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		return nil, err
	}
	const headHash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	sessionID, err := NewSessionID(bytes.NewReader(bytes.Repeat([]byte{0x41}, 16)))
	if err != nil {
		return nil, err
	}
	message, err := NewMessage(KindCancel, sessionID, identityID, headHash, sourceNodeID, identityID, targetNodeID, now, bytes.NewReader(bytes.Repeat([]byte{0x42}, 16)))
	if err != nil {
		return nil, err
	}
	message, err = Sign(message, sourcePrivate)
	if err != nil {
		return nil, err
	}
	return CanonicalMessage(message)
}
