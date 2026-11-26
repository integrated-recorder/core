package cryptoarchive

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func FuzzParseKeyEnvelopeCanonicalRoundTrip(f *testing.F) {
	valid, err := keyEnvelopeFuzzSeed()
	if err != nil {
		f.Fatalf("load canonical key-envelope seed: %v", err)
	}
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"envelope_version":"ir.key-envelope/1","envelope_version":"ir.key-envelope/1"}`))
	f.Add(append(bytes.Clone(valid), ' '))

	f.Fuzz(func(t *testing.T, input []byte) {
		envelope, err := ParseKeyEnvelope(input)
		if err != nil {
			return
		}
		encoded, err := MarshalKeyEnvelope(envelope)
		if err != nil {
			t.Fatalf("ParseKeyEnvelope accepted an envelope that Marshal rejects: %v", err)
		}
		canonicalInput, err := identityprotocol.Canonicalize(input)
		if err != nil || !bytes.Equal(encoded, canonicalInput) {
			t.Fatal("accepted key envelope did not normalize to its canonical wire bytes")
		}
	})
}

func FuzzParseSessionDEKEnvelopeCanonicalRoundTrip(f *testing.F) {
	valid, err := sessionDEKEnvelopeFuzzSeed()
	if err != nil {
		f.Fatalf("load canonical session-DEK seed: %v", err)
	}
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"envelope_version":"ir.session-dek-envelope/1","envelope_version":"ir.session-dek-envelope/1"}`))
	f.Add(append(bytes.Clone(valid), ' '))

	f.Fuzz(func(t *testing.T, input []byte) {
		envelope, err := ParseSessionDEKEnvelope(input)
		if err != nil {
			return
		}
		encoded, err := MarshalSessionDEKEnvelope(envelope)
		if err != nil {
			t.Fatalf("ParseSessionDEKEnvelope accepted an envelope that Marshal rejects: %v", err)
		}
		canonicalInput, err := identityprotocol.Canonicalize(input)
		if err != nil || !bytes.Equal(encoded, canonicalInput) {
			t.Fatal("accepted session-DEK envelope did not normalize to its canonical wire bytes")
		}
	})
}

func keyEnvelopeFuzzSeed() ([]byte, error) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "cryptoarchive-v1", "envelope-test-vector.json"))
	if err != nil {
		return nil, err
	}
	var vector struct {
		Envelope json.RawMessage `json:"envelope"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		return nil, err
	}
	if len(vector.Envelope) == 0 {
		return nil, ErrEnvelopeInvalid
	}
	return bytes.Clone(vector.Envelope), nil
}

func sessionDEKEnvelopeFuzzSeed() ([]byte, error) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "cryptoarchive-v1", "session-key-envelope-test-vector.json"))
	if err != nil {
		return nil, err
	}
	var vector struct {
		Envelope json.RawMessage `json:"envelope"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		return nil, err
	}
	if len(vector.Envelope) == 0 {
		return nil, ErrSessionEnvelopeInvalid
	}
	return bytes.Clone(vector.Envelope), nil
}
