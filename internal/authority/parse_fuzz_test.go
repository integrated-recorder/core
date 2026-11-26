package authority

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func FuzzParseSignedGrant(f *testing.F) {
	valid, err := authorityVectorGrantWire()
	if err != nil {
		f.Fatalf("load signed grant vector: %v", err)
	}
	addAuthorityParserSeeds(f, valid, maxSignedGrantBytes, func(input []byte) bool {
		_, err := ParseSignedGrant(input)
		return err == nil
	})

	f.Fuzz(func(t *testing.T, input []byte) {
		parsed, err := ParseSignedGrant(input)
		if err != nil {
			return
		}
		encoded, err := json.Marshal(parsed)
		if err != nil {
			t.Fatalf("accepted grant cannot be marshaled: %v", err)
		}
		assertAuthorityCanonicalRoundTrip(t, input, encoded)
	})
}

func FuzzParseAuthorityEvent(f *testing.F) {
	valid, err := authorityVectorEventWire()
	if err != nil {
		f.Fatalf("load signed authority event vector: %v", err)
	}
	addAuthorityParserSeeds(f, valid, maxAuthorityEventBytes, func(input []byte) bool {
		_, err := ParseAuthorityEvent(input)
		return err == nil
	})

	f.Fuzz(func(t *testing.T, input []byte) {
		parsed, err := ParseAuthorityEvent(input)
		if err != nil {
			return
		}
		encoded, err := json.Marshal(AuthorityEventWire{
			Event:     parsed.Event,
			Signature: base64.RawURLEncoding.EncodeToString(parsed.Signature),
		})
		if err != nil {
			t.Fatalf("accepted authority event cannot be marshaled: %v", err)
		}
		assertAuthorityCanonicalRoundTrip(t, input, encoded)
	})
}

func addAuthorityParserSeeds(f *testing.F, valid []byte, maximum int, accepts func([]byte) bool) {
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add(append(bytes.Clone(valid), []byte(` {}`)...))
	f.Add([]byte(strings.Replace(string(valid), "{", `{"unknown_field":true,`, 1)))
	f.Add(replaceAuthorityWireSignature(valid, "!not-base64url!"))
	f.Add(duplicateAuthorityWireSignature(valid))
	f.Add(bytes.Repeat([]byte{'x'}, maximum))
	f.Add(bytes.Repeat([]byte{'x'}, maximum+1))
	if len(valid) <= maximum {
		atLimit := append(bytes.Clone(valid), bytes.Repeat([]byte{' '}, maximum-len(valid))...)
		if accepts(atLimit) {
			f.Add(atLimit)
		}
	}
}

func duplicateAuthorityWireSignature(valid []byte) []byte {
	const marker = `"signature":"`
	text := string(valid)
	start := strings.Index(text, marker)
	if start < 0 {
		return []byte(`{"signature":"x","signature":"x"}`)
	}
	end := strings.IndexByte(text[start+len(marker):], '"')
	if end < 0 {
		return []byte(`{"signature":"x","signature":"x"}`)
	}
	fieldStart := start
	fieldEnd := start + len(marker) + end + 1
	field := text[fieldStart:fieldEnd]
	return []byte(text[:fieldEnd] + "," + field + text[fieldEnd:])
}

func replaceAuthorityWireSignature(valid []byte, replacement string) []byte {
	const marker = `"signature":"`
	text := string(valid)
	start := strings.Index(text, marker)
	if start < 0 {
		return []byte(`{"signature":"!not-base64url!"}`)
	}
	start += len(marker)
	end := strings.IndexByte(text[start:], '"')
	if end < 0 {
		return []byte(`{"signature":"!not-base64url!"}`)
	}
	return []byte(text[:start] + replacement + text[start+end:])
}

func assertAuthorityCanonicalRoundTrip(t *testing.T, input, encoded []byte) {
	t.Helper()
	canonicalInput, err := identityprotocol.Canonicalize(input)
	if err != nil {
		t.Fatalf("accepted input is not canonicalizable: %v", err)
	}
	canonicalEncoded, err := identityprotocol.Canonicalize(encoded)
	if err != nil || !bytes.Equal(canonicalEncoded, canonicalInput) {
		t.Fatalf("accepted authority wire did not round-trip canonically: err=%v", err)
	}
}

func authorityVectorGrantWire() ([]byte, error) {
	vector, err := readAuthorityVector()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Grant     json.RawMessage `json:"grant"`
		Signature string          `json:"signature"`
	}{Grant: vector.Grant, Signature: vector.Signature})
}

func authorityVectorEventWire() ([]byte, error) {
	vector, err := readAuthorityVector()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Event     json.RawMessage `json:"event"`
		Signature string          `json:"signature"`
	}{Event: vector.ArchiveClaimEvent.Event, Signature: vector.ArchiveClaimEvent.Signature})
}

type authorityParserVector struct {
	Grant             json.RawMessage `json:"grant"`
	Signature         string          `json:"signature"`
	ArchiveClaimEvent struct {
		Event     json.RawMessage `json:"event"`
		Signature string          `json:"signature"`
	} `json:"archive_claim_event"`
}

func readAuthorityVector() (authorityParserVector, error) {
	data, err := os.ReadFile(filepath.Join("..", "..", "protocol", "authority-v1", "test-vector.json"))
	if err != nil {
		return authorityParserVector{}, err
	}
	var vector authorityParserVector
	if err := json.Unmarshal(data, &vector); err != nil {
		return authorityParserVector{}, err
	}
	if len(vector.Grant) == 0 || vector.Signature == "" || len(vector.ArchiveClaimEvent.Event) == 0 || vector.ArchiveClaimEvent.Signature == "" {
		return authorityParserVector{}, ErrInvalid
	}
	return vector, nil
}
