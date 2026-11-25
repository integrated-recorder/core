package identityprotocol

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func FuzzCanonicalizeIsIdempotent(f *testing.F) {
	vector := loadVector(f)
	f.Add([]byte(vector.Event))
	f.Add([]byte(`{"a":"b","c":[true,null]}`))
	f.Add([]byte(`{"duplicate":"a","duplicate":"b"}`))
	f.Add([]byte(`{"number":1}`))

	f.Fuzz(func(t *testing.T, input []byte) {
		canonical, err := Canonicalize(input)
		if err != nil {
			return
		}
		again, err := Canonicalize(canonical)
		if err != nil {
			t.Fatalf("canonical output was rejected: %v", err)
		}
		if !bytes.Equal(canonical, again) {
			t.Fatal("canonicalization is not idempotent")
		}
	})
}

func FuzzVerifyEventFailsClosed(f *testing.F) {
	vector := loadVector(f)
	signature, err := base64.RawURLEncoding.DecodeString(vector.Signature)
	if err != nil {
		f.Fatalf("decode vector signature: %v", err)
	}
	f.Add([]byte(vector.Canonical), signature)
	f.Add([]byte(`{"event_version":"ir.identity.event/1"}`), []byte("bad signature"))
	f.Add([]byte("null"), make([]byte, 64))

	f.Fuzz(func(t *testing.T, wire, signature []byte) {
		event, err := VerifyEvent(wire, signature)
		if err != nil {
			return
		}
		if !bytes.Equal(event.Canonical, wire) {
			t.Fatal("verified event did not preserve the exact canonical wire bytes")
		}
		if event.EventHash != EventHash(wire) {
			t.Fatal("verified event hash does not bind the wire bytes")
		}
	})
}
