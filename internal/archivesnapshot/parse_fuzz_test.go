package archivesnapshot

import (
	"bytes"
	"testing"

	"github.com/integrated-recorder/core/internal/archiveindex"
)

func FuzzParseSnapshotCanonicalRoundTrip(f *testing.F) {
	valid, err := fuzzSnapshotJSON()
	if err != nil {
		f.Fatalf("build canonical fuzz seed: %v", err)
	}
	if _, err := Parse(valid); err != nil {
		f.Fatalf("canonical fuzz seed is invalid: %v", err)
	}
	f.Add(valid)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"schema_version":2,"schema_version":2}`))
	f.Add(append(bytes.Clone(valid), ' '))

	f.Fuzz(func(t *testing.T, input []byte) {
		snapshot, err := Parse(input)
		if err != nil {
			return
		}
		encoded, err := Marshal(snapshot)
		if err != nil {
			t.Fatalf("Parse accepted a snapshot that Marshal rejects: %v", err)
		}
		if !bytes.Equal(encoded, input) {
			t.Fatal("accepted snapshot did not round-trip to its canonical wire bytes")
		}
	})
}

func fuzzSnapshotJSON() ([]byte, error) {
	session, err := archiveindex.NewSessionIdentity("recording-snapshot-fuzz", "hls", nil, "broadcast-fuzz")
	if err != nil {
		return nil, err
	}
	content := Content{
		ArchiveID: "arc_AAAAAAAAAAAAAAAAAAAAAA",
		Session:   session,
		Capture:   archiveindex.CaptureClosed,
		Archive:   archiveindex.ArchiveSealed,
		Objects:   []Object{}, Coverage: []Coverage{},
	}
	provenance := []ClaimProvenance{}
	contentBytes, err := canonicalJSON(content)
	if err != nil {
		return nil, err
	}
	provenanceBytes, err := canonicalJSON(provenance)
	if err != nil {
		return nil, err
	}
	contentHash := hashWithDomain(contentDomainV2, contentBytes)
	snapshot := Snapshot{
		SchemaVersion:  SchemaVersion,
		Sequence:       1,
		SnapshotID:     snapshotID(contentHash),
		ContentHash:    contentHash,
		ProvenanceHash: hashWithDomain(provenanceDomain, provenanceBytes),
		Content:        content,
		Provenance:     provenance,
	}
	return Marshal(snapshot)
}
