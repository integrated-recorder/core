package archivesnapshot

import (
	"encoding/json"
	"os"
	"testing"
)

func TestArchiveSnapshotSharedCanonicalVector(t *testing.T) {
	data, err := os.ReadFile("../../protocol/archive-snapshot-v1/test-vector.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Input       string `json:"input"`
		Canonical   string `json:"canonical"`
		ContentHash string `json:"content_hash"`
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalizeSnapshotJSON([]byte(vector.Input))
	if err != nil || string(canonical) != vector.Canonical {
		t.Fatalf("canonical=%s err=%v want=%s", canonical, err, vector.Canonical)
	}
	if got := hashWithDomain(contentDomainV1, canonical); got != vector.ContentHash {
		t.Fatalf("content hash=%s want=%s", got, vector.ContentHash)
	}
}
