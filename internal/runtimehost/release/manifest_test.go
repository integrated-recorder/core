package release

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeAndVerifyManifest(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest := validManifest()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, data))
	got, err := DecodeAndVerifyManifest(data, signature, map[string]ed25519.PublicKey{"test-key": publicKey})
	if err != nil {
		t.Fatalf("valid signed manifest rejected: %v", err)
	}
	if got.ReleaseVersion != manifest.ReleaseVersion || got.KeyID != manifest.KeyID || len(got.Artifacts) != 4 {
		t.Fatalf("decoded manifest mismatch: %+v", got)
	}

	if _, err := DecodeAndVerifyManifest(append(data, ' '), signature, map[string]ed25519.PublicKey{"test-key": publicKey}); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("signature over non-identical manifest bytes: got %v", err)
	}
	if _, err := DecodeAndVerifyManifest(data, signature, map[string]ed25519.PublicKey{}); !errors.Is(err, ErrUnknownSigningKey) {
		t.Fatalf("unknown key error = %v", err)
	}
	if _, err := DecodeAndVerifyManifest(data, base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)), map[string]ed25519.PublicKey{"test-key": publicKey}); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("bad signature error = %v", err)
	}
	if _, err := DecodeAndVerifyManifest(data, strings.Repeat("A", 8<<20), map[string]ed25519.PublicKey{"test-key": publicKey}); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("oversized signature error = %v", err)
	}
}

func TestManifestRejectsMalformedAmbiguousAndUnsafeValues(t *testing.T) {
	tests := map[string]func(*Manifest){
		"unsupported schema":                  func(m *Manifest) { m.ManifestSchemaVersion++ },
		"malformed version":                   func(m *Manifest) { m.ReleaseVersion = "1.2" },
		"malformed timestamp":                 func(m *Manifest) { m.BuildTime = "tomorrow" },
		"invalid protocol range":              func(m *Manifest) { m.MinimumHostProtocol = 2; m.MaximumHostProtocol = 1 },
		"missing role":                        func(m *Manifest) { m.Artifacts = m.Artifacts[:3] },
		"duplicate role":                      func(m *Manifest) { m.Artifacts[1].Role = m.Artifacts[0].Role },
		"duplicate filename":                  func(m *Manifest) { m.Artifacts[1].Filename = m.Artifacts[0].Filename },
		"case-insensitive duplicate filename": func(m *Manifest) { m.Artifacts[1].Filename = "RUNTIME-HOST" },
		"traversal filename":                  func(m *Manifest) { m.Artifacts[0].Filename = "../control" },
		"backslash filename":                  func(m *Manifest) { m.Artifacts[0].Filename = `..\\control` },
		"reserved filename":                   func(m *Manifest) { m.Artifacts[0].Filename = "CON.exe" },
		"invalid hash":                        func(m *Manifest) { m.Artifacts[0].SHA256 = "xyz" },
		"zero size":                           func(m *Manifest) { m.Artifacts[0].Size = 0 },
		"oversized artifact":                  func(m *Manifest) { m.Artifacts[0].Size = MaxArtifactBytes + 1 },
		"invalid channel":                     func(m *Manifest) { m.Channel = "nightly-ish" },
		"invalid key id":                      func(m *Manifest) { m.KeyID = "../key" },
		"unsupported platform":                func(m *Manifest) { m.Platform = "madeup" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			manifest := validManifest()
			mutate(&manifest)
			if err := manifest.Validate(); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("Validate() = %v, want ErrInvalidManifest", err)
			}
		})
	}

	good, err := json.Marshal(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	unknownField := append([]byte(strings.TrimSuffix(string(good), "}")), []byte(`,"unexpected":true}`)...)
	if _, err := DecodeAndVerifyManifest(unknownField, "", nil); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("unknown JSON field error = %v", err)
	}
	duplicateField := []byte(strings.Replace(string(good), `"release_version":"1.2.3"`, `"release_version":"1.2.3","release_version":"1.2.3"`, 1))
	if _, err := DecodeAndVerifyManifest(duplicateField, "", nil); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("duplicate JSON key error = %v", err)
	}
	if _, err := DecodeAndVerifyManifest([]byte(`{"manifest_schema_version":`), "", nil); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("malformed JSON error = %v", err)
	}
	if _, err := DecodeAndVerifyManifest(make([]byte, MaxManifestBytes+1), "", nil); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("oversized manifest error = %v", err)
	}
}

func TestCheckCompatibilityUsesExplicitRanges(t *testing.T) {
	manifest := validManifest()
	host := HostCompatibility{
		Platform: "linux", Architecture: "amd64", RuntimeProtocolVersion: 1,
		ControlProtocolRange: ProtocolRange{Minimum: 1, Maximum: 2},
		EngineProtocolRange:  ProtocolRange{Minimum: 1, Maximum: 2},
		AdapterProtocolRange: ProtocolRange{Minimum: 1, Maximum: 2},
		ArchiveReadRange:     ProtocolRange{Minimum: 2, Maximum: 2}, ArchiveWriteFormat: 2,
		ManagementSchemaVersion: 2,
	}
	if err := CheckCompatibility(manifest, host); err != nil {
		t.Fatalf("compatible host rejected: %v", err)
	}
	legacy := manifest
	legacy.ArchiveReadMinimum, legacy.ArchiveReadMaximum, legacy.ArchiveWriteFormat = 1, 1, 1
	if err := CheckCompatibility(legacy, host); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("V1 release compatibility = %v, want fail-closed rejection", err)
	}
	tests := map[string]func(*HostCompatibility){
		"runtime protocol":  func(h *HostCompatibility) { h.RuntimeProtocolVersion = 3 },
		"control protocol":  func(h *HostCompatibility) { h.ControlProtocolRange = ProtocolRange{Minimum: 2, Maximum: 3} },
		"engine protocol":   func(h *HostCompatibility) { h.EngineProtocolRange = ProtocolRange{Minimum: 2, Maximum: 3} },
		"adapter protocol":  func(h *HostCompatibility) { h.AdapterProtocolRange = ProtocolRange{Minimum: 3, Maximum: 4} },
		"archive read":      func(h *HostCompatibility) { h.ArchiveReadRange = ProtocolRange{Minimum: 3, Maximum: 4} },
		"management schema": func(h *HostCompatibility) { h.ManagementSchemaVersion = 4 },
		"platform":          func(h *HostCompatibility) { h.Platform = "darwin" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := host
			mutate(&candidate)
			if err := CheckCompatibility(manifest, candidate); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("CheckCompatibility() = %v, want ErrIncompatible", err)
			}
		})
	}
}

func TestVerifyArtifactFile(t *testing.T) {
	directory := t.TempDir()
	data := []byte("release executable bytes")
	hash := sha256.Sum256(data)
	artifact := Artifact{Role: RoleRuntimeHost, Filename: "runtime-host", Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
	path := filepath.Join(directory, artifact.Filename)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifactFile(path, artifact); err != nil {
		t.Fatalf("valid file rejected: %v", err)
	}

	wrongHash := artifact
	wrongHash.SHA256 = strings.Repeat("0", 64)
	if err := VerifyArtifactFile(path, wrongHash); !errors.Is(err, ErrArtifactFile) {
		t.Fatalf("hash mismatch error = %v", err)
	}
	wrongSize := artifact
	wrongSize.Size++
	if err := VerifyArtifactFile(path, wrongSize); !errors.Is(err, ErrArtifactFile) {
		t.Fatalf("size mismatch error = %v", err)
	}

	symlink := filepath.Join(directory, "link")
	if err := os.Symlink(path, symlink); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if err := VerifyArtifactFile(symlink, artifact); !errors.Is(err, ErrArtifactFile) {
		t.Fatalf("symlink error = %v", err)
	}
}

func validManifest() Manifest {
	return Manifest{
		ManifestSchemaVersion:   1,
		ReleaseVersion:          "1.2.3",
		Commit:                  strings.Repeat("a", 40),
		BuildTime:               "2026-09-29T12:00:00Z",
		Channel:                 "stable",
		KeyID:                   "test-key",
		MinimumHostProtocol:     1,
		MaximumHostProtocol:     2,
		ControlProtocolVersion:  1,
		EngineProtocolVersion:   1,
		AdapterProtocolMinimum:  1,
		AdapterProtocolMaximum:  2,
		ArchiveReadMinimum:      2,
		ArchiveReadMaximum:      2,
		ArchiveWriteFormat:      2,
		ManagementSchemaMinimum: 1,
		ManagementSchemaMaximum: 3,
		Platform:                "linux",
		Architecture:            "amd64",
		Artifacts: []Artifact{
			{Role: RoleRuntimeHost, Filename: "runtime-host", Size: 10, SHA256: strings.Repeat("0", 64)},
			{Role: RoleControlPlane, Filename: "control-plane", Size: 11, SHA256: strings.Repeat("1", 64)},
			{Role: RoleRecorderEngine, Filename: "recorder-engine", Size: 12, SHA256: strings.Repeat("2", 64)},
			{Role: RoleAdapterRuntime, Filename: "adapter-runtime", Size: 13, SHA256: strings.Repeat("3", 64)},
		},
	}
}
