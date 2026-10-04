package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

func TestReleasePackRequiresValidSigningKey(t *testing.T) {
	if err := run(nil, func(string) string { return "" }); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("missing key error = %v", err)
	}
	secretText := "not-a-private-key"
	err := run(nil, func(string) string { return secretText })
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("invalid key error = %v", err)
	}
	if strings.Contains(err.Error(), secretText) {
		t.Fatal("error disclosed signing key material")
	}
}

func TestReleasePackSignsVerifiedArtifactsForEachTarget(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey := base64.StdEncoding.EncodeToString(privateKey)
	for _, architecture := range []string{"amd64", "arm64"} {
		t.Run(architecture, func(t *testing.T) {
			dir := t.TempDir()
			paths := make(map[string]string, 4)
			content := make(map[string][]byte, 4)
			for _, role := range []string{release.RoleRuntimeHost, release.RoleControlPlane, release.RoleRecorderEngine, release.RoleAdapterRuntime} {
				data := []byte("binary fixture " + role)
				path := filepath.Join(dir, "source-"+role)
				if err := os.WriteFile(path, data, 0700); err != nil {
					t.Fatal(err)
				}
				paths[role] = path
				content[role] = data
			}
			output := filepath.Join(dir, "package-"+architecture)
			args := []string{
				"-version", "1.2.3", "-commit", strings.Repeat("a", 40),
				"-build-time", "2026-09-29T12:00:00Z", "-channel", "stable",
				"-platform", "linux", "-architecture", architecture,
				"-key-id", "production-2026", "-output", output,
				"-runtime-host", paths[release.RoleRuntimeHost],
				"-control-plane", paths[release.RoleControlPlane],
				"-recorder-engine", paths[release.RoleRecorderEngine],
				"-adapter-runtime", paths[release.RoleAdapterRuntime],
			}
			if err := run(args, func(string) string { return encodedKey }); err != nil {
				t.Fatalf("run(): %v", err)
			}

			manifestBytes, err := os.ReadFile(filepath.Join(output, "release.json"))
			if err != nil {
				t.Fatal(err)
			}
			signature, err := os.ReadFile(filepath.Join(output, "release.json.sig"))
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := release.DecodeAndVerifyManifest(manifestBytes, string(signature), map[string]ed25519.PublicKey{"production-2026": publicKey})
			if err != nil {
				t.Fatalf("DecodeAndVerifyManifest(): %v", err)
			}
			if manifest.Platform != "linux" || manifest.Architecture != architecture {
				t.Fatalf("target = %s/%s", manifest.Platform, manifest.Architecture)
			}
			for _, artifact := range manifest.Artifacts {
				wantName := artifact.Role + "-linux-" + architecture
				if artifact.Filename != wantName {
					t.Errorf("%s filename = %q, want %q", artifact.Role, artifact.Filename, wantName)
				}
				if err := release.VerifyArtifactFile(filepath.Join(output, artifact.Filename), artifact); err != nil {
					t.Errorf("VerifyArtifactFile(%s): %v", artifact.Filename, err)
				}
				wantHash := sha256.Sum256(content[artifact.Role])
				if artifact.Size != int64(len(content[artifact.Role])) || artifact.SHA256 != hex.EncodeToString(wantHash[:]) {
					t.Errorf("artifact %s size/hash mismatch: %+v", artifact.Role, artifact)
				}
			}
			for _, name := range []string{"release.json", "release.json.sig", "runtime-host-linux-" + architecture, "control-plane-linux-" + architecture, "recorder-engine-linux-" + architecture, "adapter-runtime-linux-" + architecture} {
				info, err := os.Stat(filepath.Join(output, name))
				if err != nil {
					t.Errorf("package missing %s: %v", name, err)
				}
				if err == nil && info.Mode().Perm()&0077 != 0 {
					t.Errorf("package file %s permissions = %o, want private", name, info.Mode().Perm())
				}
			}
		})
	}
}

func TestReleasePackRejectsSymlinkArtifactAndKeepsOutputUnpublished(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	paths := make([]string, 4)
	roles := []string{release.RoleRuntimeHost, release.RoleControlPlane, release.RoleRecorderEngine, release.RoleAdapterRuntime}
	for i, role := range roles {
		path := filepath.Join(dir, "source-"+role)
		if err := os.WriteFile(path, []byte("payload"), 0700); err != nil {
			t.Fatal(err)
		}
		paths[i] = path
	}
	link := filepath.Join(dir, "linked-adapter")
	if err := os.Symlink(paths[3], link); err != nil {
		t.Skipf("symlink fixture unavailable: %v", err)
	}
	output := filepath.Join(dir, "package")
	args := []string{
		"-version", "1.2.3", "-commit", strings.Repeat("a", 40), "-build-time", "2026-09-29T12:00:00Z",
		"-channel", "stable", "-platform", "linux", "-architecture", "amd64", "-key-id", "fixture",
		"-output", output, "-runtime-host", paths[0], "-control-plane", paths[1],
		"-recorder-engine", paths[2], "-adapter-runtime", link,
	}
	err = run(args, func(string) string { return base64.StdEncoding.EncodeToString(privateKey) })
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("symlink error = %v", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed package was published at output path: %v", err)
	}
}
