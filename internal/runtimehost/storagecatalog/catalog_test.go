//go:build unix

package storagecatalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/storageproto"
)

func TestOpenReadOnlyDoesNotRemoveInFlightHostStaging(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog")
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(root, "staging", "import-host-operation")
	if err := os.Mkdir(staging, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(staging, "still-owned-by-host")
	if err := os.WriteFile(marker, []byte("partial host import"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReadOnly(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("read-only generation open removed Host staging: %v", err)
	}
}

func TestImportCreateSetReloadInstallAndStart(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	root := filepath.Join(t.TempDir(), "catalog")
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	catalog, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ID != expected.ID || artifact.Version != expected.Version || artifact.Digest != expected.SHA256 || artifact.DescriptorFingerprint == "" {
		t.Fatalf("imported artifact missing identity: %+v", artifact)
	}
	artifactPath, err := catalog.ArtifactPath(artifact.Digest)
	if err != nil || artifactPath != filepath.Join(root, "artifacts", artifact.Digest, "provider") {
		t.Fatalf("ArtifactPath() = %q, %v", artifactPath, err)
	}
	if got := fileMode(t, artifactPath); got != 0500 {
		t.Fatalf("artifact mode = %04o, want 0500", got)
	}

	objectRoot := filepath.Join(t.TempDir(), "objects")
	config := SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"` + objectRoot + `"`)}}
	set, err := catalog.CreateSet(artifact.Digest, config)
	if err != nil {
		t.Fatal(err)
	}
	again, err := catalog.CreateSet(artifact.Digest, config)
	if err != nil || again.ID != set.ID {
		t.Fatalf("same config did not produce stable set: %q / %q, %v", set.ID, again.ID, err)
	}
	otherConfig := SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"` + filepath.Join(t.TempDir(), "other") + `"`)}}
	otherSet, err := catalog.CreateSet(artifact.Digest, otherConfig)
	if err != nil || otherSet.ID == set.ID {
		t.Fatalf("different config identity: first=%q second=%q err=%v", set.ID, otherSet.ID, err)
	}
	if got := fileMode(t, filepath.Join(root, "sets", set.ID, "config.json")); got != 0600 {
		t.Fatalf("private config mode = %04o, want 0600", got)
	}

	if err := catalog.Install(artifact); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SelectDesiredSet(artifact.ID, set.ID); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.LoadSet(set.ID)
	if err != nil || loaded.ID != set.ID || loaded.Artifact != artifact || string(loaded.Config.Values["root"]) != string(config.Values["root"]) {
		t.Fatalf("LoadSet() = %+v, %v", loaded, err)
	}
	described, descriptor, err := reopened.DescribeArtifact(artifact.Digest)
	if err != nil || described != artifact || descriptor.ID != artifact.ID {
		t.Fatalf("DescribeArtifact() = %+v %+v, %v", described, descriptor, err)
	}
	installed, err := reopened.Installed()
	if err != nil || len(installed) != 1 || installed[0] != artifact {
		t.Fatalf("Installed() = %+v, %v", installed, err)
	}
	desired, err := reopened.DesiredSet(artifact.ID)
	if err != nil || desired.ID != set.ID {
		t.Fatalf("DesiredSet() = %+v, %v", desired, err)
	}
	privateRunDir, err := os.MkdirTemp("", "spc-")
	if err != nil {
		t.Fatal(err)
	}
	defer removeTreeOwned(privateRunDir)
	tokenPath := filepath.Join(privateRunDir, "token")
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("a", 64)), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := reopened.StartProvider(context.Background(), set.ID, filepath.Join(privateRunDir, "provider.sock"), tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if err := reopened.CollectGarbage([]string{otherSet.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.LoadSet(set.ID); err != nil {
		t.Fatalf("desired set was collected: %v", err)
	}
	if _, err := reopened.LoadSet(otherSet.ID); err != nil {
		t.Fatalf("protected set was collected: %v", err)
	}
	if err := reopened.Uninstall(artifact.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.DesiredSet(artifact.ID); !errors.Is(err, ErrSetMissing) {
		t.Fatalf("DesiredSet after uninstall = %v, want ErrSetMissing", err)
	}
	if err := reopened.CollectGarbage([]string{otherSet.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ArtifactPath(artifact.Digest); err != nil {
		t.Fatalf("protected set no longer retains artifact: %v", err)
	}
	if err := reopened.CollectGarbage(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ArtifactPath(artifact.Digest); !errors.Is(err, ErrArtifactMissing) {
		t.Fatalf("unreferenced artifact still exists: %v", err)
	}
}

func TestStorageInstancesShareProviderAndKeepIndependentImmutableSets(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	root := filepath.Join(t.TempDir(), "catalog")
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	catalog, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.InstallWithAttestation(artifact, plugintrust.NewOperator()); err != nil {
		t.Fatal(err)
	}
	firstConfig := SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(mustJSON(filepath.Join(t.TempDir(), "ssd")))}}
	secondConfig := SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(mustJSON(filepath.Join(t.TempDir(), "hdd")))}}
	firstSet, err := catalog.CreateInstalledSet(artifact.ID, firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	secondSet, err := catalog.CreateInstalledSet(artifact.ID, secondConfig)
	if err != nil || firstSet.ID == secondSet.ID {
		t.Fatalf("same provider did not produce distinct immutable sets: %q %q err=%v", firstSet.ID, secondSet.ID, err)
	}
	first, err := catalog.CreateStorageInstance("Local SSD", artifact.ID, firstSet.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := catalog.CreateStorageInstance("Local HDD", artifact.ID, secondSet.ID)
	if err != nil || first.ID == second.ID {
		t.Fatalf("same provider instances not independent: first=%+v second=%+v err=%v", first, second, err)
	}
	if err := catalog.UpdateStorageInstanceSet(first.ID, secondSet.ID); err != nil {
		t.Fatalf("update instance desired set: %v", err)
	}
	updated, err := catalog.LoadStorageInstance(first.ID)
	if err != nil || updated.ID != first.ID || updated.DesiredSetID != secondSet.ID {
		t.Fatalf("instance update changed identity or lost set: %+v err=%v", updated, err)
	}
	other, err := catalog.LoadStorageInstance(second.ID)
	if err != nil || other.DesiredSetID != secondSet.ID {
		t.Fatalf("update changed second instance: %+v err=%v", other, err)
	}
	if err := catalog.CollectGarbage([]string{firstSet.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.LoadSet(firstSet.ID); err != nil {
		t.Fatalf("instance update collected prior immutable set needed by existing generation pin: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	items, err := reopened.ListStorageInstances()
	if err != nil || len(items) != 2 || items[0].ID == "" || items[1].ID == "" {
		t.Fatalf("reopen instance list = %+v err=%v", items, err)
	}
	loadedFirst, err := reopened.LoadSet(firstSet.ID)
	if err != nil || string(loadedFirst.Config.Values["root"]) != string(firstConfig.Values["root"]) {
		t.Fatalf("first immutable config changed: %+v err=%v", loadedFirst, err)
	}
	loadedSecond, err := reopened.LoadSet(secondSet.ID)
	if err != nil || string(loadedSecond.Config.Values["root"]) != string(secondConfig.Values["root"]) {
		t.Fatalf("second immutable config changed: %+v err=%v", loadedSecond, err)
	}
}

func TestStorageInstanceCatalogDoesNotExposeCredentials(t *testing.T) {
	binary := buildProvider(t, "./testdata/secretprovider")
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "secret-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	root := filepath.Join(t.TempDir(), "catalog")
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	catalog, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.InstallWithAttestation(artifact, plugintrust.NewOperator()); err != nil {
		t.Fatal(err)
	}
	firstSet, err := catalog.CreateInstalledSet(artifact.ID, SetConfig{Secrets: map[string]string{"access_token": "credential-A"}})
	if err != nil {
		t.Fatal(err)
	}
	secondSet, err := catalog.CreateInstalledSet(artifact.ID, SetConfig{Secrets: map[string]string{"access_token": "credential-B"}})
	if err != nil || firstSet.ID == secondSet.ID {
		t.Fatalf("credential change did not create immutable set: %q %q err=%v", firstSet.ID, secondSet.ID, err)
	}
	if _, err := catalog.CreateStorageInstance("Account A", artifact.ID, firstSet.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateStorageInstance("Account B", artifact.ID, secondSet.ID); err != nil {
		t.Fatal(err)
	}
	state, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(state), "credential-A") || strings.Contains(string(state), "credential-B") {
		t.Fatal("catalog state exposed provider credentials")
	}
	first, err := catalog.LoadSet(firstSet.ID)
	if err != nil || first.Config.Secrets["access_token"] != "credential-A" {
		t.Fatalf("first instance credentials changed: %+v err=%v", first.Config.Secrets, err)
	}
	second, err := catalog.LoadSet(secondSet.ID)
	if err != nil || second.Config.Secrets["access_token"] != "credential-B" {
		t.Fatalf("second instance credentials changed: %+v err=%v", second.Config.Secrets, err)
	}
}

func TestLegacyDesiredSetsMigrateToStableStorageInstancesIdempotently(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	root := filepath.Join(t.TempDir(), "catalog")
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	catalog, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.InstallWithAttestation(artifact, plugintrust.NewOperator()); err != nil {
		t.Fatal(err)
	}
	set, err := catalog.CreateInstalledSet(artifact.ID, SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(mustJSON(filepath.Join(t.TempDir(), "archive")))}})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.SelectDesiredSet(artifact.ID, set.ID); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "state.json")
	stateBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var current catalogState
	if err := json.Unmarshal(stateBytes, &current); err != nil {
		t.Fatal(err)
	}
	// Serialize through the previous state field order to model a valid
	// pre-instance catalog state without the new optional field.
	legacyState := struct {
		SchemaVersion         int                                  `json:"schema_version"`
		Installed             []Artifact                           `json:"installed"`
		DesiredSets           map[string]string                    `json:"desired_sets"`
		ArtifactAttestations  map[string][]plugintrust.Attestation `json:"artifact_attestations,omitempty"`
		InstalledAttestations map[string]plugintrust.Attestation   `json:"installed_attestations,omitempty"`
	}{
		SchemaVersion: current.SchemaVersion, Installed: current.Installed, DesiredSets: current.DesiredSets,
		ArtifactAttestations: current.ArtifactAttestations, InstalledAttestations: current.InstalledAttestations,
	}
	oldBytes, err := json.Marshal(legacyState)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, oldBytes, 0600); err != nil {
		t.Fatal(err)
	}

	migrated, err := Open(root)
	if err != nil {
		t.Fatalf("migrate legacy desired set: %v", err)
	}
	instanceID := LegacyStorageInstanceID(artifact.ID)
	first, err := migrated.LoadStorageInstance(instanceID)
	if err != nil || first.DesiredSetID != set.ID || first.DisplayName != artifact.ID {
		t.Fatalf("legacy instance = %+v err=%v", first, err)
	}
	firstState, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err != nil {
		t.Fatalf("second open after migration: %v", err)
	}
	secondState, err := os.ReadFile(statePath)
	if err != nil || string(firstState) != string(secondState) {
		t.Fatalf("migration not idempotent: before=%s after=%s err=%v", firstState, secondState, err)
	}
}

func TestStorageSetAttestationAffectsIdentityButNotArtifact(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	config := SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"/tmp/objects"`)}}
	bundled, err := catalog.CreateSetWithAttestation(artifact.Digest, config, plugintrust.NewBundled())
	if err != nil {
		t.Fatal(err)
	}
	operator, err := catalog.CreateSetWithAttestation(artifact.Digest, config, plugintrust.NewOperator())
	if err != nil {
		t.Fatal(err)
	}
	if bundled.Artifact.Digest != operator.Artifact.Digest || bundled.ID == operator.ID {
		t.Fatalf("attestation did not distinguish provider sets: bundled=%+v operator=%+v", bundled, operator)
	}
	if bundled.Attestation == nil || *bundled.Attestation != plugintrust.NewBundled() || operator.Attestation == nil || *operator.Attestation != plugintrust.NewOperator() {
		t.Fatalf("set attestations were lost: bundled=%+v operator=%+v", bundled, operator)
	}
}

func TestInstalledStorageArtifactAttestationSurvivesRegistryOutage(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	root := filepath.Join(t.TempDir(), "catalog")
	catalog, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	registryAttestation := plugintrust.NewOfficialRegistry(plugintrust.ThirdParty)
	if err := catalog.InstallWithAttestation(artifact, registryAttestation); err != nil {
		t.Fatal(err)
	}

	// Reopening models a later configure/probe after the Registry is no longer
	// available. The selected admission evidence is local durable state.
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	selected, found, err := reopened.InstalledAttestation(artifact.ID)
	if err != nil || !found || selected != registryAttestation {
		t.Fatalf("InstalledAttestation() = %+v, %t, %v", selected, found, err)
	}
	attestations, err := reopened.AttestationsForArtifact(artifact.Digest)
	if err != nil || len(attestations) != 1 || attestations[0] != registryAttestation {
		t.Fatalf("AttestationsForArtifact() = %+v, %v", attestations, err)
	}
	config := SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"` + filepath.Join(t.TempDir(), "objects") + `"`)}}
	set, err := reopened.CreateInstalledSet(artifact.ID, config)
	if err != nil || set.Attestation == nil || *set.Attestation != registryAttestation {
		t.Fatalf("CreateInstalledSet() = %+v, %v", set, err)
	}

	// The same bytes can later be deliberately selected from a different
	// admission path. Both pieces of evidence remain separate, and each
	// immutable set retains the explicitly selected provenance.
	operatorAttestation := plugintrust.NewOperator()
	if err := reopened.InstallWithAttestation(artifact, operatorAttestation); err != nil {
		t.Fatal(err)
	}
	attestations, err = reopened.AttestationsForArtifact(artifact.Digest)
	if err != nil || len(attestations) != 2 {
		t.Fatalf("same-digest admission evidence collapsed: %+v, %v", attestations, err)
	}
	selected, found, err = reopened.InstalledAttestation(artifact.ID)
	if err != nil || !found || selected != operatorAttestation {
		t.Fatalf("explicit operator selection was not retained: %+v, %t, %v", selected, found, err)
	}
	operatorSet, err := reopened.CreateInstalledSet(artifact.ID, config)
	if err != nil || operatorSet.ID == set.ID || operatorSet.Attestation == nil || *operatorSet.Attestation != operatorAttestation {
		t.Fatalf("operator set did not preserve selected provenance: %+v, %v", operatorSet, err)
	}
}

func TestUnclassifiedInstallDoesNotManufactureTrust(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Install(artifact); err != nil {
		t.Fatal(err)
	}
	if _, found, err := catalog.InstalledAttestation(artifact.ID); err != nil || found {
		t.Fatalf("unclassified Install manufactured admission trust: found=%t err=%v", found, err)
	}
	if _, err := catalog.CreateInstalledSet(artifact.ID, SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"/tmp/objects"`)}}); !errors.Is(err, ErrAttestationMissing) {
		t.Fatalf("CreateInstalledSet without provenance = %v, want ErrAttestationMissing", err)
	}
}

func TestLegacyStorageSetWithoutAttestationRemainsLoadable(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	set, err := catalog.CreateSetWithAttestation(artifact.Digest, SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"/tmp/objects"`)}}, plugintrust.NewBundled())
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(catalog.root, "sets", set.ID, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest setManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Attestation = nil
	legacyBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	legacyID := digestBytes(legacyBytes)
	setDir := filepath.Dir(manifestPath)
	if err := os.Chmod(setDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manifestPath, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, legacyBytes, 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manifestPath, 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(setDir, filepath.Join(catalog.root, "sets", legacyID)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(catalog.root, "sets", legacyID), 0700); err != nil {
		t.Fatal(err)
	}
	loaded, err := catalog.LoadSet(legacyID)
	if err != nil || loaded.Attestation != nil || loaded.EffectiveAttestation().Provenance != plugintrust.LegacyUnclassified {
		t.Fatalf("legacy storage set load = %+v, %v", loaded, err)
	}
}

func TestImportRejectsExpectedIdentityHashAndSizeMismatches(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	base := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	cases := []struct {
		name    string
		mutate  func(*Expected)
		wantErr error
	}{
		{name: "wrong hash", mutate: func(value *Expected) { value.SHA256 = strings.Repeat("0", 64) }, wantErr: ErrInvalidArtifact},
		{name: "wrong size", mutate: func(value *Expected) { value.Size++ }, wantErr: ErrInvalidArtifact},
		{name: "wrong id", mutate: func(value *Expected) { value.ID = "different-provider" }, wantErr: ErrInvalidArtifact},
		{name: "wrong version", mutate: func(value *Expected) { value.Version = "2.0.0" }, wantErr: ErrInvalidArtifact},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expected := base
			tc.mutate(&expected)
			if _, err := catalog.Import(context.Background(), binary, expected); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Import() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
	if entries, err := os.ReadDir(filepath.Join(catalog.root, "artifacts")); err != nil || len(entries) != 0 {
		t.Fatalf("mismatched artifacts were published: %+v, %v", entries, err)
	}
}

func TestImportRejectsUnsafeSources(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	link := filepath.Join(t.TempDir(), "provider-link")
	if err := os.Symlink(binary, link); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Import(context.Background(), link, expected); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("symlink source accepted: %v", err)
	}
	if _, err := catalog.Import(context.Background(), t.TempDir(), expected); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("directory source accepted: %v", err)
	}
	nonExecutable := filepath.Join(t.TempDir(), "provider")
	if err := os.WriteFile(nonExecutable, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Import(context.Background(), nonExecutable, expected); !errors.Is(err, ErrInvalidArtifact) {
		t.Fatalf("non-executable source accepted: %v", err)
	}
	oversized := filepath.Join(t.TempDir(), "oversized")
	file, err := os.OpenFile(oversized, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(MaxArtifactBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	tooLarge := expected
	tooLarge.Size = MaxArtifactBytes + 1
	if _, err := catalog.Import(context.Background(), oversized, tooLarge); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("oversized source accepted: %v", err)
	}
}

func TestSetConfigValidationAndDesiredSetProviderBinding(t *testing.T) {
	binary := buildFixtureProvider(t)
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "fixture-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateSet(artifact.Digest, SetConfig{Values: map[string]json.RawMessage{"unknown": json.RawMessage(`true`)}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown config key accepted: %v", err)
	}
	set, err := catalog.CreateSet(artifact.Digest, SetConfig{Values: map[string]json.RawMessage{"root": json.RawMessage(`"/tmp/a"`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Install(artifact); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SelectDesiredSet("other-provider", set.ID); !errors.Is(err, ErrInvalidSet) {
		t.Fatalf("set selected for wrong provider: %v", err)
	}
}

func TestSecretConfigChangesSetIdentityAndStaysInPrivateConfigFile(t *testing.T) {
	binary := buildProvider(t, "./testdata/secretprovider")
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	expected := Expected{ID: "secret-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	catalog, err := Open(filepath.Join(t.TempDir(), "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	artifact, err := catalog.Import(context.Background(), binary, expected)
	if err != nil {
		t.Fatal(err)
	}
	firstConfig := SetConfig{Secrets: map[string]string{"access_token": "secret-one"}}
	secondConfig := SetConfig{Secrets: map[string]string{"access_token": "secret-two"}}
	first, err := catalog.CreateSet(artifact.Digest, firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	stable, err := catalog.CreateSet(artifact.Digest, firstConfig)
	if err != nil || stable.ID != first.ID {
		t.Fatalf("same secret config did not produce stable set: %q / %q, %v", first.ID, stable.ID, err)
	}
	second, err := catalog.CreateSet(artifact.Digest, secondConfig)
	if err != nil || second.ID == first.ID {
		t.Fatalf("secret change did not alter set identity: %q / %q, %v", first.ID, second.ID, err)
	}
	manifest, err := os.ReadFile(filepath.Join(catalog.root, "sets", first.ID, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(manifest), "secret-one") {
		t.Fatal("secret bytes were persisted in the set manifest")
	}
	configPath := filepath.Join(catalog.root, "sets", first.ID, "config.json")
	if got := fileMode(t, configPath); got != 0600 {
		t.Fatalf("private secret config mode = %04o, want 0600", got)
	}
	privateConfig, err := os.ReadFile(configPath)
	if err != nil || !strings.Contains(string(privateConfig), "secret-one") {
		t.Fatalf("secret bytes missing from private config file: %v", err)
	}
}

func buildFixtureProvider(t *testing.T) string {
	return buildProvider(t, "../../storageproto/testdata/provider")
}

func buildProvider(t *testing.T, packagePath string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "fixture-provider")
	command := exec.Command("go", "build", "-trimpath", "-o", binary, packagePath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build storage provider fixture: %v (%s)", err, output)
	}
	return binary
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
