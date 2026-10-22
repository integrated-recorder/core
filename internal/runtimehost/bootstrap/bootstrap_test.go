package bootstrap

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	"github.com/integrated-recorder/core/internal/runtimehost/resources"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/supervisor"
)

func TestConfigRejectsAuthDisabledOnPublicListener(t *testing.T) {
	config := DefaultConfig()
	config.DataDir = t.TempDir()
	config.AuthDisabled = true
	config.ListenAddr = ":8080"
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Validate() error = %v, want loopback restriction", err)
	}
	config.ListenAddr = "127.0.0.1:8080"
	if err := config.Validate(); err != nil {
		t.Fatalf("loopback auth-disabled config rejected: %v", err)
	}
}

func TestConfigFromEnvOperatorPluginOptIn(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  bool
		bad   bool
	}{
		{name: "unset defaults off"},
		{name: "explicit disabled", value: "0"},
		{name: "explicit enabled", value: "1", want: true},
		{name: "malformed value", value: "true", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := ConfigFromEnv(func(key string) string {
				if key == "IR_ALLOW_OPERATOR_PLUGINS" {
					return test.value
				}
				return ""
			})
			if test.bad {
				if err == nil {
					t.Fatal("malformed operator plugin opt-in was accepted")
				}
				return
			}
			if err != nil || config.AllowOperatorPlugins != test.want {
				t.Fatalf("ConfigFromEnv() = (allow=%t, err=%v), want allow=%t", config.AllowOperatorPlugins, err, test.want)
			}
			if config.AdapterDirs != "/external-adapters" {
				t.Fatalf("default operator source = %q, want /external-adapters", config.AdapterDirs)
			}
			if config.AllowOperatorPlugins && config.PluginRegistryURL != pluginregistry.OfficialCatalogV3URL {
				t.Fatalf("operator opt-in changed registry default: %q", config.PluginRegistryURL)
			}
		})
	}
}

func TestConfigFromEnvValidatesExplicitImageBundleTransitionIdentity(t *testing.T) {
	values := map[string]string{
		"IR_IMAGE_BUNDLE_UPGRADE_FROM_VERSION": "0.1.0-rc.9",
		"IR_IMAGE_BUNDLE_UPGRADE_FROM_COMMIT":  strings.Repeat("a", 40),
	}
	config, err := ConfigFromEnv(func(key string) string { return values[key] })
	if err != nil || config.BundleUpgradeFrom.Version != values["IR_IMAGE_BUNDLE_UPGRADE_FROM_VERSION"] || config.BundleUpgradeFrom.Commit != values["IR_IMAGE_BUNDLE_UPGRADE_FROM_COMMIT"] {
		t.Fatalf("valid transition identity = %+v, %v", config.BundleUpgradeFrom, err)
	}
	for name, invalid := range map[string]map[string]string{
		"missing commit": {"IR_IMAGE_BUNDLE_UPGRADE_FROM_VERSION": "0.1.0-rc.9"},
		"unsafe version": {"IR_IMAGE_BUNDLE_UPGRADE_FROM_VERSION": "../rc9", "IR_IMAGE_BUNDLE_UPGRADE_FROM_COMMIT": strings.Repeat("a", 40)},
		"invalid commit": {"IR_IMAGE_BUNDLE_UPGRADE_FROM_VERSION": "0.1.0-rc.9", "IR_IMAGE_BUNDLE_UPGRADE_FROM_COMMIT": "unknown"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ConfigFromEnv(func(key string) string { return invalid[key] }); err == nil {
				t.Fatal("invalid image transition identity was accepted")
			}
		})
	}
}

func TestConfigFromEnvKeepsPublishedHTTPPortForHTTPSRecovery(t *testing.T) {
	config, err := ConfigFromEnv(func(key string) string {
		if key == "IR_HTTP_PUBLIC_PORT" {
			return "18080"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.HTTPPublicPort != 18080 {
		t.Fatalf("HTTP public port=%d, want installer-published port 18080", config.HTTPPublicPort)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("valid HTTP public port was rejected: %v", err)
	}
	if _, err := ConfigFromEnv(func(key string) string {
		if key == "IR_HTTP_PUBLIC_PORT" {
			return "not-a-port"
		}
		return ""
	}); err == nil {
		t.Fatal("malformed HTTP public port was accepted")
	}
}

func TestPluginRegistryDefaultsToOfficialCatalogAndAllowsOverride(t *testing.T) {
	config, err := ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if config.PluginRegistryURL != pluginregistry.OfficialCatalogV3URL {
		t.Fatalf("unset registry URL = %q, want official catalog %q", config.PluginRegistryURL, pluginregistry.OfficialCatalogV3URL)
	}

	const customCatalog = "https://registry.example.test/catalog-v3.json"
	config, err = ConfigFromEnv(func(key string) string {
		if key == "IR_PLUGIN_REGISTRY_URL" {
			return customCatalog
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.PluginRegistryURL != customCatalog {
		t.Fatalf("configured registry URL = %q, want %q", config.PluginRegistryURL, customCatalog)
	}
}

func TestConfiguredAdapterSourcesKeepBundledAndOperatorProvenanceSeparate(t *testing.T) {
	root := t.TempDir()
	bundled := filepath.Join(root, "bundled", "integrated-recorder-adapter-hls")
	operator := filepath.Join(root, "operator")
	registryBinary := filepath.Join(root, "registry", "integrated-recorder-adapter-example")
	config := DefaultConfig()
	config.BundledHLSBinary = bundled
	config.AdapterDirs = operator
	registrySources := []adaptercatalog.Source{{
		Path: registryBinary, Attestation: plugintrust.NewOfficialRegistry(plugintrust.ThirdParty), AllowedIDs: []string{"example"},
	}}

	sources, err := configuredAdapterSources(config, registrySources)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 || sources[0].Path != bundled || sources[0].Attestation != plugintrust.NewBundled() || len(sources[0].AllowedIDs) != 1 || sources[0].AllowedIDs[0] != "hls" {
		t.Fatalf("operator-disabled classified sources = %+v, want bundled HLS and Registry only", sources)
	}
	if sources[1].Attestation != plugintrust.NewOfficialRegistry(plugintrust.ThirdParty) || len(sources[1].AllowedIDs) != 1 || sources[1].AllowedIDs[0] != "example" {
		t.Fatalf("Registry source classification changed: %+v", sources[1])
	}

	config.AllowOperatorPlugins = true
	sources, err = configuredAdapterSources(config, registrySources)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 3 || sources[2].Path != operator || sources[2].Attestation != plugintrust.NewOperator() {
		t.Fatalf("operator-enabled classified sources = %+v, want operator source appended with local provenance", sources)
	}
}

func TestUpdateReconciliationPassesOnlyAllowedClassifiedSources(t *testing.T) {
	root := t.TempDir()
	bundled := filepath.Join(root, "bundled", "integrated-recorder-adapter-hls")
	operator := filepath.Join(root, "operator")
	catalog := &classifiedSourceCapture{}
	controller := &updateController{config: Config{BundledHLSBinary: bundled, AdapterDirs: operator}, adapterCatalog: catalog}
	if _, err := controller.reconcileCatalog(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if len(catalog.sources) != 1 || catalog.sources[0].Path != bundled || catalog.sources[0].Attestation != plugintrust.NewBundled() || len(catalog.sources[0].AllowedIDs) != 1 || catalog.sources[0].AllowedIDs[0] != "hls" {
		t.Fatalf("operator-disabled update reconciliation sources = %+v", catalog.sources)
	}

	controller.config.AllowOperatorPlugins = true
	if _, err := controller.reconcileCatalog(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if len(catalog.sources) != 2 || catalog.sources[1].Path != operator || catalog.sources[1].Attestation != plugintrust.NewOperator() {
		t.Fatalf("operator-enabled update reconciliation sources = %+v", catalog.sources)
	}
}

type classifiedSourceCapture struct {
	sources []adaptercatalog.Source
}

func (c *classifiedSourceCapture) Reconcile(context.Context, string) (adaptercatalog.Snapshot, error) {
	return adaptercatalog.Snapshot{}, nil
}
func (c *classifiedSourceCapture) ReconcileClassified(_ context.Context, _ string, sources []adaptercatalog.Source) (adaptercatalog.Snapshot, error) {
	c.sources = append([]adaptercatalog.Source(nil), sources...)
	return adaptercatalog.Snapshot{ID: strings.Repeat("a", 64)}, nil
}
func (c *classifiedSourceCapture) Empty() (adaptercatalog.Snapshot, error) {
	return adaptercatalog.Snapshot{}, nil
}
func (c *classifiedSourceCapture) Load(string) (adaptercatalog.Snapshot, error) {
	return adaptercatalog.Snapshot{}, adaptercatalog.ErrSetNotFound
}
func (c *classifiedSourceCapture) Collect([]string) error { return nil }

func TestBundledHLSCannotBeShadowedByOperatorSourceDirectory(t *testing.T) {
	config := DefaultConfig()
	config.BundledHLSBinary = "/adapters/integrated-recorder-adapter-hls"
	for _, directory := range []string{"/adapters", "/external-adapters/../adapters"} {
		config.AdapterDirs = directory
		if err := config.Validate(); err == nil {
			t.Fatalf("operator source directory %q could overlap the bundled namespace", directory)
		}
	}
}

func TestMarshalAdapterTrustUsesImmutableSnapshotAttestationOnly(t *testing.T) {
	snapshot := adaptercatalog.Snapshot{
		ID:        strings.Repeat("a", 64),
		Directory: "/private/not-for-control",
		Entries: []adaptercatalog.Entry{{
			AdapterID: "hls", Version: "1.2.3", ArtifactSHA256: strings.Repeat("b", 64),
			Attestation: attestationPointer(plugintrust.NewBundled()),
		}},
	}
	got, err := marshalAdapterTrust(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `"provenance":"bundled"`) || !strings.Contains(got, `"publisher":"first_party"`) || strings.Contains(got, snapshot.Directory) || strings.Contains(got, strings.Repeat("b", 64)) {
		t.Fatalf("Control trust projection leaked set internals or lost provenance: %s", got)
	}
}

func attestationPointer(value plugintrust.Attestation) *plugintrust.Attestation { return &value }

func TestInitialEngineRecoveryIsGatedByInstallationReadiness(t *testing.T) {
	tests := []struct {
		state       installation.State
		want        string
		allowDetach bool
	}{
		{state: installation.StateUninitialized, want: "fresh", allowDetach: true},
		{state: installation.StateSetupInProgress, want: "fresh", allowDetach: true},
		{state: installation.StateRecoveryRequired, want: "fresh"},
		{state: installation.StateReady, want: "recover"},
	}
	for _, test := range tests {
		t.Run(string(test.state), func(t *testing.T) {
			if got := initialEngineRecoveryMode(test.state); got != test.want {
				t.Fatalf("initialEngineRecoveryMode(%q)=%q, want %q", test.state, got, test.want)
			}
			if got := allowsUnleasedBundleSourceDetach(test.state); got != test.allowDetach {
				t.Fatalf("allowsUnleasedBundleSourceDetach(%q)=%t, want %t", test.state, got, test.allowDetach)
			}
		})
	}
}

func TestPrivateRuntimeFilesAndDirectories(t *testing.T) {
	root := bootstrapTestDir(t, "runtime-host-private-")
	if err := makePrivateRuntimeDirs(root); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(root, "runtime"), filepath.Join(root, "runtime", "ipc"), filepath.Join(root, "runtime", "state")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
			t.Fatalf("private directory %q info=%v err=%v", path, info, err)
		}
	}
	ipc := filepath.Join(root, "runtime", "ipc")
	credential := filepath.Join(ipc, "secret")
	if err := writeAtomicPrivate(credential, []byte("01234567890123456789012345678901")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(credential)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("private credential info=%v err=%v", info, err)
	}
	link := filepath.Join(root, "runtime", "linked")
	if err := os.Symlink(ipc, link); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateDirectory(filepath.Join(link, "nested")); err == nil {
		t.Fatal("symlinked private directory path unexpectedly accepted")
	}
}

func bootstrapTestDir(t *testing.T, prefix string) string {
	t.Helper()
	base := resolvedBootstrapTempDir(t)
	root, err := os.MkdirTemp(base, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func resolvedBootstrapTempDir(t *testing.T) string {
	t.Helper()
	// Prefer shortest canonical system temp root. On macOS, os.TempDir() often
	// resolves into a long per-user path that exceeds Unix socket path limits.
	candidates := []string{os.TempDir(), filepath.Join(string(filepath.Separator), "tmp")}
	base := ""
	for _, candidate := range candidates {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		info, err := os.Stat(resolved)
		if err != nil || !info.IsDir() {
			continue
		}
		if base == "" || len(resolved) < len(base) {
			base = resolved
		}
	}
	if base == "" {
		t.Fatalf("could not resolve an accessible OS temporary directory from %q", candidates)
	}
	return base
}

func TestTrustedReleaseKeyConfigurationIsOptionalAndBounded(t *testing.T) {
	if keys, err := parseTrustedReleaseKeys(""); err != nil || len(keys) != 0 {
		t.Fatalf("empty trust configuration = %v, %v", keys, err)
	}
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(public)
	keys, err := parseTrustedReleaseKeys(`[{"key_id":"stable-2026","public_key_base64":"` + encoded + `"}]`)
	if err != nil || len(keys) != 1 || len(keys["stable-2026"]) != ed25519.PublicKeySize {
		t.Fatalf("valid trust configuration = %v, %v", keys, err)
	}
	for name, value := range map[string]string{
		"duplicate key id": `[{"key_id":"stable","public_key_base64":"` + encoded + `"},{"key_id":"stable","public_key_base64":"` + encoded + `"}]`,
		"unknown field":    `[{"key_id":"stable","public_key_base64":"` + encoded + `","private_key":"not accepted"}]`,
		"trailing data":    `[{"key_id":"stable","public_key_base64":"` + encoded + `"}] {}`,
		"invalid key":      `[{"key_id":"stable","public_key_base64":"bad"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTrustedReleaseKeys(value); err == nil {
				t.Fatal("invalid trust configuration was accepted")
			}
		})
	}
}

func TestSelectGenerationReusesMatchingActiveBundle(t *testing.T) {
	build := buildinfoForTest()
	id := strings.Repeat("a", 32)
	snapshot := generationSnapshotForTest(id, build)
	got, needsStage, err := selectGeneration(snapshot, build)
	if err != nil || needsStage || got != id {
		t.Fatalf("selectGeneration = (%q, %t, %v), want existing active identity", got, needsStage, err)
	}
	build.Commit = "different"
	got, needsStage, err = selectGeneration(snapshot, build)
	if err != nil || !needsStage || !generationPattern.MatchString(got) || got == id {
		t.Fatalf("changed release selection = (%q, %t, %v), want fresh stage identity", got, needsStage, err)
	}
}

func TestSelectRuntimeReleaseUsesDurableActiveIdentity(t *testing.T) {
	bundle := t.TempDir()
	build := buildinfoForTest()
	id := strings.Repeat("a", 32)
	selected, err := selectRuntimeRelease(generationSnapshotForTest(id, build), build, bundle, filepath.Join(t.TempDir(), "runtime"), nil, buildinfo.Info{})
	if err != nil || selected.generationID != id || selected.needsStage || !selected.preserveActivePins || selected.controlPath != filepath.Join(bundle, "control-plane") || selected.enginePath != filepath.Join(bundle, "recorder-engine") {
		t.Fatalf("matching bundled generation selection = %+v, %v", selected, err)
	}

	other := build
	other.Commit = strings.Repeat("f", 40)
	if _, err := selectRuntimeRelease(generationSnapshotForTest(id, other), build, bundle, filepath.Join(t.TempDir(), "runtime"), nil, buildinfo.Info{}); err == nil {
		t.Fatal("missing signed installation for the active generation silently fell back to the image bundle")
	}

	empty := generation.Snapshot{Generations: map[string]generation.Generation{}, Leases: map[string]generation.Lease{}}
	selected, err = selectRuntimeRelease(empty, build, bundle, filepath.Join(t.TempDir(), "runtime"), nil, buildinfo.Info{})
	if err != nil || !selected.needsStage || !generationPattern.MatchString(selected.generationID) {
		t.Fatalf("fresh bundle selection = %+v, %v", selected, err)
	}
}

func TestLegacyImageBundleUpgradeStagesExactNewGenerationAndPreservesPins(t *testing.T) {
	oldBuild := buildinfo.Info{Version: "0.1.0-rc.9", Commit: strings.Repeat("9", 40), ReleaseChannel: "prerelease"}
	newBuild := buildinfo.Info{Version: "0.1.0-rc.10-diagnostic", Commit: strings.Repeat("a", 40), ReleaseChannel: "prerelease"}
	oldID := strings.Repeat("b", 32)
	state := generationSnapshotForTest(oldID, oldBuild) // Legacy records have no image-source field.
	old := state.Generations[oldID]
	old.AdapterSetID = strings.Repeat("c", 64)
	old.StorageProviderSetID = strings.Repeat("d", 64)
	old.StorageInstanceID = "si_" + strings.Repeat("e", 32)
	state.Generations[oldID] = old
	runtimeRoot := filepath.Join(t.TempDir(), "runtime")
	bundle := filepath.Join(t.TempDir(), "bundle")

	if _, err := selectRuntimeRelease(state, newBuild, bundle, runtimeRoot, nil, buildinfo.Info{}); err == nil {
		t.Fatal("legacy active identity changed without explicit installer provenance")
	}
	selected, err := selectRuntimeRelease(state, newBuild, bundle, runtimeRoot, nil, oldBuild)
	if err != nil || !selected.needsStage || !selected.preserveActivePins || selected.generationID == oldID || selected.appBuild != newBuild {
		t.Fatalf("explicit image transition selection=%+v err=%v", selected, err)
	}
	prepared, candidate, stage, err := prepareStartupGeneration(selected, state, strings.Repeat("f", 64), strings.Repeat("1", 64), "si_"+strings.Repeat("2", 32))
	if err != nil || !stage || candidate.ID != prepared.generationID || candidate.ID == oldID {
		t.Fatalf("transition candidate=%+v selected=%+v stage=%t err=%v", candidate, prepared, stage, err)
	}
	if candidate.Version != newBuild.Version || candidate.Commit != newBuild.Commit {
		t.Fatalf("new generation identity=%s/%s, want exact new image %s/%s", candidate.Version, candidate.Commit, newBuild.Version, newBuild.Commit)
	}
	if candidate.AdapterSetID != old.AdapterSetID || candidate.StorageProviderSetID != old.StorageProviderSetID || candidate.StorageInstanceID != old.StorageInstanceID {
		t.Fatalf("image upgrade changed pinned provider tuple: old=%+v new=%+v", old, candidate)
	}
	if state.Generations[oldID] != old || state.ActiveGenerationID != oldID {
		t.Fatalf("selection mutated old active generation: %+v", state.Generations[oldID])
	}
	if selected.imageBundleUpgradeSourceGenerationID != oldID {
		t.Fatalf("transition source generation=%q, want %q", selected.imageBundleUpgradeSourceGenerationID, oldID)
	}
}

func TestImageBundleUpgradeRetryRecognizesAlreadyActiveCandidate(t *testing.T) {
	oldBuild := buildinfo.Info{Version: "0.1.0-rc.9", Commit: strings.Repeat("9", 40), ReleaseChannel: "prerelease"}
	newBuild := buildinfo.Info{Version: "0.1.0-rc.10-diagnostic", Commit: strings.Repeat("a", 40), ReleaseChannel: "prerelease"}
	oldID, activeID := strings.Repeat("b", 32), strings.Repeat("c", 32)
	old := runtimeGeneration(oldID, oldBuild)
	old.State = generation.StateDraining
	active := runtimeGeneration(activeID, newBuild)
	active.State = generation.StateActive
	snapshot := generation.Snapshot{
		SchemaVersion: generation.SchemaVersion, ActiveGenerationID: activeID, PreviousGenerationID: oldID,
		Generations: map[string]generation.Generation{oldID: old, activeID: active},
		Leases:      map[string]generation.Lease{},
	}
	selected, err := selectRuntimeRelease(snapshot, newBuild, t.TempDir(), filepath.Join(t.TempDir(), "runtime"), nil, oldBuild)
	if err != nil {
		t.Fatal(err)
	}
	if selected.generationID != activeID || selected.needsStage || !selected.preserveActivePins || selected.imageBundleUpgradeSourceGenerationID != oldID {
		t.Fatalf("retry selection lost transition provenance: %+v", selected)
	}

	// A stale or unrelated marker must not authorize source-generation cleanup.
	// The active image-bundled generation still retains its own provider pins.
	selected, err = selectRuntimeRelease(snapshot, newBuild, t.TempDir(), filepath.Join(t.TempDir(), "runtime"), nil,
		buildinfo.Info{Version: "0.1.0-rc.8", Commit: strings.Repeat("8", 40)})
	if err != nil || !selected.preserveActivePins || selected.imageBundleUpgradeSourceGenerationID != "" {
		t.Fatalf("unrelated installer marker authorized transition recovery: %+v, %v", selected, err)
	}
}

func TestImageBundleUpgradeDormantsOnlyUnleasedSourceGeneration(t *testing.T) {
	for _, withLease := range []bool{false, true} {
		name := "unleased source becomes dormant"
		if withLease {
			name = "leased source remains authoritative"
		}
		t.Run(name, func(t *testing.T) {
			registry, err := generation.Open(filepath.Join(t.TempDir(), "generations.json"))
			if err != nil {
				t.Fatal(err)
			}
			oldID, newID := strings.Repeat("b", 32), strings.Repeat("c", 32)
			oldBuild := buildinfo.Info{Version: "0.1.0-rc.9", Commit: strings.Repeat("9", 40)}
			newBuild := buildinfo.Info{Version: "0.1.0-rc.10-diagnostic", Commit: strings.Repeat("a", 40)}
			stageBootstrapGeneration(t, registry, runtimeGeneration(oldID, oldBuild))
			if err := registry.Activate(oldID); err != nil {
				t.Fatal(err)
			}
			if err := registry.FinalizeActivation(oldID); err != nil {
				t.Fatal(err)
			}
			if withLease {
				now := time.Now().UTC()
				if err := registry.ReconcileInventory(generation.EngineInventory{
					Confirmed: true, EngineGeneration: oldID, WorkerInstance: strings.Repeat("d", 32), ObservedAt: now,
					Recordings: []generation.InventoryRecording{{RecordingID: strings.Repeat("e", 32), StartedAt: now}},
				}); err != nil {
					t.Fatal(err)
				}
			}
			stageBootstrapGeneration(t, registry, runtimeGeneration(newID, newBuild))
			if err := registry.Activate(newID); err != nil {
				t.Fatal(err)
			}
			if err := registry.FinalizeActivation(newID); err != nil {
				t.Fatal(err)
			}

			before := registry.Snapshot()
			if err := reconcileImageBundleUpgradeSource(registry, oldID); err != nil {
				t.Fatalf("reconcile source generation: %v", err)
			}
			after := registry.Snapshot()
			oldAfter := after.Generations[oldID]
			if withLease {
				if oldAfter.EngineDormant || len(after.Leases) != 1 || after.Leases[strings.Repeat("e", 32)].EngineGeneration != oldID {
					t.Fatalf("leased source was changed: before=%+v after=%+v", before, after)
				}
			} else if !oldAfter.EngineDormant || len(after.Leases) != 0 {
				t.Fatalf("unleased source was not marked dormant: %+v", after)
			}
			if after.ActiveGenerationID != newID || after.PreviousGenerationID != oldID || oldAfter.State != generation.StateDraining {
				t.Fatalf("image transition changed immutable generation lifecycle pointers: %+v", after)
			}
		})
	}
}

func TestColdSetupRestartDormantsOnlyUnleasedPreviousGeneration(t *testing.T) {
	for _, withLease := range []bool{false, true} {
		name := "unleased predecessor is dormant after restart"
		if withLease {
			name = "leased predecessor remains authoritative after restart"
		}
		t.Run(name, func(t *testing.T) {
			registry, err := generation.Open(filepath.Join(t.TempDir(), "generations.json"))
			if err != nil {
				t.Fatal(err)
			}
			oldID, candidateID, restartedID := strings.Repeat("a", 32), strings.Repeat("b", 32), strings.Repeat("c", 32)
			oldBuild := buildinfo.Info{Version: "0.1.0-rc.9", Commit: strings.Repeat("9", 40)}
			candidateBuild := buildinfo.Info{Version: "0.1.0-rc.10-diagnostic", Commit: strings.Repeat("a", 40)}
			stageBootstrapGeneration(t, registry, runtimeGeneration(oldID, oldBuild))
			if err := registry.Activate(oldID); err != nil {
				t.Fatal(err)
			}
			if err := registry.FinalizeActivation(oldID); err != nil {
				t.Fatal(err)
			}
			stageBootstrapGeneration(t, registry, runtimeGeneration(candidateID, candidateBuild))
			if err := registry.Activate(candidateID); err != nil {
				t.Fatal(err)
			}
			if err := registry.FinalizeActivation(candidateID); err != nil {
				t.Fatal(err)
			}
			if err := registry.MarkEngineDormant(oldID); err != nil {
				t.Fatalf("mark original image generation dormant: %v", err)
			}
			if withLease {
				now := time.Now().UTC()
				if err := registry.ReconcileInventory(generation.EngineInventory{
					Confirmed: true, EngineGeneration: candidateID, WorkerInstance: strings.Repeat("d", 32), ObservedAt: now,
					Recordings: []generation.InventoryRecording{{RecordingID: strings.Repeat("e", 32), StartedAt: now}},
				}); err != nil {
					t.Fatal(err)
				}
			}

			// A restart may create a same-version generation to adopt a changed
			// bundled provider pin. Its predecessor is not necessarily the
			// original image generation and must be reconciled independently.
			stageBootstrapGeneration(t, registry, runtimeGeneration(restartedID, candidateBuild))
			if err := registry.Activate(restartedID); err != nil {
				t.Fatal(err)
			}
			if err := registry.FinalizeActivation(restartedID); err != nil {
				t.Fatal(err)
			}
			before := registry.Snapshot()
			if before.PreviousGenerationID != candidateID || before.Generations[candidateID].Version != candidateBuild.Version {
				t.Fatalf("precondition does not represent the restart transition: %+v", before)
			}

			if err := reconcileUnleasedPreviousGeneration(registry); err != nil {
				t.Fatalf("reconcile previous generation: %v", err)
			}
			after := registry.Snapshot()
			previous := after.Generations[candidateID]
			if withLease {
				if previous.EngineDormant || len(after.Leases) != 1 || after.Leases[strings.Repeat("e", 32)].EngineGeneration != candidateID {
					t.Fatalf("leased predecessor was changed: before=%+v after=%+v", before, after)
				}
			} else if !previous.EngineDormant || len(after.Leases) != 0 {
				t.Fatalf("unleased predecessor was not marked dormant: %+v", after)
			}
			if after.ActiveGenerationID != restartedID || after.PreviousGenerationID != candidateID || previous.State != generation.StateDraining {
				t.Fatalf("restart reconciliation changed generation identity or rollback pointers: %+v", after)
			}
			if after.Generations[oldID] != before.Generations[oldID] {
				t.Fatalf("reconciliation changed a non-predecessor generation: before=%+v after=%+v", before.Generations[oldID], after.Generations[oldID])
			}
		})
	}
}

func stageBootstrapGeneration(t *testing.T, registry *generation.Registry, item generation.Generation) {
	t.Helper()
	if err := registry.Stage(item); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkVerified(item.ID); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady(item.ID); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidInstalledReleaseNeverFallsBackToBundleTransition(t *testing.T) {
	oldBuild := buildinfo.Info{Version: "0.1.0-rc.9", Commit: strings.Repeat("9", 40)}
	newBuild := buildinfo.Info{Version: "0.1.0-rc.10-diagnostic", Commit: strings.Repeat("a", 40)}
	id := strings.Repeat("b", 32)
	state := generationSnapshotForTest(id, oldBuild)
	runtimeRoot := filepath.Join(t.TempDir(), "runtime")
	releaseDir := filepath.Join(runtimeRoot, "releases", install.ReleaseDirectoryID(oldBuild.Version, oldBuild.Commit))
	if err := os.MkdirAll(releaseDir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := selectRuntimeRelease(state, newBuild, t.TempDir(), runtimeRoot, nil, oldBuild); err == nil || !strings.Contains(err.Error(), "active installed release failed verification") {
		t.Fatalf("invalid signed-release directory fell back to bundle: %v", err)
	}
}

func TestSignedActiveReleaseIsPreservedAcrossImageUpgrade(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest, manifestBytes, signature, payloads := signedTestRelease(t, private)
	runtimeRoot := filepath.Join(t.TempDir(), "runtime")
	t.Cleanup(func() {
		_ = filepath.Walk(runtimeRoot, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			mode := os.FileMode(0600)
			if info.IsDir() {
				mode = 0700
			}
			_ = os.Chmod(path, mode)
			return nil
		})
	})
	installer, err := install.NewInstaller(runtimeRoot, map[string]ed25519.PublicKey{"test-key": public}, currentHostCompatibility())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := installer.Stage(context.Background(), &controllerSource{manifest: manifestBytes, signature: signature, payloads: payloads}); err != nil {
		t.Fatalf("stage signed fixture: %v", err)
	}
	activeID := strings.Repeat("b", 32)
	activeBuild := buildinfo.Info{Version: manifest.ReleaseVersion, Commit: manifest.Commit}
	state := generationSnapshotForTest(activeID, activeBuild)
	bundleBuild := buildinfo.Info{Version: "0.1.0-rc.10-diagnostic", Commit: strings.Repeat("a", 40)}
	selected, err := selectRuntimeRelease(state, bundleBuild, t.TempDir(), runtimeRoot, map[string]ed25519.PublicKey{"test-key": public}, activeBuild)
	if err != nil {
		t.Fatal(err)
	}
	if selected.generationID != activeID || selected.needsStage || selected.preserveActivePins || selected.manifest == nil || selected.appBuild.Version != manifest.ReleaseVersion || selected.appBuild.Commit != manifest.Commit {
		t.Fatalf("explicitly installed signed release was displaced by image bundle: %+v", selected)
	}
}

func TestPrepareStartupGenerationCreatesAdapterOnlyTupleWithoutMutatingOldGeneration(t *testing.T) {
	bundle := t.TempDir()
	build := buildinfoForTest()
	id := strings.Repeat("a", 32)
	oldSetID, newSetID := strings.Repeat("b", 64), strings.Repeat("c", 64)
	storageSetID := strings.Repeat("d", 64)
	storageInstanceID := "si_" + strings.Repeat("e", 32)
	registryState := generationSnapshotForTest(id, build)
	old := registryState.Generations[id]
	old.AdapterSetID = oldSetID
	old.StorageProviderSetID = storageSetID
	old.StorageInstanceID = storageInstanceID
	old.ArchiveReadCompatibility = generation.CompatibilityRange{Minimum: 2, Maximum: 2}
	old.ArchiveWriteFormat = 2
	registryState.Generations[id] = old
	selected, err := selectRuntimeRelease(registryState, build, bundle, filepath.Join(t.TempDir(), "runtime"), nil, buildinfo.Info{})
	if err != nil {
		t.Fatal(err)
	}
	prepared, candidate, needsStage, err := prepareStartupGeneration(selected, registryState, newSetID, storageSetID, storageInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if !needsStage || !prepared.needsStage || prepared.generationID == id || candidate.ID != prepared.generationID || candidate.AdapterSetID != newSetID || candidate.State != generation.StateStaging {
		t.Fatalf("adapter-set-only startup did not allocate a staged generation: selected=%+v candidate=%+v stage=%t", prepared, candidate, needsStage)
	}
	if candidate.Version != old.Version || candidate.Commit != old.Commit || candidate.ArchiveReadCompatibility != old.ArchiveReadCompatibility || candidate.ArchiveWriteFormat != old.ArchiveWriteFormat {
		t.Fatalf("adapter-only candidate changed application release compatibility: old=%+v new=%+v", old, candidate)
	}
	if got := registryState.Generations[id]; got.AdapterSetID != oldSetID || got.State != generation.StateActive {
		t.Fatalf("startup adapter reconciliation mutated existing generation: %+v", got)
	}

	unchanged, same, stage, err := prepareStartupGeneration(selected, registryState, oldSetID, storageSetID, storageInstanceID)
	if err != nil || stage || unchanged.generationID != id || same.ID != id || same.AdapterSetID != oldSetID {
		t.Fatalf("same adapter set should reuse active generation: selected=%+v generation=%+v stage=%t err=%v", unchanged, same, stage, err)
	}
	changedStorage := strings.Repeat("e", 64)
	prepared, candidate, needsStage, err = prepareStartupGeneration(selected, registryState, oldSetID, changedStorage, storageInstanceID)
	if err != nil || !needsStage || candidate.StorageProviderSetID != changedStorage || candidate.StorageInstanceID != storageInstanceID || candidate.AdapterSetID != oldSetID {
		t.Fatalf("storage-set-only startup change did not allocate a consistent tuple: candidate=%+v stage=%t err=%v", candidate, needsStage, err)
	}
	otherInstance := "si_" + strings.Repeat("f", 32)
	prepared, candidate, needsStage, err = prepareStartupGeneration(selected, registryState, oldSetID, storageSetID, otherInstance)
	if err != nil || !needsStage || candidate.StorageProviderSetID != storageSetID || candidate.StorageInstanceID != otherInstance {
		t.Fatalf("instance-only startup selection did not allocate a consistent tuple: candidate=%+v stage=%t err=%v", candidate, needsStage, err)
	}
}

type startupStorageCatalogFixture struct {
	sets      map[string]storagecatalog.Set
	instances map[string]storagecatalog.StorageInstance
}

func (f startupStorageCatalogFixture) LoadSet(id string) (storagecatalog.Set, error) {
	set, ok := f.sets[id]
	if !ok {
		return storagecatalog.Set{}, storagecatalog.ErrSetMissing
	}
	return set, nil
}

func (f startupStorageCatalogFixture) LoadStorageInstance(id string) (storagecatalog.StorageInstance, error) {
	instance, ok := f.instances[id]
	if !ok {
		return storagecatalog.StorageInstance{}, storagecatalog.ErrSetMissing
	}
	return instance, nil
}

func TestStartupStoragePinUsesCurrentDesiredSetForActiveInstance(t *testing.T) {
	instanceID := "si_" + strings.Repeat("e", 32)
	activeID := strings.Repeat("a", 32)
	adapterSetID := strings.Repeat("f", 64)
	legacySet := storagecatalog.Set{ID: strings.Repeat("d", 64), Artifact: storagecatalog.Artifact{ID: "local"}}
	activeSet := storagecatalog.Set{ID: strings.Repeat("b", 64), Artifact: storagecatalog.Artifact{ID: "local"}}
	desiredSet := storagecatalog.Set{ID: strings.Repeat("c", 64), Artifact: storagecatalog.Artifact{ID: "local"}}
	instance := storagecatalog.StorageInstance{ID: instanceID, ProviderID: "local", DesiredSetID: desiredSet.ID}
	catalog := startupStorageCatalogFixture{
		sets:      map[string]storagecatalog.Set{legacySet.ID: legacySet, activeSet.ID: activeSet, desiredSet.ID: desiredSet},
		instances: map[string]storagecatalog.StorageInstance{instance.ID: instance},
	}
	build := buildinfoForTest()
	state := generationSnapshotForTest(activeID, build)
	active := state.Generations[activeID]
	active.AdapterSetID = adapterSetID
	active.StorageProviderSetID = activeSet.ID
	active.StorageInstanceID = instanceID
	state.Generations[activeID] = active

	storageSetID, storageInstanceID, err := resolveStartupStoragePin(catalog, state, legacySet.ID)
	if err != nil || storageSetID != desiredSet.ID || storageInstanceID != instanceID {
		t.Fatalf("startup storage pin = (%q, %q, %v), want current instance set %q and stable ID %q", storageSetID, storageInstanceID, err, desiredSet.ID, instanceID)
	}

	selected := runtimeRelease{generationID: activeID, appBuild: build}
	_, candidate, needsStage, err := prepareStartupGeneration(selected, state, adapterSetID, storageSetID, storageInstanceID)
	if err != nil || !needsStage || candidate.StorageProviderSetID != desiredSet.ID || candidate.StorageInstanceID != instanceID {
		t.Fatalf("startup candidate = (%+v, stage=%t, err=%v), want staged generation pinned to current instance set", candidate, needsStage, err)
	}
	if got := state.Generations[activeID]; got.StorageProviderSetID != activeSet.ID || got.StorageInstanceID != instanceID || got.State != generation.StateActive {
		t.Fatalf("startup changed old generation pin before staging: %+v", got)
	}

	instance.ProviderID = "other-provider"
	catalog.instances[instanceID] = instance
	if _, _, err := resolveStartupStoragePin(catalog, state, legacySet.ID); err == nil {
		t.Fatal("startup accepted instance whose provider ID differs from its pinned set")
	}
}

func TestFreshStartupImportsValidAdaptersWhenAnotherCandidateIsRejected(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("adapter executable fixture uses a POSIX shell")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	writeStartupAdapter(t, filepath.Join(source, "integrated-recorder-adapter-good"))
	if err := os.WriteFile(filepath.Join(source, "integrated-recorder-adapter-invalid"), []byte("#!/bin/sh\nprintf 'not-json\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	catalog, err := adaptercatalog.Open(filepath.Join(root, "runtime", "adapters"), []string{source})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { makeRuntimeE2ETreeWritable(root) })
	selected, active, activeExists, err := reconcileStartupAdapterSet(context.Background(), catalog, generation.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if activeExists || active.ID != "" {
		t.Fatalf("fresh install unexpectedly has an active generation: exists=%t active=%+v", activeExists, active)
	}
	if len(selected.Entries) != 1 || selected.Entries[0].AdapterID != "startup-good" || selected.RejectedCount != 1 {
		t.Fatalf("fresh startup did not keep valid candidate alongside rejected candidate: %+v", selected)
	}
}

func TestStartupReconcileFailsClosedWhenCatalogCannotImportPluginSources(t *testing.T) {
	catalog := &testHostAdapterCatalog{sets: map[string]adaptercatalog.Snapshot{}}
	pluginSource := filepath.Join(t.TempDir(), "plugin-source")
	if _, _, _, err := reconcileStartupAdapterSet(context.Background(), catalog, generation.Snapshot{}, pluginSource); err == nil {
		t.Fatal("startup silently ignored Host-owned plugin source")
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if catalog.reconcileCalls != 0 {
		t.Fatalf("fallback reconcile calls = %d, want 0 when plugin source cannot be imported", catalog.reconcileCalls)
	}
}

func writeStartupAdapter(t *testing.T, path string) {
	t.Helper()
	descriptor := `{"id":"startup-good","name":"Startup Test","version":"1.0.0","protocol_version":1,"capabilities":["resolve"],"input_schema":{"fields":[]},"configuration_schema":{"fields":[]},"media_types":["hls"]}`
	script := `#!/bin/sh
while IFS= read -r line; do
 id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^" ]*\)".*/\1/p')
 case "$line" in
	  *'"method":"describe"'*) printf '{"protocol_version":1,"id":"%s","result":%s}\n' "$id" '` + descriptor + `' ;;
	  *'"method":"shutdown"'*) printf '{"protocol_version":1,"id":"%s","result":{}}\n' "$id"; exit 0 ;;
 esac
done
`
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
}

func TestExitedEngineReleasesHostOwnedIngestResources(t *testing.T) {
	coordinator, err := resources.New(resources.Limits{GlobalBufferBytes: 100, PerRecordingBufferBytes: 100, QueueObjects: 1, WriterConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	owner := "e" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 32)
	if err := coordinator.SetReservation(context.Background(), owner, strings.Repeat("c", 32), "payload-1", 80); err != nil {
		t.Fatal(err)
	}
	if got := coordinator.Snapshot().UsedBytes; got != 80 {
		t.Fatalf("reserved bytes = %d, want 80", got)
	}
	releaseEngineResources(coordinator, supervisor.ProcessSpec{GenerationID: strings.Repeat("a", 32), Role: supervisor.RoleEngine, Env: []string{"RUNTIME_RESOURCE_OWNER=" + owner}})
	if got := coordinator.Snapshot().UsedBytes; got != 0 {
		t.Fatalf("bytes retained after confirmed Engine exit = %d, want 0", got)
	}
	if err := coordinator.SetReservation(context.Background(), "e"+strings.Repeat("b", 32), strings.Repeat("c", 32), "payload-2", 100); err != nil {
		t.Fatalf("capacity did not recover after Engine exit: %v", err)
	}
}

func TestExitedControlReleasesProcessTelemetryOwner(t *testing.T) {
	coordinator, err := resources.New(resources.Limits{GlobalBufferBytes: 100, PerRecordingBufferBytes: 100, QueueObjects: 1, WriterConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newResourceOwnerIDForRole("c", strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReportTelemetry(owner, 10, 20, 0); err != nil {
		t.Fatal(err)
	}
	if got := coordinator.TelemetrySnapshot().Throughput.ReadBytesTotal; got != 10 {
		t.Fatalf("read bytes before Control exit = %d, want 10", got)
	}
	releaseProcessResources(coordinator, supervisor.ProcessSpec{GenerationID: strings.Repeat("a", 32), Role: supervisor.RoleControl, Env: []string{"RUNTIME_RESOURCE_OWNER=" + owner}})
	// Owner cleanup removes its cumulative baseline so an identically restarted
	// process can report counters from zero without retaining stale gauges.
	if err := coordinator.ReportTelemetry(owner, 0, 0, 0); err != nil {
		t.Fatalf("restarted Control could not report fresh telemetry: %v", err)
	}
}

func TestBootstrapSupervisorRealProcessSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("process-boundary smoke test")
	}
	work := bootstrapTestDir(t, "runtime-host-process-")
	home := filepath.Join(work, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	controlAddr := reserveControlAddress(t)
	target, err := url.Parse("http://" + controlAddr)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("b", 32)
	engineMarker := filepath.Join(work, "engine.started")
	controlMarker := filepath.Join(work, "control.started")
	makeProcess := func(role supervisor.Role, marker string, extra ...string) supervisor.ProcessSpec {
		env := []string{"PATH=/usr/bin:/bin", "HOME=" + home, "BOOTSTRAP_HELPER=1", "BOOTSTRAP_ROLE=" + string(role), "BOOTSTRAP_MARKER=" + marker}
		env = append(env, extra...)
		return supervisor.ProcessSpec{
			GenerationID: id, Role: role, Executable: executable,
			Args: []string{"-test.run=^TestBootstrapSubprocessHelper$"}, Env: env,
		}
	}
	launcher := supervisor.ExecLauncher{}
	readiness := supervisor.ReadinessFunc(func(ctx context.Context, spec supervisor.ProcessSpec, _ supervisor.Child) error {
		return waitMarker(ctx, envValueForTest(spec.Env, "BOOTSTRAP_MARKER"))
	})
	lifecycle := &smokeLifecycle{}
	sup, err := supervisor.New(supervisor.Options{Launcher: launcher, Readiness: readiness, ControlLifecycle: lifecycle, ShutdownTimeout: 4 * time.Second, CleanupTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sup.Close(ctx)
	}()
	engine := makeProcess(supervisor.RoleEngine, engineMarker)
	control := makeProcess(supervisor.RoleControl, controlMarker, "BOOTSTRAP_CONTROL_ADDR="+controlAddr)
	if err := sup.StageGeneration(context.Background(), supervisor.GenerationSpec{ID: id, Engine: engine, Control: control, ControlTarget: target}); err != nil {
		t.Fatal(err)
	}
	if err := sup.ActivateControl(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- sup.Serve(serveCtx, listener) }()
	responseBody := waitForHostResponse(t, "http://"+listener.Addr().String())
	if responseBody != "runtime-host-smoke-control" {
		t.Fatalf("stable listener response = %q", responseBody)
	}
	enginePID := readPIDMarker(t, engineMarker)
	controlPID := readPIDMarker(t, controlMarker)
	if enginePID == controlPID || enginePID == os.Getpid() || controlPID == os.Getpid() {
		t.Fatalf("expected distinct OS children (host=%d engine=%d control=%d)", os.Getpid(), enginePID, controlPID)
	}
	cancelServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Supervisor Serve shutdown: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Runtime Host smoke listener did not shut down in time")
	}
	for _, marker := range []string{engineMarker + ".stopped", controlMarker + ".stopped"} {
		if _, err := os.Stat(marker); err != nil {
			t.Fatalf("subprocess shutdown marker %q: %v", marker, err)
		}
	}
}

func envValueForTest(env []string, key string) string {
	for _, item := range env {
		name, value, ok := strings.Cut(item, "=")
		if ok && name == key {
			return value
		}
	}
	return ""
}

// TestBootstrapSubprocessHelper is re-executed as an OS child by the smoke test.
func TestBootstrapSubprocessHelper(t *testing.T) {
	if os.Getenv("BOOTSTRAP_HELPER") != "1" {
		return
	}
	role := os.Getenv("BOOTSTRAP_ROLE")
	marker := os.Getenv("BOOTSTRAP_MARKER")
	writeMarker := func(path, body string) {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			os.Exit(3)
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if role == string(supervisor.RoleControl) {
		listener, err := net.Listen("tcp", os.Getenv("BOOTSTRAP_CONTROL_ADDR"))
		if err != nil {
			os.Exit(4)
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "runtime-host-smoke-control") })}
		go func() { _ = server.Serve(listener) }()
		writeMarker(marker, fmt.Sprint(os.Getpid()))
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), time.Second)
		_ = server.Shutdown(shutdown)
		stop()
	} else if role == string(supervisor.RoleEngine) {
		writeMarker(marker, fmt.Sprint(os.Getpid()))
		<-ctx.Done()
	} else {
		os.Exit(5)
	}
	writeMarker(marker+".stopped", "stopped")
}

func reserveControlAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitMarker(ctx context.Context, path string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitForHostResponse(t *testing.T, address string) string {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(address)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				return string(body)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stable Runtime Host listener did not reach its Control child")
	return ""
}

func readPIDMarker(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil || pid <= 0 {
		t.Fatalf("process marker %q has invalid PID %q: %v", path, data, err)
	}
	return pid
}

type smokeLifecycle struct{}

func (*smokeLifecycle) PrepareActivation(context.Context, string) error      { return nil }
func (*smokeLifecycle) PrepareHandoff(context.Context, string, string) error { return nil }
func (*smokeLifecycle) Activate(context.Context, string) error               { return nil }
func (*smokeLifecycle) Rollback(context.Context, string, string) error       { return nil }

var _ supervisor.ControlLifecycle = (*smokeLifecycle)(nil)

func buildinfoForTest() buildinfo.Info {
	return buildinfo.Info{Version: "dev", Commit: "unknown", BuildTime: "unknown", ReleaseChannel: "development", RuntimeProtocolVersion: buildinfo.RuntimeProtocolVersion}
}

func generationSnapshotForTest(id string, build buildinfo.Info) generation.Snapshot {
	item := runtimeGeneration(id, build)
	item.State = generation.StateActive
	return generation.Snapshot{
		SchemaVersion: generation.SchemaVersion, ActiveGenerationID: id,
		Generations: map[string]generation.Generation{id: item}, Leases: map[string]generation.Lease{},
	}
}
