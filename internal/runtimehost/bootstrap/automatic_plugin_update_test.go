package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
)

type pluginRegistryMemoryTransport struct {
	t         *testing.T
	mu        sync.RWMutex
	version   string
	artifacts map[string][]byte
	wrongSHA  bool
	wrongSize bool
	failure   bool
	requests  chan string
}

func (f *pluginRegistryMemoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	f.mu.RLock()
	version, failure, wrongSHA, wrongSize := f.version, f.failure, f.wrongSHA, f.wrongSize
	artifact := append([]byte(nil), f.artifacts[version]...)
	requests := f.requests
	f.mu.RUnlock()
	if failure {
		return nil, errors.New("fixture registry unavailable")
	}
	if requests != nil {
		select {
		case requests <- request.URL.Path:
		default:
		}
	}
	response := httptest.NewRecorder()
	switch request.URL.Path {
	case "/registry.json":
		if wrongSHA {
			writeRegistryFixtureWithSHA(f.t, response, request, version, artifact, strings.Repeat("0", 64))
		} else if wrongSize {
			writeRegistryFixtureWithSizeOverride(f.t, response, request, version, artifact)
		} else {
			writeRegistryFixture(f.t, response, request, version, artifact)
		}
	case "/artifact":
		response.Header().Set("Content-Length", strconv.Itoa(len(artifact)))
		_, _ = response.Write(artifact)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
	result := response.Result()
	result.Request = request
	return result, nil
}

func (f *pluginRegistryMemoryTransport) setVersion(version string) {
	f.mu.Lock()
	f.version = version
	f.mu.Unlock()
}

func (f *pluginRegistryMemoryTransport) setFailure(failure bool) {
	f.mu.Lock()
	f.failure = failure
	f.mu.Unlock()
}

func (f *pluginRegistryMemoryTransport) setWrongSHA(wrong bool) {
	f.mu.Lock()
	f.wrongSHA = wrong
	f.mu.Unlock()
}

func (f *pluginRegistryMemoryTransport) setWrongSize(wrong bool) {
	f.mu.Lock()
	f.wrongSize = wrong
	f.mu.Unlock()
}

func (f *pluginRegistryMemoryTransport) setArtifact(version string, artifact []byte) {
	f.mu.Lock()
	f.artifacts[version] = append([]byte(nil), artifact...)
	f.mu.Unlock()
}

func writeRegistryFixtureWithSizeOverride(t *testing.T, w http.ResponseWriter, r *http.Request, version string, binary []byte) {
	t.Helper()
	base := "https://127.0.0.1"
	if r != nil && r.Host != "" {
		base = "https://" + r.Host
	}
	document := map[string]any{
		"schema_version": 1,
		"plugins": []any{map[string]any{
			"id": "registry_fixture", "name": "Registry Fixture", "repository": "https://github.com/example/fixture",
			"channels": map[string]string{"stable": version},
			"releases": []any{map[string]any{
				"version": version, "protocol_version": 1, "source_commit": strings.Repeat("a", 40),
				"artifacts": []any{map[string]any{
					"os": "linux", "arch": "amd64", "url": base + "/artifact?version=" + version,
					"filename": "integrated-recorder-adapter-registry_fixture", "size": len(binary) + 1, "sha256": sha256Hex(binary),
				}},
			}},
		}},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(document); err != nil {
		t.Errorf("encode incorrect-size registry fixture: %v", err)
	}
}

func newAutomaticSourcePluginFixture(t *testing.T) (*controllerFixture, *pluginregistry.Manager, *adaptercatalog.Catalog, *pluginRegistryMemoryTransport, string, string) {
	t.Helper()
	fixture := newControllerFixture(t, false)
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	v1Path := buildFixtureAdapter(t, root, "registry_fixture", "1.0.0")
	v2Path := buildFixtureAdapter(t, root, "registry_fixture", "2.0.0")
	v1, err := os.ReadFile(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := os.ReadFile(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	transport := &pluginRegistryMemoryTransport{t: t, version: "1.0.0", artifacts: map[string][]byte{"1.0.0": v1, "2.0.0": v2}}
	manager, err := pluginregistry.Open(pluginregistry.Config{
		Root: filepath.Join(fixture.root, "runtime", "plugin-registry"), RegistryURL: "https://localhost/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh initial registry: %v", err)
	}
	catalog, err := adaptercatalog.Open(filepath.Join(fixture.root, "runtime", "adapters"), nil)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := manager.DesiredSources()
	if err != nil {
		t.Fatal(err)
	}
	initialSet, err := catalog.ReconcileClassified(context.Background(), "", sources)
	if err != nil {
		t.Fatal(err)
	}
	bindActiveAdapterSet(t, fixture, initialSet.ID)
	fixture.controller.adapterCatalog = catalog
	fixture.controller.pluginRegistry = manager
	fixture.controller.installation = readyInstallation(t, fixture.root)
	if _, err := fixture.controller.InstallPlugin(context.Background(), "registry_fixture"); err != nil {
		t.Fatalf("manually install initial source plugin: %v", err)
	}
	return fixture, manager, catalog, transport, v1Path, v2Path
}

func TestAutomaticSourcePluginUpdateUsesGenerationTransactionAndPreservesRecordingPin(t *testing.T) {
	fixture, _, catalog, transport, _, _ := newAutomaticSourcePluginFixture(t)
	installedGeneration := fixture.registry.Snapshot().ActiveGenerationID
	installed := fixture.registry.Snapshot().Generations[installedGeneration]
	oldSetID := installed.AdapterSetID
	startedAt := time.Now().UTC()
	if err := fixture.registry.PinRecording(generation.Lease{
		RecordingID: strings.Repeat("e", 32), EngineGeneration: installedGeneration,
		WorkerInstance: strings.Repeat("c", 32), StartedAt: startedAt,
	}); err != nil {
		t.Fatalf("pin source recording to v1: %v", err)
	}
	transport.setVersion("2.0.0")
	if retrySoon := fixture.controller.reconcileRegistryPluginsAutomatically(context.Background()); retrySoon {
		t.Fatal("normal successful automatic pass unexpectedly scheduled an immediate retry")
	}
	status := fixture.controller.pluginStatus()
	if !pluginStatusHasVersion(status, "registry_fixture", "2.0.0") {
		t.Fatalf("automatic pass did not install stable source v2: %+v", status)
	}
	state := fixture.registry.Snapshot()
	newGeneration := state.Generations[state.ActiveGenerationID]
	if state.ActiveGenerationID == installedGeneration || newGeneration.AdapterSetID == oldSetID {
		t.Fatalf("automatic source update did not activate a new immutable set: before=%+v after=%+v", installed, newGeneration)
	}
	if lease := state.Leases[strings.Repeat("e", 32)]; lease.EngineGeneration != installedGeneration {
		t.Fatalf("existing recording moved across adapter sets: lease=%+v old=%s", lease, installedGeneration)
	}
	if _, err := catalog.Load(oldSetID); err != nil {
		t.Fatalf("old adapter set was collected while the previous recording pins it: %v", err)
	}
	if err := fixture.registry.PinRecording(generation.Lease{
		RecordingID: strings.Repeat("f", 32), EngineGeneration: state.ActiveGenerationID,
		WorkerInstance: strings.Repeat("d", 32), StartedAt: startedAt.Add(time.Second),
	}); err != nil {
		t.Fatalf("pin new recording to active generation: %v", err)
	}
	if lease := fixture.registry.Snapshot().Leases[strings.Repeat("f", 32)]; lease.EngineGeneration != state.ActiveGenerationID {
		t.Fatalf("new recording did not pin v2 generation: %+v", lease)
	}
	auto := status.AutomaticUpdate
	if auto == nil || !auto.Enabled || auto.State != "idle" || auto.LastFailureCode != "" || auto.LastCheckedAt == nil || auto.LastSuccessAt == nil {
		t.Fatalf("automatic update status did not report successful completion: %+v", auto)
	}
	if v1, err := os.Stat(catalogRootForSet(t, catalog, oldSetID)); err != nil || !v1.IsDir() {
		t.Fatalf("old immutable adapter files are missing: stat error=%v", err)
	}
	// Reopen the durable Host-owned stores without a successful Registry read:
	// restart correctness comes from the committed desired plan and immutable
	// generation/set, not a later network refresh.
	transport.setFailure(true)
	reloadedPlugins, err := pluginregistry.Open(pluginregistry.Config{
		Root: filepath.Join(fixture.root, "runtime", "plugin-registry"), RegistryURL: "https://localhost/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatalf("reopen durable Registry plugin state: %v", err)
	}
	if installed := reloadedPlugins.View().Installed; len(installed) != 1 || installed[0].Version != "2.0.0" {
		t.Fatalf("restarted Registry plugin state did not retain v2 without network refresh: %+v", installed)
	}
	reloadedGenerations, err := generation.Open(filepath.Join(fixture.root, "runtime", "state", "generations.json"))
	if err != nil {
		t.Fatalf("reopen durable generation state: %v", err)
	}
	reloadedCatalog, err := adaptercatalog.Open(filepath.Join(fixture.root, "runtime", "adapters"), nil)
	if err != nil {
		t.Fatalf("reopen immutable adapter catalog: %v", err)
	}
	reloadedState := reloadedGenerations.Snapshot()
	activeAfterRestart := reloadedState.Generations[reloadedState.ActiveGenerationID]
	if activeAfterRestart.AdapterSetID != newGeneration.AdapterSetID {
		t.Fatalf("restarted Host generation lost the v2 adapter set: before=%+v after=%+v", newGeneration, activeAfterRestart)
	}
	if v2, err := reloadedCatalog.Load(activeAfterRestart.AdapterSetID); err != nil || len(v2.Entries) != 1 || v2.Entries[0].AdapterID != "registry_fixture" || v2.Entries[0].Version != "2.0.0" {
		t.Fatalf("restarted Host could not resolve v2 immutable adapter set: set=%+v err=%v", v2, err)
	}
}

func TestAutomaticSourcePluginUpdateNoopAndFailuresAreNonDestructive(t *testing.T) {
	t.Run("stable already installed", func(t *testing.T) {
		fixture, _, _, _, _, _ := newAutomaticSourcePluginFixture(t)
		before := fixture.registry.Snapshot().ActiveGenerationID
		if retrySoon := fixture.controller.reconcileRegistryPluginsAutomatically(context.Background()); retrySoon {
			t.Fatal("no-op pass unexpectedly scheduled an immediate retry")
		}
		if after := fixture.registry.Snapshot().ActiveGenerationID; after != before {
			t.Fatalf("same stable selection churned the generation: before=%s after=%s", before, after)
		}
	})
	t.Run("digest failure", func(t *testing.T) {
		fixture, manager, _, transport, _, _ := newAutomaticSourcePluginFixture(t)
		before := fixture.registry.Snapshot().ActiveGenerationID
		transport.setVersion("2.0.0")
		transport.setWrongSHA(true)
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		if after := fixture.registry.Snapshot().ActiveGenerationID; after != before {
			t.Fatalf("bad digest changed active generation: before=%s after=%s", before, after)
		}
		if got := manager.View().Installed; len(got) != 1 || got[0].Version != "1.0.0" {
			t.Fatalf("bad digest changed desired source selection: %+v", got)
		}
		status := fixture.controller.pluginStatus()
		if status.AutomaticUpdate == nil || status.AutomaticUpdate.State != "failed" || status.AutomaticUpdate.LastFailureCode != "plugin_verification_failed" {
			t.Fatalf("bad digest was not safely reported: %+v", status.AutomaticUpdate)
		}
	})
	t.Run("size failure", func(t *testing.T) {
		fixture, manager, _, transport, _, _ := newAutomaticSourcePluginFixture(t)
		before := fixture.registry.Snapshot().ActiveGenerationID
		transport.setVersion("2.0.0")
		transport.setWrongSize(true)
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		if after := fixture.registry.Snapshot().ActiveGenerationID; after != before {
			t.Fatalf("wrong artifact size changed active generation: before=%s after=%s", before, after)
		}
		if got := manager.View().Installed; len(got) != 1 || got[0].Version != "1.0.0" {
			t.Fatalf("wrong artifact size changed desired source selection: %+v", got)
		}
		if status := fixture.controller.pluginStatus(); status.AutomaticUpdate == nil || status.AutomaticUpdate.LastFailureCode != "plugin_verification_failed" {
			t.Fatalf("wrong artifact size was not safely reported: %+v", status.AutomaticUpdate)
		}
	})
	t.Run("descriptor identity mismatch", func(t *testing.T) {
		fixture, manager, _, transport, _, _ := newAutomaticSourcePluginFixture(t)
		before := fixture.registry.Snapshot().ActiveGenerationID
		wrongPath := buildFixtureAdapter(t, t.TempDir(), "different_registry_fixture", "2.0.0")
		wrongArtifact, err := os.ReadFile(wrongPath)
		if err != nil {
			t.Fatal(err)
		}
		transport.setVersion("2.0.0")
		transport.setArtifact("2.0.0", wrongArtifact)
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		if after := fixture.registry.Snapshot().ActiveGenerationID; after != before {
			t.Fatalf("descriptor identity mismatch changed active generation: before=%s after=%s", before, after)
		}
		if got := manager.View().Installed; len(got) != 1 || got[0].Version != "1.0.0" {
			t.Fatalf("descriptor identity mismatch changed desired source selection: %+v", got)
		}
		if status := fixture.controller.pluginStatus(); status.AutomaticUpdate == nil || status.AutomaticUpdate.LastFailureCode != "plugin_identity_mismatch" {
			t.Fatalf("descriptor mismatch was not safely reported: %+v", status.AutomaticUpdate)
		}
	})
	t.Run("registry outage", func(t *testing.T) {
		fixture, manager, _, transport, _, _ := newAutomaticSourcePluginFixture(t)
		before := fixture.registry.Snapshot().ActiveGenerationID
		transport.setFailure(true)
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		if after := fixture.registry.Snapshot().ActiveGenerationID; after != before {
			t.Fatalf("registry outage changed active generation: before=%s after=%s", before, after)
		}
		if got := manager.View().Installed; len(got) != 1 || got[0].Version != "1.0.0" {
			t.Fatalf("registry outage removed installed desired plugin: %+v", got)
		}
		if status := fixture.controller.pluginStatus(); status.AutomaticUpdate == nil || status.AutomaticUpdate.LastFailureCode != "plugin_registry_unavailable" {
			t.Fatalf("registry outage was not reported safely: %+v", status.AutomaticUpdate)
		}
	})
}

func TestAutomaticPluginUpdateDefersWithoutWaitingForExplicitOperation(t *testing.T) {
	fixture, _, _, transport, _, _ := newAutomaticSourcePluginFixture(t)
	transport.setVersion("2.0.0")
	before := fixture.registry.Snapshot().ActiveGenerationID
	fixture.controller.gate <- struct{}{}
	if retrySoon := fixture.controller.reconcileRegistryPluginsAutomatically(context.Background()); !retrySoon {
		t.Fatal("busy operation gate should defer and schedule a later pass")
	}
	status := fixture.controller.pluginStatus()
	if status.AutomaticUpdate == nil || status.AutomaticUpdate.State != "deferred" {
		t.Fatalf("busy gate status = %+v, want deferred", status.AutomaticUpdate)
	}
	if got := fixture.registry.Snapshot().ActiveGenerationID; got != before {
		t.Fatalf("background pass mutated the active generation while gate was held: %s", got)
	}
	<-fixture.controller.gate
	if retrySoon := fixture.controller.reconcileRegistryPluginsAutomatically(context.Background()); retrySoon {
		t.Fatal("successful retry unexpectedly remained deferred")
	}
	if !pluginStatusHasVersion(fixture.controller.pluginStatus(), "registry_fixture", "2.0.0") {
		t.Fatal("deferred plugin update did not succeed after the explicit operation released the gate")
	}
}

func TestPluginAutoUpdateSchedulerWakeAndShutdownAreDeterministic(t *testing.T) {
	fixture := newControllerFixture(t, false)
	fixture.controller.installation = readyInstallation(t, fixture.root)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	runs := make(chan struct{}, 2)
	done := startPeriodicPluginAutoUpdateWorker(ctx, fixture.controller.installation, wake, time.Hour, func(delay time.Duration) time.Duration { return delay }, func(context.Context) bool {
		runs <- struct{}{}
		return false
	})
	select {
	case <-runs:
	case <-time.After(5 * time.Second):
		t.Fatal("ready Host did not run the initial asynchronous automatic pass")
	}
	wake <- struct{}{}
	select {
	case <-runs:
	case <-time.After(5 * time.Second):
		t.Fatal("explicit wake did not schedule another automatic pass")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic scheduler did not stop promptly after cancellation")
	}
	select {
	case <-runs:
		t.Fatal("canceled scheduler ran another pass")
	default:
	}
}

type automaticStorageRegistryTransport struct {
	mu        sync.RWMutex
	version   string
	artifacts map[string][]byte
	failure   bool
}

func (f *automaticStorageRegistryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	f.mu.RLock()
	version, failure := f.version, f.failure
	artifact := append([]byte(nil), f.artifacts[version]...)
	f.mu.RUnlock()
	if failure {
		return nil, errors.New("fixture storage registry unavailable")
	}
	response := httptest.NewRecorder()
	switch request.URL.Path {
	case "/registry.json":
		document := storageRegistryDocument(request.URL.Host, version, storageBinaryDigest(artifact), int64(len(artifact)))
		response.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(response).Encode(document); err != nil {
			return nil, err
		}
	case "/artifact/" + version:
		response.Header().Set("Content-Length", strconv.Itoa(len(artifact)))
		_, _ = response.Write(artifact)
	default:
		response.WriteHeader(http.StatusNotFound)
	}
	result := response.Result()
	result.Request = request
	return result, nil
}

func (f *automaticStorageRegistryTransport) setVersion(version string) {
	f.mu.Lock()
	f.version = version
	f.mu.Unlock()
}

func newAutomaticStoragePluginFixture(t *testing.T, v2Schema string) (*controllerFixture, *pluginregistry.Manager, *storagecatalog.Catalog, *automaticStorageRegistryTransport) {
	t.Helper()
	fixture := newControllerFixture(t, false)
	v1Path := buildStorageProviderVariant(t, "1.0.0", "legacy", true)
	v2Path := buildStorageProviderVariant(t, "2.0.0", v2Schema, true)
	transport := &automaticStorageRegistryTransport{version: "1.0.0", artifacts: map[string][]byte{
		"1.0.0": mustReadStorageBinary(t, v1Path),
		"2.0.0": mustReadStorageBinary(t, v2Path),
	}}
	manager, err := pluginregistry.Open(pluginregistry.Config{
		Root: filepath.Join(fixture.root, "runtime", "plugin-registry"), RegistryURL: "https://localhost/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh initial storage registry: %v", err)
	}
	catalog, err := storagecatalog.Open(filepath.Join(fixture.root, "runtime", "storage-providers"))
	if err != nil {
		t.Fatal(err)
	}
	installBundledLocalProviderForTest(t, fixture, catalog)
	fixture.controller.pluginRegistry = manager
	fixture.controller.storageCatalog = catalog
	fixture.controller.installation = readyInstallation(t, fixture.root)
	if _, err := fixture.controller.installStoragePlugin(context.Background(), "fixture-storage", false); err != nil {
		t.Fatalf("install initial storage provider: %v", err)
	}
	return fixture, manager, catalog, transport
}

func TestAutomaticStoragePluginUpdateKeepsArtifactConfigurationAndActivationDistinct(t *testing.T) {
	t.Run("inactive provider remains artifact only", func(t *testing.T) {
		fixture, _, catalog, transport := newAutomaticStoragePluginFixture(t, "compatible")
		activeBefore := fixture.registry.Snapshot().ActiveGenerationID
		transport.setVersion("2.0.0")
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		if activeAfter := fixture.registry.Snapshot().ActiveGenerationID; activeAfter != activeBefore {
			t.Fatalf("storage artifact update changed active generation: before=%s after=%s", activeBefore, activeAfter)
		}
		artifact, err := fixture.controller.installedStorageArtifact("fixture-storage")
		if err != nil || artifact.Version != "2.0.0" {
			t.Fatalf("stable storage artifact was not imported: artifact=%+v err=%v", artifact, err)
		}
		if _, err := catalog.DesiredSet("fixture-storage"); !errors.Is(err, storagecatalog.ErrSetMissing) {
			t.Fatalf("unconfigured provider unexpectedly gained a desired set: err=%v", err)
		}
		if got := automaticStorageState(fixture.controller.pluginStatus(), "fixture-storage"); got != "artifact_installed" {
			t.Fatalf("unconfigured provider lifecycle state=%q, want artifact_installed", got)
		}
	})

	t.Run("compatible configuration does not activate or migrate recording", func(t *testing.T) {
		fixture, _, catalog, transport := newAutomaticStoragePluginFixture(t, "compatible")
		_, err := fixture.controller.ConfigureStorage(context.Background(), "fixture-storage", httpapi.StorageConfigRequest{
			Values:  map[string]json.RawMessage{"root": json.RawMessage(mustStorageJSONString(t, filepath.Join(t.TempDir(), "provider-root")))},
			Secrets: map[string]string{"legacy_secret": "fixture-secret"},
		})
		if err != nil {
			t.Fatalf("configure v1 provider: %v", err)
		}
		v1Set, err := catalog.DesiredSet("fixture-storage")
		if err != nil {
			t.Fatalf("load v1 desired provider set: %v", err)
		}
		if _, err := fixture.controller.ProbeStorage(context.Background(), "fixture-storage"); err != nil {
			t.Fatalf("probe v1 provider: %v", err)
		}
		beforeStatus, err := fixture.controller.ActivateStorage(context.Background(), "fixture-storage")
		if err != nil || beforeStatus.Primary.Version != "1.0.0" {
			t.Fatalf("activate v1 provider: status=%+v err=%v", beforeStatus.Primary, err)
		}
		if got := automaticStorageState(fixture.controller.pluginStatus(), "fixture-storage"); got != "active" {
			t.Fatalf("activated provider lifecycle state=%q, want active", got)
		}
		before := fixture.registry.Snapshot()
		oldGeneration := before.Generations[before.ActiveGenerationID]
		if oldGeneration.StorageProviderSetID != v1Set.ID {
			t.Fatalf("active generation does not pin v1 storage set: generation=%+v set=%+v", oldGeneration, v1Set)
		}
		lease := generation.Lease{RecordingID: strings.Repeat("e", 32), EngineGeneration: oldGeneration.ID, WorkerInstance: strings.Repeat("c", 32), StartedAt: time.Now().UTC()}
		if err := fixture.registry.PinRecording(lease); err != nil {
			t.Fatalf("pin recording to v1 provider generation: %v", err)
		}
		transport.setVersion("2.0.0")
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		state := fixture.registry.Snapshot()
		if state.ActiveGenerationID != before.ActiveGenerationID || state.Leases[lease.RecordingID].EngineGeneration != oldGeneration.ID {
			t.Fatalf("automatic storage update switched or migrated active recording: active=%s lease=%+v", state.ActiveGenerationID, state.Leases[lease.RecordingID])
		}
		configured, err := catalog.DesiredSet("fixture-storage")
		if err != nil || configured.Artifact.Version != "2.0.0" || configured.Config.Secrets["legacy_secret"] != "fixture-secret" {
			t.Fatalf("compatible configuration was not carried to new immutable set: set=%+v err=%v", configured, err)
		}
		if state.Generations[state.ActiveGenerationID].StorageProviderSetID != v1Set.ID {
			t.Fatalf("new storage provider set became active without operator activation: %+v", state.Generations[state.ActiveGenerationID])
		}
		if got := automaticStorageState(fixture.controller.pluginStatus(), "fixture-storage"); got != "configured" {
			t.Fatalf("configured but inactive provider lifecycle state=%q, want configured", got)
		}
		if _, err := catalog.LoadSet(v1Set.ID); err != nil {
			t.Fatalf("old set pinned by active Recording was collected: %v", err)
		}
		storageRoots := []string{state.Generations[state.ActiveGenerationID].StorageProviderSetID}
		if storageRoots[0] != v1Set.ID {
			t.Fatalf("active generation no longer roots the old provider set: roots=%v old=%s", storageRoots, v1Set.ID)
		}
		if err := catalog.CollectGarbage(storageRoots); err != nil {
			t.Fatalf("collect storage catalog with old generation root: %v", err)
		}
		if _, err := catalog.LoadSet(v1Set.ID); err != nil {
			t.Fatalf("old set was not retained by the generation/recording pin: %v", err)
		}
	})

	t.Run("incompatible configuration retains old desired and active set", func(t *testing.T) {
		fixture, _, catalog, transport := newAutomaticStoragePluginFixture(t, "current")
		_, err := fixture.controller.ConfigureStorage(context.Background(), "fixture-storage", httpapi.StorageConfigRequest{
			Values:  map[string]json.RawMessage{"root": json.RawMessage(mustStorageJSONString(t, filepath.Join(t.TempDir(), "provider-root")))},
			Secrets: map[string]string{"legacy_secret": "fixture-secret"},
		})
		if err != nil {
			t.Fatalf("configure v1 provider: %v", err)
		}
		v1Set, err := catalog.DesiredSet("fixture-storage")
		if err != nil {
			t.Fatalf("load v1 desired provider set: %v", err)
		}
		transport.setVersion("2.0.0")
		activeBefore := fixture.registry.Snapshot().ActiveGenerationID
		fixture.controller.reconcileRegistryPluginsAutomatically(context.Background())
		activeAfter := fixture.registry.Snapshot().ActiveGenerationID
		if activeAfter != activeBefore {
			t.Fatalf("incompatible storage update changed generation: before=%s after=%s", activeBefore, activeAfter)
		}
		artifact, err := fixture.controller.installedStorageArtifact("fixture-storage")
		if err != nil || artifact.Version != "2.0.0" {
			t.Fatalf("verified incompatible artifact was not retained for operator configuration: %+v err=%v", artifact, err)
		}
		desired, err := catalog.DesiredSet("fixture-storage")
		if err != nil || desired.ID != v1Set.ID || desired.Artifact.Version != "1.0.0" {
			t.Fatalf("incompatible config moved desired set: set=%+v err=%v", desired, err)
		}
		if got := automaticStorageState(fixture.controller.pluginStatus(), "fixture-storage"); got != "artifact_installed" {
			t.Fatalf("incompatible provider lifecycle state=%q, want artifact_installed", got)
		}
	})
}

func automaticStorageState(status httpapi.PluginStatus, id string) string {
	for _, item := range status.Plugins {
		if item.ID == id {
			return item.StorageState
		}
	}
	return ""
}

func catalogRootForSet(t *testing.T, catalog *adaptercatalog.Catalog, setID string) string {
	t.Helper()
	set, err := catalog.Load(setID)
	if err != nil {
		t.Fatal(err)
	}
	return set.Directory
}
