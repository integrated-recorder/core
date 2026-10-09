package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storageprocess"
)

func TestStorageRegistryInstallConfigureProbeActivate(t *testing.T) {
	fixture := newControllerFixture(t, false)

	providerBinary := buildStorageProviderFixture(t)
	binaryBytes, err := os.ReadFile(providerBinary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binaryBytes)
	digestHex := hex.EncodeToString(digest[:])

	registryServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/registry.json":
			w.Header().Set("Content-Type", "application/json")
			artifactURL := "https://" + r.Host + "/artifact"
			document := map[string]any{
				"schema_version": 2,
				"plugins": []any{map[string]any{
					"id": "fixture-storage", "type": "storage", "name": "Fixture Storage",
					"repository": "https://example.test/fixture-storage",
					"channels":   map[string]string{"stable": "1.0.0"},
					"releases": []any{map[string]any{
						"version": "1.0.0", "protocol": map[string]any{"name": "storage", "version": 1},
						"source_commit": "0123456789abcdef",
						"artifacts": []any{map[string]any{
							"os": "linux", "arch": "amd64", "url": artifactURL,
							"filename": "integrated-recorder-storage-fixture-storage",
							"size":     len(binaryBytes), "sha256": digestHex,
						}},
					}},
				}},
			}
			if err := json.NewEncoder(w).Encode(document); err != nil {
				t.Errorf("encode storage registry fixture: %v", err)
			}
		case "/artifact":
			w.Header().Set("Content-Length", fmt.Sprint(len(binaryBytes)))
			_, _ = w.Write(binaryBytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer registryServer.Close()

	registryRoot := filepath.Join(fixture.root, "runtime", "plugin-registry")
	registry, err := pluginregistry.Open(pluginregistry.Config{
		Root: registryRoot, RegistryURL: registryServer.URL + "/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: registryServer.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogRoot := filepath.Join(fixture.root, "runtime", "storage-providers")
	catalog, err := storagecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	fixture.controller.pluginRegistry = registry
	fixture.controller.storageCatalog = catalog
	fixture.controller.installation = readyInstallation(t, fixture.root)
	localSet := installBundledLocalProviderForTest(t, fixture, catalog)

	hostAPI, err := httpapi.New(nil, true, false, fixture.controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	initialGeneration := fixture.registry.Snapshot().ActiveGenerationID

	status, code, _ := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/refresh")
	if code != http.StatusOK {
		t.Fatalf("POST plugin refresh: status %d", code)
	}
	if !storageRegistryItem(status, "fixture-storage", "storage", "1.0.0", false) {
		t.Fatalf("refreshed registry status does not show the storage release: %+v", status)
	}

	status, code, body := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/install")
	if code != http.StatusOK || !storageRegistryItem(status, "fixture-storage", "storage", "1.0.0", true) {
		t.Fatalf("storage install status=%d body=%s item=%+v", code, body, status)
	}
	if fixture.registry.Snapshot().ActiveGenerationID != initialGeneration {
		t.Fatal("storage install activated a backend before explicit configuration and activation")
	}
	artifacts, err := catalog.Installed()
	if err != nil || len(artifacts) != 2 {
		t.Fatalf("catalog installed artifacts=%+v err=%v", artifacts, err)
	}
	var artifact storagecatalog.Artifact
	for _, installed := range artifacts {
		if installed.ID == "fixture-storage" {
			artifact = installed
		}
	}
	if artifact.ID != "fixture-storage" || artifact.Version != "1.0.0" || artifact.Digest != digestHex || artifact.Size != int64(len(binaryBytes)) {
		t.Fatalf("installed artifact=%+v, want exact registry identity and bytes", artifact)
	}
	installedTrust, found, err := catalog.InstalledAttestation("fixture-storage")
	if err != nil || !found || installedTrust != plugintrust.NewCustomRegistry() {
		t.Fatalf("installed storage attestation=%+v found=%t err=%v, want custom registry", installedTrust, found, err)
	}
	artifactPath, err := catalog.ArtifactPath(artifact.Digest)
	if err != nil {
		t.Fatal(err)
	}
	installedBytes, err := os.ReadFile(artifactPath)
	if err != nil || string(installedBytes) != string(binaryBytes) {
		t.Fatalf("immutable imported binary differs from approved bytes: read=%v", err)
	}
	if _, err := catalog.DesiredSet("fixture-storage"); err == nil {
		t.Fatal("storage install created desired configuration before Configure")
	}
	localStatus := callStorageStatus(t, hostAPI)
	if localStatus.Primary.Kind != "plugin" || localStatus.Primary.ProviderID != "local" || localStatus.Primary.Version != localSet.Artifact.Version {
		t.Fatalf("install changed bundled local primary before activation: %+v", localStatus.Primary)
	}
	localSummary, ok := storageProviderSummary(localStatus, "local")
	if !ok || localSummary.Distribution != "bundled" || !localSummary.ConfigurationManaged || localSummary.Uninstallable || !localSummary.Active {
		t.Fatalf("bundled local provider projection=%+v found=%t", localSummary, ok)
	}

	objectRoot := filepath.Join(t.TempDir(), "objects")
	// Configuration must use the durable selection captured at install time,
	// not current Registry availability or catalog view.
	registryServer.Close()
	configurationBody, err := json.Marshal(httpapi.StorageConfigRequest{
		Values:  map[string]json.RawMessage{"root": mustStorageJSON(t, objectRoot)},
		Secrets: map[string]string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	configCode, configBody := callStorageAPI(t, hostAPI, http.MethodPut, httpapi.StorageProvidersPrefix+"fixture-storage/config", string(configurationBody))
	if configCode != http.StatusOK {
		t.Fatalf("PUT storage config status=%d body=%s", configCode, configBody)
	}
	configCode, configBody = callStorageAPI(t, hostAPI, http.MethodGet, httpapi.StorageProvidersPrefix+"fixture-storage/config", "")
	if configCode != http.StatusOK {
		t.Fatalf("GET storage config status=%d body=%s", configCode, configBody)
	}
	var configView httpapi.StorageConfigView
	if err := json.Unmarshal([]byte(configBody), &configView); err != nil {
		t.Fatal(err)
	}
	if got := string(configView.Values["root"]); got != mustStorageJSONString(t, objectRoot) {
		t.Fatalf("public non-secret root value=%s", got)
	}
	configured, err := catalog.DesiredSet("fixture-storage")
	if err != nil || configured.EffectiveAttestation() != plugintrust.NewCustomRegistry() {
		t.Fatalf("configured storage set attestation=%+v err=%v, want original custom Registry authority", configured.EffectiveAttestation(), err)
	}
	for name, public := range map[string]string{"config": configBody, "install status": body} {
		for _, private := range []string{registryRoot, catalogRoot, registryServer.URL + "/artifact", digestHex, "ipc_token", ".sock"} {
			if strings.Contains(public, private) {
				t.Errorf("%s exposed private runtime detail %q", name, private)
			}
		}
	}

	probeCode, probeBody := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/probe", `{}`)
	if probeCode != http.StatusOK || !strings.Contains(probeBody, `"state":"ready"`) {
		t.Fatalf("POST storage probe status=%d body=%s", probeCode, probeBody)
	}
	if fixture.registry.Snapshot().ActiveGenerationID != initialGeneration {
		t.Fatal("probe activated a backend before explicit activation")
	}

	activateCode, activateBody := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/activate", `{}`)
	if activateCode != http.StatusOK {
		t.Fatalf("POST storage activate status=%d body=%s", activateCode, activateBody)
	}
	var activeStatus httpapi.StorageProviderStatus
	if err := json.Unmarshal([]byte(activateBody), &activeStatus); err != nil {
		t.Fatal(err)
	}
	desired, err := catalog.DesiredSet("fixture-storage")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := fixture.registry.Snapshot()
	active := snapshot.Generations[snapshot.ActiveGenerationID]
	if snapshot.ActiveGenerationID == initialGeneration || active.StorageProviderSetID != desired.ID {
		t.Fatalf("active generation=%+v id=%s, expected set %s and a new generation", active, snapshot.ActiveGenerationID, desired.ID)
	}
	if activeStatus.Primary.Kind != "plugin" || activeStatus.Primary.ProviderID != "fixture-storage" || activeStatus.Primary.Version != "1.0.0" || activeStatus.Primary.State != "ready" {
		t.Fatalf("active primary storage projection=%+v", activeStatus.Primary)
	}
	fixture.sup.mu.Lock()
	engineEnv := append([]string(nil), fixture.sup.engineSpecs[len(fixture.sup.engineSpecs)-1].Env...)
	controlEnv := append([]string(nil), fixture.sup.controlSpecs[len(fixture.sup.controlSpecs)-1].Env...)
	fixture.sup.mu.Unlock()
	for _, env := range [][]string{engineEnv, controlEnv} {
		if got := childEnvValue(env, "STORAGE_PROVIDER_SET_ID"); got != desired.ID {
			t.Errorf("candidate process storage set ID=%q, want %q", got, desired.ID)
		}
		if got := childEnvValue(env, "STORAGE_PROVIDER_CATALOG_ROOT"); got != catalogRoot {
			t.Errorf("candidate process storage catalog root=%q, want %q", got, catalogRoot)
		}
	}

	// Remote-to-remote placement changes must account for Core's shared local
	// staging namespace too; checking only each remote object namespace would
	// miss a pending payload that has not reached either provider yet.
	stagingDir := filepath.Join(fixture.root, "runtime", "storage-staging")
	if err := os.MkdirAll(stagingDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "pending-payload"), []byte("staged but not published"), 0600); err != nil {
		t.Fatal(err)
	}
	newObjectRoot := filepath.Join(t.TempDir(), "provider-new-objects")
	configureStorageValues(t, hostAPI, map[string]json.RawMessage{"root": mustStorageJSON(t, newObjectRoot)}, map[string]string{})
	if code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/probe", `{}`); code != http.StatusOK {
		t.Fatalf("probe reconfigured remote provider status=%d body=%s", code, body)
	}
	beforeStagingBlockedSwitch := fixture.registry.Snapshot().ActiveGenerationID
	if code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/activate", `{}`); code != http.StatusConflict || !strings.Contains(body, `"code":"storage_backend_switch_requires_empty_archive"`) {
		t.Fatalf("remote switch with shared staged payload status=%d body=%s", code, body)
	}
	if got := fixture.registry.Snapshot().ActiveGenerationID; got != beforeStagingBlockedSwitch {
		t.Fatalf("staged-payload rejection changed active generation from %s to %s", beforeStagingBlockedSwitch, got)
	}

	// The installed/pinned provider remains usable when registry transport is
	// unavailable. A successful fresh process launch and Core-owned CRUD probe
	// here proves that playback/recording generations do not depend on registry
	// connectivity after activation.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, provider, err := storageprocess.OpenStore(ctx, fixture.root, catalogRoot, desired.ID)
	if err != nil {
		t.Fatalf("open pinned provider after registry shutdown: %v", err)
	}
	defer provider.Close()
	if store == nil || provider.Descriptor().ID != "fixture-storage" {
		t.Fatalf("reopened provider did not preserve pinned identity: store=%t descriptor=%+v", store != nil, provider.Descriptor())
	}
	if err := storage.ProbePhysicalObjectStore(ctx, provider.ObjectStore()); err != nil {
		t.Fatalf("Core object probe after registry shutdown: %v", err)
	}
}

func TestStorageProviderUpdateKeepsInstalledAndPinnedVersionsDistinct(t *testing.T) {
	fixture := newControllerFixture(t, false)
	v1Binary := buildStorageProviderVariant(t, "1.0.0", "legacy", true)
	v2Binary := buildStorageProviderVariant(t, "2.0.0", "current", false)

	v1Bytes := mustReadStorageBinary(t, v1Binary)
	v2Bytes := mustReadStorageBinary(t, v2Binary)
	v1Digest := storageBinaryDigest(v1Bytes)
	v2Digest := storageBinaryDigest(v2Bytes)

	var releaseMu sync.RWMutex
	releases := map[string][]byte{"1.0.0": v1Bytes, "2.0.0": v2Bytes}
	currentVersion := "1.0.0"
	registryServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		releaseMu.RLock()
		version := currentVersion
		binary := append([]byte(nil), releases[version]...)
		releaseMu.RUnlock()
		switch r.URL.Path {
		case "/registry.json":
			w.Header().Set("Content-Type", "application/json")
			digest := v1Digest
			if version == "2.0.0" {
				digest = v2Digest
			}
			document := storageRegistryDocument(r.Host, version, digest, int64(len(binary)))
			if err := json.NewEncoder(w).Encode(document); err != nil {
				t.Errorf("encode storage registry fixture: %v", err)
			}
		case "/artifact/1.0.0":
			w.Header().Set("Content-Length", fmt.Sprint(len(v1Bytes)))
			_, _ = w.Write(v1Bytes)
		case "/artifact/2.0.0":
			w.Header().Set("Content-Length", fmt.Sprint(len(v2Bytes)))
			_, _ = w.Write(v2Bytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer registryServer.Close()

	registryRoot := filepath.Join(fixture.root, "runtime", "plugin-registry")
	pluginManager, err := pluginregistry.Open(pluginregistry.Config{
		Root: registryRoot, RegistryURL: registryServer.URL + "/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: registryServer.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogRoot := filepath.Join(fixture.root, "runtime", "storage-providers")
	catalog, err := storagecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	installBundledLocalProviderForTest(t, fixture, catalog)
	fixture.controller.pluginRegistry = pluginManager
	fixture.controller.storageCatalog = catalog
	fixture.controller.installation = readyInstallation(t, fixture.root)
	hostAPI, err := httpapi.New(nil, true, false, fixture.controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}

	if _, code, _ := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("refresh v1 registry status=%d", code)
	}
	if status, code, body := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/install"); code != http.StatusOK || !storageRegistryItem(status, "fixture-storage", "storage", "1.0.0", true) {
		t.Fatalf("install v1 status=%d body=%s", code, body)
	}
	rootV1 := filepath.Join(t.TempDir(), "provider-v1-objects")
	secret := "old-version-secret-value"
	configureStorageValues(t, hostAPI, map[string]json.RawMessage{"root": mustStorageJSON(t, rootV1)}, map[string]string{"legacy_secret": secret})
	if code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/probe", `{}`); code != http.StatusOK {
		t.Fatalf("probe v1 status=%d body=%s", code, body)
	}
	if code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/activate", `{}`); code != http.StatusOK {
		t.Fatalf("activate v1 status=%d body=%s", code, body)
	}

	v1Set, err := catalog.DesiredSet("fixture-storage")
	if err != nil || v1Set.Artifact.Version != "1.0.0" {
		t.Fatalf("v1 desired set=%+v err=%v", v1Set, err)
	}
	v1Generation := fixture.registry.Snapshot().Generations[fixture.registry.Snapshot().ActiveGenerationID]
	if v1Generation.StorageProviderSetID != v1Set.ID {
		t.Fatalf("v1 generation does not pin v1 set: generation=%+v set=%+v", v1Generation, v1Set)
	}

	releaseMu.Lock()
	currentVersion = "2.0.0"
	releaseMu.Unlock()
	if status, code, _ := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("refresh v2 registry status=%d", code)
	} else if item, ok := pluginStatusItem(status, "fixture-storage"); !ok || item.Type != "storage" || item.AvailableVersion != "2.0.0" || item.InstalledVersion != "1.0.0" || !item.Installed || !item.UpdateAvailable {
		t.Fatalf("registry refresh should expose v2 available while v1 remains installed: %+v found=%t", item, ok)
	}
	if status, code, body := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/update"); code != http.StatusOK || !storageRegistryItem(status, "fixture-storage", "storage", "2.0.0", true) {
		t.Fatalf("update to incompatible v2 status=%d body=%s", code, body)
	}

	status := callStorageStatus(t, hostAPI)
	if status.Primary.Kind != "plugin" || status.Primary.ProviderID != "fixture-storage" || status.Primary.Version != "1.0.0" {
		t.Fatalf("v2 install changed the v1 active primary: %+v", status.Primary)
	}
	current, ok := storageProviderSummary(status, "fixture-storage")
	if !ok || current.Version != "2.0.0" || current.Configured || current.Active {
		t.Fatalf("installed v2 projection should require configuration and remain inactive: %+v found=%t", current, ok)
	}
	configCode, configBody := callStorageAPI(t, hostAPI, http.MethodGet, httpapi.StorageProvidersPrefix+"fixture-storage/config", "")
	if configCode != http.StatusOK {
		t.Fatalf("GET current v2 config status=%d body=%s", configCode, configBody)
	}
	var currentConfig httpapi.StorageConfigView
	if err := json.Unmarshal([]byte(configBody), &currentConfig); err != nil {
		t.Fatal(err)
	}
	if len(currentConfig.Values) != 0 || len(currentConfig.ConfiguredSecrets) != 0 || strings.Contains(configBody, "legacy_secret") || strings.Contains(configBody, "root") || strings.Contains(configBody, secret) {
		t.Fatalf("stale v1 values/secret indicators leaked through v2 config view: %s", configBody)
	}
	for _, endpoint := range []string{"probe", "activate"} {
		code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/"+endpoint, `{}`)
		if code != http.StatusConflict || !strings.Contains(body, `"code":"storage_provider_not_configured"`) {
			t.Errorf("stale desired set %s status=%d body=%s, want safe not-configured conflict", endpoint, code, body)
		}
	}
	if fixture.registry.Snapshot().ActiveGenerationID != v1Generation.ID {
		t.Fatal("stale v1 desired set changed the active generation")
	}

	rootV2 := filepath.Join(t.TempDir(), "provider-v2-objects")
	configureStorageValues(t, hostAPI, map[string]json.RawMessage{"endpoint": mustStorageJSON(t, rootV2)}, nil)
	status = callStorageStatus(t, hostAPI)
	current, ok = storageProviderSummary(status, "fixture-storage")
	if !ok || current.Version != "2.0.0" || !current.Configured || current.Active {
		t.Fatalf("configured v2 projection before activation=%+v found=%t", current, ok)
	}
	if code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/probe", `{}`); code != http.StatusOK {
		t.Fatalf("probe configured v2 status=%d body=%s", code, body)
	}
	if code, body := callStorageAPI(t, hostAPI, http.MethodPost, httpapi.StorageProvidersPrefix+"fixture-storage/activate", `{}`); code != http.StatusOK {
		t.Fatalf("activate configured v2 status=%d body=%s", code, body)
	}
	after := fixture.registry.Snapshot()
	active := after.Generations[after.ActiveGenerationID]
	old := after.Generations[v1Generation.ID]
	if active.StorageProviderSetID == "" || active.StorageProviderSetID == v1Set.ID || old.StorageProviderSetID != v1Set.ID || old.State != generation.StateDraining {
		t.Fatalf("v2 activation must preserve the old draining generation's v1 pin: active=%+v old=%+v v1Set=%s", active, old, v1Set.ID)
	}
	status = callStorageStatus(t, hostAPI)
	if status.Primary.Version != "2.0.0" {
		t.Fatalf("new primary version=%q, want v2", status.Primary.Version)
	}
	current, ok = storageProviderSummary(status, "fixture-storage")
	if !ok || current.Version != "2.0.0" || !current.Configured || !current.Active {
		t.Fatalf("active v2 projection=%+v found=%t", current, ok)
	}
	// If the installed marker is removed while generations still pin a provider,
	// inventory must retain the active pinned identity without claiming it is a
	// current configured install.
	if err := catalog.Uninstall("fixture-storage"); err != nil {
		t.Fatal(err)
	}
	status = callStorageStatus(t, hostAPI)
	current, ok = storageProviderSummary(status, "fixture-storage")
	if status.Primary.Version != "2.0.0" || !ok || current.Version != "2.0.0" || !current.Active || current.Configured {
		t.Fatalf("active-but-uninstalled pinned provider visibility=%+v primary=%+v found=%t", current, status.Primary, ok)
	}
}

func TestStorageProviderUpdateCarriesCompatibleConfiguration(t *testing.T) {
	fixture := newControllerFixture(t, false)
	v1Binary := buildStorageProviderVariant(t, "1.0.0", "legacy", true)
	v2Binary := buildStorageProviderVariant(t, "2.0.0", "compatible", true)
	v1Bytes, v2Bytes := mustReadStorageBinary(t, v1Binary), mustReadStorageBinary(t, v2Binary)
	v1Digest, v2Digest := storageBinaryDigest(v1Bytes), storageBinaryDigest(v2Bytes)
	currentVersion := "1.0.0"
	var versionMu sync.RWMutex
	registryServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		versionMu.RLock()
		version := currentVersion
		versionMu.RUnlock()
		var binary []byte
		digest := v1Digest
		if version == "1.0.0" {
			binary = v1Bytes
		} else {
			binary, digest = v2Bytes, v2Digest
		}
		switch r.URL.Path {
		case "/registry.json":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(storageRegistryDocument(r.Host, version, digest, int64(len(binary)))); err != nil {
				t.Errorf("encode compatible storage registry: %v", err)
			}
		case "/artifact/1.0.0":
			w.Header().Set("Content-Length", fmt.Sprint(len(v1Bytes)))
			_, _ = w.Write(v1Bytes)
		case "/artifact/2.0.0":
			w.Header().Set("Content-Length", fmt.Sprint(len(v2Bytes)))
			_, _ = w.Write(v2Bytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer registryServer.Close()
	api := setupStorageRegistryController(t, fixture, registryServer)
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("refresh v1: status=%d body=%s", code, body)
	}
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/install"); code != http.StatusOK {
		t.Fatalf("install v1: status=%d body=%s", code, body)
	}
	root := filepath.Join(t.TempDir(), "compatible-provider-objects")
	configureStorageValues(t, api, map[string]json.RawMessage{"root": mustStorageJSON(t, root)}, map[string]string{"legacy_secret": "carry-forward-secret"})
	versionMu.Lock()
	currentVersion = "2.0.0"
	versionMu.Unlock()
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("refresh v2: status=%d body=%s", code, body)
	}
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/update"); code != http.StatusOK {
		t.Fatalf("compatible update: status=%d body=%s", code, body)
	}
	status := callStorageStatus(t, api)
	current, ok := storageProviderSummary(status, "fixture-storage")
	if !ok || current.Version != "2.0.0" || !current.Configured || current.Active {
		t.Fatalf("compatible v2 update status=%+v found=%t", current, ok)
	}
	code, body := callStorageAPI(t, api, http.MethodGet, httpapi.StorageProvidersPrefix+"fixture-storage/config", "")
	if code != http.StatusOK {
		t.Fatalf("GET carried config status=%d body=%s", code, body)
	}
	var config httpapi.StorageConfigView
	if err := json.Unmarshal([]byte(body), &config); err != nil {
		t.Fatal(err)
	}
	if got := string(config.Values["root"]); got != mustStorageJSONString(t, root) {
		t.Fatalf("compatible nonsecret config was not carried forward: %s", body)
	}
	if len(config.ConfiguredSecrets) != 1 || config.ConfiguredSecrets[0] != "legacy_secret" || strings.Contains(body, "carry-forward-secret") {
		t.Fatalf("compatible secret carry-forward view=%s", body)
	}
}

func TestStorageInstanceConfigureAfterProviderUpdateUsesSubmittedConfig(t *testing.T) {
	fixture := newControllerFixture(t, false)
	v1Binary := buildStorageProviderVariant(t, "1.0.0", "legacy", true)
	v2Binary := buildStorageProviderVariant(t, "2.0.0", "compatible", true)
	v1Bytes, v2Bytes := mustReadStorageBinary(t, v1Binary), mustReadStorageBinary(t, v2Binary)
	v1Digest, v2Digest := storageBinaryDigest(v1Bytes), storageBinaryDigest(v2Bytes)
	currentVersion := "1.0.0"
	var versionMu sync.RWMutex
	registryServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		versionMu.RLock()
		version := currentVersion
		versionMu.RUnlock()
		binary, digest := v1Bytes, v1Digest
		if version == "2.0.0" {
			binary, digest = v2Bytes, v2Digest
		}
		switch r.URL.Path {
		case "/registry.json":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(storageRegistryDocument(r.Host, version, digest, int64(len(binary)))); err != nil {
				t.Errorf("encode storage instance registry: %v", err)
			}
		case "/artifact/1.0.0", "/artifact/2.0.0":
			w.Header().Set("Content-Length", fmt.Sprint(len(binary)))
			_, _ = w.Write(binary)
		default:
			http.NotFound(w, r)
		}
	}))
	defer registryServer.Close()
	api := setupStorageRegistryController(t, fixture, registryServer)
	catalog := fixture.controller.storageCatalog

	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("refresh v1: status=%d body=%s", code, body)
	}
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/install"); code != http.StatusOK {
		t.Fatalf("install v1: status=%d body=%s", code, body)
	}
	rootV1 := filepath.Join(t.TempDir(), "instance-provider-v1")
	createBody, err := json.Marshal(httpapi.StorageInstanceCreateRequest{
		DisplayName: "Fixture account", ProviderID: "fixture-storage",
		Values:  map[string]json.RawMessage{"root": mustStorageJSON(t, rootV1)},
		Secrets: map[string]string{"legacy_secret": "old-artifact-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	code, body := callStorageAPI(t, api, http.MethodPost, httpapi.StorageInstancesEndpoint, string(createBody))
	if code != http.StatusOK {
		t.Fatalf("create v1 storage instance: status=%d body=%s", code, body)
	}
	var created httpapi.StorageInstanceSummary
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatal(err)
	}
	oldSet, err := catalog.LoadSet(created.DesiredSetID)
	if err != nil || oldSet.Artifact.Digest != v1Digest || oldSet.Config.Secrets["legacy_secret"] != "old-artifact-secret" {
		t.Fatalf("created v1 immutable set has wrong artifact or secret presence: artifact_digest=%s err=%v", oldSet.Artifact.Digest, err)
	}
	if code, body := callStorageAPI(t, api, http.MethodPost, httpapi.StorageInstancesPrefix+created.ID+"/probe", `{}`); code != http.StatusOK {
		t.Fatalf("probe v1 instance: status=%d body=%s", code, body)
	}
	if code, body := callStorageAPI(t, api, http.MethodPost, httpapi.StorageInstancesPrefix+created.ID+"/activate", `{}`); code != http.StatusOK {
		t.Fatalf("activate v1 instance: status=%d body=%s", code, body)
	}
	activeBeforeUpdate := fixture.registry.Snapshot().Generations[fixture.registry.Snapshot().ActiveGenerationID]
	if activeBeforeUpdate.StorageProviderSetID != oldSet.ID || activeBeforeUpdate.StorageInstanceID != created.ID {
		t.Fatalf("active generation did not pin exact v1 instance set: %+v", activeBeforeUpdate)
	}

	versionMu.Lock()
	currentVersion = "2.0.0"
	versionMu.Unlock()
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("refresh v2: status=%d body=%s", code, body)
	}
	if _, code, body := callPluginAPI(t, api, http.MethodPost, httpapi.PluginsEndpoint+"/fixture-storage/update"); code != http.StatusOK {
		t.Fatalf("update provider to v2: status=%d body=%s", code, body)
	}

	rootV2 := filepath.Join(t.TempDir(), "instance-provider-v2")
	request := httpapi.StorageConfigRequest{
		Values: map[string]json.RawMessage{
			"root": mustStorageJSON(t, rootV2), "region": mustStorageJSON(t, "kr-central-1"),
		},
		Secrets: map[string]string{},
	}
	requestBody, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	path := httpapi.StorageInstancesPrefix + created.ID + "/config"
	code, body = callStorageAPI(t, api, http.MethodPut, path, string(requestBody))
	if code != http.StatusOK {
		t.Fatalf("configure instance after provider update: status=%d body=%s", code, body)
	}
	if strings.Contains(body, "old-artifact-secret") || strings.Contains(body, "new-artifact-secret") {
		t.Fatalf("configuration response exposed a credential: %s", body)
	}
	var view httpapi.StorageConfigView
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.ConfiguredSecrets) != 0 || string(view.Values["root"]) != mustStorageJSONString(t, rootV2) || string(view.Values["region"]) != mustStorageJSONString(t, "kr-central-1") {
		t.Fatalf("new artifact configuration view lost submitted values or inherited old secret: %+v", view)
	}
	updated, err := catalog.LoadStorageInstance(created.ID)
	if err != nil || updated.ID != created.ID || updated.DesiredSetID == oldSet.ID {
		t.Fatalf("instance identity/config update=%+v err=%v", updated, err)
	}
	newSet, err := catalog.LoadSet(updated.DesiredSetID)
	if err != nil || newSet.Artifact.Digest != v2Digest || newSet.Config.Secrets["legacy_secret"] != "" ||
		string(newSet.Config.Values["root"]) != mustStorageJSONString(t, rootV2) || string(newSet.Config.Values["region"]) != mustStorageJSONString(t, "kr-central-1") {
		t.Fatalf("new v2 immutable set lost submitted config or inherited old secret: artifact_digest=%s configured_secret=%t err=%v", newSet.Artifact.Digest, newSet.Config.Secrets["legacy_secret"] != "", err)
	}

	newSecretRequest := httpapi.StorageConfigRequest{Values: request.Values, Secrets: map[string]string{"legacy_secret": "new-artifact-secret"}}
	requestBody, err = json.Marshal(newSecretRequest)
	if err != nil {
		t.Fatal(err)
	}
	code, body = callStorageAPI(t, api, http.MethodPut, path, string(requestBody))
	if code != http.StatusOK || strings.Contains(body, "new-artifact-secret") {
		t.Fatalf("configure submitted v2 secret: status=%d body=%s", code, body)
	}
	updated, err = catalog.LoadStorageInstance(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	newSet, err = catalog.LoadSet(updated.DesiredSetID)
	if err != nil || newSet.Config.Secrets["legacy_secret"] != "new-artifact-secret" {
		t.Fatalf("submitted v2 secret was not stored in new immutable set: configured_secret=%t err=%v", newSet.Config.Secrets["legacy_secret"] != "", err)
	}

	activeAfterConfigure := fixture.registry.Snapshot().Generations[fixture.registry.Snapshot().ActiveGenerationID]
	if activeAfterConfigure.StorageProviderSetID != oldSet.ID || activeAfterConfigure.StorageInstanceID != created.ID {
		t.Fatalf("instance config update changed active generation's exact set pin: %+v", activeAfterConfigure)
	}
}

func buildStorageProviderFixture(t *testing.T) string {
	t.Helper()
	packageDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(t.TempDir(), "integrated-recorder-storage-fixture-storage")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "../../../internal/storageproto/testdata/provider")
	command.Dir = packageDir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build real storage provider fixture: %v\n%s", err, output)
	}
	info, err := os.Stat(binaryPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("built provider is not a regular executable: info=%v err=%v", info, err)
	}
	return binaryPath
}

func installBundledLocalProviderForTest(t *testing.T, fixture *controllerFixture, catalog *storagecatalog.Catalog) storagecatalog.Set {
	t.Helper()
	packageDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(t.TempDir(), "storage.local")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "../../../cmd/storage-local")
	command.Dir = packageDir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build bundled storage.local fixture: %v\n%s", err, output)
	}
	artifact, err := catalog.ImportBundled(ctx, binaryPath, "local")
	if err != nil {
		t.Fatalf("import bundled storage.local fixture: %v", err)
	}
	if err := catalog.InstallWithAttestation(artifact, plugintrust.NewBundled()); err != nil {
		t.Fatalf("install bundled storage.local fixture: %v", err)
	}
	rootValue, err := json.Marshal(filepath.Join(fixture.root, "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	set, err := catalog.CreateInstalledSet("local", storagecatalog.SetConfig{Values: map[string]json.RawMessage{"root": rootValue}})
	if err != nil {
		t.Fatalf("create bundled local provider set: %v", err)
	}
	if set.EffectiveAttestation() != plugintrust.NewBundled() {
		t.Fatalf("bundled local storage set trust = %+v, want bundled first-party", set.EffectiveAttestation())
	}
	if err := catalog.SelectDesiredSet("local", set.ID); err != nil {
		t.Fatalf("select bundled local provider set: %v", err)
	}
	if err := fixture.registry.AdoptLegacyStorageProviderSet(set.ID); err != nil {
		t.Fatalf("adopt initial generation onto bundled local provider: %v", err)
	}
	return set
}

func storageRegistryItem(status httpapi.PluginStatus, id, pluginType, version string, installed bool) bool {
	for _, item := range status.Plugins {
		if item.ID == id {
			return item.Type == pluginType && item.AvailableVersion == version && item.Installed == installed && (!installed || item.InstalledVersion == version)
		}
	}
	return false
}

func callStorageAPI(t *testing.T, handler http.Handler, method, path, body string) (int, string) {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if method != http.MethodGet {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code, response.Body.String()
}

func callStorageStatus(t *testing.T, handler http.Handler) httpapi.StorageProviderStatus {
	t.Helper()
	code, body := callStorageAPI(t, handler, http.MethodGet, httpapi.StorageProviderEndpoint, "")
	if code != http.StatusOK {
		t.Fatalf("GET storage status=%d body=%s", code, body)
	}
	var status httpapi.StorageProviderStatus
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func mustStorageJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustStorageJSONString(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func buildStorageProviderVariant(t *testing.T, version, schemaVariant string, includeLegacySecret bool) string {
	t.Helper()
	packageDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(packageDir, "../../../internal/storageproto/testdata/provider/main.go")
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, `Version: "1.0.0"`) {
		t.Fatal("storage fixture descriptor version anchor changed")
	}
	text = strings.Replace(text, `Version: "1.0.0"`, fmt.Sprintf(`Version: %q`, version), 1)
	switch schemaVariant {
	case "legacy":
		if includeLegacySecret {
			text = replaceStorageFixtureFields(t, text, []string{
				`{Key: "root", Control: "text", Label: "Object directory"}`,
				`{Key: "legacy_secret", Control: "secret", Label: "Legacy secret"}`,
			})
		}
	case "current":
		text = replaceStorageFixtureFields(t, text, []string{
			`{Key: "endpoint", Control: "text", Label: "Object directory"}`,
		})
		text = replaceStorageFixtureText(t, text, `config.Values["root"]`, `config.Values["endpoint"]`)
	case "compatible":
		if !includeLegacySecret {
			t.Fatal("compatible fixture requires the legacy secret field")
		}
		text = replaceStorageFixtureFields(t, text, []string{
			`{Key: "root", Control: "text", Label: "Object directory"}`,
			`{Key: "legacy_secret", Control: "secret", Label: "Legacy secret"}`,
			`{Key: "region", Control: "text", Label: "Region"}`,
		})
	default:
		t.Fatalf("unknown fixture schema variant %q", schemaVariant)
	}
	variantDir, err := os.MkdirTemp(".", "teststorageprovider-build-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(variantDir) })
	if err := os.WriteFile(filepath.Join(variantDir, "main.go"), []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "integrated-recorder-storage-fixture-storage")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", output, ".")
	command.Dir = variantDir
	combined, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s storage provider fixture: %v\n%s", version, err, combined)
	}
	info, err := os.Stat(output)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("built %s provider is not executable: info=%v err=%v", version, info, err)
	}
	return output
}

func replaceStorageFixtureFields(t *testing.T, source string, fields []string) string {
	t.Helper()
	const prefix = `ConfigurationSchema: storageproto.Schema{Fields: []storageproto.Field{`
	const suffix = `}}, Capabilities:`
	start := strings.Index(source, prefix)
	if start < 0 {
		t.Fatal("storage fixture configuration schema anchor changed")
	}
	start += len(prefix)
	endRelative := strings.Index(source[start:], suffix)
	if endRelative < 0 {
		t.Fatal("storage fixture configuration schema terminator changed")
	}
	var replacement strings.Builder
	replacement.WriteString("\n\t\t")
	replacement.WriteString(strings.Join(fields, ",\n\t\t"))
	replacement.WriteString(",\n\t")
	return source[:start] + replacement.String() + source[start+endRelative:]
}

func replaceStorageFixtureText(t *testing.T, source, old, replacement string) string {
	t.Helper()
	if !strings.Contains(source, old) {
		t.Fatalf("storage fixture anchor not found: %s", old)
	}
	return strings.Replace(source, old, replacement, 1)
}

func mustReadStorageBinary(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func storageBinaryDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func storageRegistryDocument(host, version, digest string, size int64) map[string]any {
	return map[string]any{
		"schema_version": 2,
		"plugins": []any{map[string]any{
			"id": "fixture-storage", "type": "storage", "name": "Fixture Storage",
			"repository": "https://example.test/fixture-storage",
			"channels":   map[string]string{"stable": version},
			"releases": []any{map[string]any{
				"version": version, "protocol": map[string]any{"name": "storage", "version": 1},
				"source_commit": "0123456789abcdef",
				"artifacts": []any{map[string]any{
					"os": "linux", "arch": "amd64", "url": "https://" + host + "/artifact/" + version,
					"filename": "integrated-recorder-storage-fixture-storage",
					"size":     size, "sha256": digest,
				}},
			}},
		}},
	}
}

func setupStorageRegistryController(t *testing.T, fixture *controllerFixture, server *httptest.Server) http.Handler {
	t.Helper()
	registryRoot := filepath.Join(fixture.root, "runtime", "plugin-registry")
	pluginManager, err := pluginregistry.Open(pluginregistry.Config{
		Root: registryRoot, RegistryURL: server.URL + "/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := storagecatalog.Open(filepath.Join(fixture.root, "runtime", "storage-providers"))
	if err != nil {
		t.Fatal(err)
	}
	installBundledLocalProviderForTest(t, fixture, catalog)
	fixture.controller.pluginRegistry = pluginManager
	fixture.controller.storageCatalog = catalog
	fixture.controller.installation = readyInstallation(t, fixture.root)
	api, err := httpapi.New(nil, true, false, fixture.controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func configureStorageValues(t *testing.T, api http.Handler, values map[string]json.RawMessage, secrets map[string]string) {
	t.Helper()
	body, err := json.Marshal(httpapi.StorageConfigRequest{Values: values, Secrets: secrets})
	if err != nil {
		t.Fatal(err)
	}
	code, response := callStorageAPI(t, api, http.MethodPut, httpapi.StorageProvidersPrefix+"fixture-storage/config", string(body))
	if code != http.StatusOK {
		t.Fatalf("configure storage status=%d body=%s", code, response)
	}
}

func storageProviderSummary(status httpapi.StorageProviderStatus, id string) (httpapi.StorageProviderSummary, bool) {
	for _, provider := range status.Providers {
		if provider.ID == id {
			return provider, true
		}
	}
	return httpapi.StorageProviderSummary{}, false
}

func pluginStatusItem(status httpapi.PluginStatus, id string) (httpapi.PluginStatusItem, bool) {
	for _, plugin := range status.Plugins {
		if plugin.ID == id {
			return plugin, true
		}
	}
	return httpapi.PluginStatusItem{}, false
}
