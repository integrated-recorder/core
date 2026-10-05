package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	productserver "github.com/integrated-recorder/core/internal/server"
)

func TestPluginRegistryInstallUpdateUninstallUsesImmutableGenerationLifecycle(t *testing.T) {
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
	var mu sync.RWMutex
	version := "1.0.0"
	artifacts := map[string][]byte{"1.0.0": v1, "2.0.0": v2}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		switch r.URL.Path {
		case "/registry.json":
			writeRegistryFixture(t, w, r, version, artifacts[version])
		case "/artifact":
			data := artifacts[r.URL.Query().Get("version")]
			if len(data) == 0 {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	registry, err := pluginregistry.Open(pluginregistry.Config{
		Root: filepath.Join(fixture.root, "runtime", "plugin-registry"), RegistryURL: server.URL + "/registry.json",
		GOOS: "linux", GOARCH: "amd64", HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh registry v1: %v", err)
	}
	catalog, err := adaptercatalog.Open(filepath.Join(fixture.root, "runtime", "adapters"), nil)
	if err != nil {
		t.Fatal(err)
	}
	pluginSources, err := registry.DesiredSources()
	if err != nil {
		t.Fatal(err)
	}
	initialSet, err := catalog.ReconcileClassified(context.Background(), "", pluginSources)
	if err != nil {
		t.Fatal(err)
	}
	bindActiveAdapterSet(t, fixture, initialSet.ID)
	fixture.controller.adapterCatalog = catalog
	fixture.controller.pluginRegistry = registry
	fixture.controller.installation = readyInstallation(t, fixture.root)
	hostAPI, err := httpapi.New(nil, true, false, fixture.controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	if status, code, _ := callPluginAPI(t, hostAPI, http.MethodGet, httpapi.PluginsEndpoint); code != http.StatusOK || status.State != "ready" || !pluginStatusHasAvailableVersion(status, "registry_fixture", "1.0.0") {
		t.Fatalf("GET plugin registry status = code %d, status %+v", code, status)
	}

	initialGeneration := fixture.registry.Snapshot().ActiveGenerationID
	status, code, body := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/registry_fixture/install")
	if code != http.StatusOK {
		t.Fatalf("POST plugin install v1: status %d body %s", code, body)
	}
	if status.State != "ready" || !pluginStatusHasVersion(status, "registry_fixture", "1.0.0") {
		t.Fatalf("install status = %+v, want registry_fixture 1.0.0 installed", status)
	}
	v1Generation := fixture.registry.Snapshot().ActiveGenerationID
	if v1Generation == initialGeneration {
		t.Fatal("install did not activate a new application generation")
	}
	v1SetID := fixture.registry.Snapshot().Generations[v1Generation].AdapterSetID
	assertCatalogPlugin(t, catalog, v1SetID, "registry_fixture", "1.0.0", sha256Hex(v1))
	assertCatalogPluginTrust(t, catalog, v1SetID, "registry_fixture", plugintrust.NewCustomRegistry())
	assertPluginAdapterAPI(t, catalog, v1SetID, "registry_fixture", "1.0.0", true)

	mu.Lock()
	version = "2.0.0"
	mu.Unlock()
	if _, code, _ := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/refresh"); code != http.StatusOK {
		t.Fatalf("POST plugin registry refresh v2: status %d", code)
	}
	status, code, body = callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/registry_fixture/update")
	if code != http.StatusOK {
		t.Fatalf("POST plugin update v2: status %d body %s", code, body)
	}
	if !pluginStatusHasVersion(status, "registry_fixture", "2.0.0") {
		t.Fatalf("update status = %+v, want registry_fixture 2.0.0 installed", status)
	}
	v2Generation := fixture.registry.Snapshot().ActiveGenerationID
	if v2Generation == v1Generation {
		t.Fatal("update did not activate a new application generation")
	}
	v2SetID := fixture.registry.Snapshot().Generations[v2Generation].AdapterSetID
	if v2SetID == v1SetID {
		t.Fatal("adapter update reused the old immutable adapter set")
	}
	assertCatalogPlugin(t, catalog, v2SetID, "registry_fixture", "2.0.0", sha256Hex(v2))
	assertCatalogPluginTrust(t, catalog, v2SetID, "registry_fixture", plugintrust.NewCustomRegistry())
	assertCatalogPlugin(t, catalog, v1SetID, "registry_fixture", "1.0.0", sha256Hex(v1))
	assertPluginAdapterAPI(t, catalog, v2SetID, "registry_fixture", "2.0.0", true)

	// A rejected catalog reconciliation may safely keep the active set. An
	// uninstall must not report success while that set still contains the
	// requested plugin; the durable desired state is rolled back in that case.
	fixture.controller.adapterCatalog = rejectedFallbackCatalog{Catalog: catalog}
	_, code, body = callPluginAPI(t, hostAPI, http.MethodDelete, httpapi.PluginsEndpoint+"/registry_fixture")
	if code != http.StatusBadGateway || !strings.Contains(body, "plugin_install_failed") {
		t.Fatalf("uninstall with rejected fallback = status %d, body %s", code, body)
	}
	if fixture.registry.Snapshot().ActiveGenerationID != v2Generation || !pluginStatusHasVersion(fixture.controller.pluginStatus(), "registry_fixture", "2.0.0") {
		t.Fatal("rejected uninstall changed active generation or desired plugin state")
	}
	fixture.controller.adapterCatalog = catalog

	status, code, _ = callPluginAPI(t, hostAPI, http.MethodDelete, httpapi.PluginsEndpoint+"/registry_fixture")
	if code != http.StatusOK {
		t.Fatalf("DELETE plugin uninstall: status %d", code)
	}
	if pluginStatusHasInstalled(status, "registry_fixture") {
		t.Fatalf("uninstall status still lists plugin installed: %+v", status)
	}
	finalGeneration := fixture.registry.Snapshot().ActiveGenerationID
	if finalGeneration == v2Generation {
		t.Fatal("uninstall did not activate a generation without the plugin")
	}
	finalSetID := fixture.registry.Snapshot().Generations[finalGeneration].AdapterSetID
	if set, err := catalog.Load(finalSetID); err != nil || hasCatalogPlugin(set, "registry_fixture") {
		t.Fatalf("uninstall adapter set = %+v, error=%v; want plugin excluded", set, err)
	}
	assertCatalogPlugin(t, catalog, v1SetID, "registry_fixture", "1.0.0", sha256Hex(v1))
	assertCatalogPlugin(t, catalog, v2SetID, "registry_fixture", "2.0.0", sha256Hex(v2))
	assertPluginAdapterAPI(t, catalog, finalSetID, "registry_fixture", "", false)
}

type rejectedFallbackCatalog struct{ *adaptercatalog.Catalog }

func (c rejectedFallbackCatalog) ReconcileClassified(_ context.Context, fallbackSetID string, _ []adaptercatalog.Source) (adaptercatalog.Snapshot, error) {
	selected, err := c.Load(fallbackSetID)
	if err != nil {
		return adaptercatalog.Snapshot{}, err
	}
	selected.RejectedCount = 1
	return selected, nil
}

func assertCatalogPluginTrust(t *testing.T, catalog *adaptercatalog.Catalog, setID, adapterID string, want plugintrust.Attestation) {
	t.Helper()
	set, err := catalog.Load(setID)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range set.Entries {
		if entry.AdapterID == adapterID {
			if got := entry.EffectiveAttestation(); got != want {
				t.Fatalf("adapter %q trust = %+v, want %+v", adapterID, got, want)
			}
			return
		}
	}
	t.Fatalf("adapter %q missing from set %s", adapterID, setID)
}

func TestPluginRegistryBadDigestDoesNotChangeDesiredOrActiveGeneration(t *testing.T) {
	fixture := newControllerFixture(t, false)
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	path := buildFixtureAdapter(t, root, "registry_fixture", "1.0.0")
	binary, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	wrongSHA := strings.Repeat("0", 64)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/registry.json":
			writeRegistryFixtureWithSHA(t, w, r, "1.0.0", binary, wrongSHA)
		case "/artifact":
			_, _ = w.Write(binary)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	manager, err := pluginregistry.Open(pluginregistry.Config{Root: filepath.Join(fixture.root, "runtime", "plugin-registry"), RegistryURL: server.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	localSources := filepath.Join(root, "local-adapters")
	if err := os.Mkdir(localSources, 0700); err != nil {
		t.Fatal(err)
	}
	catalog, err := adaptercatalog.Open(filepath.Join(fixture.root, "runtime", "adapters"), []string{localSources})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	bindActiveAdapterSet(t, fixture, initial.ID)
	fixture.controller.adapterCatalog, fixture.controller.pluginRegistry = catalog, manager
	fixture.controller.installation = readyInstallation(t, fixture.root)
	hostAPI, err := httpapi.New(nil, true, false, fixture.controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	before := fixture.registry.Snapshot().ActiveGenerationID
	_, code, body := callPluginAPI(t, hostAPI, http.MethodPost, httpapi.PluginsEndpoint+"/registry_fixture/install")
	if code != http.StatusUnprocessableEntity || !strings.Contains(body, "plugin_verification_failed") {
		t.Fatalf("install bad digest = status %d, body %s", code, body)
	}
	after := fixture.registry.Snapshot().ActiveGenerationID
	if before != after {
		t.Fatalf("bad digest changed active generation from %s to %s", before, after)
	}
	if got := manager.View().Installed; len(got) != 0 {
		t.Fatalf("bad digest changed desired state: %+v", got)
	}
}

func pluginStatusHasVersion(status httpapi.PluginStatus, id, version string) bool {
	for _, item := range status.Plugins {
		if item.ID == id && item.Installed && item.InstalledVersion == version {
			return true
		}
	}
	return false
}

func pluginStatusHasAvailableVersion(status httpapi.PluginStatus, id, version string) bool {
	for _, item := range status.Plugins {
		if item.ID == id && item.AvailableVersion == version {
			return true
		}
	}
	return false
}

func pluginStatusHasInstalled(status httpapi.PluginStatus, id string) bool {
	for _, item := range status.Plugins {
		if item.ID == id && item.Installed {
			return true
		}
	}
	return false
}

func callPluginAPI(t *testing.T, handler http.Handler, method, path string) (httpapi.PluginStatus, int, string) {
	t.Helper()
	var body *strings.Reader
	if method == http.MethodGet {
		body = strings.NewReader("")
	} else {
		body = strings.NewReader("{}")
	}
	request := httptest.NewRequest(method, path, body)
	if method != http.MethodGet {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	data := response.Body.String()
	if response.Code != http.StatusOK {
		return httpapi.PluginStatus{}, response.Code, data
	}
	var status httpapi.PluginStatus
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode plugin API status: %v", err)
	}
	return status, response.Code, data
}

func writeRegistryFixture(t *testing.T, w http.ResponseWriter, r *http.Request, version string, binary []byte) {
	t.Helper()
	writeRegistryFixtureWithSHA(t, w, r, version, binary, sha256Hex(binary))
}

func writeRegistryFixtureWithSHA(t *testing.T, w http.ResponseWriter, r *http.Request, version string, binary []byte, sha string) {
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
					"filename": "integrated-recorder-adapter-registry_fixture", "size": len(binary), "sha256": sha,
				}},
			}},
		}},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(document); err != nil {
		t.Errorf("encode registry fixture: %v", err)
	}
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func assertCatalogPlugin(t *testing.T, catalog *adaptercatalog.Catalog, setID, id, version, digest string) {
	t.Helper()
	set, err := catalog.Load(setID)
	if err != nil {
		t.Fatalf("load adapter set %s: %v", setID, err)
	}
	for _, entry := range set.Entries {
		if entry.AdapterID == id {
			if entry.Version != version || entry.ArtifactSHA256 != digest {
				t.Fatalf("catalog plugin = %+v, want version=%s sha=%s", entry, version, digest)
			}
			return
		}
	}
	t.Fatalf("adapter set %s does not contain %s", setID, id)
}

func assertPluginAdapterAPI(t *testing.T, catalog *adaptercatalog.Catalog, setID, id, version string, wantPresent bool) {
	t.Helper()
	set, err := catalog.Load(setID)
	if err != nil {
		t.Fatalf("load adapter set %s for API projection: %v", setID, err)
	}
	host, err := adapterhost.DiscoverDirs(context.Background(), []string{set.Directory}, nil)
	if err != nil {
		t.Fatalf("discover immutable adapter set %s for API: %v", setID, err)
	}
	defer host.Close()
	api := productserver.New(nil, host, nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/adapters", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/adapters status=%d body=%s", response.Code, response.Body.String())
	}
	var adapters []struct {
		Descriptor *struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"descriptor"`
		Status struct {
			ID string `json:"id"`
		} `json:"status"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &adapters); err != nil {
		t.Fatalf("decode /api/adapters response: %v", err)
	}
	found := false
	for _, adapter := range adapters {
		if adapter.Status.ID == id && adapter.Descriptor != nil && adapter.Descriptor.Version == version {
			found = true
		}
	}
	if found != wantPresent {
		t.Fatalf("GET /api/adapters entries=%+v, %s/%s present=%t, want %t", adapters, id, version, found, wantPresent)
	}
}

func hasCatalogPlugin(set adaptercatalog.Snapshot, id string) bool {
	for _, entry := range set.Entries {
		if entry.AdapterID == id {
			return true
		}
	}
	return false
}
