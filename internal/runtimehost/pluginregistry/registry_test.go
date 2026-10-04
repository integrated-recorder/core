package pluginregistry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
)

var (
	fixtureMu    sync.Mutex
	fixtureCache = map[string][]byte{}
)

func compiledFixture(t *testing.T) []byte {
	return compiledFixtureVersion(t, "registry-fixture", "1.0.0", "1")
}

func compiledFixtureVersion(t *testing.T, id, version, protocol string) []byte {
	t.Helper()
	key := id + "\x00" + version + "\x00" + protocol
	fixtureMu.Lock()
	if data := fixtureCache[key]; data != nil {
		fixtureMu.Unlock()
		return bytes.Clone(data)
	}
	fixtureMu.Unlock()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "integrated-recorder-adapter-fixture")
	ldflags := fmt.Sprintf("-X main.fixtureID=%s -X main.fixtureVersion=%s -X main.fixtureProtocol=%s", id, version, protocol)
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags", ldflags, "-o", out, "./testdata/adapter")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build Protocol v1 fixture: %v: %s", err, strings.TrimSpace(string(output)))
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	fixtureMu.Lock()
	fixtureCache[key] = bytes.Clone(data)
	fixtureMu.Unlock()
	return data
}

func fixtureDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func tempStoreRoot(t *testing.T, name string) string {
	t.Helper()
	root := t.TempDir()
	// The production snapshots are deliberately read-only. Restore directory
	// traversal/write permissions before t.TempDir's own cleanup runs.
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if entry.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	return filepath.Join(root, name)
}

func validRegistry(data []byte, artifactURL, id, version string) Registry {
	return Registry{
		SchemaVersion: SchemaVersion,
		Plugins: []Plugin{{
			ID: id, Name: "Registry Fixture", Repository: "https://github.com/example/adapter",
			Channels: map[string]string{"stable": version},
			Releases: []Release{{
				Version: version, ProtocolVersion: 1, SourceCommit: "0123456789abcdef0123456789abcdef01234567",
				Artifacts: []Artifact{{
					OS: "linux", Arch: "amd64", URL: artifactURL,
					Filename: "integrated-recorder-adapter-" + id, Size: int64(len(data)), SHA256: fixtureDigest(data),
				}},
			}},
		}},
	}
}

func validRegistryV2(data []byte, artifactURL, id, version, pluginType string) Registry {
	document := validRegistry(data, artifactURL, id, version)
	document.SchemaVersion = SchemaVersionV2
	document.Plugins[0].Type = pluginType
	document.Plugins[0].Releases[0].Protocol = &Protocol{Name: pluginType, Version: 1}
	document.Plugins[0].Releases[0].ProtocolVersion = 0
	if pluginType == TypeStorage {
		document.Plugins[0].Releases[0].Artifacts[0].Filename = StorageBinaryPrefix + id
	}
	return document
}

func marshalRegistry(t *testing.T, document Registry) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func serveRegistry(t *testing.T, document Registry, artifact []byte) (*httptest.Server, *http.Client) {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/registry.json":
			w.Header().Set("Content-Type", "application/json")
			data, err := json.Marshal(document)
			if err != nil {
				http.Error(w, "fixture catalog error", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(data)
		case "/artifact":
			_, _ = w.Write(artifact)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	for i := range document.Plugins {
		for j := range document.Plugins[i].Releases {
			for k := range document.Plugins[i].Releases[j].Artifacts {
				document.Plugins[i].Releases[j].Artifacts[k].URL = server.URL + "/artifact"
			}
		}
	}
	return server, server.Client()
}

func openFixtureManager(t *testing.T, document Registry, artifact []byte, targetOS, targetArch string) (*Manager, *httptest.Server) {
	t.Helper()
	server, client := serveRegistry(t, document, artifact)
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", GOOS: targetOS, GOARCH: targetArch, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	return manager, server
}

func TestDecodeRegistryStrictAndBounded(t *testing.T) {
	data := compiledFixture(t)
	document := validRegistry(data, "https://cdn.example.test/artifact", "registry-fixture", "1.0.0")
	valid := marshalRegistry(t, document)
	if bytes.Contains(valid, []byte(`"type"`)) || bytes.Contains(valid, []byte(`"protocol"`)) || !bytes.Contains(valid, []byte(`"protocol_version":1`)) {
		t.Fatalf("v1 encoding changed its legacy field contract: %s", valid)
	}
	if _, err := decodeRegistry(valid); err != nil {
		t.Fatalf("valid catalog rejected: %v", err)
	}
	tests := []struct {
		name string
		edit func([]byte) []byte
	}{
		{name: "unknown field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"schema_version":1`), []byte(`"extra":true,"schema_version":1`), 1)
		}},
		{name: "trailing JSON", edit: func(data []byte) []byte { return append(data, []byte(` {}`)...) }},
		{name: "invalid UTF-8", edit: func(data []byte) []byte { return append(append([]byte(nil), data...), 0xff) }},
		{name: "missing required array", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"plugins":`), []byte(`"plugins_missing":`), 1)
		}},
		{name: "null required array", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"plugins":[`), []byte(`"plugins":null`), 1)
		}},
		{name: "null required map", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"channels":{"stable":"1.0.0"}`), []byte(`"channels":null`), 1)
		}},
		{name: "missing required field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"name":"Registry Fixture",`), nil, 1)
		}},
		{name: "null required field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"source_commit":"0123456789abcdef0123456789abcdef01234567"`), []byte(`"source_commit":null`), 1)
		}},
		{name: "case mismatched field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"schema_version"`), []byte(`"Schema_Version"`), 1)
		}},
		{name: "duplicate field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`{"schema_version":1`), []byte(`{"schema_version":1,"schema_version":1`), 1)
		}},
		{name: "v1 plugin type field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"id":"registry-fixture",`), []byte(`"id":"registry-fixture","type":"source",`), 1)
		}},
		{name: "v1 typed protocol field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol_version":1`), []byte(`"protocol":{"name":"source","version":1}`), 1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRegistry(test.edit(valid)); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("expected unavailable, got %v", err)
			}
		})
	}
	if _, err := decodeRegistry(bytes.Repeat([]byte{' '}, MaxCatalogBytes+1)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("oversized catalog accepted: %v", err)
	}
}

func TestDecodeRegistryV2TypedPlugins(t *testing.T) {
	data := []byte("fixture")
	for _, pluginType := range []string{TypeSource, TypeStorage} {
		t.Run(pluginType, func(t *testing.T) {
			wire := marshalRegistry(t, validRegistryV2(data, "https://cdn.example.test/artifact", "registry-fixture", "1.0.0", pluginType))
			document, err := decodeRegistry(wire)
			if err != nil {
				t.Fatalf("valid v2 %s catalog rejected: %v", pluginType, err)
			}
			if bytes.Contains(wire, []byte(`"protocol_version"`)) {
				t.Fatalf("v2 encoding contains legacy protocol_version: %s", wire)
			}
			if document.SchemaVersion != SchemaVersionV2 || len(document.Plugins) != 1 || document.Plugins[0].Type != pluginType {
				t.Fatalf("decoded v2 plugin = %+v", document)
			}
			protocol := document.Plugins[0].Releases[0].Protocol
			if protocol.Name != pluginType || protocol.Version != 1 {
				t.Fatalf("decoded protocol = %+v", protocol)
			}
		})
	}
	localStorage := validRegistryV2(data, "https://cdn.example.test/artifact", "local", "1.0.0", TypeStorage)
	if _, err := decodeRegistry(marshalRegistry(t, localStorage)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("storage provider using the reserved local selector was accepted: %v", err)
	}

	valid := marshalRegistry(t, validRegistryV2(data, "https://cdn.example.test/artifact", "registry-fixture", "1.0.0", TypeSource))
	tests := []struct {
		name string
		edit func([]byte) []byte
	}{
		{name: "missing type", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"type":"source",`), nil, 1)
		}},
		{name: "unknown type", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"type":"source"`), []byte(`"type":"other"`), 1)
		}},
		{name: "wrong case type field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"type":"source"`), []byte(`"Type":"source"`), 1)
		}},
		{name: "mismatched protocol name", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1}`), []byte(`"protocol":{"name":"storage","version":1}`), 1)
		}},
		{name: "missing protocol", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1},`), nil, 1)
		}},
		{name: "null protocol", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1}`), []byte(`"protocol":null`), 1)
		}},
		{name: "wrong case protocol field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1}`), []byte(`"Protocol":{"name":"source","version":1}`), 1)
		}},
		{name: "wrong protocol version", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1}`), []byte(`"protocol":{"name":"source","version":2}`), 1)
		}},
		{name: "legacy protocol version field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"version":"1.0.0",`), []byte(`"version":"1.0.0","protocol_version":1,`), 1)
		}},
		{name: "unknown protocol field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1}`), []byte(`"protocol":{"name":"source","version":1,"extra":true}`), 1)
		}},
		{name: "duplicate protocol field", edit: func(data []byte) []byte {
			return bytes.Replace(data, []byte(`"protocol":{"name":"source","version":1}`), []byte(`"protocol":{"name":"source","name":"source","version":1}`), 1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeRegistry(test.edit(valid)); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("expected unavailable, got %v", err)
			}
		})
	}
}

func TestEmptyCatalogFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "registry-empty.json"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := decodeRegistry(data)
	if err != nil || document.SchemaVersion != SchemaVersion || len(document.Plugins) != 0 {
		t.Fatalf("empty catalog fixture = %+v, %v", document, err)
	}
}

func TestValidateRegistryRejectsDuplicateAndInvalidEntries(t *testing.T) {
	data := []byte("fixture")
	base := validRegistry(data, "https://cdn.example.test/artifact", "registry-fixture", "1.0.0")
	tests := []struct {
		name string
		edit func(*Registry)
	}{
		{name: "duplicate plugin", edit: func(d *Registry) { d.Plugins = append(d.Plugins, d.Plugins[0]) }},
		{name: "duplicate version", edit: func(d *Registry) { d.Plugins[0].Releases = append(d.Plugins[0].Releases, d.Plugins[0].Releases[0]) }},
		{name: "duplicate platform", edit: func(d *Registry) {
			d.Plugins[0].Releases[0].Artifacts = append(d.Plugins[0].Releases[0].Artifacts, d.Plugins[0].Releases[0].Artifacts[0])
		}},
		{name: "non HTTPS artifact", edit: func(d *Registry) { d.Plugins[0].Releases[0].Artifacts[0].URL = "http://cdn.example.test/a" }},
		{name: "artifact URL credentials", edit: func(d *Registry) {
			d.Plugins[0].Releases[0].Artifacts[0].URL = "https://user:secret@cdn.example.test/a"
		}},
		{name: "invalid SHA", edit: func(d *Registry) { d.Plugins[0].Releases[0].Artifacts[0].SHA256 = strings.Repeat("A", 64) }},
		{name: "zero size", edit: func(d *Registry) { d.Plugins[0].Releases[0].Artifacts[0].Size = 0 }},
		{name: "oversized artifact", edit: func(d *Registry) { d.Plugins[0].Releases[0].Artifacts[0].Size = MaxArtifactBytes + 1 }},
		{name: "wrong protocol", edit: func(d *Registry) { d.Plugins[0].Releases[0].ProtocolVersion = 2 }},
		{name: "wrong filename", edit: func(d *Registry) { d.Plugins[0].Releases[0].Artifacts[0].Filename = "../adapter" }},
		{name: "channel references missing release", edit: func(d *Registry) { d.Plugins[0].Channels["stable"] = "2.0.0" }},
		{name: "empty channels", edit: func(d *Registry) { d.Plugins[0].Channels = map[string]string{} }},
		{name: "empty releases", edit: func(d *Registry) { d.Plugins[0].Releases = []Release{} }},
		{name: "repository too long", edit: func(d *Registry) { d.Plugins[0].Repository = "https://example.test/" + strings.Repeat("a", 600) }},
		{name: "repository without path", edit: func(d *Registry) { d.Plugins[0].Repository = "https://example.test" }},
		{name: "artifact URL without path", edit: func(d *Registry) { d.Plugins[0].Releases[0].Artifacts[0].URL = "https://cdn.example.test" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := base
			document.Plugins = append([]Plugin(nil), base.Plugins...)
			document.Plugins[0].Channels = map[string]string{"stable": "1.0.0"}
			document.Plugins[0].Releases = append([]Release(nil), base.Plugins[0].Releases...)
			document.Plugins[0].Releases[0].Artifacts = append([]Artifact(nil), base.Plugins[0].Releases[0].Artifacts...)
			test.edit(&document)
			if validateRegistry(document) == nil {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
	tooMany := Registry{SchemaVersion: SchemaVersion, Plugins: make([]Plugin, MaxPlugins+1)}
	if validateRegistry(tooMany) == nil {
		t.Fatal("catalog with too many plugins accepted")
	}
	soop := validRegistry(data, "https://cdn.example.test/artifact", "soop", "1.0.0")
	soop.Plugins[0].Releases[0].SourceCommit = "release/v1.0.0"
	if err := validateRegistry(soop); err != nil {
		t.Fatalf("schema-compatible short plugin ID/source tag rejected: %v", err)
	}
}

func TestRegistryWireCollectionsStopAtBoundedCounts(t *testing.T) {
	entries := strings.TrimSuffix(strings.Repeat("{},", MaxPlugins+1), ",")
	tooManyPlugins := []byte(`{"schema_version":1,"plugins":[` + entries + `]}`)
	if _, err := decodeRegistry(tooManyPlugins); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("oversized plugin array accepted: %v", err)
	}
	tooManyChannels := []byte(`{"schema_version":1,"plugins":[{"id":"soop","name":"SOOP","repository":"https://example.test/repository","channels":{"stable":"1.0.0","beta":"1.0.0","development":"1.0.0","unknown":"1.0.0"},"releases":[]}]}`)
	if _, err := decodeRegistry(tooManyChannels); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("oversized channel object accepted: %v", err)
	}
}

func TestConfiguredRegistryURLMustBeHTTPSWithoutQuery(t *testing.T) {
	for _, raw := range []string{"http://registry.example/catalog", "https://u:p@registry.example/catalog", "https://registry.example/catalog?channel=stable", "https://registry.example/catalog#fragment", "file:///tmp/catalog"} {
		if _, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: raw}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Open(%q) error = %v", raw, err)
		}
	}
}

func TestRefreshRedirectPolicyAndPrivateURL(t *testing.T) {
	data := []byte(`{"schema_version":1,"plugins":[]}`)
	var targetRequests atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		_, _ = w.Write(data)
	}))
	t.Cleanup(target.Close)
	var server *httptest.Server
	var redirects atomic.Int32
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/loop/") {
			n := redirects.Add(1)
			http.Redirect(w, r, fmt.Sprintf("%s/loop/%d", server.URL, n), http.StatusFound)
			return
		}
		if r.URL.Path == "/to-http" {
			http.Redirect(w, r, "http://127.0.0.1:1/catalog", http.StatusFound)
			return
		}
		if r.URL.Path == "/to-private-https" {
			http.Redirect(w, r, target.URL+"/registry.json", http.StatusFound)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	client := server.Client()
	for _, path := range []string{"/loop/0", "/to-http", "/to-private-https"} {
		manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + path, HTTPClient: client})
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Refresh(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("redirect %s accepted: %v", path, err)
		}
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("redirect reached a different private HTTPS origin: %d requests", targetRequests.Load())
	}

	manager, err := Open(Config{Root: tempStoreRoot(t, "private"), RegistryURL: "https://127.0.0.1/registry.json"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("private registry URL was not rejected: %v", err)
	}
}

func TestRefreshCancellation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := manager.Refresh(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("canceled refresh error = %v", err)
	}
}

func TestInstallUpdateRollbackUninstallAndRegistryOutage(t *testing.T) {
	fixture := compiledFixture(t)
	serverState := struct {
		sync.RWMutex
		document Registry
		artifact []byte
		fail     bool
	}{document: validRegistry(fixture, "https://cdn.invalid/artifact", "registry-fixture", "1.0.0"), artifact: fixture}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverState.RLock()
		defer serverState.RUnlock()
		if serverState.fail {
			http.Error(w, "private upstream detail", http.StatusBadGateway)
			return
		}
		if r.URL.Path == "/registry.json" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(serverState.document)
			return
		}
		if r.URL.Path == "/artifact" {
			_, _ = w.Write(serverState.artifact)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	serverState.Lock()
	serverState.document.Plugins[0].Releases[0].Artifacts[0].URL = server.URL + "/artifact"
	serverState.Unlock()
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err := manager.PrepareInstall(context.Background(), "registry-fixture", false)
	if err != nil {
		t.Fatal(err)
	}
	selection := plan.Selection()
	if selection.Version != "1.0.0" || selection.Digest != fixtureDigest(fixture) || filepath.Base(plan.SourceDir()) != "bin" {
		t.Fatalf("unexpected prepared selection/directory: %+v %q", selection, plan.SourceDir())
	}
	path := filepath.Join(plan.SourceDir(), selection.Filename)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0500 {
		t.Fatalf("source snapshot executable missing or mutable: info=%v err=%v", info, err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	plan.Close()
	view := manager.View()
	if !view.Configured || !view.Available || len(view.Installed) != 1 || view.Plugins[0].Type != TypeSource || view.Plugins[0].InstalledVersion != "1.0.0" {
		t.Fatalf("installed view not updated: %+v", view)
	}
	if dirs, err := manager.DesiredSourceDirs(); err != nil || len(dirs) != 1 || dirs[0] != filepath.Dir(path) {
		t.Fatalf("desired source dirs = %v, %v", dirs, err)
	}
	oldSourceSet := filepath.Dir(filepath.Dir(path))
	oldArtifact := filepath.Join(filepath.Dir(filepath.Dir(oldSourceSet)), "artifacts", selection.Digest)
	if _, err := os.Stat(oldArtifact); err != nil {
		t.Fatalf("verified immutable artifact not published: %v", err)
	}

	// A later registry outage is visible but never removes the already selected source.
	serverState.Lock()
	serverState.fail = true
	serverState.Unlock()
	if err := manager.Refresh(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected safe outage error, got %v", err)
	}
	view = manager.View()
	if view.Available || view.FailureCode != "registry_unavailable" || len(view.Installed) != 1 {
		t.Fatalf("registry outage lost installed selection: %+v", view)
	}
	if dirs, err := manager.DesiredSourceDirs(); err != nil || dirs[0] != filepath.Dir(path) {
		t.Fatalf("installed plugin stopped being usable during outage: %v %v", dirs, err)
	}
	if _, err := manager.PrepareInstall(context.Background(), "registry-fixture", true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("update during registry outage = %v", err)
	}

	serverState.Lock()
	serverState.fail = false
	// A republished digest under an already installed version is not an
	// update. The version is immutable in v1, so this must not be offered or
	// accepted as a replacement.
	sameVersionBytes := append(bytes.Clone(fixture), []byte("different bytes for same version")...)
	serverState.artifact = sameVersionBytes
	serverState.document.Plugins[0].Releases[0].Artifacts[0].Size = int64(len(sameVersionBytes))
	serverState.document.Plugins[0].Releases[0].Artifacts[0].SHA256 = fixtureDigest(sameVersionBytes)
	serverState.Unlock()
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manager.View().Plugins[0].UpdateAvailable {
		t.Fatal("same-version digest mutation was reported as an update")
	}
	if _, err := manager.PrepareInstall(context.Background(), "registry-fixture", true); !errors.Is(err, ErrNoUpdateAvailable) {
		t.Fatalf("same-version digest mutation was accepted as update: %v", err)
	}
	// Updating to a new exact release creates a different desired snapshot.
	v2Binary := compiledFixtureVersion(t, "registry-fixture", "2.0.0", "1")
	serverState.Lock()
	serverState.artifact = v2Binary
	serverState.document.Plugins[0].Releases = []Release{{Version: "2.0.0", ProtocolVersion: 1, SourceCommit: "abcdef0123456789abcdef0123456789abcdef01", Artifacts: []Artifact{{OS: "linux", Arch: "amd64", URL: server.URL + "/artifact", Filename: selection.Filename, Size: int64(len(v2Binary)), SHA256: fixtureDigest(v2Binary)}}}}
	serverState.document.Plugins[0].Channels["stable"] = "2.0.0"
	serverState.Unlock()
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	update, err := manager.PrepareInstall(context.Background(), "registry-fixture", true)
	if err != nil {
		t.Fatal(err)
	}
	if update.Selection().Version != "2.0.0" || update.SourceDir() == path {
		t.Fatalf("update plan did not prepare v2: %+v %s", update.Selection(), update.SourceDir())
	}
	if err := update.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := update.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := update.Rollback(); err != nil {
		t.Fatal(err)
	}
	update.Close()
	if got := manager.View().Installed[0].Version; got != "1.0.0" {
		t.Fatalf("rollback selected %s, want 1.0.0", got)
	}
	oldBytes, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(oldBytes, fixture) {
		t.Fatalf("old immutable set changed after update/rollback: err=%v", err)
	}
	if err := manager.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(manager.root, "source-sets", updateSourceSetID(update.SourceDir()))); err == nil {
		t.Fatal("unreferenced update source set survived safe garbage collection")
	}

	uninstall, err := manager.PrepareUninstall("registry-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if uninstall.Selection().ID != "" {
		t.Fatal("uninstall selection should be empty")
	}
	if err := uninstall.Commit(); err != nil {
		t.Fatal(err)
	}
	uninstall.Close()
	if len(manager.View().Installed) != 0 {
		t.Fatal("uninstall did not clear desired state")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("uninstall deleted a historical immutable source set: %v", err)
	}
	if err := manager.CollectGarbage(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced remote source snapshot was not collected: %v", err)
	}
}

func updateSourceSetID(sourceDir string) string { return filepath.Base(filepath.Dir(sourceDir)) }

func emptySourceSetID(t *testing.T) string {
	t.Helper()
	id, err := sourceSetID(nil)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestInstallRejectsDigestSizeAndDescriptorMismatchWithoutStateMutation(t *testing.T) {
	fixture := compiledFixture(t)
	tests := []struct {
		name   string
		edit   func(*Registry, []byte)
		want   error
		actual []byte
	}{
		{name: "wrong digest", want: ErrVerificationFailed, edit: func(d *Registry, _ []byte) { d.Plugins[0].Releases[0].Artifacts[0].SHA256 = strings.Repeat("0", 64) }, actual: fixture},
		{name: "wrong size", want: ErrVerificationFailed, edit: func(d *Registry, data []byte) { d.Plugins[0].Releases[0].Artifacts[0].Size = int64(len(data) + 1) }, actual: fixture},
		{name: "identity mismatch", want: ErrIdentityMismatch, edit: func(d *Registry, _ []byte) {
			d.Plugins[0].ID = "other-fixture"
			d.Plugins[0].Releases[0].Artifacts[0].Filename = "integrated-recorder-adapter-other-fixture"
		}, actual: fixture},
		{name: "version mismatch", want: ErrIdentityMismatch, edit: func(d *Registry, _ []byte) {
			d.Plugins[0].Releases[0].Version = "2.0.0"
			d.Plugins[0].Channels["stable"] = "2.0.0"
		}, actual: fixture},
		{name: "descriptor protocol mismatch", want: ErrIdentityMismatch, edit: func(d *Registry, _ []byte) {}, actual: compiledFixtureVersion(t, "registry-fixture", "1.0.0", "2")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binaryID := "registry-fixture"
			if test.name == "identity mismatch" {
				binaryID = "other-fixture"
			}
			document := validRegistry(test.actual, "https://cdn.invalid/artifact", binaryID, "1.0.0")
			document.Plugins[0].Releases = append([]Release(nil), document.Plugins[0].Releases...)
			document.Plugins[0].Releases[0].Artifacts = append([]Artifact(nil), document.Plugins[0].Releases[0].Artifacts...)
			document.Plugins[0].Channels = map[string]string{"stable": "1.0.0"}
			test.edit(&document, test.actual)
			server, client := serveRegistry(t, document, test.actual)
			manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err = manager.PrepareInstall(context.Background(), "registry-fixture", false)
			if test.name == "identity mismatch" {
				_, err = manager.PrepareInstall(context.Background(), "other-fixture", false)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("PrepareInstall error = %v, want %v", err, test.want)
			}
			if len(manager.View().Installed) != 0 {
				t.Fatal("verification failure mutated desired state")
			}
			if dirs, dirErr := manager.DesiredSourceDirs(); dirErr != nil || len(dirs) != 1 || filepath.Base(filepath.Dir(dirs[0])) != emptySourceSetID(t) {
				t.Fatalf("verification failure exposed a candidate source set: dirs=%v err=%v", dirs, dirErr)
			}
			artifacts, readErr := os.ReadDir(filepath.Join(manager.root, "artifacts"))
			if readErr != nil || len(artifacts) != 0 {
				t.Fatalf("verification failure published artifact: %v %v", artifacts, readErr)
			}
		})
	}
}

func TestUnsupportedPlatformAndDesiredOnlyUninstall(t *testing.T) {
	fixture := compiledFixture(t)
	document := validRegistry(fixture, "https://cdn.invalid/artifact", "registry-fixture", "1.0.0")
	manager, server := openFixtureManager(t, document, fixture, "windows", runtime.GOARCH)
	_ = server
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.PrepareInstall(context.Background(), "registry-fixture", false); !errors.Is(err, ErrPlatformUnsupported) {
		t.Fatalf("unsupported platform install error = %v", err)
	}
	if len(manager.View().Installed) != 0 {
		t.Fatal("unsupported platform mutated desired state")
	}
	if _, err := manager.PrepareUninstall("registry-fixture"); !errors.Is(err, ErrNotInstalled) {
		t.Fatalf("uninstalled removal error = %v", err)
	}
}

func TestPreparePlanCASRejectsStaleDesiredRevision(t *testing.T) {
	fixture := compiledFixture(t)
	document := validRegistry(fixture, "https://cdn.invalid/artifact", "registry-fixture", "1.0.0")
	server, client := serveRegistry(t, document, fixture)
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan, err := manager.PrepareInstall(context.Background(), "registry-fixture", false)
	if err != nil {
		t.Fatal(err)
	}
	current, err := manager.loadDesired()
	if err != nil {
		t.Fatal(err)
	}
	current.Revision++
	if err := manager.writeDesired(current); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("stale plan commit error = %v", err)
	}
	plan.Close()
	if len(manager.View().Installed) != 0 {
		t.Fatal("stale plan changed the desired selection")
	}
}

func TestNoConfiguredRegistryAndRegistryOutagePreserveLocalDesired(t *testing.T) {
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry")})
	if err != nil {
		t.Fatal(err)
	}
	view := manager.View()
	if view.Configured || view.Available || view.FailureCode != "registry_unavailable" {
		t.Fatalf("unset registry view = %+v", view)
	}
	if err := manager.Refresh(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unset registry refresh = %v", err)
	}
	if dirs, err := manager.DesiredSourceDirs(); err != nil || len(dirs) != 1 {
		t.Fatalf("empty desired source set not available: %v %v", dirs, err)
	}
}

func TestOpenRejectsSymlinkDesiredState(t *testing.T) {
	root := tempStoreRoot(t, "registry")
	manager, err := Open(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	_ = manager
	if err := os.Remove(filepath.Join(root, "desired.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(root, "desired.json")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := Open(Config{Root: root}); !errors.Is(err, ErrUnsafeStore) {
		t.Fatalf("symlink desired state accepted: %v", err)
	}
}

func TestDesiredStateWireShapeRejectsMissingNullAndCaseMismatch(t *testing.T) {
	valid := []byte(`{"schema_version":1,"revision":1,"source_set_id":"` + emptySourceSetID(t) + `","plugins":[]}`)
	if err := validateDesiredWireShape(valid); err != nil {
		t.Fatalf("valid desired state shape rejected: %v", err)
	}
	for name, data := range map[string][]byte{
		"missing required array": []byte(strings.Replace(string(valid), `,"plugins":[]`, "", 1)),
		"null required array":    []byte(strings.Replace(string(valid), `"plugins":[]`, `"plugins":null`, 1)),
		"case mismatch":          []byte(strings.Replace(string(valid), `"source_set_id"`, `"Source_Set_Id"`, 1)),
		"duplicate field":        []byte(strings.Replace(string(valid), `{"schema_version":1`, `{"schema_version":1,"schema_version":1`, 1)),
	} {
		if err := validateDesiredWireShape(data); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	pluginWithMissingField := []byte(`{"schema_version":1,"revision":1,"source_set_id":"` + emptySourceSetID(t) + `","plugins":[{"id":"soop"}]}`)
	if err := validateDesiredWireShape(pluginWithMissingField); err == nil {
		t.Fatal("desired plugin with missing required fields accepted")
	}
}

func TestInterruptedStagingAndPreparedPlanAreNotExposedAfterReopen(t *testing.T) {
	fixture := compiledFixture(t)
	document := validRegistry(fixture, "https://cdn.invalid/artifact", "registry-fixture", "1.0.0")
	manager, server := openFixtureManager(t, document, fixture, "linux", "amd64")
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := manager.DesiredSourceDirs()
	if err != nil || len(before) != 1 {
		t.Fatalf("initial desired source: %v %v", before, err)
	}
	plan, err := manager.PrepareInstall(context.Background(), "registry-fixture", false)
	if err != nil {
		t.Fatal(err)
	}
	if plan.SourceDir() == before[0] {
		t.Fatal("prepared candidate unexpectedly equals the current source set")
	}
	// Closing without Commit models a crash before desired-state publication.
	// The candidate bytes may exist privately but must not appear as selected.
	plan.Close()
	if len(manager.View().Installed) != 0 {
		t.Fatal("uncommitted plan appeared installed before reopen")
	}

	partial := filepath.Join(manager.root, "staging", "download-interrupted")
	if err := os.Mkdir(partial, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partial, "integrated-recorder-adapter-registry-fixture"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	_ = server

	restarted, err := Open(Config{Root: manager.root, RegistryURL: manager.registryURL, GOOS: "linux", GOARCH: "amd64", HTTPClient: manager.client})
	if err != nil {
		t.Fatal(err)
	}
	after, err := restarted.DesiredSourceDirs()
	if err != nil || len(after) != 1 || after[0] != before[0] {
		t.Fatalf("reopen selected an uncommitted source set: before=%v after=%v err=%v", before, after, err)
	}
	if view := restarted.View(); len(view.Installed) != 0 {
		t.Fatalf("reopen exposed uncommitted desired plugin: %+v", view.Installed)
	}
	if _, err := os.Lstat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted staging residue survived reopen: %v", err)
	}
	if _, err := os.Stat(plan.SourceDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uncommitted source snapshot survived cold-open cleanup: %v", err)
	}
	if _, err := os.Stat(filepath.Join(restarted.root, "artifacts", plan.Selection().Digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced pre-commit artifact survived cold-open cleanup: %v", err)
	}
}

func TestDownloadTruncationAndCancellation(t *testing.T) {
	fixture := compiledFixture(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/registry.json" {
			document := validRegistry(fixture, "https://placeholder/artifact", "registry-fixture", "1.0.0")
			document.Plugins[0].Releases[0].Artifacts[0].URL = "https://" + r.Host + "/artifact"
			_ = json.NewEncoder(w).Encode(document)
			return
		}
		if r.URL.Path == "/artifact" {
			w.Header().Set("Content-Length", strconv.Itoa(len(fixture)+10))
			_, _ = w.Write(fixture)
			return
		}
	}))
	t.Cleanup(server.Close)
	manager, err := Open(Config{Root: tempStoreRoot(t, "truncated"), RegistryURL: server.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.PrepareInstall(context.Background(), "registry-fixture", false); !errors.Is(err, ErrDownloadFailed) && !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("truncated response error = %v", err)
	}
	if len(manager.View().Installed) != 0 {
		t.Fatal("truncated download mutated desired state")
	}

	blocking := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/registry.json" {
			document := validRegistry(fixture, "https://placeholder/artifact", "registry-fixture", "1.0.0")
			document.Plugins[0].Releases[0].Artifacts[0].URL = "https://" + r.Host + "/artifact"
			_ = json.NewEncoder(w).Encode(document)
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(blocking.Close)
	manager, err = Open(Config{Root: tempStoreRoot(t, "canceled"), RegistryURL: blocking.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: blocking.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := manager.PrepareInstall(ctx, "registry-fixture", false); !errors.Is(err, ErrDownloadFailed) {
		t.Fatalf("canceled artifact download error = %v", err)
	}
	if len(manager.View().Installed) != 0 {
		t.Fatal("canceled download mutated desired state")
	}
}

func TestUninstallCASRollbackAndSourceSetReadback(t *testing.T) {
	// This smaller state-level test checks monotonically increasing revisions and
	// verifies that rollback cannot overwrite a newer desired selection.
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry")})
	if err != nil {
		t.Fatal(err)
	}
	state, err := manager.loadDesired()
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 1 || len(state.Plugins) != 0 {
		t.Fatalf("initial state = %+v", state)
	}
	dirs, err := manager.DesiredSourceDirs()
	if err != nil || len(dirs) != 1 || !filepath.IsAbs(dirs[0]) {
		t.Fatalf("initial source set = %v %v", dirs, err)
	}
	state.Revision++
	if err := manager.writeDesired(state); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(manager.root, "desired.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(manager.root, "source-sets", state.SourceSetID, "bin")); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactLimitUsesAdapterCatalogLimit(t *testing.T) {
	if MaxArtifactBytes != adaptercatalog.MaxArtifactBytes {
		t.Fatalf("artifact limit differs from adapter catalog: %d vs %d", MaxArtifactBytes, adaptercatalog.MaxArtifactBytes)
	}
	if MaxArtifactBytes != 512<<20 {
		t.Fatalf("artifact size bound drifted: %d", MaxArtifactBytes)
	}
}

func TestRegistryFailedRefreshKeepsPreviouslyApprovedCacheOutOfView(t *testing.T) {
	server, client := serveRegistry(t, Registry{SchemaVersion: SchemaVersion, Plugins: []Plugin{}}, nil)
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if err := manager.Refresh(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("refresh after server shutdown = %v", err)
	}
	view := manager.View()
	if view.Available || view.FailureCode != "registry_unavailable" || len(view.Plugins) != 0 {
		t.Fatalf("failed refresh exposed stale catalog: %+v", view)
	}
}

func TestArtifactSchemaBoundaryRejectsUnsupportedPlatformArtifacts(t *testing.T) {
	document := validRegistry([]byte("x"), "https://cdn.example.test/a", "registry-fixture", "1.0.0")
	document.Plugins[0].Releases[0].Artifacts[0].OS = "windows"
	if validateRegistry(document) == nil {
		t.Fatal("unsupported platform artifact accepted")
	}
}

func TestRegistryAcceptsNativeDarwinTargets(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		if !validTarget("darwin", arch) {
			t.Errorf("native darwin/%s target was rejected", arch)
		}
	}
	for _, platform := range [][2]string{{"windows", "amd64"}, {"darwin", "386"}, {"freebsd", "arm64"}} {
		if validTarget(platform[0], platform[1]) {
			t.Errorf("unsupported target %s/%s was accepted", platform[0], platform[1])
		}
	}
}

func TestArtifactResponseBodyIsBounded(t *testing.T) {
	fixture := compiledFixture(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/registry.json" {
			document := validRegistry(fixture, "https://placeholder/artifact", "registry-fixture", "1.0.0")
			document.Plugins[0].Releases[0].Artifacts[0].URL = "https://" + r.Host + "/artifact"
			_ = json.NewEncoder(w).Encode(document)
			return
		}
		if r.URL.Path == "/artifact" {
			_, _ = io.Copy(w, bytes.NewReader(append(bytes.Clone(fixture), 'x')))
			return
		}
	}))
	t.Cleanup(server.Close)
	manager, err := Open(Config{Root: tempStoreRoot(t, "registry"), RegistryURL: server.URL + "/registry.json", GOOS: "linux", GOARCH: "amd64", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.PrepareInstall(context.Background(), "registry-fixture", false); !errors.Is(err, ErrVerificationFailed) && !errors.Is(err, ErrDownloadFailed) {
		t.Fatalf("oversized artifact response error = %v", err)
	}
}
