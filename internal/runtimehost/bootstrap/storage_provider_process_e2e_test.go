package bootstrap

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dltkddnr04/integrated-recorder/internal/domain"
	"github.com/dltkddnr04/integrated-recorder/internal/integrity"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/generation"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/httpapi"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/storagecatalog"
	"github.com/dltkddnr04/integrated-recorder/internal/storage"
)

const storageProcessE2EProviderID = "fixture-storage"

// TestProductionStorageProviderRegistryAcceptanceE2E drives the real Host API,
// Control/Engine binaries, and an external provider process from a typed local
// HTTPS registry. Only the registry certificate is injected by the
// runtime_e2e-only Host build; the production registry verifier still checks
// platform, exact size/hash, protocol, and descriptor identity.
func TestProductionStorageProviderRegistryAcceptanceE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires building and running production Runtime Host, Control Plane, Recorder Engine, and storage provider processes")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-tree assertions currently use the Unix ps interface")
	}

	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeUpdateArtifacts(t, fixture.server.URL)
	providerV1 := buildStorageProviderVariant(t, "1.0.0", "legacy", false)
	providerV2 := buildStorageProviderVariant(t, "2.0.0", "legacy", false)
	bytesV1 := readStorageProcessE2EBinary(t, providerV1)
	bytesV2 := readStorageProcessE2EBinary(t, providerV2)
	if string(bytesV1) == string(bytesV2) {
		t.Fatal("storage provider fixtures must have distinct immutable bytes")
	}

	var registryMu sync.RWMutex
	currentVersion := "1.0.0"
	providerBytes := map[string][]byte{"1.0.0": bytesV1, "2.0.0": bytesV2}
	registryServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/registry.json":
			registryMu.RLock()
			version := currentVersion
			binary := providerBytes[version]
			registryMu.RUnlock()
			digest := sha256.Sum256(binary)
			w.Header().Set("Content-Type", "application/json")
			document := storageProcessE2ERegistryDocument(r.Host, version, runtime.GOOS, runtime.GOARCH, len(binary), hex.EncodeToString(digest[:]))
			if err := json.NewEncoder(w).Encode(document); err != nil {
				t.Errorf("encode process E2E storage registry: %v", err)
			}
		case "/artifact/1.0.0", "/artifact/2.0.0":
			version := strings.TrimPrefix(r.URL.Path, "/artifact/")
			registryMu.RLock()
			binary, ok := providerBytes[version]
			registryMu.RUnlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(binary)))
			_, _ = w.Write(binary)
		default:
			http.NotFound(w, r)
		}
	}))
	defer registryServer.Close()
	caPath := writeRuntimeE2ERegistryCA(t, registryServer)

	root := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(root, "data")
	providerRoot := filepath.Join(root, "provider-objects")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	listenAddr := reserveRuntimeAddress(t)
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 90 * time.Second}
	streamR, streamS := "storage-provider-r", "storage-provider-s"
	fixture.reset(streamR, "Recording R", "Storage fixture R", streamR+"-session")
	fixture.reset(streamS, "Recording S", "Storage fixture S", streamS+"-session")

	startHost := func() *runtimeHostProcess {
		command := exec.Command(artifacts.hostA)
		command.Env = minimalRuntimeE2EEnv([]string{
			"DATA_DIR=" + dataDir,
			"ADDR=" + listenAddr,
			"AUTH_DISABLED=1",
			"ADAPTER_DIR=" + artifacts.adapterDir,
			"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
			"IR_PLUGIN_REGISTRY_URL=" + registryServer.URL + "/registry.json",
			"IR_RUNTIME_E2E_PLUGIN_REGISTRY_CA_FILE=" + caPath,
		})
		process := &runtimeHostProcess{command: command, done: make(chan struct{})}
		command.Stdout, command.Stderr = &process.output, &process.output
		if err := command.Start(); err != nil {
			t.Fatalf("start production Runtime Host: %v", err)
		}
		go func() {
			process.err = command.Wait()
			close(process.done)
		}()
		return process
	}

	process := startHost()
	controlBinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineBinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	var providerV1Path, providerV2Path, adapterSetBinary string
	t.Cleanup(func() {
		stopRuntimeHostProcess(process)
		for _, executable := range []string{artifacts.hostA, controlBinary, engineBinary, providerV1Path, providerV2Path, adapterSetBinary} {
			if executable == "" {
				continue
			}
			if err := waitProcessAbsent(t, executable, 20*time.Second); err != nil {
				t.Errorf("process E2E cleanup left a child alive for %s: %v", filepath.Base(executable), err)
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if status.ActiveControl == nil || status.DefaultEngine == nil {
		t.Fatalf("production Host did not start Control and Engine: %+v", status)
	}
	hostPID := process.command.Process.Pid
	controlPID := waitDirectChildForBinary(t, hostPID, controlBinary, 20*time.Second)
	enginePID := waitDirectChildForBinary(t, hostPID, engineBinary, 20*time.Second)
	if controlPID == enginePID || controlPID == hostPID || enginePID == hostPID {
		t.Fatalf("Host/Control/Engine are not separate product processes: host=%d control=%d engine=%d", hostPID, controlPID, enginePID)
	}
	initialRegistry := readRuntimeGenerationSnapshot(t, dataDir)
	initialGenerationID := initialRegistry.ActiveGenerationID
	initialGeneration := initialRegistry.Generations[initialGenerationID]
	if initialGenerationID == "" || initialGeneration.StorageProviderSetID == "" {
		t.Fatalf("fresh product generation should pin its bundled local storage provider: %+v", initialRegistry)
	}
	storageCatalogRoot := filepath.Join(dataDir, "runtime", "storage-providers")
	catalog, err := storagecatalog.Open(storageCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	localSet, err := catalog.LoadSet(initialGeneration.StorageProviderSetID)
	if err != nil || localSet.Artifact.ID != "local" {
		t.Fatalf("fresh product generation does not resolve to bundled storage.local: set=%+v err=%v", localSet, err)
	}

	// Registry refresh/install/configure/probe/activate all use the production
	// Host's stable public listener, not an in-process controller.
	plugins, code := getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodPost, httpapi.PluginsEndpoint+"/refresh", map[string]any{})
	if code != http.StatusOK || !pluginStatusHasStorageRelease(plugins, "1.0.0", "") {
		t.Fatalf("refresh storage registry v1 status=%d plugins=%+v", code, plugins)
	}
	plugins, code = getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodPost, httpapi.PluginsEndpoint+"/"+storageProcessE2EProviderID+"/install", map[string]any{})
	if code != http.StatusOK || !pluginStatusHasStorageRelease(plugins, "1.0.0", "1.0.0") {
		t.Fatalf("install provider v1 status=%d plugins=%+v", code, plugins)
	}
	storageStatus := runtimeStorageStatus(t, client, baseURL)
	if storageStatus.Primary.Kind != "plugin" || storageStatus.Primary.ProviderID != "local" || storageStatus.Primary.Version != localSet.Artifact.Version || readRuntimeGenerationSnapshot(t, dataDir).ActiveGenerationID != initialGenerationID {
		t.Fatalf("verified install activated storage without explicit configuration: primary=%+v registry=%+v", storageStatus.Primary, readRuntimeGenerationSnapshot(t, dataDir))
	}
	configureRuntimeStorageProvider(t, client, baseURL, providerRoot)
	if _, code := getRuntimeJSON[httpapi.StorageProbeResult](t, client, baseURL, http.MethodPost, httpapi.StorageProvidersPrefix+storageProcessE2EProviderID+"/probe", map[string]any{}); code != http.StatusOK {
		t.Fatalf("probe provider v1 returned %d", code)
	}
	v1Status, code := getRuntimeJSON[httpapi.StorageProviderStatus](t, client, baseURL, http.MethodPost, httpapi.StorageProvidersPrefix+storageProcessE2EProviderID+"/activate", map[string]any{})
	if code != http.StatusOK || v1Status.Primary.Kind != "plugin" || v1Status.Primary.Version != "1.0.0" {
		t.Fatalf("activate provider v1 status=%d primary=%+v", code, v1Status.Primary)
	}
	activeV1Snapshot := readRuntimeGenerationSnapshot(t, dataDir)
	v1GenerationID := activeV1Snapshot.ActiveGenerationID
	v1Generation := activeV1Snapshot.Generations[v1GenerationID]
	if v1GenerationID == initialGenerationID || v1Generation.StorageProviderSetID == "" {
		t.Fatalf("v1 activation did not create a provider-pinned generation: %+v", v1Generation)
	}
	if process.command.Process.Pid != hostPID || processForBinary(artifacts.hostA) != hostPID {
		t.Fatalf("Host process changed during provider activation: original=%d current=%d", hostPID, processForBinary(artifacts.hostA))
	}
	controlV1PID := waitDirectChildForBinaryDifferent(t, hostPID, controlBinary, controlPID, 25*time.Second)
	engineV1PID := waitDirectChildForBinaryDifferent(t, hostPID, engineBinary, enginePID, 25*time.Second)
	if controlV1PID == engineV1PID {
		t.Fatalf("storage-pinned generation reused a process boundary: control=%d engine=%d", controlV1PID, engineV1PID)
	}
	catalog, err = storagecatalog.Open(storageCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	v1Set, err := catalog.LoadSet(v1Generation.StorageProviderSetID)
	if err != nil || v1Set.Artifact.Version != "1.0.0" {
		t.Fatalf("v1 generation does not resolve to immutable v1 provider set: set=%+v err=%v", v1Set, err)
	}
	providerV1Path, err = catalog.ArtifactPath(v1Set.Artifact.Digest)
	if err != nil {
		t.Fatal(err)
	}
	adapterSnapshot := readRuntimeGenerationSnapshot(t, dataDir)
	adapterSetID := adapterSnapshot.Generations[v1GenerationID].AdapterSetID
	if adapterSetID != "" {
		adapterSetBinary = filepath.Join(dataDir, "runtime", "adapters", "sets", adapterSetID, "bin", "integrated-recorder-adapter-"+runtimeE2EAdapterID)
	}
	// The initial local generation may leave an empty layout directory behind;
	// assertions below verify that no canonical Recording is written there.

	recordingR := createRuntimeRecording(t, client, baseURL, "Storage provider v1 pinned R", map[string]string{"source_url": fixture.server.URL + "/source/" + streamR})
	leaseR := waitRecordingLease(t, dataDir, recordingR.ID, 25*time.Second)
	if leaseR.EngineGeneration != v1GenerationID {
		t.Fatalf("Recording R did not pin v1 Engine generation: lease=%+v expected=%s", leaseR, v1GenerationID)
	}
	fixture.advance(streamR, 5)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 5, 30*time.Second)
	if current := getRecording(t, client, baseURL, recordingR.ID); current.ID != recordingR.ID || current.State != domain.StateRecording {
		t.Fatalf("Recording R identity/state changed while v1 was active: %+v", current)
	}
	verifyStorageProcessE2EObjects(t, providerRoot, streamR, getRecording(t, client, baseURL, recordingR.ID), 1, 5)
	if pid := waitProcessForBinary(t, providerV1Path, 15*time.Second); pid == 0 {
		t.Fatal("v1 Engine did not spawn the immutable external storage provider")
	}
	assertNoLocalRecordingEntries(t, dataDir, recordingR.ID)

	registryMu.Lock()
	currentVersion = "2.0.0"
	registryMu.Unlock()
	plugins, code = getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodPost, httpapi.PluginsEndpoint+"/refresh", map[string]any{})
	if code != http.StatusOK || !pluginStatusHasStorageRelease(plugins, "2.0.0", "1.0.0") {
		t.Fatalf("refresh storage registry v2 status=%d plugins=%+v", code, plugins)
	}
	plugins, code = getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodPost, httpapi.PluginsEndpoint+"/"+storageProcessE2EProviderID+"/update", map[string]any{})
	if code != http.StatusOK || !pluginStatusHasStorageRelease(plugins, "2.0.0", "2.0.0") {
		t.Fatalf("install provider v2 status=%d plugins=%+v", code, plugins)
	}
	storageStatus = runtimeStorageStatus(t, client, baseURL)
	if storageStatus.Primary.Version != "1.0.0" {
		t.Fatalf("provider update switched the active primary without activation: %+v", storageStatus.Primary)
	}
	if item, ok := runtimeStorageSummary(storageStatus, storageProcessE2EProviderID); !ok || item.Version != "2.0.0" || !item.Configured || item.Active {
		t.Fatalf("provider v2 should be configured and installed but inactive: item=%+v found=%t", item, ok)
	}
	if _, code := getRuntimeJSON[httpapi.StorageProbeResult](t, client, baseURL, http.MethodPost, httpapi.StorageProvidersPrefix+storageProcessE2EProviderID+"/probe", map[string]any{}); code != http.StatusOK {
		t.Fatalf("probe provider v2 returned %d", code)
	}
	v2Status, code := getRuntimeJSON[httpapi.StorageProviderStatus](t, client, baseURL, http.MethodPost, httpapi.StorageProvidersPrefix+storageProcessE2EProviderID+"/activate", map[string]any{})
	if code != http.StatusOK || v2Status.Primary.Version != "2.0.0" {
		t.Fatalf("activate provider v2 status=%d primary=%+v", code, v2Status.Primary)
	}
	activeV2Snapshot := readRuntimeGenerationSnapshot(t, dataDir)
	v2GenerationID := activeV2Snapshot.ActiveGenerationID
	v2Generation := activeV2Snapshot.Generations[v2GenerationID]
	if v2GenerationID == v1GenerationID || v2Generation.StorageProviderSetID == v1Generation.StorageProviderSetID {
		t.Fatalf("v2 activation did not create a distinct provider generation: v1=%+v v2=%+v", v1Generation, v2Generation)
	}
	v1StillPinned := activeV2Snapshot.Generations[v1GenerationID]
	leaseR = activeV2Snapshot.Leases[recordingR.ID]
	if v1StillPinned.State != generation.StateDraining || leaseR.EngineGeneration != v1GenerationID {
		t.Fatalf("v1 generation must remain draining and pinned while R is active: generation=%+v lease=%+v", v1StillPinned, leaseR)
	}
	if info, err := os.Stat(providerV1Path); err != nil || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("old immutable v1 artifact was removed or made non-executable while R held its lease: info=%v err=%v", info, err)
	}
	if process.command.Process.Pid != hostPID || processForBinary(artifacts.hostA) != hostPID {
		t.Fatalf("Host process changed during v2 activation: original=%d current=%d", hostPID, processForBinary(artifacts.hostA))
	}
	controlV2PID := waitDirectChildForBinaryDifferent(t, hostPID, controlBinary, controlV1PID, 25*time.Second)
	engineV2PID := waitDirectChildForBinaryDifferent(t, hostPID, engineBinary, engineV1PID, 25*time.Second)
	if controlV2PID == engineV2PID {
		t.Fatalf("v2 Control and Engine are not separate processes: control=%d engine=%d", controlV2PID, engineV2PID)
	}
	catalog, err = storagecatalog.Open(storageCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	v2Set, err := catalog.LoadSet(v2Generation.StorageProviderSetID)
	if err != nil || v2Set.Artifact.Version != "2.0.0" {
		t.Fatalf("v2 generation does not resolve to immutable v2 provider set: set=%+v err=%v", v2Set, err)
	}
	providerV2Path, err = catalog.ArtifactPath(v2Set.Artifact.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if pid := waitProcessForBinary(t, providerV2Path, 15*time.Second); pid == 0 {
		t.Fatal("v2 Engine did not spawn its immutable external storage provider")
	}

	recordingS := createRuntimeRecording(t, client, baseURL, "Storage provider v2 pinned S", map[string]string{"source_url": fixture.server.URL + "/source/" + streamS})
	leaseS := waitRecordingLease(t, dataDir, recordingS.ID, 25*time.Second)
	if leaseS.EngineGeneration != v2GenerationID || readRuntimeGenerationSnapshot(t, dataDir).Leases[recordingR.ID].EngineGeneration != v1GenerationID {
		t.Fatalf("recording provider generation pinning is wrong: R=%+v S=%+v active=%s", readRuntimeGenerationSnapshot(t, dataDir).Leases[recordingR.ID], leaseS, v2GenerationID)
	}
	fixture.advance(streamR, 4)
	fixture.advance(streamS, 4)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 9, 30*time.Second)
	waitRecordingSequenceCount(t, client, baseURL, recordingS.ID, 4, 30*time.Second)
	verifyStorageProcessE2EObjects(t, providerRoot, streamR, getRecording(t, client, baseURL, recordingR.ID), 1, 9)
	verifyStorageProcessE2EObjects(t, providerRoot, streamS, getRecording(t, client, baseURL, recordingS.ID), 1, 4)
	assertNoLocalRecordingEntries(t, dataDir, recordingR.ID, recordingS.ID)

	stopRecording(t, client, baseURL, recordingR.ID)
	stopRecording(t, client, baseURL, recordingS.ID)
	waitRuntimeRecordingState(t, client, baseURL, recordingR.ID, domain.StateStopped, 25*time.Second)
	waitRuntimeRecordingState(t, client, baseURL, recordingS.ID, domain.StateStopped, 25*time.Second)
	waitLeaseAbsent(t, dataDir, recordingR.ID, 30*time.Second)
	waitLeaseAbsent(t, dataDir, recordingS.ID, 30*time.Second)
	finalR := getRecording(t, client, baseURL, recordingR.ID)
	finalS := getRecording(t, client, baseURL, recordingS.ID)
	if finalR.ID != recordingR.ID || finalS.ID != recordingS.ID || finalR.State != domain.StateStopped || finalS.State != domain.StateStopped {
		t.Fatalf("recording identities or terminal states changed: R=%+v S=%+v", finalR, finalS)
	}
	verifyStorageProcessE2EObjects(t, providerRoot, streamR, finalR, 1, 9)
	verifyStorageProcessE2EObjects(t, providerRoot, streamS, finalS, 1, 4)
	assertRuntimeStorageRange(t, client, baseURL, finalR, streamR)
	assertRuntimeStorageIntegrity(t, client, baseURL, finalR.ID)
	waitGenerationEngineDormant(t, dataDir, v1GenerationID, 30*time.Second)
	if err := waitProcessAbsent(t, providerV1Path, 20*time.Second); err != nil {
		t.Fatalf("v1 provider process did not exit after its generation lease drained: %v", err)
	}

	// The installed v2 executable, not the remote registry, is the runtime
	// dependency. Stop the Host and registry before cold-start persistence checks.
	stopRuntimeHostProcess(process)
	if err := waitProcessAbsent(t, artifacts.hostA, 20*time.Second); err != nil {
		t.Fatalf("Runtime Host did not stop before restart: %v", err)
	}
	registryServer.Close()
	process = startHost()
	status = waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if status.DefaultEngine == nil {
		t.Fatalf("restarted Host has no default Engine: %+v", status)
	}
	status, code = getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodGet, httpapi.Endpoint, nil)
	if code != http.StatusOK || status.DefaultEngine == nil || status.DefaultEngine.ID != v2GenerationID {
		t.Fatalf("cold Host recovery lost active v2 generation: status=%d %+v", code, status)
	}
	storageStatus = runtimeStorageStatus(t, client, baseURL)
	if storageStatus.Primary.Kind != "plugin" || storageStatus.Primary.Version != "2.0.0" {
		t.Fatalf("cold Host recovery did not retain v2 primary: %+v", storageStatus.Primary)
	}
	for _, recordingID := range []string{recordingR.ID, recordingS.ID} {
		reloaded := getRecording(t, client, baseURL, recordingID)
		if reloaded.ID != recordingID || reloaded.State != domain.StateStopped {
			t.Fatalf("cold Host recovery did not load remote Recording %s: %+v", recordingID, reloaded)
		}
		stream := streamR
		if recordingID == recordingS.ID {
			stream = streamS
		}
		assertRuntimeStorageRange(t, client, baseURL, reloaded, stream)
		assertRuntimeStorageIntegrity(t, client, baseURL, recordingID)
	}
	assertNoLocalRecordingEntries(t, dataDir, recordingR.ID, recordingS.ID)
	localActivation, err := runtimeRequest(client, baseURL, http.MethodPost, httpapi.StorageProvidersPrefix+"local/activate", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	localBody, readErr := io.ReadAll(io.LimitReader(localActivation.Body, 64<<10))
	_ = localActivation.Body.Close()
	if readErr != nil || localActivation.StatusCode != http.StatusConflict || !strings.Contains(string(localBody), `"code":"storage_backend_switch_requires_empty_archive"`) {
		t.Fatalf("local switch over an existing remote archive was not rejected safely: status=%d body=%s read=%v", localActivation.StatusCode, boundedOutput(localBody), readErr)
	}
}

func readStorageProcessE2EBinary(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("read external storage fixture %s: bytes=%d err=%v", filepath.Base(path), len(data), err)
	}
	return data
}

func storageProcessE2ERegistryDocument(host, version, platform, architecture string, size int, digest string) map[string]any {
	return map[string]any{
		"schema_version": 2,
		"plugins": []any{map[string]any{
			"id": storageProcessE2EProviderID, "type": "storage", "name": "Fixture Storage",
			"repository": "https://example.test/integrated-recorder-storage-fixture",
			"channels":   map[string]string{"stable": version},
			"releases": []any{map[string]any{
				"version": version, "protocol": map[string]any{"name": "storage", "version": 1},
				"source_commit": "0123456789abcdef",
				"artifacts": []any{map[string]any{
					"os": platform, "arch": architecture,
					"url":      "https://" + host + "/artifact/" + version,
					"filename": "integrated-recorder-storage-" + storageProcessE2EProviderID,
					"size":     size, "sha256": digest,
				}},
			}},
		}},
	}
}

func writeRuntimeE2ERegistryCA(t *testing.T, server *httptest.Server) string {
	t.Helper()
	certificate := server.Certificate()
	if certificate == nil || len(certificate.Raw) == 0 {
		t.Fatal("local registry TLS fixture has no certificate")
	}
	if _, err := x509.ParseCertificate(certificate.Raw); err != nil {
		t.Fatalf("parse local registry TLS fixture certificate: %v", err)
	}
	data := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	path := filepath.Join(t.TempDir(), "registry-ca.pem")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write private test registry trust root: %v", err)
	}
	return path
}

func pluginStatusHasStorageRelease(status httpapi.PluginStatus, availableVersion, installedVersion string) bool {
	for _, item := range status.Plugins {
		if item.ID == storageProcessE2EProviderID {
			return item.Type == "storage" && item.AvailableVersion == availableVersion && item.Installed == (installedVersion != "") && item.InstalledVersion == installedVersion
		}
	}
	return false
}

func runtimeStorageStatus(t *testing.T, client *http.Client, baseURL string) httpapi.StorageProviderStatus {
	t.Helper()
	status, code := getRuntimeJSON[httpapi.StorageProviderStatus](t, client, baseURL, http.MethodGet, httpapi.StorageProviderEndpoint, nil)
	if code != http.StatusOK {
		t.Fatalf("GET storage provider status returned %d: %+v", code, status)
	}
	return status
}

func runtimeStorageSummary(status httpapi.StorageProviderStatus, id string) (httpapi.StorageProviderSummary, bool) {
	for _, item := range status.Providers {
		if item.ID == id {
			return item, true
		}
	}
	return httpapi.StorageProviderSummary{}, false
}

func configureRuntimeStorageProvider(t *testing.T, client *http.Client, baseURL, root string) {
	t.Helper()
	body := httpapi.StorageConfigRequest{Values: map[string]json.RawMessage{"root": mustStorageJSON(t, root)}, Secrets: map[string]string{}}
	_, code := getRuntimeJSON[httpapi.StorageConfigView](t, client, baseURL, http.MethodPut, httpapi.StorageProvidersPrefix+storageProcessE2EProviderID+"/config", body)
	if code != http.StatusOK {
		t.Fatalf("configure provider root returned %d", code)
	}
}

func verifyStorageProcessE2EObjects(t *testing.T, root, stream string, recording *domain.Recording, first, last uint64) {
	t.Helper()
	if recording == nil || recording.Tracks["main"] == nil {
		t.Fatal("remote provider recording has no main track")
	}
	segments := recording.Tracks["main"].Segments
	if len(segments) < int(last-first+1) {
		t.Fatalf("remote Recording %s has %d segments, expected at least %d", recording.ID, len(segments), last-first+1)
	}
	seenOrdinals := map[uint64]bool{}
	seenSequences := map[uint64]bool{}
	for _, segment := range segments {
		if seenOrdinals[segment.ArchiveOrdinal] {
			t.Fatalf("remote archive has duplicate ordinal %d", segment.ArchiveOrdinal)
		}
		seenOrdinals[segment.ArchiveOrdinal] = true
		if seenSequences[segment.Sequence] {
			t.Fatalf("remote archive has duplicate source sequence %d", segment.Sequence)
		}
		seenSequences[segment.Sequence] = true
		if segment.Sequence < first || segment.Sequence > last {
			continue
		}
		key := "recordings/" + recording.ID + "/" + segment.StoragePath
		path := filepath.Join(root, filepath.FromSlash(key))
		payload, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read physical provider object %s: %v", key, err)
		}
		want := []byte(runtimeSegmentPayload(stream, segment.Sequence))
		digest := sha256.Sum256(payload)
		if string(payload) != string(want) || int64(len(payload)) != segment.PayloadSize || hex.EncodeToString(digest[:]) != segment.SHA256 {
			t.Fatalf("provider object %s differs from source bytes: size=%d/%d hash=%s/%s", key, len(payload), segment.PayloadSize, hex.EncodeToString(digest[:]), segment.SHA256)
		}
	}
	for sequence := first; sequence <= last; sequence++ {
		if !seenSequences[sequence] {
			t.Fatalf("remote archive is missing source sequence %d", sequence)
		}
	}
	metadataPath := filepath.Join(root, "recordings", recording.ID, "recording.json")
	metadata, err := os.ReadFile(metadataPath)
	var archived domain.Recording
	if err == nil {
		err = json.Unmarshal(metadata, &archived)
	}
	if err != nil || archived.ID != recording.ID {
		t.Fatalf("Core recording metadata was not published under its logical key: %s err=%v", metadataPath, err)
	}
}

func assertNoLocalRecordingEntries(t *testing.T, dataDir string, recordingIDs ...string) {
	t.Helper()
	root := filepath.Join(dataDir, "recordings")
	for _, id := range recordingIDs {
		if id == "" {
			continue
		}
		if _, err := os.Lstat(filepath.Join(root, id)); err == nil {
			t.Fatalf("local fallback archive unexpectedly contains Recording %s", id)
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect local fallback archive entry: %v", err)
		}
	}
}

func assertRuntimeStorageRange(t *testing.T, client *http.Client, baseURL string, recording *domain.Recording, stream string) {
	t.Helper()
	if recording == nil || recording.Tracks["main"] == nil || len(recording.Tracks["main"].Segments) == 0 {
		t.Fatal("range playback requires a committed segment")
	}
	segment := recording.Tracks["main"].Segments[0]
	want := []byte(runtimeSegmentPayload(stream, segment.Sequence))
	if len(want) < 12 {
		t.Fatalf("fixture payload unexpectedly short: %d", len(want))
	}
	start, end := int64(4), int64(15)
	request, err := http.NewRequest(http.MethodGet, baseURL+"/api/recordings/"+recording.ID+"/play/segments/"+segment.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("request provider-backed range playback: %v", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, end-start+2))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusPartialContent || response.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", start, end, len(want)) || string(data) != string(want[start:end+1]) {
		t.Fatalf("provider-backed range playback mismatch: status=%d range=%q bytes=%q read=%v close=%v", response.StatusCode, response.Header.Get("Content-Range"), data, readErr, closeErr)
	}
}

func assertRuntimeStorageIntegrity(t *testing.T, client *http.Client, baseURL, recordingID string) {
	t.Helper()
	job, code := getRuntimeJSON[integrity.Job](t, client, baseURL, http.MethodPost, "/api/recordings/"+recordingID+"/integrity/verify", map[string]any{})
	if code != http.StatusAccepted {
		t.Fatalf("start remote provider integrity verification returned %d: %+v", code, job)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		job, code = getRuntimeJSON[integrity.Job](t, client, baseURL, http.MethodGet, "/api/integrity/jobs/"+job.ID, nil)
		if code == http.StatusOK && job.State == integrity.StateCompleted && job.Result != nil {
			if job.Result.Status != storage.IntegrityVerified || job.Result.ObjectsMissing != 0 || job.Result.ObjectsCorrupt != 0 {
				t.Fatalf("remote provider integrity result is not verified: %+v", job.Result)
			}
			return
		}
		if code != http.StatusOK || job.State == integrity.StateFailed {
			t.Fatalf("remote provider integrity job failed: code=%d job=%+v", code, job)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("remote provider integrity job did not complete: %+v", job)
}

func waitGenerationEngineDormant(t *testing.T, dataDir, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot := readRuntimeGenerationSnapshot(t, dataDir)
		generationState, exists := snapshot.Generations[id]
		if exists && generationState.EngineDormant {
			for _, lease := range snapshot.Leases {
				if lease.EngineGeneration == id {
					t.Fatalf("generation %s became dormant while lease %s still referred to it", id, lease.RecordingID)
				}
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("old storage generation %s did not become dormant after its last recording lease", id)
}

func runtimeRequest(client *http.Client, baseURL, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequest(method, baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return client.Do(request)
}

func waitDirectChildForBinaryDifferent(t *testing.T, parentPID int, binary string, previousPID int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		processes, err := runtimeProcessTable()
		if err == nil {
			for _, process := range processes {
				if process.Parent == parentPID && process.PID != previousPID && commandRunsBinary(process.Command, filepath.Clean(binary)) {
					return process.PID
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Host PID %d did not replace child %s PID %d", parentPID, filepath.Base(binary), previousPID)
	return 0
}
