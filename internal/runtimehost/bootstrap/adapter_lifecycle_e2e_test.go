package bootstrap

import (
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
)

const addedFixtureAdapterID = "runtime-added-fixture"

// TestProductionAdapterLifecycleAcceptanceE2E exercises mutable source
// directories only as Host import sources. Runtime Host, Control, and Engine
// are production executables; only the platform/source endpoint and adapter
// executable are deterministic test fixtures.
func TestProductionAdapterLifecycleAcceptanceE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires building and running production Runtime Host, Control Plane, and Recorder Engine")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-tree assertions currently use the Unix ps interface")
	}
	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeAdapterLifecycleArtifacts(t, fixture.server.URL)
	originalAdapter := buildFixtureAdapter(t, artifacts.root, runtimeE2EAdapterID, "0.1.0")
	addedAdapter := buildFixtureAdapter(t, artifacts.root, addedFixtureAdapterID, "1.0.0")
	upgradedAdapter := buildFixtureAdapter(t, artifacts.root, runtimeE2EAdapterID, "0.2.0")
	autoV1 := buildFixtureAdapter(t, artifacts.root, "registry_fixture", "1.0.0")
	autoV2 := buildFixtureAdapter(t, artifacts.root, "registry_fixture", "2.0.0")
	for iteration := 1; iteration <= 2; iteration++ {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			runProductionAdapterLifecycleScenario(t, artifacts, fixture, originalAdapter, addedAdapter, upgradedAdapter, iteration)
		})
	}
	t.Run("automatic_registry_update_keeps_recording_pinned", func(t *testing.T) {
		runProductionAutomaticPluginUpdateScenario(t, artifacts, fixture, autoV1, autoV2)
	})
}

type runtimeAdapterLifecycleArtifacts struct {
	root               string
	fixtureURL         string
	bundleA            string
	hostA              string
	adapterDir         string
	storageLocalBinary string
}

// buildRuntimeAdapterLifecycleArtifacts builds only the production processes
// this acceptance test starts. The signed-update E2E has its own heavier
// artifact builder because it also needs packaged release A/B artifacts.
func buildRuntimeAdapterLifecycleArtifacts(t *testing.T, fixtureURL string) runtimeAdapterLifecycleArtifacts {
	t.Helper()
	moduleRoot := findRuntimeE2EModuleRoot(t)
	root := newRuntimeE2ETempDir(t)
	bin := filepath.Join(root, "bin")
	bundleA := filepath.Join(root, "initial-a")
	adapterDir := filepath.Join(root, "adapters")
	bundledAdapterDir := filepath.Join(root, "bundled-adapters")
	for _, directory := range []string{bin, bundleA, adapterDir, bundledAdapterDir} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	fixtureURL = strings.TrimRight(fixtureURL, "/")
	ldflags := strings.Join([]string{
		"-X github.com/integrated-recorder/core/internal/buildinfo.version=" + e2eVersionA,
		"-X github.com/integrated-recorder/core/internal/buildinfo.commit=" + e2eCommitA,
		"-X github.com/integrated-recorder/core/internal/buildinfo.buildTime=2026-09-30T00:00:00Z",
		"-X github.com/integrated-recorder/core/internal/buildinfo.releaseChannel=prerelease",
	}, " ")
	build := func(output, packagePath, tags, flags string) string {
		t.Helper()
		args := []string{"build", "-o", output}
		if tags != "" {
			args = append(args, "-tags", tags)
		}
		if flags != "" {
			args = append(args, "-ldflags", flags)
		}
		args = append(args, packagePath)
		runBuildCommand(t, moduleRoot, 4*time.Minute, args...)
		return output
	}
	control := build(filepath.Join(bin, "control-plane"), "./cmd/control-plane", "", ldflags)
	engineFlags := ldflags + " -X main.runtimeE2EFixtureOrigin=" + fixtureURL
	engine := build(filepath.Join(bin, "recorder-engine"), "./cmd/recorder-engine", "runtime_e2e", engineFlags)
	hls := build(filepath.Join(bin, "integrated-recorder-adapter-hls"), "./cmd/adapters/hls", "", ldflags)
	storageLocal := build(filepath.Join(bin, "storage.local"), "./cmd/storage-local", "", ldflags)
	copyRuntimeArtifact(t, control, filepath.Join(bundleA, "control-plane"), 0555)
	copyRuntimeArtifact(t, engine, filepath.Join(bundleA, "recorder-engine"), 0555)
	if err := os.Chmod(bundleA, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(bundleA, 0700)
		for _, name := range []string{"control-plane", "recorder-engine"} {
			_ = os.Chmod(filepath.Join(bundleA, name), 0600)
		}
	})
	bundledHLS := filepath.Join(bundledAdapterDir, "integrated-recorder-adapter-hls")
	copyRuntimeArtifact(t, hls, bundledHLS, 0555)
	hostFlags := ldflags + " -X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundleDir=" + bundleA +
		" -X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundledHLSBinary=" + bundledHLS
	hostA := build(filepath.Join(bin, "runtime-host-a"), "./cmd/runtime-host", "runtime_e2e", hostFlags)
	return runtimeAdapterLifecycleArtifacts{
		root: root, fixtureURL: fixtureURL, bundleA: bundleA, hostA: hostA, adapterDir: adapterDir, storageLocalBinary: storageLocal,
	}
}

func buildFixtureAdapter(t *testing.T, root, adapterID, version string) string {
	t.Helper()
	output := filepath.Join(root, "bin", "integrated-recorder-adapter-"+adapterID+"-"+version)
	flags := "-X main.fixtureAdapterID=" + adapterID + " -X main.fixtureAdapterVersion=" + version
	runBuildCommand(t, findRuntimeE2EModuleRoot(t), 4*time.Minute,
		"build", "-o", output, "-ldflags", flags, "./web/e2e/runtime_update_adapter")
	return output
}

func runProductionAutomaticPluginUpdateScenario(t *testing.T, artifacts runtimeAdapterLifecycleArtifacts, fixture *runtimeUpdateFixture, v1Path, v2Path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	v1, err := os.ReadFile(v1Path)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := os.ReadFile(v2Path)
	if err != nil {
		t.Fatal(err)
	}
	releases := map[string][]byte{"1.0.0": v1, "2.0.0": v2}
	var registryMu sync.Mutex
	stableVersion := "1.0.0"
	registryRequests := 0
	registryServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryMu.Lock()
		version := stableVersion
		registryRequests++
		artifact := append([]byte(nil), releases[version]...)
		registryMu.Unlock()
		switch r.URL.Path {
		case "/registry.json":
			writeRuntimeAutoPluginRegistry(t, w, r, version, artifact)
		case "/artifact":
			if r.URL.Query().Get("version") != version {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(artifact)))
			_, _ = w.Write(artifact)
		default:
			http.NotFound(w, r)
		}
	}))
	defer registryServer.Close()
	caPath := writeRuntimeE2ERegistryCA(t, registryServer)
	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	streamR, streamS := "auto-plugin-r", "auto-plugin-s"
	fixture.reset(streamR, "Recording R", "Pinned source v1", "auto-plugin-r-session")
	fixture.reset(streamS, "Recording S", "New source v2", "auto-plugin-s-session")
	listenAddr := reserveRuntimeAddress(t)
	baseURL := "http://" + listenAddr
	command := exec.Command(artifacts.hostA)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
		"AUTH_DISABLED=1",
		"ADAPTER_DIR=" + artifacts.adapterDir,
		"IR_ALLOW_OPERATOR_PLUGINS=1",
		"IR_PLUGIN_REGISTRY_URL=" + registryServer.URL + "/registry.json",
		"IR_RUNTIME_E2E_PLUGIN_REGISTRY_CA_FILE=" + caPath,
		"IR_RUNTIME_E2E_PLUGIN_AUTO_UPDATE_INTERVAL=100ms",
	})
	process := &runtimeHostProcess{command: command, done: make(chan struct{})}
	command.Stdout, command.Stderr = &process.output, &process.output
	if err := command.Start(); err != nil {
		t.Fatalf("start production Runtime Host for automatic plugin update: %v", err)
	}
	go func() { process.err = command.Wait(); close(process.done) }()
	t.Cleanup(func() { stopRuntimeHostProcess(process) })
	client := &http.Client{Timeout: 90 * time.Second}
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if status.DefaultEngine == nil {
		t.Fatalf("Host did not expose initial Engine status: %+v", status)
	}
	initialGeneration := status.DefaultEngine.ID
	plugins, code := getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodPost, httpapi.PluginsEndpoint+"/refresh", map[string]any{})
	if code != http.StatusOK || plugins.State != "ready" {
		t.Fatalf("initial registry refresh status=%d plugins=%+v", code, plugins)
	}
	plugins, code = getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodPost, httpapi.PluginsEndpoint+"/registry_fixture/install", map[string]any{})
	if code != http.StatusOK || !pluginStatusHasVersion(plugins, "registry_fixture", "1.0.0") {
		t.Fatalf("install initial Registry source plugin status=%d plugins=%+v", code, plugins)
	}
	status = waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	initialGeneration = status.DefaultEngine.ID
	initialRegistry := readRuntimeGenerationSnapshot(t, dataDir)
	initialSetID := initialRegistry.Generations[initialGeneration].AdapterSetID
	if initialSetID == "" {
		t.Fatalf("initial source generation has no adapter set: %+v", initialRegistry.Generations[initialGeneration])
	}
	recordingR := createRuntimeRecordingWithAdapter(t, client, baseURL, "registry_fixture", "Pinned to Registry source v1", map[string]string{"source_url": fixture.server.URL + "/source/" + streamR})
	leaseR := waitRecordingLease(t, dataDir, recordingR.ID, 20*time.Second)
	if leaseR.EngineGeneration != initialGeneration {
		t.Fatalf("R did not pin v1 generation: lease=%+v initial=%s", leaseR, initialGeneration)
	}
	fixture.advance(streamR, 3)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 3, 30*time.Second)
	registryMu.Lock()
	requestsBeforeUpdate := registryRequests
	stableVersion = "2.0.0"
	registryMu.Unlock()
	if err := waitRuntimeConditionError(90*time.Second, func() bool {
		pluginStatus, pluginCode := getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodGet, httpapi.PluginsEndpoint, nil)
		if pluginCode != http.StatusOK || !pluginStatusHasVersion(pluginStatus, "registry_fixture", "2.0.0") {
			return false
		}
		current, statusCode := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodGet, httpapi.Endpoint, nil)
		return statusCode == http.StatusOK && current.DefaultEngine != nil && current.DefaultEngine.ID != initialGeneration
	}, "automatic Registry stable selection activation"); err != nil {
		t.Fatalf("automatic update did not install and activate v2 without its update endpoint: %v; host=%s", err, process.output.String())
	}
	registryMu.Lock()
	requestsAfterUpdate := registryRequests
	registryMu.Unlock()
	if requestsAfterUpdate <= requestsBeforeUpdate {
		t.Fatalf("automatic pass did not fetch the Registry after stable changed: before=%d after=%d", requestsBeforeUpdate, requestsAfterUpdate)
	}
	plugins, _ = getRuntimeJSON[httpapi.PluginStatus](t, client, baseURL, http.MethodGet, httpapi.PluginsEndpoint, nil)
	if item, ok := pluginStatusItem(plugins, "registry_fixture"); !ok || item.InstalledVersion != "2.0.0" || item.UpdateAvailable {
		t.Fatalf("automatic plugin status did not converge to v2: %+v found=%t", item, ok)
	}
	registryState := readRuntimeGenerationSnapshot(t, dataDir)
	newGeneration := registryState.Generations[registryState.ActiveGenerationID]
	if registryState.ActiveGenerationID == initialGeneration || newGeneration.AdapterSetID == initialSetID {
		t.Fatalf("automatic source update did not activate a new immutable adapter set: %+v", newGeneration)
	}
	leaseR = readRuntimeLeases(t, dataDir)[recordingR.ID]
	if leaseR.EngineGeneration != initialGeneration {
		t.Fatalf("automatic adapter-set change migrated existing Recording R: %+v", leaseR)
	}
	oldSetBinary := filepath.Join(dataDir, "runtime", "adapters", "sets", initialSetID, "bin", "integrated-recorder-adapter-registry_fixture")
	if info, err := os.Stat(oldSetBinary); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("v1 immutable adapter disappeared while R retained its lease: info=%v err=%v", info, err)
	}
	fixture.advance(streamR, 3)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 6, 30*time.Second)
	requests := fixture.segmentRequestsFor(streamR)
	for sequence := uint64(1); sequence <= 6; sequence++ {
		if got := requests[sequence]; len(got) != 1 || !got[0].Succeeded {
			t.Fatalf("v1 pinned Recording R source request seq=%d is not exact-once success: %+v all=%+v", sequence, got, requests)
		}
	}
	if recording := getRecording(t, dataDir, client, baseURL, recordingR.ID); recording.State != domain.StateRecording || len(recording.Gaps) != 0 {
		t.Fatalf("R was interrupted by automatic plugin update: %+v", recording)
	}
	recordingS := createRuntimeRecordingWithAdapter(t, client, baseURL, "registry_fixture", "Uses Registry source v2", map[string]string{"source_url": fixture.server.URL + "/source/" + streamS})
	leaseS := waitRecordingLease(t, dataDir, recordingS.ID, 20*time.Second)
	if leaseS.EngineGeneration != newGeneration.ID || leaseS.EngineGeneration == leaseR.EngineGeneration {
		t.Fatalf("new Recording S did not use v2 generation while R remained pinned: R=%+v S=%+v new=%s", leaseR, leaseS, newGeneration.ID)
	}
	fixture.advance(streamS, 2)
	waitRecordingSequenceCount(t, client, baseURL, recordingS.ID, 2, 30*time.Second)
	stopRecording(t, client, baseURL, recordingS.ID)
	stopRecording(t, client, baseURL, recordingR.ID)
	waitRuntimeRecordingState(t, client, baseURL, recordingR.ID, domain.StateStopped, 20*time.Second)
}

func writeRuntimeAutoPluginRegistry(t *testing.T, w http.ResponseWriter, r *http.Request, version string, binary []byte) {
	t.Helper()
	digest := sha256.Sum256(binary)
	document := map[string]any{
		"schema_version": 1,
		"plugins": []any{map[string]any{
			"id": "registry_fixture", "name": "Automatic Registry Fixture", "repository": "https://github.com/example/automatic-fixture",
			"channels": map[string]string{"stable": version},
			"releases": []any{map[string]any{
				"version": version, "protocol_version": 1, "source_commit": strings.Repeat("b", 40),
				"artifacts": []any{map[string]any{
					"os": runtime.GOOS, "arch": runtime.GOARCH, "url": "https://" + r.Host + "/artifact?version=" + version,
					"filename": "integrated-recorder-adapter-registry_fixture", "size": len(binary), "sha256": hex.EncodeToString(digest[:]),
				}},
			}},
		}},
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(document); err != nil {
		t.Errorf("encode automatic plugin Registry fixture: %v", err)
	}
}

type lifecycleAdapterView struct {
	Descriptor *struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	} `json:"descriptor"`
	Status struct {
		ID    string `json:"id"`
		State string `json:"state"`
	} `json:"status"`
}

func runProductionAdapterLifecycleScenario(t *testing.T, artifacts runtimeAdapterLifecycleArtifacts, fixture *runtimeUpdateFixture, originalAdapter, addedAdapter, upgradedAdapter string, iteration int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	atomicInstallAdapter(t, artifacts.adapterDir, "integrated-recorder-adapter-"+runtimeE2EAdapterID, originalAdapter)
	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	markerDir := filepath.Join(dataRoot, "failpoints")
	for _, directory := range []string{dataDir, markerDir} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { makeRuntimeE2ETreeWritable(dataDir) })
	streamR := fmt.Sprintf("hot-r-%d", iteration)
	streamS := fmt.Sprintf("hot-s-%d", iteration)
	streamT := fmt.Sprintf("hot-t-%d", iteration)
	fixture.reset(streamR, "Before adapter update", "Initial metadata", "hot-r-session")
	fixture.reset(streamS, "Added adapter", "Added adapter metadata", "hot-s-session")
	fixture.reset(streamT, "Upgraded adapter", "Upgraded adapter metadata", "hot-t-session")

	listenAddr := reserveRuntimeAddress(t)
	command := exec.Command(artifacts.hostA)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
		"AUTH_DISABLED=1",
		"ADAPTER_DIR=" + artifacts.adapterDir,
		"IR_ALLOW_OPERATOR_PLUGINS=1",
		"IR_RUNTIME_E2E_FAILPOINT=" + string(runtimehook.AfterSourceDrain),
		"IR_RUNTIME_E2E_MARKER_DIR=" + markerDir,
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
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 3 * time.Minute}
	controlABinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineABinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	fixtureV1Path := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-"+runtimeE2EAdapterID)
	addedPath := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-"+addedFixtureAdapterID)
	invalidPath := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-invalid-fixture")
	t.Cleanup(func() {
		stopRuntimeHostProcess(process)
		for _, executable := range []string{artifacts.hostA, controlABinary, engineABinary, fixtureV1Path, addedPath, invalidPath, filepath.Join(filepath.Dir(fixtureV1Path), "integrated-recorder-adapter-runtime-update-fixture-0.2.0")} {
			if err := waitProcessAbsent(t, executable, 15*time.Second); err != nil {
				t.Errorf("acceptance cleanup left product process running for %s: %v", filepath.Base(executable), err)
			}
		}
	})
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if status.ActiveControl == nil || status.DefaultEngine == nil {
		t.Fatalf("production A did not expose active Control and Engine: %+v", status)
	}
	hostPID := command.Process.Pid
	controlAPID := waitDirectChildForBinary(t, hostPID, controlABinary, 20*time.Second)
	engineAPID := waitDirectChildForBinary(t, hostPID, engineABinary, 20*time.Second)
	if controlAPID == engineAPID || controlAPID == hostPID || engineAPID == hostPID {
		t.Fatalf("Host/Control/Engine are not separate production processes: host=%d control=%d engine=%d", hostPID, controlAPID, engineAPID)
	}
	initialGeneration := status.DefaultEngine.ID
	initialRegistry := readRuntimeGenerationSnapshot(t, dataDir)
	initialSetID := initialRegistry.Generations[initialGeneration].AdapterSetID
	if len(initialSetID) != 64 {
		t.Fatalf("initial production generation has no immutable adapter set: %+v", initialRegistry.Generations[initialGeneration])
	}
	initialSetDir := filepath.Join(dataDir, "runtime", "adapters", "sets", initialSetID)
	immutableV1 := filepath.Join(initialSetDir, "bin", filepath.Base(fixtureV1Path))
	if info, err := os.Stat(immutableV1); err != nil || info.Mode().Perm()&0222 != 0 {
		t.Fatalf("initial imported adapter is not present as immutable set content: info=%v err=%v", info, err)
	}

	inputR := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + streamR}
	recordingR := createRuntimeRecording(t, client, baseURL, "Pinned to adapter set A", inputR)
	leaseR := waitRecordingLease(t, dataDir, recordingR.ID, 20*time.Second)
	if leaseR.EngineGeneration != initialGeneration {
		t.Fatalf("Recording R did not pin the initial Engine: lease=%+v generation=%s", leaseR, initialGeneration)
	}
	fixture.advance(streamR, 6)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 6, 20*time.Second)
	metadataR := waitMetadataTimeline(t, ctx, client, baseURL, recordingR.ID, 1, 35*time.Second)
	if len(metadataR.Items) != 1 || stringValue(metadataR.Items[0].Title) != "Before adapter update" {
		t.Fatalf("initial metadata was not recorded by Engine A: %+v", metadataR.Items)
	}
	if pid := waitDirectChildForBinary(t, engineAPID, immutableV1, 15*time.Second); pid == 0 {
		t.Fatal("Engine A did not spawn the adapter from its immutable v1 set")
	}
	baselineGeneration := status.DefaultEngine.ID

	// Add a second adapter using an atomic final-name publication. The Host's
	// periodic reconciler, not a direct catalog call, must roll a new generation.
	atomicInstallAdapter(t, artifacts.adapterDir, filepath.Base(addedPath), addedAdapter)
	status = waitForAdapterGeneration(t, ctx, client, baseURL, baselineGeneration, addedFixtureAdapterID, "1.0.0", true)
	if status.DefaultEngine.ID == initialGeneration || process.command.Process.Pid != hostPID {
		t.Fatalf("hot add did not switch application generation under the same Host process: initial=%s status=%+v hostPid=%d", initialGeneration, status, hostPID)
	}
	addedGeneration := status.DefaultEngine.ID
	leaseR = readRuntimeLeases(t, dataDir)[recordingR.ID]
	if leaseR.EngineGeneration != initialGeneration || processForBinary(artifacts.hostA) != hostPID || processForBinary(engineABinary) == 0 {
		t.Fatalf("hot add changed/ended the old Recording Engine: R=%+v hostPid=%d", leaseR, hostPID)
	}
	inputS := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + streamS}
	recordingS := createRuntimeRecordingWithAdapter(t, client, baseURL, addedFixtureAdapterID, "Pinned to added adapter", inputS)
	leaseS := waitRecordingLease(t, dataDir, recordingS.ID, 20*time.Second)
	if leaseS.EngineGeneration != addedGeneration {
		t.Fatalf("new Recording S did not use the added adapter generation: lease=%+v want=%s", leaseS, addedGeneration)
	}
	fixture.advance(streamS, 4)
	waitRecordingSequenceCount(t, client, baseURL, recordingS.ID, 4, 20*time.Second)
	fixture.advance(streamR, 4)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 10, 20*time.Second)

	// A malformed candidate is rejected through the product Host API and cannot
	// change the active tuple or the adapter list.
	if err := os.WriteFile(invalidPath, []byte("#!/bin/sh\necho not-a-protocol-frame\n"), 0700); err != nil {
		t.Fatal(err)
	}
	beforeInvalid := status.DefaultEngine.ID
	invalidResult, code := getRuntimeJSON[httpapi.AdapterReconcileResult](t, client, baseURL, http.MethodPost, httpapi.AdaptersEndpoint, map[string]any{})
	if code != http.StatusOK || invalidResult.State != "rejected" || invalidResult.RejectedCount == 0 {
		t.Fatalf("invalid adapter candidate was not safely reported by Host API: status=%d result=%+v", code, invalidResult)
	}
	status = waitRuntimeStatus(t, ctx, client, baseURL, func(current httpapi.Status) bool {
		return current.DefaultEngine != nil && current.DefaultEngine.ID == beforeInvalid
	})
	if status.DefaultEngine.ID != beforeInvalid || !adapterListHas(t, client, baseURL, addedFixtureAdapterID, "1.0.0") {
		t.Fatalf("invalid candidate changed the active generation or hid valid adapters: status=%+v", status)
	}
	if err := os.Remove(invalidPath); err != nil {
		t.Fatal(err)
	}
	_, code = getRuntimeJSON[httpapi.AdapterReconcileResult](t, client, baseURL, http.MethodPost, httpapi.AdaptersEndpoint, map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("explicit reconcile after invalid candidate returned %d", code)
	}

	// Removal drops the adapter from new work while S remains attached to the
	// old Engine/set which admitted it.
	// The removal set is exactly R's original adapter set and is therefore a
	// valid handover target. Advance one new source object only after the
	// deterministic drained boundary, so the source cannot race ahead and
	// consume the continuation payload before the target stages it.
	if err := writeRuntimeHookArm(markerDir, runtimehook.Arm{Point: runtimehook.AfterSourceDrain, RecordingID: recordingR.ID}); err != nil {
		t.Fatalf("arm source-drained boundary for R: %v", err)
	}
	if err := os.Remove(addedPath); err != nil {
		t.Fatal(err)
	}
	status = waitForAdapterGeneration(t, ctx, client, baseURL, beforeInvalid, addedFixtureAdapterID, "", false)
	removedGeneration := status.DefaultEngine.ID
	if removedGeneration == beforeInvalid || adapterListHas(t, client, baseURL, addedFixtureAdapterID, "") {
		t.Fatalf("removed adapter was not omitted from the new active set: %+v", status)
	}
	if leaseS.EngineGeneration != addedGeneration {
		t.Fatalf("removal migrated the existing Recording S: lease=%+v added=%s", leaseS, addedGeneration)
	}
	fixture.advance(streamS, 4)
	waitRecordingSequenceCount(t, client, baseURL, recordingS.ID, 8, 20*time.Second)
	readyPath := runtimehook.ReadyMarkerPath(markerDir, runtimehook.AfterSourceDrain, recordingR.ID)
	if err := waitRuntimeConditionError(30*time.Second, func() bool {
		info, statErr := os.Lstat(readyPath)
		return statErr == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == 0600
	}, "source-drained handover boundary for adapter removal"); err != nil {
		t.Fatalf("R did not reach a drained handover boundary before continuation publication: %v; host=%s", err, process.output.String())
	}
	fixture.advance(streamR, 1)
	if err := writeRuntimeHookRelease(markerDir, runtimehook.AfterSourceDrain, recordingR.ID); err != nil {
		t.Fatalf("release drained source after publishing its next segment: %v", err)
	}
	if err := os.Remove(runtimehook.ArmPath(markerDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disarm source-drained boundary: %v", err)
	}
	leaseR = waitRecordingLeaseGeneration(t, dataDir, recordingR.ID, removedGeneration, updateOperationTimeout+10*time.Second, process.output.String)
	type runtimeTimedEventPage struct {
		Items []struct {
			EventArchiveOrdinal uint64 `json:"event_archive_ordinal"`
			SourceEventID       string `json:"source_event_id"`
			Message             struct {
				Text string `json:"text"`
			} `json:"message"`
		} `json:"items"`
	}
	var timedEvents runtimeTimedEventPage
	if err := waitRuntimeConditionError(30*time.Second, func() bool {
		var code int
		timedEvents, code = getRuntimeJSON[runtimeTimedEventPage](t, client, baseURL, http.MethodGet, "/api/recordings/"+recordingR.ID+"/timed-events?limit=10", nil)
		return code == http.StatusOK && len(timedEvents.Items) >= 2
	}, "timed event resume across Runtime Host generation handover"); err != nil {
		t.Fatalf("timed events did not resume after generation handover: %v; host=%s", err, process.output.String())
	}
	if len(timedEvents.Items) != 2 || timedEvents.Items[0].EventArchiveOrdinal != 1 || timedEvents.Items[0].SourceEventID != "fixture-chat-1" || timedEvents.Items[0].Message.Text != "generation A" || timedEvents.Items[1].EventArchiveOrdinal != 2 || timedEvents.Items[1].SourceEventID != "fixture-chat-2" || timedEvents.Items[1].Message.Text != "generation B" {
		t.Fatalf("timed event handover produced a duplicate, gap, or ordering change: %+v", timedEvents.Items)
	}

	// Replace the original adapter identity with a new implementation. The new
	// default sees v2, while R's Engine retains its v1 artifact and set.
	atomicInstallAdapter(t, artifacts.adapterDir, filepath.Base(fixtureV1Path), upgradedAdapter)
	status = waitForAdapterGeneration(t, ctx, client, baseURL, removedGeneration, runtimeE2EAdapterID, "0.2.0", true)
	upgradedGeneration := status.DefaultEngine.ID
	removedRegistry := readRuntimeGenerationSnapshot(t, dataDir)
	if removedRegistry.Generations[removedGeneration].AdapterSetID != initialSetID {
		t.Fatalf("removal generation did not restore R's original immutable adapter set: initial=%s removed=%+v", initialSetID, removedRegistry.Generations[removedGeneration])
	}
	// R has already moved to the same-set removal generation before v2 became
	// active; adding or upgrading a different set remains ineligible under this
	// conservative policy.
	if err := waitRuntimeConditionError(20*time.Second, func() bool {
		state := readRuntimeGenerationSnapshot(t, dataDir)
		_, retained := state.Generations[initialGeneration]
		return !retained && !runtimePIDExists(engineAPID)
	}, "initial Engine retirement after exact-same-set handover"); err != nil {
		t.Fatalf("original generation stayed alive after R moved to the same adapter set: %v", err)
	}
	recordingREnginePID := waitEngineOwningAdapter(t, hostPID, engineABinary, immutableV1, 15*time.Second)
	if recordingREnginePID == engineAPID {
		t.Fatalf("R still uses the original Engine after same-set handover: initial=%d current=%d", engineAPID, recordingREnginePID)
	}
	currentR := getRecording(t, dataDir, client, baseURL, recordingR.ID)
	if currentR.State != domain.StateRecording || currentR.ID != recordingR.ID || len(currentR.Gaps) != 0 {
		t.Fatalf("adapter update interrupted R or created a gap: %+v", currentR)
	}
	leaseR = readRuntimeLeases(t, dataDir)[recordingR.ID]
	if leaseR.EngineGeneration != removedGeneration {
		t.Fatalf("adapter update moved R away from its v1 adapter-set generation: %+v", leaseR)
	}
	inputT := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + streamT}
	recordingT := createRuntimeRecording(t, client, baseURL, "Pinned to upgraded adapter", inputT)
	leaseT := waitRecordingLease(t, dataDir, recordingT.ID, 20*time.Second)
	if leaseT.EngineGeneration != upgradedGeneration || leaseT.EngineGeneration == leaseR.EngineGeneration {
		t.Fatalf("new Recording T did not use v2 while R stayed on v1: R=%s T=%s new=%s", leaseR.EngineGeneration, leaseT.EngineGeneration, upgradedGeneration)
	}
	fixture.advance(streamT, 3)
	waitRecordingSequenceCount(t, client, baseURL, recordingT.ID, 3, 20*time.Second)

	// Kill the v1 adapter process under the old Engine. R's next refresh must
	// lazily start the binary from the old immutable set, not the mutable source
	// now containing v2.
	v1AdapterPID := waitDirectChildForBinary(t, recordingREnginePID, immutableV1, 10*time.Second)
	adapterProcess, err := os.FindProcess(v1AdapterPID)
	if err != nil {
		t.Fatalf("find v1 adapter child process: %v", err)
	}
	if err := adapterProcess.Kill(); err != nil {
		t.Fatalf("kill old adapter fixture process: %v", err)
	}
	if err := waitRuntimeConditionError(10*time.Second, func() bool { return !runtimePIDExists(v1AdapterPID) }, "injected adapter child exit"); err != nil {
		t.Fatalf("old adapter process did not exit after injection: %v", err)
	}
	// The event monitor is expected to restart a killed optional event child
	// independently of media acquisition. Observe the replacement by PID rather
	// than requiring a process-wide quiet interval.
	_ = waitDirectChildForBinaryDifferent(t, recordingREnginePID, immutableV1, v1AdapterPID, 10*time.Second)
	refreshStarted, releaseRefresh := fixture.expireAndBlockRefresh(streamR)
	t.Cleanup(releaseRefresh)
	fixture.advance(streamR, 4)
	select {
	case <-refreshStarted:
	case <-ctx.Done():
		t.Fatalf("old Engine did not refresh R after adapter process termination: %v", ctx.Err())
	case <-time.After(20 * time.Second):
		t.Fatalf("old Engine did not invoke the v1 adapter refresh capability; fixture=%s", fixture.describe(streamR))
	}
	releaseRefresh()
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 15, 20*time.Second)
	waitDirectChildForBinary(t, recordingREnginePID, immutableV1, 10*time.Second)
	fixture.setMetadata(streamR, "After adapter update", "Still on v1 adapter")
	metadataAfter := waitMetadataTimeline(t, ctx, client, baseURL, recordingR.ID, 2, 45*time.Second)
	if len(metadataAfter.Items) != 2 || stringValue(metadataAfter.Items[1].Title) != "After adapter update" {
		t.Fatalf("Engine A metadata monitor did not survive adapter set activation: %+v", metadataAfter.Items)
	}
	finalR := getRecording(t, dataDir, client, baseURL, recordingR.ID)
	verifyRuntimeRecordingSegments(t, dataDir, finalR, streamR, 1, 15)
	if finalR.State != domain.StateRecording || len(finalR.Gaps) != 0 || !equalSequenceRange(recordingSequences(finalR), 1, 15) {
		t.Fatalf("Recording R continuity failed across adapter generations: state=%s gaps=%+v sequences=%v", finalR.State, finalR.Gaps, recordingSequences(finalR))
	}
	if refreshes := fixture.refreshesFor(streamR); len(refreshes) != 1 {
		t.Fatalf("adapter v1 did not refresh exactly once after Engine child restart: %+v", refreshes)
	}
	if info, err := os.Stat(immutableV1); err != nil || info.Mode().Perm()&0222 != 0 {
		t.Fatalf("old adapter set was not retained while R lease was active: info=%v err=%v", info, err)
	}

	stopRecording(t, client, baseURL, recordingT.ID)
	stopRecording(t, client, baseURL, recordingS.ID)
	stopRecording(t, client, baseURL, recordingR.ID)
	waitRuntimeRecordingState(t, client, baseURL, recordingR.ID, domain.StateStopped, 20*time.Second)
	waitLeaseAbsent(t, dataDir, recordingR.ID, 20*time.Second)
	waitLeaseAbsent(t, dataDir, recordingS.ID, 20*time.Second)
	waitLeaseAbsent(t, dataDir, recordingT.ID, 20*time.Second)
	// Lease projection and retirement are separate durable steps in the Host's
	// reconciliation cycle. Observe the completed drain instead of racing the
	// brief interval after the last lease is removed but before retirement runs.
	err = waitRuntimeConditionError(20*time.Second, func() bool {
		state := readRuntimeGenerationSnapshot(t, dataDir)
		retired, retained := state.Generations[removedGeneration]
		return retained && retired.EngineDormant && !runtimePIDExists(recordingREnginePID)
	}, "v1-set Engine process retirement after its final Recording lease drained")
	if err != nil {
		state := readRuntimeGenerationSnapshot(t, dataDir)
		t.Fatalf("v1-set Engine process did not retire after its last lease drained: %v; generation=%+v", err, state.Generations[removedGeneration])
	}
	state := readRuntimeGenerationSnapshot(t, dataDir)
	if generationState, exists := state.Generations[removedGeneration]; !exists || !generationState.EngineDormant {
		t.Fatalf("rollback-referenced v1-set generation was not retained with its Engine dormant: %+v", generationState)
	}
	if state.PreviousGenerationID != removedGeneration {
		t.Fatalf("expected removal generation to remain the rollback target, got previous=%s want=%s", state.PreviousGenerationID, removedGeneration)
	}
	if _, exists := state.Leases[recordingR.ID]; exists {
		t.Fatalf("R still has a generation lease after it stopped: %+v", state.Leases[recordingR.ID])
	}
	if _, err := os.Lstat(initialSetDir); err != nil {
		t.Fatalf("the v1 set should remain while the previous-generation rollback target references it: %v", err)
	}

	// The v1 set is currently retained by the rollback target. A further set
	// transition moves that rollback target forward; only then may v1 be GC'd.
	if err := os.Remove(fixtureV1Path); err != nil {
		t.Fatal(err)
	}
	status = waitForAdapterGeneration(t, ctx, client, baseURL, upgradedGeneration, runtimeE2EAdapterID, "", false)
	if status.DefaultEngine.ID == upgradedGeneration || !adapterListHas(t, client, baseURL, "hls", e2eVersionA) {
		t.Fatal("removing the final operator fixture adapter did not retain the bundled HLS-only set")
	}
	err = waitRuntimeConditionError(30*time.Second, func() bool {
		state := readRuntimeGenerationSnapshot(t, dataDir)
		_, oldGenerationRetained := state.Generations[removedGeneration]
		_, setErr := os.Lstat(initialSetDir)
		return !oldGenerationRetained && errors.Is(setErr, os.ErrNotExist)
	}, "unreferenced adapter set collection after rollback reference moved")
	if err != nil {
		state := readRuntimeGenerationSnapshot(t, dataDir)
		t.Fatalf("v1 adapter set was not collected after its last rollback reference moved: %v; previous=%s generations=%+v", err, state.PreviousGenerationID, state.Generations)
	}
	t.Logf("adapter lifecycle iteration %d: Host pid=%d stable; R=%s stayed on set %s and captured sequence=1..15 through v1 adapter restart; S=%s stayed on removed-adapter generation %s; T=%s used upgraded generation %s", iteration, hostPID, recordingR.ID, initialSetID, recordingS.ID, addedGeneration, recordingT.ID, upgradedGeneration)
}

func createRuntimeRecordingWithAdapter(t *testing.T, client *http.Client, baseURL, adapterID, title string, input map[string]string) domain.Recording {
	t.Helper()
	value, code := getRuntimeJSON[domain.Recording](t, client, baseURL, http.MethodPost, "/api/recordings", map[string]any{
		"adapter_id": adapterID, "input": input, "title": title, "preview_mode": "disabled",
	})
	if code != http.StatusCreated {
		t.Fatalf("create recording for adapter %q returned %d: %+v", adapterID, code, value)
	}
	return value
}

func atomicInstallAdapter(t *testing.T, directory, finalName, binary string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	temporary, err := os.CreateTemp(directory, ".adapter-install-*")
	if err != nil {
		_ = input.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(temporary, input)
	closeInputErr := input.Close()
	if copyErr == nil {
		copyErr = closeInputErr
	}
	if copyErr == nil {
		copyErr = temporary.Sync()
	}
	if copyErr == nil {
		copyErr = temporary.Chmod(0755)
	}
	closeOutputErr := temporary.Close()
	if copyErr == nil {
		copyErr = closeOutputErr
	}
	if copyErr != nil {
		_ = os.Remove(temporary.Name())
		t.Fatalf("write adapter staging copy: %v", copyErr)
	}
	final := filepath.Join(directory, finalName)
	if err := os.Rename(temporary.Name(), final); err != nil {
		_ = os.Remove(temporary.Name())
		t.Fatalf("atomically publish adapter source: %v", err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		t.Fatalf("sync adapter source directory: %v", syncErr)
	}
	if closeErr != nil {
		t.Fatalf("close adapter source directory: %v", closeErr)
	}
}

func waitForAdapterGeneration(t *testing.T, ctx context.Context, client *http.Client, baseURL, previousGeneration, adapterID, version string, present bool) httpapi.Status {
	t.Helper()
	var result httpapi.Status
	err := waitRuntimeConditionError(90*time.Second, func() bool {
		code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, httpapi.Endpoint, nil, &result)
		if err != nil || code != http.StatusOK || result.DefaultEngine == nil || result.ActiveControl == nil || result.DefaultEngine.ID == previousGeneration || result.ActiveControl.ID != result.DefaultEngine.ID {
			return false
		}
		return adapterListHas(t, client, baseURL, adapterID, version) == present
	}, "adapter set generation activation")
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("adapter generation did not activate before deadline: %v; status=%+v", ctx.Err(), result)
		}
		t.Fatalf("adapter set generation did not activate: %v; status=%+v; adapters=%s", err, result, boundedJSON(t, client, baseURL, "/api/adapters"))
	}
	return result
}

func adapterListHas(t *testing.T, client *http.Client, baseURL, adapterID, version string) bool {
	t.Helper()
	var adapters []lifecycleAdapterView
	code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/adapters", nil, &adapters)
	if err != nil || code != http.StatusOK {
		return false
	}
	for _, adapter := range adapters {
		id := adapter.Status.ID
		if adapter.Descriptor != nil {
			id = adapter.Descriptor.ID
		}
		if id == adapterID {
			return version == "" || (adapter.Descriptor != nil && adapter.Descriptor.Version == version)
		}
	}
	return false
}

func boundedJSON(t *testing.T, client *http.Client, baseURL, path string) string {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, baseURL+path, nil)
	response, err := client.Do(request)
	if err != nil {
		return "<unavailable>"
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return strings.TrimSpace(string(data))
}

func waitDirectChildForBinary(t *testing.T, parentPID int, binary string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		processes, err := runtimeProcessTable()
		if err == nil {
			for _, process := range processes {
				if process.Parent == parentPID && strings.Contains(process.Command, filepath.Clean(binary)) {
					return process.PID
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no child process for %s under PID %d", filepath.Base(binary), parentPID)
	return 0
}

func waitEngineOwningAdapter(t *testing.T, hostPID int, engineBinary, adapterBinary string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	enginePath := filepath.Clean(engineBinary)
	adapterPath := filepath.Clean(adapterBinary)
	for time.Now().Before(deadline) {
		processes, err := runtimeProcessTable()
		if err == nil {
			for _, engine := range processes {
				if engine.Parent != hostPID || !strings.Contains(engine.Command, enginePath) {
					continue
				}
				for _, child := range processes {
					if child.Parent == engine.PID && strings.Contains(child.Command, adapterPath) {
						return engine.PID
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no Runtime Host Engine process owns adapter process %s", filepath.Base(adapterBinary))
	return 0
}

func runtimePIDExists(pid int) bool {
	processes, err := runtimeProcessTable()
	if err != nil {
		return true
	}
	for _, process := range processes {
		if process.PID == pid {
			return true
		}
	}
	return false
}
