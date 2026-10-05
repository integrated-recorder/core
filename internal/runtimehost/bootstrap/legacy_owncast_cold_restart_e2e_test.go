package bootstrap

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/release"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
)

const legacyOwncastBaselineSHA = "3cf2c90280873f98f5120f67e39516c5af8abc04"
const legacyOwncastApplicationVersion = "0.1.0"

// TestLegacyBundledOwncastColdRestartE2E creates a completed archive with the
// historical Owncast 0.1.0 implementation, then cold-starts the current
// production Host on the same durable data directory. This is deliberately a
// process-level compatibility test: the old generation and its adapter set
// must remain readable without acquiring new trust metadata or being remapped
// to the bundled HLS reference adapter.
func TestLegacyBundledOwncastColdRestartE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires building and running historical and current production Runtime Host generations")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-tree assertions currently use the Unix ps interface")
	}

	moduleRoot := findRuntimeE2EModuleRoot(t)
	fixture := newLegacyOwncastFixture(t)
	old := buildLegacyOwncastBaseline(t, moduleRoot, fixture.server.URL)
	current := buildRuntimeAdapterLifecycleArtifacts(t, fixture.server.URL)

	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	diagnosticDir := filepath.Join(dataRoot, "child-diagnostics")
	legacySourceDir := filepath.Join(dataRoot, "legacy-adapters")
	for _, directory := range []string{dataDir, legacySourceDir, diagnosticDir} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatalf("create fresh legacy Runtime Host directory: %v", err)
		}
	}
	// Persist a signed release containing the exact historical binaries. An
	// image-bundled generation is ephemeral and cannot survive replacing the
	// image; a retained signed release is the supported cold-restart contract.
	trustedReleaseKeys := installLegacyOwncastRelease(t, moduleRoot, old, dataDir)
	legacySource := filepath.Join(legacySourceDir, "integrated-recorder-adapter-owncast")
	copyRuntimeArtifact(t, old.owncast, legacySource, 0555)
	t.Cleanup(func() { makeRuntimeE2ETreeWritable(dataDir) })

	listenAddr := reserveRuntimeAddress(t)
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 20 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	var oldProcess, currentProcess *runtimeHostProcess
	var oldImmutableAdapter string
	t.Cleanup(func() {
		stopRuntimeHostProcess(currentProcess)
		stopRuntimeHostProcess(oldProcess)
		if t.Failed() {
			if oldProcess != nil {
				t.Logf("historical Runtime Host output: %s", oldProcess.output.String())
			}
			if currentProcess != nil {
				t.Logf("current Runtime Host output: %s", currentProcess.output.String())
			}
			if output := readRuntimeE2EChildDiagnostics(diagnosticDir); output != "" {
				t.Logf("Runtime Host child diagnostics:%s", output)
			}
		}
		for _, executable := range []string{
			old.host, filepath.Join(old.bundle, "control-plane"), filepath.Join(old.bundle, "recorder-engine"), old.owncast,
			current.hostA, filepath.Join(current.bundleA, "control-plane"), filepath.Join(current.bundleA, "recorder-engine"),
			oldImmutableAdapter,
		} {
			if executable == "" {
				continue
			}
			if err := waitProcessAbsent(t, executable, 15*time.Second); err != nil {
				t.Errorf("acceptance cleanup left product process running for %s: %v", filepath.Base(executable), err)
			}
		}
	})

	oldProcess = startLegacyOwncastHost(t, old.host, dataDir, listenAddr, legacySourceDir, old.storageLocal, trustedReleaseKeys, diagnosticDir)
	oldStatus := waitRuntimeStatus(t, ctx, client, baseURL, func(status httpapi.Status) bool {
		return status.Host.Version == legacyOwncastApplicationVersion
	}, oldProcess)
	if oldStatus.ActiveControl == nil || oldStatus.DefaultEngine == nil {
		t.Fatalf("historical Runtime Host did not activate Control and Engine: %+v", oldStatus)
	}
	assertLegacyAdapterAvailable(t, client, baseURL)

	beforeRegistry := readRuntimeGenerationSnapshot(t, dataDir)
	oldGenerationID := oldStatus.DefaultEngine.ID
	oldGeneration, ok := beforeRegistry.Generations[oldGenerationID]
	if !ok || oldGeneration.AdapterSetID == "" {
		t.Fatalf("historical generation did not pin an Owncast adapter set: generation=%+v registry=%+v", oldGeneration, beforeRegistry)
	}
	oldSetID := oldGeneration.AdapterSetID
	oldSetDir := filepath.Join(dataDir, "runtime", "adapters", "sets", oldSetID)
	oldSetManifestPath := filepath.Join(oldSetDir, "adapter-set.json")
	oldSetManifest, err := os.ReadFile(oldSetManifestPath)
	if err != nil {
		t.Fatalf("read historical adapter set manifest: %v", err)
	}
	assertLegacyManifestHasNoAttestation(t, oldSetManifest)
	oldImmutableAdapter = filepath.Join(oldSetDir, "bin", "integrated-recorder-adapter-owncast")
	oldArtifactSHA := legacyOwncastFileSHA(t, oldImmutableAdapter)
	if info, err := os.Stat(oldImmutableAdapter); err != nil || info.Mode().Perm()&0222 != 0 {
		t.Fatalf("historical adapter artifact is not immutable: mode=%v err=%v", modeOrZero(info), err)
	}

	recording := createRuntimeRecordingWithAdapter(t, client, baseURL, "owncast", "Legacy Owncast archive", map[string]string{
		"source_url": fixture.server.URL,
	})
	if recording.ID == "" {
		t.Fatal("historical Control API returned an empty recording ID")
	}
	waitRecordingSequenceCount(t, client, baseURL, recording.ID, 5, 40*time.Second)
	metadataBefore := waitMetadataTimeline(t, ctx, client, baseURL, recording.ID, 1, 30*time.Second)
	if got := stringValue(metadataBefore.Items[0].Title); got != fixture.title {
		t.Fatalf("historical Owncast metadata title=%q, want %q", got, fixture.title)
	}
	stopRecording(t, client, baseURL, recording.ID)
	waitRuntimeRecordingState(t, client, baseURL, recording.ID, domain.StateStopped, 25*time.Second)
	completedBefore := getRecording(t, client, baseURL, recording.ID)
	assertLegacyOwncastRecording(t, completedBefore, true)
	oldRecordingIdentity := legacyRecordingIdentity(completedBefore)
	verifyLegacyOwncastVOD(t, client, baseURL, completedBefore, false)
	if _, exists := readRuntimeGenerationSnapshot(t, dataDir).Leases[recording.ID]; exists {
		t.Fatalf("completed legacy recording unexpectedly retains an Engine lease: %s", recording.ID)
	}
	if beforeRegistry.ActiveGenerationID != oldGenerationID {
		t.Fatalf("historical generation changed during archive creation: before=%s active=%s", oldGenerationID, beforeRegistry.ActiveGenerationID)
	}
	oldHostPID := oldProcess.command.Process.Pid
	stopRuntimeHostProcess(oldProcess)
	if err := waitProcessAbsent(t, old.host, 20*time.Second); err != nil {
		t.Fatalf("historical Runtime Host did not stop before cold restart: %v; output=%s", err, oldProcess.output.String())
	}
	if processForBinary(old.host) == oldHostPID {
		t.Fatalf("historical Host PID %d survived its shutdown", oldHostPID)
	}

	// The historical Engine is intentionally not relaunched after upgrade: this
	// acceptance covers completed archive and immutable artifact compatibility,
	// not the documented fail-stop recovery policy for active old recordings.
	currentGenerationID := stageLegacyArchiveCurrentGeneration(t, dataDir, current)
	assertLegacySignedReleaseRetained(t, dataDir, trustedReleaseKeys)

	// The current production builder supplies the new bundled HLS and
	// storage.local binaries. Its operator source directory is intentionally
	// empty; the old Owncast executable must be retained only by its immutable
	// historical generation, never re-imported as a new selectable adapter.
	currentProcess = startLegacyOwncastHost(t, current.hostA, dataDir, listenAddr, current.adapterDir, current.storageLocalBinary, trustedReleaseKeys, diagnosticDir)
	currentStatus := waitRuntimeHostStatus(t, ctx, client, baseURL, currentProcess)
	if currentStatus.ActiveControl == nil || currentStatus.DefaultEngine == nil || currentStatus.DefaultEngine.ID != currentGenerationID {
		t.Fatalf("current Core did not activate a new HLS-backed generation: old=%s status=%+v", oldGenerationID, currentStatus)
	}
	currentHostPID := currentProcess.command.Process.Pid
	if currentHostPID == oldHostPID {
		t.Fatalf("Runtime Host process PID was reused across the cold restart: %d", currentHostPID)
	}
	assertCurrentHLSOnly(t, client, baseURL)

	afterRegistry := readRuntimeGenerationSnapshot(t, dataDir)
	if afterRegistry.ActiveGenerationID != currentGenerationID || afterRegistry.PreviousGenerationID != oldGenerationID {
		t.Fatalf("cold start did not retain old generation as the rollback target: active=%s previous=%s old=%s registry=%+v", afterRegistry.ActiveGenerationID, afterRegistry.PreviousGenerationID, oldGenerationID, afterRegistry)
	}
	oldGenerationAfterUpgrade, ok := afterRegistry.Generations[oldGenerationID]
	if !ok || oldGenerationAfterUpgrade.State != generation.StateDraining || !oldGenerationAfterUpgrade.EngineDormant {
		t.Fatalf("completed old generation was not retained as a dormant rollback target: %+v", oldGenerationAfterUpgrade)
	}
	if _, exists := afterRegistry.Leases[recording.ID]; exists {
		t.Fatalf("stopped legacy recording unexpectedly acquired a new Engine lease during restart: %s", recording.ID)
	}
	currentCatalog, err := adaptercatalog.Open(filepath.Join(dataDir, "runtime", "adapters"), nil)
	if err != nil {
		t.Fatalf("open current adapter catalog for retained old set: %v", err)
	}
	legacySet, err := currentCatalog.Load(oldSetID)
	if err != nil {
		t.Fatalf("current Core cannot load the old immutable Owncast set %s: %v", oldSetID, err)
	}
	if len(legacySet.Entries) != 1 || legacySet.Entries[0].AdapterID != "owncast" || legacySet.Entries[0].Version != "0.1.0" || legacySet.Entries[0].Attestation != nil || legacySet.Entries[0].EffectiveAttestation().Provenance != plugintrust.LegacyUnclassified {
		t.Fatalf("historical Owncast set was remapped or trust-promoted: %+v", legacySet.Entries)
	}
	if !bytes.Equal(oldSetManifest, mustReadLegacyFile(t, oldSetManifestPath)) {
		t.Fatal("historical adapter-set manifest was rewritten during cold start")
	}
	if got := legacyOwncastFileSHA(t, oldImmutableAdapter); got != oldArtifactSHA {
		t.Fatalf("historical immutable adapter changed after cold start: before=%s after=%s", oldArtifactSHA, got)
	}
	if info, err := os.Stat(oldImmutableAdapter); err != nil || info.Mode().Perm()&0222 != 0 {
		t.Fatalf("retained historical adapter is missing or writable: mode=%v err=%v", modeOrZero(info), err)
	}
	currentOldRecording := getRecording(t, client, baseURL, recording.ID)
	assertLegacyOwncastRecording(t, currentOldRecording, true)
	if !bytes.Equal(oldRecordingIdentity, legacyRecordingIdentity(currentOldRecording)) {
		t.Fatal("completed archive identity, source sequences, ordinals, or hashes changed across Core cold restart")
	}
	metadataAfter := waitMetadataTimeline(t, ctx, client, baseURL, recording.ID, len(metadataBefore.Items), 20*time.Second)
	if !bytes.Equal(mustMarshalLegacy(t, metadataBefore.Items), mustMarshalLegacy(t, metadataAfter.Items)) {
		t.Fatalf("metadata timeline changed across cold restart: before=%+v after=%+v", metadataBefore.Items, metadataAfter.Items)
	}
	verifyLegacyOwncastVOD(t, client, baseURL, currentOldRecording, true)
	verifyLegacyOwncastIntegrity(t, client, baseURL, recording.ID)

	t.Logf("legacy Owncast cold restart passed: recording=%s old_generation=%s old_adapter_set=%s artifact_sha256=%s new_generation=%s segments=%d; archive root stayed %s", recording.ID, oldGenerationID, oldSetID, oldArtifactSHA, currentStatus.DefaultEngine.ID, len(currentOldRecording.Tracks["main"].Segments), filepath.Join(dataDir, "recordings"))
}

func stageLegacyArchiveCurrentGeneration(t *testing.T, dataDir string, current runtimeAdapterLifecycleArtifacts) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	adapterCatalog, err := adaptercatalog.Open(filepath.Join(dataDir, "runtime", "adapters"), nil)
	if err != nil {
		t.Fatalf("open production adapter catalog to stage upgraded HLS generation: %v", err)
	}
	hlsPath := filepath.Join(current.root, "bundled-adapters", "integrated-recorder-adapter-hls")
	adapterSet, err := adapterCatalog.ReconcileClassified(ctx, "", []adaptercatalog.Source{{
		Path: hlsPath, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"},
	}})
	if err != nil || adapterSet.ID == "" || len(adapterSet.Entries) != 1 || adapterSet.Entries[0].AdapterID != "hls" {
		t.Fatalf("production adapter catalog could not publish exact bundled HLS set: set=%+v err=%v", adapterSet, err)
	}
	storageCatalog, err := storagecatalog.Open(filepath.Join(dataDir, "runtime", "storage-providers"))
	if err != nil {
		t.Fatalf("open production storage catalog to stage upgraded local generation: %v", err)
	}
	localSet, err := ensureBundledLocalStorage(ctx, storageCatalog, current.storageLocalBinary, filepath.Join(dataDir, "recordings"))
	if err != nil {
		t.Fatalf("production storage catalog could not publish exact bundled storage.local set: %v", err)
	}
	registry, err := generation.Open(filepath.Join(dataDir, "runtime", "state", "generations.json"))
	if err != nil {
		t.Fatalf("open production generation registry for post-upgrade activation: %v", err)
	}
	before := registry.Snapshot()
	if before.ActiveGenerationID == "" || len(before.Leases) != 0 {
		t.Fatalf("post-upgrade setup requires a completed recording and a single old active generation: %+v", before)
	}
	id, err := newGenerationID()
	if err != nil {
		t.Fatalf("allocate current application generation ID: %v", err)
	}
	candidate := runtimeGeneration(id, buildinfo.Info{Version: e2eVersionA, Commit: e2eCommitA})
	candidate.AdapterSetID = adapterSet.ID
	candidate.StorageProviderSetID = localSet.ID
	if err := registry.Stage(candidate); err != nil {
		t.Fatalf("stage current production generation: %v", err)
	}
	for _, transition := range []struct {
		name string
		fn   func(string) error
	}{
		{name: "verify", fn: registry.MarkVerified},
		{name: "ready", fn: registry.MarkReady},
	} {
		if err := transition.fn(id); err != nil {
			t.Fatalf("mark current production generation %s: %v", transition.name, err)
		}
	}
	if err := registry.Activate(id); err != nil {
		t.Fatalf("activate current production generation: %v", err)
	}
	if err := registry.FinalizeActivation(id); err != nil {
		t.Fatalf("finalize current production generation activation: %v", err)
	}
	if err := registry.MarkEngineDormant(before.ActiveGenerationID); err != nil {
		t.Fatalf("mark completed historical Engine dormant after Host stop: %v", err)
	}
	after := registry.Snapshot()
	if after.ActiveGenerationID != id || after.PreviousGenerationID != before.ActiveGenerationID || len(after.Leases) != 0 {
		t.Fatalf("production generation transitions did not preserve the old rollback target: before=%+v after=%+v", before, after)
	}
	return id
}

func assertLegacySignedReleaseRetained(t *testing.T, dataDir, trustedKeysJSON string) {
	t.Helper()
	keys, err := parseTrustedReleaseKeys(trustedKeysJSON)
	if err != nil {
		t.Fatalf("decode test public release key: %v", err)
	}
	installed, err := install.InspectInstalledRelease(filepath.Join(dataDir, "runtime"), install.ReleaseDirectoryID(legacyOwncastApplicationVersion, legacyOwncastBaselineSHA), keys, currentHostCompatibility())
	if err != nil {
		t.Fatalf("historical signed application release is not retained for rollback: %v", err)
	}
	if installed.Manifest.ReleaseVersion != legacyOwncastApplicationVersion || !strings.EqualFold(installed.Manifest.Commit, legacyOwncastBaselineSHA) {
		t.Fatalf("retained historical release does not match old generation identity: %+v", installed.Manifest)
	}
	for _, role := range []string{release.RoleControlPlane, release.RoleRecorderEngine} {
		if _, err := installed.ValidateExecutableRole(role); err != nil {
			t.Fatalf("retained historical signed release is missing runnable role %s: %v", role, err)
		}
	}
	for _, artifact := range installed.Manifest.Artifacts {
		path := filepath.Join(installed.Directory, artifact.Filename)
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != artifact.Size || legacyOwncastFileSHA(t, path) != artifact.SHA256 {
			t.Fatalf("retained historical signed release artifact %s failed byte identity verification: info=%v err=%v", artifact.Role, info, statErr)
		}
	}
}

type legacyOwncastArtifacts struct {
	root           string
	bundle         string
	host           string
	storageLocal   string
	owncast        string
	adapterRuntime string
}

func buildLegacyOwncastBaseline(t *testing.T, moduleRoot, fixtureOrigin string) legacyOwncastArtifacts {
	t.Helper()
	const sha = legacyOwncastBaselineSHA
	root := newRuntimeE2ETempDir(t)
	tree := filepath.Join(root, "historical-core")
	if err := os.Mkdir(tree, 0700); err != nil {
		t.Fatal(err)
	}
	extractLegacyOwncastGitArchive(t, moduleRoot, sha, tree)
	bin := filepath.Join(root, "bin")
	bundle := filepath.Join(root, "initial-legacy")
	for _, dir := range []string{bin, bundle} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	legacyWrapper := filepath.Join(tree, "cmd", "adapters", "owncast-legacy-cold-restart")
	if err := os.Mkdir(legacyWrapper, 0700); err != nil {
		t.Fatalf("create isolated historical adapter wrapper: %v", err)
	}
	wrapperPath := filepath.Join(legacyWrapper, "main.go")
	if err := os.WriteFile(wrapperPath, []byte(legacyOwncastTestWrapper), 0600); err != nil {
		t.Fatalf("write isolated historical adapter wrapper: %v", err)
	}
	ldflags := strings.Join([]string{
		"-X github.com/integrated-recorder/core/internal/buildinfo.version=" + legacyOwncastApplicationVersion,
		"-X github.com/integrated-recorder/core/internal/buildinfo.commit=" + sha,
		"-X github.com/integrated-recorder/core/internal/buildinfo.buildTime=2026-09-30T00:00:00Z",
		"-X github.com/integrated-recorder/core/internal/buildinfo.releaseChannel=prerelease",
	}, " ")
	build := func(output, pkg, tags, flags string) string {
		t.Helper()
		args := []string{"build", "-mod=readonly", "-o", output}
		if tags != "" {
			args = append(args, "-tags", tags)
		}
		if flags != "" {
			args = append(args, "-ldflags", flags)
		}
		args = append(args, pkg)
		runBuildCommand(t, tree, 4*time.Minute, args...)
		return output
	}
	control := build(filepath.Join(bin, "control-plane-legacy"), "./cmd/control-plane", "", ldflags)
	engineFlags := ldflags + " -X main.runtimeE2EFixtureOrigin=" + fixtureOrigin
	engine := build(filepath.Join(bin, "recorder-engine-legacy"), "./cmd/recorder-engine", "runtime_e2e", engineFlags)
	storageLocal := build(filepath.Join(bin, "storage.local-legacy"), "./cmd/storage-local", "", ldflags)
	adapterRuntime := build(filepath.Join(bin, "adapter-runtime-legacy"), "./cmd/adapters/owncast", "", "")
	owncast := build(filepath.Join(bin, "integrated-recorder-adapter-owncast-legacy"), "./cmd/adapters/owncast-legacy-cold-restart", "", "-X main.fixtureOrigin="+fixtureOrigin)
	copyRuntimeArtifact(t, control, filepath.Join(bundle, "control-plane"), 0555)
	copyRuntimeArtifact(t, engine, filepath.Join(bundle, "recorder-engine"), 0555)
	if err := os.Chmod(bundle, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(bundle, 0700)
		for _, name := range []string{"control-plane", "recorder-engine"} {
			_ = os.Chmod(filepath.Join(bundle, name), 0600)
		}
	})
	hostFlags := ldflags + " -X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundleDir=" + bundle
	host := build(filepath.Join(bin, "runtime-host-legacy"), "./cmd/runtime-host", "runtime_e2e", hostFlags)
	return legacyOwncastArtifacts{root: root, bundle: bundle, host: host, storageLocal: storageLocal, owncast: owncast, adapterRuntime: adapterRuntime}
}

func extractLegacyOwncastGitArchive(t *testing.T, moduleRoot, sha, destination string) {
	t.Helper()
	cmd := exec.Command("git", "archive", "--format=tar", sha)
	cmd.Dir = moduleRoot
	archivePath := filepath.Join(filepath.Dir(destination), "historical-core.tar")
	archiveFile, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("create temporary historical Core archive: %v", err)
	}
	cmd.Stdout = archiveFile
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	closeErr := archiveFile.Close()
	if err != nil {
		t.Fatalf("historical Core commit %s is required for this acceptance test: %v\n%s", sha, err, boundedOutput(stderr.Bytes()))
	}
	if closeErr != nil {
		t.Fatalf("close historical Core source archive: %v", closeErr)
	}
	cmd = exec.Command("tar", "-xf", archivePath, "-C", destination)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("extract historical Core git archive: %v\n%s", err, boundedOutput(output))
	}
}

func startLegacyOwncastHost(t *testing.T, executable, dataDir, listenAddr, adapterDir, storageLocal, trustedKeys, diagnosticDir string) *runtimeHostProcess {
	t.Helper()
	command := exec.Command(executable)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"ADAPTER_DIR=" + adapterDir,
		"IR_STORAGE_LOCAL_PLUGIN=" + storageLocal,
		"IR_RELEASE_TRUSTED_KEYS_JSON=" + trustedKeys,
		"IR_RUNTIME_E2E_FAILPOINT=after_source_drain",
		"IR_RUNTIME_E2E_MARKER_DIR=" + diagnosticDir,
		"AUTH_DISABLED=1",
	})
	process := &runtimeHostProcess{command: command, done: make(chan struct{})}
	command.Stdout, command.Stderr = &process.output, &process.output
	if err := command.Start(); err != nil {
		t.Fatalf("start Runtime Host process %s: %v", filepath.Base(executable), err)
	}
	go func() {
		process.err = command.Wait()
		close(process.done)
	}()
	return process
}

func installLegacyOwncastRelease(t *testing.T, moduleRoot string, old legacyOwncastArtifacts, dataDir string) string {
	t.Helper()
	bin := filepath.Join(old.root, "signed-release-tools")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatalf("create signed legacy release tooling directory: %v", err)
	}
	packager := filepath.Join(bin, "release-pack")
	runBuildCommand(t, moduleRoot, 4*time.Minute, "build", "-mod=readonly", "-o", packager, "./cmd/release-pack")
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("create ephemeral test release signing key: %v", err)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	const keyID = "legacy-owncast-cold-restart"
	publicKeyJSON, err := json.Marshal([]map[string]string{{
		"key_id": keyID, "public_key_base64": base64.StdEncoding.EncodeToString(publicKey),
	}})
	if err != nil {
		t.Fatalf("encode ephemeral test release public key: %v", err)
	}
	packageParent := filepath.Join(old.root, "signed-releases")
	if err := os.Mkdir(packageParent, 0700); err != nil {
		t.Fatalf("create signed release package directory: %v", err)
	}
	packageDir := filepath.Join(packageParent, "legacy")
	packageReleaseWithProductionTool(t, moduleRoot, packager, base64.StdEncoding.EncodeToString(privateKey), packageDir,
		legacyOwncastApplicationVersion, legacyOwncastBaselineSHA,
		old.host, filepath.Join(old.bundle, "control-plane"), filepath.Join(old.bundle, "recorder-engine"), old.adapterRuntime, keyID)
	installer, err := install.NewInstaller(filepath.Join(dataDir, "runtime"), map[string]ed25519.PublicKey{keyID: publicKey}, currentHostCompatibility())
	if err != nil {
		t.Fatalf("create production release installer for historical test release: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	staged, err := installer.Stage(ctx, install.LocalDirectorySource{Directory: packageDir})
	if err != nil {
		t.Fatalf("install exact historical binaries as a signed immutable release: %v", err)
	}
	if !staged.Verified || staged.Version != legacyOwncastApplicationVersion || !strings.EqualFold(staged.Commit, legacyOwncastBaselineSHA) || staged.ArtifactCount != 4 {
		t.Fatalf("historical signed release identity differs from old Host generation: %+v", staged)
	}
	return string(publicKeyJSON)
}

type legacyOwncastFixture struct {
	server    *httptest.Server
	title     string
	startedAt string
}

func newLegacyOwncastFixture(t *testing.T) *legacyOwncastFixture {
	t.Helper()
	fixture := &legacyOwncastFixture{
		title:     "Historical Owncast broadcast",
		startedAt: "2026-10-01T12:30:00Z",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		writeRuntimeJSON(w, map[string]any{"online": true, "lastConnectTime": fixture.startedAt, "streamTitle": fixture.title})
	})
	mux.HandleFunc("GET /hls/stream.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n")
		for sequence := uint64(1); sequence <= 12; sequence++ {
			_, _ = fmt.Fprintf(w, "#EXTINF:1.0,\n/hls/segments/%06d.ts\n", sequence)
		}
	})
	mux.HandleFunc("GET /hls/segments/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/hls/segments/")
		sequence, err := strconv.ParseUint(strings.TrimSuffix(name, ".ts"), 10, 64)
		if err != nil || sequence == 0 || sequence > 12 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(legacyOwncastSegmentPayload(sequence))
	})
	fixture.server = httptest.NewServer(mux)
	t.Cleanup(fixture.server.Close)
	return fixture
}

func legacyOwncastSegmentPayload(sequence uint64) []byte {
	return []byte(fmt.Sprintf("legacy-owncast-segment-%06d::original-media-bytes\n", sequence))
}

func assertLegacyAdapterAvailable(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	var adapters []struct {
		Descriptor *struct {
			ID              string `json:"id"`
			Version         string `json:"version"`
			ProtocolVersion int    `json:"protocol_version"`
		} `json:"descriptor"`
	}
	code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/adapters", nil, &adapters)
	if err != nil || code != http.StatusOK {
		t.Fatalf("historical GET /api/adapters returned code=%d err=%v", code, err)
	}
	for _, adapter := range adapters {
		if adapter.Descriptor != nil && adapter.Descriptor.ID == "owncast" {
			if adapter.Descriptor.Version != "0.1.0" || adapter.Descriptor.ProtocolVersion != 1 {
				t.Fatalf("historical Owncast descriptor identity changed: %+v", adapter.Descriptor)
			}
			return
		}
	}
	t.Fatalf("historical Runtime Host did not discover Owncast 0.1.0: %+v", adapters)
}

func assertCurrentHLSOnly(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	var adapters []struct {
		Descriptor *struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"descriptor"`
		Trust *struct {
			Provenance string `json:"provenance"`
			Authority  string `json:"authority"`
			Publisher  string `json:"publisher"`
			Reviewed   bool   `json:"reviewed"`
		} `json:"trust"`
	}
	code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/adapters", nil, &adapters)
	if err != nil || code != http.StatusOK {
		t.Fatalf("current GET /api/adapters returned code=%d err=%v", code, err)
	}
	foundHLS := false
	for _, adapter := range adapters {
		if adapter.Descriptor == nil {
			continue
		}
		if adapter.Descriptor.ID == "owncast" {
			t.Fatalf("historical Owncast was exposed as a newly selectable current adapter: %+v", adapter)
		}
		if adapter.Descriptor.ID == "hls" {
			foundHLS = true
			if adapter.Trust == nil || adapter.Trust.Provenance != "bundled" || adapter.Trust.Authority != "core_release" || adapter.Trust.Publisher != "first_party" || !adapter.Trust.Reviewed {
				t.Fatalf("current HLS adapter lacks Host-assigned bundled provenance: %+v", adapter.Trust)
			}
		}
	}
	if !foundHLS {
		t.Fatalf("current generation does not expose its bundled HLS reference adapter: %+v", adapters)
	}
}

func assertLegacyOwncastRecording(t *testing.T, recording *domain.Recording, checkHashes bool) {
	t.Helper()
	if recording == nil || recording.ID == "" || recording.AdapterID != "owncast" || recording.State != domain.StateStopped || recording.Title != "Legacy Owncast archive" {
		t.Fatalf("legacy recording identity/state changed: %+v", recording)
	}
	if recording.Adapter == nil || recording.Adapter.ID != "owncast" || recording.Adapter.Version != "0.1.0" || recording.Adapter.ProtocolVersion != 1 {
		t.Fatalf("legacy recording adapter provenance changed: %+v", recording.Adapter)
	}
	track := recording.Tracks["main"]
	if track == nil || len(track.Segments) < 5 || len(recording.Gaps) != 0 {
		t.Fatalf("legacy recording archive is incomplete or contains gaps: track=%+v gaps=%+v", track, recording.Gaps)
	}
	sequences := recordingSequences(recording)
	if !equalSequenceRange(sequences, 1, uint64(len(track.Segments))) {
		t.Fatalf("legacy recording source sequence is not continuous: %v", sequences)
	}
	seenOrdinals := make(map[uint64]struct{}, len(track.Segments))
	for _, segment := range track.Segments {
		if _, exists := seenOrdinals[segment.ArchiveOrdinal]; exists {
			t.Fatalf("legacy archive contains duplicate ordinal %d", segment.ArchiveOrdinal)
		}
		seenOrdinals[segment.ArchiveOrdinal] = struct{}{}
		payload := legacyOwncastSegmentPayload(segment.Sequence)
		digest := sha256.Sum256(payload)
		if checkHashes && (segment.PayloadSize != int64(len(payload)) || segment.SHA256 != hex.EncodeToString(digest[:])) {
			t.Fatalf("legacy source payload identity changed for sequence %d: size=%d/%d sha=%s/%s", segment.Sequence, segment.PayloadSize, len(payload), segment.SHA256, hex.EncodeToString(digest[:]))
		}
	}
}

func verifyLegacyOwncastVOD(t *testing.T, client *http.Client, baseURL string, recording *domain.Recording, checkRange bool) {
	t.Helper()
	masterPath := "/api/recordings/" + recording.ID + "/play/master.m3u8"
	master, code := getRuntimeBody(t, client, baseURL, masterPath)
	trackPath := "/api/recordings/" + recording.ID + "/play/tracks/main/playlist.m3u8"
	if code != http.StatusOK || !strings.Contains(string(master), trackPath) {
		t.Fatalf("legacy VOD master could not resolve the old archive: status=%d body=%q", code, master)
	}
	playlist, code := getRuntimeBody(t, client, baseURL, trackPath)
	if code != http.StatusOK {
		t.Fatalf("legacy VOD track playlist returned %d: %q", code, playlist)
	}
	segmentByID := make(map[string]domain.Segment, len(recording.Tracks["main"].Segments))
	for _, segment := range recording.Tracks["main"].Segments {
		segmentByID[segment.ID] = segment
	}
	var routes []string
	for _, line := range strings.Split(string(playlist), "\n") {
		line = strings.TrimSpace(line)
		prefix := "/api/recordings/" + recording.ID + "/play/segments/"
		if strings.HasPrefix(line, prefix) {
			routes = append(routes, line)
		}
	}
	if len(routes) != len(recording.Tracks["main"].Segments) {
		t.Fatalf("legacy VOD playlist routes=%d, archive segments=%d: %q", len(routes), len(recording.Tracks["main"].Segments), playlist)
	}
	indices := []int{0, len(routes) / 2, len(routes) - 1}
	for _, index := range indices {
		route := routes[index]
		segmentID := strings.TrimPrefix(route, "/api/recordings/"+recording.ID+"/play/segments/")
		segment, exists := segmentByID[segmentID]
		if !exists {
			t.Fatalf("VOD route refers to an unknown archived segment %q", route)
		}
		body, status := getRuntimeBody(t, client, baseURL, route)
		want := legacyOwncastSegmentPayload(segment.Sequence)
		if status != http.StatusOK || !bytes.Equal(body, want) {
			t.Fatalf("legacy VOD payload differs from original source sequence %d: status=%d got=%q want=%q", segment.Sequence, status, body, want)
		}
		if checkRange && index == indices[1] {
			verifyLegacyOwncastRange(t, client, baseURL, route, want)
		}
	}
}

func verifyLegacyOwncastRange(t *testing.T, client *http.Client, baseURL, route string, payload []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, baseURL+route, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Range", "bytes=0-7")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("range read from legacy VOD segment: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1024))
	if err != nil {
		t.Fatalf("read legacy VOD range: %v", err)
	}
	if response.StatusCode != http.StatusPartialContent || response.Header.Get("Content-Range") != fmt.Sprintf("bytes 0-7/%d", len(payload)) || !bytes.Equal(body, payload[:8]) {
		t.Fatalf("legacy VOD range mismatch: status=%d range=%q body=%q", response.StatusCode, response.Header.Get("Content-Range"), body)
	}
}

func verifyLegacyOwncastIntegrity(t *testing.T, client *http.Client, baseURL, recordingID string) {
	t.Helper()
	var job map[string]any
	code, err := requestRuntimeJSON(client, baseURL, http.MethodPost, "/api/recordings/"+recordingID+"/integrity/verify", map[string]any{}, &job)
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("request current-Core integrity verification returned code=%d err=%v body=%v", code, err, job)
	}
	jobID, _ := job["id"].(string)
	if jobID == "" {
		t.Fatalf("current Core returned an empty integrity job ID: %v", job)
	}
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		var current map[string]any
		code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/integrity/jobs/"+jobID, nil, &current)
		if err == nil && code == http.StatusOK {
			state, _ := current["state"].(string)
			if state == "completed" {
				integrity, code := getRuntimeJSON[map[string]any](t, client, baseURL, http.MethodGet, "/api/recordings/"+recordingID+"/integrity", nil)
				if code != http.StatusOK || integrity["status"] != "verified" {
					t.Fatalf("current-Core integrity result is not verified: status=%d result=%v", code, integrity)
				}
				return
			}
			if state == "failed" || state == "canceled" {
				t.Fatalf("current-Core integrity verification failed: %v", current)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("current-Core integrity verification did not complete within 25 seconds for %s", recordingID)
}

func assertLegacyManifestHasNoAttestation(t *testing.T, data []byte) {
	t.Helper()
	var manifest struct {
		SchemaVersion int               `json:"schema_version"`
		Entries       []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil || len(manifest.Entries) != 1 {
		t.Fatalf("historical adapter-set manifest is invalid: entries=%d err=%v", len(manifest.Entries), err)
	}
	var entry map[string]json.RawMessage
	if err := json.Unmarshal(manifest.Entries[0], &entry); err != nil {
		t.Fatalf("decode historical adapter-set entry: %v", err)
	}
	if _, exists := entry["attestation"]; exists {
		t.Fatalf("historical adapter-set unexpectedly has a trust attestation: %s", data)
	}
}

func legacyRecordingIdentity(recording *domain.Recording) []byte {
	if recording == nil {
		return nil
	}
	identity := struct {
		ID        string                    `json:"id"`
		Title     string                    `json:"title"`
		AdapterID string                    `json:"adapter_id"`
		Adapter   *domain.AdapterProvenance `json:"adapter"`
		State     domain.RecordingState     `json:"state"`
		Segments  []domain.Segment          `json:"segments"`
		Gaps      []domain.Gap              `json:"gaps"`
	}{ID: recording.ID, Title: recording.Title, AdapterID: recording.AdapterID, Adapter: recording.Adapter, State: recording.State, Gaps: recording.Gaps}
	if track := recording.Tracks["main"]; track != nil {
		identity.Segments = track.Segments
	}
	data, _ := json.Marshal(identity)
	return data
}

func legacyOwncastFileSHA(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open immutable historical Owncast artifact: %v", err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("hash historical Owncast artifact: copy=%v close=%v", copyErr, closeErr)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func mustReadLegacyFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy immutable file %s: %v", filepath.Base(path), err)
	}
	return data
}

func modeOrZero(info os.FileInfo) os.FileMode {
	if info == nil {
		return 0
	}
	return info.Mode()
}

func mustMarshalLegacy(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal compatibility value for comparison: %v", err)
	}
	return data
}

// legacyOwncastTestWrapper is compiled only under a temp git-archive source
// tree. It exercises the exact historical Owncast package and protocol types,
// replacing only its public-network HTTP dependencies with a test-only client
// pinned to the one local fixture origin. The strict validator, DialContext,
// and redirect policy make any non-fixture network destination unreachable.
const legacyOwncastTestWrapper = `package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/adapters/owncast"
)

var fixtureOrigin string

func main() {
	client, validate, err := fixtureHTTP()
	if err != nil { log.Printf("legacy adapter test fixture setup failed: %v", err); os.Exit(1) }
	if err := serve(client, validate); err != nil { log.Printf("legacy adapter test fixture stopped: %v", err) }
}

func fixtureHTTP() (*http.Client, func(context.Context, string) error, error) {
	origin, err := url.Parse(fixtureOrigin)
	if err != nil || origin.Scheme != "http" || origin.Hostname() != "127.0.0.1" || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" {
		return nil, nil, errors.New("fixture origin must be a single loopback HTTP origin")
	}
	port := origin.Port()
	if port == "" || "http://"+origin.Host != fixtureOrigin { return nil, nil, errors.New("fixture origin is not canonical") }
	dialer := &net.Dialer{Timeout: 3*time.Second, KeepAlive: 10*time.Second}
	transport := &http.Transport{Proxy:nil, MaxConnsPerHost:2, MaxIdleConnsPerHost:1, ResponseHeaderTimeout:3*time.Second, IdleConnTimeout:5*time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, destinationPort, splitErr := net.SplitHostPort(address)
			if splitErr != nil || network != "tcp" || host != "127.0.0.1" || destinationPort != port { return nil, errors.New("refused non-fixture network destination") }
			return dialer.DialContext(ctx, network, address)
		},
	}
	client := &http.Client{Transport:transport, Timeout:5*time.Second, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) >= 2 || validateFixtureURL(request.URL, origin) != nil { return errors.New("refused redirect outside fixture origin") }
		return nil
	}}
	return client, func(_ context.Context, raw string) error {
		u, parseErr := url.ParseRequestURI(raw)
		if parseErr != nil || validateFixtureURL(u, origin) != nil { return errors.New("refused URL outside fixture origin") }
		return nil
	}, nil
}

func validateFixtureURL(u, origin *url.URL) error {
	if u == nil || origin == nil || u.Scheme != "http" || u.Host != origin.Host || u.Hostname() != "127.0.0.1" || u.User != nil || u.Fragment != "" || u.Path != "/api/status" || u.RawQuery != "" { return errors.New("URL is outside exact Owncast fixture endpoint") }
	return nil
}

func serve(client *http.Client, validate func(context.Context, string) error) error {
	reader := bufio.NewReaderSize(os.Stdin, 32<<10)
	for {
		request, err := adapterproto.ReadRequest(reader)
		if err == io.EOF { return nil }
		if err != nil {
			id := request.ID; if id == "" { id = "0" }
			code := "malformed_request"
			if strings.Contains(err.Error(), "unsupported protocol version") { code = "unsupported_protocol_version" }
			_ = adapterproto.WriteResponse(os.Stdout, adapterproto.Failure(id, code, "invalid adapter request", nil))
			if code == "malformed_request" { return nil }
			continue
		}
		var response adapterproto.Response
		switch request.Method {
		case adapterproto.MethodDescribe:
			response, _ = adapterproto.Success(request.ID, owncast.Describe())
		case adapterproto.MethodResolve:
			var params adapterproto.ResolveParams
			if err = json.Unmarshal(request.Params, &params); err != nil { response = adapterproto.Failure(request.ID, "invalid_params", "resolve params are invalid", nil); break }
			media, resolveErr := owncast.Resolve(params.Input)
			if resolveErr != nil { response = adapterproto.Failure(request.ID, "invalid_input", "input is invalid", nil) } else { response, _ = adapterproto.Success(request.ID, media) }
		case adapterproto.MethodWatchCheck:
			var params adapterproto.WatchCheckParams
			if err = json.Unmarshal(request.Params, &params); err != nil { response = adapterproto.Failure(request.ID, "invalid_params", "watch check params are invalid", nil); break }
			result, checkErr := owncast.WatchCheckWith(params.Input, client, validate)
			if checkErr != nil { code := "watch_check_failed"; if checkErr.Error() == "status service requires attention" { code = "authentication_required" }; response = adapterproto.Failure(request.ID, code, "status check failed", nil) } else { response, _ = adapterproto.Success(request.ID, result) }
		case adapterproto.MethodMetadata:
			var params adapterproto.MetadataParams
			if err = json.Unmarshal(request.Params, &params); err != nil { response = adapterproto.Failure(request.ID, "invalid_params", "metadata params are invalid", nil); break }
			result, metadataErr := owncast.MetadataWith(params.Current, client, validate)
			if metadataErr != nil { response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil) } else { response, _ = adapterproto.Success(request.ID, result) }
		case adapterproto.MethodShutdown:
			response, _ = adapterproto.Success(request.ID, map[string]bool{"stopped":true})
		default:
			response = adapterproto.Failure(request.ID, "unsupported_method", "method is not supported", map[string]any{"method":request.Method})
		}
		if err := adapterproto.WriteResponse(os.Stdout, response); err != nil { return err }
		if request.Method == adapterproto.MethodShutdown { return nil }
	}
}
`
