package bootstrap

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimehost/release"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	runtimeE2EAdapterID = "runtime-update-fixture"
	e2eVersionA         = "0.0.0-e2e.1"
	e2eVersionB         = "0.0.0-e2e.2"
	e2eCommitA          = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	e2eCommitB          = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// TestProductionSignedUpdateAcceptanceE2E drives a signed A→B application
// update through the actual Runtime Host executable and its public HTTP API.
// The only test-specific product behavior is the Recorder Engine's narrowly
// allowlisted loopback source transport, compiled with runtime_e2e.
func TestProductionSignedUpdateAcceptanceE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires building and running the production Runtime Host, Control Plane, and Recorder Engine")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-table assertions currently use the Unix ps interface")
	}
	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeUpdateArtifacts(t, fixture.server.URL)
	for iteration := 1; iteration <= 3; iteration++ {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			runProductionUpdateScenario(t, artifacts, fixture, iteration)
		})
	}
}

// TestProductionTargetAdapterRefreshPreflightE2E exercises the target's real
// adapter refresh and staged-media readiness before the durable owner CAS.
// The Host, Control, Engine, and adapter are production executables; only the
// source and signed release feed are deterministic local fixtures.
func TestProductionTargetAdapterRefreshPreflightE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires production Runtime Host/Control/Engine processes")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process assertions use Unix process controls")
	}
	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeUpdateArtifacts(t, fixture.server.URL)
	t.Run("target refresh and stages next media before transfer", func(t *testing.T) {
		runTargetRefreshPreflightScenario(t, artifacts, fixture, targetPreflightFailureNone)
	})
	t.Run("target refresh failure preserves source owner", func(t *testing.T) {
		runTargetRefreshPreflightScenario(t, artifacts, fixture, targetPreflightFailureRefresh)
	})
	t.Run("fresh manifest failure preserves source owner", func(t *testing.T) {
		runTargetRefreshPreflightScenario(t, artifacts, fixture, targetPreflightFailureManifest)
	})
	t.Run("candidate transport failure preserves source owner", func(t *testing.T) {
		runTargetRefreshPreflightScenario(t, artifacts, fixture, targetPreflightFailureSegmentTransport)
	})
	t.Run("truncated candidate payload preserves source owner", func(t *testing.T) {
		runTargetRefreshPreflightScenario(t, artifacts, fixture, targetPreflightFailureSegmentTruncated)
	})
	t.Run("empty candidate payload preserves source owner", func(t *testing.T) {
		runTargetRefreshPreflightScenario(t, artifacts, fixture, targetPreflightFailureSegmentEmpty)
	})
}

type targetPreflightFailure string

const (
	targetPreflightFailureNone             targetPreflightFailure = ""
	targetPreflightFailureRefresh          targetPreflightFailure = "refresh"
	targetPreflightFailureManifest         targetPreflightFailure = "manifest"
	targetPreflightFailureSegmentTransport targetPreflightFailure = "segment_transport"
	targetPreflightFailureSegmentTruncated targetPreflightFailure = "segment_truncated"
	targetPreflightFailureSegmentEmpty     targetPreflightFailure = "segment_empty"
)

// TestProductionHandoverHostCrashRecoveryE2E hard-kills the real Runtime Host
// at each durable owner/commit boundary and verifies cold recovery from the
// same canonical archive. The three security-critical boundaries run three
// times each; the remaining failpoints run once each as deterministic crash
// matrix coverage.
func TestProductionHandoverHostCrashRecoveryE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires building and hard-killing production Runtime Host/Control/Engine processes")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process cleanup assertions currently use the Unix ps interface")
	}
	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeUpdateArtifacts(t, fixture.server.URL)
	points := []runtimehook.Point{
		runtimehook.BeforeTargetPrepare,
		runtimehook.DuringTargetPrepare,
		runtimehook.AfterTargetReady,
		runtimehook.AfterSourceAdmissionStop,
		runtimehook.DuringSourceDrain,
		runtimehook.AfterSourceDrain,
		runtimehook.BeforeOwnerCAS,
		runtimehook.AfterOwnerCAS,
		runtimehook.BeforeTargetActivation,
		runtimehook.BeforeTargetFirstCommit,
		runtimehook.AfterTargetFirstCommit,
		runtimehook.BeforeSourceRetirement,
		runtimehook.DuringGenerationLeaseReconcile,
	}
	for pointIndex, point := range points {
		repetitions := 1
		if point == runtimehook.BeforeOwnerCAS || point == runtimehook.AfterOwnerCAS || point == runtimehook.AfterTargetFirstCommit {
			repetitions = 3
		}
		for iteration := 1; iteration <= repetitions; iteration++ {
			name := fmt.Sprintf("%02d_%s_run_%d", pointIndex+1, point, iteration)
			t.Run(name, func(t *testing.T) {
				runProductionHandoverCrashScenario(t, artifacts, fixture, point, pointIndex*10+iteration)
			})
		}
	}
}

type runtimeUpdateArtifacts struct {
	root               string
	fixtureURL         string
	bundleA            string
	hostA              string
	storageLocalBinary string
	adapterDir         string
	packageB           string
	publicKeys         string
	manifestB          release.Manifest
}

func buildRuntimeUpdateArtifacts(t *testing.T, fixtureURL string) runtimeUpdateArtifacts {
	t.Helper()
	moduleRoot := findRuntimeE2EModuleRoot(t)
	root := newRuntimeE2ETempDir(t)
	for _, name := range []string{"bin", "releases", "initial-a", "adapters", "bundled-adapters"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	fixtureURL = strings.TrimRight(fixtureURL, "/")
	ldflags := func(version, commit string) string {
		return strings.Join([]string{
			"-X github.com/integrated-recorder/core/internal/buildinfo.version=" + version,
			"-X github.com/integrated-recorder/core/internal/buildinfo.commit=" + commit,
			"-X github.com/integrated-recorder/core/internal/buildinfo.buildTime=2026-09-30T00:00:00Z",
			"-X github.com/integrated-recorder/core/internal/buildinfo.releaseChannel=prerelease",
		}, " ")
	}
	build := func(output, packagePath string, tags string, flags string) string {
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
	bin := filepath.Join(root, "bin")
	controlA := build(filepath.Join(bin, "control-a"), "./cmd/control-plane", "", ldflags(e2eVersionA, e2eCommitA))
	engineFlagsA := ldflags(e2eVersionA, e2eCommitA) + " -X main.runtimeE2EFixtureOrigin=" + fixtureURL
	engineA := build(filepath.Join(bin, "engine-a"), "./cmd/recorder-engine", "runtime_e2e", engineFlagsA)
	controlB := build(filepath.Join(bin, "control-b"), "./cmd/control-plane", "", ldflags(e2eVersionB, e2eCommitB))
	engineFlagsB := ldflags(e2eVersionB, e2eCommitB) + " -X main.runtimeE2EFixtureOrigin=" + fixtureURL
	engineB := build(filepath.Join(bin, "engine-b"), "./cmd/recorder-engine", "runtime_e2e", engineFlagsB)
	// adapter-runtime remains a required signed release role for compatibility;
	// it now carries the bundled generic HLS source executable.
	adapterRuntime := build(filepath.Join(bin, "adapter-runtime"), "./cmd/adapters/hls", "", "")
	bundledHLS := filepath.Join(root, "bundled-adapters", "integrated-recorder-adapter-hls")
	copyRuntimeArtifact(t, adapterRuntime, bundledHLS, 0555)
	storageLocalBinary := build(filepath.Join(bin, "storage-local"), "./cmd/storage-local", "", "")
	packager := build(filepath.Join(bin, "release-pack"), "./cmd/release-pack", "", "")
	fixtureAdapter := build(filepath.Join(root, "adapters", "integrated-recorder-adapter-runtime-update-fixture"), "./web/e2e/runtime_update_adapter", "", "")
	_ = fixtureAdapter

	bundleA := filepath.Join(root, "initial-a")
	copyRuntimeArtifact(t, controlA, filepath.Join(bundleA, "control-plane"), 0555)
	copyRuntimeArtifact(t, engineA, filepath.Join(bundleA, "recorder-engine"), 0555)
	if err := os.Chmod(bundleA, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(bundleA, 0700)
		_ = os.Chmod(filepath.Join(bundleA, "control-plane"), 0600)
		_ = os.Chmod(filepath.Join(bundleA, "recorder-engine"), 0600)
	})
	hostFlags := func(version, commit string) string {
		return ldflags(version, commit) + " -X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundleDir=" + bundleA +
			" -X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundledHLSBinary=" + bundledHLS
	}
	hostA := build(filepath.Join(bin, "runtime-host-a"), "./cmd/runtime-host", "runtime_e2e", hostFlags(e2eVersionA, e2eCommitA))
	hostB := build(filepath.Join(bin, "runtime-host-b"), "./cmd/runtime-host", "runtime_e2e", hostFlags(e2eVersionB, e2eCommitB))

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID := "runtime-e2e-test"
	publicKeys, err := json.Marshal([]map[string]string{{"key_id": keyID, "public_key_base64": base64.StdEncoding.EncodeToString(publicKey)}})
	if err != nil {
		t.Fatal(err)
	}
	privateKeyEnv := base64.StdEncoding.EncodeToString(privateKey)

	packageA := filepath.Join(root, "releases", "a")
	packageReleaseWithProductionTool(t, root, packager, privateKeyEnv, packageA, e2eVersionA, e2eCommitA, hostA, controlA, engineA, adapterRuntime, keyID)
	packageB := filepath.Join(root, "releases", "b")
	packageReleaseWithProductionTool(t, root, packager, privateKeyEnv, packageB, e2eVersionB, e2eCommitB, hostB, controlB, engineB, adapterRuntime, keyID)
	manifestBytes, err := os.ReadFile(filepath.Join(packageB, "release.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifestB release.Manifest
	if err := json.Unmarshal(manifestBytes, &manifestB); err != nil {
		t.Fatal(err)
	}
	verifyTestPackageArtifacts(t, packageB, manifestB)
	return runtimeUpdateArtifacts{
		root: root, fixtureURL: fixtureURL, bundleA: bundleA, hostA: hostA,
		storageLocalBinary: storageLocalBinary,
		adapterDir:         filepath.Join(root, "adapters"), packageB: packageB,
		publicKeys: string(publicKeys), manifestB: manifestB,
	}
}

func findRuntimeE2EModuleRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not locate repository go.mod from test working directory")
		}
		directory = parent
	}
}

func newRuntimeE2ETempDir(t *testing.T) string {
	t.Helper()
	// Keep the path short enough for the Runtime Host's bounded Unix socket
	// path while retaining a private, symlink-free temporary root.
	root, err := os.MkdirTemp("/private/tmp", "ir-e2e-")
	if err != nil {
		t.Fatalf("create symlink-free runtime E2E directory: %v", err)
	}
	t.Cleanup(func() {
		makeRuntimeE2ETreeWritable(root)
		_ = os.RemoveAll(root)
	})
	return root
}

func packageReleaseWithProductionTool(t *testing.T, root, packager, signingKey, output, version, commit, host, control, engine, adapterRuntime, keyID string) {
	t.Helper()
	args := []string{
		"-version", version, "-commit", commit, "-build-time", "2026-09-30T00:00:00Z",
		"-channel", "prerelease", "-platform", runtime.GOOS, "-architecture", runtime.GOARCH,
		"-key-id", keyID, "-output", output, "-runtime-host", host,
		"-control-plane", control, "-recorder-engine", engine, "-adapter-runtime", adapterRuntime,
	}
	runCommand(t, root, 2*time.Minute, []string{"IR_RELEASE_SIGNING_PRIVATE_KEY_BASE64=" + signingKey}, packager, args...)
}

func runBuildCommand(t *testing.T, directory string, timeout time.Duration, args ...string) {
	t.Helper()
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal("go tool is unavailable")
	}
	runCommand(t, directory, timeout, nil, goBinary, args...)
}

func runCommand(t *testing.T, directory string, timeout time.Duration, extraEnv []string, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = directory
	command.Env = minimalRuntimeE2EEnv(extraEnv)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("command timed out (%s %s): %v\n%s", name, strings.Join(args, " "), ctx.Err(), boundedOutput(output))
	}
	if err != nil {
		t.Fatalf("command failed (%s %s): %v\n%s", name, strings.Join(args, " "), err, boundedOutput(output))
	}
}

func minimalRuntimeE2EEnv(extra []string) []string {
	values := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir()}
	if cache := os.Getenv("GOCACHE"); cache != "" {
		values = append(values, "GOCACHE="+cache)
	}
	if modcache := os.Getenv("GOMODCACHE"); modcache != "" {
		values = append(values, "GOMODCACHE="+modcache)
	}
	return append(values, extra...)
}

func boundedOutput(output []byte) string {
	const limit = 12 << 10
	if len(output) > limit {
		output = output[len(output)-limit:]
	}
	return string(output)
}

func copyRuntimeArtifact(t *testing.T, source, destination string, mode os.FileMode) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Chmod(mode); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}

func verifyTestPackageArtifacts(t *testing.T, directory string, manifest release.Manifest) {
	t.Helper()
	if err := manifest.Validate(); err != nil {
		t.Fatalf("production release-pack emitted invalid B manifest: %v", err)
	}
	for _, artifact := range manifest.Artifacts {
		path := filepath.Join(directory, artifact.Filename)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != artifact.Size {
			t.Fatalf("B artifact %q does not match signed size: info=%v err=%v", artifact.Role, info, err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
			t.Fatalf("B artifact %q does not match signed SHA-256", artifact.Role)
		}
	}
}

type runtimeUpdateFixture struct {
	server  *httptest.Server
	mu      sync.Mutex
	streams map[string]*runtimeUpdateStream
}

type runtimeUpdateStream struct {
	latest           uint64
	online           bool
	sessionID        string
	title            string
	description      string
	tokenGeneration  uint64
	expired          bool
	segmentRequests  map[uint64][]runtimeSegmentRequest
	manifestRequests []runtimeManifestRequest
	refreshes        []runtimeRefreshRequest
	metadataRequests []time.Time
	watchRequests    []time.Time
	refreshStarted   chan time.Time
	refreshRelease   chan struct{}
	failNextRefresh  bool
	failNextManifest bool
	failNextSegment  targetPreflightFailure
	blockedSegment   uint64
	segmentStarted   chan runtimeSegmentRequest
	segmentRelease   chan struct{}
}

type runtimeSegmentRequest struct {
	Token     string
	At        time.Time
	Succeeded bool
	Failure   targetPreflightFailure
}
type runtimeManifestRequest struct {
	Token  string
	At     time.Time
	Failed bool
}
type runtimeRefreshRequest struct {
	Token     string
	At        time.Time
	Succeeded bool
	Failed    bool
}

func newRuntimeUpdateFixture(t *testing.T) *runtimeUpdateFixture {
	t.Helper()
	fixture := &runtimeUpdateFixture{streams: make(map[string]*runtimeUpdateStream)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /e2e/state", fixture.handleState)
	mux.HandleFunc("GET /e2e/metadata", fixture.handleMetadata)
	mux.HandleFunc("GET /e2e/refresh", fixture.handleRefresh)
	mux.HandleFunc("GET /hls/stream.m3u8", fixture.handleManifest)
	mux.HandleFunc("GET /hls/segments/{sequence}", fixture.handleSegment)
	fixture.server = httptest.NewServer(mux)
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *runtimeUpdateFixture) reset(stream, title, description, sessionID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams[stream] = &runtimeUpdateStream{
		sessionID: sessionID, title: title, description: description,
		segmentRequests: make(map[uint64][]runtimeSegmentRequest),
	}
}

func (f *runtimeUpdateFixture) advance(stream string, count uint64) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	value := f.streams[stream]
	value.latest += count
	return value.latest
}

func (f *runtimeUpdateFixture) setOnline(stream string, online bool) {
	f.mu.Lock()
	f.streams[stream].online = online
	f.mu.Unlock()
}

func (f *runtimeUpdateFixture) setMetadata(stream, title, description string) {
	f.mu.Lock()
	f.streams[stream].title, f.streams[stream].description = title, description
	f.mu.Unlock()
}

func (f *runtimeUpdateFixture) expireAndBlockRefresh(stream string) (<-chan time.Time, func()) {
	f.mu.Lock()
	value := f.streams[stream]
	value.expired = true
	value.refreshStarted = make(chan time.Time, 1)
	value.refreshRelease = make(chan struct{})
	started, release := value.refreshStarted, value.refreshRelease
	f.mu.Unlock()
	var once sync.Once
	return started, func() { once.Do(func() { close(release) }) }
}

func (f *runtimeUpdateFixture) expireAndFailNextRefresh(stream string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value := f.streams[stream]
	value.expired = true
	value.failNextRefresh = true
}

func (f *runtimeUpdateFixture) failNextManifest(stream string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams[stream].failNextManifest = true
}

func (f *runtimeUpdateFixture) failNextCandidate(stream string, failure targetPreflightFailure) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams[stream].failNextSegment = failure
}

func (f *runtimeUpdateFixture) blockSegment(stream string, sequence uint64) (<-chan runtimeSegmentRequest, func()) {
	f.mu.Lock()
	value := f.streams[stream]
	value.blockedSegment = sequence
	value.segmentStarted = make(chan runtimeSegmentRequest, 1)
	value.segmentRelease = make(chan struct{})
	started, release := value.segmentStarted, value.segmentRelease
	f.mu.Unlock()
	var once sync.Once
	return started, func() { once.Do(func() { close(release) }) }
}

func (f *runtimeUpdateFixture) refreshFailureCount(stream string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, request := range f.streams[stream].refreshes {
		if request.Failed {
			count++
		}
	}
	return count
}

func (f *runtimeUpdateFixture) manifestFailureCount(stream string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, request := range f.streams[stream].manifestRequests {
		if request.Failed {
			count++
		}
	}
	return count
}

func (f *runtimeUpdateFixture) candidateFailureCount(stream string, failure targetPreflightFailure) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, requests := range f.streams[stream].segmentRequests {
		for _, request := range requests {
			if request.Failure == failure {
				count++
			}
		}
	}
	return count
}

func (f *runtimeUpdateFixture) segmentRequestsFor(stream string) map[uint64][]runtimeSegmentRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make(map[uint64][]runtimeSegmentRequest)
	for sequence, requests := range f.streams[stream].segmentRequests {
		result[sequence] = append([]runtimeSegmentRequest(nil), requests...)
	}
	return result
}

func (f *runtimeUpdateFixture) manifestRequestsFor(stream string) []runtimeManifestRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]runtimeManifestRequest(nil), f.streams[stream].manifestRequests...)
}

func (f *runtimeUpdateFixture) refreshesFor(stream string) []runtimeRefreshRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]runtimeRefreshRequest(nil), f.streams[stream].refreshes...)
}

func (f *runtimeUpdateFixture) metadataRequestsFor(stream string) []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.streams[stream].metadataRequests...)
}

func (f *runtimeUpdateFixture) watchRequestCount(stream string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.streams[stream].watchRequests)
}

func (f *runtimeUpdateFixture) handleState(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	value := f.streams[r.URL.Query().Get("stream")]
	if value == nil {
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Query().Get("stream"), "w-") {
		value.watchRequests = append(value.watchRequests, time.Now().UTC())
	}
	result := map[string]any{"online": value.online, "session_id": value.sessionID, "title": value.title}
	f.mu.Unlock()
	writeRuntimeJSON(w, result)
}

func (f *runtimeUpdateFixture) handleMetadata(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	value := f.streams[r.URL.Query().Get("stream")]
	if value == nil {
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	value.metadataRequests = append(value.metadataRequests, time.Now().UTC())
	result := map[string]string{"title": value.title, "description": value.description}
	f.mu.Unlock()
	writeRuntimeJSON(w, result)
}

func (f *runtimeUpdateFixture) handleRefresh(w http.ResponseWriter, r *http.Request) {
	stream, token := r.URL.Query().Get("stream"), r.URL.Query().Get("token")
	f.mu.Lock()
	value := f.streams[stream]
	if value == nil || !value.expired || token != fmt.Sprintf("token-%d", value.tokenGeneration) {
		f.mu.Unlock()
		http.Error(w, "refresh is not currently required", http.StatusConflict)
		return
	}
	started, release := value.refreshStarted, value.refreshRelease
	at := time.Now().UTC()
	value.refreshes = append(value.refreshes, runtimeRefreshRequest{Token: token, At: at})
	requestIndex := len(value.refreshes) - 1
	failOnce := value.failNextRefresh
	if failOnce {
		value.failNextRefresh = false
		value.refreshes[requestIndex].Failed = true
	}
	f.mu.Unlock()
	if failOnce {
		http.Error(w, "fixture refresh temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if started != nil {
		select {
		case started <- at:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-r.Context().Done():
			http.Error(w, "refresh request canceled", http.StatusGatewayTimeout)
			return
		case <-time.After(40 * time.Second):
			http.Error(w, "refresh fixture timed out", http.StatusGatewayTimeout)
			return
		}
	}
	f.mu.Lock()
	value = f.streams[stream]
	if value == nil {
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	if value.expired {
		value.tokenGeneration++
		value.expired = false
	}
	refreshed := fmt.Sprintf("token-%d", value.tokenGeneration)
	if requestIndex < len(value.refreshes) {
		value.refreshes[requestIndex].Succeeded = true
	}
	f.mu.Unlock()
	writeRuntimeJSON(w, map[string]string{"token": refreshed})
}

func (f *runtimeUpdateFixture) handleManifest(w http.ResponseWriter, r *http.Request) {
	stream, token := r.URL.Query().Get("stream"), r.URL.Query().Get("token")
	f.mu.Lock()
	value := f.streams[stream]
	if value == nil {
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	requestIndex := len(value.manifestRequests)
	value.manifestRequests = append(value.manifestRequests, runtimeManifestRequest{Token: token, At: time.Now().UTC()})
	failOnce := value.failNextManifest
	if failOnce {
		value.failNextManifest = false
		value.manifestRequests[requestIndex].Failed = true
	}
	valid := token == fmt.Sprintf("token-%d", value.tokenGeneration) && !value.expired
	latest := value.latest
	f.mu.Unlock()
	if failOnce {
		http.Error(w, "fixture manifest temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if !valid {
		http.Error(w, "fixture media token expired", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n")
	for sequence := uint64(1); sequence <= latest; sequence++ {
		_, _ = fmt.Fprintf(w, "#EXTINF:1.0,\n/hls/segments/%06d.ts?stream=%s&token=%s\n", sequence, urlQueryEscape(stream), urlQueryEscape(token))
	}
}

func (f *runtimeUpdateFixture) handleSegment(w http.ResponseWriter, r *http.Request) {
	stream, token := r.URL.Query().Get("stream"), r.URL.Query().Get("token")
	sequence, err := strconv.ParseUint(strings.TrimSuffix(r.PathValue("sequence"), ".ts"), 10, 64)
	if err != nil || sequence == 0 {
		http.NotFound(w, r)
		return
	}
	f.mu.Lock()
	value := f.streams[stream]
	if value == nil {
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	valid := token == fmt.Sprintf("token-%d", value.tokenGeneration) && !value.expired && sequence <= value.latest
	failure := targetPreflightFailureNone
	if valid && sequence == 4 && value.failNextSegment != targetPreflightFailureNone {
		failure = value.failNextSegment
		value.failNextSegment = targetPreflightFailureNone
	}
	requestRecord := runtimeSegmentRequest{Token: token, At: time.Now().UTC(), Succeeded: valid && failure == targetPreflightFailureNone, Failure: failure}
	value.segmentRequests[sequence] = append(value.segmentRequests[sequence], requestRecord)
	blocked := sequence == value.blockedSegment && value.segmentRelease != nil
	started, release := value.segmentStarted, value.segmentRelease
	f.mu.Unlock()
	if !valid {
		http.Error(w, "fixture media token expired", http.StatusForbidden)
		return
	}
	if failure == targetPreflightFailureSegmentTransport {
		http.Error(w, "fixture candidate segment temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if blocked && failure == targetPreflightFailureNone {
		if started != nil {
			select {
			case started <- requestRecord:
			default:
			}
		}
		select {
		case <-release:
		case <-r.Context().Done():
			http.Error(w, "candidate segment request canceled", http.StatusGatewayTimeout)
			return
		case <-time.After(40 * time.Second):
			http.Error(w, "candidate segment fixture timed out", http.StatusGatewayTimeout)
			return
		}
	}
	payload := runtimeSegmentPayload(stream, sequence)
	switch failure {
	case targetPreflightFailureSegmentTruncated:
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)+16))
		_, _ = io.WriteString(w, payload[:len(payload)/2])
		return
	case targetPreflightFailureSegmentEmpty:
		w.Header().Set("Content-Length", "0")
		return
	}
	_, _ = io.WriteString(w, payload)
}

func runtimeSegmentPayload(stream string, sequence uint64) string {
	return fmt.Sprintf("stream=%s;sequence=%06d;canonical-source-payload", stream, sequence)
}

func runTargetRefreshPreflightScenario(t *testing.T, artifacts runtimeUpdateArtifacts, fixture *runtimeUpdateFixture, failure targetPreflightFailure) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	markerDir := filepath.Join(dataRoot, "failpoints")
	for _, directory := range []string{dataDir, markerDir} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	stream := "target-preflight-success"
	if failure != targetPreflightFailureNone {
		stream = "target-preflight-" + string(failure)
	}
	fixture.reset(stream, "Preflight title", "Preflight description", stream+"-session")
	controlABinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineABinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	installedDir := filepath.Join(dataDir, "runtime", "releases", install.ReleaseDirectoryID(e2eVersionB, e2eCommitB))
	controlBBinary := filepath.Join(installedDir, fmt.Sprintf("%s-%s-%s", release.RoleControlPlane, runtime.GOOS, runtime.GOARCH))
	engineBBinary := filepath.Join(installedDir, fmt.Sprintf("%s-%s-%s", release.RoleRecorderEngine, runtime.GOOS, runtime.GOARCH))
	fixtureAdapterBinary := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-runtime-update-fixture")
	listenAddr := reserveRuntimeAddress(t)
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 30 * time.Second}
	command := exec.Command(artifacts.hostA)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"AUTH_DISABLED=1",
		"ADAPTER_DIR=" + artifacts.adapterDir,
		"IR_ALLOW_OPERATOR_PLUGINS=1",
		"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
		"IR_RELEASE_BUNDLE_DIR=" + artifacts.packageB,
		"IR_RELEASE_TRUSTED_KEYS_JSON=" + artifacts.publicKeys,
		"IR_RUNTIME_E2E_FAILPOINT=" + string(runtimehook.AfterSourceDrain),
		"IR_RUNTIME_E2E_MARKER_DIR=" + markerDir,
	})
	process := &runtimeHostProcess{command: command, done: make(chan struct{}), diagnosticDir: markerDir}
	command.Stdout, command.Stderr = &process.output, &process.output
	if err := command.Start(); err != nil {
		t.Fatalf("start production Runtime Host: %v", err)
	}
	go func() {
		process.err = command.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		stopRuntimeHostProcess(process)
		for _, executable := range []string{artifacts.hostA, controlABinary, engineABinary, controlBBinary, engineBBinary, fixtureAdapterBinary} {
			if err := waitProcessAbsent(t, executable, 10*time.Second); err != nil {
				t.Errorf("target preflight cleanup left product process running for %s: %v", filepath.Base(executable), err)
			}
		}
		makeRuntimeE2ETreeWritable(dataDir)
	})

	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if status.DefaultEngine == nil || status.DefaultEngine.Version != e2eVersionA {
		t.Fatalf("release A Engine was not initially default: %+v", status)
	}
	recording := createRuntimeRecording(t, client, baseURL, "Pinned refresh preflight", map[string]string{"source_url": artifacts.fixtureURL + "/source/" + stream})
	leaseA := waitRecordingLease(t, dataDir, recording.ID, 30*time.Second)
	ownerStore, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ownerA, err := ownerStore.Current(recording.ID)
	if err != nil || ownerA.EngineGeneration != leaseA.EngineGeneration || ownerA.WorkerInstance != leaseA.WorkerInstance {
		t.Fatalf("initial owner does not match recording lease: owner=%+v lease=%+v err=%v", ownerA, leaseA, err)
	}
	fixture.advance(stream, 3)
	waitRecordingSequenceCount(t, client, baseURL, recording.ID, 3, 20*time.Second)
	baseline := getRecording(t, client, baseURL, recording.ID)
	if baseline.State != domain.StateRecording || len(baseline.Gaps) != 0 || !equalSequenceRange(recordingSequences(baseline), 1, 3) {
		t.Fatalf("preflight baseline is not a live contiguous archive: state=%s sequences=%v gaps=%+v", baseline.State, recordingSequences(baseline), baseline.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, baseline, stream, 1, 3)
	rootPath := filepath.Join(dataDir, "recordings", recording.ID, "recording.json")
	rootBefore := mustReadFile(t, rootPath)
	registryBefore := readRuntimeGenerationSnapshot(t, dataDir)
	adapterSetA := registryBefore.Generations[leaseA.EngineGeneration].AdapterSetID
	if adapterSetA == "" {
		t.Fatalf("source Engine generation does not pin an immutable adapter set: %+v", registryBefore.Generations[leaseA.EngineGeneration])
	}

	check, code := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodPost, httpapi.Endpoint+"/check", map[string]any{})
	if code != http.StatusOK || !check.UpdatesAvailable || check.AvailableRelease == nil || check.AvailableRelease.Version != e2eVersionB || check.VerificationState != "verified" {
		t.Fatalf("signed release B check failed: code=%d status=%+v", code, check)
	}
	stage, code := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodPost, httpapi.Endpoint+"/stage", map[string]any{})
	if code != http.StatusOK || stage.StagedRelease == nil || stage.StagedRelease.Version != e2eVersionB || stage.VerificationState != "verified" {
		t.Fatalf("signed release B stage failed: code=%d status=%+v", code, stage)
	}
	if err := writeRuntimeHookArm(markerDir, runtimehook.Arm{Point: runtimehook.AfterSourceDrain, RecordingID: recording.ID}); err != nil {
		t.Fatalf("arm AfterSourceDrain for exact Recording: %v", err)
	}
	activation := make(chan productionActivationOutcome, 1)
	go func() {
		outcome := productionActivationOutcome{started: time.Now().UTC()}
		outcome.code, outcome.err = requestRuntimeJSON(client, baseURL, http.MethodPost, httpapi.Endpoint+"/activate", map[string]any{}, &outcome.status)
		outcome.finished = time.Now().UTC()
		activation <- outcome
	}()
	if err := waitHostHandoverLogEvent(process, "recording handover_source_drained", recording.ID, 1, 45*time.Second); err != nil {
		t.Fatalf("source did not reach deterministic drained boundary: %v; host=%s", err, process.output.String())
	}
	readyMarker := runtimehook.ReadyMarkerPath(markerDir, runtimehook.AfterSourceDrain, recording.ID)
	if err := waitRuntimeConditionError(10*time.Second, func() bool {
		info, statErr := os.Lstat(readyMarker)
		return statErr == nil && info.Mode().IsRegular() && info.Mode().Perm() == 0600
	}, "AfterSourceDrain pause"); err != nil {
		t.Fatalf("Host did not pause after source drain: %v", err)
	}
	ownerAtDrain, err := ownerStore.Current(recording.ID)
	if err != nil || ownerAtDrain != ownerA {
		t.Fatalf("ownership changed before target preflight started: before=%+v at_drain=%+v err=%v", ownerA, ownerAtDrain, err)
	}
	leaseAtDrain := readRuntimeLeases(t, dataDir)[recording.ID]
	if leaseAtDrain.EngineGeneration != leaseA.EngineGeneration {
		t.Fatalf("generation lease moved before target readiness: A=%+v now=%+v", leaseA, leaseAtDrain)
	}
	mediaAtDrain := snapshotRuntimeCommittedMedia(t, dataDir, recording.ID)
	fixture.advance(stream, 1) // sequence 4 is the first candidate past the canonical tail

	if failure != targetPreflightFailureNone {
		switch failure {
		case targetPreflightFailureRefresh:
			fixture.expireAndFailNextRefresh(stream)
		case targetPreflightFailureManifest:
			fixture.failNextManifest(stream)
		case targetPreflightFailureSegmentTransport, targetPreflightFailureSegmentTruncated, targetPreflightFailureSegmentEmpty:
			fixture.failNextCandidate(stream, failure)
		default:
			t.Fatalf("unknown target preflight failure mode %q", failure)
		}
		// Hold the source's retry of sequence 4 so canonical state can be
		// compared after target preflight fails but before the source admits it.
		sourceSegmentStarted, releaseSourceSegment := fixture.blockSegment(stream, 4)
		t.Cleanup(releaseSourceSegment)
		if err := writeRuntimeHookRelease(markerDir, runtimehook.AfterSourceDrain, recording.ID); err != nil {
			t.Fatalf("release exact-recording source drain hook: %v", err)
		}
		if err := os.Remove(runtimehook.ArmPath(markerDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("disarm source-drain hook after its single deterministic use: %v", err)
		}
		waitFailure := func() error {
			switch failure {
			case targetPreflightFailureRefresh:
				return waitRuntimeConditionError(20*time.Second, func() bool { return fixture.refreshFailureCount(stream) == 1 }, "target's one-shot adapter refresh failure")
			case targetPreflightFailureManifest:
				return waitRuntimeConditionError(20*time.Second, func() bool { return fixture.manifestFailureCount(stream) == 1 }, "target's one-shot fresh manifest failure")
			default:
				return waitRuntimeConditionError(20*time.Second, func() bool { return fixture.candidateFailureCount(stream, failure) == 1 }, "target's one-shot candidate media failure")
			}
		}
		if err := waitFailure(); err != nil {
			t.Fatalf("target did not perform injected %s preflight failure: %v; fixture=%s; host=%s", failure, err, fixture.describe(stream), process.output.String())
		}
		ownerAfterFailure, err := ownerStore.Current(recording.ID)
		if err != nil || ownerAfterFailure != ownerA {
			if failure == targetPreflightFailureSegmentEmpty && err == nil {
				accepted := waitCanonicalSegmentCount(t, dataDir, recording.ID, 4, 15*time.Second)
				var segment *domain.Segment
				if track := accepted.Tracks["main"]; track != nil {
					for index := range track.Segments {
						if track.Segments[index].Sequence == 4 {
							segment = &track.Segments[index]
							break
						}
					}
				}
				var payload []byte
				if segment != nil {
					store, storeErr := storage.New(dataDir)
					if storeErr == nil {
						reader, openErr := store.OpenPayloadReader(recording.ID, segment.StoragePath)
						if openErr == nil {
							payload, _ = io.ReadAll(reader)
							_ = reader.Close()
						}
					}
				}
				digest := sha256.Sum256(payload)
				t.Fatalf("production defect: target accepted empty HTTP 200 continuation media before owner CAS: owner_before=%+v owner_after=%+v sequence4=%+v payload_size=%d payload_sha256=%s fixture=%s", ownerA, ownerAfterFailure, segment, len(payload), hex.EncodeToString(digest[:]), fixture.describe(stream))
			}
			t.Fatalf("target %s failure changed durable owner before source resumed: before=%+v after=%+v err=%v", failure, ownerA, ownerAfterFailure, err)
		}
		select {
		case request := <-sourceSegmentStarted:
			sourceToken := "token-0"
			if failure == targetPreflightFailureRefresh {
				sourceToken = "token-1"
			}
			if request.Token != sourceToken || !request.Succeeded || request.Failure != targetPreflightFailureNone {
				t.Fatalf("source did not resume with valid media after %s preflight failure: got=%+v want token=%s fixture=%s", failure, request, sourceToken, fixture.describe(stream))
			}
		case <-ctx.Done():
			t.Fatalf("source did not resume to fetch sequence 4 after %s preflight failure: %v", failure, ctx.Err())
		case <-time.After(30 * time.Second):
			t.Fatalf("source did not resume to fetch sequence 4 after %s preflight failure; fixture=%s; host=%s", failure, fixture.describe(stream), process.output.String())
		}
		if current, ownerErr := ownerStore.Current(recording.ID); ownerErr != nil || current != ownerA {
			t.Fatalf("source lost ownership before its sequence-4 continuation request after %s failure: owner=%+v err=%v want=%+v", failure, current, ownerErr, ownerA)
		}
		if !bytesEqual(mediaAtDrain, snapshotRuntimeCommittedMedia(t, dataDir, recording.ID)) {
			t.Fatalf("target %s preflight changed committed media segments/payloads before source sequence 4 response was admitted", failure)
		}
		releaseSourceSegment()
		resumed := waitCanonicalSegmentCount(t, dataDir, recording.ID, 4, 30*time.Second)
		if resumed.State != domain.StateRecording || len(resumed.Gaps) != 0 || !equalSequenceRange(recordingSequences(resumed), 1, 4) {
			t.Fatalf("source Engine did not resume and capture the newly advanced segment after target %s failed: state=%s sequences=%v gaps=%+v", failure, resumed.State, recordingSequences(resumed), resumed.Gaps)
		}
		verifyRuntimeRecordingSegments(t, dataDir, resumed, stream, 1, 4)
		if failure == targetPreflightFailureRefresh {
			if err := waitRuntimeConditionError(10*time.Second, func() bool {
				return len(fixture.refreshesFor(stream)) >= 2 && fixture.refreshesFor(stream)[1].Succeeded
			}, "source Engine retry of fixture refresh"); err != nil {
				t.Fatalf("source did not recover the one-shot failed refresh: %v; fixture=%s", err, fixture.describe(stream))
			}
			refreshes := fixture.refreshesFor(stream)
			if len(refreshes) < 2 || !refreshes[0].Failed || refreshes[0].Token != "token-0" || !refreshes[1].Succeeded || refreshes[1].Token != "token-0" {
				t.Fatalf("expected target failure followed by source recovery against the same pinned token: %+v", refreshes)
			}
		} else {
			requests := fixture.segmentRequestsFor(stream)[4]
			if failure == targetPreflightFailureManifest {
				if len(requests) != 1 || requests[0].Failure != targetPreflightFailureNone || !requests[0].Succeeded {
					t.Fatalf("manifest failure should be followed by one successful source candidate request: %+v", requests)
				}
			} else if len(requests) != 2 || requests[0].Failure != failure || requests[0].Succeeded || requests[1].Failure != targetPreflightFailureNone || !requests[1].Succeeded {
				t.Fatalf("expected target %s candidate failure followed by one successful source request: %+v", failure, requests)
			}
		}
		outcome := waitActivationOutcome(t, activation, 45*time.Second)
		if outcome.err != nil || outcome.code != http.StatusOK || outcome.status.DefaultEngine == nil || outcome.status.DefaultEngine.Version != e2eVersionB {
			t.Fatalf("release B activation should remain successful despite this Recording's pre-CAS failure: code=%d status=%+v err=%v host=%s", outcome.code, outcome.status, outcome.err, process.output.String())
		}
		t.Logf("production preflight failure proof: Recording=%s failure=%s owner remained %s/%d, source captured sequence 4 without a gap", recording.ID, failure, ownerA.EngineGeneration, ownerA.Epoch)
		return
	}

	refreshStarted, releaseRefresh := fixture.expireAndBlockRefresh(stream)
	t.Cleanup(releaseRefresh)
	segmentStarted, releaseSegment := fixture.blockSegment(stream, 4)
	t.Cleanup(releaseSegment)
	if err := writeRuntimeHookRelease(markerDir, runtimehook.AfterSourceDrain, recording.ID); err != nil {
		t.Fatalf("release exact-recording source drain hook: %v", err)
	}
	if err := os.Remove(runtimehook.ArmPath(markerDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disarm source-drain hook after its single deterministic use: %v", err)
	}
	select {
	case refresh := <-refreshStarted:
		_ = refresh
	case <-ctx.Done():
		t.Fatalf("target did not invoke pinned adapter refresh before transfer: %v", ctx.Err())
	case <-time.After(20 * time.Second):
		t.Fatalf("target did not invoke adapter refresh before transfer; fixture=%s; host=%s", fixture.describe(stream), process.output.String())
	}
	ownerDuringRefresh, err := ownerStore.Current(recording.ID)
	if err != nil || ownerDuringRefresh != ownerA {
		t.Fatalf("owner changed while target's adapter refresh was still blocked: before=%+v during_refresh=%+v err=%v", ownerA, ownerDuringRefresh, err)
	}
	if got := readRuntimeLeases(t, dataDir)[recording.ID].EngineGeneration; got != leaseA.EngineGeneration {
		t.Fatalf("generation lease changed while target refresh was still blocked: got=%s source=%s", got, leaseA.EngineGeneration)
	}
	releaseRefresh()
	select {
	case candidate := <-segmentStarted:
		if candidate.Token != "token-1" || !candidate.Succeeded {
			t.Fatalf("target did not fetch continuation candidate with refreshed source token: %+v", candidate)
		}
	case <-ctx.Done():
		t.Fatalf("target did not fetch next continuation payload before transfer: %v", ctx.Err())
	case <-time.After(20 * time.Second):
		t.Fatalf("target did not fetch sequence 4 before ownership transfer; fixture=%s; host=%s", fixture.describe(stream), process.output.String())
	}
	manifests := fixture.manifestRequestsFor(stream)
	if !containsManifestTokenAfter(manifests, "token-1", refreshStartedAt(fixture, stream)) {
		t.Fatalf("target did not fetch a fresh token-1 manifest after adapter refresh: %+v", manifests)
	}
	ownerWhileCandidateBlocked, err := ownerStore.Current(recording.ID)
	if err != nil || ownerWhileCandidateBlocked != ownerA {
		t.Fatalf("ownership changed before target candidate payload was released: before=%+v now=%+v err=%v", ownerA, ownerWhileCandidateBlocked, err)
	}
	releaseSegment()
	outcome := waitActivationOutcome(t, activation, 45*time.Second)
	if outcome.err != nil || outcome.code != http.StatusOK || outcome.status.DefaultEngine == nil || outcome.status.DefaultEngine.Version != e2eVersionB {
		t.Fatalf("production application activation did not complete after candidate readiness: code=%d status=%+v err=%v host=%s", outcome.code, outcome.status, outcome.err, process.output.String())
	}
	leaseB := waitRecordingLeaseGeneration(t, dataDir, recording.ID, outcome.status.DefaultEngine.ID, 20*time.Second, process.output.String, func() string { return fixture.describe(stream) })
	ownerB, err := ownerStore.Current(recording.ID)
	if err != nil || ownerB.EngineGeneration != outcome.status.DefaultEngine.ID || ownerB.Epoch <= ownerA.Epoch {
		t.Fatalf("durable owner did not transfer monotonically to ready target: old=%+v new=%+v active=%+v err=%v; host=%s; fixture=%s", ownerA, ownerB, outcome.status.DefaultEngine, err, process.output.String(), fixture.describe(stream))
	}
	registryAfter := readRuntimeGenerationSnapshot(t, dataDir)
	if registryAfter.Generations[leaseA.EngineGeneration].AdapterSetID != adapterSetA || registryAfter.Generations[leaseB.EngineGeneration].AdapterSetID != adapterSetA {
		t.Fatalf("handover changed the Recording's pinned immutable adapter set: A=%q B=%q expected=%q", registryAfter.Generations[leaseA.EngineGeneration].AdapterSetID, registryAfter.Generations[leaseB.EngineGeneration].AdapterSetID, adapterSetA)
	}
	final := waitCanonicalSegmentCount(t, dataDir, recording.ID, 4, 20*time.Second)
	if final.ID != recording.ID || final.State != domain.StateRecording || len(final.Gaps) != 0 || !equalSequenceRange(recordingSequences(final), 1, 4) {
		t.Fatalf("successful target preflight did not continue the same Recording without a gap: id=%s state=%s sequences=%v gaps=%+v", final.ID, final.State, recordingSequences(final), final.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, final, stream, 1, 4)
	requests := fixture.segmentRequestsFor(stream)
	if len(requests[4]) != 1 || requests[4][0].Token != "token-1" || !requests[4][0].Succeeded {
		t.Fatalf("the preflight payload was not committed exactly once from refreshed media: %+v", requests[4])
	}
	if bytesEqual(rootBefore, mustReadFile(t, rootPath)) {
		t.Fatal("canonical root did not advance when the staged target candidate was committed")
	}
	t.Logf("production target preflight proof: Recording=%s source=%s/%d target=%s/%d, adapter_set=%s, refresh token-0→token-1, manifest and candidate sequence 4 fetched before owner CAS", recording.ID, ownerA.EngineGeneration, ownerA.Epoch, ownerB.EngineGeneration, ownerB.Epoch, adapterSetA)
}

func containsManifestTokenAfter(requests []runtimeManifestRequest, token string, after time.Time) bool {
	for _, request := range requests {
		if request.Token == token && (after.IsZero() || request.At.After(after)) {
			return true
		}
	}
	return false
}

func refreshStartedAt(fixture *runtimeUpdateFixture, stream string) time.Time {
	requests := fixture.refreshesFor(stream)
	if len(requests) == 0 {
		return time.Time{}
	}
	return requests[len(requests)-1].At
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read canonical recording root: %v", err)
	}
	return data
}

func snapshotRuntimeCommittedMedia(t *testing.T, dataDir, recordingID string) []byte {
	t.Helper()
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatalf("open storage for committed media snapshot: %v", err)
	}
	recording, err := store.LoadRecordingReadOnly(recordingID)
	if err != nil || recording == nil {
		t.Fatalf("load recording for committed media snapshot: recording=%v err=%v", recording != nil, err)
	}
	type committedTrack struct {
		Segments     []domain.Segment `json:"segments"`
		InitSegments []domain.Segment `json:"init_segments"`
	}
	tracks := make(map[string]committedTrack, len(recording.Tracks))
	for id, track := range recording.Tracks {
		if track != nil {
			tracks[id] = committedTrack{Segments: track.Segments, InitSegments: track.InitSegments}
		}
	}
	root := filepath.Join(dataDir, "recordings", recordingID)
	files := make(map[string]string)
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		// A-side source acquisition is allowed to persist fresh manifest
		// snapshots while it resumes. Only canonical media segment/init files
		// are included here, together with their committed root references.
		if !strings.HasPrefix(filepath.ToSlash(relative), "tracks/") {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("canonical archive contains nonregular entry %q", filepath.Base(path))
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		files[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot canonical archive files: %v", err)
	}
	snapshot := struct {
		ID             string                    `json:"id"`
		State          domain.RecordingState     `json:"state"`
		Title          string                    `json:"title"`
		AdapterID      string                    `json:"adapter_id"`
		Adapter        *domain.AdapterProvenance `json:"adapter,omitempty"`
		Resource       *domain.ResourceReference `json:"resource,omitempty"`
		CreatedAt      time.Time                 `json:"created_at"`
		StartedAt      time.Time                 `json:"started_at"`
		StoppedAt      *time.Time                `json:"stopped_at,omitempty"`
		Tracks         map[string]committedTrack `json:"tracks"`
		Gaps           []domain.Gap              `json:"gaps,omitempty"`
		Metadata       []domain.MetadataRevision `json:"metadata_timeline,omitempty"`
		MetadataCut    bool                      `json:"metadata_timeline_truncated,omitempty"`
		PayloadDigests map[string]string         `json:"payload_digests"`
	}{
		ID: recording.ID, State: recording.State, Title: recording.Title, AdapterID: recording.AdapterID,
		Adapter: recording.Adapter, Resource: recording.Resource, CreatedAt: recording.CreatedAt,
		StartedAt: recording.StartedAt, StoppedAt: recording.StoppedAt, Tracks: tracks,
		Gaps: recording.Gaps, Metadata: recording.MetadataTimeline, MetadataCut: recording.MetadataTimelineTruncated,
		PayloadDigests: files,
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode committed media snapshot: %v", err)
	}
	return data
}

func bytesEqual(left, right []byte) bool {
	return reflect.DeepEqual(left, right)
}

func urlQueryEscape(value string) string { return url.QueryEscape(value) }

func writeRuntimeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

type runtimeHostProcess struct {
	command       *exec.Cmd
	done          chan struct{}
	err           error
	output        testOutputBuffer
	diagnosticDir string
}

type productionActivationOutcome struct {
	status   httpapi.Status
	code     int
	err      error
	started  time.Time
	finished time.Time
}

func runProductionHandoverCrashScenario(t *testing.T, artifacts runtimeUpdateArtifacts, fixture *runtimeUpdateFixture, point runtimehook.Point, runID int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	markerDir := filepath.Join(dataRoot, "failpoints")
	for _, directory := range []string{dataDir, markerDir} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	stream := fmt.Sprintf("crash-%s-%d", point, runID)
	fixture.reset(stream, "Before host crash", "Stable description", fmt.Sprintf("session-%d", runID))
	controlABinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineABinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	installedDir := filepath.Join(dataDir, "runtime", "releases", install.ReleaseDirectoryID(e2eVersionB, e2eCommitB))
	controlBBinary := filepath.Join(installedDir, fmt.Sprintf("%s-%s-%s", release.RoleControlPlane, runtime.GOOS, runtime.GOARCH))
	engineBBinary := filepath.Join(installedDir, fmt.Sprintf("%s-%s-%s", release.RoleRecorderEngine, runtime.GOOS, runtime.GOARCH))
	fixtureAdapterBinary := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-runtime-update-fixture")
	listenAddr := reserveRuntimeAddress(t)
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 8 * time.Second}
	tracked := make(map[int]string)
	var currentHost *runtimeHostProcess
	trackProducts := func(paths ...string) {
		for _, path := range paths {
			for _, pid := range processIDsForBinary(path) {
				tracked[pid] = path
			}
		}
	}
	startHost := func(failpoint bool) *runtimeHostProcess {
		extra := []string{
			"DATA_DIR=" + dataDir,
			"ADDR=" + listenAddr,
			"AUTH_DISABLED=1",
			"ADAPTER_DIR=" + artifacts.adapterDir,
			"IR_ALLOW_OPERATOR_PLUGINS=1",
			"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
			"IR_RELEASE_BUNDLE_DIR=" + artifacts.packageB,
			"IR_RELEASE_TRUSTED_KEYS_JSON=" + artifacts.publicKeys,
		}
		if failpoint {
			extra = append(extra, "IR_RUNTIME_E2E_FAILPOINT="+string(point), "IR_RUNTIME_E2E_MARKER_DIR="+markerDir)
		}
		command := exec.Command(artifacts.hostA)
		command.Env = minimalRuntimeE2EEnv(extra)
		process := &runtimeHostProcess{command: command, done: make(chan struct{})}
		if failpoint {
			process.diagnosticDir = markerDir
		}
		command.Stdout, command.Stderr = &process.output, &process.output
		if err := command.Start(); err != nil {
			t.Fatalf("start tagged production Runtime Host: %v", err)
		}
		go func() {
			process.err = command.Wait()
			close(process.done)
		}()
		return process
	}
	t.Cleanup(func() {
		stopRuntimeHostProcess(currentHost)
		if err := terminateRecordedProcesses(tracked, 10*time.Second); err != nil {
			t.Errorf("crash acceptance cleanup left a recorded product process running: %v", err)
		}
		makeRuntimeE2ETreeWritable(dataDir)
	})

	currentHost = startHost(true)
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, currentHost)
	installationBefore := installation.ReadOnly(dataDir)
	if installationBefore.State != installation.StateReady || installationBefore.InstallationID == "" || status.ActiveControl == nil || status.ActiveControl.Version != e2eVersionA {
		t.Fatalf("production release A did not bootstrap ready: install=%+v status=%+v; host=%s", installationBefore, status, currentHost.output.String())
	}
	trackProducts(controlABinary, engineABinary)
	input := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + stream}
	recording := createRuntimeRecording(t, client, baseURL, "Crash-boundary recording", input)
	if recording.ID == "" || recording.State != domain.StateRecording {
		t.Fatalf("production API did not create active Recording: %+v", recording)
	}
	leaseA := waitRecordingLease(t, dataDir, recording.ID, 30*time.Second)
	if leaseA.EngineGeneration != status.DefaultEngine.ID {
		t.Fatalf("Recording is not pinned to release A: lease=%+v active=%+v", leaseA, status.DefaultEngine)
	}
	ownerStore, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	sourceOwner, err := ownerStore.Current(recording.ID)
	if err != nil || sourceOwner.EngineGeneration != leaseA.EngineGeneration || sourceOwner.WorkerInstance != leaseA.WorkerInstance {
		t.Fatalf("could not capture authoritative source owner: owner=%+v lease=%+v err=%v", sourceOwner, leaseA, err)
	}
	if err := writeRuntimeHookArm(markerDir, runtimehook.Arm{Point: point, RecordingID: recording.ID}); err != nil {
		t.Fatalf("atomically arm exact recording failpoint: %v", err)
	}
	fixture.advance(stream, 5)
	waitRecordingSequenceCount(t, client, baseURL, recording.ID, 5, 25*time.Second)
	baseline := getRecording(t, client, baseURL, recording.ID)
	if !equalSequenceRange(recordingSequences(baseline), 1, 5) || len(baseline.Gaps) != 0 {
		t.Fatalf("pre-crash archive is not contiguous through sequence 5: sequences=%v gaps=%+v", recordingSequences(baseline), baseline.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, baseline, stream, 1, 5)
	metadataBefore := waitMetadataTimeline(t, ctx, client, baseURL, recording.ID, 1, 35*time.Second)
	if len(metadataBefore.Items) != 1 || stringValue(metadataBefore.Items[0].Title) != "Before host crash" || stringValue(metadataBefore.Items[0].Description) != "Stable description" {
		t.Fatalf("metadata baseline is invalid: %+v", metadataBefore.Items)
	}
	archiveDir := filepath.Join(dataDir, "recordings", recording.ID)
	if _, err := os.Stat(archiveDir); err != nil {
		t.Fatalf("canonical archive directory is missing before update: %v", err)
	}

	check, code := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodPost, httpapi.Endpoint+"/check", map[string]any{})
	if code != http.StatusOK || !check.UpdatesAvailable || check.AvailableRelease == nil || check.AvailableRelease.Version != e2eVersionB || check.VerificationState != "verified" {
		t.Fatalf("production signed check did not verify B: code=%d status=%+v", code, check)
	}
	stage, code := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodPost, httpapi.Endpoint+"/stage", map[string]any{})
	if code != http.StatusOK || stage.StagedRelease == nil || stage.StagedRelease.Version != e2eVersionB || stage.VerificationState != "verified" {
		t.Fatalf("production signed stage did not install B: code=%d status=%+v", code, stage)
	}
	if err := verifyInstalledRuntimeRelease(installedDir, artifacts.manifestB); err != nil {
		t.Fatalf("staged B release failed production artifact checks: %v", err)
	}

	activation := make(chan productionActivationOutcome, 1)
	go func() {
		outcome := productionActivationOutcome{started: time.Now().UTC()}
		outcome.code, outcome.err = requestRuntimeJSON(client, baseURL, http.MethodPost, httpapi.Endpoint+"/activate", map[string]any{}, &outcome.status)
		outcome.finished = time.Now().UTC()
		activation <- outcome
	}()
	activated := waitActivationOutcome(t, activation, 30*time.Second)
	if activated.err != nil || activated.code != http.StatusOK || activated.status.DefaultEngine == nil || activated.status.DefaultEngine.Version != e2eVersionB || activated.status.ActiveControl == nil || activated.status.ActiveControl.Version != e2eVersionB {
		t.Fatalf("production API failed to activate B before handover crash: code=%d status=%+v err=%v", activated.code, activated.status, activated.err)
	}
	trackProducts(controlABinary, engineABinary, controlBBinary, engineBBinary, fixtureAdapterBinary)
	if pointNeedsPreparedContinuation(point) {
		if err := waitHostHandoverLogEvent(currentHost, "recording handover_source_drained", recording.ID, 1, 40*time.Second); err != nil {
			t.Fatalf("handover did not reach its explicit source-drain boundary for %s: %v; host=%s", point, err, currentHost.output.String())
		}
		fixture.advance(stream, 1)
	}
	readyPath := runtimehook.ReadyMarkerPath(markerDir, point, recording.ID)
	if err := waitRuntimeConditionError(45*time.Second, func() bool {
		info, err := os.Lstat(readyPath)
		return err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == 0600
	}, "failpoint marker for "+string(point)); err != nil {
		t.Fatalf("production boundary %s was not reached: %v; host=%s; fixture=%s", point, err, currentHost.output.String(), fixture.describe(stream))
	}
	trackProducts(controlABinary, engineABinary, controlBBinary, engineBBinary, fixtureAdapterBinary)

	expectedOwner, err := ownerStore.Current(recording.ID)
	if err != nil {
		t.Fatalf("read durable owner at failpoint %s: %v", point, err)
	}
	wantGeneration := leaseA.EngineGeneration
	if pointAfterOwnerCAS(point) {
		registryBefore := readRuntimeGenerationSnapshot(t, dataDir)
		wantGeneration = registryBefore.ActiveGenerationID
	}
	if expectedOwner.EngineGeneration != wantGeneration || expectedOwner.Epoch < sourceOwner.Epoch {
		t.Fatalf("durable owner at %s is inconsistent: owner=%+v expected_generation=%s source_lease=%+v", point, expectedOwner, wantGeneration, leaseA)
	}
	if point == runtimehook.AfterTargetFirstCommit {
		targetEnginePID := processForBinary(engineBBinary)
		if targetEnginePID == 0 {
			t.Fatal("target Engine B PID was not live at first canonical commit failpoint")
		}
		tracked[targetEnginePID] = engineBBinary
	}
	lastBeforeCrash := uint64(5)
	if point == runtimehook.AfterTargetFirstCommit || point == runtimehook.BeforeSourceRetirement {
		lastBeforeCrash = 6
		// BeforeSourceRetirement deliberately freezes the Host after B has
		// acquired the durable owner but before A has detached its parked entry.
		// The management router rejects that transient duplicate Engine
		// inventory instead of guessing an owner, so observe the canonical root
		// directly while the Host is held at this failpoint.
		if point == runtimehook.BeforeSourceRetirement {
			waitCanonicalSegmentCount(t, dataDir, recording.ID, int(lastBeforeCrash), 30*time.Second)
		} else {
			waitRecordingSequenceCount(t, client, baseURL, recording.ID, int(lastBeforeCrash), 30*time.Second)
		}
	}
	var preCrashArchive *domain.Recording
	if point == runtimehook.BeforeSourceRetirement {
		store, err := storage.New(dataDir)
		if err != nil {
			t.Fatal(err)
		}
		preCrashArchive, err = store.LoadRecordingReadOnly(recording.ID)
		if err != nil {
			t.Fatalf("read canonical archive while source retirement is held: %v", err)
		}
	} else {
		preCrashArchive = getRecording(t, client, baseURL, recording.ID)
	}
	if !equalSequenceRange(recordingSequences(preCrashArchive), 1, lastBeforeCrash) || len(preCrashArchive.Gaps) != 0 {
		t.Fatalf("archive at %s has unexpected source sequence/gaps: sequence=%v gaps=%+v", point, recordingSequences(preCrashArchive), preCrashArchive.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, preCrashArchive, stream, 1, lastBeforeCrash)
	preCrashObjects, err := snapshotRecordingObjects(archiveDir)
	if err != nil {
		t.Fatalf("snapshot canonical archive objects at %s: %v", point, err)
	}
	if err := validateManifestSnapshotReferences(preCrashArchive, preCrashObjects); err != nil {
		t.Fatalf("pre-crash canonical manifest references are inconsistent at %s: %v", point, err)
	}
	var metadataPreCrash []domain.MetadataRevision
	if point == runtimehook.BeforeSourceRetirement {
		// The stable management route intentionally fails closed while both
		// Engine inventories report the handover boundary. The canonical root is
		// still the authoritative projection to inspect at this exact failpoint.
		metadataPreCrash = preCrashArchive.MetadataTimeline
	} else {
		metadataSnapshot := waitMetadataTimeline(t, ctx, client, baseURL, recording.ID, len(metadataBefore.Items), 10*time.Second)
		metadataPreCrash = metadataSnapshot.Items
	}
	if !reflect.DeepEqual(metadataBefore.Items, metadataPreCrash) {
		t.Fatalf("metadata changed semantically before crash at %s: before=%+v now=%+v", point, metadataBefore.Items, metadataPreCrash)
	}

	oldHostPID := currentHost.command.Process.Pid
	if err := hardKillRuntimeHost(currentHost, 10*time.Second); err != nil {
		t.Fatalf("SIGKILL production Host at %s: %v", point, err)
	}
	if processPIDPresent(oldHostPID) {
		t.Fatalf("SIGKILLed Runtime Host PID %d remains present", oldHostPID)
	}
	currentHost = startHost(false)
	recovered := waitRuntimeStatus(t, ctx, client, baseURL, func(s httpapi.Status) bool {
		return s.ActiveControl != nil && s.ActiveControl.Version == e2eVersionB && s.DefaultEngine != nil && s.DefaultEngine.Version == e2eVersionB
	}, currentHost)
	trackProducts(controlBBinary, engineBBinary, fixtureAdapterBinary)
	installationAfter := installation.ReadOnly(dataDir)
	if installationAfter.State != installation.StateReady || installationAfter.InstallationID != installationBefore.InstallationID {
		t.Fatalf("Host crash recovery changed durable installation state: before=%+v after=%+v", installationBefore, installationAfter)
	}
	if recovered.Host.Version != e2eVersionA || recovered.ActiveControl == nil || recovered.ActiveControl.Version != e2eVersionB || recovered.DefaultEngine == nil || recovered.DefaultEngine.Version != e2eVersionB {
		t.Fatalf("cold recovery did not reconcile the active B generation: %+v", recovered)
	}
	if _, err := os.Stat(archiveDir); err != nil {
		t.Fatalf("cold recovery changed/removed canonical Recording directory: %v", err)
	}
	postCrash := getRecording(t, client, baseURL, recording.ID)
	if postCrash.ID != recording.ID || postCrash.State != domain.StateInterrupted || len(postCrash.Gaps) != 0 || !equalSequenceRange(recordingSequences(postCrash), 1, lastBeforeCrash) {
		t.Fatalf("cold recovery must preserve the same archive and make its active state explicitly interrupted: id=%s state=%s seq=%v gaps=%+v", postCrash.ID, postCrash.State, recordingSequences(postCrash), postCrash.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, postCrash, stream, 1, lastBeforeCrash)
	postCrashObjects, err := snapshotRecordingObjects(archiveDir)
	if err != nil {
		t.Fatalf("snapshot canonical archive objects after cold recovery: %v", err)
	}
	if err := validateManifestSnapshotReferences(postCrash, postCrashObjects); err != nil {
		t.Fatalf("cold recovery exposed an incomplete manifest snapshot reference: %v", err)
	}
	var addedArchiveObjects []string
	for object := range postCrashObjects {
		if _, existed := preCrashObjects[object]; !existed {
			addedArchiveObjects = append(addedArchiveObjects, object)
		}
	}
	sort.Strings(addedArchiveObjects)
	t.Logf("append-only canonical archive objects observed after recovery: %v", addedArchiveObjects)
	if err := compareArchiveSnapshotsAllowingManifestAppend(preCrashObjects, postCrashObjects); err != nil {
		t.Fatalf("cold recovery rewrote or unexpectedly added canonical archive objects: %v; before=%v after=%v", err, preCrashObjects, postCrashObjects)
	}
	var metadataAfter []domain.MetadataRevision
	if point == runtimehook.BeforeSourceRetirement {
		// The handover fixture intentionally held both Engine inventories at
		// the same owner transition boundary. The public recording detail omits
		// the bounded source timeline, so compare the canonical root directly.
		store, err := storage.New(dataDir)
		if err != nil {
			t.Fatal(err)
		}
		afterArchive, err := store.LoadRecordingReadOnly(recording.ID)
		if err != nil {
			t.Fatalf("read canonical archive after source-retirement crash: %v", err)
		}
		metadataAfter = afterArchive.MetadataTimeline
	} else {
		metadataSnapshot := waitMetadataTimeline(t, ctx, client, baseURL, recording.ID, len(metadataBefore.Items), 10*time.Second)
		metadataAfter = metadataSnapshot.Items
	}
	if !reflect.DeepEqual(metadataBefore.Items, metadataAfter) {
		t.Fatalf("cold recovery changed canonical metadata timeline: before=%+v after=%+v", metadataBefore.Items, metadataAfter)
	}
	registryAfter := readRuntimeGenerationSnapshot(t, dataDir)
	if len(registryAfter.Leases) != 0 {
		t.Fatalf("cold recovery retained stale generation leases: %+v", registryAfter.Leases)
	}
	if registryAfter.ActiveGenerationID == "" || registryAfter.Generations[registryAfter.ActiveGenerationID].Version != e2eVersionB {
		t.Fatalf("cold recovery lost active B generation: %+v", registryAfter)
	}
	for _, stale := range []recordingowner.Owner{sourceOwner, expectedOwner} {
		called := false
		commitErr := ownerStore.WithCommit(stale, func() error { called = true; return nil })
		if (!errors.Is(commitErr, recordingowner.ErrNotFound) && !errors.Is(commitErr, recordingowner.ErrStaleOwner)) || called {
			t.Fatalf("cold recovery accepted stale owner tuple %+v: err=%v callback=%v", stale, commitErr, called)
		}
	}
	ownerRecordPath := filepath.Join(dataDir, "runtime", "recording-owners", recording.ID+".json")
	ownerRecordBytes, err := os.ReadFile(ownerRecordPath)
	if err != nil {
		t.Fatal(err)
	}
	var ownerHighWater struct {
		Epoch uint64 `json:"epoch"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(ownerRecordBytes, &ownerHighWater); err != nil || ownerHighWater.Epoch < expectedOwner.Epoch || ownerHighWater.State != "fenced" {
		t.Fatalf("cold recovery owner tombstone/high-water is invalid: record=%+v err=%v", ownerHighWater, err)
	}

	if point == runtimehook.BeforeTargetFirstCommit || point == runtimehook.AfterTargetFirstCommit {
		// Cold recovery has already installed a newer durable owner fence and
		// explicitly interrupted this archive. Release the real Engine process
		// only after recovery; the before-commit case has an already-fetched
		// candidate waiting at the common canonical commit path.
		rootBeforeRelease, err := os.ReadFile(filepath.Join(archiveDir, "recording.json"))
		if err != nil {
			t.Fatalf("read recovered canonical root before releasing Engine: %v", err)
		}
		if err := writeRuntimeHookRelease(markerDir, point, recording.ID); err != nil {
			t.Fatal(err)
		}
		if point == runtimehook.BeforeTargetFirstCommit {
			markerPath := runtimehook.ObservationMarkerPath(markerDir, runtimehook.StaleOwnerCommitRejected, recording.ID)
			if err := waitRuntimeConditionError(20*time.Second, func() bool {
				info, statErr := os.Lstat(markerPath)
				return statErr == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm() == 0600 && info.Size() == 0
			}, "orphan Engine common-fence stale-owner rejection marker"); err != nil {
				t.Fatalf("released Engine did not prove its staged canonical commit was rejected by the owner fence: %v; child=%s", err, readRuntimeE2EChildDiagnostics(markerDir))
			}
		}
		staleCommitCalled := false
		staleCommitErr := ownerStore.WithCommit(expectedOwner, func() error {
			staleCommitCalled = true
			return nil
		})
		if (!errors.Is(staleCommitErr, recordingowner.ErrNotFound) && !errors.Is(staleCommitErr, recordingowner.ErrStaleOwner)) || staleCommitCalled {
			t.Fatalf("released orphan owner token could still reach canonical commit: err=%v callback=%v", staleCommitErr, staleCommitCalled)
		}
		afterStaleAttempt := getRecording(t, client, baseURL, recording.ID)
		if afterStaleAttempt.State != domain.StateInterrupted || !equalSequenceRange(recordingSequences(afterStaleAttempt), 1, lastBeforeCrash) || len(afterStaleAttempt.Gaps) != 0 {
			t.Fatalf("cold recovery or orphan polling changed the canonical archive after fencing: state=%s sequences=%v gaps=%+v", afterStaleAttempt.State, recordingSequences(afterStaleAttempt), afterStaleAttempt.Gaps)
		}
		if after, snapshotErr := snapshotRecordingObjects(archiveDir); snapshotErr != nil || !reflect.DeepEqual(postCrashObjects, after) {
			t.Fatalf("old fenced Engine rewrote canonical payload/sidecar objects: err=%v before=%v after=%v", snapshotErr, postCrashObjects, after)
		}
		rootAfterRelease, err := os.ReadFile(filepath.Join(archiveDir, "recording.json"))
		if err != nil || !reflect.DeepEqual(rootBeforeRelease, rootAfterRelease) {
			t.Fatalf("old fenced Engine changed canonical recording root: err=%v", err)
		}
	}

	trackProducts(controlABinary, engineABinary, controlBBinary, engineBBinary, fixtureAdapterBinary)
	t.Logf("production Host crash matrix point=%s Recording=%s owner=%s/%d preCrashSequences=1..%d coldState=%s activeGeneration=%s", point, recording.ID, expectedOwner.EngineGeneration, expectedOwner.Epoch, lastBeforeCrash, postCrash.State, registryAfter.ActiveGenerationID)
}

func runProductionUpdateScenario(t *testing.T, artifacts runtimeUpdateArtifacts, fixture *runtimeUpdateFixture, iteration int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { makeRuntimeE2ETreeWritable(dataDir) })
	streamR := fmt.Sprintf("r-%d", iteration)
	streamW := fmt.Sprintf("w-%d", iteration)
	streamS := fmt.Sprintf("s-%d", iteration)
	fixture.reset(streamR, "Before update", "Before update", "session-r-"+strconv.Itoa(iteration))
	fixture.reset(streamW, "Watch source", "Watch description", fmt.Sprintf("session-w-%d", iteration))
	fixture.reset(streamS, "New source", "New description", "session-s-"+strconv.Itoa(iteration))
	controlABinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineABinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	installedDir := filepath.Join(dataDir, "runtime", "releases", install.ReleaseDirectoryID(e2eVersionB, e2eCommitB))
	controlBBinary := filepath.Join(installedDir, fmt.Sprintf("%s-%s-%s", release.RoleControlPlane, runtime.GOOS, runtime.GOARCH))
	engineBBinary := filepath.Join(installedDir, fmt.Sprintf("%s-%s-%s", release.RoleRecorderEngine, runtime.GOOS, runtime.GOARCH))
	fixtureAdapterBinary := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-runtime-update-fixture")
	var localStorageArtifact string

	listenAddr := reserveRuntimeAddress(t)
	command := exec.Command(artifacts.hostA)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"AUTH_DISABLED=1",
		"ADAPTER_DIR=" + artifacts.adapterDir,
		"IR_ALLOW_OPERATOR_PLUGINS=1",
		"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
		"IR_RELEASE_BUNDLE_DIR=" + artifacts.packageB,
		"IR_RELEASE_TRUSTED_KEYS_JSON=" + artifacts.publicKeys,
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
	t.Cleanup(func() {
		stopRuntimeHostProcess(process)
		executables := []string{artifacts.hostA, controlABinary, engineABinary, controlBBinary, engineBBinary, fixtureAdapterBinary}
		if localStorageArtifact != "" {
			executables = append(executables, localStorageArtifact)
		}
		for _, executable := range executables {
			if err := waitProcessAbsent(t, executable, 10*time.Second); err != nil {
				t.Errorf("acceptance cleanup left product process running for %s: %v", filepath.Base(executable), err)
			}
		}
	})
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 3 * time.Minute}
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	installationBefore := installation.ReadOnly(dataDir)
	if installationBefore.State != installation.StateReady || installationBefore.InstallationID == "" {
		t.Fatalf("AUTH_DISABLED bootstrap did not durably initialize installation: %+v", installationBefore)
	}
	if status.Host.Version != e2eVersionA || status.Host.Commit != e2eCommitA || status.Application.Version != e2eVersionA || status.Application.Commit != e2eCommitA {
		t.Fatalf("initial production Host/Application identity = host=%+v application=%+v, want release A", status.Host, status.Application)
	}
	if status.ActiveControl == nil || status.ActiveControl.Version != e2eVersionA || status.ActiveControl.Commit != e2eCommitA || status.DefaultEngine == nil || status.DefaultEngine.Version != e2eVersionA || status.DefaultEngine.Commit != e2eCommitA {
		t.Fatalf("release A is not active/default at bootstrap: %+v", status)
	}
	if status.VerificationState != "not_checked" {
		t.Fatalf("fresh Host verification state=%q, want not_checked", status.VerificationState)
	}

	controlAPID := waitProcessForBinary(t, controlABinary, 20*time.Second)
	engineAPID := waitProcessForBinary(t, engineABinary, 20*time.Second)
	if controlAPID == engineAPID || controlAPID == command.Process.Pid || engineAPID == command.Process.Pid {
		t.Fatalf("Host/Control A/Engine A are not separate production processes: host=%d control=%d engine=%d", command.Process.Pid, controlAPID, engineAPID)
	}
	localStorageArtifact = pinnedLocalStorageArtifact(t, dataDir, status.DefaultEngine.ID)
	localStoragePID := waitDirectChildForBinary(t, engineAPID, localStorageArtifact, 20*time.Second)
	if localStoragePID == engineAPID || localStoragePID == command.Process.Pid {
		t.Fatalf("storage.local is not a separate immutable provider child of Engine A: host=%d engine=%d provider=%d artifact=%s", command.Process.Pid, engineAPID, localStoragePID, localStorageArtifact)
	}

	inputR := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + streamR}
	recordingR := createRuntimeRecording(t, client, baseURL, "Recording R", inputR)
	if recordingR.Title != "Recording R" || recordingR.State != domain.StateRecording {
		t.Fatalf("manual recording R was not created through Control API: id=%s title=%q state=%s", recordingR.ID, recordingR.Title, recordingR.State)
	}
	leaseA := waitRecordingLease(t, dataDir, recordingR.ID, 30*time.Second)
	if leaseA.EngineGeneration != status.DefaultEngine.ID {
		t.Fatalf("R is pinned to generation %s, active A generation is %s", leaseA.EngineGeneration, status.DefaultEngine.ID)
	}
	fixture.advance(streamR, 20)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 20, 20*time.Second)
	baseline := getRecording(t, client, baseURL, recordingR.ID)
	verifyRuntimeRecordingSegments(t, dataDir, baseline, streamR, 1, 20)
	baselineSequences := recordingSequences(baseline)
	if !equalSequenceRange(baselineSequences, 1, 20) {
		t.Fatalf("pre-update source sequence baseline is not contiguous: %v", baselineSequences)
	}
	assertRuntimeStorageRange(t, client, baseURL, baseline, streamR)
	metadataBaseline := waitMetadataTimeline(t, ctx, client, baseURL, recordingR.ID, 1, 35*time.Second)
	if len(metadataBaseline.Items) != 1 || stringValue(metadataBaseline.Items[0].Title) != "Before update" || stringValue(metadataBaseline.Items[0].Description) != "Before update" {
		t.Fatalf("unexpected source metadata baseline: %+v", metadataBaseline.Items)
	}

	inputW := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + streamW}
	watchID := createRuntimeWatch(t, client, baseURL, "Watch W", inputW)
	waitFixtureWatchPoll(t, fixture, streamW, 20*time.Second)
	watchBefore, _ := getRuntimeJSON[map[string]any](t, client, baseURL, http.MethodGet, "/api/watches/"+watchID, nil)
	if watchBefore["enabled"] != true {
		t.Fatalf("Watch W did not persist enabled: %#v", watchBefore)
	}

	// The Host API performs signed discovery and immutable staging. Package
	// hashes are also checked above, while successful Host staging proves its
	// actual Ed25519 verifier accepted the exact manifest and artifacts.
	checkStatus, code := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodPost, httpapi.Endpoint+"/check", map[string]any{})
	if code != http.StatusOK || !checkStatus.UpdatesAvailable || checkStatus.AvailableRelease == nil || checkStatus.AvailableRelease.Version != e2eVersionB || checkStatus.VerificationState != "verified" {
		t.Fatalf("production update check did not discover/verify release B: code=%d status=%+v", code, checkStatus)
	}
	stageStatus, code := getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodPost, httpapi.Endpoint+"/stage", map[string]any{})
	if code != http.StatusOK || stageStatus.StagedRelease == nil || stageStatus.StagedRelease.Version != e2eVersionB || stageStatus.StagedRelease.Commit != e2eCommitB || stageStatus.VerificationState != "verified" {
		t.Fatalf("production update stage did not install verified B: code=%d status=%+v", code, stageStatus)
	}
	if err := verifyInstalledRuntimeRelease(installedDir, artifacts.manifestB); err != nil {
		t.Fatalf("staged immutable B release failed independent size/hash/signature checks: %v", err)
	}
	if engineProcess := processForBinary(engineABinary); engineProcess == 0 {
		t.Fatal("Engine A exited during signed B staging")
	}

	refreshStarted, releaseRefresh := fixture.expireAndBlockRefresh(streamR)
	t.Cleanup(releaseRefresh)
	fixture.advance(streamR, 10)
	var refreshAt time.Time
	select {
	case refreshAt = <-refreshStarted:
	case <-ctx.Done():
		t.Fatalf("Engine A did not require and begin media refresh after the old token expired: %v", ctx.Err())
	case <-time.After(10 * time.Second):
		t.Fatal("Engine A did not call the fixture adapter's refresh capability after source expiry")
	}
	activationResult := make(chan productionActivationOutcome, 1)
	go func() {
		outcome := productionActivationOutcome{started: time.Now().UTC()}
		outcome.code, outcome.err = requestRuntimeJSON(client, baseURL, http.MethodPost, httpapi.Endpoint+"/activate", map[string]any{}, &outcome.status)
		outcome.finished = time.Now().UTC()
		activationResult <- outcome
	}()
	activation := waitActivationOutcome(t, activationResult, 30*time.Second)
	activateStarted, activateFinished := activation.started, activation.finished
	activateStatus, code := activation.status, activation.code
	if activation.err != nil {
		releaseRefresh()
		t.Fatalf("production activate request failed: %v; host output=%s", activation.err, process.output.String())
	}
	if code != http.StatusOK || activateStatus.ActiveControl == nil || activateStatus.ActiveControl.Version != e2eVersionB || activateStatus.ActiveControl.Commit != e2eCommitB || activateStatus.DefaultEngine == nil || activateStatus.DefaultEngine.Version != e2eVersionB || activateStatus.DefaultEngine.Commit != e2eCommitB {
		releaseRefresh()
		t.Fatalf("production update API failed to activate B: code=%d status=%+v; host output=%s", code, activateStatus, process.output.String())
	}
	// The blocked A refresh makes the first target probe return a safe
	// refresh-required response. Wait for the Host's structured drain event
	// before releasing A; this is an explicit lifecycle synchronization point,
	// not a timing assumption.
	if err := waitHostHandoverLogEvent(process, "recording handover_source_drain_started", recordingR.ID, 1, 30*time.Second); err != nil {
		releaseRefresh()
		runtime := readRuntimeGenerationSnapshot(t, dataDir)
		ownerStore, ownerErr := recordingowner.Open(dataDir)
		var owner recordingowner.Owner
		if ownerErr == nil {
			owner, ownerErr = ownerStore.Current(recordingR.ID)
		}
		var generations []generation.Generation
		for _, item := range runtime.Generations {
			generations = append(generations, item)
		}
		sort.Slice(generations, func(i, j int) bool { return generations[i].ID < generations[j].ID })
		t.Fatalf("%v; activation returned code=%d status=%+v at=%s..%s; owner=%+v owner_err=%v generations=%+v leases=%+v; host output=%s", err, code, activateStatus, activateStarted, activateFinished, owner, ownerErr, generations, runtime.Leases, process.output.String())
	}
	if refreshAt.After(activateFinished) {
		releaseRefresh()
		t.Fatalf("fixture refresh began after activation returned; expected an in-flight Engine A refresh: refresh=%s activation=%s..%s", refreshAt, activateStarted, activateFinished)
	}
	installationAfter := installation.ReadOnly(dataDir)
	if installationAfter.State != installation.StateReady || installationAfter.InstallationID != installationBefore.InstallationID {
		releaseRefresh()
		t.Fatalf("A→B application activation changed Host-owned installation state: before=%+v after=%+v", installationBefore, installationAfter)
	}
	if err := waitProcessAbsent(t, controlABinary, 20*time.Second); err != nil {
		releaseRefresh()
		t.Fatalf("old production Control A process did not exit after activation: %v", err)
	}
	fixture.setMetadata(streamR, "After update title", "After update description")
	controlBPID := waitProcessForBinary(t, controlBBinary, 20*time.Second)
	engineBPID := waitProcessForBinary(t, engineBBinary, 20*time.Second)
	if controlBPID == controlAPID || engineBPID == engineAPID || controlBPID == engineBPID {
		releaseRefresh()
		t.Fatalf("release B did not spawn distinct product Control/Engine processes: A=(%d,%d) B=(%d,%d)", controlAPID, engineAPID, controlBPID, engineBPID)
	}
	localStorageArtifactB := pinnedLocalStorageArtifact(t, dataDir, activateStatus.DefaultEngine.ID)
	if localStorageArtifactB != localStorageArtifact {
		releaseRefresh()
		t.Fatalf("application-only update changed the pinned bundled storage.local artifact: A=%s B=%s", localStorageArtifact, localStorageArtifactB)
	}
	if providerPID := waitDirectChildForBinary(t, engineBPID, localStorageArtifactB, 20*time.Second); providerPID == engineBPID || providerPID == command.Process.Pid {
		releaseRefresh()
		t.Fatalf("storage.local is not a separate immutable provider child of Engine B: host=%d engine=%d provider=%d", command.Process.Pid, engineBPID, providerPID)
	}
	if processForBinary(engineABinary) == 0 {
		releaseRefresh()
		t.Fatal("Engine A exited while R still held its generation lease")
	}
	status = waitRuntimeStatus(t, ctx, client, baseURL, func(s httpapi.Status) bool {
		return s.ActiveControl != nil && s.ActiveControl.Version == e2eVersionB
	})
	leaseNow := readRuntimeLeases(t, dataDir)
	if leaseNow[recordingR.ID].EngineGeneration != leaseA.EngineGeneration {
		releaseRefresh()
		t.Fatalf("R changed Engine generation before its blocked source refresh could finish: before=%s after=%s", leaseA.EngineGeneration, leaseNow[recordingR.ID].EngineGeneration)
	}
	ownerStoreBeforeFailure, err := recordingowner.Open(dataDir)
	if err != nil {
		releaseRefresh()
		t.Fatal(err)
	}
	ownerBeforeTargetFailure, err := ownerStoreBeforeFailure.Current(recordingR.ID)
	if err != nil {
		releaseRefresh()
		t.Fatal(err)
	}
	// Freeze the actual B Engine process while R is still blocked in its A-side
	// refresh. The Host has already requested a source drain based on the typed
	// preflight result; after A drains, its target request must fail without an
	// owner transfer, then the source resumes and remains the only writer.
	if err := syscall.Kill(engineBPID, syscall.SIGSTOP); err != nil {
		releaseRefresh()
		t.Fatalf("could not suspend target Engine B for handover failure injection: %v", err)
	}
	resumeTargetEngine := func() { _ = syscall.Kill(engineBPID, syscall.SIGCONT) }
	t.Cleanup(resumeTargetEngine)
	if refreshes := fixture.refreshesFor(streamR); len(refreshes) != 1 {
		releaseRefresh()
		t.Fatalf("target preflight issued a concurrent adapter refresh while Engine A was refreshing: %+v", refreshes)
	}
	releaseRefresh()
	if err := waitHostHandoverLogEvent(process, "recording handover_source_drained", recordingR.ID, 1, 30*time.Second); err != nil {
		t.Fatalf("%v; host output=%s", err, process.output.String())
	}
	// Publish the exact next object after A has reached the durable drained
	// boundary. The stopped target forces one pre-CAS failure; the source must
	// resume and capture this object before a later target retry.
	fixture.advance(streamR, 1)
	recordingDuringTargetFailure := waitCanonicalSegmentCount(t, dataDir, recordingR.ID, 31, 30*time.Second)
	if recordingDuringTargetFailure.State != domain.StateRecording || len(recordingDuringTargetFailure.Gaps) != 0 {
		t.Fatalf("R did not continue on source Engine A while target Engine B was unavailable: state=%s gaps=%+v; fixture=%s", recordingDuringTargetFailure.State, recordingDuringTargetFailure.Gaps, fixture.describe(streamR))
	}
	ownerAfterTargetFailure, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	durableOwnerAfterTargetFailure, err := ownerAfterTargetFailure.Current(recordingR.ID)
	if err != nil || durableOwnerAfterTargetFailure != ownerBeforeTargetFailure || durableOwnerAfterTargetFailure.EngineGeneration != leaseA.EngineGeneration {
		t.Fatalf("unavailable target changed durable R ownership: before=%+v after=%+v err=%v; host=%s", ownerBeforeTargetFailure, durableOwnerAfterTargetFailure, err, process.output.String())
	}
	resumeTargetEngine()
	if err := waitHostHandoverLogEvent(process, "recording handover_source_drain_started", recordingR.ID, 2, 30*time.Second); err != nil {
		t.Fatalf("%v; host output=%s", err, process.output.String())
	}
	if err := waitHostHandoverLogEvent(process, "recording handover_source_drained", recordingR.ID, 2, 30*time.Second); err != nil {
		t.Fatalf("%v; host output=%s", err, process.output.String())
	}
	// The latest source object is now the committed tail. Publish the next one
	// while A is paused so drained target preflight can poll a fresh manifest
	// and stage it before the Host's durable owner CAS.
	fixture.advance(streamR, 1)
	leaseAfterHandover := waitRecordingLeaseGeneration(t, dataDir, recordingR.ID, activateStatus.DefaultEngine.ID, 40*time.Second, process.output.String, func() string { return fixture.describe(streamR) })
	if leaseAfterHandover.EngineGeneration == leaseA.EngineGeneration {
		t.Fatalf("eligible R was not handed from Engine A to active Engine B: lease=%+v", leaseAfterHandover)
	}
	if err := waitProcessAbsent(t, engineABinary, 30*time.Second); err != nil {
		t.Fatalf("old Engine A did not retire after R ownership moved to B: %v", err)
	}
	refreshes := fixture.refreshesFor(streamR)
	if len(refreshes) != 1 || refreshes[0].Token != "token-0" || !refreshes[0].At.Before(activateFinished) {
		t.Fatalf("expired source did not require exactly one pre-activation Engine A refresh: %+v, activation=%s", refreshes, activateFinished)
	}
	if err := waitWatchPollingAfter(t, fixture, streamW, activateFinished, 20*time.Second); err != nil {
		t.Fatalf("Watch W did not resume polling after Control B activation: %v", err)
	}
	fixture.setOnline(streamW, true)
	watchRecordingID := waitWatchRecording(t, client, baseURL, watchID, 30*time.Second)
	watchRelations := waitWatchRelationCount(t, client, baseURL, watchID, 1, 30*time.Second)
	if len(watchRelations) != 1 || watchRelations[0].ID != watchRecordingID {
		t.Fatalf("one live Watch session did not produce exactly one linked Recording: watch=%s recording=%s relations=%+v", watchID, watchRecordingID, watchRelations)
	}
	watchLease := waitRecordingLease(t, dataDir, watchRecordingID, 20*time.Second)
	if watchLease.EngineGeneration != activateStatus.DefaultEngine.ID {
		t.Fatalf("Watch-created recording is pinned to %s, want B %s", watchLease.EngineGeneration, activateStatus.DefaultEngine.ID)
	}

	inputS := map[string]string{"source_url": artifacts.fixtureURL + "/source/" + streamS}
	recordingS := createRuntimeRecording(t, client, baseURL, "Recording S", inputS)
	leaseS := waitRecordingLease(t, dataDir, recordingS.ID, 20*time.Second)
	if leaseS.EngineGeneration != activateStatus.DefaultEngine.ID || leaseS.EngineGeneration == leaseA.EngineGeneration {
		t.Fatalf("post-update Recording S does not use B while R uses A: R=%s S=%s B=%s", leaseA.EngineGeneration, leaseS.EngineGeneration, activateStatus.DefaultEngine.ID)
	}
	fixture.advance(streamS, 2)
	waitRecordingSequenceCount(t, client, baseURL, recordingS.ID, 2, 15*time.Second)

	fixture.advance(streamR, 28)
	waitRecordingSequenceCount(t, client, baseURL, recordingR.ID, 60, 20*time.Second)
	finalR := getRecording(t, client, baseURL, recordingR.ID)
	if finalR.ID != recordingR.ID || finalR.Title != "Recording R" || finalR.State != domain.StateRecording || len(finalR.Gaps) != 0 {
		t.Fatalf("R identity/title/state/gaps changed during application update: id=%s title=%q state=%s gaps=%+v", finalR.ID, finalR.Title, finalR.State, finalR.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, finalR, streamR, 1, 60)
	finalSequences := recordingSequences(finalR)
	if !equalSequenceRange(finalSequences, 1, 60) {
		t.Fatalf("source sequence continuity failed across activation/refresh: %v", finalSequences)
	}
	requests := fixture.segmentRequestsFor(streamR)
	for sequence := uint64(1); sequence <= 60; sequence++ {
		if len(requests[sequence]) != 1 || !requests[sequence][0].Succeeded {
			t.Fatalf("source sequence %d request count/success = %+v; want one successful request", sequence, requests[sequence])
		}
	}
	var firstRefreshedRequest time.Time
	for sequence := uint64(21); sequence <= 60; sequence++ {
		for _, request := range requests[sequence] {
			if request.Token == "token-1" && (firstRefreshedRequest.IsZero() || request.At.Before(firstRefreshedRequest)) {
				firstRefreshedRequest = request.At
			}
		}
	}
	if firstRefreshedRequest.IsZero() || firstRefreshedRequest.Before(refreshes[0].At) {
		t.Fatalf("no post-refresh source sequence was captured with the refreshed token; first=%s refresh=%+v", firstRefreshedRequest, refreshes)
	}
	metadataAfterActivation := waitMetadataTimeline(t, ctx, client, baseURL, recordingR.ID, 2, 40*time.Second)
	if len(metadataAfterActivation.Items) != 2 || stringValue(metadataAfterActivation.Items[0].Title) != "Before update" || stringValue(metadataAfterActivation.Items[1].Title) != "After update title" {
		t.Fatalf("R metadata timeline is not exactly Before→After: %+v", metadataAfterActivation.Items)
	}
	metadataRequests := fixture.metadataRequestsFor(streamR)
	if !containsTimeAfter(metadataRequests, activateFinished) {
		t.Fatalf("no source metadata poll occurred after old Control A exited/Control B activated: %v activation=%s", metadataRequests, activateFinished)
	}
	// Repeated identical metadata polling must not add semantic revisions.
	if len(metadataAfterActivation.Items) != 2 {
		t.Fatalf("identical metadata polling created duplicate revisions: %+v", metadataAfterActivation.Items)
	}

	status = waitRuntimeStatus(t, ctx, client, baseURL, func(s httpapi.Status) bool {
		return s.ActiveControl != nil && s.ActiveControl.Version == e2eVersionB
	})
	if processForBinary(engineABinary) != 0 {
		t.Fatal("old Engine A remained alive after its final Recording handover lease was released")
	}
	currentLeases := readRuntimeLeases(t, dataDir)
	if currentLeases[recordingR.ID].EngineGeneration != leaseAfterHandover.EngineGeneration || currentLeases[recordingS.ID].EngineGeneration != leaseS.EngineGeneration || currentLeases[watchRecordingID].EngineGeneration != watchLease.EngineGeneration {
		t.Fatalf("recording generation pins are inconsistent during overlap: leases=%+v", currentLeases)
	}
	if got := len(waitWatchRelationCount(t, client, baseURL, watchID, 1, 5*time.Second)); got != 1 {
		t.Fatalf("Watch W created duplicate recordings for one live session: relation count=%d", got)
	}

	// End B-owned work before proving that A's last lease is the only blocker.
	setWatchEnabled(t, client, baseURL, watchID, false)
	stopRecording(t, client, baseURL, watchRecordingID)
	stopRecording(t, client, baseURL, recordingS.ID)
	stopRecording(t, client, baseURL, recordingR.ID)
	waitRuntimeRecordingState(t, client, baseURL, recordingR.ID, domain.StateStopped, 20*time.Second)
	assertRuntimeStorageIntegrity(t, client, baseURL, recordingR.ID)
	waitLeaseAbsent(t, dataDir, recordingR.ID, 20*time.Second)
	if err := waitProcessAbsent(t, engineABinary, 30*time.Second); err != nil {
		t.Fatalf("A Engine was not already retired after R handover and final lease drain: %v", err)
	}
	state := readRuntimeGenerationSnapshot(t, dataDir)
	if lease, exists := state.Leases[recordingR.ID]; exists {
		t.Fatalf("R lease remained after stop/drain: %+v", lease)
	}
	retained, exists := state.Generations[leaseA.EngineGeneration]
	if !exists || retained.State != generation.StateDraining || !retained.EngineDormant || state.PreviousGenerationID != leaseA.EngineGeneration {
		t.Fatalf("A rollback generation was not retained dormant after its final lease: generation=%+v exists=%v snapshot=%+v", retained, exists, state)
	}
	if _, err := os.Stat(filepath.Join(artifacts.bundleA, "control-plane")); err != nil {
		t.Fatalf("immutable A rollback release was removed: %v", err)
	}
	if status.PreviousRelease == nil || status.PreviousRelease.Version != e2eVersionA || status.PreviousRelease.Commit != e2eCommitA {
		t.Fatalf("Host status no longer exposes retained A rollback release: %+v", status.PreviousRelease)
	}
	registryPath := filepath.Join(dataDir, "runtime", "state", "generations.json")
	registryInfo, err := os.Stat(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	reconcileAfter := registryInfo.ModTime()
	waitRuntimeCondition(t, 12*time.Second, func() bool {
		info, err := os.Stat(registryPath)
		return err == nil && info.ModTime().After(reconcileAfter)
	}, "post-drain lease reconciliation")
	status, code = getRuntimeJSON[httpapi.Status](t, client, baseURL, http.MethodGet, httpapi.Endpoint, nil)
	if code != http.StatusOK || status.ActiveControl == nil || status.ActiveControl.Version != e2eVersionB || status.PreviousRelease == nil || status.PreviousRelease.Version != e2eVersionA {
		t.Fatalf("Runtime Host did not remain ready after dormant-generation lease reconciliation: code=%d status=%+v", code, status)
	}
	if processForBinary(controlABinary) != 0 || processForBinary(engineABinary) != 0 {
		t.Fatal("release A product processes remain after terminal drain")
	}
	if processForBinary(artifacts.hostA) == 0 {
		t.Fatal("Runtime Host exited before scenario cleanup")
	}
	// Keep evidence that the same session created one Watch relation and that
	// the new manual Recording used B after the route switch.
	if recordingS.State == domain.StateInterrupted || recordingS.ID == recordingR.ID {
		t.Fatalf("post-update Recording S identity/state is invalid: %+v", recordingS)
	}
	t.Logf("production process E2E iteration %d: host pid=%d; Control A pid=%d exited; Engine A pid=%d handed R to Engine B and retired; Control B pid=%d; Engine B pid=%d; R %s sequence=1..60 on A→B with metadata/refresh continuity; S %s on B; Watch session %s produced %s", iteration, command.Process.Pid, controlAPID, engineAPID, controlBPID, engineBPID, recordingR.ID, recordingS.ID, fmt.Sprintf("session-w-%d", iteration), watchRecordingID)
}

func pinnedLocalStorageArtifact(t *testing.T, dataDir, generationID string) string {
	t.Helper()
	snapshot := readRuntimeGenerationSnapshot(t, dataDir)
	generationRecord, ok := snapshot.Generations[generationID]
	if !ok || generationRecord.StorageProviderSetID == "" {
		t.Fatalf("generation %q does not pin a storage provider set: %+v", generationID, generationRecord)
	}
	catalog, err := storagecatalog.OpenReadOnly(filepath.Join(dataDir, "runtime", "storage-providers"))
	if err != nil {
		t.Fatalf("open Host-owned storage provider catalog: %v", err)
	}
	set, err := catalog.LoadSet(generationRecord.StorageProviderSetID)
	if err != nil || set.Artifact.ID != "local" {
		t.Fatalf("generation %q is not pinned to storage.local: set=%+v err=%v", generationID, set, err)
	}
	path, err := catalog.ArtifactPath(set.Artifact.Digest)
	if err != nil {
		t.Fatalf("resolve immutable storage.local artifact: %v", err)
	}
	return path
}

func verifyInstalledRuntimeRelease(directory string, manifest release.Manifest) error {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0500 {
		return errors.New("installed release directory is not immutable/private")
	}
	for _, artifact := range manifest.Artifacts {
		path := filepath.Join(directory, artifact.Filename)
		fileInfo, err := os.Lstat(path)
		if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode()&os.ModeSymlink != 0 || fileInfo.Size() != artifact.Size || fileInfo.Mode().Perm() != 0500 {
			return fmt.Errorf("installed artifact %s metadata differs from signed manifest", artifact.Role)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 {
			return fmt.Errorf("installed artifact %s hash differs from signed manifest", artifact.Role)
		}
	}
	return nil
}

func makeRuntimeE2ETreeWritable(root string) {
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		mode := info.Mode().Perm()
		if info.IsDir() {
			mode |= 0700
		} else {
			mode |= 0600
		}
		_ = os.Chmod(path, mode)
		return nil
	})
}

func waitRuntimeHostStatus(t *testing.T, ctx context.Context, client *http.Client, baseURL string, process *runtimeHostProcess) httpapi.Status {
	t.Helper()
	return waitRuntimeStatus(t, ctx, client, baseURL, func(status httpapi.Status) bool {
		return status.Host.Version == e2eVersionA
	}, process)
}

func waitRuntimeStatus(t *testing.T, ctx context.Context, client *http.Client, baseURL string, condition func(httpapi.Status) bool, process ...*runtimeHostProcess) httpapi.Status {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		var status httpapi.Status
		code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, httpapi.Endpoint, map[string]any{}, &status)
		if err == nil && code == http.StatusOK && condition(status) {
			return status
		}
		for _, item := range process {
			select {
			case <-item.done:
				diagnostics := readRuntimeE2EChildDiagnostics(item.diagnosticDir)
				t.Fatalf("production Runtime Host exited before update API readiness (err=%v): %s%s", item.err, item.output.String(), diagnostics)
			default:
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for Runtime Host status failed: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("Runtime Host status did not reach expected state at %s", baseURL)
	return httpapi.Status{}
}

func getRuntimeJSON[T any](t *testing.T, client *http.Client, baseURL, method, path string, body any) (T, int) {
	t.Helper()
	var result T
	code, err := requestRuntimeJSON(client, baseURL, method, path, body, &result)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return result, code
}

func requestRuntimeJSON(client *http.Client, baseURL, method, path string, body any, target any) (int, error) {
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		requestBody = strings.NewReader(string(encoded))
	}
	request, err := http.NewRequest(method, baseURL+path, requestBody)
	if err != nil {
		return 0, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return response.StatusCode, err
	}
	if target != nil && len(data) != 0 {
		if err := json.Unmarshal(data, target); err != nil {
			return response.StatusCode, fmt.Errorf("decode response status %d: %w; body=%s", response.StatusCode, err, boundedOutput(data))
		}
	}
	return response.StatusCode, nil
}

func createRuntimeRecording(t *testing.T, client *http.Client, baseURL, title string, input map[string]string) domain.Recording {
	t.Helper()
	value, code := getRuntimeJSON[domain.Recording](t, client, baseURL, http.MethodPost, "/api/recordings", map[string]any{
		"adapter_id": runtimeE2EAdapterID, "input": input, "title": title, "preview_mode": "disabled",
	})
	if code != http.StatusCreated {
		t.Fatalf("create recording through Control API returned %d: %+v", code, value)
	}
	return value
}

func createRuntimeWatch(t *testing.T, client *http.Client, baseURL, title string, input map[string]string) string {
	t.Helper()
	value, code := getRuntimeJSON[map[string]any](t, client, baseURL, http.MethodPost, "/api/watches", map[string]any{
		"adapter_id": runtimeE2EAdapterID, "input": input, "title": title, "preview_mode": "disabled", "check_interval_seconds": 2,
	})
	if code != http.StatusCreated {
		t.Fatalf("create Watch through Control API returned %d: %#v", code, value)
	}
	id, _ := value["id"].(string)
	if len(id) != 32 {
		t.Fatalf("Watch API returned invalid ID: %#v", value)
	}
	return id
}

func waitFixtureWatchPoll(t *testing.T, fixture *runtimeUpdateFixture, stream string, timeout time.Duration) {
	t.Helper()
	waitRuntimeCondition(t, timeout, func() bool { return fixture.watchRequestCount(stream) > 0 }, "offline Watch poll")
}

func waitWatchPollingAfter(t *testing.T, fixture *runtimeUpdateFixture, stream string, after time.Time, timeout time.Duration) error {
	t.Helper()
	return waitRuntimeConditionError(timeout, func() bool {
		f := fixture
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, at := range f.streams[stream].watchRequests {
			if at.After(after) {
				return true
			}
		}
		return false
	}, "post-activation Watch poll")
}

func waitWatchRecording(t *testing.T, client *http.Client, baseURL, watchID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, code := getRuntimeJSON[map[string]any](t, client, baseURL, http.MethodGet, "/api/watches/"+watchID, nil)
		if code == http.StatusOK && value["state"] == "recording" {
			if id, _ := value["current_recording_id"].(string); len(id) == 32 {
				return id
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Watch %s did not create a recording; latest=%#v", watchID, mustRuntimeMap(t, client, baseURL, "/api/watches/"+watchID))
	return ""
}

type watchRecordingSummary struct {
	ID string `json:"id"`
}

func waitWatchRelationCount(t *testing.T, client *http.Client, baseURL, watchID string, expected int, timeout time.Duration) []watchRecordingSummary {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, code := getRuntimeJSON[struct {
			Items []watchRecordingSummary `json:"items"`
		}](t, client, baseURL, http.MethodGet, "/api/watches/"+watchID+"/recordings?limit=20", nil)
		if code == http.StatusOK && len(value.Items) == expected {
			return value.Items
		}
		time.Sleep(100 * time.Millisecond)
	}
	value := mustRuntimeMap(t, client, baseURL, "/api/watches/"+watchID+"/recordings?limit=20")
	t.Fatalf("Watch relation count did not become %d: %#v", expected, value)
	return nil
}

func mustRuntimeMap(t *testing.T, client *http.Client, baseURL, path string) map[string]any {
	t.Helper()
	value, code := getRuntimeJSON[map[string]any](t, client, baseURL, http.MethodGet, path, nil)
	if code != http.StatusOK {
		t.Fatalf("GET %s returned %d: %#v", path, code, value)
	}
	return value
}

func getRecording(t *testing.T, client *http.Client, baseURL, id string) *domain.Recording {
	t.Helper()
	recording, code := getRuntimeJSON[domain.Recording](t, client, baseURL, http.MethodGet, "/api/recordings/"+id, nil)
	if code != http.StatusOK {
		t.Fatalf("get recording %s returned %d", id, code)
	}
	return &recording
}

func waitRecordingSequenceCount(t *testing.T, client *http.Client, baseURL, id string, minimum int, timeout time.Duration) {
	t.Helper()
	if err := waitRuntimeRecordingSequenceCount(client, baseURL, id, minimum, timeout); err != nil {
		t.Fatal(err)
	}
}

func waitRuntimeRecordingSequenceCount(client *http.Client, baseURL, id string, minimum int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var latest *domain.Recording
	for time.Now().Before(deadline) {
		var recording domain.Recording
		code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/recordings/"+id, nil, &recording)
		if err == nil && code == http.StatusOK {
			latest = &recording
			if recording.Tracks["main"] != nil && len(recording.Tracks["main"].Segments) >= minimum {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if latest == nil {
		return fmt.Errorf("recording %s could not be read while waiting for %d segments", id, minimum)
	}
	var count int
	var sequences []uint64
	if track := latest.Tracks["main"]; track != nil {
		count = len(track.Segments)
		sequences = recordingSequences(latest)
	}
	return fmt.Errorf("recording %s did not reach %d segments (state=%s count=%d sequences=%v gaps=%+v error=%q)", id, minimum, latest.State, count, sequences, latest.Gaps, latest.LastError)
}

func (f *runtimeUpdateFixture) describe(stream string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	value := f.streams[stream]
	if value == nil {
		return "missing stream"
	}
	return fmt.Sprintf("latest=%d token_generation=%d expired=%t manifests=%+v refreshes=%+v segment_requests=%+v", value.latest, value.tokenGeneration, value.expired, value.manifestRequests, value.refreshes, value.segmentRequests)
}

func waitMetadataTimeline(t *testing.T, ctx context.Context, client *http.Client, baseURL, id string, minimum int, timeout time.Duration) struct {
	Current   *domain.MetadataRevision  `json:"current"`
	Items     []domain.MetadataRevision `json:"items"`
	Truncated bool                      `json:"truncated"`
} {
	t.Helper()
	type response struct {
		Current   *domain.MetadataRevision  `json:"current"`
		Items     []domain.MetadataRevision `json:"items"`
		Truncated bool                      `json:"truncated"`
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var value response
		code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/recordings/"+id+"/metadata", nil, &value)
		if err == nil && code == http.StatusOK && len(value.Items) >= minimum {
			return value
		}
		select {
		case <-ctx.Done():
			t.Fatalf("metadata timeline wait canceled: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("recording %s metadata timeline did not reach %d items", id, minimum)
	return response{}
}

func stringValue(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}

func recordingSequences(recording *domain.Recording) []uint64 {
	if recording == nil || recording.Tracks["main"] == nil {
		return nil
	}
	result := make([]uint64, 0, len(recording.Tracks["main"].Segments))
	for _, segment := range recording.Tracks["main"].Segments {
		result = append(result, segment.Sequence)
	}
	return result
}

func equalSequenceRange(sequences []uint64, first, last uint64) bool {
	if uint64(len(sequences)) != last-first+1 {
		return false
	}
	for index, sequence := range sequences {
		if sequence != first+uint64(index) {
			return false
		}
	}
	return true
}

func verifyRuntimeRecordingSegments(t *testing.T, dataDir string, recording *domain.Recording, stream string, first, last uint64) {
	t.Helper()
	if recording == nil || recording.Tracks["main"] == nil {
		t.Fatal("recording has no main track")
	}
	segments := recording.Tracks["main"].Segments
	if uint64(len(segments)) < last-first+1 {
		t.Fatalf("recording segment count=%d, want at least %d", len(segments), last-first+1)
	}
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	seenOrdinals := make(map[uint64]struct{}, len(segments))
	seenSequence := make(map[uint64]struct{}, len(segments))
	for _, segment := range segments {
		if _, exists := seenOrdinals[segment.ArchiveOrdinal]; exists {
			t.Fatalf("duplicate archive ordinal %d", segment.ArchiveOrdinal)
		}
		seenOrdinals[segment.ArchiveOrdinal] = struct{}{}
		if _, exists := seenSequence[segment.Sequence]; exists {
			t.Fatalf("duplicate source sequence %d", segment.Sequence)
		}
		seenSequence[segment.Sequence] = struct{}{}
		if segment.Sequence < first || segment.Sequence > last {
			continue
		}
		reader, err := store.OpenPayloadReader(recording.ID, segment.StoragePath)
		if err != nil {
			t.Fatalf("open canonical payload for sequence %d: %v", segment.Sequence, err)
		}
		payload, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		digest := sha256.Sum256(payload)
		if readErr != nil || closeErr != nil || string(payload) != runtimeSegmentPayload(stream, segment.Sequence) || int64(len(payload)) != segment.PayloadSize || hex.EncodeToString(digest[:]) != segment.SHA256 {
			t.Fatalf("canonical payload/hash mismatch for source sequence %d: read=%v close=%v", segment.Sequence, readErr, closeErr)
		}
	}
	for sequence := first; sequence <= last; sequence++ {
		if _, exists := seenSequence[sequence]; !exists {
			t.Fatalf("canonical recording is missing source sequence %d", sequence)
		}
	}
}

func waitRecordingLease(t *testing.T, dataDir, recordingID string, timeout time.Duration) generation.Lease {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state := readRuntimeGenerationSnapshot(t, dataDir)
		if lease, ok := state.Leases[recordingID]; ok {
			return lease
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("recording %s did not receive a Host generation lease", recordingID)
	return generation.Lease{}
}

func waitRecordingLeaseGeneration(t *testing.T, dataDir, recordingID, generationID string, timeout time.Duration, hostOutput func() string, fixtureState ...func() string) generation.Lease {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if lease, ok := readRuntimeGenerationSnapshot(t, dataDir).Leases[recordingID]; ok && lease.EngineGeneration == generationID {
			return lease
		}
		time.Sleep(100 * time.Millisecond)
	}
	ownerStore, ownerErr := recordingowner.Open(dataDir)
	owner, ownerReadErr := ownerStore.Current(recordingID)
	registry := readRuntimeGenerationSnapshot(t, dataDir)
	sourceGeneration := registry.Generations[owner.EngineGeneration]
	targetGeneration := registry.Generations[generationID]
	lease := registry.Leases[recordingID]
	diagnostic := "unavailable"
	if len(fixtureState) > 0 && fixtureState[0] != nil {
		diagnostic = fixtureState[0]()
	}
	t.Fatalf("recording %s did not hand over to generation %s; active=%s staged=%s source=%+v target=%+v lease=%+v durable_owner=%+v owner_errors=%v/%v; fixture=%s; host output=%s", recordingID, generationID, registry.ActiveGenerationID, registry.StagedGenerationID, sourceGeneration, targetGeneration, lease, owner, ownerErr, ownerReadErr, diagnostic, hostOutput())
	return generation.Lease{}
}

func waitCanonicalSegmentCount(t *testing.T, dataDir, recordingID string, minimum int, timeout time.Duration) *domain.Recording {
	t.Helper()
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		recording, loadErr := store.LoadRecordingReadOnly(recordingID)
		if loadErr == nil && recording.SegmentCount() >= minimum {
			return recording
		}
		time.Sleep(100 * time.Millisecond)
	}
	recording, err := store.LoadRecordingReadOnly(recordingID)
	if err != nil {
		t.Fatalf("read canonical Recording after waiting for %d segments: %v", minimum, err)
	}
	t.Fatalf("canonical Recording %s stopped at %d segments, want at least %d", recordingID, recording.SegmentCount(), minimum)
	return nil
}

func waitLeaseAbsent(t *testing.T, dataDir, recordingID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, exists := readRuntimeGenerationSnapshot(t, dataDir).Leases[recordingID]; !exists {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("recording %s Host generation lease did not clear", recordingID)
}

func readRuntimeLeases(t *testing.T, dataDir string) map[string]generation.Lease {
	t.Helper()
	return readRuntimeGenerationSnapshot(t, dataDir).Leases
}

func readRuntimeGenerationSnapshot(t *testing.T, dataDir string) generation.Snapshot {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dataDir, "runtime", "state", "generations.json"))
	if err != nil {
		t.Fatalf("read Host runtime diagnostics: %v", err)
	}
	var snapshot generation.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("decode Host runtime diagnostics: %v", err)
	}
	return snapshot
}

func hasGeneration(values []httpapi.GenerationSummary, version string, minLeases int) bool {
	for _, value := range values {
		if value.Version == version && value.ActiveRecordings >= minLeases {
			return true
		}
	}
	return false
}

func waitRuntimeRecordingState(t *testing.T, client *http.Client, baseURL, recordingID string, expected domain.RecordingState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		current := getRecording(t, client, baseURL, recordingID)
		if current.State == expected {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("recording %s did not reach terminal state %q", recordingID, expected)
}

func stopRecording(t *testing.T, client *http.Client, baseURL, recordingID string) {
	t.Helper()
	var value domain.Recording
	code, err := requestRuntimeJSON(client, baseURL, http.MethodPost, "/api/recordings/"+recordingID+"/stop", map[string]any{}, &value)
	if err != nil || (code != http.StatusOK && code != http.StatusConflict) {
		t.Fatalf("stop recording %s returned code=%d err=%v", recordingID, code, err)
	}
}

func setWatchEnabled(t *testing.T, client *http.Client, baseURL, watchID string, enabled bool) {
	t.Helper()
	method := http.MethodPost
	path := "/api/watches/" + watchID + "/disable"
	if enabled {
		path = "/api/watches/" + watchID + "/enable"
	}
	var value map[string]any
	code, err := requestRuntimeJSON(client, baseURL, method, path, map[string]any{}, &value)
	if err != nil || code != http.StatusOK {
		t.Fatalf("set Watch enabled=%v returned code=%d err=%v", enabled, code, err)
	}
}

func containsTimeAfter(values []time.Time, after time.Time) bool {
	for _, value := range values {
		if value.After(after) {
			return true
		}
	}
	return false
}

func waitRuntimeCondition(t *testing.T, timeout time.Duration, condition func() bool, what string) {
	t.Helper()
	if err := waitRuntimeConditionError(timeout, condition, what); err != nil {
		t.Fatal(err)
	}
}

func waitRuntimeConditionError(timeout time.Duration, condition func() bool, what string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", what)
}

func waitHostHandoverLogEvent(process *runtimeHostProcess, event, recordingID string, occurrence int, timeout time.Duration) error {
	if err := waitRuntimeConditionError(timeout, func() bool {
		count := 0
		for _, line := range strings.Split(process.output.String(), "\n") {
			if strings.Contains(line, event) && strings.Contains(line, recordingID) {
				count++
			}
		}
		return count >= occurrence
	}, fmt.Sprintf("Host event %s for Recording %s occurrence %d", event, recordingID, occurrence)); err != nil {
		return err
	}
	return nil
}

func waitActivationOutcome(t *testing.T, result <-chan productionActivationOutcome, timeout time.Duration) productionActivationOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-time.After(timeout):
		t.Fatalf("Runtime Host activation API did not return within %s", timeout)
		return productionActivationOutcome{}
	}
}

func reserveRuntimeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

type runtimeProcessInfo struct {
	PID     int
	Parent  int
	Command string
}

func runtimeProcessTable() ([]runtimeProcessInfo, error) {
	output, err := exec.Command("ps", "-axo", "pid=,ppid=,command=").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("read process table: %w: %s", err, boundedOutput(output))
	}
	lines := strings.Split(string(output), "\n")
	result := make([]runtimeProcessInfo, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pid, pidErr := strconv.Atoi(fields[0])
		parent, parentErr := strconv.Atoi(fields[1])
		if pidErr != nil || parentErr != nil {
			continue
		}
		result = append(result, runtimeProcessInfo{PID: pid, Parent: parent, Command: strings.Join(fields[2:], " ")})
	}
	return result, nil
}

func processForBinary(path string) int {
	processes, err := runtimeProcessTable()
	if err != nil {
		return 0
	}
	clean := filepath.Clean(path)
	for _, process := range processes {
		if commandRunsBinary(process.Command, clean) {
			return process.PID
		}
	}
	return 0
}

func waitProcessForBinary(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pid := processForBinary(path); pid != 0 {
			return pid
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("process for production executable %s did not start", filepath.Base(path))
	return 0
}

func waitProcessAbsent(t *testing.T, path string, timeout time.Duration) error {
	t.Helper()
	return waitRuntimeConditionError(timeout, func() bool { return processForBinary(path) == 0 }, "process exit for "+filepath.Base(path))
}

func stopRuntimeHostProcess(process *runtimeHostProcess) {
	if process == nil || process.command == nil || process.command.Process == nil {
		return
	}
	select {
	case <-process.done:
		return
	default:
	}
	_ = process.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-process.done:
	case <-time.After(25 * time.Second):
		_ = process.command.Process.Kill()
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
		}
	}
}
