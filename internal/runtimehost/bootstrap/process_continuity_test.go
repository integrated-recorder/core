package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehost/supervisor"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

// TestRuntimeGenerationProcessBoundaryContinuity composes the real supervisor,
// runtime IPC server/client, acquire.Manager, recorderengine.Engine, and local
// canonical archive across independent OS processes. The child helper below
// supplies only the process entry points used by ExecLauncher.
func TestRuntimeGenerationProcessBoundaryContinuity(t *testing.T) {
	if testing.Short() {
		t.Skip("requires separate Runtime Host, Control, and Engine processes")
	}
	root, err := os.MkdirTemp("/private/tmp", "ir-gen-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	for _, dir := range []string{"ipc", "tokens", "markers", "data"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	dataDir := filepath.Join(root, "data")
	fixture := newProcessContinuityFixture(t)
	defer fixture.Close()
	aPump := fixture.StartPump("a")
	bPump := fixture.StartPump("b")
	t.Cleanup(aPump)
	t.Cleanup(bPump)

	genA, genB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	tokenA := writeProcessToken(t, root, genA)
	tokenB := writeProcessToken(t, root, genB)
	controlAddrA := reserveProcessAddress(t)
	controlAddrB := reserveProcessAddress(t)
	publicAddr := reserveProcessAddress(t)
	adminAddr := reserveProcessAddress(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	hostMarker := filepath.Join(root, "markers", "host.json")
	command := exec.Command(executable, "-test.run=^TestRuntimeGenerationProcessHelper$")
	command.Env = append(filteredEnvironment(os.Environ(), "IR_PROCESS_HELPER"),
		"IR_PROCESS_HELPER=host",
		"IR_PROCESS_ROOT="+root,
		"IR_PROCESS_DATA="+dataDir,
		"IR_PROCESS_EXECUTABLE="+executable,
		"IR_PROCESS_PUBLIC_ADDR="+publicAddr,
		"IR_PROCESS_ADMIN_ADDR="+adminAddr,
		"IR_PROCESS_CONTROL_A_ADDR="+controlAddrA,
		"IR_PROCESS_CONTROL_B_ADDR="+controlAddrB,
		"IR_PROCESS_HOST_MARKER="+hostMarker,
	)
	var hostOutput testOutputBuffer
	command.Stdout, command.Stderr = &hostOutput, &hostOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	hostDone := make(chan struct{})
	var hostErr error
	go func() {
		hostErr = command.Wait()
		close(hostDone)
	}()
	t.Cleanup(func() {
		select {
		case <-hostDone:
			return
		default:
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-hostDone:
		case <-time.After(8 * time.Second):
			_ = command.Process.Kill()
			select {
			case <-hostDone:
			case <-time.After(2 * time.Second):
			}
		}
	})

	deadline := time.Now().Add(12 * time.Second)
	for {
		if _, err := os.Stat(hostMarker); err == nil {
			break
		}
		select {
		case <-hostDone:
			t.Fatalf("Runtime Host process exited before readiness (%v): %s", hostErr, hostOutput.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("Runtime Host process did not become ready: %s", hostOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	hostPID := command.Process.Pid
	controlPIDa := readProcessPID(t, filepath.Join(root, "markers", "control-a.json"), 8*time.Second)
	enginePIDa := readProcessPID(t, filepath.Join(root, "markers", "engine-a.json"), 8*time.Second)
	assertDistinctProcessIDs(t, hostPID, controlPIDa, enginePIDa)
	publicURL := "http://" + publicAddr
	adminURL := "http://" + adminAddr
	if got := getProcessRoute(t, publicURL, 4*time.Second); got != "control-a" {
		t.Fatalf("stable Runtime Host initially routed to %q, want control-a", got)
	}

	managerA := newProcessEngineClient(t, root, genA, tokenA)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	startedR, err := managerA.StartResolvedWithID(ctx, strings.Repeat("1", 32), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: fixture.ManifestURL("a")}, nil, "recording R", nil)
	cancel()
	if err != nil {
		t.Fatalf("Engine A start_resolved failed: %v", err)
	}
	if startedR.State != domain.StateRecording || startedR.ID != strings.Repeat("1", 32) {
		t.Fatalf("Engine A returned unexpected recording identity/state: id=%q state=%q", startedR.ID, startedR.State)
	}
	waitProcessRecording(t, managerA, startedR.ID, func(r *domain.Recording) bool { return len(r.Tracks["main"].Segments) >= 3 }, 12*time.Second)
	baselineR := latestRecordingSequence(t, managerA, startedR.ID)
	if baselineR == 0 {
		t.Fatal("recording R has no committed source sequence before update")
	}

	if err := postProcessCommand(adminURL+"/stage-b", 15*time.Second); err != nil {
		t.Fatalf("stage generation B: %v (%s)", err, hostOutput.String())
	}
	controlPIDb := readProcessPID(t, filepath.Join(root, "markers", "control-b.json"), 8*time.Second)
	enginePIDb := readProcessPID(t, filepath.Join(root, "markers", "engine-b.json"), 8*time.Second)
	assertDistinctProcessIDs(t, hostPID, controlPIDa, enginePIDa, controlPIDb, enginePIDb)
	controlAProcess, err := os.FindProcess(controlPIDa)
	if err != nil {
		t.Fatalf("locate Control A process: %v", err)
	}
	if err := controlAProcess.Kill(); err != nil {
		t.Fatalf("SIGKILL Control A: %v", err)
	}
	if err := waitControlFailed(t, adminURL, genA, 6*time.Second); err != nil {
		t.Fatalf("Runtime Host did not observe Control A SIGKILL: %v", err)
	}
	activatedAt := time.Now()
	if err := postProcessCommand(adminURL+"/activate-b", 15*time.Second); err != nil {
		t.Fatalf("activate generation B after Control A SIGKILL: %v (%s)", err, hostOutput.String())
	}
	if got := getProcessRoute(t, publicURL, 4*time.Second); got != "control-b" {
		t.Fatalf("stable Runtime Host route after activation = %q, want control-b", got)
	}
	state := getProcessSnapshot(t, adminURL)
	if state.ActiveControlGeneration != genB || processGeneration(state, genA).Engine.State != supervisor.ProcessReady {
		t.Fatalf("old Engine was not retained after Control activation: active=%s old=%+v", state.ActiveControlGeneration, processGeneration(state, genA))
	}
	engineAAfterControlDeath, err := managerA.GetContext(context.Background(), startedR.ID)
	if err != nil || engineAAfterControlDeath.ID != startedR.ID || engineAAfterControlDeath.State != domain.StateRecording {
		t.Fatalf("recording R did not remain active on Engine A after Control A died: state=%v err=%v", recordingState(engineAAfterControlDeath), err)
	}
	activeA, err := managerA.Inventory(context.Background())
	if err != nil || len(activeA.Active) != 1 || activeA.Active[0].RecordingID != startedR.ID {
		t.Fatalf("Engine A inventory after Control A death = %#v, err=%v", activeA, err)
	}

	managerB := newProcessEngineClient(t, root, genB, tokenB)
	ctx, cancel = context.WithTimeout(context.Background(), 8*time.Second)
	startedS, err := managerB.StartResolvedWithID(ctx, strings.Repeat("2", 32), "fixture", adapterproto.MediaSource{Type: "hls", ManifestURL: fixture.ManifestURL("b")}, nil, "recording S", nil)
	cancel()
	if err != nil || startedS.ID != strings.Repeat("2", 32) || startedS.State != domain.StateRecording {
		t.Fatalf("new recording S was not admitted by Engine B: recording=%#v err=%v", startedS, err)
	}
	waitProcessRecording(t, managerB, startedS.ID, func(r *domain.Recording) bool { return len(r.Tracks["main"].Segments) >= 2 }, 12*time.Second)

	postActivationLatest := fixture.Latest("a")
	waitProcessRecording(t, managerA, startedR.ID, func(r *domain.Recording) bool { return latestTrackSequence(r) >= postActivationLatest+3 }, 12*time.Second)
	if fixture.countRequestsAfter("a", postActivationLatest+1, activatedAt) < 3 {
		t.Fatalf("Engine A did not fetch new source sequence IDs after Control replacement; baseline=%d", postActivationLatest)
	}
	if got := getProcessRoute(t, publicURL, 4*time.Second); got != "control-b" {
		t.Fatalf("stable routing changed away from generation B: %q", got)
	}

	finalA := fixture.Freeze("a")
	waitProcessRecording(t, managerA, startedR.ID, func(r *domain.Recording) bool { return len(r.Tracks["main"].Segments) == int(finalA-100+1) }, 15*time.Second)
	ctx, cancel = context.WithTimeout(context.Background(), 12*time.Second)
	stoppedR, err := managerA.StopContext(ctx, startedR.ID)
	cancel()
	if err != nil {
		t.Fatalf("stop Engine A recording R: %v", err)
	}
	verifyProcessArchive(t, dataDir, stoppedR, "a", 100, finalA)
	for sequence := uint64(100); sequence <= finalA; sequence++ {
		if count := fixture.RequestCount("a", sequence); count != 1 {
			t.Fatalf("source segment %d was fetched %d times across the Control update; want one", sequence, count)
		}
	}
	if stoppedR.State != domain.StateStopped || len(stoppedR.Gaps) != 0 {
		t.Fatalf("recording R terminal state/gaps after generation handoff: state=%q gaps=%#v", stoppedR.State, stoppedR.Gaps)
	}
	if processGeneration(getProcessSnapshot(t, adminURL), genA).Engine.State != supervisor.ProcessReady {
		t.Fatal("Engine A was retired before the recording lease was released")
	}
	if err := postProcessCommand(adminURL+"/retire-a", 12*time.Second); err != nil {
		t.Fatalf("retire Engine A after recording R terminated: %v", err)
	}
	if err := waitProcessRetired(t, adminURL, genA, 5*time.Second); err != nil {
		t.Fatalf("Engine A did not retire after its last recording stopped: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "markers", "engine-a.json.stopped")); err != nil {
		t.Fatalf("Engine A did not perform clean drain after recording R: %v", err)
	}

	finalB := fixture.Freeze("b")
	waitProcessRecording(t, managerB, startedS.ID, func(r *domain.Recording) bool { return len(r.Tracks["main"].Segments) == int(finalB-1000+1) }, 15*time.Second)
	ctx, cancel = context.WithTimeout(context.Background(), 12*time.Second)
	stoppedS, err := managerB.StopContext(ctx, startedS.ID)
	cancel()
	if err != nil {
		t.Fatalf("stop Engine B recording S: %v", err)
	}
	verifyProcessArchive(t, dataDir, stoppedS, "b", 1000, finalB)
	for sequence := uint64(1000); sequence <= finalB; sequence++ {
		if count := fixture.RequestCount("b", sequence); count != 1 {
			t.Fatalf("new-generation source segment %d was fetched %d times; want one", sequence, count)
		}
	}
	if stoppedS.State != domain.StateStopped || len(stoppedS.Gaps) != 0 {
		t.Fatalf("recording S terminal state/gaps: state=%q gaps=%#v", stoppedS.State, stoppedS.Gaps)
	}
}

// TestRuntimeGenerationProcessHelper is invoked only by the process-boundary
// test above. It is intentionally not a production entry point or URL policy.
func TestRuntimeGenerationProcessHelper(t *testing.T) {
	switch os.Getenv("IR_PROCESS_HELPER") {
	case "host":
		runProcessHostHelper(t)
	case "control":
		runProcessControlHelper(t)
	case "engine":
		runProcessEngineHelper(t)
	}
}

func runProcessHostHelper(t *testing.T) {
	root, dataDir := os.Getenv("IR_PROCESS_ROOT"), os.Getenv("IR_PROCESS_DATA")
	executable := os.Getenv("IR_PROCESS_EXECUTABLE")
	publicAddr, adminAddr := os.Getenv("IR_PROCESS_PUBLIC_ADDR"), os.Getenv("IR_PROCESS_ADMIN_ADDR")
	if root == "" || dataDir == "" || executable == "" || publicAddr == "" || adminAddr == "" {
		t.Fatal("test Runtime Host configuration is incomplete")
	}
	genA, genB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	controlA := "http://" + os.Getenv("IR_PROCESS_CONTROL_A_ADDR")
	controlB := "http://" + os.Getenv("IR_PROCESS_CONTROL_B_ADDR")
	targetA, err := url.Parse(controlA)
	if err != nil {
		t.Fatal(err)
	}
	targetB, err := url.Parse(controlB)
	if err != nil {
		t.Fatal(err)
	}
	launcher := supervisor.ExecLauncher{}
	readiness := supervisor.ReadinessFunc(func(ctx context.Context, spec supervisor.ProcessSpec, _ supervisor.Child) error {
		deadline := time.NewTicker(10 * time.Millisecond)
		defer deadline.Stop()
		for {
			if spec.Role == supervisor.RoleEngine {
				token, tokenErr := os.ReadFile(processTokenPath(root, spec.GenerationID))
				if tokenErr == nil {
					client, clientErr := runtimeipc.NewClient(processEngineSocket(root, spec.GenerationID), spec.GenerationID, token, 2*time.Second)
					if clientErr == nil {
						var ready recorderengine.ReadyResult
						if callErr := client.Call(ctx, recorderengine.OperationReady, nil, &ready); callErr == nil && ready.Ready && ready.GenerationID == spec.GenerationID {
							return nil
						}
					}
				}
			} else {
				client := &http.Client{Timeout: 300 * time.Millisecond}
				response, requestErr := client.Get(spec.Endpoint)
				if requestErr == nil {
					_ = response.Body.Close()
					if response.StatusCode == http.StatusOK {
						return nil
					}
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
			}
		}
	})
	sup, err := supervisor.New(supervisor.Options{Launcher: launcher, Readiness: readiness, ControlLifecycle: &smokeLifecycle{}, ShutdownTimeout: 8 * time.Second, CleanupTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		_ = sup.Close(ctx)
	}()
	makeEngine := func(id string) supervisor.ProcessSpec {
		return supervisor.ProcessSpec{GenerationID: id, Role: supervisor.RoleEngine, Executable: executable, Args: []string{"-test.run=^TestRuntimeGenerationProcessHelper$"}, Env: []string{
			"PATH=/usr/bin:/bin", "HOME=/private/tmp", "IR_PROCESS_HELPER=engine", "IR_PROCESS_GENERATION=" + id,
			"IR_PROCESS_ROOT=" + root, "IR_PROCESS_DATA=" + dataDir,
			"IR_PROCESS_MARKER=" + filepath.Join(root, "markers", "engine-"+id[:1]+".json"),
		}}
	}
	makeControl := func(id, addr, label string) supervisor.ProcessSpec {
		return supervisor.ProcessSpec{GenerationID: id, Role: supervisor.RoleControl, Executable: executable, Endpoint: "http://" + addr + "/ready", Args: []string{"-test.run=^TestRuntimeGenerationProcessHelper$"}, Env: []string{
			"PATH=/usr/bin:/bin", "HOME=/private/tmp", "IR_PROCESS_HELPER=control", "IR_PROCESS_LABEL=" + label,
			"IR_PROCESS_ADDR=" + addr, "IR_PROCESS_MARKER=" + filepath.Join(root, "markers", "control-"+label+".json"),
		}}
	}
	startCtx, startCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer startCancel()
	if err := sup.StageGeneration(startCtx, supervisor.GenerationSpec{ID: genA, Engine: makeEngine(genA), Control: makeControl(genA, os.Getenv("IR_PROCESS_CONTROL_A_ADDR"), "a"), ControlTarget: targetA}); err != nil {
		t.Fatalf("stage generation A: %v", err)
	}
	if err := sup.ActivateControl(startCtx, genA); err != nil {
		t.Fatalf("activate generation A: %v", err)
	}
	publicListener, err := net.Listen("tcp4", publicAddr)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- sup.Serve(serveCtx, publicListener) }()

	var stateMu sync.Mutex
	tokenA, err := os.ReadFile(processTokenPath(root, genA))
	if err != nil {
		t.Fatal("Engine A token was not provisioned")
	}
	admin := http.NewServeMux()
	admin.HandleFunc("GET /state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sup.Snapshot())
	})
	admin.HandleFunc("POST /stage-b", func(w http.ResponseWriter, r *http.Request) {
		stateMu.Lock()
		defer stateMu.Unlock()
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if err := sup.StageGeneration(ctx, supervisor.GenerationSpec{ID: genB, Engine: makeEngine(genB), Control: makeControl(genB, os.Getenv("IR_PROCESS_CONTROL_B_ADDR"), "b"), ControlTarget: targetB}); err != nil {
			http.Error(w, "candidate stage failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	admin.HandleFunc("POST /activate-b", func(w http.ResponseWriter, r *http.Request) {
		stateMu.Lock()
		defer stateMu.Unlock()
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if err := sup.ActivateControl(ctx, genB); err != nil {
			http.Error(w, "candidate activation failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	admin.HandleFunc("POST /retire-a", func(w http.ResponseWriter, r *http.Request) {
		stateMu.Lock()
		defer stateMu.Unlock()
		client, err := runtimeipc.NewClient(processEngineSocket(root, genA), genA, tokenA, 3*time.Second)
		if err != nil {
			http.Error(w, "old engine client unavailable", http.StatusInternalServerError)
			return
		}
		manager, err := recorderengine.NewManagerClient(client)
		if err == nil {
			ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
			defer cancel()
			err = sup.RetireEngine(ctx, genA, manager)
		}
		if err != nil {
			http.Error(w, "old engine retirement failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	adminListener, err := net.Listen("tcp4", adminAddr)
	if err != nil {
		cancelServe()
		t.Fatal(err)
	}
	adminServer := &http.Server{Handler: admin, ReadHeaderTimeout: time.Second}
	adminDone := make(chan error, 1)
	go func() { adminDone <- adminServer.Serve(adminListener) }()
	if err := writePrivateProcessMarker(os.Getenv("IR_PROCESS_HOST_MARKER"), fmt.Sprintf(`{"pid":%d}`, os.Getpid())); err != nil {
		t.Fatal(err)
	}

	signalCtx, stopSignal := processSignalContext()
	<-signalCtx.Done()
	stopSignal()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 4*time.Second)
	_ = adminServer.Shutdown(shutdownCtx)
	shutdownCancel()
	cancelServe()
	select {
	case <-serveDone:
	case <-time.After(4 * time.Second):
		_ = sup.Close(context.Background())
	}
	select {
	case <-adminDone:
	case <-time.After(time.Second):
	}
}

func runProcessControlHelper(t *testing.T) {
	addr, label, marker := os.Getenv("IR_PROCESS_ADDR"), os.Getenv("IR_PROCESS_LABEL"), os.Getenv("IR_PROCESS_MARKER")
	if addr == "" || label == "" || marker == "" {
		t.Fatal("test Control configuration is incomplete")
	}
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "control-"+label)
	}), ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	if err := writePrivateProcessMarker(marker, fmt.Sprintf(`{"pid":%d,"label":"%s"}`, os.Getpid(), label)); err != nil {
		t.Fatal(err)
	}
	signalCtx, stopSignal := processSignalContext()
	<-signalCtx.Done()
	stopSignal()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	select {
	case <-done:
	case <-ctx.Done():
	}
	_ = writePrivateProcessMarker(marker+".stopped", "stopped")
}

func runProcessEngineHelper(t *testing.T) {
	root, dataDir := os.Getenv("IR_PROCESS_ROOT"), os.Getenv("IR_PROCESS_DATA")
	generationID, marker := os.Getenv("IR_PROCESS_GENERATION"), os.Getenv("IR_PROCESS_MARKER")
	if root == "" || dataDir == "" || generationID == "" || marker == "" {
		t.Fatal("test Engine configuration is incomplete")
	}
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithMode(store, &http.Client{Timeout: 3 * time.Second}, nil, allowLoopbackFixtureURL, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	adapters := &adapterhost.Host{}
	engine, err := recorderengine.New(manager, adapters, generationID, "engine-instance-"+generationID[:1])
	if err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(processTokenPath(root, generationID))
	if err != nil {
		_ = engine.Close(context.Background())
		t.Fatal(err)
	}
	server, err := runtimeipc.NewServer(processEngineSocket(root, generationID), generationID, token, engine)
	if err != nil {
		_ = engine.Close(context.Background())
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx) }()
	if err := writePrivateProcessMarker(marker, fmt.Sprintf(`{"pid":%d,"generation":"%s"}`, os.Getpid(), generationID)); err != nil {
		cancelServe()
		_ = engine.Close(context.Background())
		t.Fatal(err)
	}
	signalCtx, stopSignal := processSignalContext()
	<-signalCtx.Done()
	stopSignal()
	cancelServe()
	select {
	case <-serveDone:
	case <-time.After(2 * time.Second):
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := engine.Close(ctx); err != nil {
		t.Fatal("Engine close did not drain within the test deadline")
	}
	_ = writePrivateProcessMarker(marker+".stopped", "stopped")
}

// This validation exception is compiled only into this test binary and only
// permits loopback HTTP fixture URLs. Production URL policy is untouched.
func allowLoopbackFixtureURL(_ context.Context, raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Hostname() == "" {
		return errors.New("fixture URL is invalid")
	}
	ip := net.ParseIP(parsed.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return errors.New("only loopback fixture URLs are allowed")
	}
	return nil
}

func newProcessEngineClient(t *testing.T, root, generationID string, token []byte) *recorderengine.ManagerClient {
	t.Helper()
	client, err := runtimeipc.NewClient(processEngineSocket(root, generationID), generationID, token, 4*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := recorderengine.NewManagerClient(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	inventory, err := manager.Inventory(ctx)
	if err != nil || inventory.GenerationID != generationID {
		t.Fatalf("Engine %s IPC identity check failed: inventory=%#v err=%v", generationID[:1], inventory, err)
	}
	return manager
}

func waitProcessRecording(t *testing.T, manager *recorderengine.ManagerClient, id string, condition func(*domain.Recording) bool, timeout time.Duration) *domain.Recording {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		current, err := manager.GetContext(ctx, id)
		cancel()
		if err == nil && current != nil && condition(current) {
			return current
		}
		time.Sleep(25 * time.Millisecond)
	}
	current, err := manager.GetContext(context.Background(), id)
	t.Fatalf("recording %s did not reach expected state before deadline: latest=%v err=%v", id[:1], recordingSummary(current), err)
	return nil
}

func latestRecordingSequence(t *testing.T, manager *recorderengine.ManagerClient, id string) uint64 {
	t.Helper()
	return latestTrackSequence(waitProcessRecording(t, manager, id, func(*domain.Recording) bool { return true }, time.Second))
}

func latestTrackSequence(recording *domain.Recording) uint64 {
	if recording == nil || recording.Tracks["main"] == nil || len(recording.Tracks["main"].Segments) == 0 {
		return 0
	}
	segments := recording.Tracks["main"].Segments
	return segments[len(segments)-1].Sequence
}

func verifyProcessArchive(t *testing.T, dataDir string, recording *domain.Recording, stream string, first, last uint64) {
	t.Helper()
	if recording == nil || recording.Tracks["main"] == nil {
		t.Fatal("recording archive has no main track")
	}
	segments := recording.Tracks["main"].Segments
	if uint64(len(segments)) != last-first+1 {
		t.Fatalf("stream %s canonical segment count=%d want=%d", stream, len(segments), last-first+1)
	}
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	seenOrdinals := make(map[uint64]struct{}, len(segments))
	for index, segment := range segments {
		wantSequence := first + uint64(index)
		if segment.Sequence != wantSequence || segment.ArchiveOrdinal != uint64(index+1) {
			t.Fatalf("stream %s segment[%d] identity/order sequence=%d ordinal=%d want sequence=%d ordinal=%d", stream, index, segment.Sequence, segment.ArchiveOrdinal, wantSequence, index+1)
		}
		if _, exists := seenOrdinals[segment.ArchiveOrdinal]; exists {
			t.Fatalf("stream %s duplicate archive ordinal %d", stream, segment.ArchiveOrdinal)
		}
		seenOrdinals[segment.ArchiveOrdinal] = struct{}{}
		payload, err := store.OpenPayloadReader(recording.ID, segment.StoragePath)
		if err != nil {
			t.Fatalf("stream %s open canonical payload for sequence %d: %v", stream, segment.Sequence, err)
		}
		body, readErr := io.ReadAll(payload)
		_ = payload.Close()
		if readErr != nil {
			t.Fatalf("stream %s read canonical payload for sequence %d: %v", stream, segment.Sequence, readErr)
		}
		wantBody := []byte(fmt.Sprintf("stream=%s;sequence=%d;", stream, wantSequence))
		digest := sha256.Sum256(body)
		if string(body) != string(wantBody) || segment.PayloadSize != int64(len(body)) || segment.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("stream %s canonical payload identity/hash mismatch at sequence %d", stream, segment.Sequence)
		}
	}
}

func reserveProcessAddress(t *testing.T) string {
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

func writeProcessToken(t *testing.T, root, generationID string) []byte {
	t.Helper()
	token := []byte("0123456789abcdef0123456789abcdef")
	path := processTokenPath(root, generationID)
	if err := os.WriteFile(path, token, 0600); err != nil {
		t.Fatal(err)
	}
	return token
}

func processTokenPath(root, generationID string) string {
	return filepath.Join(root, "tokens", generationID+".token")
}

func processEngineSocket(root, generationID string) string {
	return filepath.Join(root, "ipc", generationID+".sock")
}

func writePrivateProcessMarker(path, value string) error {
	if path == "" {
		return errors.New("process marker path is empty")
	}
	return os.WriteFile(path, []byte(value), 0600)
}

func readProcessPID(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var marker struct {
				PID int `json:"pid"`
			}
			if json.Unmarshal(data, &marker) == nil && marker.PID > 0 {
				return marker.PID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process PID marker did not appear: %s", filepath.Base(path))
	return 0
}

func assertDistinctProcessIDs(t *testing.T, pids ...int) {
	t.Helper()
	seen := make(map[int]struct{}, len(pids))
	for _, pid := range pids {
		if pid <= 0 || pid == os.Getpid() {
			t.Fatalf("expected independent child process PID, got %d (test PID %d)", pid, os.Getpid())
		}
		if _, exists := seen[pid]; exists {
			t.Fatalf("runtime roles shared an OS process PID %d", pid)
		}
		seen[pid] = struct{}{}
	}
}

func postProcessCommand(address string, timeout time.Duration) error {
	client := &http.Client{Timeout: timeout}
	request, err := http.NewRequest(http.MethodPost, address, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Runtime Host command returned HTTP %d", response.StatusCode)
	}
	return nil
}

func getProcessRoute(t *testing.T, address string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		client := &http.Client{Timeout: 500 * time.Millisecond}
		response, err := client.Get(address)
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 64))
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				return string(body)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("stable Runtime Host route did not become available: %s", address)
	return ""
}

func getProcessSnapshot(t *testing.T, adminURL string) supervisor.Snapshot {
	t.Helper()
	response, err := (&http.Client{Timeout: 2 * time.Second}).Get(adminURL + "/state")
	if err != nil {
		t.Fatal("Runtime Host state endpoint unavailable")
	}
	defer response.Body.Close()
	var snapshot supervisor.Snapshot
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&snapshot) != nil {
		t.Fatal("Runtime Host process snapshot was malformed")
	}
	return snapshot
}

func processGeneration(snapshot supervisor.Snapshot, id string) supervisor.GenerationSnapshot {
	for _, generation := range snapshot.Generations {
		if generation.ID == id {
			return generation
		}
	}
	return supervisor.GenerationSnapshot{}
}

func waitControlFailed(t *testing.T, adminURL, id string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state := processGeneration(getProcessSnapshot(t, adminURL), id).Control.State
		if state == supervisor.ProcessFailed || state == supervisor.ProcessExited {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return errors.New("old Control process exit was not observed")
}

func waitProcessRetired(t *testing.T, adminURL, id string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state := getProcessSnapshot(t, adminURL)
		if generation := processGeneration(state, id); generation.ID == "" {
			return nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return errors.New("old Engine generation remains registered")
}

func recordingState(recording *domain.Recording) domain.RecordingState {
	if recording == nil {
		return "missing"
	}
	return recording.State
}

func recordingSummary(recording *domain.Recording) string {
	if recording == nil {
		return "missing"
	}
	return fmt.Sprintf("state=%s segments=%d gaps=%d", recording.State, recording.SegmentCount(), len(recording.Gaps))
}

func filteredEnvironment(environment []string, remove ...string) []string {
	blocked := make(map[string]struct{}, len(remove))
	for _, key := range remove {
		blocked[key] = struct{}{}
	}
	result := make([]string, 0, len(environment)+8)
	for _, item := range environment {
		key, _, _ := strings.Cut(item, "=")
		if _, exists := blocked[key]; !exists {
			result = append(result, item)
		}
	}
	return result
}

func processSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

type testOutputBuffer struct {
	mu    sync.Mutex
	value strings.Builder
}

func (b *testOutputBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.value.Write(data)
}

func (b *testOutputBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.value.String()
}

type processContinuityFixture struct {
	server    *http.Server
	baseURL   string
	mu        sync.Mutex
	latest    map[string]uint64
	frozen    map[string]bool
	requests  map[string]map[uint64][]time.Time
	pumps     map[string]func()
	closed    chan struct{}
	closeOnce sync.Once
}

func newProcessContinuityFixture(t *testing.T) *processContinuityFixture {
	t.Helper()
	fixture := &processContinuityFixture{latest: map[string]uint64{"a": 100, "b": 1000}, frozen: map[string]bool{}, requests: map[string]map[uint64][]time.Time{"a": {}, "b": {}}, pumps: map[string]func(){}, closed: make(chan struct{})}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture.baseURL = "http://" + listener.Addr().String()
	mux := http.NewServeMux()
	mux.HandleFunc("/", fixture.serveHTTP)
	fixture.server = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = fixture.server.Serve(listener) }()
	return fixture
}

func (f *processContinuityFixture) ManifestURL(stream string) string {
	return f.baseURL + "/" + stream + "/live.m3u8"
}

func (f *processContinuityFixture) StartPump(stream string) func() {
	f.mu.Lock()
	if f.pumps[stream] != nil {
		stop := f.pumps[stream]
		f.mu.Unlock()
		return stop
	}
	stop := make(chan struct{})
	var once sync.Once
	stopPump := func() { once.Do(func() { close(stop) }) }
	f.pumps[stream] = stopPump
	f.mu.Unlock()
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-f.closed:
				return
			case <-ticker.C:
				f.mu.Lock()
				if !f.frozen[stream] {
					f.latest[stream]++
				}
				f.mu.Unlock()
			}
		}
	}()
	return stopPump
}

func (f *processContinuityFixture) Freeze(stream string) uint64 {
	f.mu.Lock()
	f.frozen[stream] = true
	latest := f.latest[stream]
	stop := f.pumps[stream]
	f.mu.Unlock()
	if stop != nil {
		stop()
		f.mu.Lock()
		f.pumps[stream] = nil
		f.mu.Unlock()
	}
	return latest
}

func (f *processContinuityFixture) Latest(stream string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latest[stream]
}

func (f *processContinuityFixture) countRequestsAfter(stream string, first uint64, after time.Time) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for seq, times := range f.requests[stream] {
		if seq < first {
			continue
		}
		for _, at := range times {
			if at.After(after) {
				count++
				break
			}
		}
	}
	return count
}

func (f *processContinuityFixture) RequestCount(stream string, sequence uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests[stream][sequence])
}

func (f *processContinuityFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) == 2 && parts[1] == "live.m3u8" && (parts[0] == "a" || parts[0] == "b") {
		stream := parts[0]
		f.mu.Lock()
		latest := f.latest[stream]
		f.mu.Unlock()
		first := uint64(100)
		if stream == "b" {
			first = 1000
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:%d\n", first)
		for sequence := first; sequence <= latest; sequence++ {
			_, _ = fmt.Fprintf(w, "#EXTINF:1.0,sequence-%d\nsegment/%d.ts\n", sequence, sequence)
		}
		return
	}
	if len(parts) == 3 && parts[1] == "segment" && (parts[0] == "a" || parts[0] == "b") {
		sequence, err := strconv.ParseUint(strings.TrimSuffix(parts[2], ".ts"), 10, 64)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		stream := parts[0]
		f.mu.Lock()
		f.requests[stream][sequence] = append(f.requests[stream][sequence], time.Now())
		f.mu.Unlock()
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = fmt.Fprintf(w, "stream=%s;sequence=%d;", stream, sequence)
		return
	}
	http.NotFound(w, r)
}

func (f *processContinuityFixture) Close() {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		pumps := []func(){}
		for _, stop := range f.pumps {
			if stop != nil {
				pumps = append(pumps, stop)
			}
		}
		f.mu.Unlock()
		for _, stop := range pumps {
			stop()
		}
		close(f.closed)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = f.server.Shutdown(ctx)
	})
}
