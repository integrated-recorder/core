package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCandidateReadinessFailureLeavesActiveRouteUnchanged(t *testing.T) {
	oldBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "old-control") }))
	defer oldBackend.Close()
	newBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "new-control") }))
	defer newBackend.Close()

	launcher := &fakeLauncher{}
	ready := ReadinessFunc(func(_ context.Context, spec ProcessSpec, _ Child) error {
		if spec.GenerationID == generationB && spec.Role == RoleControl {
			return errors.New("candidate not ready")
		}
		return nil
	})
	s := newTestSupervisor(t, launcher, ready, &fakeLifecycle{})
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, oldBackend.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(serveCtx, listener) }()
	baseURL := "http://" + listener.Addr().String()
	waitHTTPReady(t, baseURL)

	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, newBackend.URL)); err == nil {
		t.Fatal("candidate with failed readiness unexpectedly staged")
	}
	response := getBody(t, baseURL)
	if response != "old-control" {
		t.Fatalf("active route changed after candidate failure: got %q", response)
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationA {
		t.Fatalf("active generation = %q, want %q", got, generationA)
	}

	cancelServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not stop within its bound")
	}
}

func TestActivationDrainsControlAndRetainsOldEngineUntilProvenEmpty(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	launcher := &fakeLauncher{}
	lifecycle := &fakeLifecycle{}
	s := newTestSupervisor(t, launcher, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationB); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	steps := append([]string(nil), lifecycle.steps...)
	lifecycle.mu.Unlock()
	wantSteps := []string{"prepare::" + generationA, "prepare-activation:" + generationA, "activate:" + generationA, "prepare:" + generationA + ":" + generationB, "prepare-activation:" + generationB, "activate:" + generationB}
	if fmt.Sprint(steps) != fmt.Sprint(wantSteps) {
		t.Fatalf("Control handoff order = %v, want %v", steps, wantSteps)
	}

	if _, ok := launcherChildFromSupervisor(t, s, generationA, RoleControl); ok {
		t.Fatal("old Control is still registered after completed drain")
	}
	oldControlChild, ok := launcher.child(generationA, RoleControl)
	if !ok {
		t.Fatal("old Control test process was not created")
	}
	select {
	case <-oldControlChild.Done():
	default:
		t.Fatal("old Control child was not stopped")
	}
	oldEngine, ok := launcher.child(generationA, RoleEngine)
	if !ok {
		t.Fatal("old Engine was removed with its Control")
	}
	select {
	case <-oldEngine.Done():
		t.Fatal("old Engine stopped during Control replacement")
	default:
	}

	drain := &fakeDrain{active: 1}
	if err := s.RetireEngine(context.Background(), generationA, drain); !errors.Is(err, ErrEngineNotDrained) {
		t.Fatalf("RetireEngine with active recording = %v, want ErrEngineNotDrained", err)
	}
	select {
	case <-oldEngine.Done():
		t.Fatal("Engine stopped before zero-lease proof")
	default:
	}
	drain.active = 0
	if err := s.RetireEngine(context.Background(), generationA, drain); err != nil {
		t.Fatalf("RetireEngine after drain proof: %v", err)
	}
	select {
	case <-oldEngine.Done():
	case <-time.After(time.Second):
		t.Fatal("old Engine was not stopped after zero-lease proof")
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationB {
		t.Fatalf("active generation = %q, want %q", got, generationB)
	}
	if body := func() string {
		recorder := httptest.NewRecorder()
		s.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		return recorder.Body.String()
	}(); !strings.Contains(body, "B") {
		t.Fatalf("new route body = %q, want B", body)
	}
	closeTestSupervisor(t, s)
}

func TestActivationRecoversWhenActiveControlChildIsConfirmedDead(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	launcher := &fakeLauncher{}
	lifecycle := &fakeLifecycle{}
	s := newTestSupervisor(t, launcher, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}

	deadControl, ok := launcher.child(generationA, RoleControl)
	if !ok {
		t.Fatal("active Control child was not created")
	}
	oldEngine, ok := launcher.child(generationA, RoleEngine)
	if !ok {
		t.Fatal("active Engine child was not created")
	}
	lifecycle.mu.Lock()
	lifecycle.steps = nil
	lifecycle.mu.Unlock()

	if err := deadControl.Kill(); err != nil {
		t.Fatalf("kill active Control: %v", err)
	}
	waitControlState(t, s, generationA, ProcessExited)

	if err := s.ActivateControl(context.Background(), generationB); err != nil {
		t.Fatalf("activate candidate after active Control exit: %v", err)
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationB {
		t.Fatalf("active Control = %q, want %q", got, generationB)
	}
	lifecycle.mu.Lock()
	steps := append([]string(nil), lifecycle.steps...)
	lifecycle.mu.Unlock()
	if got, want := fmt.Sprint(steps), fmt.Sprint([]string{"prepare-activation:" + generationB, "activate:" + generationB}); got != want {
		t.Fatalf("recovery lifecycle calls = %v, want %v", steps, want)
	}

	if _, ok := launcherChildFromSupervisor(t, s, generationA, RoleControl); ok {
		t.Fatal("exited old Control remained registered after route recovery")
	}
	snapshot := s.Snapshot()
	oldEngineReady := false
	for _, generation := range snapshot.Generations {
		if generation.ID == generationA && generation.Engine.State == ProcessReady {
			oldEngineReady = true
		}
	}
	if !oldEngineReady {
		t.Fatalf("generation snapshot after recovery = %+v; old Engine should remain ready", snapshot)
	}
	select {
	case <-oldEngine.Done():
		t.Fatal("old Engine stopped while recovering the Control plane")
	default:
	}
	closeTestSupervisor(t, s)
}

func TestActivationDoesNotSkipHandoffForAliveStoppingControl(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	launcher := &fakeLauncher{}
	lifecycle := &fakeLifecycle{}
	s := newTestSupervisor(t, launcher, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	lifecycle.steps = nil
	lifecycle.mu.Unlock()

	s.mu.Lock()
	oldControl := s.gens[generationA].control
	oldControl.state = ProcessStopping
	s.mu.Unlock()
	select {
	case <-oldControl.child.Done():
		t.Fatal("test setup unexpectedly exited the old Control")
	default:
	}

	if err := s.ActivateControl(context.Background(), generationB); err != nil {
		t.Fatalf("activate candidate with live stopping Control: %v", err)
	}
	lifecycle.mu.Lock()
	steps := append([]string(nil), lifecycle.steps...)
	lifecycle.mu.Unlock()
	wantPrefix := "prepare:" + generationA + ":" + generationB
	if len(steps) == 0 || steps[0] != wantPrefix {
		t.Fatalf("live stopping Control handoff calls = %v, want first call %q", steps, wantPrefix)
	}
	closeTestSupervisor(t, s)
}

func TestControlActivationFailureRestoresPreviousRoute(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	lifecycle := &fakeLifecycle{failActivate: generationB}
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationB); err == nil {
		t.Fatal("failed Control activation unexpectedly succeeded")
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationA {
		t.Fatalf("route owner after activation failure = %q, want %q", got, generationA)
	}
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if body := recorder.Body.String(); !strings.Contains(body, "A") {
		t.Fatalf("route after rollback = %q, want A", body)
	}
	closeTestSupervisor(t, s)
}

func TestActivateControlWithPersistsBeforeRouteAndRunsSchedulerAfterRoute(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	lifecycle := &fakeLifecycle{}
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}

	var steps []string
	lifecycle.eventLog = &steps
	lifecycle.mu.Lock()
	lifecycle.steps = nil
	lifecycle.mu.Unlock()
	if err := s.ActivateControlWith(context.Background(), generationB, func() error {
		steps = append(steps, "persist")
		if got := s.Snapshot().ActiveControlGeneration; got != generationA {
			return fmt.Errorf("route changed before durable transition: %s", got)
		}
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(steps); got != fmt.Sprint([]string{"prepare:" + generationA + ":" + generationB, "prepare-activation:" + generationB, "persist", "activate:" + generationB}) {
		t.Fatalf("activation sequence = %v", steps)
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationB {
		t.Fatalf("active Control = %s, want %s", got, generationB)
	}
	closeTestSupervisor(t, s)
}

func TestCandidatePreparationFailureLeavesOldRouteUntouched(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	lifecycle := &fakeLifecycle{}
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}
	lifecycle.failPrepareActivation = generationB
	beforeRouteCalled := false
	err := s.ActivateControlWith(context.Background(), generationB, func() error {
		beforeRouteCalled = true
		return nil
	}, nil)
	if err == nil {
		t.Fatal("candidate with failed application preparation unexpectedly activated")
	}
	if beforeRouteCalled {
		t.Fatal("durable activation callback ran before candidate preparation succeeded")
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationA {
		t.Fatalf("route owner after candidate preparation failure = %s, want %s", got, generationA)
	}
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if !strings.Contains(recorder.Body.String(), "A") {
		t.Fatalf("route after candidate preparation failure = %q, want A", recorder.Body.String())
	}
	lifecycle.mu.Lock()
	steps := append([]string(nil), lifecycle.steps...)
	lifecycle.mu.Unlock()
	wantSuffix := []string{"prepare:" + generationA + ":" + generationB, "prepare-activation:" + generationB, "rollback:" + generationA + ":" + generationB}
	if got := fmt.Sprint(steps[len(steps)-len(wantSuffix):]); got != fmt.Sprint(wantSuffix) {
		t.Fatalf("failed preparation lifecycle order = %v, want suffix %v", steps, wantSuffix)
	}
	closeTestSupervisor(t, s)
}

func TestColdStartPreparesCandidateBeforeRoutingAndActivation(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "cold") }))
	defer backend.Close()
	lifecycle := &fakeLifecycle{}
	var events []string
	lifecycle.eventLog = &events
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backend.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	want := []string{"prepare::" + generationA, "prepare-activation:" + generationA, "activate:" + generationA}
	if got := fmt.Sprint(events); got != fmt.Sprint(want) {
		t.Fatalf("cold-start activation order = %v, want %v", events, want)
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationA {
		t.Fatalf("cold-start active Control = %s, want %s", got, generationA)
	}
	closeTestSupervisor(t, s)
}

func TestActivateControlWithRestoresDurableTransitionWhenCandidateFails(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	lifecycle := &fakeLifecycle{failActivate: generationB}
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}
	durableActive := generationA
	err := s.ActivateControlWith(context.Background(), generationB, func() error {
		durableActive = generationB
		return nil
	}, func() error {
		durableActive = generationA
		return nil
	})
	if err == nil {
		t.Fatal("candidate activation unexpectedly succeeded")
	}
	if got := s.Snapshot().ActiveControlGeneration; got != generationA {
		t.Fatalf("route = %s, want restored %s", got, generationA)
	}
	if durableActive != generationA {
		t.Fatalf("durable active generation = %s, want restored %s", durableActive, generationA)
	}
	closeTestSupervisor(t, s)
}

func TestActivateControlWithAbortFailureLeavesRouteUnavailableWithoutRollback(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()
	lifecycle := &fakeLifecycle{failActivate: generationB}
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), lifecycle)
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}

	abortCause := errors.New("generation remains incompatible")
	abortCalls := 0
	err := s.ActivateControlWith(context.Background(), generationB, func() error { return nil }, func() error {
		abortCalls++
		return abortCause
	})
	if err == nil || err.Error() != "durable control activation abort failed" {
		t.Fatalf("activation error = %v, want stable durable-abort failure", err)
	}
	if !errors.Is(err, abortCause) {
		t.Fatalf("activation error %v does not preserve abort cause", err)
	}
	if abortCalls != 1 {
		t.Fatalf("abort callback calls = %d, want 1", abortCalls)
	}

	snapshot := s.Snapshot()
	if snapshot.ActiveControlGeneration != "" {
		t.Fatalf("active route = %q, want unavailable", snapshot.ActiveControlGeneration)
	}
	for _, generation := range snapshot.Generations {
		if generation.ControlActive {
			t.Fatalf("generation %s remains marked Control active", generation.ID)
		}
	}
	recorder := httptest.NewRecorder()
	s.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("request status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(recorder.Body.String(), "control plane unavailable") {
		t.Fatalf("request body = %q, want unavailable response", recorder.Body.String())
	}

	lifecycle.mu.Lock()
	steps := append([]string(nil), lifecycle.steps...)
	lifecycle.mu.Unlock()
	for _, step := range steps {
		if strings.HasPrefix(step, "rollback:") {
			t.Fatalf("handoff rollback ran after failed durable abort: %v", steps)
		}
	}
	closeTestSupervisor(t, s)
}

func TestServeWithHostHandlerInterceptsOnlyItsPrefix(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "control") }))
	defer backend.Close()
	s := newTestSupervisor(t, &fakeLauncher{}, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), &fakeLifecycle{})
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backend.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	host := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/runtime/update" {
			_, _ = io.WriteString(w, "host")
			return
		}
		s.ServeHTTP(w, r)
	})
	go func() { serveDone <- s.ServeWithHostHandler(serveCtx, listener, "/api/runtime/update", host) }()
	baseURL := "http://" + listener.Addr().String()
	waitHTTPReady(t, baseURL)
	if got := getBody(t, baseURL+"/api/runtime/update"); got != "host" {
		t.Fatalf("Host route = %q", got)
	}
	if got := getBody(t, baseURL+"/recordings"); got != "control" {
		t.Fatalf("Control route = %q", got)
	}
	cancelServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("ServeWithHostHandler: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeWithHostHandler did not stop within its bound")
	}
}

func TestInFlightOldControlRequestCompletesBeforeControlStop(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/slow" {
			_, _ = io.WriteString(w, "ready")
			return
		}
		close(entered)
		<-release
		_, _ = io.WriteString(w, "drained")
	}))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "new") }))
	defer backendB.Close()
	launcher := &fakeLauncher{}
	s := newTestSupervisor(t, launcher, ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }), &fakeLifecycle{})
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationA, backendA.URL)); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(serveCtx, listener) }()
	baseURL := "http://" + listener.Addr().String()
	waitHTTPReady(t, baseURL)
	requestDone := make(chan string, 1)
	go func() { requestDone <- getBody(t, baseURL+"/slow") }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("old route was not entered")
	}
	if err := s.StageGeneration(context.Background(), fakeGeneration(generationB, backendB.URL)); err != nil {
		t.Fatal(err)
	}
	activationDone := make(chan error, 1)
	go func() { activationDone <- s.ActivateControl(context.Background(), generationB) }()
	select {
	case err := <-activationDone:
		t.Fatalf("activation returned before in-flight old request drained: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case body := <-requestDone:
		if body != "drained" {
			t.Fatalf("old request body = %q", body)
		}
	case <-time.After(time.Second):
		t.Fatal("old request did not complete")
	}
	select {
	case err := <-activationDone:
		if err != nil {
			t.Fatalf("activation failed after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("activation did not finish after old request drained")
	}
	cancelServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not stop")
	}
}

func TestRealChildProcessesControlReplacementDoesNotStopEngine(t *testing.T) {
	launcher := ExecLauncher{}
	ready := ReadinessFunc(func(ctx context.Context, spec ProcessSpec, _ Child) error {
		marker := envValue(spec.Env, "IR_SUPERVISOR_MARKER")
		return waitMarker(ctx, marker)
	})
	s := newTestSupervisor(t, launcher, ready, &fakeLifecycle{})
	t.Cleanup(func() { closeTestSupervisor(t, s) })
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "A") }))
	defer backendA.Close()
	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "B") }))
	defer backendB.Close()

	g1 := subprocessGeneration(t, generationA, backendA.URL)
	if err := s.StageGeneration(context.Background(), g1); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationA); err != nil {
		t.Fatal(err)
	}

	g2 := subprocessGeneration(t, generationB, backendB.URL)
	if err := s.StageGeneration(context.Background(), g2); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateControl(context.Background(), generationB); err != nil {
		t.Fatal(err)
	}
	engineProcess, ok := launcherChildFromSupervisor(t, s, generationA, RoleEngine)
	if !ok {
		t.Fatal("old Engine not retained")
	}
	oldEngine := engineProcess.child
	select {
	case <-oldEngine.Done():
		t.Fatal("actual Engine subprocess exited when old Control was replaced")
	default:
	}
	time.Sleep(30 * time.Millisecond)
	select {
	case <-oldEngine.Done():
		t.Fatal("actual Engine subprocess did not outlive Control replacement")
	default:
	}
	oldControl, ok := launcherChildFromSupervisor(t, s, generationA, RoleControl)
	if ok {
		t.Fatalf("old Control process remains registered: %+v", oldControl)
	}
	drain := &fakeDrain{active: 0}
	if err := s.RetireEngine(context.Background(), generationA, drain); err != nil {
		t.Fatalf("retire drained old process: %v", err)
	}
}

func TestChildEnvironmentDoesNotInheritHostSecrets(t *testing.T) {
	t.Setenv("IR_RUNTIME_HOST_SECRET_TEST", "must-not-cross-generation-boundary")
	got, err := childEnvironment([]string{"PATH=/usr/bin"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "PATH=/usr/bin" {
		t.Fatalf("child environment = %v, want only explicit variables", got)
	}
}

func TestProcessExitObserverRunsAfterChildExitEvenIfRoleWasRemoved(t *testing.T) {
	launcher := &fakeLauncher{}
	exits := make(chan ProcessSpec, 1)
	s, err := New(Options{
		Launcher:         launcher,
		Readiness:        ReadinessFunc(func(context.Context, ProcessSpec, Child) error { return nil }),
		ControlLifecycle: &fakeLifecycle{},
		OnProcessExit:    func(spec ProcessSpec, _ error) { exits <- spec },
		ShutdownTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTestSupervisor(t, s) })
	if err := s.StartEngine(context.Background(), ProcessSpec{GenerationID: generationA, Role: RoleEngine, Executable: "engine"}); err != nil {
		t.Fatal(err)
	}
	child, ok := launcher.child(generationA, RoleEngine)
	if !ok {
		t.Fatal("Engine child was not launched")
	}
	child.Kill()
	if err := s.RetireEngine(context.Background(), generationA, &fakeDrain{}); err != nil {
		t.Fatalf("retire exited Engine: %v", err)
	}
	select {
	case got := <-exits:
		if got.GenerationID != generationA || got.Role != RoleEngine {
			t.Fatalf("exit observer received %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("process exit observer was not called")
	}
}

// TestSupervisorSubprocessHelper is re-executed by the process-boundary test.
func TestSupervisorSubprocessHelper(t *testing.T) {
	if os.Getenv("IR_SUPERVISOR_CHILD_MODE") != "1" {
		return
	}
	marker := os.Getenv("IR_SUPERVISOR_MARKER")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := os.WriteFile(marker, []byte("running"), 0600); err != nil {
		os.Exit(2)
	}
	<-ctx.Done()
}

func newTestSupervisor(t *testing.T, launcher Launcher, readiness Readiness, lifecycle ControlLifecycle) *Supervisor {
	t.Helper()
	s, err := New(Options{Launcher: launcher, Readiness: readiness, ControlLifecycle: lifecycle, ShutdownTimeout: 2 * time.Second, CleanupTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fakeGeneration(id, target string) GenerationSpec {
	u, _ := url.Parse(target)
	return GenerationSpec{
		ID:            id,
		Engine:        ProcessSpec{GenerationID: id, Role: RoleEngine, Executable: "engine"},
		Control:       ProcessSpec{GenerationID: id, Role: RoleControl, Executable: "control"},
		ControlTarget: u,
	}
}

func subprocessGeneration(t *testing.T, id, target string) GenerationSpec {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	makeSpec := func(role Role) ProcessSpec {
		marker := filepath.Join(t.TempDir(), string(role)+".marker")
		return ProcessSpec{
			GenerationID: id, Role: role, Executable: executable,
			Args: []string{"-test.run=^TestSupervisorSubprocessHelper$"},
			Env:  []string{"IR_SUPERVISOR_CHILD_MODE=1", "IR_SUPERVISOR_MARKER=" + marker},
		}
	}
	u, _ := url.Parse(target)
	return GenerationSpec{ID: id, Engine: makeSpec(RoleEngine), Control: makeSpec(RoleControl), ControlTarget: u}
}

func waitHTTPReady(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, base, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Close = true
		resp, err := http.DefaultClient.Do(request)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("runtime host listener did not start")
}

func getBody(t *testing.T, base string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, base, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Close = true
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Errorf("GET runtime listener: %v", err)
		return ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("read runtime response: %v", err)
	}
	return strings.TrimSpace(string(body))
}

func waitMarker(ctx context.Context, path string) error {
	if path == "" {
		return ErrCandidateNotReady
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func envValue(env []string, key string) string {
	for _, item := range env {
		name, value, ok := strings.Cut(item, "=")
		if ok && name == key {
			return value
		}
	}
	return ""
}

func launcherChildFromSupervisor(t *testing.T, s *Supervisor, generationID string, role Role) (*managedProcess, bool) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gens[generationID]
	if g == nil {
		return nil, false
	}
	process := processForRole(g, role)
	return process, process != nil
}

func waitControlState(t *testing.T, s *Supervisor, generationID string, want ProcessState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := s.Snapshot()
		for _, generation := range snapshot.Generations {
			if generation.ID == generationID && generation.Control.State == want {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Control %s did not reach state %s: %+v", generationID, want, s.Snapshot())
}

func closeTestSupervisor(t *testing.T, s *Supervisor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Errorf("close supervisor: %v", err)
	}
}

type fakeLauncher struct {
	mu       sync.Mutex
	children map[string]*fakeChild
}

func (l *fakeLauncher) Start(_ context.Context, spec ProcessSpec) (Child, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.children == nil {
		l.children = make(map[string]*fakeChild)
	}
	key := spec.GenerationID + "/" + string(spec.Role)
	child := &fakeChild{done: make(chan struct{})}
	l.children[key] = child
	return child, nil
}

func (l *fakeLauncher) child(id string, role Role) (*fakeChild, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	child, ok := l.children[id+"/"+string(role)]
	return child, ok
}

type fakeChild struct {
	done chan struct{}
	once sync.Once
	mu   sync.RWMutex
	err  error
}

func (c *fakeChild) Done() <-chan struct{} { return c.done }
func (c *fakeChild) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}
func (c *fakeChild) Stop(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.once.Do(func() { close(c.done) })
	return nil
}
func (c *fakeChild) Kill() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

type fakeLifecycle struct {
	mu                    sync.Mutex
	steps                 []string
	eventLog              *[]string
	failActivate          string
	failPrepareActivation string
}

func (l *fakeLifecycle) record(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.steps = append(l.steps, step)
	if l.eventLog != nil {
		*l.eventLog = append(*l.eventLog, step)
	}
}

func (l *fakeLifecycle) PrepareHandoff(_ context.Context, oldID, newID string) error {
	l.record(fmt.Sprintf("prepare:%s:%s", oldID, newID))
	return nil
}
func (l *fakeLifecycle) PrepareActivation(_ context.Context, id string) error {
	l.record("prepare-activation:" + id)
	if id == l.failPrepareActivation {
		return errors.New("application preparation rejected")
	}
	return nil
}
func (l *fakeLifecycle) Activate(_ context.Context, id string) error {
	l.record("activate:" + id)
	if id == l.failActivate {
		return errors.New("activation rejected")
	}
	return nil
}
func (l *fakeLifecycle) Rollback(_ context.Context, oldID, newID string) error {
	l.record(fmt.Sprintf("rollback:%s:%s", oldID, newID))
	return nil
}

type fakeDrain struct {
	active int
}

func (d *fakeDrain) BeginDrain(context.Context, string) error { return nil }
func (d *fakeDrain) ActiveRecordings(context.Context, string) (int, error) {
	return d.active, nil
}

const (
	generationA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	generationB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)
