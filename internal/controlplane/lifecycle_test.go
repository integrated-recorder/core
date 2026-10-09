package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleStartsAndDrainsExactlyOnce(t *testing.T) {
	var started, drained atomic.Int32
	lifecycle, err := NewLifecycle("control-a", true, func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		started.Add(1)
		return nil
	}, func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		drained.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := lifecycle.Snapshot(); got.State != LifecyclePassive || !got.Ready || got.Active {
		t.Fatalf("initial lifecycle = %#v", got)
	}
	if got := lifecycle.RuntimeInstanceID(); got == "control-a" || got == "" || got != lifecycle.Snapshot().InstanceID {
		t.Fatalf("runtime instance identity is not process-specific: %q", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got := lifecycle.Snapshot(); got.State != LifecycleActive || !got.Active {
		t.Fatalf("active lifecycle = %#v", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if got := lifecycle.Snapshot(); got.State != LifecycleDeactivated || got.Active || got.Prepared {
		t.Fatalf("deactivated lifecycle = %#v", got)
	}
	if started.Load() != 1 || drained.Load() != 1 {
		t.Fatalf("callbacks started=%d drained=%d", started.Load(), drained.Load())
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err == nil {
		t.Fatal("reactivation unexpectedly succeeded")
	}
}

func TestLifecycleInstallationValidationAndReadySignalRequireActiveControl(t *testing.T) {
	var validated, activated atomic.Int32
	lifecycle, err := NewLifecycleWithHooks("control-setup", true, LifecycleHooks{
		Start:   func(context.Context) error { return nil },
		Prepare: func(context.Context) error { return nil },
		Resume:  func(context.Context) error { return nil },
		Drain:   func(context.Context) error { return nil },
		ValidateInstallation: func(context.Context) error {
			validated.Add(1)
			return nil
		},
		InstallationReady: func(context.Context) error {
			activated.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{OperationControlValidateInstall, OperationControlInstallReady} {
		if _, err := lifecycle.Handle(context.Background(), operation, json.RawMessage(`{}`)); err == nil {
			t.Fatalf("passive Control accepted %s", operation)
		}
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{OperationControlValidateInstall, OperationControlInstallReady} {
		if _, err := lifecycle.Handle(context.Background(), operation, json.RawMessage(`{"unexpected":true}`)); err == nil {
			t.Fatalf("%s accepted unknown request fields", operation)
		}
		if _, err := lifecycle.Handle(context.Background(), operation, json.RawMessage(`{}`)); err != nil {
			t.Fatalf("active Control rejected %s: %v", operation, err)
		}
	}
	if validated.Load() != 1 || activated.Load() != 1 {
		t.Fatalf("installation callbacks validate=%d activate=%d", validated.Load(), activated.Load())
	}
}

func TestLifecycleAuditAppendIsActiveOnlyAndPreservesValidatedActor(t *testing.T) {
	var appended []AuditAppendRequest
	lifecycle, err := NewLifecycleWithHooks("control-audit", true, LifecycleHooks{
		Start:   func(context.Context) error { return nil },
		Prepare: func(context.Context) error { return nil },
		Resume:  func(context.Context) error { return nil },
		Drain:   func(context.Context) error { return nil },
		AppendAudit: func(_ context.Context, request AuditAppendRequest) error {
			appended = append(appended, request)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	userRequest := AuditAppendRequest{Action: AuditPluginInstalled, ObjectID: "fixture", ActorType: "user", UserID: "usr-0123456789abcdef0123456789abcdef"}
	userPayload, err := json.Marshal(userRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlAppendAudit, userPayload); err == nil {
		t.Fatal("passive Control accepted audit append")
	}
	if len(appended) != 0 {
		t.Fatalf("passive Control invoked audit hook: %+v", appended)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	for _, request := range []AuditAppendRequest{
		userRequest,
		{Action: AuditRuntimeUpdateStaged, ObjectID: "runtime-update", ActorType: "system"},
	} {
		payload, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lifecycle.Handle(context.Background(), OperationControlAppendAudit, payload); err != nil {
			t.Fatalf("active Control rejected actor %q: %v", request.ActorType, err)
		}
	}
	if len(appended) != 2 || appended[0] != userRequest || appended[1].ActorType != "system" || appended[1].UserID != "" {
		t.Fatalf("audit hook requests=%+v", appended)
	}
	invalid, _ := json.Marshal(AuditAppendRequest{Action: AuditPluginInstalled, ActorType: "user", UserID: "../forged"})
	if _, err := lifecycle.Handle(context.Background(), OperationControlAppendAudit, invalid); err == nil {
		t.Fatal("invalid audit actor crossed lifecycle boundary")
	}
	if len(appended) != 2 {
		t.Fatalf("invalid audit request reached hook: %+v", appended)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlPrepareHandoff, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlAppendAudit, userPayload); err == nil {
		t.Fatal("fenced Control accepted audit append")
	}
	if len(appended) != 2 {
		t.Fatalf("fenced audit request reached hook: %+v", appended)
	}
}

func TestLifecyclePreparationKeepsGenerationPassiveUntilActivation(t *testing.T) {
	var prepared, started, drained atomic.Int32
	lifecycle, err := NewLifecycleWithHooks("control-candidate", true, LifecycleHooks{
		PrepareActivation: func(context.Context) error { prepared.Add(1); return nil },
		Start:             func(context.Context) error { started.Add(1); return nil },
		Prepare:           func(context.Context) error { return nil },
		Resume:            func(context.Context) error { return nil },
		Drain:             func(context.Context) error { drained.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := lifecycle.Snapshot()
	if !initial.Ready || initial.Prepared || initial.Active || initial.State != LifecyclePassive {
		t.Fatalf("process-level readiness = %#v", initial)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlPrepareActivation, nil); err != nil {
		t.Fatalf("prepare activation: %v", err)
	}
	preparedSnapshot := lifecycle.Snapshot()
	if !preparedSnapshot.Ready || !preparedSnapshot.Prepared || preparedSnapshot.Active || preparedSnapshot.State != LifecyclePassive {
		t.Fatalf("prepared passive lifecycle = %#v", preparedSnapshot)
	}
	if prepared.Load() != 1 || started.Load() != 0 {
		t.Fatalf("prepare/start callbacks = %d/%d", prepared.Load(), started.Load())
	}
	// Preparation is idempotent and does not rebuild a potentially stale
	// application after the old generation has been fenced.
	if _, err := lifecycle.Handle(context.Background(), OperationControlPrepareActivation, nil); err != nil {
		t.Fatalf("repeat prepare activation: %v", err)
	}
	if prepared.Load() != 1 {
		t.Fatalf("preparation callback repeated %d times", prepared.Load())
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if got := lifecycle.Snapshot(); !got.Ready || !got.Prepared || !got.Active || got.State != LifecycleActive {
		t.Fatalf("active lifecycle = %#v", got)
	}
	if started.Load() != 1 {
		t.Fatalf("activation callback count = %d", started.Load())
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if drained.Load() != 1 {
		t.Fatalf("prepared app drain count = %d", drained.Load())
	}
}

func TestLifecyclePreparationFailureCannotActivate(t *testing.T) {
	lifecycle, err := NewLifecycleWithHooks("control-candidate", true, LifecycleHooks{
		PrepareActivation: func(context.Context) error { return errors.New("private setup detail") },
		Start:             func(context.Context) error { t.Fatal("start ran after preparation failure"); return nil },
		Prepare:           func(context.Context) error { return nil },
		Resume:            func(context.Context) error { return nil },
		Drain:             func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = lifecycle.Handle(context.Background(), OperationControlPrepareActivation, nil)
	var public interface{ PublicIPCError() (string, string) }
	if !errors.As(err, &public) {
		t.Fatalf("preparation failure is not safe IPC error: %v", err)
	}
	code, message := public.PublicIPCError()
	if code != "preparation_failed" || message == "private setup detail" {
		t.Fatalf("public preparation error = %q %q", code, message)
	}
	if got := lifecycle.Snapshot(); got.Ready || got.Prepared || got.State != LifecycleFailed {
		t.Fatalf("failed preparation snapshot = %#v", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err == nil {
		t.Fatal("unprepared lifecycle unexpectedly activated")
	}
}

func TestLifecycleDrainClosesPreparedPassiveApplication(t *testing.T) {
	var drained atomic.Int32
	lifecycle, err := NewLifecycleWithHooks("control-candidate", true, LifecycleHooks{
		PrepareActivation: func(context.Context) error { return nil },
		Start:             func(context.Context) error { return nil },
		Prepare:           func(context.Context) error { return nil },
		Resume:            func(context.Context) error { return nil },
		Drain:             func(context.Context) error { drained.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlPrepareActivation, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil); err != nil {
		t.Fatalf("drain prepared passive lifecycle: %v", err)
	}
	if drained.Load() != 1 {
		t.Fatalf("prepared passive drain count = %d", drained.Load())
	}
}

func TestLifecycleCandidateMutationFenceDrainAndRollbackResume(t *testing.T) {
	gate := NewMutationGate()
	started := make(chan struct{})
	release := make(chan struct{})
	var blockFirst atomic.Bool
	handler := gate.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && blockFirst.CompareAndSwap(false, true) {
			close(started)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	lifecycle, err := NewLifecycleWithHooks("control-b", true, LifecycleHooks{
		Start:   func(context.Context) error { gate.Activate(); return nil },
		Prepare: func(ctx context.Context) error { gate.Fence(); return gate.WaitForDrain(ctx) },
		Resume:  func(context.Context) error { gate.Activate(); return nil },
		Drain:   func(ctx context.Context) error { gate.Fence(); return gate.WaitForDrain(ctx) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if response := serveGateRequest(handler, http.MethodGet); response.Code != http.StatusNoContent {
		t.Fatalf("passive GET status=%d", response.Code)
	}
	if response := serveGateRequest(handler, http.MethodPost); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("passive mutation status=%d", response.Code)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		response := serveGateRequest(handler, http.MethodPost)
		if response.Code != http.StatusNoContent {
			t.Errorf("admitted mutation status=%d", response.Code)
		}
	}()
	<-started

	prepareDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.Handle(context.Background(), OperationControlPrepareHandoff, nil)
		prepareDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if response := serveGateRequest(handler, http.MethodPost); response.Code == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("prepare did not fence later mutations")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-prepareDone:
		t.Fatalf("prepare returned before in-flight mutation drained: %v", err)
	default:
	}
	close(release)
	<-firstDone
	if err := <-prepareDone; err != nil {
		t.Fatalf("prepare after mutation drain: %v", err)
	}
	if got := lifecycle.Snapshot().State; got != LifecyclePreparing {
		t.Fatalf("prepared state=%q", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlResume, nil); err != nil {
		t.Fatalf("resume after candidate rollback: %v", err)
	}
	if got := lifecycle.Snapshot().State; got != LifecycleActive {
		t.Fatalf("resumed state=%q", got)
	}
	if response := serveGateRequest(handler, http.MethodPost); response.Code != http.StatusNoContent {
		t.Fatalf("mutation after resume status=%d", response.Code)
	}
}

func serveGateRequest(handler http.Handler, method string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/api/test", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestLifecycleDrainCanBeRetriedAfterDeadline(t *testing.T) {
	var attempts atomic.Int32
	lifecycle, err := NewLifecycle("control-a", true, func(context.Context) error { return nil }, func(ctx context.Context) error {
		if attempts.Add(1) == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil); err == nil {
		t.Fatal("first drain unexpectedly succeeded")
	}
	if got := lifecycle.Snapshot().State; got != LifecycleDraining {
		t.Fatalf("state after incomplete drain = %q", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil); err != nil {
		t.Fatalf("retry drain: %v", err)
	}
	if got := lifecycle.Snapshot().State; got != LifecycleDeactivated {
		t.Fatalf("state after retry = %q", got)
	}
	if attempts.Load() != 2 {
		t.Fatalf("drain attempts = %d", attempts.Load())
	}
}

func TestLifecycleRejectsActivationFailure(t *testing.T) {
	want := errors.New("private detail")
	lifecycle, err := NewLifecycle("control-a", true, func(context.Context) error { return want }, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = lifecycle.Handle(context.Background(), OperationControlActivate, nil)
	var public interface{ PublicIPCError() (string, string) }
	if !errors.As(err, &public) {
		t.Fatalf("activation error is not safe IPC error: %v", err)
	}
	code, message := public.PublicIPCError()
	if code != "activation_failed" || message == "private detail" {
		t.Fatalf("public error = %q %q", code, message)
	}
	if got := lifecycle.Snapshot().State; got != LifecycleFailed {
		t.Fatalf("state after activation failure = %q", got)
	}
}

func TestLifecycleRequiresReadyGeneration(t *testing.T) {
	if _, err := NewLifecycle("control-a", false, func(context.Context) error { return nil }, func(context.Context) error { return nil }); err == nil {
		t.Fatal("unready lifecycle unexpectedly constructed")
	}
	if _, err := NewLifecycle("", true, func(context.Context) error { return nil }, func(context.Context) error { return nil }); err == nil {
		t.Fatal("empty generation unexpectedly accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	lifecycle, _ := NewLifecycle("control-a", true, func(context.Context) error { return nil }, func(context.Context) error { return nil })
	if _, err := lifecycle.Handle(ctx, OperationControlActivate, nil); err == nil {
		t.Fatal("canceled activation unexpectedly succeeded")
	}
}

func TestLifecycleDetachEngineRequiresActiveStateAndStrictRequest(t *testing.T) {
	var detached atomic.Int32
	lifecycle, err := NewLifecycleWithHooks("control-a", true, LifecycleHooks{
		Start:   func(context.Context) error { return nil },
		Prepare: func(context.Context) error { return nil },
		Resume:  func(context.Context) error { return nil },
		Drain:   func(context.Context) error { return nil },
		DetachEngine: func(_ context.Context, generationID string) error {
			if generationID != "engine-old_1" {
				t.Errorf("detached generation = %q", generationID)
			}
			detached.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	validPayload := json.RawMessage(`{"generation_id":"engine-old_1"}`)

	if _, err := lifecycle.Handle(context.Background(), OperationControlDetachEngine, validPayload); err == nil {
		t.Fatal("passive Control detached an Engine")
	}
	for _, malformed := range []json.RawMessage{
		nil,
		json.RawMessage(`{}`),
		json.RawMessage(`{"generation_id":"engine/a"}`),
		json.RawMessage(`{"generation_id":"engine-a","extra":true}`),
		json.RawMessage(`{"generation_id":"engine-a"} {}`),
		json.RawMessage(`null`),
	} {
		if _, err := lifecycle.Handle(context.Background(), OperationControlDetachEngine, malformed); err == nil {
			t.Errorf("malformed detach payload %q was accepted", malformed)
		}
	}
	if got := detached.Load(); got != 0 {
		t.Fatalf("detach hook ran before activation: %d", got)
	}

	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlPrepareHandoff, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDetachEngine, validPayload); err == nil {
		t.Fatal("handoff-preparing Control detached an Engine")
	}
	if got := detached.Load(); got != 0 {
		t.Fatalf("detach hook ran while Control was preparing: %d", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlResume, nil); err != nil {
		t.Fatal(err)
	}
	snapshotValue, err := lifecycle.Handle(context.Background(), OperationControlDetachEngine, validPayload)
	if err != nil {
		t.Fatalf("active detach: %v", err)
	}
	snapshot, ok := snapshotValue.(LifecycleSnapshot)
	if !ok || !snapshot.Active || snapshot.State != LifecycleActive || snapshot.GenerationID != "control-a" {
		t.Fatalf("detach result snapshot = %#v", snapshotValue)
	}
	if got := detached.Load(); got != 1 {
		t.Fatalf("detach hook invocation count = %d", got)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlDetachEngine, validPayload); err == nil {
		t.Fatal("deactivated Control detached an Engine")
	}
}

func TestLifecycleDetachEngineMapsHookFailureToSafeIPCError(t *testing.T) {
	lifecycle, err := NewLifecycleWithHooks("control-a", true, LifecycleHooks{
		Start:   func(context.Context) error { return nil },
		Prepare: func(context.Context) error { return nil },
		Resume:  func(context.Context) error { return nil },
		Drain:   func(context.Context) error { return nil },
		DetachEngine: func(context.Context, string) error {
			return errors.New("private socket path /secret/runtime.sock")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	_, err = lifecycle.Handle(context.Background(), OperationControlDetachEngine, json.RawMessage(`{"generation_id":"engine-old"}`))
	var public interface{ PublicIPCError() (string, string) }
	if !errors.As(err, &public) {
		t.Fatalf("detach error is not a safe IPC error: %v", err)
	}
	code, message := public.PublicIPCError()
	if code != "engine_detach_failed" || message != "recorder engine generation could not be detached" {
		t.Fatalf("public detach error = %q %q", code, message)
	}
}

func TestLifecycleDetachEngineRejectsWhileDraining(t *testing.T) {
	drainEntered := make(chan struct{})
	releaseDrain := make(chan struct{})
	var detachCalls atomic.Int32
	lifecycle, err := NewLifecycleWithHooks("control-a", true, LifecycleHooks{
		Start:   func(context.Context) error { return nil },
		Prepare: func(context.Context) error { return nil },
		Resume:  func(context.Context) error { return nil },
		Drain: func(context.Context) error {
			close(drainEntered)
			<-releaseDrain
			return nil
		},
		DetachEngine: func(context.Context, string) error { detachCalls.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Handle(context.Background(), OperationControlActivate, nil); err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseDrain) })
	deactivateDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.Handle(context.Background(), OperationControlDeactivate, nil)
		deactivateDone <- err
	}()
	select {
	case <-drainEntered:
	case <-time.After(time.Second):
		t.Fatal("drain hook was not entered")
	}
	if got := lifecycle.Snapshot().State; got != LifecycleDraining {
		t.Fatalf("state during drain = %q", got)
	}
	detachDone := make(chan error, 1)
	go func() {
		_, err := lifecycle.Handle(context.Background(), OperationControlDetachEngine, json.RawMessage(`{"generation_id":"engine-old"}`))
		detachDone <- err
	}()
	releaseOnce.Do(func() { close(releaseDrain) })
	if err := <-deactivateDone; err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if err := <-detachDone; err == nil {
		t.Fatal("draining/deactivated Control detached an Engine")
	}
	if got := detachCalls.Load(); got != 0 {
		t.Fatalf("detach hook invocation count = %d", got)
	}
}
