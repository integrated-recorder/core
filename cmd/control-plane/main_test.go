package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/recorderengine"
)

func TestNormalizeEngineDetachResultIsIdempotentOnlyWhenAlreadyAbsent(t *testing.T) {
	if err := normalizeEngineDetachResult(recorderengine.ErrGenerationNotAttached); err != nil {
		t.Fatalf("already absent Engine detach error = %v, want idempotent success", err)
	}
	other := context.DeadlineExceeded
	if err := normalizeEngineDetachResult(other); err != other {
		t.Fatalf("unrelated Engine detach error = %v, want original %v", err, other)
	}
}

func TestPassiveControlPreparationUsesLatestSharedStateWithoutAdmission(t *testing.T) {
	gate := controlplane.NewMutationGate()
	passive := passiveHealthHandler("candidate")
	slot := newHandlerSlot(passive)
	persistedSettings := "old"
	var opens atomic.Int32
	var prepared *fakeControlApplication
	lifecycle := newControlApplicationLifecycle(gate, slot, passive, context.Background(), func(context.Context) (controlApplicationRuntime, error) {
		opens.Add(1)
		prepared = &fakeControlApplication{observedSettings: persistedSettings}
		return prepared, nil
	})
	mutationHandler := gate.Wrap(slot)

	if opens.Load() != 0 {
		t.Fatal("passive process startup opened shared application state")
	}
	if err := lifecycle.detachEngine(context.Background(), "engine-old"); err == nil {
		t.Fatal("unprepared Control detached an Engine")
	}
	if got := requestMutation(mutationHandler); got != http.StatusServiceUnavailable {
		t.Fatalf("passive mutation status = %d, want 503", got)
	}

	// The existing Control may commit settings while this process is staged.
	// Full construction happens only after the Host invokes preparation.
	persistedSettings = "latest"
	if err := lifecycle.prepareActivation(context.Background()); err != nil {
		t.Fatalf("prepare candidate: %v", err)
	}
	if opens.Load() != 1 || prepared == nil || prepared.observedSettings != "latest" {
		t.Fatalf("prepared state opened=%d app=%#v", opens.Load(), prepared)
	}
	if prepared.backgroundStarts.Load() != 0 {
		t.Fatalf("preparation started %d background loops", prepared.backgroundStarts.Load())
	}
	if err := lifecycle.detachEngine(context.Background(), "engine-old"); err == nil {
		t.Fatal("prepared passive Control detached an Engine")
	}
	if got := requestMutation(mutationHandler); got != http.StatusServiceUnavailable {
		t.Fatalf("prepared passive mutation status = %d, want 503", got)
	}
	if err := lifecycle.prepareActivation(context.Background()); err != nil {
		t.Fatalf("repeat candidate prepare: %v", err)
	}
	if opens.Load() != 1 {
		t.Fatalf("idempotent preparation reopened application %d times", opens.Load())
	}

	if err := lifecycle.start(context.Background()); err != nil {
		t.Fatalf("activate candidate services: %v", err)
	}
	if prepared.backgroundStarts.Load() != 1 {
		t.Fatalf("background start count = %d, want 1", prepared.backgroundStarts.Load())
	}
	if err := lifecycle.detachEngine(context.Background(), "engine-old"); err != nil {
		t.Fatalf("active Control detach wiring: %v", err)
	}
	if prepared.detaches.Load() != 1 {
		t.Fatalf("active Control detach callback count = %d", prepared.detaches.Load())
	}
	if got := requestMutation(mutationHandler); got != http.StatusNoContent {
		t.Fatalf("active mutation status = %d, want 204", got)
	}

	if err := lifecycle.prepareHandoff(context.Background()); err != nil {
		t.Fatalf("prepare active generation for handoff: %v", err)
	}
	if err := lifecycle.detachEngine(context.Background(), "engine-old"); err == nil {
		t.Fatal("handoff-preparing Control detached an Engine")
	}
	if prepared.pauses.Load() != 1 {
		t.Fatalf("handoff pause count = %d, want 1", prepared.pauses.Load())
	}
	if got := requestMutation(mutationHandler); got != http.StatusServiceUnavailable {
		t.Fatalf("fenced mutation status = %d, want 503", got)
	}
	if err := lifecycle.resume(context.Background()); err != nil {
		t.Fatalf("resume after rollback: %v", err)
	}
	if prepared.resumes.Load() != 1 {
		t.Fatalf("resume count = %d, want 1", prepared.resumes.Load())
	}
	if got := requestMutation(mutationHandler); got != http.StatusNoContent {
		t.Fatalf("mutation after resume status = %d, want 204", got)
	}
	if err := lifecycle.drain(context.Background()); err != nil {
		t.Fatalf("drain application: %v", err)
	}
	if prepared.closes.Load() != 1 {
		t.Fatalf("close count = %d, want 1", prepared.closes.Load())
	}
	if got := requestMutation(mutationHandler); got != http.StatusServiceUnavailable {
		t.Fatalf("mutation after drain status = %d, want 503", got)
	}
}

func TestFailedPassivePreparationLeavesNoPreparedApplication(t *testing.T) {
	gate := controlplane.NewMutationGate()
	passive := passiveHealthHandler("candidate")
	slot := newHandlerSlot(passive)
	lifecycle := newControlApplicationLifecycle(gate, slot, passive, context.Background(), func(context.Context) (controlApplicationRuntime, error) {
		return nil, errors.New("unreadable persisted dependency")
	})
	if err := lifecycle.prepareActivation(context.Background()); err == nil {
		t.Fatal("preparation unexpectedly succeeded")
	}
	if lifecycle.app != nil {
		t.Fatal("failed preparation retained an application")
	}
	if got := requestMutation(gate.Wrap(slot)); got != http.StatusServiceUnavailable {
		t.Fatalf("failed candidate mutation status = %d, want 503", got)
	}
}

func TestPreparedButNeverActivatedApplicationIsClosed(t *testing.T) {
	gate := controlplane.NewMutationGate()
	passive := passiveHealthHandler("candidate")
	slot := newHandlerSlot(passive)
	app := &fakeControlApplication{}
	lifecycle := newControlApplicationLifecycle(gate, slot, passive, context.Background(), func(context.Context) (controlApplicationRuntime, error) {
		return app, nil
	})
	if err := lifecycle.prepareActivation(context.Background()); err != nil {
		t.Fatalf("prepare passive application: %v", err)
	}
	if err := lifecycle.drain(context.Background()); err != nil {
		t.Fatalf("close prepared application before activation: %v", err)
	}
	if app.backgroundStarts.Load() != 0 || app.closes.Load() != 1 {
		t.Fatalf("prepared app background starts=%d closes=%d", app.backgroundStarts.Load(), app.closes.Load())
	}
}

func TestCanceledPreparationClosesConstructedServices(t *testing.T) {
	gate := controlplane.NewMutationGate()
	passive := passiveHealthHandler("candidate")
	slot := newHandlerSlot(passive)
	app := &fakeControlApplication{}
	ctx, cancel := context.WithCancel(context.Background())
	lifecycle := newControlApplicationLifecycle(gate, slot, passive, context.Background(), func(context.Context) (controlApplicationRuntime, error) {
		cancel()
		return app, nil
	})
	if err := lifecycle.prepareActivation(ctx); err == nil {
		t.Fatal("canceled preparation unexpectedly succeeded")
	}
	if app.closes.Load() != 1 || lifecycle.app != nil {
		t.Fatalf("canceled preparation retained app=%v, close count=%d", lifecycle.app != nil, app.closes.Load())
	}
	if got := requestMutation(gate.Wrap(slot)); got != http.StatusServiceUnavailable {
		t.Fatalf("canceled candidate mutation status = %d, want 503", got)
	}
}

func requestMutation(handler http.Handler) int {
	request := httptest.NewRequest(http.MethodPost, "/api/test", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response.Code
}

type fakeControlApplication struct {
	observedSettings string
	backgroundStarts atomic.Int32
	pauses           atomic.Int32
	resumes          atomic.Int32
	detaches         atomic.Int32
	closes           atomic.Int32
}

func (a *fakeControlApplication) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
}
func (a *fakeControlApplication) startBackground(context.Context) error {
	a.backgroundStarts.Add(1)
	return nil
}
func (a *fakeControlApplication) pauseForHandoff(context.Context) error {
	a.pauses.Add(1)
	return nil
}
func (a *fakeControlApplication) resumeAfterHandoff() error {
	a.resumes.Add(1)
	return nil
}
func (a *fakeControlApplication) detachEngine(context.Context, string) error {
	a.detaches.Add(1)
	return nil
}
func (a *fakeControlApplication) close(context.Context) error {
	a.closes.Add(1)
	return nil
}
