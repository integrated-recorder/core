package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestAdapterEnableDisableCancelsOnlyItsWorkflows(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"control-a", "workflow_required", "control-a", true)
	writeAdapter(t, dir, binaryPrefix+"control-b", "workflow_required", "control-b", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	input := json.RawMessage(`{"manifest_url":"https://media.example/live.m3u8"}`)
	workflowA, err := host.BeginResolution(context.Background(), "control-a", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	workflowB, err := host.BeginResolution(context.Background(), "control-b", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.SetEnabled("missing", false); !errors.Is(err, ErrAdapterUnavailable) {
		t.Fatalf("unknown adapter disable error=%v", err)
	}
	if err := host.SetEnabled("control-a", false); err != nil {
		t.Fatal(err)
	}
	got, err := host.Get("control-a")
	if err != nil || got.Status.State != "disabled" {
		t.Fatalf("disabled status=%#v err=%v", got.Status, err)
	}
	if _, err := host.Workflow(workflowA.WorkflowID); err == nil {
		t.Fatal("disabled adapter workflow remained active")
	}
	events := host.PendingWorkflowLifecycleEvents()
	if len(events) != 1 || events[0].WorkflowID != workflowA.WorkflowID || events[0].State != "canceled" {
		t.Fatalf("disable lifecycle events = %#v", events)
	}
	if _, err := host.Workflow(workflowB.WorkflowID); err != nil {
		t.Fatalf("unrelated adapter workflow was canceled: %v", err)
	}
	if _, err := host.call(context.Background(), "control-a", adapterproto.MethodDescribe, map[string]any{}); !errors.Is(err, ErrAdapterDisabled) {
		t.Fatalf("disabled adapter call error=%v", err)
	}
	if err := host.SetEnabled("control-a", true); err != nil {
		t.Fatal(err)
	}
	got, err = host.Get("control-a")
	if err != nil || got.Status.State != "ready" || got.Status.Generation != 2 {
		t.Fatalf("enabled status=%#v err=%v", got.Status, err)
	}
	if _, err := host.call(context.Background(), "control-a", adapterproto.MethodDescribe, map[string]any{}); err != nil {
		t.Fatalf("enabled adapter call failed: %v", err)
	}
}

func TestManualAdapterRestartRevalidatesDescriptor(t *testing.T) {
	t.Run("same descriptor", func(t *testing.T) {
		dir := t.TempDir()
		writeAdapter(t, dir, binaryPrefix+"restart-control", "normal", "restart-control", true)
		host, err := Discover(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		before, _ := host.Get("restart-control")
		got, err := host.Restart(context.Background(), "restart-control")
		if err != nil || got.Status.State != "ready" || got.Status.Generation != before.Status.Generation+1 {
			t.Fatalf("restart=%#v err=%v", got.Status, err)
		}
	})

	t.Run("only matching workflows are expired", func(t *testing.T) {
		dir := t.TempDir()
		writeAdapter(t, dir, binaryPrefix+"restart-workflow", "workflow_required", "restart-workflow", true)
		host, err := Discover(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		progress, err := host.BeginResolution(context.Background(), "restart-workflow", json.RawMessage(`{"manifest_url":"https://media.example/live.m3u8"}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := host.Restart(context.Background(), "restart-workflow"); err != nil {
			t.Fatal(err)
		}
		if _, err := host.Workflow(progress.WorkflowID); err == nil {
			t.Fatal("restart left a workflow bound to the old process generation")
		}
		events := host.PendingWorkflowLifecycleEvents()
		if len(events) != 1 || events[0].WorkflowID != progress.WorkflowID || events[0].State != "canceled" {
			t.Fatalf("restart lifecycle events = %#v", events)
		}
	})

	t.Run("changed descriptor rejected", func(t *testing.T) {
		dir := t.TempDir()
		writeRestartingAdapter(t, dir, "restart-stable", "normal", "restart-changed", "normal")
		host, err := Discover(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		got, err := host.Restart(context.Background(), "restart-stable")
		if err == nil || got.Status.State != "rejected" {
			t.Fatalf("mismatched restart=%#v err=%v", got.Status, err)
		}
		if _, err := host.call(context.Background(), "restart-stable", adapterproto.MethodDescribe, map[string]any{}); !errors.Is(err, ErrAdapterRestartRejected) {
			t.Fatalf("rejected adapter call error=%v", err)
		}
	})

	t.Run("candidate cannot be restarted", func(t *testing.T) {
		dir := t.TempDir()
		writeAdapter(t, dir, binaryPrefix+"candidate", "badjson", "candidate", true)
		host, err := Discover(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		if _, err := host.Restart(context.Background(), "candidate"); !errors.Is(err, ErrAdapterUnavailable) {
			t.Fatalf("candidate restart error=%v", err)
		}
	})
}

func TestDisableConflictsWithExecutingAdapterOperation(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"busy", "hangresolve", "busy", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		_, callErr := host.call(ctx, "busy", adapterproto.MethodResolve, map[string]any{})
		callDone <- callErr
	}()
	e := host.entries["busy"]
	deadline := time.Now().Add(3 * time.Second)
	for {
		if !e.opMu.TryLock() {
			break
		}
		e.opMu.Unlock()
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("adapter did not begin the operation")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := host.SetEnabled("busy", false); !errors.Is(err, ErrAdapterBusy) {
		cancel()
		t.Fatalf("disable while request active error=%v", err)
	}
	if got, err := host.Get("busy"); err != nil || got.Status.State != "ready" {
		cancel()
		t.Fatalf("busy adapter changed state after conflict: %#v err=%v", got.Status, err)
	}
	cancel()
	select {
	case <-callDone:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled adapter call did not finish")
	}
}
