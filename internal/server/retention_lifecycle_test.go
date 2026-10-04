package server

import (
	"context"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/controlplane"
)

func TestScheduledRetentionWaitsAcrossControlFenceAndResumes(t *testing.T) {
	gate := controlplane.NewMutationGate()
	s := &Server{backgroundMutationGate: gate, retentionGate: make(chan struct{}, 1)}
	s.retentionGate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- s.runScheduledRetentionPassWithAdmission(ctx) }()
	select {
	case <-done:
		t.Fatal("scheduled retention ran while Control was passive")
	case <-time.After(20 * time.Millisecond):
	}
	gate.Activate()
	select {
	case continued := <-done:
		if !continued {
			t.Fatal("retention did not resume after Control activation")
		}
	case <-ctx.Done():
		t.Fatal("retention remained blocked after Control activation")
	}
	gate.Fence()
	done = make(chan bool, 1)
	go func() { done <- s.runScheduledRetentionPassWithAdmission(ctx) }()
	select {
	case <-done:
		t.Fatal("scheduled retention ignored the Control handoff fence")
	case <-time.After(20 * time.Millisecond):
	}
	gate.Activate()
	select {
	case continued := <-done:
		if !continued {
			t.Fatal("retention did not resume after handoff rollback")
		}
	case <-ctx.Done():
		t.Fatal("retention remained blocked after handoff rollback")
	}
}
