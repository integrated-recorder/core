package main

import (
	"context"
	"testing"
	"time"
)

type leaseExpiryRecorder struct{ passes chan time.Time }

func (r leaseExpiryRecorder) Expire(ctx context.Context, at time.Time) ([]string, error) {
	select {
	case r.passes <- at:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestControlReplicaLeaseExpiryLoopStopsAndRestartsAcrossHandoff(t *testing.T) {
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	recorder := leaseExpiryRecorder{passes: make(chan time.Time, 2)}
	app := &controlApplication{replicaLeases: recorder, lifetime: lifetime}

	if err := app.startReplicaLeaseExpiryLoop(lifetime); err != nil {
		t.Fatal(err)
	}
	if err := app.startReplicaLeaseExpiryLoop(lifetime); err != nil {
		t.Fatalf("duplicate start was not idempotent: %v", err)
	}
	select {
	case <-recorder.passes:
	case <-time.After(time.Second):
		t.Fatal("startup lease reconciliation did not run")
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()
	if err := app.stopReplicaLeaseExpiryLoop(stopCtx); err != nil {
		t.Fatalf("stop expiry loop for handoff: %v", err)
	}
	if app.leaseExpiryCancel != nil || app.leaseExpiryDone != nil {
		t.Fatal("expiry loop retained its lifecycle handles after stop")
	}
	if err := app.startReplicaLeaseExpiryLoop(lifetime); err != nil {
		t.Fatalf("restart expiry loop after handoff: %v", err)
	}
	select {
	case <-recorder.passes:
	case <-time.After(time.Second):
		t.Fatal("lease reconciliation did not resume after handoff")
	}
	cancelLifetime()
	if err := app.stopReplicaLeaseExpiryLoop(stopCtx); err != nil {
		t.Fatalf("join expiry loop on shutdown: %v", err)
	}
}
