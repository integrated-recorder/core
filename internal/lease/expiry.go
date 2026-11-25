package lease

import (
	"context"
	"time"
)

// Expirer advances durable lease state whose retention deadline has passed.
// Implementations must not interpret expiry as permission to delete archive
// bytes; it ends only the provider's recorded retention responsibility.
type Expirer interface {
	Expire(context.Context, time.Time) ([]string, error)
}

// RunExpiryLoop reconciles lease deadlines once at startup and after every
// interval. Errors are reported without ending the loop so transient storage
// failures can recover on the next pass. The caller owns and joins the loop.
func RunExpiryLoop(ctx context.Context, store Expirer, interval time.Duration, onError func(error)) {
	RunMaintenanceLoop(ctx, store, interval, nil, onError)
}

// MaintenancePass runs after lease expiry in the same joined lifecycle loop.
// It may block on networking without delaying later expiry ticks; passes never
// overlap, and the parent loop cancels and joins the active pass before return.
// This lets durable replica outboxes retry without leaving an unowned goroutine
// across Control generation handoff.
type MaintenancePass func(context.Context, time.Time) error

func RunMaintenanceLoop(ctx context.Context, store Expirer, interval time.Duration, maintenance MaintenancePass, onError func(error)) {
	if ctx == nil || store == nil || interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	runMaintenanceLoop(ctx, store, time.Now, ticker.C, maintenance, onError)
}

func runExpiryLoop(ctx context.Context, store Expirer, now func() time.Time, ticks <-chan time.Time, onError func(error)) {
	runMaintenanceLoop(ctx, store, now, ticks, nil, onError)
}

func runMaintenanceLoop(ctx context.Context, store Expirer, now func() time.Time, ticks <-chan time.Time, maintenance MaintenancePass, onError func(error)) {
	if ctx == nil || store == nil || now == nil || ticks == nil {
		return
	}
	type maintenanceResult struct {
		done chan struct{}
		err  error
	}
	type maintenanceRun struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	results := make(chan maintenanceResult, 1)
	var active *maintenanceRun
	startMaintenance := func(at time.Time) {
		if maintenance == nil || active != nil || ctx.Err() != nil {
			return
		}
		workCtx, cancel := context.WithCancel(ctx)
		run := &maintenanceRun{cancel: cancel, done: make(chan struct{})}
		active = run
		go func() {
			err := maintenance(workCtx, at)
			results <- maintenanceResult{done: run.done, err: err}
			close(run.done)
		}()
	}
	finishMaintenance := func(result maintenanceResult) {
		if active == nil || active.done != result.done {
			return
		}
		<-result.done
		active.cancel()
		active = nil
		if result.err != nil && ctx.Err() == nil && onError != nil {
			onError(result.err)
		}
	}
	expire := func(at time.Time) bool {
		if ctx.Err() != nil {
			return false
		}
		_, err := store.Expire(ctx, at)
		if err != nil && ctx.Err() == nil && onError != nil {
			onError(err)
		}
		if ctx.Err() != nil {
			return false
		}
		startMaintenance(at)
		return true
	}
	if !expire(now().UTC()) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			if active != nil {
				active.cancel()
				<-active.done
			}
			return
		case result := <-results:
			finishMaintenance(result)
		case _, ok := <-ticks:
			if !ok {
				if active != nil {
					active.cancel()
					<-active.done
				}
				return
			}
			// Consume a completed outbox result before deciding whether this tick
			// can start another pass. A slow peer operation never delays durable
			// lease expiry, and outbox passes never overlap.
			select {
			case result := <-results:
				finishMaintenance(result)
			default:
			}
			if !expire(now().UTC()) {
				if active != nil {
					active.cancel()
					<-active.done
				}
				return
			}
		}
	}
}
