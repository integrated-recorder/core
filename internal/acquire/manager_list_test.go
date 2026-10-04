package acquire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestListForManagementRejectsLimitBeforeEntryCloning(t *testing.T) {
	e := &entry{recording: &domain.Recording{ID: "one"}}
	e.mu.Lock()
	m := &Manager{entries: map[string]*entry{"one": e}}

	done := make(chan error, 1)
	go func() {
		rows, err := m.ListForManagement(context.Background(), 0)
		if len(rows) != 0 && err == nil {
			err = errors.New("oversized snapshot returned rows")
		}
		done <- err
	}()

	select {
	case err := <-done:
		e.mu.Unlock()
		if !errors.Is(err, ErrListLimit) {
			t.Fatalf("ListForManagement error = %v, want ErrListLimit", err)
		}
	case <-time.After(time.Second):
		e.mu.Unlock()
		<-done
		t.Fatal("ListForManagement tried to clone an entry before rejecting the limit")
	}
}

func TestListForManagementObservesCancellationWhileWaitingForEntry(t *testing.T) {
	e := &entry{recording: &domain.Recording{ID: "one"}}
	e.mu.Lock()
	defer e.mu.Unlock()
	m := &Manager{entries: map[string]*entry{"one": e}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	rows, err := m.ListForManagement(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ListForManagement error = %v, want deadline exceeded", err)
	}
	if rows != nil {
		t.Fatalf("canceled snapshot returned partial rows: %#v", rows)
	}
}

func TestListForManagementPreservesNewestFirstOrdering(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := &Manager{entries: map[string]*entry{
		"older": {recording: &domain.Recording{ID: "older", CreatedAt: base}},
		"newer": {recording: &domain.Recording{ID: "newer", CreatedAt: base.Add(time.Minute)}},
	}}
	rows, err := m.ListForManagement(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != "newer" || rows[1].ID != "older" {
		t.Fatalf("management snapshot order = %#v", rows)
	}
}
