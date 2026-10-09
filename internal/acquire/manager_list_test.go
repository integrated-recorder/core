package acquire

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestGetManagementHeaderReadsBoundedV2Root(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	root := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion,
		ID:            id,
		State:         domain.StateRecording,
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", MediaCount: 100_000, MediaHighWater: 100_000},
		},
		ShardedArchive: &domain.ShardedArchiveSummary{
			MediaCount: 100_000, DurationSeconds: 100_000, PayloadBytes: 188_000_000,
		},
	}
	if err := store.CreateShardedRecording(root); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{store: store, entries: map[string]*entry{id: {recording: root}}}

	header, err := manager.GetManagementHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if header.FormatVersion != storage.ShardedArchiveFormatVersion || header.State != domain.StateRecording || header.SegmentCount() != 100_000 {
		t.Fatalf("bounded V2 header summary = %#v", header)
	}
	if len(header.Tracks["main"].Segments) != 0 || len(header.Tracks["main"].InitSegments) != 0 {
		t.Fatalf("management header materialized media history: %#v", header.Tracks["main"])
	}
}

func TestListForManagementDoesNotCloneV2ArchiveHistory(t *testing.T) {
	const id = "1123456789abcdef0123456789abcdef"
	root := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion,
		ID:            id,
		State:         domain.StateRecording,
		ShardedArchive: &domain.ShardedArchiveSummary{
			MediaCount:   100_000,
			PayloadBytes: 188_000_000,
		},
		Tracks: map[string]*domain.Track{
			"main": {
				ID:           "main",
				MediaCount:   100_000,
				Segments:     []domain.Segment{{Duration: math.NaN()}},
				InitSegments: []domain.Segment{{ID: "init"}},
			},
		},
	}
	e := &entry{recording: root}
	m := &Manager{entries: map[string]*entry{id: e}}

	rows, err := m.ListForManagement(context.Background(), 1)
	if err != nil {
		t.Fatalf("ListForManagement cloned V2 archive history: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != id || rows[0].SegmentCount() != 100_000 {
		t.Fatalf("bounded list snapshot = %#v", rows)
	}
	track := rows[0].Tracks["main"]
	if track == nil || track.MediaCount != 100_000 || len(track.Segments) != 0 || len(track.InitSegments) != 0 {
		t.Fatalf("V2 list snapshot did not retain only bounded track summary: %#v", track)
	}
	track.MediaCount = 1
	rows[0].ShardedArchive.MediaCount = 1
	if root.Tracks["main"].MediaCount != 100_000 || root.ShardedArchive.MediaCount != 100_000 {
		t.Fatal("management snapshot aliases mutable V2 root summary")
	}
}

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
