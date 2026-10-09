package acquire

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestListForManagementPageTraversesBeyondTenThousandSummaries(t *testing.T) {
	const count = 10_025
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	manager := &Manager{entries: make(map[string]*entry, count)}
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("%032x", index+1)
		title := "bounded summary"
		if index == count-1 {
			title = "needle beyond boundary"
		}
		recording := &domain.Recording{
			FormatVersion: storage.ShardedArchiveFormatVersion,
			ID:            id,
			Title:         title,
			State:         domain.StateCompleted,
			CreatedAt:     base.Add(time.Duration(index) * time.Second),
			StartedAt:     base.Add(time.Duration(index) * time.Second),
			Tracks: map[string]*domain.Track{
				"main": {ID: "main", MediaCount: uint64(index + 1), MediaHighWater: uint64(index + 1)},
			},
			ShardedArchive: &domain.ShardedArchiveSummary{MediaCount: uint64(index + 1)},
		}
		manager.entries[id] = &entry{recording: recording}
	}

	seen := make(map[string]struct{}, count)
	cursor := ""
	var target *domain.Recording
	for {
		rows, next, err := manager.ListForManagementPage(context.Background(), cursor, storage.ManagementPageLimit)
		if err != nil {
			t.Fatalf("ListForManagementPage after %q: %v", cursor, err)
		}
		for _, row := range rows {
			if row.ID <= cursor {
				t.Fatalf("page returned non-advancing ID %q after %q", row.ID, cursor)
			}
			if _, duplicate := seen[row.ID]; duplicate {
				t.Fatalf("duplicate summary %q", row.ID)
			}
			seen[row.ID] = struct{}{}
			if row.Title == "needle beyond boundary" {
				target = row
			}
		}
		if len(rows) > storage.ManagementPageLimit {
			t.Fatalf("page returned %d rows, bound=%d", len(rows), storage.ManagementPageLimit)
		}
		if next == "" {
			break
		}
		if next <= cursor {
			t.Fatalf("cursor did not advance: %q after %q", next, cursor)
		}
		cursor = next
	}
	if len(seen) != count {
		t.Fatalf("traversed %d summaries, want %d", len(seen), count)
	}
	if target == nil || target.ID != fmt.Sprintf("%032x", count) {
		t.Fatalf("summary beyond first 10k pages missing: %#v", target)
	}
}
