package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestLoadManagementPageUsesBoundedV2RootCursor(t *testing.T) {
	backends := []struct {
		name  string
		store func(*testing.T) *Store
	}{
		{
			name: "local",
			store: func(t *testing.T) *Store {
				store, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
		},
		{
			name: "object",
			store: func(t *testing.T) *Store {
				store, err := NewWithObjectStore(t.TempDir(), newMemoryPhysicalObjects())
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
		},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.store(t)
			ids := []string{
				"0123456789abcdef0123456789abcdef",
				"1123456789abcdef0123456789abcdef",
				"2123456789abcdef0123456789abcdef",
			}
			for index, id := range ids {
				root := &domain.Recording{
					FormatVersion: ShardedArchiveFormatVersion,
					ID:            id,
					CreatedAt:     time.Unix(int64(index), 0).UTC(),
					Tracks: map[string]*domain.Track{
						"main": {ID: "main", MediaCount: 100_000, MediaHighWater: 100_000},
					},
					ShardedArchive: &domain.ShardedArchiveSummary{MediaCount: 100_000, PayloadBytes: 1},
				}
				if err := store.CreateShardedRecording(root); err != nil {
					t.Fatalf("create root %s: %v", id, err)
				}
			}

			first, err := store.LoadManagementPageContext(context.Background(), "", 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Items) != 2 || first.NextCursor != ids[1] {
				t.Fatalf("first page = (%d rows, cursor %q), want (2, %q)", len(first.Items), first.NextCursor, ids[1])
			}
			for index, row := range first.Items {
				if row.ID != ids[index] || row.SegmentCount() != 100_000 || len(row.Tracks["main"].Segments) != 0 {
					t.Fatalf("first page row %d is not bounded V2 summary: %#v", index, row)
				}
			}
			last, err := store.LoadManagementPageContext(context.Background(), first.NextCursor, 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(last.Items) != 1 || last.Items[0].ID != ids[2] || last.NextCursor != "" {
				t.Fatalf("last page = %#v, want final ID %q and no cursor", last, ids[2])
			}
		})
	}
}

func TestLoadManagementPageRejectsUnboundedRequest(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadManagementPageContext(context.Background(), "", ManagementPageLimit+1); err == nil {
		t.Fatal("management page accepted limit above fixed page bound")
	}
	if _, err := store.LoadManagementPageContext(context.Background(), fmt.Sprintf("%032x", 1), ManagementPageLimit); err != nil {
		t.Fatalf("valid ID cursor rejected: %v", err)
	}
}

func TestObjectManagementPageWarmCacheCopiesOnlyBoundedPage(t *testing.T) {
	const recordingCount = 10_025
	backend := &ObjectStoreArchiveBackend{
		objects:       newMemoryPhysicalObjects(),
		rootIDsLoaded: true,
		rootIDList:    make([]string, recordingCount),
	}
	for index := range backend.rootIDList {
		backend.rootIDList[index] = fmt.Sprintf("%032x", index+1)
	}
	var fullSnapshotCopied int
	backend.onRootIDSnapshotClone = func(copied int) { fullSnapshotCopied += copied }

	first, next, err := backend.managementRootIDsPageContext(context.Background(), "", ManagementPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != ManagementPageLimit || next != backend.rootIDList[ManagementPageLimit-1] {
		t.Fatalf("first page len=%d next=%q", len(first), next)
	}
	if fullSnapshotCopied != 0 {
		t.Fatalf("warm page copied %d full-index IDs, want 0", fullSnapshotCopied)
	}

	second, _, err := backend.managementRootIDsPageContext(context.Background(), next, ManagementPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != ManagementPageLimit || second[0] != backend.rootIDList[ManagementPageLimit] {
		t.Fatalf("second page did not continue from cursor: len=%d first=%q", len(second), second[0])
	}
	if fullSnapshotCopied != 0 {
		t.Fatalf("warm page traversal copied %d full-index IDs, want 0", fullSnapshotCopied)
	}
}
