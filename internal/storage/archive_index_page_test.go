package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
)

type archiveIndexReadCounts struct {
	formatProbes     int
	rootReads        int
	sidecarReads     int
	mediaPageReads   int
	statCalls        int
	payloadStats     int
	listRequests     int
	payloadOpenCalls int
}

type archiveIndexCountingBackend struct {
	StorageBackend
	local *LocalFilesystemBackend
	mu    sync.Mutex
	count archiveIndexReadCounts
}

func (b *archiveIndexCountingBackend) LoadRecordingContext(ctx context.Context, id string) (*domain.Recording, error) {
	b.mu.Lock()
	b.count.rootReads++
	b.mu.Unlock()
	return b.local.LoadRecordingContext(ctx, id)
}

func (b *archiveIndexCountingBackend) RecordingFormatVersion(ctx context.Context, id string) (int, error) {
	b.mu.Lock()
	b.count.formatProbes++
	b.mu.Unlock()
	return b.local.RecordingFormatVersion(ctx, id)
}

func (b *archiveIndexCountingBackend) LoadSidecarContext(ctx context.Context, id, relative string, maxBytes int64, output any) error {
	b.mu.Lock()
	b.count.sidecarReads++
	if strings.HasPrefix(relative, "archive/v2/media/") {
		b.count.mediaPageReads++
	}
	b.mu.Unlock()
	return b.local.LoadSidecarContext(ctx, id, relative, maxBytes, output)
}

func (b *archiveIndexCountingBackend) StatPayload(id, relative string) (ObjectInfo, error) {
	b.mu.Lock()
	b.count.statCalls++
	if relative != "recording.json" {
		b.count.payloadStats++
	}
	b.mu.Unlock()
	return b.local.StatPayload(id, relative)
}

func (b *archiveIndexCountingBackend) OpenPayloadReader(id, relative string) (io.ReadCloser, error) {
	b.mu.Lock()
	b.count.payloadOpenCalls++
	b.mu.Unlock()
	return b.local.OpenPayloadReader(id, relative)
}

func (b *archiveIndexCountingBackend) counts() archiveIndexReadCounts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

func TestShardedArchiveIndexPageHasStableCursorAndInvalidatesOnMutation(t *testing.T) {
	ctx := context.Background()
	store, header := newShardedArchiveTestStore(t, strings.Repeat("e", 32))
	seedArchiveIndexMediaPages(t, store, header, 137)

	var all []ArchiveEntry
	cursor := ""
	for {
		page, err := store.ShardedArchiveIndexPage(ctx, header.ID, cursor, 50)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Entries...)
		if !page.HasMore {
			if page.NextCursor != "" {
				t.Fatalf("terminal page cursor=%q", page.NextCursor)
			}
			break
		}
		if page.NextCursor == "" {
			t.Fatal("non-terminal page omitted cursor")
		}
		cursor = page.NextCursor
	}
	if len(all) != 138 || all[0].Path != "recording.json" {
		t.Fatalf("entry count/order first=%+v count=%d", all[0], len(all))
	}
	seen := make(map[string]bool, len(all))
	for index, entry := range all {
		if seen[entry.Path] {
			t.Fatalf("duplicate entry at %d: %s", index, entry.Path)
		}
		seen[entry.Path] = true
		if index > 0 {
			want := fmt.Sprintf("tracks/main/objects/seg-%020d/original.ts", index)
			if entry.Path != want {
				t.Fatalf("entry %d path=%q want=%q", index, entry.Path, want)
			}
		}
	}

	first, err := store.ShardedArchiveIndexPage(ctx, header.ID, "", 1)
	if err != nil || !first.HasMore {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	title := "revision changed"
	if err := store.AppendShardedMetadata(ctx, header.ID, domain.MetadataRevision{ObservedAt: header.CreatedAt.Add(1), Title: &title}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ShardedArchiveIndexPage(ctx, header.ID, first.NextCursor, 1); err != ErrArchiveIndexStaleCursor {
		t.Fatalf("cursor after archive mutation err=%v, want stale cursor", err)
	}
	if _, err := store.ShardedArchiveIndexPage(ctx, header.ID, "not-a-cursor", 1); err != ErrArchiveIndexInvalidCursor {
		t.Fatalf("invalid cursor err=%v", err)
	}
}

func TestShardedArchiveIndexPageReadCostIsIndependentOfHistory(t *testing.T) {
	for _, count := range []uint64{1_000, 10_000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, header := newShardedArchiveTestStore(t, strings.Repeat("f", 32))
			seedArchiveIndexMediaPages(t, store, header, count)
			local, ok := store.StorageBackend.(*LocalFilesystemBackend)
			if !ok {
				t.Fatalf("fixture backend type=%T", store.StorageBackend)
			}
			counter := &archiveIndexCountingBackend{StorageBackend: store.StorageBackend, local: local}
			store.StorageBackend = counter
			version, err := store.RecordingFormatVersion(context.Background(), header.ID)
			if err != nil || version != ShardedArchiveFormatVersion {
				t.Fatalf("recording format version=%d err=%v", version, err)
			}
			page, err := store.ShardedArchiveIndexPage(context.Background(), header.ID, "", 50)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Entries) != 50 || !page.HasMore {
				t.Fatalf("page len=%d has_more=%t", len(page.Entries), page.HasMore)
			}
			got := counter.counts()
			if got.formatProbes != 1 || got.rootReads != 1 || got.mediaPageReads != 1 || got.sidecarReads != 1 || got.statCalls != 1 || got.payloadStats != 0 || got.listRequests != 0 || got.payloadOpenCalls != 0 {
				t.Fatalf("history=%d page read counts=%+v, want one bounded format probe, one page root, one media page, one root stat, and no payload/list operations", count, got)
			}
		})
	}
}

func seedArchiveIndexMediaPages(t *testing.T, store *Store, header *domain.Recording, count uint64) {
	t.Helper()
	const trackID = "main"
	page := v2MediaPage{Version: 1, TrackID: trackID}
	for ordinal := uint64(1); ordinal <= count; ordinal++ {
		pageNo, offset := mediaPageSlot(ordinal)
		if page.Number != pageNo {
			if len(page.Entries) != 0 {
				t.Fatalf("fixture page transition with pending entries: page=%d offset=%d entries=%d", page.Number, offset, len(page.Entries))
			}
			page.Number = pageNo
		}
		path := fmt.Sprintf("tracks/main/objects/seg-%020d/original.ts", ordinal)
		coordinate := archiveindex.Coordinate{SessionID: header.SourceSessionID, TrackID: trackID, Sequence: ordinal, Kind: archiveindex.ObjectMedia}
		record := V2MediaRecord{
			Coordinate:   coordinate,
			IndexOrdinal: ordinal,
			Segment: domain.Segment{
				ID: fmt.Sprintf("seg-%020d", ordinal), TrackID: trackID, Sequence: ordinal, ArchiveOrdinal: ordinal, SourceURI: "https://fixture.invalid/live.m3u8",
				StoragePath: path, PayloadSize: 7, SHA256: strings.Repeat("a", 64),
			},
		}
		if err := validateV2Record(header, record); err != nil {
			t.Fatalf("fixture media ordinal %d invalid: %v", ordinal, err)
		}
		page.Entries = append(page.Entries, record)
		if uint64(len(page.Entries)) == mediaShardMaxEntries || ordinal == count {
			if err := store.saveV2JSON(context.Background(), header.ID, v2MediaPagePath(trackID, false, page.Number), page, archiveShardMaxBytes); err != nil {
				t.Fatalf("write media page %d: %v", page.Number, err)
			}
			page = v2MediaPage{Version: 1, TrackID: trackID}
		}
	}
	track := header.Tracks[trackID]
	track.MediaCount = count
	track.MediaHighWater = count
	track.NextArchiveOrdinal = count + 1
	track.PayloadBytes = count * 7
	header.ShardedArchive.MediaCount = count
	header.ShardedArchive.PayloadBytes = count * 7
	header.ArchiveRevision = count
	if err := store.SaveRecordingHeader(context.Background(), header); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(store.root, "recordings", header.ID, "recording.json"))
	if err != nil || info.Size() > archiveRootMaxBytes {
		t.Fatalf("fixture root stat size=%v err=%v", info, err)
	}
}
