package storage

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
)

func TestV2SteadyStateLiveCommitUsesFourMetadataPuts(t *testing.T) {
	ctx := context.Background()
	objects := newMemoryPhysicalObjects()
	store, err := NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	const id = "c4e2f247ab144c6a850fd5c07fb1e6a4"
	header := v2ScaleHeader(id)
	header.Tracks["main"].LivePresentation = &domain.LivePresentationState{NextOrdinal: 1}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}

	appendLive := func(sequence, ordinal uint64) string {
		t.Helper()
		payload := []byte(fmt.Sprintf("live-payload-%d", ordinal))
		payloadPath := fmt.Sprintf("tracks/main/objects/live-%d.ts", ordinal)
		result, err := store.StorageBackend.SavePayloadExact(id, payloadPath, bytes.NewReader(payload), 1024, int64(len(payload)))
		if err != nil {
			t.Fatalf("save live payload %d: %v", ordinal, err)
		}
		coordinate := archiveindex.Coordinate{
			SessionID: header.SourceSessionID, TrackID: "main", Sequence: sequence,
			Kind: archiveindex.ObjectMedia,
		}
		segmentID, err := archiveindex.SegmentIdentity(coordinate)
		if err != nil {
			t.Fatal(err)
		}
		record := v2ScaleMediaRecord(coordinate, ordinal, payloadPath, result.Size, result.SHA256)
		record.Segment.LivePresentationOrdinal = ordinal
		record.SelectedClaim = &archiveindex.Claim{
			ID:     "live_origin:" + segmentID + ":" + result.SHA256,
			Source: archiveindex.ClaimLiveOrigin, AcquiredAt: time.Date(2026, 10, 9, 1, 0, int(ordinal), 0, time.UTC),
			PayloadPath: payloadPath, Size: result.Size, SHA256: result.SHA256,
			Verification: archiveindex.VerificationVerified, Disposition: archiveindex.DispositionAccepted,
		}
		record.ClaimState = archiveindex.CoveragePresent
		root, err := store.LoadRecordingHeader(ctx, id)
		if err != nil {
			t.Fatalf("load root for live commit %d: %v", ordinal, err)
		}
		root.ArchiveRevision++
		root.TimelineRevision++
		if err := store.PublishShardedMedia(ctx, root, record); err != nil {
			t.Fatalf("publish live media %d: %v", ordinal, err)
		}
		return payloadPath
	}

	// First commit initializes all bounded page families. Second commit updates
	// those existing pages and therefore exercises steady-state publication.
	_ = appendLive(1, 1)
	objects.mu.Lock()
	objects.putKeys = nil
	objects.mu.Unlock()
	nextPayloadPath := appendLive(2, 2)

	objects.mu.Lock()
	got := append([]string(nil), objects.putKeys...)
	objects.mu.Unlock()
	want := []string{
		"recordings/" + id + "/" + nextPayloadPath,
		"recordings/" + id + "/" + v2MediaPagePath("main", false, 0) + ".json",
		"recordings/" + id + "/" + v2TimelinePagePath(archiveindex.Coordinate{
			SessionID: header.SourceSessionID, TrackID: "main", Sequence: 2, Kind: archiveindex.ObjectMedia,
		}) + ".json",
		"recordings/" + id + "/" + v2LivePagePath("main", 0) + ".json",
		"recordings/" + id + "/recording.json",
	}
	if !reflect.DeepEqual(got, want) {
		sortedGot, sortedWant := append([]string(nil), got...), append([]string(nil), want...)
		sort.Strings(sortedGot)
		sort.Strings(sortedWant)
		if !reflect.DeepEqual(sortedGot, sortedWant) {
			t.Fatalf("steady-state Put keys = %v, want exactly payload + media page + timeline page + live page + root = %v", got, want)
		}
	}
	if len(got) != 5 {
		t.Fatalf("steady-state Put count = %d, want payload 1 + metadata 4: %v", len(got), got)
	}
	for _, key := range got {
		if strings.Contains(key, "/archive/v2/coordinate/") || strings.Contains(key, "/archive/v2/claims/") {
			t.Fatalf("steady-state commit wrote standalone coordinate/claim object: %q", key)
		}
	}
}
