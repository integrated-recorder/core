package acquire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestLifecycleSnapshotLoadsBoundedV2RootWithoutEntry(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion:    storage.ShardedArchiveFormatVersion,
		ID:               "feebdaedfeebdaedfeebdaedfeebdaed",
		State:            domain.StateCompleted,
		CreatedAt:        now,
		StartedAt:        now,
		TimelineRevision: 7,
		ArchiveRevision:  11,
		ShardedArchive:   &domain.ShardedArchiveSummary{},
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, nil, nil, nil, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())

	snapshot, err := manager.LifecycleSnapshot(context.Background(), header.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CaptureState != domain.StateCompleted || snapshot.ArchiveSealed || !snapshot.Repairable ||
		snapshot.TimelineRevision != 7 || snapshot.ArchiveRevision != 11 || snapshot.RecoveryState != "idle" {
		t.Fatalf("unexpected lifecycle snapshot: %#v", snapshot)
	}
}

func TestLifecycleSnapshotLoadsSealedV2RootWithoutEntry(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion:  storage.ShardedArchiveFormatVersion,
		ID:             "0123456789abcdef0123456789abcdef",
		State:          domain.StateCompleted,
		ArchiveSealed:  true,
		CreatedAt:      now,
		StartedAt:      now,
		ShardedArchive: &domain.ShardedArchiveSummary{},
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}},
		},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithMode(store, nil, nil, nil, FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())

	snapshot, err := manager.LifecycleSnapshot(context.Background(), header.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.ArchiveSealed || snapshot.Repairable || snapshot.RecoveryState != "sealed" {
		t.Fatalf("unexpected sealed lifecycle snapshot: %#v", snapshot)
	}
}

func TestV2LifecycleSurvivesRestartRepairAndSeal(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "fedcba9876543210fedcba9876543210"
	identity, err := archiveindex.NewSessionIdentity(id, "fixture", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion:   storage.ShardedArchiveFormatVersion,
		ID:              id,
		SourceSessionID: identity.ID,
		AdapterID:       "fixture",
		State:           domain.StateStopped,
		CreatedAt:       now,
		StartedAt:       now,
		StoppedAt:       &now,
		ArchiveRevision: 1,
		ShardedArchive:  &domain.ShardedArchiveSummary{},
		Tracks: map[string]*domain.Track{
			"main": {
				ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{},
				NextArchiveOrdinal: 1, LivePresentation: &domain.LivePresentationState{NextOrdinal: 1},
			},
		},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}

	first, err := NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := first.CompleteRecording(context.Background(), id)
	if err != nil || completed.State != domain.StateCompleted || completed.ArchiveSealed {
		t.Fatalf("V2 stopped-to-completed transition: recording=%#v err=%v", completed, err)
	}
	closeManagerForLifecycleTest(t, first)

	second, err := NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := second.entry(id)
	if !ok {
		t.Fatal("V2 completed recording was not reopened")
	}
	snapshot, err := second.LifecycleSnapshot(context.Background(), id)
	if err != nil || snapshot.CaptureState != domain.StateCompleted || !snapshot.Repairable || snapshot.ArchiveSealed {
		t.Fatalf("reopened V2 archive should remain repairable: snapshot=%#v err=%v", snapshot, err)
	}

	// A terminal, unsealed V2 archive still accepts a canonical historical repair.
	segment := domain.Segment{
		TrackID: "main", Sequence: 10, SourceEpoch: 0, DiscontinuitySequence: 0,
		SourceURI: "https://source.invalid/repaired.ts", Duration: 2,
	}
	if _, _, err := second.commitArchiveSegment(entry, nil, segment, archiveindex.ClaimHistorical, []byte("repaired-original-bytes")); err != nil {
		t.Fatalf("repair of completed unsealed V2 archive: %v", err)
	}
	if err := second.SealArchiveContext(context.Background(), id); err != nil {
		t.Fatalf("seal V2 archive: %v", err)
	}
	sealedHeader, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	sealedRevision := sealedHeader.ArchiveRevision
	blocked := domain.Segment{
		TrackID: "main", Sequence: 11, SourceEpoch: 0, DiscontinuitySequence: 0,
		SourceURI: "https://source.invalid/after-seal.ts", Duration: 2,
	}
	if _, _, err := second.commitArchiveSegment(entry, nil, blocked, archiveindex.ClaimHistorical, []byte("must-not-publish")); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("post-seal canonical mutation error=%v, want ErrArchiveSealed", err)
	}
	if err := second.recordHistoricalCoverage(entry, nil, true, archiveindex.Coordinate{
		SessionID: identity.ID, TrackID: "main", Sequence: 12, Kind: archiveindex.ObjectMedia,
	}, archiveindex.CoverageKnownMissing, "late gap"); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("post-seal coverage mutation error=%v, want ErrArchiveSealed", err)
	}
	currentHeader, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if currentHeader.ArchiveRevision != sealedRevision || currentHeader.ShardedArchive.MediaCount != 1 {
		t.Fatalf("post-seal rejected writes changed canonical root: revision=%d media=%d; want %d/1", currentHeader.ArchiveRevision, currentHeader.ShardedArchive.MediaCount, sealedRevision)
	}
	closeManagerForLifecycleTest(t, second)

	third, err := NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeManagerForLifecycleTest(t, third)
	reopened, err := third.Get(id)
	if err != nil || reopened.FormatVersion != storage.ShardedArchiveFormatVersion || reopened.State != domain.StateCompleted || !reopened.ArchiveSealed {
		t.Fatalf("sealed V2 archive reopen: recording=%#v err=%v", reopened, err)
	}
}

func TestV2SealReconcilesPendingClaimAccountingBeforeSeal(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "ab12cd34ef56ab78cd90ef12ab34cd56"
	identity, err := archiveindex.NewSessionIdentity(id, "fixture", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	header := &domain.Recording{
		FormatVersion: storage.ShardedArchiveFormatVersion, ID: id,
		SourceSessionID: identity.ID, AdapterID: "fixture",
		State: domain.StateStopped, CreatedAt: now, StartedAt: now,
		ShardedArchive: &domain.ShardedArchiveSummary{},
		Tracks: map[string]*domain.Track{
			"main": {ID: "main", NextArchiveOrdinal: 1, LivePresentation: &domain.LivePresentationState{NextOrdinal: 1}},
		},
	}
	if err := store.CreateShardedRecording(header); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeManagerForLifecycleTest(t, manager)

	persisted, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	persisted.ShardedArchive.ClaimReconcilePending = true
	if err := store.SaveRecordingHeader(context.Background(), persisted); err != nil {
		t.Fatal(err)
	}
	e, ok := manager.entry(id)
	if !ok {
		t.Fatal("recording entry not loaded")
	}
	e.mu.Lock()
	e.recording.ShardedArchive.ClaimReconcilePending = true
	e.mu.Unlock()

	if err := manager.SealArchiveContext(context.Background(), id); err != nil {
		t.Fatalf("seal should reconcile safe pending claim accounting first: %v", err)
	}
	sealed, err := store.LoadRecordingHeader(context.Background(), id)
	if err != nil || !sealed.ArchiveSealed || sealed.ShardedArchive.ClaimReconcilePending {
		t.Fatalf("sealed root has unresolved claim accounting: root=%+v err=%v", sealed, err)
	}
}

func closeManagerForLifecycleTest(t *testing.T, manager *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Close(ctx); err != nil {
		t.Errorf("close lifecycle manager: %v", err)
	}
}
