package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

func TestMetadataTimelinePersistsInCanonicalRootAndLegacyRootLoads(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	title, description := "source title", ""
	recording := &domain.Recording{FormatVersion: 1, ID: id, Title: "recording title", State: domain.StateCompleted, Tracks: map[string]*domain.Track{}, MetadataTimeline: []domain.MetadataRevision{{ObservedAt: time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC), Title: &title, Description: &description}}}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	rootJSON, err := os.ReadFile(filepath.Join(root, "recordings", id, "recording.json"))
	if err != nil || !json.Valid(rootJSON) {
		t.Fatalf("canonical root read/JSON valid=%v err=%v", json.Valid(rootJSON), err)
	}
	index, err := store.ArchiveIndex(recording)
	if err != nil || len(index) == 0 || index[0].Path != "recording.json" {
		t.Fatalf("metadata root missing from archive index: %#v err=%v", index, err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || len(loaded[0].MetadataTimeline) != 1 || loaded[0].MetadataTimeline[0].Description == nil || *loaded[0].MetadataTimeline[0].Description != "" {
		t.Fatalf("canonical timeline reload=%#v err=%v", loaded, err)
	}

	const oldID = "1234567890abcdef1234567890abcdef"
	if err := store.NewRecordingDir(oldID); err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"format_version":1,"id":"` + oldID + `","state":"completed","tracks":{}}`)
	if err := os.WriteFile(filepath.Join(root, "recordings", oldID, "recording.json"), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.LoadAll()
	if err != nil || len(loaded) != 2 {
		t.Fatalf("old recording without metadata timeline did not load: count=%d err=%v", len(loaded), err)
	}
}

func TestInvalidMetadataTimelineIsDroppedWithoutDiscardingMediaArchive(t *testing.T) {
	root := t.TempDir()
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	if err := store.NewRecordingDir(id); err != nil {
		t.Fatal(err)
	}
	bad := "bad\x00title"
	recording := &domain.Recording{FormatVersion: 1, ID: id, Title: "still available", State: domain.StateCompleted, Tracks: map[string]*domain.Track{}, MetadataTimeline: []domain.MetadataRevision{{ObservedAt: time.Now().UTC(), Title: &bad}}}
	data, err := json.Marshal(recording)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "recordings", id, "recording.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAll()
	if err != nil || len(loaded) != 1 || loaded[0].Title != "still available" || len(loaded[0].MetadataTimeline) != 0 || !loaded[0].MetadataTimelineTruncated {
		t.Fatalf("invalid source timeline damaged recording: %#v err=%v", loaded, err)
	}
	issues := store.RecoveryIssues()
	found := false
	for _, issue := range issues {
		if issue.Code == "metadata_timeline_invalid" && issue.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("invalid timeline did not produce safe recovery issue: %#v", issues)
	}
}
