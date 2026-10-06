package recorderengine

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

const automaticRecoveryOwnerTestID = "c0ffee00000000000000000000000001"

type automaticRecoveryOwnerTestContext struct {
	SchemaVersion int                      `json:"schema_version"`
	Media         adapterproto.MediaSource `json:"media"`
}

type invalidatingRecoveryOwnerClient struct {
	mu          sync.Mutex
	owners      *recordingowner.Store
	archive     *storage.Store
	recordingID string
	claims      []recordingowner.Owner
	releases    []recordingowner.Owner
}

func (c *invalidatingRecoveryOwnerClient) ClaimRecording(_ context.Context, recordingID string) (recordingowner.Owner, error) {
	owner, err := c.owners.Claim(recordingID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		return recordingowner.Owner{}, err
	}
	if recordingID == c.recordingID {
		if err := c.archive.SaveSidecar(recordingID, "archive-index/acquisition-context", map[string]any{"schema_version": 99}); err != nil {
			_ = c.owners.Release(owner)
			return recordingowner.Owner{}, err
		}
	}
	c.mu.Lock()
	c.claims = append(c.claims, owner)
	c.mu.Unlock()
	return owner, nil
}

func (c *invalidatingRecoveryOwnerClient) ReleaseRecording(_ context.Context, owner recordingowner.Owner) error {
	if err := c.owners.Release(owner); err != nil {
		return err
	}
	c.mu.Lock()
	c.releases = append(c.releases, owner)
	c.mu.Unlock()
	return nil
}

func (c *invalidatingRecoveryOwnerClient) snapshot() (claims, releases []recordingowner.Owner) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]recordingowner.Owner(nil), c.claims...), append([]recordingowner.Owner(nil), c.releases...)
}

type unexpectedRecoveryTransport struct{}

func (unexpectedRecoveryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected HTTP request")
}

func TestAutomaticRecoveryReleasesUnadoptedTerminalOwner(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	archive, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	stoppedAt := now
	root := &domain.Recording{
		FormatVersion: 1, ID: automaticRecoveryOwnerTestID, AdapterID: "fixture",
		State: domain.StateCompleted, CreatedAt: now, StartedAt: now, StoppedAt: &stoppedAt,
		Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}, InitSegments: []domain.Segment{}}},
	}
	if err := archive.CreateRecording(root); err != nil {
		t.Fatal(err)
	}
	media := adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://fixture.example/live.m3u8",
		HistoricalAvailability: &adapterproto.HistoricalAvailability{
			Mode:                  adapterproto.HistoricalModeSequenceRanges,
			SequenceRanges:        []adapterproto.HistoricalSequenceRange{{Start: 1, End: 1}},
			HistoricalManifestURL: "https://fixture.example/history.m3u8",
		},
	}
	if err := archive.SaveSidecar(automaticRecoveryOwnerTestID, "archive-index/acquisition-context", automaticRecoveryOwnerTestContext{SchemaVersion: 1, Media: media}); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithFencedRecovery(
		archive,
		&http.Client{Transport: unexpectedRecoveryTransport{}},
		nil,
		func(context.Context, string) error { return nil },
		ownerStore,
		ownerStore,
	)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(manager, &adapterhost.Host{}, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	ownerClient := &invalidatingRecoveryOwnerClient{owners: ownerStore, archive: archive, recordingID: automaticRecoveryOwnerTestID}
	if err := engine.ConfigureRecordingOwnerClient(ownerClient); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := engine.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close Engine: %v", err)
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		claims, releases := ownerClient.snapshot()
		if len(claims) == 1 && len(releases) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	claims, releases := ownerClient.snapshot()
	if len(claims) != 1 || len(releases) != 1 || claims[0] != releases[0] {
		t.Fatalf("unadopted terminal owner was not released: claims=%+v releases=%+v", claims, releases)
	}
	if _, err := ownerStore.Current(automaticRecoveryOwnerTestID); !errors.Is(err, recordingowner.ErrNotFound) {
		t.Fatalf("Host owner remains after pre-adoption pass failure: %v", err)
	}
	engine.ownerMu.Lock()
	_, engineRetainsOwner := engine.owners[automaticRecoveryOwnerTestID]
	engine.ownerMu.Unlock()
	if engineRetainsOwner {
		t.Fatal("Engine retained callback owner after release")
	}

	// A second callback claim for the same archive must succeed. A stale Engine
	// owners-map entry would make claimAutomaticRecoveryOwner reject this token.
	second, err := engine.claimAutomaticRecoveryOwner(context.Background(), automaticRecoveryOwnerTestID)
	if err != nil {
		t.Fatalf("Engine recovery owner callback could not claim again: %v", err)
	}
	if second == claims[0] {
		t.Fatal("second Host claim reused released owner token")
	}
	if err := engine.releaseOwner(context.Background(), second); err != nil {
		t.Fatalf("release second owner claim: %v", err)
	}
}
