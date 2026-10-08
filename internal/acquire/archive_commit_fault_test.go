package acquire

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

type canonicalCommitFaultBackend struct {
	storage.StorageBackend
	target string
	after  bool
	fired  atomic.Bool
	cause  error
}

func (b *canonicalCommitFaultBackend) fail(target string, operation func() error) error {
	if target != b.target || !b.fired.CompareAndSwap(false, true) {
		return operation()
	}
	if b.after {
		if err := operation(); err != nil {
			return err
		}
	}
	return b.cause
}

func (b *canonicalCommitFaultBackend) SavePayloadExact(id, relativePath string, reader io.Reader, expectedSize, maxBytes int64) (storage.PayloadResult, error) {
	var result storage.PayloadResult
	err := b.fail("payload", func() error {
		var err error
		result, err = b.StorageBackend.SavePayloadExact(id, relativePath, reader, expectedSize, maxBytes)
		return err
	})
	return result, err
}

func (b *canonicalCommitFaultBackend) SaveSidecar(id, relativePath string, value any) error {
	target := ""
	switch {
	case strings.HasPrefix(relativePath, "archive-index/claims/"):
		target = "claim_shard"
	case relativePath == archiveIndexManifestPath:
		target = "index_manifest"
	case strings.HasPrefix(relativePath, "tracks/"):
		target = "segment_sidecar"
	}
	return b.fail(target, func() error { return b.StorageBackend.SaveSidecar(id, relativePath, value) })
}

func (b *canonicalCommitFaultBackend) SaveRecording(recording *domain.Recording) error {
	return b.fail("root", func() error { return b.StorageBackend.SaveRecording(recording) })
}

func (b *canonicalCommitFaultBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	reader, ok := b.StorageBackend.(interface {
		LoadSidecar(string, string, int64, any) error
	})
	if !ok {
		return storage.ErrSidecarReadUnsupported
	}
	return reader.LoadSidecar(id, relativePath, maxBytes, output)
}

func TestCanonicalCommitFaultPointsRemainRetryableAndRootSafe(t *testing.T) {
	for _, target := range []string{"payload", "segment_sidecar", "claim_shard", "index_manifest", "root"} {
		for _, objectKind := range []string{"media", "init"} {
			for _, after := range []bool{false, true} {
				name := target + "/" + objectKind + "/before_apply"
				if after {
					name = target + "/" + objectKind + "/applied_response_lost"
				}
				t.Run(name, func(t *testing.T) {
					store, err := storage.New(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					media := repairMediaContextForTest()
					root := newRepairTestRoot(t, store, domain.StateCompleted, media)
					cause := errors.New("injected canonical step failure")
					backend := &canonicalCommitFaultBackend{StorageBackend: store.StorageBackend, target: target, after: after, cause: cause}
					store.StorageBackend = backend
					fence := &repairOwnerFence{}
					manager, entry, closeManager := newRepairTestManager(t, store, &repairFixtureTransport{manifest: repairManifestFixture()}, root, fence, nil)
					segment := domain.Segment{
						TrackID: "main", Sequence: 41, SourceEpoch: 0, DiscontinuitySequence: 9,
						SourceURI: "https://media.example/archive/fault-41.ts", Duration: 3,
						IsInit: objectKind == "init",
					}
					if segment.IsInit {
						segment.SourceURI = "https://media.example/archive/fault-init.m4v"
						segment.ID = initSegmentID(hls.Map{URI: segment.SourceURI}, segment.SourceEpoch, segment.DiscontinuitySequence)
					}
					payload := []byte("canonical-byte-payload")
					owner := ownerForRepair(root.ID, 1)
					if _, _, err := manager.commitArchiveSegmentOwned(entry, &owner, segment, archiveindex.ClaimHistorical, payload, true); !errors.Is(err, cause) {
						t.Fatalf("first commit error = %v, want injected %s failure", err, target)
					}
					if !backend.fired.Load() {
						t.Fatalf("fault point %q was not reached", target)
					}
					if err := manager.releaseRepairOwner(context.Background(), entry, owner); err != nil {
						t.Fatal(err)
					}
					if err := closeManager(); err != nil {
						t.Fatal(err)
					}

					loaded, err := store.LoadRecordingReadOnly(root.ID)
					if err != nil {
						t.Fatal(err)
					}
					if track := loaded.Tracks["main"]; track != nil {
						selectedSegments := track.Segments
						if segment.IsInit {
							selectedSegments = track.InitSegments
						}
						for _, selected := range selectedSegments {
							if selected.Sequence == segment.Sequence {
								if _, statErr := store.StatPayload(root.ID, selected.StoragePath); statErr != nil {
									t.Fatalf("root selected absent payload %q: %v", selected.StoragePath, statErr)
								}
								if integrity := store.VerifyRecording(loaded); integrity.Status == storage.IntegrityFailed || integrity.ObjectsMissing != 0 || integrity.ObjectsCorrupt != 0 {
									t.Fatalf("root-selected payload failed integrity check: %+v", integrity)
								}
							}
						}
					}

					manager2, entry2, closeManager2 := newRepairTestManager(t, store, &repairFixtureTransport{manifest: repairManifestFixture()}, loaded, &repairOwnerFence{last: 1}, nil)
					owner2 := ownerForRepair(root.ID, 2)
					if _, _, err := manager2.commitArchiveSegmentOwned(entry2, &owner2, segment, archiveindex.ClaimHistorical, payload, true); err != nil {
						t.Fatalf("same claim retry after %s failure: %v", target, err)
					}
					if err := manager2.releaseRepairOwner(context.Background(), entry2, owner2); err != nil {
						t.Fatal(err)
					}
					if err := closeManager2(); err != nil {
						t.Fatal(err)
					}
					final, err := store.LoadRecordingReadOnly(root.ID)
					if err != nil {
						t.Fatal(err)
					}
					var selected domain.Segment
					selectedSegments := final.Tracks["main"].Segments
					if segment.IsInit {
						selectedSegments = final.Tracks["main"].InitSegments
					}
					for _, candidate := range selectedSegments {
						if candidate.Sequence == segment.Sequence {
							selected = candidate
							break
						}
					}
					if selected.ID == "" {
						t.Fatal("retry did not publish selected payload")
					}
					if selected.PayloadSize != int64(len(payload)) || selected.SHA256 != sha256Hex(payload) {
						t.Fatalf("retry changed immutable identity: size=%d hash=%q", selected.PayloadSize, selected.SHA256)
					}
					assertStoredPayload(t, store, root.ID, selected.StoragePath, payload)
				})
			}
		}
	}
}

func TestRepeatedLegacyInitIdentityIsNotPersistedAsSupplementalClaim(t *testing.T) {
	recording := &domain.Recording{
		ID: "legacy-init-repeat", AdapterID: "fixture", SourceSessionID: "session-" + strings.Repeat("c", 64),
		State: domain.StateRecording, StartedAt: time.Now().UTC(),
		Tracks: map[string]*domain.Track{"main": {
			ID: "main",
			InitSegments: []domain.Segment{
				{ID: "init-shared", TrackID: "main", Sequence: 10, StoragePath: "tracks/main/init-a.mp4", PayloadSize: 4, SHA256: strings.Repeat("a", 64), IsInit: true},
				{ID: "init-shared", TrackID: "main", Sequence: 11, StoragePath: "tracks/main/init-b.mp4", PayloadSize: 4, SHA256: strings.Repeat("a", 64), IsInit: true},
			},
		}},
	}
	inventory, err := archiveindex.FromRecording(recording, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, segment := range inventory.Segments {
		_, encoded, err := archiveClaimShardForInventory(inventory, segment.ID, segment.Coordinate, "init-shared")
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != 0 {
			t.Fatal("synthetic root-adoption claim was persisted as source provenance")
		}
	}
}
