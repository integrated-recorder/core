package replication

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestVerifyRekeyHistoryChecksCompleteEncryptedChainAndRecipientEnvelope(t *testing.T) {
	fixture := newRekeyHistoryFixture(t)
	result, err := fixture.store.VerifyRekeyHistory(
		context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
		fixture.previousDEK, fixture.newDEK, fixture.signerPublic, fixture.recipientPublic,
	)
	if err != nil {
		t.Fatalf("verify complete rekey history: %v", err)
	}
	if result.ArchiveID != fixture.archiveID || result.PreviousKeyEpoch != "4" || result.NewKeyEpoch != "5" ||
		result.SnapshotCount != "2" || result.HeadSequence != "2" || result.HeadSnapshotID != fixture.snapshots[1].SnapshotID ||
		len(result.BundleSetCommitment) != len("sha256:")+64 || len(result.EnvelopeSHA256) != len("sha256:")+64 ||
		result.RecipientKeyID != cryptoarchive.RecipientKeyID(fixture.recipientPublic) {
		t.Fatalf("unexpected non-secret verification result: %+v", result)
	}
	again, err := fixture.store.VerifyRekeyHistory(
		context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
		fixture.previousDEK, fixture.newDEK, fixture.signerPublic, fixture.recipientPublic,
	)
	if err != nil || again.BundleSetCommitment != result.BundleSetCommitment || again.EnvelopeSHA256 != result.EnvelopeSHA256 {
		t.Fatalf("verification commitment was not deterministic: first=%+v second=%+v err=%v", result, again, err)
	}
}

func TestVerifyRekeyHistoryFailsClosedForMissingBundleAndWrongRecipient(t *testing.T) {
	t.Run("missing bundle", func(t *testing.T) {
		fixture := newRekeyHistoryFixture(t)
		path := fixture.store.bundlePath(fixture.archiveID, fixture.snapshots[0].SnapshotID, fixture.newEpoch)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.VerifyRekeyHistory(
			context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
			fixture.previousDEK, fixture.newDEK, fixture.signerPublic, fixture.recipientPublic,
		); !errors.Is(err, ErrRekeyHistoryIncomplete) {
			t.Fatalf("missing immutable epoch bundle did not fail closed: %v", err)
		}
	})

	t.Run("wrong recovery recipient", func(t *testing.T) {
		fixture := newRekeyHistoryFixture(t)
		wrongRecipient, err := ecdh.X25519().GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.VerifyRekeyHistory(
			context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
			fixture.previousDEK, fixture.newDEK, fixture.signerPublic, wrongRecipient.PublicKey().Bytes(),
		); !errors.Is(err, ErrRekeyHistoryIncomplete) {
			t.Fatalf("history accepted an envelope for a different recovery key: %v", err)
		}
	})

	t.Run("low-order recovery public key", func(t *testing.T) {
		fixture := newRekeyHistoryFixture(t)
		if _, err := fixture.store.VerifyRekeyHistory(
			context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
			fixture.previousDEK, fixture.newDEK, fixture.signerPublic, make([]byte, 32),
		); !errors.Is(err, ErrInvalid) {
			t.Fatalf("low-order recovery key was not rejected as invalid input: %v", err)
		}
	})
}

func TestVerifyRekeyHistoryRejectsCorruptCiphertextAndInvalidEnvelopeSignature(t *testing.T) {
	t.Run("corrupt ciphertext", func(t *testing.T) {
		fixture := newRekeyHistoryFixture(t)
		bundle, err := fixture.store.LoadBundle(fixture.snapshots[1], fixture.newEpoch)
		if err != nil || len(bundle.Objects) == 0 {
			t.Fatalf("load new epoch bundle: objects=%d err=%v", len(bundle.Objects), err)
		}
		path := filepath.Join(fixture.replicationRoot, objectDir, bundle.Objects[0].ObjectID+".irac")
		ciphertext, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		ciphertext[len(ciphertext)-1] ^= 0x40
		if err := os.WriteFile(path, ciphertext, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.VerifyRekeyHistory(
			context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
			fixture.previousDEK, fixture.newDEK, fixture.signerPublic, fixture.recipientPublic,
		); !errors.Is(err, ErrRekeyHistoryIncomplete) {
			t.Fatalf("tampered ciphertext was accepted into the rekey history: %v", err)
		}
	})

	t.Run("invalid envelope signature", func(t *testing.T) {
		fixture := newRekeyHistoryFixture(t)
		envelopePath := fixture.store.keyEnvelopePath(fixture.archiveID, cryptoarchive.RecipientKeyID(fixture.recipientPublic), fixture.newEpoch)
		envelopeBytes, err := os.ReadFile(envelopePath)
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := cryptoarchive.ParseKeyEnvelope(envelopeBytes)
		if err != nil {
			t.Fatal(err)
		}
		if envelope.Signature[0] == 'A' {
			envelope.Signature = "B" + envelope.Signature[1:]
		} else {
			envelope.Signature = "A" + envelope.Signature[1:]
		}
		encoded, err := cryptoarchive.MarshalKeyEnvelope(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(envelopePath, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.VerifyRekeyHistory(
			context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
			fixture.previousDEK, fixture.newDEK, fixture.signerPublic, fixture.recipientPublic,
		); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("invalid old-owner envelope signature did not fail closed: %v", err)
		}
	})
}

func TestVerifyRekeyHistoryRejectsMissingCatalogAncestor(t *testing.T) {
	fixture := newRekeyHistoryFixture(t)
	path := filepath.Join(fixture.catalogRoot, fixture.archiveID, "snapshots", fixture.snapshots[0].SnapshotID+".json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.VerifyRekeyHistory(
		context.Background(), fixture.catalog, fixture.archiveID, fixture.previousEpoch, fixture.newEpoch,
		fixture.previousDEK, fixture.newDEK, fixture.signerPublic, fixture.recipientPublic,
	); !errors.Is(err, archivecatalog.ErrNotFound) {
		t.Fatalf("incomplete catalog chain was accepted: %v", err)
	}
}

func TestRekeyArchiveHistoryKeepsGateInputsIncompleteAndResumesPartialWork(t *testing.T) {
	fixture := newRekeyHistoryFixture(t)
	oldest := fixture.snapshots[0]
	oldestBundlePath := fixture.store.bundlePath(fixture.archiveID, oldest.SnapshotID, fixture.newEpoch)
	if err := os.Remove(oldestBundlePath); err != nil {
		t.Fatal(err)
	}
	envelopePath := fixture.store.keyEnvelopePath(fixture.archiveID, cryptoarchive.RecipientKeyID(fixture.recipientPublic), fixture.newEpoch)
	if err := os.Remove(envelopePath); err != nil {
		t.Fatal(err)
	}

	failedSource := func(_ context.Context, snapshot archivesnapshot.Snapshot) (CanonicalSource, error) {
		if snapshot.Sequence == oldest.Sequence {
			return nil, errors.New("fixture source temporarily unavailable")
		}
		return fixture.source, nil
	}
	if _, err := fixture.store.RekeyArchiveHistory(
		context.Background(), fixture.catalog, fixture.archiveID, failedSource,
		fixture.previousEpoch, fixture.newEpoch, fixture.previousDEK, fixture.newDEK,
		fixture.envelope, fixture.signerPublic, fixture.recipientPublic, testAuthorization(&testAuthorizer{}),
	); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("partial canonical source failure was not returned safely: %v", err)
	}
	if _, err := fixture.store.LoadBundle(oldest, fixture.newEpoch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete history published the missing snapshot bundle: %v", err)
	}
	if _, err := os.Lstat(envelopePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete history published the recipient envelope: %v", err)
	}
	if _, err := fixture.store.LoadBundle(fixture.snapshots[1], fixture.newEpoch); err != nil {
		t.Fatalf("already completed immutable work was discarded: %v", err)
	}

	result, err := fixture.store.RekeyArchiveHistory(
		context.Background(), fixture.catalog, fixture.archiveID,
		func(context.Context, archivesnapshot.Snapshot) (CanonicalSource, error) { return fixture.source, nil },
		fixture.previousEpoch, fixture.newEpoch, fixture.previousDEK, fixture.newDEK,
		fixture.envelope, fixture.signerPublic, fixture.recipientPublic, testAuthorization(&testAuthorizer{}),
	)
	if err != nil || result.SnapshotCount != "2" || result.HeadSnapshotID != fixture.snapshots[1].SnapshotID {
		t.Fatalf("retry did not complete the full immutable history: result=%+v err=%v", result, err)
	}
}

type rekeyHistoryFixture struct {
	store           *Store
	catalog         *archivecatalog.Store
	replicationRoot string
	catalogRoot     string
	archiveID       string
	previousEpoch   uint64
	newEpoch        uint64
	previousDEK     *cryptoarchive.DEK
	newDEK          *cryptoarchive.DEK
	signerPublic    ed25519.PublicKey
	recipientPublic []byte
	snapshots       []archivesnapshot.Snapshot
	source          coordinateMapSource
	envelope        cryptoarchive.KeyEnvelope
}

func newRekeyHistoryFixture(t *testing.T) rekeyHistoryFixture {
	t.Helper()
	ctx := context.Background()
	replicationRoot := filepath.Join(t.TempDir(), "replication")
	catalogRoot := filepath.Join(t.TempDir(), "catalog")
	store, err := Open(replicationRoot)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := archivecatalog.Open(catalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	previousDEK, err := cryptoarchive.DEKFromBytes(bytes.Repeat([]byte{0x71}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(previousDEK.Destroy)
	newDEK, err := cryptoarchive.DEKFromBytes(bytes.Repeat([]byte{0x72}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(newDEK.Destroy)
	signerPublic, signerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	const recordingID = "rekey-history-fixture"
	session, err := archiveindex.NewSessionIdentity(recordingID, "hls", nil, "broadcast-rekey-history")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory(recordingID, session)
	if err != nil {
		t.Fatal(err)
	}
	source := coordinateMapSource{}
	snapshots := make([]archivesnapshot.Snapshot, 0, 2)
	previousSnapshotID := ""
	for sequence := uint64(1); sequence <= 2; sequence++ {
		coordinate := archiveindex.Coordinate{
			SessionID: session.ID, TrackID: "main", SourceEpoch: 1,
			DiscontinuitySequence: 1, Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}
		payload := []byte("canonical-rekey-history-segment-" + strconv.FormatUint(sequence, 10))
		digest := sha256.Sum256(payload)
		_, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: "rekey-history-" + strconv.FormatUint(sequence, 10),
			Source: archiveindex.ClaimLiveOrigin, AcquiredAt: time.Date(2026, 11, 4, 0, 0, int(sequence), 0, time.UTC),
			PayloadPath: "recordings/rekey-history/" + strconv.FormatUint(sequence, 10) + ".ts",
			Size:        int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
			Verification: archiveindex.VerificationVerified, Duration: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := archivesnapshot.Build(inventory, sequence, previousSnapshotID)
		if err != nil {
			t.Fatal(err)
		}
		if err := catalog.Append(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
		sourceWithCoordinate(source, coordinate, payload)
		if _, err := store.EncryptSnapshot(ctx, snapshot, source, previousDEK, 4, testAuthorization(&testAuthorizer{})); err != nil {
			t.Fatalf("encrypt previous snapshot %d: %v", sequence, err)
		}
		snapshots = append(snapshots, snapshot)
		previousSnapshotID = snapshot.SnapshotID
	}
	oldOwnerKeyID, err := identityprotocol.KeyID(signerPublic)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := cryptoarchive.WrapDEK(newDEK, snapshots[1].Content.ArchiveID, 5, recipient.PublicKey().Bytes(), oldOwnerKeyID, signerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	sourceFor := func(context.Context, archivesnapshot.Snapshot) (CanonicalSource, error) {
		return source, nil
	}
	if _, err := store.RekeyArchiveHistory(ctx, catalog, snapshots[1].Content.ArchiveID, sourceFor, 4, 5,
		previousDEK, newDEK, envelope, signerPublic, recipient.PublicKey().Bytes(), testAuthorization(&testAuthorizer{})); err != nil {
		t.Fatalf("prepare and verify rekeyed history: %v", err)
	}
	return rekeyHistoryFixture{
		store: store, catalog: catalog, replicationRoot: replicationRoot, catalogRoot: catalogRoot,
		archiveID: snapshots[1].Content.ArchiveID, previousEpoch: 4, newEpoch: 5,
		previousDEK: previousDEK, newDEK: newDEK, signerPublic: signerPublic,
		recipientPublic: recipient.PublicKey().Bytes(), snapshots: snapshots, source: source, envelope: envelope,
	}
}

func sourceWithCoordinate(source coordinateMapSource, coordinate archiveindex.Coordinate, payload []byte) coordinateMapSource {
	source[coordinate] = append([]byte(nil), payload...)
	return source
}
