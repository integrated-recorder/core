package replication

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/integrated-recorder/core/internal/lease"
)

type tombstoneLeaseFixture struct {
	leases     []lease.Lease
	tombstones []lease.TombstoneRecord
	err        error
}

func (f tombstoneLeaseFixture) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return f.leases, f.tombstones, f.err
}

func TestEraseTombstonedArchiveRemovesPrivateReplicaAndPreservesSharedCiphertext(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	targetArchive := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(0, 16))
	otherArchive := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(1, 16))
	// Snapshot IDs are canonical lowercase SHA-256 hex values.
	targetSnapshot := "snap_" + string(repeatByte('a', 64))
	otherSnapshot := "snap_"
	for i := 0; i < 64; i++ {
		otherSnapshot += "b"
	}
	targetOnly := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(1, 32)))
	shared := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(2, 32)))
	otherOnly := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(3, 32)))
	writeBundleFixture(t, store.root, targetArchive, targetSnapshot, "1", targetOnly, shared)
	writeBundleFixture(t, store.root, otherArchive, otherSnapshot, "1", shared, otherOnly)
	for _, id := range []string{targetOnly, shared, otherOnly} {
		writePrivateFixture(t, filepath.Join(store.root, objectDir, id+".irac"), []byte("encrypted:"+id))
	}
	if _, _, _, err := store.replicaBundleReferences(targetArchive); err != nil {
		t.Fatalf("fixture reference scan failed: %v", err)
	}
	writePrivateFixture(t, store.snapshotPath(targetArchive, targetSnapshot), []byte("descriptor"))
	writePrivateFixture(t, filepath.Join(store.root, keyEnvelopeDir, targetArchive, "rk_"+string(repeatByte('c', 43)), "1.json"), []byte("envelope"))
	writePrivateFixture(t, store.archiveDEKPath(targetArchive, 1), []byte("wrapped-dek"))
	leaseID := "rl_" + string(repeatByte('d', 22))
	otherLeaseID := "rl_" + string(repeatByte('e', 22))
	targetPartial := filepath.Join(store.root, stageDir, "transfer-"+leaseID+"-"+targetOnly+".part")
	otherPartial := filepath.Join(store.root, stageDir, "transfer-"+otherLeaseID+"-"+otherOnly+".part")
	writePrivateFixture(t, targetPartial, []byte("partial"))
	writePrivateFixture(t, otherPartial, []byte("keep"))
	leaseItem := lease.Lease{Offer: lease.SignedOffer{Offer: lease.Offer{LeaseID: leaseID, ArchiveID: targetArchive}}, Status: lease.StatusTerminating}
	tombstoneHash := "sha256:" + hex.EncodeToString(bytesOf(9, 32))
	state := tombstoneLeaseFixture{
		leases:     []lease.Lease{leaseItem},
		tombstones: []lease.TombstoneRecord{{Hash: tombstoneHash, Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: targetArchive}}}},
	}
	result, err := store.EraseTombstonedArchive(ctx, targetArchive, tombstoneHash, state)
	if err != nil {
		t.Fatalf("erase tombstoned archive: %v", err)
	}
	if result.BundleFilesRemoved != 1 || result.CiphertextRemoved != 1 || result.SharedCiphertextKept != 1 || result.PrivateFilesRemoved < 3 {
		t.Fatalf("unexpected erasure receipt: %+v", result)
	}
	for _, id := range []string{targetOnly} {
		if _, err := os.Lstat(filepath.Join(store.root, objectDir, id+".irac")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("target-only ciphertext remains: %s err=%v", id, err)
		}
	}
	for _, id := range []string{shared, otherOnly} {
		if _, err := os.Lstat(filepath.Join(store.root, objectDir, id+".irac")); err != nil {
			t.Fatalf("unrelated/shared ciphertext removed: %s err=%v", id, err)
		}
	}
	if _, err := os.Lstat(store.bundlePath(targetArchive, targetSnapshot, 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target bundle remains: %v", err)
	}
	if _, err := os.Lstat(store.bundlePath(otherArchive, otherSnapshot, 1)); err != nil {
		t.Fatalf("unrelated bundle removed: %v", err)
	}
	if _, err := os.Lstat(targetPartial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target transfer partial remains: %v", err)
	}
	if _, err := os.Stat(otherPartial); err != nil {
		t.Fatalf("unrelated transfer partial removed: %v", err)
	}
	// Replaying the exact durable tombstone is safe after partial or full work.
	if _, err := store.EraseTombstonedArchive(ctx, targetArchive, tombstoneHash, state); err != nil {
		t.Fatalf("idempotent erasure retry: %v", err)
	}
}

func TestEraseTombstonedArchiveFailsClosedBeforeMutation(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(0, 16))
	snapshotID := "snap_" + string(repeatByte('a', 64))
	objectID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(7, 32)))
	writeBundleFixture(t, store.root, archiveID, snapshotID, "1", objectID)
	writePrivateFixture(t, filepath.Join(store.root, objectDir, objectID+".irac"), []byte("ciphertext"))
	hash := "sha256:" + hex.EncodeToString(bytesOf(8, 32))
	leaseItem := lease.Lease{Offer: lease.SignedOffer{Offer: lease.Offer{LeaseID: "rl_" + string(repeatByte('e', 22)), ArchiveID: archiveID}}, Status: lease.StatusActive}
	for _, state := range []tombstoneLeaseFixture{
		{leases: []lease.Lease{leaseItem}, tombstones: []lease.TombstoneRecord{{Hash: hash, Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: archiveID}}}}},
		{leases: []lease.Lease{leaseItem}},
	} {
		if _, err := store.EraseTombstonedArchive(context.Background(), archiveID, hash, state); err == nil {
			t.Fatal("erasure succeeded without a complete terminating lease state and tombstone")
		}
		if _, err := os.Stat(filepath.Join(store.root, objectDir, objectID+".irac")); err != nil {
			t.Fatalf("fail-closed attempt modified ciphertext: %v", err)
		}
	}
}

func TestEraseTombstonedArchiveRejectsEveryLiveLeaseStateBeforeMutation(t *testing.T) {
	for _, status := range []lease.Status{
		lease.StatusOffered,
		lease.StatusAccepted,
		lease.StatusTransferring,
		lease.StatusVerifying,
		lease.StatusActive,
		lease.StatusRenewing,
	} {
		t.Run(string(status), func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "replica"))
			if err != nil {
				t.Fatal(err)
			}
			archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(byte(status[0]), 16))
			snapshotID := "snap_" + string(repeatByte('a', 64))
			objectID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(10, 32)))
			writeBundleFixture(t, store.root, archiveID, snapshotID, "1", objectID)
			objectPath := filepath.Join(store.root, objectDir, objectID+".irac")
			writePrivateFixture(t, objectPath, []byte("ciphertext"))
			leaseID := "rl_" + string(repeatByte('f', 22))
			hash := "sha256:" + hex.EncodeToString(bytesOf(11, 32))
			state := tombstoneLeaseFixture{
				leases: []lease.Lease{{
					Offer:  lease.SignedOffer{Offer: lease.Offer{LeaseID: leaseID, ArchiveID: archiveID}},
					Status: status,
				}},
				tombstones: []lease.TombstoneRecord{{
					Hash:   hash,
					Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: archiveID}},
				}},
			}

			if _, err := store.EraseTombstonedArchive(context.Background(), archiveID, hash, state); !errors.Is(err, lease.ErrTransition) {
				t.Fatalf("live lease state %q did not block erasure: %v", status, err)
			}
			if _, err := os.Stat(objectPath); err != nil {
				t.Fatalf("live lease state %q removed ciphertext: %v", status, err)
			}
			if _, err := os.Stat(store.bundlePath(archiveID, snapshotID, 1)); err != nil {
				t.Fatalf("live lease state %q removed bundle metadata: %v", status, err)
			}
		})
	}
}

func TestProviderDeletionReportMustMatchArchiveTombstoneBeforeLeaseReferenceIsReleased(t *testing.T) {
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(9, 16))
	leaseID := "rl_" + string(repeatByte('9', 22))
	providerID := "nd_" + string(repeatByte('9', 22))
	offerHash := "sha256:" + hex.EncodeToString(bytesOf(12, 32))
	tombstoneHash := "sha256:" + hex.EncodeToString(bytesOf(13, 32))
	item := lease.Lease{
		Offer:     lease.SignedOffer{Offer: lease.Offer{ArchiveID: archiveID, LeaseID: leaseID, ProviderNodeID: providerID}},
		OfferHash: offerHash,
		Status:    lease.StatusDeleted,
		DeletionReport: &lease.SignedDeletionReport{Report: lease.DeletionReport{
			LeaseID: leaseID, OfferHash: offerHash, ProviderNodeID: providerID,
			Status: string(lease.StatusDeleted), TombstoneHash: tombstoneHash,
		}},
	}
	matching := lease.TombstoneRecord{
		Hash: tombstoneHash, Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: archiveID}},
	}

	if !providerDeletionCompletedForTombstone(item, map[string]lease.TombstoneRecord{archiveID: matching}) {
		t.Fatal("exact provider deletion report did not release lease object references")
	}
	cases := []struct {
		name      string
		lease     lease.Lease
		tombstone map[string]lease.TombstoneRecord
	}{
		{name: "missing tombstone", lease: item},
		{name: "frozen tombstone", lease: item, tombstone: map[string]lease.TombstoneRecord{archiveID: {Hash: tombstoneHash, Frozen: true, Signed: matching.Signed}}},
		{name: "different tombstone hash", lease: item, tombstone: map[string]lease.TombstoneRecord{archiveID: {Hash: "sha256:" + hex.EncodeToString(bytesOf(14, 32)), Signed: matching.Signed}}},
		{name: "different report hash", lease: func() lease.Lease {
			v := item
			v.DeletionReport = &lease.SignedDeletionReport{Report: item.DeletionReport.Report}
			v.DeletionReport.Report.TombstoneHash = "sha256:" + hex.EncodeToString(bytesOf(15, 32))
			return v
		}(), tombstone: map[string]lease.TombstoneRecord{archiveID: matching}},
		{name: "different lease ID", lease: func() lease.Lease {
			v := item
			v.DeletionReport = &lease.SignedDeletionReport{Report: item.DeletionReport.Report}
			v.DeletionReport.Report.LeaseID = "rl_" + string(repeatByte('8', 22))
			return v
		}(), tombstone: map[string]lease.TombstoneRecord{archiveID: matching}},
		{name: "not deleted", lease: func() lease.Lease { v := item; v.Status = lease.StatusTerminating; return v }(), tombstone: map[string]lease.TombstoneRecord{archiveID: matching}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if providerDeletionCompletedForTombstone(tc.lease, tc.tombstone) {
				t.Fatal("unmatched or incomplete deletion evidence released lease references")
			}
		})
	}
}

func TestEraseTombstonedArchiveResumesAfterMetadataWasPartiallyRemoved(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(6, 16))
	otherArchiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(8, 16))
	snapshotID := "snap_" + string(repeatByte('6', 64))
	otherSnapshotID := "snap_" + string(repeatByte('8', 64))
	objectID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(6, 32)))
	offerOnlyID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(16, 32)))
	sharedID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(19, 32)))
	leaseOnlySharedID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(21, 32)))
	writeBundleFixture(t, store.root, archiveID, snapshotID, "1", objectID, offerOnlyID, sharedID)
	writeBundleFixture(t, store.root, otherArchiveID, otherSnapshotID, "1", sharedID)
	for _, id := range []string{objectID, offerOnlyID, sharedID, leaseOnlySharedID} {
		writePrivateFixture(t, filepath.Join(store.root, objectDir, id+".irac"), []byte("ciphertext:"+id))
	}
	// Simulate interruption after archive metadata removal but before object
	// cleanup. One object has a checkpoint; the other exists only in the
	// immutable signed Offer object set. Both must remain discoverable.
	if err := os.RemoveAll(filepath.Join(store.root, bundleDir, archiveID)); err != nil {
		t.Fatal(err)
	}
	leaseItem := lease.Lease{
		Offer: lease.SignedOffer{Offer: lease.Offer{
			LeaseID: "rl_" + string(repeatByte('6', 22)), ArchiveID: archiveID,
			EncryptedObjectSet: []lease.EncryptedObject{
				{ObjectID: objectID, CiphertextSize: "16", CiphertextSHA256: "sha256:" + hex.EncodeToString(bytesOf(17, 32))},
				{ObjectID: offerOnlyID, CiphertextSize: "16", CiphertextSHA256: "sha256:" + hex.EncodeToString(bytesOf(18, 32))},
				{ObjectID: sharedID, CiphertextSize: "16", CiphertextSHA256: "sha256:" + hex.EncodeToString(bytesOf(20, 32))},
				{ObjectID: leaseOnlySharedID, CiphertextSize: "16", CiphertextSHA256: "sha256:" + hex.EncodeToString(bytesOf(22, 32))},
			},
		}},
		Status: lease.StatusTerminating, Progress: []lease.Progress{{ObjectID: objectID, CommittedOffset: "11"}},
	}
	otherLease := lease.Lease{
		Offer: lease.SignedOffer{Offer: lease.Offer{
			LeaseID: "rl_" + string(repeatByte('8', 22)), ArchiveID: otherArchiveID,
			EncryptedObjectSet: []lease.EncryptedObject{{
				ObjectID: leaseOnlySharedID, CiphertextSize: "16", CiphertextSHA256: "sha256:" + hex.EncodeToString(bytesOf(23, 32)),
			}},
		}},
		Status: lease.StatusActive,
	}
	hash := "sha256:" + hex.EncodeToString(bytesOf(6, 32))
	state := tombstoneLeaseFixture{
		leases:     []lease.Lease{leaseItem, otherLease},
		tombstones: []lease.TombstoneRecord{{Hash: hash, Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: archiveID}}}},
	}
	result, err := store.EraseTombstonedArchive(context.Background(), archiveID, hash, state)
	if err != nil || result.CiphertextRemoved != 2 || result.SharedCiphertextKept != 2 {
		t.Fatalf("partial erasure retry result=%+v err=%v", result, err)
	}
	for _, id := range []string{objectID, offerOnlyID} {
		if _, err := os.Stat(filepath.Join(store.root, objectDir, id+".irac")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan ciphertext %s survived retry: %v", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(store.root, objectDir, sharedID+".irac")); err != nil {
		t.Fatalf("ciphertext shared with another archive was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.root, objectDir, leaseOnlySharedID+".irac")); err != nil {
		t.Fatalf("ciphertext referenced only by another active lease was removed: %v", err)
	}
	if _, err := os.Stat(store.bundlePath(otherArchiveID, otherSnapshotID, 1)); err != nil {
		t.Fatalf("other archive bundle was removed during retry: %v", err)
	}
}

func TestConcurrentTombstoneErasureIsIdempotent(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(7, 16))
	snapshotID := "snap_" + string(repeatByte('7', 64))
	objectID := objectIDForDigest("sha256:" + hex.EncodeToString(bytesOf(7, 32)))
	writeBundleFixture(t, store.root, archiveID, snapshotID, "1", objectID)
	writePrivateFixture(t, filepath.Join(store.root, objectDir, objectID+".irac"), []byte("ciphertext"))
	hash := "sha256:" + hex.EncodeToString(bytesOf(7, 32))
	state := tombstoneLeaseFixture{tombstones: []lease.TombstoneRecord{{Hash: hash, Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: archiveID}}}}}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.EraseTombstonedArchive(context.Background(), archiveID, hash, state)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent erasure returned error: %v", err)
		}
	}
	if _, err := os.Stat(filepath.Join(store.root, objectDir, objectID+".irac")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("concurrent erasure left ciphertext: %v", err)
	}
}

func TestEraseTombstonedArchiveRejectsMalformedReferenceIndex(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytesOf(0, 16))
	snapshotID := "snap_" + string(repeatByte('a', 64))
	path := filepath.Join(store.root, bundleDir, archiveID, snapshotID, "1.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{} trailing"), 0600); err != nil {
		t.Fatal(err)
	}
	hash := "sha256:" + hex.EncodeToString(bytesOf(5, 32))
	state := tombstoneLeaseFixture{tombstones: []lease.TombstoneRecord{{Hash: hash, Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: archiveID}}}}}
	if _, err := store.EraseTombstonedArchive(context.Background(), archiveID, hash, state); !errors.Is(err, ErrInvalid) {
		t.Fatalf("malformed reference index did not fail closed: %v", err)
	}
}

func writeBundleFixture(t *testing.T, root, archiveID, snapshotID, epoch string, objects ...string) {
	t.Helper()
	disk := bundleDisk{Version: BundleVersion, ArchiveID: archiveID, SnapshotID: snapshotID, KeyEpoch: epoch}
	for _, id := range objects {
		disk.Objects = append(disk.Objects, objectBindingDisk{ObjectID: id, CiphertextSize: 12, CiphertextSHA256: "sha256:" + hex.EncodeToString(bytesOf(4, 32))})
	}
	data, err := json.Marshal(disk)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, bundleDir, archiveID, snapshotID, epoch+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func writePrivateFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func repeatByte(value byte, count int) []byte { return []byte(string(bytesOf(value, count))) }

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for i := range result {
		result[i] = value
	}
	return result
}
