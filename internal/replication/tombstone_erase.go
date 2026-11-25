package replication

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/integrated-recorder/core/internal/lease"
)

const (
	maxTombstoneBundleFiles = 16384
	maxTombstoneObjectRefs  = 262144
)

// TombstoneLeaseState is the already-validated durable lease projection used
// to authorize provider-side replica erasure. Implementations must return the
// authenticated lease store's complete snapshot, not a filtered cache.
type TombstoneLeaseState interface {
	Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error)
}

// ReplicaErasure is an operational receipt, not proof of secure media
// overwriting. Shared content-addressed ciphertext is retained while any
// surviving bundle references it.
type ReplicaErasure struct {
	ArchiveID            string `json:"archive_id"`
	TombstoneHash        string `json:"tombstone_hash"`
	BundleFilesRemoved   int    `json:"bundle_files_removed"`
	PrivateFilesRemoved  int    `json:"private_files_removed"`
	CiphertextRemoved    int    `json:"ciphertext_objects_removed"`
	SharedCiphertextKept int    `json:"shared_ciphertext_objects_kept"`
}

// EraseTombstonedArchive removes the local encrypted replica and its private
// metadata only after a matching, non-forked tombstone is durable and every
// lease for the archive is terminating or terminal. The replication lock is
// acquired before re-reading lease state, matching import lock order. A crash
// during deletion is safe to retry with the same tombstone.
func (s *Store) EraseTombstonedArchive(ctx context.Context, archiveID, tombstoneHash string, state TombstoneLeaseState) (ReplicaErasure, error) {
	result := ReplicaErasure{ArchiveID: archiveID, TombstoneHash: tombstoneHash}
	if s == nil || ctx == nil || state == nil || !validReplicaArchiveID(archiveID) || !validSHA256(tombstoneHash) {
		return ReplicaErasure{}, ErrInvalid
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return ReplicaErasure{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return ReplicaErasure{}, err
	}
	leases, tombstones, err := state.Snapshot(ctx)
	if err != nil {
		return ReplicaErasure{}, ErrObjectUnavailable
	}
	matched := false
	tombstoneByArchive := make(map[string]lease.TombstoneRecord, len(tombstones))
	for _, tombstone := range tombstones {
		tombArchiveID := tombstone.Signed.Tombstone.ArchiveID
		if tombArchiveID != "" {
			tombstoneByArchive[tombArchiveID] = tombstone
		}
		if tombArchiveID == archiveID {
			if tombstone.Frozen || tombstone.Hash != tombstoneHash {
				return ReplicaErasure{}, lease.ErrFrozen
			}
			matched = true
		}
	}
	if !matched {
		return ReplicaErasure{}, lease.ErrNotFound
	}
	for _, item := range leases {
		if item.Offer.Offer.ArchiveID != archiveID {
			continue
		}
		switch item.Status {
		case lease.StatusTerminating, lease.StatusDeleted, lease.StatusUnconfirmed, lease.StatusFailed, lease.StatusExpired:
		default:
			return ReplicaErasure{}, lease.ErrTransition
		}
	}

	// Parse every bundle before deleting anything. A malformed or unsafe
	// reference index must fail closed because it could hide a shared object.
	retainedRefs, targetRefs, bundleFiles, err := s.replicaBundleReferences(archiveID)
	if err != nil {
		return ReplicaErasure{}, err
	}
	result.BundleFilesRemoved = len(bundleFiles)
	// A content-addressed ciphertext can be shared by a different archive
	// whose bundle has not been published yet. Its durable signed Offer and
	// progress checkpoint must therefore protect the object too. A matching
	// provider deletion report is the only lease-based evidence that a separate
	// tombstoned archive has already completed its own cleanup.
	for _, item := range leases {
		otherArchiveID := item.Offer.Offer.ArchiveID
		if otherArchiveID == "" || otherArchiveID == archiveID || providerDeletionCompletedForTombstone(item, tombstoneByArchive) {
			continue
		}
		for _, object := range item.Offer.Offer.EncryptedObjectSet {
			if !validObjectID(object.ObjectID) {
				return ReplicaErasure{}, ErrInvalid
			}
			retainedRefs[object.ObjectID] = struct{}{}
			if len(retainedRefs) > maxTombstoneObjectRefs {
				return ReplicaErasure{}, ErrInvalid
			}
		}
		for _, progress := range item.Progress {
			if !validObjectID(progress.ObjectID) {
				return ReplicaErasure{}, ErrInvalid
			}
			retainedRefs[progress.ObjectID] = struct{}{}
			if len(retainedRefs) > maxTombstoneObjectRefs {
				return ReplicaErasure{}, ErrInvalid
			}
		}
	}
	candidateObjects := make(map[string]struct{}, len(targetRefs))
	for objectID := range targetRefs {
		candidateObjects[objectID] = struct{}{}
	}
	for _, item := range leases {
		if item.Offer.Offer.ArchiveID != archiveID {
			continue
		}
		// The immutable signed Offer survives after a crash removes one or
		// more bundle files. Its object set is therefore the recovery index for
		// cleanup even when no transfer checkpoint was ever written (for
		// example, an accepted/expired lease whose bundle disappeared first).
		for _, object := range item.Offer.Offer.EncryptedObjectSet {
			if !validObjectID(object.ObjectID) {
				return ReplicaErasure{}, ErrInvalid
			}
			candidateObjects[object.ObjectID] = struct{}{}
			if len(candidateObjects) > maxTombstoneObjectRefs {
				return ReplicaErasure{}, ErrInvalid
			}
		}
		for _, progress := range item.Progress {
			if !validObjectID(progress.ObjectID) {
				return ReplicaErasure{}, ErrInvalid
			}
			candidateObjects[progress.ObjectID] = struct{}{}
			if len(candidateObjects) > maxTombstoneObjectRefs {
				return ReplicaErasure{}, ErrInvalid
			}
		}
	}
	// Count sharing from the complete retry candidate set, not just the
	// surviving bundle index. The signed Offer/checkpoint set can be the only
	// remaining index after an interrupted cleanup removed the target bundle.
	for objectID := range candidateObjects {
		if _, shared := retainedRefs[objectID]; shared {
			result.SharedCiphertextKept++
		}
	}
	// Validate the complete object directory before any destructive mutation;
	// an unexpected file or symlink must not be discovered after keys vanish.
	objectRoot := filepath.Join(s.root, objectDir)
	dirInfo, err := os.Lstat(objectRoot)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 || dirInfo.Mode().Perm() != 0700 {
		return ReplicaErasure{}, ErrObjectUnavailable
	}
	directory, err := os.Open(objectRoot)
	if err != nil {
		return ReplicaErasure{}, ErrObjectUnavailable
	}
	openedInfo, statErr := directory.Stat()
	if statErr != nil || !os.SameFile(dirInfo, openedInfo) {
		_ = directory.Close()
		return ReplicaErasure{}, ErrInvalid
	}
	for {
		entries, readErr := directory.ReadDir(256)
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".irac") || !validObjectID(strings.TrimSuffix(name, ".irac")) {
				_ = directory.Close()
				return ReplicaErasure{}, ErrInvalid
			}
			info, statErr := os.Lstat(filepath.Join(objectRoot, name))
			if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
				_ = directory.Close()
				return ReplicaErasure{}, ErrInvalid
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = directory.Close()
			return ReplicaErasure{}, ErrObjectUnavailable
		}
	}
	if err := directory.Close(); err != nil {
		return ReplicaErasure{}, ErrObjectUnavailable
	}

	// Remove key material first. If interrupted, the encrypted bytes remain
	// inaccessible and a repeated tombstone can finish metadata/object cleanup.
	for _, dir := range []string{keyEnvelopeDir, archiveKeyDir} {
		removed, err := erasePrivateTree(filepath.Join(s.root, dir, archiveID))
		if err != nil {
			return ReplicaErasure{}, err
		}
		result.PrivateFilesRemoved += removed
	}
	for _, dir := range []string{bundleDir, snapshotDir} {
		removed, err := erasePrivateTree(filepath.Join(s.root, dir, archiveID))
		if err != nil {
			return ReplicaErasure{}, err
		}
		if dir == snapshotDir {
			result.PrivateFilesRemoved += removed
		}
	}

	// Transfer chunks are lease scoped. They are safe to discard only for this
	// archive's now-terminating leases; unrelated transfers remain untouched.
	leaseIDs := make(map[string]struct{})
	for _, item := range leases {
		if item.Offer.Offer.ArchiveID == archiveID {
			leaseIDs[item.Offer.Offer.LeaseID] = struct{}{}
		}
	}
	removed, err := s.eraseLeasePartials(leaseIDs)
	if err != nil {
		return ReplicaErasure{}, err
	}
	result.PrivateFilesRemoved += removed

	// Remove unreferenced object files, including an orphan published during a
	// transfer interrupted by the tombstone. The store lock excludes writers.
	for objectID := range candidateObjects {
		if err := ctx.Err(); err != nil {
			return ReplicaErasure{}, err
		}
		if _, referenced := retainedRefs[objectID]; referenced {
			continue
		}
		name := objectID + ".irac"
		path := filepath.Join(s.root, objectDir, name)
		info, statErr := os.Lstat(path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
			return ReplicaErasure{}, ErrInvalid
		}
		if err := os.Remove(path); err != nil {
			return ReplicaErasure{}, ErrObjectUnavailable
		}
		if err := syncDirectory(filepath.Join(s.root, objectDir)); err != nil {
			return ReplicaErasure{}, ErrObjectUnavailable
		}
		result.CiphertextRemoved++
	}
	return result, nil
}

func providerDeletionCompletedForTombstone(item lease.Lease, tombstones map[string]lease.TombstoneRecord) bool {
	if item.Status != lease.StatusDeleted || item.DeletionReport == nil {
		return false
	}
	archiveID := item.Offer.Offer.ArchiveID
	tombstone, ok := tombstones[archiveID]
	if !ok || tombstone.Frozen || archiveID == "" {
		return false
	}
	report := item.DeletionReport.Report
	return report.Status == string(lease.StatusDeleted) && report.TombstoneHash == tombstone.Hash &&
		report.LeaseID == item.Offer.Offer.LeaseID && report.OfferHash == item.OfferHash &&
		report.ProviderNodeID == item.Offer.Offer.ProviderNodeID
}

func (s *Store) replicaBundleReferences(targetArchive string) (retained, target map[string]struct{}, paths []string, err error) {
	retained, target = make(map[string]struct{}), make(map[string]struct{})
	root := filepath.Join(s.root, bundleDir)
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return ErrObjectUnavailable
		}
		info, statErr := os.Lstat(path)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		if entry.IsDir() {
			if !info.IsDir() || info.Mode().Perm() != 0700 {
				return ErrInvalid
			}
			return nil
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() > maxBundleSize || filepath.Ext(path) != ".json" {
			return ErrInvalid
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil || int64(len(data)) != info.Size() {
			return ErrObjectUnavailable
		}
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		var disk bundleDisk
		if decoder.Decode(&disk) != nil || decoder.Decode(&struct{}{}) != io.EOF || disk.Version != BundleVersion || !validReplicaArchiveID(disk.ArchiveID) || !validReplicaSnapshotID(disk.SnapshotID) {
			return ErrInvalid
		}
		if _, parseErr := strconv.ParseUint(disk.KeyEpoch, 10, 64); parseErr != nil || disk.KeyEpoch == "0" {
			return ErrInvalid
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil || filepath.Clean(relative) != filepath.Join(disk.ArchiveID, disk.SnapshotID, disk.KeyEpoch+".json") {
			return ErrInvalid
		}
		if disk.ArchiveID == targetArchive {
			paths = append(paths, path)
			if len(paths) > maxTombstoneBundleFiles {
				return ErrInvalid
			}
		}
		for _, object := range disk.Objects {
			if !validObjectID(object.ObjectID) || object.CiphertextSize <= 0 || !validSHA256(object.CiphertextSHA256) {
				return ErrInvalid
			}
			if disk.ArchiveID == targetArchive {
				target[object.ObjectID] = struct{}{}
			} else {
				retained[object.ObjectID] = struct{}{}
			}
			if len(target)+len(retained) > maxTombstoneObjectRefs {
				return ErrInvalid
			}
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	return retained, target, paths, err
}

func (s *Store) eraseLeasePartials(leaseIDs map[string]struct{}) (int, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, stageDir))
	if err != nil {
		return 0, ErrObjectUnavailable
	}
	removed := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "transfer-") {
			continue
		}
		matchedLease := ""
		for leaseID := range leaseIDs {
			if strings.HasPrefix(name, "transfer-"+leaseID+"-") {
				matchedLease = leaseID
				break
			}
		}
		if matchedLease == "" {
			continue
		}
		objectName := strings.TrimSuffix(strings.TrimPrefix(name, "transfer-"+matchedLease+"-"), ".part")
		if !strings.HasSuffix(name, ".part") || !validObjectID(objectName) {
			return removed, ErrInvalid
		}
		path := filepath.Join(s.root, stageDir, name)
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
			return removed, ErrInvalid
		}
		if err := os.Remove(path); err != nil {
			return removed, ErrObjectUnavailable
		}
		removed++
	}
	if removed > 0 {
		if err := syncDirectory(filepath.Join(s.root, stageDir)); err != nil {
			return removed, ErrObjectUnavailable
		}
	}
	return removed, nil
}

func erasePrivateTree(path string) (int, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return 0, ErrInvalid
	}
	removed := 0
	if info.IsDir() {
		if info.Mode().Perm() != 0700 {
			return 0, ErrInvalid
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return 0, ErrObjectUnavailable
		}
		for _, entry := range entries {
			count, err := erasePrivateTree(filepath.Join(path, entry.Name()))
			removed += count
			if err != nil {
				return removed, err
			}
		}
		if err := os.Remove(path); err != nil {
			return removed, ErrObjectUnavailable
		}
	} else {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return 0, ErrInvalid
		}
		if err := os.Remove(path); err != nil {
			return 0, ErrObjectUnavailable
		}
		removed++
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return removed, ErrObjectUnavailable
	}
	return removed, nil
}
