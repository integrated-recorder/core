package replication

import (
	"strconv"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
)

// SealSessionDEKEnvelope releases a source-local DEK only after the immutable
// snapshot and its encrypted manifest are validated. The caller must already
// have authorized the exact archive.view capability and authenticated peer
// session; this method adds recipient and snapshot binding, not authority.
func (s *Store) SealSessionDEKEnvelope(
	snapshot archivesnapshot.Snapshot,
	keyEpoch uint64,
	recipientPublicKey []byte,
	binding cryptoarchive.SessionDEKBinding,
	protector ArchiveDEKProtector,
) (cryptoarchive.SessionDEKEnvelope, error) {
	if s == nil || protector == nil || archivesnapshot.Verify(snapshot) != nil ||
		binding.ArchiveID != snapshot.Content.ArchiveID || binding.SnapshotID != snapshot.SnapshotID ||
		binding.KeyEpoch != keyEpoch || keyEpoch == 0 {
		return cryptoarchive.SessionDEKEnvelope{}, ErrInvalid
	}
	bundle, err := s.LoadBundle(snapshot, keyEpoch)
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	if _, err := PublicReplicaManifest(snapshot, bundle); err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, ErrInvalid
	}
	dataKey, err := s.LoadArchiveDEK(snapshot.Content.ArchiveID, keyEpoch, protector)
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	defer clear(dataKey)
	envelope, err := cryptoarchive.SealSessionDEK(dataKey, recipientPublicKey, binding)
	if err != nil || envelope.ArchiveID != snapshot.Content.ArchiveID || envelope.SnapshotID != snapshot.SnapshotID ||
		envelope.KeyEpoch != strconv.FormatUint(keyEpoch, 10) {
		return cryptoarchive.SessionDEKEnvelope{}, ErrInvalid
	}
	return envelope, nil
}

// HasSessionDEK reports whether this source can unlock the key epoch without
// returning key material. It is intended only to filter a bounded key-epoch
// discovery response; the later envelope request repeats all validation.
func (s *Store) HasSessionDEK(archiveID string, keyEpoch uint64, protector ArchiveDEKProtector) bool {
	if s == nil || protector == nil {
		return false
	}
	dataKey, err := s.LoadArchiveDEK(archiveID, keyEpoch, protector)
	if err != nil {
		return false
	}
	clear(dataKey)
	return true
}
