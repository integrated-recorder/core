package replication

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strconv"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

// CanonicalSourceForSnapshot opens canonical objects for one immutable
// snapshot. Implementations must not mutate the catalog being walked.
type CanonicalSourceForSnapshot func(context.Context, archivesnapshot.Snapshot) (CanonicalSource, error)

// RekeyArchiveHistory rotates one encrypted Archive history to a distinct key
// epoch, stores the recipient-bound envelope after every snapshot bundle has
// been prepared, then verifies the complete old/new history. The catalog is
// walked newest-first under its read lock. If a later callback fails, already
// published immutable bundles are safe partial work; the envelope is not
// published and authority must remain rekey-locked. Retrying with the same
// epoch, DEK, envelope, and authorization reuses completed bundles and
// completes missing snapshots. A changed envelope or key requires a new epoch.
//
// This operation does not change archive metadata or OwnerSubject state. In
// particular, the verification result is not portable proof and does not
// establish that the recipient opened the envelope; only the recipient-side
// client may attest to that through the separately signed authority event.
func (s *Store) RekeyArchiveHistory(
	ctx context.Context,
	catalog *archivecatalog.Store,
	archiveID string,
	sourceFor CanonicalSourceForSnapshot,
	previousEpoch, newEpoch uint64,
	previousDEK, newDEK *cryptoarchive.DEK,
	envelope cryptoarchive.KeyEnvelope,
	oldOwnerSigningKey ed25519.PublicKey,
	newRecipientRecoveryKey []byte,
	auth Authorization,
) (RekeyHistoryVerification, error) {
	if s == nil || ctx == nil || catalog == nil || sourceFor == nil || !validReplicaArchiveID(archiveID) ||
		previousEpoch == 0 || newEpoch <= previousEpoch || previousDEK == nil || newDEK == nil ||
		previousDEK.SameKey(newDEK) || len(oldOwnerSigningKey) != ed25519.PublicKeySize ||
		!validRekeyRecipientKey(newRecipientRecoveryKey) {
		return RekeyHistoryVerification{}, ErrInvalid
	}
	oldOwnerKeyID, err := identityprotocol.KeyID(oldOwnerSigningKey)
	if err != nil || envelope.ArchiveID != archiveID || envelope.KeyEpoch != decimalEpoch(newEpoch) ||
		envelope.RecipientKeyID != cryptoarchive.RecipientKeyID(newRecipientRecoveryKey) ||
		envelope.RecipientPublicKeySHA256 != cryptoarchive.RecipientPublicKeyHash(newRecipientRecoveryKey) ||
		envelope.SignerKeyID != oldOwnerKeyID || cryptoarchive.VerifyKeyEnvelopeSignature(envelope, oldOwnerSigningKey) != nil {
		return RekeyHistoryVerification{}, ErrUnauthorized
	}

	var headSnapshot archivesnapshot.Snapshot
	var snapshots uint64
	_, err = catalog.WalkHistory(ctx, archiveID, func(snapshot archivesnapshot.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if snapshot.Content.ArchiveID != archiveID || archivesnapshot.Verify(snapshot) != nil {
			return ErrInvalid
		}
		if snapshots == 0 {
			headSnapshot = snapshot
		}
		source, sourceErr := sourceFor(ctx, snapshot)
		if sourceErr != nil || source == nil {
			return ErrObjectUnavailable
		}
		if _, rekeyErr := s.RekeySnapshot(ctx, snapshot, source, previousDEK, previousEpoch, newDEK, newEpoch, auth); rekeyErr != nil {
			return rekeyErr
		}
		snapshots++
		return nil
	})
	if err != nil {
		return RekeyHistoryVerification{}, err
	}
	if snapshots == 0 || headSnapshot.SnapshotID == "" {
		return RekeyHistoryVerification{}, ErrRekeyHistoryIncomplete
	}
	if err := s.PersistKeyEnvelope(headSnapshot, envelope, oldOwnerSigningKey); err != nil {
		if errors.Is(err, ErrInvalid) {
			return RekeyHistoryVerification{}, ErrUnauthorized
		}
		return RekeyHistoryVerification{}, err
	}
	verified, err := s.VerifyRekeyHistory(ctx, catalog, archiveID, previousEpoch, newEpoch, previousDEK, newDEK, oldOwnerSigningKey, newRecipientRecoveryKey)
	if err != nil {
		return RekeyHistoryVerification{}, err
	}
	if verified.SnapshotCount != decimalEpoch(snapshots) || verified.HeadSnapshotID != headSnapshot.SnapshotID {
		return RekeyHistoryVerification{}, ErrRekeyHistoryIncomplete
	}
	return verified, nil
}

func decimalEpoch(value uint64) string {
	return strconv.FormatUint(value, 10)
}
