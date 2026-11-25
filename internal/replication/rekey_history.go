package replication

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const rekeyHistoryDomain = "IR/archive-rekey-history/v1\x00"

var ErrRekeyHistoryIncomplete = errors.New("archive rekey history is incomplete or invalid")

// RekeyHistoryVerification is a non-secret commitment to a complete local
// archive history whose old and new encrypted bundles were both independently
// decrypted and checked against each immutable snapshot. It is not a portable
// proof and does not establish that the recipient opened the envelope; callers
// need a recipient-signed acknowledgement before changing OwnerSubject policy.
type RekeyHistoryVerification struct {
	ArchiveID           string
	PreviousKeyEpoch    string
	NewKeyEpoch         string
	SnapshotCount       string
	HeadSequence        string
	HeadSnapshotID      string
	BundleSetCommitment string
	EnvelopeSHA256      string
	RecipientKeyID      string
}

// VerifyRekeyHistory checks the complete local catalog chain while holding
// the catalog head stable. For every snapshot it verifies the complete old
// and new ciphertext bundles by decrypting each object and comparing the
// plaintext bytes to the immutable snapshot digests. It also verifies that
// the new epoch's signed envelope targets the exact recipient recovery key
// and was signed by the supplied old-owner identity key. Bundle and
// ciphertext files are immutable and atomically published, so this read does
// not acquire the replication mutation lock while the catalog lock is held.
//
// This method is read-only. It neither creates an envelope nor claims that the
// recipient can open it. It also does not clear authority's rekey-required
// gate; the recipient must open the envelope and acknowledge the resulting
// commitment through the separately verified OwnerSubject transfer protocol.
func (s *Store) VerifyRekeyHistory(
	ctx context.Context,
	catalog *archivecatalog.Store,
	archiveID string,
	previousEpoch, newEpoch uint64,
	previousDEK, newDEK *cryptoarchive.DEK,
	oldOwnerSigningKey ed25519.PublicKey,
	newRecipientRecoveryKey []byte,
) (RekeyHistoryVerification, error) {
	if s == nil || ctx == nil || catalog == nil || !validReplicaArchiveID(archiveID) ||
		previousEpoch == 0 || newEpoch <= previousEpoch || previousDEK == nil || newDEK == nil ||
		previousDEK.SameKey(newDEK) || len(oldOwnerSigningKey) != ed25519.PublicKeySize || !validRekeyRecipientKey(newRecipientRecoveryKey) {
		return RekeyHistoryVerification{}, ErrInvalid
	}
	oldOwnerKeyID, err := identityprotocol.KeyID(oldOwnerSigningKey)
	if err != nil {
		return RekeyHistoryVerification{}, ErrUnauthorized
	}
	recipientKeyID := cryptoarchive.RecipientKeyID(newRecipientRecoveryKey)
	recipientKeyHash := cryptoarchive.RecipientPublicKeyHash(newRecipientRecoveryKey)
	commitment := sha256.Sum256([]byte(rekeyHistoryDomain + "begin\x00" + archiveID))
	var count uint64
	var envelopeHash string
	var catalogHead archivecatalog.HistoryHead

	head, err := catalog.WalkHistory(ctx, archiveID, func(snapshot archivesnapshot.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		previousBundle, err := s.LoadBundle(snapshot, previousEpoch)
		if err != nil {
			return ErrRekeyHistoryIncomplete
		}
		newBundle, err := s.LoadBundle(snapshot, newEpoch)
		if err != nil {
			return ErrRekeyHistoryIncomplete
		}
		if err := s.verifyBundleKey(ctx, snapshot, previousBundle, previousDEK); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrRekeyHistoryIncomplete
		}
		if err := s.verifyBundleKey(ctx, snapshot, newBundle, newDEK); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrRekeyHistoryIncomplete
		}
		previousManifest, err := PublicReplicaManifest(snapshot, previousBundle)
		if err != nil {
			return ErrInvalid
		}
		previousManifestHash, err := ReplicaManifestHash(snapshot, previousManifest)
		if err != nil {
			return ErrInvalid
		}
		newManifest, err := PublicReplicaManifest(snapshot, newBundle)
		if err != nil {
			return ErrInvalid
		}
		newManifestHash, err := ReplicaManifestHash(snapshot, newManifest)
		if err != nil {
			return ErrInvalid
		}
		envelope, err := s.LoadKeyEnvelope(snapshot, newEpoch, recipientKeyID)
		if err != nil || envelope.ArchiveID != archiveID || envelope.RecipientKeyID != recipientKeyID ||
			envelope.RecipientPublicKeySHA256 != recipientKeyHash || envelope.KeyEpoch != strconv.FormatUint(newEpoch, 10) ||
			envelope.SignerKeyID != oldOwnerKeyID {
			return ErrRekeyHistoryIncomplete
		}
		if err := cryptoarchive.VerifyKeyEnvelopeSignature(envelope, oldOwnerSigningKey); err != nil {
			return ErrUnauthorized
		}
		envelopeBytes, err := cryptoarchive.MarshalKeyEnvelope(envelope)
		if err != nil {
			return ErrInvalid
		}
		envelopeDigest := sha256.Sum256(envelopeBytes)
		encodedEnvelopeHash := "sha256:" + hex.EncodeToString(envelopeDigest[:])
		if envelopeHash == "" {
			envelopeHash = encodedEnvelopeHash
		} else if envelopeHash != encodedEnvelopeHash {
			return ErrRekeyHistoryIncomplete
		}

		step := sha256.New()
		_, _ = step.Write([]byte(rekeyHistoryDomain))
		_, _ = step.Write(commitment[:])
		_, _ = step.Write([]byte(strconv.FormatUint(snapshot.Sequence, 10)))
		_, _ = step.Write([]byte{0})
		_, _ = step.Write([]byte(snapshot.SnapshotID))
		_, _ = step.Write([]byte{0})
		_, _ = step.Write([]byte(previousManifestHash))
		_, _ = step.Write([]byte{0})
		_, _ = step.Write([]byte(newManifestHash))
		copy(commitment[:], step.Sum(nil))
		count++
		return nil
	})
	if err != nil {
		return RekeyHistoryVerification{}, err
	}
	if count == 0 || head.Sequence != count || head.SnapshotID == "" {
		return RekeyHistoryVerification{}, ErrRekeyHistoryIncomplete
	}
	catalogHead = head
	return RekeyHistoryVerification{
		ArchiveID: archiveID, PreviousKeyEpoch: strconv.FormatUint(previousEpoch, 10),
		NewKeyEpoch: strconv.FormatUint(newEpoch, 10), SnapshotCount: strconv.FormatUint(count, 10),
		HeadSequence: strconv.FormatUint(catalogHead.Sequence, 10), HeadSnapshotID: catalogHead.SnapshotID,
		BundleSetCommitment: "sha256:" + hex.EncodeToString(commitment[:]),
		EnvelopeSHA256:      envelopeHash, RecipientKeyID: recipientKeyID,
	}, nil
}

func validRekeyRecipientKey(publicKey []byte) bool {
	curve := ecdh.X25519()
	public, err := curve.NewPublicKey(publicKey)
	if err != nil {
		return false
	}
	probe, err := curve.NewPrivateKey(bytes.Repeat([]byte{0x5a}, 32))
	if err != nil {
		return false
	}
	_, err = probe.ECDH(public)
	return err == nil
}
