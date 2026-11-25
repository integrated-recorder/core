package replication

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/lease"
)

// LeaseOfferSpec contains the caller-selected lease identity and provider.
// The offer itself is built only from a verified snapshot and public
// ciphertext manifest; callers cannot supply object hashes or reserved size.
type LeaseOfferSpec struct {
	LeaseID        string
	ProviderNodeID string
	OwnerSubjectID string
	KeyEpoch       uint64
	IssuedAt       time.Time
	ExpiresAt      time.Time
}

// BuildLeaseOffer binds a durable lease to one verified sealed snapshot and
// the exact public ciphertext mapping produced for that snapshot. It contains
// no plaintext digest beyond the snapshot's authenticated content hash, no
// local path, and no decryption key.
func BuildLeaseOffer(snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, spec LeaseOfferSpec) (lease.Offer, error) {
	if archivesnapshot.Verify(snapshot) != nil || spec.KeyEpoch == 0 || manifest.KeyEpoch != strconv.FormatUint(spec.KeyEpoch, 10) ||
		spec.IssuedAt.IsZero() || spec.ExpiresAt.IsZero() ||
		spec.IssuedAt.Location() != time.UTC || spec.ExpiresAt.Location() != time.UTC ||
		!spec.ExpiresAt.After(spec.IssuedAt) || spec.ExpiresAt.Sub(spec.IssuedAt) > lease.MaxLeaseDuration {
		return lease.Offer{}, ErrInvalid
	}
	if _, err := BundleFromPublicReplicaManifest(snapshot, manifest); err != nil {
		return lease.Offer{}, ErrInvalid
	}
	manifestHash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil {
		return lease.Offer{}, ErrInvalid
	}
	objects := make([]lease.EncryptedObject, 0, len(manifest.Objects))
	var reserved uint64
	for _, object := range manifest.Objects {
		size, parseErr := strconv.ParseUint(object.CiphertextSize, 10, 64)
		if parseErr != nil || size == 0 || size > math.MaxUint64-reserved {
			return lease.Offer{}, ErrInvalid
		}
		reserved += size
		objects = append(objects, lease.EncryptedObject{
			ObjectID: object.ObjectID, CiphertextSize: object.CiphertextSize,
			CiphertextSHA256: object.CiphertextSHA256,
		})
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].ObjectID < objects[j].ObjectID })
	offer := lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: spec.LeaseID,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: strconv.FormatUint(spec.KeyEpoch, 10),
		PlaintextManifestHash: snapshot.ContentHash, ReplicaManifestHash: manifestHash,
		EncryptedObjectSet: objects, ProviderNodeID: spec.ProviderNodeID,
		OwnerSubjectID: spec.OwnerSubjectID, ReservedBytes: strconv.FormatUint(reserved, 10),
		IssuedAt: spec.IssuedAt.Format(time.RFC3339), ExpiresAt: spec.ExpiresAt.Format(time.RFC3339),
		RenewalPolicy: "owner_approval",
	}
	// AuthorizedEvidence is deliberately left empty here. lease.SignOffer fills
	// it from the exact signed grant chain before canonical validation.
	return offer, nil
}
