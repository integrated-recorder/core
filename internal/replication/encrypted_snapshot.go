// Package replication connects immutable archive snapshots to the encrypted
// object format. It never rewrites the canonical RC12 archive. The package
// stores encrypted objects and a private local binding manifest; callers send
// only CiphertextRefs to an untrusted peer.
package replication

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
)

const (
	BundleVersion                 = "ir.encrypted-snapshot/1"
	MaxSnapshotKeyEpochs          = 64
	objectDir                     = "objects"
	bundleDir                     = "bundles"
	keyEnvelopeDir                = "key-envelopes"
	snapshotDir                   = "snapshots"
	stageDir                      = "staging"
	archiveKeyDir                 = "archive-keys"
	maxBundleSize                 = 16 << 20
	MaxCiphertextChunkBytes int64 = 4 << 20
	replicaManifestDomain         = "IR/replica-manifest/v1\x00"
)

var (
	ErrInvalid            = errors.New("encrypted snapshot is invalid")
	ErrUnauthorized       = errors.New("archive replication is not authorized")
	ErrObjectUnavailable  = errors.New("encrypted replica object is unavailable")
	ErrSnapshotNotFound   = errors.New("replica snapshot was not found")
	ErrTransferConflict   = errors.New("ciphertext transfer offset conflicts with durable state")
	ErrRestoreConflict    = errors.New("restore destination already exists")
	errCipherFileMismatch = errors.New("ciphertext object does not match its digest binding")
)

// CanonicalSource must return the exact bytes represented by snapshot and
// coordinate. EncryptObject independently checks size and digest before any
// bundle manifest is committed.
type CanonicalSource interface {
	OpenCanonical(context.Context, archiveindex.Coordinate) (io.ReadCloser, error)
}

// ArchiveDEKProtector keeps the local wrapping key inside the trusted Node
// Key store. Implementations must bind ciphertext to archive and epoch and
// must never expose their wrapping key.
type ArchiveDEKProtector interface {
	SealArchiveDEK(archiveID string, keyEpoch uint64, dataKey []byte) ([]byte, error)
	OpenArchiveDEK(archiveID string, keyEpoch uint64, sealed []byte) ([]byte, error)
}

// CiphertextRangeSource reads a bounded encrypted range from a remote peer.
// Implementations must bind each request to an authenticated peer session
// and authorize the requested archive/snapshot/object before returning bytes.
type CiphertextRangeSource interface {
	OpenCiphertextRange(context.Context, string, string, string, uint64, int64, int64) (io.ReadCloser, error)
}

// PeerLeaseObjectReader exposes one provider object as a bounded stream for
// source-side lease readback. The lease descriptor fixes the archive, snapshot,
// key epoch, provider, object ID, size and digest; no caller can widen the
// range beyond that signed object set.
type PeerLeaseObjectReader struct {
	Lease  lease.Lease
	Source CiphertextRangeSource
}

func (r PeerLeaseObjectReader) OpenCiphertext(ctx context.Context, providerNodeID, leaseID, objectID string) (io.ReadCloser, error) {
	if ctx == nil || r.Source == nil || r.Lease.Status != lease.StatusVerifying || providerNodeID == "" ||
		providerNodeID != r.Lease.Offer.Offer.ProviderNodeID || leaseID == "" || leaseID != r.Lease.Offer.Offer.LeaseID || !validObjectID(objectID) {
		return nil, ErrUnauthorized
	}
	offer := r.Lease.Offer.Offer
	keyEpoch, err := strconv.ParseUint(offer.KeyEpoch, 10, 64)
	if err != nil || keyEpoch == 0 || strconv.FormatUint(keyEpoch, 10) != offer.KeyEpoch {
		return nil, ErrInvalid
	}
	var size int64
	for _, object := range offer.EncryptedObjectSet {
		if object.ObjectID != objectID {
			continue
		}
		size, err = strconv.ParseInt(object.CiphertextSize, 10, 64)
		if err != nil || size <= 0 || strconv.FormatInt(size, 10) != object.CiphertextSize {
			return nil, ErrInvalid
		}
		break
	}
	if size <= 0 || offer.ArchiveID == "" || offer.SnapshotID == "" {
		return nil, ErrUnauthorized
	}
	streamCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	go func() {
		defer cancel()
		defer writer.Close()
		for offset := int64(0); offset < size; {
			if err := streamCtx.Err(); err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			length := size - offset
			if length > MaxCiphertextChunkBytes {
				length = MaxCiphertextChunkBytes
			}
			chunkReader, err := r.Source.OpenCiphertextRange(streamCtx, offer.ArchiveID, offer.SnapshotID, objectID, keyEpoch, offset, length)
			if err != nil || chunkReader == nil {
				if chunkReader != nil {
					_ = chunkReader.Close()
				}
				_ = writer.CloseWithError(ErrObjectUnavailable)
				return
			}
			chunk, readErr := io.ReadAll(io.LimitReader(chunkReader, length+1))
			closeErr := chunkReader.Close()
			if readErr != nil || closeErr != nil || int64(len(chunk)) != length {
				_ = writer.CloseWithError(ErrObjectUnavailable)
				return
			}
			if _, err := writer.Write(chunk); err != nil {
				return
			}
			offset += length
		}
	}()
	return &peerLeaseObjectStream{PipeReader: reader, cancel: cancel}, nil
}

type peerLeaseObjectStream struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (s *peerLeaseObjectStream) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	return s.PipeReader.Close()
}

type Authorization struct {
	SubjectID string
	Actor     authority.Principal
	Chain     []authority.SignedGrant
	Verifier  lease.Authorizer
}

type ObjectBinding struct {
	ObjectID         string
	Coordinate       archiveindex.Coordinate
	PlaintextSize    int64
	PlaintextSHA256  string
	CiphertextSize   int64
	CiphertextSHA256 string
}

// Bundle is private local metadata. Do not send its coordinate or plaintext
// digest fields to a replica; use CiphertextRefs for the lease wire object set.
type Bundle struct {
	Version    string
	ArchiveID  string
	SnapshotID string
	KeyEpoch   string
	Objects    []ObjectBinding
}

type bundleDisk struct {
	Version    string              `json:"bundle_version"`
	ArchiveID  string              `json:"archive_id"`
	SnapshotID string              `json:"snapshot_id"`
	KeyEpoch   string              `json:"key_epoch"`
	Objects    []objectBindingDisk `json:"objects"`
}

type objectBindingDisk struct {
	ObjectID         string                  `json:"object_id"`
	Coordinate       archiveindex.Coordinate `json:"coordinate"`
	PlaintextSize    int64                   `json:"plaintext_size"`
	PlaintextSHA256  string                  `json:"plaintext_sha256"`
	CiphertextSize   int64                   `json:"ciphertext_size"`
	CiphertextSHA256 string                  `json:"ciphertext_sha256"`
}

func (Bundle) MarshalJSON() ([]byte, error)        { return nil, ErrInvalid }
func (*Bundle) UnmarshalJSON([]byte) error         { return ErrInvalid }
func (ObjectBinding) MarshalJSON() ([]byte, error) { return nil, ErrInvalid }
func (*ObjectBinding) UnmarshalJSON([]byte) error  { return ErrInvalid }

type CiphertextRef struct {
	ObjectID         string `json:"object_id"`
	CiphertextSize   string `json:"ciphertext_size"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
}

// ReplicaManifest is the explicit public mapping needed to pair ciphertext
// objects with an independently delivered immutable snapshot. It contains no
// plaintext digest, local path, key material, or source credential.
type ReplicaManifest struct {
	Version    string          `json:"replica_manifest_version"`
	ArchiveID  string          `json:"archive_id"`
	SnapshotID string          `json:"snapshot_id"`
	KeyEpoch   string          `json:"key_epoch"`
	Objects    []ReplicaObject `json:"objects"`
}

type ReplicaObject struct {
	Coordinate       archiveindex.Coordinate `json:"coordinate"`
	ObjectID         string                  `json:"object_id"`
	CiphertextSize   string                  `json:"ciphertext_size"`
	CiphertextSHA256 string                  `json:"ciphertext_sha256"`
}

const ReplicaManifestVersion = "ir.replica-manifest/1"

type leaseStateReader interface {
	Get(context.Context, string) (lease.Lease, error)
	Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error)
}

type leaseCheckpointWriter interface {
	Checkpoint(context.Context, lease.SignedCheckpoint) (lease.Lease, error)
}

type leaseReplicaVerifier interface {
	VerifyReplica(context.Context, string, lease.ObjectReader) (lease.Lease, error)
}

// ReplicaCheckpointSigner signs receipts with the receiving node's protected
// Node Key. Implementations must not return or expose private key material.
type ReplicaCheckpointSigner interface {
	SignReplicaCheckpoint(lease.Checkpoint) (lease.SignedCheckpoint, error)
}

// LeaseImportAuthorizer binds inbound ciphertext to the local provider's
// durable transferring lease. Production callers should supply the local
// lease.Store and this node's configured Node ID, never request fields.
type LeaseImportAuthorizer struct {
	LeaseID           string
	ProviderNodeID    string
	Leases            leaseStateReader
	ForwardCheckpoint func(context.Context, lease.SignedCheckpoint) error
}

func (a LeaseImportAuthorizer) authorize(ctx context.Context, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, object ReplicaObject) error {
	if a.Leases == nil || a.LeaseID == "" || a.ProviderNodeID == "" {
		return ErrUnauthorized
	}
	bundle, err := BundleFromPublicReplicaManifest(snapshot, manifest)
	if err != nil {
		return ErrUnauthorized
	}
	manifestHash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil {
		return ErrUnauthorized
	}
	matched := false
	for _, candidate := range bundle.Objects {
		if candidate.Coordinate == object.Coordinate && candidate.ObjectID == object.ObjectID &&
			strconv.FormatInt(candidate.CiphertextSize, 10) == object.CiphertextSize && candidate.CiphertextSHA256 == object.CiphertextSHA256 {
			matched = true
			break
		}
	}
	if !matched {
		return ErrUnauthorized
	}
	current, err := a.Leases.Get(ctx, a.LeaseID)
	if err != nil {
		return ErrUnauthorized
	}
	offer := current.Offer.Offer
	if current.Status != lease.StatusTransferring || offer.ProviderNodeID != a.ProviderNodeID || offer.KeyEpoch != manifest.KeyEpoch ||
		offer.ArchiveID != manifest.ArchiveID || offer.SnapshotID != manifest.SnapshotID ||
		offer.PlaintextManifestHash != snapshot.ContentHash || offer.ReplicaManifestHash != manifestHash {
		return ErrUnauthorized
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, offer.ExpiresAt)
	if err != nil || !expiresAt.After(time.Now().UTC()) {
		return ErrUnauthorized
	}
	listed := false
	for _, candidate := range offer.EncryptedObjectSet {
		if candidate.ObjectID == object.ObjectID && candidate.CiphertextSize == object.CiphertextSize && candidate.CiphertextSHA256 == object.CiphertextSHA256 {
			listed = true
			break
		}
	}
	if !listed {
		return ErrUnauthorized
	}
	_, tombstones, err := a.Leases.Snapshot(ctx)
	if err != nil {
		return ErrUnauthorized
	}
	for _, tombstone := range tombstones {
		if tombstone.Signed.Tombstone.ArchiveID == manifest.ArchiveID {
			return ErrUnauthorized
		}
	}
	return nil
}

type transferPartialFile interface {
	io.Writer
	io.Seeker
	Truncate(int64) error
	Sync() error
	Close() error
}

// Store holds ciphertext objects, private mapping metadata, public-key
// envelopes, and source-only Node-Key-wrapped DEKs. Plaintext DEKs and recovery
// private keys are never stored. Directory and file permissions are checked
// on every open; symlinks are rejected.
type Store struct {
	root        string
	openPartial func(string, int64) (transferPartialFile, bool, error)
}

func Open(root string) (*Store, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, ErrInvalid
	}
	for _, name := range []string{"", objectDir, bundleDir, keyEnvelopeDir, snapshotDir, stageDir, archiveKeyDir} {
		path := root
		if name != "" {
			path = filepath.Join(root, name)
		}
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, ErrObjectUnavailable
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
			return nil, ErrInvalid
		}
	}
	return &Store{root: root, openPartial: openTransferPartial}, nil
}

// PersistArchiveDEK stores an immutable source-node copy of a DEK protected by
// the local Node Key. It is distinct from replica ciphertext and from the
// recovery-key envelope: no plaintext DEK is written to disk or sent to peers.
// Repeating the same archive/epoch/key is idempotent; changing the key at an
// existing epoch fails closed.
func (s *Store) PersistArchiveDEK(snapshot archivesnapshot.Snapshot, keyEpoch uint64, dataKey []byte, protector ArchiveDEKProtector) error {
	if s == nil || protector == nil || archivesnapshot.Verify(snapshot) != nil || !validReplicaArchiveID(snapshot.Content.ArchiveID) || keyEpoch == 0 || len(dataKey) != 32 {
		return ErrInvalid
	}
	unlock, err := s.lock(context.Background())
	if err != nil {
		return err
	}
	defer unlock()
	path := s.archiveDEKPath(snapshot.Content.ArchiveID, keyEpoch)
	if existing, err := readPrivateArchiveDEK(path); err == nil {
		plain, openErr := protector.OpenArchiveDEK(snapshot.Content.ArchiveID, keyEpoch, existing)
		clear(existing)
		if openErr != nil {
			clear(plain)
			return ErrInvalid
		}
		matches := subtle.ConstantTimeCompare(plain, dataKey) == 1
		clear(plain)
		if !matches {
			return ErrInvalid
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	sealed, err := protector.SealArchiveDEK(snapshot.Content.ArchiveID, keyEpoch, dataKey)
	if err != nil || len(sealed) < 32 || len(sealed) > 4096 {
		clear(sealed)
		return ErrObjectUnavailable
	}
	defer clear(sealed)
	if err := writeAtomicImmutable(s.root, path, sealed); err != nil {
		return err
	}
	return nil
}

// LoadArchiveDEK recovers a source-local key only through the trusted Node Key
// protector and the exact original archive/epoch binding. The caller owns and
// must clear the returned bytes after converting them into a DEK.
func (s *Store) LoadArchiveDEK(archiveID string, keyEpoch uint64, protector ArchiveDEKProtector) ([]byte, error) {
	if s == nil || protector == nil || !validReplicaArchiveID(archiveID) || keyEpoch == 0 {
		return nil, ErrInvalid
	}
	unlock, err := s.lock(context.Background())
	if err != nil {
		return nil, err
	}
	defer unlock()
	sealed, err := readPrivateArchiveDEK(s.archiveDEKPath(archiveID, keyEpoch))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrObjectUnavailable
		}
		return nil, err
	}
	defer clear(sealed)
	plain, err := protector.OpenArchiveDEK(archiveID, keyEpoch, sealed)
	if err != nil || len(plain) != 32 {
		clear(plain)
		return nil, ErrInvalid
	}
	return plain, nil
}

func readPrivateArchiveDEK(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) || info.Size() < 32 || info.Size() > 4096 {
		return nil, ErrInvalid
	}
	data, readErr := io.ReadAll(io.LimitReader(file, 4097))
	if readErr != nil || int64(len(data)) != info.Size() {
		clear(data)
		return nil, ErrObjectUnavailable
	}
	return data, nil
}

// PersistSnapshot stores the verified immutable descriptor needed to serve a
// received replica after restart. It is private metadata, not a canonical
// archive import or a new authority record. A snapshot slot is immutable: a
// conflicting descriptor for the same archive/snapshot identity fails closed.
func (s *Store) PersistSnapshot(snapshot archivesnapshot.Snapshot) error {
	if s == nil || archivesnapshot.Verify(snapshot) != nil || !validReplicaArchiveID(snapshot.Content.ArchiveID) || !validReplicaSnapshotID(snapshot.SnapshotID) {
		return ErrInvalid
	}
	encoded, err := archivesnapshot.Marshal(snapshot)
	if err != nil || len(encoded) > maxBundleSize {
		return ErrInvalid
	}
	return writeAtomicImmutable(s.root, s.snapshotPath(snapshot.Content.ArchiveID, snapshot.SnapshotID), encoded)
}

// LoadSnapshot returns a private replica descriptor only after checking file
// identity, mode, size, canonical encoding, content hash, and the requested
// archive/snapshot binding. It never consults the plaintext archive catalog.
func (s *Store) LoadSnapshot(archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	if s == nil || !validReplicaArchiveID(archiveID) || !validReplicaSnapshotID(snapshotID) {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	path := s.snapshotPath(archiveID, snapshotID)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return archivesnapshot.Snapshot{}, ErrSnapshotNotFound
		}
		return archivesnapshot.Snapshot{}, ErrObjectUnavailable
	}
	defer file.Close()
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) || info.Size() <= 0 || info.Size() > maxBundleSize {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxBundleSize+1))
	if readErr != nil || int64(len(data)) != info.Size() {
		return archivesnapshot.Snapshot{}, ErrObjectUnavailable
	}
	snapshot, err := archivesnapshot.Parse(data)
	if err != nil || snapshot.Content.ArchiveID != archiveID || snapshot.SnapshotID != snapshotID {
		return archivesnapshot.Snapshot{}, ErrInvalid
	}
	return snapshot, nil
}

// EncryptSnapshot encrypts each PRESENT object of a verified immutable
// snapshot into bounded authenticated chunks. It checks the committed
// archive.replicate grant before reading data and again before publishing the
// bundle. It is idempotent for the same snapshot/key epoch and verifies the
// supplied DEK against existing ciphertext before returning an existing
// bundle. A failed attempt may leave unreferenced ciphertext, never plaintext
// or a committed partial bundle.
func (s *Store) EncryptSnapshot(ctx context.Context, snapshot archivesnapshot.Snapshot, source CanonicalSource, dek *cryptoarchive.DEK, keyEpoch uint64, auth Authorization) (Bundle, error) {
	if s == nil || ctx == nil || source == nil || dek == nil || archivesnapshot.Verify(snapshot) != nil || keyEpoch == 0 {
		return Bundle{}, ErrInvalid
	}
	if err := authorize(ctx, auth, snapshot, "archive.replicate"); err != nil {
		return Bundle{}, err
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return Bundle{}, err
	}
	defer unlock()

	manifestPath := s.bundlePath(snapshot.Content.ArchiveID, snapshot.SnapshotID, keyEpoch)
	if err := ensurePrivateDirectories(s.root, filepath.Dir(manifestPath)); err != nil {
		return Bundle{}, err
	}
	if existing, readErr := s.readBundle(manifestPath, snapshot); readErr == nil {
		if err := s.verifyBundleKey(ctx, snapshot, existing, dek); err != nil {
			return Bundle{}, err
		}
		return existing, nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return Bundle{}, readErr
	}

	stage, err := os.MkdirTemp(filepath.Join(s.root, stageDir), "snapshot-")
	if err != nil {
		return Bundle{}, ErrObjectUnavailable
	}
	if err := os.Chmod(stage, 0700); err != nil {
		_ = os.RemoveAll(stage)
		return Bundle{}, ErrObjectUnavailable
	}
	defer os.RemoveAll(stage)

	bundle := Bundle{Version: BundleVersion, ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: strconv.FormatUint(keyEpoch, 10), Objects: make([]ObjectBinding, 0)}
	for _, object := range snapshot.Content.Objects {
		if err := ctx.Err(); err != nil {
			return Bundle{}, err
		}
		if object.State != archiveindex.CoveragePresent {
			continue
		}
		descriptor, err := cryptoarchive.DescriptorForSnapshotObject(snapshot, object.Coordinate, keyEpoch)
		if err != nil {
			return Bundle{}, ErrInvalid
		}
		reader, err := source.OpenCanonical(ctx, object.Coordinate)
		if err != nil || reader == nil {
			if reader != nil {
				_ = reader.Close()
			}
			return Bundle{}, ErrObjectUnavailable
		}
		cipherPath := filepath.Join(stage, "cipher-"+strconv.Itoa(len(bundle.Objects)))
		file, err := os.OpenFile(cipherPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = reader.Close()
			return Bundle{}, ErrObjectUnavailable
		}
		result, encryptErr := cryptoarchive.EncryptObject(ctx, dek, descriptor, reader, file)
		closeErr := reader.Close()
		if encryptErr == nil {
			encryptErr = closeErr
		}
		if encryptErr == nil {
			encryptErr = file.Sync()
		}
		if closeFileErr := file.Close(); encryptErr == nil {
			encryptErr = closeFileErr
		}
		if encryptErr != nil {
			return Bundle{}, ErrObjectUnavailable
		}
		objectID := objectIDForDigest(result.CiphertextSHA256)
		binding := ObjectBinding{
			ObjectID: objectID, Coordinate: object.Coordinate,
			PlaintextSize: result.PlaintextSize, PlaintextSHA256: result.PlaintextSHA256,
			CiphertextSize: result.CiphertextSize, CiphertextSHA256: result.CiphertextSHA256,
		}
		if err := s.publishObject(cipherPath, binding); err != nil {
			return Bundle{}, err
		}
		bundle.Objects = append(bundle.Objects, binding)
	}
	if len(bundle.Objects) == 0 || validateBundle(snapshot, bundle) != nil {
		return Bundle{}, ErrInvalid
	}
	if err := authorize(ctx, auth, snapshot, "archive.replicate"); err != nil {
		return Bundle{}, err
	}
	encoded, err := json.Marshal(bundleToDisk(bundle))
	if err != nil || len(encoded) == 0 || len(encoded) > maxBundleSize {
		return Bundle{}, ErrInvalid
	}
	if err := writeAtomicImmutable(s.root, manifestPath, encoded); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

// RekeySnapshot validates every object in the previous encrypted snapshot
// with its existing DEK, then writes a complete immutable bundle under a
// strictly newer epoch and a distinct DEK. The original key epoch and
// ciphertext remain available for rollback and audit. This is a storage
// primitive only: callers must separately distribute and verify a signed key
// envelope and complete the OwnerSubject transfer protocol. In particular,
// it does not clear a durable rekey-required authority state.
func (s *Store) RekeySnapshot(ctx context.Context, snapshot archivesnapshot.Snapshot, source CanonicalSource, previousDEK *cryptoarchive.DEK, previousEpoch uint64, nextDEK *cryptoarchive.DEK, nextEpoch uint64, auth Authorization) (Bundle, error) {
	if s == nil || ctx == nil || source == nil || previousDEK == nil || nextDEK == nil ||
		previousEpoch == 0 || nextEpoch <= previousEpoch || previousDEK.SameKey(nextDEK) ||
		archivesnapshot.Verify(snapshot) != nil {
		return Bundle{}, ErrInvalid
	}
	if err := authorize(ctx, auth, snapshot, "key.rotate"); err != nil {
		return Bundle{}, err
	}
	previous, err := s.LoadBundle(snapshot, previousEpoch)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Bundle{}, ErrObjectUnavailable
		}
		return Bundle{}, err
	}
	if err := s.verifyBundleKey(ctx, snapshot, previous, previousDEK); err != nil {
		return Bundle{}, err
	}
	return s.EncryptSnapshot(ctx, snapshot, source, nextDEK, nextEpoch, auth)
}

// CiphertextRefs returns the deliberately reduced peer-visible object set.
// Plaintext digests and source coordinates remain in the local Bundle and the
// independently verified immutable snapshot, not in a replica lease request.
func (b Bundle) CiphertextRefs() []CiphertextRef {
	refs := make([]CiphertextRef, 0, len(b.Objects))
	for _, object := range b.Objects {
		refs = append(refs, CiphertextRef{ObjectID: object.ObjectID, CiphertextSize: strconv.FormatInt(object.CiphertextSize, 10), CiphertextSHA256: object.CiphertextSHA256})
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ObjectID < refs[j].ObjectID })
	return refs
}

// PublicReplicaManifest exports only the immutable coordinate-to-ciphertext
// mapping. Plaintext size/hash are already committed by the separate snapshot
// and are intentionally not duplicated in this object.
func PublicReplicaManifest(snapshot archivesnapshot.Snapshot, bundle Bundle) (ReplicaManifest, error) {
	if validateBundle(snapshot, bundle) != nil {
		return ReplicaManifest{}, ErrInvalid
	}
	manifest := ReplicaManifest{
		Version: ReplicaManifestVersion, ArchiveID: bundle.ArchiveID,
		SnapshotID: bundle.SnapshotID, KeyEpoch: bundle.KeyEpoch,
		Objects: make([]ReplicaObject, 0, len(bundle.Objects)),
	}
	for _, object := range bundle.Objects {
		manifest.Objects = append(manifest.Objects, ReplicaObject{
			Coordinate: object.Coordinate, ObjectID: object.ObjectID,
			CiphertextSize:   strconv.FormatInt(object.CiphertextSize, 10),
			CiphertextSHA256: object.CiphertextSHA256,
		})
	}
	return manifest, nil
}

// ReplicaManifestHash binds the public coordinate-to-ciphertext map into the
// owner-signed lease offer. The snapshot separately binds the plaintext
// coordinate, size, and digest; no DEK or plaintext digest is added here.
func ReplicaManifestHash(snapshot archivesnapshot.Snapshot, manifest ReplicaManifest) (string, error) {
	if _, err := BundleFromPublicReplicaManifest(snapshot, manifest); err != nil {
		return "", ErrInvalid
	}
	document := replicaManifestHashDocument{
		Version: manifest.Version, ArchiveID: manifest.ArchiveID, SnapshotID: manifest.SnapshotID, KeyEpoch: manifest.KeyEpoch,
		Objects: make([]replicaManifestHashObject, 0, len(manifest.Objects)),
	}
	for _, object := range manifest.Objects {
		coordinate := object.Coordinate
		document.Objects = append(document.Objects, replicaManifestHashObject{
			SessionID: coordinate.SessionID, TrackID: coordinate.TrackID,
			SourceEpoch:           strconv.FormatUint(coordinate.SourceEpoch, 10),
			DiscontinuitySequence: strconv.FormatUint(coordinate.DiscontinuitySequence, 10),
			Sequence:              strconv.FormatUint(coordinate.Sequence, 10), Kind: string(coordinate.Kind),
			ObjectID: object.ObjectID, CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256,
		})
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return "", ErrInvalid
	}
	canonical, err := identityprotocol.Canonicalize(encoded)
	if err != nil {
		return "", ErrInvalid
	}
	hash := sha256.Sum256(append([]byte(replicaManifestDomain), canonical...))
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

type replicaManifestHashDocument struct {
	Version    string                      `json:"replica_manifest_version"`
	ArchiveID  string                      `json:"archive_id"`
	SnapshotID string                      `json:"snapshot_id"`
	KeyEpoch   string                      `json:"key_epoch"`
	Objects    []replicaManifestHashObject `json:"objects"`
}

type replicaManifestHashObject struct {
	SessionID             string `json:"session_id"`
	TrackID               string `json:"track_id"`
	SourceEpoch           string `json:"source_epoch"`
	DiscontinuitySequence string `json:"discontinuity_sequence"`
	Sequence              string `json:"sequence"`
	Kind                  string `json:"kind"`
	ObjectID              string `json:"object_id"`
	CiphertextSize        string `json:"ciphertext_size"`
	CiphertextSHA256      string `json:"ciphertext_sha256"`
}

// BundleFromPublicReplicaManifest reconstructs private bindings using the
// independently verified snapshot. It rejects missing, duplicate, extra, or
// mismatched coordinates before any plaintext can be restored.
func BundleFromPublicReplicaManifest(snapshot archivesnapshot.Snapshot, manifest ReplicaManifest) (Bundle, error) {
	if archivesnapshot.Verify(snapshot) != nil || manifest.Version != ReplicaManifestVersion ||
		manifest.ArchiveID != snapshot.Content.ArchiveID || manifest.SnapshotID != snapshot.SnapshotID ||
		!positiveDecimal(manifest.KeyEpoch) || manifest.Objects == nil || len(manifest.Objects) == 0 ||
		len(manifest.Objects) > len(snapshot.Content.Objects) {
		return Bundle{}, ErrInvalid
	}
	bundle := Bundle{
		Version: BundleVersion, ArchiveID: manifest.ArchiveID, SnapshotID: manifest.SnapshotID,
		KeyEpoch: manifest.KeyEpoch, Objects: make([]ObjectBinding, 0, len(manifest.Objects)),
	}
	for _, object := range manifest.Objects {
		size, err := strconv.ParseInt(object.CiphertextSize, 10, 64)
		if err != nil || size <= 0 || strconv.FormatInt(size, 10) != object.CiphertextSize {
			return Bundle{}, ErrInvalid
		}
		var selected *archivesnapshot.Object
		for i := range snapshot.Content.Objects {
			candidate := &snapshot.Content.Objects[i]
			if candidate.Coordinate == object.Coordinate && candidate.State == archiveindex.CoveragePresent {
				selected = candidate
				break
			}
		}
		if selected == nil {
			return Bundle{}, ErrInvalid
		}
		bundle.Objects = append(bundle.Objects, ObjectBinding{
			ObjectID: object.ObjectID, Coordinate: object.Coordinate,
			PlaintextSize: selected.PlaintextSize, PlaintextSHA256: selected.PlaintextSHA256,
			CiphertextSize: size, CiphertextSHA256: object.CiphertextSHA256,
		})
	}
	if validateBundle(snapshot, bundle) != nil {
		return Bundle{}, ErrInvalid
	}
	return bundle, nil
}

// CiphertextTransferOffset reports the provider's durable offset for one
// object in the matching active transfer. The caller must still authenticate
// and authorize the peer request at its transport boundary.
func (s *Store) CiphertextTransferOffset(ctx context.Context, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, object ReplicaObject) (string, error) {
	if s == nil || ctx == nil || authorization.authorize(ctx, snapshot, manifest, object) != nil {
		return "", ErrUnauthorized
	}
	current, err := authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || current.Status != lease.StatusTransferring {
		return "", ErrUnauthorized
	}
	for _, progress := range current.Progress {
		if progress.ObjectID == object.ObjectID {
			return progress.CommittedOffset, nil
		}
	}
	return "", ErrUnauthorized
}

// ImportCiphertextChunk durably appends at most MaxCiphertextChunkBytes to a
// lease-bound staging object and records a provider-signed offset only after
// the file data is synced. A completed object is verified and atomically
// published before its final checkpoint can move the lease to verifying.
// Retrying after a crash truncates uncheckpointed tail bytes; it never lowers
// a durable lease offset.
func (s *Store) ImportCiphertextChunk(ctx context.Context, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, object ReplicaObject, offset string, source io.Reader, signer ReplicaCheckpointSigner) (lease.SignedCheckpoint, error) {
	if s == nil || ctx == nil || source == nil || signer == nil {
		return lease.SignedCheckpoint{}, ErrInvalid
	}
	if err := authorization.authorize(ctx, snapshot, manifest, object); err != nil {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	writer, ok := authorization.Leases.(leaseCheckpointWriter)
	if !ok {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	if err := validateImportObject(snapshot, manifest, object); err != nil {
		return lease.SignedCheckpoint{}, err
	}
	size, _ := strconv.ParseInt(object.CiphertextSize, 10, 64)
	requestedOffset, err := parseTransferOffset(offset, size)
	if err != nil {
		return lease.SignedCheckpoint{}, err
	}
	current, err := authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || current.Status != lease.StatusTransferring {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	progress, ok := transferProgress(current, object.ObjectID)
	if !ok {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	committed, err := strconv.ParseInt(progress.CommittedOffset, 10, 64)
	if err != nil || committed != requestedOffset {
		return lease.SignedCheckpoint{}, ErrTransferConflict
	}
	if err := ctx.Err(); err != nil {
		return lease.SignedCheckpoint{}, err
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return lease.SignedCheckpoint{}, err
	}
	defer unlock()
	// A second receiver process may have advanced the lease while this request
	// waited for the storage lock. Re-read both authorization and offset before
	// touching any bytes so a stale request cannot truncate committed progress.
	if err := authorization.authorize(ctx, snapshot, manifest, object); err != nil {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	current, err = authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || current.Status != lease.StatusTransferring {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	progress, ok = transferProgress(current, object.ObjectID)
	if !ok {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	committed, err = strconv.ParseInt(progress.CommittedOffset, 10, 64)
	if err != nil || committed != requestedOffset {
		return lease.SignedCheckpoint{}, ErrTransferConflict
	}

	binding := ObjectBinding{ObjectID: object.ObjectID, CiphertextSize: size, CiphertextSHA256: object.CiphertextSHA256}
	finalPath := filepath.Join(s.root, objectDir, object.ObjectID+".irac")
	var corruptObject os.FileInfo
	if existing, inspectErr := inspectCipherFile(finalPath, size, object.CiphertextSHA256); inspectErr == nil && existing != nil {
		if committed >= size {
			return lease.SignedCheckpoint{}, ErrTransferConflict
		}
		if err := authorization.authorize(ctx, snapshot, manifest, object); err != nil {
			return lease.SignedCheckpoint{}, ErrUnauthorized
		}
		checkpoint, err := s.signTransferCheckpoint(signer, current, object.ObjectID, size, progress, time.Now().UTC().Truncate(time.Second).Format(time.RFC3339))
		if err != nil {
			return lease.SignedCheckpoint{}, err
		}
		if _, err := writer.Checkpoint(ctx, checkpoint); err != nil {
			return lease.SignedCheckpoint{}, err
		}
		if err := authorization.forwardCheckpoint(ctx, checkpoint); err != nil {
			return checkpoint, err
		}
		partialPath := s.transferPartialPath(authorization.LeaseID, object.ObjectID)
		if err := os.Remove(partialPath); err == nil {
			if err := syncDirectory(filepath.Dir(partialPath)); err != nil {
				return lease.SignedCheckpoint{}, ErrObjectUnavailable
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return lease.SignedCheckpoint{}, ErrObjectUnavailable
		}
		return checkpoint, nil
	} else if errors.Is(inspectErr, errCipherFileMismatch) {
		// Keep the old bytes until an authorized complete transfer is staged and
		// verified. Unsafe paths (including symlinks) are never repair targets.
		corruptObject = existing
	} else if inspectErr != nil {
		return lease.SignedCheckpoint{}, ErrInvalid
	}

	partialPath := s.transferPartialPath(authorization.LeaseID, object.ObjectID)
	partial, created, err := s.openPartial(partialPath, requestedOffset)
	if err != nil {
		return lease.SignedCheckpoint{}, err
	}
	if created {
		if err := syncDirectory(filepath.Dir(partialPath)); err != nil {
			_ = partial.Close()
			return lease.SignedCheckpoint{}, ErrObjectUnavailable
		}
	}
	if _, err := partial.Seek(requestedOffset, io.SeekStart); err != nil {
		_ = partial.Close()
		return lease.SignedCheckpoint{}, ErrObjectUnavailable
	}
	written, copyErr := io.Copy(partial, io.LimitReader(&contextReader{ctx: ctx, reader: source}, MaxCiphertextChunkBytes+1))
	if copyErr != nil || written == 0 || written > MaxCiphertextChunkBytes || requestedOffset+written > size {
		_ = partial.Truncate(requestedOffset)
		_ = partial.Sync()
		_ = partial.Close()
		if ctx.Err() != nil {
			return lease.SignedCheckpoint{}, ctx.Err()
		}
		if copyErr != nil {
			return lease.SignedCheckpoint{}, ErrObjectUnavailable
		}
		return lease.SignedCheckpoint{}, ErrInvalid
	}
	endOffset := requestedOffset + written
	if err := partial.Sync(); err != nil {
		_ = partial.Close()
		return lease.SignedCheckpoint{}, ErrObjectUnavailable
	}
	if err := partial.Close(); err != nil {
		return lease.SignedCheckpoint{}, ErrObjectUnavailable
	}
	if endOffset == size {
		if err := verifyCipherFile(partialPath, size, object.CiphertextSHA256); err != nil {
			return lease.SignedCheckpoint{}, ErrInvalid
		}
		if err := authorization.authorize(ctx, snapshot, manifest, object); err != nil {
			return lease.SignedCheckpoint{}, ErrUnauthorized
		}
		var publishErr error
		if corruptObject != nil {
			publishErr = s.replaceCorruptObject(partialPath, binding, corruptObject)
		} else {
			publishErr = s.publishObject(partialPath, binding)
		}
		if publishErr != nil {
			return lease.SignedCheckpoint{}, publishErr
		}
	}

	if err := authorization.authorize(ctx, snapshot, manifest, object); err != nil {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	checkpoint, err := s.signTransferCheckpoint(signer, current, object.ObjectID, endOffset, progress, time.Now().UTC().Truncate(time.Second).Format(time.RFC3339))
	if err != nil {
		return lease.SignedCheckpoint{}, err
	}
	if _, err := writer.Checkpoint(ctx, checkpoint); err != nil {
		return lease.SignedCheckpoint{}, err
	}
	if err := authorization.forwardCheckpoint(ctx, checkpoint); err != nil {
		return checkpoint, err
	}
	if endOffset == size {
		if err := os.Remove(partialPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return lease.SignedCheckpoint{}, ErrObjectUnavailable
		}
		if err := syncDirectory(filepath.Dir(partialPath)); err != nil {
			return lease.SignedCheckpoint{}, ErrObjectUnavailable
		}
	}
	return checkpoint, nil
}

// ImportCiphertextObjectFromPeer resumes one lease-authorized object transfer
// by requesting bounded ranges from an authenticated peer. Each chunk is
// fsynced and checkpointed before the next range is requested, so a crash or
// peer disconnect can resume from the provider's durable offset. It does not
// finalize the replica; callers finalize only after every manifest object has
// been imported.
func (s *Store) ImportCiphertextObjectFromPeer(ctx context.Context, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, object ReplicaObject, source CiphertextRangeSource, signer ReplicaCheckpointSigner) ([]lease.SignedCheckpoint, error) {
	if s == nil || ctx == nil || source == nil || signer == nil || validateImportObject(snapshot, manifest, object) != nil {
		return nil, ErrInvalid
	}
	size, err := strconv.ParseInt(object.CiphertextSize, 10, 64)
	if err != nil || size <= 0 {
		return nil, ErrInvalid
	}
	keyEpoch, err := strconv.ParseUint(manifest.KeyEpoch, 10, 64)
	if err != nil || keyEpoch == 0 || strconv.FormatUint(keyEpoch, 10) != manifest.KeyEpoch {
		return nil, ErrInvalid
	}
	checkpoints := make([]lease.SignedCheckpoint, 0)
	for {
		if err := ctx.Err(); err != nil {
			return checkpoints, err
		}
		complete, err := s.peerImportObjectComplete(ctx, authorization, snapshot, manifest, object, size)
		if err != nil {
			return checkpoints, err
		}
		if complete {
			return checkpoints, nil
		}
		offsetText, err := s.CiphertextTransferOffset(ctx, authorization, snapshot, manifest, object)
		if err != nil {
			return checkpoints, err
		}
		offset, err := parseTransferOffset(offsetText, size)
		if err != nil {
			return checkpoints, err
		}
		if offset == size {
			return checkpoints, nil
		}
		length := size - offset
		if length > MaxCiphertextChunkBytes {
			length = MaxCiphertextChunkBytes
		}
		reader, err := source.OpenCiphertextRange(ctx, manifest.ArchiveID, manifest.SnapshotID, object.ObjectID, keyEpoch, offset, length)
		if err != nil {
			return checkpoints, err
		}
		chunk, readErr := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: reader}, length+1))
		closeErr := reader.Close()
		if readErr != nil || int64(len(chunk)) != length {
			if ctx.Err() != nil {
				return checkpoints, ctx.Err()
			}
			return checkpoints, ErrObjectUnavailable
		}
		if closeErr != nil {
			return checkpoints, ErrObjectUnavailable
		}
		checkpoint, importErr := s.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, object, offsetText, bytes.NewReader(chunk), signer)
		if importErr != nil {
			return checkpoints, importErr
		}
		committed, err := strconv.ParseInt(checkpoint.Checkpoint.CommittedOffset, 10, 64)
		if err != nil || committed <= offset || committed > offset+length || committed > size {
			return checkpoints, ErrTransferConflict
		}
		checkpoints = append(checkpoints, checkpoint)
		if committed == size {
			return checkpoints, nil
		}
	}
}

// ImportReplicaFromPeer imports every object in an already accepted durable
// lease, verifies the local readback, and finalizes the replica. Lease offer
// creation/acceptance and owner authorization remain explicit caller steps;
// this method only runs after the provider is in transferring, verifying or
// active state and never changes archive plaintext.
func (s *Store) ImportReplicaFromPeer(ctx context.Context, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, source CiphertextRangeSource, signer ReplicaCheckpointSigner) (Bundle, error) {
	if s == nil || ctx == nil || source == nil || signer == nil || authorization.Leases == nil || archivesnapshot.Verify(snapshot) != nil {
		return Bundle{}, ErrInvalid
	}
	if _, err := BundleFromPublicReplicaManifest(snapshot, manifest); err != nil {
		return Bundle{}, ErrInvalid
	}
	manifestHash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil {
		return Bundle{}, ErrInvalid
	}
	current, err := authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || current.Offer.Offer.LeaseID != authorization.LeaseID || !leaseMatchesReplica(current, authorization, snapshot, manifest, manifestHash) ||
		(current.Status != lease.StatusTransferring && current.Status != lease.StatusVerifying && current.Status != lease.StatusActive) {
		return Bundle{}, ErrUnauthorized
	}
	for _, object := range manifest.Objects {
		if _, err := s.ImportCiphertextObjectFromPeer(ctx, authorization, snapshot, manifest, object, source, signer); err != nil {
			return Bundle{}, err
		}
	}
	// Re-forward the latest durable checkpoint for every object after the full
	// pass. This repairs a lost source receipt when the provider had committed
	// bytes locally but the peer response failed; exact checkpoint replay is
	// idempotent at the source lease store.
	if authorization.ForwardCheckpoint != nil {
		current, err := authorization.Leases.Get(ctx, authorization.LeaseID)
		if err != nil {
			return Bundle{}, ErrUnauthorized
		}
		for _, checkpoint := range current.Checkpoints {
			if err := authorization.ForwardCheckpoint(ctx, checkpoint); err != nil {
				return Bundle{}, err
			}
		}
	}
	return s.FinalizeImportedReplica(ctx, authorization, snapshot, manifest)
}

func (a LeaseImportAuthorizer) forwardCheckpoint(ctx context.Context, checkpoint lease.SignedCheckpoint) error {
	if a.ForwardCheckpoint == nil {
		return nil
	}
	if ctx == nil || checkpoint.Checkpoint.LeaseID != a.LeaseID || checkpoint.Checkpoint.ProviderNodeID != a.ProviderNodeID {
		return ErrUnauthorized
	}
	return a.ForwardCheckpoint(ctx, checkpoint)
}

func (s *Store) peerImportObjectComplete(ctx context.Context, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, object ReplicaObject, size int64) (bool, error) {
	if authorization.Leases == nil {
		return false, ErrUnauthorized
	}
	current, err := authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil {
		return false, ErrUnauthorized
	}
	if current.Status == lease.StatusTransferring {
		return false, nil
	}
	manifestHash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil || (current.Status != lease.StatusVerifying && current.Status != lease.StatusActive) || !leaseMatchesReplica(current, authorization, snapshot, manifest, manifestHash) {
		return false, ErrUnauthorized
	}
	progress, found := transferProgress(current, object.ObjectID)
	if !found || progress.CommittedOffset != strconv.FormatInt(size, 10) {
		return false, ErrUnauthorized
	}
	if err := verifyCipherFile(filepath.Join(s.root, objectDir, object.ObjectID+".irac"), size, object.CiphertextSHA256); err != nil {
		return false, ErrObjectUnavailable
	}
	return true, nil
}

// FinalizeImportedReplica performs the lease's full ciphertext readback and
// atomically persists the private coordinate binding needed to recover the
// replica after restart. It does not publish plaintext or receive a DEK.
func (s *Store) FinalizeImportedReplica(ctx context.Context, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest) (Bundle, error) {
	if s == nil || ctx == nil || authorization.Leases == nil || archivesnapshot.Verify(snapshot) != nil {
		return Bundle{}, ErrInvalid
	}
	bundle, err := BundleFromPublicReplicaManifest(snapshot, manifest)
	if err != nil {
		return Bundle{}, ErrInvalid
	}
	manifestHash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil {
		return Bundle{}, ErrInvalid
	}
	current, err := authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || !leaseMatchesReplica(current, authorization, snapshot, manifest, manifestHash) {
		return Bundle{}, ErrUnauthorized
	}
	if current.Status == lease.StatusVerifying {
		verifier, ok := authorization.Leases.(leaseReplicaVerifier)
		if !ok {
			return Bundle{}, ErrUnauthorized
		}
		reader := leaseCiphertextReader{store: s, providerNodeID: authorization.ProviderNodeID, leaseID: authorization.LeaseID, objects: bundle.CiphertextRefs()}
		if _, err := verifier.VerifyReplica(ctx, authorization.LeaseID, reader); err != nil {
			return Bundle{}, err
		}
	} else if current.Status != lease.StatusActive {
		return Bundle{}, ErrUnauthorized
	}
	current, err = authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || current.Status != lease.StatusActive || !leaseMatchesReplica(current, authorization, snapshot, manifest, manifestHash) {
		return Bundle{}, ErrUnauthorized
	}
	if leaseHasArchiveTombstone(ctx, authorization.Leases, manifest.ArchiveID) {
		return Bundle{}, ErrUnauthorized
	}
	for _, object := range bundle.Objects {
		if err := verifyCipherFile(filepath.Join(s.root, objectDir, object.ObjectID+".irac"), object.CiphertextSize, object.CiphertextSHA256); err != nil {
			return Bundle{}, ErrInvalid
		}
	}
	keyEpoch, err := strconv.ParseUint(bundle.KeyEpoch, 10, 64)
	if err != nil || keyEpoch == 0 {
		return Bundle{}, ErrInvalid
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return Bundle{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return Bundle{}, err
	}
	current, err = authorization.Leases.Get(ctx, authorization.LeaseID)
	if err != nil || current.Status != lease.StatusActive || !leaseMatchesReplica(current, authorization, snapshot, manifest, manifestHash) || leaseHasArchiveTombstone(ctx, authorization.Leases, manifest.ArchiveID) {
		return Bundle{}, ErrUnauthorized
	}
	for _, object := range bundle.Objects {
		if err := verifyCipherFile(filepath.Join(s.root, objectDir, object.ObjectID+".irac"), object.CiphertextSize, object.CiphertextSHA256); err != nil {
			return Bundle{}, ErrInvalid
		}
	}
	if err := s.PersistSnapshot(snapshot); err != nil {
		return Bundle{}, err
	}
	path := s.bundlePath(bundle.ArchiveID, bundle.SnapshotID, keyEpoch)
	disk, err := json.Marshal(bundleToDisk(bundle))
	if err != nil || len(disk) > maxBundleSize {
		return Bundle{}, ErrInvalid
	}
	if err := writeAtomicImmutable(s.root, path, disk); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

// LoadBundle reloads the private local binding for an immutable snapshot.
// This metadata contains no DEK; callers still need a recovery-authorized key
// envelope before RestoreSnapshot can produce plaintext.
func (s *Store) LoadBundle(snapshot archivesnapshot.Snapshot, keyEpoch uint64) (Bundle, error) {
	if s == nil || keyEpoch == 0 || archivesnapshot.Verify(snapshot) != nil {
		return Bundle{}, ErrInvalid
	}
	return s.readBundle(s.bundlePath(snapshot.Content.ArchiveID, snapshot.SnapshotID, keyEpoch), snapshot)
}

// KeyEpochs returns only epochs whose private bundle metadata validates
// against the supplied immutable snapshot. The result is newest-first and
// bounded so callers can offer a browser a small public candidate list.
// Ciphertext and decryption keys are never returned by this method.
func (s *Store) KeyEpochs(snapshot archivesnapshot.Snapshot) ([]uint64, error) {
	if s == nil || archivesnapshot.Verify(snapshot) != nil ||
		!validReplicaArchiveID(snapshot.Content.ArchiveID) || !validReplicaSnapshotID(snapshot.SnapshotID) {
		return nil, ErrInvalid
	}
	directory := filepath.Join(s.root, bundleDir, snapshot.Content.ArchiveID, snapshot.SnapshotID)
	for _, path := range []string{
		filepath.Join(s.root, bundleDir),
		filepath.Join(s.root, bundleDir, snapshot.Content.ArchiveID),
		directory,
	} {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return []uint64{}, nil
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
			return nil, ErrInvalid
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, ErrObjectUnavailable
	}
	// Snapshot epoch directories are private local metadata. Bound enumeration
	// even if the directory is malformed or was modified outside the Store.
	if len(entries) > MaxSnapshotKeyEpochs {
		return nil, ErrInvalid
	}
	epochs := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		text := strings.TrimSuffix(name, ".json")
		epoch, parseErr := strconv.ParseUint(text, 10, 64)
		if parseErr != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != text {
			continue
		}
		bundle, loadErr := s.LoadBundle(snapshot, epoch)
		if loadErr != nil {
			continue
		}
		if _, manifestErr := PublicReplicaManifest(snapshot, bundle); manifestErr != nil {
			continue
		}
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] > epochs[j] })
	if len(epochs) > MaxSnapshotKeyEpochs {
		epochs = epochs[:MaxSnapshotKeyEpochs]
	}
	return epochs, nil
}

// PersistKeyEnvelope durably stores the signed public-key envelope for one
// archive key epoch and recovery recipient. The envelope contains encrypted
// DEK material, never the DEK or a recovery private key. This method validates
// its canonical format, archive binding, and Ed25519 signature against the
// supplied public key, but is not an identity-authorization decision: callers
// and clients must resolve that key from current, unfrozen identity history.
// A recipient/epoch slot is immutable; a rotation must use a new key epoch.
func (s *Store) PersistKeyEnvelope(snapshot archivesnapshot.Snapshot, envelope cryptoarchive.KeyEnvelope, signerPublicKey ed25519.PublicKey) error {
	if s == nil || archivesnapshot.Verify(snapshot) != nil ||
		envelope.ArchiveID != snapshot.Content.ArchiveID {
		return ErrInvalid
	}
	epoch, err := strconv.ParseUint(envelope.KeyEpoch, 10, 64)
	if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != envelope.KeyEpoch {
		return ErrInvalid
	}
	encoded, err := cryptoarchive.MarshalKeyEnvelope(envelope)
	if err != nil {
		return ErrInvalid
	}
	parsed, err := cryptoarchive.ParseKeyEnvelope(encoded)
	if err != nil || parsed != envelope {
		return ErrInvalid
	}
	if err := cryptoarchive.VerifyKeyEnvelopeSignature(envelope, signerPublicKey); err != nil {
		return ErrInvalid
	}
	path := s.keyEnvelopePath(snapshot.Content.ArchiveID, envelope.RecipientKeyID, epoch)
	if err := writeAtomicImmutable(s.root, path, encoded); err != nil {
		return err
	}
	return nil
}

// LoadKeyEnvelope returns only the signed envelope bound to the requested
// archive, key epoch, and recovery recipient. It does not prove that the
// signer is currently authorized; consumers must resolve that from validated
// identity history and reject stale or forked histories before opening it.
func (s *Store) LoadKeyEnvelope(snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientKeyID string) (cryptoarchive.KeyEnvelope, error) {
	if s == nil || keyEpoch == 0 || archivesnapshot.Verify(snapshot) != nil || !validRecipientKeyID(recipientKeyID) {
		return cryptoarchive.KeyEnvelope{}, ErrInvalid
	}
	path := s.keyEnvelopePath(snapshot.Content.ArchiveID, recipientKeyID, keyEpoch)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cryptoarchive.KeyEnvelope{}, ErrObjectUnavailable
		}
		return cryptoarchive.KeyEnvelope{}, ErrObjectUnavailable
	}
	defer file.Close()
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) || info.Size() <= 0 || info.Size() > 16<<10 {
		return cryptoarchive.KeyEnvelope{}, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(file, (16<<10)+1))
	if err != nil || len(data) > 16<<10 {
		return cryptoarchive.KeyEnvelope{}, ErrInvalid
	}
	envelope, err := cryptoarchive.ParseKeyEnvelope(data)
	if err != nil || envelope.ArchiveID != snapshot.Content.ArchiveID || envelope.RecipientKeyID != recipientKeyID || envelope.KeyEpoch != strconv.FormatUint(keyEpoch, 10) {
		return cryptoarchive.KeyEnvelope{}, ErrInvalid
	}
	return envelope, nil
}

type leaseCiphertextReader struct {
	store          *Store
	providerNodeID string
	leaseID        string
	objects        []CiphertextRef
}

func (r leaseCiphertextReader) OpenCiphertext(ctx context.Context, providerNodeID, leaseID, objectID string) (io.ReadCloser, error) {
	if r.store == nil || providerNodeID != r.providerNodeID || leaseID != r.leaseID {
		return nil, ErrObjectUnavailable
	}
	for _, object := range r.objects {
		if object.ObjectID == objectID {
			return r.store.openCiphertext(ctx, objectID)
		}
	}
	return nil, ErrObjectUnavailable
}

func leaseMatchesReplica(current lease.Lease, authorization LeaseImportAuthorizer, snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, manifestHash string) bool {
	offer := current.Offer.Offer
	if offer.ProviderNodeID != authorization.ProviderNodeID || offer.KeyEpoch != manifest.KeyEpoch || offer.ArchiveID != manifest.ArchiveID || offer.SnapshotID != manifest.SnapshotID ||
		offer.PlaintextManifestHash != snapshot.ContentHash || offer.ReplicaManifestHash != manifestHash || len(offer.EncryptedObjectSet) != len(manifest.Objects) {
		return false
	}
	expiresAt := offer.ExpiresAt
	if len(current.Renewals) > 0 {
		expiresAt = current.Renewals[len(current.Renewals)-1].Renewal.ExpiresAt
	}
	expires, err := time.Parse(time.RFC3339, expiresAt)
	if err != nil || !expires.After(time.Now().UTC()) {
		return false
	}
	leasedObjects := make(map[string]lease.EncryptedObject, len(offer.EncryptedObjectSet))
	for _, object := range offer.EncryptedObjectSet {
		if _, duplicate := leasedObjects[object.ObjectID]; duplicate {
			return false
		}
		leasedObjects[object.ObjectID] = object
	}
	for _, object := range manifest.Objects {
		leased, ok := leasedObjects[object.ObjectID]
		if !ok || leased.CiphertextSize != object.CiphertextSize || leased.CiphertextSHA256 != object.CiphertextSHA256 {
			return false
		}
		delete(leasedObjects, object.ObjectID)
	}
	return len(leasedObjects) == 0
}

func leaseHasArchiveTombstone(ctx context.Context, state leaseStateReader, archiveID string) bool {
	_, tombstones, err := state.Snapshot(ctx)
	if err != nil {
		return true
	}
	for _, tombstone := range tombstones {
		if tombstone.Signed.Tombstone.ArchiveID == archiveID {
			return true
		}
	}
	return false
}

func validateImportObject(snapshot archivesnapshot.Snapshot, manifest ReplicaManifest, object ReplicaObject) error {
	bundle, err := BundleFromPublicReplicaManifest(snapshot, manifest)
	if err != nil {
		return ErrInvalid
	}
	for _, candidate := range bundle.Objects {
		if candidate.Coordinate == object.Coordinate && candidate.ObjectID == object.ObjectID &&
			strconv.FormatInt(candidate.CiphertextSize, 10) == object.CiphertextSize && candidate.CiphertextSHA256 == object.CiphertextSHA256 {
			return nil
		}
	}
	return ErrInvalid
}

func (s *Store) transferPartialPath(leaseID, objectID string) string {
	return filepath.Join(s.root, stageDir, "transfer-"+leaseID+"-"+objectID+".part")
}

func openTransferPartial(path string, offset int64) (transferPartialFile, bool, error) {
	info, err := os.Lstat(path)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		if offset != 0 {
			return nil, false, ErrObjectUnavailable
		}
		file, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if createErr != nil {
			return nil, false, ErrObjectUnavailable
		}
		if err := file.Chmod(0600); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return nil, false, ErrObjectUnavailable
		}
		return file, true, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return nil, false, ErrInvalid
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, false, ErrObjectUnavailable
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0600 || opened.Size() < offset {
		_ = file.Close()
		return nil, false, ErrInvalid
	}
	if opened.Size() > offset {
		if err := file.Truncate(offset); err != nil || file.Sync() != nil {
			_ = file.Close()
			return nil, false, ErrObjectUnavailable
		}
	}
	return file, created, nil
}

func transferProgress(current lease.Lease, objectID string) (lease.Progress, bool) {
	for _, progress := range current.Progress {
		if progress.ObjectID == objectID {
			return progress, true
		}
	}
	return lease.Progress{}, false
}

func parseTransferOffset(value string, size int64) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || parsed > size || strconv.FormatInt(parsed, 10) != value {
		return 0, ErrInvalid
	}
	return parsed, nil
}

func (s *Store) signTransferCheckpoint(signer ReplicaCheckpointSigner, current lease.Lease, objectID string, offset int64, progress lease.Progress, recordedAt string) (lease.SignedCheckpoint, error) {
	at, err := time.Parse(time.RFC3339, recordedAt)
	if err != nil || at.Location() != time.UTC || at.Format(time.RFC3339) != recordedAt {
		return lease.SignedCheckpoint{}, ErrInvalid
	}
	if previousAt := progress.UpdatedAt; previousAt != "" {
		previous, err := time.Parse(time.RFC3339, previousAt)
		if err != nil || at.Before(previous) {
			return lease.SignedCheckpoint{}, ErrTransferConflict
		}
	}
	checkpoint := lease.Checkpoint{
		Version: lease.ProtocolVersion, LeaseID: current.Offer.Offer.LeaseID,
		OfferHash: current.OfferHash, ObjectID: objectID,
		CommittedOffset: strconv.FormatInt(offset, 10), RecordedAt: recordedAt,
	}
	signed, err := signer.SignReplicaCheckpoint(checkpoint)
	if err != nil {
		return lease.SignedCheckpoint{}, err
	}
	if signed.Checkpoint.LeaseID != checkpoint.LeaseID || signed.Checkpoint.OfferHash != checkpoint.OfferHash || signed.Checkpoint.ObjectID != checkpoint.ObjectID || signed.Checkpoint.CommittedOffset != checkpoint.CommittedOffset || signed.Checkpoint.RecordedAt != checkpoint.RecordedAt || signed.Checkpoint.ProviderNodeID != current.Offer.Offer.ProviderNodeID {
		return lease.SignedCheckpoint{}, ErrUnauthorized
	}
	return signed, nil
}

// OpenCiphertextForPeer opens a ciphertext object only after checking current
// OwnerSubject/identity freshness and an exact grant for this archive and
// snapshot. The object must be in the validated bundle. A peer transport must
// additionally apply request quotas and range/backpressure limits.
func (s *Store) OpenCiphertextForPeer(ctx context.Context, snapshot archivesnapshot.Snapshot, bundle Bundle, auth Authorization, action, objectID string) (io.ReadCloser, error) {
	if s == nil || validateBundle(snapshot, bundle) != nil || action != "archive.view" && action != "archive.export" && action != "archive.replicate" {
		return nil, ErrInvalid
	}
	if err := authorize(ctx, auth, snapshot, action); err != nil {
		return nil, err
	}
	found := false
	for _, object := range bundle.Objects {
		if object.ObjectID == objectID {
			found = true
			break
		}
	}
	if !found {
		return nil, ErrObjectUnavailable
	}
	return s.openCiphertext(ctx, objectID)
}

// OpenCiphertextRangeForPeer opens a bounded ciphertext range for a resumable
// transfer. It applies the same exact-snapshot authorization and full-object
// digest check as OpenCiphertextForPeer before exposing any bytes.
func (s *Store) OpenCiphertextRangeForPeer(ctx context.Context, snapshot archivesnapshot.Snapshot, bundle Bundle, auth Authorization, action, objectID string, offset, length int64) (io.ReadCloser, error) {
	if s == nil || ctx == nil || offset < 0 || length <= 0 || length > MaxCiphertextChunkBytes || validateBundle(snapshot, bundle) != nil || action != "archive.view" && action != "archive.export" && action != "archive.replicate" {
		return nil, ErrInvalid
	}
	if err := authorize(ctx, auth, snapshot, action); err != nil {
		return nil, err
	}
	var size int64
	found := false
	for _, object := range bundle.Objects {
		if object.ObjectID == objectID {
			size, found = object.CiphertextSize, true
			break
		}
	}
	if !found || offset > size || length > size-offset {
		return nil, ErrObjectUnavailable
	}
	file, err := s.openCiphertextForRange(ctx, objectID, size)
	if err != nil {
		return nil, err
	}
	return ciphertextRange{Reader: io.NewSectionReader(file, offset, length), closer: file}, nil
}

func (s *Store) openCiphertextForRange(ctx context.Context, objectID string, expectedSize int64) (*os.File, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || !validObjectID(objectID) || expectedSize <= 0 || expectedSize > archiveindex.MaxPayloadBytes+(2<<20) {
		return nil, ErrObjectUnavailable
	}
	path := filepath.Join(s.root, objectDir, objectID+".irac")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || info.Size() != expectedSize {
		return nil, ErrObjectUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrObjectUnavailable
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Size() != expectedSize || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0600 {
		_ = file.Close()
		return nil, ErrObjectUnavailable
	}
	return file, nil
}

type ciphertextRange struct {
	io.Reader
	closer io.Closer
}

func (r ciphertextRange) Close() error { return r.closer.Close() }

func (s *Store) openCiphertext(ctx context.Context, objectID string) (io.ReadCloser, error) {
	if s == nil || ctx == nil || !validObjectID(objectID) || ctx.Err() != nil {
		return nil, ErrObjectUnavailable
	}
	path := filepath.Join(s.root, objectDir, objectID+".irac")
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrObjectUnavailable
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) || info.Size() <= 0 || info.Size() > archiveindex.MaxPayloadBytes+(2<<20) {
		_ = file.Close()
		return nil, ErrObjectUnavailable
	}
	digest, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(objectID, "ob_"))
	if err := verifyCipherFile(path, info.Size(), "sha256:"+hex.EncodeToString(digest)); err != nil {
		_ = file.Close()
		return nil, ErrObjectUnavailable
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, ErrObjectUnavailable
	}
	return file, nil
}

type RestoredObject struct {
	Coordinate archiveindex.Coordinate `json:"coordinate"`
	Path       string                  `json:"path"`
	Size       int64                   `json:"size"`
	SHA256     string                  `json:"sha256"`
}

type RestoreManifest struct {
	Version          string             `json:"restore_version"`
	ArchiveID        string             `json:"archive_id"`
	SnapshotID       string             `json:"snapshot_id"`
	PlaybackPlaylist string             `json:"playback_playlist,omitempty"`
	TrackPlaylists   []RestoredPlaylist `json:"track_playlists,omitempty"`
	Objects          []RestoredObject   `json:"objects"`
}

// RestoreSnapshot decrypts every object into a private sibling staging
// directory and atomically renames the complete directory into destination.
// The destination must not exist. Any size, hash, coordinate, key, or tag
// failure discards staging and leaves no partial plaintext tree.
func (s *Store) RestoreSnapshot(ctx context.Context, snapshot archivesnapshot.Snapshot, bundle Bundle, dek *cryptoarchive.DEK, destination string, auth Authorization) (RestoreManifest, error) {
	if s == nil || ctx == nil || dek == nil || destination == "" || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || archivesnapshot.Verify(snapshot) != nil || validateBundle(snapshot, bundle) != nil {
		return RestoreManifest{}, ErrInvalid
	}
	if err := authorize(ctx, auth, snapshot, "archive.restore"); err != nil {
		return RestoreManifest{}, err
	}
	if _, err := os.Lstat(destination); err == nil {
		return RestoreManifest{}, ErrRestoreConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	parent := filepath.Dir(destination)
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	stage, err := os.MkdirTemp(parent, ".ir-restore-")
	if err != nil {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	if err := os.Chmod(stage, 0700); err != nil {
		_ = os.RemoveAll(stage)
		return RestoreManifest{}, ErrObjectUnavailable
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(stage)
		}
	}()
	objectsDir := filepath.Join(stage, "objects")
	if err := os.Mkdir(objectsDir, 0700); err != nil {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	manifest := RestoreManifest{Version: "ir.restored-snapshot/1", ArchiveID: bundle.ArchiveID, SnapshotID: bundle.SnapshotID, Objects: make([]RestoredObject, 0, len(bundle.Objects))}
	for _, binding := range bundle.Objects {
		if err := ctx.Err(); err != nil {
			return RestoreManifest{}, err
		}
		ciphertext, err := s.openCiphertext(ctx, binding.ObjectID)
		if err != nil {
			return RestoreManifest{}, err
		}
		descriptor, err := cryptoarchive.DescriptorForSnapshotObject(snapshot, binding.Coordinate, mustParseEpoch(bundle.KeyEpoch))
		if err != nil {
			_ = ciphertext.Close()
			return RestoreManifest{}, ErrInvalid
		}
		descriptor.CiphertextSize, descriptor.CiphertextSHA256 = binding.CiphertextSize, binding.CiphertextSHA256
		name := binding.ObjectID + ".payload"
		path := filepath.Join(objectsDir, name)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = ciphertext.Close()
			return RestoreManifest{}, ErrObjectUnavailable
		}
		decryptErr := cryptoarchive.DecryptObject(ctx, dek, descriptor, ciphertext, file)
		closeCipherErr := ciphertext.Close()
		if decryptErr == nil {
			decryptErr = closeCipherErr
		}
		if decryptErr == nil {
			decryptErr = file.Sync()
		}
		if closeFileErr := file.Close(); decryptErr == nil {
			decryptErr = closeFileErr
		}
		if decryptErr != nil {
			return RestoreManifest{}, ErrInvalid
		}
		manifest.Objects = append(manifest.Objects, RestoredObject{Coordinate: binding.Coordinate, Path: filepath.ToSlash(filepath.Join("objects", name)), Size: binding.PlaintextSize, SHA256: binding.PlaintextSHA256})
	}
	masterPlaylist, trackPlaylists, playlistFiles, err := buildRestoredPlaylists(snapshot, manifest.Objects)
	if err != nil {
		return RestoreManifest{}, err
	}
	manifest.PlaybackPlaylist = masterPlaylist
	manifest.TrackPlaylists = trackPlaylists
	for relative, data := range playlistFiles {
		playlistPath := filepath.Join(stage, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(playlistPath), 0700); err != nil {
			return RestoreManifest{}, ErrObjectUnavailable
		}
		if err := writeAtomic(playlistPath, data); err != nil {
			return RestoreManifest{}, err
		}
	}
	if masterPlaylist != "" {
		manifest.Version = "ir.restored-snapshot/2"
	}
	if err := syncDirectory(objectsDir); err != nil {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	encoded, err := json.Marshal(manifest)
	if err != nil || len(encoded) > maxBundleSize {
		return RestoreManifest{}, ErrInvalid
	}
	if err := writeAtomic(filepath.Join(stage, "manifest.json"), encoded); err != nil {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	if err := ctx.Err(); err != nil {
		return RestoreManifest{}, err
	}
	if err := authorize(ctx, auth, snapshot, "archive.restore"); err != nil {
		return RestoreManifest{}, err
	}
	if err := syncDirectory(stage); err != nil {
		return RestoreManifest{}, ErrObjectUnavailable
	}
	if err := replicationRenameNoReplace(stage, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return RestoreManifest{}, ErrRestoreConflict
		}
		return RestoreManifest{}, ErrObjectUnavailable
	}
	if err := syncDirectory(parent); err != nil {
		// The directory is already atomically visible. Return failure rather than
		// claiming durability; leave the complete verified tree for safe retry.
		committed = true
		return RestoreManifest{}, ErrObjectUnavailable
	}
	committed = true
	return manifest, nil
}

func authorize(ctx context.Context, auth Authorization, snapshot archivesnapshot.Snapshot, action string) error {
	if ctx.Err() != nil || auth.Verifier == nil || len(auth.Chain) == 0 || auth.Actor.Kind != authority.PrincipalNode {
		return ErrUnauthorized
	}
	if err := auth.Verifier.Authorize(ctx, auth.SubjectID, snapshot.Content.ArchiveID, snapshot.SnapshotID, action, auth.Actor, auth.Chain, time.Now().UTC()); err != nil {
		return ErrUnauthorized
	}
	return nil
}

func validateBundle(snapshot archivesnapshot.Snapshot, bundle Bundle) error {
	if archivesnapshot.Verify(snapshot) != nil || bundle.Version != BundleVersion || bundle.ArchiveID != snapshot.Content.ArchiveID || bundle.SnapshotID != snapshot.SnapshotID || !positiveDecimal(bundle.KeyEpoch) || bundle.Objects == nil || len(bundle.Objects) > len(snapshot.Content.Objects) {
		return ErrInvalid
	}
	seen := make(map[archiveindex.Coordinate]struct{}, len(bundle.Objects))
	for _, object := range bundle.Objects {
		if !validObjectID(object.ObjectID) || object.PlaintextSize <= 0 || object.CiphertextSize < 65 || object.PlaintextSHA256 == "" || !validSHA256(object.PlaintextSHA256) || !validSHA256(object.CiphertextSHA256) || object.CiphertextSize > archiveindex.MaxPayloadBytes+(2<<20) {
			return ErrInvalid
		}
		if _, duplicate := seen[object.Coordinate]; duplicate {
			return ErrInvalid
		}
		seen[object.Coordinate] = struct{}{}
		matched := false
		for _, candidate := range snapshot.Content.Objects {
			if candidate.Coordinate == object.Coordinate && candidate.State == archiveindex.CoveragePresent && candidate.PlaintextSize == object.PlaintextSize && candidate.PlaintextSHA256 == object.PlaintextSHA256 {
				matched = true
				break
			}
		}
		if !matched {
			return ErrInvalid
		}
	}
	if len(bundle.Objects) == 0 {
		return ErrInvalid
	}
	for _, candidate := range snapshot.Content.Objects {
		if candidate.State == archiveindex.CoveragePresent {
			if _, ok := seen[candidate.Coordinate]; !ok {
				return ErrInvalid
			}
		}
	}
	return nil
}

func (s *Store) verifyBundleKey(ctx context.Context, snapshot archivesnapshot.Snapshot, bundle Bundle, dek *cryptoarchive.DEK) error {
	if err := validateBundle(snapshot, bundle); err != nil {
		return err
	}
	keyEpoch := mustParseEpoch(bundle.KeyEpoch)
	for _, object := range bundle.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := s.openCiphertext(ctx, object.ObjectID)
		if err != nil {
			return err
		}
		descriptor, err := cryptoarchive.DescriptorForSnapshotObject(snapshot, object.Coordinate, keyEpoch)
		if err != nil {
			_ = reader.Close()
			return ErrInvalid
		}
		descriptor.CiphertextSize, descriptor.CiphertextSHA256 = object.CiphertextSize, object.CiphertextSHA256
		decryptErr := cryptoarchive.DecryptObject(ctx, dek, descriptor, reader, io.Discard)
		closeErr := reader.Close()
		if decryptErr != nil || closeErr != nil {
			return ErrInvalid
		}
	}
	return nil
}

func (s *Store) publishObject(staged string, binding ObjectBinding) error {
	destination := filepath.Join(s.root, objectDir, binding.ObjectID+".irac")
	if err := os.Link(staged, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return verifyCipherFile(destination, binding.CiphertextSize, binding.CiphertextSHA256)
		}
		return ErrObjectUnavailable
	}
	if err := os.Chmod(destination, 0600); err != nil {
		_ = os.Remove(destination)
		return ErrObjectUnavailable
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		_ = os.Remove(destination)
		return ErrObjectUnavailable
	}
	return nil
}

// replaceCorruptObject atomically replaces only the same private regular file
// observed as digest-mismatched by the caller. The caller holds the Store lock
// and invokes this only after a full authorized transfer passed digest checks.
func (s *Store) replaceCorruptObject(staged string, binding ObjectBinding, observed os.FileInfo) error {
	destination := filepath.Join(s.root, objectDir, binding.ObjectID+".irac")
	existing, err := inspectCipherFile(destination, binding.CiphertextSize, binding.CiphertextSHA256)
	if err == nil && existing != nil {
		// Another valid publication won the race; preserve it.
		return nil
	}
	if !errors.Is(err, errCipherFileMismatch) || existing == nil || observed == nil || !os.SameFile(observed, existing) {
		return ErrInvalid
	}
	if err := verifyCipherFile(staged, binding.CiphertextSize, binding.CiphertextSHA256); err != nil {
		return ErrInvalid
	}
	source, err := os.Open(staged)
	if err != nil {
		return ErrObjectUnavailable
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".repair-"+binding.ObjectID+"-*")
	if err != nil {
		_ = source.Close()
		return ErrObjectUnavailable
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = source.Close()
		_ = temporary.Close()
		return ErrObjectUnavailable
	}
	_, copyErr := io.Copy(temporary, source)
	sourceCloseErr := source.Close()
	if copyErr != nil || sourceCloseErr != nil || temporary.Sync() != nil {
		_ = temporary.Close()
		return ErrObjectUnavailable
	}
	if err := temporary.Close(); err != nil {
		return ErrObjectUnavailable
	}
	if err := verifyCipherFile(temporaryPath, binding.CiphertextSize, binding.CiphertextSHA256); err != nil {
		return ErrInvalid
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return ErrObjectUnavailable
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return ErrObjectUnavailable
	}
	return nil
}

func (s *Store) readBundle(path string, snapshot archivesnapshot.Snapshot) (Bundle, error) {
	file, err := os.Open(path)
	if err != nil {
		return Bundle{}, err
	}
	defer file.Close()
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) || info.Size() <= 0 || info.Size() > maxBundleSize {
		return Bundle{}, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBundleSize+1))
	if err != nil || len(data) > maxBundleSize {
		return Bundle{}, ErrInvalid
	}
	var disk bundleDisk
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&disk) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return Bundle{}, ErrInvalid
	}
	bundle := bundleFromDisk(disk)
	if validateBundle(snapshot, bundle) != nil {
		return Bundle{}, ErrInvalid
	}
	return bundle, nil
}

func bundleToDisk(bundle Bundle) bundleDisk {
	disk := bundleDisk{
		Version: bundle.Version, ArchiveID: bundle.ArchiveID, SnapshotID: bundle.SnapshotID,
		KeyEpoch: bundle.KeyEpoch, Objects: make([]objectBindingDisk, 0, len(bundle.Objects)),
	}
	for _, object := range bundle.Objects {
		disk.Objects = append(disk.Objects, objectBindingDisk{
			ObjectID: object.ObjectID, Coordinate: object.Coordinate,
			PlaintextSize: object.PlaintextSize, PlaintextSHA256: object.PlaintextSHA256,
			CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256,
		})
	}
	return disk
}

func bundleFromDisk(disk bundleDisk) Bundle {
	bundle := Bundle{
		Version: disk.Version, ArchiveID: disk.ArchiveID, SnapshotID: disk.SnapshotID,
		KeyEpoch: disk.KeyEpoch, Objects: make([]ObjectBinding, 0, len(disk.Objects)),
	}
	for _, object := range disk.Objects {
		bundle.Objects = append(bundle.Objects, ObjectBinding{
			ObjectID: object.ObjectID, Coordinate: object.Coordinate,
			PlaintextSize: object.PlaintextSize, PlaintextSHA256: object.PlaintextSHA256,
			CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256,
		})
	}
	return bundle
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (s *Store) bundlePath(archiveID, snapshotID string, keyEpoch uint64) string {
	return filepath.Join(s.root, bundleDir, archiveID, snapshotID, strconv.FormatUint(keyEpoch, 10)+".json")
}

func (s *Store) snapshotPath(archiveID, snapshotID string) string {
	return filepath.Join(s.root, snapshotDir, archiveID, snapshotID+".json")
}

func validReplicaArchiveID(value string) bool {
	if !strings.HasPrefix(value, "arc_") {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "arc_"))
	return err == nil && len(decoded) == 16 && "arc_"+base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validReplicaSnapshotID(value string) bool {
	if len(value) != len("snap_")+sha256.Size*2 || !strings.HasPrefix(value, "snap_") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "snap_"))
	return err == nil && len(decoded) == sha256.Size && "snap_"+hex.EncodeToString(decoded) == value
}

func (s *Store) keyEnvelopePath(archiveID, recipientKeyID string, keyEpoch uint64) string {
	return filepath.Join(s.root, keyEnvelopeDir, archiveID, recipientKeyID, strconv.FormatUint(keyEpoch, 10)+".json")
}

func (s *Store) archiveDEKPath(archiveID string, keyEpoch uint64) string {
	return filepath.Join(s.root, archiveKeyDir, archiveID, strconv.FormatUint(keyEpoch, 10)+".dek")
}

func validRecipientKeyID(value string) bool {
	return len(value) == 46 && strings.HasPrefix(value, "rk_") && strings.Trim(value[3:], "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-") == ""
}

func (s *Store) lock(ctx context.Context) (func(), error) {
	path := filepath.Join(s.root, ".replication.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrObjectUnavailable
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, ErrInvalid
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := replicationFlock(file); err == nil {
			return func() { replicationFunlock(file); _ = file.Close() }, nil
		} else if !replicationLockWouldBlock(err) {
			_ = file.Close()
			return nil, ErrObjectUnavailable
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func writeAtomicImmutable(root, path string, data []byte) error {
	if existingInfo, err := os.Lstat(path); err == nil {
		if !existingInfo.Mode().IsRegular() || existingInfo.Mode()&os.ModeSymlink != 0 || existingInfo.Mode().Perm() != 0600 {
			return ErrInvalid
		}
		existing, readErr := os.ReadFile(path)
		if readErr != nil {
			return ErrObjectUnavailable
		}
		if bytes.Equal(existing, data) {
			return nil
		}
		return ErrInvalid
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrObjectUnavailable
	}
	if err := ensurePrivateDirectories(root, filepath.Dir(path)); err != nil {
		return err
	}
	if err := writeAtomic(path, data); err != nil {
		return err
	}
	return nil
}

func ensurePrivateDirectories(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return ErrInvalid
	}
	current := root
	if rel == "." {
		return nil
	}
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." || part == ".." {
			return ErrInvalid
		}
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return ErrObjectUnavailable
		}
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
			return ErrInvalid
		}
	}
	return nil
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".replica-*")
	if err != nil {
		return ErrObjectUnavailable
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return ErrObjectUnavailable
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return ErrObjectUnavailable
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return ErrObjectUnavailable
	}
	if err := file.Close(); err != nil {
		return ErrObjectUnavailable
	}
	if err := os.Rename(tmp, path); err != nil {
		return ErrObjectUnavailable
	}
	if err := syncDirectory(dir); err != nil {
		return ErrObjectUnavailable
	}
	return nil
}

func verifyCipherFile(path string, expectedSize int64, expectedHash string) error {
	info, err := inspectCipherFile(path, expectedSize, expectedHash)
	if err == nil && info == nil {
		return ErrObjectUnavailable
	}
	return err
}

// inspectCipherFile returns the inode observed at path when it exists. A
// regular, private file with wrong bytes or size is distinct from an unsafe
// path so an authorized full transfer may repair only the former.
func inspectCipherFile(path string, expectedSize int64, expectedHash string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrObjectUnavailable
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return info, ErrInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return info, ErrObjectUnavailable
	}
	defer file.Close()
	opened, fileErr := file.Stat()
	if fileErr != nil || !os.SameFile(info, opened) {
		return info, ErrInvalid
	}
	if info.Size() != expectedSize {
		return info, errCipherFileMismatch
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(file, expectedSize+1))
	if err != nil || n != expectedSize || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != expectedHash {
		return info, errCipherFileMismatch
	}
	return info, nil
}

func objectIDForDigest(digest string) string {
	decoded, _ := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	return "ob_" + base64.RawURLEncoding.EncodeToString(decoded)
}

func validObjectID(value string) bool {
	if !strings.HasPrefix(value, "ob_") || len(value) != 46 {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "ob_"))
	return err == nil && len(decoded) == sha256.Size && "ob_"+base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validSHA256(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func positiveDecimal(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed > 0 && strconv.FormatUint(parsed, 10) == value
}

func mustParseEpoch(value string) uint64 {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return parsed
}

type contextReader struct {
	ctx        context.Context
	reader     io.Reader
	emptyReads int
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(buffer)
	if n == 0 && err == nil {
		r.emptyReads++
		if r.emptyReads >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.emptyReads = 0
	}
	return n, err
}

func (b Bundle) String() string {
	return fmt.Sprintf("encrypted snapshot %s/%s (%d ciphertext objects)", b.ArchiveID, b.SnapshotID, len(b.Objects))
}
