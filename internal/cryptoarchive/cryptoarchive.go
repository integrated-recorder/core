// Package cryptoarchive contains versioned, transport-independent primitives
// for encrypting sealed archive snapshot objects and wrapping archive DEKs.
// The replication integration supplies OwnerSubject/Capability authorization
// and durable replica leases; callers must still write stream output to an
// unpublished staging object and publish only after this package returns
// success.
package cryptoarchive

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/hpke"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	FormatVersion   = 1
	ChunkSize       = 64 << 10
	keyBytes        = 32
	objectSaltBytes = 32
	headerBytes     = 4 + 1 + 4 + 8 + objectSaltBytes
	maxChunks       = (archiveindex.MaxPayloadBytes + ChunkSize - 1) / ChunkSize
	KeyEnvelopeType = "ir.key-envelope/1"
	kemID           = "DHKEM(X25519,HKDF-SHA256)"
	kdfID           = "HKDF-SHA256"
	aeadID          = "AES-256-GCM"
)

var (
	ErrInvalidInput         = errors.New("archive cryptographic input is invalid")
	ErrPlaintextMismatch    = errors.New("archive plaintext does not match its declared identity")
	ErrCiphertextInvalid    = errors.New("archive ciphertext is invalid")
	ErrEnvelopeInvalid      = errors.New("archive key envelope is invalid")
	ErrEnvelopeUnauthorized = errors.New("archive key envelope signer or recipient is not trusted")
	archiveIDPattern        = regexp.MustCompile(`^arc_[A-Za-z0-9_-]{22}$`)
	sha256Pattern           = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	keyEpochPattern         = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
	recipientKeyIDPattern   = regexp.MustCompile(`^rk_[A-Za-z0-9_-]{43}$`)
	signerKeyIDPattern      = regexp.MustCompile(`^ik_[A-Za-z0-9_-]{43}$`)
)

var objectMagic = [4]byte{'I', 'R', 'A', 'C'}

// DEK is an archive's 256-bit content-encryption key. It intentionally has no
// JSON representation or String method. Concurrent operations are safe;
// Destroy waits for in-flight operations and prevents later operations. Go
// cannot guarantee erasure of prior copies or compiler-created temporaries,
// so callers must also minimize copies and lifetime.
type DEK struct {
	mu        sync.RWMutex
	key       [keyBytes]byte
	destroyed bool
}

func NewDEK() (*DEK, error) {
	dek := &DEK{}
	if _, err := io.ReadFull(rand.Reader, dek.key[:]); err != nil {
		clear(dek.key[:])
		return nil, errors.New("archive key generation failed")
	}
	return dek, nil
}

// DescriptorForSnapshotObject derives encryption identity only from a
// validated immutable snapshot. Conflicted objects are not eligible for
// encryption or replication. EncryptObject still verifies the actual source
// stream against the snapshot's size and plaintext digest.
func DescriptorForSnapshotObject(snapshot archivesnapshot.Snapshot, coordinate archiveindex.Coordinate, keyEpoch uint64) (ObjectDescriptor, error) {
	if archivesnapshot.Verify(snapshot) != nil || keyEpoch == 0 {
		return ObjectDescriptor{}, ErrInvalidInput
	}
	for _, object := range snapshot.Content.Objects {
		if object.Coordinate != coordinate {
			continue
		}
		if object.State != archiveindex.CoveragePresent {
			return ObjectDescriptor{}, ErrInvalidInput
		}
		return ObjectDescriptor{
			ArchiveID: snapshot.Content.ArchiveID, Coordinate: object.Coordinate,
			ObjectType: string(object.Coordinate.Kind), PlaintextSize: object.PlaintextSize,
			PlaintextSHA256: object.PlaintextSHA256, KeyEpoch: keyEpoch,
		}, nil
	}
	return ObjectDescriptor{}, ErrInvalidInput
}

func DEKFromBytes(raw []byte) (*DEK, error) {
	if len(raw) != keyBytes {
		return nil, ErrInvalidInput
	}
	dek := &DEK{}
	copy(dek.key[:], raw)
	return dek, nil
}

func (d *DEK) Destroy() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.key[:])
	d.destroyed = true
}

// SameKey reports whether two live DEKs contain identical key bytes without
// exposing either key. It is intended for rotation checks that must reject a
// new epoch which merely reuses the prior epoch's key.
func (d *DEK) SameKey(other *DEK) bool {
	if d == nil || other == nil {
		return false
	}
	var first, second [keyBytes]byte
	d.mu.RLock()
	if d.destroyed {
		d.mu.RUnlock()
		return false
	}
	first = d.key
	d.mu.RUnlock()

	other.mu.RLock()
	if other.destroyed {
		other.mu.RUnlock()
		clear(first[:])
		return false
	}
	second = other.key
	other.mu.RUnlock()
	equal := subtle.ConstantTimeCompare(first[:], second[:]) == 1
	clear(first[:])
	clear(second[:])
	return equal
}

type ObjectDescriptor struct {
	ArchiveID        string
	Coordinate       archiveindex.Coordinate
	ObjectType       string
	PlaintextSize    int64
	PlaintextSHA256  string
	CiphertextSize   int64
	CiphertextSHA256 string
	KeyEpoch         uint64
}

type ObjectResult struct {
	FormatVersion    int    `json:"crypto_format_version"`
	PlaintextSize    int64  `json:"plaintext_size"`
	PlaintextSHA256  string `json:"plaintext_sha256"`
	CiphertextSize   int64  `json:"ciphertext_size"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	ChunkCount       uint64 `json:"chunk_count"`
}

type objectBinding struct {
	ArchiveID       string         `json:"archive_id"`
	Coordinate      wireCoordinate `json:"object_coordinate"`
	ObjectType      string         `json:"object_type"`
	PlaintextSize   string         `json:"plaintext_size"`
	PlaintextSHA256 string         `json:"plaintext_sha256"`
	FormatVersion   string         `json:"crypto_format_version"`
	KeyEpoch        string         `json:"key_epoch"`
}

type wireCoordinate struct {
	SessionID             string `json:"session_id"`
	TrackID               string `json:"track_id"`
	SourceEpoch           string `json:"source_epoch"`
	DiscontinuitySequence string `json:"discontinuity_sequence"`
	Sequence              string `json:"sequence"`
	Kind                  string `json:"kind"`
}

type chunkAAD struct {
	ArchiveID       string         `json:"archive_id"`
	Coordinate      wireCoordinate `json:"object_coordinate"`
	ObjectType      string         `json:"object_type"`
	PlaintextSize   string         `json:"plaintext_size"`
	PlaintextSHA256 string         `json:"plaintext_sha256"`
	ChunkIndex      string         `json:"chunk_index"`
	ByteOffset      string         `json:"byte_offset"`
	FormatVersion   string         `json:"crypto_format_version"`
	KeyEpoch        string         `json:"key_epoch"`
}

// EncryptObject emits IRAC v1: a bounded header followed by independently
// authenticated 64-KiB AES-256-GCM chunks. It verifies the full source size
// and SHA-256 before returning success. The destination is a staging writer:
// discard it on every error and publish only after success.
func EncryptObject(ctx context.Context, dek *DEK, descriptor ObjectDescriptor, source io.Reader, destination io.Writer) (ObjectResult, error) {
	if dek == nil {
		return ObjectResult{}, ErrInvalidInput
	}
	dek.mu.RLock()
	defer dek.mu.RUnlock()
	return encryptObject(ctx, dek, descriptor, source, destination, rand.Reader)
}

func encryptObject(ctx context.Context, dek *DEK, descriptor ObjectDescriptor, source io.Reader, destination io.Writer, random io.Reader) (ObjectResult, error) {
	if err := validateObjectInputs(ctx, dek, source, destination); err != nil {
		return ObjectResult{}, err
	}
	normalized, binding, expectedHash, err := normalizeObjectDescriptor(descriptor)
	if err != nil {
		return ObjectResult{}, err
	}
	chunkCount := uint64((normalized.PlaintextSize + ChunkSize - 1) / ChunkSize)
	if chunkCount == 0 || chunkCount > uint64(maxChunks) {
		return ObjectResult{}, ErrInvalidInput
	}
	var objectSalt [objectSaltBytes]byte
	if _, err := io.ReadFull(random, objectSalt[:]); err != nil {
		return ObjectResult{}, errors.New("archive object key separation failed")
	}
	key, err := deriveObjectKey(dek, objectSalt[:], binding)
	if err != nil {
		return ObjectResult{}, err
	}
	defer clear(key)
	aead, err := newObjectAEAD(key)
	if err != nil {
		return ObjectResult{}, err
	}
	header := make([]byte, headerBytes)
	copy(header[:4], objectMagic[:])
	header[4] = FormatVersion
	binary.BigEndian.PutUint32(header[5:9], ChunkSize)
	binary.BigEndian.PutUint64(header[9:17], uint64(normalized.PlaintextSize))
	copy(header[17:], objectSalt[:])
	cipherHash := sha256.New()
	var cipherSize int64
	if n, err := writeHashed(ctx, destination, cipherHash, header); err != nil {
		return ObjectResult{}, err
	} else {
		cipherSize += n
	}
	plainHash := sha256.New()
	remaining := normalized.PlaintextSize
	buffer := make([]byte, ChunkSize)
	defer clear(buffer)
	for index := uint64(0); remaining > 0; index++ {
		if err := ctx.Err(); err != nil {
			return ObjectResult{}, err
		}
		want := int64(len(buffer))
		if remaining < want {
			want = remaining
		}
		plainChunk := buffer[:int(want)]
		if _, err := io.ReadFull(source, plainChunk); err != nil {
			return ObjectResult{}, ErrPlaintextMismatch
		}
		_, _ = plainHash.Write(plainChunk)
		aad, err := makeChunkAAD(binding, index, normalized.PlaintextSize)
		if err != nil {
			return ObjectResult{}, ErrInvalidInput
		}
		sealed := aead.Seal(nil, objectNonce(index), plainChunk, aad)
		if n, err := writeHashed(ctx, destination, cipherHash, sealed); err != nil {
			clear(sealed)
			return ObjectResult{}, err
		} else {
			cipherSize += n
		}
		clear(sealed)
		remaining -= want
	}
	if err := requireEOF(source); err != nil {
		return ObjectResult{}, ErrPlaintextMismatch
	}
	if subtle.ConstantTimeCompare(plainHash.Sum(nil), expectedHash) != 1 {
		return ObjectResult{}, ErrPlaintextMismatch
	}
	return ObjectResult{
		FormatVersion: FormatVersion, PlaintextSize: normalized.PlaintextSize,
		PlaintextSHA256: normalized.PlaintextSHA256, CiphertextSize: cipherSize,
		CiphertextSHA256: "sha256:" + hex.EncodeToString(cipherHash.Sum(nil)), ChunkCount: chunkCount,
	}, nil
}

// DecryptObject validates every chunk tag, exact ciphertext length, and final
// plaintext size/hash. Its destination is also a staging writer and must be
// discarded on error; publish plaintext only after success.
func DecryptObject(ctx context.Context, dek *DEK, descriptor ObjectDescriptor, source io.Reader, destination io.Writer) error {
	if dek == nil {
		return ErrInvalidInput
	}
	dek.mu.RLock()
	defer dek.mu.RUnlock()
	if err := validateObjectInputs(ctx, dek, source, destination); err != nil {
		return err
	}
	normalized, binding, expectedHash, err := normalizeObjectDescriptor(descriptor)
	if err != nil {
		return err
	}
	chunkCount := uint64((normalized.PlaintextSize + ChunkSize - 1) / ChunkSize)
	if chunkCount == 0 || chunkCount > uint64(maxChunks) {
		return ErrInvalidInput
	}
	header := make([]byte, headerBytes)
	if _, err := io.ReadFull(source, header); err != nil || !bytes.Equal(header[:4], objectMagic[:]) || header[4] != FormatVersion ||
		binary.BigEndian.Uint32(header[5:9]) != ChunkSize || binary.BigEndian.Uint64(header[9:17]) != uint64(normalized.PlaintextSize) {
		return ErrCiphertextInvalid
	}
	cipherHash := sha256.New()
	_, _ = cipherHash.Write(header)
	cipherSize := int64(len(header))
	key, err := deriveObjectKey(dek, header[17:], binding)
	if err != nil {
		return err
	}
	defer clear(key)
	aead, err := newObjectAEAD(key)
	if err != nil {
		return err
	}
	plainHash := sha256.New()
	remaining := normalized.PlaintextSize
	for index := uint64(0); remaining > 0; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := int64(ChunkSize)
		if remaining < want {
			want = remaining
		}
		sealed := make([]byte, int(want)+aead.Overhead())
		if _, err := io.ReadFull(source, sealed); err != nil {
			clear(sealed)
			return ErrCiphertextInvalid
		}
		_, _ = cipherHash.Write(sealed)
		cipherSize += int64(len(sealed))
		aad, err := makeChunkAAD(binding, index, normalized.PlaintextSize)
		if err != nil {
			clear(sealed)
			return ErrInvalidInput
		}
		plainChunk, err := aead.Open(nil, objectNonce(index), sealed, aad)
		clear(sealed)
		if err != nil || int64(len(plainChunk)) != want {
			clear(plainChunk)
			return ErrCiphertextInvalid
		}
		_, _ = plainHash.Write(plainChunk)
		if _, err := writePlain(ctx, destination, plainChunk); err != nil {
			clear(plainChunk)
			return err
		}
		clear(plainChunk)
		remaining -= want
	}
	if err := requireEOF(source); err != nil || subtle.ConstantTimeCompare(plainHash.Sum(nil), expectedHash) != 1 {
		return ErrCiphertextInvalid
	}
	if normalized.CiphertextSHA256 != "" && (normalized.CiphertextSize != cipherSize || normalized.CiphertextSHA256 != "sha256:"+hex.EncodeToString(cipherHash.Sum(nil))) {
		return ErrCiphertextInvalid
	}
	return nil
}

func validateObjectInputs(ctx context.Context, dek *DEK, source io.Reader, destination io.Writer) error {
	if ctx == nil || dek == nil || dek.destroyed || source == nil || destination == nil {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func normalizeObjectDescriptor(input ObjectDescriptor) (ObjectDescriptor, objectBinding, []byte, error) {
	if !archiveIDPattern.MatchString(input.ArchiveID) || input.PlaintextSize <= 0 || input.PlaintextSize > archiveindex.MaxPayloadBytes ||
		input.KeyEpoch == 0 || !sha256Pattern.MatchString(input.PlaintextSHA256) {
		return ObjectDescriptor{}, objectBinding{}, nil, ErrInvalidInput
	}
	if (input.CiphertextSize == 0) != (input.CiphertextSHA256 == "") || (input.CiphertextSHA256 != "" &&
		(input.CiphertextSize < headerBytes+16 || input.CiphertextSize > archiveindex.MaxPayloadBytes+maxChunks*16+headerBytes || !sha256Pattern.MatchString(input.CiphertextSHA256))) {
		return ObjectDescriptor{}, objectBinding{}, nil, ErrInvalidInput
	}
	coordinate := input.Coordinate
	if coordinate.Kind == "" {
		coordinate.Kind = archiveindex.ObjectMedia
	}
	if _, err := archiveindex.SegmentIdentity(coordinate); err != nil {
		return ObjectDescriptor{}, objectBinding{}, nil, ErrInvalidInput
	}
	if input.ObjectType == "" {
		input.ObjectType = string(coordinate.Kind)
	}
	if input.ObjectType != string(coordinate.Kind) || (input.ObjectType != string(archiveindex.ObjectMedia) && input.ObjectType != string(archiveindex.ObjectInit)) {
		return ObjectDescriptor{}, objectBinding{}, nil, ErrInvalidInput
	}
	input.Coordinate = coordinate
	binding := objectBinding{
		ArchiveID: input.ArchiveID, Coordinate: wireCoordinate{
			SessionID: coordinate.SessionID, TrackID: coordinate.TrackID,
			SourceEpoch:           strconv.FormatUint(coordinate.SourceEpoch, 10),
			DiscontinuitySequence: strconv.FormatUint(coordinate.DiscontinuitySequence, 10),
			Sequence:              strconv.FormatUint(coordinate.Sequence, 10), Kind: string(coordinate.Kind),
		},
		ObjectType: input.ObjectType, PlaintextSize: strconv.FormatInt(input.PlaintextSize, 10),
		PlaintextSHA256: input.PlaintextSHA256, FormatVersion: strconv.Itoa(FormatVersion),
		KeyEpoch: strconv.FormatUint(input.KeyEpoch, 10),
	}
	expectedHash, _ := hex.DecodeString(strings.TrimPrefix(input.PlaintextSHA256, "sha256:"))
	return input, binding, expectedHash, nil
}

func deriveObjectKey(dek *DEK, salt []byte, binding objectBinding) ([]byte, error) {
	canonical, err := canonicalStruct(binding)
	if err != nil {
		return nil, ErrInvalidInput
	}
	info := "IR/archive-object-key/v1\x00" + string(canonical)
	return hkdf.Key(sha256.New, dek.key[:], salt, info, keyBytes)
}

func newObjectAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidInput
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrInvalidInput
	}
	return aead, nil
}

func objectNonce(index uint64) []byte {
	nonce := make([]byte, 12)
	binary.BigEndian.PutUint64(nonce[4:], index)
	return nonce
}

func makeChunkAAD(binding objectBinding, index uint64, totalSize int64) ([]byte, error) {
	value := chunkAAD{
		ArchiveID: binding.ArchiveID, Coordinate: binding.Coordinate, ObjectType: binding.ObjectType,
		PlaintextSize: strconv.FormatInt(totalSize, 10), PlaintextSHA256: binding.PlaintextSHA256,
		ChunkIndex: strconv.FormatUint(index, 10), ByteOffset: strconv.FormatUint(index*ChunkSize, 10),
		FormatVersion: binding.FormatVersion, KeyEpoch: binding.KeyEpoch,
	}
	canonical, err := canonicalStruct(value)
	if err != nil {
		return nil, err
	}
	return append([]byte("IR/archive-chunk-aad/v1\x00"), canonical...), nil
}

func writeHashed(ctx context.Context, dst io.Writer, digest hash.Hash, data []byte) (int64, error) {
	var total int64
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := dst.Write(data)
		if n > 0 {
			_, _ = digest.Write(data[:n])
			total += int64(n)
			data = data[n:]
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func writePlain(ctx context.Context, dst io.Writer, data []byte) (int64, error) {
	var total int64
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := dst.Write(data)
		if n > 0 {
			total += int64(n)
			data = data[n:]
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func requireEOF(reader io.Reader) error {
	var extra [1]byte
	n, err := io.ReadFull(reader, extra[:])
	if n != 0 {
		return ErrPlaintextMismatch
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func canonicalStruct(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return identityprotocol.Canonicalize(data)
}

type KeyEnvelope struct {
	EnvelopeVersion          string `json:"envelope_version"`
	ArchiveID                string `json:"archive_id"`
	KeyEpoch                 string `json:"key_epoch"`
	RecipientKeyID           string `json:"recipient_key_id"`
	RecipientPublicKeySHA256 string `json:"recipient_public_key_sha256"`
	KEM                      string `json:"kem"`
	KDF                      string `json:"kdf"`
	AEAD                     string `json:"aead"`
	Encapsulation            string `json:"encapsulation"`
	Ciphertext               string `json:"ciphertext"`
	SignerKeyID              string `json:"signer_key_id"`
	BindingHash              string `json:"binding_hash"`
	Signature                string `json:"signature"`
}

type envelopeUnsigned struct {
	EnvelopeVersion          string `json:"envelope_version"`
	ArchiveID                string `json:"archive_id"`
	KeyEpoch                 string `json:"key_epoch"`
	RecipientKeyID           string `json:"recipient_key_id"`
	RecipientPublicKeySHA256 string `json:"recipient_public_key_sha256"`
	KEM                      string `json:"kem"`
	KDF                      string `json:"kdf"`
	AEAD                     string `json:"aead"`
	Encapsulation            string `json:"encapsulation"`
	Ciphertext               string `json:"ciphertext"`
	SignerKeyID              string `json:"signer_key_id"`
}

type envelopeSigned struct {
	EnvelopeUnsigned envelopeUnsigned `json:"envelope"`
	BindingHash      string           `json:"binding_hash"`
}

type hpkeContext struct {
	EnvelopeVersion          string `json:"envelope_version"`
	ArchiveID                string `json:"archive_id"`
	KeyEpoch                 string `json:"key_epoch"`
	RecipientKeyID           string `json:"recipient_key_id"`
	RecipientPublicKeySHA256 string `json:"recipient_public_key_sha256"`
	SignerKeyID              string `json:"signer_key_id"`
}

// WrapDEK creates a Base-mode HPKE envelope and signs its complete binding.
// The caller must separately authorize the signer through current identity,
// OwnerSubject, and Capability policy. A valid signature alone grants no
// archive rights and does not prove key-history freshness.
func WrapDEK(dek *DEK, archiveID string, keyEpoch uint64, recipientPublicKey []byte, signerKeyID string, signer ed25519.PrivateKey) (KeyEnvelope, error) {
	if dek == nil {
		return KeyEnvelope{}, ErrInvalidInput
	}
	dek.mu.RLock()
	defer dek.mu.RUnlock()
	if dek.destroyed || len(recipientPublicKey) != 32 || keyEpoch == 0 || !archiveIDPattern.MatchString(archiveID) || len(signer) != ed25519.PrivateKeySize {
		return KeyEnvelope{}, ErrInvalidInput
	}
	publicSigner := signer.Public().(ed25519.PublicKey)
	derivedSignerID, err := identityprotocol.KeyID(publicSigner)
	if err != nil || signerKeyID != derivedSignerID {
		return KeyEnvelope{}, ErrInvalidInput
	}
	kem := hpke.DHKEM(ecdh.X25519())
	recipient, err := kem.NewPublicKey(recipientPublicKey)
	if err != nil {
		return KeyEnvelope{}, ErrInvalidInput
	}
	recipientID := RecipientKeyID(recipientPublicKey)
	context := hpkeContext{
		EnvelopeVersion: KeyEnvelopeType, ArchiveID: archiveID,
		KeyEpoch: strconv.FormatUint(keyEpoch, 10), RecipientKeyID: recipientID,
		RecipientPublicKeySHA256: RecipientPublicKeyHash(recipientPublicKey), SignerKeyID: signerKeyID,
	}
	contextBytes, err := canonicalStruct(context)
	if err != nil {
		return KeyEnvelope{}, ErrInvalidInput
	}
	info := append([]byte("IR/key-envelope/v1/hpke-info\x00"), contextBytes...)
	aad := append([]byte("IR/key-envelope/v1/hpke-aad\x00"), contextBytes...)
	enc, sender, err := hpke.NewSender(recipient, hpke.HKDFSHA256(), hpke.AES256GCM(), info)
	if err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	ciphertext, err := sender.Seal(aad, dek.key[:])
	if err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	unsigned := envelopeUnsigned{
		EnvelopeVersion: KeyEnvelopeType, ArchiveID: archiveID, KeyEpoch: context.KeyEpoch,
		RecipientKeyID: recipientID, RecipientPublicKeySHA256: context.RecipientPublicKeySHA256,
		KEM: kemID, KDF: kdfID, AEAD: aeadID,
		Encapsulation: base64.RawURLEncoding.EncodeToString(enc),
		Ciphertext:    base64.RawURLEncoding.EncodeToString(ciphertext), SignerKeyID: signerKeyID,
	}
	bindingHash, err := envelopeBindingHash(unsigned)
	if err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	signBytes, err := canonicalStruct(envelopeSigned{EnvelopeUnsigned: unsigned, BindingHash: bindingHash})
	if err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	signature := ed25519.Sign(signer, append([]byte("IR/key-envelope/v1/sign\x00"), signBytes...))
	return KeyEnvelope{
		EnvelopeVersion: unsigned.EnvelopeVersion, ArchiveID: unsigned.ArchiveID,
		KeyEpoch: unsigned.KeyEpoch, RecipientKeyID: unsigned.RecipientKeyID,
		RecipientPublicKeySHA256: unsigned.RecipientPublicKeySHA256,
		KEM:                      unsigned.KEM, KDF: unsigned.KDF, AEAD: unsigned.AEAD,
		Encapsulation: unsigned.Encapsulation, Ciphertext: unsigned.Ciphertext,
		SignerKeyID: unsigned.SignerKeyID, BindingHash: bindingHash,
		Signature: base64.RawURLEncoding.EncodeToString(signature),
	}, nil
}

type EnvelopeExpectation struct {
	ArchiveID        string
	KeyEpoch         uint64
	RecipientKeyID   string
	SignerKeyID      string
	SignerPublicKey  ed25519.PublicKey
	RecipientPrivate []byte
}

// VerifyKeyEnvelopeSignature verifies envelope structure, signer key identity,
// binding digest, and Ed25519 signature without opening the HPKE ciphertext.
// Callers must still verify signer authorization at the current
// identity-history head and enforce current epoch/fork policy.
func VerifyKeyEnvelopeSignature(envelope KeyEnvelope, signerPublicKey ed25519.PublicKey) error {
	unsigned, err := unsignedFromEnvelope(envelope)
	if err != nil || len(signerPublicKey) != ed25519.PublicKeySize {
		return ErrEnvelopeInvalid
	}
	derivedSignerID, err := identityprotocol.KeyID(signerPublicKey)
	if err != nil || derivedSignerID != envelope.SignerKeyID {
		return ErrEnvelopeUnauthorized
	}
	computedBinding, err := envelopeBindingHash(unsigned)
	if err != nil || computedBinding != envelope.BindingHash {
		return ErrEnvelopeInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return ErrEnvelopeInvalid
	}
	signedBytes, err := canonicalStruct(envelopeSigned{EnvelopeUnsigned: unsigned, BindingHash: envelope.BindingHash})
	if err != nil || !ed25519.Verify(signerPublicKey, append([]byte("IR/key-envelope/v1/sign\x00"), signedBytes...), signature) {
		return ErrEnvelopeUnauthorized
	}
	return nil
}

// OpenDEK checks the canonical binding and an externally trusted signer key
// before HPKE decapsulation. Callers must still verify signer authorization at
// the current identity-history head and enforce current epoch/fork policy.
func OpenDEK(envelope KeyEnvelope, expected EnvelopeExpectation) (*DEK, error) {
	unsigned, err := unsignedFromEnvelope(envelope)
	if err != nil || validateEnvelopeUnsigned(unsigned) != nil || !archiveIDPattern.MatchString(expected.ArchiveID) || expected.KeyEpoch == 0 ||
		expected.ArchiveID != envelope.ArchiveID || strconv.FormatUint(expected.KeyEpoch, 10) != envelope.KeyEpoch ||
		expected.RecipientKeyID != envelope.RecipientKeyID || expected.SignerKeyID != envelope.SignerKeyID ||
		len(expected.SignerPublicKey) != ed25519.PublicKeySize || len(expected.RecipientPrivate) != 32 {
		return nil, ErrEnvelopeInvalid
	}
	privateKey, err := ecdh.X25519().NewPrivateKey(expected.RecipientPrivate)
	if err != nil {
		return nil, ErrEnvelopeUnauthorized
	}
	recipientPublic := privateKey.PublicKey().Bytes()
	if RecipientKeyID(recipientPublic) != expected.RecipientKeyID || RecipientPublicKeyHash(recipientPublic) != envelope.RecipientPublicKeySHA256 {
		return nil, ErrEnvelopeUnauthorized
	}
	if err := VerifyKeyEnvelopeSignature(envelope, expected.SignerPublicKey); err != nil {
		return nil, err
	}
	enc, err := base64.RawURLEncoding.DecodeString(envelope.Encapsulation)
	if err != nil || len(enc) != 32 {
		return nil, ErrEnvelopeInvalid
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) != keyBytes+16 {
		return nil, ErrEnvelopeInvalid
	}
	context := hpkeContext{
		EnvelopeVersion: envelope.EnvelopeVersion, ArchiveID: envelope.ArchiveID,
		KeyEpoch: envelope.KeyEpoch, RecipientKeyID: envelope.RecipientKeyID,
		RecipientPublicKeySHA256: envelope.RecipientPublicKeySHA256, SignerKeyID: envelope.SignerKeyID,
	}
	contextBytes, err := canonicalStruct(context)
	if err != nil {
		return nil, ErrEnvelopeInvalid
	}
	info := append([]byte("IR/key-envelope/v1/hpke-info\x00"), contextBytes...)
	aad := append([]byte("IR/key-envelope/v1/hpke-aad\x00"), contextBytes...)
	recipient, err := hpke.DHKEM(ecdh.X25519()).NewPrivateKey(expected.RecipientPrivate)
	if err != nil {
		return nil, ErrEnvelopeUnauthorized
	}
	contextRecipient, err := hpke.NewRecipient(enc, recipient, hpke.HKDFSHA256(), hpke.AES256GCM(), info)
	if err != nil {
		return nil, ErrEnvelopeInvalid
	}
	plain, err := contextRecipient.Open(aad, ciphertext)
	if err != nil || len(plain) != keyBytes {
		clear(plain)
		return nil, ErrEnvelopeInvalid
	}
	dek, err := DEKFromBytes(plain)
	clear(plain)
	if err != nil {
		return nil, ErrEnvelopeInvalid
	}
	return dek, nil
}

func RecipientPublicKeyHash(publicKey []byte) string {
	digest := sha256.Sum256(append([]byte("IR/recipient-public-key/v1\x00"), publicKey...))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func RecipientKeyID(publicKey []byte) string {
	digest := sha256.Sum256(append([]byte("IR/recipient-key-id/v1\x00"), publicKey...))
	return "rk_" + base64.RawURLEncoding.EncodeToString(digest[:])
}

func ParseKeyEnvelope(input []byte) (KeyEnvelope, error) {
	if len(input) == 0 || len(input) > 16<<10 {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	canonical, err := identityprotocol.Canonicalize(input)
	if err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	var envelope KeyEnvelope
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	reencoded, err := identityprotocol.Canonicalize(encoded)
	if err != nil || !bytes.Equal(canonical, reencoded) {
		return KeyEnvelope{}, ErrEnvelopeInvalid
	}
	if _, err := unsignedFromEnvelope(envelope); err != nil {
		return KeyEnvelope{}, err
	}
	return envelope, nil
}

func MarshalKeyEnvelope(envelope KeyEnvelope) ([]byte, error) {
	if _, err := unsignedFromEnvelope(envelope); err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil || len(data) > 16<<10 {
		return nil, ErrEnvelopeInvalid
	}
	return identityprotocol.Canonicalize(data)
}

func unsignedFromEnvelope(envelope KeyEnvelope) (envelopeUnsigned, error) {
	unsigned := envelopeUnsigned{
		EnvelopeVersion: envelope.EnvelopeVersion, ArchiveID: envelope.ArchiveID,
		KeyEpoch: envelope.KeyEpoch, RecipientKeyID: envelope.RecipientKeyID,
		RecipientPublicKeySHA256: envelope.RecipientPublicKeySHA256,
		KEM:                      envelope.KEM, KDF: envelope.KDF, AEAD: envelope.AEAD,
		Encapsulation: envelope.Encapsulation, Ciphertext: envelope.Ciphertext,
		SignerKeyID: envelope.SignerKeyID,
	}
	if err := validateEnvelopeUnsigned(unsigned); err != nil || !sha256Pattern.MatchString(envelope.BindingHash) {
		return envelopeUnsigned{}, ErrEnvelopeInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return envelopeUnsigned{}, ErrEnvelopeInvalid
	}
	return unsigned, nil
}

func validateEnvelopeUnsigned(unsigned envelopeUnsigned) error {
	if unsigned.EnvelopeVersion != KeyEnvelopeType || !archiveIDPattern.MatchString(unsigned.ArchiveID) ||
		!keyEpochPattern.MatchString(unsigned.KeyEpoch) || !recipientKeyIDPattern.MatchString(unsigned.RecipientKeyID) ||
		!sha256Pattern.MatchString(unsigned.RecipientPublicKeySHA256) || unsigned.KEM != kemID || unsigned.KDF != kdfID || unsigned.AEAD != aeadID ||
		!signerKeyIDPattern.MatchString(unsigned.SignerKeyID) || len(unsigned.Encapsulation) > 128 || len(unsigned.Ciphertext) > 256 {
		return ErrEnvelopeInvalid
	}
	encapsulation, err := base64.RawURLEncoding.DecodeString(unsigned.Encapsulation)
	if err != nil || len(encapsulation) != 32 {
		return ErrEnvelopeInvalid
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(unsigned.Ciphertext)
	if err != nil || len(ciphertext) != keyBytes+16 {
		return ErrEnvelopeInvalid
	}
	return nil
}

func envelopeBindingHash(unsigned envelopeUnsigned) (string, error) {
	canonical, err := canonicalStruct(unsigned)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("IR/key-envelope/v1/binding\x00"), canonical...))
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
