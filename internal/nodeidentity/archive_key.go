package nodeidentity

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

const localArchiveKeyEnvelopeVersion = 1

var localArchiveKeyMagic = [4]byte{'I', 'R', 'N', 'K'}

// SealArchiveDEK protects a source node's archive key for durable local
// recovery. The wrapping key is domain-separated from the Node Key's signing
// use and is never returned. This only protects the key file from accidental
// disclosure independently of the private Node Key; compromise of the trusted
// node's protected security directory compromises both by design.
func (s *Store) SealArchiveDEK(archiveID string, keyEpoch uint64, dataKey []byte) ([]byte, error) {
	if s == nil || !validArchiveKeyBinding(archiveID, keyEpoch) || len(dataKey) != ed25519.SeedSize {
		return nil, ErrInvalid
	}
	key, err := s.read()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotCreated
		}
		return nil, err
	}
	private, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(private) != ed25519.PrivateKeySize {
		clear(private)
		return nil, ErrCorrupt
	}
	defer clear(private)
	wrapKey, err := deriveArchiveKeyWrapKey(private[:ed25519.SeedSize], key.NodeID)
	if err != nil {
		return nil, ErrCorrupt
	}
	defer clear(wrapKey)
	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return nil, ErrCorrupt
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrCorrupt
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		clear(nonce)
		return nil, errors.New("local archive key protection is unavailable")
	}
	sealed := make([]byte, len(localArchiveKeyMagic)+1+len(nonce), len(localArchiveKeyMagic)+1+len(nonce)+len(dataKey)+aead.Overhead())
	copy(sealed, localArchiveKeyMagic[:])
	sealed[len(localArchiveKeyMagic)] = localArchiveKeyEnvelopeVersion
	copy(sealed[len(localArchiveKeyMagic)+1:], nonce)
	sealed = aead.Seal(sealed, nonce, dataKey, archiveKeyAAD(key.NodeID, archiveID, keyEpoch))
	clear(nonce)
	return sealed, nil
}

// OpenArchiveDEK opens a locally protected archive key only for its original
// Node Key, archive ID and immutable key epoch. Authentication or binding
// failures fail closed without returning partial key material.
func (s *Store) OpenArchiveDEK(archiveID string, keyEpoch uint64, sealed []byte) ([]byte, error) {
	if s == nil || !validArchiveKeyBinding(archiveID, keyEpoch) || len(sealed) != 4+1+12+ed25519.SeedSize+16 ||
		!bytes.Equal(sealed[:4], localArchiveKeyMagic[:]) || sealed[4] != localArchiveKeyEnvelopeVersion {
		return nil, ErrInvalid
	}
	key, err := s.read()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotCreated
		}
		return nil, err
	}
	private, err := base64.RawURLEncoding.DecodeString(key.PrivateKey)
	if err != nil || len(private) != ed25519.PrivateKeySize {
		clear(private)
		return nil, ErrCorrupt
	}
	defer clear(private)
	wrapKey, err := deriveArchiveKeyWrapKey(private[:ed25519.SeedSize], key.NodeID)
	if err != nil {
		return nil, ErrCorrupt
	}
	defer clear(wrapKey)
	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return nil, ErrCorrupt
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrCorrupt
	}
	start := len(localArchiveKeyMagic) + 1
	nonce := sealed[start : start+aead.NonceSize()]
	plain, err := aead.Open(nil, nonce, sealed[start+aead.NonceSize():], archiveKeyAAD(key.NodeID, archiveID, keyEpoch))
	if err != nil || len(plain) != ed25519.SeedSize {
		clear(plain)
		return nil, ErrCorrupt
	}
	return plain, nil
}

func deriveArchiveKeyWrapKey(seed []byte, nodeID string) ([]byte, error) {
	return hkdf.Key(sha256.New, seed, []byte("IR/node-local-archive-dek-wrap/v1\x00"+nodeID), "IR/node-local-archive-dek-wrap-key/v1/AES-256-GCM", 32)
}

func archiveKeyAAD(nodeID, archiveID string, keyEpoch uint64) []byte {
	return []byte("IR/node-local-archive-dek/v1\x00" + nodeID + "\x00" + archiveID + "\x00" + strconv.FormatUint(keyEpoch, 10))
}

func validArchiveKeyBinding(archiveID string, keyEpoch uint64) bool {
	if keyEpoch == 0 || !strings.HasPrefix(archiveID, "arc_") {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(archiveID, "arc_"))
	return err == nil && len(decoded) == 16 && "arc_"+base64.RawURLEncoding.EncodeToString(decoded) == archiveID
}
