package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"syscall"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	peerConnectionCursorVersion  = "1"
	peerConnectionCursorName     = "peer-connection-cursor-v1.json"
	peerConnectionCursorLockName = ".peer-connection-cursor.lock"
	peerConnectionCursorMaxSize  = 4096
)

var (
	peerConnectionIdentityPattern = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	peerConnectionNodePattern     = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)
	peerConnectionHashPattern     = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

type peerConnectionCursorStore interface {
	LastPlanned(identityID, desiredSetHash string) (string, error)
	SaveLastPlanned(identityID, desiredSetHash, nodeID string) error
}

type peerConnectionCursorState struct {
	Version       string `json:"state_version"`
	IdentityID    string `json:"identity_id"`
	DesiredSet    string `json:"desired_set_hash"`
	LastPlannedID string `json:"last_planned_node_id"`
}

type durablePeerConnectionCursorStore struct {
	root string
}

func openPeerConnectionCursorStore(root string) (*durablePeerConnectionCursorStore, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("peer connection cursor directory is invalid")
	}
	if err := os.Mkdir(root, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, errors.New("peer connection cursor directory is unavailable")
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return nil, errors.New("peer connection cursor directory is invalid")
	}
	store := &durablePeerConnectionCursorStore{root: root}
	if err := store.withLocked(func() error {
		_, err := store.readState()
		return err
	}); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *durablePeerConnectionCursorStore) LastPlanned(identityID, desiredSetHash string) (string, error) {
	if !validPeerConnectionCursorScope(identityID, desiredSetHash) {
		return "", errors.New("peer connection cursor scope is invalid")
	}
	var last string
	err := s.withLocked(func() error {
		state, err := s.readState()
		if err != nil {
			return err
		}
		if state.IdentityID == identityID && state.DesiredSet == desiredSetHash {
			last = state.LastPlannedID
		}
		return nil
	})
	return last, err
}

func (s *durablePeerConnectionCursorStore) SaveLastPlanned(identityID, desiredSetHash, nodeID string) error {
	if !validPeerConnectionCursorScope(identityID, desiredSetHash) || !peerConnectionNodePattern.MatchString(nodeID) {
		return errors.New("peer connection cursor value is invalid")
	}
	return s.withLocked(func() error {
		// Read and validate the prior record before replacement. A corrupt state
		// file is not silently reset, even though this metadata carries no authority.
		if _, err := s.readState(); err != nil {
			return err
		}
		state := peerConnectionCursorState{
			Version: peerConnectionCursorVersion, IdentityID: identityID,
			DesiredSet: desiredSetHash, LastPlannedID: nodeID,
		}
		return s.writeState(state)
	})
}

func peerConnectionDesiredSetHash(localNodeID string, desired map[string]struct{}) string {
	nodes := make([]string, 0, len(desired))
	for nodeID := range desired {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)
	encoded, _ := json.Marshal(struct {
		LocalNodeID string   `json:"local_node_id"`
		NodeIDs     []string `json:"node_ids"`
	}{LocalNodeID: localNodeID, NodeIDs: nodes})
	hash := sha256.Sum256(append([]byte("IR/peer-connection-desired-set/v1\x00"), encoded...))
	return hex.EncodeToString(hash[:])
}

func validPeerConnectionCursorScope(identityID, desiredSetHash string) bool {
	return peerConnectionIdentityPattern.MatchString(identityID) && peerConnectionHashPattern.MatchString(desiredSetHash)
}

func (s *durablePeerConnectionCursorStore) withLocked(operation func() error) error {
	if s == nil || s.root == "" || operation == nil {
		return errors.New("peer connection cursor store is unavailable")
	}
	lockPath := filepath.Join(s.root, peerConnectionCursorLockName)
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return errors.New("peer connection cursor lock is unavailable")
	}
	defer lock.Close()
	if err := verifyPrivatePeerCursorFile(lockPath, lock); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return errors.New("peer connection cursor lock is unavailable")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return operation()
}

func (s *durablePeerConnectionCursorStore) readState() (peerConnectionCursorState, error) {
	path := filepath.Join(s.root, peerConnectionCursorName)
	entry, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return peerConnectionCursorState{Version: peerConnectionCursorVersion}, nil
	}
	if err != nil || !entry.Mode().IsRegular() || entry.Mode()&os.ModeSymlink != 0 || entry.Mode().Perm() != 0600 || entry.Size() <= 0 || entry.Size() > peerConnectionCursorMaxSize {
		return peerConnectionCursorState{}, errors.New("peer connection cursor state is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return peerConnectionCursorState{}, errors.New("peer connection cursor state is unavailable")
	}
	defer file.Close()
	if err := verifyPrivatePeerCursorFile(path, file); err != nil {
		return peerConnectionCursorState{}, err
	}
	data, err := io.ReadAll(io.LimitReader(file, peerConnectionCursorMaxSize+1))
	if err != nil || len(data) > peerConnectionCursorMaxSize || !bytes.HasSuffix(data, []byte{'\n'}) {
		return peerConnectionCursorState{}, errors.New("peer connection cursor state is invalid")
	}
	jsonBytes := data[:len(data)-1]
	canonical, err := identityprotocol.Canonicalize(jsonBytes)
	if err != nil || !bytes.Equal(canonical, jsonBytes) {
		return peerConnectionCursorState{}, errors.New("peer connection cursor state is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes))
	decoder.DisallowUnknownFields()
	var state peerConnectionCursorState
	if decoder.Decode(&state) != nil || state.Version != peerConnectionCursorVersion ||
		!validPeerConnectionCursorScope(state.IdentityID, state.DesiredSet) ||
		!peerConnectionNodePattern.MatchString(state.LastPlannedID) {
		return peerConnectionCursorState{}, errors.New("peer connection cursor state is invalid")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return peerConnectionCursorState{}, errors.New("peer connection cursor state is invalid")
	}
	return state, nil
}

func verifyPrivatePeerCursorFile(path string, opened *os.File) error {
	entry, err := os.Lstat(path)
	if err != nil || !entry.Mode().IsRegular() || entry.Mode()&os.ModeSymlink != 0 || entry.Mode().Perm() != 0600 {
		return errors.New("peer connection cursor file permissions are invalid")
	}
	info, err := opened.Stat()
	if err != nil || !os.SameFile(entry, info) {
		return errors.New("peer connection cursor file identity is invalid")
	}
	return nil
}

func (s *durablePeerConnectionCursorStore) writeState(state peerConnectionCursorState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return errors.New("peer connection cursor state could not be encoded")
	}
	canonical, err := identityprotocol.Canonicalize(data)
	if err != nil || len(canonical)+1 > peerConnectionCursorMaxSize {
		return errors.New("peer connection cursor state could not be encoded")
	}
	path := filepath.Join(s.root, peerConnectionCursorName)
	tmp, err := os.CreateTemp(s.root, ".peer-connection-cursor-*")
	if err != nil {
		return errors.New("peer connection cursor state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return errors.New("peer connection cursor state could not be written")
	}
	if _, err := tmp.Write(append(canonical, '\n')); err != nil {
		tmp.Close()
		return errors.New("peer connection cursor state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return errors.New("peer connection cursor state could not be written")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("peer connection cursor state could not be written")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("peer connection cursor state could not be published")
	}
	dir, err := os.Open(s.root)
	if err != nil {
		return errors.New("peer connection cursor directory sync failed")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.New("peer connection cursor directory sync failed")
	}
	return nil
}
