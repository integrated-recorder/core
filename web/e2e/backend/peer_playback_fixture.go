package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/replication"
)

var e2eNodeIDPattern = regexp.MustCompile(`^nd_[A-Za-z0-9_-]{22}$`)

// e2ePlaybackPeer supplies a deterministic, isolated peer session for the
// real-server Passkey playback API test. It is enabled only by the focused
// two-node E2E backend and never participates in production construction.
type e2ePlaybackPeer struct {
	mu       sync.RWMutex
	identity *globalidentity.Store
	nodeID   string
	dek      []byte
	archives map[string]e2ePlaybackArchive
}

type e2ePlaybackArchive struct {
	snapshot   archivesnapshot.Snapshot
	manifest   replication.ReplicaManifest
	ciphertext map[string][]byte
}

func newE2EPlaybackPeer(identity *globalidentity.Store) *e2ePlaybackPeer {
	return &e2ePlaybackPeer{identity: identity, dek: bytes.Repeat([]byte{0x39}, 32), archives: make(map[string]e2ePlaybackArchive)}
}

func validE2EPeerNodeID(nodeID string) bool { return e2eNodeIDPattern.MatchString(nodeID) }

func (p *e2ePlaybackPeer) ConnectNode(nodeID string) {
	p.mu.Lock()
	p.nodeID = nodeID
	p.mu.Unlock()
}

func (p *e2ePlaybackPeer) List() []peerapi.SessionInfo {
	p.mu.RLock()
	nodeID := p.nodeID
	p.mu.RUnlock()
	if nodeID == "" || p.identity == nil {
		return []peerapi.SessionInfo{}
	}
	identity, initialized, err := p.identity.Snapshot()
	if err != nil || !initialized || identity.Frozen || identity.IdentityID == "" {
		return []peerapi.SessionInfo{}
	}
	return []peerapi.SessionInfo{{IdentityID: identity.IdentityID, NodeID: nodeID, SessionID: "e2e-peer-session", ConnectedAt: time.Now().UTC()}}
}

func (p *e2ePlaybackPeer) Connect(_ context.Context, identityID, nodeID string) error {
	if !validE2EPeerNodeID(nodeID) {
		return errors.New("invalid E2E peer node")
	}
	if p.identity == nil {
		return errors.New("E2E identity unavailable")
	}
	identity, initialized, err := p.identity.Snapshot()
	if err != nil || !initialized || identity.IdentityID != identityID {
		return errors.New("E2E identity mismatch")
	}
	p.ConnectNode(nodeID)
	return nil
}

func (p *e2ePlaybackPeer) Disconnect(nodeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nodeID == nodeID {
		p.nodeID = ""
	}
	return nil
}

func (p *e2ePlaybackPeer) HeadSnapshot(_ context.Context, nodeID string, scope peerapi.RequestScope) (archivesnapshot.Snapshot, error) {
	if !p.connected(nodeID) {
		return archivesnapshot.Snapshot{}, peerapi.ErrPeerNotConnected
	}
	archive, err := p.archive(scope.ArchiveID)
	return archive.snapshot, err
}

func (p *e2ePlaybackPeer) Snapshot(ctx context.Context, nodeID string, scope peerapi.RequestScope, snapshotID string) (archivesnapshot.Snapshot, error) {
	archive, err := p.archive(scope.ArchiveID)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	snapshot := archive.snapshot
	if !p.connected(nodeID) {
		return archivesnapshot.Snapshot{}, peerapi.ErrPeerNotConnected
	}
	if snapshot.SnapshotID != snapshotID {
		return archivesnapshot.Snapshot{}, peerapi.ErrPeerNotConnected
	}
	return snapshot, nil
}

func (p *e2ePlaybackPeer) ReplicaManifest(_ context.Context, nodeID string, scope peerapi.RequestScope, snapshot archivesnapshot.Snapshot, keyEpoch uint64) (replication.ReplicaManifest, error) {
	if !p.connected(nodeID) || keyEpoch != 1 || snapshot.Content.ArchiveID != scope.ArchiveID {
		return replication.ReplicaManifest{}, peerapi.ErrPeerNotConnected
	}
	archive, err := p.archive(scope.ArchiveID)
	if err != nil || archive.snapshot.SnapshotID != snapshot.SnapshotID {
		return replication.ReplicaManifest{}, peerapi.ErrPeerNotConnected
	}
	return archive.manifest, nil
}

func (p *e2ePlaybackPeer) KeyEpochs(_ context.Context, nodeID string, scope peerapi.RequestScope, snapshot archivesnapshot.Snapshot, _ string) ([]uint64, error) {
	if !p.connected(nodeID) || snapshot.Content.ArchiveID != scope.ArchiveID {
		return nil, peerapi.ErrPeerNotConnected
	}
	return []uint64{1}, nil
}

func (p *e2ePlaybackPeer) KeyEnvelope(context.Context, string, peerapi.RequestScope, archivesnapshot.Snapshot, uint64, string) (cryptoarchive.KeyEnvelope, error) {
	return cryptoarchive.KeyEnvelope{}, peerapi.ErrPeerNotConnected
}

func (p *e2ePlaybackPeer) OpenCiphertextRange(_ context.Context, nodeID string, scope peerapi.RequestScope, _ string, objectID string, keyEpoch uint64, offset, length int64) (io.ReadCloser, error) {
	if !p.connected(nodeID) || offset < 0 || length < 0 {
		return nil, peerapi.ErrPeerNotConnected
	}
	archive, err := p.archive(scope.ArchiveID)
	if err != nil || keyEpoch != 1 {
		return nil, peerapi.ErrPeerNotConnected
	}
	ciphertext, ok := archive.ciphertext[objectID]
	if !ok || offset > int64(len(ciphertext)) || length > int64(len(ciphertext))-offset {
		return nil, peerapi.ErrPeerNotConnected
	}
	return io.NopCloser(bytes.NewReader(ciphertext[offset : offset+length])), nil
}

func (p *e2ePlaybackPeer) SessionKeyEpochs(_ context.Context, nodeID string, scope peerapi.RequestScope, snapshot archivesnapshot.Snapshot) ([]uint64, error) {
	if !p.connected(nodeID) || snapshot.Content.ArchiveID != scope.ArchiveID {
		return nil, peerapi.ErrPeerNotConnected
	}
	return []uint64{1}, nil
}

func (p *e2ePlaybackPeer) SessionDEKEnvelope(_ context.Context, nodeID string, scope peerapi.RequestScope, snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientPublicKey []byte) (cryptoarchive.SessionDEKEnvelope, error) {
	if !p.connected(nodeID) || keyEpoch != 1 || snapshot.Content.ArchiveID != scope.ArchiveID || snapshot.SnapshotID != scope.SnapshotID || len(scope.GrantChain) == 0 {
		return cryptoarchive.SessionDEKEnvelope{}, peerapi.ErrPeerNotConnected
	}
	grantHash, err := authority.GrantHash(scope.GrantChain[len(scope.GrantChain)-1].Grant)
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	p.mu.RLock()
	dek := append([]byte(nil), p.dek...)
	p.mu.RUnlock()
	defer clear(dek)
	return cryptoarchive.SealSessionDEK(dek, recipientPublicKey, cryptoarchive.SessionDEKBinding{
		ArchiveID: scope.ArchiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: keyEpoch,
		OwnerSubjectID: scope.OwnerSubjectID, GrantHash: grantHash,
		SupplierNodeID: nodeID,
	})
}

func (p *e2ePlaybackPeer) archive(archiveID string) (e2ePlaybackArchive, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if archive, ok := p.archives[archiveID]; ok {
		return archive, nil
	}
	archive, err := buildE2EPlaybackArchive(archiveID, p.dek)
	if err != nil {
		return e2ePlaybackArchive{}, err
	}
	p.archives[archiveID] = archive
	return archive, nil
}

func buildE2EPlaybackArchive(archiveID string, rawDEK []byte) (e2ePlaybackArchive, error) {
	const recordingID = "e2e-remote-recording"
	session, err := archiveindex.NewSessionIdentity(recordingID, "hls", nil, "e2e-peer-session")
	if err != nil {
		return e2ePlaybackArchive{}, err
	}
	inventory, err := archiveindex.NewInventory(recordingID, session)
	if err != nil {
		return e2ePlaybackArchive{}, err
	}
	inventory.ArchiveID = archiveID
	fixtureDir := filepath.Join(repositoryRoot(), "web", "e2e", "fixtures", "peer-playback")
	fixtures := []struct {
		name     string
		kind     archiveindex.ObjectKind
		sequence uint64
		duration float64
	}{
		{name: "init.mp4", kind: archiveindex.ObjectInit, sequence: 1},
		{name: "segment00.m4s", kind: archiveindex.ObjectMedia, sequence: 1, duration: 1},
		{name: "segment01.m4s", kind: archiveindex.ObjectMedia, sequence: 2, duration: 1},
		{name: "segment02.m4s", kind: archiveindex.ObjectMedia, sequence: 3, duration: 1},
	}
	payloads := make(map[archiveindex.Coordinate][]byte, len(fixtures))
	for _, fixture := range fixtures {
		payload, readErr := os.ReadFile(filepath.Join(fixtureDir, fixture.name))
		if readErr != nil || len(payload) == 0 {
			return e2ePlaybackArchive{}, errors.New("E2E encrypted playback fixture is unavailable")
		}
		digest := sha256.Sum256(payload)
		coordinate := archiveindex.Coordinate{
			SessionID: session.ID, TrackID: "main", SourceEpoch: 1,
			DiscontinuitySequence: 1, Sequence: fixture.sequence, Kind: fixture.kind,
		}
		_, applyErr := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: "e2e-" + fixture.name, Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), PayloadPath: "e2e-fixture/" + fixture.name,
			Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
			Verification: archiveindex.VerificationVerified, Duration: fixture.duration,
			InitIdentity: "e2e-source-init-map",
		})
		if applyErr != nil {
			return e2ePlaybackArchive{}, applyErr
		}
		payloads[coordinate] = payload
	}
	if err := archiveindex.CloseCapture(&inventory); err != nil {
		return e2ePlaybackArchive{}, err
	}
	if err := archiveindex.SealArchive(&inventory); err != nil {
		return e2ePlaybackArchive{}, err
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		return e2ePlaybackArchive{}, err
	}
	dek, err := cryptoarchive.DEKFromBytes(rawDEK)
	if err != nil {
		return e2ePlaybackArchive{}, err
	}
	defer dek.Destroy()
	bundle := replication.Bundle{
		Version: replication.BundleVersion, ArchiveID: snapshot.Content.ArchiveID,
		SnapshotID: snapshot.SnapshotID, KeyEpoch: "1", Objects: make([]replication.ObjectBinding, 0, len(snapshot.Content.Objects)),
	}
	ciphertexts := make(map[string][]byte, len(snapshot.Content.Objects))
	for _, object := range snapshot.Content.Objects {
		if object.State != archiveindex.CoveragePresent {
			continue
		}
		payload := payloads[object.Coordinate]
		descriptor, descriptorErr := cryptoarchive.DescriptorForSnapshotObject(snapshot, object.Coordinate, 1)
		if descriptorErr != nil || len(payload) == 0 {
			return e2ePlaybackArchive{}, errors.New("E2E snapshot object binding is invalid")
		}
		var ciphertext bytes.Buffer
		result, encryptErr := cryptoarchive.EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(payload), &ciphertext)
		if encryptErr != nil {
			return e2ePlaybackArchive{}, encryptErr
		}
		objectID := e2eObjectID(result.CiphertextSHA256)
		if objectID == "" {
			return e2ePlaybackArchive{}, errors.New("E2E ciphertext digest is invalid")
		}
		bundle.Objects = append(bundle.Objects, replication.ObjectBinding{
			ObjectID: objectID, Coordinate: object.Coordinate,
			PlaintextSize: result.PlaintextSize, PlaintextSHA256: result.PlaintextSHA256,
			CiphertextSize: result.CiphertextSize, CiphertextSHA256: result.CiphertextSHA256,
		})
		ciphertexts[objectID] = append([]byte(nil), ciphertext.Bytes()...)
	}
	manifest, err := replication.PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		return e2ePlaybackArchive{}, err
	}
	return e2ePlaybackArchive{snapshot: snapshot, manifest: manifest, ciphertext: ciphertexts}, nil
}

func e2eObjectID(ciphertextSHA256 string) string {
	if !strings.HasPrefix(ciphertextSHA256, "sha256:") {
		return ""
	}
	digest, err := hex.DecodeString(strings.TrimPrefix(ciphertextSHA256, "sha256:"))
	if err != nil || len(digest) != sha256.Size {
		return ""
	}
	return "ob_" + base64.RawURLEncoding.EncodeToString(digest)
}

func (p *e2ePlaybackPeer) connected(nodeID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return nodeID != "" && p.nodeID == nodeID
}
