package peerapi

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/replication"
)

// ErrSessionNetworkingUnavailable means this installation is missing the
// optional identity, Node-Key delegation, or independent Discovery settings
// required for peer sessions. It does not affect local recording.
var ErrSessionNetworkingUnavailable = errors.New("peer networking is unavailable")

// SessionManagerFactory builds a manager from current durable identity state.
// It is called lazily so first-run identity and Node enrollment can happen
// after the Control Plane process starts.
type SessionManagerFactory func(context.Context) (*SessionManager, error)

// SessionManagerSlot exposes a restartable peer manager across Control
// generation handoffs. It closes sessions before the old generation drains
// and constructs a fresh manager on resume, re-reading current identity state.
type SessionManagerSlot struct {
	mu      sync.Mutex
	factory SessionManagerFactory
	ctx     context.Context
	manager *SessionManager
	closed  bool
}

func NewSessionManagerSlot(factory SessionManagerFactory) (*SessionManagerSlot, error) {
	if factory == nil {
		return nil, ErrInvalidRequest
	}
	return &SessionManagerSlot{factory: factory}, nil
}

// Start arms the slot for this Control generation. Missing optional
// distributed setup is expected and is retried on later API access.
func (s *SessionManagerSlot) Start(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidRequest
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionManagerClosed
	}
	s.ctx = ctx
	s.mu.Unlock()
	if _, err := s.ensure(); err != nil && !errors.Is(err, ErrSessionNetworkingUnavailable) {
		return err
	}
	return nil
}

func (s *SessionManagerSlot) Available() bool {
	manager, err := s.ensure()
	return err == nil && manager != nil
}

func (s *SessionManagerSlot) List() []SessionInfo {
	manager, err := s.ensure()
	if err != nil || manager == nil {
		return []SessionInfo{}
	}
	return manager.List()
}

func (s *SessionManagerSlot) Connect(ctx context.Context, identityID, nodeID string) error {
	manager, err := s.ensure()
	if err != nil {
		return err
	}
	if manager == nil {
		return ErrSessionNetworkingUnavailable
	}
	return manager.Connect(ctx, identityID, nodeID)
}

func (s *SessionManagerSlot) Disconnect(nodeID string) error {
	manager, err := s.ensure()
	if err != nil {
		return err
	}
	if manager == nil {
		return ErrSessionNetworkingUnavailable
	}
	return manager.Disconnect(nodeID)
}

// HeadSnapshot requests the current immutable archive head over an already
// authenticated peer session. The caller supplies a capability scope; the
// underlying Client signs the command with this Node's key and the remote
// peer rechecks the current OwnerSubject authority before returning data.
func (s *SessionManagerSlot) HeadSnapshot(ctx context.Context, nodeID string, scope RequestScope) (archivesnapshot.Snapshot, error) {
	manager, err := s.ensure()
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	if manager == nil {
		return archivesnapshot.Snapshot{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return client.HeadSnapshot(ctx)
}

// Snapshot requests one immutable descriptor by ID through the scoped peer
// session. It cannot widen a snapshot-pinned capability.
func (s *SessionManagerSlot) Snapshot(ctx context.Context, nodeID string, scope RequestScope, snapshotID string) (archivesnapshot.Snapshot, error) {
	manager, err := s.ensure()
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	if manager == nil {
		return archivesnapshot.Snapshot{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return archivesnapshot.Snapshot{}, err
	}
	return client.GetSnapshot(ctx, snapshotID)
}

// ReplicaManifest requests a validated ciphertext mapping from the selected
// peer over the authenticated, scope-bound Node-Key session.
func (s *SessionManagerSlot) ReplicaManifest(ctx context.Context, nodeID string, scope RequestScope, snapshot archivesnapshot.Snapshot, keyEpoch uint64) (replication.ReplicaManifest, error) {
	manager, err := s.ensure()
	if err != nil {
		return replication.ReplicaManifest{}, err
	}
	if manager == nil {
		return replication.ReplicaManifest{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return replication.ReplicaManifest{}, err
	}
	return client.ReplicaManifest(ctx, snapshot, keyEpoch)
}

// KeyEpochs asks an authenticated peer for validated epochs it can serve to
// the local identity's recovery recipient. The peer returns no key material.
func (s *SessionManagerSlot) KeyEpochs(ctx context.Context, nodeID string, scope RequestScope, snapshot archivesnapshot.Snapshot, recipientKeyID string) ([]uint64, error) {
	manager, err := s.ensure()
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return nil, err
	}
	return client.KeyEpochs(ctx, snapshot, recipientKeyID)
}

// ReceiveReplicaOffer retrieves a signed offer from the selected peer and
// commits it through the local lease Store's full authority/signature/
// tombstone validation boundary. It does not sign provider acceptance.
func (s *SessionManagerSlot) ReceiveReplicaOffer(ctx context.Context, nodeID string, scope RequestScope, destination *lease.Store, archiveID, snapshotID, leaseID string) (lease.Lease, error) {
	manager, err := s.ensure()
	if err != nil {
		return lease.Lease{}, err
	}
	if manager == nil {
		return lease.Lease{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return lease.Lease{}, err
	}
	return client.ReceiveReplicaOffer(ctx, destination, archiveID, snapshotID, leaseID)
}

// ListReplicaOffers queries one authenticated source for offers it has
// explicitly addressed to the local Node. The returned entries are metadata
// only; import still retrieves and validates the original signed offer.
func (s *SessionManagerSlot) ListReplicaOffers(ctx context.Context, nodeID string, scope RequestScope) ([]lease.PendingOffer, error) {
	manager, err := s.ensure()
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return nil, err
	}
	return client.ReplicaOffers(ctx)
}

// AcceptReplicaOffer adds provider signing and acceptance propagation to the
// durable offer receive path. A failed remote receipt delivery leaves the
// local signed acceptance available for an idempotent retry.
func (s *SessionManagerSlot) AcceptReplicaOffer(ctx context.Context, nodeID string, scope RequestScope, destination *lease.Store, signer ReplicaAcceptanceSigner, archiveID, snapshotID, leaseID string) (lease.Lease, error) {
	manager, err := s.ensure()
	if err != nil {
		return lease.Lease{}, err
	}
	if manager == nil {
		return lease.Lease{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return lease.Lease{}, err
	}
	return client.AcceptReplicaOffer(ctx, destination, signer, archiveID, snapshotID, leaseID)
}

// ImportReplicaFromPeer runs the explicit provider-side offer acceptance,
// resumable ciphertext import and source readback verification workflow over
// one authenticated peer session.
func (s *SessionManagerSlot) ImportReplicaFromPeer(
	ctx context.Context,
	nodeID string,
	scope RequestScope,
	leases *lease.Store,
	store *replication.Store,
	acceptanceSigner ReplicaAcceptanceSigner,
	checkpointSigner replication.ReplicaCheckpointSigner,
	archiveID, snapshotID, leaseID string,
) (ReplicaImportResult, error) {
	manager, err := s.ensure()
	if err != nil {
		return ReplicaImportResult{}, err
	}
	if manager == nil {
		return ReplicaImportResult{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return ReplicaImportResult{}, err
	}
	return client.ImportReplicaFromPeer(ctx, leases, store, acceptanceSigner, checkpointSigner, archiveID, snapshotID, leaseID)
}

// RenewReplicaLease processes a pending or response-retryable owner renewal
// over the authenticated peer session and both nodes' durable lease Stores.
func (s *SessionManagerSlot) RenewReplicaLease(ctx context.Context, nodeID string, scope RequestScope, destination *lease.Store, signer ReplicaRenewalSigner, archiveID, snapshotID, leaseID string) (lease.Lease, error) {
	manager, err := s.ensure()
	if err != nil {
		return lease.Lease{}, err
	}
	if manager == nil {
		return lease.Lease{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return lease.Lease{}, err
	}
	return client.RenewReplicaLease(ctx, destination, signer, archiveID, snapshotID, leaseID)
}

// SubmitTombstone delivers the exact persisted archive deletion decision to
// one authenticated peer. A successful receipt means the peer durably
// recorded the tombstone; it does not prove physical media erasure.
func (s *SessionManagerSlot) SubmitTombstone(ctx context.Context, nodeID string, scope RequestScope, signed lease.SignedTombstone) error {
	_, err := s.SubmitTombstoneWithReceipt(ctx, nodeID, scope, signed)
	return err
}

// SubmitTombstoneWithReceipt returns the authenticated provider's erasure
// receipt and any Node-Key-signed per-lease deletion observations.
func (s *SessionManagerSlot) SubmitTombstoneWithReceipt(ctx context.Context, nodeID string, scope RequestScope, signed lease.SignedTombstone) (TombstoneReceipt, error) {
	manager, err := s.ensure()
	if err != nil {
		return TombstoneReceipt{}, err
	}
	if manager == nil {
		return TombstoneReceipt{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return TombstoneReceipt{}, err
	}
	return client.SubmitTombstoneWithReceipt(ctx, signed)
}

// KeyEnvelope requests only the signed encrypted DEK wrapper for a recipient
// key. The receiving client must hold that private key to open the envelope.
func (s *SessionManagerSlot) KeyEnvelope(ctx context.Context, nodeID string, scope RequestScope, snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientKeyID string) (cryptoarchive.KeyEnvelope, error) {
	manager, err := s.ensure()
	if err != nil {
		return cryptoarchive.KeyEnvelope{}, err
	}
	if manager == nil {
		return cryptoarchive.KeyEnvelope{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return cryptoarchive.KeyEnvelope{}, err
	}
	return client.KeyEnvelope(ctx, snapshot, keyEpoch, recipientKeyID)
}

func (s *SessionManagerSlot) SessionKeyEpochs(ctx context.Context, nodeID string, scope RequestScope, snapshot archivesnapshot.Snapshot) ([]uint64, error) {
	manager, err := s.ensure()
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return nil, err
	}
	return client.SessionKeyEpochs(ctx, snapshot)
}

func (s *SessionManagerSlot) SessionDEKEnvelope(ctx context.Context, nodeID string, scope RequestScope, snapshot archivesnapshot.Snapshot, keyEpoch uint64, recipientPublicKey []byte) (cryptoarchive.SessionDEKEnvelope, error) {
	manager, err := s.ensure()
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	if manager == nil {
		return cryptoarchive.SessionDEKEnvelope{}, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return cryptoarchive.SessionDEKEnvelope{}, err
	}
	return client.SessionDEKEnvelope(ctx, snapshot, keyEpoch, recipientPublicKey)
}

// OpenCiphertextRange returns one validated bounded ciphertext range from the
// peer. The response is fully correlated and bounded by peerapi.Client before
// it is exposed to the HTTP caller.
func (s *SessionManagerSlot) OpenCiphertextRange(ctx context.Context, nodeID string, scope RequestScope, snapshotID, objectID string, keyEpoch uint64, offset, length int64) (io.ReadCloser, error) {
	manager, err := s.ensure()
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	client, err := manager.Client(nodeID, MaxPeerResponseBytes, scope)
	if err != nil {
		return nil, err
	}
	return client.OpenCiphertextRange(ctx, scope.ArchiveID, snapshotID, objectID, keyEpoch, offset, length)
}

func (s *SessionManagerSlot) Stop() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	manager := s.manager
	s.manager = nil
	s.ctx = nil
	s.mu.Unlock()
	if manager != nil {
		return manager.Close()
	}
	return nil
}

func (s *SessionManagerSlot) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	manager := s.manager
	s.manager = nil
	s.ctx = nil
	s.mu.Unlock()
	if manager != nil {
		return manager.Close()
	}
	return nil
}

func (s *SessionManagerSlot) ensure() (*SessionManager, error) {
	if s == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrSessionManagerClosed
	}
	if s.manager != nil {
		return s.manager, nil
	}
	if s.ctx == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	manager, err := s.factory(s.ctx)
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, ErrSessionNetworkingUnavailable
	}
	if err := manager.Start(s.ctx); err != nil {
		_ = manager.Close()
		return nil, err
	}
	s.manager = manager
	return manager, nil
}
