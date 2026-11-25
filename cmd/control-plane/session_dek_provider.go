package main

import (
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/replication"
)

type controlPlaneSessionDEKProvider struct {
	store     *replication.Store
	protector *nodeidentity.Store
	nodeID    string
}

func (p controlPlaneSessionDEKProvider) HasSessionDEK(snapshot archivesnapshot.Snapshot, keyEpoch uint64) bool {
	if p.store == nil || p.protector == nil || p.nodeID == "" {
		return false
	}
	bundle, err := p.store.LoadBundle(snapshot, keyEpoch)
	if err != nil {
		return false
	}
	if _, err := replication.PublicReplicaManifest(snapshot, bundle); err != nil {
		return false
	}
	return p.store.HasSessionDEK(snapshot.Content.ArchiveID, keyEpoch, p.protector)
}

func (p controlPlaneSessionDEKProvider) SealSessionDEK(
	snapshot archivesnapshot.Snapshot,
	keyEpoch uint64,
	recipientPublicKey []byte,
	binding cryptoarchive.SessionDEKBinding,
) (cryptoarchive.SessionDEKEnvelope, error) {
	if p.store == nil || p.protector == nil || p.nodeID == "" || binding.SupplierNodeID != p.nodeID {
		return cryptoarchive.SessionDEKEnvelope{}, peerapi.ErrInvalidRequest
	}
	return p.store.SealSessionDEKEnvelope(snapshot, keyEpoch, recipientPublicKey, binding, p.protector)
}
