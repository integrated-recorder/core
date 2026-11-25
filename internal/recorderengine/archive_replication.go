package recorderengine

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/replication"
)

type archiveReplicationDependencies struct {
	ciphertexts *replication.Store
	identity    *globalidentity.Store
	authority   *authority.Store
	node        *nodeidentity.Store
}

// EncryptArchiveSnapshotRequest is accepted only over authenticated Engine
// IPC. DataKey is short-lived input. The Engine persists only a locally
// wrapped copy under the Node Key for restart recovery; it is never returned
// or sent to a replica.
type EncryptArchiveSnapshotRequest struct {
	RecordingID  string                    `json:"recording_id"`
	SnapshotID   string                    `json:"snapshot_id"`
	SubjectID    string                    `json:"owner_subject_id"`
	GrantChain   []authority.SignedGrant   `json:"grant_chain"`
	KeyEpoch     uint64                    `json:"key_epoch"`
	DataKey      []byte                    `json:"data_key"`
	UseStoredKey bool                      `json:"use_stored_key,omitempty"`
	KeyEnvelope  cryptoarchive.KeyEnvelope `json:"key_envelope"`
}

type EncryptArchiveSnapshotResult struct {
	Manifest replication.ReplicaManifest `json:"manifest"`
}

// RekeyArchiveHistoryRequest rotates every immutable snapshot in one sealed
// archive to a new DEK and a recipient-bound recovery envelope. Recipient is
// a public projection assembled by Control Plane only after its configured
// identity federation resolver has verified the latest unforked history. The
// Engine revalidates its shape/freshness and treats the authenticated local
// IPC caller as the provenance boundary; it never accepts this projection
// directly from a public HTTP request.
//
// DataKey is one-shot input and is cleared after handling. It is persisted
// only in Node-Key-wrapped form so a same-epoch retry can resume partial
// immutable work after a process restart.
type RekeyArchiveHistoryRequest struct {
	RecordingID      string                    `json:"recording_id"`
	SnapshotID       string                    `json:"snapshot_id"`
	SubjectID        string                    `json:"owner_subject_id"`
	GrantChain       []authority.SignedGrant   `json:"grant_chain"`
	PreviousKeyEpoch uint64                    `json:"previous_key_epoch"`
	KeyEpoch         uint64                    `json:"key_epoch"`
	DataKey          []byte                    `json:"data_key"`
	UseStoredKey     bool                      `json:"use_stored_key,omitempty"`
	KeyEnvelope      cryptoarchive.KeyEnvelope `json:"key_envelope"`
	Recipient        authority.IdentityState   `json:"recipient_identity"`
}

type RekeyArchiveHistoryResult struct {
	Verification replication.RekeyHistoryVerification `json:"verification"`
	Envelope     cryptoarchive.KeyEnvelope            `json:"key_envelope"`
}

// ConfigureArchiveReplication enables the encrypted snapshot path only when
// all durable local identity and authorization stores were opened and
// validated. Corrupt or absent distributed identity state remains isolated
// from local recording and leaves this operation unavailable.
func (e *Engine) ConfigureArchiveReplication(ciphertexts *replication.Store, identity *globalidentity.Store, authorities *authority.Store, node *nodeidentity.Store) error {
	if e == nil || ciphertexts == nil || identity == nil || authorities == nil || node == nil {
		return errors.New("archive replication dependencies are incomplete")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.started || e.archiveReplication.ciphertexts != nil {
		return errors.New("archive replication must be configured once before Engine starts")
	}
	e.archiveReplication = archiveReplicationDependencies{ciphertexts: ciphertexts, identity: identity, authority: authorities, node: node}
	return nil
}

func (e *Engine) encryptArchiveSnapshot(ctx context.Context, request EncryptArchiveSnapshotRequest) (EncryptArchiveSnapshotResult, error) {
	defer func() { clear(request.DataKey) }()
	if ctx == nil || ctx.Err() != nil || request.RecordingID == "" || request.SnapshotID == "" ||
		request.SubjectID == "" || request.KeyEpoch == 0 || len(request.GrantChain) == 0 ||
		request.UseStoredKey && len(request.DataKey) != 0 || !request.UseStoredKey && len(request.DataKey) != 32 {
		return EncryptArchiveSnapshotResult{}, replication.ErrInvalid
	}
	e.mu.RLock()
	deps, catalog := e.archiveReplication, e.archiveCatalog
	e.mu.RUnlock()
	if deps.ciphertexts == nil || deps.identity == nil || deps.authority == nil || deps.node == nil || catalog == nil {
		return EncryptArchiveSnapshotResult{}, replication.ErrObjectUnavailable
	}

	local, initialized, err := deps.identity.Snapshot()
	if err != nil || !initialized || local.Frozen || local.IdentityID == "" {
		return EncryptArchiveSnapshotResult{}, replication.ErrUnauthorized
	}
	node, err := deps.node.Info()
	if err != nil || node.NodeID == "" {
		return EncryptArchiveSnapshotResult{}, replication.ErrUnauthorized
	}
	nodePublic, err := base64.RawURLEncoding.DecodeString(node.PublicKey)
	if err != nil || len(nodePublic) != 32 {
		return EncryptArchiveSnapshotResult{}, replication.ErrUnauthorized
	}
	now := time.Now().UTC()
	resolve := authority.IdentityStateResolverFromLocalAndRegistry(deps.identity, nil)
	identityState, err := resolve(local.IdentityID, "", now)
	if err != nil || identityState.Frozen || identityState.HeadHash == "" ||
		!equalReplicationPublicKey(identityState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], nodePublic) {
		return EncryptArchiveSnapshotResult{}, replication.ErrUnauthorized
	}

	recording, err := e.manager.Get(request.RecordingID)
	if err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	if recording == nil || !recording.ArchiveSealed ||
		(recording.State != domain.StateStopped && recording.State != domain.StateCompleted && recording.State != domain.StateInterrupted) {
		return EncryptArchiveSnapshotResult{}, replication.ErrInvalid
	}
	archiveID := recording.ArchiveID
	if archiveID == "" {
		archiveID, err = archivesnapshot.ArchiveIDForRecording(request.RecordingID)
		if err != nil {
			return EncryptArchiveSnapshotResult{}, replication.ErrInvalid
		}
	}
	snapshot, err := catalog.Head(ctx, archiveID)
	if err != nil || snapshot.SnapshotID != request.SnapshotID || archivesnapshot.Verify(snapshot) != nil ||
		snapshot.Content.Archive != archiveindex.ArchiveSealed {
		return EncryptArchiveSnapshotResult{}, replication.ErrInvalid
	}

	authoritySnapshot, err := deps.authority.Snapshot(request.SubjectID, resolve, now)
	if err != nil || authoritySnapshot.Frozen || authoritySnapshot.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: local.IdentityID}) {
		return EncryptArchiveSnapshotResult{}, replication.ErrUnauthorized
	}
	grantHash, err := authority.GrantHash(request.GrantChain[len(request.GrantChain)-1].Grant)
	chain, committed := authoritySnapshot.Grants[grantHash]
	if err != nil || !committed || !equalReplicationGrantChain(chain, request.GrantChain) ||
		deps.authority.AuthorizeChain(request.SubjectID, request.GrantChain, resolve, now,
			authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}, archiveID, snapshot.SnapshotID, "archive.replicate") != nil {
		return EncryptArchiveSnapshotResult{}, replication.ErrUnauthorized
	}

	recipientKey, err := base64.RawURLEncoding.DecodeString(local.RecoveryEncryptionPublicKey)
	if err != nil || len(recipientKey) != 32 ||
		request.KeyEnvelope.ArchiveID != archiveID || request.KeyEnvelope.KeyEpoch != decimalKeyEpoch(request.KeyEpoch) ||
		request.KeyEnvelope.RecipientKeyID != cryptoarchive.RecipientKeyID(recipientKey) ||
		request.KeyEnvelope.RecipientPublicKeySHA256 != cryptoarchive.RecipientPublicKeyHash(recipientKey) {
		return EncryptArchiveSnapshotResult{}, cryptoarchive.ErrEnvelopeUnauthorized
	}
	rootPublic := identityState.CurrentKeys[authority.PrincipalIdentity+":"+local.IdentityID]
	signerKeyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil || request.KeyEnvelope.SignerKeyID != signerKeyID {
		return EncryptArchiveSnapshotResult{}, cryptoarchive.ErrEnvelopeUnauthorized
	}
	if err := cryptoarchive.VerifyKeyEnvelopeSignature(request.KeyEnvelope, rootPublic); err != nil {
		return EncryptArchiveSnapshotResult{}, cryptoarchive.ErrEnvelopeUnauthorized
	}
	if request.UseStoredKey {
		request.DataKey, err = deps.ciphertexts.LoadArchiveDEK(archiveID, request.KeyEpoch, deps.node)
		if err != nil || len(request.DataKey) != 32 {
			clear(request.DataKey)
			return EncryptArchiveSnapshotResult{}, replication.ErrObjectUnavailable
		}
	}

	source, err := e.manager.SnapshotObjectSource(ctx, request.RecordingID, snapshot)
	if err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	dek, err := cryptoarchive.DEKFromBytes(request.DataKey)
	if err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	defer dek.Destroy()
	if err := deps.ciphertexts.PersistArchiveDEK(snapshot, request.KeyEpoch, request.DataKey, deps.node); err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	authorizer := lease.DurableAuthority{Store: deps.authority, Resolve: resolve}
	bundle, err := deps.ciphertexts.EncryptSnapshot(ctx, snapshot, source, dek, request.KeyEpoch, replication.Authorization{
		SubjectID: request.SubjectID, Actor: authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID},
		Chain: request.GrantChain, Verifier: authorizer,
	})
	if err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	if err := deps.ciphertexts.PersistKeyEnvelope(snapshot, request.KeyEnvelope, rootPublic); err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	manifest, err := replication.PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		return EncryptArchiveSnapshotResult{}, err
	}
	return EncryptArchiveSnapshotResult{Manifest: manifest}, nil
}

func (e *Engine) rekeyArchiveHistory(ctx context.Context, request RekeyArchiveHistoryRequest) (RekeyArchiveHistoryResult, error) {
	defer func() { clear(request.DataKey) }()
	if ctx == nil || ctx.Err() != nil || request.RecordingID == "" || request.SnapshotID == "" ||
		request.SubjectID == "" || len(request.GrantChain) == 0 || request.PreviousKeyEpoch == 0 ||
		request.KeyEpoch <= request.PreviousKeyEpoch || request.UseStoredKey && len(request.DataKey) != 0 ||
		!request.UseStoredKey && len(request.DataKey) != 32 {
		return RekeyArchiveHistoryResult{}, replication.ErrInvalid
	}
	e.mu.RLock()
	deps, catalog := e.archiveReplication, e.archiveCatalog
	e.mu.RUnlock()
	if deps.ciphertexts == nil || deps.identity == nil || deps.authority == nil || deps.node == nil || catalog == nil {
		return RekeyArchiveHistoryResult{}, replication.ErrObjectUnavailable
	}

	now := time.Now().UTC()
	if authority.ValidateIdentityState(request.Recipient, now) != nil || request.Recipient.Frozen {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}
	local, initialized, err := deps.identity.Snapshot()
	if err != nil || !initialized || local.Frozen || local.IdentityID == "" || local.IdentityID == request.Recipient.IdentityID {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}
	localState, err := authority.IdentityStateResolverFromLocalAndRegistry(deps.identity, nil)(local.IdentityID, "", now)
	if err != nil || localState.Frozen || localState.HeadHash == "" {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}
	rootPublic := localState.CurrentKeys[authority.PrincipalIdentity+":"+local.IdentityID]
	rootKeyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil || request.KeyEnvelope.SignerKeyID != rootKeyID ||
		request.KeyEnvelope.RecipientKeyID != cryptoarchive.RecipientKeyID(request.Recipient.RecoveryEncryptionPublicKey) ||
		request.KeyEnvelope.RecipientPublicKeySHA256 != cryptoarchive.RecipientPublicKeyHash(request.Recipient.RecoveryEncryptionPublicKey) ||
		request.KeyEnvelope.ArchiveID == "" || request.KeyEnvelope.KeyEpoch != decimalKeyEpoch(request.KeyEpoch) ||
		cryptoarchive.VerifyKeyEnvelopeSignature(request.KeyEnvelope, rootPublic) != nil {
		return RekeyArchiveHistoryResult{}, cryptoarchive.ErrEnvelopeUnauthorized
	}
	node, err := deps.node.Info()
	if err != nil || node.NodeID == "" {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}
	nodePublic, err := base64.RawURLEncoding.DecodeString(node.PublicKey)
	if err != nil || len(nodePublic) != 32 || !equalReplicationPublicKey(localState.CurrentKeys[authority.PrincipalNode+":"+node.NodeID], nodePublic) {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}

	recording, err := e.manager.Get(request.RecordingID)
	if err != nil {
		return RekeyArchiveHistoryResult{}, err
	}
	if recording == nil || !recording.ArchiveSealed ||
		(recording.State != domain.StateStopped && recording.State != domain.StateCompleted && recording.State != domain.StateInterrupted) {
		return RekeyArchiveHistoryResult{}, replication.ErrInvalid
	}
	archiveID := recording.ArchiveID
	if archiveID == "" {
		archiveID, err = archivesnapshot.ArchiveIDForRecording(request.RecordingID)
		if err != nil {
			return RekeyArchiveHistoryResult{}, replication.ErrInvalid
		}
	}
	if request.KeyEnvelope.ArchiveID != archiveID {
		return RekeyArchiveHistoryResult{}, replication.ErrInvalid
	}
	snapshot, err := catalog.Head(ctx, archiveID)
	if err != nil || snapshot.SnapshotID != request.SnapshotID || archivesnapshot.Verify(snapshot) != nil || snapshot.Content.Archive != archiveindex.ArchiveSealed {
		return RekeyArchiveHistoryResult{}, replication.ErrInvalid
	}
	resolve := authority.IdentityStateResolverFromLocalAndRegistry(deps.identity, nil)
	authoritySnapshot, err := deps.authority.Snapshot(request.SubjectID, resolve, now)
	if err != nil || authoritySnapshot.Frozen || authoritySnapshot.Subject.Controller != (authority.Principal{Kind: authority.PrincipalIdentity, ID: local.IdentityID}) {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}
	if _, claimed := authoritySnapshot.RekeyRequiredArchives[archiveID]; claimed {
		return RekeyArchiveHistoryResult{}, authority.ErrRekeyRequired
	}
	if !containsArchiveID(authoritySnapshot.ArchiveIDs, archiveID) {
		return RekeyArchiveHistoryResult{}, authority.ErrArchiveUnclaimed
	}
	grantHash, err := authority.GrantHash(request.GrantChain[len(request.GrantChain)-1].Grant)
	chain, committed := authoritySnapshot.Grants[grantHash]
	if err != nil || !committed || !equalReplicationGrantChain(chain, request.GrantChain) {
		return RekeyArchiveHistoryResult{}, replication.ErrUnauthorized
	}
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: node.NodeID}
	authorizer := lease.DurableAuthority{Store: deps.authority, Resolve: resolve}
	for _, action := range []string{"key.rotate", "archive.replicate"} {
		if err := deps.authority.AuthorizeChain(request.SubjectID, request.GrantChain, resolve, now, actor, archiveID, snapshot.SnapshotID, action); err != nil {
			return RekeyArchiveHistoryResult{}, err
		}
	}
	if request.UseStoredKey {
		request.DataKey, err = deps.ciphertexts.LoadArchiveDEK(archiveID, request.KeyEpoch, deps.node)
		if err != nil || len(request.DataKey) != 32 {
			clear(request.DataKey)
			return RekeyArchiveHistoryResult{}, replication.ErrObjectUnavailable
		}
	}
	previousKey, err := deps.ciphertexts.LoadArchiveDEK(archiveID, request.PreviousKeyEpoch, deps.node)
	if err != nil || len(previousKey) != 32 {
		clear(previousKey)
		return RekeyArchiveHistoryResult{}, replication.ErrObjectUnavailable
	}
	defer clear(previousKey)
	previousDEK, err := cryptoarchive.DEKFromBytes(previousKey)
	if err != nil {
		return RekeyArchiveHistoryResult{}, err
	}
	defer previousDEK.Destroy()
	newDEK, err := cryptoarchive.DEKFromBytes(request.DataKey)
	if err != nil {
		return RekeyArchiveHistoryResult{}, err
	}
	defer newDEK.Destroy()
	if previousDEK.SameKey(newDEK) {
		return RekeyArchiveHistoryResult{}, replication.ErrInvalid
	}
	if err := deps.ciphertexts.PersistArchiveDEK(snapshot, request.KeyEpoch, request.DataKey, deps.node); err != nil {
		return RekeyArchiveHistoryResult{}, err
	}
	sourceFor := func(sourceCtx context.Context, item archivesnapshot.Snapshot) (replication.CanonicalSource, error) {
		return e.manager.SnapshotHistoryObjectSource(sourceCtx, request.RecordingID, item)
	}
	verification, err := deps.ciphertexts.RekeyArchiveHistory(ctx, catalog, archiveID, sourceFor,
		request.PreviousKeyEpoch, request.KeyEpoch, previousDEK, newDEK, request.KeyEnvelope, rootPublic,
		request.Recipient.RecoveryEncryptionPublicKey, replication.Authorization{
			SubjectID: request.SubjectID, Actor: actor, Chain: request.GrantChain, Verifier: authorizer,
		})
	if err != nil {
		return RekeyArchiveHistoryResult{}, err
	}
	return RekeyArchiveHistoryResult{Verification: verification, Envelope: request.KeyEnvelope}, nil
}

func containsArchiveID(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func equalReplicationPublicKey(first, second []byte) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}

func equalReplicationGrantChain(first, second []authority.SignedGrant) bool {
	return reflect.DeepEqual(first, second)
}

func decimalKeyEpoch(value uint64) string {
	return strconv.FormatUint(value, 10)
}
