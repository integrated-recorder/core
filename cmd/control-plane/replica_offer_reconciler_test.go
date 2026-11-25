package main

import (
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/replication"
)

func TestReplicaOfferPlannerCountsActiveAndPendingOnceAndTargetsLatestSnapshot(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	policy := authority.ReplicationPolicy{MinimumReplicas: "2", GracePeriodSeconds: "0", CapacityLimitBytes: "0", RelocateAfterGrace: true}
	base := replicaOfferPlanningLease("rl_0000000000000000000001", "owner", "archive", "snapshot-current", "provider-a", lease.StatusActive, "20", now.Add(time.Hour))
	duplicatePending := replicaOfferPlanningLease("rl_0000000000000000000002", "owner", "archive", "snapshot-current", "provider-a", lease.StatusOffered, "20", now.Add(time.Hour))
	if got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot-current", "source",
		[]lease.Lease{base, duplicatePending}, nil, []string{"provider-a"}, now, 2, 10); err != nil || len(got) != 0 {
		t.Fatalf("duplicate active/pending provider created extra offer: providers=%v err=%v", got, err)
	}
	// A provider holding an older immutable snapshot may be offered the new
	// snapshot. Content-addressed ciphertext import can reuse matching objects.
	oldSnapshotLease := replicaOfferPlanningLease("rl_0000000000000000000003", "owner", "archive", "snapshot-old", "provider-b", lease.StatusActive, "20", now.Add(time.Hour))
	got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot-current", "source",
		[]lease.Lease{base, oldSnapshotLease}, nil, []string{"provider-a", "provider-b", "provider-c"}, now, 2, 10)
	if err != nil || !reflect.DeepEqual(got, []string{"provider-b"}) {
		t.Fatalf("latest snapshot plan=%v err=%v, want provider-b only", got, err)
	}
}

func TestReplicaOfferPlannerEnforcesSubjectCapacityAndRelocationGrace(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	policy := authority.ReplicationPolicy{MinimumReplicas: "1", GracePeriodSeconds: "60", CapacityLimitBytes: "100", RelocateAfterGrace: true}
	otherArchive := replicaOfferPlanningLease("rl_0000000000000000000001", "owner", "other-archive", "old", "provider-x", lease.StatusActive, "90", now.Add(time.Hour))
	if got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source",
		[]lease.Lease{otherArchive}, nil, []string{"provider-a"}, now, 2, 11); err != nil || len(got) != 0 {
		t.Fatalf("subject-wide capacity overcommit plan=%v err=%v", got, err)
	}
	if got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source",
		[]lease.Lease{otherArchive}, nil, []string{"provider-a", "provider-b"}, now, 2, 10); err != nil || !reflect.DeepEqual(got, []string{"provider-a"}) {
		t.Fatalf("capacity-limited plan=%v err=%v, want one provider", got, err)
	}

	expired := replicaOfferPlanningLease("rl_0000000000000000000002", "owner", "archive", "snapshot-old", "provider-old", lease.StatusExpired, "20", now.Add(-30*time.Second))
	policy.CapacityLimitBytes = "0"
	if got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source",
		[]lease.Lease{expired}, nil, []string{"provider-new"}, now, 1, 10); err != nil || len(got) != 0 {
		t.Fatalf("relocation occurred inside grace: providers=%v err=%v", got, err)
	}
	if got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source",
		[]lease.Lease{expired}, nil, []string{"provider-new"}, now.Add(31*time.Second), 1, 10); err != nil || !reflect.DeepEqual(got, []string{"provider-new"}) {
		t.Fatalf("relocation after grace=%v err=%v", got, err)
	}
	policy.RelocateAfterGrace = false
	if got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source",
		[]lease.Lease{expired}, nil, []string{"provider-new"}, now.Add(time.Hour), 1, 10); err != nil || len(got) != 0 {
		t.Fatalf("disabled relocation produced offer: providers=%v err=%v", got, err)
	}
}

func TestReplicaOfferPlannerNeverReintroducesTombstonedArchive(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	policy := authority.ReplicationPolicy{MinimumReplicas: "1", GracePeriodSeconds: "0", CapacityLimitBytes: "0", RelocateAfterGrace: true}
	tombstone := lease.TombstoneRecord{Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: "archive"}}}
	got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source", nil,
		[]lease.TombstoneRecord{tombstone}, []string{"provider"}, now, 1, 1)
	if err != nil || len(got) != 0 {
		t.Fatalf("tombstoned archive plan=%v err=%v", got, err)
	}
}

func TestReplicaOfferPlannerHonorsBoundedPassAndProviderGrantScope(t *testing.T) {
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	policy := authority.ReplicationPolicy{MinimumReplicas: "4", GracePeriodSeconds: "0", CapacityLimitBytes: "0", RelocateAfterGrace: true}
	got, err := planAutomaticReplicaOffers(policy, "owner", "archive", "snapshot", "source",
		nil, nil, []string{"provider-a", "provider-b", "provider-c", "provider-d"}, now, 2, 10)
	if err != nil || !reflect.DeepEqual(got, []string{"provider-a", "provider-b"}) {
		t.Fatalf("bounded per-pass plan=%v err=%v", got, err)
	}
	grant := authority.Grant{
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: "provider-a"},
		ArchiveID: "archive", Actions: []string{"archive.replicate"},
	}
	chain := []authority.SignedGrant{{Grant: grant}}
	if !replicaGrantTargets(chain, grant.Audience, "archive", "snapshot") {
		t.Fatal("matching provider/archive grant was rejected")
	}
	if replicaGrantTargets(chain, authority.Principal{Kind: authority.PrincipalNode, ID: "provider-b"}, "archive", "snapshot") ||
		replicaGrantTargets(chain, grant.Audience, "other-archive", "snapshot") {
		t.Fatal("provider grant was accepted outside its audience or archive scope")
	}
	grant.Actions = []string{"archive.view"}
	chain[0].Grant = grant
	if replicaGrantTargets(chain, grant.Audience, "archive", "snapshot") {
		t.Fatal("non-replication grant authorized an offer")
	}
	grant.Actions = []string{"archive.replicate"}
	grant.SnapshotID = "snapshot-exact"
	chain[0].Grant = grant
	if replicaGrantTargets(chain, grant.Audience, "archive", "snapshot") || !replicaGrantTargets(chain, grant.Audience, "archive", "snapshot-exact") {
		t.Fatal("snapshot-scoped provider grant was widened")
	}
}

func TestReplicaOfferDiagnosticExposesOnlyBoundedStage(t *testing.T) {
	if got := (replicaOfferReconcileError{stage: "snapshot_encryption"}).DiagnosticStage(); got != "snapshot_encryption" {
		t.Fatalf("safe diagnostic stage=%q", got)
	}
	if got := (replicaOfferReconcileError{stage: "/private/path?token=secret"}).DiagnosticStage(); got != "unknown" {
		t.Fatalf("unbounded diagnostic stage leaked: %q", got)
	}
	if got := (replicaOfferReconcileError{stage: "snapshot_encryption"}).Error(); strings.Contains(got, "snapshot_encryption") || strings.Contains(got, "secret") {
		t.Fatalf("error string exposes internal diagnostics: %q", got)
	}
}

func TestReplicaOfferReconcilerCreatesBoundedOwnerAuthorizedOffersAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	identityID, rootPrivate, localNodeID, localNodePrivate, providerNodeID, providerNodePublic := replicaOfferTestIdentity(t)
	archiveID, err := domain.NewArchiveID()
	if err != nil {
		t.Fatal(err)
	}
	ownerID := "os_aaaaaaaaaaaaaaaaaaaaaa"
	recordingID := "recording-replica-policy-test"
	snapshot := replicaOfferTestSnapshot(t, recordingID, archiveID)
	recipientPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootPublic := rootPrivate.Public().(ed25519.PublicKey)
	rootKeyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.DEKFromBytes(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := cryptoarchive.WrapDEK(dek, archiveID, 1, recipientPrivate.PublicKey().Bytes(), rootKeyID, rootPrivate)
	dek.Destroy()
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID},
		ArchiveID: archiveID, Actions: []string{"archive.replicate"},
		NotBefore: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), PolicyEpoch: "1", IssuerKeyID: rootKeyID,
	}, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	providerGrant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: ownerID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: providerNodeID},
		ArchiveID: archiveID, Actions: []string{"archive.replicate"},
		NotBefore: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("\x01", 32))), PolicyEpoch: "1", IssuerKeyID: rootKeyID,
	}, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	unauthorizedProviderPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	unauthorizedProviderID, err := identityprotocol.NodeID(unauthorizedProviderPublic)
	if err != nil {
		t.Fatal(err)
	}
	owner := authority.AuthoritySnapshot{
		Subject:           authority.OwnerSubject{ID: ownerID, Controller: authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}},
		ReplicationPolicy: authority.ReplicationPolicy{MinimumReplicas: "1", GracePeriodSeconds: "0", CapacityLimitBytes: "0", RelocateAfterGrace: true},
		ArchiveIDs:        []string{archiveID}, Grants: map[string][]authority.SignedGrant{"source-grant": {grant}, "provider-grant": {providerGrant}},
	}
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: "sha256:" + strings.Repeat("1", 64),
		VerifiedAt: now.Add(-time.Minute), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + identityID:         rootPublic,
			authority.PrincipalNode + ":" + localNodeID:            localNodePrivate.Public().(ed25519.PublicKey),
			authority.PrincipalNode + ":" + providerNodeID:         providerNodePublic,
			authority.PrincipalNode + ":" + unauthorizedProviderID: unauthorizedProviderPublic,
		},
	}
	manifestCiphertext := []byte(strings.Repeat("c", 100))
	cipherHash := sha256.Sum256(manifestCiphertext)
	manifest := replication.ReplicaManifest{
		Version: replication.ReplicaManifestVersion, ArchiveID: archiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: "1",
		Objects: []replication.ReplicaObject{{
			Coordinate:     snapshot.Content.Objects[0].Coordinate,
			ObjectID:       "ob_" + base64.RawURLEncoding.EncodeToString(cipherHash[:]),
			CiphertextSize: strconv.Itoa(len(manifestCiphertext)), CiphertextSHA256: "sha256:" + hex.EncodeToString(cipherHash[:]),
		}},
	}
	leases := &replicaOfferTestLeaseStore{}
	manager := &replicaOfferTestManager{
		recordings: []*domain.Recording{{ID: recordingID, ArchiveID: archiveID, ArchiveSealed: true, State: domain.StateStopped}},
		manifest:   manifest,
	}
	reconciler := &replicaOfferReconciler{
		leases: leases,
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{
			IdentityID:                  identityID,
			RecoveryEncryptionPublicKey: base64.RawURLEncoding.EncodeToString(recipientPrivate.PublicKey().Bytes()),
		}, found: true},
		resolve: func(candidate, head string, at time.Time) (authority.IdentityState, error) {
			if candidate != identityID || head != "" || at.Before(now) {
				return authority.IdentityState{}, errors.New("unexpected identity resolution")
			}
			return state, nil
		},
		authority: replicaOfferTestAuthority{owner: owner},
		node:      replicaOfferTestNode{info: nodeidentity.Info{NodeID: localNodeID, PublicKey: base64.RawURLEncoding.EncodeToString(localNodePrivate.Public().(ed25519.PublicKey))}, private: localNodePrivate},
		peers: &fakePeerConnectionManager{sessions: []peerapi.SessionInfo{
			{IdentityID: identityID, NodeID: providerNodeID},
			{IdentityID: identityID, NodeID: unauthorizedProviderID},
		}, connectErr: map[string]error{}},
		catalog:    replicaOfferTestCatalog{snapshot: snapshot},
		ciphertext: replicaOfferTestCiphertext{envelope: envelope},
		manager:    manager,
	}
	if err := reconciler.Reconcile(ctx, now); err != nil {
		t.Fatalf("first offer pass: %v (encrypt calls=%d issued=%d)", err, manager.encryptCalls, len(leases.issued))
	}
	if len(leases.issued) != 1 || leases.issued[0].Offer.Offer.ProviderNodeID != providerNodeID || manager.encryptCalls != 1 {
		t.Fatalf("first pass offers=%d manager encrypt calls=%d", len(leases.issued), manager.encryptCalls)
	}
	if manager.lastRequest.SubjectID != ownerID || manager.lastRequest.SnapshotID != snapshot.SnapshotID ||
		manager.lastRequest.KeyEpoch != 1 || !manager.lastRequest.UseStoredKey || len(manager.lastRequest.DataKey) != 0 || manager.lastRequest.KeyEnvelope != envelope {
		t.Fatalf("scheduler did not use the existing protected DEK/recovery envelope scope: %+v", manager.lastRequest)
	}
	if got := leases.issued[0].Offer.Offer.ReservedBytes; got != strconv.Itoa(len(manifestCiphertext)) {
		t.Fatalf("reserved bytes=%q, want exact ciphertext size %d", got, len(manifestCiphertext))
	}
	if err := reconciler.Reconcile(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("idempotent second pass: %v", err)
	}
	if len(leases.issued) != 1 || manager.encryptCalls != 1 {
		t.Fatalf("pending offer was duplicated: offers=%d encrypt calls=%d", len(leases.issued), manager.encryptCalls)
	}
}

type replicaOfferTestLeaseStore struct{ issued []lease.Lease }

func (s *replicaOfferTestLeaseStore) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	return append([]lease.Lease(nil), s.issued...), nil, nil
}

func (s *replicaOfferTestLeaseStore) IssueOffer(_ context.Context, signed lease.SignedOffer) (lease.Lease, error) {
	hash, err := lease.OfferHash(signed.Offer)
	if err != nil {
		return lease.Lease{}, err
	}
	item := lease.Lease{Offer: signed, OfferHash: hash, Status: lease.StatusOffered, Revision: "1", Progress: []lease.Progress{}}
	s.issued = append(s.issued, item)
	return item, nil
}

type replicaOfferTestAuthority struct{ owner authority.AuthoritySnapshot }

func (a replicaOfferTestAuthority) List(_ authority.AuthorityIdentityResolver, _ time.Time) ([]authority.AuthoritySnapshot, error) {
	return []authority.AuthoritySnapshot{a.owner}, nil
}

func (a replicaOfferTestAuthority) AuthorizeChain(subjectID string, chain []authority.SignedGrant, _ authority.AuthorityIdentityResolver,
	_ time.Time, audience authority.Principal, archiveID, snapshotID, action string) error {
	if subjectID != a.owner.Subject.ID || len(chain) != 1 || audience.Kind != authority.PrincipalNode ||
		audience != chain[0].Grant.Audience || archiveID != chain[0].Grant.ArchiveID ||
		(snapshotID != "" && chain[0].Grant.SnapshotID != "" && snapshotID != chain[0].Grant.SnapshotID) || action != "archive.replicate" {
		return authority.ErrDenied
	}
	return nil
}

type replicaOfferTestNode struct {
	info    nodeidentity.Info
	private ed25519.PrivateKey
}

func (n replicaOfferTestNode) Info() (nodeidentity.Info, error) { return n.info, nil }
func (n replicaOfferTestNode) SignReplicaOffer(offer lease.Offer, chain []authority.SignedGrant) (lease.SignedOffer, error) {
	return lease.SignOffer(offer, authority.Principal{Kind: authority.PrincipalNode, ID: n.info.NodeID}, chain, n.private)
}

type replicaOfferTestCatalog struct{ snapshot archivesnapshot.Snapshot }

func (c replicaOfferTestCatalog) Head(_ context.Context, archiveID string) (archivesnapshot.Snapshot, error) {
	if archiveID != c.snapshot.Content.ArchiveID {
		return archivesnapshot.Snapshot{}, archivecatalog.ErrNotFound
	}
	return c.snapshot, nil
}

type replicaOfferTestCiphertext struct{ envelope cryptoarchive.KeyEnvelope }

func (c replicaOfferTestCiphertext) KeyEpochs(archivesnapshot.Snapshot) ([]uint64, error) {
	return []uint64{1}, nil
}
func (c replicaOfferTestCiphertext) LoadKeyEnvelope(_ archivesnapshot.Snapshot, epoch uint64, _ string) (cryptoarchive.KeyEnvelope, error) {
	if epoch != 1 {
		return cryptoarchive.KeyEnvelope{}, errors.New("missing test envelope")
	}
	return c.envelope, nil
}

type replicaOfferTestManager struct {
	recordings   []*domain.Recording
	manifest     replication.ReplicaManifest
	lastRequest  recorderengine.EncryptArchiveSnapshotRequest
	encryptCalls int
}

func (m *replicaOfferTestManager) List() []*domain.Recording {
	return append([]*domain.Recording(nil), m.recordings...)
}
func (m *replicaOfferTestManager) EncryptArchiveSnapshot(_ context.Context, request recorderengine.EncryptArchiveSnapshotRequest) (replication.ReplicaManifest, error) {
	m.lastRequest = request
	m.encryptCalls++
	if request.SnapshotID != m.manifest.SnapshotID || strconv.FormatUint(request.KeyEpoch, 10) != m.manifest.KeyEpoch {
		return replication.ReplicaManifest{}, errors.New("unexpected snapshot encryption request")
	}
	return m.manifest, nil
}

func replicaOfferPlanningLease(id, ownerID, archiveID, snapshotID, providerID string, status lease.Status, reserved string, expires time.Time) lease.Lease {
	return lease.Lease{Offer: lease.SignedOffer{Offer: lease.Offer{
		LeaseID: id, OwnerSubjectID: ownerID, ArchiveID: archiveID, SnapshotID: snapshotID,
		ProviderNodeID: providerID, ReservedBytes: reserved, ExpiresAt: expires.UTC().Format(time.RFC3339),
	}}, Status: status}
}

func replicaOfferTestIdentity(t *testing.T) (string, ed25519.PrivateKey, string, ed25519.PrivateKey, string, ed25519.PublicKey) {
	t.Helper()
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	localPublic, localPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	localNodeID, err := identityprotocol.NodeID(localPublic)
	if err != nil {
		t.Fatal(err)
	}
	providerPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	providerNodeID, err := identityprotocol.NodeID(providerPublic)
	if err != nil {
		t.Fatal(err)
	}
	return identityID, rootPrivate, localNodeID, localPrivate, providerNodeID, providerPublic
}

func replicaOfferTestSnapshot(t *testing.T, recordingID, archiveID string) archivesnapshot.Snapshot {
	t.Helper()
	session, err := archiveindex.NewSessionIdentity(recordingID, "hls", nil, "broadcast-test")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory(recordingID, session)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ArchiveID = archiveID
	payload := []byte("canonical media")
	digest := sha256.Sum256(payload)
	_, err = archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia},
		ClaimID:    "claim-scheduler-1", Source: archiveindex.ClaimLiveOrigin, AcquiredAt: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC),
		PayloadPath: "recordings/test/media.bin", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		Verification: archiveindex.VerificationVerified, Duration: 2.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	inventory.Capture = archiveindex.CaptureClosed
	inventory.Archive = archiveindex.ArchiveSealed
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
