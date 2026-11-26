package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
	"github.com/integrated-recorder/core/internal/replication"
)

// This is a command/data-channel fixture, not a WebRTC fixture. It keeps the
// authenticated Node session boundary explicit while exercising the actual
// peerapi command signatures, current-authority checks, replay journal,
// encrypted transfer, durable lease transitions, and HTTP handlers.
type replicaInboxMemoryLink struct {
	commands [2]chan []byte
	data     [2]chan []byte
}

type replicaInboxMemorySession struct {
	link      *replicaInboxMemoryLink
	side      int
	remote    authority.Principal
	sessionID string
}

func newReplicaInboxMemorySessions(localNodeID, sourceNodeID string) (*replicaInboxMemorySession, *replicaInboxMemorySession) {
	link := &replicaInboxMemoryLink{}
	for i := range 2 {
		link.commands[i] = make(chan []byte, 32)
		link.data[i] = make(chan []byte, 128)
	}
	const sessionID = "ps_" + "AAAAAAAAAAAAAAAAAAAAAA"
	return &replicaInboxMemorySession{link: link, side: 0, remote: authority.Principal{Kind: authority.PrincipalNode, ID: sourceNodeID}, sessionID: sessionID},
		&replicaInboxMemorySession{link: link, side: 1, remote: authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}, sessionID: sessionID}
}

func (s *replicaInboxMemorySession) Remote() authority.Principal { return s.remote }
func (s *replicaInboxMemorySession) SessionID() string           { return s.sessionID }
func (s *replicaInboxMemorySession) SendCommand(ctx context.Context, command peerprotocol.Command) error {
	raw, err := json.Marshal(command)
	if err != nil {
		return err
	}
	select {
	case s.link.commands[1-s.side] <- raw:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *replicaInboxMemorySession) Send(ctx context.Context, payload []byte) error {
	copyOfPayload := append([]byte(nil), payload...)
	select {
	case s.link.data[1-s.side] <- copyOfPayload:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *replicaInboxMemorySession) Receive(ctx context.Context) ([]byte, error) {
	select {
	case payload := <-s.link.data[s.side]:
		return append([]byte(nil), payload...), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *replicaInboxMemorySession) ReceiveAuthorizedCommand(ctx context.Context, resolve peerprotocol.IdentityResolver, authorizer peerprotocol.CommandAuthorizer,
	replay peertransport.PeerCommandReplayConsumer, localIdentityID, localNodeID string) (peerprotocol.Command, error) {
	select {
	case raw := <-s.link.commands[s.side]:
		command, err := peerprotocol.ParseCommand(raw)
		if err != nil {
			return peerprotocol.Command{}, err
		}
		now := time.Now().UTC()
		if err := peerprotocol.VerifyAndAuthorizeCommand(ctx, command, resolve, authorizer, now, s.sessionID, s.remote, localIdentityID, localNodeID); err != nil {
			return peerprotocol.Command{}, err
		}
		if err := replay.ConsumeCommand(command, resolve, now, s.sessionID, s.remote, localIdentityID, localNodeID); err != nil {
			return peerprotocol.Command{}, err
		}
		return command, nil
	case <-ctx.Done():
		return peerprotocol.Command{}, ctx.Err()
	}
}

type replicaInboxMemoryPeerManager struct {
	localSession *replicaInboxMemorySession
	infos        []peerapi.SessionInfo
	lastErr      error
}

func (m *replicaInboxMemoryPeerManager) List() []peerapi.SessionInfo {
	return append([]peerapi.SessionInfo(nil), m.infos...)
}
func (m *replicaInboxMemoryPeerManager) Connect(context.Context, string, string) error { return nil }
func (m *replicaInboxMemoryPeerManager) Disconnect(string) error                       { return nil }
func (m *replicaInboxMemoryPeerManager) ListReplicaOffers(ctx context.Context, nodeID string, scope peerapi.RequestScope) ([]lease.PendingOffer, error) {
	if nodeID != m.infos[0].NodeID {
		return nil, peerapi.ErrPeerNotConnected
	}
	client, err := peerapi.NewScopedClient(m.localSession, peerapi.MaxPeerResponseBytes, scope)
	if err != nil {
		return nil, err
	}
	offers, err := client.ReplicaOffers(ctx)
	m.lastErr = err
	return offers, err
}
func (m *replicaInboxMemoryPeerManager) ImportReplicaFromPeer(ctx context.Context, sourceNodeID string, scope peerapi.RequestScope, leases *lease.Store,
	ciphertexts *replication.Store, acceptance peerapi.ReplicaAcceptanceSigner, checkpoint replication.ReplicaCheckpointSigner,
	archiveID, snapshotID, leaseID string) (peerapi.ReplicaImportResult, error) {
	if sourceNodeID != m.infos[0].NodeID {
		return peerapi.ReplicaImportResult{}, peerapi.ErrPeerNotConnected
	}
	client, err := peerapi.NewScopedClient(m.localSession, peerapi.MaxPeerResponseBytes, scope)
	if err != nil {
		return peerapi.ReplicaImportResult{}, err
	}
	return client.ImportReplicaFromPeer(ctx, leases, ciphertexts, acceptance, checkpoint, archiveID, snapshotID, leaseID)
}

type replicaInboxAuthorityAdapter struct {
	store   *authority.Store
	resolve authority.AuthorityIdentityResolver
}

func (a replicaInboxAuthorityAdapter) Authorize(ctx context.Context, subjectID, archiveID, snapshotID, action string,
	actor authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.store.AuthorizeChain(subjectID, chain, a.resolve, now, actor, archiveID, snapshotID, action)
}

type replicaInboxKeys map[string]ed25519.PublicKey

func (k replicaInboxKeys) VerifiedKey(_ context.Context, principal authority.Principal, _ time.Time) (ed25519.PublicKey, error) {
	key, ok := k[string(principal.Kind)+":"+principal.ID]
	if !ok {
		return nil, authority.ErrDenied
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

type replicaInboxCanonicalObject struct {
	coordinate archiveindex.Coordinate
	data       []byte
}

func (s replicaInboxCanonicalObject) OpenCanonical(_ context.Context, coordinate archiveindex.Coordinate) (io.ReadCloser, error) {
	if coordinate != s.coordinate {
		return nil, replication.ErrObjectUnavailable
	}
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func TestReplicaInboxHTTPImportsCrossIdentityCiphertextThroughPeerAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	now := time.Now().UTC().Truncate(time.Second)

	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("replica-owner", "replica-owner-password"); err != nil {
		t.Fatal(err)
	}
	webSession, err := users.LoginAs("replica-owner", "replica-owner-password")
	if err != nil {
		t.Fatal(err)
	}

	localIdentity, localRootKey, localIdentityID := replicaInboxCreateIdentity(t, filepath.Join(root, "local-identity"))
	sourceIdentity, sourceRootKey, sourceIdentityID := replicaInboxCreateIdentity(t, filepath.Join(root, "source-identity"))
	localNodeStore, localNode := replicaInboxCreateDelegatedNode(t, filepath.Join(root, "local-node"), localIdentity, localRootKey, now)
	sourceNodeStore, sourceNode := replicaInboxCreateDelegatedNode(t, filepath.Join(root, "source-node"), sourceIdentity, sourceRootKey, now)
	sourceState, err := authority.IdentityStateFromStore(sourceIdentity, now)
	if err != nil {
		t.Fatal(err)
	}
	resolve := authority.IdentityStateResolverFromStores(map[string]*globalidentity.Store{
		localIdentityID: localIdentity, sourceIdentityID: sourceIdentity,
	})
	registry, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "identity-registry"))
	if err != nil {
		t.Fatal(err)
	}
	sourceHistory, err := sourceIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(sourceHistory); err != nil {
		t.Fatal(err)
	}
	directoryOne, err := discovery.OpenDirectory(filepath.Join(root, "directory-one"))
	if err != nil {
		t.Fatal(err)
	}
	directoryTwo, err := discovery.OpenDirectory(filepath.Join(root, "directory-two"))
	if err != nil {
		t.Fatal(err)
	}
	if err := directoryOne.PublishIdentityHistory(sourceHistory); err != nil {
		t.Fatal(err)
	}
	if err := directoryTwo.PublishIdentityHistory(sourceHistory); err != nil {
		t.Fatal(err)
	}
	federation, err := discovery.NewFederationClient([]string{"https://directory-one.example", "https://directory-two.example"}, &http.Client{
		Transport: &serverHistoryFederationTransport{handlers: map[string]http.Handler{
			"directory-one.example": discovery.HTTPHandler(directoryOne), "directory-two.example": discovery.HTTPHandler(directoryTwo),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	archiveSession, err := archiveindex.NewSessionIdentity("replica-inbox-integration", "hls", nil, "source-broadcast")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("replica-inbox-integration", archiveSession)
	if err != nil {
		t.Fatal(err)
	}
	archiveID, err := archivesnapshot.ArchiveIDForRecording(inventory.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ArchiveID = archiveID
	canonical := []byte("cross-identity canonical segment bytes")
	digest := sha256.Sum256(canonical)
	coordinate := archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia}
	if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: coordinate, ClaimID: "replica-inbox-canonical", Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: now, PayloadPath: "private/segment.bin", Size: int64(len(canonical)), SHA256: hex.EncodeToString(digest[:]),
		Verification: archiveindex.VerificationVerified, Duration: 2.5,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}

	subject, err := authority.NewPersonOwnerSubject(sourceIdentityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x41}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, sourceState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(create, sourceRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.Apply(createSigned, resolve, now); err != nil {
		t.Fatal(err)
	}
	sourceGrant := replicaInboxIssueGrant(t, ownerStore, sourceRootKey, sourceState, subject, archiveID, sourceNode.NodeID, now, "source")
	targetGrant := replicaInboxIssueGrant(t, ownerStore, sourceRootKey, sourceState, subject, archiveID, localNode.NodeID, now, "target")

	// Model separate source/target OwnerSubject stores after authority-history
	// synchronization: each side independently replays the same signed events.
	targetOwnerStore, err := authority.Open(filepath.Join(root, "target-owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	authorityEvents, err := ownerStore.Export(subject.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range authorityEvents {
		if err := targetOwnerStore.Apply(event, resolve, now); err != nil {
			t.Fatalf("replicate signed OwnerSubject history to target: %v", err)
		}
	}
	sourceAuthorizer := replicaInboxAuthorityAdapter{store: ownerStore, resolve: resolve}
	targetAuthorizer := replicaInboxAuthorityAdapter{store: targetOwnerStore, resolve: resolve}
	keys := replicaInboxKeys{}
	for _, entry := range []struct {
		kind string
		id   string
		key  ed25519.PublicKey
	}{{"identity", localIdentityID, localRootKey.Public().(ed25519.PublicKey)}, {"identity", sourceIdentityID, sourceRootKey.Public().(ed25519.PublicKey)},
		{"node", localNode.NodeID, replicaInboxNodePublic(t, localNode)}, {"node", sourceNode.NodeID, replicaInboxNodePublic(t, sourceNode)}} {
		keys[entry.kind+":"+entry.id] = entry.key
	}
	leaseID := "rl_" + strings.Repeat("B", 22)
	sourceLeases, err := lease.Open(lease.Options{Root: filepath.Join(root, "source-leases"), Keys: keys, Authorizer: sourceAuthorizer})
	if err != nil {
		t.Fatal(err)
	}
	targetLeaseRoot := filepath.Join(root, "target-leases")
	targetLeases, err := lease.Open(lease.Options{Root: targetLeaseRoot, Keys: keys, Authorizer: targetAuthorizer, LocalProviderNodeID: localNode.NodeID})
	if err != nil {
		t.Fatal(err)
	}
	sourceCiphertexts, err := replication.Open(filepath.Join(root, "source-ciphertexts"))
	if err != nil {
		t.Fatal(err)
	}
	targetCiphertextRoot := filepath.Join(root, "target-ciphertexts")
	targetCiphertexts, err := replication.Open(targetCiphertextRoot)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	sourceAuth := replication.Authorization{SubjectID: subject.ID, Actor: authority.Principal{Kind: authority.PrincipalNode, ID: sourceNode.NodeID}, Chain: []authority.SignedGrant{sourceGrant}, Verifier: sourceAuthorizer}
	bundle, err := sourceCiphertexts.EncryptSnapshot(ctx, snapshot, replicaInboxCanonicalObject{coordinate: coordinate, data: canonical}, dek, 1, sourceAuth)
	if err != nil || len(bundle.Objects) != 1 {
		t.Fatalf("encrypt source snapshot: objects=%d err=%v", len(bundle.Objects), err)
	}
	publicManifest, err := replication.PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := replication.ReplicaManifestHash(snapshot, publicManifest)
	if err != nil {
		t.Fatal(err)
	}
	refs := bundle.CiphertextRefs()
	objects := make([]lease.EncryptedObject, 0, len(refs))
	var reserved int64
	for _, ref := range refs {
		size, parseErr := strconv.ParseInt(ref.CiphertextSize, 10, 64)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		reserved += size
		objects = append(objects, lease.EncryptedObject{ObjectID: ref.ObjectID, CiphertextSize: ref.CiphertextSize, CiphertextSHA256: ref.CiphertextSHA256})
	}
	offer, err := sourceNodeStore.SignReplicaOffer(lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: leaseID, ArchiveID: archiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: "1",
		PlaintextManifestHash: snapshot.ContentHash, ReplicaManifestHash: manifestHash, EncryptedObjectSet: objects,
		ProviderNodeID: localNode.NodeID, OwnerSubjectID: subject.ID, ReservedBytes: strconv.FormatInt(reserved, 10),
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), RenewalPolicy: "manual",
	}, []authority.SignedGrant{sourceGrant})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceLeases.IssueOffer(ctx, offer); err != nil {
		t.Fatalf("persist source offer: %v", err)
	}

	localPeerSession, sourcePeerSession := newReplicaInboxMemorySessions(localNode.NodeID, sourceNode.NodeID)
	sourceService := &peerapi.Service{Catalog: replicaInboxSnapshotCatalog{snapshot}, Ciphertexts: sourceCiphertexts, Leases: sourceLeases, Authorizer: sourceAuthorizer,
		Signer: sourceNodeStore, IdentityID: sourceIdentityID, NodeID: sourceNode.NodeID, Resolve: peerprotocol.IdentityResolver(resolve),
		Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: sourceNodeStore}}
	targetService := &peerapi.Service{Catalog: replicaInboxSnapshotCatalog{snapshot}, ReplicaSnapshots: targetCiphertexts, Ciphertexts: targetCiphertexts,
		Leases: targetLeases, Authorizer: targetAuthorizer, Signer: localNodeStore, IdentityID: localIdentityID, NodeID: localNode.NodeID,
		Resolve: peerprotocol.IdentityResolver(resolve), Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: localNodeStore}}
	serveCtx, stopServe := context.WithCancel(ctx)
	defer stopServe()
	serveErr := make(chan error, 2)
	go func() { serveErr <- sourceService.Serve(serveCtx, sourcePeerSession) }()
	go func() { serveErr <- targetService.Serve(serveCtx, localPeerSession) }()
	manager := &replicaInboxMemoryPeerManager{localSession: localPeerSession,
		infos: []peerapi.SessionInfo{{IdentityID: sourceIdentityID, NodeID: sourceNode.NodeID}}}

	products, err := management.Open(filepath.Join(root, "management"))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: users, Management: products, GlobalIdentity: localIdentity, GlobalIdentityRegistry: registry, IdentityFederation: federation,
		NodeIdentity: localNodeStore, OwnerAuthority: targetOwnerStore, PeerSessions: manager, ReplicaLeases: targetLeases, ReplicaCiphertexts: targetCiphertexts})
	query := "/api/peer-archives/" + sourceNode.NodeID + "/replica-offers?owner_subject_id=" + subject.ID + "&archive_id=" + archiveID
	inbox := httptest.NewRecorder()
	handler.ServeHTTP(inbox, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.test"+query, "", webSession))
	if inbox.Code != http.StatusOK || !strings.Contains(inbox.Body.String(), leaseID) {
		t.Fatalf("real cross-identity inbox status=%d peer_error=%v body=%s", inbox.Code, manager.lastErr, inbox.Body.String())
	}
	if _, err := targetLeases.Get(ctx, leaseID); err == nil {
		t.Fatal("listing an offer unexpectedly accepted or persisted a target lease")
	}
	targetGrantHash, err := authority.GrantHash(targetGrant.Grant)
	if err != nil {
		t.Fatal(err)
	}
	requestBody := `{"source_node_id":"` + sourceNode.NodeID + `","owner_subject_id":"` + subject.ID + `","grant_hash":"` + targetGrantHash +
		`","archive_id":"` + archiveID + `","snapshot_id":"` + snapshot.SnapshotID + `","lease_id":"` + leaseID + `"}`
	imported := httptest.NewRecorder()
	handler.ServeHTTP(imported, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.test/api/replica-imports", requestBody, webSession))
	if imported.Code != http.StatusOK || !strings.Contains(imported.Body.String(), `"objects":1`) {
		t.Fatalf("real cross-identity import status=%d body=%s", imported.Code, imported.Body.String())
	}
	active, err := targetLeases.Get(ctx, leaseID)
	if err != nil || active.Status != lease.StatusActive {
		t.Fatalf("target durable lease status=%q err=%v", active.Status, err)
	}
	sourceActive, err := sourceLeases.Get(ctx, leaseID)
	if err != nil || sourceActive.Status != lease.StatusActive || sourceActive.Acceptance.Signature == "" {
		t.Fatalf("source durable lease status=%q accepted=%t err=%v", sourceActive.Status, sourceActive.Acceptance.Signature != "", err)
	}
	storedSnapshot, err := targetCiphertexts.LoadSnapshot(archiveID, snapshot.SnapshotID)
	if err != nil || storedSnapshot.ContentHash != snapshot.ContentHash {
		t.Fatalf("target persisted snapshot hash=%q err=%v", storedSnapshot.ContentHash, err)
	}
	storedBundle, err := targetCiphertexts.LoadBundle(snapshot, 1)
	if err != nil || len(storedBundle.Objects) != 1 || storedBundle.Objects[0].CiphertextSHA256 != bundle.Objects[0].CiphertextSHA256 {
		t.Fatalf("target persisted ciphertext binding=%+v err=%v", storedBundle.Objects, err)
	}
	reopenedTargetLeases, err := lease.Open(lease.Options{Root: targetLeaseRoot, Keys: keys, Authorizer: targetAuthorizer, LocalProviderNodeID: localNode.NodeID})
	if err != nil {
		t.Fatalf("reopen target durable lease store: %v", err)
	}
	reopenedActive, err := reopenedTargetLeases.Get(ctx, leaseID)
	if err != nil || reopenedActive.Status != lease.StatusActive || reopenedActive.OfferHash != active.OfferHash {
		t.Fatalf("reopened target lease status=%q hash=%q err=%v", reopenedActive.Status, reopenedActive.OfferHash, err)
	}
	reopenedTargetCiphertexts, err := replication.Open(targetCiphertextRoot)
	if err != nil {
		t.Fatalf("reopen target durable ciphertext store: %v", err)
	}
	reopenedBundle, err := reopenedTargetCiphertexts.LoadBundle(snapshot, 1)
	if err != nil || len(reopenedBundle.Objects) != 1 || reopenedBundle.Objects[0].CiphertextSHA256 != bundle.Objects[0].CiphertextSHA256 {
		t.Fatalf("reopened ciphertext binding=%+v err=%v", reopenedBundle.Objects, err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	stopServe()
	for range 2 {
		select {
		case err := <-serveErr:
			if err != nil && err != context.Canceled {
				t.Fatalf("peer service stopped unexpectedly: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("peer service did not stop")
		}
	}
}

type replicaInboxSnapshotCatalog struct{ snapshot archivesnapshot.Snapshot }

func (c replicaInboxSnapshotCatalog) Head(context.Context, string) (archivesnapshot.Snapshot, error) {
	return c.snapshot, nil
}
func (c replicaInboxSnapshotCatalog) Snapshot(_ context.Context, archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	if archiveID != c.snapshot.Content.ArchiveID || snapshotID != c.snapshot.SnapshotID {
		return archivesnapshot.Snapshot{}, peerapi.ErrInvalidRequest
	}
	return c.snapshot, nil
}

func replicaInboxCreateIdentity(t *testing.T, root string) (*globalidentity.Store, ed25519.PrivateKey, string) {
	t.Helper()
	identity, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	private, genesis, signature, handle, credential, challenge, identityID := signedTestGenesis(t)
	if err := identity.CreateGenesis(genesis, signature, handle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	return identity, private, identityID
}

func replicaInboxCreateDelegatedNode(t *testing.T, root string, identity *globalidentity.Store, rootKey ed25519.PrivateKey, now time.Time) (*nodeidentity.Store, nodeidentity.Info) {
	t.Helper()
	store, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	state, initialized, err := identity.Snapshot()
	if err != nil || !initialized {
		t.Fatalf("read identity before node delegation: initialized=%t err=%v", initialized, err)
	}
	proof, err := store.BeginEnrollmentProof(state.IdentityID, state.HeadHash, now)
	if err != nil {
		t.Fatal(err)
	}
	event, signature := signedTestNodeDelegation(t, rootKey, proof, "1", now)
	if err := identity.ApplyNodeEvent(event, signature); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeEnrollmentEvent(event, signature, now); err != nil {
		t.Fatal(err)
	}
	return store, info
}

func replicaInboxIssueGrant(t *testing.T, store *authority.Store, rootKey ed25519.PrivateKey, issuerState authority.IdentityState,
	subject authority.OwnerSubject, archiveID, audienceNode string, now time.Time, nonceSuffix string) authority.SignedGrant {
	t.Helper()
	keyID, err := identityprotocol.KeyID(rootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	nonceHash := sha256.Sum256([]byte("replica-inbox-" + nonceSuffix))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:   authority.Principal{Kind: authority.PrincipalIdentity, ID: subject.Controller.ID},
		Audience: authority.Principal{Kind: authority.PrincipalNode, ID: audienceNode}, ArchiveID: archiveID,
		Actions: []string{"archive.replicate"}, NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonceHash[:]), PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: keyID,
	}, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.Snapshot(subject.ID, func(id, head string, at time.Time) (authority.IdentityState, error) {
		if id != issuerState.IdentityID || head != "" && head != issuerState.HeadHash || !issuerState.FreshUntil.After(at) {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return issuerState, nil
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	event, err := authority.NewAuthorityEvent(subject.ID, strconv.Itoa(len(current.Grants)+2), current.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{grant}}, issuerState, now)
	if err != nil {
		t.Fatal(err)
	}
	signedEvent, err := authority.SignAuthorityEvent(event, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Apply(signedEvent, func(id, head string, at time.Time) (authority.IdentityState, error) {
		if id != issuerState.IdentityID || head != "" && head != issuerState.HeadHash || !issuerState.FreshUntil.After(at) {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return issuerState, nil
	}, now); err != nil {
		t.Fatal(err)
	}
	return grant
}

func replicaInboxNodePublic(t *testing.T, info nodeidentity.Info) ed25519.PublicKey {
	t.Helper()
	key, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		t.Fatalf("decode node public key: %v", err)
	}
	return ed25519.PublicKey(key)
}
