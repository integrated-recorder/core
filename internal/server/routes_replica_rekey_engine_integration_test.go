package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/replication"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestArchiveReplicaRekeyHTTPRouteThroughEngineIPCAndDurableStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("/tmp", "irk-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	archiveStore, err := storage.New(filepath.Join(root, "recordings"))
	if err != nil {
		t.Fatal(err)
	}
	const recordingID = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	writeProductRecording(t, archiveStore, recordingID, domain.StateStopped)
	acquirer, err := acquire.NewManager(archiveStore, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquirer.SealArchiveContext(ctx, recordingID); err != nil {
		t.Fatalf("seal archive fixture: %v", err)
	}
	catalog, err := archivecatalog.Open(filepath.Join(root, "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := acquirer.PersistArchiveSnapshot(ctx, catalog, recordingID)
	if err != nil || snapshot.SnapshotID == "" || snapshot.Content.ArchiveID == "" {
		t.Fatalf("persist sealed snapshot: snapshot=%+v err=%v", snapshot, err)
	}
	archiveID := snapshot.Content.ArchiveID

	users, err := authn.Open(filepath.Join(root, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	if err := users.Bootstrap("rekey-engine-owner", "rekey-engine-test-password"); err != nil {
		t.Fatal(err)
	}
	session, err := users.LoginAs("rekey-engine-owner", "rekey-engine-test-password")
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(filepath.Join(root, "management"))
	if err != nil {
		t.Fatal(err)
	}

	localIdentity, err := globalidentity.Open(filepath.Join(root, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	localRootKey, localGenesis, localSignature, localHandle, localCredential, localChallenge, localIdentityID := signedTestGenesis(t)
	if err := localIdentity.CreateGenesis(localGenesis, localSignature, localHandle, localCredential, localChallenge); err != nil {
		t.Fatal(err)
	}
	nodeStore, err := nodeidentity.Open(filepath.Join(root, "node"))
	if err != nil {
		t.Fatal(err)
	}
	localNode, err := nodeStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	localBefore, initialized, err := localIdentity.Snapshot()
	if err != nil || !initialized {
		t.Fatalf("local identity snapshot: initialized=%v err=%v", initialized, err)
	}
	proof, err := nodeStore.BeginEnrollmentProof(localIdentityID, localBefore.HeadHash, now)
	if err != nil {
		t.Fatal(err)
	}
	delegation, delegationSignature := signedTestNodeDelegation(t, localRootKey, proof, "1", now)
	if err := localIdentity.ApplyNodeEvent(delegation, delegationSignature); err != nil {
		t.Fatalf("delegate local node: %v", err)
	}

	_, recipientGenesis, recipientSignature, recipientHandle, recipientCredential, recipientChallenge, recipientIdentityID := signedTestGenesis(t)
	recipientIdentity, err := globalidentity.Open(filepath.Join(root, "recipient-identity"))
	if err != nil {
		t.Fatal(err)
	}
	if err := recipientIdentity.CreateGenesis(recipientGenesis, recipientSignature, recipientHandle, recipientCredential, recipientChallenge); err != nil {
		t.Fatal(err)
	}
	recipientHistory, err := recipientIdentity.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	directoryA, err := discovery.OpenDirectory(filepath.Join(root, "discovery-a"))
	if err != nil {
		t.Fatal(err)
	}
	directoryB, err := discovery.OpenDirectory(filepath.Join(root, "discovery-b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := directoryA.PublishIdentityHistory(recipientHistory); err != nil {
		t.Fatal(err)
	}
	if err := directoryB.PublishIdentityHistory(recipientHistory); err != nil {
		t.Fatal(err)
	}
	transport := &countingHistoryFederationTransport{
		handlers: map[string]http.Handler{
			"rekey-a.example": discovery.HTTPHandler(directoryA),
			"rekey-b.example": discovery.HTTPHandler(directoryB),
		},
		calls: map[string]int{},
	}
	federation, err := discovery.NewFederationClient([]string{"https://rekey-a.example", "https://rekey-b.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "identity-registry"))
	if err != nil {
		t.Fatal(err)
	}
	ownerAuthority, err := authority.Open(filepath.Join(root, "owner-authority"))
	if err != nil {
		t.Fatal(err)
	}
	localState, err := authority.IdentityStateFromStore(localIdentity, now)
	if err != nil {
		t.Fatal(err)
	}
	resolveLocal := authority.IdentityStateResolverFromStores(map[string]*globalidentity.Store{localIdentityID: localIdentity})
	subject, err := authority.NewPersonOwnerSubject(localIdentityID, "1", now, bytes.NewReader(bytes.Repeat([]byte{0x59}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	create, err := authority.NewAuthorityEvent(subject.ID, "1", "", authority.AuthorityCreateSubject,
		authority.SubjectCreatePayload{Subject: subject, ArchiveIDs: []string{archiveID}}, localState, now)
	if err != nil {
		t.Fatal(err)
	}
	createSigned, err := authority.SignAuthorityEvent(create, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerAuthority.Apply(createSigned, resolveLocal, now); err != nil {
		t.Fatalf("claim archive: %v", err)
	}
	rootPublic := localRootKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	nonce := sha256.Sum256([]byte("rekey engine integration capability"))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subject.ID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: localIdentityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: localNode.NodeID},
		ArchiveID: archiveID, SnapshotID: snapshot.SnapshotID,
		Actions:   []string{"archive.replicate", "key.rotate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce[:]),
		PolicyEpoch: subject.PolicyEpoch, IssuerKeyID: keyID,
	}, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	ownerSnapshot, err := ownerAuthority.Snapshot(subject.ID, resolveLocal, now)
	if err != nil {
		t.Fatal(err)
	}
	issue, err := authority.NewAuthorityEvent(subject.ID, "2", ownerSnapshot.HeadHash, authority.AuthorityIssueGrant,
		authority.GrantIssuePayload{Chain: []authority.SignedGrant{grant}}, localState, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned, err := authority.SignAuthorityEvent(issue, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerAuthority.Apply(issueSigned, resolveLocal, now); err != nil {
		t.Fatalf("issue rekey grants: %v", err)
	}
	grantHash, err := authority.GrantHash(grant.Grant)
	if err != nil {
		t.Fatal(err)
	}

	localRecoveryKey, err := base64.RawURLEncoding.DecodeString(localBefore.RecoveryEncryptionPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	localDEKBytes := make([]byte, 32)
	if _, err := rand.Read(localDEKBytes); err != nil {
		t.Fatal(err)
	}
	localDEK, err := cryptoarchive.DEKFromBytes(localDEKBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer localDEK.Destroy()
	localEnvelope, err := cryptoarchive.WrapDEK(localDEK, archiveID, 1, localRecoveryKey, keyID, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	replicaStore, err := replication.Open(filepath.Join(root, "replication"))
	if err != nil {
		t.Fatal(err)
	}

	engine, err := recorderengine.New(acquirer, &adapterhost.Host{}, strings.Repeat("a", 32), strings.Repeat("b", 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.ConfigureArchiveCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if err := engine.ConfigureArchiveReplication(replicaStore, localIdentity, ownerAuthority, nodeStore); err != nil {
		t.Fatal(err)
	}
	var ipcToken [32]byte
	if _, err := rand.Read(ipcToken[:]); err != nil {
		t.Fatal(err)
	}
	ipcServer, err := runtimeipc.NewServer(filepath.Join(root, "ipc", "engine.sock"), strings.Repeat("a", 32), ipcToken[:], engine)
	if err != nil {
		t.Fatal(err)
	}
	ipcCtx, stopIPC := context.WithCancel(ctx)
	serveDone := make(chan error, 1)
	go func() { serveDone <- ipcServer.Serve(ipcCtx) }()
	ipcClient, err := runtimeipc.NewClient(filepath.Join(root, "ipc", "engine.sock"), strings.Repeat("a", 32), ipcToken[:], 5*time.Second)
	if err != nil {
		stopIPC()
		t.Fatal(err)
	}
	var ready recorderengine.ReadyResult
	readyBy := time.Now().Add(3 * time.Second)
	for {
		if err := ipcClient.Call(ctx, recorderengine.OperationReady, nil, &ready); err == nil && ready.Ready {
			break
		}
		if time.Now().After(readyBy) {
			stopIPC()
			t.Fatal("real Recorder Engine IPC did not become ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	managerClient, err := recorderengine.NewManagerClient(ipcClient)
	if err != nil {
		stopIPC()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopIPC()
		if err := <-serveDone; err != nil {
			t.Errorf("Engine IPC server shutdown: %v", err)
		}
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		if err := engine.Close(closeCtx); err != nil {
			t.Errorf("Engine shutdown: %v", err)
		}
	})

	manifest, err := managerClient.EncryptArchiveSnapshot(ctx, recorderengine.EncryptArchiveSnapshotRequest{
		RecordingID: recordingID, SnapshotID: snapshot.SnapshotID, SubjectID: subject.ID,
		GrantChain: []authority.SignedGrant{grant}, KeyEpoch: 1, DataKey: append([]byte(nil), localDEKBytes...), KeyEnvelope: localEnvelope,
	})
	clear(localDEKBytes)
	if err != nil || manifest.ArchiveID != archiveID || manifest.SnapshotID != snapshot.SnapshotID || len(manifest.Objects) == 0 {
		t.Fatalf("seed real epoch-1 ciphertext and DEK: manifest=%+v err=%v", manifest, err)
	}

	newDEKBytes := make([]byte, 32)
	if _, err := rand.Read(newDEKBytes); err != nil {
		t.Fatal(err)
	}
	newDEK, err := cryptoarchive.DEKFromBytes(newDEKBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer newDEK.Destroy()
	recipientFixtureState, err := authority.IdentityStateFromStore(recipientIdentity, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	recipientEnvelope, err := cryptoarchive.WrapDEK(newDEK, archiveID, 2, recipientFixtureState.RecoveryEncryptionPublicKey, keyID, localRootKey)
	if err != nil {
		t.Fatal(err)
	}
	httpHandler := NewWithOptions(managerClient, nil, nil, Options{
		Auth: users, Management: products, GlobalIdentity: localIdentity,
		GlobalIdentityRegistry: registry, IdentityFederation: federation,
		OwnerAuthority: ownerAuthority, ArchiveCatalog: catalog, NodeIdentity: nodeStore,
	})
	body, err := json.Marshal(archiveReplicaRekeyRequest{
		OwnerSubjectID: subject.ID, GrantHash: grantHash, RecipientIdentityID: recipientIdentityID,
		PreviousKeyEpoch: 1, KeyEpoch: 2, DataKey: base64.StdEncoding.EncodeToString(newDEKBytes), KeyEnvelope: recipientEnvelope,
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	httpHandler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodPost,
		"https://recorder.example.com/api/recordings/"+recordingID+"/replicas/rekey", string(body), session))
	clear(newDEKBytes)
	if response.Code != http.StatusOK {
		t.Fatalf("real Engine rekey route status=%d body=%s", response.Code, response.Body.String())
	}
	var result recorderengine.RekeyArchiveHistoryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode route rekey result: %v; body=%s", err, response.Body.String())
	}
	verification := result.Verification
	if verification.ArchiveID != archiveID || verification.PreviousKeyEpoch != "1" || verification.NewKeyEpoch != "2" ||
		verification.HeadSnapshotID != snapshot.SnapshotID || verification.SnapshotCount != "1" ||
		verification.RecipientKeyID != recipientEnvelope.RecipientKeyID || result.Envelope != recipientEnvelope {
		t.Fatalf("route returned incomplete durable rekey verification: result=%+v", result)
	}
	audit := products.Audit(10)
	if len(audit) != 1 || audit[0].Type != "archive_history_rekey_prepared" || audit[0].ObjectID != archiveID {
		t.Fatalf("successful real rekey audit=%+v", audit)
	}
	if transport.callsFor("rekey-a.example") == 0 || transport.callsFor("rekey-b.example") == 0 {
		t.Fatalf("route did not query every configured identity mirror: calls=%d,%d", transport.callsFor("rekey-a.example"), transport.callsFor("rekey-b.example"))
	}

	// Reopen both durable stores, recover Node-Key-wrapped source DEKs, and
	// independently verify the complete old/new ciphertext history.
	reopenedReplication, err := replication.Open(filepath.Join(root, "replication"))
	if err != nil {
		t.Fatal(err)
	}
	oldKeyBytes, err := reopenedReplication.LoadArchiveDEK(archiveID, 1, nodeStore)
	if err != nil {
		t.Fatalf("load durable epoch-1 DEK: %v", err)
	}
	defer clear(oldKeyBytes)
	newKeyBytes, err := reopenedReplication.LoadArchiveDEK(archiveID, 2, nodeStore)
	if err != nil {
		t.Fatalf("load durable epoch-2 DEK: %v", err)
	}
	defer clear(newKeyBytes)
	oldDEK, err := cryptoarchive.DEKFromBytes(oldKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer oldDEK.Destroy()
	loadedNewDEK, err := cryptoarchive.DEKFromBytes(newKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer loadedNewDEK.Destroy()
	if !loadedNewDEK.SameKey(newDEK) {
		t.Fatal("durable epoch-2 key does not match the requested rekey key")
	}
	reopenedCatalog, err := archivecatalog.Open(filepath.Join(root, "archive-catalog"))
	if err != nil {
		t.Fatal(err)
	}
	independent, err := reopenedReplication.VerifyRekeyHistory(ctx, reopenedCatalog, archiveID, 1, 2, oldDEK, loadedNewDEK, rootPublic, recipientFixtureState.RecoveryEncryptionPublicKey)
	if err != nil {
		t.Fatalf("independently verify durable rekey history: %v", err)
	}
	if independent != verification {
		t.Fatalf("durable verification differs from HTTP result: durable=%+v route=%+v", independent, verification)
	}
}
