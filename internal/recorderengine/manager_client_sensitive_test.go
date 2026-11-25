package recorderengine_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/replication"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestManagerClientClearsOneShotArchiveKeyOnFailure(t *testing.T) {
	root := shortTempRoot(t)
	store, err := storage.New(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	engine := startTestEngine(t, filepath.Join(root, "ipc"), "key-clear", store, false)
	key := bytes.Repeat([]byte{0x7a}, 32)
	_, err = engine.client.EncryptArchiveSnapshot(context.Background(), recorderengine.EncryptArchiveSnapshotRequest{
		DataKey: key,
	})
	if err == nil {
		t.Fatal("invalid encrypted archive request unexpectedly succeeded")
	}
	if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatal("one-shot archive data key remained in the caller's buffer after the IPC operation")
	}
}

func TestManagerClientClearsOneShotArchiveRekeyKeyOnFailure(t *testing.T) {
	root := shortTempRoot(t)
	generation := "rekey-key-clear"
	handler := archiveRekeyFailure{}
	token := bytes.Repeat([]byte{0x2a}, 32)
	server, err := runtimeipc.NewServer(filepath.Join(root, "ipc", "engine.sock"), generation, token, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-serveDone
	})
	client, err := runtimeipc.NewClient(filepath.Join(root, "ipc", "engine.sock"), generation, token, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := recorderengine.NewManagerClient(client)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{0x73}, 32)
	_, err = manager.RekeyArchiveHistory(context.Background(), recorderengine.RekeyArchiveHistoryRequest{DataKey: key})
	if err == nil {
		t.Fatal("failed archive rekey unexpectedly succeeded")
	}
	if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatal("one-shot archive rekey key remained in the caller's buffer")
	}
}

func TestManagerClientCarriesKeylessStoredArchiveKeyModeOverIPC(t *testing.T) {
	root := shortTempRoot(t)
	generation := "keyless-retry"
	handler := &archiveKeyModeCapture{}
	token := bytes.Repeat([]byte{0x17}, 32)
	server, err := runtimeipc.NewServer(filepath.Join(root, "ipc", "engine.sock"), generation, token, handler)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-serveDone
	})
	client, err := runtimeipc.NewClient(filepath.Join(root, "ipc", "engine.sock"), generation, token, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := recorderengine.NewManagerClient(client)
	if err != nil {
		t.Fatal(err)
	}
	request := recorderengine.EncryptArchiveSnapshotRequest{
		RecordingID: "recording-keyless-retry", SnapshotID: "snapshot-keyless-retry", SubjectID: "owner-subject-keyless-retry",
		GrantChain: []authority.SignedGrant{archiveReplicationTestGrant(t)}, KeyEpoch: 17, UseStoredKey: true,
	}
	_, err = manager.EncryptArchiveSnapshot(context.Background(), request)
	if err != nil {
		t.Fatalf("keyless stored-key request failed: %v", err)
	}
	captured, _ := handler.snapshot()
	if captured.RecordingID != "recording-keyless-retry" || !captured.UseStoredKey || len(captured.DataKey) != 0 || captured.KeyEpoch != 17 {
		t.Fatalf("keyless request was not preserved across Engine IPC: %#v", captured)
	}
	key := bytes.Repeat([]byte{0x7a}, 32)
	if _, err := manager.EncryptArchiveSnapshot(context.Background(), recorderengine.EncryptArchiveSnapshotRequest{
		UseStoredKey: true, DataKey: key,
	}); err == nil {
		t.Fatal("stored-key mode accepted clear key bytes")
	}
	if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatal("rejected stored-key request did not clear caller key bytes")
	}
	if handler.callCount() != 1 {
		t.Fatalf("invalid stored-key request reached Engine IPC: calls=%d", handler.callCount())
	}
}

func archiveReplicationTestGrant(t *testing.T) authority.SignedGrant {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := identityprotocol.KeyID(public)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: "os_AAAAAAAAAAAAAAAAAAAAAA",
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: "gi_AAAAAAAAAAAAAAAAAAAAAA"},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: "nd_BBBBBBBBBBBBBBBBBBBBBB"},
		ArchiveID: "arc_CCCCCCCCCCCCCCCCCCCCCC", Actions: []string{"archive.replicate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(nonce), PolicyEpoch: "1", IssuerKeyID: keyID,
	}
	signed, err := authority.SignGrant(grant, private)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

type archiveKeyModeCapture struct {
	mu      sync.Mutex
	request recorderengine.EncryptArchiveSnapshotRequest
	calls   int
}

type archiveRekeyFailure struct{}

func (archiveRekeyFailure) Handle(_ context.Context, operation string, _ json.RawMessage) (any, error) {
	if operation != recorderengine.OperationRekeyArchiveHistory {
		return nil, errors.New("unexpected operation")
	}
	return nil, errors.New("simulated failure")
}

func (h *archiveKeyModeCapture) Handle(_ context.Context, operation string, payload json.RawMessage) (any, error) {
	if operation != recorderengine.OperationEncryptArchiveSnapshot {
		return nil, errors.New("unexpected operation")
	}
	var request recorderengine.EncryptArchiveSnapshotRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.request = request
	h.calls++
	h.mu.Unlock()
	return recorderengine.EncryptArchiveSnapshotResult{Manifest: replication.ReplicaManifest{
		Version: replication.ReplicaManifestVersion, ArchiveID: "arc_keyless_retry", SnapshotID: request.SnapshotID,
		KeyEpoch: strconv.FormatUint(request.KeyEpoch, 10), Objects: []replication.ReplicaObject{{ObjectID: "ob_keyless_retry"}},
	}}, nil
}

func (h *archiveKeyModeCapture) snapshot() (recorderengine.EncryptArchiveSnapshotRequest, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.request, h.calls
}

func (h *archiveKeyModeCapture) callCount() int {
	_, count := h.snapshot()
	return count
}
