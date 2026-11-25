package replication

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
)

type mapSource struct {
	coordinate archiveindex.Coordinate
	payload    []byte
	opened     int
}

func TestSourceArchiveDEKIsLocallyWrappedImmutableAndRestartable(t *testing.T) {
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	identityRoot := t.TempDir()
	node, err := nodeidentity.Open(identityRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Create(); err != nil {
		t.Fatal(err)
	}
	snapshot, _, _ := snapshotFixture(t)
	dataKey := bytes.Repeat([]byte{0x37}, 32)
	if err := store.PersistArchiveDEK(snapshot, 9, dataKey, node); err != nil {
		t.Fatalf("persist source DEK: %v", err)
	}
	path := store.archiveDEKPath(snapshot.Content.ArchiveID, 9)
	sealed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, dataKey) {
		t.Fatal("durable archive key file contains plaintext DEK")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("archive key file mode info=%v err=%v", info, err)
	}
	if err := store.PersistArchiveDEK(snapshot, 9, dataKey, node); err != nil {
		t.Fatalf("same-key retry was not idempotent: %v", err)
	}
	if err := store.PersistArchiveDEK(snapshot, 9, bytes.Repeat([]byte{0x38}, 32), node); !errors.Is(err, ErrInvalid) {
		t.Fatalf("changed key reused immutable epoch: %v", err)
	}

	reopenedStore, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	reopenedNode, err := nodeidentity.Open(identityRoot)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopenedStore.LoadArchiveDEK(snapshot.Content.ArchiveID, 9, reopenedNode)
	if err != nil || !bytes.Equal(loaded, dataKey) {
		matched := bytes.Equal(loaded, dataKey)
		clear(loaded)
		t.Fatalf("restart key recovery matches=%t err=%v", matched, err)
	}
	clear(loaded)
	if _, err := reopenedStore.LoadArchiveDEK(snapshot.Content.ArchiveID, 10, reopenedNode); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("missing key epoch returned unexpected error: %v", err)
	}
	wrongNodeRoot := t.TempDir()
	wrongNode, err := nodeidentity.Open(wrongNodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrongNode.Create(); err != nil {
		t.Fatal(err)
	}
	if _, err := reopenedStore.LoadArchiveDEK(snapshot.Content.ArchiveID, 9, wrongNode); !errors.Is(err, ErrInvalid) {
		t.Fatalf("different Node Key loaded source DEK: %v", err)
	}
	sealed[len(sealed)-1] ^= 0x01
	if err := os.WriteFile(path, sealed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reopenedStore.LoadArchiveDEK(snapshot.Content.ArchiveID, 9, reopenedNode); !errors.Is(err, ErrInvalid) {
		t.Fatalf("tampered wrapped DEK accepted: %v", err)
	}
}

func TestSessionDEKEnvelopeRequiresValidatedSnapshotAndReopensForBoundBrowserKey(t *testing.T) {
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	node, err := nodeidentity.Open(filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	nodeInfo, err := node.Create()
	if err != nil {
		t.Fatal(err)
	}
	nodePublic, err := base64.RawURLEncoding.DecodeString(nodeInfo.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	source := &mapSource{coordinate: coordinate, payload: payload}
	keyBytes := bytes.Repeat([]byte{0x42}, 32)
	dek, err := cryptoarchive.DEKFromBytes(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	if _, err := store.EncryptSnapshot(context.Background(), snapshot, source, dek, 4, testAuthorization(&testAuthorizer{})); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistArchiveDEK(snapshot, 4, keyBytes, node); err != nil {
		t.Fatal(err)
	}
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identityprotocol.NodeID(ed25519.PublicKey(nodePublic))
	if err != nil {
		t.Fatal(err)
	}
	binding := cryptoarchive.SessionDEKBinding{
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: 4,
		OwnerSubjectID: "os_aaaaaaaaaaaaaaaaaaaaaa", GrantHash: "sha256:" + strings.Repeat("a", 64),
		SupplierNodeID: nodeID,
	}
	envelope, err := store.SealSessionDEKEnvelope(snapshot, 4, recipient.PublicKey().Bytes(), binding, node)
	if err != nil {
		t.Fatalf("seal session DEK envelope: %v", err)
	}
	opened, err := cryptoarchive.OpenSessionDEK(envelope, recipient.Bytes(), binding)
	if err != nil || !bytes.Equal(opened, keyBytes) {
		clear(opened)
		t.Fatalf("open session DEK matches=%t err=%v", bytes.Equal(opened, keyBytes), err)
	}
	clear(opened)
	wrongSnapshot := binding
	wrongSnapshot.SnapshotID = "snap_" + strings.Repeat("2", 64)
	if _, err := cryptoarchive.OpenSessionDEK(envelope, recipient.Bytes(), wrongSnapshot); !errors.Is(err, cryptoarchive.ErrSessionEnvelopeUnauthorized) {
		t.Fatalf("wrong snapshot binding accepted: %v", err)
	}
}

func TestConcurrentArchiveDEKMutationCannotReplaceEpoch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	nodeRoot := t.TempDir()
	node, err := nodeidentity.Open(nodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Create(); err != nil {
		t.Fatal(err)
	}
	snapshot, _, _ := snapshotFixture(t)
	keys := [][]byte{bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 32)}
	start := make(chan struct{})
	results := make(chan error, len(keys))
	var wait sync.WaitGroup
	for _, key := range keys {
		wait.Add(1)
		go func(key []byte) {
			defer wait.Done()
			<-start
			results <- store.PersistArchiveDEK(snapshot, 12, key, node)
		}(key)
	}
	close(start)
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrInvalid):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent key persistence result: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successful immutable writes=%d conflicts=%d", successes, conflicts)
	}
	loaded, err := store.LoadArchiveDEK(snapshot.Content.ArchiveID, 12, node)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(loaded)
	if !bytes.Equal(loaded, keys[0]) && !bytes.Equal(loaded, keys[1]) {
		t.Fatal("durable epoch contains neither concurrently proposed key")
	}
}

func TestLocallyRecoveredDEKRestoresEncryptedSnapshotAfterStoreReopen(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	nodeRoot := t.TempDir()
	node, err := nodeidentity.Open(nodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Create(); err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dataKey := bytes.Repeat([]byte{0x53}, 32)
	dek, err := cryptoarchive.DEKFromBytes(dataKey)
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	if err := store.PersistArchiveDEK(snapshot, 3, dataKey, node); err != nil {
		t.Fatal(err)
	}
	bundle, err := store.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 3, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatalf("encrypt with source key: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	reopenedNode, err := nodeidentity.Open(nodeRoot)
	if err != nil {
		t.Fatal(err)
	}
	recoveredBytes, err := reopened.LoadArchiveDEK(snapshot.Content.ArchiveID, 3, reopenedNode)
	if err != nil {
		t.Fatalf("recover source key after reopen: %v", err)
	}
	recovered, err := cryptoarchive.DEKFromBytes(recoveredBytes)
	clear(recoveredBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Destroy()
	destination := filepath.Join(t.TempDir(), "restored")
	manifest, err := reopened.RestoreSnapshot(ctx, snapshot, bundle, recovered, destination, testAuthorization(&testAuthorizer{}))
	if err != nil || len(manifest.Objects) != 1 {
		t.Fatalf("restore with recovered source key objects=%d err=%v", len(manifest.Objects), err)
	}
	actual, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(manifest.Objects[0].Path)))
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatalf("recovered-key plaintext differs from canonical source: size=%d err=%v", len(actual), err)
	}
}

func TestEncryptSnapshotRetryVerifiesEveryCiphertextObject(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, source := twoObjectSnapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()

	bundle, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil || len(bundle.Objects) != 2 {
		t.Fatalf("encrypt two-object snapshot: objects=%d err=%v", len(bundle.Objects), err)
	}
	if _, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(&testAuthorizer{})); err != nil {
		t.Fatalf("valid immutable retry failed: %v", err)
	}

	// A retry must verify the entire immutable object set, not just the first
	// object whose key happens to be valid. Corrupt the second ciphertext while
	// leaving its bundle metadata and the first object intact.
	secondPath := filepath.Join(root, objectDir, bundle.Objects[1].ObjectID+".irac")
	ciphertext, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext[len(ciphertext)-1] ^= 0x80
	if err := os.WriteFile(secondPath, ciphertext, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(&testAuthorizer{})); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("retry accepted a corrupted non-first ciphertext object: %v", err)
	}
	nextDEK, err := cryptoarchive.DEKFromBytes(bytes.Repeat([]byte{0x67}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer nextDEK.Destroy()
	if _, err := store.RekeySnapshot(ctx, snapshot, source, dek, 1, nextDEK, 2, testAuthorization(&testAuthorizer{})); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("rekey accepted an invalid prior object set: %v", err)
	}
	if _, err := store.LoadBundle(snapshot, 2); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed rekey published a new epoch manifest: %v", err)
	}
}

func TestRekeySnapshotPreservesCanonicalBytesAndRetainsOldEpoch(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, source := twoObjectSnapshotFixture(t)
	previousDEK, err := cryptoarchive.DEKFromBytes(bytes.Repeat([]byte{0x41}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer previousDEK.Destroy()
	nextDEK, err := cryptoarchive.DEKFromBytes(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer nextDEK.Destroy()
	auth := testAuthorization(&testAuthorizer{})
	previous, err := store.EncryptSnapshot(ctx, snapshot, source, previousDEK, 4, auth)
	if err != nil || len(previous.Objects) != 2 {
		t.Fatalf("create prior key epoch: objects=%d err=%v", len(previous.Objects), err)
	}
	next, err := store.RekeySnapshot(ctx, snapshot, source, previousDEK, 4, nextDEK, 5, auth)
	if err != nil || len(next.Objects) != 2 || next.KeyEpoch != "5" {
		t.Fatalf("rekey snapshot: bundle=%+v err=%v", next, err)
	}
	for index := range previous.Objects {
		if previous.Objects[index].Coordinate != next.Objects[index].Coordinate || previous.Objects[index].ObjectID == next.Objects[index].ObjectID {
			t.Fatalf("rekey did not preserve coordinates with distinct ciphertext: previous=%+v next=%+v", previous.Objects[index], next.Objects[index])
		}
	}

	for _, item := range []struct {
		name string
		key  *cryptoarchive.DEK
		b    Bundle
	}{{"old", previousDEK, previous}, {"new", nextDEK, next}} {
		destination := filepath.Join(t.TempDir(), item.name)
		manifest, err := store.RestoreSnapshot(ctx, snapshot, item.b, item.key, destination, auth)
		if err != nil || len(manifest.Objects) != len(source) {
			t.Fatalf("restore %s epoch: object count=%d err=%v", item.name, len(manifest.Objects), err)
		}
		for _, restored := range manifest.Objects {
			actual, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(restored.Path)))
			if err != nil || !bytes.Equal(actual, source[restored.Coordinate]) {
				t.Fatalf("%s epoch changed canonical bytes at %+v: err=%v", item.name, restored.Coordinate, err)
			}
		}
	}
	deniedAuth := testAuthorization(&testAuthorizer{err: errors.New("key rotation capability denied")})
	if _, err := store.RekeySnapshot(ctx, snapshot, source, previousDEK, 4, nextDEK, 6, deniedAuth); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("rekey proceeded without key.rotate authorization: %v", err)
	}
	if _, err := store.LoadBundle(snapshot, 6); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unauthorized rekey published a new epoch: %v", err)
	}

	if _, err := store.RekeySnapshot(ctx, snapshot, source, previousDEK, 4, previousDEK, 6, auth); !errors.Is(err, ErrInvalid) {
		t.Fatalf("same DEK reused at a newer key epoch: %v", err)
	}
	if _, err := store.RekeySnapshot(ctx, snapshot, source, previousDEK, 3, nextDEK, 6, auth); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("missing prior epoch did not fail closed: %v", err)
	}
}

type coordinateMapSource map[archiveindex.Coordinate][]byte

type ciphertextRangeMap struct {
	objectID string
	data     []byte
	requests [][2]int64
	extra    bool
}

func (s *ciphertextRangeMap) OpenCiphertextRange(_ context.Context, archiveID, snapshotID, objectID string, keyEpoch uint64, offset, length int64) (io.ReadCloser, error) {
	if archiveID == "" || snapshotID == "" || objectID != s.objectID || keyEpoch == 0 || offset < 0 || length < 1 || offset+length > int64(len(s.data)) {
		return nil, ErrObjectUnavailable
	}
	s.requests = append(s.requests, [2]int64{offset, length})
	body := append([]byte(nil), s.data[offset:offset+length]...)
	if s.extra {
		body = append(body, 0)
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (source coordinateMapSource) OpenCanonical(_ context.Context, coordinate archiveindex.Coordinate) (io.ReadCloser, error) {
	payload, ok := source[coordinate]
	if !ok {
		return nil, errors.New("fixture source coordinate mismatch")
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (s *mapSource) OpenCanonical(_ context.Context, coordinate archiveindex.Coordinate) (io.ReadCloser, error) {
	s.opened++
	if coordinate != s.coordinate {
		return nil, errors.New("fixture source coordinate mismatch")
	}
	return io.NopCloser(bytes.NewReader(s.payload)), nil
}

type testAuthorizer struct {
	err       error
	calls     int
	action    string
	denyOn    int
	checkedAt []time.Time
}

type replicaLeaseTestKeys map[string]ed25519.PublicKey

func (keys replicaLeaseTestKeys) VerifiedKey(_ context.Context, principal authority.Principal, _ time.Time) (ed25519.PublicKey, error) {
	key, ok := keys[principal.Kind+":"+principal.ID]
	if !ok {
		return nil, lease.ErrDenied
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

type replicaLeaseTestAuthorizer struct {
	subjectID  string
	archiveID  string
	snapshotID string
}

func (a replicaLeaseTestAuthorizer) Authorize(_ context.Context, subjectID, archiveID, snapshotID, action string, actor authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	if subjectID != a.subjectID || archiveID != a.archiveID || snapshotID != a.snapshotID || action != "archive.replicate" ||
		actor.Kind != authority.PrincipalIdentity || len(chain) != 1 || now.IsZero() {
		return lease.ErrDenied
	}
	return nil
}

type replicaLeaseState struct {
	mu             sync.Mutex
	current        lease.Lease
	tombstones     []lease.TombstoneRecord
	checkpointKey  ed25519.PublicKey
	store          *Store
	checkpointPath string
}

func (s *replicaLeaseState) Get(context.Context, string) (lease.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneTestLease(s.current), nil
}

func (s *replicaLeaseState) Snapshot(context.Context) ([]lease.Lease, []lease.TombstoneRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []lease.Lease{cloneTestLease(s.current)}, append([]lease.TombstoneRecord(nil), s.tombstones...), nil
}

func (s *replicaLeaseState) Checkpoint(_ context.Context, signed lease.SignedCheckpoint) (lease.Lease, error) {
	canonical, err := lease.CanonicalCheckpoint(signed.Checkpoint)
	signature, decodeErr := base64.RawURLEncoding.DecodeString(signed.Signature)
	if err != nil || decodeErr != nil || !ed25519.Verify(s.checkpointKey, append([]byte("IR/replica-lease/v1/checkpoint\x00"), canonical...), signature) {
		return lease.Lease{}, lease.ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkpointPath != "" {
		info, statErr := os.Stat(s.checkpointPath)
		if statErr != nil || info.Size() != mustInt64(signed.Checkpoint.CommittedOffset) {
			return lease.Lease{}, errors.New("checkpoint preceded durable staged bytes")
		}
	}
	checkpoint := signed.Checkpoint
	if s.current.Status != lease.StatusTransferring || checkpoint.LeaseID != s.current.Offer.Offer.LeaseID || checkpoint.OfferHash != s.current.OfferHash || checkpoint.ProviderNodeID != s.current.Offer.Offer.ProviderNodeID {
		return lease.Lease{}, lease.ErrTransition
	}
	for i := range s.current.Progress {
		progress := &s.current.Progress[i]
		if progress.ObjectID != checkpoint.ObjectID {
			continue
		}
		previous := mustInt64(progress.CommittedOffset)
		next := mustInt64(checkpoint.CommittedOffset)
		if next < previous {
			return lease.Lease{}, lease.ErrConflict
		}
		if next == previous {
			return cloneTestLease(s.current), nil
		}
		progress.CommittedOffset, progress.UpdatedAt = checkpoint.CommittedOffset, checkpoint.RecordedAt
		s.current.Checkpoints = []lease.SignedCheckpoint{signed}
		all := true
		for index, candidate := range s.current.Progress {
			if candidate.CommittedOffset != s.current.Offer.Offer.EncryptedObjectSet[index].CiphertextSize {
				all = false
				break
			}
		}
		if all {
			s.current.Status = lease.StatusVerifying
		}
		return cloneTestLease(s.current), nil
	}
	return lease.Lease{}, lease.ErrInvalid
}

func (s *replicaLeaseState) VerifyReplica(ctx context.Context, leaseID string, reader lease.ObjectReader) (lease.Lease, error) {
	current, err := s.Get(ctx, leaseID)
	if err != nil || current.Status != lease.StatusVerifying {
		return lease.Lease{}, lease.ErrTransition
	}
	for _, object := range current.Offer.Offer.EncryptedObjectSet {
		stream, err := reader.OpenCiphertext(ctx, current.Offer.Offer.ProviderNodeID, leaseID, object.ObjectID)
		if err != nil {
			return lease.Lease{}, err
		}
		digest := sha256.New()
		count, readErr := io.Copy(digest, stream)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || strconv.FormatInt(count, 10) != object.CiphertextSize || "sha256:"+hex.EncodeToString(digest.Sum(nil)) != object.CiphertextSHA256 {
			return lease.Lease{}, lease.ErrConflict
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current.Status != lease.StatusVerifying {
		return lease.Lease{}, lease.ErrConflict
	}
	s.current.Status = lease.StatusActive
	return cloneTestLease(s.current), nil
}

func cloneTestLease(in lease.Lease) lease.Lease {
	out := in
	out.Offer.Offer.EncryptedObjectSet = append([]lease.EncryptedObject(nil), in.Offer.Offer.EncryptedObjectSet...)
	out.Progress = append([]lease.Progress(nil), in.Progress...)
	out.Checkpoints = append([]lease.SignedCheckpoint(nil), in.Checkpoints...)
	return out
}

func mustInt64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

type callbackReader struct {
	reader io.Reader
	onRead func()
}

func (r callbackReader) Read(buffer []byte) (int, error) {
	if r.onRead != nil {
		r.onRead()
	}
	return r.reader.Read(buffer)
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

type diskFullPartialFile struct {
	transferPartialFile
	remaining int
}

func (f *diskFullPartialFile) Write(p []byte) (int, error) {
	if f.remaining <= 0 {
		return 0, syscall.ENOSPC
	}
	if len(p) > f.remaining {
		written, err := f.transferPartialFile.Write(p[:f.remaining])
		f.remaining -= written
		if err != nil {
			return written, err
		}
		return written, syscall.ENOSPC
	}
	written, err := f.transferPartialFile.Write(p)
	f.remaining -= written
	return written, err
}

func (a *testAuthorizer) Authorize(_ context.Context, subject, archive, snapshot, action string, actor authority.Principal, chain []authority.SignedGrant, now time.Time) error {
	a.calls++
	a.checkedAt = append(a.checkedAt, now)
	if subject == "" || archive == "" || snapshot == "" || actor.Kind != authority.PrincipalNode || len(chain) == 0 || now.IsZero() {
		return errors.New("incomplete authorization context")
	}
	a.action = action
	if a.denyOn == a.calls {
		return errors.New("authorization became stale")
	}
	return a.err
}

func TestEncryptRestoreSnapshotPreservesOriginalBytesAndExposesCiphertextOnlyRefs(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "replica-state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	source := &mapSource{coordinate: coordinate, payload: payload}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	verifier := &testAuthorizer{}
	auth := testAuthorization(verifier)

	bundle, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, auth)
	if err != nil {
		t.Fatalf("encrypt snapshot: %v", err)
	}
	if verifier.calls != 2 || verifier.action != "archive.replicate" || len(bundle.Objects) != 1 {
		t.Fatalf("authorization/object count calls=%d action=%q objects=%d", verifier.calls, verifier.action, len(bundle.Objects))
	}
	refs, err := json.Marshal(bundle.CiphertextRefs())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(refs, []byte(coordinate.SessionID)) || bytes.Contains(refs, []byte(bundle.Objects[0].PlaintextSHA256)) || bytes.Contains(refs, payload[:64]) {
		t.Fatalf("peer ciphertext references disclosed plaintext metadata or bytes: %s", refs)
	}
	if _, err := json.Marshal(bundle); !errors.Is(err, ErrInvalid) {
		t.Fatalf("private bundle unexpectedly supports public JSON serialization: %v", err)
	}
	if _, err := json.Marshal(bundle.Objects[0]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("private object binding unexpectedly supports public JSON serialization: %v", err)
	}
	ciphertext, err := os.ReadFile(filepath.Join(root, objectDir, bundle.Objects[0].ObjectID+".irac"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, payload[:64]) {
		t.Fatal("ciphertext object contains a plaintext sample")
	}
	publicManifest, err := PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatalf("create explicit public replica mapping: %v", err)
	}
	publicBytes, err := json.Marshal(publicManifest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(publicBytes, []byte(coordinate.SessionID)) || bytes.Contains(publicBytes, []byte(bundle.Objects[0].PlaintextSHA256)) || bytes.Contains(publicBytes, payload[:64]) {
		t.Fatalf("replica manifest omitted coordinate mapping or exposed private/plaintext bytes: %s", publicBytes)
	}
	if hash, err := ReplicaManifestHash(snapshot, publicManifest); err != nil {
		t.Fatalf("hash public manifest: %v", err)
	} else if hash == "" {
		t.Fatal("public manifest hash is empty")
	}
	if _, err := BundleFromPublicReplicaManifest(snapshot, publicManifest); err != nil {
		t.Fatalf("rebuild public manifest: snapshot=%+v manifest=%+v err=%v", snapshot.Content.Objects, publicManifest, err)
	}
	alteredManifest := publicManifest
	alteredManifest.KeyEpoch = "2"
	originalHash, _ := ReplicaManifestHash(snapshot, publicManifest)
	alteredHash, _ := ReplicaManifestHash(snapshot, alteredManifest)
	if originalHash == alteredHash {
		t.Fatal("lease replica-manifest hash did not bind the key epoch")
	}
	state := &replicaLeaseState{current: testTransferringLease(snapshot, publicManifest)}
	importAuthorization := LeaseImportAuthorizer{LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA", ProviderNodeID: "nd_test_provider", Leases: state}
	if err := importAuthorization.authorize(ctx, snapshot, publicManifest, publicManifest.Objects[0]); err != nil {
		t.Fatalf("fixture lease rejected manifest before import: offer=%+v manifest=%+v err=%v", state.current.Offer.Offer, publicManifest, err)
	}
	peerReader, err := store.OpenCiphertextForPeer(ctx, snapshot, bundle, auth, "archive.replicate", bundle.Objects[0].ObjectID)
	if err != nil {
		t.Fatalf("authorized peer ciphertext read: %v", err)
	}
	peerBytes, err := io.ReadAll(peerReader)
	closeErr := peerReader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(peerBytes, ciphertext) {
		t.Fatalf("peer ciphertext read differs from stored object: bytes=%d err=%v close=%v", len(peerBytes), err, closeErr)
	}
	rangeReader, err := store.OpenCiphertextRangeForPeer(ctx, snapshot, bundle, auth, "archive.replicate", bundle.Objects[0].ObjectID, 17, 31)
	if err != nil {
		t.Fatalf("authorized bounded ciphertext range: %v", err)
	}
	rangeBytes, rangeErr := io.ReadAll(rangeReader)
	rangeCloseErr := rangeReader.Close()
	if rangeErr != nil || rangeCloseErr != nil || !bytes.Equal(rangeBytes, ciphertext[17:48]) {
		t.Fatalf("ciphertext range mismatch: data=%d err=%v close=%v", len(rangeBytes), rangeErr, rangeCloseErr)
	}
	if _, err := store.OpenCiphertextRangeForPeer(ctx, snapshot, bundle, auth, "archive.replicate", bundle.Objects[0].ObjectID, int64(len(ciphertext)-1), 2); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("out-of-bounds ciphertext range was accepted: %v", err)
	}
	// The committed bundle is stable across retries, and the DEK is checked
	// against existing ciphertext before that bundle is reused.
	retry, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, auth)
	if err != nil || retry.Objects[0].ObjectID != bundle.Objects[0].ObjectID || source.opened != 1 {
		t.Fatalf("idempotent retry bundle=%+v source opens=%d err=%v", retry, source.opened, err)
	}
	wrongKey, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.EncryptSnapshot(ctx, snapshot, source, wrongKey, 1, auth)
	wrongKey.Destroy()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("existing encrypted bundle accepted a different key: %v", err)
	}

	destination := filepath.Join(t.TempDir(), "restored")
	manifest, err := store.RestoreSnapshot(ctx, snapshot, bundle, dek, destination, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if len(manifest.Objects) != 1 || manifest.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("restore manifest=%+v", manifest)
	}
	if manifest.PlaybackPlaylist != "index.m3u8" || len(manifest.TrackPlaylists) != 1 {
		t.Fatalf("v2 restore did not publish playable playlist references: %+v", manifest)
	}
	restored, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(manifest.Objects[0].Path)))
	if err != nil || !bytes.Equal(restored, payload) {
		t.Fatalf("restored canonical bytes differ: size=%d err=%v", len(restored), err)
	}
	if _, err := store.RestoreSnapshot(ctx, snapshot, bundle, dek, destination, testAuthorization(&testAuthorizer{})); !errors.Is(err, ErrRestoreConflict) {
		t.Fatalf("existing restore destination was replaced: %v", err)
	}
}

func TestRestoreSnapshotRebuildsInitMapAndVODOrderWithoutChangingBytes(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "replica-state"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := archiveindex.NewSessionIdentity("recording-replica-init", "hls", nil, "broadcast-init")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("recording-replica-init", session)
	if err != nil {
		t.Fatal(err)
	}
	initPayload := []byte("original-init-object\x00")
	firstPayload := []byte("original-media-one\x00")
	secondPayload := []byte("original-media-two\x00")
	initCoordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectInit}
	repeatedInitCoordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 2, Kind: archiveindex.ObjectInit}
	firstCoordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia}
	secondCoordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 2, Kind: archiveindex.ObjectMedia}
	apply := func(coordinate archiveindex.Coordinate, claimID string, payload []byte, duration float64, initIdentity string) {
		t.Helper()
		digest := sha256.Sum256(payload)
		if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: claimID, Source: archiveindex.ClaimLiveOrigin,
			AcquiredAt: time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC), PayloadPath: "recordings/private/" + claimID,
			Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]), Verification: archiveindex.VerificationVerified,
			Duration: duration, InitIdentity: initIdentity,
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(initCoordinate, "claim-init", initPayload, 0, "init-source-identity")
	apply(repeatedInitCoordinate, "claim-init-repeat", initPayload, 0, "init-source-identity")
	apply(firstCoordinate, "claim-media-1", firstPayload, 2.0, "init-source-identity")
	apply(secondCoordinate, "claim-media-2", secondPayload, 2.5, "init-source-identity")
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	source := coordinateMapSource{initCoordinate: initPayload, repeatedInitCoordinate: initPayload, firstCoordinate: firstPayload, secondCoordinate: secondPayload}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatalf("encrypt complete archive: %v", err)
	}
	destination := filepath.Join(t.TempDir(), "restored-archive")
	manifest, err := store.RestoreSnapshot(ctx, snapshot, bundle, dek, destination, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatalf("restore complete archive: %v", err)
	}
	if manifest.PlaybackPlaylist != "index.m3u8" || len(manifest.TrackPlaylists) != 1 {
		t.Fatalf("playback manifest=%+v", manifest)
	}
	objects := make(map[archiveindex.Coordinate]RestoredObject, len(manifest.Objects))
	for _, object := range manifest.Objects {
		objects[object.Coordinate] = object
		got, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(object.Path)))
		if err != nil {
			t.Fatal(err)
		}
		want := source[object.Coordinate]
		if !bytes.Equal(got, want) {
			t.Fatalf("restored object %v bytes changed", object.Coordinate)
		}
	}
	trackPlaylist, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(manifest.TrackPlaylists[0].Path)))
	if err != nil {
		t.Fatal(err)
	}
	initPath := objects[initCoordinate].Path
	firstPath := objects[firstCoordinate].Path
	secondPath := objects[secondCoordinate].Path
	if !bytes.Contains(trackPlaylist, []byte(`#EXT-X-MAP:URI="../`+initPath+`"`)) {
		t.Fatalf("init map missing from restored VOD playlist: %s", trackPlaylist)
	}
	if count := bytes.Count(trackPlaylist, []byte("#EXT-X-MAP:")); count != 1 {
		t.Fatalf("identical repeated init map was not resolved deterministically: map directives=%d playlist=%s", count, trackPlaylist)
	}
	firstIndex, secondIndex := bytes.Index(trackPlaylist, []byte("../"+firstPath)), bytes.Index(trackPlaylist, []byte("../"+secondPath))
	if firstIndex < 0 || secondIndex <= firstIndex || !bytes.Contains(trackPlaylist, []byte("#EXT-X-ENDLIST")) {
		t.Fatalf("restored VOD ordering/end marker invalid: %s", trackPlaylist)
	}
	master, err := os.ReadFile(filepath.Join(destination, manifest.PlaybackPlaylist))
	if err != nil || !bytes.Contains(master, []byte(manifest.TrackPlaylists[0].Path)) {
		t.Fatalf("restored master playlist invalid: body=%s err=%v", master, err)
	}
}

func TestRestoreSnapshotRechecksCapabilityBeforePublishingPlaintext(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "replica-state"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := store.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "must-not-publish")
	verifier := &testAuthorizer{denyOn: 2}
	if _, err := store.RestoreSnapshot(ctx, snapshot, bundle, dek, destination, testAuthorization(verifier)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked restore authorization was accepted: %v", err)
	}
	if verifier.calls != 2 {
		t.Fatalf("restore authorization checks=%d, want a pre-publish recheck", verifier.calls)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("plaintext restore became visible after authorization revocation: %v", err)
	}
}

func TestRestoredPlaybackRejectsKnownMissingCoverage(t *testing.T) {
	session, err := archiveindex.NewSessionIdentity("recording-replica-gap", "hls", nil, "broadcast-gap")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("recording-replica-gap", session)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia}
	payload := []byte("canonical bytes")
	digest := sha256.Sum256(payload)
	if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: coordinate, ClaimID: "present", Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC), PayloadPath: "private/one.ts",
		Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]), Verification: archiveindex.VerificationVerified, Duration: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if err := archiveindex.ApplyCoverage(&inventory, archiveindex.Coverage{
		SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1,
		FromSequence: 2, ToSequence: 2, State: archiveindex.CoverageKnownMissing,
		ObservedAt: time.Date(2026, 11, 3, 1, 1, 0, 0, time.UTC), Kind: archiveindex.ObjectMedia,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	object := RestoredObject{Coordinate: coordinate, Path: "objects/ob_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)) + ".payload", Size: int64(len(payload)), SHA256: "sha256:" + hex.EncodeToString(digest[:])}
	if _, _, _, err := buildRestoredPlaylists(snapshot, []RestoredObject{object}); !errors.Is(err, ErrPlaybackIncomplete) {
		t.Fatalf("snapshot with known missing coverage produced complete playback: %v", err)
	}
}

func TestCiphertextImportRechecksLeaseAndTombstoneBeforePublication(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	sourceStore, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := sourceStore.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	objectReader, err := sourceStore.openCiphertext(ctx, manifest.Objects[0].ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := io.ReadAll(objectReader)
	closeErr := objectReader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read fixture ciphertext: %v / %v", err, closeErr)
	}

	providerRoot := t.TempDir()
	provider, err := nodeidentity.Open(providerRoot)
	if err != nil {
		t.Fatal(err)
	}
	providerInfo, err := provider.Create()
	if err != nil {
		t.Fatal(err)
	}
	state := &replicaLeaseState{current: testTransferringLease(snapshot, manifest), checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey)}
	state.current.Offer.Offer.ProviderNodeID = providerInfo.NodeID
	state.current.OfferHash = "sha256:" + strings.Repeat("d", 64)
	for _, encrypted := range state.current.Offer.Offer.EncryptedObjectSet {
		state.current.Progress = append(state.current.Progress, lease.Progress{ObjectID: encrypted.ObjectID, CommittedOffset: "0"})
	}
	authorization := LeaseImportAuthorizer{LeaseID: state.current.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: state}
	state.checkpointPath = store.transferPartialPath(authorization.LeaseID, manifest.Objects[0].ObjectID)
	tamperedManifest := manifest
	tamperedManifest.KeyEpoch = "2"
	if _, err := store.ImportCiphertextChunk(ctx, authorization, snapshot, tamperedManifest, tamperedManifest.Objects[0], "0", bytes.NewReader(ciphertext), provider); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("lease did not bind the exact replica manifest: %v", err)
	}
	_, err = store.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, manifest.Objects[0], "0", callbackReader{
		reader: bytes.NewReader(ciphertext), onRead: func() { state.current.Status = lease.StatusTerminating },
	}, provider)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("lease revoked during upload was not denied: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(store.root, objectDir, manifest.Objects[0].ObjectID+".irac")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ciphertext published after lease termination: %v", err)
	}

	state.mu.Lock()
	state.current = testTransferringLease(snapshot, manifest)
	state.current.Offer.Offer.ProviderNodeID = providerInfo.NodeID
	state.current.OfferHash = "sha256:" + strings.Repeat("d", 64)
	for _, encrypted := range state.current.Offer.Offer.EncryptedObjectSet {
		state.current.Progress = append(state.current.Progress, lease.Progress{ObjectID: encrypted.ObjectID, CommittedOffset: "0"})
	}
	state.tombstones = []lease.TombstoneRecord{{Signed: lease.SignedTombstone{Tombstone: lease.Tombstone{ArchiveID: manifest.ArchiveID}}}}
	state.mu.Unlock()
	if _, err := store.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, manifest.Objects[0], "0", bytes.NewReader(ciphertext), provider); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("tombstoned archive accepted ciphertext: %v", err)
	}
}

func TestCiphertextImportResumesFromDurableProviderCheckpoint(t *testing.T) {
	ctx := context.Background()
	sourceStore, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := sourceStore.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	object := manifest.Objects[0]
	reader, err := sourceStore.openCiphertext(ctx, object.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(ciphertext) < 2 {
		t.Fatalf("read source ciphertext: size=%d err=%v close=%v", len(ciphertext), err, closeErr)
	}

	providerRoot := t.TempDir()
	provider, err := nodeidentity.Open(providerRoot)
	if err != nil {
		t.Fatal(err)
	}
	providerInfo, err := provider.Create()
	if err != nil {
		t.Fatal(err)
	}
	state := &replicaLeaseState{current: testTransferringLease(snapshot, manifest), checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey)}
	state.current.Offer.Offer.ProviderNodeID = providerInfo.NodeID
	state.current.OfferHash = "sha256:" + strings.Repeat("a", 64)
	for _, encrypted := range state.current.Offer.Offer.EncryptedObjectSet {
		state.current.Progress = append(state.current.Progress, lease.Progress{ObjectID: encrypted.ObjectID, CommittedOffset: "0"})
	}
	importer, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	authorization := LeaseImportAuthorizer{LeaseID: state.current.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: state}
	state.checkpointPath = importer.transferPartialPath(authorization.LeaseID, object.ObjectID)

	initialOffset, err := importer.CiphertextTransferOffset(ctx, authorization, snapshot, manifest, object)
	if err != nil || initialOffset != "0" {
		t.Fatalf("initial durable offset=%q err=%v", initialOffset, err)
	}
	split := len(ciphertext) / 3
	streamErr := errors.New("simulated peer stream interruption")
	if _, err := importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, object, "0", io.MultiReader(
		bytes.NewReader(ciphertext[:split/2]), failingReader{err: streamErr},
	), provider); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("interrupted chunk error=%v, want unavailable", err)
	}
	if offset, err := importer.CiphertextTransferOffset(ctx, authorization, snapshot, manifest, object); err != nil || offset != "0" {
		t.Fatalf("interrupted transfer advanced durable offset=%q err=%v", offset, err)
	}
	if info, err := os.Stat(state.checkpointPath); err != nil || info.Size() != 0 {
		t.Fatalf("interrupted transfer retained uncommitted bytes: size=%v err=%v", info, err)
	}
	if state.current.Status != lease.StatusTransferring || len(state.current.Checkpoints) != 0 {
		t.Fatalf("interrupted transfer changed lease state: status=%s checkpoints=%d", state.current.Status, len(state.current.Checkpoints))
	}
	// Retry the same durable offset after a stream interruption.
	firstReceipt, err := importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, object, "0", bytes.NewReader(ciphertext[:split]), provider)
	if err != nil || firstReceipt.Checkpoint.CommittedOffset != strconv.Itoa(split) {
		t.Fatalf("first chunk receipt=%+v err=%v", firstReceipt, err)
	}
	// Inject ENOSPC after an already checkpointed prefix. Only the new tail may
	// be discarded; the committed prefix and provider checkpoint must survive.
	openPartial := importer.openPartial
	importer.openPartial = func(path string, requested int64) (transferPartialFile, bool, error) {
		file, created, err := openTransferPartial(path, requested)
		if err != nil {
			return nil, false, err
		}
		return &diskFullPartialFile{transferPartialFile: file, remaining: len(ciphertext) / 4}, created, nil
	}
	if _, err := importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, object, strconv.Itoa(split), bytes.NewReader(ciphertext[split:]), provider); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("disk-full tail error=%v, want unavailable", err)
	}
	importer.openPartial = openPartial
	diskOffset, err := importer.CiphertextTransferOffset(ctx, authorization, snapshot, manifest, object)
	if err != nil || diskOffset != strconv.Itoa(split) {
		t.Fatalf("disk-full write changed durable offset=%q want=%d err=%v", diskOffset, split, err)
	}
	if info, err := os.Stat(state.checkpointPath); err != nil || info.Size() != int64(split) || len(state.current.Checkpoints) != 1 {
		t.Fatalf("disk-full write changed committed prefix: size=%v checkpoints=%d err=%v", info, len(state.current.Checkpoints), err)
	}
	partialInfo, err := os.Lstat(state.checkpointPath)
	if err != nil || partialInfo.Mode().Perm() != 0600 || !partialInfo.Mode().IsRegular() || partialInfo.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("partial ciphertext file metadata=%v err=%v", partialInfo, err)
	}
	offset, err := importer.CiphertextTransferOffset(ctx, authorization, snapshot, manifest, object)
	if err != nil || offset != strconv.Itoa(split) {
		t.Fatalf("resumed offset=%q err=%v", offset, err)
	}
	if _, err := importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, object, "0", bytes.NewReader(ciphertext[split:]), provider); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("stale sender offset accepted: %v", err)
	}

	// Simulate a crash after bytes reached disk but before the next lease
	// checkpoint was committed. Retry must truncate this uncheckpointed tail.
	partial, err := os.OpenFile(state.checkpointPath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partial.Write([]byte("uncommitted-tail")); err != nil {
		t.Fatal(err)
	}
	if err := partial.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := partial.Close(); err != nil {
		t.Fatal(err)
	}
	secondReceipt, err := importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, object, offset, bytes.NewReader(ciphertext[split:]), provider)
	if err != nil || secondReceipt.Checkpoint.CommittedOffset != strconv.Itoa(len(ciphertext)) {
		t.Fatalf("final chunk receipt=%+v err=%v", secondReceipt, err)
	}
	if state.current.Status != lease.StatusVerifying {
		t.Fatalf("complete transfer status=%s, want verifying", state.current.Status)
	}
	stored, err := os.ReadFile(filepath.Join(importer.root, objectDir, object.ObjectID+".irac"))
	if err != nil || !bytes.Equal(stored, ciphertext) {
		t.Fatalf("published ciphertext differs: size=%d expected=%d err=%v", len(stored), len(ciphertext), err)
	}
	if _, err := os.Lstat(state.checkpointPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed staging file retained: %v", err)
	}
	importedBundle, err := importer.FinalizeImportedReplica(ctx, authorization, snapshot, manifest)
	if err != nil || state.current.Status != lease.StatusActive {
		t.Fatalf("finalize/readback imported replica: status=%s err=%v", state.current.Status, err)
	}
	// A later lease for the same digest-bound ciphertext can reuse the
	// already verified object without reading or rewriting a second copy.
	dedupLease := testTransferringLease(snapshot, manifest)
	dedupLease.Offer.Offer.LeaseID = "rl_BBBBBBBBBBBBBBBBBBBBBB"
	dedupLease.Offer.Offer.ProviderNodeID = providerInfo.NodeID
	dedupLease.OfferHash = "sha256:" + strings.Repeat("b", 64)
	for _, encrypted := range dedupLease.Offer.Offer.EncryptedObjectSet {
		dedupLease.Progress = append(dedupLease.Progress, lease.Progress{ObjectID: encrypted.ObjectID, CommittedOffset: "0"})
	}
	finalPath := filepath.Join(importer.root, objectDir, object.ObjectID+".irac")
	dedupState := &replicaLeaseState{
		current: dedupLease, checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey), checkpointPath: finalPath,
	}
	dedupAuthorization := LeaseImportAuthorizer{
		LeaseID: dedupLease.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: dedupState,
	}
	if err := dedupAuthorization.authorize(ctx, snapshot, manifest, object); err != nil {
		t.Fatalf("fixture digest-bound lease rejected before import: current=%+v err=%v", dedupState.current, err)
	}
	unreadSource := bytes.NewReader([]byte("existing verified ciphertext must be reused"))
	deduplicated, err := importer.ImportCiphertextChunk(ctx, dedupAuthorization, snapshot, manifest, object, "0", unreadSource, provider)
	if err != nil || deduplicated.Checkpoint.CommittedOffset != object.CiphertextSize || dedupState.current.Status != lease.StatusVerifying {
		t.Fatalf("digest-bound object reuse receipt=%+v state=%s err=%v", deduplicated, dedupState.current.Status, err)
	}
	if unreadSource.Len() == 0 {
		t.Fatal("deduplicated transfer consumed source bytes despite the existing verified object")
	}
	afterDedup, err := os.ReadFile(finalPath)
	if err != nil || !bytes.Equal(afterDedup, ciphertext) {
		t.Fatalf("dedup changed stored ciphertext: size=%d want=%d err=%v", len(afterDedup), len(ciphertext), err)
	}
	reopened, err := Open(importer.root)
	if err != nil {
		t.Fatal(err)
	}
	recoveredBundle, err := reopened.LoadBundle(snapshot, 1)
	if err != nil || len(recoveredBundle.Objects) != len(importedBundle.Objects) || recoveredBundle.Objects[0] != importedBundle.Objects[0] {
		t.Fatalf("reloaded private replica binding=%+v expected=%+v err=%v", recoveredBundle, importedBundle, err)
	}
	restoredRoot := filepath.Join(t.TempDir(), "recovered")
	if _, err := reopened.RestoreSnapshot(ctx, snapshot, recoveredBundle, dek, restoredRoot, testAuthorization(&testAuthorizer{})); err != nil {
		t.Fatalf("restore from imported replica after store reopen: %v", err)
	}
	restored, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash("objects/"+object.ObjectID+".payload")))
	if err != nil || !bytes.Equal(restored, payload) {
		t.Fatalf("recovered plaintext differs from original canonical bytes: size=%d err=%v", len(restored), err)
	}
	// A new authorized lease can repair a digest-mismatched private regular
	// object, but only after the replacement has been fully verified.
	corrupted := bytes.Repeat([]byte("x"), len(ciphertext))
	if err := os.WriteFile(finalPath, corrupted, 0600); err != nil {
		t.Fatal(err)
	}
	corruptLease := dedupLease
	corruptLease.Offer.Offer.LeaseID = "rl_CCCCCCCCCCCCCCCCCCCCCC"
	corruptLease.OfferHash = "sha256:" + strings.Repeat("c", 64)
	corruptLease.Progress = []lease.Progress{{ObjectID: object.ObjectID, CommittedOffset: "0"}}
	corruptState := &replicaLeaseState{
		current: corruptLease, checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey), checkpointPath: importer.transferPartialPath(corruptLease.Offer.Offer.LeaseID, object.ObjectID),
	}
	corruptAuthorization := LeaseImportAuthorizer{
		LeaseID: corruptLease.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: corruptState,
	}
	if _, err := importer.ImportCiphertextChunk(ctx, corruptAuthorization, snapshot, manifest, object, "0", bytes.NewReader(corrupted), provider); !errors.Is(err, ErrInvalid) {
		t.Fatalf("digest-invalid replacement accepted: %v", err)
	}
	stillCorrupt, err := os.ReadFile(finalPath)
	if err != nil || !bytes.Equal(stillCorrupt, corrupted) || corruptState.current.Progress[0].CommittedOffset != "0" || len(corruptState.current.Checkpoints) != 0 {
		t.Fatalf("digest-invalid replacement changed canonical object or checkpoint: bytesMatch=%v progress=%+v err=%v", bytes.Equal(stillCorrupt, corrupted), corruptState.current.Progress, err)
	}
	repairSplit := len(ciphertext) / 2
	if _, err := importer.ImportCiphertextChunk(ctx, corruptAuthorization, snapshot, manifest, object, "0", bytes.NewReader(ciphertext[:repairSplit]), provider); err != nil {
		t.Fatalf("authorized repair prefix failed: %v", err)
	}
	stillCorrupt, err = os.ReadFile(finalPath)
	if err != nil || !bytes.Equal(stillCorrupt, corrupted) || corruptState.current.Progress[0].CommittedOffset != strconv.Itoa(repairSplit) {
		t.Fatalf("partial repair replaced object before full digest verification: bytesMatch=%v progress=%+v err=%v", bytes.Equal(stillCorrupt, corrupted), corruptState.current.Progress, err)
	}
	repairRanges := &ciphertextRangeMap{objectID: object.ObjectID, data: ciphertext}
	if _, err := importer.ImportCiphertextObjectFromPeer(ctx, corruptAuthorization, snapshot, manifest, object, repairRanges, provider); err != nil {
		t.Fatalf("peer-range repair of corrupt regular object failed: %v", err)
	}
	if len(repairRanges.requests) != 1 || repairRanges.requests[0] != [2]int64{int64(repairSplit), int64(len(ciphertext) - repairSplit)} {
		t.Fatalf("repair resumed from wrong durable offset: ranges=%v", repairRanges.requests)
	}
	repaired, err := os.ReadFile(finalPath)
	if err != nil || !bytes.Equal(repaired, ciphertext) || corruptState.current.Progress[0].CommittedOffset != strconv.Itoa(len(ciphertext)) || len(corruptState.current.Checkpoints) != 1 {
		t.Fatalf("corrupt digest object was not repaired and checkpointed: bytesMatch=%v progress=%+v checkpoints=%d err=%v", bytes.Equal(repaired, ciphertext), corruptState.current.Progress, len(corruptState.current.Checkpoints), err)
	}

	// A symlink at a digest path is never a repair target, even when the
	// authorized peer can provide bytes that match the object digest.
	sentinelPath := filepath.Join(t.TempDir(), "outside-object")
	if err := os.WriteFile(sentinelPath, corrupted, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(finalPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinelPath, finalPath); err != nil {
		t.Fatal(err)
	}
	symlinkLease := dedupLease
	symlinkLease.Offer.Offer.LeaseID = "rl_DDDDDDDDDDDDDDDDDDDDDD"
	symlinkLease.OfferHash = "sha256:" + strings.Repeat("d", 64)
	symlinkLease.Progress = []lease.Progress{{ObjectID: object.ObjectID, CommittedOffset: "0"}}
	symlinkState := &replicaLeaseState{
		current: symlinkLease, checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey),
		checkpointPath: importer.transferPartialPath(symlinkLease.Offer.Offer.LeaseID, object.ObjectID),
	}
	symlinkAuthorization := LeaseImportAuthorizer{
		LeaseID: symlinkLease.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: symlinkState,
	}
	if _, err := importer.ImportCiphertextChunk(ctx, symlinkAuthorization, snapshot, manifest, object, "0", bytes.NewReader(ciphertext), provider); !errors.Is(err, ErrInvalid) {
		t.Fatalf("digest-path symlink accepted for repair: %v", err)
	}
	stillCorrupt, err = os.ReadFile(sentinelPath)
	linkInfo, linkErr := os.Lstat(finalPath)
	if err != nil || !bytes.Equal(stillCorrupt, corrupted) || linkErr != nil || linkInfo.Mode()&os.ModeSymlink == 0 || symlinkState.current.Progress[0].CommittedOffset != "0" || len(symlinkState.current.Checkpoints) != 0 {
		t.Fatalf("unsafe digest path changed or checkpointed: sentinel=%v link=%v progress=%+v err=%v", bytes.Equal(stillCorrupt, corrupted), linkInfo, symlinkState.current.Progress, err)
	}
	if err := os.Remove(finalPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(finalPath, corrupted, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(finalPath, 0640); err != nil {
		t.Fatal(err)
	}
	unsafeLease := symlinkLease
	unsafeLease.Offer.Offer.LeaseID = "rl_EEEEEEEEEEEEEEEEEEEEEE"
	unsafeLease.OfferHash = "sha256:" + strings.Repeat("e", 64)
	unsafeLease.Progress = []lease.Progress{{ObjectID: object.ObjectID, CommittedOffset: "0"}}
	unsafeState := &replicaLeaseState{
		current: unsafeLease, checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey),
		checkpointPath: importer.transferPartialPath(unsafeLease.Offer.Offer.LeaseID, object.ObjectID),
	}
	unsafeAuthorization := LeaseImportAuthorizer{
		LeaseID: unsafeLease.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: unsafeState,
	}
	if _, err := importer.ImportCiphertextChunk(ctx, unsafeAuthorization, snapshot, manifest, object, "0", bytes.NewReader(ciphertext), provider); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unsafe-permission digest path accepted for repair: %v", err)
	}
	unsafeBytes, err := os.ReadFile(finalPath)
	unsafeInfo, statErr := os.Lstat(finalPath)
	if err != nil || !bytes.Equal(unsafeBytes, corrupted) || statErr != nil || unsafeInfo.Mode().Perm() != 0640 || unsafeState.current.Progress[0].CommittedOffset != "0" || len(unsafeState.current.Checkpoints) != 0 {
		t.Fatalf("unsafe-permission object changed or checkpointed: bytesMatch=%v mode=%v progress=%+v err=%v", bytes.Equal(unsafeBytes, corrupted), unsafeInfo.Mode().Perm(), unsafeState.current.Progress, err)
	}
}

func TestCiphertextTransferIntegratesNodeKeyAndDurableLeaseStore(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	source, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	sourceAuthorization := testAuthorization(&testAuthorizer{})
	sourceBundle, err := source.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 3, sourceAuthorization)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := PublicReplicaManifest(snapshot, sourceBundle)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil {
		t.Fatal(err)
	}

	ownerPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6d}, ed25519.SeedSize))
	ownerPublic := ownerPrivate.Public().(ed25519.PublicKey)
	ownerID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	owner := authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID}
	subjectID := "os_" + strings.Repeat("B", 22)
	providerRoot := t.TempDir()
	provider, err := nodeidentity.Open(providerRoot)
	if err != nil {
		t.Fatal(err)
	}
	providerInfo, err := provider.Create()
	if err != nil {
		t.Fatal(err)
	}
	providerPublic, err := base64.RawURLEncoding.DecodeString(providerInfo.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	providerPrincipal := authority.Principal{Kind: authority.PrincipalNode, ID: providerInfo.NodeID}
	nonce := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subjectID, Issuer: owner, Audience: providerPrincipal,
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Actions: []string{"archive.replicate"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: nonce, PolicyEpoch: "1", IssuerKeyID: ownerKeyID,
	}
	signedGrant, err := authority.SignGrant(grant, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	var reserved uint64
	objects := make([]lease.EncryptedObject, 0, len(manifest.Objects))
	for _, object := range manifest.Objects {
		size, err := strconv.ParseUint(object.CiphertextSize, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		reserved += size
		objects = append(objects, lease.EncryptedObject{ObjectID: object.ObjectID, CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256})
	}
	leaseID := "rl_" + strings.Repeat("D", 22)
	offer := lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: leaseID, ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		KeyEpoch:              manifest.KeyEpoch,
		PlaintextManifestHash: snapshot.ContentHash, ReplicaManifestHash: manifestHash,
		EncryptedObjectSet: objects, ProviderNodeID: providerInfo.NodeID, OwnerSubjectID: subjectID,
		ReservedBytes: strconv.FormatUint(reserved, 10), IssuedAt: now.Add(-time.Second).Format(time.RFC3339),
		ExpiresAt: now.Add(2 * time.Hour).Format(time.RFC3339), RenewalPolicy: "manual",
	}
	signedOffer, err := lease.SignOffer(offer, owner, []authority.SignedGrant{signedGrant}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	keyResolver := replicaLeaseTestKeys{
		owner.Kind + ":" + owner.ID:                         ownerPublic,
		authority.PrincipalNode + ":" + providerInfo.NodeID: ed25519.PublicKey(providerPublic),
	}
	stateRoot := filepath.Join(t.TempDir(), "lease-state")
	leaseOptions := lease.Options{
		Root: stateRoot, Authorizer: replicaLeaseTestAuthorizer{subjectID: subjectID, archiveID: snapshot.Content.ArchiveID, snapshotID: snapshot.SnapshotID},
		Keys: keyResolver, Now: func() time.Time { return now },
	}
	state, err := lease.Open(leaseOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.IssueOffer(ctx, signedOffer); err != nil {
		t.Fatalf("persist source-Node-signed offer: %v", err)
	}
	offerHash, err := lease.OfferHash(signedOffer.Offer)
	if err != nil {
		t.Fatal(err)
	}
	acceptance, err := provider.SignReplicaAcceptance(lease.Acceptance{
		Version: lease.ProtocolVersion, LeaseID: leaseID, OfferHash: offerHash,
		ReservedBytes: strconv.FormatUint(reserved, 10), AcceptedAt: now.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Accept(ctx, acceptance); err != nil {
		t.Fatalf("persist Node Key-signed acceptance: %v", err)
	}
	if _, err := state.BeginTransfer(ctx, leaseID); err != nil {
		t.Fatal(err)
	}

	receiver, err := Open(filepath.Join(t.TempDir(), "receiver"))
	if err != nil {
		t.Fatal(err)
	}
	receiveAuthorization := LeaseImportAuthorizer{LeaseID: leaseID, ProviderNodeID: providerInfo.NodeID, Leases: state}
	forwardErr := errors.New("simulated lost source checkpoint acknowledgement")
	forwardFailure := true
	forwarded := make([]lease.SignedCheckpoint, 0)
	receiveAuthorization.ForwardCheckpoint = func(_ context.Context, checkpoint lease.SignedCheckpoint) error {
		if forwardFailure {
			forwardFailure = false
			return forwardErr
		}
		forwarded = append(forwarded, checkpoint)
		return nil
	}
	object := manifest.Objects[0]
	offset, err := receiver.CiphertextTransferOffset(ctx, receiveAuthorization, snapshot, manifest, object)
	if err != nil || offset != "0" {
		t.Fatalf("initial provider offset=%q err=%v", offset, err)
	}
	sourceReader, err := source.OpenCiphertextForPeer(ctx, snapshot, sourceBundle, sourceAuthorization, "archive.replicate", object.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := io.ReadAll(sourceReader)
	closeErr := sourceReader.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read source ciphertext: %v close=%v", err, closeErr)
	}
	split := len(ciphertext) / 2
	streamErr := errors.New("simulated peer stream interruption")
	if _, err := receiver.ImportCiphertextChunk(ctx, receiveAuthorization, snapshot, manifest, object, offset, io.MultiReader(
		bytes.NewReader(ciphertext[:split/2]), failingReader{err: streamErr},
	), provider); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("interrupted provider write error=%v, want unavailable", err)
	}
	interruptedLease, err := state.Get(ctx, leaseID)
	if err != nil || interruptedLease.Status != lease.StatusTransferring || interruptedLease.Progress[0].CommittedOffset != "0" || len(interruptedLease.Checkpoints) != 0 {
		t.Fatalf("interrupted transfer advanced durable lease=%+v err=%v", interruptedLease, err)
	}
	partialPath := receiver.transferPartialPath(leaseID, object.ObjectID)
	if info, err := os.Stat(partialPath); err != nil || info.Size() != 0 {
		t.Fatalf("interrupted transfer retained uncommitted bytes: size=%v err=%v", info, err)
	}
	// Reopen all provider-side durable components before retrying: Node Key,
	// Lease Store, and encrypted replica Store.
	provider, err = nodeidentity.Open(providerRoot)
	if err != nil {
		t.Fatal(err)
	}
	restartedProvider, err := provider.Info()
	if err != nil || restartedProvider.NodeID != providerInfo.NodeID || restartedProvider.PublicKey != providerInfo.PublicKey {
		t.Fatalf("provider Node Key changed on restart: before=%+v after=%+v err=%v", providerInfo, restartedProvider, err)
	}
	state, err = lease.Open(leaseOptions)
	if err != nil {
		t.Fatalf("reopen lease store after interrupted transfer: %v", err)
	}
	receiveAuthorization.Leases = state
	receiver, err = Open(receiver.root)
	if err != nil {
		t.Fatalf("reopen replica store after interrupted transfer: %v", err)
	}
	if offset, err = receiver.CiphertextTransferOffset(ctx, receiveAuthorization, snapshot, manifest, object); err != nil || offset != "0" {
		t.Fatalf("restart recovered the wrong checkpoint offset=%q err=%v", offset, err)
	}
	firstChunk := bytes.NewReader(ciphertext[:split])
	firstReceipt, err := receiver.ImportCiphertextChunk(ctx, receiveAuthorization, snapshot, manifest, object, offset, firstChunk, provider)
	if !errors.Is(err, forwardErr) || firstReceipt.Checkpoint.CommittedOffset != strconv.Itoa(split) {
		t.Fatalf("lost source acknowledgement should retain the provider's signed durable checkpoint=%+v err=%v", firstReceipt, err)
	}
	current, err := state.Get(ctx, leaseID)
	if err != nil || current.Progress[0].CommittedOffset != strconv.Itoa(split) {
		t.Fatalf("checkpoint was not durably stored: progress=%+v err=%v", current.Progress, err)
	}
	malformedRanges := &ciphertextRangeMap{objectID: object.ObjectID, data: ciphertext, extra: true}
	if _, err := receiver.ImportCiphertextObjectFromPeer(ctx, receiveAuthorization, snapshot, manifest, object, malformedRanges, provider); !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("oversized peer range should fail before checkpointing: %v", err)
	}
	stillResumable, err := receiver.CiphertextTransferOffset(ctx, receiveAuthorization, snapshot, manifest, object)
	if err != nil || stillResumable != strconv.Itoa(split) {
		t.Fatalf("malformed range changed durable offset=%q err=%v", stillResumable, err)
	}
	remoteRanges := &ciphertextRangeMap{objectID: object.ObjectID, data: ciphertext}
	importedBundle, err := receiver.ImportReplicaFromPeer(ctx, receiveAuthorization, snapshot, manifest, remoteRanges, provider)
	if err != nil || len(importedBundle.Objects) != len(manifest.Objects) {
		t.Fatalf("resumed peer replica import bundle=%+v err=%v", importedBundle, err)
	}
	if len(remoteRanges.requests) != 1 || remoteRanges.requests[0] != [2]int64{int64(split), int64(len(ciphertext) - split)} {
		t.Fatalf("peer range requests=%v; resume should request only the uncommitted tail", remoteRanges.requests)
	}
	if len(forwarded) < 2 || forwarded[len(forwarded)-1].Checkpoint.CommittedOffset != strconv.Itoa(len(ciphertext)) {
		t.Fatalf("durable checkpoints were not forwarded and replayed after retry: %+v", forwarded)
	}
	completedRetry, err := receiver.ImportReplicaFromPeer(ctx, receiveAuthorization, snapshot, manifest, remoteRanges, provider)
	if err != nil || len(completedRetry.Objects) != len(importedBundle.Objects) || len(remoteRanges.requests) != 1 {
		t.Fatalf("completed replica retry should verify durable bytes without fetching again: ranges=%v err=%v", remoteRanges.requests, err)
	}
	current, err = state.Get(ctx, leaseID)
	if err != nil || current.Status != lease.StatusActive {
		t.Fatalf("durable lease status=%s err=%v", current.Status, err)
	}
	reopenedState, err := lease.Open(leaseOptions)
	if err != nil {
		t.Fatalf("reopen durable lease: %v", err)
	}
	reopenedLease, err := reopenedState.Get(ctx, leaseID)
	if err != nil || reopenedLease.Status != lease.StatusActive || reopenedLease.Progress[0].CommittedOffset != strconv.Itoa(len(ciphertext)) {
		t.Fatalf("reopened lease=%+v err=%v", reopenedLease, err)
	}
	reopenedReceiver, err := Open(receiver.root)
	if err != nil {
		t.Fatal(err)
	}
	recoveredBundle, err := reopenedReceiver.LoadBundle(snapshot, 3)
	if err != nil {
		t.Fatalf("load persisted receiver binding: %v", err)
	}
	restoredRoot := filepath.Join(t.TempDir(), "restored")
	if _, err := reopenedReceiver.RestoreSnapshot(ctx, snapshot, recoveredBundle, dek, restoredRoot, testAuthorization(&testAuthorizer{})); err != nil {
		t.Fatalf("restore original bytes from encrypted replica: %v", err)
	}
	restored, err := os.ReadFile(filepath.Join(restoredRoot, filepath.FromSlash("objects/"+object.ObjectID+".payload")))
	if err != nil || !bytes.Equal(restored, payload) {
		t.Fatalf("restored original bytes mismatch: size=%d err=%v", len(restored), err)
	}
}

func TestCiphertextImportDoesNotCheckpointAfterLeaseIsTerminatedDuringRead(t *testing.T) {
	ctx := context.Background()
	sourceStore, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := sourceStore.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	providerInfo, err := provider.Create()
	if err != nil {
		t.Fatal(err)
	}
	state := &replicaLeaseState{current: testTransferringLease(snapshot, manifest), checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey)}
	state.current.Offer.Offer.ProviderNodeID = providerInfo.NodeID
	state.current.OfferHash = "sha256:" + strings.Repeat("b", 64)
	for _, encrypted := range state.current.Offer.Offer.EncryptedObjectSet {
		state.current.Progress = append(state.current.Progress, lease.Progress{ObjectID: encrypted.ObjectID, CommittedOffset: "0"})
	}
	importer, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	authorization := LeaseImportAuthorizer{LeaseID: state.current.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: state}
	reader, err := sourceStore.openCiphertext(ctx, manifest.Objects[0].ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	callback := callbackReader{reader: reader, onRead: func() { state.current.Status = lease.StatusTerminating }}
	_, err = importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, manifest.Objects[0], "0", callback, provider)
	_ = reader.Close()
	if !errors.Is(err, ErrUnauthorized) || len(state.current.Checkpoints) != 0 {
		t.Fatalf("terminated lease received checkpoint: status=%s checkpoints=%d err=%v", state.current.Status, len(state.current.Checkpoints), err)
	}
	if _, err := os.Lstat(filepath.Join(importer.root, objectDir, manifest.Objects[0].ObjectID+".irac")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminated transfer published object: %v", err)
	}
}

func TestConcurrentStaleCiphertextChunkCannotOverwriteDurablePrefix(t *testing.T) {
	ctx := context.Background()
	sourceStore, err := Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := sourceStore.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := nodeidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	providerInfo, err := provider.Create()
	if err != nil {
		t.Fatal(err)
	}
	state := &replicaLeaseState{current: testTransferringLease(snapshot, manifest), checkpointKey: mustDecodePublicKey(t, providerInfo.PublicKey)}
	state.current.Offer.Offer.ProviderNodeID = providerInfo.NodeID
	state.current.OfferHash = "sha256:" + strings.Repeat("c", 64)
	for _, encrypted := range state.current.Offer.Offer.EncryptedObjectSet {
		state.current.Progress = append(state.current.Progress, lease.Progress{ObjectID: encrypted.ObjectID, CommittedOffset: "0"})
	}
	importer, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	authorization := LeaseImportAuthorizer{LeaseID: state.current.Offer.Offer.LeaseID, ProviderNodeID: providerInfo.NodeID, Leases: state}
	first, err := sourceStore.openCiphertext(ctx, manifest.Objects[0].ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := io.ReadAll(first)
	closeErr := first.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read ciphertext: %v close=%v", err, closeErr)
	}
	split := len(ciphertext) / 4
	chunkA := append([]byte(nil), ciphertext[:split]...)
	chunkB := append([]byte(nil), chunkA...)
	chunkB[0] ^= 0xff
	start := make(chan struct{})
	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, candidate := range [][]byte{chunkA, chunkB} {
		wait.Add(1)
		go func(candidate []byte) {
			defer wait.Done()
			<-start
			_, err := importer.ImportCiphertextChunk(ctx, authorization, snapshot, manifest, manifest.Objects[0], "0", bytes.NewReader(candidate), provider)
			results <- result{data: candidate, err: err}
		}(candidate)
	}
	close(start)
	wait.Wait()
	close(results)
	var committed []byte
	var conflicts int
	for outcome := range results {
		if outcome.err == nil {
			committed = outcome.data
		} else if errors.Is(outcome.err, ErrTransferConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent transfer result: %v", outcome.err)
		}
	}
	if len(committed) == 0 || conflicts != 1 || state.current.Progress[0].CommittedOffset != strconv.Itoa(split) {
		t.Fatalf("concurrent result committed=%d conflicts=%d progress=%+v", len(committed), conflicts, state.current.Progress)
	}
	partial, err := os.ReadFile(importer.transferPartialPath(authorization.LeaseID, manifest.Objects[0].ObjectID))
	if err != nil || !bytes.Equal(partial, committed) {
		t.Fatalf("stale request overwrote committed prefix: got=%x want=%x err=%v", partial, committed, err)
	}
}

func mustDecodePublicKey(t *testing.T, encoded string) ed25519.PublicKey {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PublicKeySize {
		t.Fatalf("decode node public key: %v", err)
	}
	return ed25519.PublicKey(decoded)
}

func TestEncryptSnapshotRequiresCommittedCapabilityBeforeReadingCanonicalData(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	source := &mapSource{coordinate: coordinate, payload: payload}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	verifier := &testAuthorizer{err: errors.New("revoked capability")}
	_, err = store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(verifier))
	if !errors.Is(err, ErrUnauthorized) || source.opened != 0 || verifier.calls != 1 {
		t.Fatalf("unauthorized replication read source: err=%v opens=%d auth calls=%d", err, source.opened, verifier.calls)
	}
}

func TestPeerCiphertextReadRequiresCurrentSnapshotCapability(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := store.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	denied := &testAuthorizer{err: errors.New("grant revoked")}
	_, err = store.OpenCiphertextForPeer(ctx, snapshot, bundle, testAuthorization(denied), "archive.view", bundle.Objects[0].ObjectID)
	if !errors.Is(err, ErrUnauthorized) || denied.calls != 1 {
		t.Fatalf("revoked capability read err=%v calls=%d", err, denied.calls)
	}
	wrongSnapshot := snapshot
	wrongSnapshot.SnapshotID = "snap_" + strings.Repeat("0", 64)
	if _, err := store.OpenCiphertextForPeer(ctx, wrongSnapshot, bundle, testAuthorization(&testAuthorizer{}), "archive.view", bundle.Objects[0].ObjectID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("object read under wrong snapshot was not rejected: %v", err)
	}
}

func TestRestoreSnapshotDiscardsPartialPlaintextAfterCiphertextCorruption(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	source := &mapSource{coordinate: coordinate, payload: payload}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	bundle, err := store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, objectDir, bundle.Objects[0].ObjectID+".irac")
	if err := os.Chmod(objectPath, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(objectPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := []byte{0}
	if _, err := file.ReadAt(corrupt, 60); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	corrupt[0] ^= 0xff
	if _, err := file.WriteAt(corrupt, 60); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "restore-must-not-appear")
	if _, err := store.RestoreSnapshot(ctx, snapshot, bundle, dek, destination, testAuthorization(&testAuthorizer{})); err == nil {
		t.Fatal("corrupt ciphertext was restored")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial plaintext destination exists after failed restore: %v", err)
	}
}

func TestEncryptSnapshotRejectsAlteredCanonicalSourceWithoutCommittingBundle(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	changed := append([]byte(nil), payload...)
	changed[len(changed)-1] ^= 0xff
	source := &mapSource{coordinate: coordinate, payload: changed}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	_, err = store.EncryptSnapshot(ctx, snapshot, source, dek, 1, testAuthorization(&testAuthorizer{}))
	if !errors.Is(err, ErrObjectUnavailable) {
		t.Fatalf("altered source result=%v", err)
	}
	manifestPath := store.bundlePath(snapshot.Content.ArchiveID, snapshot.SnapshotID, 1)
	if _, err := os.Lstat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle manifest committed after source mismatch: %v", err)
	}
}

func TestKeyEpochsReturnsOnlyValidatedSnapshotBundlesNewestFirst(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "replica-state"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	for _, epoch := range []uint64{1, 3} {
		dek, err := cryptoarchive.NewDEK()
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, epoch, testAuthorization(&testAuthorizer{}))
		dek.Destroy()
		if err != nil {
			t.Fatalf("encrypt key epoch %d: %v", epoch, err)
		}
	}
	if err := os.WriteFile(store.bundlePath(snapshot.Content.ArchiveID, snapshot.SnapshotID, 2), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(store.bundlePath(snapshot.Content.ArchiveID, snapshot.SnapshotID, 1)), "02.json"), []byte("not an epoch"), 0600); err != nil {
		t.Fatal(err)
	}

	epochs, err := store.KeyEpochs(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(epochs) != 2 || epochs[0] != 3 || epochs[1] != 1 {
		t.Fatalf("validated key epochs=%v, want [3 1]", epochs)
	}
}

func TestBuildLeaseOfferBindsVerifiedSnapshotAndCiphertextManifest(t *testing.T) {
	ctx := context.Background()
	snapshot, coordinate, payload := snapshotFixture(t)
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	source, err := Open(filepath.Join(t.TempDir(), "offer-source"))
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := source.EncryptSnapshot(ctx, snapshot, &mapSource{coordinate: coordinate, payload: payload}, dek, 4, testAuthorization(&testAuthorizer{}))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := PublicReplicaManifest(snapshot, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BundleFromPublicReplicaManifest(snapshot, manifest); err != nil {
		t.Fatalf("fixture public manifest invalid: %v", err)
	}
	if _, err := ReplicaManifestHash(snapshot, manifest); err != nil {
		t.Fatalf("fixture manifest hash invalid: %v", err)
	}
	issued := time.Date(2026, 11, 4, 5, 6, 7, 0, time.UTC)
	offer, err := BuildLeaseOffer(snapshot, manifest, LeaseOfferSpec{
		LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA", ProviderNodeID: "nd_BBBBBBBBBBBBBBBBBBBBBB",
		OwnerSubjectID: "os_CCCCCCCCCCCCCCCCCCCCCC", KeyEpoch: 4, IssuedAt: issued, ExpiresAt: issued.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("build lease offer: %v", err)
	}
	_, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := identityprotocol.IdentityID(ownerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(ownerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	actor := authority.Principal{Kind: authority.PrincipalNode, ID: "nd_DDDDDDDDDDDDDDDDDDDDDD"}
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: offer.OwnerSubjectID,
		Issuer: authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID}, Audience: actor,
		ArchiveID: offer.ArchiveID, SnapshotID: offer.SnapshotID, Actions: []string{"archive.replicate"},
		NotBefore: issued.Format(time.RFC3339), ExpiresAt: issued.Add(24 * time.Hour).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: strings.Repeat("n", 43), PolicyEpoch: "1", IssuerKeyID: ownerKeyID,
	}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	_, signerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := lease.SignOffer(offer, actor, []authority.SignedGrant{grant}, signerKey)
	if err != nil {
		t.Fatalf("built offer failed durable lease validation after Node signing: %v", err)
	}
	if _, err := lease.CanonicalOffer(signed.Offer); err != nil {
		t.Fatalf("signed offer failed canonical validation: %v", err)
	}
	hash, err := ReplicaManifestHash(snapshot, manifest)
	if err != nil || offer.ArchiveID != snapshot.Content.ArchiveID || offer.SnapshotID != snapshot.SnapshotID ||
		offer.PlaintextManifestHash != snapshot.ContentHash || offer.ReplicaManifestHash != hash ||
		offer.EncryptedObjectSet[0].ObjectID != manifest.Objects[0].ObjectID || offer.ReservedBytes != manifest.Objects[0].CiphertextSize {
		t.Fatalf("offer is not bound to the verified snapshot/manifest: offer=%+v manifest=%+v err=%v", offer, manifest, err)
	}
	changed := manifest
	changed.KeyEpoch = "5"
	if _, err := BuildLeaseOffer(snapshot, changed, LeaseOfferSpec{
		LeaseID: offer.LeaseID, ProviderNodeID: offer.ProviderNodeID, OwnerSubjectID: offer.OwnerSubjectID,
		KeyEpoch: 4, IssuedAt: issued, ExpiresAt: issued.Add(24 * time.Hour),
	}); err == nil {
		t.Fatal("offer builder accepted a replica manifest whose epoch is not bound to the ciphertext bundle")
	}
}

func TestReplicaSnapshotPersistenceIsImmutableAndFailClosed(t *testing.T) {
	snapshot, _, _ := snapshotFixture(t)
	store, err := Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistSnapshot(snapshot); err != nil {
		t.Fatalf("persist verified snapshot descriptor: %v", err)
	}
	if err := store.PersistSnapshot(snapshot); err != nil {
		t.Fatalf("idempotent snapshot persistence: %v", err)
	}
	loaded, err := store.LoadSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID)
	loadedBytes, loadedMarshalErr := archivesnapshot.Marshal(loaded)
	wantBytes, wantMarshalErr := archivesnapshot.Marshal(snapshot)
	if err != nil || loadedMarshalErr != nil || wantMarshalErr != nil || !bytes.Equal(loadedBytes, wantBytes) {
		t.Fatalf("loaded snapshot mismatch: equal=%t err=%v", bytes.Equal(loadedBytes, wantBytes), err)
	}
	if _, err := store.LoadSnapshot(snapshot.Content.ArchiveID, "snap_"+strings.Repeat("0", 64)); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("missing snapshot error=%v, want not found", err)
	}

	path := store.snapshotPath(snapshot.Content.ArchiveID, snapshot.SnapshotID)
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("snapshot with non-private mode was accepted: %v", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSnapshot(snapshot.Content.ArchiveID, snapshot.SnapshotID); !errors.Is(err, ErrInvalid) {
		t.Fatalf("corrupt snapshot was accepted: %v", err)
	}
	if err := store.PersistSnapshot(snapshot); !errors.Is(err, ErrInvalid) {
		t.Fatalf("conflicting immutable snapshot slot was replaced: %v", err)
	}
}

func TestPeerLeaseObjectReaderStreamsOnlySignedObjectRanges(t *testing.T) {
	data := bytes.Repeat([]byte("ciphertext-readback/"), int(MaxCiphertextChunkBytes/20)+3)
	digest := sha256.Sum256(data)
	objectID := objectIDForDigest(hex.EncodeToString(digest[:]))
	providerID := "nd_aaaaaaaaaaaaaaaaaaaaaa"
	leaseID := "rl_AAAAAAAAAAAAAAAAAAAAAA"
	current := lease.Lease{Status: lease.StatusVerifying, Offer: lease.SignedOffer{Offer: lease.Offer{
		LeaseID: leaseID, ArchiveID: "arc_AAAAAAAAAAAAAAAAAAAAAA", SnapshotID: "snap_" + strings.Repeat("a", 64),
		KeyEpoch: "7", ProviderNodeID: providerID,
		EncryptedObjectSet: []lease.EncryptedObject{{ObjectID: objectID, CiphertextSize: strconv.FormatInt(int64(len(data)), 10), CiphertextSHA256: "sha256:" + hex.EncodeToString(digest[:])}},
	}}}
	source := &ciphertextRangeMap{objectID: objectID, data: data}
	stream, err := (PeerLeaseObjectReader{Lease: current, Source: source}).OpenCiphertext(context.Background(), providerID, leaseID, objectID)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(got, data) {
		t.Fatalf("streamed bytes mismatch: size=%d read=%v close=%v", len(got), readErr, closeErr)
	}
	wantCalls := int((int64(len(data)) + MaxCiphertextChunkBytes - 1) / MaxCiphertextChunkBytes)
	if len(source.requests) != wantCalls || wantCalls < 2 {
		t.Fatalf("range requests=%v, want %d bounded calls", source.requests, wantCalls)
	}
	for _, request := range source.requests {
		if request[1] <= 0 || request[1] > MaxCiphertextChunkBytes {
			t.Fatalf("unbounded ciphertext range requested: %+v", request)
		}
	}
	if _, err := (PeerLeaseObjectReader{Lease: current, Source: source}).OpenCiphertext(context.Background(), "nd_bbbbbbbbbbbbbbbbbbbbbb", leaseID, objectID); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("readback accepted the wrong provider: %v", err)
	}
	if _, err := (PeerLeaseObjectReader{Lease: current, Source: source}).OpenCiphertext(context.Background(), providerID, leaseID, objectIDForDigest(strings.Repeat("0", 64))); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("readback accepted an object outside the signed offer: %v", err)
	}
}

func snapshotFixture(t *testing.T) (archivesnapshot.Snapshot, archiveindex.Coordinate, []byte) {
	t.Helper()
	session, err := archiveindex.NewSessionIdentity("recording-replica-01", "hls", nil, "broadcast-replica-01")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("recording-replica-01", session)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("segment-native-byte-sequence/"), 4096)
	digest := sha256.Sum256(payload)
	coordinate := archiveindex.Coordinate{SessionID: session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia}
	_, err = archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: coordinate, ClaimID: "replica-fixture", Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt:  time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
		PayloadPath: "recordings/local/segment.ts", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		Verification: archiveindex.VerificationVerified, Duration: 2.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, coordinate, payload
}

func twoObjectSnapshotFixture(t *testing.T) (archivesnapshot.Snapshot, coordinateMapSource) {
	t.Helper()
	session, err := archiveindex.NewSessionIdentity("recording-rekey-fixture", "hls", nil, "broadcast-rekey-fixture")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("recording-rekey-fixture", session)
	if err != nil {
		t.Fatal(err)
	}
	source := coordinateMapSource{}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		coordinate := archiveindex.Coordinate{
			SessionID: session.ID, TrackID: "main", SourceEpoch: 1,
			DiscontinuitySequence: 1, Sequence: sequence, Kind: archiveindex.ObjectMedia,
		}
		payload := []byte("original-canonical-segment-" + strconv.FormatUint(sequence, 10))
		digest := sha256.Sum256(payload)
		if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
			Coordinate: coordinate, ClaimID: "rekey-fixture-" + strconv.FormatUint(sequence, 10),
			Source:      archiveindex.ClaimLiveOrigin,
			AcquiredAt:  time.Date(2026, 11, 3, 0, 0, int(sequence), 0, time.UTC),
			PayloadPath: "recordings/rekey/segment-" + strconv.FormatUint(sequence, 10) + ".ts",
			Size:        int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
			Verification: archiveindex.VerificationVerified, Duration: 2,
		}); err != nil {
			t.Fatal(err)
		}
		source[coordinate] = payload
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, source
}

func testAuthorization(verifier *testAuthorizer) Authorization {
	return Authorization{
		SubjectID: "os_test_subject", Actor: authority.Principal{Kind: authority.PrincipalNode, ID: "nd_test_node"},
		Chain: []authority.SignedGrant{{Grant: authority.Grant{}}}, Verifier: verifier,
	}
}

func testTransferringLease(snapshot archivesnapshot.Snapshot, manifest ReplicaManifest) lease.Lease {
	objects := make([]lease.EncryptedObject, 0, len(manifest.Objects))
	var reserved int64
	for _, object := range manifest.Objects {
		objects = append(objects, lease.EncryptedObject{ObjectID: object.ObjectID, CiphertextSize: object.CiphertextSize, CiphertextSHA256: object.CiphertextSHA256})
		size, _ := strconv.ParseInt(object.CiphertextSize, 10, 64)
		reserved += size
	}
	manifestHash, _ := ReplicaManifestHash(snapshot, manifest)
	return lease.Lease{
		Status: lease.StatusTransferring,
		Offer: lease.SignedOffer{Offer: lease.Offer{
			LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA", ArchiveID: manifest.ArchiveID, SnapshotID: manifest.SnapshotID,
			KeyEpoch:              manifest.KeyEpoch,
			ReplicaManifestHash:   manifestHash,
			PlaintextManifestHash: snapshot.ContentHash,
			ProviderNodeID:        "nd_test_provider", ExpiresAt: time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
			EncryptedObjectSet: objects, ReservedBytes: strconv.FormatInt(reserved, 10),
		}},
	}
}

func TestEncryptSnapshotRechecksCapabilityBeforePublishingManifest(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "state")
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, coordinate, payload := snapshotFixture(t)
	source := &mapSource{coordinate: coordinate, payload: payload}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	verifier := &testAuthorizer{denyOn: 2}
	auth := testAuthorization(verifier)
	_, err = store.EncryptSnapshot(ctx, snapshot, source, dek, 1, auth)
	if !errors.Is(err, ErrUnauthorized) || verifier.calls != 2 || len(verifier.checkedAt) != 2 || verifier.checkedAt[1].Before(verifier.checkedAt[0]) {
		t.Fatalf("stale capability result err=%v authorize=%d checkedAt=%v", err, verifier.calls, verifier.checkedAt)
	}
	if _, err := os.Lstat(store.bundlePath(snapshot.Content.ArchiveID, snapshot.SnapshotID, 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle manifest published after capability revocation: %v", err)
	}
}

func TestCiphertextObjectIDIsDigestBound(t *testing.T) {
	value := strings.Repeat("a", 64)
	id := objectIDForDigest("sha256:" + value)
	if !validObjectID(id) {
		t.Fatalf("generated object id is invalid: %q", id)
	}
	changed := id[:len(id)-1] + "B"
	if validObjectID(changed) {
		t.Fatalf("accepted modified object id %q", changed)
	}
}
