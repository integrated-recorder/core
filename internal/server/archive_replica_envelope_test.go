package server

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/replication"
)

func TestResolveArchiveReplicaKeyEnvelopeLoadsDurableEnvelopeOnRetry(t *testing.T) {
	store, err := replication.Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := archiveReplicaEnvelopeSnapshot(t)
	recipientPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootKeyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	envelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 7, recipientPrivate.PublicKey().Bytes(), rootKeyID, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistKeyEnvelope(snapshot, envelope, rootPublic); err != nil {
		t.Fatal(err)
	}

	loaded, err := resolveArchiveReplicaKeyEnvelope(store, snapshot, 7, recipientPrivate.PublicKey().Bytes(), rootPublic, cryptoarchive.KeyEnvelope{})
	if err != nil || loaded != envelope {
		t.Fatalf("durable retry envelope=%+v err=%v", loaded, err)
	}
	if _, err := resolveArchiveReplicaKeyEnvelope(nil, snapshot, 7, recipientPrivate.PublicKey().Bytes(), rootPublic, cryptoarchive.KeyEnvelope{}); !errors.Is(err, replication.ErrObjectUnavailable) {
		t.Fatalf("missing durable envelope error=%v", err)
	}
	wrongEpoch := envelope
	wrongEpoch.KeyEpoch = "8"
	if _, err := resolveArchiveReplicaKeyEnvelope(store, snapshot, 7, recipientPrivate.PublicKey().Bytes(), rootPublic, wrongEpoch); !errors.Is(err, cryptoarchive.ErrEnvelopeUnauthorized) {
		t.Fatalf("envelope from another key epoch accepted: %v", err)
	}
	wrongRoot := make([]byte, ed25519.PublicKeySize)
	if _, err := resolveArchiveReplicaKeyEnvelope(store, snapshot, 7, recipientPrivate.PublicKey().Bytes(), wrongRoot, envelope); !errors.Is(err, cryptoarchive.ErrEnvelopeUnauthorized) {
		t.Fatalf("envelope accepted under another identity root: %v", err)
	}
}

func archiveReplicaEnvelopeSnapshot(t *testing.T) archivesnapshot.Snapshot {
	t.Helper()
	session, err := archiveindex.NewSessionIdentity("replica-envelope-test", "hls", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("replica-envelope-test", session)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ArchiveID = "arc_AAAAAAAAAAAAAAAAAAAAAA"
	inventory.Capture = archiveindex.CaptureClosed
	inventory.Archive = archiveindex.ArchiveSealed
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
