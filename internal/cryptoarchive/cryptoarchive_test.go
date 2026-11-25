package cryptoarchive

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestChunkedObjectRoundTripAndRetryKeySeparation(t *testing.T) {
	dek, err := DEKFromBytes(bytes.Repeat([]byte{0x42}, keyBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	plain := bytes.Repeat([]byte("recording-bytes-"), ChunkSize/16+7)
	descriptor := testDescriptor(plain)

	var first, retry bytes.Buffer
	firstResult, err := EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(plain), &first)
	if err != nil {
		t.Fatal(err)
	}
	retryResult, err := EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(plain), &retry)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Bytes(), retry.Bytes()) {
		t.Fatal("a retry reused the object key/nonce stream and ciphertext")
	}
	if firstResult.ChunkCount < 2 || firstResult.CiphertextSize != int64(first.Len()) || firstResult.CiphertextSHA256 == retryResult.CiphertextSHA256 {
		t.Fatalf("unexpected encryption results: first=%+v retry=%+v", firstResult, retryResult)
	}
	verifiedDescriptor := descriptor
	verifiedDescriptor.CiphertextSize = firstResult.CiphertextSize
	verifiedDescriptor.CiphertextSHA256 = firstResult.CiphertextSHA256
	var restored bytes.Buffer
	if err := DecryptObject(context.Background(), dek, verifiedDescriptor, bytes.NewReader(first.Bytes()), &restored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.Bytes(), plain) {
		t.Fatal("restored payload differs from original bytes")
	}
}

func TestDEKSameKeyReportsRotationReuseWithoutExposingKey(t *testing.T) {
	first, err := DEKFromBytes(bytes.Repeat([]byte{0x31}, keyBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Destroy()
	same, err := DEKFromBytes(bytes.Repeat([]byte{0x31}, keyBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer same.Destroy()
	different, err := DEKFromBytes(bytes.Repeat([]byte{0x32}, keyBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer different.Destroy()
	if !first.SameKey(same) || first.SameKey(different) || first.SameKey(nil) {
		t.Fatal("DEK comparison returned an incorrect rotation-reuse result")
	}
	same.Destroy()
	if first.SameKey(same) {
		t.Fatal("destroyed DEK was treated as a usable matching rotation key")
	}
}

func TestSnapshotDescriptorEncryptsAndRestoresStoredCanonicalBytes(t *testing.T) {
	const recordingID = "0123456789abcdef0123456789abcdef"
	const relativePath = "tracks/main/segment-0001.ts"
	plain := bytes.Repeat([]byte("segment-native-original\x00"), 4096)
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.NewRecordingDir(recordingID); err != nil {
		t.Fatal(err)
	}
	result, err := store.SavePayload(recordingID, relativePath, bytes.NewReader(plain), int64(len(plain)))
	if err != nil {
		t.Fatal(err)
	}
	session, err := archiveindex.NewSessionIdentity(recordingID, "hls", nil, "broadcast-fixture")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory(recordingID, session)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := archiveindex.Coordinate{
		SessionID: session.ID, TrackID: "main", SourceEpoch: 1,
		DiscontinuitySequence: 2, Sequence: 3, Kind: archiveindex.ObjectMedia,
	}
	if _, err := archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: coordinate, ClaimID: "canonical-origin-claim", Source: archiveindex.ClaimLiveOrigin,
		AcquiredAt: time.Now().UTC(), PayloadPath: relativePath, Size: result.Size,
		SHA256: result.SHA256, Verification: archiveindex.VerificationVerified, Duration: 2.25,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := DescriptorForSnapshotObject(snapshot, coordinate, 7)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	canonicalReader, err := store.OpenPayloadReader(recordingID, relativePath)
	if err != nil {
		t.Fatal(err)
	}
	var encrypted bytes.Buffer
	sealed, encryptErr := EncryptObject(context.Background(), dek, descriptor, canonicalReader, &encrypted)
	closeErr := canonicalReader.Close()
	if encryptErr != nil || closeErr != nil {
		t.Fatalf("encrypt stored canonical bytes: encrypt=%v close=%v", encryptErr, closeErr)
	}
	verifiedDescriptor := descriptor
	verifiedDescriptor.CiphertextSize = sealed.CiphertextSize
	verifiedDescriptor.CiphertextSHA256 = sealed.CiphertextSHA256
	var restored bytes.Buffer
	if err := DecryptObject(context.Background(), dek, verifiedDescriptor, bytes.NewReader(encrypted.Bytes()), &restored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.Bytes(), plain) {
		t.Fatal("snapshot-based encrypted restore changed canonical media bytes")
	}
	changed := []byte("different media bytes")
	if _, err := store.SavePayload(recordingID, relativePath, bytes.NewReader(changed), int64(len(changed))); err != nil {
		t.Fatal(err)
	}
	changedReader, err := store.OpenPayloadReader(recordingID, relativePath)
	if err != nil {
		t.Fatal(err)
	}
	var rejectedStage bytes.Buffer
	_, encryptErr = EncryptObject(context.Background(), dek, descriptor, changedReader, &rejectedStage)
	closeErr = changedReader.Close()
	if !errors.Is(encryptErr, ErrPlaintextMismatch) || closeErr != nil {
		t.Fatalf("changed canonical payload result encrypt=%v close=%v", encryptErr, closeErr)
	}
	wrong := coordinate
	wrong.Sequence++
	if _, err := DescriptorForSnapshotObject(snapshot, wrong, 7); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("descriptor for unlisted coordinate error = %v", err)
	}
}

func TestChunkedObjectRejectsTamperingAndDescriptorMismatch(t *testing.T) {
	dek, _ := DEKFromBytes(bytes.Repeat([]byte{0x33}, keyBytes))
	defer dek.Destroy()
	plain := bytes.Repeat([]byte("segment"), 11000)
	descriptor := testDescriptor(plain)
	var encrypted bytes.Buffer
	if _, err := EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(plain), &encrypted); err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), encrypted.Bytes()...)
	tampered[len(tampered)-1] ^= 0x80
	var out bytes.Buffer
	if err := DecryptObject(context.Background(), dek, descriptor, bytes.NewReader(tampered), &out); !errors.Is(err, ErrCiphertextInvalid) {
		t.Fatalf("tampering error = %v", err)
	}
	wrongCipherHash := descriptor
	wrongCipherHash.CiphertextSize = int64(encrypted.Len())
	wrongCipherHash.CiphertextSHA256 = "sha256:" + strings.Repeat("0", 64)
	out.Reset()
	if err := DecryptObject(context.Background(), dek, wrongCipherHash, bytes.NewReader(encrypted.Bytes()), &out); !errors.Is(err, ErrCiphertextInvalid) {
		t.Fatalf("ciphertext digest mismatch error = %v", err)
	}

	wrongCoordinate := descriptor
	wrongCoordinate.Coordinate.Sequence++
	out.Reset()
	if err := DecryptObject(context.Background(), dek, wrongCoordinate, bytes.NewReader(encrypted.Bytes()), &out); !errors.Is(err, ErrCiphertextInvalid) {
		t.Fatalf("descriptor mismatch error = %v", err)
	}
}

func TestEncryptRequiresExactDeclaredSourceIdentity(t *testing.T) {
	dek, _ := DEKFromBytes(bytes.Repeat([]byte{0x11}, keyBytes))
	defer dek.Destroy()
	plain := []byte("actual source")
	descriptor := testDescriptor(append([]byte(nil), plain...))
	descriptor.PlaintextSHA256 = "sha256:" + strings.Repeat("0", 64)
	var staging bytes.Buffer
	if _, err := EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(plain), &staging); !errors.Is(err, ErrPlaintextMismatch) {
		t.Fatalf("hash mismatch error = %v", err)
	}
	if staging.Len() == 0 {
		t.Fatal("test did not exercise partial staging output")
	}
	// This API deliberately does not publish: callers must discard staging on
	// every error. The source/archive layer owns that atomic commit boundary.
}

func TestEnvelopeWrapOpenAndAuthorityBinding(t *testing.T) {
	dek, _ := DEKFromBytes(bytes.Repeat([]byte{0xa5}, keyBytes))
	defer dek.Destroy()
	signerSeed := bytes.Repeat([]byte{0x17}, ed25519.SeedSize)
	signer := ed25519.NewKeyFromSeed(signerSeed)
	signerID, err := identityprotocol.KeyID(signer.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	recipientPrivate, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{0x29}, 32))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := WrapDEK(dek, "arc_0123456789abcdefghijkl", 3, recipientPrivate.PublicKey().Bytes(), signerID, signer)
	if err != nil {
		t.Fatal(err)
	}
	expectation := EnvelopeExpectation{
		ArchiveID: "arc_0123456789abcdefghijkl", KeyEpoch: 3,
		RecipientKeyID: RecipientKeyID(recipientPrivate.PublicKey().Bytes()),
		SignerKeyID:    signerID, SignerPublicKey: signer.Public().(ed25519.PublicKey),
		RecipientPrivate: recipientPrivate.Bytes(),
	}
	encoded, err := MarshalKeyEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseKeyEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyKeyEnvelopeSignature(parsed, signer.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("verify signed envelope without opening HPKE ciphertext: %v", err)
	}
	wrongSignerPublic := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x18}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if err := VerifyKeyEnvelopeSignature(parsed, wrongSignerPublic); !errors.Is(err, ErrEnvelopeUnauthorized) {
		t.Fatalf("wrong envelope signer error = %v", err)
	}
	opened, err := OpenDEK(parsed, expectation)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Destroy()
	if !bytes.Equal(opened.key[:], dek.key[:]) {
		t.Fatal("HPKE recipient recovered a different DEK")
	}

	wrong := expectation
	wrong.KeyEpoch++
	if _, err := OpenDEK(parsed, wrong); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("wrong epoch error = %v", err)
	}
	wrong = expectation
	wrong.RecipientPrivate = bytes.Repeat([]byte{0x31}, 32)
	if _, err := OpenDEK(parsed, wrong); !errors.Is(err, ErrEnvelopeUnauthorized) {
		t.Fatalf("wrong recipient error = %v", err)
	}
	wrong = expectation
	wrong.SignerPublicKey = wrongSignerPublic
	if _, err := OpenDEK(parsed, wrong); !errors.Is(err, ErrEnvelopeUnauthorized) {
		t.Fatalf("wrong signer error = %v", err)
	}

	// Change a guaranteed character. Replacing the final Base64URL character
	// with a fixed value can be a no-op when random ciphertext already ends in
	// that value, making this tamper test flaky under repeated/race runs.
	first := parsed.Ciphertext[0]
	if first == 'A' {
		parsed.Ciphertext = "B" + parsed.Ciphertext[1:]
	} else {
		parsed.Ciphertext = "A" + parsed.Ciphertext[1:]
	}
	if _, err := OpenDEK(parsed, expectation); err == nil {
		t.Fatal("modified envelope was accepted")
	}
}

func TestEnvelopeParserRejectsDuplicateUnknownAndOversizedInput(t *testing.T) {
	if _, err := ParseKeyEnvelope([]byte(`{"envelope_version":"ir.key-envelope/1","envelope_version":"ir.key-envelope/1"}`)); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("duplicate field error = %v", err)
	}
	if _, err := ParseKeyEnvelope([]byte(`{"unexpected":"value"}`)); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := ParseKeyEnvelope(bytes.Repeat([]byte{'x'}, 16<<10+1)); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("oversized input error = %v", err)
	}
}

func TestContextAndDestroyedDEKFailClosed(t *testing.T) {
	dek, _ := DEKFromBytes(bytes.Repeat([]byte{0x21}, keyBytes))
	plain := []byte("small recording payload")
	descriptor := testDescriptor(plain)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := EncryptObject(ctx, dek, descriptor, bytes.NewReader(plain), io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	dek.Destroy()
	if _, err := EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(plain), io.Discard); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("destroyed key error = %v", err)
	}
	if err := DecryptObject(context.Background(), dek, descriptor, bytes.NewReader(nil), io.Discard); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("decrypt with destroyed key error = %v", err)
	}
}

func TestDestroyWaitsForInFlightEncryption(t *testing.T) {
	dek, err := NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("in-flight archive object")
	descriptor := testDescriptor(plain)
	reader := &blockedFirstRead{reader: bytes.NewReader(plain), entered: make(chan struct{}), release: make(chan struct{})}
	var encrypted bytes.Buffer
	encryptDone := make(chan error, 1)
	go func() {
		_, err := EncryptObject(context.Background(), dek, descriptor, reader, &encrypted)
		encryptDone <- err
	}()
	<-reader.entered
	if dek.mu.TryLock() {
		dek.mu.Unlock()
		t.Fatal("in-flight encryption did not hold the DEK read lock")
	}

	destroyStarted := make(chan struct{})
	destroyDone := make(chan struct{})
	go func() {
		close(destroyStarted)
		dek.Destroy()
		close(destroyDone)
	}()
	<-destroyStarted
	close(reader.release)

	if err := <-encryptDone; err != nil {
		t.Fatalf("in-flight encryption failed while Destroy was waiting: %v", err)
	}
	<-destroyDone
	if _, err := EncryptObject(context.Background(), dek, descriptor, bytes.NewReader(plain), io.Discard); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("encryption after Destroy = %v, want ErrInvalidInput", err)
	}
}

type blockedFirstRead struct {
	reader  *bytes.Reader
	entered chan struct{}
	release chan struct{}
	blocked bool
}

func (r *blockedFirstRead) Read(p []byte) (int, error) {
	if !r.blocked {
		r.blocked = true
		close(r.entered)
		<-r.release
	}
	return r.reader.Read(p)
}

func testDescriptor(plain []byte) ObjectDescriptor {
	digest := sha256.Sum256(plain)
	return ObjectDescriptor{
		ArchiveID: "arc_0123456789abcdefghijkl",
		Coordinate: archiveindex.Coordinate{
			SessionID: "session-" + strings.Repeat("a", 64),
			TrackID:   "track-0", SourceEpoch: 1,
			DiscontinuitySequence: 2, Sequence: 42,
			Kind: archiveindex.ObjectMedia,
		},
		ObjectType: string(archiveindex.ObjectMedia), PlaintextSize: int64(len(plain)),
		PlaintextSHA256: "sha256:" + hex.EncodeToString(digest[:]), KeyEpoch: 1,
	}
}

func TestObjectRejectsMalformedCoordinatesAndEpoch(t *testing.T) {
	dek, _ := NewDEK()
	defer dek.Destroy()
	plain := []byte("payload")
	descriptor := testDescriptor(plain)
	for name, mutate := range map[string]func(*ObjectDescriptor){
		"archive id":     func(value *ObjectDescriptor) { value.ArchiveID = "archive-1" },
		"track mismatch": func(value *ObjectDescriptor) { value.ObjectType = string(archiveindex.ObjectInit) },
		"missing epoch":  func(value *ObjectDescriptor) { value.KeyEpoch = 0 },
		"coordinate":     func(value *ObjectDescriptor) { value.Coordinate.TrackID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := descriptor
			mutate(&invalid)
			if _, err := EncryptObject(context.Background(), dek, invalid, bytes.NewReader(plain), io.Discard); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("malformed descriptor error = %v", err)
			}
		})
	}
}
