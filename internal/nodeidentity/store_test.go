package nodeidentity

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

func TestAdvertisementSigningBindsNodeKeyAndPersistsSequence(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	identityHint, err := discoveryprotocol.GlobalIdentityHint("gi_AAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	headHash := "sha256:" + strings.Repeat("b", 64)
	addressEnvelope := nodeTestAddressEnvelope(t, identityHint, headHash, info.NodeID, 0x31)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	ad := discoveryprotocol.Advertisement{
		AdvertisementVersion: discoveryprotocol.Version,
		GlobalIdentityHint:   identityHint,
		IdentityHeadHash:     headHash,
		NodeID:               "nd_AAAAAAAAAAAAAAAAAAAAAA", // ignored; the protected key is authoritative.
		Sequence:             "1", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339),
		AddressEnvelope: addressEnvelope,
		Capabilities:    []string{"webrtc-datachannel/1"},
		RelayIDs:        []string{},
	}
	canonical, signature, err := store.SignAdvertisement(ad)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := discoveryprotocol.Verify(canonical, signature, now.Add(time.Minute))
	if err != nil || verified.Advertisement.NodeID != info.NodeID || verified.Advertisement.Sequence != "1" {
		t.Fatalf("signed advertisement=%+v err=%v", verified.Advertisement, err)
	}
	sequencePath := filepath.Join(root, "security", advertisementSequenceFile)
	if mode := fileModeAt(t, sequencePath); mode != fileMode {
		t.Fatalf("advertisement sequence mode=%#o, want %#o", mode, fileMode)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err = reopened.SignAdvertisement(ad)
	if err != nil {
		t.Fatal(err)
	}
	verified, err = discoveryprotocol.Verify(canonical, signature, now.Add(time.Minute))
	if err != nil || verified.Advertisement.Sequence != "2" {
		t.Fatalf("reopened advertisement sequence=%q err=%v", verified.Advertisement.Sequence, err)
	}
}

func TestPeerSignalSigningAndReplayConsumptionSurviveRestart(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("a", 64)
	sourceStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source, err := sourceStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	targetStore, err := Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	target, err := targetStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	otherStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	other, err := otherStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	state := signalingprotocol.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"node:" + source.NodeID: decodeNodePublicForTest(t, source.PublicKey),
			"node:" + target.NodeID: decodeNodePublicForTest(t, target.PublicKey),
			"node:" + other.NodeID:  decodeNodePublicForTest(t, other.PublicKey),
		},
	}
	resolve := func(id, requestedHead string, _ time.Time) (signalingprotocol.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return signalingprotocol.IdentityState{}, signalingprotocol.ErrStale
		}
		return state, nil
	}
	sessionID, err := signalingprotocol.NewSessionID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ephemeral, err := signalingprotocol.NewEphemeralKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	message, err := signalingprotocol.NewMessage(signalingprotocol.KindInit, sessionID, identityID, head, source.NodeID, identityID, target.NodeID, now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	message.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(ephemeral.PublicKey().Bytes())
	message, err = sourceStore.SignPeerSignal(message, resolve, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := targetStore.ConsumePeerSignal(message, resolve, now.Add(time.Second)); err != nil {
		t.Fatalf("first signal consume failed: %v", err)
	}
	if err := targetStore.ConsumePeerSignal(message, resolve, now.Add(time.Second)); !errors.Is(err, signalingprotocol.ErrReplay) {
		t.Fatalf("duplicate signal accepted: %v", err)
	}
	reopenedTarget, err := Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedTarget.ConsumePeerSignal(message, resolve, now.Add(2*time.Second)); !errors.Is(err, signalingprotocol.ErrReplay) {
		t.Fatalf("signal replay accepted after restart: %v", err)
	}
	wrongTarget := message
	wrongTarget.TargetNodeID = other.NodeID
	if err := targetStore.ConsumePeerSignal(wrongTarget, resolve, now.Add(time.Second)); !errors.Is(err, signalingprotocol.ErrDenied) {
		t.Fatalf("message for another Node Key accepted: %v", err)
	}
	wrongSource := message
	wrongSource.SourceNodeID = other.NodeID
	wrongSource.Signature = ""
	if _, err := sourceStore.SignPeerSignal(wrongSource, resolve, now); !errors.Is(err, signalingprotocol.ErrDenied) {
		t.Fatalf("store signed as another source Node: %v", err)
	}
	wrongTarget = message
	wrongTarget.TargetNodeID = "nd_" + strings.Repeat("x", 22)
	wrongTarget.Signature = ""
	if _, err := sourceStore.SignPeerSignal(wrongTarget, resolve, now); !errors.Is(err, signalingprotocol.ErrDenied) {
		t.Fatalf("store signed for an undelegated target Node: %v", err)
	}
}

func TestPeerCommandSigningAndReplayConsumptionSurviveRestart(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("b", 64)
	sourceStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source, err := sourceStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	targetStore, err := Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	target, err := targetStore.Create()
	if err != nil {
		t.Fatal(err)
	}
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			authority.PrincipalIdentity + ":" + identityID: rootPublic,
			authority.PrincipalNode + ":" + source.NodeID:  decodeNodePublicForTest(t, source.PublicKey),
			authority.PrincipalNode + ":" + target.NodeID:  decodeNodePublicForTest(t, target.PublicKey),
		},
	}
	resolve := func(id, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}
	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	const subjectID = "os_aaaaaaaaaaaaaaaaaaaaaa"
	const archiveID = "arc_bbbbbbbbbbbbbbbbbbbbbb"
	grantNonce := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x53}, 32))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: subjectID,
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: source.NodeID},
		ArchiveID: archiveID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: grantNonce, PolicyEpoch: "1", IssuerKeyID: keyID,
	}, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := peerprotocol.NewHello(identityID, head, source.NodeID, now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := challenge.SessionID
	requestID, nonce, err := peerprotocol.NewCommand(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"type":"archive.snapshot.get"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: sessionID,
		SourceIdentityID: identityID, SourceHeadHash: head, SourceNodeID: source.NodeID,
		TargetIdentityID: identityID, TargetNodeID: target.NodeID, OwnerSubjectID: subjectID,
		ArchiveID: archiveID, Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{grant}, Payload: payload,
	}
	signed, err := sourceStore.SignPeerCommand(command, resolve, now)
	if err != nil {
		t.Fatalf("sign command using protected Node Key: %v", err)
	}
	remote := authority.Principal{Kind: authority.PrincipalNode, ID: source.NodeID}
	reopenedTarget, err := Open(targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	start := make(chan struct{})
	for _, candidate := range []*Store{targetStore, reopenedTarget} {
		go func(candidate *Store) {
			<-start
			results <- candidate.ConsumePeerCommand(signed, resolve, now.Add(time.Second), sessionID, remote, identityID, target.NodeID)
		}(candidate)
	}
	close(start)
	var accepted, replayed int
	for range 2 {
		switch err := <-results; {
		case err == nil:
			accepted++
		case errors.Is(err, peerprotocol.ErrReplay):
			replayed++
		default:
			t.Fatalf("concurrent replay consume error=%v", err)
		}
	}
	if accepted != 1 || replayed != 1 {
		t.Fatalf("replay winners accepted=%d replayed=%d, want 1 each", accepted, replayed)
	}
	if err := targetStore.ConsumePeerCommand(signed, resolve, now.Add(time.Second), sessionID, remote, identityID, target.NodeID); !errors.Is(err, peerprotocol.ErrReplay) {
		t.Fatalf("duplicate command accepted: %v", err)
	}
	if err := reopenedTarget.ConsumePeerCommand(signed, resolve, now.Add(2*time.Second), sessionID, remote, identityID, target.NodeID); !errors.Is(err, peerprotocol.ErrReplay) {
		t.Fatalf("command replay accepted after restart: %v", err)
	}
	wrongTarget := signed
	wrongTarget.TargetNodeID = source.NodeID
	if err := targetStore.ConsumePeerCommand(wrongTarget, resolve, now.Add(time.Second), sessionID, remote, identityID, target.NodeID); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("command for another Node accepted: %v", err)
	}
	if mode := fileModeAt(t, filepath.Join(targetRoot, "security", peerReplayFile)); mode != fileMode {
		t.Fatalf("peer replay journal mode=%#o, want %#o", mode, fileMode)
	}
}

func TestAdvertisementSequenceCorruptionFailsClosed(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "security", advertisementSequenceFile)
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"sequence":"broken"}`), fileMode); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.SignAdvertisement(discoveryprotocol.Advertisement{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt sequence state was not rejected: %v", err)
	}
}

func TestConcurrentAdvertisementSigningUsesUniqueDurableSequences(t *testing.T) {
	root := t.TempDir()
	first, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := first.Create()
	if err != nil {
		t.Fatal(err)
	}
	stores := make([]*Store, 8)
	stores[0] = first
	for i := 1; i < len(stores); i++ {
		stores[i], err = Open(root)
		if err != nil {
			t.Fatal(err)
		}
	}
	identityHint, err := discoveryprotocol.GlobalIdentityHint("gi_AAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	headHash := "sha256:" + strings.Repeat("c", 64)
	addressEnvelope := nodeTestAddressEnvelope(t, identityHint, headHash, info.NodeID, 0x29)
	now := time.Now().UTC().Truncate(time.Second)
	base := discoveryprotocol.Advertisement{
		AdvertisementVersion: discoveryprotocol.Version,
		GlobalIdentityHint:   identityHint,
		IdentityHeadHash:     headHash,
		IssuedAt:             now.Format(time.RFC3339), ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339),
		NodeID: info.NodeID, AddressEnvelope: addressEnvelope,
		Capabilities: []string{"webrtc-datachannel/1"}, RelayIDs: []string{},
	}
	type result struct {
		sequence string
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, len(stores))
	var wait sync.WaitGroup
	for _, store := range stores {
		wait.Add(1)
		go func(store *Store) {
			defer wait.Done()
			<-start
			canonical, signature, err := store.SignAdvertisement(base)
			if err != nil {
				results <- result{err: err}
				return
			}
			verified, err := discoveryprotocol.Verify(canonical, signature, now.Add(time.Second))
			results <- result{sequence: verified.Advertisement.Sequence, err: err}
		}(store)
	}
	close(start)
	wait.Wait()
	close(results)
	seen := map[string]bool{}
	for result := range results {
		if result.err != nil || result.sequence == "" || seen[result.sequence] {
			t.Fatalf("concurrent sequence result=%+v seen=%v", result, seen)
		}
		seen[result.sequence] = true
	}
	if len(seen) != len(stores) || !seen["1"] || !seen["8"] {
		t.Fatalf("concurrent sequence set=%v", seen)
	}
}

func nodeTestAddressEnvelope(t *testing.T, hint, headHash, nodeID string, keyByte byte) string {
	t.Helper()
	privateKey, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{keyByte}, 32))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := discoveryprotocol.SealAddressEnvelope(privateKey.PublicKey().Bytes(), discoveryprotocol.AddressContext{
		GlobalIdentityHint: hint, IdentityHeadHash: headHash, NodeID: nodeID,
	}, []byte(`{"addresses":["wss://nodeidentity.example/signal"]}`))
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestNodeKeyPersistsAndSignsEnrollmentProofWithoutExportingPrivateKey(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Info(); !errors.Is(err, ErrNotCreated) {
		t.Fatalf("missing key error=%v", err)
	}
	created, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Info()
	if err != nil || info != created {
		t.Fatalf("info=%+v created=%+v err=%v", info, created, err)
	}
	if mode := fileModeAt(t, filepath.Join(root, "security", fileName)); mode != 0600 {
		t.Fatalf("private key file mode=%#o, want 0600", mode)
	}
	if mode := fileModeAt(t, filepath.Join(root, "security")); mode&0077 != 0 {
		t.Fatalf("security directory mode=%#o, must not grant group/other access", mode)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	reopenedInfo, err := reopened.Info()
	if err != nil || reopenedInfo != created {
		t.Fatalf("reopened identity=%+v expected=%+v err=%v", reopenedInfo, created, err)
	}
	if _, err := reopened.Create(); !errors.Is(err, ErrExists) {
		t.Fatalf("repeated create error=%v", err)
	}

	identityKey := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(identityKey); err != nil {
		t.Fatal(err)
	}
	identityPublic := ed25519.NewKeyFromSeed(identityKey).Public().(ed25519.PublicKey)
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	previousHash := "sha256:" + strings.Repeat("a1", 32)
	proof, err := reopened.BeginEnrollmentProof(identityID, previousHash, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if proof.NodeID != created.NodeID || proof.PublicKey != created.PublicKey || proof.Nonce == "" {
		t.Fatalf("proof public context=%+v", proof)
	}
	message, err := identityprotocol.NodeEnrollmentProofMessage(identityID, previousHash, proof.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, _ := base64.RawURLEncoding.DecodeString(proof.PublicKey)
	signature, _ := base64.RawURLEncoding.DecodeString(proof.Signature)
	if !ed25519.Verify(ed25519.PublicKey(publicKey), message, signature) {
		t.Fatal("proof did not verify against the persisted Node Key")
	}
}

func TestNodeKeySignsMutualPeerHandshakeWithoutExportingPrivateKey(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	identityPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstInfo, err := first.Create()
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	secondInfo, err := second.Create()
	if err != nil {
		t.Fatal(err)
	}
	head := "sha256:" + strings.Repeat("e", 64)
	initiator, err := first.SignPeerHello(identityID, head, now)
	if err != nil {
		t.Fatal(err)
	}
	responder, err := second.SignPeerHelloForSession(identityID, head, initiator.SessionID, now)
	if err != nil {
		t.Fatal(err)
	}
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{
			"node:" + firstInfo.NodeID:  decodeNodePublicForTest(t, firstInfo.PublicKey),
			"node:" + secondInfo.NodeID: decodeNodePublicForTest(t, secondInfo.PublicKey),
		},
	}
	resolve := func(id, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}
	transcript := peerprotocol.Transcript{Version: peerprotocol.HandshakeVersion, Initiator: initiator, Responder: responder}
	transcript.InitiatorProof, err = first.SignPeerTranscriptProof(transcript)
	if err != nil {
		t.Fatal(err)
	}
	transcript.ResponderProof, err = second.SignPeerTranscriptProof(transcript)
	if err != nil {
		t.Fatal(err)
	}
	left, right, err := peerprotocol.VerifyTranscript(transcript, resolve, now)
	if err != nil || left.ID != firstInfo.NodeID || right.ID != secondInfo.NodeID {
		t.Fatalf("handshake peers=(%+v,%+v), err=%v", left, right, err)
	}
	encoded, err := json.Marshal(transcript)
	if err != nil || bytes.Contains(encoded, []byte("private_key")) || bytes.Contains(encoded, []byte("PrivateKey")) {
		t.Fatalf("private key leaked through handshake serialization: %s err=%v", encoded, err)
	}
}

func TestPeerHandshakeReplayStateSurvivesRestartAndSerializesConcurrentStores(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	identityPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(identityPublic)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	head := "sha256:" + strings.Repeat("f", 64)
	hello, err := store.SignPeerHello(identityID, head, now)
	if err != nil {
		t.Fatal(err)
	}
	state := authority.IdentityState{
		IdentityID: identityID, HeadHash: head, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(4 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{"node:" + info.NodeID: decodeNodePublicForTest(t, info.PublicKey)},
	}
	resolve := func(id, requestedHead string, _ time.Time) (authority.IdentityState, error) {
		if id != identityID || requestedHead != "" && requestedHead != head {
			return authority.IdentityState{}, authority.ErrStaleIdentity
		}
		return state, nil
	}
	second, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, candidate := range []*Store{store, second} {
		go func(candidate *Store) {
			<-start
			results <- candidate.ConsumePeerHello(hello, resolve, now)
		}(candidate)
	}
	close(start)
	var accepted, replayed int
	for range 2 {
		switch err := <-results; {
		case err == nil:
			accepted++
		case errors.Is(err, peerprotocol.ErrReplay):
			replayed++
		default:
			t.Fatalf("unexpected concurrent replay result: %v", err)
		}
	}
	if accepted != 1 || replayed != 1 {
		t.Fatalf("accepted=%d replayed=%d", accepted, replayed)
	}
	restarted, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ConsumePeerHello(hello, resolve, now); !errors.Is(err, peerprotocol.ErrReplay) {
		t.Fatalf("replay accepted after Node Store restart: %v", err)
	}
	if mode := fileModeAt(t, filepath.Join(root, "security", peerReplayFile)); mode != fileMode {
		t.Fatalf("replay state mode=%#o, want %#o", mode, fileMode)
	}
	serialized, err := os.ReadFile(filepath.Join(root, "security", peerReplayFile))
	if err != nil || bytes.Contains(serialized, []byte("private_key")) || bytes.Contains(serialized, []byte(hello.Signature)) {
		t.Fatalf("replay state contains secret handshake material: bytes=%d err=%v", len(serialized), err)
	}
}

func decodeNodePublicForTest(t *testing.T, value string) ed25519.PublicKey {
	t.Helper()
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(encoded) != ed25519.PublicKeySize {
		t.Fatalf("decode public Node Key: bytes=%d err=%v", len(encoded), err)
	}
	return ed25519.PublicKey(encoded)
}

func TestNodeKeySignsReplicaCheckpointForItsOwnIdentity(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := lease.Checkpoint{
		Version: lease.ProtocolVersion, LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA",
		OfferHash:       "sha256:" + strings.Repeat("a", 64),
		ProviderNodeID:  "nd_AAAAAAAAAAAAAAAAAAAAAA", // caller supplied identity is replaced.
		ObjectID:        "ob_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		CommittedOffset: "4096", RecordedAt: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
	}
	if _, err := store.SignReplicaCheckpoint(checkpoint); !errors.Is(err, ErrNotCreated) {
		t.Fatalf("checkpoint signed before Node Key creation: %v", err)
	}
	info, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	signed, err := store.SignReplicaCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if signed.Checkpoint.ProviderNodeID != info.NodeID {
		t.Fatalf("signer accepted caller provider identity %q, want own %q", signed.Checkpoint.ProviderNodeID, info.NodeID)
	}
	canonical, err := lease.CanonicalCheckpoint(signed.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(signed.Signature)
	publicKey, errPublic := base64.RawURLEncoding.DecodeString(info.PublicKey)
	if err != nil || errPublic != nil || !ed25519.Verify(ed25519.PublicKey(publicKey), append([]byte("IR/replica-lease/v1/checkpoint\x00"), canonical...), signature) {
		t.Fatal("provider checkpoint signature did not verify against the public Node Key")
	}
	data, err := os.ReadFile(filepath.Join(root, "security", fileName))
	if err != nil || bytes.Contains(data, signature) {
		t.Fatalf("Node Key storage unexpectedly contains emitted signature or could not be read: %v", err)
	}
}

func TestNodeKeySignsReplicaOfferAsOnlyItsOwnNode(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Create()
	if err != nil {
		t.Fatal(err)
	}
	_, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerPublic := ownerPrivate.Public().(ed25519.PublicKey)
	ownerID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: "os_aaaaaaaaaaaaaaaaaaaaaa",
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID},
		ArchiveID: "arc_bbbbbbbbbbbbbbbbbbbbbb", SnapshotID: "snap_" + strings.Repeat("a", 64),
		Actions: []string{"archive.replicate"}, NotBefore: now.Add(-time.Minute).Format(time.RFC3339),
		ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), DelegationDepth: "0",
		Nonce:       base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x72}, 32)),
		PolicyEpoch: "1", IssuerKeyID: ownerKeyID,
	}, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	offer := lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: "rl_AAAAAAAAAAAAAAAAAAAAAA",
		ArchiveID: grant.Grant.ArchiveID, SnapshotID: grant.Grant.SnapshotID,
		KeyEpoch:              "1",
		PlaintextManifestHash: "sha256:" + strings.Repeat("c", 64),
		ReplicaManifestHash:   "sha256:" + strings.Repeat("d", 64),
		EncryptedObjectSet: []lease.EncryptedObject{{
			ObjectID:       "ob_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x31}, 32)),
			CiphertextSize: "1", CiphertextSHA256: "sha256:" + strings.Repeat("e", 64),
		}},
		ProviderNodeID: info.NodeID, OwnerSubjectID: grant.Grant.SubjectID, ReservedBytes: "1",
		IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(30 * time.Minute).Format(time.RFC3339), RenewalPolicy: "manual",
	}
	signed, err := store.SignReplicaOffer(offer, []authority.SignedGrant{grant})
	if err != nil {
		t.Fatalf("sign offer with local Node Key: %v", err)
	}
	if signed.Actor != (authority.Principal{Kind: authority.PrincipalNode, ID: info.NodeID}) || signed.Offer.AuthorizedEvidence == "" {
		t.Fatalf("offer actor/evidence were not bound to the local Node: actor=%+v evidence=%q", signed.Actor, signed.Offer.AuthorizedEvidence)
	}
	canonical, err := lease.CanonicalOffer(signed.Offer)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(signed.Signature)
	publicKey := decodeNodePublicForTest(t, info.PublicKey)
	if err != nil || !ed25519.Verify(publicKey, append([]byte("IR/replica-lease/v1/offer\x00"), canonical...), signature) {
		t.Fatal("replica offer signature did not verify against the local public Node Key")
	}
	keyFile, err := os.ReadFile(filepath.Join(store.root, "security", fileName))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(keyFile, signature) {
		t.Fatal("emitted replica signature was persisted with Node Key material")
	}
}

func TestEnrollmentChallengeIsDurableBoundAndOneUse(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(); err != nil {
		t.Fatal(err)
	}
	rootSeed := bytes.Repeat([]byte{0x2a}, ed25519.SeedSize)
	rootKey := ed25519.NewKeyFromSeed(rootSeed)
	rootPublic := rootKey.Public().(ed25519.PublicKey)
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	previousHash := "sha256:" + strings.Repeat("ab", 32)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	proof, err := store.BeginEnrollmentProof(identityID, previousHash, now)
	if err != nil {
		t.Fatal(err)
	}
	if proof.IdentityID != identityID || proof.PreviousHash != previousHash || !proof.ExpiresAt.Equal(now.Add(enrollmentChallengeTTL)) {
		t.Fatalf("challenge context mismatch: %+v", proof)
	}
	if mode := fileModeAt(t, filepath.Join(root, "security", enrollmentChallengeFile)); mode != 0600 {
		t.Fatalf("enrollment challenge mode=%#o, want 0600", mode)
	}
	if _, err := store.BeginEnrollmentProof(identityID, previousHash, now); !errors.Is(err, ErrEnrollmentChallengeActive) {
		t.Fatalf("second active challenge was issued: %v", err)
	}
	canonical, signature := signDelegationEvent(t, rootKey, proof, now)
	eventHash, err := store.ValidateEnrollmentEvent(canonical, signature, now.Add(time.Minute))
	if err != nil || eventHash == "" {
		t.Fatalf("issued proof did not validate against signed delegation: hash=%q err=%v", eventHash, err)
	}
	if err := store.ConsumeEnrollmentEvent(canonical, signature, now.Add(time.Minute)); err != nil {
		t.Fatalf("consume challenge: %v", err)
	}
	if err := store.ConsumeEnrollmentEvent(canonical, signature, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("same accepted event replay must be idempotent: %v", err)
	}
	changed, changedSignature := signDelegationEvent(t, rootKey, proof, now.Add(time.Second))
	if _, err := store.ValidateEnrollmentEvent(changed, changedSignature, now.Add(2*time.Minute)); !errors.Is(err, ErrEnrollmentChallengeConsumed) {
		t.Fatalf("one-use nonce accepted a different event: %v", err)
	}
	next, err := store.BeginEnrollmentProof(identityID, previousHash, now.Add(2*time.Minute))
	if err != nil || next.Nonce == proof.Nonce {
		t.Fatalf("new ceremony after consumption was not issued: proof=%+v err=%v", next, err)
	}
}

func TestExpiredEnrollmentChallengeFailsClosedAfterRestart(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(); err != nil {
		t.Fatal(err)
	}
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x2b}, ed25519.SeedSize))
	identityID, err := identityprotocol.IdentityID(rootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	proof, err := store.BeginEnrollmentProof(identityID, "sha256:"+strings.Repeat("cd", 32), now)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature := signDelegationEvent(t, rootKey, proof, now)
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ValidateEnrollmentEvent(canonical, signature, now.Add(enrollmentChallengeTTL)); !errors.Is(err, ErrEnrollmentChallengeExpired) {
		t.Fatalf("expired enrollment proof accepted after restart: %v", err)
	}
}

func TestConcurrentEnrollmentEventConsumptionHasOneWinnerAcrossStores(t *testing.T) {
	root := t.TempDir()
	first, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Create(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x5c}, ed25519.SeedSize))
	identityID, err := identityprotocol.IdentityID(rootKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	previousHash := "sha256:" + strings.Repeat("ef", 32)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	proof, err := first.BeginEnrollmentProof(identityID, previousHash, now)
	if err != nil {
		t.Fatal(err)
	}
	canonicalA, signatureA := signDelegationEvent(t, rootKey, proof, now)
	canonicalB, signatureB := signDelegationEvent(t, rootKey, proof, now.Add(time.Second))
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- first.ConsumeEnrollmentEvent(canonicalA, signatureA, now.Add(2*time.Second))
	}()
	go func() {
		<-start
		results <- second.ConsumeEnrollmentEvent(canonicalB, signatureB, now.Add(2*time.Second))
	}()
	close(start)
	firstErr, secondErr := <-results, <-results
	if firstErr == nil && secondErr == nil || !errors.Is(firstErr, ErrEnrollmentChallengeConsumed) && !errors.Is(secondErr, ErrEnrollmentChallengeConsumed) {
		t.Fatalf("competing enrollment events must have exactly one winner: first=%v second=%v", firstErr, secondErr)
	}
	if firstErr != nil && !errors.Is(firstErr, ErrEnrollmentChallengeConsumed) || secondErr != nil && !errors.Is(secondErr, ErrEnrollmentChallengeConsumed) {
		t.Fatalf("unexpected consume result: first=%v second=%v", firstErr, secondErr)
	}
}

func signDelegationEvent(t *testing.T, rootKey ed25519.PrivateKey, proof EnrollmentProof, issuedAt time.Time) ([]byte, []byte) {
	t.Helper()
	rootPublic := rootKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         proof.IdentityID,
		"sequence":            "1",
		"previous_event_hash": proof.PreviousHash,
		"operation":           "node_delegate",
		"issued_at":           issuedAt.UTC().Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(rootPublic),
		"payload": map[string]any{
			"enrollment_nonce": proof.Nonce,
			"node_id":          proof.NodeID,
			"node_proof":       proof.Signature,
			"node_public_key":  proof.PublicKey,
		},
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func TestConcurrentNodeKeyCreationHasOneWinner(t *testing.T) {
	root := t.TempDir()
	stores := make([]*Store, 8)
	for i := range stores {
		store, err := Open(root)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
	}
	var wait sync.WaitGroup
	results := make(chan error, len(stores))
	for _, store := range stores {
		wait.Add(1)
		go func(store *Store) {
			defer wait.Done()
			_, err := store.Create()
			results <- err
		}(store)
	}
	wait.Wait()
	close(results)
	created, exists := 0, 0
	for err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, ErrExists):
			exists++
		default:
			t.Fatalf("unexpected create result: %v", err)
		}
	}
	if created != 1 || exists != len(stores)-1 {
		t.Fatalf("created=%d exists=%d", created, exists)
	}
}

func TestNodeKeyCorruptionFailsClosedWithoutReplacingKey(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "security", fileName)
	if err := os.WriteFile(path, []byte(`{"schema_version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt key state accepted: %v", err)
	}
	if _, err := store.Create(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt key was replaced: %v", err)
	}
}

func TestNodeKeyInsecureModeFailsClosed(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "security", fileName)
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("insecure private-key permissions accepted: %v", err)
	}
}

func TestArchiveDEKLocalProtectionSurvivesRestartAndBindsContext(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x01}, 16))
	if _, err := store.SealArchiveDEK(archiveID, 7, bytes.Repeat([]byte{0x6d}, 32)); !errors.Is(err, ErrNotCreated) {
		t.Fatalf("archive key protected before Node Key creation: %v", err)
	}
	if _, err := store.Create(); err != nil {
		t.Fatal(err)
	}
	dataKey := bytes.Repeat([]byte{0x6d}, 32)
	sealed, err := store.SealArchiveDEK(archiveID, 7, dataKey)
	if err != nil {
		t.Fatalf("seal archive key: %v", err)
	}
	if bytes.Contains(sealed, dataKey) {
		t.Fatal("sealed archive key contains the plaintext DEK")
	}
	if len(sealed) != 65 {
		t.Fatalf("sealed key size=%d, want 65", len(sealed))
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := reopened.OpenArchiveDEK(archiveID, 7, sealed)
	if err != nil || !bytes.Equal(opened, dataKey) {
		matched := bytes.Equal(opened, dataKey)
		clear(opened)
		t.Fatalf("reopen archive key: matches=%t err=%v", matched, err)
	}
	clear(opened)

	if _, err := reopened.OpenArchiveDEK(archiveID, 8, sealed); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("key epoch substitution accepted: %v", err)
	}
	otherArchiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x02}, 16))
	if _, err := reopened.OpenArchiveDEK(otherArchiveID, 7, sealed); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("archive substitution accepted: %v", err)
	}
	corrupt := append([]byte(nil), sealed...)
	corrupt[len(corrupt)-1] ^= 0x80
	if _, err := reopened.OpenArchiveDEK(archiveID, 7, corrupt); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("tampered key envelope accepted: %v", err)
	}

	otherRoot := t.TempDir()
	other, err := Open(otherRoot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Create(); err != nil {
		t.Fatal(err)
	}
	if _, err := other.OpenArchiveDEK(archiveID, 7, sealed); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("different Node Key opened archive key: %v", err)
	}
}

func fileModeAt(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
