package globalidentity

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestGenesisStoreIsSingleWriteAndPreservesExistingLocalAuth(t *testing.T) {
	root := t.TempDir()
	securityDir := filepath.Join(root, "security")
	if err := os.Mkdir(securityDir, 0700); err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(securityDir, "users.json")
	legacyBytes := []byte(`{"schema_version":2,"users":[{"user_id":"usr-rc12"}]}`)
	if err := os.WriteFile(legacyPath, legacyBytes, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot, exists, err := store.Snapshot(); err != nil || exists {
		t.Fatalf("opening the v2 identity store promoted an existing local account: snapshot=%+v exists=%v err=%v", snapshot, exists, err)
	}
	canonical, signature, userHandle, credential, challenge := testGenesis(t)

	const requests = 16
	var wg sync.WaitGroup
	results := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- store.CreateGenesis(canonical, signature, userHandle, credential, challenge)
		}()
	}
	wg.Wait()
	close(results)
	succeeded, rejected := 0, 0
	for result := range results {
		switch {
		case result == nil:
			succeeded++
		case errors.Is(result, ErrAlreadyInitialized):
			rejected++
		default:
			t.Fatalf("unexpected concurrent bootstrap result: %v", result)
		}
	}
	if succeeded != 1 || rejected != requests-1 {
		t.Fatalf("successes=%d rejected=%d; want one success and the rest rejected", succeeded, rejected)
	}

	snapshot, exists, err := store.Snapshot()
	if err != nil || !exists || snapshot.IdentityID == "" || snapshot.CredentialCount != 1 || len(snapshot.RecoveryEncryptionPublicKey) != 43 {
		t.Fatalf("snapshot=%+v exists=%v err=%v", snapshot, exists, err)
	}
	loadedID, loadedHandle, credentials, err := store.LoadForWebAuthn()
	if err != nil || loadedID != snapshot.IdentityID || string(loadedHandle) != string(userHandle) || len(credentials) != 1 || string(credentials[0].ID) != string(credential.ID) {
		t.Fatalf("loaded webauthn state id=%q handle=%x credentials=%d err=%v", loadedID, loadedHandle, len(credentials), err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if reopenedSnapshot, found, err := reopened.Snapshot(); err != nil || !found || reopenedSnapshot.IdentityID != snapshot.IdentityID || reopenedSnapshot.RecoveryEncryptionPublicKey != snapshot.RecoveryEncryptionPublicKey {
		t.Fatalf("reopened snapshot=%+v found=%v err=%v", reopenedSnapshot, found, err)
	}
	if got, err := os.ReadFile(legacyPath); err != nil || string(got) != string(legacyBytes) {
		t.Fatalf("existing local auth state changed: %s err=%v", got, err)
	}
}

func TestGenesisRecoveryEncryptionKeyMustBeUsableX25519PublicKey(t *testing.T) {
	challenge := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	base := map[string]any{
		"identity_format":    "global-identity/v1",
		"webauthn_challenge": challenge,
		"credential_id":      base64.RawURLEncoding.EncodeToString([]byte("credential")),
	}
	for name, key := range map[string][]byte{
		"zero low-order key": make([]byte, 32),
		"wrong length":       []byte{1, 2, 3},
	} {
		t.Run(name, func(t *testing.T) {
			payload := clonePayload(base)
			payload["recovery_encryption_public_key"] = base64.RawURLEncoding.EncodeToString(key)
			if validGenesisPayload(payload, "", "") {
				t.Fatal("unusable recovery encryption public key was accepted")
			}
		})
	}
	validPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	base["recovery_encryption_public_key"] = base64.RawURLEncoding.EncodeToString(validPrivate.PublicKey().Bytes())
	if !validGenesisPayload(base, "", "") {
		t.Fatal("valid X25519 recovery encryption key was rejected")
	}
}

func clonePayload(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func TestCredentialHistoryRequiresRecoverySignedLinearEvents(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, genesisSignature, userHandle, initialCredential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, genesisSignature, userHandle, initialCredential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, found, err := store.Snapshot()
	if err != nil || !found {
		t.Fatalf("initial snapshot: found=%v err=%v", found, err)
	}

	addedCredential := webauthn.Credential{ID: []byte("recovery-added-credential"), PublicKey: []byte("verified-public-key")}
	addCanonical, addSignature := signCredentialEvent(t, "credential_add", "1", &initial.HeadHash, initial.IdentityID, addedCredential.ID, challenge)
	if err := store.AddCredential(addCanonical, addSignature, challenge, addedCredential); err != nil {
		t.Fatalf("add recovery-signed credential: %v", err)
	}
	updated, _, err := store.Snapshot()
	if err != nil || updated.Sequence != "1" || updated.CredentialCount != 2 {
		t.Fatalf("updated snapshot=%+v err=%v", updated, err)
	}

	forkStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := forkStore.CreateGenesis(genesis, genesisSignature, userHandle, initialCredential, challenge); err != nil {
		t.Fatal(err)
	}
	forkStart, _, _ := forkStore.Snapshot()
	forkAdd, forkAddSignature := signCredentialEvent(t, "credential_add", "1", &forkStart.HeadHash, forkStart.IdentityID, addedCredential.ID, challenge)
	if err := forkStore.AddCredential(forkAdd, forkAddSignature, challenge, addedCredential); err != nil {
		t.Fatal(err)
	}
	badCanonical, badSignature := signCredentialEvent(t, "credential_revoke", "1", &forkStart.HeadHash, forkStart.IdentityID, initialCredential.ID, "")
	if err := forkStore.RevokeCredential(badCanonical, badSignature, base64.RawURLEncoding.EncodeToString(initialCredential.ID)); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("valid stale-head successor must freeze rather than be ignored: %v", err)
	}
	forked, _, err := forkStore.Snapshot()
	if err != nil || !forked.Frozen || forked.ForkCount != 1 {
		t.Fatalf("competing credential event was not durably frozen: %+v err=%v", forked, err)
	}

	revokeCanonical, revokeSignature := signCredentialEvent(t, "credential_revoke", "2", &updated.HeadHash, updated.IdentityID, initialCredential.ID, "")
	if err := store.RevokeCredential(revokeCanonical, revokeSignature, base64.RawURLEncoding.EncodeToString(initialCredential.ID)); err != nil {
		t.Fatalf("revoke prior credential: %v", err)
	}
	final, _, err := store.Snapshot()
	if err != nil || final.Sequence != "2" || final.CredentialCount != 1 || final.CredentialIDs[0] != base64.RawURLEncoding.EncodeToString(addedCredential.ID) {
		t.Fatalf("final snapshot=%+v err=%v", final, err)
	}

	finalRevokeEvent, finalRevokeSignature := signCredentialEventAndGet(t, "credential_revoke", "3", final.HeadHash, final.IdentityID, addedCredential.ID)
	if err := store.RevokeCredential(finalRevokeEvent, finalRevokeSignature, base64.RawURLEncoding.EncodeToString(addedCredential.ID)); err != nil {
		t.Fatalf("revoke final credential for offline-recovery-only state: %v", err)
	}
	if _, _, credentials, err := store.LoadForWebAuthn(); err != nil || len(credentials) != 0 {
		t.Fatalf("final passkey inventory after explicit revoke: credentials=%d err=%v", len(credentials), err)
	}
}

func TestCredentialRevocationInvalidatesGlobalIdentitySessions(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	snapshot, found, err := store.Snapshot()
	if err != nil || !found {
		t.Fatalf("snapshot: found=%v err=%v", found, err)
	}
	manager := NewCeremonyManager(store)
	manager.sessions["stale-session"] = identitySession{identityID: snapshot.IdentityID, historyHead: snapshot.HeadHash, csrfToken: "csrf", expiresAt: time.Now().Add(time.Hour)}
	event, eventSignature := signCredentialEventAndGet(t, "credential_revoke", "1", snapshot.HeadHash, snapshot.IdentityID, credential.ID)
	credentialID := base64.RawURLEncoding.EncodeToString(credential.ID)
	if err := manager.RevokeCredential(event, eventSignature, credentialID); err != nil {
		t.Fatalf("revoke credential: %v", err)
	}
	if _, _, authenticated := manager.Session("stale-session"); authenticated {
		t.Fatal("identity session remained active after credential revocation")
	}
}

func TestImportedCredentialRevocationInvalidatesGlobalIdentitySessions(t *testing.T) {
	owner, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	for _, store := range []*Store{owner, target} {
		if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
			t.Fatal(err)
		}
	}
	initial, _, err := target.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCeremonyManager(target)
	manager.sessions["active-session"] = identitySession{
		identityID: initial.IdentityID, historyHead: initial.HeadHash,
		csrfToken: "csrf", expiresAt: time.Now().Add(time.Hour),
	}
	if _, _, ok := manager.Session("active-session"); !ok {
		t.Fatal("valid session was rejected before importing a history update")
	}
	staleBundle, err := target.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ImportHistory(staleBundle, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := manager.Session("active-session"); !ok {
		t.Fatal("an idempotent same-head history import invalidated the session")
	}
	ownerInitial, _, err := owner.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	revoke, revokeSignature := signCredentialEventAndGet(t, "credential_revoke", "1", ownerInitial.HeadHash, ownerInitial.IdentityID, credential.ID)
	if err := owner.RevokeCredential(revoke, revokeSignature, base64.RawURLEncoding.EncodeToString(credential.ID)); err != nil {
		t.Fatal(err)
	}
	bundle, err := owner.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ImportHistory(bundle, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := manager.Session("active-session"); ok {
		t.Fatal("session authenticated under a stale history head after remote credential revocation")
	}
}

func TestNodeKeyDelegationAndRevocationAreRootAuthorizedAndPersistent(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	delegate, delegateSignature, nodeID := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x31}, 32))
	if err := store.ApplyNodeEvent(delegate, delegateSignature); err != nil {
		t.Fatalf("apply recovery-root-signed Node Key delegation: %v", err)
	}
	afterDelegate, _, err := store.Snapshot()
	if err != nil || len(afterDelegate.NodeIDs) != 1 || afterDelegate.NodeIDs[0] != nodeID || afterDelegate.Frozen {
		t.Fatalf("delegated node projection=%+v err=%v", afterDelegate, err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := reopened.Snapshot()
	if err != nil || len(loaded.NodeIDs) != 1 || loaded.NodeIDs[0] != nodeID {
		t.Fatalf("node delegation did not survive reopen: snapshot=%+v err=%v", loaded, err)
	}
	revoke, revokeSignature := signNodeRevoke(t, loaded.IdentityID, "2", loaded.HeadHash, nodeID)
	if err := reopened.ApplyNodeEvent(revoke, revokeSignature); err != nil {
		t.Fatalf("apply recovery-root-signed Node Key revocation: %v", err)
	}
	final, _, err := reopened.Snapshot()
	if err != nil || len(final.NodeIDs) != 0 || final.Sequence != "2" {
		t.Fatalf("revoked node projection=%+v err=%v", final, err)
	}
}

func TestPlaybackRecipientIsChallengeBoundAndConsumedOnce(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	manager := NewCeremonyManager(store)
	firstPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	secondPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, firstOptions, err := manager.BeginLogin("recorder.example", "https://recorder.example", firstPrivate.PublicKey().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	_, secondOptions, err := manager.BeginLogin("recorder.example", "https://recorder.example", secondPrivate.PublicKey().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(firstOptions.Response.Challenge, secondOptions.Response.Challenge) ||
		bytes.Contains(firstOptions.Response.Challenge, firstPrivate.PublicKey().Bytes()) ||
		bytes.Contains(secondOptions.Response.Challenge, secondPrivate.PublicKey().Bytes()) {
		t.Fatal("Passkey challenge did not bind the ephemeral key without embedding it")
	}

	snapshot, exists, err := store.Snapshot()
	if err != nil || !exists {
		t.Fatalf("identity snapshot exists=%t err=%v", exists, err)
	}
	manager.sessions["playback-session"] = identitySession{
		identityID: snapshot.IdentityID, historyHead: snapshot.HeadHash, csrfToken: "csrf",
		playbackPublicKey: append([]byte(nil), firstPrivate.PublicKey().Bytes()...), expiresAt: time.Now().Add(time.Hour),
	}
	if publicKey, ok := manager.SessionPlaybackPublicKey("playback-session"); !ok || !bytes.Equal(publicKey, firstPrivate.PublicKey().Bytes()) {
		t.Fatalf("session playback key read ok=%t match=%t", ok, bytes.Equal(publicKey, firstPrivate.PublicKey().Bytes()))
	}
	const readers = 8
	var wg sync.WaitGroup
	results := make(chan bool, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publicKey, ok := manager.TakeSessionPlaybackPublicKey("playback-session")
			results <- ok && bytes.Equal(publicKey, firstPrivate.PublicKey().Bytes())
			clear(publicKey)
		}()
	}
	wg.Wait()
	close(results)
	consumed := 0
	for ok := range results {
		if ok {
			consumed++
		}
	}
	if consumed != 1 {
		t.Fatalf("one-time playback recipient consumptions=%d, want 1", consumed)
	}
	if _, ok := manager.SessionPlaybackPublicKey("playback-session"); ok {
		t.Fatal("consumed playback recipient remained available")
	}
}

func TestConcurrentValidNodeSuccessorsPersistEvidenceAndFreezeIdentity(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, _ := store.Snapshot()
	first, firstSignature, _ := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x41}, 32))
	second, secondSignature, _ := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x42}, 32))
	manager := NewCeremonyManager(store)
	manager.sessions["identity-session"] = identitySession{identityID: initial.IdentityID, historyHead: initial.HeadHash, csrfToken: "csrf", expiresAt: time.Now().Add(time.Hour)}
	if err := store.ApplyNodeEvent(first, firstSignature); err != nil {
		t.Fatalf("first valid successor: %v", err)
	}
	if err := store.ApplyNodeEvent(second, secondSignature); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("second valid successor should freeze instead of selecting a winner: %v", err)
	}

	frozen, _, err := store.Snapshot()
	if err != nil || !frozen.Frozen || frozen.ForkCount != 1 || frozen.Sequence != "1" {
		t.Fatalf("fork snapshot=%+v err=%v", frozen, err)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, _, err := reopened.Snapshot()
	if err != nil || !afterRestart.Frozen || afterRestart.ForkCount != 1 || afterRestart.HeadHash != frozen.HeadHash {
		t.Fatalf("fork freeze/evidence did not survive restart: snapshot=%+v err=%v", afterRestart, err)
	}
	if _, _, authenticated := manager.Session("identity-session"); authenticated {
		t.Fatal("a global identity session remained authoritative after the history fork was frozen")
	}
	if _, _, err := manager.BeginLogin("recorder.example", "https://recorder.example"); !errors.Is(err, ErrIdentityFrozen) {
		t.Fatalf("new passkey login started while identity was frozen: %v", err)
	}
	blocked, blockedSignature := signCredentialEventAndGet(t, "credential_revoke", "2", afterRestart.HeadHash, afterRestart.IdentityID, credential.ID)
	if err := reopened.RevokeCredential(blocked, blockedSignature, base64.RawURLEncoding.EncodeToString(credential.ID)); !errors.Is(err, ErrIdentityFrozen) {
		t.Fatalf("sensitive identity mutation proceeded while forked: %v", err)
	}
}

func TestRecoveryRootForkResolutionSelectsExplicitBranchAndPersists(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first, firstSignature, firstNode := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x41}, 32))
	second, secondSignature, secondNode := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x42}, 32))
	if err := store.ApplyNodeEvent(first, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyNodeEvent(second, secondSignature); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("competing signed successor did not freeze: %v", err)
	}
	choices, err := store.ForkChoices()
	if err != nil || len(choices) != 1 {
		t.Fatalf("fork choices=%+v err=%v", choices, err)
	}
	choice := choices[0]
	beforeInvalid, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if choice.FirstHeadHash == choice.SecondHeadHash || choice.FirstSequence != "1" || choice.SecondSequence != "1" {
		t.Fatalf("fork choice did not expose both distinct signed heads: %+v", choice)
	}

	badHead := "sha256:" + strings.Repeat("f", 64)
	bad, badSignature := signIdentityEvent(t, rootRecoveryPrivateKey(t), initial.IdentityID, "2", &choice.SecondHeadHash, "fork_resolve", map[string]any{
		"fork_id": choice.ForkID, "previous_event_hash": choice.PreviousHash,
		"first_head_hash": choice.FirstHeadHash, "second_head_hash": badHead,
		"selected_head_hash": choice.SecondHeadHash,
	})
	if err := store.ApplyForkResolution(bad, badSignature); !errors.Is(err, ErrForkResolutionInvalid) {
		t.Fatalf("resolution naming an unobserved head accepted: %v", err)
	}
	stillFrozen, _, err := store.Snapshot()
	if err != nil || !stillFrozen.Frozen || stillFrozen.HeadHash != beforeInvalid.HeadHash || stillFrozen.Sequence != beforeInvalid.Sequence || stillFrozen.ForkCount != beforeInvalid.ForkCount {
		t.Fatalf("invalid resolution changed authority state: %+v err=%v", stillFrozen, err)
	}

	resolvedEvent, resolvedSignature := signIdentityEvent(t, rootRecoveryPrivateKey(t), initial.IdentityID, "2", &choice.SecondHeadHash, "fork_resolve", map[string]any{
		"fork_id": choice.ForkID, "previous_event_hash": choice.PreviousHash,
		"first_head_hash": choice.FirstHeadHash, "second_head_hash": choice.SecondHeadHash,
		"selected_head_hash": choice.SecondHeadHash,
	})
	if err := store.ApplyForkResolution(resolvedEvent, resolvedSignature); err != nil {
		t.Fatalf("apply root-approved branch selection: %v", err)
	}
	resolved, _, err := store.Snapshot()
	if err != nil || resolved.Frozen || resolved.ForkCount != 1 || resolved.Sequence != "2" || len(resolved.NodeIDs) != 1 || resolved.NodeIDs[0] != secondNode || firstNode == secondNode {
		t.Fatalf("resolved projection=%+v firstNode=%q secondNode=%q err=%v", resolved, firstNode, secondNode, err)
	}
	if err := store.ApplyForkResolution(resolvedEvent, resolvedSignature); err != nil {
		t.Fatalf("exact resolution retry should be idempotent: %v", err)
	}
	bundle, err := store.ExportHistory()
	if err != nil || len(bundle.ForkEvidence) != 1 {
		t.Fatalf("resolution discarded competing branch evidence: forks=%d err=%v", len(bundle.ForkEvidence), err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, _, err := reopened.Snapshot()
	if err != nil || afterRestart.Frozen || afterRestart.HeadHash != resolved.HeadHash || len(afterRestart.NodeIDs) != 1 || afterRestart.NodeIDs[0] != secondNode {
		t.Fatalf("resolved authority did not survive restart: %+v err=%v", afterRestart, err)
	}
	revoke, revokeSignature := signNodeRevoke(t, afterRestart.IdentityID, "3", afterRestart.HeadHash, secondNode)
	if err := reopened.ApplyNodeEvent(revoke, revokeSignature); err != nil {
		t.Fatalf("authorized mutations did not resume after explicit resolution: %v", err)
	}
	final, _, err := reopened.Snapshot()
	if err != nil || final.Frozen || len(final.NodeIDs) != 0 {
		t.Fatalf("post-resolution revoke projection=%+v err=%v", final, err)
	}
}

func TestRecoveryRootResolutionPreservesAndOrphansNestedForkEvidence(t *testing.T) {
	owner, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := owner.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, _ := owner.Snapshot()
	baseEvent, baseSignature, nodeID := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x51}, 32))
	if err := owner.ApplyNodeEvent(baseEvent, baseSignature); err != nil {
		t.Fatal(err)
	}
	base := mustExportHistory(t, owner)
	branchA, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	branchB, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := branchA.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	if err := branchB.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	baseHead, _, _ := owner.Snapshot()
	aEvent, aSignature, _ := signNodeDelegate(t, initial.IdentityID, "2", baseHead.HeadHash, bytes.Repeat([]byte{0x52}, 32))
	bEvent, bSignature, _ := signNodeDelegate(t, initial.IdentityID, "2", baseHead.HeadHash, bytes.Repeat([]byte{0x53}, 32))
	if err := branchA.ApplyNodeEvent(aEvent, aSignature); err != nil {
		t.Fatal(err)
	}
	if err := branchB.ApplyNodeEvent(bEvent, bSignature); err != nil {
		t.Fatal(err)
	}
	aHead, _, _ := branchA.Snapshot()
	aNestedFirst, aNestedFirstSignature, _ := signNodeDelegate(t, initial.IdentityID, "3", aHead.HeadHash, bytes.Repeat([]byte{0x54}, 32))
	aNestedSecond, aNestedSecondSignature, _ := signNodeDelegate(t, initial.IdentityID, "3", aHead.HeadHash, bytes.Repeat([]byte{0x55}, 32))
	if err := branchA.ApplyNodeEvent(aNestedFirst, aNestedFirstSignature); err != nil {
		t.Fatal(err)
	}
	if err := branchA.ApplyNodeEvent(aNestedSecond, aNestedSecondSignature); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("nested competing successor did not freeze: %v", err)
	}
	if err := branchA.ImportHistory(mustExportHistory(t, branchB), ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("ancestor fork was not retained: %v", err)
	}
	frozen, _, err := branchA.Snapshot()
	if err != nil || !frozen.Frozen || frozen.ForkCount != 2 {
		t.Fatalf("expected both ancestor and nested forks to freeze: %+v err=%v", frozen, err)
	}
	frozenBundle := mustExportHistory(t, branchA)
	peer, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.ImportHistory(frozenBundle, nodeID); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("peer did not retain nested fork evidence: %v", err)
	}
	choices, err := branchA.ForkChoices()
	if err != nil || len(choices) != 2 {
		t.Fatalf("expected explicit choices for both unresolved forks: choices=%+v err=%v", choices, err)
	}
	bHead, _, _ := branchB.Snapshot()
	var ancestorChoice *ForkChoice
	for index := range choices {
		if choices[index].PreviousHash == baseHead.HeadHash {
			ancestorChoice = &choices[index]
		}
	}
	if ancestorChoice == nil || bHead.HeadHash != ancestorChoice.FirstHeadHash && bHead.HeadHash != ancestorChoice.SecondHeadHash {
		t.Fatalf("ancestor fork choice=%+v branch B head=%s", ancestorChoice, bHead.HeadHash)
	}
	rootKey := rootRecoveryPrivateKey(t)
	resolution, resolutionSignature := signIdentityEvent(t, rootKey, initial.IdentityID, "3", &bHead.HeadHash, "fork_resolve", map[string]any{
		"fork_id": ancestorChoice.ForkID, "previous_event_hash": ancestorChoice.PreviousHash,
		"first_head_hash": ancestorChoice.FirstHeadHash, "second_head_hash": ancestorChoice.SecondHeadHash,
		"selected_head_hash": bHead.HeadHash,
	})
	if err := branchA.ApplyForkResolution(resolution, resolutionSignature); err != nil {
		t.Fatalf("explicitly selecting the other ancestor branch should orphan nested evidence safely: %v", err)
	}
	resolved, exists, err := branchA.Snapshot()
	if err != nil || !exists || resolved.Frozen || resolved.ForkCount != 2 || resolved.Sequence != "3" {
		t.Fatalf("ancestor choice did not preserve evidence while clearing discarded branch freeze: %+v exists=%v err=%v", resolved, exists, err)
	}
	remainingChoices, err := branchA.ForkChoices()
	if err != nil || len(remainingChoices) != 0 {
		t.Fatalf("discarded branch fork remained actionable: choices=%+v err=%v", remainingChoices, err)
	}
	bundle, err := branchA.ExportHistory()
	if err != nil || len(bundle.ForkEvidence) != 2 {
		t.Fatalf("explicit resolution discarded nested signed evidence: forks=%d err=%v", len(bundle.ForkEvidence), err)
	}
	if err := peer.ImportHistory(bundle, ""); err != nil {
		t.Fatalf("peer rejected root-approved ancestor resolution: %v", err)
	}
	peerSnapshot, exists, err := peer.Snapshot()
	if err != nil || !exists || peerSnapshot.Frozen || peerSnapshot.HeadHash != resolved.HeadHash || peerSnapshot.ForkCount != 2 {
		t.Fatalf("peer did not adopt explicitly resolved history: %+v exists=%v err=%v", peerSnapshot, exists, err)
	}
	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(frozenBundle); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("history registry did not freeze on retained nested evidence: %v", err)
	}
	if err := registry.Import(bundle); err != nil {
		t.Fatalf("history registry rejected signed ancestor resolution: %v", err)
	}
	projection, err := registry.AuthorityProjection(initial.IdentityID)
	registered, listErr := registry.List()
	if err != nil || listErr != nil || projection.Frozen || projection.HeadHash != resolved.HeadHash || len(registered) != 1 || registered[0].ForkCount != 2 {
		t.Fatalf("registry did not keep evidence while adopting resolved head: %+v err=%v", projection, err)
	}
	reopened, err := Open(branchA.root)
	if err != nil {
		t.Fatalf("resolved nested evidence failed restart validation: %v", err)
	}
	afterRestart, exists, err := reopened.Snapshot()
	if err != nil || !exists || afterRestart.Frozen || afterRestart.HeadHash != resolved.HeadHash || afterRestart.ForkCount != 2 {
		t.Fatalf("resolved nested evidence changed across restart: %+v exists=%v err=%v", afterRestart, exists, err)
	}
}

func TestNodeSuccessorForksWithAcceptedCredentialSuccessorWithoutSelectingEither(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	added := webauthn.Credential{ID: []byte("parallel-registration"), PublicKey: []byte("credential-public-key")}
	credentialEvent, credentialSignature := signCredentialEvent(t, "credential_add", "1", &initial.HeadHash, initial.IdentityID, added.ID, challenge)
	if err := store.AddCredential(credentialEvent, credentialSignature, challenge, added); err != nil {
		t.Fatal(err)
	}
	delegation, delegationSignature, _ := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x62}, 32))
	if err := store.ApplyNodeEvent(delegation, delegationSignature); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("valid cross-operation successor fork was not frozen: %v", err)
	}
	frozen, exists, err := store.Snapshot()
	if err != nil || !exists || !frozen.Frozen || frozen.ForkCount != 1 || frozen.Sequence != "1" {
		t.Fatalf("competing identity successors were selected or lost: snapshot=%+v exists=%v err=%v", frozen, exists, err)
	}
	if _, _, err := NewCeremonyManager(store).BeginRegistration("recorder.example", "https://recorder.example", frozen.IdentityID); !errors.Is(err, ErrIdentityFrozen) {
		t.Fatalf("passkey registration continued while the identity history was frozen: %v", err)
	}
}

func TestNodeDelegationRejectsBadProofAndNonRootSigner(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, _ := store.Snapshot()
	badProof, badProofSignature, _ := signNodeDelegateWithSigner(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x51}, 32), nil, bytes.Repeat([]byte{0x9a}, ed25519.SignatureSize))
	if err := store.ApplyNodeEvent(badProof, badProofSignature); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("delegation without Node Key proof of possession accepted: %v", err)
	}
	nonRoot, nonRootSignature, _ := signNodeDelegateWithSigner(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x52}, 32), bytes.Repeat([]byte{0x77}, 32))
	if err := store.ApplyNodeEvent(nonRoot, nonRootSignature); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("delegation signed by untrusted key accepted: %v", err)
	}
	unchanged, _, err := store.Snapshot()
	if err != nil || unchanged.Sequence != "0" || len(unchanged.NodeIDs) != 0 || unchanged.Frozen {
		t.Fatalf("rejected event changed identity state: snapshot=%+v err=%v", unchanged, err)
	}
}

func TestHistoryBundleJoinsDelegatedNodeAndSyncsWithoutCredentialExport(t *testing.T) {
	owner, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, ownerCredential, challenge := testGenesis(t)
	if err := owner.CreateGenesis(genesis, signature, userHandle, ownerCredential, challenge); err != nil {
		t.Fatal(err)
	}
	ownerSnapshot, _, err := owner.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	delegation, delegationSignature, nodeID := signNodeDelegate(t, ownerSnapshot.IdentityID, "1", ownerSnapshot.HeadHash, bytes.Repeat([]byte{0x71}, 32))
	if err := owner.ApplyNodeEvent(delegation, delegationSignature); err != nil {
		t.Fatal(err)
	}
	bundle, err := owner.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	serialized, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, privateMaterial := range []string{"test-credential-public-key", "user_handle", "credential_inventory_mode"} {
		if bytes.Contains(serialized, []byte(privateMaterial)) {
			t.Fatalf("history export included local credential material %q", privateMaterial)
		}
	}

	joiningNode, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := joiningNode.ImportHistory(bundle, nodeID); err != nil {
		t.Fatalf("first import for delegated node: %v", err)
	}
	joined, found, err := joiningNode.Snapshot()
	if err != nil || !found || joined.IdentityID != ownerSnapshot.IdentityID || len(joined.NodeIDs) != 1 || joined.NodeIDs[0] != nodeID || joined.CredentialCount != 0 {
		t.Fatalf("joined node projection=%+v found=%v err=%v", joined, found, err)
	}
	if _, _, credentials, err := joiningNode.LoadForWebAuthn(); err != nil || len(credentials) != 0 {
		t.Fatalf("local WebAuthn credentials were unexpectedly imported: count=%d err=%v", len(credentials), err)
	}

	localChallengeCredential := webauthn.Credential{ID: []byte("second-node-passkey"), PublicKey: []byte("second-node-public-key")}
	credentialEvent, credentialSignature := signCredentialEvent(t, "credential_add", "2", &joined.HeadHash, joined.IdentityID, localChallengeCredential.ID, challenge)
	if err := joiningNode.AddCredential(credentialEvent, credentialSignature, challenge, localChallengeCredential); err != nil {
		t.Fatalf("append local credential after identity import: %v", err)
	}
	updatedBundle, err := joiningNode.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.ImportHistory(updatedBundle, ""); err != nil {
		t.Fatalf("sync appended public event back to owner: %v", err)
	}
	updatedOwner, _, err := owner.Snapshot()
	if err != nil || updatedOwner.Sequence != "2" || updatedOwner.NodeIDs[0] != nodeID || updatedOwner.CredentialCount != 1 {
		t.Fatalf("owner did not preserve local auth while advancing log: snapshot=%+v err=%v", updatedOwner, err)
	}
	if err := owner.ImportHistory(bundle, ""); err != nil {
		t.Fatalf("stale history replay should be harmless: %v", err)
	}
	afterStaleReplay, _, err := owner.Snapshot()
	if err != nil || afterStaleReplay.HeadHash != updatedOwner.HeadHash || afterStaleReplay.Sequence != updatedOwner.Sequence {
		t.Fatalf("stale history rolled back identity head: snapshot=%+v err=%v", afterStaleReplay, err)
	}
}

func TestDivergentImportedHistoriesFreezeBothNodes(t *testing.T) {
	first, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := first.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	genesisSnapshot, _, _ := first.Snapshot()
	baseEvent, baseSignature, secondNodeID := signNodeDelegate(t, genesisSnapshot.IdentityID, "1", genesisSnapshot.HeadHash, bytes.Repeat([]byte{0x72}, 32))
	if err := first.ApplyNodeEvent(baseEvent, baseSignature); err != nil {
		t.Fatal(err)
	}
	baseBundle, err := first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ImportHistory(baseBundle, secondNodeID); err != nil {
		t.Fatal(err)
	}
	baseSnapshot, _, _ := first.Snapshot()
	firstFork, firstForkSignature, _ := signNodeDelegate(t, baseSnapshot.IdentityID, "2", baseSnapshot.HeadHash, bytes.Repeat([]byte{0x73}, 32))
	secondFork, secondForkSignature, _ := signNodeDelegate(t, baseSnapshot.IdentityID, "2", baseSnapshot.HeadHash, bytes.Repeat([]byte{0x74}, 32))
	if err := first.ApplyNodeEvent(firstFork, firstForkSignature); err != nil {
		t.Fatal(err)
	}
	if err := second.ApplyNodeEvent(secondFork, secondForkSignature); err != nil {
		t.Fatal(err)
	}
	secondBundle, err := second.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ImportHistory(secondBundle, ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("divergent valid histories did not freeze first node: %v", err)
	}
	firstFrozen, _, err := first.Snapshot()
	if err != nil || !firstFrozen.Frozen || firstFrozen.ForkCount != 1 {
		t.Fatalf("first node did not retain fork evidence: snapshot=%+v err=%v", firstFrozen, err)
	}
	firstBundle, err := first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ImportHistory(firstBundle, ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("fork evidence did not propagate to second node: %v", err)
	}
	secondFrozen, _, err := second.Snapshot()
	if err != nil || !secondFrozen.Frozen || secondFrozen.ForkCount != 1 {
		t.Fatalf("second node did not freeze on received fork evidence: snapshot=%+v err=%v", secondFrozen, err)
	}
	joining, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := joining.ImportHistory(firstBundle, secondNodeID); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("first import of a frozen public history was reported as success: %v", err)
	}
	joined, exists, err := joining.Snapshot()
	if err != nil || !exists || !joined.Frozen || joined.ForkCount != 1 {
		t.Fatalf("first-import fork evidence was not durable: snapshot=%+v exists=%v err=%v", joined, exists, err)
	}
}

func TestCompetingGenesisForSameRecoveryIdentityFreezesInsteadOfChoosing(t *testing.T) {
	seed := bytes.Repeat([]byte{0x33}, ed25519.SeedSize)
	canonicalA, signatureA, handleA, credentialA, challengeA := testGenesisWithSeed(t, seed)
	canonicalB, signatureB, handleB, credentialB, challengeB := testGenesisWithSeed(t, seed)
	first, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.CreateGenesis(canonicalA, signatureA, handleA, credentialA, challengeA); err != nil {
		t.Fatal(err)
	}
	if err := second.CreateGenesis(canonicalB, signatureB, handleB, credentialB, challengeB); err != nil {
		t.Fatal(err)
	}
	firstSnapshot, _, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	secondBundle, err := second.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ImportHistory(secondBundle, ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("competing valid Genesis did not freeze: %v", err)
	}
	frozen, exists, err := first.Snapshot()
	if err != nil || !exists || !frozen.Frozen || frozen.ForkCount != 1 || frozen.Sequence != "0" || frozen.HeadHash != firstSnapshot.HeadHash {
		t.Fatalf("genesis fork was selected or not retained: snapshot=%+v exists=%v err=%v", frozen, exists, err)
	}
	if _, err := first.ExportHistory(); err != nil {
		t.Fatalf("frozen genesis history could not be exported: %v", err)
	}
}

func TestLegacyIdentitySchemaMigratesToLocalCredentialInventoryV2(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	path := store.storePath()
	var prior map[string]any
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &prior) != nil {
		t.Fatalf("read legacy identity fixture: %v", err)
	}
	prior["schema_version"] = float64(1)
	delete(prior, "credential_inventory_mode")
	legacy, err := json.Marshal(prior)
	if err != nil || os.WriteFile(path, legacy, 0600) != nil {
		t.Fatalf("write schema v1 fixture: %v", err)
	}
	if _, err := Open(root); err != nil {
		t.Fatalf("open did not migrate prior complete local credential inventory: %v", err)
	}
	var migrated map[string]any
	data, err = os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &migrated) != nil || migrated["schema_version"] != float64(SchemaVersion) || migrated["credential_inventory_mode"] != "complete" {
		t.Fatalf("durable schema migration mismatch: %s err=%v", data, err)
	}
	_, _, credentials, err := store.LoadForWebAuthn()
	if err != nil || len(credentials) != 1 || !bytes.Equal(credentials[0].ID, credential.ID) {
		t.Fatalf("migration lost local credential: count=%d err=%v", len(credentials), err)
	}
}

func TestGenesisStoreFailClosedOnCorruptionAndUnsafePermissions(t *testing.T) {
	for _, mode := range []os.FileMode{0600, 0644} {
		t.Run(mode.String(), func(t *testing.T) {
			root := t.TempDir()
			store, err := Open(root)
			if err != nil {
				t.Fatal(err)
			}
			canonical, signature, userHandle, credential, challenge := testGenesis(t)
			if err := store.CreateGenesis(canonical, signature, userHandle, credential, challenge); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "security", storeFilename)
			if mode == 0644 {
				if err := os.Chmod(path, mode); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte(`{"schema_version":99}`), mode); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(root); !errors.Is(err, ErrCorruptStore) {
				t.Fatalf("unsafe/corrupt state opened: %v", err)
			}
		})
	}
}

func TestGenesisStoreRejectsWrongChallengeOrSignatureWithoutCreatingState(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(canonical, signature, userHandle, credential, challenge+"x"); !errors.Is(err, ErrInvalidGenesis) {
		t.Fatalf("wrong challenge accepted: %v", err)
	}
	signature[0] ^= 1
	if err := store.CreateGenesis(canonical, signature, userHandle, credential, challenge); !errors.Is(err, ErrInvalidGenesis) {
		t.Fatalf("invalid signature accepted: %v", err)
	}
	if _, exists, err := store.Snapshot(); err != nil || exists {
		t.Fatalf("failed bootstrap changed state: exists=%v err=%v", exists, err)
	}
}

func testGenesis(t *testing.T) ([]byte, []byte, []byte, webauthn.Credential, string) {
	t.Helper()
	seed, err := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	if err != nil {
		t.Fatal(err)
	}
	return testGenesisWithSeed(t, seed)
}

func testGenesisWithSeed(t *testing.T, seed []byte) ([]byte, []byte, []byte, webauthn.Credential, string) {
	t.Helper()
	if len(seed) != ed25519.SeedSize {
		t.Fatalf("identity seed length = %d", len(seed))
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	identityID, err := identityprotocol.IdentityID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	challengeBytes := make([]byte, 32)
	if _, err := rand.Read(challengeBytes); err != nil {
		t.Fatal(err)
	}
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes)
	encryptionPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encryptionPublic := base64.RawURLEncoding.EncodeToString(encryptionPrivate.PublicKey().Bytes())
	event := map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         identityID,
		"sequence":            "0",
		"previous_event_hash": nil,
		"operation":           "genesis",
		"issued_at":           time.Now().UTC().Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(publicKey),
		"payload": map[string]any{
			"identity_format":                "global-identity/v1",
			"recovery_encryption_public_key": encryptionPublic,
			"webauthn_challenge":             challenge,
			"credential_id":                  base64.RawURLEncoding.EncodeToString([]byte("test-credential-id")),
		},
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	userHandle := make([]byte, 32)
	if _, err := rand.Read(userHandle); err != nil {
		t.Fatal(err)
	}
	credential := webauthn.Credential{ID: []byte("test-credential-id"), PublicKey: []byte("test-credential-public-key")}
	return canonical, signature, userHandle, credential, challenge
}

func signCredentialEvent(t *testing.T, operation, sequence string, previous *string, identityID string, credentialID []byte, challenge string) ([]byte, []byte) {
	t.Helper()
	seed, _ := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"event_version":       identityprotocol.EventVersion,
		"identity_id":         identityID,
		"sequence":            sequence,
		"previous_event_hash": previous,
		"operation":           operation,
		"issued_at":           time.Now().UTC().Format(time.RFC3339),
		"signer_key_id":       keyID,
		"signer_public_key":   base64.RawURLEncoding.EncodeToString(publicKey),
		"payload": func() map[string]any {
			payload := map[string]any{"credential_id": base64.RawURLEncoding.EncodeToString(credentialID)}
			if operation == "credential_add" {
				payload["webauthn_challenge"] = challenge
			}
			return payload
		}(),
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func signCredentialEventAndGet(t *testing.T, operation, sequence, previous, identityID string, credentialID []byte) ([]byte, []byte) {
	t.Helper()
	canonical, signature := signCredentialEvent(t, operation, sequence, &previous, identityID, credentialID, "")
	return canonical, signature
}

func signNodeDelegate(t *testing.T, identityID, sequence, previous string, seed []byte) ([]byte, []byte, string) {
	return signNodeDelegateWithSigner(t, identityID, sequence, previous, seed, nil)
}

func signNodeDelegateWithSigner(t *testing.T, identityID, sequence, previous string, nodeSeed, rootSeed []byte, forcedProof ...[]byte) ([]byte, []byte, string) {
	t.Helper()
	nodeKey := ed25519.NewKeyFromSeed(nodeSeed)
	nodePublic := nodeKey.Public().(ed25519.PublicKey)
	nodeID, err := identityprotocol.NodeID(nodePublic)
	if err != nil {
		t.Fatal(err)
	}
	nonce := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x61}, 32))
	message, err := identityprotocol.NodeEnrollmentProofMessage(identityID, previous, nonce)
	if err != nil {
		t.Fatal(err)
	}
	proof := ed25519.Sign(nodeKey, message)
	if len(forcedProof) > 0 {
		proof = forcedProof[0]
	}
	if len(rootSeed) == 0 {
		rootSeed, _ = hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	}
	rootKey := ed25519.NewKeyFromSeed(rootSeed)
	canonical, signature := signIdentityEvent(t, rootKey, identityID, sequence, &previous, "node_delegate", map[string]any{
		"node_id": nodeID, "node_public_key": base64.RawURLEncoding.EncodeToString(nodePublic),
		"enrollment_nonce": nonce, "node_proof": base64.RawURLEncoding.EncodeToString(proof),
	})
	return canonical, signature, nodeID
}

func signNodeRevoke(t *testing.T, identityID, sequence, previous, nodeID string) ([]byte, []byte) {
	seed, _ := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	return signIdentityEvent(t, ed25519.NewKeyFromSeed(seed), identityID, sequence, &previous, "node_revoke", map[string]any{"node_id": nodeID})
}

func signIdentityEvent(t *testing.T, privateKey ed25519.PrivateKey, identityID, sequence string, previous *string, operation string, payload map[string]any) ([]byte, []byte) {
	t.Helper()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID, err := identityprotocol.KeyID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID, "sequence": sequence,
		"previous_event_hash": previous, "operation": operation, "issued_at": time.Now().UTC().Format(time.RFC3339),
		"signer_key_id": keyID, "signer_public_key": base64.RawURLEncoding.EncodeToString(publicKey), "payload": payload,
	}
	input, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(input, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func rootRecoveryPrivateKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	seed, err := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	if err != nil {
		t.Fatal(err)
	}
	return ed25519.NewKeyFromSeed(seed)
}
