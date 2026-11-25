package globalidentity

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func createRegistryFixtureWithChallenge(t *testing.T, root string, seed byte) (*Store, HistoryBundle) {
	t.Helper()
	canonical, signature, userHandle, credential, challenge := testGenesisWithSeed(t, bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateGenesis(canonical, signature, userHandle, credential, challenge); err != nil {
		t.Fatalf("create fixture identity: %v", err)
	}
	bundle, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	return store, bundle
}

func appendRegistryNodeEvent(t *testing.T, store *Store, rootSeed, nodeSeed byte) {
	t.Helper()
	snapshot, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := strconv.Atoi(snapshot.Sequence)
	if err != nil {
		t.Fatalf("parse identity sequence %q: %v", snapshot.Sequence, err)
	}
	canonical, signature, _ := signNodeDelegateWithSigner(
		t, snapshot.IdentityID, strconv.Itoa(sequence+1), snapshot.HeadHash,
		bytes.Repeat([]byte{nodeSeed}, ed25519.SeedSize), bytes.Repeat([]byte{rootSeed}, ed25519.SeedSize),
	)
	if err := store.ApplyNodeEvent(canonical, signature); err != nil {
		t.Fatalf("append signed node event at sequence %d: %v", sequence+1, err)
	}
}

func TestHistoryRegistryKeepsMultiplePublicIdentitiesWithoutLocalCredentials(t *testing.T) {
	root := t.TempDir()
	registry, err := OpenHistoryRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	_, first := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x31)
	_, second := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x32)
	if first.IdentityID == second.IdentityID {
		t.Fatal("fixtures unexpectedly share an identity ID")
	}
	for _, bundle := range []HistoryBundle{first, second} {
		if err := registry.Import(bundle); err != nil {
			t.Fatalf("import %s: %v", bundle.IdentityID, err)
		}
		if err := registry.Import(bundle); err != nil {
			t.Fatalf("idempotent re-import %s: %v", bundle.IdentityID, err)
		}
	}
	identities, err := registry.List()
	if err != nil || len(identities) != 2 {
		t.Fatalf("registered identities=%+v err=%v", identities, err)
	}
	for _, identity := range identities {
		if identity.Frozen || identity.Sequence != "0" || identity.HeadHash == "" {
			t.Fatalf("unexpected projection: %+v", identity)
		}
	}

	path := filepath.Join(root, "security", historyRegistryFile)
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("registry permissions=%v err=%v", info, err)
	}
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{
		[]byte("test-credential-public-key"),
		[]byte("user_handle"),
		[]byte("credential_inventory_mode"),
		[]byte("recovery_signing_private_key"),
	} {
		if bytes.Contains(persisted, forbidden) {
			t.Fatalf("public registry persisted local/private material %q", forbidden)
		}
	}

	reopened, err := OpenHistoryRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	reopenedIdentities, err := reopened.List()
	if err != nil || len(reopenedIdentities) != 2 {
		t.Fatalf("reopened registry identities=%+v err=%v", reopenedIdentities, err)
	}
}

func TestHistoryRegistryRejectsInvalidSignatureWithoutMutation(t *testing.T) {
	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, bundle := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x41)
	signature, err := base64.RawURLEncoding.DecodeString(bundle.Events[0].Signature)
	if err != nil {
		t.Fatal(err)
	}
	signature[0] ^= 1
	bundle.Events[0].Signature = base64.RawURLEncoding.EncodeToString(signature)
	if err := registry.Import(bundle); !errors.Is(err, ErrHistoryInvalid) {
		t.Fatalf("invalid identity signature accepted: %v", err)
	}
	identities, err := registry.List()
	if err != nil || len(identities) != 0 {
		t.Fatalf("invalid import changed registry: identities=%+v err=%v", identities, err)
	}
}

func TestHistoryRegistryFreezesCompetingValidGenesisEvents(t *testing.T) {
	_, first := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x45)
	_, second := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x45)
	if first.IdentityID != second.IdentityID {
		t.Fatal("same recovery root did not reproduce identity ID")
	}
	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(second); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("competing valid genesis did not freeze: %v", err)
	}
	projection, err := registry.AuthorityProjection(first.IdentityID)
	if err != nil || !projection.Frozen {
		t.Fatalf("genesis fork projection=%+v err=%v", projection, err)
	}
	bundle, found, err := registry.Get(first.IdentityID)
	if err != nil || !found || len(bundle.ForkEvidence) != 1 || bundle.ForkEvidence[0].PreviousHash != "" {
		t.Fatalf("genesis fork evidence=%+v found=%v err=%v", bundle.ForkEvidence, found, err)
	}
}

func TestHistoryRegistryRejectsRollbackAndRetainsVerifiedHead(t *testing.T) {
	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, initial := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x51)
	if err := registry.Import(initial); err != nil {
		t.Fatal(err)
	}
	initialSnapshot, _, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	delegation, signature, _ := signNodeDelegateWithSigner(t, initialSnapshot.IdentityID, "1", initialSnapshot.HeadHash, bytes.Repeat([]byte{0x61}, ed25519.SeedSize), bytes.Repeat([]byte{0x51}, ed25519.SeedSize))
	if err := store.ApplyNodeEvent(delegation, signature); err != nil {
		t.Fatal(err)
	}
	advanced, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(advanced); err != nil {
		t.Fatalf("import advanced history: %v", err)
	}
	if err := registry.Import(initial); !errors.Is(err, ErrHistoryStale) {
		t.Fatalf("stale history was not identified: %v", err)
	}
	projection, err := registry.AuthorityProjection(initial.IdentityID)
	if err != nil || projection.HeadHash != identityprotocol.EventHash(mustDecodeCanonical(t, advanced.Events[1])) || len(projection.ActiveNodes) != 1 {
		t.Fatalf("registry head rolled back: projection=%+v err=%v", projection, err)
	}
	beforeReplay, err := registry.AuthorityProjection(initial.IdentityID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(advanced); err != nil {
		t.Fatalf("idempotent current history import: %v", err)
	}
	afterReplay, err := registry.AuthorityProjection(initial.IdentityID)
	if err != nil || !afterReplay.VerifiedAt.Equal(beforeReplay.VerifiedAt) {
		t.Fatalf("idempotent replay refreshed verified-at: before=%s after=%s err=%v", beforeReplay.VerifiedAt, afterReplay.VerifiedAt, err)
	}
}

func TestHistoryRegistryFreezesAndPersistsCompetingValidSuccessors(t *testing.T) {
	firstStore, _ := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x71)
	firstGenesis, _, err := firstStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sharedDelegate, sharedSignature, secondNodeID := signNodeDelegateWithSigner(t, firstGenesis.IdentityID, "1", firstGenesis.HeadHash, bytes.Repeat([]byte{0x72}, ed25519.SeedSize), bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	if err := firstStore.ApplyNodeEvent(sharedDelegate, sharedSignature); err != nil {
		t.Fatal(err)
	}
	baseBundle, err := firstStore.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := secondStore.ImportHistory(baseBundle, secondNodeID); err != nil {
		t.Fatalf("initialize second branch from shared signed history: %v", err)
	}
	sharedHead, _, err := secondStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	firstEvent, firstSignature, _ := signNodeDelegateWithSigner(t, firstGenesis.IdentityID, "2", sharedHead.HeadHash, bytes.Repeat([]byte{0x73}, ed25519.SeedSize), bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	secondEvent, secondSignature, _ := signNodeDelegateWithSigner(t, firstGenesis.IdentityID, "2", sharedHead.HeadHash, bytes.Repeat([]byte{0x74}, ed25519.SeedSize), bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	if err := firstStore.ApplyNodeEvent(firstEvent, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := secondStore.ApplyNodeEvent(secondEvent, secondSignature); err != nil {
		t.Fatal(err)
	}
	firstBranch, err := firstStore.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	secondBranch, err := secondStore.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	registry, err := OpenHistoryRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(baseBundle); err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(firstBranch); err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(secondBranch); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("competing valid successor did not freeze: %v", err)
	}
	projection, err := registry.AuthorityProjection(firstBranch.IdentityID)
	if err != nil || !projection.Frozen {
		t.Fatalf("forked projection not frozen: projection=%+v err=%v", projection, err)
	}
	retained, found, err := registry.Get(firstBranch.IdentityID)
	if err != nil || !found || len(retained.ForkEvidence) != 1 {
		t.Fatalf("fork evidence not retained: found=%v forks=%d err=%v", found, len(retained.ForkEvidence), err)
	}
	if err := registry.Import(secondBranch); !errors.Is(err, ErrIdentityFrozen) {
		t.Fatalf("new branch must remain rejected after freeze: %v", err)
	}
	reopened, err := OpenHistoryRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	projection, err = reopened.AuthorityProjection(firstBranch.IdentityID)
	if err != nil || !projection.Frozen {
		t.Fatalf("fork freeze did not survive reopen: projection=%+v err=%v", projection, err)
	}
	if err := reopened.Import(firstBranch); err != nil {
		t.Fatalf("known canonical branch replay should be idempotent: %v", err)
	}
}

func TestHistoryRegistryFrozenForkCanLearnVerifiedLongerBranchEvidence(t *testing.T) {
	rootSeed := byte(0x31)
	first, base := createRegistryFixtureWithChallenge(t, t.TempDir(), rootSeed)
	genesis, _, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	shared, sharedSig, nodeID := signNodeDelegateWithSigner(t, genesis.IdentityID, "1", genesis.HeadHash,
		bytes.Repeat([]byte{0x32}, ed25519.SeedSize), bytes.Repeat([]byte{rootSeed}, ed25519.SeedSize))
	if err := first.ApplyNodeEvent(shared, sharedSig); err != nil {
		t.Fatal(err)
	}
	base, err = first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	appendRegistryNodeEvent(t, first, rootSeed, 0x33)
	appendRegistryNodeEvent(t, first, rootSeed, 0x34)
	appendRegistryNodeEvent(t, second, rootSeed, 0x35)

	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(base); err != nil {
		t.Fatal(err)
	}
	firstBranch, err := first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(firstBranch); err != nil {
		t.Fatal(err)
	}
	shortSecondBranch, err := second.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(shortSecondBranch); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("competing valid branch did not freeze registry: %v", err)
	}
	shortEvidence, found, err := registry.Get(firstBranch.IdentityID)
	if err != nil || !found || len(shortEvidence.ForkEvidence) != 1 {
		t.Fatalf("initial fork evidence=%+v found=%v err=%v", shortEvidence.ForkEvidence, found, err)
	}
	if len(shortEvidence.ForkEvidence[0].FirstBranch) != 2 || len(shortEvidence.ForkEvidence[0].SecondBranch) != 1 {
		t.Fatalf("unexpected initial branch lengths: first=%d second=%d", len(shortEvidence.ForkEvidence[0].FirstBranch), len(shortEvidence.ForkEvidence[0].SecondBranch))
	}

	appendRegistryNodeEvent(t, second, rootSeed, 0x36)
	longSecondBranch, err := second.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ImportHistory(longSecondBranch, ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("frozen local store did not retain verified branch evidence: %v", err)
	}
	enriched, err := first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(enriched); err != nil {
		t.Fatalf("frozen registry rejected a valid prefix extension of branch evidence: %v", err)
	}
	projection, err := registry.AuthorityProjection(firstBranch.IdentityID)
	firstHead := identityprotocol.EventHash(mustDecodeCanonical(t, firstBranch.Events[len(firstBranch.Events)-1]))
	if err != nil || !projection.Frozen || projection.HeadHash != firstHead {
		t.Fatalf("evidence enrichment changed frozen authority state: projection=%+v err=%v", projection, err)
	}
	retained, found, err := registry.Get(firstBranch.IdentityID)
	if err != nil || !found || len(retained.ForkEvidence) != 1 || len(retained.ForkEvidence[0].FirstBranch) != 2 || len(retained.ForkEvidence[0].SecondBranch) != 2 {
		t.Fatalf("longer signed branch evidence was not retained: evidence=%+v found=%v err=%v", retained.ForkEvidence, found, err)
	}
	if err := registry.Import(enriched); err != nil {
		t.Fatalf("enriched evidence replay was not idempotent: %v", err)
	}
	if err := second.ImportHistory(enriched, ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("peer failed to retain enriched fork evidence with reversed branch orientation: %v", err)
	}
	peerEvidence, err := second.ExportHistory()
	if err != nil || len(peerEvidence.ForkEvidence) != 1 || len(peerEvidence.ForkEvidence[0].FirstBranch) != 2 || len(peerEvidence.ForkEvidence[0].SecondBranch) != 2 {
		t.Fatalf("branch orientation was not preserved across peer import: evidence=%+v err=%v", peerEvidence.ForkEvidence, err)
	}
	corrupted := cloneBundle(enriched)
	signature, err := base64.RawURLEncoding.DecodeString(corrupted.ForkEvidence[0].SecondBranch[1].Signature)
	if err != nil {
		t.Fatal(err)
	}
	signature[0] ^= 0x80
	corrupted.ForkEvidence[0].SecondBranch[1].Signature = base64.RawURLEncoding.EncodeToString(signature)
	if err := registry.Import(corrupted); !errors.Is(err, ErrHistoryInvalid) {
		t.Fatalf("tampered branch evidence was accepted: %v", err)
	}
	afterTamper, found, err := registry.Get(firstBranch.IdentityID)
	if err != nil || !found || afterTamper.ForkEvidence[0].SecondBranch[1].Signature != retained.ForkEvidence[0].SecondBranch[1].Signature {
		t.Fatalf("invalid branch import mutated retained evidence: evidence=%+v found=%v err=%v", afterTamper.ForkEvidence, found, err)
	}
	reopened, err := OpenHistoryRegistry(registry.root)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart, found, err := reopened.Get(firstBranch.IdentityID)
	if err != nil || !found || len(afterRestart.ForkEvidence[0].SecondBranch) != 2 {
		t.Fatalf("enriched signed fork evidence did not survive restart: evidence=%+v found=%v err=%v", afterRestart.ForkEvidence, found, err)
	}
}

func TestLegacyForkBundleWithoutBranchTranscriptsRemainsValid(t *testing.T) {
	first, base := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x38)
	genesis, _, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	shared, sharedSig, nodeID := signNodeDelegateWithSigner(t, genesis.IdentityID, "1", genesis.HeadHash,
		bytes.Repeat([]byte{0x39}, ed25519.SeedSize), bytes.Repeat([]byte{0x38}, ed25519.SeedSize))
	if err := first.ApplyNodeEvent(shared, sharedSig); err != nil {
		t.Fatal(err)
	}
	base, err = first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	firstSnapshot, _, _ := first.Snapshot()
	secondSnapshot, _, _ := second.Snapshot()
	firstEvent, firstSignature, _ := signNodeDelegateWithSigner(t, firstSnapshot.IdentityID, "2", firstSnapshot.HeadHash,
		bytes.Repeat([]byte{0x3a}, ed25519.SeedSize), bytes.Repeat([]byte{0x38}, ed25519.SeedSize))
	secondEvent, secondSignature, _ := signNodeDelegateWithSigner(t, secondSnapshot.IdentityID, "2", secondSnapshot.HeadHash,
		bytes.Repeat([]byte{0x3b}, ed25519.SeedSize), bytes.Repeat([]byte{0x38}, ed25519.SeedSize))
	if err := first.ApplyNodeEvent(firstEvent, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := second.ApplyNodeEvent(secondEvent, secondSignature); err != nil {
		t.Fatal(err)
	}
	if err := first.ImportHistory(mustExportHistory(t, second), ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("fork setup failed: %v", err)
	}
	legacyRecord, err := first.readLocked()
	if err != nil || len(legacyRecord.ForkEvidence) != 1 {
		t.Fatalf("read forked store before legacy-field simulation: record=%+v err=%v", legacyRecord, err)
	}
	legacyRecord.ForkEvidence[0].FirstBranch = nil
	legacyRecord.ForkEvidence[0].SecondBranch = nil
	if err := first.writeLocked(legacyRecord); err != nil {
		t.Fatalf("persist legacy direct-event-only fork evidence: %v", err)
	}
	legacyReopened, err := Open(first.root)
	if err != nil {
		t.Fatalf("reopen legacy fork store without branch transcript fields: %v", err)
	}
	legacySnapshot, exists, err := legacyReopened.Snapshot()
	if err != nil || !exists || !legacySnapshot.Frozen || legacySnapshot.ForkCount != 1 {
		t.Fatalf("legacy persisted fork did not remain frozen: snapshot=%+v exists=%v err=%v", legacySnapshot, exists, err)
	}
	fork, err := first.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	for i := range fork.ForkEvidence {
		fork.ForkEvidence[i].FirstBranch = nil
		fork.ForkEvidence[i].SecondBranch = nil
	}
	if _, err := SummarizeHistoryBundle(fork); err != nil {
		t.Fatalf("legacy fork evidence without branch fields was rejected: %v", err)
	}
}

func mustExportHistory(t *testing.T, store *Store) HistoryBundle {
	t.Helper()
	bundle, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestConcurrentRegistryForkImportsFreezeWithoutChoosingWinner(t *testing.T) {
	firstStore, _ := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x21)
	initial, _, err := firstStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	baseEvent, baseSignature, nodeID := signNodeDelegateWithSigner(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x22}, ed25519.SeedSize), bytes.Repeat([]byte{0x21}, ed25519.SeedSize))
	if err := firstStore.ApplyNodeEvent(baseEvent, baseSignature); err != nil {
		t.Fatal(err)
	}
	base, err := firstStore.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := secondStore.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	firstHead, _, _ := firstStore.Snapshot()
	firstEvent, firstSignature, _ := signNodeDelegateWithSigner(t, initial.IdentityID, "2", firstHead.HeadHash, bytes.Repeat([]byte{0x23}, ed25519.SeedSize), bytes.Repeat([]byte{0x21}, ed25519.SeedSize))
	secondEvent, secondSignature, _ := signNodeDelegateWithSigner(t, initial.IdentityID, "2", firstHead.HeadHash, bytes.Repeat([]byte{0x24}, ed25519.SeedSize), bytes.Repeat([]byte{0x21}, ed25519.SeedSize))
	if err := firstStore.ApplyNodeEvent(firstEvent, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := secondStore.ApplyNodeEvent(secondEvent, secondSignature); err != nil {
		t.Fatal(err)
	}
	firstBranch, err := firstStore.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	secondBranch, err := secondStore.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(base); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, bundle := range []HistoryBundle{firstBranch, secondBranch} {
		bundle := bundle
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- registry.Import(bundle)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	accepted, forked := 0, 0
	for result := range results {
		switch {
		case result == nil:
			accepted++
		case errors.Is(result, ErrIdentityFork):
			forked++
		default:
			t.Fatalf("concurrent registry import returned %v", result)
		}
	}
	projection, err := registry.AuthorityProjection(initial.IdentityID)
	if err != nil || accepted != 1 || forked != 1 || !projection.Frozen {
		t.Fatalf("concurrent fork result accepted=%d forked=%d projection=%+v err=%v", accepted, forked, projection, err)
	}
}

func TestSignedForkResolutionPropagatesToNodesAndHistoryRegistry(t *testing.T) {
	firstStore, _ := createRegistryFixtureWithChallenge(t, t.TempDir(), 0x31)
	initial, _, err := firstStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	baseEvent, baseSignature, nodeID := signNodeDelegateWithSigner(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x32}, ed25519.SeedSize), bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	if err := firstStore.ApplyNodeEvent(baseEvent, baseSignature); err != nil {
		t.Fatal(err)
	}
	base := mustExportHistory(t, firstStore)
	secondStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := secondStore.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	baseOnlyStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := baseOnlyStore.ImportHistory(base, nodeID); err != nil {
		t.Fatal(err)
	}
	baseHead, _, _ := firstStore.Snapshot()
	firstEvent, firstSignature, _ := signNodeDelegateWithSigner(t, initial.IdentityID, "2", baseHead.HeadHash, bytes.Repeat([]byte{0x33}, ed25519.SeedSize), bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	secondEvent, secondSignature, _ := signNodeDelegateWithSigner(t, initial.IdentityID, "2", baseHead.HeadHash, bytes.Repeat([]byte{0x34}, ed25519.SeedSize), bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	if err := firstStore.ApplyNodeEvent(firstEvent, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := secondStore.ApplyNodeEvent(secondEvent, secondSignature); err != nil {
		t.Fatal(err)
	}
	if err := firstStore.ImportHistory(mustExportHistory(t, secondStore), ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("first node did not freeze on peer branch: %v", err)
	}
	if err := secondStore.ImportHistory(mustExportHistory(t, firstStore), ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("second node did not retain peer fork: %v", err)
	}

	registry, err := OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	frozenBundle := mustExportHistory(t, firstStore)
	if err := registry.Import(frozenBundle); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("registry did not retain forked identity: %v", err)
	}

	choices, err := firstStore.ForkChoices()
	if err != nil || len(choices) != 1 {
		t.Fatalf("fork choices=%+v err=%v", choices, err)
	}
	choice := choices[0]
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	canonical, signature := signIdentityEvent(t, rootKey, initial.IdentityID, "3", &choice.FirstHeadHash, "fork_resolve", map[string]any{
		"fork_id": choice.ForkID, "previous_event_hash": choice.PreviousHash,
		"first_head_hash": choice.FirstHeadHash, "second_head_hash": choice.SecondHeadHash,
		"selected_head_hash": choice.FirstHeadHash,
	})
	if err := firstStore.ApplyForkResolution(canonical, signature); err != nil {
		t.Fatalf("resolve fork at owner: %v", err)
	}
	resolvedBundle := mustExportHistory(t, firstStore)
	decodedResolved, decodeErr := decodeHistoryBundle(resolvedBundle)
	if decodeErr != nil || decodedResolved.Frozen {
		t.Fatalf("owner resolution bundle invalid: frozen=%v err=%v", decodedResolved.Frozen, decodeErr)
	}
	frozenPeer, decodeErr := decodeHistoryBundle(mustExportHistory(t, secondStore))
	if decodeErr != nil || !frozenPeer.Frozen {
		t.Fatalf("peer fork bundle invalid: frozen=%v err=%v", frozenPeer.Frozen, decodeErr)
	}
	if !isAuthorizedForkResolutionAdvance(frozenPeer, decodedResolved) {
		t.Fatal("peer did not recognize signed resolution as an authorized fork advance")
	}
	if err := secondStore.ImportHistory(resolvedBundle, ""); err != nil {
		t.Fatalf("propagate root-approved branch to frozen node: %v", err)
	}
	if err := baseOnlyStore.ImportHistory(resolvedBundle, ""); err != nil {
		t.Fatalf("join directly to explicitly resolved history: %v", err)
	}
	baseOnlySnapshot, _, err := baseOnlyStore.Snapshot()
	if err != nil || baseOnlySnapshot.Frozen || baseOnlySnapshot.HeadHash == "" {
		t.Fatalf("resolved incoming history was incorrectly frozen: %+v err=%v", baseOnlySnapshot, err)
	}
	secondSnapshot, _, err := secondStore.Snapshot()
	if err != nil || secondSnapshot.Frozen || secondSnapshot.HeadHash == "" {
		t.Fatalf("peer did not adopt resolved signed head: %+v err=%v", secondSnapshot, err)
	}
	if err := registry.Import(resolvedBundle); err != nil {
		t.Fatalf("advance frozen registry with root-approved branch: %v", err)
	}
	registered, err := registry.AuthorityProjection(initial.IdentityID)
	if err != nil || registered.Frozen || registered.HeadHash != secondSnapshot.HeadHash {
		t.Fatalf("registry did not adopt resolved head: %+v err=%v", registered, err)
	}
}

func mustDecodeCanonical(t *testing.T, event HistoryEvent) []byte {
	t.Helper()
	canonical, err := base64.RawURLEncoding.DecodeString(event.CanonicalEvent)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
