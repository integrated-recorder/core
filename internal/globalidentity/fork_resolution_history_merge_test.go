package globalidentity

import (
	"bytes"
	"errors"
	"testing"
)

func TestImportHistoryFreezesConflictingRootForkResolutions(t *testing.T) {
	genesis, genesisSignature, userHandle, credential, challenge := testGenesis(t)
	identity, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := identity.CreateGenesis(genesis, genesisSignature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	initial, _, err := identity.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first, firstSignature, _ := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x71}, 32))
	second, secondSignature, _ := signNodeDelegate(t, initial.IdentityID, "1", initial.HeadHash, bytes.Repeat([]byte{0x72}, 32))
	if err := identity.ApplyNodeEvent(first, firstSignature); err != nil {
		t.Fatal(err)
	}
	if err := identity.ApplyNodeEvent(second, secondSignature); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("competing delegated event did not freeze identity: %v", err)
	}
	forkedHistory := mustExportHistory(t, identity)

	stores := make([]*Store, 2)
	for i := range stores {
		stores[i], err = Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := stores[i].CreateGenesis(genesis, genesisSignature, userHandle, credential, challenge); err != nil {
			t.Fatal(err)
		}
		if err := stores[i].ImportHistory(forkedHistory, ""); err != nil && !errors.Is(err, ErrIdentityFork) {
			t.Fatalf("store %d import fork: %v", i, err)
		}
	}

	resolutionEvents := make([][]byte, 2)
	resolutionSignatures := make([][]byte, 2)
	for i, store := range stores {
		choices, err := store.ForkChoices()
		if err != nil || len(choices) != 1 {
			t.Fatalf("store %d fork choices=%+v err=%v", i, choices, err)
		}
		choice := choices[0]
		selected := choice.FirstHeadHash
		if i == 1 {
			selected = choice.SecondHeadHash
		}
		resolutionEvents[i], resolutionSignatures[i] = signIdentityEvent(t, rootRecoveryPrivateKey(t), initial.IdentityID, "2", &selected, "fork_resolve", map[string]any{
			"fork_id": choice.ForkID, "previous_event_hash": choice.PreviousHash,
			"first_head_hash": choice.FirstHeadHash, "second_head_hash": choice.SecondHeadHash,
			"selected_head_hash": selected,
		})
		if err := store.ApplyForkResolution(resolutionEvents[i], resolutionSignatures[i]); err != nil {
			t.Fatalf("store %d root resolution: %v", i, err)
		}
	}

	if err := stores[0].ImportHistory(mustExportHistory(t, stores[1]), ""); !errors.Is(err, ErrIdentityFork) {
		t.Fatalf("opposing valid root resolutions must freeze as a fork, got %v", err)
	}
	state, exists, err := stores[0].Snapshot()
	if err != nil || !exists || !state.Frozen {
		t.Fatalf("opposing resolutions did not leave authority frozen: state=%+v exists=%v err=%v", state, exists, err)
	}
	bundle, err := stores[0].ExportHistory()
	if err != nil || len(bundle.ForkEvidence) != 1 {
		t.Fatalf("competing branch evidence was not retained: forks=%d err=%v", len(bundle.ForkEvidence), err)
	}
	merged, err := decodeHistoryBundle(bundle)
	if err != nil {
		t.Fatalf("retained competing evidence is not verifiable: %v", err)
	}
	firstBranch, secondBranch := normalizedForkBranches(merged.ForkEvidence[0])
	if len(firstBranch) < 2 || len(secondBranch) < 2 {
		t.Fatalf("resolution tails were not retained on both branches: first=%d second=%d", len(firstBranch), len(secondBranch))
	}
	reopened, err := Open(stores[0].root)
	if err != nil {
		t.Fatal(err)
	}
	restarted, exists, err := reopened.Snapshot()
	if err != nil || !exists || !restarted.Frozen || restarted.HeadHash != state.HeadHash {
		t.Fatalf("frozen resolution conflict did not survive restart: state=%+v exists=%v err=%v", restarted, exists, err)
	}
}
