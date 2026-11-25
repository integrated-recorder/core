package globalidentity

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

var (
	ErrForkResolutionInvalid     = errors.New("global identity fork resolution is invalid")
	ErrForkResolutionUnavailable = errors.New("global identity fork cannot be resolved safely")
)

// ForkChoice is a public, signature-verifiable summary of the two observed
// branch heads. It contains no local credentials or recovery material.
type ForkChoice struct {
	ForkID         string `json:"fork_id"`
	PreviousHash   string `json:"previous_event_hash"`
	FirstHeadHash  string `json:"first_head_hash"`
	SecondHeadHash string `json:"second_head_hash"`
	FirstSequence  string `json:"first_sequence"`
	SecondSequence string `json:"second_sequence"`
}

type forkResolution struct {
	ForkID         string
	PreviousHash   string
	FirstHeadHash  string
	SecondHeadHash string
	SelectedHead   string
}

// ForkChoices returns only unresolved forks on the current linear history.
// Branch choice is never inferred from arrival order or timestamps.
func (s *Store) ForkChoices() ([]ForkChoice, error) {
	if s == nil {
		return nil, ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return nil, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return nil, err
	}
	choices := make([]ForkChoice, 0, len(stored.ForkEvidence))
	for _, fork := range stored.ForkEvidence {
		if forkResolvedInHistory(stored.History, fork) || forkOrphanedByResolution(stored, fork) {
			continue
		}
		choice, err := forkChoice(stored, fork)
		if err != nil {
			return nil, ErrCorruptStore
		}
		choices = append(choices, choice)
	}
	return choices, nil
}

// ApplyForkResolution accepts only a recovery-root-signed fork_resolve event.
// The event must reference both observed branch heads and append to the
// explicitly selected branch. Competing branches remain in ForkEvidence.
func (s *Store) ApplyForkResolution(canonicalEvent, signature []byte) error {
	if s == nil {
		return ErrForkResolutionInvalid
	}
	event, err := identityprotocol.VerifyEvent(canonicalEvent, signature)
	if err != nil || event.Operation != "fork_resolve" {
		return ErrForkResolutionInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return err
	}
	if eventKnown(stored, event.EventHash) {
		return nil
	}
	if !stored.Frozen {
		return ErrForkResolutionUnavailable
	}
	resolution, err := parseForkResolution(event)
	if err != nil || resolution.ForkID == "" || event.IdentityID != stored.IdentityID {
		return ErrForkResolutionInvalid
	}
	var selected forkEvidence
	found := false
	for _, fork := range stored.ForkEvidence {
		id, idErr := forkEvidenceID(fork)
		if idErr != nil || id != resolution.ForkID {
			continue
		}
		if forkResolvedInHistory(stored.History, fork) {
			return ErrForkResolutionUnavailable
		}
		selected, found = fork, true
		break
	}
	if !found {
		return ErrForkResolutionInvalid
	}
	choice, err := forkChoice(stored, selected)
	if err != nil || resolution.PreviousHash != choice.PreviousHash || resolution.FirstHeadHash != choice.FirstHeadHash || resolution.SecondHeadHash != choice.SecondHeadHash || resolution.SelectedHead != choice.FirstHeadHash && resolution.SelectedHead != choice.SecondHeadHash {
		return ErrForkResolutionInvalid
	}
	firstBranch, secondBranch := normalizedForkBranches(selected)
	branch := firstBranch
	if resolution.SelectedHead == choice.SecondHeadHash {
		branch = secondBranch
	}
	prefix := []signedEvent(nil)
	if selected.PreviousHash != "" {
		var ok bool
		prefix, ok = historyPrefixForHash(stored, selected.PreviousHash)
		if !ok {
			return ErrForkResolutionUnavailable
		}
	}
	selectedBranch, ok := branchThroughHash(branch, resolution.SelectedHead)
	if !ok {
		return ErrForkResolutionInvalid
	}
	candidate := cloneSignedEvents(prefix)
	candidate = append(candidate, cloneSignedEvents(selectedBranch)...)
	if len(candidate) == 0 {
		return ErrForkResolutionInvalid
	}
	if err := validateSuccessorInRecord(stored, candidate, event); err != nil {
		return ErrForkResolutionInvalid
	}
	if len(stored.History) == 0 {
		return ErrCorruptStore
	}
	root, err := identityprotocol.VerifyEvent(candidate[0].Canonical, candidate[0].Signature)
	if err != nil || !bytes.Equal(event.SignerPublicKey, root.SignerPublicKey) {
		return ErrForkResolutionInvalid
	}
	candidate = append(candidate, signedEvent{Canonical: append([]byte(nil), event.Canonical...), Signature: append([]byte(nil), event.Signature...)})
	stored.History = candidate
	for _, other := range stored.ForkEvidence {
		if other.PreviousHash == "" || historyIndexByHash(stored.History, other.PreviousHash) >= 0 || forkOrphanedByResolution(stored, other) {
			continue
		}
		return ErrForkResolutionUnavailable
	}
	stored.CredentialInventoryMode = "local"
	active, err := activeCredentialIDs(stored.History)
	if err != nil {
		return ErrForkResolutionInvalid
	}
	credentials := stored.Credentials[:0]
	for _, credential := range stored.Credentials {
		if _, ok := active[base64CredentialID(credential.ID)]; ok {
			credentials = append(credentials, credential)
		}
	}
	stored.Credentials = credentials
	stored.Frozen = hasUnresolvedForks(stored)
	return s.writeLocked(stored)
}

func forkChoice(stored record, fork forkEvidence) (ForkChoice, error) {
	firstBranch, secondBranch := normalizedForkBranches(fork)
	genesis := fork.PreviousHash == ""
	var prefix []signedEvent
	if !genesis {
		var ok bool
		prefix, ok = historyPrefixForHash(stored, fork.PreviousHash)
		if !ok {
			return ForkChoice{}, ErrForkResolutionUnavailable
		}
	}
	_, firstHead, _, firstErr := validateForkBranch(stored, prefix, firstBranch, genesis)
	_, secondHead, _, secondErr := validateForkBranch(stored, prefix, secondBranch, genesis)
	if firstErr != nil || secondErr != nil {
		return ForkChoice{}, ErrForkResolutionInvalid
	}
	firstPresent := branchPresent(stored.History, firstBranch, genesis)
	secondPresent := branchPresent(stored.History, secondBranch, genesis)
	if firstPresent == secondPresent {
		return ForkChoice{}, ErrForkResolutionUnavailable
	}
	id, err := forkEvidenceID(fork)
	if err != nil {
		return ForkChoice{}, err
	}
	return ForkChoice{
		ForkID: id, PreviousHash: fork.PreviousHash,
		FirstHeadHash: firstHead.EventHash, SecondHeadHash: secondHead.EventHash,
		FirstSequence: firstHead.Sequence, SecondSequence: secondHead.Sequence,
	}, nil
}

func normalizedForkBranches(fork forkEvidence) ([]signedEvent, []signedEvent) {
	first, second := fork.FirstBranch, fork.SecondBranch
	if len(first) == 0 {
		first = []signedEvent{fork.First}
	}
	if len(second) == 0 {
		second = []signedEvent{fork.Second}
	}
	return first, second
}

func forkEvidenceID(fork forkEvidence) (string, error) {
	first, err := identityprotocol.VerifyEvent(fork.First.Canonical, fork.First.Signature)
	if err != nil {
		return "", err
	}
	second, err := identityprotocol.VerifyEvent(fork.Second.Canonical, fork.Second.Signature)
	if err != nil {
		return "", err
	}
	a, b := first.EventHash, second.EventHash
	if b < a {
		a, b = b, a
	}
	input := []byte("IR/global-identity-fork/v1\x00" + fork.PreviousHash + "\x00" + a + "\x00" + b)
	digest := sha256.Sum256(input)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func forkBranchHead(branch []signedEvent) (identityprotocol.Event, error) {
	if len(branch) == 0 {
		return identityprotocol.Event{}, ErrForkResolutionInvalid
	}
	return identityprotocol.VerifyEvent(branch[len(branch)-1].Canonical, branch[len(branch)-1].Signature)
}

func branchThroughHash(branch []signedEvent, hash string) ([]signedEvent, bool) {
	for index, signed := range branch {
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err == nil && event.EventHash == hash {
			return cloneSignedEvents(branch[:index+1]), true
		}
	}
	return nil, false
}

func historyIndexByHash(history []signedEvent, hash string) int {
	for index, signed := range history {
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err == nil && event.EventHash == hash {
			return index
		}
	}
	return -1
}

// historyPrefixForHash reconstructs a signed prefix from the authoritative
// history and retained fork transcripts. It is used only to validate evidence
// whose parent is no longer on the selected canonical history. Every event is
// signature-checked and the complete path is replayed under the same
// successor/authority rules as an ordinary history.
func historyPrefixForHash(stored record, target string) ([]signedEvent, bool) {
	if target == "" || !validHistoryHash(target) {
		return nil, false
	}
	known := make(map[string]signedEvent)
	add := func(signed signedEvent) bool {
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err != nil || event.IdentityID != stored.IdentityID {
			return false
		}
		if existing, exists := known[event.EventHash]; exists {
			return sameSignedEvent(existing, signed)
		}
		known[event.EventHash] = cloneSignedEvent(signed)
		return true
	}
	for _, signed := range stored.History {
		if !add(signed) {
			return nil, false
		}
	}
	for _, fork := range stored.ForkEvidence {
		first, second := normalizedForkBranches(fork)
		for _, branch := range [][]signedEvent{first, second} {
			for _, signed := range branch {
				if !add(signed) {
					return nil, false
				}
			}
		}
	}
	current, ok := known[target]
	if !ok {
		return nil, false
	}
	backwards := make([]signedEvent, 0, 16)
	seen := make(map[string]struct{})
	for len(backwards) <= 10000 {
		event, err := identityprotocol.VerifyEvent(current.Canonical, current.Signature)
		if err != nil || event.IdentityID != stored.IdentityID {
			return nil, false
		}
		if _, duplicate := seen[event.EventHash]; duplicate {
			return nil, false
		}
		seen[event.EventHash] = struct{}{}
		backwards = append(backwards, cloneSignedEvent(current))
		if event.PreviousHash == nil {
			if event.Operation != "genesis" || event.Sequence != "0" || !validGenesisPayload(event.Payload, "", "") {
				return nil, false
			}
			break
		}
		parent, exists := known[*event.PreviousHash]
		if !exists {
			return nil, false
		}
		current = parent
	}
	if len(backwards) == 0 || len(backwards) > 10000 || len(backwards[len(backwards)-1].Canonical) == 0 {
		return nil, false
	}
	prefix := make([]signedEvent, len(backwards))
	for i := range backwards {
		prefix[len(backwards)-1-i] = backwards[i]
	}
	for index := 1; index < len(prefix); index++ {
		event, err := identityprotocol.VerifyEvent(prefix[index].Canonical, prefix[index].Signature)
		if err != nil || validateSuccessorInRecord(stored, prefix[:index], event) != nil {
			return nil, false
		}
	}
	last, err := identityprotocol.VerifyEvent(prefix[len(prefix)-1].Canonical, prefix[len(prefix)-1].Signature)
	if err != nil || last.EventHash != target {
		return nil, false
	}
	return prefix, true
}

func parseForkResolution(event identityprotocol.Event) (forkResolution, error) {
	if event.Operation != "fork_resolve" || len(event.Payload) != 5 || event.PreviousHash == nil {
		return forkResolution{}, ErrForkResolutionInvalid
	}
	resolution := forkResolution{
		ForkID:         stringPayload(event.Payload, "fork_id"),
		PreviousHash:   stringPayload(event.Payload, "previous_event_hash"),
		FirstHeadHash:  stringPayload(event.Payload, "first_head_hash"),
		SecondHeadHash: stringPayload(event.Payload, "second_head_hash"),
		SelectedHead:   stringPayload(event.Payload, "selected_head_hash"),
	}
	if !validHistoryHash(resolution.ForkID) || !validHistoryHash(resolution.FirstHeadHash) || !validHistoryHash(resolution.SecondHeadHash) || !validHistoryHash(resolution.SelectedHead) || resolution.PreviousHash != "" && !validHistoryHash(resolution.PreviousHash) || resolution.SelectedHead != *event.PreviousHash {
		return forkResolution{}, ErrForkResolutionInvalid
	}
	return resolution, nil
}

func validateSuccessorInRecord(stored record, prefix []signedEvent, event identityprotocol.Event) error {
	if err := validateSuccessorInHistory(stored.IdentityID, prefix, event); err != nil {
		return err
	}
	if event.Operation != "fork_resolve" {
		return nil
	}
	resolution, err := parseForkResolution(event)
	if err != nil {
		return err
	}
	if resolution.PreviousHash == "" {
		if len(prefix) == 0 || prefix[0].Canonical == nil {
			return ErrForkResolutionInvalid
		}
	} else if historyIndexByHash(prefix, resolution.PreviousHash) < 0 {
		return ErrForkResolutionInvalid
	}
	for _, entry := range prefix[1:] {
		prior, verifyErr := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
		if verifyErr != nil {
			return ErrForkResolutionInvalid
		}
		if prior.Operation == "fork_resolve" {
			priorResolution, parseErr := parseForkResolution(prior)
			if parseErr == nil && priorResolution.ForkID == resolution.ForkID {
				return ErrForkResolutionInvalid
			}
		}
	}
	for _, fork := range stored.ForkEvidence {
		id, idErr := forkEvidenceID(fork)
		if idErr != nil || id != resolution.ForkID || fork.PreviousHash != resolution.PreviousHash {
			continue
		}
		firstBranch, secondBranch := normalizedForkBranches(fork)
		if _, ok := branchThroughHash(firstBranch, resolution.FirstHeadHash); !ok {
			return ErrForkResolutionInvalid
		}
		if _, ok := branchThroughHash(secondBranch, resolution.SecondHeadHash); !ok {
			return ErrForkResolutionInvalid
		}
		selectedBranch := firstBranch
		if resolution.SelectedHead == resolution.SecondHeadHash {
			selectedBranch = secondBranch
		} else if resolution.SelectedHead != resolution.FirstHeadHash {
			return ErrForkResolutionInvalid
		}
		genesis := fork.PreviousHash == ""
		if !branchPresent(prefix, mustBranchThrough(selectedBranch, resolution.SelectedHead), genesis) {
			return ErrForkResolutionInvalid
		}
		return nil
	}
	return ErrForkResolutionInvalid
}

func mustBranchThrough(branch []signedEvent, hash string) []signedEvent {
	selected, ok := branchThroughHash(branch, hash)
	if !ok {
		return nil
	}
	return selected
}

func forkResolvedInHistory(history []signedEvent, fork forkEvidence) bool {
	id, err := forkEvidenceID(fork)
	if err != nil {
		return false
	}
	for _, signed := range history {
		event, verifyErr := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if verifyErr != nil || event.Operation != "fork_resolve" {
			continue
		}
		resolution, parseErr := parseForkResolution(event)
		if parseErr == nil && resolution.ForkID == id && resolution.PreviousHash == fork.PreviousHash {
			return true
		}
	}
	return false
}

func hasUnresolvedForks(stored record) bool {
	for _, fork := range stored.ForkEvidence {
		if forkResolvedInHistory(stored.History, fork) || forkOrphanedByResolution(stored, fork) {
			continue
		}
		first, second := normalizedForkBranches(fork)
		if branchPresent(stored.History, first, fork.PreviousHash == "") != branchPresent(stored.History, second, fork.PreviousHash == "") {
			return true
		}
	}
	return false
}

func isAuthorizedForkResolutionAdvance(current, incoming record) bool {
	if !current.Frozen || incoming.Frozen || len(current.ForkEvidence) == 0 || !forkEvidenceSubset(current.ForkEvidence, incoming.ForkEvidence) {
		return false
	}
	resolvedAny := false
	for _, fork := range current.ForkEvidence {
		if forkResolvedInHistory(current.History, fork) || forkOrphanedByResolution(current, fork) {
			continue
		}
		if !forkResolvedInHistory(incoming.History, fork) && !forkOrphanedByResolution(incoming, fork) {
			return false
		}
		resolvedAny = true
	}
	return resolvedAny
}

// forkOrphanedByResolution reports whether a root-signed resolution on the
// selected history explicitly discarded the branch containing this fork's
// parent. Its evidence remains durable, but it no longer represents a live
// ambiguity in the selected identity history.
func forkOrphanedByResolution(stored record, candidate forkEvidence) bool {
	if candidate.PreviousHash == "" || len(stored.History) < 2 {
		return false
	}
	if len(stored.History[0].Canonical) == 0 {
		return false
	}
	root, err := identityprotocol.VerifyEvent(stored.History[0].Canonical, stored.History[0].Signature)
	if err != nil {
		return false
	}
	for index, signed := range stored.History[1:] {
		actualIndex := index + 1
		resolutionEvent, verifyErr := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if verifyErr != nil || resolutionEvent.Operation != "fork_resolve" || !bytes.Equal(resolutionEvent.SignerPublicKey, root.SignerPublicKey) {
			continue
		}
		resolution, parseErr := parseForkResolution(resolutionEvent)
		if parseErr != nil || validateSuccessorInRecord(stored, stored.History[:actualIndex], resolutionEvent) != nil {
			continue
		}
		for _, ancestor := range stored.ForkEvidence {
			ancestorID, idErr := forkEvidenceID(ancestor)
			if idErr != nil || ancestorID != resolution.ForkID || ancestor.PreviousHash != resolution.PreviousHash {
				continue
			}
			firstBranch, secondBranch := normalizedForkBranches(ancestor)
			if _, ok := branchThroughHash(firstBranch, resolution.FirstHeadHash); !ok {
				continue
			}
			if _, ok := branchThroughHash(secondBranch, resolution.SecondHeadHash); !ok {
				continue
			}
			var discarded []signedEvent
			switch resolution.SelectedHead {
			case resolution.FirstHeadHash:
				discarded = secondBranch
			case resolution.SecondHeadHash:
				discarded = firstBranch
			default:
				continue
			}
			if branchContainsHash(discarded, candidate.PreviousHash) {
				return true
			}
		}
	}
	return false
}

func branchContainsHash(branch []signedEvent, target string) bool {
	for _, signed := range branch {
		event, err := identityprotocol.VerifyEvent(signed.Canonical, signed.Signature)
		if err == nil && event.EventHash == target {
			return true
		}
	}
	return false
}

func base64CredentialID(value []byte) string {
	return base64.RawURLEncoding.EncodeToString(value)
}
