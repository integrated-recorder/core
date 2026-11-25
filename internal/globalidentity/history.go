package globalidentity

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const HistoryBundleVersion = "ir.identity.history/1"

var (
	ErrHistoryInvalid       = errors.New("global identity history bundle is invalid")
	ErrHistoryDifferentID   = errors.New("global identity history belongs to a different identity")
	ErrHistoryNodeNotActive = errors.New("local node is not authorized by imported identity history")
)

// HistoryEvent transports public signed event bytes only. WebAuthn credential
// records and installation-local user handles never cross node boundaries.
type HistoryEvent struct {
	CanonicalEvent string `json:"canonical_event"`
	Signature      string `json:"signature"`
}

type HistoryFork struct {
	PreviousHash string         `json:"previous_event_hash"`
	First        HistoryEvent   `json:"first_event"`
	Second       HistoryEvent   `json:"second_event"`
	FirstBranch  []HistoryEvent `json:"first_branch,omitempty"`
	SecondBranch []HistoryEvent `json:"second_branch,omitempty"`
}

type HistoryBundle struct {
	Format       string         `json:"format"`
	IdentityID   string         `json:"identity_id"`
	Events       []HistoryEvent `json:"events"`
	ForkEvidence []HistoryFork  `json:"fork_evidence,omitempty"`
}

// ExportHistory returns the verified append-only public history. It never
// includes local Passkey credentials, sessions, user handles, or private keys.
func (s *Store) ExportHistory() (HistoryBundle, error) {
	if s == nil {
		return HistoryBundle{}, ErrCorruptStore
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return HistoryBundle{}, err
	}
	defer unlock()
	stored, err := s.readLocked()
	if err != nil {
		return HistoryBundle{}, err
	}
	bundle := HistoryBundle{Format: HistoryBundleVersion, IdentityID: stored.IdentityID, Events: make([]HistoryEvent, 0, len(stored.History))}
	for _, event := range stored.History {
		bundle.Events = append(bundle.Events, encodeHistoryEvent(event))
	}
	for _, evidence := range stored.ForkEvidence {
		bundle.ForkEvidence = append(bundle.ForkEvidence, HistoryFork{
			PreviousHash: evidence.PreviousHash,
			First:        encodeHistoryEvent(evidence.First),
			Second:       encodeHistoryEvent(evidence.Second),
			FirstBranch:  encodeHistoryEvents(evidence.FirstBranch),
			SecondBranch: encodeHistoryEvents(evidence.SecondBranch),
		})
	}
	return bundle, nil
}

// ImportHistory merges an untrusted public history bundle without rolling the
// local head backward. A first import requires that localNodeID is actively
// delegated in the verified history. Divergent valid children are retained as
// fork evidence and freeze the identity; no branch is selected by arrival time.
func (s *Store) ImportHistory(bundle HistoryBundle, localNodeID string) error {
	if s == nil {
		return ErrCorruptStore
	}
	incoming, err := decodeHistoryBundle(bundle)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := lockGlobalIdentity(s.root)
	if err != nil {
		return err
	}
	defer unlock()
	local, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		if !identityprotocol.IsNodeID(localNodeID) {
			return ErrHistoryNodeNotActive
		}
		active, activeErr := activeNodeIDs(incoming)
		if activeErr != nil || !containsString(active, localNodeID) {
			return ErrHistoryNodeNotActive
		}
		incoming.SchemaVersion = SchemaVersion
		incoming.CredentialInventoryMode = "local"
		incoming.UserHandle = make([]byte, 32)
		if _, err := io.ReadFull(rand.Reader, incoming.UserHandle); err != nil {
			return errors.New("global identity local account could not be initialized")
		}
		if err := s.writeLocked(incoming); err != nil {
			return err
		}
		if incoming.Frozen {
			return ErrIdentityFork
		}
		return nil
	}
	if err != nil {
		return err
	}
	if local.IdentityID != incoming.IdentityID {
		return ErrHistoryDifferentID
	}
	if isAuthorizedForkResolutionAdvance(local, incoming) {
		local.History = cloneSignedEvents(incoming.History)
		local.ForkEvidence = cloneForkEvidence(incoming.ForkEvidence)
		local.CredentialInventoryMode = "local"
		active, activeErr := activeCredentialIDs(local.History)
		if activeErr != nil {
			return ErrHistoryInvalid
		}
		credentials := local.Credentials[:0]
		for _, credential := range local.Credentials {
			if _, ok := active[base64.RawURLEncoding.EncodeToString(credential.ID)]; ok {
				credentials = append(credentials, credential)
			}
		}
		local.Credentials = credentials
		local.Frozen = hasUnresolvedForks(local)
		if err := s.writeLocked(local); err != nil {
			return err
		}
		return nil
	}
	localHashes := eventHashes(local.History)
	incomingHashes := eventHashes(incoming.History)
	common := 0
	for common < len(localHashes) && common < len(incomingHashes) && localHashes[common] == incomingHashes[common] {
		common++
	}
	if common == 0 {
		if fork, ok := competingGenesis(local, incoming); ok {
			if err := appendForkEvidence(&local, fork); err != nil {
				return err
			}
			local.Frozen = true
			if err := s.writeLocked(local); err != nil {
				return err
			}
			return ErrIdentityFork
		}
		return ErrHistoryInvalid
	}
	if common < len(localHashes) && common < len(incomingHashes) {
		parent, err := identityprotocol.VerifyEvent(local.History[common-1].Canonical, local.History[common-1].Signature)
		if err != nil {
			return ErrCorruptStore
		}
		firstEvent, err := identityprotocol.VerifyEvent(local.History[common].Canonical, local.History[common].Signature)
		if err != nil {
			return ErrCorruptStore
		}
		secondEvent, err := identityprotocol.VerifyEvent(incoming.History[common].Canonical, incoming.History[common].Signature)
		if err != nil || validateSuccessorInHistory(local.IdentityID, local.History[:common], secondEvent) != nil {
			return ErrHistoryInvalid
		}
		if firstEvent.PreviousHash == nil || *firstEvent.PreviousHash != parent.EventHash || secondEvent.PreviousHash == nil || *secondEvent.PreviousHash != parent.EventHash {
			return ErrHistoryInvalid
		}
		if err := appendForkEvidence(&local, forkEvidence{
			PreviousHash: parent.EventHash,
			First:        cloneSignedEvent(local.History[common]),
			Second:       cloneSignedEvent(incoming.History[common]),
			FirstBranch:  cloneSignedEvents(local.History[common:]),
			SecondBranch: cloneSignedEvents(incoming.History[common:]),
		}); err != nil {
			return err
		}
		local.Frozen = true
		if err := s.writeLocked(local); err != nil {
			return err
		}
		return ErrIdentityFork
	}

	if common == len(localHashes) && len(incomingHashes) > common {
		local.History = cloneSignedEvents(incoming.History)
		local.CredentialInventoryMode = "local"
		activeCredentials, activeErr := activeCredentialIDs(local.History)
		if activeErr != nil {
			return ErrHistoryInvalid
		}
		filtered := local.Credentials[:0]
		for _, credential := range local.Credentials {
			if _, active := activeCredentials[base64.RawURLEncoding.EncodeToString(credential.ID)]; active {
				filtered = append(filtered, credential)
			}
		}
		local.Credentials = filtered
	}
	for _, evidence := range incoming.ForkEvidence {
		if err := appendForkEvidence(&local, evidence); err != nil {
			return err
		}
	}
	local.Frozen = hasUnresolvedForks(local)
	if local.SchemaVersion == 1 || local.CredentialInventoryMode == "" || local.Frozen || common == len(localHashes) && len(incomingHashes) > common || len(incoming.ForkEvidence) > 0 {
		if local.SchemaVersion == 1 {
			local.CredentialInventoryMode = "complete"
		}
		if common == len(localHashes) && len(incomingHashes) > common {
			local.CredentialInventoryMode = "local"
		}
		return s.writeLocked(local)
	}
	return nil
}

func decodeHistoryBundle(bundle HistoryBundle) (record, error) {
	if bundle.Format != HistoryBundleVersion || bundle.IdentityID == "" || len(bundle.Events) == 0 || len(bundle.Events) > 10000 || len(bundle.ForkEvidence) > maxForkEvidence {
		return record{}, ErrHistoryInvalid
	}
	var total int
	decodeEvent := func(input HistoryEvent) (signedEvent, error) {
		if input.CanonicalEvent == "" || input.Signature == "" || len(input.CanonicalEvent) > maxStoreBytes*2 || len(input.Signature) > 128 {
			return signedEvent{}, ErrHistoryInvalid
		}
		canonical, err := base64.RawURLEncoding.DecodeString(input.CanonicalEvent)
		if err != nil || len(canonical) == 0 || base64.RawURLEncoding.EncodeToString(canonical) != input.CanonicalEvent {
			return signedEvent{}, ErrHistoryInvalid
		}
		signature, err := base64.RawURLEncoding.DecodeString(input.Signature)
		if err != nil || len(signature) != 64 || base64.RawURLEncoding.EncodeToString(signature) != input.Signature {
			return signedEvent{}, ErrHistoryInvalid
		}
		total += len(canonical) + len(signature)
		if total > maxStoreBytes {
			return signedEvent{}, ErrHistoryInvalid
		}
		return signedEvent{Canonical: canonical, Signature: signature}, nil
	}

	stored := record{SchemaVersion: SchemaVersion, CredentialInventoryMode: "local", IdentityID: bundle.IdentityID, UserHandle: make([]byte, 32), History: make([]signedEvent, 0, len(bundle.Events)), ForkEvidence: make([]forkEvidence, 0, len(bundle.ForkEvidence)), CreatedAt: time.Time{}}
	for _, input := range bundle.Events {
		entry, err := decodeEvent(input)
		if err != nil {
			return record{}, err
		}
		stored.History = append(stored.History, entry)
	}
	for _, input := range bundle.ForkEvidence {
		first, err := decodeEvent(input.First)
		if err != nil {
			return record{}, err
		}
		second, err := decodeEvent(input.Second)
		if err != nil {
			return record{}, err
		}
		firstBranch, err := decodeEvents(input.FirstBranch, decodeEvent)
		if err != nil {
			return record{}, err
		}
		secondBranch, err := decodeEvents(input.SecondBranch, decodeEvent)
		if err != nil {
			return record{}, err
		}
		stored.ForkEvidence = append(stored.ForkEvidence, forkEvidence{PreviousHash: input.PreviousHash, First: first, Second: second, FirstBranch: firstBranch, SecondBranch: secondBranch})
	}
	genesis, err := identityprotocol.VerifyEvent(stored.History[0].Canonical, stored.History[0].Signature)
	if err != nil || genesis.IdentityID != bundle.IdentityID || genesis.Operation != "genesis" {
		return record{}, ErrHistoryInvalid
	}
	stored.CreatedAt = genesis.IssuedAt
	stored.Frozen = hasUnresolvedForks(stored)
	if validateRecord(stored) != nil {
		return record{}, ErrHistoryInvalid
	}
	return stored, nil
}

func decodeEvents(input []HistoryEvent, decode func(HistoryEvent) (signedEvent, error)) ([]signedEvent, error) {
	if len(input) > 10000 {
		return nil, ErrHistoryInvalid
	}
	output := make([]signedEvent, 0, len(input))
	for _, item := range input {
		event, err := decode(item)
		if err != nil {
			return nil, err
		}
		output = append(output, event)
	}
	return output, nil
}

func encodeHistoryEvents(input []signedEvent) []HistoryEvent {
	if len(input) == 0 {
		return nil
	}
	output := make([]HistoryEvent, len(input))
	for i, event := range input {
		output[i] = encodeHistoryEvent(event)
	}
	return output
}

func encodeHistoryEvent(event signedEvent) HistoryEvent {
	return HistoryEvent{CanonicalEvent: base64.RawURLEncoding.EncodeToString(event.Canonical), Signature: base64.RawURLEncoding.EncodeToString(event.Signature)}
}

func eventHashes(events []signedEvent) []string {
	hashes := make([]string, len(events))
	for index, entry := range events {
		event, _ := identityprotocol.VerifyEvent(entry.Canonical, entry.Signature)
		hashes[index] = event.EventHash
	}
	return hashes
}

func cloneSignedEvents(input []signedEvent) []signedEvent {
	output := make([]signedEvent, len(input))
	for index, entry := range input {
		output[index] = cloneSignedEvent(entry)
	}
	return output
}

func cloneForkEvidence(input []forkEvidence) []forkEvidence {
	output := make([]forkEvidence, len(input))
	for i, fork := range input {
		output[i] = forkEvidence{
			PreviousHash: fork.PreviousHash, First: cloneSignedEvent(fork.First), Second: cloneSignedEvent(fork.Second),
			FirstBranch: cloneSignedEvents(fork.FirstBranch), SecondBranch: cloneSignedEvents(fork.SecondBranch),
		}
	}
	return output
}

func appendForkEvidence(stored *record, incoming forkEvidence) error {
	first, err1 := identityprotocol.VerifyEvent(incoming.First.Canonical, incoming.First.Signature)
	second, err2 := identityprotocol.VerifyEvent(incoming.Second.Canonical, incoming.Second.Signature)
	if err1 != nil || err2 != nil {
		return ErrHistoryInvalid
	}
	for index := range stored.ForkEvidence {
		existing := &stored.ForkEvidence[index]
		a, _ := identityprotocol.VerifyEvent(existing.First.Canonical, existing.First.Signature)
		b, _ := identityprotocol.VerifyEvent(existing.Second.Canonical, existing.Second.Signature)
		if existing.PreviousHash == incoming.PreviousHash && (a.EventHash == first.EventHash && b.EventHash == second.EventHash || a.EventHash == second.EventHash && b.EventHash == first.EventHash) {
			firstBranch, secondBranch := incoming.FirstBranch, incoming.SecondBranch
			if a.EventHash == second.EventHash {
				firstBranch, secondBranch = incoming.SecondBranch, incoming.FirstBranch
			}
			mergedFirst, okFirst := mergeForkBranch(existing.FirstBranch, firstBranch, existing.First)
			mergedSecond, okSecond := mergeForkBranch(existing.SecondBranch, secondBranch, existing.Second)
			if !okFirst || !okSecond {
				return ErrHistoryInvalid
			}
			existing.FirstBranch, existing.SecondBranch = mergedFirst, mergedSecond
			return nil
		}
	}
	if len(stored.ForkEvidence) >= maxForkEvidence {
		return ErrForkEvidenceLimit
	}
	stored.ForkEvidence = append(stored.ForkEvidence, forkEvidence{
		PreviousHash: incoming.PreviousHash, First: cloneSignedEvent(incoming.First), Second: cloneSignedEvent(incoming.Second),
		FirstBranch: cloneSignedEvents(incoming.FirstBranch), SecondBranch: cloneSignedEvents(incoming.SecondBranch),
	})
	return nil
}

// mergeForkBranch accepts only a prefix extension of previously verified
// signed evidence. It never replaces one valid branch with an unrelated fork.
func mergeForkBranch(existing, incoming []signedEvent, direct signedEvent) ([]signedEvent, bool) {
	if len(existing) == 0 {
		existing = []signedEvent{cloneSignedEvent(direct)}
	}
	if len(incoming) == 0 {
		return cloneSignedEvents(existing), true
	}
	if !sameSignedEvent(existing[0], direct) || !sameSignedEvent(incoming[0], direct) {
		return nil, false
	}
	common := len(existing)
	if len(incoming) < common {
		common = len(incoming)
	}
	for i := 0; i < common; i++ {
		if !sameSignedEvent(existing[i], incoming[i]) {
			return nil, false
		}
	}
	if len(incoming) > len(existing) {
		return cloneSignedEvents(incoming), true
	}
	return cloneSignedEvents(existing), true
}

// competingGenesis identifies two distinct valid genesis events signed by
// the same recovery root for the same ID. Genesis has no parent hash, so its
// fork evidence is represented with an empty PreviousHash and validated as a
// special root-level fork rather than selecting either initial account.
func competingGenesis(first, second record) (forkEvidence, bool) {
	if first.IdentityID == "" || first.IdentityID != second.IdentityID || len(first.History) == 0 || len(second.History) == 0 {
		return forkEvidence{}, false
	}
	a, errA := identityprotocol.VerifyEvent(first.History[0].Canonical, first.History[0].Signature)
	b, errB := identityprotocol.VerifyEvent(second.History[0].Canonical, second.History[0].Signature)
	if errA != nil || errB != nil || a.Operation != "genesis" || b.Operation != "genesis" || a.IdentityID != b.IdentityID || a.EventHash == b.EventHash || !bytes.Equal(a.SignerPublicKey, b.SignerPublicKey) || !validGenesisPayload(a.Payload, "", "") || !validGenesisPayload(b.Payload, "", "") {
		return forkEvidence{}, false
	}
	return forkEvidence{
		First: cloneSignedEvent(first.History[0]), Second: cloneSignedEvent(second.History[0]),
		FirstBranch: cloneSignedEvents(first.History), SecondBranch: cloneSignedEvents(second.History),
	}, true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
