package globalidentity

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	historyRegistryVersion = 1
	maxRegistryBytes       = 32 << 20
	maxRegistryIdentities  = 256
	maxRegistryEvents      = 65536
	historyRegistryFile    = "global-identity-registry-v1.json"
)

var (
	ErrHistoryStale = errors.New("global identity history is older than the registered history")
	ErrRegistryFull = errors.New("global identity history registry reached its safe limit")
)

// HistoryRegistry stores only public, signed identity history. It is separate
// from Store because a node's local WebAuthn credentials and user handle must
// never be copied when learning about another identity.
type HistoryRegistry struct {
	mu   sync.Mutex
	root string
}

type historyRegistryDisk struct {
	Version    int                    `json:"schema_version"`
	Identities []historyRegistryEntry `json:"identities"`
}

type historyRegistryEntry struct {
	IdentityID string        `json:"identity_id"`
	History    HistoryBundle `json:"history"`
	VerifiedAt time.Time     `json:"verified_at"`
}

// RegisteredIdentity is a verified public projection. It contains no
// credential records, local user handles, or private key material.
type RegisteredIdentity struct {
	IdentityID string    `json:"identity_id"`
	Sequence   string    `json:"sequence"`
	HeadHash   string    `json:"head_hash"`
	Frozen     bool      `json:"frozen"`
	ForkCount  int       `json:"fork_count"`
	NodeIDs    []string  `json:"node_ids"`
	VerifiedAt time.Time `json:"verified_at"`
}

// HistorySummary is the minimal verified head projection used when comparing
// identity-history mirrors. It contains no credential records or key material.
type HistorySummary struct {
	IdentityID string
	Sequence   string
	HeadHash   string
	Frozen     bool
	ForkCount  int
}

// SummarizeHistoryBundle validates the complete signed history and returns its
// verified head. The summary is not a freshness proof; callers must obtain
// independent, current observations before using it for authorization.
func SummarizeHistoryBundle(bundle HistoryBundle) (HistorySummary, error) {
	stored, err := decodeHistoryBundle(bundle)
	if err != nil || len(stored.History) == 0 {
		return HistorySummary{}, ErrHistoryInvalid
	}
	head, err := identityprotocol.VerifyEvent(stored.History[len(stored.History)-1].Canonical, stored.History[len(stored.History)-1].Signature)
	if err != nil || head.IdentityID != stored.IdentityID {
		return HistorySummary{}, ErrHistoryInvalid
	}
	return HistorySummary{
		IdentityID: stored.IdentityID, Sequence: head.Sequence, HeadHash: head.EventHash,
		Frozen: stored.Frozen, ForkCount: len(stored.ForkEvidence),
	}, nil
}

// OpenHistoryRegistry opens the public multi-identity history registry under
// dataRoot/security. An absent registry is an empty registry. The file is
// private and atomically replaced, although its contents are public signed
// history, to match the security boundary of adjacent identity state.
func OpenHistoryRegistry(dataRoot string) (*HistoryRegistry, error) {
	if strings.TrimSpace(dataRoot) == "" || !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot {
		return nil, ErrCorruptStore
	}
	securityDir := filepath.Join(dataRoot, "security")
	if err := os.MkdirAll(securityDir, 0700); err != nil {
		return nil, errors.New("global identity registry storage is unavailable")
	}
	info, err := os.Lstat(securityDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, ErrCorruptStore
	}
	registry := &HistoryRegistry{root: dataRoot}
	unlock, err := lockGlobalIdentity(dataRoot)
	if err != nil {
		return nil, err
	}
	defer unlock()
	_, err = registry.readLocked()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return registry, nil
}

// Import verifies and atomically merges an untrusted public history bundle.
// It never trusts source location or timestamps to select a head. A valid
// competing successor is retained as fork evidence and freezes that identity.
func (r *HistoryRegistry) Import(bundle HistoryBundle) error {
	if r == nil {
		return ErrCorruptStore
	}
	incoming, err := decodeHistoryBundle(bundle)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := lockGlobalIdentity(r.root)
	if err != nil {
		return err
	}
	defer unlock()
	disk, err := r.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		disk = historyRegistryDisk{Version: historyRegistryVersion, Identities: []historyRegistryEntry{}}
	} else if err != nil {
		return err
	}
	index := -1
	for i := range disk.Identities {
		if disk.Identities[i].IdentityID == incoming.IdentityID {
			index = i
			break
		}
	}
	if index < 0 {
		if len(disk.Identities) >= maxRegistryIdentities {
			return ErrRegistryFull
		}
		disk.Identities = append(disk.Identities, historyRegistryEntry{IdentityID: incoming.IdentityID, History: recordToBundle(incoming), VerifiedAt: time.Now().UTC()})
		if err := r.writeLocked(disk); err != nil {
			return err
		}
		if incoming.Frozen {
			return ErrIdentityFork
		}
		return nil
	}

	current, err := decodeHistoryBundle(disk.Identities[index].History)
	if err != nil {
		return ErrCorruptStore
	}
	merged, changed, stale, forked, err := mergeRegisteredHistory(current, incoming)
	if err != nil {
		return err
	}
	if !changed {
		if stale {
			return ErrHistoryStale
		}
		return nil
	}
	disk.Identities[index].History = recordToBundle(merged)
	disk.Identities[index].VerifiedAt = time.Now().UTC()
	if err := r.writeLocked(disk); err != nil {
		return err
	}
	if forked {
		return ErrIdentityFork
	}
	if stale {
		return ErrHistoryStale
	}
	return nil
}

// mergeRegisteredHistory joins only histories with a common verified prefix.
// When both have a different valid successor, it keeps the existing linear
// projection solely as evidence ordering and freezes authorization; it does
// not declare that branch authoritative.
func mergeRegisteredHistory(current, incoming record) (record, bool, bool, bool, error) {
	if current.IdentityID != incoming.IdentityID {
		return record{}, false, false, false, ErrHistoryDifferentID
	}
	if current.Frozen {
		if isAuthorizedForkResolutionAdvance(current, incoming) {
			resolved := cloneHistoryRecord(incoming)
			if err := validateRecord(resolved); err != nil {
				return record{}, false, false, false, ErrHistoryInvalid
			}
			return resolved, true, false, false, nil
		}
		// A frozen identity may collect a longer, fully signed branch transcript
		// for recovery review. This only enriches retained evidence; it never
		// changes the selected history or clears the authorization freeze.
		if sameHistory(current.History, incoming.History) {
			if forkEvidenceSubset(incoming.ForkEvidence, current.ForkEvidence) {
				return current, false, false, false, nil
			}
			merged := cloneHistoryRecord(current)
			for _, evidence := range incoming.ForkEvidence {
				if err := appendForkEvidence(&merged, evidence); err != nil {
					return record{}, false, false, false, err
				}
			}
			merged.Frozen = true
			if err := validateRecord(merged); err != nil {
				return record{}, false, false, false, ErrHistoryInvalid
			}
			return merged, true, false, false, nil
		}
		return current, false, true, false, ErrIdentityFrozen
	}
	currentHashes := eventHashes(current.History)
	incomingHashes := eventHashes(incoming.History)
	common := 0
	for common < len(currentHashes) && common < len(incomingHashes) && currentHashes[common] == incomingHashes[common] {
		common++
	}
	if common == 0 {
		if fork, ok := competingGenesis(current, incoming); ok {
			merged := cloneHistoryRecord(current)
			if err := appendForkEvidence(&merged, fork); err != nil {
				return record{}, false, false, false, err
			}
			merged.Frozen = true
			if err := validateRecord(merged); err != nil {
				return record{}, false, false, false, ErrHistoryInvalid
			}
			return merged, true, false, true, nil
		}
		return record{}, false, false, false, ErrHistoryInvalid
	}
	merged := cloneHistoryRecord(current)
	changed := false
	stale := false
	forked := false
	switch {
	case common < len(currentHashes) && common < len(incomingHashes):
		parent, err := identityprotocol.VerifyEvent(current.History[common-1].Canonical, current.History[common-1].Signature)
		if err != nil {
			return record{}, false, false, false, ErrCorruptStore
		}
		first := current.History[common]
		second := incoming.History[common]
		secondEvent, err := identityprotocol.VerifyEvent(second.Canonical, second.Signature)
		if err != nil || validateSuccessorInHistory(current.IdentityID, current.History[:common], secondEvent) != nil {
			return record{}, false, false, false, ErrHistoryInvalid
		}
		if err := appendForkEvidence(&merged, forkEvidence{
			PreviousHash: parent.EventHash, First: cloneSignedEvent(first), Second: cloneSignedEvent(second),
			FirstBranch: cloneSignedEvents(current.History[common:]), SecondBranch: cloneSignedEvents(incoming.History[common:]),
		}); err != nil {
			return record{}, false, false, false, err
		}
		changed, forked = true, true
	case common == len(currentHashes) && len(incomingHashes) > common:
		merged.History = cloneSignedEvents(incoming.History)
		changed = true
	case common == len(incomingHashes) && len(currentHashes) > common:
		stale = true
	}

	for _, evidence := range incoming.ForkEvidence {
		before := len(merged.ForkEvidence)
		if err := appendForkEvidence(&merged, evidence); err != nil {
			return record{}, false, false, false, err
		}
		if len(merged.ForkEvidence) != before {
			changed, forked = true, true
		}
	}
	merged.Frozen = hasUnresolvedForks(merged)
	if err := validateRecord(merged); err != nil {
		return record{}, false, false, false, ErrHistoryInvalid
	}
	if stale && !changed {
		return merged, false, true, false, nil
	}
	return merged, changed, stale, merged.Frozen && (forked || !current.Frozen), nil
}

func cloneHistoryRecord(input record) record {
	output := input
	output.UserHandle = append([]byte(nil), input.UserHandle...)
	output.History = cloneSignedEvents(input.History)
	output.Credentials = cloneCredentials(input.Credentials)
	output.ForkEvidence = make([]forkEvidence, len(input.ForkEvidence))
	for i, fork := range input.ForkEvidence {
		output.ForkEvidence[i] = forkEvidence{
			PreviousHash: fork.PreviousHash, First: cloneSignedEvent(fork.First), Second: cloneSignedEvent(fork.Second),
			FirstBranch: cloneSignedEvents(fork.FirstBranch), SecondBranch: cloneSignedEvents(fork.SecondBranch),
		}
	}
	return output
}

func sameHistory(first, second []signedEvent) bool {
	if len(first) != len(second) {
		return false
	}
	for i := range first {
		if !bytes.Equal(first[i].Canonical, second[i].Canonical) || !bytes.Equal(first[i].Signature, second[i].Signature) {
			return false
		}
	}
	return true
}

func forkEvidenceSubset(candidate, existing []forkEvidence) bool {
	if len(candidate) > len(existing) {
		return false
	}
	for _, left := range candidate {
		found := false
		for _, right := range existing {
			if !sameForkPair(left, right) {
				continue
			}
			if sameSignedEvent(left.First, right.First) && sameSignedEvent(left.Second, right.Second) &&
				forkBranchesPrefix(left.FirstBranch, right.FirstBranch) && forkBranchesPrefix(left.SecondBranch, right.SecondBranch) {
				found = true
				break
			}
			if sameSignedEvent(left.First, right.Second) && sameSignedEvent(left.Second, right.First) &&
				forkBranchesPrefix(left.FirstBranch, right.SecondBranch) && forkBranchesPrefix(left.SecondBranch, right.FirstBranch) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sameForkPair(first, second forkEvidence) bool {
	return first.PreviousHash == second.PreviousHash &&
		(sameSignedEvent(first.First, second.First) && sameSignedEvent(first.Second, second.Second) ||
			sameSignedEvent(first.First, second.Second) && sameSignedEvent(first.Second, second.First))
}

func forkBranchesPrefix(candidate, existing []signedEvent) bool {
	if len(candidate) == 0 {
		return true
	}
	if len(candidate) > len(existing) {
		return false
	}
	for i := range candidate {
		if !sameSignedEvent(candidate[i], existing[i]) {
			return false
		}
	}
	return true
}

func sameSignedEvent(first, second signedEvent) bool {
	return bytes.Equal(first.Canonical, second.Canonical) && bytes.Equal(first.Signature, second.Signature)
}

func recordToBundle(stored record) HistoryBundle {
	bundle := HistoryBundle{Format: HistoryBundleVersion, IdentityID: stored.IdentityID, Events: make([]HistoryEvent, 0, len(stored.History))}
	for _, event := range stored.History {
		bundle.Events = append(bundle.Events, encodeHistoryEvent(event))
	}
	for _, evidence := range stored.ForkEvidence {
		bundle.ForkEvidence = append(bundle.ForkEvidence, HistoryFork{
			PreviousHash: evidence.PreviousHash, First: encodeHistoryEvent(evidence.First), Second: encodeHistoryEvent(evidence.Second),
			FirstBranch: encodeHistoryEvents(evidence.FirstBranch), SecondBranch: encodeHistoryEvents(evidence.SecondBranch),
		})
	}
	return bundle
}

// Get returns a deep copy of one verified public history bundle.
func (r *HistoryRegistry) Get(identityID string) (HistoryBundle, bool, error) {
	if r == nil {
		return HistoryBundle{}, false, ErrCorruptStore
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := lockGlobalIdentity(r.root)
	if err != nil {
		return HistoryBundle{}, false, err
	}
	defer unlock()
	disk, err := r.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return HistoryBundle{}, false, nil
	}
	if err != nil {
		return HistoryBundle{}, false, err
	}
	for _, entry := range disk.Identities {
		if entry.IdentityID == identityID {
			return cloneBundle(entry.History), true, nil
		}
	}
	return HistoryBundle{}, false, nil
}

// List returns deterministic, verified projections for all registered IDs.
func (r *HistoryRegistry) List() ([]RegisteredIdentity, error) {
	if r == nil {
		return nil, ErrCorruptStore
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := lockGlobalIdentity(r.root)
	if err != nil {
		return nil, err
	}
	defer unlock()
	disk, err := r.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return []RegisteredIdentity{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]RegisteredIdentity, 0, len(disk.Identities))
	for _, entry := range disk.Identities {
		stored, decodeErr := decodeHistoryBundle(entry.History)
		if decodeErr != nil {
			return nil, ErrCorruptStore
		}
		out = append(out, historyProjection(stored, entry.VerifiedAt))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IdentityID < out[j].IdentityID })
	return out, nil
}

// AuthorityProjection returns the current cryptographically verified public
// key set for an identity. A frozen history is returned as frozen so callers
// can fail closed; it is never silently resolved to a branch.
func (r *HistoryRegistry) AuthorityProjection(identityID string) (AuthorityProjection, error) {
	return r.authorityProjection(identityID, "")
}

// AuthorityProjectionAt returns an event-time key projection. Callers must
// also consult the current projection before authorizing a sensitive action.
func (r *HistoryRegistry) AuthorityProjectionAt(identityID, headHash string) (AuthorityProjection, error) {
	return r.authorityProjection(identityID, headHash)
}

func (r *HistoryRegistry) authorityProjection(identityID, headHash string) (AuthorityProjection, error) {
	bundle, verifiedAt, found, err := r.getEntry(identityID)
	if err != nil {
		return AuthorityProjection{}, err
	}
	if !found {
		return AuthorityProjection{}, ErrHistoryInvalid
	}
	stored, err := decodeHistoryBundle(bundle)
	if err != nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	return projectionAt(stored, headHash, verifiedAt)
}

func (r *HistoryRegistry) getEntry(identityID string) (HistoryBundle, time.Time, bool, error) {
	if r == nil {
		return HistoryBundle{}, time.Time{}, false, ErrCorruptStore
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := lockGlobalIdentity(r.root)
	if err != nil {
		return HistoryBundle{}, time.Time{}, false, err
	}
	defer unlock()
	disk, err := r.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return HistoryBundle{}, time.Time{}, false, nil
	}
	if err != nil {
		return HistoryBundle{}, time.Time{}, false, err
	}
	for _, entry := range disk.Identities {
		if entry.IdentityID == identityID {
			return cloneBundle(entry.History), entry.VerifiedAt, true, nil
		}
	}
	return HistoryBundle{}, time.Time{}, false, nil
}

func historyProjection(stored record, verifiedAt time.Time) RegisteredIdentity {
	head, _ := identityprotocol.VerifyEvent(stored.History[len(stored.History)-1].Canonical, stored.History[len(stored.History)-1].Signature)
	nodes, _ := activeNodesAt(stored.History)
	return RegisteredIdentity{IdentityID: stored.IdentityID, Sequence: head.Sequence, HeadHash: head.EventHash, Frozen: stored.Frozen, ForkCount: len(stored.ForkEvidence), NodeIDs: nodes, VerifiedAt: verifiedAt}
}

func projectionAt(stored record, headHash string, verifiedAt time.Time) (AuthorityProjection, error) {
	position := len(stored.History) - 1
	if headHash != "" {
		position = -1
		for i, entry := range stored.History {
			if identityprotocol.EventHash(entry.Canonical) == headHash {
				position = i
				break
			}
		}
		if position < 0 {
			return AuthorityProjection{}, ErrHistoryInvalid
		}
	}
	prefix := stored.History[:position+1]
	genesis, err := identityprotocol.VerifyEvent(prefix[0].Canonical, prefix[0].Signature)
	if err != nil || genesis.Operation != "genesis" {
		return AuthorityProjection{}, ErrCorruptStore
	}
	head, err := identityprotocol.VerifyEvent(prefix[len(prefix)-1].Canonical, prefix[len(prefix)-1].Signature)
	if err != nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	activeNodes, err := activeNodePublicKeysAt(prefix)
	if err != nil {
		return AuthorityProjection{}, err
	}
	recoveryKey, err := recoveryEncryptionPublicKey(genesis.Payload)
	if err != nil {
		return AuthorityProjection{}, ErrCorruptStore
	}
	nodes := make([]ActiveNodeKey, 0, len(activeNodes))
	for nodeID, key := range activeNodes {
		nodes = append(nodes, ActiveNodeKey{NodeID: nodeID, PublicKey: append([]byte(nil), key...)})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	return AuthorityProjection{
		IdentityID: stored.IdentityID, HeadHash: head.EventHash,
		RootPublicKey:               append([]byte(nil), genesis.SignerPublicKey...),
		RecoveryEncryptionPublicKey: append([]byte(nil), recoveryKey...),
		ActiveNodes:                 nodes, Frozen: stored.Frozen, VerifiedAt: verifiedAt,
	}, nil
}

func cloneBundle(input HistoryBundle) HistoryBundle {
	output := HistoryBundle{Format: input.Format, IdentityID: input.IdentityID, Events: append([]HistoryEvent(nil), input.Events...), ForkEvidence: append([]HistoryFork(nil), input.ForkEvidence...)}
	for i := range output.ForkEvidence {
		output.ForkEvidence[i].FirstBranch = append([]HistoryEvent(nil), input.ForkEvidence[i].FirstBranch...)
		output.ForkEvidence[i].SecondBranch = append([]HistoryEvent(nil), input.ForkEvidence[i].SecondBranch...)
	}
	return output
}

func (r *HistoryRegistry) registryPath() string {
	return filepath.Join(r.root, "security", historyRegistryFile)
}

func (r *HistoryRegistry) readLocked() (historyRegistryDisk, error) {
	path := r.registryPath()
	info, err := os.Lstat(path)
	if err != nil {
		return historyRegistryDisk{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return historyRegistryDisk{}, ErrCorruptStore
	}
	f, err := os.Open(path)
	if err != nil {
		return historyRegistryDisk{}, ErrCorruptStore
	}
	opened, statErr := f.Stat()
	if statErr != nil || !os.SameFile(info, opened) {
		_ = f.Close()
		return historyRegistryDisk{}, ErrCorruptStore
	}
	data, readErr := io.ReadAll(io.LimitReader(f, maxRegistryBytes+1))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil || len(data) > maxRegistryBytes {
		return historyRegistryDisk{}, ErrCorruptStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var disk historyRegistryDisk
	if err := decoder.Decode(&disk); err != nil {
		return historyRegistryDisk{}, ErrCorruptStore
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return historyRegistryDisk{}, ErrCorruptStore
	}
	if err := validateHistoryRegistry(disk); err != nil {
		return historyRegistryDisk{}, err
	}
	return disk, nil
}

func validateHistoryRegistry(disk historyRegistryDisk) error {
	if disk.Version != historyRegistryVersion || len(disk.Identities) > maxRegistryIdentities {
		return ErrCorruptStore
	}
	seen := make(map[string]struct{}, len(disk.Identities))
	eventCount := 0
	for _, entry := range disk.Identities {
		if entry.IdentityID == "" || entry.IdentityID != entry.History.IdentityID || entry.VerifiedAt.IsZero() || entry.VerifiedAt.Location() != time.UTC {
			return ErrCorruptStore
		}
		if _, duplicate := seen[entry.IdentityID]; duplicate {
			return ErrCorruptStore
		}
		seen[entry.IdentityID] = struct{}{}
		stored, err := decodeHistoryBundle(entry.History)
		if err != nil || stored.IdentityID != entry.IdentityID {
			return ErrCorruptStore
		}
		eventCount += len(entry.History.Events)
		for _, fork := range entry.History.ForkEvidence {
			firstCount, secondCount := len(fork.FirstBranch), len(fork.SecondBranch)
			if firstCount == 0 {
				firstCount = 1 // Legacy evidence stores only the direct child.
			}
			if secondCount == 0 {
				secondCount = 1
			}
			eventCount += firstCount + secondCount
		}
		if eventCount > maxRegistryEvents {
			return ErrCorruptStore
		}
	}
	return nil
}

func (r *HistoryRegistry) writeLocked(disk historyRegistryDisk) error {
	if disk.Version == 0 {
		disk.Version = historyRegistryVersion
	}
	if err := validateHistoryRegistry(disk); err != nil {
		return err
	}
	data, err := json.Marshal(disk)
	if err != nil || len(data) > maxRegistryBytes {
		return ErrRegistryFull
	}
	path := r.registryPath()
	if info, statErr := os.Lstat(path); statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return ErrCorruptStore
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return ErrCorruptStore
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".global-identity-registry-*.tmp")
	if err != nil {
		return errors.New("global identity registry could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return errors.New("global identity registry permissions could not be secured")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("global identity registry could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("global identity registry could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("global identity registry could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("global identity registry could not be published")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("global identity registry directory could not be synchronized")
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("global identity registry directory could not be synchronized")
	}
	return nil
}
