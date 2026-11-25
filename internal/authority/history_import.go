package authority

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/discovery"
)

// ImportFederatedHistory validates and atomically merges one complete signed
// OwnerSubject history. Unlike Apply, import accepts historical identity
// heads for already-signed events, but replay verifies each event against its
// signed historical head and validates the current identity before commit.
func (s *Store) ImportFederatedHistory(bundle discovery.OwnerAuthorityHistoryBundle, resolve AuthorityIdentityResolver, now time.Time) error {
	if s == nil || resolve == nil || now.IsZero() || bundle.Version != 1 || !identityIDPattern.MatchString(bundle.Identity) || !subjectIDPattern.MatchString(bundle.Subject) || len(bundle.Events) == 0 || len(bundle.Events) > maxAuthorityEvents || len(bundle.ForkEvents) > maxAuthorityForks {
		return ErrInvalid
	}
	candidate := authoritySubjectDisk{SubjectID: bundle.Subject, Events: make([]SignedAuthorityEvent, 0, len(bundle.Events))}
	for _, raw := range bundle.Events {
		event, err := decodeFederatedAuthorityEvent(raw, bundle.Identity, bundle.Subject)
		if err != nil {
			return err
		}
		candidate.Events = append(candidate.Events, event)
	}
	for _, raw := range bundle.ForkEvents {
		second, err := decodeFederatedAuthorityEvent(raw, bundle.Identity, bundle.Subject)
		if err != nil {
			return err
		}
		firstIndex := -1
		for i, event := range candidate.Events {
			if event.Event.Sequence == second.Event.Sequence && event.Event.PreviousHash == second.Event.PreviousHash {
				firstIndex = i
				break
			}
		}
		if firstIndex < 0 {
			return ErrInvalid
		}
		first := candidate.Events[firstIndex]
		firstHash, firstErr := AuthorityEventHash(first.Event)
		secondHash, secondErr := AuthorityEventHash(second.Event)
		if firstErr != nil || secondErr != nil || firstHash == secondHash {
			return ErrInvalid
		}
		candidate.Forks = append(candidate.Forks, authorityFork{PreviousHash: second.Event.PreviousHash, First: first, Second: second})
	}
	if _, err := validateSubjectLog(candidate, resolve, now); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := s.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		stored = authorityDisk{Version: 1, Subjects: []authoritySubjectDisk{}}
	} else if err != nil {
		return err
	}
	index := subjectRecordIndex(stored.Subjects, bundle.Subject)
	if err := ensureImportedArchiveClaimsAvailable(stored.Subjects, candidate, resolve, now); err != nil {
		return err
	}
	if index < 0 {
		stored.Subjects = append(stored.Subjects, candidate)
		sort.Slice(stored.Subjects, func(i, j int) bool { return stored.Subjects[i].SubjectID < stored.Subjects[j].SubjectID })
		if err := s.writeLocked(stored); err != nil {
			return err
		}
		if len(candidate.Forks) > 0 {
			return ErrAuthorityFork
		}
		return nil
	}

	existing := stored.Subjects[index]
	if _, err := validateSubjectLog(existing, resolve, now); err != nil {
		return err
	}
	merged := cloneAuthoritySubject(existing)
	common := 0
	for common < len(merged.Events) && common < len(candidate.Events) {
		a, aErr := AuthorityEventHash(merged.Events[common].Event)
		b, bErr := AuthorityEventHash(candidate.Events[common].Event)
		if aErr != nil || bErr != nil || a != b || !bytes.Equal(merged.Events[common].Signature, candidate.Events[common].Signature) {
			break
		}
		common++
	}
	switch {
	case common < len(merged.Events) && common < len(candidate.Events):
		first, second := merged.Events[common], candidate.Events[common]
		if !forkEventAlreadyRecorded(merged.Forks, second) {
			if len(merged.Forks) >= maxAuthorityForks {
				return ErrAuthorityLimit
			}
			merged.Forks = append(merged.Forks, authorityFork{PreviousHash: first.Event.PreviousHash, First: first, Second: second})
		}
	case len(merged.Forks) == 0 && common == len(merged.Events) && len(candidate.Events) > len(merged.Events):
		merged.Events = append(merged.Events, candidate.Events[len(merged.Events):]...)
	case common == len(candidate.Events):
		// The incoming history is an already-known prefix. Keep the longer
		// durable head and merge only independently valid fork evidence.
	}
	for _, incomingFork := range candidate.Forks {
		if forkEventAlreadyRecorded(merged.Forks, incomingFork.Second) {
			continue
		}
		if !acceptedEventPresent(merged.Events, incomingFork.First) {
			continue
		}
		if len(merged.Forks) >= maxAuthorityForks {
			return ErrAuthorityLimit
		}
		merged.Forks = append(merged.Forks, incomingFork)
	}
	if len(merged.Events) > maxAuthorityEvents {
		return ErrAuthorityLimit
	}
	if _, err := validateSubjectLog(merged, resolve, now); err != nil {
		return err
	}
	if !sameAuthoritySubject(existing, merged) {
		stored.Subjects[index] = merged
		if err := s.writeLocked(stored); err != nil {
			return err
		}
	}
	if len(merged.Forks) > 0 {
		return ErrAuthorityFork
	}
	return nil
}

func decodeFederatedAuthorityEvent(raw []byte, identityID, subjectID string) (SignedAuthorityEvent, error) {
	var event SignedAuthorityEvent
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&event) != nil || decoder.Decode(&struct{}{}) != io.EOF || event.Event.IdentityID != identityID || event.Event.SubjectID != subjectID {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	if _, err := CanonicalAuthorityEvent(event.Event); err != nil || len(event.Signature) != 64 {
		return SignedAuthorityEvent{}, ErrInvalid
	}
	return event, nil
}

func ensureImportedArchiveClaimsAvailable(subjects []authoritySubjectDisk, candidate authoritySubjectDisk, resolve AuthorityIdentityResolver, now time.Time) error {
	candidateState, err := validateSubjectLog(candidate, resolve, now)
	if err != nil {
		return err
	}
	for _, archiveID := range candidateState.ArchiveIDs {
		if err := ensureArchiveUnclaimedExcept(subjects, archiveID, candidate.SubjectID, resolve, now); err != nil {
			return err
		}
	}
	for _, fork := range candidate.Forks {
		for _, archiveID := range candidateForkArchiveClaims(fork) {
			if err := ensureArchiveUnclaimedExcept(subjects, archiveID, candidate.SubjectID, resolve, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureArchiveUnclaimedExcept(subjects []authoritySubjectDisk, archiveID, exceptSubject string, resolve AuthorityIdentityResolver, now time.Time) error {
	if !archiveIDPattern.MatchString(archiveID) {
		return ErrInvalid
	}
	for _, record := range subjects {
		if record.SubjectID == exceptSubject {
			continue
		}
		state, err := validateSubjectLog(record, resolve, now)
		if err != nil {
			return err
		}
		if containsArchiveID(state.ArchiveIDs, archiveID) {
			return ErrArchiveClaimed
		}
		for _, fork := range record.Forks {
			if eventClaimsArchive(fork.First.Event, archiveID) || eventClaimsArchive(fork.Second.Event, archiveID) {
				return ErrArchiveClaimed
			}
		}
	}
	return nil
}

func candidateForkArchiveClaims(fork authorityFork) []string {
	result := []string{}
	for _, event := range []SignedAuthorityEvent{fork.First, fork.Second} {
		switch event.Event.Operation {
		case AuthorityCreateSubject:
			var payload SubjectCreatePayload
			if decodeAuthorityPayload(event.Event.Payload, &payload) == nil {
				result = append(result, payload.ArchiveIDs...)
			}
		case AuthorityClaimArchive:
			var payload ArchiveClaimPayload
			if decodeAuthorityPayload(event.Event.Payload, &payload) == nil {
				result = append(result, payload.ArchiveID)
			}
		}
	}
	return result
}

func acceptedEventPresent(events []SignedAuthorityEvent, candidate SignedAuthorityEvent) bool {
	candidateHash, err := AuthorityEventHash(candidate.Event)
	if err != nil {
		return false
	}
	for _, event := range events {
		hash, hashErr := AuthorityEventHash(event.Event)
		if hashErr == nil && hash == candidateHash && bytes.Equal(event.Signature, candidate.Signature) {
			return true
		}
	}
	return false
}

func forkEventAlreadyRecorded(forks []authorityFork, candidate SignedAuthorityEvent) bool {
	hash, err := AuthorityEventHash(candidate.Event)
	if err != nil {
		return false
	}
	for _, fork := range forks {
		for _, event := range []SignedAuthorityEvent{fork.First, fork.Second} {
			existing, existingErr := AuthorityEventHash(event.Event)
			if existingErr == nil && existing == hash && bytes.Equal(event.Signature, candidate.Signature) {
				return true
			}
		}
	}
	return false
}

func cloneAuthoritySubject(record authoritySubjectDisk) authoritySubjectDisk {
	clone := authoritySubjectDisk{SubjectID: record.SubjectID, Events: append([]SignedAuthorityEvent(nil), record.Events...), Forks: append([]authorityFork(nil), record.Forks...)}
	for i := range clone.Events {
		clone.Events[i].Signature = append([]byte(nil), record.Events[i].Signature...)
	}
	for i := range clone.Forks {
		clone.Forks[i].First.Signature = append([]byte(nil), record.Forks[i].First.Signature...)
		clone.Forks[i].Second.Signature = append([]byte(nil), record.Forks[i].Second.Signature...)
	}
	return clone
}

func sameAuthoritySubject(a, b authoritySubjectDisk) bool {
	if len(a.Events) != len(b.Events) || len(a.Forks) != len(b.Forks) {
		return false
	}
	for i := range a.Events {
		aHash, aErr := AuthorityEventHash(a.Events[i].Event)
		bHash, bErr := AuthorityEventHash(b.Events[i].Event)
		if aErr != nil || bErr != nil || aHash != bHash || !bytes.Equal(a.Events[i].Signature, b.Events[i].Signature) {
			return false
		}
	}
	for i := range a.Forks {
		if !forkEventAlreadyRecorded([]authorityFork{a.Forks[i]}, b.Forks[i].First) || !forkEventAlreadyRecorded([]authorityFork{a.Forks[i]}, b.Forks[i].Second) {
			return false
		}
	}
	return true
}
