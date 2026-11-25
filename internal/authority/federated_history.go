package authority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/integrated-recorder/core/internal/discovery"
)

// ExportFederatedHistory returns only public signed OwnerSubject evidence. It
// validates the local log before encoding it; private keys and recovery
// material are never part of this representation.
func (s *Store) ExportFederatedHistory(subjectID string, resolve AuthorityIdentityResolver, now time.Time) (discovery.OwnerAuthorityHistoryBundle, error) {
	snapshot, err := s.Snapshot(subjectID, resolve, now)
	if err != nil {
		return discovery.OwnerAuthorityHistoryBundle{}, err
	}
	events, forks, err := s.ExportEvidence(subjectID)
	if err != nil {
		return discovery.OwnerAuthorityHistoryBundle{}, err
	}
	bundle := discovery.OwnerAuthorityHistoryBundle{
		Version: 1, Identity: snapshot.Subject.Controller.ID, Subject: subjectID,
		Events: make([]json.RawMessage, 0, len(events)), ForkEvents: make([]json.RawMessage, 0, len(forks)),
	}
	for _, event := range events {
		encoded, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return discovery.OwnerAuthorityHistoryBundle{}, ErrInvalid
		}
		bundle.Events = append(bundle.Events, encoded)
	}
	for _, fork := range forks {
		// The accepted branch event is already in Events. Carrying the
		// competing signed successor is sufficient for a recipient to persist
		// the fork through Store.Apply without any branch selection.
		encoded, marshalErr := json.Marshal(fork.Second)
		if marshalErr != nil {
			return discovery.OwnerAuthorityHistoryBundle{}, ErrInvalid
		}
		bundle.ForkEvents = append(bundle.ForkEvents, encoded)
	}
	return bundle, nil
}

// SynchronizeFederatedHistory fetches every configured authority mirror on
// each remote authorization boundary, validates every signed event through
// Store.Apply, persists valid fork evidence, and requires the local durable
// head to match one of the fully agreed current mirror histories. Provider
// agreement is an operational freshness signal, not cryptographic consensus.
func (s *Store) SynchronizeFederatedHistory(ctx context.Context, subjectID string, federation *discovery.FederationClient, resolve AuthorityIdentityResolver, now time.Time) error {
	if s == nil || ctx == nil || federation == nil || resolve == nil || now.IsZero() {
		return ErrAuthorityStale
	}
	lookup, lookupErr := federation.LookupOwnerAuthorityHistories(ctx, subjectID)
	var applyErr error
	forkFound := false
	for _, raw := range lookup.Histories {
		var bundle discovery.OwnerAuthorityHistoryBundle
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&bundle) != nil || decoder.Decode(&struct{}{}) != io.EOF || bundle.Version != 1 || bundle.Subject != subjectID || len(bundle.Events) == 0 {
			applyErr = errors.Join(applyErr, ErrInvalid)
			continue
		}
		if err := s.ImportFederatedHistory(bundle, resolve, now); err != nil {
			if errors.Is(err, ErrAuthorityFork) {
				forkFound = true
			} else {
				applyErr = errors.Join(applyErr, err)
			}
		}
	}
	if lookupErr != nil || !lookup.Complete || !lookup.Agreed || applyErr != nil {
		if forkFound || errors.Is(applyErr, ErrAuthorityFork) {
			return ErrAuthorityFork
		}
		return ErrAuthorityStale
	}
	local, err := s.ExportFederatedHistory(subjectID, resolve, now)
	if err != nil {
		return ErrAuthorityStale
	}
	if len(local.ForkEvents) > 0 {
		return ErrAuthorityFork
	}
	localSnapshot, err := s.Snapshot(subjectID, resolve, now)
	if err != nil || localSnapshot.Frozen {
		return ErrAuthorityFork
	}
	for _, raw := range lookup.Histories {
		var candidate discovery.OwnerAuthorityHistoryBundle
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&candidate) != nil || decoder.Decode(&struct{}{}) != io.EOF || candidate.Identity != local.Identity || candidate.Subject != local.Subject {
			continue
		}
		if sameSignedEventSequence(local.Events, candidate.Events) {
			return nil
		}
	}
	return ErrAuthorityStale
}

func sameSignedEventSequence(local []json.RawMessage, candidate []json.RawMessage) bool {
	if len(local) != len(candidate) {
		return false
	}
	for i := range local {
		var a, b SignedAuthorityEvent
		if json.Unmarshal(local[i], &a) != nil || json.Unmarshal(candidate[i], &b) != nil {
			return false
		}
		aHash, aErr := AuthorityEventHash(a.Event)
		bHash, bErr := AuthorityEventHash(b.Event)
		if aErr != nil || bErr != nil || aHash != bHash || !bytes.Equal(a.Signature, b.Signature) {
			return false
		}
	}
	return true
}
