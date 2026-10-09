package acquire

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/hls"
	"github.com/integrated-recorder/core/internal/storage"
)

const maxHistoricalRecoveryWorkPerPass = 128

type historicalRepairProgress struct {
	failed       int
	acquired     int
	knownMissing int
	more         bool
	needsRecheck bool
}

type historicalRecoveryWork struct {
	source     hls.MediaSegment
	coordinate archiveindex.Coordinate
	epoch      uint64
	gap        bool
}

type historicalSourceCoordinate struct {
	discontinuity uint64
	sequence      uint64
}

type historicalSourceTarget struct {
	index    int
	sequence uint64
}

type shardedHistoricalInitKey struct {
	epoch         uint64
	discontinuity uint64
	sourceURI     string
}

type retryableHistoricalError struct{ cause error }

func (e *retryableHistoricalError) Error() string { return e.cause.Error() }
func (e *retryableHistoricalError) Unwrap() error { return e.cause }

func retryableHistorical(cause error) error {
	if cause == nil {
		cause = ErrHistoricalUnavailable
	}
	return &retryableHistoricalError{cause: cause}
}

func retryableHistoricalFetch(err error) bool {
	var fetchErr *FetchError
	return errors.As(err, &fetchErr) && fetchErr.Retryable
}

// RepairDeclaredHistory acquires only coordinates declared by the selected
// adapter's HistoricalAvailability. It accepts no caller-supplied URL. A
// stopped archive requires a fresh Host owner; an active archive requires its
// exact live worker token.
func (m *Manager) RepairDeclaredHistory(ctx context.Context, owner OwnershipToken, id string) (returnErr error) {
	return m.repairDeclaredHistoryCore(ctx, owner, id, nil, true)
}

func (m *Manager) repairDeclaredHistoryPass(ctx context.Context, owner OwnershipToken, id string) (historicalRepairProgress, error) {
	var progress historicalRepairProgress
	err := m.repairDeclaredHistoryCore(ctx, owner, id, &progress, false)
	return progress, err
}

func (m *Manager) repairDeclaredHistoryCore(ctx context.Context, owner OwnershipToken, id string, progress *historicalRepairProgress, releaseTerminalOwner bool) (returnErr error) {
	if ctx == nil {
		return context.Canceled
	}
	ctx = withHistoricalAcquisitionPriority(ctx)
	if !validOwnershipToken(owner) || owner.RecordingID != id {
		return ErrInvalidOwnershipToken
	}
	e, ok := m.entry(id)
	if !ok {
		return storage.ErrNotFound
	}
	media, generation, active, err := m.repairMediaContext(e, owner)
	if err != nil {
		return err
	}
	ownerAdopted := false
	if !active {
		if err := m.withCanonicalMutationOwner(e, &owner, true, func() error {
			ownerAdopted = true
			root := m.recordingSnapshotForArchive(e)
			if root == nil {
				return storage.ErrNotFound
			}
			if root.ArchiveSealed {
				return ErrArchiveSealed
			}
			return nil
		}); err != nil {
			if ownerAdopted {
				if releaseErr := m.releaseRepairOwner(ctx, e, owner); releaseErr != nil {
					return errors.Join(err, releaseErr)
				}
			}
			return err
		}
		if releaseTerminalOwner {
			defer func() {
				if !ownerAdopted {
					return
				}
				if releaseErr := m.releaseRepairOwner(ctx, e, owner); returnErr == nil && releaseErr != nil {
					returnErr = releaseErr
				}
			}()
		}
	}
	if media.HistoricalAvailability == nil {
		return ErrHistoricalUnavailable
	}
	if err := media.HistoricalAvailability.Validate(); err != nil {
		return ErrHistoricalUnavailable
	}
	if due, _ := proactiveRefreshDue(media, time.Now()); due {
		media, generation, err = m.refreshMediaAtGenerationOwned(ctx, e, media, generation, &owner, terminalRepairMutationAllowed(e, !active))
		if err != nil {
			return retryableHistorical(errors.New("historical source refresh failed"))
		}
	}
	root := m.recordingSnapshotForArchive(e)
	if root == nil {
		return storage.ErrNotFound
	}
	if root.ArchiveSealed {
		return ErrArchiveSealed
	}
	identity, err := archiveSessionIdentity(root, root.AdapterID, media.SessionRef)
	if err != nil {
		return ErrHistoricalUnavailable
	}
	manifestURL := media.HistoricalAvailability.HistoricalManifestURL
	if manifestURL == "" {
		manifestURL = media.ManifestURL
	}
	historicalMedia := cloneMediaSource(media)
	historicalMedia.ManifestURL = manifestURL
	manifest, requestedManifestURL, err := m.fetchManifestForMedia(ctx, id, historicalMedia, manifestURL, manifestURL, adapterproto.RequestScopeManifest)
	if err != nil {
		if m.shouldRefresh(historicalMedia, err) {
			if _, _, refreshErr := m.refreshMediaAtGenerationOwned(ctx, e, media, generation, &owner, terminalRepairMutationAllowed(e, !active)); refreshErr != nil {
				return retryableHistorical(errors.New("historical source refresh failed"))
			}
			return retryableHistorical(errors.New("historical source refreshed; retry recovery pass"))
		}
		var fetchErr *FetchError
		if errors.As(err, &fetchErr) && fetchErr.Retryable {
			return retryableHistorical(ErrHistoricalUnavailable)
		}
		return ErrHistoricalUnavailable
	}
	if manifest == nil {
		return ErrHistoricalUnavailable
	}
	manifestURL = requestedManifestURL
	body := manifest.Bytes()
	manifest.Release()
	if hls.IsMasterPlaylist(body) {
		master, parseErr := hls.ParseMaster(body, requestedManifestURL)
		if parseErr != nil {
			return ErrHistoricalUnavailable
		}
		variant, selectErr := selectVariant(master)
		if selectErr != nil {
			return ErrHistoricalUnavailable
		}
		manifest, manifestURL, err = m.fetchManifestForMedia(ctx, id, historicalMedia, requestedManifestURL, variant.URI, adapterproto.RequestScopeVariant)
		if err != nil || manifest == nil {
			if manifest != nil {
				manifest.Release()
			}
			if m.shouldRefresh(historicalMedia, err) {
				if _, _, refreshErr := m.refreshMediaAtGenerationOwned(ctx, e, media, generation, &owner, terminalRepairMutationAllowed(e, !active)); refreshErr != nil {
					return retryableHistorical(errors.New("historical source refresh failed"))
				}
				return retryableHistorical(errors.New("historical source refreshed; retry recovery pass"))
			}
			var fetchErr *FetchError
			if errors.As(err, &fetchErr) && fetchErr.Retryable {
				return retryableHistorical(ErrHistoricalUnavailable)
			}
			return ErrHistoricalUnavailable
		}
		body = manifest.Bytes()
		manifest.Release()
	}
	playlist, err := hls.ParseMedia(body, manifestURL)
	if err != nil {
		return ErrHistoricalUnavailable
	}
	if len(playlist.Segments) == 0 {
		if progress != nil {
			progress.needsRecheck = historicalDeclarationNeedsRecheck(media.HistoricalAvailability, playlist.Segments)
		}
		return nil
	}
	if progress != nil {
		progress.needsRecheck = historicalDeclarationNeedsRecheck(media.HistoricalAvailability, playlist.Segments)
	}
	if playlist.EndList {
		// EndList describes the source window; it does not seal the archive.
	}
	trackID := "main"
	rootTrack := root.Tracks[trackID]
	if rootTrack == nil {
		return errors.New("main track is missing")
	}
	var inventory archiveindex.Inventory
	var shardedEpochs []uint64
	var shardedStates []archiveindex.CoverageState
	selected := historicalAvailabilitySelection(media.HistoricalAvailability, playlist.Segments, time.Now())
	if isShardedRecording(root) {
		shardedEpochs, shardedStates, err = m.resolveShardedRecoveryCoordinates(ctx, id, rootTrack, playlist.Segments, selected)
		if err != nil {
			return err
		}
		if err := m.populateShardedHistoricalInitIdentities(ctx, id, rootTrack, playlist.Segments, selected, shardedEpochs); err != nil {
			return err
		}
	} else {
		inventory, err = m.ArchiveInventory(id)
		if err != nil {
			return err
		}
	}
	candidates := make([]historicalRecoveryWork, 0, min(len(playlist.Segments), maxHistoricalRecoveryWorkPerPass))
	work := 0
	plannedInit := make(map[string]struct{})
	for index, source := range playlist.Segments {
		if !selected[index] {
			continue
		}
		epoch := historicalSourceEpoch(root, source)
		state := archiveindex.CoverageUnknown
		if isShardedRecording(root) {
			epoch = shardedEpochs[index]
			state = shardedStates[index]
		} else {
			coordinate := archiveindex.Coordinate{
				SessionID: identity.ID, TrackID: trackID, SourceEpoch: epoch,
				DiscontinuitySequence: source.DiscontinuitySequence, Sequence: source.Sequence,
				Kind: archiveindex.ObjectMedia,
			}
			state = archiveindex.CoverageAt(inventory, coordinate)
		}
		coordinate := archiveindex.Coordinate{
			SessionID: identity.ID, TrackID: trackID, SourceEpoch: epoch,
			DiscontinuitySequence: source.DiscontinuitySequence, Sequence: source.Sequence,
			Kind: archiveindex.ObjectMedia,
		}
		if state == archiveindex.CoveragePresent || state == archiveindex.CoverageConflict {
			continue
		}
		if source.Gap {
			if progress != nil {
				progress.knownMissing++
				progress.needsRecheck = true
			}
			if state == archiveindex.CoverageKnownMissing {
				continue
			}
			if work == maxHistoricalRecoveryWorkPerPass {
				if progress != nil {
					progress.more = true
				}
				break
			}
			candidates = append(candidates, historicalRecoveryWork{source: source, coordinate: coordinate, epoch: epoch, gap: true})
			work++
			continue
		}
		candidateCost := 1 // one media coordinate, including failure publication
		initID := ""
		if source.Init != nil && historicalInitIdentity(rootTrack, source, epoch) == "" {
			initID = initSegmentID(*source.Init, epoch, source.DiscontinuitySequence)
			asset := domain.Segment{
				TrackID: trackID, Sequence: source.Sequence, SourceEpoch: epoch,
				DiscontinuitySequence: source.DiscontinuitySequence,
				SourceURI:             source.Init.URI, ByteRange: cloneRange(source.Init.ByteRange), IsInit: true,
			}
			if _, exists := findRootSegment(root, coordinateForSegment(identity.ID, asset)); !exists {
				if _, alreadyPlanned := plannedInit[initID]; !alreadyPlanned {
					candidateCost++
				}
			}
		}
		if work+candidateCost > maxHistoricalRecoveryWorkPerPass {
			if progress != nil {
				progress.more = true
			}
			break
		}
		work += candidateCost
		if initID != "" {
			plannedInit[initID] = struct{}{}
		}
		candidates = append(candidates, historicalRecoveryWork{source: source, coordinate: coordinate, epoch: epoch})
	}
	for cursor := 0; cursor < len(candidates); {
		candidate := candidates[cursor]
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.withOwnershipCommit(&owner, func() error { return nil }); err != nil {
			return err
		}
		if active {
			_, currentGeneration := currentMediaVersion(e)
			if currentGeneration != generation {
				return errStaleMediaGeneration
			}
		}
		if candidate.gap {
			if err := m.recordHistoricalCoverage(e, &owner, terminalRepairMutationAllowed(e, !active), candidate.coordinate, archiveindex.CoverageKnownMissing, "source manifest declared media missing"); err != nil {
				return err
			}
			cursor++
			continue
		}

		windowCapacity := m.historicalWindowCapacity()
		if windowCapacity < 1 {
			return retryableHistorical(errHistoricalScratch)
		}
		windowSize, err := m.historicalFetch.waitWindow(ctx, windowCapacity)
		if err != nil {
			return err
		}
		windowEnd := cursor
		for windowEnd < len(candidates) && windowEnd-cursor < windowSize && !candidates[windowEnd].gap {
			windowEnd++
		}
		results, cleanup, fetchErr := m.fetchHistoricalWindow(ctx, id, historicalMedia, e, generation, candidates[cursor:windowEnd])
		if fetchErr != nil {
			return fetchErr
		}
		windowErr := func() error {
			for offset := range results {
				candidate := candidates[cursor+offset]
				result := results[offset]
				source, epoch, coordinate := candidate.source, candidate.epoch, candidate.coordinate
				if result.err != nil {
					if errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded) {
						return result.err
					}
					if errors.Is(result.err, storage.ErrIngestCoordinatorUnavailable) {
						return retryableHistorical(result.err)
					}
					if errors.Is(result.err, errHistoricalScratch) {
						return retryableHistorical(errHistoricalScratch)
					}
					if m.shouldRefresh(historicalMedia, result.err) {
						if _, _, refreshErr := m.refreshMediaAtGenerationOwned(ctx, e, historicalMedia, generation, &owner, terminalRepairMutationAllowed(e, !active)); refreshErr != nil {
							return retryableHistorical(errors.New("historical source refresh failed"))
						}
						return retryableHistorical(errors.New("historical source refreshed; retry recovery pass"))
					}
					if err := m.recordHistoricalCoverage(e, &owner, terminalRepairMutationAllowed(e, !active), coordinate, archiveindex.CoverageAcquisitionFailed, "historical media acquisition failed"); err != nil {
						return err
					}
					if progress != nil && retryableHistoricalFetch(result.err) {
						progress.failed++
					}
					continue
				}
				initID, initErr := m.acquireHistoricalInit(ctx, e, owner, terminalRepairMutationAllowed(e, !active), historicalMedia, generation, identity.ID, rootTrack, source, epoch)
				if initErr != nil {
					if errors.Is(initErr, storage.ErrIngestCoordinatorUnavailable) {
						return retryableHistorical(initErr)
					}
					if errors.Is(initErr, storage.ErrCanonicalCommitFailed) {
						return initErr
					}
					if m.shouldRefresh(historicalMedia, initErr) {
						if _, _, refreshErr := m.refreshMediaAtGenerationOwned(ctx, e, historicalMedia, generation, &owner, terminalRepairMutationAllowed(e, !active)); refreshErr != nil {
							return retryableHistorical(errors.New("historical source refresh failed"))
						}
						return retryableHistorical(errors.New("historical source refreshed; retry recovery pass"))
					}
					if errors.Is(initErr, errHistoricalScratch) {
						return retryableHistorical(errHistoricalScratch)
					}
					if !errors.Is(initErr, context.Canceled) && !errors.Is(initErr, context.DeadlineExceeded) {
						if err := m.recordHistoricalCoverage(e, &owner, terminalRepairMutationAllowed(e, !active), coordinate, archiveindex.CoverageAcquisitionFailed, "historical initialization media acquisition failed"); err != nil {
							return err
						}
						if progress != nil && retryableHistoricalFetch(initErr) {
							progress.failed++
						}
						continue
					}
					return initErr
				}
				payload, err := result.spool.readPayload(ctx, m.ingest, id)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return err
					}
					if errors.Is(err, storage.ErrIngestCoordinatorUnavailable) {
						return retryableHistorical(err)
					}
					return retryableHistorical(errHistoricalScratch)
				}
				segment := makeArchiveSegment(source, epoch, 0, initID, payload.Result())
				if commitErr := m.commitHistoricalPayload(ctx, e, owner, terminalRepairMutationAllowed(e, !active), segment, payload); commitErr != nil {
					return commitErr
				}
				if progress != nil {
					progress.acquired++
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if active {
					_, generation = currentMediaVersion(e)
				}
				root = m.recordingSnapshotForArchive(e)
				if root == nil {
					return storage.ErrNotFound
				}
				if !isShardedRecording(root) {
					rootTrack = root.Tracks[trackID]
					if rootTrack == nil {
						return errors.New("main track is missing")
					}
				}
				if !isShardedRecording(root) {
					inventory, err = m.ArchiveInventory(id)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}()
		cleanupErr := cleanup()
		if windowErr != nil {
			if cleanupErr != nil {
				return errors.Join(windowErr, retryableHistorical(cleanupErr))
			}
			return windowErr
		}
		if cleanupErr != nil {
			return retryableHistorical(cleanupErr)
		}
		cursor = windowEnd
	}
	return nil
}

// A recovery pass can start while capture is live and finish after Stop has
// made the recording terminal. Re-evaluate this permission at each canonical
// mutation boundary. The shared owner fence still rejects work until the live
// worker has fully quiesced, then allows the same retained token to continue.
func terminalRepairMutationAllowed(e *entry, alreadyTerminal bool) bool {
	if alreadyTerminal {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.recording != nil && e.recording.State != domain.StateRecording
}

func (m *Manager) refreshMediaAtGenerationOwned(ctx context.Context, e *entry, current adapterproto.MediaSource, expectedGeneration uint64, owner *OwnershipToken, terminal bool) (adapterproto.MediaSource, uint64, error) {
	e.mu.Lock()
	if e.refreshGate == nil {
		e.refreshGate = make(chan struct{}, 1)
	}
	gate := e.refreshGate
	e.mu.Unlock()
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return adapterproto.MediaSource{}, 0, ctx.Err()
	}
	defer func() { <-gate }()

	e.mu.Lock()
	if e.mediaGeneration != expectedGeneration {
		latest, generation := cloneMediaSource(e.media), e.mediaGeneration
		e.mu.Unlock()
		return latest, generation, nil
	}
	adapterID, resource := e.adapterID, cloneResourceRef(e.resource)
	e.mu.Unlock()

	var candidate adapterproto.MediaSource
	var stateCommit func() error
	var err error
	if preparer, ok := m.resolver.(RefreshPreparer); ok {
		candidate, stateCommit, err = preparer.PrepareRefresh(ctx, adapterID, resource, cloneMediaSource(current))
	} else if refresher, ok := m.resolver.(Refresher); ok {
		candidate, err = refresher.Refresh(ctx, adapterID, resource, cloneMediaSource(current))
	} else {
		return adapterproto.MediaSource{}, expectedGeneration, errors.New("adapter does not support refresh")
	}
	if err != nil {
		return adapterproto.MediaSource{}, expectedGeneration, errors.New("adapter refresh failed")
	}
	if err := adapterproto.ValidateMediaSource(candidate, []string{"hls"}); err != nil {
		return adapterproto.MediaSource{}, expectedGeneration, errors.New("adapter returned invalid refreshed media source")
	}
	if err := m.validate(ctx, candidate.ManifestURL); err != nil {
		return adapterproto.MediaSource{}, expectedGeneration, errors.New("refreshed media URL is invalid")
	}
	contextJSON, err := mediaContext(candidate)
	if err != nil {
		return adapterproto.MediaSource{}, expectedGeneration, err
	}
	copy := cloneMediaSource(candidate)
	var nextGeneration uint64
	var updatedScheduler *segmentScheduler
	err = m.withCanonicalMutationOwner(e, owner, terminal, func() error {
		e.mu.Lock()
		if e.deleted || e.mediaGeneration != expectedGeneration {
			latest, generation := cloneMediaSource(e.media), e.mediaGeneration
			e.mu.Unlock()
			copy, nextGeneration = latest, generation
			return nil
		}
		e.mu.Unlock()
		if err := m.persistAcquisitionContext(e.recording.ID, contextJSON); err != nil {
			return errors.New("historical acquisition context could not be committed")
		}
		if stateCommit != nil {
			if err := stateCommit(); err != nil {
				return errors.New("adapter refresh state could not be committed")
			}
		}
		if err := m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
			if candidate.SourceURIClassification() == "sensitive" {
				recording.SourceURIClassification = "sensitive"
			}
			return nil
		}); err != nil {
			return err
		}
		e.mu.Lock()
		e.media = cloneMediaSource(candidate)
		e.mediaGeneration++
		nextGeneration = e.mediaGeneration
		updatedScheduler = e.scheduler
		e.mu.Unlock()
		return nil
	})
	if err != nil {
		return adapterproto.MediaSource{}, expectedGeneration, err
	}
	if updatedScheduler != nil {
		updatedScheduler.noteMediaGeneration(nextGeneration)
	}
	if nextGeneration != expectedGeneration {
		// A refreshed historical manifest/context can expose work that the
		// current pass could not see. Promote any delayed retry after publishing
		// the new media generation.
		m.signalAutomaticArchiveRecovery(recordingID(e))
	}
	return copy, nextGeneration, nil
}

// SealArchive is an explicit, fenced transition. Stopped capture remains
// repairable until this operation succeeds.
func (m *Manager) SealArchive(owner OwnershipToken, id string) (returnErr error) {
	if !validOwnershipToken(owner) || owner.RecordingID != id {
		return ErrInvalidOwnershipToken
	}
	e, ok := m.entry(id)
	if !ok {
		return storage.ErrNotFound
	}
	if err := acquireArchiveRecoveryGate(context.Background(), e); err != nil {
		return err
	}
	defer releaseArchiveRecoveryGate(e)
	state, err := terminalLifecycleState(e)
	if err != nil {
		return err
	}
	if state == domain.StateRecording {
		return ErrLifecycleConflict
	}
	if archiveSealed(e) {
		return m.releaseLifecycleOwner(context.Background(), e, owner)
	}
	if err := m.sealArchiveUnderRecoveryGate(context.Background(), e, &owner); err != nil {
		return err
	}
	return m.releaseLifecycleOwner(context.Background(), e, owner)
}

func (m *Manager) sealArchiveUnderRecoveryGate(ctx context.Context, e *entry, owner *OwnershipToken) error {
	return m.withCanonicalMutationOwner(e, owner, true, func() error {
		root := m.recordingSnapshotForArchive(e)
		if root == nil {
			return storage.ErrNotFound
		}
		if isShardedRecording(root) {
			persisted, err := m.store.LoadRecordingHeader(ctx, root.ID)
			if err != nil {
				return err
			}
			if persisted.ShardedArchive.ClaimReconcilePending {
				if err := m.store.ReconcileShardedArchive(ctx, root.ID); err != nil {
					return err
				}
				persisted, err = m.store.LoadRecordingHeader(ctx, root.ID)
				if err != nil {
					return err
				}
				if persisted.ShardedArchive.ClaimReconcilePending {
					return storage.ErrArchiveRecoveryPending
				}
				e.mu.Lock()
				if e.deleted || e.recording == nil {
					e.mu.Unlock()
					return storage.ErrNotFound
				}
				if err := mergeShardedHeader(e.recording, persisted); err != nil {
					e.mu.Unlock()
					return err
				}
				e.mu.Unlock()
			}
			return m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
				if recording.State == domain.StateRecording {
					return ErrLifecycleConflict
				}
				recording.ArchiveSealed = true
				return nil
			})
		}
		inventory, err := m.inventoryForRecording(root, false)
		if err != nil {
			return err
		}
		if err := archiveindex.CloseCapture(&inventory); err != nil {
			return err
		}
		if err := archiveindex.SealArchive(&inventory); err != nil {
			return err
		}
		return m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
			if recording.State == domain.StateRecording {
				return ErrLifecycleConflict
			}
			recording.ArchiveSealed = true
			return nil
		})
	})
}

func (m *Manager) repairMediaContext(e *entry, owner OwnershipToken) (adapterproto.MediaSource, uint64, bool, error) {
	e.mu.Lock()
	if e.recording == nil || e.deleted {
		e.mu.Unlock()
		return adapterproto.MediaSource{}, 0, false, storage.ErrNotFound
	}
	active := e.recording.State == domain.StateRecording
	if active && (!sameOwner(e.ownership, owner) || e.cancel == nil || channelClosed(e.done)) {
		e.mu.Unlock()
		return adapterproto.MediaSource{}, 0, false, ErrHandoverOwnerMismatch
	}
	if !active && (!channelClosed(e.done) || e.cancel != nil) {
		e.mu.Unlock()
		return adapterproto.MediaSource{}, 0, false, ErrActiveRecording
	}
	if !active && e.ownership != nil && *e.ownership != owner && owner.Epoch <= e.ownership.Epoch {
		e.mu.Unlock()
		return adapterproto.MediaSource{}, 0, false, ErrInvalidOwnershipToken
	}
	media, generation := cloneMediaSource(e.media), e.mediaGeneration
	root := clone(e.recording)
	e.mu.Unlock()
	if active {
		if media.ManifestURL == "" || adapterproto.ValidateMediaSource(media, []string{"hls"}) != nil {
			return adapterproto.MediaSource{}, 0, true, ErrHistoricalUnavailable
		}
		return media, generation, true, nil
	}
	media, err := m.loadAcquisitionContext(root.ID)
	if err != nil {
		return adapterproto.MediaSource{}, 0, false, ErrHistoricalUnavailable
	}
	e.mu.Lock()
	e.media = cloneMediaSource(media)
	e.adapterID = root.AdapterID
	e.resource = adapterResource(root.Resource)
	generation = e.mediaGeneration
	e.mu.Unlock()
	return media, generation, false, nil
}

func withinHistoricalAvailability(availability *adapterproto.HistoricalAvailability, source hls.MediaSegment, now time.Time) bool {
	if availability == nil {
		return false
	}
	switch availability.Mode {
	case adapterproto.HistoricalModeSequenceRanges:
		for _, span := range availability.SequenceRanges {
			if source.Sequence >= span.Start && source.Sequence <= span.End {
				return true
			}
		}
	case adapterproto.HistoricalModeTimeRanges:
		if source.ProgramTime == nil {
			return false
		}
		for _, span := range availability.TimeRanges {
			if !source.ProgramTime.Before(span.Start) && source.ProgramTime.Before(span.End) {
				return true
			}
		}
	case adapterproto.HistoricalModeRollingWindow:
		return source.ProgramTime != nil && !source.ProgramTime.Before(now.Add(-time.Duration(availability.WindowSeconds)*time.Second)) && !source.ProgramTime.After(now)
	}
	return false
}

func historicalAvailabilitySelection(availability *adapterproto.HistoricalAvailability, segments []hls.MediaSegment, now time.Time) []bool {
	selected := make([]bool, len(segments))
	if availability == nil {
		return selected
	}
	if availability.Mode == adapterproto.HistoricalModeManifest {
		// A manifest declaration describes only the coordinates observed in
		// this playlist. Absence from a later playlist is not a missing claim.
		for i := range selected {
			selected[i] = true
		}
		return selected
	}
	if availability.Mode != adapterproto.HistoricalModeRollingWindow {
		for i, source := range segments {
			selected[i] = withinHistoricalAvailability(availability, source, now)
		}
		return selected
	}
	allHaveProgramTime := len(segments) > 0
	for i, source := range segments {
		if source.ProgramTime == nil {
			allHaveProgramTime = false
		}
		selected[i] = withinHistoricalAvailability(availability, source, now)
	}
	if allHaveProgramTime {
		return selected
	}
	for i := range selected {
		selected[i] = false
	}
	// Rolling HLS windows commonly omit EXT-X-PROGRAM-DATE-TIME. In that case
	// the manifest's declared order and EXTINF durations bound a suffix window:
	// walk backward from the latest segment and select at most WindowSeconds.
	remaining := float64(availability.WindowSeconds)
	for i := len(segments) - 1; i >= 0 && remaining > 0; i-- {
		if segments[i].Duration <= 0 {
			break
		}
		selected[i] = true
		remaining -= segments[i].Duration
	}
	return selected
}

func hasLaterSelectedHistoricalCandidate(segments []hls.MediaSegment, selected []bool, start int) bool {
	for index := start; index < len(segments) && index < len(selected); index++ {
		if selected[index] && !segments[index].Gap {
			return true
		}
	}
	return false
}

func historicalDeclarationNeedsRecheck(availability *adapterproto.HistoricalAvailability, segments []hls.MediaSegment) bool {
	if availability == nil {
		return false
	}
	switch availability.Mode {
	case adapterproto.HistoricalModeManifest:
		// The advertised set can move or grow without a new source declaration.
		// Recheck slowly after no progress; external events still trigger now.
		return true
	case adapterproto.HistoricalModeSequenceRanges:
		for _, span := range availability.SequenceRanges {
			foundStart, foundEnd := false, false
			for _, segment := range segments {
				if segment.Sequence < span.Start || segment.Sequence > span.End {
					continue
				}
				foundStart = foundStart || segment.Sequence == span.Start
				foundEnd = foundEnd || segment.Sequence == span.End
			}
			if !foundStart || !foundEnd {
				return true
			}
		}
	case adapterproto.HistoricalModeTimeRanges:
		for _, span := range availability.TimeRanges {
			foundStart, foundEnd := false, false
			for _, segment := range segments {
				if segment.ProgramTime == nil || segment.ProgramTime.Before(span.Start) || !segment.ProgramTime.Before(span.End) {
					continue
				}
				foundStart = foundStart || !segment.ProgramTime.After(span.Start)
				foundEnd = true
			}
			if !foundStart || !foundEnd {
				return true
			}
		}
	}
	return false
}

func historicalSourceEpoch(root *domain.Recording, source hls.MediaSegment) uint64 {
	track := root.Tracks["main"]
	if track == nil {
		return 0
	}
	for _, gap := range root.Gaps {
		if gap.TrackID == track.ID && gap.DiscontinuitySequence == source.DiscontinuitySequence && source.Sequence >= gap.FromSequence && source.Sequence <= gap.ToSequence {
			return gap.SourceEpoch
		}
	}
	for _, segment := range track.Segments {
		if segment.DiscontinuitySequence == source.DiscontinuitySequence && segment.Sequence == source.Sequence {
			return segment.SourceEpoch
		}
	}
	bestEpoch, bestDistance := track.SourceEpoch, ^uint64(0)
	for _, segment := range track.Segments {
		if segment.DiscontinuitySequence != source.DiscontinuitySequence {
			continue
		}
		distance := segment.Sequence
		if distance < source.Sequence {
			distance = source.Sequence - distance
		} else {
			distance -= source.Sequence
		}
		if distance < bestDistance {
			bestEpoch, bestDistance = segment.SourceEpoch, distance
		}
	}
	return bestEpoch
}

// resolveShardedRecoveryCoordinates streams durable media, gap, and coverage
// records once. It retains state only for coordinates in the source playlist.
func (m *Manager) resolveShardedRecoveryCoordinates(ctx context.Context, id string, track *domain.Track, sources []hls.MediaSegment, selected []bool) ([]uint64, []archiveindex.CoverageState, error) {
	if track == nil {
		return nil, nil, errors.New("main track is missing")
	}
	targetsByCoordinate := make(map[historicalSourceCoordinate][]int)
	targetsByDiscontinuity := make(map[uint64][]historicalSourceTarget)
	for index, source := range sources {
		if !selected[index] {
			continue
		}
		key := historicalSourceCoordinate{discontinuity: source.DiscontinuitySequence, sequence: source.Sequence}
		targetsByCoordinate[key] = append(targetsByCoordinate[key], index)
		targetsByDiscontinuity[key.discontinuity] = append(targetsByDiscontinuity[key.discontinuity], historicalSourceTarget{index: index, sequence: key.sequence})
	}
	for discontinuity := range targetsByDiscontinuity {
		sort.Slice(targetsByDiscontinuity[discontinuity], func(i, j int) bool {
			left, right := targetsByDiscontinuity[discontinuity][i], targetsByDiscontinuity[discontinuity][j]
			if left.sequence != right.sequence {
				return left.sequence < right.sequence
			}
			return left.index < right.index
		})
	}
	epochs := make([]uint64, len(sources))
	states := make([]archiveindex.CoverageState, len(sources))
	exactMedia := make([]bool, len(sources))
	exactMediaEpoch := make([]uint64, len(sources))
	exactGap := make([]bool, len(sources))
	exactGapEpoch := make([]uint64, len(sources))
	bestDistance := make([]uint64, len(sources))
	bestEpoch := make([]uint64, len(sources))
	newestCoverage := make([]time.Time, len(sources))
	for index := range sources {
		epochs[index] = track.SourceEpoch
		states[index] = archiveindex.CoverageUnknown
		bestDistance[index] = ^uint64(0)
	}

	if err := m.store.IterateShardedMedia(ctx, id, track.ID, func(record storage.V2MediaRecord) error {
		coordinate := record.Coordinate
		if coordinate.Kind == archiveindex.ObjectInit {
			return nil
		}
		if indices := targetsByCoordinate[historicalSourceCoordinate{discontinuity: coordinate.DiscontinuitySequence, sequence: coordinate.Sequence}]; len(indices) != 0 {
			for _, index := range indices {
				if !exactMedia[index] {
					exactMedia[index] = true
					exactMediaEpoch[index] = coordinate.SourceEpoch
				}
			}
		}
		refs := targetsByDiscontinuity[coordinate.DiscontinuitySequence]
		position := sort.Search(len(refs), func(i int) bool { return refs[i].sequence >= coordinate.Sequence })
		for _, targetPosition := range []int{position - 1, position} {
			if targetPosition < 0 || targetPosition >= len(refs) {
				continue
			}
			sequence := refs[targetPosition].sequence
			start := targetPosition
			for start > 0 && refs[start-1].sequence == sequence {
				start--
			}
			end := targetPosition + 1
			for end < len(refs) && refs[end].sequence == sequence {
				end++
			}
			distance := coordinate.Sequence
			if distance < sequence {
				distance = sequence - distance
			} else {
				distance -= sequence
			}
			for _, target := range refs[start:end] {
				if distance < bestDistance[target.index] {
					bestDistance[target.index] = distance
					bestEpoch[target.index] = coordinate.SourceEpoch
				}
			}
		}
		return nil
	}); err != nil {
		return nil, nil, newStorageStageError("iterate sharded recovery media", err)
	}
	if err := m.store.IterateShardedGaps(ctx, id, func(gap domain.Gap) error {
		if gap.TrackID != track.ID {
			return nil
		}
		refs := targetsByDiscontinuity[gap.DiscontinuitySequence]
		start := sort.Search(len(refs), func(i int) bool { return refs[i].sequence >= gap.FromSequence })
		for _, target := range refs[start:] {
			if target.sequence > gap.ToSequence {
				break
			}
			if !exactGap[target.index] {
				exactGap[target.index] = true
				exactGapEpoch[target.index] = gap.SourceEpoch
			}
		}
		return nil
	}); err != nil {
		return nil, nil, newStorageStageError("iterate sharded recovery gaps", err)
	}
	for index := range sources {
		if !selected[index] {
			continue
		}
		switch {
		case exactGap[index]:
			epochs[index] = exactGapEpoch[index]
		case exactMedia[index]:
			epochs[index] = exactMediaEpoch[index]
		case bestDistance[index] != ^uint64(0):
			epochs[index] = bestEpoch[index]
		}
		if exactMedia[index] && exactMediaEpoch[index] == epochs[index] {
			states[index] = archiveindex.CoveragePresent
		}
	}
	if err := m.store.IterateShardedCoverage(ctx, id, func(coverage archiveindex.Coverage) error {
		if coverage.TrackID != track.ID || coverage.Kind == archiveindex.ObjectInit {
			return nil
		}
		refs := targetsByDiscontinuity[coverage.DiscontinuitySequence]
		start := sort.Search(len(refs), func(i int) bool { return refs[i].sequence >= coverage.FromSequence })
		for _, target := range refs[start:] {
			if target.sequence > coverage.ToSequence {
				break
			}
			if epochs[target.index] != coverage.SourceEpoch {
				continue
			}
			state := states[target.index]
			if state == archiveindex.CoverageConflict {
				continue
			}
			if coverage.State == archiveindex.CoverageConflict {
				states[target.index] = archiveindex.CoverageConflict
				continue
			}
			if state == archiveindex.CoveragePresent {
				continue
			}
			if coverage.State == archiveindex.CoveragePresent {
				states[target.index] = archiveindex.CoveragePresent
				continue
			}
			if coverage.ObservedAt.After(newestCoverage[target.index]) {
				newestCoverage[target.index] = coverage.ObservedAt
				states[target.index] = coverage.State
			}
		}
		return nil
	}); err != nil {
		return nil, nil, newStorageStageError("iterate sharded recovery coverage", err)
	}
	return epochs, states, nil
}

func (m *Manager) populateShardedHistoricalInitIdentities(ctx context.Context, id string, track *domain.Track, sources []hls.MediaSegment, selected []bool, epochs []uint64) error {
	if track == nil {
		return errors.New("main track is missing")
	}
	wanted := make(map[shardedHistoricalInitKey]struct{})
	for index, source := range sources {
		if !selected[index] || source.Init == nil {
			continue
		}
		wanted[shardedHistoricalInitKey{epoch: epochs[index], discontinuity: source.DiscontinuitySequence, sourceURI: sourceURIIdentity(source.Init.URI)}] = struct{}{}
	}
	found := make(map[shardedHistoricalInitKey]struct{})
	for _, segment := range track.InitSegments {
		key := shardedHistoricalInitKey{epoch: segment.SourceEpoch, discontinuity: segment.DiscontinuitySequence, sourceURI: sourceURIIdentity(segment.SourceURI)}
		found[key] = struct{}{}
	}
	if err := m.store.IterateShardedInitSegments(ctx, id, track.ID, func(record storage.V2MediaRecord) error {
		segment := record.Segment
		key := shardedHistoricalInitKey{epoch: segment.SourceEpoch, discontinuity: segment.DiscontinuitySequence, sourceURI: sourceURIIdentity(segment.SourceURI)}
		if _, ok := wanted[key]; !ok {
			return nil
		}
		if _, exists := found[key]; !exists {
			track.InitSegments = append(track.InitSegments, segment)
			found[key] = struct{}{}
		}
		return nil
	}); err != nil {
		return newStorageStageError("iterate sharded recovery init media", err)
	}
	return nil
}

func historicalInitIdentity(track *domain.Track, source hls.MediaSegment, epoch uint64) string {
	if source.Init == nil {
		return ""
	}
	for _, init := range track.InitSegments {
		if init.SourceEpoch == epoch && init.DiscontinuitySequence == source.DiscontinuitySequence && sourceURIIdentity(init.SourceURI) == sourceURIIdentity(source.Init.URI) {
			return init.ID
		}
	}
	return ""
}

func (m *Manager) acquireHistoricalInit(ctx context.Context, e *entry, owner OwnershipToken, terminal bool, requestMedia adapterproto.MediaSource, generation uint64, sessionID string, track *domain.Track, source hls.MediaSegment, epoch uint64) (string, error) {
	if source.Init == nil {
		return "", nil
	}
	if id := historicalInitIdentity(track, source, epoch); id != "" {
		return id, nil
	}
	asset := domain.Segment{
		TrackID: "main", Sequence: source.Sequence, SourceEpoch: epoch,
		DiscontinuitySequence: source.DiscontinuitySequence,
		SourceURI:             source.Init.URI, ByteRange: cloneRange(source.Init.ByteRange), IsInit: true,
	}
	coordinate := coordinateForSegment(sessionID, asset)
	if existing, ok := findRootSegment(m.recordingSnapshotForArchive(e), coordinate); ok {
		rememberShardedHistoricalInit(track, existing)
		return existing.ID, nil
	}
	id := initSegmentID(hls.Map{URI: source.Init.URI, ByteRange: source.Init.ByteRange}, epoch, source.DiscontinuitySequence)
	asset.ID = id
	asset.InitSegmentID = ""
	payload, err := m.downloadObjectBufferedOnceAtGenerationScope(ctx, source.Init.URI, source.Init.ByteRange, recordingID(e), requestMedia, e, generation, adapterproto.RequestScopeInit)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			coverageCoordinate := coordinateForSegment(sessionID, asset)
			if coverageErr := m.recordHistoricalCoverage(e, &owner, terminal, coverageCoordinate, archiveindex.CoverageAcquisitionFailed, "historical initialization acquisition failed"); coverageErr != nil {
				return "", coverageErr
			}
		}
		return "", err
	}
	result := payload.Result()
	asset.PayloadSize, asset.SHA256 = result.Size, result.SHA256
	if err := m.commitHistoricalPayload(ctx, e, owner, terminal, asset, payload); err != nil {
		return "", err
	}
	rememberShardedHistoricalInit(track, asset)
	return asset.ID, nil
}

func rememberShardedHistoricalInit(track *domain.Track, segment domain.Segment) {
	if track == nil || !segment.IsInit {
		return
	}
	for _, existing := range track.InitSegments {
		if existing.ID == segment.ID {
			return
		}
	}
	track.InitSegments = append(track.InitSegments, segment)
}

func (m *Manager) commitHistoricalPayload(ctx context.Context, e *entry, owner OwnershipToken, terminal bool, segment domain.Segment, payload *storage.IngestPayload) error {
	done := make(chan error, 1)
	kind := storage.IngestJobKindHistoricalMedia
	stage := "historical media payload commit"
	if segment.IsInit {
		kind = storage.IngestJobKindHistoricalInit
		stage = "historical init payload commit"
	}
	err := m.ingest.SubmitWithKind(ctx, payload, kind, func(data []byte) (storage.PayloadResult, error) {
		result, _, commitErr := m.commitArchiveSegmentOwned(e, &owner, segment, archiveindex.ClaimHistorical, data, terminal)
		return result, commitErr
	}, func(_ storage.PayloadResult, commitErr error) {
		if errors.Is(commitErr, storage.ErrCanonicalCommitFailed) {
			m.recordStorageFailureDiagnostic(e, newStorageCommitFailure(stage, commitErr))
		}
		done <- commitErr
	})
	if err != nil {
		payload.Release()
		return err
	}
	// Once accepted, the payload callback owns mutation completion. Never return
	// early on cancellation while the terminal repair token may be released.
	return <-done
}

func (m *Manager) recordHistoricalCoverage(e *entry, owner *OwnershipToken, terminal bool, coordinate archiveindex.Coordinate, state archiveindex.CoverageState, reason string) error {
	if state != archiveindex.CoverageKnownMissing && state != archiveindex.CoverageAcquisitionFailed {
		return archiveindex.ErrInvalidInventory
	}
	return m.withCanonicalMutationOwner(e, owner, terminal, func() error {
		root := m.recordingSnapshotForArchive(e)
		if root == nil {
			return storage.ErrNotFound
		}
		if root.ArchiveSealed {
			return ErrArchiveSealed
		}
		coverage := recordHistoricalCoverage(coordinate, state, reason, time.Now().UTC())
		if isShardedRecording(root) {
			if state == archiveindex.CoverageKnownMissing {
				if err := m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
					track := recording.Tracks[coordinate.TrackID]
					if track == nil {
						return errors.New("main track is missing")
					}
					addMissingRangesDS(recording, track, coordinate.SourceEpoch, coordinate.DiscontinuitySequence, coordinate.Sequence, coordinate.Sequence, reason, capturedSequenceSetDS(track, coordinate.SourceEpoch, coordinate.DiscontinuitySequence))
					return nil
				}); err != nil {
					return err
				}
			}
			result, err := m.store.AppendShardedCoverageWithRevision(context.Background(), root.ID, coverage)
			if err != nil {
				return err
			}
			e.mu.Lock()
			mergeShardedRevisionResult(e.recording, result)
			e.mu.Unlock()
			return nil
		}
		inventory, err := m.inventoryForRecording(root, false)
		if err != nil {
			return err
		}
		previousCoverageState := archiveindex.CoverageAt(inventory, coordinate)
		if err := archiveindex.ApplyCoverage(&inventory, coverage); err != nil {
			if isArchiveIndexCapacityError(err) {
				return ErrArchiveIndexLimit
			}
			return err
		}
		currentCoverageState := archiveindex.CoverageAt(inventory, coordinate)
		coverageStateChanged := currentCoverageState != previousCoverageState && currentCoverageState == archiveindex.CoverageKnownMissing
		if state == archiveindex.CoverageKnownMissing {
			if err := m.updateWithinAuthorizedCommit(e, func(recording *domain.Recording) error {
				track := recording.Tracks[coordinate.TrackID]
				if track == nil {
					return errors.New("main track is missing")
				}
				previousGapCount := len(recording.Gaps)
				previousRevision := recording.ArchiveRevision
				addMissingRangesDS(recording, track, coordinate.SourceEpoch, coordinate.DiscontinuitySequence, coordinate.Sequence, coordinate.Sequence, reason, capturedSequenceSetDS(track, coordinate.SourceEpoch, coordinate.DiscontinuitySequence))
				if coverageStateChanged && len(recording.Gaps) == previousGapCount && recording.ArchiveRevision == previousRevision {
					return advanceArchiveRevision(recording)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return m.persistCoverage(root.ID, inventory)
	})
}

func (m *Manager) releaseRepairOwner(ctx context.Context, e *entry, owner OwnershipToken) error {
	e.mu.Lock()
	terminal := e.recording != nil && e.recording.State != domain.StateRecording && sameOwner(e.ownership, owner)
	e.mu.Unlock()
	if !terminal {
		return nil
	}
	m.mu.RLock()
	release := m.terminalOwnerRelease
	m.mu.RUnlock()
	if release == nil {
		return ErrCanonicalFenceRequired
	}
	if err := release(ctx, owner); err != nil {
		return err
	}
	e.mu.Lock()
	if sameOwner(e.ownership, owner) {
		e.ownership = nil
	}
	e.mu.Unlock()
	return nil
}
