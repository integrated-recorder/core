package acquire

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/streammeta"
)

const (
	defaultMetadataPollInterval = 30 * time.Second
	metadataObservationTimeout  = 15 * time.Second
	maximumMetadataBackoff      = 5 * time.Minute
)

func (m *Manager) metadataInterval() time.Duration {
	if m.metadataPollInterval > 0 {
		return m.metadataPollInterval
	}
	return defaultMetadataPollInterval
}

func (m *Manager) runMetadataMonitor(ctx context.Context, e *entry) {
	observer, ok := m.resolver.(MetadataPreparer)
	if !ok {
		return
	}
	interval := m.metadataInterval()
	failures := 0
	for ctx.Err() == nil {
		e.mu.Lock()
		if e.deleted || e.recording == nil || e.recording.State != domain.StateRecording || e.recording.MetadataTimelineTruncated {
			e.mu.Unlock()
			return
		}
		adapterID := e.adapterID
		resource := cloneResourceRef(e.resource)
		media := cloneMediaSource(e.media)
		generation := e.mediaGeneration
		e.mu.Unlock()

		callCtx, cancel := context.WithTimeout(ctx, metadataObservationTimeout)
		result, commit, supported, err := observer.PrepareMetadata(callCtx, adapterID, resource, media)
		cancel()
		if !supported && err == nil {
			return
		}
		if err == nil {
			err = result.Validate()
		}
		var observedAt time.Time
		if err == nil {
			// Stamp at Core receipt/validation time, before waiting for the
			// canonical mutation lock or any storage I/O.
			observedAt = m.metadataNow().UTC()
		}
		if err == nil && ctx.Err() == nil {
			active, truncated, commitErr := m.commitMetadataObservation(e, generation, observedAt, result, commit)
			if commitErr != nil {
				err = commitErr
			} else if !active {
				// A refresh may have replaced the media during IPC. Re-observe
				// immediately using the new generation; terminal state exits above.
				continue
			} else if truncated {
				return
			} else {
				failures = 0
			}
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			failures++
			interval = metadataBackoff(m.metadataInterval(), failures)
		} else {
			interval = m.metadataInterval()
		}
		if !waitMetadata(ctx, interval) {
			return
		}
	}
}

func (m *Manager) metadataNow() time.Time {
	if m.metadataClock != nil {
		return m.metadataClock()
	}
	return time.Now()
}

func metadataBackoff(base time.Duration, failures int) time.Duration {
	delay := base
	for i := 1; i < failures && delay < maximumMetadataBackoff; i++ {
		if delay > maximumMetadataBackoff/2 {
			return maximumMetadataBackoff
		}
		delay *= 2
	}
	if delay > maximumMetadataBackoff {
		return maximumMetadataBackoff
	}
	return delay
}

func waitMetadata(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// commitMetadataObservation is serialized with every other durable recording
// mutation. It persists the root before adapter state mutations are committed;
// stale media generations discard both.
func (m *Manager) commitMetadataObservation(e *entry, generation uint64, observedAt time.Time, result adapterproto.MetadataResult, commit func() error) (active, truncated bool, err error) {
	if err := result.Validate(); err != nil {
		return false, false, err
	}
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	e.mu.Lock()
	if e.deleted || e.recording == nil || e.recording.State != domain.StateRecording || e.mediaGeneration != generation {
		e.mu.Unlock()
		return false, false, nil
	}
	next := clone(e.recording)
	e.mu.Unlock()
	if next == nil {
		return false, false, errors.New("recording metadata could not be copied")
	}

	var previousTitle, previousDescription *string
	if count := len(next.MetadataTimeline); count > 0 {
		last := next.MetadataTimeline[count-1]
		previousTitle = cloneMetadataString(last.Title)
		previousDescription = cloneMetadataString(last.Description)
	}
	title := previousTitle
	description := previousDescription
	changed := false
	if result.Metadata.Title != nil {
		if !sameMetadataString(title, result.Metadata.Title) {
			changed = true
		}
		title = cloneMetadataString(result.Metadata.Title)
	}
	if result.Metadata.Description != nil {
		if !sameMetadataString(description, result.Metadata.Description) {
			changed = true
		}
		description = cloneMetadataString(result.Metadata.Description)
	}
	active = true
	var adapterStateCommitFailed bool
	if err := m.withCanonicalCommit(e, func() error {
		if changed && !next.MetadataTimelineTruncated {
			revision := domain.MetadataRevision{
				ObservedAt: observedAt.UTC(), Title: title, Description: description,
				SourceUpdatedAt: cloneMetadataTime(result.SourceUpdatedAt),
			}
			candidate := append(append([]domain.MetadataRevision(nil), next.MetadataTimeline...), revision)
			encoded, marshalErr := json.Marshal(candidate)
			if marshalErr != nil || len(candidate) > streammeta.MaxTimelineRevisions || len(encoded) > streammeta.MaxTimelineBytes {
				next.MetadataTimelineTruncated = true
				truncated = true
			} else {
				next.MetadataTimeline = candidate
			}
			if err := domain.ValidateMetadataTimeline(next.MetadataTimeline); err != nil {
				return errors.New("recording metadata timeline is invalid")
			}
			if err := m.store.SaveRecording(next); err != nil {
				return newStorageStageError("recording metadata root commit", err)
			}
			e.mu.Lock()
			if e.deleted || e.mediaGeneration != generation || e.recording.State != domain.StateRecording {
				e.mu.Unlock()
				active = false
				return nil
			}
			e.recording = next
			e.mu.Unlock()
		}
		if commit != nil {
			if err := commit(); err != nil {
				adapterStateCommitFailed = true
				return errors.New("adapter metadata state could not be saved")
			}
		}
		return nil
	}); err != nil {
		if adapterStateCommitFailed {
			return true, truncated, err
		}
		return false, false, err
	}
	return active, truncated, nil
}

func cloneMetadataString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneMetadataTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func sameMetadataString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
