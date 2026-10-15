package acquire

import (
	"context"
	"errors"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const timedEventReconnectDelay = time.Second

func (m *Manager) runTimedEventMonitor(ctx context.Context, e *entry) {
	preparer, ok := m.resolver.(TimedEventPreparer)
	if !ok {
		return
	}
	for ctx.Err() == nil {
		e.mu.Lock()
		if e.deleted || e.recording == nil || e.recording.State != domain.StateRecording {
			e.mu.Unlock()
			return
		}
		adapterID, recordingID := e.adapterID, e.recording.ID
		resource, current := cloneResourceRef(e.resource), cloneMediaSource(e.media)
		e.mu.Unlock()
		resume, err := m.store.LoadLatestShardedTimedEventResumeState(ctx, recordingID)
		if err != nil {
			if !waitMetadata(ctx, timedEventReconnectDelay) {
				return
			}
			continue
		}
		stream, supported, err := preparer.PrepareTimedEvents(ctx, adapterID, recordingID, resource, current, resume)
		if err != nil {
			if !waitMetadata(ctx, timedEventReconnectDelay) {
				return
			}
			continue
		}
		if !supported {
			return
		}
		reconnect := false
		for !reconnect && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				reconnect = true
			case <-stream.Done:
				reconnect = true
			case streamErr := <-stream.Errors:
				if streamErr != nil {
					reconnect = true
				}
			case batch := <-stream.Batches:
				if err := batch.Validate(); err != nil {
					reconnect = true
					continue
				}
				if !e.beginObserverCanonicalCommit() {
					// Do not acknowledge a batch that did not cross the source
					// canonical-commit admission boundary. The target resumes from
					// the last durable cursor and may safely replay this batch.
					reconnect = true
					continue
				}
				// Once admitted, the append must not inherit observer cancellation:
				// handover joins this callback and transfers only after its durable
				// resume cursor has advanced atomically with the event batch.
				if _, err := m.commitTimedEventBatch(context.Background(), e, batch); err != nil {
					if errors.Is(err, storage.ErrArchiveSealed) {
						return
					}
					reconnect = true
					continue
				}
				ackCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				var err error
				if stream.Ack == nil {
					err = storage.ErrShardedArchiveInvalid
				} else {
					err = stream.Ack(ackCtx, adapterproto.TimedEventAckParams{StreamID: batch.StreamID, BatchID: batch.BatchID})
				}
				cancel()
				if err != nil {
					reconnect = true
				}
			}
		}
		if stream.Stop != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = stream.Stop(stopCtx)
			cancel()
		}
		if ctx.Err() != nil {
			return
		}
		if !waitMetadata(ctx, timedEventReconnectDelay) {
			return
		}
	}
}

func (m *Manager) commitTimedEventBatch(ctx context.Context, e *entry, batch adapterproto.TimedEventBatch) (storage.TimedEventAppendResult, error) {
	if e == nil || batch.Validate() != nil {
		return storage.TimedEventAppendResult{}, storage.ErrShardedArchiveInvalid
	}
	var result storage.TimedEventAppendResult
	err := m.withCanonicalMutation(e, func() error {
		e.mu.Lock()
		if e.deleted || e.recording == nil || e.recording.State != domain.StateRecording {
			e.mu.Unlock()
			return storage.ErrNotFound
		}
		recordingID := e.recording.ID
		e.mu.Unlock()
		var err error
		result, err = m.store.AppendShardedTimedEventBatch(ctx, recordingID, batch)
		if err != nil {
			return err
		}
		header, err := m.store.LoadRecordingHeader(ctx, recordingID)
		if err != nil {
			return err
		}
		e.mu.Lock()
		if e.recording != nil && e.recording.ID == recordingID && e.recording.ShardedArchive != nil && header.ShardedArchive != nil {
			e.recording.ArchiveRevision = header.ArchiveRevision
			e.recording.ShardedArchive.TimedEventCount = header.ShardedArchive.TimedEventCount
			e.recording.ShardedArchive.TimedEventBatchCount = header.ShardedArchive.TimedEventBatchCount
			e.recording.ShardedArchive.TimedEventIdentityHighWater = header.ShardedArchive.TimedEventIdentityHighWater
			e.recording.ShardedArchive.TimedEventLastOffsetMillis = header.ShardedArchive.TimedEventLastOffsetMillis
		}
		e.mu.Unlock()
		return nil
	})
	return result, err
}
