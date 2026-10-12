package acquire

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

type timedEventTestPreparer struct {
	stream adapterproto.TimedEventStream
	batch  adapterproto.TimedEventBatch
	starts atomic.Int32
}

func (*timedEventTestPreparer) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, nil
}

func (p *timedEventTestPreparer) PrepareTimedEvents(_ context.Context, _ string, _ string, _ *adapterproto.ResourceRef, _ adapterproto.MediaSource, resume json.RawMessage) (adapterproto.TimedEventStream, bool, error) {
	p.starts.Add(1)
	if len(resume) != 0 && string(resume) != `{"cursor":"next"}` {
		return adapterproto.TimedEventStream{}, true, context.Canceled
	}
	return p.stream, true, nil
}

func TestTimedEventMonitorCommitsBeforeAcknowledgingAndStopsWithWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	recording := &domain.Recording{FormatVersion: storage.ShardedArchiveFormatVersion, ShardedArchive: &domain.ShardedArchiveSummary{}, ID: "aee2f247ab144c6a850fd5c07fb1e6b1", AdapterID: "fixture", State: domain.StateRecording, CreatedAt: now, StartedAt: now, ArchiveRevision: 1, Tracks: map[string]*domain.Track{"main": {ID: "main", NextArchiveOrdinal: 1}}}
	if err := store.CreateShardedRecording(recording); err != nil {
		t.Fatal(err)
	}
	batches := make(chan adapterproto.TimedEventBatch, 1)
	batches <- adapterproto.TimedEventBatch{StreamID: "s", BatchID: "b", ResumeState: json.RawMessage(`{"cursor":"next"}`), Events: []adapterproto.TimedEvent{{EventType: "chat.message", SourceEventID: "stable", Payload: json.RawMessage(`{"text":"hello"}`)}}}
	var acked atomic.Bool
	var stopped atomic.Bool
	preparer := &timedEventTestPreparer{batch: adapterproto.TimedEventBatch{}, stream: adapterproto.TimedEventStream{Batches: batches, Errors: make(chan error), Ack: func(_ context.Context, ack adapterproto.TimedEventAckParams) error {
		root, err := store.LoadRecordingHeader(context.Background(), recording.ID)
		if err != nil || root.ShardedArchive.TimedEventCount != 1 || root.ArchiveRevision != 2 {
			t.Errorf("ack before durable append: root=%#v err=%v", root, err)
			return context.Canceled
		}
		acked.Store(ack.BatchID == "b")
		return nil
	}, Stop: func(context.Context) error { stopped.Store(true); return nil }}}
	manager := &Manager{store: store, resolver: preparer}
	entry := &entry{recording: recording, adapterID: "fixture", done: make(chan struct{})}
	done := make(chan struct{})
	go func() { manager.runTimedEventMonitor(ctx, entry); close(done) }()
	deadline := time.After(2 * time.Second)
	for !acked.Load() {
		select {
		case <-deadline:
			t.Fatal("event batch was not acknowledged")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("event monitor did not stop")
	}
	if !stopped.Load() {
		t.Fatal("event source stop was not called")
	}
	root, err := store.LoadRecordingHeader(context.Background(), recording.ID)
	if err != nil || root.ShardedArchive.TimedEventCount != 1 {
		t.Fatalf("durable event state=%#v err=%v", root, err)
	}
}
