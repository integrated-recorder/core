package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestShardedTimedEventsDedupeOrderCursorAndSeal(t *testing.T) {
	ctx := context.Background()
	store, recording := newShardedArchiveTestStore(t, "aee2f247ab144c6a850fd5c07fb1e6a1")
	firstTime := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
	olderTime := firstTime.Add(-time.Minute)
	sequence := uint64(4)
	first := adapterproto.TimedEventBatch{StreamID: "s1", BatchID: "b1", ResumeState: json.RawMessage(`{"cursor":"opaque-1"}`), Events: []adapterproto.TimedEvent{
		{EventType: "chat.message", SourceEventID: "stable-1", SourceTimestamp: &firstTime, Payload: json.RawMessage(`{"text":"one"}`)},
		{EventType: "custom.example", SourceSequence: &sequence, SourceTimestamp: &olderTime, Extension: json.RawMessage(`{"custom":true}`)},
	}}
	result, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, first)
	if err != nil || result.Inserted != 2 || result.EventCount != 2 {
		t.Fatalf("append first: %#v err=%v", result, err)
	}
	root, err := store.LoadRecordingHeader(ctx, recording.ID)
	if err != nil || root.ArchiveRevision != 1 || root.TimelineRevision != 0 {
		t.Fatalf("root revisions: %#v err=%v", root, err)
	}
	duplicateBatch, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, first)
	if err != nil || !duplicateBatch.Duplicate || duplicateBatch.EventCount != 2 {
		t.Fatalf("replay batch: %#v err=%v", duplicateBatch, err)
	}
	changedBatch := first
	changedBatch.Events = append([]adapterproto.TimedEvent(nil), first.Events...)
	changedBatch.Events[0].Message = &adapterproto.TimedEventMessage{Text: "reused batch id with changed content"}
	if _, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, changedBatch); err != ErrShardedArchiveConflict {
		t.Fatalf("reused batch id with different content err=%v", err)
	}
	second := adapterproto.TimedEventBatch{StreamID: "s2", BatchID: "b2", ResumeState: json.RawMessage(`{"cursor":"opaque-2"}`), Events: []adapterproto.TimedEvent{{EventType: "chat.message", SourceEventID: "stable-1", Payload: json.RawMessage(`{"text":"duplicate"}`)}}}
	result, err = store.AppendShardedTimedEventBatch(ctx, recording.ID, second)
	if err != nil || result.Inserted != 0 || result.EventCount != 2 {
		t.Fatalf("cross-batch duplicate: %#v err=%v", result, err)
	}
	resume, err := store.LoadLatestShardedTimedEventResumeState(ctx, recording.ID)
	if err != nil || string(resume) != `{"cursor":"opaque-2"}` {
		t.Fatalf("resume cursor=%s err=%v", resume, err)
	}
	page, err := store.PageShardedTimedEvents(ctx, recording.ID, 1, 0, 10, "", nil, nil)
	if err != nil || len(page.Events) != 2 || page.Events[0].EventArchiveOrdinal != 1 || page.Events[1].EventArchiveOrdinal != 2 {
		t.Fatalf("event page=%#v err=%v", page, err)
	}
	if page.Events[1].PresentationOffsetMS < page.Events[0].PresentationOffsetMS {
		t.Fatalf("presentation offset moved backward: %#v", page.Events)
	}
	root, err = store.LoadRecordingHeader(ctx, recording.ID)
	if err != nil || root.ShardedArchive.TimedEventBatchCount != 2 || root.ShardedArchive.TimedEventCount != 2 || root.ArchiveRevision != 2 || root.TimelineRevision != 0 {
		t.Fatalf("post-dedupe root=%#v err=%v", root, err)
	}
	if count, objects, err := store.VerifyShardedTimedEvents(ctx, recording.ID); err != nil || count != 2 || objects < 3 {
		t.Fatalf("verify timed event pages count=%d objects=%d err=%v", count, objects, err)
	}
	root, err = store.LoadRecordingHeader(ctx, recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	root.ArchiveSealed = true
	if err := store.SaveRecordingHeader(ctx, root); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, adapterproto.TimedEventBatch{StreamID: "s1", BatchID: "b3", Events: []adapterproto.TimedEvent{{EventType: "chat.message"}}}); err != ErrArchiveSealed {
		t.Fatalf("append after seal err=%v", err)
	}
}

func TestShardedTimedEventIntegrityDetectsCanonicalPageMutation(t *testing.T) {
	ctx := context.Background()
	store, recording := newShardedArchiveTestStore(t, "dee2f247ab144c6a850fd5c07fb1e6a4")
	batch := adapterproto.TimedEventBatch{StreamID: "s", BatchID: "integrity", Events: []adapterproto.TimedEvent{{EventType: "chat.message", SourceEventID: "message-1", Message: &adapterproto.TimedEventMessage{Text: "original"}}}}
	if _, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, batch); err != nil {
		t.Fatal(err)
	}
	var page timedEventBatchPage
	if err := store.loadV2JSON(ctx, recording.ID, timedEventBatchPath(1), archiveShardMaxBytes, &page); err != nil {
		t.Fatal(err)
	}
	page.Events[0].Message.Text = "tampered"
	if err := store.saveV2JSON(ctx, recording.ID, timedEventBatchPath(1), page, archiveShardMaxBytes); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.VerifyShardedTimedEvents(ctx, recording.ID); err == nil {
		t.Fatal("tampered event page passed integrity validation")
	}
}

func TestShardedTimedEventAppendReconcilesCrashAfterEachPublishStage(t *testing.T) {
	event := adapterproto.TimedEvent{EventType: "chat.message", SourceEventID: "crash-event", Message: &adapterproto.TimedEventMessage{Text: "durable after restart"}}
	batch := adapterproto.TimedEventBatch{StreamID: "stream", BatchID: "crash-batch", ResumeState: json.RawMessage(`{"cursor":"after"}`), Events: []adapterproto.TimedEvent{event}}
	identity := timedEventIdentity(event, timedEventIdentityBase(event), batch.BatchID, 0)
	bucket, err := timedEventBucket(identity)
	if err != nil {
		t.Fatal(err)
	}
	for index, path := range []string{timedEventBatchPath(1), timedEventIdentityPath(bucket, 0), timedEventBatchKey(batch.BatchID)} {
		t.Run(fmt.Sprintf("stage-%d", index+1), func(t *testing.T) {
			store, recording := newShardedArchiveTestStore(t, fmt.Sprintf("%032x", index+1))
			failure := errors.New("simulated process crash after durable sidecar")
			backend := store.StorageBackend
			store.StorageBackend = &failAfterV2SidecarWrite{StorageBackend: backend, relative: path, failure: failure}
			if _, err := store.AppendShardedTimedEventBatch(context.Background(), recording.ID, batch); !errors.Is(err, failure) {
				t.Fatalf("append error=%v, want injected crash", err)
			}
			store.StorageBackend = backend

			restarted, err := New(store.root)
			if err != nil {
				t.Fatal(err)
			}
			result, err := restarted.AppendShardedTimedEventBatch(context.Background(), recording.ID, batch)
			if err != nil || !result.Duplicate || result.EventCount != 1 {
				t.Fatalf("reconciled append=%#v err=%v", result, err)
			}
			count, _, err := restarted.VerifyShardedTimedEvents(context.Background(), recording.ID)
			if err != nil || count != 1 {
				t.Fatalf("verified event count=%d err=%v", count, err)
			}
		})
	}
}

func TestShardedTimedEventsPagesAndTypeFilterAreBounded(t *testing.T) {
	ctx := context.Background()
	store, recording := newShardedArchiveTestStore(t, "bee2f247ab144c6a850fd5c07fb1e6a2")
	for batchIndex := 0; batchIndex < 3; batchIndex++ {
		events := make([]adapterproto.TimedEvent, 0, 4)
		for i := 0; i < 4; i++ {
			events = append(events, adapterproto.TimedEvent{EventType: "chat.message", SourceEventID: string(rune('a' + batchIndex*4 + i)), Payload: json.RawMessage(`{"text":"x"}`)})
		}
		if _, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, adapterproto.TimedEventBatch{StreamID: "s", BatchID: string(rune('1' + batchIndex)), Events: events}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.PageShardedTimedEvents(ctx, recording.ID, 1, 0, 5, "chat.message", nil, nil)
	if err != nil || len(page.Events) != 5 || !page.HasMore {
		t.Fatalf("first page=%#v err=%v", page, err)
	}
	page, err = store.PageShardedTimedEvents(ctx, recording.ID, page.NextBatch, page.NextEventIndex, 5, "chat.message", nil, nil)
	if err != nil || len(page.Events) != 5 || !page.HasMore {
		t.Fatalf("second page=%#v err=%v", page, err)
	}
	page, err = store.PageShardedTimedEvents(ctx, recording.ID, page.NextBatch, page.NextEventIndex, 5, "chat.message", nil, nil)
	if err != nil || len(page.Events) != 2 || page.HasMore {
		t.Fatalf("final page=%#v err=%v", page, err)
	}
}

func TestShardedTimedEventsRetryIdentityWithoutSourceIDAndTail(t *testing.T) {
	ctx := context.Background()
	store, recording := newShardedArchiveTestStore(t, "cee2f247ab144c6a850fd5c07fb1e6a3")
	stamp := time.Date(2026, 10, 10, 1, 2, 3, 0, time.UTC)
	identical := adapterproto.TimedEvent{EventType: "chat.message", SourceTimestamp: &stamp, Actor: &adapterproto.TimedEventActor{DisplayName: "viewer"}, Message: &adapterproto.TimedEventMessage{Text: "same"}}
	first := adapterproto.TimedEventBatch{StreamID: "one", BatchID: "batch-a", Events: []adapterproto.TimedEvent{identical, identical}}
	result, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, first)
	if err != nil || result.Inserted != 2 {
		t.Fatalf("append repeated source-less events: %#v err=%v", result, err)
	}
	replay := adapterproto.TimedEventBatch{StreamID: "two", BatchID: "batch-b", Events: []adapterproto.TimedEvent{identical, identical}}
	result, err = store.AppendShardedTimedEventBatch(ctx, recording.ID, replay)
	if err != nil || result.Inserted != 2 {
		t.Fatalf("source-less distinct batch must preserve events without a source identity: %#v err=%v", result, err)
	}
	result, err = store.AppendShardedTimedEventBatch(ctx, recording.ID, replay)
	if err != nil || !result.Duplicate || result.Inserted != 0 {
		t.Fatalf("source-less same-batch retry: %#v err=%v", result, err)
	}
	for batchIndex := 0; batchIndex < 2; batchIndex++ {
		events := make([]adapterproto.TimedEvent, 64)
		for eventIndex := range events {
			events[eventIndex] = adapterproto.TimedEvent{EventType: "custom.example", SourceEventID: fmt.Sprintf("event-%d-%d", batchIndex, eventIndex)}
		}
		batch := adapterproto.TimedEventBatch{StreamID: "tail", BatchID: fmt.Sprintf("tail-%d", batchIndex), Events: events}
		if _, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, batch); err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.PageLatestShardedTimedEvents(ctx, recording.ID, 100)
	if err != nil || len(page.Events) != 100 || page.Events[0].EventArchiveOrdinal != 33 || page.Events[len(page.Events)-1].EventArchiveOrdinal != 132 || page.HasMore {
		t.Fatalf("latest page=%#v err=%v", page, err)
	}
}

// TestShardedTimedEvents200HourScaleAcceptance writes a normal high-volume
// event archive through the production append path. At one event per second,
// 720,000 events represent 200 hours. It is opt-in because the test exercises
// real local filesystem indexing, page writes, and integrity verification.
func TestShardedTimedEvents200HourScaleAcceptance(t *testing.T) {
	if os.Getenv("IR_RUN_SCALE_TESTS") != "1" {
		t.Skip("set IR_RUN_SCALE_TESTS=1 to run the 200-hour timed-event acceptance")
	}
	if testing.Short() {
		t.Skip("large local filesystem acceptance")
	}
	const eventCount = 720_000
	ctx := context.Background()
	store, recording := newShardedArchiveTestStore(t, "a7f7a9bd3f70400da92abdc0f042e017")
	identityEntries := make([][]timedEventIdentityEntry, timedEventIdentityBuckets)
	const batchCount = eventCount / adapterproto.MaxTimedEventBatchEvents
	for start := 0; start < eventCount; start += adapterproto.MaxTimedEventBatchEvents {
		end := start + adapterproto.MaxTimedEventBatchEvents
		if end > eventCount {
			end = eventCount
		}
		events := make([]adapterproto.TimedEvent, end-start)
		batchID := fmt.Sprintf("scale-batch-%06d", start/adapterproto.MaxTimedEventBatchEvents)
		for index := range events {
			ordinal := start + index + 1
			events[index] = adapterproto.TimedEvent{EventType: "chat.message", SourceEventID: fmt.Sprintf("scale-%07d", ordinal), Message: &adapterproto.TimedEventMessage{Text: "fixture"}}
		}
		batch := adapterproto.TimedEventBatch{StreamID: "scale-stream", BatchID: batchID, ResumeState: json.RawMessage(fmt.Sprintf(`{"cursor":%q}`, batchID)), Events: events}
		inputDigest, err := timedEventInputDigest(batch)
		if err != nil {
			t.Fatal(err)
		}
		page := timedEventBatchPage{Version: 1, Number: uint64(start/adapterproto.MaxTimedEventBatchEvents + 1), FirstEventOrdinal: uint64(start + 1), LastEventOrdinal: uint64(end), BatchID: batchID, InputSHA256: inputDigest, ResumeState: append(json.RawMessage(nil), batch.ResumeState...), Events: make([]TimedEventRecord, 0, len(events))}
		for index, event := range events {
			ordinal := uint64(start + index + 1)
			identity := timedEventIdentity(event, timedEventIdentityBase(event), batchID, index)
			bucket, err := timedEventBucket(identity)
			if err != nil {
				t.Fatal(err)
			}
			identityEntries[bucket] = append(identityEntries[bucket], timedEventIdentityEntry{Hash: identity, Ordinal: ordinal})
			offset := int64(ordinal-1) * 1000
			page.Events = append(page.Events, TimedEventRecord{EventArchiveOrdinal: ordinal, EventTimelineOrdinal: ordinal, EventType: event.EventType, SourceEventID: event.SourceEventID, ObservedAt: recording.StartedAt.Add(time.Duration(offset) * time.Millisecond), PresentationOffsetMS: offset, Message: event.Message, IdentityHash: identity})
		}
		page.PageSHA256, err = timedEventPageDigest(page)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.saveV2JSON(ctx, recording.ID, timedEventBatchPath(page.Number), page, archiveShardMaxBytes); err != nil {
			t.Fatal(err)
		}
		batchIdentity := timedEventBatchIdentity{Version: 1, Ordinal: page.Number, BatchID: batchID, InputSHA256: inputDigest, PageSHA256: page.PageSHA256}
		if err := store.saveV2JSON(ctx, recording.ID, timedEventBatchKey(batchID), batchIdentity, 4096); err != nil {
			t.Fatal(err)
		}
	}
	for bucket, entries := range identityEntries {
		if len(entries) == 0 {
			continue
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Hash < entries[j].Hash })
		for pageNo, start := uint64(0), 0; start < len(entries); pageNo++ {
			end := start + timedEventIndexPageEntries
			if end > len(entries) {
				end = len(entries)
			}
			page := timedEventIdentityPage{Version: 1, Number: pageNo, Entries: entries[start:end]}
			var err error
			page.PageSHA256, err = timedEventIdentityPageDigest(page)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.saveV2JSON(ctx, recording.ID, timedEventIdentityPath(bucket, pageNo), page, archiveShardMaxBytes); err != nil {
				t.Fatal(err)
			}
			start = end
		}
	}
	header, err := store.LoadRecordingHeader(ctx, recording.ID)
	if err != nil {
		t.Fatal(err)
	}
	header.ArchiveRevision = batchCount
	header.ShardedArchive.TimedEventCount = eventCount
	header.ShardedArchive.TimedEventBatchCount = batchCount
	header.ShardedArchive.TimedEventLastOffsetMillis = int64(eventCount-1) * 1000
	header.ShardedArchive.TimedEventIdentityHighWater = make([]uint64, timedEventIdentityBuckets)
	for bucket, entries := range identityEntries {
		header.ShardedArchive.TimedEventIdentityHighWater[bucket] = uint64(len(entries))
	}
	if err := store.SaveRecordingHeader(ctx, header); err != nil {
		t.Fatal(err)
	}
	header, err = store.LoadRecordingHeader(ctx, recording.ID)
	if err != nil || header.ShardedArchive.TimedEventCount != eventCount || len(header.ShardedArchive.TimedEventIdentityHighWater) != timedEventIdentityBuckets {
		t.Fatalf("200-hour archive summary count=%d buckets=%d err=%v", header.ShardedArchive.TimedEventCount, len(header.ShardedArchive.TimedEventIdentityHighWater), err)
	}
	first, err := store.PageShardedTimedEvents(ctx, recording.ID, 1, 0, 10, "chat.message", nil, nil)
	if err != nil || len(first.Events) != 10 || first.Events[0].EventArchiveOrdinal != 1 || first.Events[9].EventArchiveOrdinal != 10 || !first.HasMore {
		t.Fatalf("first event page=%#v err=%v", first, err)
	}
	last, err := store.PageLatestShardedTimedEvents(ctx, recording.ID, 10)
	if err != nil || len(last.Events) != 10 || last.Events[0].EventArchiveOrdinal != eventCount-9 || last.Events[9].EventArchiveOrdinal != eventCount || last.HasMore {
		t.Fatalf("last event page=%#v err=%v", last, err)
	}
	count, objects, err := store.VerifyShardedTimedEvents(ctx, recording.ID)
	if err != nil || count != eventCount || objects < 2*eventCount/adapterproto.MaxTimedEventBatchEvents {
		t.Fatalf("verify count=%d objects=%d err=%v", count, objects, err)
	}
	// Exercise the production append and dedupe path against the 200-hour tail.
	final := adapterproto.TimedEventBatch{StreamID: "scale-tail", BatchID: "scale-final", ResumeState: json.RawMessage(`{"cursor":"final"}`), Events: []adapterproto.TimedEvent{{EventType: "chat.message", SourceEventID: "scale-final-event", Message: &adapterproto.TimedEventMessage{Text: "tail"}}}}
	appended, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, final)
	if err != nil || appended.Inserted != 1 || appended.EventCount != eventCount+1 {
		t.Fatalf("production tail append=%+v err=%v", appended, err)
	}
	replayed, err := store.AppendShardedTimedEventBatch(ctx, recording.ID, final)
	if err != nil || !replayed.Duplicate || replayed.EventCount != eventCount+1 {
		t.Fatalf("production tail replay=%+v err=%v", replayed, err)
	}
	last, err = store.PageLatestShardedTimedEvents(ctx, recording.ID, 10)
	if err != nil || len(last.Events) != 10 || last.Events[9].EventArchiveOrdinal != eventCount+1 || last.Events[9].SourceEventID != "scale-final-event" {
		t.Fatalf("production tail page=%#v err=%v", last, err)
	}
}
