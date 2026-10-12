package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
)

const (
	timedEventIndexPageEntries = 4096
	timedEventIdentityBuckets  = 4096
)

// TimedEventRecord is the queryable canonical event projection. It contains no
// acquisition cursor, credentials, or transport frame.
type TimedEventRecord struct {
	EventArchiveOrdinal  uint64                          `json:"event_archive_ordinal"`
	EventTimelineOrdinal uint64                          `json:"event_timeline_ordinal"`
	EventType            string                          `json:"event_type"`
	SourceEventID        string                          `json:"source_event_id,omitempty"`
	SourceSequence       *uint64                         `json:"source_sequence,omitempty"`
	SourceTimestamp      *time.Time                      `json:"source_timestamp,omitempty"`
	ObservedAt           time.Time                       `json:"observed_at"`
	PresentationOffsetMS int64                           `json:"presentation_offset_ms"`
	Actor                *adapterproto.TimedEventActor   `json:"actor,omitempty"`
	Message              *adapterproto.TimedEventMessage `json:"message,omitempty"`
	ReplyTo              *adapterproto.TimedEventReply   `json:"reply_to,omitempty"`
	Amount               string                          `json:"amount,omitempty"`
	Currency             string                          `json:"currency,omitempty"`
	RelatedEventID       string                          `json:"related_event_id,omitempty"`
	Payload              json.RawMessage                 `json:"payload,omitempty"`
	Extension            json.RawMessage                 `json:"extension,omitempty"`
	IdentityHash         string                          `json:"identity_hash"`
}

type timedEventBatchPage struct {
	Version           int                `json:"version"`
	Number            uint64             `json:"number"`
	FirstEventOrdinal uint64             `json:"first_event_ordinal,omitempty"`
	LastEventOrdinal  uint64             `json:"last_event_ordinal,omitempty"`
	BatchID           string             `json:"batch_id"`
	InputSHA256       string             `json:"input_sha256"`
	PageSHA256        string             `json:"page_sha256"`
	Events            []TimedEventRecord `json:"events"`
	ResumeState       json.RawMessage    `json:"resume_state,omitempty"`
}

type timedEventIdentityEntry struct {
	Hash    string `json:"hash"`
	Ordinal uint64 `json:"ordinal"`
}

type timedEventIdentityPage struct {
	Version    int                       `json:"version"`
	Number     uint64                    `json:"number"`
	Entries    []timedEventIdentityEntry `json:"entries"`
	PageSHA256 string                    `json:"page_sha256"`
}

type timedEventBatchIdentity struct {
	Version     int    `json:"version"`
	Ordinal     uint64 `json:"ordinal"`
	BatchID     string `json:"batch_id"`
	InputSHA256 string `json:"input_sha256"`
	PageSHA256  string `json:"page_sha256"`
}

type TimedEventAppendResult struct {
	BatchOrdinal    uint64
	EventCount      uint64
	Inserted        int
	Duplicate       bool
	ArchiveRevision uint64
}

type TimedEventQueryPage struct {
	Events         []TimedEventRecord
	NextBatch      uint64
	NextEventIndex int
	HasMore        bool
}

// AppendShardedTimedEventBatch atomically publishes one immutable batch page
// and bounded identity index updates before advancing the v2 root high-water.
func (s *Store) AppendShardedTimedEventBatch(ctx context.Context, recordingID string, batch adapterproto.TimedEventBatch) (TimedEventAppendResult, error) {
	if err := checkV2Context(ctx); err != nil {
		return TimedEventAppendResult{}, err
	}
	if batch.Validate() != nil {
		return TimedEventAppendResult{}, ErrShardedArchiveInvalid
	}
	lock := s.v2Lock(recordingID)
	lock.Lock()
	defer lock.Unlock()
	header, err := s.loadRecordingContext(ctx, recordingID)
	if err != nil {
		return TimedEventAppendResult{}, err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || validateV2Header(header) != nil {
		return TimedEventAppendResult{}, ErrShardedArchiveUnavailable
	}
	if header.ArchiveSealed {
		return TimedEventAppendResult{}, ErrArchiveSealed
	}
	if len(header.ShardedArchive.TimedEventIdentityHighWater) == 0 {
		header.ShardedArchive.TimedEventIdentityHighWater = make([]uint64, timedEventIdentityBuckets)
	}
	if len(header.ShardedArchive.TimedEventIdentityHighWater) != timedEventIdentityBuckets {
		return TimedEventAppendResult{}, ErrShardedArchiveInvalid
	}
	if err := s.reconcileTimedEventTailLocked(ctx, recordingID, header); err != nil {
		return TimedEventAppendResult{}, err
	}
	if err := s.loadRecordingContextInto(ctx, recordingID, header); err != nil {
		return TimedEventAppendResult{}, err
	}
	batchKey := timedEventBatchKey(batch.BatchID)
	inputDigest, err := timedEventInputDigest(batch)
	if err != nil {
		return TimedEventAppendResult{}, ErrShardedArchiveInvalid
	}
	var prior timedEventBatchIdentity
	if err := s.loadV2JSON(ctx, recordingID, batchKey, 4096, &prior); err == nil {
		if prior.Version != 1 || prior.BatchID != batch.BatchID || prior.Ordinal == 0 || prior.Ordinal > header.ShardedArchive.TimedEventBatchCount || !validLowerDigest(prior.InputSHA256) || !validLowerDigest(prior.PageSHA256) {
			return TimedEventAppendResult{}, ErrShardedArchiveInvalid
		}
		if prior.InputSHA256 != inputDigest {
			return TimedEventAppendResult{}, ErrShardedArchiveConflict
		}
		return TimedEventAppendResult{BatchOrdinal: prior.Ordinal, EventCount: header.ShardedArchive.TimedEventCount, Duplicate: true, ArchiveRevision: header.ArchiveRevision}, nil
	} else if !errors.Is(err, ErrNotFound) {
		return TimedEventAppendResult{}, err
	}
	if header.ShardedArchive.TimedEventBatchCount == ^uint64(0) {
		return TimedEventAppendResult{}, ErrShardedArchiveInvalid
	}
	pageOrdinal := header.ShardedArchive.TimedEventBatchCount + 1
	page := timedEventBatchPage{Version: 1, Number: pageOrdinal, BatchID: batch.BatchID, InputSHA256: inputDigest, ResumeState: append(json.RawMessage(nil), batch.ResumeState...), Events: make([]TimedEventRecord, 0, len(batch.Events))}
	page.LastEventOrdinal = header.ShardedArchive.TimedEventCount
	seen := make(map[string]struct{}, len(batch.Events))
	lastOffset := header.ShardedArchive.TimedEventLastOffsetMillis
	for eventIndex, event := range batch.Events {
		base := timedEventIdentityBase(event)
		identity := timedEventIdentity(event, base, batch.BatchID, eventIndex)
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		exists, err := s.timedEventIdentityExistsLocked(ctx, recordingID, header, identity)
		if err != nil {
			return TimedEventAppendResult{}, err
		}
		if exists {
			continue
		}
		if header.ShardedArchive.TimedEventCount > ^uint64(0)-uint64(len(page.Events))-1 {
			return TimedEventAppendResult{}, ErrShardedArchiveInvalid
		}
		observed := time.Now().UTC()
		offset := observed.Sub(header.StartedAt).Milliseconds()
		if offset < 0 {
			offset = 0
		}
		if offset < lastOffset {
			offset = lastOffset
		}
		lastOffset = offset
		record := TimedEventRecord{EventArchiveOrdinal: header.ShardedArchive.TimedEventCount + uint64(len(page.Events)) + 1, EventTimelineOrdinal: header.ShardedArchive.TimedEventCount + uint64(len(page.Events)) + 1, EventType: event.EventType, SourceEventID: event.SourceEventID, SourceSequence: event.SourceSequence, SourceTimestamp: event.SourceTimestamp, ObservedAt: observed, PresentationOffsetMS: offset, Actor: event.Actor, Message: event.Message, ReplyTo: event.ReplyTo, Amount: event.Amount, Currency: event.Currency, RelatedEventID: event.RelatedEventID, Payload: append(json.RawMessage(nil), event.Payload...), Extension: append(json.RawMessage(nil), event.Extension...), IdentityHash: identity}
		page.Events = append(page.Events, record)
		if page.FirstEventOrdinal == 0 {
			page.FirstEventOrdinal = record.EventArchiveOrdinal
		}
		page.LastEventOrdinal = record.EventArchiveOrdinal
	}
	page.PageSHA256, err = timedEventPageDigest(page)
	if err != nil {
		return TimedEventAppendResult{}, ErrShardedArchiveInvalid
	}
	pagePath := timedEventBatchPath(pageOrdinal)
	if err := s.saveV2JSON(ctx, recordingID, pagePath, page, archiveShardMaxBytes); err != nil {
		return TimedEventAppendResult{}, err
	}
	if err := s.publishTimedEventPageLocked(ctx, recordingID, header, page); err != nil {
		return TimedEventAppendResult{}, err
	}
	return TimedEventAppendResult{BatchOrdinal: pageOrdinal, EventCount: header.ShardedArchive.TimedEventCount, Inserted: len(page.Events), ArchiveRevision: header.ArchiveRevision}, nil
}

// LoadLatestShardedTimedEventResumeState reads the private cursor committed
// with the most recent event batch. It is never returned by public queries.
func (s *Store) LoadLatestShardedTimedEventResumeState(ctx context.Context, recordingID string) (json.RawMessage, error) {
	header, err := s.LoadRecordingHeader(ctx, recordingID)
	if err != nil {
		return nil, err
	}
	count := header.ShardedArchive.TimedEventBatchCount
	if count == 0 {
		return nil, nil
	}
	var page timedEventBatchPage
	if err := s.loadV2JSON(ctx, recordingID, timedEventBatchPath(count), archiveShardMaxBytes, &page); err != nil {
		return nil, err
	}
	if page.Version != 1 || page.Number != count || !validTimedEventPageDigest(page) {
		return nil, ErrShardedArchiveInvalid
	}
	return append(json.RawMessage(nil), page.ResumeState...), nil
}

// PageShardedTimedEvents returns a bounded ordered page. Its cursor is a
// batch page and event offset, independent of media ordinals and revisions.
func (s *Store) PageShardedTimedEvents(ctx context.Context, recordingID string, batchOrdinal uint64, eventIndex, limit int, eventType string, fromOffsetMS, toOffsetMS *int64) (TimedEventQueryPage, error) {
	if limit < 1 || limit > 500 || eventIndex < 0 {
		return TimedEventQueryPage{}, ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, recordingID)
	if err != nil {
		return TimedEventQueryPage{}, err
	}
	if batchOrdinal == 0 {
		batchOrdinal = 1
	}
	page := TimedEventQueryPage{Events: make([]TimedEventRecord, 0, limit), NextBatch: batchOrdinal, NextEventIndex: eventIndex}
	for batchOrdinal <= header.ShardedArchive.TimedEventBatchCount {
		if err := checkV2Context(ctx); err != nil {
			return TimedEventQueryPage{}, err
		}
		var batch timedEventBatchPage
		if err := s.loadV2JSON(ctx, recordingID, timedEventBatchPath(batchOrdinal), archiveShardMaxBytes, &batch); err != nil {
			return TimedEventQueryPage{}, err
		}
		if batch.Version != 1 || batch.Number != batchOrdinal || len(batch.Events) > adapterproto.MaxTimedEventBatchEvents || !validTimedEventPageDigest(batch) {
			return TimedEventQueryPage{}, ErrShardedArchiveInvalid
		}
		for eventIndex < len(batch.Events) {
			event := batch.Events[eventIndex]
			currentIndex := eventIndex
			page.NextBatch, page.NextEventIndex = batchOrdinal, currentIndex+1
			eventIndex++
			if event.EventArchiveOrdinal > header.ShardedArchive.TimedEventCount {
				continue
			}
			if eventType != "" && event.EventType != eventType {
				continue
			}
			if fromOffsetMS != nil && event.PresentationOffsetMS < *fromOffsetMS || toOffsetMS != nil && event.PresentationOffsetMS > *toOffsetMS {
				continue
			}
			if len(page.Events) == limit {
				page.NextEventIndex = currentIndex
				page.HasMore = true
				return page, nil
			}
			page.Events = append(page.Events, event)
		}
		batchOrdinal++
		eventIndex = 0
		page.NextBatch, page.NextEventIndex = batchOrdinal, 0
	}
	return page, nil
}

func (s *Store) PageShardedTimedEventsAfter(ctx context.Context, recordingID string, after uint64, limit int, eventType string) (TimedEventQueryPage, error) {
	header, err := s.LoadRecordingHeader(ctx, recordingID)
	if err != nil {
		return TimedEventQueryPage{}, err
	}
	count := header.ShardedArchive.TimedEventBatchCount
	if count == 0 {
		return TimedEventQueryPage{Events: []TimedEventRecord{}}, nil
	}
	low, high := uint64(1), count
	for low < high {
		middle := low + (high-low)/2
		var candidate timedEventBatchPage
		if err := s.loadV2JSON(ctx, recordingID, timedEventBatchPath(middle), archiveShardMaxBytes, &candidate); err != nil {
			return TimedEventQueryPage{}, err
		}
		if candidate.Version != 1 || candidate.Number != middle || !validTimedEventPageDigest(candidate) {
			return TimedEventQueryPage{}, ErrShardedArchiveInvalid
		}
		if candidate.LastEventOrdinal > after {
			high = middle
		} else {
			low = middle + 1
		}
	}
	if low == count && after >= header.ShardedArchive.TimedEventCount {
		return TimedEventQueryPage{Events: []TimedEventRecord{}}, nil
	}
	var first timedEventBatchPage
	if err := s.loadV2JSON(ctx, recordingID, timedEventBatchPath(low), archiveShardMaxBytes, &first); err != nil {
		return TimedEventQueryPage{}, err
	}
	if first.Version != 1 || first.Number != low || !validTimedEventPageDigest(first) {
		return TimedEventQueryPage{}, ErrShardedArchiveInvalid
	}
	index := 0
	for index < len(first.Events) && first.Events[index].EventArchiveOrdinal <= after {
		index++
	}
	return s.PageShardedTimedEvents(ctx, recordingID, low, index, limit, eventType, nil, nil)
}

func (s *Store) PageLatestShardedTimedEvents(ctx context.Context, recordingID string, limit int) (TimedEventQueryPage, error) {
	if limit < 1 || limit > 500 {
		return TimedEventQueryPage{}, ErrShardedArchiveInvalid
	}
	header, err := s.LoadRecordingHeader(ctx, recordingID)
	if err != nil {
		return TimedEventQueryPage{}, err
	}
	count := header.ShardedArchive.TimedEventBatchCount
	eventCount := header.ShardedArchive.TimedEventCount
	if count == 0 || eventCount == 0 {
		return TimedEventQueryPage{Events: []TimedEventRecord{}}, nil
	}
	after := uint64(0)
	if eventCount > uint64(limit) {
		after = eventCount - uint64(limit)
	}
	return s.PageShardedTimedEventsAfter(ctx, recordingID, after, limit, "")
}

func (s *Store) IterateShardedTimedEvents(ctx context.Context, recordingID string, visit func(TimedEventRecord) error) error {
	if visit == nil {
		return ErrShardedArchiveInvalid
	}
	batch, index := uint64(1), 0
	for {
		page, err := s.PageShardedTimedEvents(ctx, recordingID, batch, index, 500, "", nil, nil)
		if err != nil {
			return err
		}
		for _, event := range page.Events {
			if err := visit(event); err != nil {
				return err
			}
		}
		if !page.HasMore {
			return nil
		}
		batch, index = page.NextBatch, page.NextEventIndex
	}
}

// VerifyShardedTimedEvents validates the bounded event pages and their
// persisted batch identities without retaining archive-sized state.
func (s *Store) VerifyShardedTimedEvents(ctx context.Context, recordingID string) (uint64, uint64, error) {
	header, err := s.LoadRecordingHeader(ctx, recordingID)
	if err != nil {
		return 0, 0, err
	}
	if header.FormatVersion != ShardedArchiveFormatVersion || validateV2Header(header) != nil {
		return 0, 0, ErrShardedArchiveInvalid
	}
	var count uint64
	var objects uint64
	for ordinal := uint64(1); ordinal <= header.ShardedArchive.TimedEventBatchCount; ordinal++ {
		if err := checkV2Context(ctx); err != nil {
			return count, objects, err
		}
		var page timedEventBatchPage
		if err := s.loadV2JSON(ctx, recordingID, timedEventBatchPath(ordinal), archiveShardMaxBytes, &page); err != nil {
			return count, objects, err
		}
		if page.Version != 1 || page.Number != ordinal || !validTimedEventPageDigest(page) {
			return count, objects, ErrShardedArchiveInvalid
		}
		var indexed timedEventBatchIdentity
		if err := s.loadV2JSON(ctx, recordingID, timedEventBatchKey(page.BatchID), 4096, &indexed); err != nil {
			return count, objects, err
		}
		if indexed.Version != 1 || indexed.Ordinal != ordinal || indexed.BatchID != page.BatchID || indexed.InputSHA256 != page.InputSHA256 || indexed.PageSHA256 != page.PageSHA256 {
			return count, objects, ErrShardedArchiveInvalid
		}
		objects += 2
		for _, event := range page.Events {
			if err := checkV2Context(ctx); err != nil {
				return count, objects, err
			}
			if event.EventArchiveOrdinal != count+1 || event.EventTimelineOrdinal != event.EventArchiveOrdinal {
				return count, objects, ErrShardedArchiveInvalid
			}
			count++
		}
	}
	for bucket, highWater := range header.ShardedArchive.TimedEventIdentityHighWater {
		for pageNo := uint64(0); pageNo < timedEventIdentityPageCount(highWater); pageNo++ {
			if err := checkV2Context(ctx); err != nil {
				return count, objects, err
			}
			var page timedEventIdentityPage
			if err := s.loadV2JSON(ctx, recordingID, timedEventIdentityPath(bucket, pageNo), archiveShardMaxBytes, &page); err != nil {
				return count, objects, err
			}
			start := pageNo * timedEventIndexPageEntries
			want := highWater - start
			if want > timedEventIndexPageEntries {
				want = timedEventIndexPageEntries
			}
			if page.Version != 1 || page.Number != pageNo || uint64(len(page.Entries)) != want || !validTimedEventIdentityPage(page, bucket, count) {
				return count, objects, ErrShardedArchiveInvalid
			}
			objects++
		}
	}
	if count != header.ShardedArchive.TimedEventCount {
		return count, objects, ErrShardedArchiveInvalid
	}
	return count, objects, nil
}

func (s *Store) reconcileTimedEventTailLocked(ctx context.Context, recordingID string, header *domain.Recording) error {
	if header.ShardedArchive == nil {
		return ErrShardedArchiveInvalid
	}
	for header.ShardedArchive.TimedEventBatchCount < ^uint64(0) {
		next := header.ShardedArchive.TimedEventBatchCount + 1
		var page timedEventBatchPage
		err := s.loadV2JSON(ctx, recordingID, timedEventBatchPath(next), archiveShardMaxBytes, &page)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if page.Version != 1 || page.Number != next || len(page.Events) > adapterproto.MaxTimedEventBatchEvents || !validTimedEventPageDigest(page) {
			return ErrShardedArchiveInvalid
		}
		if err := s.publishTimedEventPageLocked(ctx, recordingID, header, page); err != nil {
			return err
		}
		if err := s.loadRecordingContextInto(ctx, recordingID, header); err != nil {
			return err
		}
	}
	return ErrShardedArchiveInvalid
}

func (s *Store) publishTimedEventPageLocked(ctx context.Context, recordingID string, header *domain.Recording, page timedEventBatchPage) error {
	if header.ShardedArchive == nil || page.Number != header.ShardedArchive.TimedEventBatchCount+1 {
		return ErrShardedArchiveInvalid
	}
	if header.ShardedArchive.TimedEventCount > ^uint64(0)-uint64(len(page.Events)) {
		return ErrShardedArchiveInvalid
	}
	if len(page.Events) == 0 {
		if page.FirstEventOrdinal != 0 || page.LastEventOrdinal != header.ShardedArchive.TimedEventCount {
			return ErrShardedArchiveInvalid
		}
	} else if page.FirstEventOrdinal != header.ShardedArchive.TimedEventCount+1 || page.LastEventOrdinal != header.ShardedArchive.TimedEventCount+uint64(len(page.Events)) {
		return ErrShardedArchiveInvalid
	}
	identityEntries := make([]timedEventIdentityEntry, 0, len(page.Events))
	lastOffset := header.ShardedArchive.TimedEventLastOffsetMillis
	for _, event := range page.Events {
		if event.EventArchiveOrdinal == 0 || event.EventTimelineOrdinal == 0 || event.IdentityHash == "" {
			return ErrShardedArchiveInvalid
		}
		if event.PresentationOffsetMS < lastOffset {
			return ErrShardedArchiveInvalid
		}
		lastOffset = event.PresentationOffsetMS
		identityEntries = append(identityEntries, timedEventIdentityEntry{Hash: event.IdentityHash, Ordinal: event.EventArchiveOrdinal})
	}
	if err := s.appendTimedEventIdentityEntriesLocked(ctx, recordingID, header, identityEntries); err != nil {
		return err
	}
	page.PageSHA256 = ""
	pageDigest, err := timedEventPageDigest(page)
	if err != nil {
		return ErrShardedArchiveInvalid
	}
	page.PageSHA256 = pageDigest
	index := timedEventBatchIdentity{Version: 1, Ordinal: page.Number, BatchID: page.BatchID, InputSHA256: page.InputSHA256, PageSHA256: page.PageSHA256}
	if err := s.saveV2JSON(ctx, recordingID, timedEventBatchKey(page.BatchID), index, 4096); err != nil {
		return err
	}
	header.ShardedArchive.TimedEventBatchCount = page.Number
	if len(page.Events) > 0 {
		last := page.Events[len(page.Events)-1]
		header.ShardedArchive.TimedEventCount = last.EventArchiveOrdinal
		header.ShardedArchive.TimedEventLastOffsetMillis = last.PresentationOffsetMS
	}
	if err := advanceV2ArchiveRevision(header); err != nil {
		return err
	}
	return s.saveHeaderLocked(ctx, header, nil)
}

func (s *Store) appendTimedEventIdentityEntriesLocked(ctx context.Context, recordingID string, header *domain.Recording, entries []timedEventIdentityEntry) error {
	if len(header.ShardedArchive.TimedEventIdentityHighWater) == 0 {
		header.ShardedArchive.TimedEventIdentityHighWater = make([]uint64, timedEventIdentityBuckets)
	}
	if len(header.ShardedArchive.TimedEventIdentityHighWater) != timedEventIdentityBuckets {
		return ErrShardedArchiveInvalid
	}
	grouped := make(map[int][]timedEventIdentityEntry)
	maxIndexedOrdinal := header.ShardedArchive.TimedEventCount
	for _, entry := range entries {
		bucket, err := timedEventBucket(entry.Hash)
		if err != nil {
			return err
		}
		if entry.Ordinal > maxIndexedOrdinal {
			maxIndexedOrdinal = entry.Ordinal
		}
		grouped[bucket] = append(grouped[bucket], entry)
	}
	for bucket, additions := range grouped {
		pages := make(map[uint64]*timedEventIdentityPage)
		present := make(map[uint64]map[string]uint64)
		dirty := make(map[uint64]bool)
		for _, add := range additions {
			count := header.ShardedArchive.TimedEventIdentityHighWater[bucket]
			pageNo := count / timedEventIndexPageEntries
			page := pages[pageNo]
			if page == nil {
				page = &timedEventIdentityPage{Version: 1, Number: pageNo}
				err := s.loadV2JSON(ctx, recordingID, timedEventIdentityPath(bucket, pageNo), archiveShardMaxBytes, page)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
				if err == nil && (page.Version != 1 || page.Number != pageNo || uint64(len(page.Entries)) > timedEventIndexPageEntries) {
					return ErrShardedArchiveInvalid
				}
				if err == nil && !validTimedEventIdentityPage(*page, bucket, maxIndexedOrdinal) {
					return ErrShardedArchiveInvalid
				}
				pages[pageNo] = page
				present[pageNo] = make(map[string]uint64, len(page.Entries)+len(additions))
				for _, item := range page.Entries {
					present[pageNo][item.Hash] = item.Ordinal
				}
			}
			expectedOffset := int(count % timedEventIndexPageEntries)
			if ordinal, exists := present[pageNo][add.Hash]; exists {
				if ordinal != add.Ordinal {
					return ErrShardedArchiveConflict
				}
				header.ShardedArchive.TimedEventIdentityHighWater[bucket]++
				continue
			}
			if len(page.Entries) > expectedOffset {
				return ErrShardedArchiveConflict
			}
			if len(page.Entries) != expectedOffset {
				return ErrShardedArchiveInvalid
			}
			page.Entries = append(page.Entries, add)
			present[pageNo][add.Hash] = add.Ordinal
			dirty[pageNo] = true
			header.ShardedArchive.TimedEventIdentityHighWater[bucket]++
		}
		for pageNo, page := range pages {
			if !dirty[pageNo] {
				continue
			}
			sort.Slice(page.Entries, func(i, j int) bool { return page.Entries[i].Hash < page.Entries[j].Hash })
			digest, err := timedEventIdentityPageDigest(*page)
			if err != nil {
				return ErrShardedArchiveInvalid
			}
			page.PageSHA256 = digest
			if err := s.saveV2JSON(ctx, recordingID, timedEventIdentityPath(bucket, pageNo), page, archiveShardMaxBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) timedEventIdentityExistsLocked(ctx context.Context, recordingID string, header *domain.Recording, identity string) (bool, error) {
	bucket, err := timedEventBucket(identity)
	if err != nil {
		return false, err
	}
	return s.timedEventIdentityExistsInBucket(ctx, recordingID, header, bucket, identity)
}

func (s *Store) timedEventIdentityExistsInBucket(ctx context.Context, recordingID string, header *domain.Recording, bucket int, identity string) (bool, error) {
	if len(header.ShardedArchive.TimedEventIdentityHighWater) == 0 {
		return false, nil
	}
	if len(header.ShardedArchive.TimedEventIdentityHighWater) != timedEventIdentityBuckets || bucket < 0 || bucket >= timedEventIdentityBuckets {
		return false, ErrShardedArchiveInvalid
	}
	count := header.ShardedArchive.TimedEventIdentityHighWater[bucket]
	for ordinal := uint64(0); ordinal < count; {
		pageNo := ordinal / timedEventIndexPageEntries
		var page timedEventIdentityPage
		if err := s.loadV2JSON(ctx, recordingID, timedEventIdentityPath(bucket, pageNo), archiveShardMaxBytes, &page); err != nil {
			return false, err
		}
		if page.Version != 1 || page.Number != pageNo || len(page.Entries) == 0 || uint64(len(page.Entries)) > timedEventIndexPageEntries || !validTimedEventIdentityPage(page, bucket, header.ShardedArchive.TimedEventCount) {
			return false, ErrShardedArchiveInvalid
		}
		if !sort.SliceIsSorted(page.Entries, func(i, j int) bool { return page.Entries[i].Hash < page.Entries[j].Hash }) {
			return false, ErrShardedArchiveInvalid
		}
		index := sort.Search(len(page.Entries), func(index int) bool { return page.Entries[index].Hash >= identity })
		if index < len(page.Entries) && page.Entries[index].Hash == identity {
			return true, nil
		}
		ordinal += uint64(len(page.Entries))
	}
	return false, nil
}

func (s *Store) loadTimedEventIdentityOrdinal(ctx context.Context, recordingID string, header *domain.Recording, bucket int, identity string) (uint64, error) {
	if len(header.ShardedArchive.TimedEventIdentityHighWater) != timedEventIdentityBuckets || bucket < 0 || bucket >= timedEventIdentityBuckets {
		return 0, ErrNotFound
	}
	count := header.ShardedArchive.TimedEventIdentityHighWater[bucket]
	for ordinal := uint64(0); ordinal < count; {
		pageNo := ordinal / timedEventIndexPageEntries
		var page timedEventIdentityPage
		if err := s.loadV2JSON(ctx, recordingID, timedEventIdentityPath(bucket, pageNo), archiveShardMaxBytes, &page); err != nil {
			return 0, err
		}
		index := sort.Search(len(page.Entries), func(index int) bool { return page.Entries[index].Hash >= identity })
		if index < len(page.Entries) && page.Entries[index].Hash == identity {
			return page.Entries[index].Ordinal, nil
		}
		ordinal += uint64(len(page.Entries))
	}
	return 0, ErrNotFound
}

func (s *Store) loadRecordingContextInto(ctx context.Context, id string, target *domain.Recording) error {
	latest, err := s.loadRecordingContext(ctx, id)
	if err != nil {
		return err
	}
	*target = *latest
	return nil
}

func timedEventBatchPath(ordinal uint64) string {
	return fmt.Sprintf("archive/v2/timed-events/batches/%020d", ordinal)
}
func timedEventIdentityPath(bucket int, page uint64) string {
	return fmt.Sprintf("archive/v2/timed-events/identities/%04x/%020d", bucket, page)
}
func timedEventBatchKey(batchID string) string {
	sum := sha256.Sum256([]byte(batchID))
	return "archive/v2/timed-events/batch-ids/" + hex.EncodeToString(sum[:])
}

func timedEventInputDigest(batch adapterproto.TimedEventBatch) (string, error) {
	events := make([]adapterproto.TimedEvent, len(batch.Events))
	copy(events, batch.Events)
	for i := range events {
		events[i].Payload = canonicalEventJSON(events[i].Payload)
		events[i].Extension = canonicalEventJSON(events[i].Extension)
	}
	resume := canonicalEventJSON(batch.ResumeState)
	data, err := json.Marshal(struct {
		BatchID     string                    `json:"batch_id"`
		Events      []adapterproto.TimedEvent `json:"events"`
		ResumeState json.RawMessage           `json:"resume_state,omitempty"`
	}{batch.BatchID, events, resume})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func timedEventPageDigest(page timedEventBatchPage) (string, error) {
	page.PageSHA256 = ""
	data, err := json.Marshal(page)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validTimedEventPageDigest(page timedEventBatchPage) bool {
	if page.Version != 1 || page.Number == 0 || page.BatchID == "" || !validLowerDigest(page.InputSHA256) || !validLowerDigest(page.PageSHA256) || len(page.Events) > adapterproto.MaxTimedEventBatchEvents {
		return false
	}
	if len(page.Events) == 0 {
		if page.FirstEventOrdinal != 0 {
			return false
		}
	} else if page.FirstEventOrdinal == 0 || page.LastEventOrdinal < page.FirstEventOrdinal || page.LastEventOrdinal-page.FirstEventOrdinal+1 != uint64(len(page.Events)) {
		return false
	}
	seen := make(map[string]struct{}, len(page.Events))
	for index, event := range page.Events {
		input := timedEventFromRecord(event)
		if input.Validate() != nil || event.EventArchiveOrdinal != page.FirstEventOrdinal+uint64(index) || event.EventTimelineOrdinal != event.EventArchiveOrdinal || !validLowerDigest(event.IdentityHash) {
			return false
		}
		base := timedEventIdentityBase(input)
		identity := timedEventIdentity(input, base, page.BatchID, index)
		if identity != event.IdentityHash {
			return false
		}
		if _, duplicate := seen[identity]; duplicate {
			return false
		}
		seen[identity] = struct{}{}
	}
	digest, err := timedEventPageDigest(page)
	return err == nil && digest == page.PageSHA256
}

func timedEventIdentityPageDigest(page timedEventIdentityPage) (string, error) {
	page.PageSHA256 = ""
	data, err := json.Marshal(page)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func validTimedEventIdentityPage(page timedEventIdentityPage, bucket int, eventCount uint64) bool {
	if page.Version != 1 || !validLowerDigest(page.PageSHA256) || len(page.Entries) == 0 || uint64(len(page.Entries)) > timedEventIndexPageEntries || !sort.SliceIsSorted(page.Entries, func(i, j int) bool { return page.Entries[i].Hash < page.Entries[j].Hash }) {
		return false
	}
	for index, entry := range page.Entries {
		if !validLowerDigest(entry.Hash) || entry.Ordinal == 0 || entry.Ordinal > eventCount {
			return false
		}
		resolvedBucket, err := timedEventBucket(entry.Hash)
		if err != nil || resolvedBucket != bucket || index > 0 && page.Entries[index-1].Hash == entry.Hash {
			return false
		}
	}
	digest, err := timedEventIdentityPageDigest(page)
	return err == nil && digest == page.PageSHA256
}

func timedEventFromRecord(record TimedEventRecord) adapterproto.TimedEvent {
	return adapterproto.TimedEvent{EventType: record.EventType, SourceEventID: record.SourceEventID, SourceSequence: record.SourceSequence, SourceTimestamp: record.SourceTimestamp, Actor: record.Actor, Message: record.Message, ReplyTo: record.ReplyTo, Amount: record.Amount, Currency: record.Currency, RelatedEventID: record.RelatedEventID, Payload: record.Payload, Extension: record.Extension}
}

func canonicalEventJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return append(json.RawMessage(nil), raw...)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	return encoded
}
func timedEventBucket(identity string) (int, error) {
	data, err := hex.DecodeString(identity)
	if err != nil || len(data) != sha256.Size {
		return 0, ErrShardedArchiveInvalid
	}
	return int(data[0])<<4 | int(data[1]>>4), nil
}
func timedEventIdentityBase(event adapterproto.TimedEvent) string {
	switch {
	case event.SourceEventID != "":
		return "id\x00" + event.EventType + "\x00" + event.SourceEventID
	case event.SourceSequence != nil:
		return fmt.Sprintf("seq\x00%s\x00%d", event.EventType, *event.SourceSequence)
	}
	identity, _ := json.Marshal(struct {
		EventType       string                          `json:"event_type"`
		SourceTimestamp *time.Time                      `json:"source_timestamp,omitempty"`
		Actor           *adapterproto.TimedEventActor   `json:"actor,omitempty"`
		Message         *adapterproto.TimedEventMessage `json:"message,omitempty"`
		ReplyTo         *adapterproto.TimedEventReply   `json:"reply_to,omitempty"`
		Amount          string                          `json:"amount,omitempty"`
		Currency        string                          `json:"currency,omitempty"`
		RelatedEventID  string                          `json:"related_event_id,omitempty"`
		Payload         json.RawMessage                 `json:"payload,omitempty"`
		Extension       json.RawMessage                 `json:"extension,omitempty"`
	}{event.EventType, event.SourceTimestamp, event.Actor, event.Message, event.ReplyTo, event.Amount, event.Currency, event.RelatedEventID, canonicalEventJSON(event.Payload), canonicalEventJSON(event.Extension)})
	return "fingerprint\x00" + string(identity)
}

func timedEventIdentity(event adapterproto.TimedEvent, base, batchID string, eventIndex int) string {
	identity := base
	if event.SourceEventID == "" && event.SourceSequence == nil {
		// Without a plugin-owned ID or sequence Core cannot distinguish a
		// reconnect replay from a second legitimate event with identical
		// content. Retry identity is therefore limited to this stable batch.
		identity = fmt.Sprintf("batch\x00%s\x00%d", batchID, eventIndex)
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}
