package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/storage"
)

type timedEventCursor struct {
	Batch uint64 `json:"b"`
	Index int    `json:"i"`
}

type timedEventItem struct {
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
}

func timedEventProjection(event storage.TimedEventRecord) timedEventItem {
	return timedEventItem{EventArchiveOrdinal: event.EventArchiveOrdinal, EventTimelineOrdinal: event.EventTimelineOrdinal, EventType: event.EventType, SourceEventID: event.SourceEventID, SourceSequence: event.SourceSequence, SourceTimestamp: event.SourceTimestamp, ObservedAt: event.ObservedAt, PresentationOffsetMS: event.PresentationOffsetMS, Actor: event.Actor, Message: event.Message, ReplyTo: event.ReplyTo, Amount: event.Amount, Currency: event.Currency, RelatedEventID: event.RelatedEventID, Payload: event.Payload, Extension: event.Extension}
}

func (s *Server) timedEventsGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.storage == nil {
		writeError(w, http.StatusServiceUnavailable, "timed event archive is unavailable")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	if recording.FormatVersion != storage.ShardedArchiveFormatVersion {
		writeJSON(w, http.StatusOK, map[string]any{"items": []timedEventItem{}, "next_cursor": "", "has_more": false, "archive_revision": recording.ArchiveRevision, "timeline_revision": recording.TimelineRevision})
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > 500 {
			writeError(w, http.StatusBadRequest, "timed event page limit is invalid")
			return
		}
		limit = parsed
	}
	batch, index := uint64(1), 0
	var after *uint64
	if raw := r.URL.Query().Get("after"); raw != "" {
		value, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, "timed event cursor is invalid")
			return
		}
		after = &value
	}
	tailValue := r.URL.Query().Get("tail")
	if tailValue != "" && tailValue != "0" && tailValue != "1" {
		writeError(w, http.StatusBadRequest, "timed event cursor is invalid")
		return
	}
	tail := tailValue == "1"
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if after != nil || tail {
			writeError(w, http.StatusBadRequest, "timed event cursor is invalid")
			return
		}
		if len(raw) > 256 {
			writeError(w, http.StatusBadRequest, "timed event cursor is invalid")
			return
		}
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(raw)
		var cursor timedEventCursor
		if decodeErr != nil || json.Unmarshal(decoded, &cursor) != nil || cursor.Batch == 0 || cursor.Index < 0 || cursor.Index >= adapterproto.MaxTimedEventBatchEvents {
			writeError(w, http.StatusBadRequest, "timed event cursor is invalid")
			return
		}
		batch, index = cursor.Batch, cursor.Index
	}
	var fromOffset, toOffset *int64
	for key, target := range map[string]**int64{"from_ms": &fromOffset, "to_ms": &toOffset} {
		if raw := r.URL.Query().Get(key); raw != "" {
			value, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil || value < 0 {
				writeError(w, http.StatusBadRequest, "timed event time range is invalid")
				return
			}
			*target = &value
		}
	}
	if fromOffset != nil && toOffset != nil && *fromOffset > *toOffset {
		writeError(w, http.StatusBadRequest, "timed event time range is invalid")
		return
	}
	eventType := r.URL.Query().Get("type")
	if len(eventType) > 128 {
		writeError(w, http.StatusBadRequest, "timed event type filter is invalid")
		return
	}
	if tail && (after != nil || eventType != "" || fromOffset != nil || toOffset != nil) {
		writeError(w, http.StatusBadRequest, "timed event cursor is invalid")
		return
	}
	var page storage.TimedEventQueryPage
	if tail {
		page, err = s.storage.PageLatestShardedTimedEvents(r.Context(), id, limit)
	} else if after != nil {
		if fromOffset != nil || toOffset != nil {
			writeError(w, http.StatusBadRequest, "timed event time range is invalid")
			return
		}
		page, err = s.storage.PageShardedTimedEventsAfter(r.Context(), id, *after, limit, eventType)
	} else {
		page, err = s.storage.PageShardedTimedEvents(r.Context(), id, batch, index, limit, eventType, fromOffset, toOffset)
	}
	if err != nil {
		writeStorageError(w, err)
		return
	}
	items := make([]timedEventItem, 0, len(page.Events))
	for _, event := range page.Events {
		items = append(items, timedEventProjection(event))
	}
	next := ""
	if page.HasMore {
		encoded, _ := json.Marshal(timedEventCursor{Batch: page.NextBatch, Index: page.NextEventIndex})
		next = base64.RawURLEncoding.EncodeToString(encoded)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next, "has_more": page.HasMore, "archive_revision": recording.ArchiveRevision, "timeline_revision": recording.TimelineRevision})
}

func (s *Server) timedEventsExport(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.storage == nil {
		writeError(w, http.StatusServiceUnavailable, "timed event archive is unavailable")
		return
	}
	lock := s.productLock(id)
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.recordingSnapshot(r.Context(), id)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s-timed-events.jsonl\"", recording.ID))
	w.Header().Set("Cache-Control", "no-store")
	if recording.FormatVersion != storage.ShardedArchiveFormatVersion {
		return
	}
	encoder := json.NewEncoder(w)
	err = s.storage.IterateShardedTimedEvents(r.Context(), id, func(event storage.TimedEventRecord) error {
		if err := encoder.Encode(timedEventProjection(event)); err != nil {
			return err
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		return r.Context().Err()
	})
	if err != nil && r.Context().Err() == nil {
		return
	}
}
