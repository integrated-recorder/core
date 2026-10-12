package adapterproto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	MethodEventsStart = "events.start"
	MethodEventsAck   = "events.ack"
	MethodEventsStop  = "events.stop"
	MethodEventsBatch = "events.batch"
	MethodEventsEnded = "events.ended"

	MaxTimedEventBytes       = 32 << 10
	MaxTimedEventBatchBytes  = 256 << 10
	MaxTimedEventBatchEvents = 64
	MaxTimedEventResumeBytes = 16 << 10
	MaxTimedEventExtension   = 8 << 10
)

var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z0-9][a-z0-9_-]*)+$`)

// TimedEvent is a platform-neutral projection. Payload and Extension are
// validated JSON objects; Core never interprets platform-specific fields.
type TimedEvent struct {
	EventType       string             `json:"event_type"`
	SourceEventID   string             `json:"source_event_id,omitempty"`
	SourceSequence  *uint64            `json:"source_sequence,omitempty"`
	SourceTimestamp *time.Time         `json:"source_timestamp,omitempty"`
	Actor           *TimedEventActor   `json:"actor,omitempty"`
	Message         *TimedEventMessage `json:"message,omitempty"`
	ReplyTo         *TimedEventReply   `json:"reply_to,omitempty"`
	Amount          string             `json:"amount,omitempty"`
	Currency        string             `json:"currency,omitempty"`
	RelatedEventID  string             `json:"related_event_id,omitempty"`
	Payload         json.RawMessage    `json:"payload,omitempty"`
	Extension       json.RawMessage    `json:"extension,omitempty"`
}

type TimedEventMessage struct {
	Text      string               `json:"text,omitempty"`
	Fragments []TimedEventFragment `json:"fragments,omitempty"`
}

type TimedEventFragment struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type TimedEventReply struct {
	SourceEventID string           `json:"source_event_id,omitempty"`
	Actor         *TimedEventActor `json:"actor,omitempty"`
	Text          string           `json:"text,omitempty"`
}

type TimedEventActor struct {
	SourceID    string   `json:"source_id,omitempty"`
	DisplayName string   `json:"display_name,omitempty"`
	Badges      []string `json:"badges,omitempty"`
}

type TimedEventBatch struct {
	StreamID    string          `json:"stream_id"`
	BatchID     string          `json:"batch_id"`
	Events      []TimedEvent    `json:"events"`
	ResumeState json.RawMessage `json:"resume_state,omitempty"`
}

type TimedEventStartParams struct {
	StreamID      string                     `json:"stream_id"`
	RecordingID   string                     `json:"recording_id"`
	Resource      *ResourceRef               `json:"resource,omitempty"`
	Current       MediaSource                `json:"current"`
	Configuration map[string]json.RawMessage `json:"configuration,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	State         []StateDocument            `json:"state,omitempty"`
	ResumeState   json.RawMessage            `json:"resume_state,omitempty"`
}

type TimedEventStartResult struct {
	Accepted bool `json:"accepted"`
}

type TimedEventAckParams struct {
	StreamID string `json:"stream_id"`
	BatchID  string `json:"batch_id"`
}

type TimedEventStopParams struct {
	StreamID string `json:"stream_id"`
}

// TimedEventStream is the Core-side bounded connection to an optional source
// event capability. Cursor acknowledgement occurs only after canonical commit.
type TimedEventStream struct {
	Batches <-chan TimedEventBatch
	Errors  <-chan error
	Done    <-chan struct{}
	Ack     func(context.Context, TimedEventAckParams) error
	Stop    func(context.Context) error
}

func (b TimedEventBatch) Validate() error {
	if strings.TrimSpace(b.StreamID) == "" || len(b.StreamID) > 128 || strings.TrimSpace(b.BatchID) == "" || len(b.BatchID) > 256 {
		return errors.New("timed event stream and batch identities are required")
	}
	if len(b.Events) == 0 || len(b.Events) > MaxTimedEventBatchEvents {
		return errors.New("timed event batch size is invalid")
	}
	if len(b.ResumeState) > MaxTimedEventResumeBytes || len(b.ResumeState) > 0 && !json.Valid(b.ResumeState) {
		return errors.New("timed event resume state is invalid")
	}
	for _, event := range b.Events {
		if err := event.Validate(); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(b)
	if err != nil || len(encoded) > MaxTimedEventBatchBytes {
		return errors.New("timed event batch exceeds its size limit")
	}
	return nil
}

func (e TimedEvent) Validate() error {
	if !eventTypePattern.MatchString(e.EventType) || len(e.EventType) > 128 {
		return errors.New("timed event type is invalid")
	}
	if len(e.SourceEventID) > 512 || len(e.RelatedEventID) > 512 {
		return errors.New("timed event identity exceeds its size limit")
	}
	if e.SourceTimestamp != nil && e.SourceTimestamp.IsZero() {
		return errors.New("timed event source timestamp is invalid")
	}
	if e.Actor != nil {
		if len(e.Actor.SourceID) > 512 || len(e.Actor.DisplayName) > 512 || len(e.Actor.Badges) > 32 {
			return errors.New("timed event actor exceeds its size limit")
		}
		for _, badge := range e.Actor.Badges {
			if len(badge) > 128 {
				return errors.New("timed event badge exceeds its size limit")
			}
		}
	}
	if e.Message != nil {
		if len(e.Message.Text) > MaxTimedEventBytes || len(e.Message.Fragments) > 256 {
			return errors.New("timed event message exceeds its size limit")
		}
		for _, fragment := range e.Message.Fragments {
			if fragment.Type == "" || len(fragment.Type) > 64 || len(fragment.Text) > MaxTimedEventBytes {
				return errors.New("timed event fragment is invalid")
			}
		}
	}
	if e.ReplyTo != nil && (len(e.ReplyTo.SourceEventID) > 512 || len(e.ReplyTo.Text) > MaxTimedEventBytes) {
		return errors.New("timed event reply exceeds its size limit")
	}
	if len(e.Amount) > 64 || len(e.Currency) > 8 {
		return errors.New("timed event amount exceeds its size limit")
	}
	for name, raw := range map[string]json.RawMessage{"payload": e.Payload, "extension": e.Extension} {
		if len(raw) == 0 {
			continue
		}
		limit := MaxTimedEventBytes
		if name == "extension" {
			limit = MaxTimedEventExtension
		}
		if len(raw) > limit || !json.Valid(raw) || !isJSONSafeObject(raw, 0) {
			return fmt.Errorf("timed event %s is invalid", name)
		}
	}
	encoded, err := json.Marshal(e)
	if err != nil || len(encoded) > MaxTimedEventBytes {
		return errors.New("timed event exceeds its size limit")
	}
	return nil
}

func isJSONSafeObject(raw []byte, depth int) bool {
	if depth > 8 {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return false
	}
	return safeJSONValue(value, depth)
}

func safeJSONValue(value any, depth int) bool {
	if depth > 8 {
		return false
	}
	switch v := value.(type) {
	case map[string]any:
		if len(v) > 128 {
			return false
		}
		for key, child := range v {
			lower := strings.ToLower(strings.NewReplacer("-", "", "_", "", ".", "").Replace(key))
			for _, forbidden := range []string{"cookie", "authorization", "token", "password", "secret", "signedurl", "setcookie", "header", "credential", "accesskey", "privatekey"} {
				if strings.Contains(lower, forbidden) {
					return false
				}
			}
			if !safeJSONValue(child, depth+1) {
				return false
			}
		}
	case []any:
		if len(v) > 256 {
			return false
		}
		for _, child := range v {
			if !safeJSONValue(child, depth+1) {
				return false
			}
		}
	case string:
		if len(v) > MaxTimedEventBytes {
			return false
		}
		if containsSecretURLMaterial(v) {
			return false
		}
	}
	return true
}

func containsSecretURLMaterial(value string) bool {
	u, err := url.Parse(strings.TrimSpace(value))
	if err != nil || !u.IsAbs() || u.Host == "" {
		return false
	}
	if u.User != nil {
		return true
	}
	for key := range u.Query() {
		normalized := strings.ToLower(strings.NewReplacer("-", "", "_", "", ".", "").Replace(key))
		for _, forbidden := range []string{"authorization", "token", "cookie", "password", "secret", "credential", "signature", "signedurl", "apikey", "privatekey", "accesskey", "keypairid", "policy", "sig", "key", "xamz", "xgoog"} {
			if strings.Contains(normalized, forbidden) {
				return true
			}
		}
	}
	return false
}
