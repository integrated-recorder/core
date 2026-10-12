package adapterproto

import (
	"encoding/json"
	"testing"
)

func TestTimedEventValidationPreservesUnknownTypesAndRejectsSecrets(t *testing.T) {
	valid := TimedEventBatch{StreamID: "stream", BatchID: "batch-1", Events: []TimedEvent{{
		EventType: "custom.example", Payload: json.RawMessage(`{"value":1}`), Extension: json.RawMessage(`{"platform_field":"preserved"}`),
	}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid custom event rejected: %v", err)
	}
	for name, event := range map[string]TimedEvent{
		"credential key":      {EventType: "chat.message", Payload: json.RawMessage(`{"authorization":"Bearer secret"}`)},
		"signed URL":          {EventType: "chat.message", Extension: json.RawMessage(`{"url":"https://example.test/live?X-Amz-Signature=secret"}`)},
		"key URL":             {EventType: "chat.message", Payload: json.RawMessage(`{"url":"https://example.test/live?key=secret"}`)},
		"oversized extension": {EventType: "chat.message", Extension: json.RawMessage(`{"blob":"` + string(make([]byte, MaxTimedEventExtension)) + `"}`)},
		"invalid type":        {EventType: "Chat.Message"},
	} {
		t.Run(name, func(t *testing.T) {
			batch := TimedEventBatch{StreamID: "stream", BatchID: "batch", Events: []TimedEvent{event}}
			if err := batch.Validate(); err == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
}

func TestTimedEventBatchBounds(t *testing.T) {
	batch := TimedEventBatch{StreamID: "stream", BatchID: "batch", Events: make([]TimedEvent, MaxTimedEventBatchEvents+1)}
	if err := batch.Validate(); err == nil {
		t.Fatal("oversized event count accepted")
	}
}
