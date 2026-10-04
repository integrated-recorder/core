package interaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestInteractionGenericStateTransitions(t *testing.T) {
	tracker := NewTracker()
	prompt := adapterproto.InteractionMessage{Type: "secret_prompt", InteractionID: "opaque-id", Fields: []adapterproto.InteractionField{{Key: "value", Control: "secret", Label: "Value"}}}
	state, err := tracker.Apply(prompt)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "active" || len(state.Messages) != 1 {
		t.Fatalf("state = %#v", state)
	}
	state, err = tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: "opaque-id", Message: "waiting"})
	if err != nil || len(state.Messages) != 2 {
		t.Fatalf("progress state = %#v, %v", state, err)
	}
	state, err = tracker.Apply(adapterproto.InteractionMessage{Type: "complete", InteractionID: "opaque-id"})
	if err != nil || state.Status != "complete" {
		t.Fatalf("terminal state = %#v, %v", state, err)
	}
	if _, err = tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: "opaque-id"}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("terminal update error = %v", err)
	}
}

func TestTrackerBoundsHistoryExpiresAndRemoves(t *testing.T) {
	tracker := NewTracker()
	for i := 0; i < maxInteractionMessages+5; i++ {
		if _, err := tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: "bounded", Message: "progress"}); err != nil {
			t.Fatal(err)
		}
	}
	state, err := tracker.Get("bounded")
	if err != nil || len(state.Messages) != maxInteractionMessages {
		t.Fatalf("bounded message history=%d, err=%v", len(state.Messages), err)
	}
	tracker.mu.Lock()
	expired := tracker.states["bounded"]
	expired.UpdatedAt = time.Now().Add(-interactionTTL - time.Second)
	tracker.states["bounded"] = expired
	tracker.mu.Unlock()
	if removed := tracker.Cleanup(); removed != 1 {
		t.Fatalf("cleanup removed %d states", removed)
	}
	if _, err = tracker.Get("bounded"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired state lookup = %v", err)
	}
	if _, err = tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: "remove-me"}); err != nil {
		t.Fatal(err)
	}
	tracker.Remove("remove-me")
	if _, err = tracker.Get("remove-me"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("removed state lookup = %v", err)
	}
}

func TestTrackerBoundsActiveStates(t *testing.T) {
	tracker := NewTracker()
	for i := 0; i < maxActiveInteractions; i++ {
		if _, err := tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: fmt.Sprintf("interaction-%d", i)}); err != nil {
			t.Fatalf("state %d: %v", i, err)
		}
	}
	if _, err := tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: "overflow"}); !errors.Is(err, ErrLimit) {
		t.Fatalf("limit error = %v", err)
	}
}

func TestTrackerBoundsAggregateMessageBytes(t *testing.T) {
	tracker := NewTracker()
	data := json.RawMessage(`"` + strings.Repeat("x", 60<<10) + `"`)
	accepted := 0
	for i := 0; i < maxActiveInteractions; i++ {
		_, err := tracker.Apply(adapterproto.InteractionMessage{Type: "display", InteractionID: fmt.Sprintf("large-%d", i), Data: data})
		if errors.Is(err, ErrLimit) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		accepted++
	}
	if accepted == 0 || tracker.bytes > maxTrackerBytes || accepted >= maxActiveInteractions {
		t.Fatalf("aggregate byte bound failed: accepted=%d bytes=%d", accepted, tracker.bytes)
	}
	if len(tracker.states) != accepted {
		t.Fatalf("rejected interaction mutated tracker: states=%d accepted=%d", len(tracker.states), accepted)
	}
}
