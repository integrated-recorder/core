// Package interaction holds generic adapter interaction progress without
// assigning platform-specific meaning to message types or fields.
package interaction

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

var ErrNotFound = errors.New("interaction not found")
var ErrTerminal = errors.New("interaction is terminal")
var ErrLimit = errors.New("interaction limit reached")

const maxActiveInteractions = 256
const maxInteractionMessages = 100
const maxInteractionBytes = 512 << 10
const maxTrackerBytes = 8 << 20
const interactionTTL = 30 * time.Minute

type State struct {
	ID        string                            `json:"id"`
	Status    string                            `json:"status"`
	Messages  []adapterproto.InteractionMessage `json:"messages"`
	UpdatedAt time.Time                         `json:"updated_at"`
	byteSizes []int                             `json:"-"`
	byteSize  int                               `json:"-"`
}

type Tracker struct {
	mu     sync.RWMutex
	states map[string]State
	bytes  int
}

func NewTracker() *Tracker { return &Tracker{states: map[string]State{}} }
func (t *Tracker) Apply(message adapterproto.InteractionMessage) (State, error) {
	if err := message.Validate(); err != nil {
		return State{}, err
	}
	encoded, err := json.Marshal(message)
	if err != nil || len(encoded) > maxInteractionBytes {
		return State{}, ErrLimit
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	t.cleanupLocked(now)
	state, ok := t.states[message.InteractionID]
	if !ok {
		if len(t.states) >= maxActiveInteractions {
			return State{}, ErrLimit
		}
		state = State{ID: message.InteractionID, Status: "active", Messages: []adapterproto.InteractionMessage{}}
	} else if state.Status != "active" {
		return State{}, ErrTerminal
	}
	oldSize := state.byteSize
	switch message.Type {
	case "complete":
		state.Status = "complete"
	case "error":
		state.Status = "error"
	default:
		state.Status = "active"
	}
	state.Messages = append(append([]adapterproto.InteractionMessage(nil), state.Messages...), message)
	state.byteSizes = append(append([]int(nil), state.byteSizes...), len(encoded))
	state.byteSize += len(encoded)
	for len(state.Messages) > maxInteractionMessages || state.byteSize > maxInteractionBytes {
		state.byteSize -= state.byteSizes[0]
		state.byteSizes = state.byteSizes[1:]
		state.Messages = state.Messages[1:]
	}
	// byteSize after trimming is authoritative; replacing an existing state
	// releases its old history before applying the global bound.
	newTotal := t.bytes - oldSize + state.byteSize
	if newTotal > maxTrackerBytes {
		return State{}, ErrLimit
	}
	state.UpdatedAt = now
	t.states[state.ID] = state
	t.bytes = newTotal
	return clone(state), nil
}
func (t *Tracker) Get(id string) (State, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleanupLocked(time.Now().UTC())
	state, ok := t.states[id]
	if !ok {
		return State{}, ErrNotFound
	}
	state.UpdatedAt = time.Now().UTC()
	t.states[id] = state
	return clone(state), nil
}

func (t *Tracker) Update(message adapterproto.InteractionMessage) (State, error) {
	return t.Apply(message)
}

func (t *Tracker) Remove(id string) {
	t.mu.Lock()
	if state, ok := t.states[id]; ok {
		t.bytes -= state.byteSize
		delete(t.states, id)
	}
	t.mu.Unlock()
}

func (t *Tracker) Cleanup() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cleanupLocked(time.Now().UTC())
}

func (t *Tracker) cleanupLocked(now time.Time) int {
	removed := 0
	for id, state := range t.states {
		if now.Sub(state.UpdatedAt) > interactionTTL {
			t.bytes -= state.byteSize
			delete(t.states, id)
			removed++
		}
	}
	return removed
}

func clone(state State) State {
	data, _ := json.Marshal(state)
	var copy State
	_ = json.Unmarshal(data, &copy)
	return copy
}
