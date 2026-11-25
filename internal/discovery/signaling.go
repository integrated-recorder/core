package discovery

import (
	"bytes"
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

const (
	maxSignalMessages        = 2048
	maxSignalMessagesPerNode = 16
	maxSignalRelayBytes      = 64 << 20
)

var (
	ErrSignalInvalid  = errors.New("signaling relay message is invalid")
	ErrSignalConflict = errors.New("signaling message ID conflict")
	ErrSignalCapacity = errors.New("signaling relay capacity is exhausted")
	signalNodeIDRE    = regexp.MustCompile("^nd_[A-Za-z0-9_-]{22}$")
)

type relayedSignal struct {
	message   signalingprotocol.Message
	canonical []byte
	expires   time.Time
}

// PublishSignal stores only a bounded, signed envelope in process memory.
// SDP/ICE appears only as ciphertext. The relay deliberately does not decide
// whether the source Node Key is currently authorized; recipients verify that
// against independently refreshed identity history.
func (d *Directory) PublishSignal(message signalingprotocol.Message, now time.Time) (bool, error) {
	if d == nil || now.IsZero() || !signalNodeIDRE.MatchString(message.TargetNodeID) {
		return false, ErrSignalInvalid
	}
	canonical, err := signalingprotocol.CanonicalMessage(message)
	if err != nil || len(canonical) > signalingprotocol.MaxEnvelopeBytes {
		return false, ErrSignalInvalid
	}
	issued, err := time.Parse(time.RFC3339, message.IssuedAt)
	if err != nil || now.Before(issued.Add(-signalingprotocol.MaxClockSkew)) {
		return false, ErrSignalInvalid
	}
	expires, err := time.Parse(time.RFC3339, message.ExpiresAt)
	if err != nil || !expires.After(now) {
		return false, signalingprotocol.ErrStale
	}
	d.signalMu.Lock()
	defer d.signalMu.Unlock()
	d.expireSignalsLocked(now)
	if d.signals == nil {
		d.signals = map[string][]relayedSignal{}
	}
	queue := d.signals[message.TargetNodeID]
	for _, existing := range queue {
		if existing.message.MessageID != message.MessageID {
			continue
		}
		if bytes.Equal(existing.canonical, canonical) {
			return true, nil
		}
		return false, ErrSignalConflict
	}
	count := 0
	for _, pending := range d.signals {
		count += len(pending)
	}
	if len(queue) >= maxSignalMessagesPerNode || count >= maxSignalMessages || d.signalBytes+len(canonical) > maxSignalRelayBytes {
		return false, ErrSignalCapacity
	}
	queue = append(queue, relayedSignal{message: message, canonical: append([]byte(nil), canonical...), expires: expires})
	sort.Slice(queue, func(i, j int) bool {
		if queue[i].message.IssuedAt == queue[j].message.IssuedAt {
			return queue[i].message.MessageID < queue[j].message.MessageID
		}
		return queue[i].message.IssuedAt < queue[j].message.IssuedAt
	})
	d.signals[message.TargetNodeID] = queue
	d.signalBytes += len(canonical)
	return false, nil
}

// LookupSignals returns a bounded copy of unexpired envelopes for one recipient.
// Repeated reads are intentional: recipients durably consume each message ID;
// the relay forgets envelopes on expiry or process restart.
func (d *Directory) LookupSignals(targetNodeID string, now time.Time) ([]signalingprotocol.Message, error) {
	if d == nil || now.IsZero() || !signalNodeIDRE.MatchString(targetNodeID) {
		return nil, ErrSignalInvalid
	}
	d.signalMu.Lock()
	defer d.signalMu.Unlock()
	d.expireSignalsLocked(now)
	queue := d.signals[targetNodeID]
	messages := make([]signalingprotocol.Message, len(queue))
	for i := range queue {
		messages[i] = queue[i].message
	}
	return messages, nil
}

func (d *Directory) expireSignalsLocked(now time.Time) {
	for nodeID, queue := range d.signals {
		kept := queue[:0]
		for _, signal := range queue {
			if signal.expires.After(now) {
				kept = append(kept, signal)
			} else {
				d.signalBytes -= len(signal.canonical)
			}
		}
		if len(kept) == 0 {
			delete(d.signals, nodeID)
		} else {
			d.signals[nodeID] = kept
		}
	}
}
