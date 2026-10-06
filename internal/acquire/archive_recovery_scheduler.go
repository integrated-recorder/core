package acquire

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	maxAutomaticRecoveryQueue              = 256
	automaticRecoveryRetryBase             = time.Second
	automaticRecoveryRetryCap              = 5 * time.Minute
	automaticRecoveryRecheck               = 5 * time.Minute
	maxAutomaticRecoveryNoProgressRechecks = 3
)

// automaticRecoveryScheduler owns one historical acquisition worker per
// Engine process. Its queue coalesces recording IDs and refills by key order
// when bounded capacity is reached.
type automaticRecoveryScheduler struct {
	manager *Manager
	claim   func(context.Context, string) (OwnershipToken, error)
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	wake    chan struct{}

	mu       sync.Mutex
	queue    []string
	queued   map[string]time.Time
	inFlight string
	dirty    bool
	rescan   bool
	cursor   string
}

type automaticRecoveryOutcome struct {
	retry      bool
	recheck    bool
	more       bool
	progressed bool
}

func newAutomaticRecoveryScheduler(manager *Manager, claim func(context.Context, string) (OwnershipToken, error)) *automaticRecoveryScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &automaticRecoveryScheduler{
		manager: manager, claim: claim, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		wake: make(chan struct{}, 1), queued: make(map[string]time.Time),
	}
}

func (s *automaticRecoveryScheduler) start() { go s.run() }

func (s *automaticRecoveryScheduler) enqueue(id string, due time.Time) {
	if s == nil || id == "" || !s.manager.automaticRecoveryEligible(id) {
		return
	}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	if id == s.inFlight {
		s.dirty = true
		s.mu.Unlock()
		s.signal()
		return
	}
	if prior, ok := s.queued[id]; ok {
		if !due.IsZero() && !prior.IsZero() && due.Before(prior) {
			s.queued[id] = due
		}
		s.mu.Unlock()
		s.signal()
		return
	}
	if len(s.queue) >= maxAutomaticRecoveryQueue {
		s.rescan = true
		s.mu.Unlock()
		s.signal()
		return
	}
	s.queue = append(s.queue, id)
	s.queued[id] = due
	s.mu.Unlock()
	s.signal()
}

func (s *automaticRecoveryScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *automaticRecoveryScheduler) run() {
	defer close(s.done)
	for {
		id, wait, ok := s.next()
		if !ok {
			return
		}
		if id == "" {
			var timer *time.Timer
			var timerC <-chan time.Time
			if wait > 0 {
				timer = time.NewTimer(wait)
				timerC = timer.C
			}
			select {
			case <-s.ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case <-s.wake:
			case <-timerC:
			}
			if timer != nil {
				timer.Stop()
			}
			continue
		}
		outcome := s.process(id)
		s.complete(id, outcome)
	}
}

func (s *automaticRecoveryScheduler) next() (string, time.Duration, bool) {
	for {
		if s.ctx.Err() != nil {
			return "", 0, false
		}
		s.refill()
		now := time.Now()
		s.mu.Lock()
		var earliest time.Time
		for remaining := len(s.queue); remaining > 0; remaining-- {
			id := s.queue[0]
			s.queue = s.queue[1:]
			due := s.queued[id]
			if due.IsZero() || !now.Before(due) {
				delete(s.queued, id)
				s.inFlight = id
				s.dirty = false
				s.mu.Unlock()
				return id, 0, true
			}
			s.queue = append(s.queue, id)
			if earliest.IsZero() || due.Before(earliest) {
				earliest = due
			}
		}
		s.mu.Unlock()
		if s.ctx.Err() != nil {
			return "", 0, false
		}
		if earliest.IsZero() {
			return "", 0, true
		}
		wait := time.Until(earliest)
		if wait < 0 {
			wait = 0
		}
		return "", wait, true
	}
}

func (s *automaticRecoveryScheduler) refill() {
	for {
		s.mu.Lock()
		if !s.rescan || len(s.queue) >= maxAutomaticRecoveryQueue || s.ctx.Err() != nil {
			s.mu.Unlock()
			return
		}
		room := maxAutomaticRecoveryQueue - len(s.queue)
		cursor := s.cursor
		s.mu.Unlock()

		ids, more := s.manager.recoveryIDsAfter(cursor, room)
		if len(ids) == 0 {
			s.mu.Lock()
			if cursor != "" {
				s.cursor = ""
				s.rescan = true
				s.mu.Unlock()
				continue
			}
			s.rescan = false
			s.cursor = ""
			s.mu.Unlock()
			return
		}
		for _, id := range ids {
			if s.ctx.Err() != nil {
				return
			}
			if s.manager.automaticRecoveryEligible(id) {
				s.addRefilled(id)
			}
		}
		s.mu.Lock()
		s.cursor = ids[len(ids)-1]
		s.rescan = more
		if !more {
			s.cursor = ""
		}
		s.mu.Unlock()
		if !more || len(ids) == 0 {
			return
		}
	}
}

func (s *automaticRecoveryScheduler) addRefilled(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || id == s.inFlight {
		return
	}
	if _, exists := s.queued[id]; exists {
		return
	}
	if len(s.queue) >= maxAutomaticRecoveryQueue {
		s.rescan = true
		return
	}
	s.queue = append(s.queue, id)
	s.queued[id] = time.Time{}
}

func (s *automaticRecoveryScheduler) process(id string) automaticRecoveryOutcome {
	if s.ctx.Err() != nil {
		return automaticRecoveryOutcome{}
	}
	e, ok := s.manager.entry(id)
	if !ok {
		return automaticRecoveryOutcome{}
	}
	if err := acquireArchiveRecoveryGate(s.ctx, e); err != nil {
		return automaticRecoveryOutcome{}
	}
	defer releaseArchiveRecoveryGate(e)
	if !s.manager.automaticRecoveryEligible(id) {
		return automaticRecoveryOutcome{}
	}
	e.mu.Lock()
	active := e.recording != nil && e.recording.State == domain.StateRecording
	var owner *OwnershipToken
	if e.ownership != nil {
		copy := *e.ownership
		owner = &copy
	}
	e.mu.Unlock()

	claimedHere := false
	if active {
		if owner == nil {
			return automaticRecoveryOutcome{}
		}
	} else if owner == nil {
		claimed, err := s.claim(s.ctx, id)
		if err != nil {
			return automaticRecoveryOutcome{retry: s.ctx.Err() == nil}
		}
		owner = &claimed
		claimedHere = true
	}
	progress, err := s.manager.repairDeclaredHistoryPass(s.ctx, *owner, id)
	if claimedHere {
		// A terminal pass may fail before the canonical fence adopts this Host
		// claim, for example when its persisted acquisition context is corrupt.
		// Release only this exact local claim. If adoption changed or another
		// owner took over, the owner callback's token check protects that owner.
		e.mu.Lock()
		adopted := sameOwner(e.ownership, *owner)
		e.mu.Unlock()
		if !adopted {
			_ = s.manager.releaseAutomaticRecoveryOwner(e, *owner)
		}
	}
	if err != nil {
		var retryable *retryableHistoricalError
		if errors.As(err, &retryable) {
			return automaticRecoveryOutcome{retry: s.ctx.Err() == nil}
		}
		if !active && (errors.Is(err, recordingowner.ErrStaleOwner) || errors.Is(err, recordingowner.ErrNotFound)) {
			e.mu.Lock()
			if sameOwner(e.ownership, *owner) {
				e.ownership = nil
			}
			e.mu.Unlock()
		}
		if s.ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, ErrArchiveSealed) || errors.Is(err, ErrHistoricalUnavailable) || errors.Is(err, storage.ErrNotFound) ||
			errors.Is(err, recordingowner.ErrStaleOwner) || errors.Is(err, recordingowner.ErrNotFound) ||
			errors.Is(err, ErrInvalidOwnershipToken) || errors.Is(err, ErrHandoverOwnerMismatch) {
			return automaticRecoveryOutcome{}
		}
		if errors.Is(err, ErrActiveRecording) {
			return automaticRecoveryOutcome{retry: true}
		}
		return automaticRecoveryOutcome{retry: true}
	}
	if progress.failed > 0 {
		return automaticRecoveryOutcome{retry: true, progressed: progress.acquired > 0}
	}
	return automaticRecoveryOutcome{more: progress.more, recheck: progress.needsRecheck, progressed: progress.acquired > 0}
}

func (s *automaticRecoveryScheduler) complete(id string, outcome automaticRecoveryOutcome) {
	e, ok := s.manager.entry(id)
	due := time.Time{}
	if ok {
		e.mu.Lock()
		switch {
		case outcome.retry:
			if e.recoveryAttempts < 31 {
				e.recoveryAttempts++
			}
			delay := automaticRecoveryRetryBase
			for i := 1; i < e.recoveryAttempts && delay < automaticRecoveryRetryCap; i++ {
				delay *= 2
			}
			if delay > automaticRecoveryRetryCap {
				delay = automaticRecoveryRetryCap
			}
			due = time.Now().Add(delay)
		case outcome.recheck:
			e.recoveryAttempts = 0
			if outcome.progressed {
				e.recoveryNoProgress = 0
			}
			if e.recoveryNoProgress < maxAutomaticRecoveryNoProgressRechecks {
				e.recoveryNoProgress++
				due = time.Now().Add(automaticRecoveryRecheck)
			}
		case outcome.more:
			e.recoveryAttempts = 0
			e.recoveryNoProgress = 0
		default:
			e.recoveryAttempts = 0
		}
		if outcome.progressed {
			e.recoveryNoProgress = 0
		}
		e.mu.Unlock()
	}

	s.mu.Lock()
	if s.inFlight == id {
		s.inFlight = ""
	}
	dirty := s.dirty
	s.dirty = false
	s.mu.Unlock()
	if dirty && !outcome.retry && !outcome.recheck {
		due = time.Time{}
	}
	if dirty && !outcome.retry && !outcome.recheck && !outcome.more {
		outcome.more = true
	}
	continueRecovery := outcome.retry || outcome.recheck && !due.IsZero() || outcome.more
	if continueRecovery {
		s.enqueue(id, due)
	} else if e, ok := s.manager.entry(id); ok {
		e.mu.Lock()
		terminal := e.recording != nil && e.recording.State != domain.StateRecording
		var owner *OwnershipToken
		if terminal && e.ownership != nil {
			copy := *e.ownership
			owner = &copy
		}
		e.mu.Unlock()
		if owner != nil {
			_ = s.manager.releaseAutomaticRecoveryOwner(e, *owner)
		}
	}
	s.signal()
}

func (s *automaticRecoveryScheduler) stop() { s.cancel() }

func (s *automaticRecoveryScheduler) wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) automaticRecoveryEligible(id string) bool {
	e, ok := m.entry(id)
	if !ok {
		return false
	}
	e.mu.Lock()
	if e.deleted || e.recording == nil || e.recording.ArchiveSealed || e.handover != nil {
		e.mu.Unlock()
		return false
	}
	active := e.recording.State == domain.StateRecording
	if active {
		eligible := e.cancel != nil && !channelClosed(e.done) && e.ownership != nil && validOwnershipToken(*e.ownership)
		media := cloneMediaSource(e.media)
		e.mu.Unlock()
		return eligible && validHistoricalAvailability(media)
	}
	quiescent := e.cancel == nil && channelClosed(e.done)
	root := clone(e.recording)
	media := cloneMediaSource(e.media)
	e.mu.Unlock()
	if !quiescent || root == nil || root.State != domain.StateStopped && root.State != domain.StateCompleted && root.State != domain.StateInterrupted {
		return false
	}
	if !validHistoricalAvailability(media) {
		loaded, err := m.loadAcquisitionContext(id)
		if err != nil || !validHistoricalAvailability(loaded) {
			return false
		}
	}
	return true
}

func (m *Manager) releaseAutomaticRecoveryOwner(e *entry, owner OwnershipToken) error {
	m.mu.RLock()
	release := m.terminalOwnerRelease
	m.mu.RUnlock()
	if release == nil {
		return ErrCanonicalFenceRequired
	}
	if err := release(owner); err != nil {
		return err
	}
	e.mu.Lock()
	if sameOwner(e.ownership, owner) {
		e.ownership = nil
	}
	e.mu.Unlock()
	return nil
}

func (m *Manager) signalAutomaticArchiveRecovery(id string) {
	m.mu.RLock()
	scheduler, closed := m.autoRecovery, m.closed
	m.mu.RUnlock()
	if scheduler != nil && !closed {
		scheduler.enqueue(id, time.Time{})
	}
}

func validHistoricalAvailability(media adapterproto.MediaSource) bool {
	return media.HistoricalAvailability != nil && media.HistoricalAvailability.Validate() == nil
}

// recoveryIDsAfter returns at most limit IDs in deterministic key order. It
// scans the bounded manager inventory without materializing all recording IDs.
func (m *Manager) recoveryIDsAfter(cursor string, limit int) ([]string, bool) {
	if limit <= 0 {
		return nil, false
	}
	m.mu.RLock()
	ids := make([]string, 0, limit+1)
	for id := range m.entries {
		if id <= cursor {
			continue
		}
		index := sort.SearchStrings(ids, id)
		if index >= limit && len(ids) == limit {
			continue
		}
		ids = append(ids, "")
		copy(ids[index+1:], ids[index:])
		ids[index] = id
		if len(ids) > limit {
			ids = ids[:limit]
		}
	}
	m.mu.RUnlock()
	// Determine whether keys remain after the last returned key. This second
	// bounded-memory scan avoids losing overflowed recordings.
	more := false
	if len(ids) == limit {
		last := ids[len(ids)-1]
		m.mu.RLock()
		for id := range m.entries {
			if id > last {
				more = true
				break
			}
		}
		m.mu.RUnlock()
	}
	return ids, more
}
