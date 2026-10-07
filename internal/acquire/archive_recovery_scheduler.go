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

type automaticRecoveryQueueItem struct {
	due                   time.Time
	release               *OwnershipToken
	reconcileAfterRelease bool
}

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
	now     func() time.Time

	mu               sync.Mutex
	queue            []string
	queued           map[string]automaticRecoveryQueueItem
	inFlight         string
	dirty            bool
	rescan           bool
	cursor           string
	rescanGeneration uint64
	lastCompletedID  string
}

type automaticRecoveryOutcome struct {
	retry      bool
	recheck    bool
	more       bool
	progressed bool
	releaseErr error
	release    *OwnershipToken
}

func newAutomaticRecoveryScheduler(manager *Manager, claim func(context.Context, string) (OwnershipToken, error)) *automaticRecoveryScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &automaticRecoveryScheduler{
		manager: manager, claim: claim, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		wake: make(chan struct{}, 1), queued: make(map[string]automaticRecoveryQueueItem), now: time.Now,
	}
}

func (s *automaticRecoveryScheduler) start() { go s.run() }

// enqueue records a meaningful event. Immediate events promote an existing
// delayed slot without changing its FIFO position.
func (s *automaticRecoveryScheduler) enqueue(id string, due time.Time) {
	if s == nil || id == "" {
		return
	}
	eligible := s.manager.automaticRecoveryEligible(id)
	e, exists := s.manager.entry(id)
	if !exists {
		return
	}
	var release *OwnershipToken
	var releaseDue time.Time
	if e != nil {
		e.mu.Lock()
		if e.pendingRecoveryRelease != nil {
			copy := *e.pendingRecoveryRelease
			release = &copy
			releaseDue = e.recoveryReleaseDue
		}
		e.mu.Unlock()
	}
	if !eligible && release == nil {
		return
	}

	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	if id == s.inFlight {
		s.dirty = true
		if release != nil {
			e.mu.Lock()
			e.recoveryAfterRelease = true
			e.recoveryReleaseDue = earlierRecoveryDue(e.recoveryReleaseDue, due)
			e.mu.Unlock()
		}
		s.mu.Unlock()
		s.signal()
		return
	}
	if prior, ok := s.queued[id]; ok {
		prior.due = earlierRecoveryDue(prior.due, due)
		if release != nil && prior.release == nil {
			prior.release = release
			prior.due = earlierRecoveryDue(prior.due, releaseDue)
			prior.reconcileAfterRelease = true
		}
		if prior.release != nil {
			prior.reconcileAfterRelease = true
			e.mu.Lock()
			e.recoveryAfterRelease = true
			e.recoveryReleaseDue = earlierRecoveryDue(e.recoveryReleaseDue, due)
			e.mu.Unlock()
		}
		s.queued[id] = prior
		s.mu.Unlock()
		s.signal()
		return
	}
	if len(s.queue) >= maxAutomaticRecoveryQueue {
		s.rescan = true
		s.cursor = ""
		s.rescanGeneration++
		s.lastCompletedID = ""
		if release != nil {
			e.mu.Lock()
			e.recoveryAfterRelease = true
			e.recoveryReleaseDue = earlierRecoveryDue(e.recoveryReleaseDue, due)
			e.mu.Unlock()
		}
		s.mu.Unlock()
		s.signal()
		return
	}
	item := automaticRecoveryQueueItem{due: due}
	if release != nil {
		item.release = release
		item.due = earlierRecoveryDue(releaseDue, due)
		item.reconcileAfterRelease = true
		e.mu.Lock()
		e.recoveryAfterRelease = true
		e.recoveryReleaseDue = item.due
		e.mu.Unlock()
	}
	s.queue = append(s.queue, id)
	s.queued[id] = item
	s.mu.Unlock()
	s.signal()
}

func earlierRecoveryDue(a, b time.Time) time.Time {
	if a.IsZero() || b.IsZero() {
		return time.Time{}
	}
	if b.Before(a) {
		return b
	}
	return a
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
		id, item, wait, ok := s.next()
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
		outcome := s.process(id, item)
		s.complete(id, item, outcome)
	}
}

func (s *automaticRecoveryScheduler) next() (string, automaticRecoveryQueueItem, time.Duration, bool) {
	for {
		if s.ctx.Err() != nil {
			return "", automaticRecoveryQueueItem{}, 0, false
		}
		s.refill()
		now := s.now()
		s.mu.Lock()
		var earliest time.Time
		for remaining := len(s.queue); remaining > 0; remaining-- {
			id := s.queue[0]
			s.queue = s.queue[1:]
			item := s.queued[id]
			if item.due.IsZero() || !now.Before(item.due) {
				delete(s.queued, id)
				s.inFlight = id
				s.dirty = false
				s.mu.Unlock()
				return id, item, 0, true
			}
			s.queue = append(s.queue, id)
			if earliest.IsZero() || item.due.Before(earliest) {
				earliest = item.due
			}
		}
		s.mu.Unlock()
		if s.ctx.Err() != nil {
			return "", automaticRecoveryQueueItem{}, 0, false
		}
		if earliest.IsZero() {
			return "", automaticRecoveryQueueItem{}, 0, true
		}
		wait := earliest.Sub(now)
		if wait < 0 {
			wait = 0
		}
		return "", automaticRecoveryQueueItem{}, wait, true
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
		generation := s.rescanGeneration
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
			s.addRefilled(id)
		}
		s.mu.Lock()
		if generation != s.rescanGeneration {
			s.cursor = ""
			s.rescan = true
			s.mu.Unlock()
			return
		}
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
	if s.ctx.Err() != nil {
		return
	}
	e, exists := s.manager.entry(id)
	if !exists {
		return
	}
	item := automaticRecoveryQueueItem{}
	e.mu.Lock()
	if e.pendingRecoveryRelease != nil {
		copy := *e.pendingRecoveryRelease
		item.release = &copy
		item.due = e.recoveryReleaseDue
		item.reconcileAfterRelease = e.recoveryAfterRelease
	}
	e.mu.Unlock()
	if item.release == nil && !s.manager.automaticRecoveryEligible(id) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || id == s.inFlight || id == s.lastCompletedID {
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
	s.queued[id] = item
}

func (s *automaticRecoveryScheduler) process(id string, item automaticRecoveryQueueItem) automaticRecoveryOutcome {
	if item.release != nil {
		e, ok := s.manager.entry(id)
		if !ok {
			return automaticRecoveryOutcome{}
		}
		return automaticRecoveryOutcome{release: item.release, releaseErr: s.manager.releaseAutomaticRecoveryOwner(s.ctx, e, *item.release)}
	}
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
		e.mu.Lock()
		adopted := sameOwner(e.ownership, *owner)
		e.mu.Unlock()
		if !adopted {
			releaseErr := s.manager.releaseAutomaticRecoveryOwner(s.ctx, e, *owner)
			if releaseErr != nil {
				s.manager.retainAutomaticRecoveryRelease(e, *owner, time.Time{}, false)
				return automaticRecoveryOutcome{release: owner, releaseErr: releaseErr}
			}
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

func (s *automaticRecoveryScheduler) complete(id string, item automaticRecoveryQueueItem, outcome automaticRecoveryOutcome) {
	s.mu.Lock()
	if s.inFlight == id {
		s.inFlight = ""
	}
	s.lastCompletedID = id
	dirty := s.dirty
	s.dirty = false
	s.mu.Unlock()

	if item.release != nil || outcome.release != nil {
		token := item.release
		if token == nil {
			token = outcome.release
		}
		if token == nil {
			return
		}
		if outcome.releaseErr != nil {
			followup := item.reconcileAfterRelease || dirty
			s.retainAutomaticRecoveryReleaseForRetry(id, *token, followup)
			return
		}
		e, ok := s.manager.entry(id)
		if !ok {
			return
		}
		e.mu.Lock()
		followup := item.reconcileAfterRelease || dirty || e.recoveryAfterRelease
		e.recoveryAfterRelease = false
		e.mu.Unlock()
		if followup && s.manager.automaticRecoveryEligible(id) {
			s.queueRecovery(id, time.Time{})
		}
		return
	}

	e, ok := s.manager.entry(id)
	due := time.Time{}
	if ok {
		e.mu.Lock()
		if dirty {
			// The triggering event invalidated this pass's scheduling result.
			// Preserve counters, then reconcile once immediately.
			due = time.Time{}
		} else {
			switch {
			case outcome.retry:
				if e.recoveryAttempts < 31 {
					e.recoveryAttempts++
				}
				due = s.now().Add(recoveryBackoff(e.recoveryAttempts))
			case outcome.recheck:
				e.recoveryAttempts = 0
				if outcome.progressed {
					e.recoveryNoProgress = 0
				}
				if e.recoveryNoProgress < maxAutomaticRecoveryNoProgressRechecks {
					e.recoveryNoProgress++
					due = s.now().Add(automaticRecoveryRecheck)
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
		}
		e.mu.Unlock()
	}
	continueRecovery := dirty || outcome.retry || outcome.recheck && !due.IsZero() || outcome.more
	if continueRecovery {
		s.queueRecovery(id, due)
		return
	}
	if !ok {
		return
	}
	e.mu.Lock()
	terminal := e.recording != nil && e.recording.State != domain.StateRecording
	var owner *OwnershipToken
	if terminal && e.ownership != nil {
		copy := *e.ownership
		owner = &copy
	}
	e.mu.Unlock()
	if owner != nil {
		err := s.manager.releaseAutomaticRecoveryOwner(s.ctx, e, *owner)
		if err != nil {
			s.retainAutomaticRecoveryReleaseForRetry(id, *owner, false)
		}
	}
}

func recoveryBackoff(attempt int) time.Duration {
	delay := automaticRecoveryRetryBase
	for i := 1; i < attempt && delay < automaticRecoveryRetryCap; i++ {
		delay *= 2
	}
	if delay > automaticRecoveryRetryCap {
		return automaticRecoveryRetryCap
	}
	return delay
}

func (s *automaticRecoveryScheduler) queueRecovery(id string, due time.Time) {
	if !s.manager.automaticRecoveryEligible(id) {
		return
	}
	if owner, pendingDue, _ := s.manager.pendingAutomaticRecoveryRelease(id); owner != nil {
		s.queueRelease(id, *owner, earlierRecoveryDue(pendingDue, due), true)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return
	}
	if id == s.inFlight {
		s.dirty = true
		return
	}
	if prior, ok := s.queued[id]; ok {
		prior.due = earlierRecoveryDue(prior.due, due)
		s.queued[id] = prior
		s.signal()
		return
	}
	if len(s.queue) >= maxAutomaticRecoveryQueue {
		s.rescan = true
		s.cursor = ""
		s.rescanGeneration++
		s.lastCompletedID = ""
		s.signal()
		return
	}
	s.queue = append(s.queue, id)
	s.queued[id] = automaticRecoveryQueueItem{due: due}
	s.signal()
}

func (s *automaticRecoveryScheduler) queueRelease(id string, owner OwnershipToken, due time.Time, followup bool) {
	e, ok := s.manager.entry(id)
	if !ok {
		return
	}
	s.manager.retainAutomaticRecoveryRelease(e, owner, due, followup)
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.rescan = true
		s.mu.Unlock()
		return
	}
	if id == s.inFlight {
		s.dirty = s.dirty || followup
		s.mu.Unlock()
		return
	}
	if prior, exists := s.queued[id]; exists {
		followup = followup || prior.release == nil || prior.reconcileAfterRelease
		prior.release = &owner
		prior.due = earlierRecoveryDue(prior.due, due)
		prior.reconcileAfterRelease = prior.reconcileAfterRelease || followup
		s.queued[id] = prior
		s.mu.Unlock()
		s.signal()
		return
	}
	if len(s.queue) >= maxAutomaticRecoveryQueue {
		s.rescan = true
		s.cursor = ""
		s.rescanGeneration++
		s.lastCompletedID = ""
		s.mu.Unlock()
		s.signal()
		return
	}
	s.queue = append(s.queue, id)
	s.queued[id] = automaticRecoveryQueueItem{due: due, release: &owner, reconcileAfterRelease: followup}
	s.mu.Unlock()
	s.signal()
}

func (s *automaticRecoveryScheduler) retainAutomaticRecoveryReleaseForRetry(id string, owner OwnershipToken, followup bool) {
	e, ok := s.manager.entry(id)
	if !ok {
		return
	}
	e.mu.Lock()
	if e.recoveryReleaseAttempts < 31 {
		e.recoveryReleaseAttempts++
	}
	attempt := e.recoveryReleaseAttempts
	e.mu.Unlock()
	due := s.now().Add(recoveryBackoff(attempt))
	s.queueRelease(id, owner, due, followup)
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

func (m *Manager) retainAutomaticRecoveryRelease(e *entry, owner OwnershipToken, due time.Time, followup bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pendingRecoveryRelease == nil || sameOwner(e.pendingRecoveryRelease, owner) {
		copy := owner
		e.pendingRecoveryRelease = &copy
		e.recoveryReleaseDue = due
		e.recoveryAfterRelease = e.recoveryAfterRelease || followup
	}
}

func (m *Manager) pendingAutomaticRecoveryRelease(id string) (*OwnershipToken, time.Time, bool) {
	e, ok := m.entry(id)
	if !ok {
		return nil, time.Time{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pendingRecoveryRelease == nil {
		return nil, time.Time{}, false
	}
	copy := *e.pendingRecoveryRelease
	return &copy, e.recoveryReleaseDue, e.recoveryAfterRelease
}

func (m *Manager) releaseAutomaticRecoveryOwner(ctx context.Context, e *entry, owner OwnershipToken) error {
	m.mu.RLock()
	release := m.terminalOwnerRelease
	m.mu.RUnlock()
	if release == nil {
		return ErrCanonicalFenceRequired
	}
	err := release(ctx, owner)
	if errors.Is(err, recordingowner.ErrStaleOwner) || errors.Is(err, recordingowner.ErrNotFound) {
		err = nil
	}
	if err != nil {
		return err
	}
	e.mu.Lock()
	if sameOwner(e.ownership, owner) {
		e.ownership = nil
	}
	if sameOwner(e.pendingRecoveryRelease, owner) {
		e.pendingRecoveryRelease = nil
		e.recoveryReleaseDue = time.Time{}
		e.recoveryReleaseAttempts = 0
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
