package watch

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	workerCount        = 4
	queueCapacity      = 128
	checkTimeout       = 25 * time.Second
	schedulerInterval  = 500 * time.Millisecond
	linkReconcileEvery = 2 * time.Second
)

type checkJob struct {
	id     string
	done   chan struct{}
	err    error
	cancel context.CancelFunc
}

type recordingStartGate struct {
	mu   sync.Mutex
	refs int
}

// Service schedules generic adapter checks and creates recordings only via
// the existing recording manager. It has no platform-specific behavior.
type Service struct {
	store    *Store
	adapters AdapterRuntime
	manager  RecordingManager
	options  Options
	queue    chan *checkJob

	mu          sync.Mutex
	jobs        map[string]*checkJob
	jobsChanged chan struct{}
	closed      bool
	running     bool
	paused      bool
	runDone     chan struct{}
	runStarted  chan struct{}
	runCancel   context.CancelFunc
	closeOnce   sync.Once
	workers     sync.WaitGroup
	startGates  map[string]*recordingStartGate
	starting    map[string]string
}

func New(root string, adapters AdapterRuntime, manager RecordingManager, options Options) (*Service, error) {
	if adapters == nil || manager == nil {
		return nil, errors.New("watch adapter runtime and recording manager are required")
	}
	store, err := Open(root)
	if err != nil {
		return nil, err
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	if options.Jitter == nil {
		options.Jitter = func(bound time.Duration) time.Duration {
			if bound <= 0 {
				return 0
			}
			return time.Duration(mrand.Int63n(int64(bound)*2+1)) - bound
		}
	}
	if options.CheckTimeout <= 0 {
		options.CheckTimeout = checkTimeout
	}
	return &Service{store: store, adapters: adapters, manager: manager, options: options, queue: make(chan *checkJob, queueCapacity), jobs: map[string]*checkJob{}, jobsChanged: make(chan struct{}), startGates: map[string]*recordingStartGate{}, starting: map[string]string{}, runDone: make(chan struct{}), runStarted: make(chan struct{})}, nil
}

func (s *Service) Create(request Update) (View, error) {
	request.AdapterID = strings.TrimSpace(request.AdapterID)
	if request.AdapterID == "" || len(request.Input) == 0 || len(request.Input) > MaxInputBytes {
		return View{}, ErrInvalid
	}
	title := ""
	if request.Title != nil {
		title = strings.TrimSpace(*request.Title)
	}
	if err := validateTitle(title); err != nil {
		return View{}, err
	}
	interval := DefaultIntervalSeconds
	if request.CheckIntervalSeconds != nil {
		interval = *request.CheckIntervalSeconds
	}
	if interval < MinIntervalSeconds || interval > MaxIntervalSeconds {
		return View{}, ErrInvalid
	}
	mode := request.PreviewMode
	if mode == "" {
		mode = "disabled"
	}
	if mode != "disabled" && mode != "segment" {
		return View{}, ErrInvalid
	}
	if err := s.validateAdapterForWatch(request.AdapterID, request.Input, request.InputSecrets, request.Resource); err != nil {
		return View{}, err
	}
	input, err := normalizeObject(request.Input)
	if err != nil {
		return View{}, ErrInvalid
	}
	id, err := newID()
	if err != nil {
		return View{}, err
	}
	now := s.options.Now().UTC()
	def := Definition{ID: id, AdapterID: request.AdapterID, Resource: cloneRef(request.Resource), Input: input, Enabled: true, Title: title, PreviewMode: mode, CheckIntervalSeconds: interval, CreatedAt: now, UpdatedAt: now}
	runtime := Runtime{State: StateChecking, NextCheckAt: timePointer(now.Add(startupStagger(id, interval)))}
	if err := s.store.Create(def, request.InputSecrets, runtime); err != nil {
		return View{}, err
	}
	_ = s.store.AddEvent(id, Event{Type: "watch_created", At: now})
	return s.view(def, runtime, s.store.SecretConfigured(id), true), nil
}

func (s *Service) validateAdapterForWatch(adapterID string, input json.RawMessage, secrets map[string]string, resource *adapterproto.ResourceRef) error {
	adapter, err := s.adapters.Get(adapterID)
	if err != nil || adapter.Descriptor == nil {
		return ErrUnsupported
	}
	if adapter.Status.State != "ready" {
		return adapterhost.ErrAdapterUnavailable
	}
	if !containsCapability(*adapter.Descriptor, adapterproto.CapabilityWatch) {
		return ErrUnsupported
	}
	if err := s.adapters.ValidateResource(adapterID, resource); err != nil {
		return fmt.Errorf("invalid resource reference")
	}
	if err := s.adapters.ValidateWatchInput(adapterID, input, secrets, resource); err != nil {
		return ErrInvalid
	}
	if !validSecrets(secrets) {
		return ErrInvalid
	}
	return nil
}

func (s *Service) Get(id string) (View, error) {
	def, runtime, _, err := s.store.Get(id)
	if err != nil {
		return View{}, err
	}
	return s.view(def, runtime, s.store.SecretConfigured(id), true), nil
}

func (s *Service) List() []View {
	snapshots := s.store.Snapshots()
	out := make([]View, 0, len(snapshots))
	for _, snapshot := range snapshots {
		view := s.view(snapshot.Definition, snapshot.Runtime, s.store.SecretConfigured(snapshot.Definition.ID), false)
		out = append(out, view)
	}
	return out
}

func (s *Service) view(def Definition, runtime Runtime, configured map[string]bool, includeInput bool) View {
	if !includeInput {
		def.Input = nil
	}
	view := View{Definition: def, State: runtime.State, LastCheckAt: runtime.LastCheckAt, NextCheckAt: runtime.NextCheckAt, FailureCount: runtime.FailureCount, LastErrorCode: runtime.LastErrorCode, CurrentRecordingID: runtime.ActiveRecordingID, InputSecrets: configured}
	if view.State == "" {
		if !def.Enabled {
			view.State = StateDisabled
		} else {
			view.State = StateChecking
		}
	}
	if !def.Enabled && runtime.ActiveRecordingID == "" {
		view.State = StateDisabled
	}
	if adapter, err := s.adapters.Get(def.AdapterID); err == nil && adapter.Descriptor != nil {
		view.AdapterName = adapter.Descriptor.Name
	}
	if runtime.ActiveRecordingID != "" {
		if recording, err := s.manager.Get(runtime.ActiveRecordingID); err == nil {
			view.CurrentRecordingState = string(recording.State)
		}
		if s.options.CurrentPreview != nil {
			view.CurrentRecordingPreview = s.options.CurrentPreview(runtime.ActiveRecordingID)
		}
	}
	return view
}

func (s *Service) Update(id string, request Update) (View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobs[id] != nil {
		return View{}, ErrConflict
	}
	def, runtime, oldSecrets, err := s.store.Get(id)
	if err != nil {
		return View{}, err
	}
	if runtime.ActiveRecordingID != "" || runtime.State == StateStarting {
		return View{}, ErrConflict
	}
	if request.AdapterID == "" {
		request.AdapterID = def.AdapterID
	}
	if len(request.Input) == 0 {
		request.Input = def.Input
	}
	resource := request.Resource
	if resource == nil && !request.ClearResource {
		resource = def.Resource
	}
	title := def.Title
	if request.Title != nil {
		title = strings.TrimSpace(*request.Title)
	}
	if request.PreviewMode == "" {
		request.PreviewMode = def.PreviewMode
	}
	if request.PreviewMode == "" {
		request.PreviewMode = "disabled"
	}
	if request.CheckIntervalSeconds == nil {
		value := def.CheckIntervalSeconds
		request.CheckIntervalSeconds = &value
	}
	if request.Enabled == nil {
		value := def.Enabled
		request.Enabled = &value
	}
	if err := validateTitle(title); err != nil {
		return View{}, err
	}
	interval := *request.CheckIntervalSeconds
	if interval < MinIntervalSeconds || interval > MaxIntervalSeconds || (request.PreviewMode != "disabled" && request.PreviewMode != "segment") {
		return View{}, ErrInvalid
	}
	newSecrets := oldSecrets
	replaceSecrets := request.InputSecrets != nil || len(request.ClearInputSecrets) > 0
	if replaceSecrets {
		newSecrets = cloneSecrets(oldSecrets)
		for _, key := range request.ClearInputSecrets {
			delete(newSecrets, key)
		}
		for key, value := range request.InputSecrets {
			if value == "" {
				delete(newSecrets, key)
			} else {
				newSecrets[key] = value
			}
		}
	}
	if err := s.validateAdapterForWatch(request.AdapterID, request.Input, newSecrets, resource); err != nil {
		return View{}, err
	}
	input, err := normalizeObject(request.Input)
	if err != nil {
		return View{}, ErrInvalid
	}
	changedSource := request.AdapterID != def.AdapterID || !sameRef(resource, def.Resource) || !bytesEqual(input, def.Input)
	now := s.options.Now().UTC()
	def.AdapterID = request.AdapterID
	def.Resource = cloneRef(resource)
	def.Input = input
	def.Title = title
	def.PreviewMode = request.PreviewMode
	def.CheckIntervalSeconds = interval
	def.Enabled = *request.Enabled
	def.UpdatedAt = now
	if changedSource {
		runtime.SessionDigest = ""
		runtime.SuppressedSessionDigest = ""
		runtime.Suppressed = false
		runtime.LastObservedLive = false
		runtime.TerminalOutcome = ""
	}
	if !def.Enabled {
		runtime.State = StateDisabled
		runtime.NextCheckAt = nil
	} else if changedSource || runtime.State == StateDisabled {
		runtime.State = StateChecking
		runtime.FailureCount = 0
		runtime.NextCheckAt = timePointer(now.Add(startupStagger(id, interval)))
	}
	if err := s.store.Update(def, newSecrets, replaceSecrets); err != nil {
		return View{}, err
	}
	if err := s.store.SaveRuntime(id, runtime); err != nil {
		return View{}, err
	}
	_ = s.store.AddEvent(id, Event{Type: "watch_updated", At: now})
	return s.view(def, runtime, s.store.SecretConfigured(id), true), nil
}

func (s *Service) SetEnabled(id string, enabled bool) (View, error) {
	unlockGate := s.lockStartGate(id)
	defer unlockGate()
	s.mu.Lock()
	defer s.mu.Unlock()
	def, runtime, _, err := s.store.Get(id)
	if err != nil {
		return View{}, err
	}
	if def.Enabled == enabled {
		return s.view(def, runtime, s.store.SecretConfigured(id), true), nil
	}
	now := s.options.Now().UTC()
	def.Enabled = enabled
	def.UpdatedAt = now
	if runtime.ActiveRecordingID != "" {
		runtime.State = StateRecording
		runtime.NextCheckAt = nil
	} else if enabled {
		runtime.State = StateChecking
		runtime.FailureCount = 0
		runtime.NextCheckAt = timePointer(now.Add(startupStagger(id, def.CheckIntervalSeconds)))
	} else {
		runtime.State = StateDisabled
		runtime.NextCheckAt = nil
	}
	if !enabled {
		if job := s.jobs[id]; job != nil && job.cancel != nil {
			job.cancel()
		}
	}
	if err = s.store.Update(def, nil, false); err != nil {
		return View{}, err
	}
	if err = s.store.SaveRuntime(id, runtime); err != nil {
		return View{}, err
	}
	eventType := "watch_enabled"
	if !enabled {
		eventType = "watch_disabled"
	}
	_ = s.store.AddEvent(id, Event{Type: eventType, At: now})
	return s.view(def, runtime, s.store.SecretConfigured(id), true), nil
}

func (s *Service) Delete(id string) error {
	unlockGate := s.lockStartGate(id)
	defer unlockGate()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, _, err := s.store.Get(id); err != nil {
		return err
	}
	if job := s.jobs[id]; job != nil && job.cancel != nil {
		job.cancel()
	}
	return s.store.Delete(id)
}

func (s *Service) Events(id string, limit int) ([]Event, error) { return s.store.Events(id, limit) }
func (s *Service) Relations(id string, limit int) ([]RecordingRelation, error) {
	return s.store.RelationsForWatch(id, limit)
}
func (s *Service) RelationsPage(id string, limit int) ([]RecordingRelation, bool, error) {
	return s.store.RelationsForWatchPage(id, limit)
}

func (s *Service) ManualCheck(ctx context.Context, id string) error {
	def, _, _, err := s.store.Get(id)
	if err != nil {
		return err
	}
	if !def.Enabled {
		return ErrConflict
	}
	job, err := s.enqueue(id)
	if err != nil {
		return err
	}
	select {
	case <-job.done:
		return job.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Pause stops new scheduled/manual checks and cancels the checks already in
// flight, then waits until every check has left the worker pool. It is used by
// Control generation handoff; unlike Close it is reversible if activation is
// rolled back.
func (s *Service) Pause(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServiceClosed
	}
	s.paused = true
	for _, job := range s.jobs {
		if job.cancel != nil {
			job.cancel()
		}
	}
	for len(s.jobs) > 0 {
		changed := s.jobsChanged
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrServiceClosed
		}
	}
	s.mu.Unlock()
	return nil
}

// Resume reopens check admission after an aborted Control handoff.
func (s *Service) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrServiceClosed
	}
	s.paused = false
	return nil
}

func (s *Service) enqueue(id string) (*checkJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrServiceClosed
	}
	if !s.running {
		return nil, errors.New("watch scheduler is not running")
	}
	if s.paused {
		return nil, ErrHandoffPaused
	}
	if current := s.jobs[id]; current != nil {
		return current, nil
	}
	job := &checkJob{id: id, done: make(chan struct{})}
	s.jobs[id] = job
	select {
	case s.queue <- job:
		return job, nil
	default:
		delete(s.jobs, id)
		return nil, ErrQueueFull
	}
}

// ScheduleDueAt is the deterministic scheduler seam used by tests. Queue-full
// watches remain due and are retried by the next scheduler tick.
func (s *Service) ScheduleDueAt(now time.Time) int {
	queued := 0
	for _, id := range s.store.DueIDs(now) {
		if _, err := s.enqueue(id); err == nil {
			queued++
		}
	}
	return queued
}

func (s *Service) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServiceClosed
	}
	if s.running {
		s.mu.Unlock()
		return errors.New("watch scheduler already running")
	}
	s.running = true
	runCtx, cancel := context.WithCancel(ctx)
	s.runCancel = cancel
	s.mu.Unlock()
	s.recoverLinks()
	for i := 0; i < workerCount; i++ {
		s.workers.Add(1)
		go func() { defer s.workers.Done(); s.worker(runCtx) }()
	}
	close(s.runStarted)
	lastLinks := s.options.Now()
	ticker := time.NewTicker(schedulerInterval)
	defer ticker.Stop()
	defer func() {
		cancel()
		s.workers.Wait()
		s.failQueued(context.Canceled)
		s.mu.Lock()
		s.running = false
		s.closed = true
		s.mu.Unlock()
		close(s.runDone)
	}()
	for {
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case now := <-ticker.C:
			if now.Sub(lastLinks) >= linkReconcileEvery {
				s.reconcileLinks(now)
				lastLinks = now
			}
			s.ScheduleDueAt(now)
		}
	}
}

func (s *Service) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		if s.runCancel != nil {
			s.runCancel()
		}
		s.mu.Unlock()
	})
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	if !running {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.runDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-s.queue:
			if job == nil {
				continue
			}
			if err := ctx.Err(); err != nil {
				job.err = err
				s.finishJob(job)
				continue
			}
			jobCtx, cancel := context.WithCancel(ctx)
			s.mu.Lock()
			if s.jobs[job.id] == job {
				job.cancel = cancel
			}
			paused := s.paused
			s.mu.Unlock()
			if paused {
				job.err = ErrHandoffPaused
			} else if err := jobCtx.Err(); err != nil {
				job.err = err
			} else {
				job.err = s.runCheck(jobCtx, job.id)
			}
			cancel()
			s.finishJob(job)
		}
	}
}

func (s *Service) finishJob(job *checkJob) {
	s.mu.Lock()
	if s.jobs[job.id] == job {
		delete(s.jobs, job.id)
		close(job.done)
		close(s.jobsChanged)
		s.jobsChanged = make(chan struct{})
	}
	s.mu.Unlock()
}

func (s *Service) failQueued(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, job := range s.jobs {
		job.err = err
		close(job.done)
		delete(s.jobs, id)
	}
	close(s.jobsChanged)
	s.jobsChanged = make(chan struct{})
}

func (s *Service) runCheck(parent context.Context, id string) error {
	s.mu.Lock()
	def, runtime, secrets, err := s.store.Get(id)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !def.Enabled {
		s.mu.Unlock()
		return nil
	}
	if runtime.ActiveRecordingID != "" {
		s.mu.Unlock()
		s.reconcileOne(def, runtime, s.options.Now())
		return nil
	}
	now := s.options.Now().UTC()
	runtime.State = StateChecking
	runtime.LastCheckAt = timePointer(now)
	if err = s.store.SaveRuntime(id, runtime); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, s.options.CheckTimeout)
	defer cancel()
	adapter, err := s.adapters.Get(def.AdapterID)
	if err != nil || adapter.Descriptor == nil {
		return s.failed(def, runtime, "adapter_unavailable", false)
	}
	if adapter.Status.State == "disabled" || adapter.Status.State == "rejected" {
		return s.failed(def, runtime, "adapter_disabled", true)
	}
	if adapter.Status.State != "ready" {
		return s.failed(def, runtime, "adapter_unavailable", false)
	}
	if !containsCapability(*adapter.Descriptor, adapterproto.CapabilityWatch) {
		return s.failed(def, runtime, "adapter_unavailable", true)
	}
	result, err := s.adapters.WatchCheck(ctx, def.AdapterID, def.Input, secrets, def.Resource)
	if err != nil {
		if parent.Err() != nil {
			return parent.Err()
		}
		var protocolErr *adapterhost.SafeProtocolError
		if errors.As(err, &protocolErr) && safeAttentionCode(protocolErr.Code) {
			return s.failed(def, runtime, protocolErr.Code, true)
		}
		return s.failed(def, runtime, "check_failed", false)
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	if result.State == "offline" {
		return s.offline(def, runtime, now)
	}
	return s.live(ctx, def, runtime, secrets, result, adapter.Descriptor)
}

func safeAttentionCode(code string) bool {
	switch code {
	case "authentication_required", "interaction_required", "configuration_required":
		return true
	default:
		return false
	}
}

func (s *Service) failed(def Definition, runtime Runtime, code string, attention bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	freshDef, freshRuntime, _, err := s.store.Get(def.ID)
	if err != nil {
		return err
	}
	def, runtime = freshDef, freshRuntime
	if !def.Enabled {
		runtime.State = StateDisabled
		runtime.NextCheckAt = nil
		runtime.StartingAt = nil
		if runtime.PendingRecordingID != "" {
			_ = s.store.DeleteRelation(runtime.PendingRecordingID)
			runtime.PendingRecordingID = ""
			delete(s.starting, def.ID)
		}
		return s.store.SaveRuntime(def.ID, runtime)
	}
	runtime.StartingAt = nil
	if runtime.PendingRecordingID != "" {
		_ = s.store.DeleteRelation(runtime.PendingRecordingID)
		runtime.PendingRecordingID = ""
		delete(s.starting, def.ID)
	}
	if runtime.FailureCount < 16 {
		runtime.FailureCount++
	}
	if attention {
		runtime.State = StateAttentionRequired
		runtime.NextCheckAt = timePointer(s.options.Now().Add(backoffDelay(runtime.FailureCount, s.options.Jitter)))
	} else {
		runtime.State = StateBackoff
		runtime.NextCheckAt = timePointer(s.options.Now().Add(backoffDelay(runtime.FailureCount, s.options.Jitter)))
	}
	runtime.LastErrorCode = code
	if err := s.store.SaveRuntime(def.ID, runtime); err != nil {
		return err
	}
	_ = s.store.AddEvent(def.ID, Event{Type: "check_failed", At: s.options.Now().UTC(), Code: code})
	if attention {
		_ = s.store.AddEvent(def.ID, Event{Type: "attention_required", At: s.options.Now().UTC(), Code: code})
	}
	return nil
}

func (s *Service) offline(def Definition, runtime Runtime, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	freshDef, freshRuntime, _, err := s.store.Get(def.ID)
	if err != nil {
		return err
	}
	def, runtime = freshDef, freshRuntime
	wasLive := runtime.LastObservedLive
	if def.Enabled {
		runtime.State = StateOffline
	} else {
		runtime.State = StateDisabled
	}
	runtime.FailureCount = 0
	runtime.LastErrorCode = ""
	runtime.LastObservedLive = false
	runtime.SessionDigest = ""
	runtime.SuppressedSessionDigest = ""
	runtime.Suppressed = false
	runtime.TerminalOutcome = ""
	runtime.ActiveRecordingID = ""
	runtime.StartingAt = nil
	if def.Enabled {
		runtime.NextCheckAt = timePointer(now.Add(intervalDelay(def, s.options.Jitter)))
	} else {
		runtime.NextCheckAt = nil
	}
	if err := s.store.SaveRuntime(def.ID, runtime); err != nil {
		return err
	}
	if wasLive {
		_ = s.store.AddEvent(def.ID, Event{Type: "recording_ended", At: now})
	}
	return nil
}

func (s *Service) live(ctx context.Context, def Definition, runtime Runtime, secrets map[string]string, result adapterproto.WatchCheckResult, descriptor *adapterproto.Descriptor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	freshDef, freshRuntime, freshSecrets, getErr := s.store.Get(def.ID)
	if getErr != nil || !freshDef.Enabled {
		s.mu.Unlock()
		return nil
	}
	def, runtime, secrets = freshDef, freshRuntime, freshSecrets
	now := s.options.Now().UTC()
	wasLive := runtime.LastObservedLive
	previousDigest := runtime.SessionDigest
	digest := ""
	if result.SessionRef != "" {
		sum := sha256.Sum256([]byte(result.SessionRef))
		digest = hex.EncodeToString(sum[:])
	}
	newSession := digest != "" && previousDigest != "" && digest != previousDigest
	if !wasLive || newSession {
		_ = s.store.AddEvent(def.ID, Event{Type: "live_detected", At: now})
	}
	if newSession {
		runtime.Suppressed = false
		runtime.SuppressedSessionDigest = ""
		runtime.TerminalOutcome = ""
	}
	if digest != "" {
		runtime.SessionDigest = digest
	}
	runtime.LastObservedLive = true
	runtime.FailureCount = 0
	runtime.LastErrorCode = ""
	if !def.Enabled {
		s.mu.Unlock()
		return nil
	}
	sameSession := digest != "" && digest == previousDigest
	if digest == "" {
		sameSession = wasLive
	}
	if runtime.Suppressed {
		// If the stopped session had no stable identity, a later first session
		// reference may merely be newly available metadata for the same live
		// broadcast. Keep suppression until an offline observation. When both
		// identities are known, a different digest proves a new session.
		sameSuppressedSession := runtime.SuppressedSessionDigest == "" || digest == "" || runtime.SuppressedSessionDigest == digest
		if sameSuppressedSession {
			runtime.State = StateSuppressed
			runtime.NextCheckAt = timePointer(now.Add(intervalDelay(def, s.options.Jitter)))
			err := s.store.SaveRuntime(def.ID, runtime)
			s.mu.Unlock()
			return err
		}
		// A different known session identity is a new broadcast.
		runtime.Suppressed = false
		runtime.SuppressedSessionDigest = ""
		runtime.TerminalOutcome = ""
	}
	if sameSession && (runtime.TerminalOutcome == "completed" || runtime.TerminalOutcome == "stopped") {
		runtime.State = StateSuppressed
		runtime.NextCheckAt = timePointer(now.Add(intervalDelay(def, s.options.Jitter)))
		err := s.store.SaveRuntime(def.ID, runtime)
		s.mu.Unlock()
		return err
	}
	if runtime.ActiveRecordingID != "" {
		runtime.State = StateRecording
		runtime.NextCheckAt = nil
		err := s.store.SaveRuntime(def.ID, runtime)
		s.mu.Unlock()
		return err
	}
	media := result.Media
	if media == nil {
		if !containsCapability(*descriptor, adapterproto.CapabilityResolve) {
			runtime.State = StateAttentionRequired
			runtime.NextCheckAt = timePointer(now.Add(intervalDelay(def, s.options.Jitter)))
			_ = s.store.SaveRuntime(def.ID, runtime)
			_ = s.store.AddEvent(def.ID, Event{Type: "attention_required", At: now, Code: "resolve_unavailable"})
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
		resolved, err := s.adapters.ResolveLegacyWithInputSecrets(ctx, def.AdapterID, def.Input, secrets, def.Resource)
		if err != nil {
			return s.failed(def, runtime, "resolve_unavailable", false)
		}
		media = &resolved
	} else {
		s.mu.Unlock()
	}
	pendingID, err := s.claimStarting(def.ID, &runtime, now)
	if err != nil {
		return err
	}
	provenance, err := s.adapters.Provenance(def.AdapterID)
	if err != nil {
		return s.failed(def, runtime, "adapter_unavailable", false)
	}
	title := def.Title
	if title == "" {
		title = result.Title
	}
	if err := ctx.Err(); err != nil {
		s.clearPendingStart(def.ID, pendingID)
		return err
	}
	// Serialize only this Watch's final start with delete/disable. The recording
	// manager may validate URLs or touch storage, so never hold the global service
	// mutex across StartResolvedWithID.
	unlockGate := s.lockStartGate(def.ID)
	defer unlockGate()
	s.mu.Lock()
	currentDef, currentRuntime, _, getErr := s.store.Get(def.ID)
	if getErr != nil || !currentDef.Enabled || currentRuntime.ActiveRecordingID != "" || currentRuntime.PendingRecordingID != pendingID {
		if getErr == nil && currentRuntime.PendingRecordingID == pendingID {
			_ = s.store.DeleteRelation(pendingID)
			currentRuntime.PendingRecordingID = ""
			currentRuntime.StartingAt = nil
			if currentDef.Enabled {
				currentRuntime.State = StateChecking
				currentRuntime.NextCheckAt = timePointer(now.Add(intervalDelay(currentDef, s.options.Jitter)))
			} else {
				currentRuntime.State = StateDisabled
				currentRuntime.NextCheckAt = nil
			}
			_ = s.store.SaveRuntime(def.ID, currentRuntime)
		} else {
			_ = s.store.DeleteRelation(pendingID)
		}
		delete(s.starting, def.ID)
		s.mu.Unlock()
		return nil
	}
	part := s.store.PartCount(def.ID, digest) + 1
	relation := RecordingRelation{RecordingID: pendingID, WatchID: def.ID, SessionDigest: digest, PartIndex: part, CreatedAt: now}
	if err := s.store.SaveRelation(relation); err != nil {
		currentRuntime.PendingRecordingID = ""
		currentRuntime.StartingAt = nil
		delete(s.starting, def.ID)
		_ = s.store.SaveRuntime(def.ID, currentRuntime)
		s.mu.Unlock()
		return s.failed(def, runtime, "storage_failed", false)
	}
	s.mu.Unlock()
	recording, err := s.manager.StartResolvedWithID(ctx, pendingID, def.AdapterID, *media, def.Resource, title, &provenance)
	s.mu.Lock()
	if err != nil {
		_ = s.store.DeleteRelation(pendingID)
		delete(s.starting, def.ID)
		currentRuntime.PendingRecordingID = ""
		currentRuntime.StartingAt = nil
		if currentDef.Enabled {
			currentRuntime.State = StateChecking
			currentRuntime.NextCheckAt = timePointer(now.Add(intervalDelay(currentDef, s.options.Jitter)))
		} else {
			currentRuntime.State = StateDisabled
			currentRuntime.NextCheckAt = nil
		}
		_ = s.store.SaveRuntime(def.ID, currentRuntime)
		s.mu.Unlock()
		return s.failed(def, runtime, "start_failed", false)
	}
	currentDef, runtime, _, err = s.store.Get(def.ID)
	if err != nil || runtime.PendingRecordingID != pendingID || !currentDef.Enabled {
		// The gate protects disable/delete. If persisted runtime is damaged, keep
		// the pending correlation ID for exact startup recovery.
		delete(s.starting, def.ID)
		s.mu.Unlock()
		return nil
	}
	runtime.ActiveRecordingID = recording.ID
	runtime.PendingRecordingID = ""
	runtime.State = StateRecording
	runtime.StartingAt = nil
	runtime.NextCheckAt = nil
	runtime.TerminalOutcome = ""
	delete(s.starting, def.ID)
	if err = s.store.SaveRuntime(def.ID, runtime); err != nil {
		s.mu.Unlock()
		return nil
	}
	if s.options.Preview != nil {
		_ = s.options.Preview(recording.ID, def.PreviewMode)
	}
	_ = s.store.AddEvent(def.ID, Event{Type: "recording_started", At: recording.StartedAt, RecordingID: recording.ID})
	s.mu.Unlock()
	return nil
}

func (s *Service) claimStarting(id string, runtime *Runtime, now time.Time) (string, error) {
	pendingID, err := newID()
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	def, current, _, err := s.store.Get(id)
	if err != nil {
		return "", err
	}
	if !def.Enabled || current.ActiveRecordingID != "" || current.PendingRecordingID != "" || current.State == StateStarting {
		return "", ErrActiveRecording
	}
	runtime.State = StateStarting
	runtime.StartingAt = timePointer(now)
	runtime.PendingRecordingID = pendingID
	runtime.NextCheckAt = nil
	if err := s.store.SaveRuntime(id, *runtime); err != nil {
		return "", err
	}
	s.starting[id] = pendingID
	return pendingID, nil
}

// lockStartGate serializes a single Watch's short recording handoff with its
// disable/delete operations without preventing unrelated Watches from moving.
func (s *Service) lockStartGate(id string) func() {
	s.mu.Lock()
	gate := s.startGates[id]
	if gate == nil {
		gate = &recordingStartGate{}
		s.startGates[id] = gate
	}
	gate.refs++
	s.mu.Unlock()
	gate.mu.Lock()
	return func() {
		gate.mu.Unlock()
		s.mu.Lock()
		gate.refs--
		if gate.refs == 0 && s.startGates[id] == gate {
			delete(s.startGates, id)
		}
		s.mu.Unlock()
	}
}

func (s *Service) clearPendingStart(id, pendingID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.store.DeleteRelation(pendingID)
	def, runtime, _, err := s.store.Get(id)
	delete(s.starting, id)
	if err != nil || runtime.PendingRecordingID != pendingID {
		return
	}
	runtime.PendingRecordingID = ""
	runtime.StartingAt = nil
	if def.Enabled {
		runtime.State = StateChecking
		runtime.NextCheckAt = timePointer(s.options.Now().UTC().Add(intervalDelay(def, s.options.Jitter)))
	} else {
		runtime.State = StateDisabled
		runtime.NextCheckAt = nil
	}
	_ = s.store.SaveRuntime(id, runtime)
}

func (s *Service) NotifyRecordingStopped(recordingID string) {
	if !watchIDPattern.MatchString(recordingID) {
		return
	}
	for _, snapshot := range s.store.Snapshots() {
		def, runtime := snapshot.Definition, snapshot.Runtime
		if runtime.ActiveRecordingID != recordingID {
			continue
		}
		s.mu.Lock()
		def, runtime, _, err := s.store.Get(def.ID)
		if err != nil || runtime.ActiveRecordingID != recordingID {
			s.mu.Unlock()
			return
		}
		runtime.ActiveRecordingID = ""
		if def.Enabled {
			runtime.State = StateSuppressed
			runtime.NextCheckAt = timePointer(s.options.Now().UTC().Add(intervalDelay(def, s.options.Jitter)))
		} else {
			runtime.State = StateDisabled
			runtime.NextCheckAt = nil
		}
		runtime.Suppressed = true
		runtime.SuppressedSessionDigest = runtime.SessionDigest
		runtime.TerminalOutcome = "stopped"
		runtime.StartingAt = nil
		runtime.PendingRecordingID = ""
		_ = s.store.SaveRuntime(def.ID, runtime)
		_ = s.store.AddEvent(def.ID, Event{Type: "recording_ended", At: s.options.Now().UTC(), RecordingID: recordingID})
		s.mu.Unlock()
		return
	}
}

func (s *Service) recoverLinks() {
	now := s.options.Now().UTC()
	for _, snapshot := range s.store.Snapshots() {
		def, runtime := snapshot.Definition, snapshot.Runtime
		if runtime.ActiveRecordingID != "" {
			rec, getErr := s.manager.Get(runtime.ActiveRecordingID)
			if getErr == nil && rec.State == domain.StateRecording {
				runtime.State = StateRecording
				runtime.NextCheckAt = nil
				_ = s.store.SaveRuntime(def.ID, runtime)
				s.applyPreview(def, rec.ID)
				continue
			}
			if getErr == nil {
				s.markTerminal(def, &runtime, rec, now)
				s.applyPreview(def, rec.ID)
			} else if errors.Is(getErr, storage.ErrNotFound) {
				_ = s.store.DeleteRelation(runtime.ActiveRecordingID)
				runtime.ActiveRecordingID = ""
				if def.Enabled {
					runtime.State = StateChecking
					runtime.NextCheckAt = timePointer(now)
				} else {
					runtime.State = StateDisabled
					runtime.NextCheckAt = nil
				}
			} else {
				runtime.State = StateAttentionRequired
				runtime.LastErrorCode = "storage_failed"
				runtime.NextCheckAt = nil
				_ = s.store.SaveRuntime(def.ID, runtime)
				continue
			}
		}
		if runtime.PendingRecordingID != "" {
			pendingID := runtime.PendingRecordingID
			recording, getErr := s.manager.Get(pendingID)
			if getErr == nil {
				s.applyPreview(def, recording.ID)
				relation, relationErr := s.store.Relation(pendingID)
				if errors.Is(relationErr, ErrNotFound) {
					part := s.store.PartCount(def.ID, runtime.SessionDigest) + 1
					_ = s.store.SaveRelation(RecordingRelation{RecordingID: pendingID, WatchID: def.ID, SessionDigest: runtime.SessionDigest, PartIndex: part, CreatedAt: recording.StartedAt})
				} else if relationErr != nil || relation.WatchID != def.ID {
					runtime.State = StateAttentionRequired
					runtime.LastErrorCode = "storage_failed"
					runtime.NextCheckAt = nil
					_ = s.store.SaveRuntime(def.ID, runtime)
					continue
				}
				if recording.State == domain.StateRecording {
					runtime.ActiveRecordingID = pendingID
					runtime.PendingRecordingID = ""
					runtime.State = StateRecording
					runtime.StartingAt = nil
					runtime.NextCheckAt = nil
				} else {
					runtime.ActiveRecordingID = pendingID
					runtime.PendingRecordingID = ""
					s.markTerminal(def, &runtime, recording, now)
				}
			} else if errors.Is(getErr, storage.ErrNotFound) {
				_ = s.store.DeleteRelation(pendingID)
				runtime.PendingRecordingID = ""
				runtime.StartingAt = nil
				if def.Enabled {
					runtime.State = StateChecking
					runtime.NextCheckAt = timePointer(now)
				} else {
					runtime.State = StateDisabled
					runtime.NextCheckAt = nil
				}
			} else {
				runtime.State = StateAttentionRequired
				runtime.LastErrorCode = "storage_failed"
				runtime.NextCheckAt = nil
				_ = s.store.SaveRuntime(def.ID, runtime)
				continue
			}
		} else if runtime.State == StateStarting {
			// A pre-upgrade or damaged claim has no exact correlation ID. Never
			// guess by source or timestamp; stop until an operator resolves it.
			runtime.State = StateAttentionRequired
			runtime.LastErrorCode = "start_recovery_ambiguous"
			runtime.StartingAt = nil
			runtime.NextCheckAt = nil
		}
		if def.Enabled && runtime.ActiveRecordingID == "" && (runtime.NextCheckAt == nil || runtime.NextCheckAt.Before(now)) {
			runtime.NextCheckAt = timePointer(now.Add(startupStagger(def.ID, def.CheckIntervalSeconds)))
			if runtime.State == StateOffline {
				runtime.State = StateChecking
			}
		}
		if !def.Enabled && runtime.ActiveRecordingID == "" {
			runtime.State = StateDisabled
			runtime.NextCheckAt = nil
		}
		_ = s.store.SaveRuntime(def.ID, runtime)
	}
}

func (s *Service) reconcileLinks(now time.Time) {
	for _, snapshot := range s.store.Snapshots() {
		def, runtime := snapshot.Definition, snapshot.Runtime
		if runtime.PendingRecordingID != "" {
			s.reconcilePending(def.ID, now)
			continue
		}
		if runtime.ActiveRecordingID == "" {
			continue
		}
		s.reconcileOne(def, runtime, now)
	}
}

func (s *Service) reconcilePending(id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	def, runtime, _, err := s.store.Get(id)
	if err != nil || runtime.PendingRecordingID == "" {
		return
	}
	pendingID := runtime.PendingRecordingID
	if s.starting[id] == pendingID {
		return
	}
	recording, getErr := s.manager.Get(pendingID)
	if errors.Is(getErr, storage.ErrNotFound) {
		_ = s.store.DeleteRelation(pendingID)
		runtime.PendingRecordingID = ""
		runtime.StartingAt = nil
		if def.Enabled {
			runtime.State = StateChecking
			runtime.NextCheckAt = timePointer(now)
		} else {
			runtime.State = StateDisabled
			runtime.NextCheckAt = nil
		}
		_ = s.store.SaveRuntime(id, runtime)
		return
	}
	if getErr != nil {
		runtime.State = StateAttentionRequired
		runtime.LastErrorCode = "storage_failed"
		runtime.NextCheckAt = nil
		_ = s.store.SaveRuntime(id, runtime)
		return
	}
	relation, relationErr := s.store.Relation(pendingID)
	if errors.Is(relationErr, ErrNotFound) {
		part := s.store.PartCount(id, runtime.SessionDigest) + 1
		relationErr = s.store.SaveRelation(RecordingRelation{RecordingID: pendingID, WatchID: id, SessionDigest: runtime.SessionDigest, PartIndex: part, CreatedAt: recording.StartedAt})
		if relationErr == nil {
			relation = RecordingRelation{RecordingID: pendingID, WatchID: id, SessionDigest: runtime.SessionDigest, PartIndex: part, CreatedAt: recording.StartedAt}
		}
	}
	if relationErr != nil || relation.WatchID != id {
		runtime.State = StateAttentionRequired
		runtime.LastErrorCode = "storage_failed"
		runtime.NextCheckAt = nil
		_ = s.store.SaveRuntime(id, runtime)
		return
	}
	if recording.State == domain.StateRecording {
		runtime.ActiveRecordingID = pendingID
		runtime.PendingRecordingID = ""
		runtime.State = StateRecording
		runtime.StartingAt = nil
		runtime.NextCheckAt = nil
		_ = s.store.SaveRuntime(id, runtime)
		s.applyPreview(def, pendingID)
		return
	}
	runtime.ActiveRecordingID = pendingID
	runtime.PendingRecordingID = ""
	s.markTerminal(def, &runtime, recording, now)
	s.applyPreview(def, pendingID)
}

func (s *Service) applyPreview(def Definition, recordingID string) {
	if s.options.Preview != nil {
		_ = s.options.Preview(recordingID, def.PreviewMode)
	}
}

func (s *Service) reconcileOne(def Definition, runtime Runtime, now time.Time) {
	recording, err := s.manager.Get(runtime.ActiveRecordingID)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			runtime.State = StateAttentionRequired
			runtime.LastErrorCode = "storage_failed"
			runtime.NextCheckAt = nil
			_ = s.store.SaveRuntime(def.ID, runtime)
			return
		}
		_ = s.store.DeleteRelation(runtime.ActiveRecordingID)
		runtime.ActiveRecordingID = ""
		if def.Enabled {
			runtime.State = StateChecking
			runtime.NextCheckAt = timePointer(now)
		} else {
			runtime.State = StateDisabled
			runtime.NextCheckAt = nil
		}
		_ = s.store.SaveRuntime(def.ID, runtime)
		return
	}
	if recording.State == domain.StateRecording {
		runtime.State = StateRecording
		runtime.NextCheckAt = nil
		_ = s.store.SaveRuntime(def.ID, runtime)
		return
	}
	s.markTerminal(def, &runtime, recording, now)
}

func (s *Service) markTerminal(def Definition, runtime *Runtime, recording *domain.Recording, now time.Time) {
	runtime.ActiveRecordingID = ""
	runtime.StartingAt = nil
	runtime.TerminalOutcome = string(recording.State)
	delay := time.Duration(0)
	if recording.State == domain.StateInterrupted {
		runtime.FailureCount++
		delay = backoffDelay(runtime.FailureCount, s.options.Jitter)
	}
	if def.Enabled {
		runtime.State = StateChecking
		runtime.NextCheckAt = timePointer(now.Add(delay))
	} else {
		runtime.State = StateDisabled
		runtime.NextCheckAt = nil
	}
	_ = s.store.SaveRuntime(def.ID, *runtime)
	_ = s.store.AddEvent(def.ID, Event{Type: "recording_ended", At: now, RecordingID: recording.ID})
}

func (s *Service) Summary() map[string]int {
	result := map[string]int{"total": 0, "enabled": 0, "recording": 0, "offline": 0, "backoff": 0, "attention_required": 0}
	for _, snapshot := range s.store.Snapshots() {
		def, runtime := snapshot.Definition, snapshot.Runtime
		result["total"]++
		if def.Enabled {
			result["enabled"]++
		}
		switch runtime.State {
		case StateRecording:
			result["recording"]++
		case StateOffline:
			result["offline"]++
		case StateBackoff:
			result["backoff"]++
		case StateAttentionRequired:
			result["attention_required"]++
		}
	}
	return result
}

func intervalDelay(def Definition, jitter func(time.Duration) time.Duration) time.Duration {
	return clampJitter(time.Duration(def.CheckIntervalSeconds)*time.Second, jitter)
}
func backoffDelay(failures int, jitter func(time.Duration) time.Duration) time.Duration {
	seconds := 10
	for i := 1; i < failures && seconds < 300; i++ {
		seconds *= 2
		if seconds > 300 {
			seconds = 300
		}
	}
	delay := clampJitter(time.Duration(seconds)*time.Second, jitter)
	if delay > 300*time.Second {
		return 300 * time.Second
	}
	return delay
}
func clampJitter(base time.Duration, jitter func(time.Duration) time.Duration) time.Duration {
	bound := base / 10
	delta := time.Duration(0)
	if jitter != nil {
		delta = jitter(bound)
	}
	if delta < -bound {
		delta = -bound
	}
	if delta > bound {
		delta = bound
	}
	value := base + delta
	if value < time.Second {
		return time.Second
	}
	return value
}
func startupStagger(id string, interval int) time.Duration {
	if interval <= 0 {
		return 0
	}
	sum := sha256.Sum256([]byte(id))
	millis := int64(interval * 1000)
	if millis <= 0 {
		return 0
	}
	n := int64(sum[0])<<24 | int64(sum[1])<<16 | int64(sum[2])<<8 | int64(sum[3])
	return time.Duration(n%millis) * time.Millisecond
}
func timePointer(value time.Time) *time.Time { copy := value.UTC(); return &copy }
func containsCapability(descriptor adapterproto.Descriptor, value string) bool {
	for _, item := range descriptor.Capabilities {
		if item == value {
			return true
		}
	}
	return false
}
func normalizeObject(raw json.RawMessage) (json.RawMessage, error) {
	if err := adapterproto.ValidateObject(raw); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return nil, ErrInvalid
	}
	data, err := json.Marshal(object)
	if err != nil || len(data) > MaxInputBytes {
		return nil, ErrInvalid
	}
	return data, nil
}
func validateTitle(title string) error {
	if len(title) > 1024 || !utf8.ValidString(title) {
		return ErrInvalid
	}
	return nil
}
func newID() (string, error) {
	var raw [16]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}
func cloneSecrets(values map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range values {
		out[key] = value
	}
	return out
}
func bytesEqual(left, right []byte) bool { return string(left) == string(right) }
func sameRef(a, b *adapterproto.ResourceRef) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Type == b.Type && a.ID == b.ID && sameRef(a.Parent, b.Parent)
}
func sameArchiveResource(a *domain.ResourceReference, b *adapterproto.ResourceRef) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Type == b.Type && a.ID == b.ID && sameArchiveResource(a.Parent, b.Parent)
}
