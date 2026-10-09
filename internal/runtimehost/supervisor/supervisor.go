package supervisor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultShutdownTimeout = 15 * time.Second
	defaultCleanupTimeout  = 2 * time.Second
	maxGenerationProcesses = 32
)

type managedProcess struct {
	spec         ProcessSpec
	child        Child
	state        ProcessState
	proxy        *fixedProxy
	target       *url.URL
	exitObserved bool
}

type managedGeneration struct {
	id             string
	control        *managedProcess
	engine         *managedProcess
	controlActive  bool
	inflight       int
	inflightChange chan struct{}
}

// Supervisor owns one stable HTTP listener and immutable Control/Engine
// processes. Replacing a Control never signals its generation's Engine.
type Supervisor struct {
	launcher  Launcher
	readiness Readiness
	handoff   ControlLifecycle
	onExit    ProcessExitObserver
	shutdown  time.Duration
	cleanup   time.Duration
	maxGen    int

	opGate chan struct{}
	mu     sync.Mutex
	gens   map[string]*managedGeneration

	activeControl string
	server        *http.Server
	listener      net.Listener
	serving       bool
	closed        bool
}

func New(options Options) (*Supervisor, error) {
	if options.Launcher == nil || options.Readiness == nil || options.ControlLifecycle == nil {
		return nil, ErrInvalidConfig
	}
	shutdown := options.ShutdownTimeout
	if shutdown <= 0 {
		shutdown = defaultShutdownTimeout
	}
	if shutdown > 2*time.Minute {
		return nil, ErrInvalidConfig
	}
	cleanup := options.CleanupTimeout
	if cleanup <= 0 {
		cleanup = defaultCleanupTimeout
	}
	if cleanup > 30*time.Second {
		return nil, ErrInvalidConfig
	}
	maxGen := options.MaxGenerations
	if maxGen == 0 {
		maxGen = maxGenerationProcesses
	}
	if maxGen < 1 || maxGen > maxGenerationProcesses {
		return nil, ErrInvalidConfig
	}
	return &Supervisor{
		launcher: options.Launcher, readiness: options.Readiness, handoff: options.ControlLifecycle,
		onExit:   options.OnProcessExit,
		shutdown: shutdown, cleanup: cleanup, maxGen: maxGen,
		opGate: make(chan struct{}, 1), gens: make(map[string]*managedGeneration),
	}, nil
}

// StageGeneration starts and verifies both roles before making either one
// active. If either process or readiness check fails, the candidate children
// are stopped and the previously active generation is untouched.
func (s *Supervisor) StageGeneration(ctx context.Context, spec GenerationSpec) error {
	if ctx == nil || !validGenerationID(spec.ID) || spec.Engine.GenerationID != spec.ID || spec.Control.GenerationID != spec.ID || spec.Engine.Role != RoleEngine || spec.Control.Role != RoleControl {
		return ErrInvalidProcess
	}
	if err := validateControlTarget(spec.ControlTarget); err != nil {
		return err
	}
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := s.startRoleLocked(ctx, spec.Engine, nil); err != nil {
		return err
	}
	if err := s.startRoleLocked(ctx, spec.Control, spec.ControlTarget); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
		defer cancel()
		_ = s.stopAndRemoveRole(cleanupCtx, spec.ID, RoleEngine)
		return err
	}
	return nil
}

// StartEngine starts one generation's Engine process and waits for readiness.
// It may be called before StartControl to support explicit host orchestration.
func (s *Supervisor) StartEngine(ctx context.Context, spec ProcessSpec) error {
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if spec.Role != RoleEngine {
		return ErrInvalidProcess
	}
	return s.startRoleLocked(ctx, spec, nil)
}

// StartControl starts and readies a candidate Control process. It does not
// change the active route; ActivateControl is the only routing transition.
func (s *Supervisor) StartControl(ctx context.Context, spec ProcessSpec, target *url.URL) error {
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if spec.Role != RoleControl {
		return ErrInvalidProcess
	}
	if err := validateControlTarget(target); err != nil {
		return err
	}
	return s.startRoleLocked(ctx, spec, target)
}

func (s *Supervisor) startRoleLocked(ctx context.Context, spec ProcessSpec, target *url.URL) error {
	if ctx == nil || ctx.Err() != nil || !validGenerationID(spec.GenerationID) || (spec.Role != RoleControl && spec.Role != RoleEngine) {
		return ErrInvalidProcess
	}
	if spec.Role == RoleControl && validateControlTarget(target) != nil {
		return ErrInvalidProcess
	}
	cloned := cloneProcessSpec(spec)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSupervisorClosed
	}
	g := s.gens[spec.GenerationID]
	if g == nil {
		if len(s.gens) >= s.maxGen {
			s.mu.Unlock()
			return ErrGenerationLimit
		}
		g = &managedGeneration{id: spec.GenerationID, inflightChange: make(chan struct{})}
		s.gens[spec.GenerationID] = g
	}
	roleSlot := &g.engine
	if spec.Role == RoleControl {
		roleSlot = &g.control
	}
	if *roleSlot != nil {
		s.mu.Unlock()
		return ErrGenerationExists
	}
	mp := &managedProcess{spec: cloned, state: ProcessStarting}
	if target != nil {
		copyTarget := *target
		mp.target = &copyTarget
		mp.proxy = makeProxy(&copyTarget)
	}
	*roleSlot = mp
	s.mu.Unlock()

	child, err := s.launcher.Start(ctx, cloned)
	if err != nil {
		if child != nil {
			s.mu.Lock()
			mp.child = child
			s.mu.Unlock()
			go s.observeExit(g.id, spec.Role, mp)
			cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
			stopErr := stopChild(cleanupCtx, child)
			cancel()
			if stopErr != nil {
				s.mu.Lock()
				mp.state = ProcessStopping
				s.mu.Unlock()
				return fmt.Errorf("start %s generation process failed", spec.Role)
			}
		}
		s.removeRoleIf(g.id, spec.Role, mp)
		return fmt.Errorf("start %s generation process: %w", spec.Role, err)
	}
	if child == nil {
		s.removeRoleIf(g.id, spec.Role, mp)
		return ErrCandidateNotReady
	}
	s.mu.Lock()
	mp.child = child
	s.mu.Unlock()
	go s.observeExit(g.id, spec.Role, mp)
	if err := s.readiness.WaitReady(ctx, cloned, child); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
		stopErr := stopChild(cleanupCtx, child)
		cancel()
		if stopErr == nil {
			s.removeRoleIf(g.id, spec.Role, mp)
		} else {
			s.mu.Lock()
			mp.state = ProcessStopping
			s.mu.Unlock()
		}
		return fmt.Errorf("%s generation readiness failed", spec.Role)
	}
	select {
	case <-child.Done():
		s.removeRoleIf(g.id, spec.Role, mp)
		return ErrCandidateNotReady
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.gens[g.id] != g || processForRole(g, spec.Role) != mp || mp.state != ProcessStarting {
		return ErrCandidateNotReady
	}
	mp.state = ProcessReady
	return nil
}

// ActivateControl atomically switches public routing to a ready candidate,
// waits for old routed requests to finish, and stops only the old Control.
// Its Recorder Engine is intentionally retained until RetireEngine proves it
// owns no active recordings.
func (s *Supervisor) ActivateControl(ctx context.Context, generationID string) error {
	return s.ActivateControlWith(ctx, generationID, nil, nil)
}

// ActivateControlWith performs a durable activation transaction around the
// route switch. beforeRoute runs after the old Control has fenced mutations
// and drained admitted requests, but before public routing changes. If the
// candidate cannot become active after routing, afterAbort reverses the
// durable state transition.
func (s *Supervisor) ActivateControlWith(ctx context.Context, generationID string, beforeRoute func() error, afterAbort func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	candidate := s.gens[generationID]
	if candidate == nil || candidate.engine == nil || candidate.control == nil || candidate.engine.state != ProcessReady || candidate.control.state != ProcessReady {
		s.mu.Unlock()
		return ErrCandidateNotReady
	}
	if s.activeControl == generationID {
		s.mu.Unlock()
		return nil
	}
	oldID := s.activeControl
	old := s.gens[oldID]
	oldControlExited := old != nil && confirmedDeadControl(old.control)
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// A dead active Control cannot fence itself over IPC. The Host may recover
	// its route through a ready candidate, but only when the exact old child is
	// confirmed exited. A live, starting, stopping, or otherwise uncertain
	// process must still complete the normal ownership handoff.
	if !oldControlExited {
		if err := s.handoff.PrepareHandoff(ctx, oldID, generationID); err != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
			_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
			cancel()
			return errors.New("control handoff preparation failed")
		}
	}
	// The old generation must first fence new durable mutations, then finish
	// requests admitted before that fence. Do this before enabling the new
	// scheduler or changing the active epoch/route so an old mutation cannot
	// commit after ownership has moved.
	if old != nil && old.control != nil {
		if err := s.waitControlIdle(ctx, old); err != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
			_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
			cancel()
			return ErrControlDrainPending
		}
	}
	// Only prepare the candidate after the previous generation has stopped
	// admitting and completed all shared durable management work. The passive
	// process-level readiness check intentionally does not open these stores.
	if err := s.handoff.PrepareActivation(ctx, generationID); err != nil {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
		_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
		cancel()
		return errors.New("candidate Control application preparation failed")
	}
	if beforeRoute != nil {
		if err := beforeRoute(); err != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
			_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
			cancel()
			return errors.New("durable control activation preparation failed")
		}
	}

	// Keep route publication under the same lock used by request admission.
	// The candidate remains passive until it owns the stable route, so only
	// one generation can run Watch/retention/background admission at a time.
	s.mu.Lock()
	if s.closed || s.gens[generationID] != candidate || candidate.control == nil || candidate.control.state != ProcessReady {
		routeBeforeAbort := s.activeControl
		s.mu.Unlock()
		if err := s.abortDurableActivation(afterAbort, ErrCandidateNotReady); err != nil {
			return err
		}
		s.setActiveControlRoute(routeBeforeAbort)
		rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
		_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
		cancel()
		return ErrCandidateNotReady
	}
	if s.activeControl != oldID {
		routeBeforeAbort := s.activeControl
		s.mu.Unlock()
		if err := s.abortDurableActivation(afterAbort, ErrCandidateNotReady); err != nil {
			return err
		}
		s.setActiveControlRoute(routeBeforeAbort)
		rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
		_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
		cancel()
		return ErrCandidateNotReady
	}
	s.activeControl = generationID
	candidate.controlActive = true
	if old != nil {
		old.controlActive = false
	}
	s.mu.Unlock()
	// Routing changes before the candidate starts background work. A passive
	// candidate may serve safe reads during this small interval, while mutation
	// requests remain fenced by its Control lifecycle gate.
	if activateErr := s.handoff.Activate(ctx, generationID); activateErr != nil {
		if err := s.abortDurableActivation(afterAbort, activateErr); err != nil {
			return err
		}
		s.setActiveControlRoute(oldID)
		rollbackCtx, cancel := context.WithTimeout(context.Background(), s.cleanup)
		_ = s.handoff.Rollback(rollbackCtx, oldID, generationID)
		cancel()
		return errors.New("candidate control activation failed")
	}
	if old != nil && old.control != nil {
		drainCtx, cancel := context.WithTimeout(context.Background(), s.shutdown)
		defer cancel()
		if err := s.drainControlLocked(drainCtx, oldID); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return ErrControlDrainPending
			}
			return err
		}
	}
	return nil
}

// activationAbortFailure keeps the returned error text stable while retaining
// both the activation failure and durable abort failure for internal
// classification with errors.Is/errors.As.
type activationAbortFailure struct {
	cause error
}

func (e activationAbortFailure) Error() string { return "durable control activation abort failed" }
func (e activationAbortFailure) Unwrap() error { return e.cause }

// abortDurableActivation disables routing before reversing durable activation
// state. If reversal fails, caller must leave routing unavailable and skip
// handoff rollback because the old generation may no longer be compatible.
func (s *Supervisor) abortDurableActivation(afterAbort func() error, cause error) error {
	if afterAbort == nil {
		return nil
	}
	s.setActiveControlRoute("")
	if err := afterAbort(); err != nil {
		return activationAbortFailure{cause: errors.Join(cause, err)}
	}
	return nil
}

func (s *Supervisor) setActiveControlRoute(generationID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeControl = generationID
	for _, generation := range s.gens {
		generation.controlActive = generationID != "" && generation.id == generationID
	}
}

// confirmedDeadControl reports whether this specific Control child is both
// recorded as exited and has closed its process-done signal. State alone is
// not sufficient: ProcessStopping can still describe a live process whose
// handoff must be coordinated normally.
func confirmedDeadControl(control *managedProcess) bool {
	if control == nil || control.child == nil || (control.state != ProcessFailed && control.state != ProcessExited) {
		return false
	}
	select {
	case <-control.child.Done():
		return true
	default:
		return false
	}
}

// DrainControl completes a Control drain begun by ActivateControl. It is
// useful for retrying after long-lived requests outlast the initial bounded
// drain window. The active Control cannot be stopped through this method.
func (s *Supervisor) DrainControl(ctx context.Context, generationID string) error {
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if ctx == nil {
		ctx = context.Background()
	}
	return s.drainControlLocked(ctx, generationID)
}

func (s *Supervisor) drainControlLocked(ctx context.Context, generationID string) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	g := s.gens[generationID]
	if g == nil {
		s.mu.Unlock()
		return ErrGenerationNotFound
	}
	if s.activeControl == generationID {
		s.mu.Unlock()
		return ErrControlIsActive
	}
	control := g.control
	s.mu.Unlock()
	if control == nil {
		return nil
	}
	if err := s.waitControlIdle(ctx, g); err != nil {
		return err
	}
	if err := stopChild(ctx, control.child); err != nil {
		return err
	}
	s.mu.Lock()
	if g.control == control {
		if control.proxy != nil {
			control.proxy.transport.CloseIdleConnections()
		}
		control.state = ProcessExited
		g.control = nil
		g.controlActive = false
		s.removeEmptyGenerationLocked(g)
	}
	s.mu.Unlock()
	return nil
}

// StopControl stops a non-active Control process while retaining its Engine.
func (s *Supervisor) StopControl(ctx context.Context, generationID string) error {
	return s.DrainControl(ctx, generationID)
}

// RetireEngine begins an idempotent engine admission drain and requires a
// fresh zero-active-recording proof before stopping/removing the old Engine.
// Active generations and generations with a running Control are never retired.
func (s *Supervisor) RetireEngine(ctx context.Context, generationID string, drain EngineDrain) error {
	if drain == nil {
		return ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if err := s.ensureOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	g := s.gens[generationID]
	if g == nil || g.engine == nil {
		s.mu.Unlock()
		return ErrGenerationNotFound
	}
	if s.activeControl == generationID {
		s.mu.Unlock()
		return ErrEngineStillDefault
	}
	if g.control != nil {
		s.mu.Unlock()
		return ErrControlStillRunning
	}
	engine := g.engine
	s.mu.Unlock()
	if err := drain.BeginDrain(ctx, generationID); err != nil {
		return errors.New("engine admission drain could not be confirmed")
	}
	active, err := drain.ActiveRecordings(ctx, generationID)
	if err != nil {
		return errors.New("engine recording inventory could not be confirmed")
	}
	if active < 0 {
		return errors.New("engine recording inventory is invalid")
	}
	if active != 0 {
		return ErrEngineNotDrained
	}
	if err := stopChild(ctx, engine.child); err != nil {
		return err
	}
	s.mu.Lock()
	if g.engine == engine {
		g.engine = nil
		s.removeEmptyGenerationLocked(g)
	}
	s.mu.Unlock()
	return nil
}

// Serve starts the Host-owned public listener and reverse-proxies each
// request to the atomically selected ready Control process.
func (s *Supervisor) Serve(ctx context.Context, listener net.Listener) error {
	return s.ServeWithHostHandler(ctx, listener, "", nil)
}

// ServeWithHostHandler lets the stable Host own a narrow route family, such
// as release update operations, while all other traffic follows the active
// Control generation. hostHandler should use s as its fallback handler.
func (s *Supervisor) ServeWithHostHandler(ctx context.Context, listener net.Listener, prefix string, hostHandler http.Handler) error {
	if ctx == nil || listener == nil {
		return ErrInvalidConfig
	}
	if (prefix == "") != (hostHandler == nil) || (prefix != "" && (!strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "?#\x00"))) {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = listener.Close()
		return ErrSupervisorClosed
	}
	if s.serving {
		s.mu.Unlock()
		_ = listener.Close()
		return ErrListenerAlreadySet
	}
	var handler http.Handler = s
	if hostHandler != nil {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == prefix || strings.HasPrefix(r.URL.Path, prefix+"/") {
				hostHandler.ServeHTTP(w, r)
				return
			}
			s.ServeHTTP(w, r)
		})
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	s.server, s.listener, s.serving = server, listener, true
	s.mu.Unlock()

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	var serveErr error
	select {
	case serveErr = <-serveDone:
	case <-ctx.Done():
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), s.shutdown)
	defer cancel()
	closeErr := s.Close(closeCtx)
	if errors.Is(serveErr, http.ErrServerClosed) || (serveErr == nil && ctx.Err() != nil) {
		serveErr = nil
	}
	if serveErr != nil {
		return serveErr
	}
	return closeErr
}

// Close stops public admission, then Controls, then Engines. It is safe to
// call repeatedly after a deadline to retry stopping any remaining children.
func (s *Supervisor) Close(ctx context.Context) error {
	if err := s.lockOps(ctx); err != nil {
		return err
	}
	defer s.unlockOps()
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	s.closed = true
	server := s.server
	controls := make([]*managedProcess, 0, len(s.gens))
	engines := make([]*managedProcess, 0, len(s.gens))
	ids := sortedGenerationIDs(s.gens)
	for _, id := range ids {
		g := s.gens[id]
		if g.control != nil {
			g.control.state = ProcessStopping
			controls = append(controls, g.control)
		}
	}
	for _, id := range ids {
		g := s.gens[id]
		if g.engine != nil {
			g.engine.state = ProcessStopping
			engines = append(engines, g.engine)
		}
	}
	s.mu.Unlock()

	var errs []error
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			errs = append(errs, errors.New("public listener shutdown timed out"))
		}
	}
	for _, child := range append(controls, engines...) {
		if child.child == nil {
			continue
		}
		if err := stopChild(ctx, child.child); err != nil {
			errs = append(errs, errors.New("runtime child shutdown was not confirmed"))
			continue
		}
		s.mu.Lock()
		child.state = ProcessExited
		s.mu.Unlock()
	}
	return errors.Join(errs...)
}

func (s *Supervisor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.closed || s.activeControl == "" {
		s.mu.Unlock()
		http.Error(w, "control plane unavailable", http.StatusServiceUnavailable)
		return
	}
	g := s.gens[s.activeControl]
	if g == nil || g.control == nil || g.control.state != ProcessReady || g.control.proxy == nil {
		s.mu.Unlock()
		http.Error(w, "control plane unavailable", http.StatusServiceUnavailable)
		return
	}
	g.inflight++
	proxy := g.control.proxy
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		g.inflight--
		close(g.inflightChange)
		g.inflightChange = make(chan struct{})
		s.mu.Unlock()
	}()
	proxy.ServeHTTP(w, r)
}

func (s *Supervisor) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := Snapshot{Serving: s.serving && !s.closed, Closed: s.closed, ActiveControlGeneration: s.activeControl}
	for _, id := range sortedGenerationIDs(s.gens) {
		g := s.gens[id]
		row := GenerationSnapshot{ID: id, ControlActive: g.controlActive}
		if g.control != nil {
			row.Control.State = g.control.state
		}
		if g.engine != nil {
			row.Engine.State = g.engine.state
		}
		result.Generations = append(result.Generations, row)
	}
	return result
}

func (s *Supervisor) waitControlIdle(ctx context.Context, g *managedGeneration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		s.mu.Lock()
		if g.inflight == 0 {
			s.mu.Unlock()
			return nil
		}
		changed := g.inflightChange
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Supervisor) observeExit(generationID string, role Role, process *managedProcess) {
	<-process.child.Done()
	s.mu.Lock()
	if process.exitObserved {
		s.mu.Unlock()
		return
	}
	process.exitObserved = true
	if generation := s.gens[generationID]; generation != nil && processForRole(generation, role) == process {
		if process.state == ProcessStopping {
			process.state = ProcessExited
		} else if process.child.Err() != nil {
			process.state = ProcessFailed
		} else {
			process.state = ProcessExited
		}
	}
	observer := s.onExit
	spec := cloneProcessSpec(process.spec)
	err := process.child.Err()
	s.mu.Unlock()
	if observer != nil {
		observer(spec, err)
	}
}

func (s *Supervisor) removeRoleIf(generationID string, role Role, process *managedProcess) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.gens[generationID]
	if g == nil {
		return
	}
	if role == RoleEngine && g.engine == process {
		g.engine = nil
	}
	if role == RoleControl && g.control == process {
		g.control = nil
	}
	s.removeEmptyGenerationLocked(g)
}

func (s *Supervisor) removeEmptyGenerationLocked(g *managedGeneration) {
	if g.engine == nil && g.control == nil && s.activeControl != g.id {
		delete(s.gens, g.id)
	}
}

func (s *Supervisor) stopAndRemoveRole(ctx context.Context, generationID string, role Role) error {
	s.mu.Lock()
	g := s.gens[generationID]
	if g == nil {
		s.mu.Unlock()
		return nil
	}
	process := processForRole(g, role)
	s.mu.Unlock()
	if process == nil || process.child == nil {
		return nil
	}
	if err := stopChild(ctx, process.child); err != nil {
		s.mu.Lock()
		process.state = ProcessStopping
		s.mu.Unlock()
		return err
	}
	s.removeRoleIf(generationID, role, process)
	return nil
}

func (s *Supervisor) ensureOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSupervisorClosed
	}
	return nil
}

func (s *Supervisor) lockOps(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case s.opGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Supervisor) unlockOps() { <-s.opGate }

func stopChild(ctx context.Context, child Child) error {
	if child == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-child.Done():
		return nil
	default:
	}
	if err := child.Stop(ctx); err != nil {
		_ = child.Kill()
		return err
	}
	select {
	case <-child.Done():
		return nil
	case <-ctx.Done():
		_ = child.Kill()
		return ctx.Err()
	}
}

func processForRole(g *managedGeneration, role Role) *managedProcess {
	if role == RoleControl {
		return g.control
	}
	return g.engine
}

func cloneProcessSpec(spec ProcessSpec) ProcessSpec {
	spec.Args = append([]string(nil), spec.Args...)
	spec.Env = append([]string(nil), spec.Env...)
	return spec
}

func sortedGenerationIDs(gens map[string]*managedGeneration) []string {
	ids := make([]string, 0, len(gens))
	for id := range gens {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func validateControlTarget(target *url.URL) error {
	if target == nil || target.Scheme != "http" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") {
		return ErrInvalidProcess
	}
	if target.Opaque != "" || target.Hostname() == "" {
		return ErrInvalidProcess
	}
	ip := net.ParseIP(target.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return ErrInvalidProcess
	}
	port, err := strconv.Atoi(target.Port())
	if err != nil || port < 1 || port > 65535 {
		return ErrInvalidProcess
	}
	return nil
}

func validGenerationID(id string) bool {
	if len(id) == 36 && id[8] == '-' && id[13] == '-' && id[18] == '-' && id[23] == '-' {
		for index, r := range id {
			if index == 8 || index == 13 || index == 18 || index == 23 {
				continue
			}
			if !isHex(r) {
				return false
			}
		}
		return true
	}
	if len(id) < 32 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !isHex(r) {
			return false
		}
	}
	return true
}

func isHex(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

func (r Role) String() string { return strings.ToLower(string(r)) }
