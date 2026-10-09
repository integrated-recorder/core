// Package controlplane contains the small runtime boundary used by one
// Control Plane generation. Business services remain owned by the generation;
// the stable Runtime Host can only ask it to become active, fence mutations,
// resume after a rolled-back handoff, or drain.
package controlplane

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/runtimeipc"
)

const (
	OperationControlReady             = "control_ready"
	OperationControlPrepareActivation = "control_prepare_activation"
	OperationControlActivate          = "control_activate"
	OperationControlPrepareHandoff    = "control_prepare_handoff"
	OperationControlResume            = "control_resume"
	OperationControlDeactivate        = "control_deactivate"
	OperationControlDetachEngine      = "control_detach_engine"
	OperationControlValidateInstall   = "control_validate_installation"
	OperationControlInstallReady      = "control_installation_ready"
	OperationControlAppendAudit       = "control_append_audit"
)

const maxDetachEngineRequestBytes = 512
const maxAuditAppendRequestBytes = 1024

// AuditAppendRequest is the bounded Runtime Host -> active Control bridge.
// The Runtime Host derives ActorType/UserID from its authenticated principal;
// this DTO is accepted only over the private, authenticated lifecycle IPC.
type AuditAppendRequest struct {
	Action    string `json:"action"`
	ObjectID  string `json:"object_id,omitempty"`
	ActorType string `json:"actor_type"`
	UserID    string `json:"user_id,omitempty"`
}

const (
	AuditRuntimeUpdateStaged        = "runtime_update_staged"
	AuditRuntimeUpdateActivated     = "runtime_update_activated"
	AuditRuntimeUpdateRolledBack    = "runtime_update_rolled_back"
	AuditAdapterReconciled          = "adapter_reconciled"
	AuditPluginRegistryRefreshed    = "plugin_registry_refreshed"
	AuditPluginInstalled            = "plugin_installed"
	AuditPluginUpdated              = "plugin_updated"
	AuditPluginUninstalled          = "plugin_uninstalled"
	AuditStorageInstanceCreated     = "storage_instance_created"
	AuditStorageInstanceConfigured  = "storage_instance_configured"
	AuditStorageInstanceProbed      = "storage_instance_probed"
	AuditStorageInstanceActivated   = "storage_instance_activated"
	AuditStorageProviderConfigured  = "storage_provider_configured"
	AuditStorageProviderProbed      = "storage_provider_probed"
	AuditStorageProviderActivated   = "storage_provider_activated"
	AuditInstallationSetupBegun     = "installation_setup_begun"
	AuditInstallationSetupCompleted = "installation_setup_completed"
)

var auditAppendActions = map[string]struct{}{
	AuditRuntimeUpdateStaged: {}, AuditRuntimeUpdateActivated: {}, AuditRuntimeUpdateRolledBack: {},
	AuditAdapterReconciled: {}, AuditPluginRegistryRefreshed: {}, AuditPluginInstalled: {},
	AuditPluginUpdated: {}, AuditPluginUninstalled: {}, AuditStorageInstanceCreated: {},
	AuditStorageInstanceConfigured: {}, AuditStorageInstanceProbed: {}, AuditStorageInstanceActivated: {},
	AuditStorageProviderConfigured: {}, AuditStorageProviderProbed: {}, AuditStorageProviderActivated: {},
	AuditInstallationSetupBegun: {}, AuditInstallationSetupCompleted: {},
}

// Validate rejects unbounded or arbitrary audit content before it crosses
// lifecycle IPC. Object IDs are opaque identifiers only; paths and payloads
// are never accepted.
func (r AuditAppendRequest) Validate() error {
	if _, ok := auditAppendActions[r.Action]; !ok || len(r.Action) > 64 || !validAuditIdentifier(r.ObjectID, 256) {
		return errors.New("invalid audit append request")
	}
	switch r.ActorType {
	case "system":
		if r.UserID != "" {
			return errors.New("invalid system audit actor")
		}
	case "user":
		if !validAuditUserID(r.UserID) {
			return errors.New("invalid user audit actor")
		}
	default:
		return errors.New("invalid audit actor type")
	}
	return nil
}

func validAuditUserID(value string) bool {
	if len(value) != 36 || value[:4] != "usr-" {
		return false
	}
	for _, character := range value[4:] {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validAuditIdentifier(value string, max int) bool {
	if value == "" {
		return true
	}
	if len(value) > max {
		return false
	}
	for index, character := range value {
		valid := character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || index > 0 && (character == '.' || character == '_' || character == ':' || character == '-')
		if !valid {
			return false
		}
	}
	return true
}

type LifecycleState string

const (
	LifecyclePassive             LifecycleState = "passive"
	LifecyclePreparingActivation LifecycleState = "preparing_activation"
	LifecycleActivating          LifecycleState = "activating"
	LifecycleActive              LifecycleState = "active"
	LifecyclePreparing           LifecycleState = "preparing_handoff"
	LifecycleDraining            LifecycleState = "draining"
	LifecycleDeactivated         LifecycleState = "deactivated"
	LifecycleFailed              LifecycleState = "failed"
)

type LifecycleSnapshot struct {
	Ready        bool           `json:"ready"`
	Prepared     bool           `json:"prepared"`
	Active       bool           `json:"active"`
	State        LifecycleState `json:"state"`
	GenerationID string         `json:"generation_id"`
	InstanceID   string         `json:"instance_id"`
	StartedAt    time.Time      `json:"started_at"`
}

// LifecycleHooks describe the reversible part of a Control handoff and the
// final process shutdown. Start must start background loops and return;
// Prepare must fence their admission and join admitted Watch checks; Resume
// reverses a successful Prepare when the Host aborts activation; Drain closes
// all Control-owned services permanently.
type LifecycleHooks struct {
	// PrepareActivation constructs and validates generation-owned services after
	// the previous active Control has fenced its writers. It must not start the
	// generation's background loops or activate durable mutation admission.
	PrepareActivation func(context.Context) error
	Start             func(context.Context) error
	Prepare           func(context.Context) error
	Resume            func(context.Context) error
	Drain             func(context.Context) error
	// DetachEngine removes a fully drained, non-default Engine from the active
	// Control's manager router. Host lease accounting must establish that the
	// Engine is safe to retire before invoking this operation.
	DetachEngine         func(context.Context, string) error
	ValidateInstallation func(context.Context) error
	InstallationReady    func(context.Context) error
	AppendAudit          func(context.Context, AuditAppendRequest) error
}

// Lifecycle gates a Control generation's management background work. The
// operation mutex serializes Host commands so activation, fencing, rollback,
// and final drain cannot race each other.
type Lifecycle struct {
	opMu       sync.Mutex
	mu         sync.Mutex
	generation string
	instance   string
	startedAt  time.Time
	state      LifecycleState
	prepared   bool
	hooks      LifecycleHooks
}

// NewLifecycle preserves the original small constructor for package-local
// users. Its handoff operations are no-ops; production uses NewLifecycleWithHooks.
func NewLifecycle(generation string, ready bool, start func(context.Context) error, drain func(context.Context) error) (*Lifecycle, error) {
	return NewLifecycleWithHooks(generation, ready, LifecycleHooks{
		Start: start, Prepare: func(context.Context) error { return nil },
		Resume: func(context.Context) error { return nil }, Drain: drain,
	})
}

func NewLifecycleWithHooks(generation string, ready bool, hooks LifecycleHooks) (*Lifecycle, error) {
	if generation == "" || !ready || hooks.Start == nil || hooks.Prepare == nil || hooks.Resume == nil || hooks.Drain == nil {
		return nil, errors.New("ready control lifecycle callbacks and generation are required")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, errors.New("control runtime instance identity is unavailable")
	}
	// Older embedders with no preparation hook have no separately constructed
	// application state to prepare. Treat that no-op boundary as already ready
	// while production Control generations provide the explicit hook.
	prepared := hooks.PrepareActivation == nil
	if hooks.PrepareActivation == nil {
		hooks.PrepareActivation = func(context.Context) error { return nil }
	}
	if hooks.DetachEngine == nil {
		hooks.DetachEngine = func(context.Context, string) error {
			return errors.New("engine detach is unavailable")
		}
	}
	return &Lifecycle{generation: generation, instance: hex.EncodeToString(random[:]), startedAt: time.Now().UTC(), state: LifecyclePassive, prepared: prepared, hooks: hooks}, nil
}

func (l *Lifecycle) RuntimeInstanceID() string { return l.instance }
func (l *Lifecycle) GenerationID() string      { return l.generation }

func (l *Lifecycle) Snapshot() LifecycleSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return LifecycleSnapshot{Ready: l.state != LifecycleFailed, Prepared: l.prepared, Active: l.state == LifecycleActive, State: l.state, GenerationID: l.generation, InstanceID: l.instance, StartedAt: l.startedAt}
}

func (l *Lifecycle) Handle(ctx context.Context, operation string, payload json.RawMessage) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if operation == OperationControlDetachEngine {
		request, err := decodeDetachEngineRequest(payload)
		if err != nil {
			return nil, err
		}
		if err := l.detachEngine(ctx, request.GenerationID); err != nil {
			return nil, err
		}
		return l.Snapshot(), nil
	}
	if operation == OperationControlValidateInstall {
		if len(payload) != 0 && string(payload) != "null" && string(payload) != "{}" {
			return nil, controlError("invalid_request", "installation validation request must be empty")
		}
		if err := l.validateInstallation(ctx); err != nil {
			return nil, err
		}
		return l.Snapshot(), nil
	}
	if operation == OperationControlInstallReady {
		if len(payload) != 0 && string(payload) != "null" && string(payload) != "{}" {
			return nil, controlError("invalid_request", "installation readiness request must be empty")
		}
		if err := l.installationReady(ctx); err != nil {
			return nil, err
		}
		return l.Snapshot(), nil
	}
	if operation == OperationControlAppendAudit {
		request, err := decodeAuditAppendRequest(payload)
		if err != nil {
			return nil, err
		}
		if err := l.appendAudit(ctx, request); err != nil {
			return nil, err
		}
		return l.Snapshot(), nil
	}
	if len(payload) != 0 && string(payload) != "null" && string(payload) != "{}" {
		return nil, controlError("invalid_request", "control lifecycle request must be empty")
	}
	switch operation {
	case OperationControlReady:
		return l.Snapshot(), nil
	case OperationControlPrepareActivation:
		if err := l.prepareActivation(ctx); err != nil {
			return nil, err
		}
	case OperationControlActivate:
		if err := l.activate(ctx); err != nil {
			return nil, err
		}
	case OperationControlPrepareHandoff:
		if err := l.prepare(ctx); err != nil {
			return nil, err
		}
	case OperationControlResume:
		if err := l.resume(ctx); err != nil {
			return nil, err
		}
	case OperationControlDeactivate:
		if err := l.deactivate(ctx); err != nil {
			return nil, err
		}
	default:
		return nil, controlError("unsupported_operation", "control lifecycle operation is unsupported")
	}
	return l.Snapshot(), nil
}

func decodeAuditAppendRequest(payload json.RawMessage) (AuditAppendRequest, error) {
	var request AuditAppendRequest
	if len(payload) == 0 || len(payload) > maxAuditAppendRequestBytes {
		return request, controlError("invalid_request", "audit request is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return AuditAppendRequest{}, controlError("invalid_request", "audit request is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return AuditAppendRequest{}, controlError("invalid_request", "audit request is invalid")
	}
	if request.Validate() != nil {
		return AuditAppendRequest{}, controlError("invalid_request", "audit request is invalid")
	}
	return request, nil
}

func (l *Lifecycle) appendAudit(ctx context.Context, request AuditAppendRequest) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return controlError("invalid_request", "audit request is invalid")
	}
	l.mu.Lock()
	active := l.state == LifecycleActive
	hook := l.hooks.AppendAudit
	l.mu.Unlock()
	if !active {
		return controlError("invalid_state", "Control generation is not active")
	}
	if hook == nil {
		return controlError("audit_unavailable", "audit append is unavailable")
	}
	if err := hook(ctx, request); err != nil {
		return controlError("audit_append_failed", "audit event could not be recorded")
	}
	return nil
}

func (l *Lifecycle) validateInstallation(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	active := l.state == LifecycleActive
	hook := l.hooks.ValidateInstallation
	l.mu.Unlock()
	if !active {
		return controlError("invalid_state", "Control generation is not active")
	}
	if hook == nil {
		return nil
	}
	if err := hook(ctx); err != nil {
		return controlError("installation_validation_failed", "Control installation validation failed")
	}
	return nil
}

func (l *Lifecycle) installationReady(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	active := l.state == LifecycleActive
	hook := l.hooks.InstallationReady
	l.mu.Unlock()
	if !active {
		return controlError("invalid_state", "Control generation is not active")
	}
	if hook == nil {
		return nil
	}
	if err := hook(ctx); err != nil {
		return controlError("installation_activation_failed", "Control background services could not be activated")
	}
	return nil
}

type detachEngineRequest struct {
	GenerationID string `json:"generation_id"`
}

func decodeDetachEngineRequest(payload json.RawMessage) (detachEngineRequest, error) {
	var request detachEngineRequest
	if len(payload) == 0 || len(payload) > maxDetachEngineRequestBytes {
		return request, controlError("invalid_request", "engine detach request is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return detachEngineRequest{}, controlError("invalid_request", "engine detach request is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return detachEngineRequest{}, controlError("invalid_request", "engine detach request is invalid")
	}
	if !runtimeIdentity.MatchString(request.GenerationID) {
		return detachEngineRequest{}, controlError("invalid_request", "engine detach generation identity is invalid")
	}
	return request, nil
}

func (l *Lifecycle) detachEngine(ctx context.Context, generationID string) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.state != LifecycleActive {
		l.mu.Unlock()
		return controlError("invalid_state", "control generation is not active")
	}
	l.mu.Unlock()
	if err := l.hooks.DetachEngine(ctx, generationID); err != nil {
		return controlError("engine_detach_failed", "recorder engine generation could not be detached")
	}
	return nil
}

func (l *Lifecycle) prepareActivation(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.state != LifecyclePassive {
		l.mu.Unlock()
		return controlError("invalid_state", "control generation cannot be prepared in its current state")
	}
	if l.prepared {
		l.mu.Unlock()
		return nil
	}
	l.state = LifecyclePreparingActivation
	l.mu.Unlock()
	if err := l.hooks.PrepareActivation(ctx); err != nil {
		l.mu.Lock()
		l.state = LifecycleFailed
		l.mu.Unlock()
		return controlError("preparation_failed", "control generation could not prepare its application services")
	}
	if err := ctx.Err(); err != nil {
		l.mu.Lock()
		l.state = LifecycleFailed
		l.mu.Unlock()
		return controlError("preparation_failed", "control generation could not prepare its application services")
	}
	l.mu.Lock()
	l.prepared = true
	l.state = LifecyclePassive
	l.mu.Unlock()
	return nil
}

func (l *Lifecycle) activate(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.state != LifecyclePassive || !l.prepared {
		l.mu.Unlock()
		return controlError("invalid_state", "control generation is not prepared for activation")
	}
	l.state = LifecycleActivating
	l.mu.Unlock()
	if err := l.hooks.Start(ctx); err != nil {
		l.mu.Lock()
		l.state = LifecycleFailed
		l.prepared = false
		l.mu.Unlock()
		return controlError("activation_failed", "control generation could not start background services")
	}
	l.mu.Lock()
	l.state = LifecycleActive
	l.mu.Unlock()
	return nil
}

func (l *Lifecycle) prepare(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.state != LifecycleActive {
		l.mu.Unlock()
		return controlError("invalid_state", "control generation is not active")
	}
	l.mu.Unlock()
	if err := l.hooks.Prepare(ctx); err != nil {
		return controlError("handoff_prepare_failed", "control generation could not prepare for handoff")
	}
	l.mu.Lock()
	l.state = LifecyclePreparing
	l.mu.Unlock()
	return nil
}

func (l *Lifecycle) resume(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.state != LifecyclePreparing {
		l.mu.Unlock()
		return controlError("invalid_state", "control generation is not waiting for handoff rollback")
	}
	l.mu.Unlock()
	if err := l.hooks.Resume(ctx); err != nil {
		return controlError("resume_failed", "control generation could not resume after handoff rollback")
	}
	l.mu.Lock()
	l.state = LifecycleActive
	l.mu.Unlock()
	return nil
}

func (l *Lifecycle) deactivate(ctx context.Context) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.state == LifecycleDeactivated {
		l.mu.Unlock()
		return nil
	}
	if l.state == LifecycleActivating {
		l.mu.Unlock()
		return controlError("invalid_state", "control generation cannot be deactivated in its current state")
	}
	previous := l.state
	l.state = LifecycleDraining
	l.mu.Unlock()
	if previous == LifecycleActive {
		// The final drain uses the same admission fence as a handoff. Prepare is
		// intentionally best-effort here because Drain still owns final cleanup.
		_ = l.hooks.Prepare(ctx)
	}
	if err := l.hooks.Drain(ctx); err != nil {
		l.mu.Lock()
		l.state = LifecycleDraining
		l.mu.Unlock()
		return controlError("drain_timeout", "control generation did not finish draining")
	}
	l.mu.Lock()
	l.state = LifecycleDeactivated
	l.prepared = false
	l.mu.Unlock()
	return nil
}

type controlOperationError struct{ code, message string }

func (e controlOperationError) Error() string                    { return e.message }
func (e controlOperationError) PublicIPCError() (string, string) { return e.code, e.message }

func controlError(code, message string) error {
	return controlOperationError{code: code, message: message}
}

var _ runtimeipc.Handler = (*Lifecycle)(nil)
