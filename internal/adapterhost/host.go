// Package adapterhost discovers and supervises standalone adapter binaries.
// Only explicit adapter directories are scanned; PATH is never searched.
package adapterhost

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/interaction"
	"github.com/integrated-recorder/core/internal/pluginconfig"
)

const binaryPrefix = "integrated-recorder-adapter-"
const maxWorkflowTransitions = 32
const maxActiveWorkflows = 128
const workflowTTL = 30 * time.Minute
const maxPendingWorkflowLifecycleEvents = 512

var (
	errAdapterDescriptorInvalid = errors.New("invalid adapter descriptor")
	// ErrUnsupportedResourceBrowse indicates that an adapter did not declare
	// the generic resource browsing capability.
	ErrUnsupportedResourceBrowse = errors.New("adapter does not support resource browsing")
)

const (
	defaultResourcePageLimit = 20
	maxResourcePageLimit     = 50
	maxResourceCursorBytes   = 4096
	maxResourceQueryBytes    = 1024
)

type Status struct {
	ID              string `json:"id"`
	Name            string `json:"name,omitempty"`
	Version         string `json:"version,omitempty"`
	ProtocolVersion int    `json:"protocol_version,omitempty"`
	State           string `json:"state"`
	Error           string `json:"error,omitempty"`
	Generation      uint64 `json:"generation,omitempty"`
	RestartAttempts int    `json:"restart_attempts,omitempty"`
}

type Adapter struct {
	Descriptor *adapterproto.Descriptor `json:"descriptor,omitempty"`
	Status     Status                   `json:"status"`
}

type entry struct {
	opMu            sync.Mutex
	stateMu         sync.Mutex
	descriptor      *adapterproto.Descriptor
	process         *process
	status          Status
	path            string
	nextRestart     time.Time
	baseFingerprint string
	restartRejected bool
	disabled        bool
	restarting      bool
}

type workflowSession struct {
	adapterID      string
	workflowID     string
	resource       *adapterproto.ResourceRef
	progress       WorkflowProgress
	cancel         context.CancelFunc
	generation     uint64
	createdAt      time.Time
	updatedAt      time.Time
	transitions    int
	continuing     bool
	continuationID uint64
}

// WorkflowLifecycleChallenge is a redacted, structural challenge projection.
// It never contains adapter-provided prompt text, schema defaults, or answers.
type WorkflowLifecycleChallenge struct {
	PromptType      string `json:"prompt_type,omitempty"`
	FieldCount      int    `json:"field_count"`
	HasSecretFields bool   `json:"has_secret_fields"`
}

// WorkflowLifecycleEvent reports an active workflow's terminal transition.
// Media sources and adapter errors are intentionally not represented.
type WorkflowLifecycleEvent struct {
	ID         string                      `json:"id"`
	WorkflowID string                      `json:"workflow_id"`
	AdapterID  string                      `json:"adapter_id"`
	State      string                      `json:"state"`
	At         time.Time                   `json:"at"`
	Resource   *adapterproto.ResourceRef   `json:"resource,omitempty"`
	Challenge  *WorkflowLifecycleChallenge `json:"challenge,omitempty"`
}

type WorkflowProgress struct {
	WorkflowID string                          `json:"workflow_id"`
	AdapterID  string                          `json:"adapter_id"`
	State      string                          `json:"state"`
	Resource   *adapterproto.ResourceRef       `json:"resource,omitempty"`
	Challenge  *adapterproto.WorkflowChallenge `json:"challenge,omitempty"`
	Media      *adapterproto.MediaSource       `json:"media,omitempty"`
	Provenance adapterproto.AdapterProvenance  `json:"adapter,omitempty"`
}

type Host struct {
	mu             sync.RWMutex
	entries        map[string]*entry
	configs        *pluginconfig.Service
	state          *pluginconfig.StateService
	interactions   *interaction.Tracker
	workflows      map[string]workflowSession
	workflowCall   uint64
	workflowStarts int
	lifecycleSeq   uint64
	lifecycleQueue []WorkflowLifecycleEvent
	closed         bool
}

func Discover(ctx context.Context, dir string, configs *pluginconfig.Service) (*Host, error) {
	return DiscoverWithState(ctx, dir, configs, nil)
}

func DiscoverWithState(ctx context.Context, dir string, configs *pluginconfig.Service, state *pluginconfig.StateService) (*Host, error) {
	h := &Host{entries: map[string]*entry{}, configs: configs, state: state, interactions: interaction.NewTracker(), workflows: map[string]workflowSession{}}
	if strings.TrimSpace(dir) == "" {
		return h, nil
	}
	files, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return h, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read adapter directory: %w", err)
	}
	paths := []string{}
	for _, item := range files {
		if !strings.HasPrefix(item.Name(), binaryPrefix) {
			continue
		}
		info, infoErr := item.Info()
		if infoErr != nil {
			continue
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			continue
		}
		paths = append(paths, filepath.Join(dir, item.Name()))
	}
	sort.Strings(paths)
	for _, path := range paths {
		candidateID := strings.TrimPrefix(filepath.Base(path), binaryPrefix)
		p, startErr := startProcess(path)
		if startErr != nil {
			h.entries["candidate:"+candidateID] = &entry{status: Status{ID: candidateID, State: "failed", Error: "adapter process could not start"}, path: path}
			continue
		}
		d, callErr := describeProcess(ctx, p)
		if callErr != nil {
			p.kill()
			h.entries["candidate:"+candidateID] = &entry{process: p, status: Status{ID: candidateID, State: "failed", Error: "adapter describe handshake failed"}, path: path}
			continue
		}
		fingerprint := descriptorFingerprint(d)
		if existing, ok := h.entries[d.ID]; ok {
			if existing.process != nil {
				existing.process.kill()
			}
			p.kill()
			existing.status = Status{ID: d.ID, State: "rejected", Error: "duplicate adapter id"}
			existing.process = nil
			existing.descriptor = nil
			continue
		}
		copy := d
		h.entries[d.ID] = &entry{descriptor: &copy, process: p, path: path, baseFingerprint: fingerprint, status: Status{ID: d.ID, Name: d.Name, Version: d.Version, ProtocolVersion: d.ProtocolVersion, State: "ready", Generation: 1}}
	}
	return h, nil
}

// DiscoverDirs scans only the explicit directories supplied by the caller.
// Directory and binary ordering are deterministic, and duplicate descriptor
// identities are rejected across the entire configured set.
func DiscoverDirs(ctx context.Context, dirs []string, configs *pluginconfig.Service, states ...*pluginconfig.StateService) (*Host, error) {
	var state *pluginconfig.StateService
	if len(states) > 0 {
		state = states[0]
	}
	ordered := append([]string(nil), dirs...)
	sort.Strings(ordered)
	h := &Host{entries: map[string]*entry{}, configs: configs, state: state, interactions: interaction.NewTracker(), workflows: map[string]workflowSession{}}
	for _, dir := range ordered {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		child, err := DiscoverWithState(ctx, dir, configs, state)
		if err != nil {
			h.Close()
			return nil, err
		}
		for key, candidate := range child.entries {
			if existing, duplicate := h.entries[key]; duplicate && !strings.HasPrefix(key, "candidate:") {
				if existing.process != nil {
					existing.process.kill()
				}
				if candidate.process != nil {
					candidate.process.kill()
				}
				existing.process = nil
				existing.descriptor = nil
				existing.status = Status{ID: key, State: "rejected", Error: "duplicate adapter id"}
				continue
			}
			if _, exists := h.entries[key]; exists {
				key = "candidate:" + filepath.Clean(candidate.path)
			}
			h.entries[key] = candidate
		}
		child.entries = map[string]*entry{}
		child.Close()
	}
	return h, nil
}

func describeProcess(ctx context.Context, p *process) (adapterproto.Descriptor, error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := p.call(callCtx, adapterproto.MethodDescribe, map[string]any{})
	if err != nil {
		return adapterproto.Descriptor{}, err
	}
	var d adapterproto.Descriptor
	if json.Unmarshal(result, &d) != nil {
		return adapterproto.Descriptor{}, errAdapterDescriptorInvalid
	}
	if d.ProtocolVersion != adapterproto.Version {
		return adapterproto.Descriptor{}, fmt.Errorf("%w: descriptor declares unsupported version", adapterproto.ErrUnsupportedProtocolVersion)
	}
	if err := d.Validate(); err != nil {
		return adapterproto.Descriptor{}, fmt.Errorf("%w: %v", errAdapterDescriptorInvalid, err)
	}
	return d, nil
}

func descriptorFingerprint(d adapterproto.Descriptor) string {
	// Branding is presentation metadata and may change independently from an
	// adapter's protocol semantics. Keep the historical descriptor hash byte
	// for byte identical when branding is absent.
	d.Branding = nil
	data, _ := json.Marshal(d)
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if decoder.Decode(&value) == nil {
		if canonical, err := json.Marshal(value); err == nil {
			data = canonical
		}
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func refresh(e *entry) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.process != nil && e.process.isUnusable() && e.status.State == "ready" {
		e.status.State = "unavailable"
		e.status.Error = "adapter process needs restart"
	}
}

func (h *Host) List() []Adapter {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Adapter, 0, len(h.entries))
	for _, e := range h.entries {
		refresh(e)
		e.stateMu.Lock()
		item := Adapter{Status: e.status}
		if e.descriptor != nil {
			d := cloneDescriptor(*e.descriptor)
			item.Descriptor = &d
		}
		e.stateMu.Unlock()
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Status.ID < out[j].Status.ID })
	return out
}

func (h *Host) Get(id string) (Adapter, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.entries[id]
	if !ok {
		for _, candidate := range h.entries {
			if candidate.status.ID == id && candidate.descriptor == nil {
				e, ok = candidate, true
				break
			}
		}
	}
	if !ok {
		return Adapter{}, fmt.Errorf("adapter not found")
	}
	refresh(e)
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	out := Adapter{Status: e.status}
	if e.descriptor != nil {
		d := cloneDescriptor(*e.descriptor)
		out.Descriptor = &d
	}
	return out, nil
}

func cloneDescriptor(descriptor adapterproto.Descriptor) adapterproto.Descriptor {
	if descriptor.Branding != nil {
		branding := *descriptor.Branding
		if branding.Icon != nil {
			icon := *branding.Icon
			icon.Data = append([]byte(nil), icon.Data...)
			branding.Icon = &icon
		}
		descriptor.Branding = &branding
	}
	return descriptor
}

func (h *Host) Descriptor(id string) (adapterproto.Descriptor, error) {
	a, err := h.Get(id)
	if err != nil {
		return adapterproto.Descriptor{}, err
	}
	if a.Descriptor == nil {
		return adapterproto.Descriptor{}, fmt.Errorf("adapter is unavailable")
	}
	return *a.Descriptor, nil
}

func (h *Host) Schema(id string, resource *adapterproto.ResourceRef) (adapterproto.Schema, error) {
	chain, err := h.schemaChain(id, resource)
	if err != nil {
		return adapterproto.Schema{}, err
	}
	return mergeSchemas(chain), nil
}

// ValidateResource checks a structurally opaque resource chain against the
// adapter's declared resource graph.
func (h *Host) ValidateResource(id string, ref *adapterproto.ResourceRef) error {
	d, err := h.Descriptor(id)
	if err != nil {
		return err
	}
	if err := adapterproto.ValidateResourceRefForDescriptor(d, ref); err != nil {
		return fmt.Errorf("invalid resource reference")
	}
	return nil
}

// BrowseResources invokes the adapter's generic resource listing or search
// operation. Resource identifiers and attributes are passed through without
// interpreting their platform-defined meaning.
func (h *Host) BrowseResources(ctx context.Context, adapterID string, parent *adapterproto.ResourceRef, resourceType, query, cursor string, limit int) (adapterproto.ResourcePage, error) {
	descriptor, err := h.Descriptor(adapterID)
	if err != nil {
		return adapterproto.ResourcePage{}, err
	}
	if !hasCapability(descriptor, adapterproto.CapabilityResourceBrowse) {
		return adapterproto.ResourcePage{}, ErrUnsupportedResourceBrowse
	}
	if parent != nil {
		if err := adapterproto.ValidateResourceRefForDescriptor(descriptor, parent); err != nil {
			return adapterproto.ResourcePage{}, fmt.Errorf("invalid resource parent")
		}
	}
	if resourceType != "" {
		if len(resourceType) > 64 || !declaresResourceType(descriptor, resourceType) {
			return adapterproto.ResourcePage{}, fmt.Errorf("invalid resource type")
		}
	}
	if len(cursor) > maxResourceCursorBytes {
		return adapterproto.ResourcePage{}, fmt.Errorf("resource cursor exceeds limit")
	}
	if len(query) > maxResourceQueryBytes {
		return adapterproto.ResourcePage{}, fmt.Errorf("resource query exceeds limit")
	}
	if limit < 0 {
		return adapterproto.ResourcePage{}, fmt.Errorf("resource page limit is invalid")
	}
	if limit == 0 {
		limit = defaultResourcePageLimit
	} else if limit > maxResourcePageLimit {
		limit = maxResourcePageLimit
	}

	method := adapterproto.MethodResourceList
	var params any = adapterproto.ResourceListParams{Parent: cloneResourceRef(parent), ResourceType: resourceType, Cursor: cursor, Limit: limit}
	if query != "" {
		method = adapterproto.MethodResourceSearch
		params = adapterproto.ResourceSearchParams{Parent: cloneResourceRef(parent), ResourceType: resourceType, Query: query, Cursor: cursor, Limit: limit}
	}
	result, err := h.call(ctx, adapterID, method, params)
	if err != nil {
		return adapterproto.ResourcePage{}, err
	}
	var page adapterproto.ResourcePage
	if err := json.Unmarshal(result, &page); err != nil || page.Items == nil {
		return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned an invalid resource page")
	}
	if len(page.Items) > limit || len(page.NextCursor) > maxResourceCursorBytes {
		return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned an invalid resource page")
	}
	seen := make(map[string]struct{}, len(page.Items))
	for _, item := range page.Items {
		if err := adapterproto.ValidateResourceRefForDescriptor(descriptor, &item.ResourceRef); err != nil {
			return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned an invalid resource reference")
		}
		if !sameResourceRef(item.Parent, parent) {
			return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned a resource outside the requested parent")
		}
		if resourceType != "" && item.Type != resourceType {
			return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned a resource outside the requested type")
		}
		key, marshalErr := json.Marshal(item.ResourceRef)
		if marshalErr != nil {
			return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned an invalid resource reference")
		}
		if _, duplicate := seen[string(key)]; duplicate {
			return adapterproto.ResourcePage{}, fmt.Errorf("adapter returned duplicate resources")
		}
		seen[string(key)] = struct{}{}
	}
	return page, nil
}

func declaresResourceType(descriptor adapterproto.Descriptor, resourceType string) bool {
	for _, declared := range descriptor.ResourceTypes {
		if declared.Type == resourceType {
			return true
		}
	}
	return false
}

func sameResourceRef(left, right *adapterproto.ResourceRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Type == right.Type && left.ID == right.ID && sameResourceRef(left.Parent, right.Parent)
}

func (h *Host) schemaChain(id string, resource *adapterproto.ResourceRef) ([]pluginconfig.ScopeSchema, error) {
	if err := h.ValidateResource(id, resource); err != nil {
		return nil, err
	}
	scopes, err := pluginconfig.ResourceScopes(id, resource)
	if err != nil {
		return nil, err
	}
	d, err := h.Descriptor(id)
	if err != nil {
		return nil, err
	}
	chain := make([]pluginconfig.ScopeSchema, 0, len(scopes))
	merged := []adapterproto.Field{}
	for _, scope := range scopes {
		schema := d.ConfigurationSchema
		if scope.Resource != nil {
			schema = adapterproto.Schema{}
			for _, declared := range d.ResourceTypes {
				if declared.Type == scope.Resource.Type {
					schema = declared.ConfigurationSchema
					break
				}
			}
		}
		merged = mergeFieldSlice(merged, schema.Fields)
		chain = append(chain, pluginconfig.ScopeSchema{Scope: scope, Schema: adapterproto.Schema{Fields: append([]adapterproto.Field(nil), merged...)}})
	}
	return chain, nil
}

func mergeFieldSlice(existing, incoming []adapterproto.Field) []adapterproto.Field {
	fields := map[string]adapterproto.Field{}
	order := []string{}
	for _, field := range existing {
		if _, ok := fields[field.Key]; !ok {
			order = append(order, field.Key)
		}
		fields[field.Key] = field
	}
	for _, field := range incoming {
		if _, ok := fields[field.Key]; !ok {
			order = append(order, field.Key)
		}
		fields[field.Key] = field
	}
	out := make([]adapterproto.Field, 0, len(fields))
	for _, key := range order {
		out = append(out, fields[key])
	}
	return out
}

func mergeSchemas(chain []pluginconfig.ScopeSchema) adapterproto.Schema {
	fields := map[string]adapterproto.Field{}
	order := []string{}
	for _, item := range chain {
		for _, field := range item.Schema.Fields {
			if _, ok := fields[field.Key]; !ok {
				order = append(order, field.Key)
			}
			fields[field.Key] = field
		}
	}
	out := adapterproto.Schema{Fields: make([]adapterproto.Field, 0, len(fields))}
	for _, key := range order {
		out.Fields = append(out.Fields, fields[key])
	}
	return out
}

func (h *Host) effective(id string, resource *adapterproto.ResourceRef) (pluginconfig.EffectiveDocument, error) {
	chain, err := h.schemaChain(id, resource)
	if err != nil {
		return pluginconfig.EffectiveDocument{}, err
	}
	if h.configs == nil {
		return pluginconfig.EffectiveDocument{Stored: pluginconfig.Document{Values: map[string]json.RawMessage{}, Secrets: map[string]string{}}, EffectiveValues: map[string]json.RawMessage{}, EffectiveSecrets: map[string]string{}, EffectiveSecretConfigured: map[string]bool{}, ValueSources: map[string]string{}, SecretSources: map[string]string{}}, nil
	}
	return h.configs.Effective(chain)
}

func (h *Host) ConfigSnapshot(id string, resource *adapterproto.ResourceRef) (pluginconfig.Snapshot, error) {
	effective, err := h.effective(id, resource)
	if err != nil {
		return pluginconfig.Snapshot{}, err
	}
	return effective.Snapshot(), nil
}

func (h *Host) Resolve(ctx context.Context, id string, input json.RawMessage, resource *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	if hasCapability(d, adapterproto.CapabilityResolveWorkflow) {
		progress, beginErr := h.BeginResolution(ctx, id, input, resource)
		if beginErr != nil {
			return adapterproto.MediaSource{}, beginErr
		}
		if progress.State != "resolved" || progress.Media == nil {
			return adapterproto.MediaSource{}, fmt.Errorf("adapter resolution requires additional interaction")
		}
		return *progress.Media, nil
	}
	return h.ResolveLegacy(ctx, id, input, resource)
}

func hasCapability(d adapterproto.Descriptor, capability string) bool {
	for _, item := range d.Capabilities {
		if item == capability {
			return true
		}
	}
	return false
}

func (h *Host) entryFor(id string) (*entry, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, ErrAdapterHostClosed
	}
	e, ok := h.entries[id]
	if !ok || e.descriptor == nil {
		return nil, ErrAdapterUnavailable
	}
	e.stateMu.Lock()
	disabled, restarting := e.disabled, e.restarting
	e.stateMu.Unlock()
	if disabled {
		return nil, ErrAdapterDisabled
	}
	if restarting {
		return nil, ErrAdapterBusy
	}
	return e, nil
}

func (h *Host) ensureProcess(ctx context.Context, e *entry) (*process, error) {
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("adapter host is closed")
	}
	e.stateMu.Lock()
	if e.disabled {
		e.stateMu.Unlock()
		return nil, ErrAdapterDisabled
	}
	if e.restarting {
		e.stateMu.Unlock()
		return nil, ErrAdapterBusy
	}
	if e.restartRejected {
		e.stateMu.Unlock()
		return nil, ErrAdapterRestartRejected
	}
	if e.process != nil && !e.process.isUnusable() {
		p := e.process
		if e.status.RestartAttempts > 0 && time.Since(p.startedAt) >= processStableUptime {
			e.status.RestartAttempts = 0
		}
		e.stateMu.Unlock()
		return p, nil
	}
	if e.process != nil {
		p := e.process
		e.process = nil
		e.status.State = "unavailable"
		e.status.Error = "adapter process needs restart"
		if e.status.RestartAttempts == 0 {
			// Preserve immediate recovery for the first observed death. Once a
			// replacement has been attempted, subsequent short-lived generations
			// use the bounded exponential backoff.
			e.nextRestart = time.Now()
		} else {
			scheduleRestartLocked(e, p, time.Now())
		}
	}
	now := time.Now()
	if !e.nextRestart.IsZero() && now.Before(e.nextRestart) {
		e.stateMu.Unlock()
		return nil, fmt.Errorf("adapter restart is backing off")
	}
	// The previous deadline has been served. Clear it before attempting the
	// replacement so a failed spawn/handshake can schedule the next backoff.
	e.nextRestart = time.Time{}
	e.status.RestartAttempts++
	e.stateMu.Unlock()
	p, err := startProcess(e.path)
	descriptorMismatch := false
	var restartedDescriptor adapterproto.Descriptor
	if err == nil {
		restartedDescriptor, err = describeProcess(ctx, p)
		if err == nil && (restartedDescriptor.ID != e.descriptor.ID || restartedDescriptor.Version != e.descriptor.Version || restartedDescriptor.ProtocolVersion != e.descriptor.ProtocolVersion || descriptorFingerprint(restartedDescriptor) != e.baseFingerprint) {
			descriptorMismatch = true
			err = fmt.Errorf("adapter descriptor fingerprint changed")
		} else if errors.Is(err, adapterproto.ErrUnsupportedProtocolVersion) || errors.Is(err, errAdapterDescriptorInvalid) {
			descriptorMismatch = true
		}
	}
	if err != nil {
		if p != nil {
			p.kill()
		}
		e.stateMu.Lock()
		if descriptorMismatch {
			e.restartRejected = true
		}
		if !e.restartRejected {
			scheduleRestartLocked(e, nil, time.Now())
		}
		e.status.State = "unavailable"
		e.status.Error = "adapter restart failed validation"
		if e.restartRejected {
			e.status.State = "rejected"
		}
		e.stateMu.Unlock()
		return nil, fmt.Errorf("adapter process could not be restarted")
	}
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		p.kill()
		return nil, fmt.Errorf("adapter host is closed")
	}
	e.stateMu.Lock()
	e.process = p
	descriptorCopy := cloneDescriptor(restartedDescriptor)
	e.descriptor = &descriptorCopy
	e.nextRestart = time.Time{}
	e.status.State = "ready"
	e.status.Error = ""
	e.status.Generation++
	e.stateMu.Unlock()
	h.mu.RUnlock()
	return p, nil
}

func (h *Host) call(ctx context.Context, id, method string, params any) (json.RawMessage, error) {
	result, _, err := h.callGeneration(ctx, id, method, params, 0)
	return result, err
}

var errAdapterGenerationChanged = fmt.Errorf("workflow expired because adapter process restarted")

func (h *Host) callAtGeneration(ctx context.Context, id, method string, params any, expected uint64) (json.RawMessage, error) {
	result, _, err := h.callGeneration(ctx, id, method, params, expected)
	return result, err
}

func (h *Host) callGeneration(ctx context.Context, id, method string, params any, expected uint64) (json.RawMessage, uint64, error) {
	e, err := h.entryFor(id)
	if err != nil {
		return nil, 0, err
	}
	e.opMu.Lock()
	defer e.opMu.Unlock()
	p, err := h.ensureProcess(ctx, e)
	if err != nil {
		return nil, 0, err
	}
	e.stateMu.Lock()
	generation := e.status.Generation
	e.stateMu.Unlock()
	if expected != 0 && generation != expected {
		return nil, generation, errAdapterGenerationChanged
	}
	result, err := p.call(ctx, method, params)
	if p.isUnusable() {
		e.stateMu.Lock()
		defer e.stateMu.Unlock()
		if e.process == p {
			e.process = nil
			e.status.State = "unavailable"
			e.status.Error = "adapter process needs restart"
			scheduleRestartLocked(e, p, time.Now())
		}
	}
	return result, generation, err
}

const processStableUptime = 30 * time.Second

func restartBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 100 * time.Millisecond << min(attempt-1, 8)
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

// scheduleRestartLocked bounds replacement attempts across successive
// short-lived process generations. Attempts are cleared only after an
// adapter has remained usable for a stable interval.
func scheduleRestartLocked(e *entry, p *process, now time.Time) {
	if !e.nextRestart.IsZero() {
		return
	}
	if p != nil && !p.startedAt.IsZero() && now.Sub(p.startedAt) >= processStableUptime {
		e.status.RestartAttempts = 0
	}
	e.nextRestart = now.Add(restartBackoff(e.status.RestartAttempts + 1))
}

func newWorkflowID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
