package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/pluginconfig"
)

const hostCloseTimeout = 20 * time.Second

// ErrWorkflowFailed marks an adapter's explicit terminal workflow result.
// Callers should return a generic gateway error and never expose adapter data.
var ErrWorkflowFailed = errors.New("adapter workflow failed")

func provenance(d adapterproto.Descriptor) adapterproto.AdapterProvenance {
	return adapterproto.AdapterProvenance{ID: d.ID, Version: d.Version, ProtocolVersion: d.ProtocolVersion, Fingerprint: descriptorFingerprint(d)}
}

// Provenance returns the current semantic descriptor identity for a recording
// resolved by this adapter. Presentation metadata is excluded by the existing
// descriptor fingerprint implementation.
func (h *Host) Provenance(id string) (adapterproto.AdapterProvenance, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.AdapterProvenance{}, err
	}
	return provenance(d), nil
}

func (h *Host) BeginResolution(ctx context.Context, id string, input json.RawMessage, resource *adapterproto.ResourceRef) (WorkflowProgress, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return WorkflowProgress{}, err
	}
	if err = adapterproto.ValidateObjectAgainstSchema(d.InputSchema, input); err != nil {
		return WorkflowProgress{}, err
	}
	if err = adapterproto.ValidateResourceRefForDescriptor(d, resource); err != nil {
		return WorkflowProgress{}, fmt.Errorf("invalid resource reference")
	}
	input = marshalObject(adapterproto.ApplyDefaults(d.InputSchema, decodeObject(input)))
	wfID, err := newWorkflowID()
	if err != nil {
		return WorkflowProgress{}, err
	}
	if !hasCapability(d, adapterproto.CapabilityResolveWorkflow) {
		media, resolveErr := h.ResolveLegacy(ctx, id, input, resource)
		if resolveErr != nil {
			return WorkflowProgress{}, resolveErr
		}
		return WorkflowProgress{WorkflowID: wfID, AdapterID: id, State: "resolved", Resource: cloneResourceRef(resource), Media: &media, Provenance: provenance(d)}, nil
	}
	release, err := h.reserveWorkflow()
	if err != nil {
		return WorkflowProgress{}, err
	}
	defer release()
	effective, err := h.effective(id, resource)
	if err != nil {
		return WorkflowProgress{}, err
	}
	schema, err := h.Schema(id, resource)
	if err != nil {
		return WorkflowProgress{}, err
	}
	configuration, secrets, err := effectiveConfiguration(schema, effective, false)
	if err != nil {
		return WorkflowProgress{}, fmt.Errorf("effective adapter configuration is invalid: %w", err)
	}
	state, err := h.stateDocuments(id, resource)
	if err != nil {
		return WorkflowProgress{}, fmt.Errorf("adapter state is unavailable")
	}
	params := adapterproto.ResolveBeginParams{WorkflowID: wfID, Input: input, Resource: cloneResourceRef(resource), Configuration: configuration, Secrets: secrets, State: state}
	raw, generation, err := h.callGeneration(ctx, id, adapterproto.MethodResolveBegin, params, 0)
	if err != nil {
		return WorkflowProgress{}, err
	}
	var result adapterproto.ResolveWorkflowResult
	if json.Unmarshal(raw, &result) != nil {
		return WorkflowProgress{}, fmt.Errorf("adapter returned invalid workflow result")
	}
	if result.State == "error" {
		if result.WorkflowID != wfID {
			return WorkflowProgress{}, fmt.Errorf("adapter returned invalid workflow result")
		}
		eventResource := safeWorkflowResource(d, result.Resource, resource)
		h.recordWorkflowLifecycle(id, wfID, "failed", eventResource, result.Challenge)
		return WorkflowProgress{}, ErrWorkflowFailed
	}
	if err = h.validateWorkflowResult(d, result, wfID, resource); err != nil {
		return WorkflowProgress{}, err
	}
	session := workflowSession{adapterID: id, workflowID: wfID, resource: cloneResourceRef(resource), generation: generation, createdAt: time.Now().UTC(), updatedAt: time.Now().UTC()}
	return h.advanceWorkflow(ctx, d, session, result)
}

// ResolveLegacy runs the stateless resolve operation. V1 adapters may return a
// MediaSource directly; the extensible result envelope may accompany it with
// adapter-owned state mutations.
func (h *Host) ResolveLegacy(ctx context.Context, id string, input json.RawMessage, resource *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	// Preserve the legacy resolve contract: adapters may receive secret fields
	// inline in Input. Only Watch persistence requires a split secret map.
	if err = adapterproto.ValidateObjectAgainstSchema(d.InputSchema, input); err != nil {
		return adapterproto.MediaSource{}, err
	}
	return h.resolveLegacyValidated(ctx, id, input, resource, d)
}

// ResolveLegacyWithInputSecrets performs the same stateless composition as
// ResolveLegacy while accepting input-schema secret values separately from
// ordinary input. The secret fields are merged only in memory immediately
// before the adapter call.
func (h *Host) ResolveLegacyWithInputSecrets(ctx context.Context, id string, input json.RawMessage, inputSecrets map[string]string, resource *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	input, err = composeInputSecrets(d.InputSchema, input, inputSecrets)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	return h.resolveLegacyValidated(ctx, id, input, resource, d)
}

func (h *Host) resolveLegacyValidated(ctx context.Context, id string, input json.RawMessage, resource *adapterproto.ResourceRef, d adapterproto.Descriptor) (adapterproto.MediaSource, error) {
	if err := adapterproto.ValidateResourceRefForDescriptor(d, resource); err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("invalid resource reference")
	}
	input = marshalObject(adapterproto.ApplyDefaults(d.InputSchema, decodeObject(input)))
	effective, err := h.effective(id, resource)
	if err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("adapter configuration unavailable")
	}
	schema, err := h.Schema(id, resource)
	if err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("adapter configuration schema unavailable")
	}
	configuration, secrets, err := effectiveConfiguration(schema, effective, true)
	if err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("effective adapter configuration is invalid: %w", err)
	}
	state, err := h.stateDocuments(id, resource)
	if err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("adapter state is unavailable")
	}
	params := adapterproto.ResolveParams{Input: input, Resource: cloneResourceRef(resource), Configuration: configuration, Secrets: secrets, State: state}
	raw, err := h.call(ctx, id, adapterproto.MethodResolve, params)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	media, mutations, err := parseResolveResult(raw)
	if err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("adapter returned invalid media source")
	}
	if err = validateMediaForDescriptor(d, media); err != nil {
		return adapterproto.MediaSource{}, err
	}
	if err = h.applyStateMutations(id, resource, mutations); err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("adapter state could not be saved")
	}
	return media, nil
}

// ValidateWatchInput validates a Watch's split ordinary/secret input without
// returning or persisting a combined object containing secret values.
func (h *Host) ValidateWatchInput(id string, input json.RawMessage, inputSecrets map[string]string, resource *adapterproto.ResourceRef) error {
	d, err := h.Descriptor(id)
	if err != nil {
		return err
	}
	if err := adapterproto.ValidateResourceRefForDescriptor(d, resource); err != nil {
		return fmt.Errorf("invalid resource reference")
	}
	_, err = composeInputSecrets(d.InputSchema, input, inputSecrets)
	return err
}

// WatchCheck runs one generic adapter-defined observation. Adapter configuration
// inheritance, secrets, and state use the same composition path as resolve.
// Watch input secrets are merged into Input only for the adapter call.
func (h *Host) WatchCheck(ctx context.Context, id string, input json.RawMessage, inputSecrets map[string]string, resource *adapterproto.ResourceRef) (adapterproto.WatchCheckResult, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.WatchCheckResult{}, err
	}
	if !hasCapability(d, adapterproto.CapabilityWatch) {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("adapter does not support watch checks")
	}
	if err := adapterproto.ValidateResourceRefForDescriptor(d, resource); err != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("invalid resource reference")
	}
	composedInput, err := composeInputSecrets(d.InputSchema, input, inputSecrets)
	if err != nil {
		return adapterproto.WatchCheckResult{}, err
	}
	effective, err := h.effective(id, resource)
	if err != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("adapter configuration unavailable")
	}
	schema, err := h.Schema(id, resource)
	if err != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("adapter configuration schema unavailable")
	}
	configuration, configSecrets, err := effectiveConfiguration(schema, effective, true)
	if err != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("effective adapter configuration is invalid")
	}
	state, err := h.stateDocuments(id, resource)
	if err != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("adapter state is unavailable")
	}
	params := adapterproto.WatchCheckParams{Input: composedInput, Resource: cloneResourceRef(resource), Configuration: configuration, Secrets: configSecrets, State: state}
	raw, err := h.call(ctx, id, adapterproto.MethodWatchCheck, params)
	if err != nil {
		return adapterproto.WatchCheckResult{}, err
	}
	var result adapterproto.WatchCheckResult
	if err := json.Unmarshal(raw, &result); err != nil || result.Validate(d.MediaTypes) != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("adapter returned an invalid watch result")
	}
	if err := h.applyStateMutations(id, resource, result.StateMutations); err != nil {
		return adapterproto.WatchCheckResult{}, fmt.Errorf("adapter state could not be saved")
	}
	return result, nil
}

func composeInputSecrets(schema adapterproto.Schema, input json.RawMessage, secrets map[string]string) (json.RawMessage, error) {
	if err := adapterproto.ValidateObject(input); err != nil {
		return nil, err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(input, &values); err != nil || values == nil {
		return nil, fmt.Errorf("input must be a JSON object")
	}
	secretFields := make(map[string]adapterproto.Field)
	for _, field := range schema.Fields {
		if field.Control == "secret" {
			secretFields[field.Key] = field
		}
	}
	for key := range values {
		if _, secret := secretFields[key]; secret {
			return nil, fmt.Errorf("secret input must be supplied separately")
		}
	}
	if len(secrets) > len(secretFields) {
		return nil, fmt.Errorf("invalid secret input")
	}
	for key, value := range secrets {
		_, ok := secretFields[key]
		if !ok || len(value) > 16<<10 || !utf8.ValidString(value) {
			return nil, fmt.Errorf("invalid secret input")
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("invalid secret input")
		}
		values[key] = encoded
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("invalid input")
	}
	if err := adapterproto.ValidateObjectAgainstSchema(schema, encoded); err != nil {
		return nil, err
	}
	values = adapterproto.ApplyDefaults(schema, values)
	return json.Marshal(values)
}

func parseResolveResult(raw json.RawMessage) (adapterproto.MediaSource, []adapterproto.StateMutation, error) {
	var shape map[string]json.RawMessage
	if json.Unmarshal(raw, &shape) != nil || shape == nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("invalid resolve result")
	}
	if _, ok := shape["media"]; ok {
		var result adapterproto.ResolveResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return adapterproto.MediaSource{}, nil, err
		}
		return result.Media, result.State, nil
	}
	var media adapterproto.MediaSource
	if err := json.Unmarshal(raw, &media); err != nil {
		return adapterproto.MediaSource{}, nil, err
	}
	return media, nil, nil
}

func effectiveConfiguration(schema adapterproto.Schema, effective pluginconfig.EffectiveDocument, require bool) (map[string]json.RawMessage, map[string]string, error) {
	combined := make(map[string]json.RawMessage, len(effective.EffectiveValues)+len(effective.EffectiveSecrets))
	for _, field := range schema.Fields {
		switch field.Control {
		case "secret":
			if value, ok := effective.EffectiveSecrets[field.Key]; ok {
				encoded, err := json.Marshal(value)
				if err != nil {
					return nil, nil, err
				}
				combined[field.Key] = encoded
			}
		case "action", "status":
			// Display-only schema fields never enter the adapter configuration.
		default:
			if value, ok := effective.EffectiveValues[field.Key]; ok {
				combined[field.Key] = append(json.RawMessage(nil), value...)
			}
		}
	}
	combined = adapterproto.ApplyDefaults(schema, combined)
	// Stored overrides can become hidden when their controlling value changes.
	// Keep them in storage so the user can reveal them again later, but do not
	// send or validate dormant values for this resolution.
	for _, field := range schema.Fields {
		if field.Control == "action" || field.Control == "status" {
			continue
		}
		visible, err := adapterproto.IsVisible(schema, field.Key, combined)
		if err != nil {
			return nil, nil, err
		}
		if !visible {
			delete(combined, field.Key)
		}
	}
	validate := adapterproto.ValidateProvidedValues
	if require {
		validate = adapterproto.ValidateValues
	}
	if err := validate(schema, combined); err != nil {
		return nil, nil, err
	}
	values := map[string]json.RawMessage{}
	for _, field := range schema.Fields {
		if field.Control == "secret" || field.Control == "action" || field.Control == "status" {
			continue
		}
		if raw, ok := combined[field.Key]; ok {
			values[field.Key] = append(json.RawMessage(nil), raw...)
		}
	}
	secrets := map[string]string{}
	for _, field := range schema.Fields {
		if field.Control == "secret" {
			if value, ok := effective.EffectiveSecrets[field.Key]; ok {
				if _, active := combined[field.Key]; active {
					secrets[field.Key] = value
				}
			}
		}
	}
	return values, secrets, nil
}

func decodeObject(raw json.RawMessage) map[string]json.RawMessage {
	values := map[string]json.RawMessage{}
	_ = json.Unmarshal(raw, &values)
	return values
}

func marshalObject(values map[string]json.RawMessage) json.RawMessage {
	raw, _ := json.Marshal(values)
	return raw
}

func validateMediaForDescriptor(d adapterproto.Descriptor, media adapterproto.MediaSource) error {
	if err := adapterproto.ValidateMediaSource(media, d.MediaTypes); err != nil {
		return err
	}
	if media.RefreshPolicy != nil && !hasCapability(d, adapterproto.CapabilityRefresh) {
		return fmt.Errorf("adapter returned refresh policy without refresh capability")
	}
	return nil
}

func (h *Host) validateWorkflowResult(d adapterproto.Descriptor, result adapterproto.ResolveWorkflowResult, expectedID string, fallback *adapterproto.ResourceRef) error {
	if err := adapterproto.ValidateWorkflowResult(result, expectedID, d.MediaTypes); err != nil {
		return err
	}
	if result.Resource == nil {
		result.Resource = fallback
	}
	if err := adapterproto.ValidateResourceRefForDescriptor(d, result.Resource); err != nil {
		return fmt.Errorf("adapter returned an invalid resource")
	}
	if result.Media != nil {
		if err := validateMediaForDescriptor(d, *result.Media); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) advanceWorkflow(ctx context.Context, d adapterproto.Descriptor, session workflowSession, result adapterproto.ResolveWorkflowResult) (WorkflowProgress, error) {
	for {
		if result.State == "error" {
			if result.WorkflowID != session.workflowID {
				return WorkflowProgress{}, fmt.Errorf("adapter returned invalid workflow result")
			}
			resource := safeWorkflowResource(d, result.Resource, session.resource)
			h.finishWorkflowLifecycle(session, "failed", resource, result.Challenge)
			return WorkflowProgress{}, ErrWorkflowFailed
		}
		if session.continuing && !h.continuationActive(session) {
			return WorkflowProgress{}, fmt.Errorf("workflow canceled")
		}
		session.transitions++
		if session.transitions > maxWorkflowTransitions {
			return WorkflowProgress{}, fmt.Errorf("adapter workflow exceeded transition limit")
		}
		if result.Resource == nil {
			result.Resource = cloneResourceRef(session.resource)
		}
		if err := h.validateWorkflowResult(d, result, session.workflowID, session.resource); err != nil {
			return WorkflowProgress{}, err
		}
		if err := h.applyStateMutations(session.adapterID, result.Resource, result.StateMutations); err != nil {
			return WorkflowProgress{}, fmt.Errorf("adapter state could not be saved")
		}
		if result.State != "resource_discovered" {
			break
		}
		session.resource = cloneResourceRef(result.Resource)
		effective, err := h.effective(session.adapterID, session.resource)
		if err != nil {
			return WorkflowProgress{}, err
		}
		schemaChain, err := h.schemaChain(session.adapterID, session.resource)
		if err != nil {
			return WorkflowProgress{}, err
		}
		configuration, secrets, err := effectiveConfiguration(mergeSchemas(schemaChain), effective, false)
		if err != nil {
			return WorkflowProgress{}, fmt.Errorf("effective adapter configuration is invalid")
		}
		state, err := h.stateDocuments(session.adapterID, session.resource)
		if err != nil {
			return WorkflowProgress{}, fmt.Errorf("adapter state is unavailable")
		}
		if session.continuing && !h.continuationActive(session) {
			return WorkflowProgress{}, fmt.Errorf("workflow canceled")
		}
		params := adapterproto.ResolveContinueParams{WorkflowID: result.WorkflowID, Resource: cloneResourceRef(session.resource), Configuration: configuration, Secrets: secrets, State: state}
		raw, err := h.callAtGeneration(ctx, session.adapterID, adapterproto.MethodResolveContinue, params, session.generation)
		if errors.Is(err, errAdapterGenerationChanged) {
			h.expireWorkflow(result.WorkflowID)
			return WorkflowProgress{}, errAdapterGenerationChanged
		}
		if err != nil {
			return WorkflowProgress{}, err
		}
		if json.Unmarshal(raw, &result) != nil {
			return WorkflowProgress{}, fmt.Errorf("adapter returned invalid workflow result")
		}
	}
	if session.continuing && !h.continuationActive(session) {
		return WorkflowProgress{}, fmt.Errorf("workflow canceled")
	}
	progress := WorkflowProgress{WorkflowID: result.WorkflowID, AdapterID: d.ID, State: result.State, Resource: cloneResourceRef(result.Resource), Challenge: result.Challenge, Media: result.Media, Provenance: provenance(d)}
	if result.Challenge != nil && result.Challenge.Prompt != nil {
		if _, err := h.interactions.Apply(*result.Challenge.Prompt); err != nil {
			return WorkflowProgress{}, fmt.Errorf("adapter prompt is invalid")
		}
	}
	now := time.Now().UTC()
	if result.State == "configuration_required" || result.State == "interaction_required" {
		session.resource = cloneResourceRef(result.Resource)
		session.progress = progress
		session.updatedAt = now
		h.mu.Lock()
		if !h.workflowGenerationCurrentLocked(session) {
			if current, exists := h.workflows[result.WorkflowID]; exists && current.generation == session.generation {
				h.removeWorkflowLocked(result.WorkflowID, current, "expired")
			}
			h.mu.Unlock()
			if newID := interactionID(progress); newID != "" && h.interactions != nil {
				h.interactions.Remove(newID)
			}
			return WorkflowProgress{}, errAdapterGenerationChanged
		}
		if existing, exists := h.workflows[result.WorkflowID]; exists {
			if session.continuing && (!existing.continuing || existing.continuationID != session.continuationID) {
				h.mu.Unlock()
				if newID := interactionID(progress); newID != "" {
					h.interactions.Remove(newID)
				}
				return WorkflowProgress{}, fmt.Errorf("workflow canceled")
			}
			session.continuing = existing.continuing
			session.continuationID = existing.continuationID
			if oldID := interactionID(existing.progress); oldID != "" && oldID != interactionID(progress) {
				h.interactions.Remove(oldID)
			}
		} else if session.continuing {
			h.mu.Unlock()
			if newID := interactionID(progress); newID != "" {
				h.interactions.Remove(newID)
			}
			return WorkflowProgress{}, fmt.Errorf("workflow canceled")
		} else if len(h.workflows) >= maxActiveWorkflows {
			h.mu.Unlock()
			return WorkflowProgress{}, fmt.Errorf("too many active workflows")
		}
		if session.createdAt.IsZero() {
			session.createdAt = now
		}
		h.workflows[result.WorkflowID] = session
		h.mu.Unlock()
	} else {
		h.removeWorkflow(result.WorkflowID, "resolved")
	}
	return progress, nil
}

func (h *Host) continuationActive(session workflowSession) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	current, ok := h.workflows[session.workflowID]
	return ok && current.continuing && current.continuationID == session.continuationID
}

func interactionID(progress WorkflowProgress) string {
	if progress.Challenge != nil && progress.Challenge.Prompt != nil {
		return progress.Challenge.Prompt.InteractionID
	}
	return ""
}

func (h *Host) reserveWorkflow() (func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupWorkflowsLocked(time.Now().UTC())
	if h.closed {
		return nil, fmt.Errorf("adapter host is closed")
	}
	// Keep one terminal-event slot available for every active or in-flight
	// workflow. This provides backpressure if the management history writer is
	// unavailable instead of allowing terminal-event loss when the queue fills.
	if len(h.lifecycleQueue)+len(h.workflows)+h.workflowStarts >= maxPendingWorkflowLifecycleEvents {
		return nil, fmt.Errorf("too many pending workflow history events")
	}
	if len(h.workflows)+h.workflowStarts >= maxActiveWorkflows {
		return nil, fmt.Errorf("too many active workflows")
	}
	h.workflowStarts++
	var once bool
	return func() {
		h.mu.Lock()
		if !once {
			once = true
			h.workflowStarts--
		}
		h.mu.Unlock()
	}, nil
}

func (h *Host) cleanupWorkflowsLocked(now time.Time) {
	for id, session := range h.workflows {
		if now.Sub(session.updatedAt) > workflowTTL {
			h.removeWorkflowLocked(id, session, "expired")
		}
	}
	if h.interactions != nil {
		h.interactions.Cleanup()
	}
}

func (h *Host) removeWorkflowLocked(id string, session workflowSession, terminalState string) {
	delete(h.workflows, id)
	h.queueWorkflowLifecycleLocked(session.adapterID, session.workflowID, terminalState, session.resource, session.progress.Challenge)
	if session.cancel != nil {
		session.cancel()
	}
	if interaction := interactionID(session.progress); interaction != "" && h.interactions != nil {
		h.interactions.Remove(interaction)
	}
}

func (h *Host) removeWorkflow(id, terminalState string) {
	h.mu.Lock()
	if session, ok := h.workflows[id]; ok {
		h.removeWorkflowLocked(id, session, terminalState)
	}
	h.mu.Unlock()
}

func (h *Host) expireWorkflow(id string) {
	h.removeWorkflow(id, "expired")
}

// safeWorkflowResource uses only a descriptor-validated opaque reference. If
// an adapter error result contains an invalid resource, retain the validated
// resource already known to the workflow instead.
func safeWorkflowResource(descriptor adapterproto.Descriptor, candidate, fallback *adapterproto.ResourceRef) *adapterproto.ResourceRef {
	if candidate != nil && adapterproto.ValidateResourceRefForDescriptor(descriptor, candidate) == nil {
		return cloneResourceRef(candidate)
	}
	return cloneResourceRef(fallback)
}

func (h *Host) recordWorkflowLifecycle(adapterID, workflowID, state string, resource *adapterproto.ResourceRef, challenge *adapterproto.WorkflowChallenge) {
	h.mu.Lock()
	h.queueWorkflowLifecycleLocked(adapterID, workflowID, state, resource, challenge)
	h.mu.Unlock()
}

func (h *Host) finishWorkflowLifecycle(session workflowSession, state string, resource *adapterproto.ResourceRef, challenge *adapterproto.WorkflowChallenge) {
	h.mu.Lock()
	if current, exists := h.workflows[session.workflowID]; exists {
		if current.generation == session.generation {
			h.removeWorkflowLocked(session.workflowID, current, state)
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		return
	}
	// A continuing session that disappeared has already been canceled or
	// expired by another operation. Do not race that terminal event with a
	// second adapter-result event.
	if session.continuing {
		h.mu.Unlock()
		return
	}
	h.queueWorkflowLifecycleLocked(session.adapterID, session.workflowID, state, resource, challenge)
	h.mu.Unlock()
}

func (h *Host) queueWorkflowLifecycleLocked(adapterID, workflowID, state string, resource *adapterproto.ResourceRef, challenge *adapterproto.WorkflowChallenge) {
	if workflowID == "" || (state != "resolved" && state != "canceled" && state != "expired" && state != "failed") {
		return
	}
	h.lifecycleSeq++
	event := WorkflowLifecycleEvent{
		ID:         fmt.Sprintf("%s-%d", workflowID, h.lifecycleSeq),
		WorkflowID: workflowID,
		AdapterID:  adapterID,
		State:      state,
		At:         time.Now().UTC(),
		Resource:   cloneResourceRef(resource),
		Challenge:  summarizeWorkflowChallenge(challenge),
	}
	if len(h.lifecycleQueue) >= maxPendingWorkflowLifecycleEvents {
		copy(h.lifecycleQueue, h.lifecycleQueue[1:])
		h.lifecycleQueue = h.lifecycleQueue[:len(h.lifecycleQueue)-1]
	}
	h.lifecycleQueue = append(h.lifecycleQueue, event)
}

func summarizeWorkflowChallenge(challenge *adapterproto.WorkflowChallenge) *WorkflowLifecycleChallenge {
	if challenge == nil {
		return nil
	}
	fieldCount := len(challenge.Schema.Fields)
	hasSecret := false
	for _, field := range challenge.Schema.Fields {
		if field.Control == "secret" {
			hasSecret = true
			break
		}
	}
	if fieldCount == 0 && challenge.Prompt != nil {
		fieldCount = len(challenge.Prompt.Fields)
		for _, field := range challenge.Prompt.Fields {
			if field.Control == "secret" {
				hasSecret = true
				break
			}
		}
	}
	if fieldCount > 256 {
		fieldCount = 256
	}
	summary := &WorkflowLifecycleChallenge{FieldCount: fieldCount, HasSecretFields: hasSecret}
	if challenge.Prompt != nil {
		switch challenge.Prompt.Type {
		case "action", "prompt", "secret_prompt", "navigate", "display", "status", "complete", "error":
			summary.PromptType = challenge.Prompt.Type
		}
	}
	return summary
}

// PendingWorkflowLifecycleEvents returns answer-free terminal events in
// queue order. Event IDs remain stable until the corresponding acknowledgement.
func (h *Host) PendingWorkflowLifecycleEvents() []WorkflowLifecycleEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	items := make([]WorkflowLifecycleEvent, len(h.lifecycleQueue))
	for i, event := range h.lifecycleQueue {
		items[i] = cloneWorkflowLifecycleEvent(event)
	}
	return items
}

// AckWorkflowLifecycleEvent removes one event only after its consumer has
// persisted it. Unknown IDs are harmless and return false.
func (h *Host) AckWorkflowLifecycleEvent(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, event := range h.lifecycleQueue {
		if event.ID == id {
			copy(h.lifecycleQueue[i:], h.lifecycleQueue[i+1:])
			h.lifecycleQueue = h.lifecycleQueue[:len(h.lifecycleQueue)-1]
			return true
		}
	}
	return false
}

// DiscardWorkflowLifecycleEvents is used when no management history store is
// configured. It keeps the bounded in-memory queue from accumulating forever.
func (h *Host) DiscardWorkflowLifecycleEvents() {
	h.mu.Lock()
	h.lifecycleQueue = nil
	h.mu.Unlock()
}

func cloneWorkflowLifecycleEvent(event WorkflowLifecycleEvent) WorkflowLifecycleEvent {
	copy := event
	copy.Resource = cloneResourceRef(event.Resource)
	if event.Challenge != nil {
		challenge := *event.Challenge
		copy.Challenge = &challenge
	}
	return copy
}

func (h *Host) Workflow(id string) (WorkflowProgress, error) {
	h.mu.Lock()
	h.cleanupWorkflowsLocked(time.Now().UTC())
	session, ok := h.workflows[id]
	if !ok {
		h.mu.Unlock()
		return WorkflowProgress{}, fmt.Errorf("workflow not found or expired")
	}
	session.updatedAt = time.Now().UTC()
	h.workflows[id] = session
	h.mu.Unlock()
	return cloneProgress(session.progress), nil
}

// WorkflowChallengeSummary contains only bounded structural information about
// an active challenge. Adapter-provided prompt text, schema defaults, answer
// values, and media source data are intentionally omitted because those fields
// are not guaranteed to be safe for a workflow-list response.
type WorkflowChallengeSummary struct {
	PromptType      string `json:"prompt_type,omitempty"`
	FieldCount      int    `json:"field_count"`
	HasSecretFields bool   `json:"has_secret_fields"`
}

// WorkflowSummary is the safe management-plane projection of an active
// workflow. It deliberately does not contain WorkflowProgress.
type WorkflowSummary struct {
	WorkflowID string                    `json:"workflow_id"`
	AdapterID  string                    `json:"adapter_id"`
	State      string                    `json:"state"`
	Resource   *adapterproto.ResourceRef `json:"resource,omitempty"`
	Challenge  *WorkflowChallengeSummary `json:"challenge,omitempty"`
	CreatedAt  time.Time                 `json:"created_at"`
	UpdatedAt  time.Time                 `json:"updated_at"`
	ExpiresAt  time.Time                 `json:"expires_at"`
	InProgress bool                      `json:"in_progress"`
}

// ListWorkflows returns active workflow summaries ordered by most recently
// updated first. Expired sessions are removed under the same lock and through
// the same cleanup path used by other workflow operations.
func (h *Host) ListWorkflows() []WorkflowSummary {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupWorkflowsLocked(time.Now().UTC())

	out := make([]WorkflowSummary, 0, len(h.workflows))
	for _, session := range h.workflows {
		item := WorkflowSummary{
			WorkflowID: session.workflowID,
			AdapterID:  session.adapterID,
			State:      session.progress.State,
			Resource:   cloneResourceRef(session.resource),
			CreatedAt:  session.createdAt,
			UpdatedAt:  session.updatedAt,
			ExpiresAt:  session.updatedAt.Add(workflowTTL),
			InProgress: session.continuing,
		}
		if challenge := session.progress.Challenge; challenge != nil {
			summary := &WorkflowChallengeSummary{FieldCount: len(challenge.Schema.Fields)}
			for _, field := range challenge.Schema.Fields {
				if field.Control == "secret" {
					summary.HasSecretFields = true
					break
				}
			}
			if prompt := challenge.Prompt; prompt != nil {
				summary.PromptType = prompt.Type
			}
			item.Challenge = summary
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].WorkflowID < out[j].WorkflowID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

func cloneProgress(progress WorkflowProgress) WorkflowProgress {
	data, _ := json.Marshal(progress)
	var copy WorkflowProgress
	_ = json.Unmarshal(data, &copy)
	return copy
}

func (h *Host) CancelWorkflow(id string) error {
	h.mu.Lock()
	h.cleanupWorkflowsLocked(time.Now().UTC())
	session, ok := h.workflows[id]
	if ok {
		h.removeWorkflowLocked(id, session, "canceled")
	}
	h.mu.Unlock()
	if !ok {
		return fmt.Errorf("workflow not found or expired")
	}
	return nil
}

// ContinueResolution retains the v1 boolean persistence call shape. True opts
// into persistence for all optional fields that legacy challenges declared as
// persistable; explicit forbidden policies still take precedence.
func (h *Host) ContinueResolution(ctx context.Context, id string, values map[string]json.RawMessage, secrets map[string]string, persist bool) (WorkflowProgress, error) {
	fields := []string{}
	if persist {
		progress, err := h.Workflow(id)
		if err != nil || progress.Challenge == nil {
			return WorkflowProgress{}, fmt.Errorf("workflow challenge does not allow persistence")
		}
		if !progress.Challenge.Persistable {
			return WorkflowProgress{}, fmt.Errorf("workflow challenge does not allow persistence")
		}
		fields = legacyPersistableFields(progress.Challenge)
	}
	return h.ContinueResolutionFields(ctx, id, values, secrets, fields)
}

func legacyPersistableFields(challenge *adapterproto.WorkflowChallenge) []string {
	if challenge == nil {
		return nil
	}
	fields := make([]string, 0, len(challenge.Schema.Fields))
	for _, field := range challenge.Schema.Fields {
		if field.Control == "action" || field.Control == "status" {
			continue
		}
		if persistenceFor(field, challenge).Mode == adapterproto.PersistenceForbidden {
			continue
		}
		fields = append(fields, field.Key)
	}
	return fields
}

func (h *Host) ContinueResolutionFields(ctx context.Context, id string, values map[string]json.RawMessage, secrets map[string]string, persistFields []string) (WorkflowProgress, error) {
	session, err := h.lockWorkflowForContinue(id)
	if err != nil {
		return WorkflowProgress{}, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	session.cancel = cancel
	defer cancel()
	defer h.unlockWorkflowContinue(id, session.continuationID)
	if !h.setWorkflowCancel(id, session.continuationID, cancel) {
		return WorkflowProgress{}, fmt.Errorf("workflow canceled")
	}
	if err = h.ensureWorkflowGeneration(callCtx, id, session); err != nil {
		return WorkflowProgress{}, err
	}
	challenge := session.progress.Challenge
	if challenge == nil {
		return WorkflowProgress{}, fmt.Errorf("workflow has no active challenge")
	}
	provided := map[string]json.RawMessage{}
	for key, value := range values {
		provided[key] = append(json.RawMessage(nil), value...)
	}
	for key, value := range secrets {
		encoded, _ := json.Marshal(value)
		if _, dup := provided[key]; dup {
			return WorkflowProgress{}, fmt.Errorf("challenge answer fields overlap")
		}
		provided[key] = encoded
	}
	if err := adapterproto.ValidateValues(challenge.Schema, provided); err != nil {
		return WorkflowProgress{}, err
	}
	ordinary, secretAnswers, err := splitAnswers(challenge.Schema, values, secrets)
	if err != nil {
		return WorkflowProgress{}, err
	}
	persistValues := ordinary
	persistSecrets := secretAnswers
	// Defaults fill the adapter-facing continuation payload, while persistence
	// still receives only values explicitly supplied by the user.
	ordinary = adapterproto.ApplyDefaults(challenge.Schema, ordinary)
	for _, field := range challenge.Schema.Fields {
		if field.Control == "action" || field.Control == "status" {
			delete(ordinary, field.Key)
		}
	}
	if _, err = h.persistChallengeAnswers(session, challenge, persistValues, persistSecrets, persistFields); err != nil {
		return WorkflowProgress{}, err
	}
	effective, err := h.effective(session.adapterID, session.resource)
	if err != nil {
		return WorkflowProgress{}, err
	}
	d, err := h.Descriptor(session.adapterID)
	if err != nil {
		return WorkflowProgress{}, err
	}
	schemaChain, err := h.schemaChain(session.adapterID, session.resource)
	if err != nil {
		return WorkflowProgress{}, err
	}
	configuration, configSecrets, err := effectiveConfiguration(mergeSchemas(schemaChain), effective, false)
	if err != nil {
		return WorkflowProgress{}, fmt.Errorf("effective adapter configuration is invalid")
	}
	state, err := h.stateDocuments(session.adapterID, session.resource)
	if err != nil {
		return WorkflowProgress{}, fmt.Errorf("adapter state is unavailable")
	}
	params := adapterproto.ResolveContinueParams{WorkflowID: id, Resource: cloneResourceRef(session.resource), Configuration: configuration, Secrets: configSecrets, Answers: ordinary, AnswerSecrets: secretAnswers, State: state}
	raw, err := h.callAtGeneration(callCtx, session.adapterID, adapterproto.MethodResolveContinue, params, session.generation)
	if errors.Is(err, errAdapterGenerationChanged) {
		h.expireWorkflow(id)
		return WorkflowProgress{}, errAdapterGenerationChanged
	}
	if err != nil {
		return WorkflowProgress{}, err
	}
	var result adapterproto.ResolveWorkflowResult
	if json.Unmarshal(raw, &result) != nil {
		return WorkflowProgress{}, fmt.Errorf("adapter returned invalid workflow result")
	}
	if result.State == "error" {
		if result.WorkflowID != id {
			return WorkflowProgress{}, fmt.Errorf("adapter returned invalid workflow result")
		}
		resource := safeWorkflowResource(d, result.Resource, session.resource)
		h.finishWorkflowLifecycle(session, "failed", resource, result.Challenge)
		return WorkflowProgress{}, ErrWorkflowFailed
	}
	if err = h.validateWorkflowResult(d, result, id, session.resource); err != nil {
		return WorkflowProgress{}, err
	}
	return h.advanceWorkflow(callCtx, d, session, result)
}

func (h *Host) setWorkflowCancel(id string, token uint64, cancel context.CancelFunc) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	current, ok := h.workflows[id]
	if !ok || !current.continuing || current.continuationID != token {
		return false
	}
	current.cancel = cancel
	h.workflows[id] = current
	return true
}

func (h *Host) lockWorkflowForContinue(id string) (workflowSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupWorkflowsLocked(time.Now().UTC())
	session, ok := h.workflows[id]
	if !ok {
		return workflowSession{}, fmt.Errorf("workflow not found or expired")
	}
	if session.continuing {
		return workflowSession{}, fmt.Errorf("workflow is already continuing")
	}
	h.workflowCall++
	session.continuing = true
	session.continuationID = h.workflowCall
	session.updatedAt = time.Now().UTC()
	h.workflows[id] = session
	return session, nil
}

func (h *Host) unlockWorkflowContinue(id string, token uint64) {
	h.mu.Lock()
	if current, ok := h.workflows[id]; ok && current.continuing && current.continuationID == token {
		current.continuing = false
		current.continuationID = 0
		current.cancel = nil
		current.updatedAt = time.Now().UTC()
		h.workflows[id] = current
	}
	h.mu.Unlock()
}

func (h *Host) ensureWorkflowGeneration(ctx context.Context, id string, session workflowSession) error {
	e, err := h.entryFor(session.adapterID)
	if err != nil {
		return err
	}
	e.opMu.Lock()
	defer e.opMu.Unlock()
	if _, err = h.ensureProcess(ctx, e); err != nil {
		return err
	}
	e.stateMu.Lock()
	generation := e.status.Generation
	e.stateMu.Unlock()
	if generation != session.generation {
		h.expireWorkflow(id)
		return errAdapterGenerationChanged
	}
	return nil
}

func splitAnswers(schema adapterproto.Schema, values map[string]json.RawMessage, secrets map[string]string) (map[string]json.RawMessage, map[string]string, error) {
	ordinary, secretAnswers := map[string]json.RawMessage{}, map[string]string{}
	knownValues, knownSecrets := map[string]bool{}, map[string]bool{}
	for _, field := range schema.Fields {
		if field.Control == "action" || field.Control == "status" {
			continue
		}
		if field.Control == "secret" {
			knownSecrets[field.Key] = true
			if value, ok := secrets[field.Key]; ok {
				secretAnswers[field.Key] = value
			}
			continue
		}
		knownValues[field.Key] = true
		if value, ok := values[field.Key]; ok {
			ordinary[field.Key] = append(json.RawMessage(nil), value...)
		}
	}
	for key := range values {
		if !knownValues[key] {
			return nil, nil, fmt.Errorf("challenge answers contain unknown or mismatched fields")
		}
	}
	for key := range secrets {
		if !knownSecrets[key] {
			return nil, nil, fmt.Errorf("challenge answers contain unknown or mismatched fields")
		}
	}
	return ordinary, secretAnswers, nil
}

func persistenceFor(field adapterproto.Field, challenge *adapterproto.WorkflowChallenge) adapterproto.FieldPersistence {
	if field.Persistence != nil {
		return *field.Persistence
	}
	if challenge.Persistable {
		return adapterproto.FieldPersistence{Mode: adapterproto.PersistenceOptional, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceCurrent}}
	}
	return adapterproto.FieldPersistence{Mode: adapterproto.PersistenceForbidden, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceCurrent}}
}

func (h *Host) persistChallengeAnswers(session workflowSession, challenge *adapterproto.WorkflowChallenge, values map[string]json.RawMessage, secrets map[string]string, persistFields []string) (bool, error) {
	requested := map[string]bool{}
	for _, key := range persistFields {
		if requested[key] {
			return false, fmt.Errorf("duplicate persistence field")
		}
		requested[key] = true
	}
	fields := map[string]adapterproto.Field{}
	for _, field := range challenge.Schema.Fields {
		fields[field.Key] = field
	}
	for key := range requested {
		if _, ok := fields[key]; !ok {
			return false, fmt.Errorf("unknown persistence field")
		}
	}
	groups := map[string]*persistGroup{}
	chain, err := h.schemaChain(session.adapterID, session.resource)
	if err != nil {
		return false, err
	}
	for _, field := range challenge.Schema.Fields {
		policy := persistenceFor(field, challenge)
		persist := policy.Mode == adapterproto.PersistenceRequired || policy.Mode == adapterproto.PersistenceOptional && requested[field.Key]
		if requested[field.Key] && policy.Mode == adapterproto.PersistenceForbidden {
			return false, fmt.Errorf("challenge field cannot be persisted")
		}
		if !persist {
			continue
		}
		if h.configs == nil {
			return false, fmt.Errorf("persistent configuration is unavailable")
		}
		scope, err := persistenceScope(session.adapterID, session.resource, policy.Target)
		if err != nil {
			return false, err
		}
		targetSchema, ok := schemaAtScope(chain, scope)
		if !ok {
			return false, fmt.Errorf("persistence target is outside the resource chain")
		}
		declared, ok := findSchemaField(targetSchema, field.Key)
		if !ok || declared.Control != field.Control {
			return false, fmt.Errorf("persistent challenge field is not declared by the target schema")
		}
		key := pluginconfig.ScopeIdentity(scope)
		group := groups[key]
		if group == nil {
			group = &persistGroup{scope: scope, schema: targetSchema, values: map[string]json.RawMessage{}, secrets: map[string]string{}}
			groups[string(key)] = group
		}
		if field.Control == "secret" {
			if value, exists := secrets[field.Key]; exists {
				group.secrets[field.Key] = value
			}
		} else if value, exists := values[field.Key]; exists {
			group.values[field.Key] = append(json.RawMessage(nil), value...)
		}
	}
	for key := range requested {
		if persistenceFor(fields[key], challenge).Mode == adapterproto.PersistenceForbidden {
			return false, fmt.Errorf("challenge field cannot be persisted")
		}
	}
	updates := make([]pluginconfig.ScopeUpdate, 0, len(groups))
	for _, scope := range chain {
		if group := groups[pluginconfig.ScopeIdentity(scope.Scope)]; group != nil {
			updates = append(updates, pluginconfig.ScopeUpdate{Scope: group.scope, Schema: group.schema, Values: group.values, Secrets: group.secrets})
		}
	}
	if err := h.configs.PutBatch(updates); err != nil {
		return false, fmt.Errorf("persistent configuration could not be saved")
	}
	return len(groups) != 0, nil
}

type persistGroup struct {
	scope   pluginconfig.Scope
	schema  adapterproto.Schema
	values  map[string]json.RawMessage
	secrets map[string]string
}

func persistenceScope(pluginID string, current *adapterproto.ResourceRef, target adapterproto.PersistenceTarget) (pluginconfig.Scope, error) {
	switch target.Scope {
	case adapterproto.PersistencePlugin:
		return pluginconfig.Scope{PluginID: pluginID}, nil
	case adapterproto.PersistenceCurrent:
		if current == nil {
			return pluginconfig.Scope{}, fmt.Errorf("current resource persistence target is unavailable")
		}
		return pluginconfig.Scope{PluginID: pluginID, Resource: cloneResourceRef(current)}, nil
	case adapterproto.PersistenceResource:
		if target.Resource == nil || !resourceInChain(current, target.Resource) {
			return pluginconfig.Scope{}, fmt.Errorf("persistence target is outside the resource chain")
		}
		return pluginconfig.Scope{PluginID: pluginID, Resource: cloneResourceRef(target.Resource)}, nil
	default:
		return pluginconfig.Scope{}, fmt.Errorf("invalid persistence target")
	}
}

func resourceInChain(current, target *adapterproto.ResourceRef) bool {
	if target == nil {
		return false
	}
	for item := current; item != nil; item = item.Parent {
		if sameResource(item, target) {
			return true
		}
	}
	return false
}

func sameResource(a, b *adapterproto.ResourceRef) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Type != b.Type || a.ID != b.ID {
		return false
	}
	return sameResource(a.Parent, b.Parent)
}

func schemaAtScope(chain []pluginconfig.ScopeSchema, scope pluginconfig.Scope) (adapterproto.Schema, bool) {
	identity := pluginconfig.ScopeIdentity(scope)
	for _, item := range chain {
		if pluginconfig.ScopeIdentity(item.Scope) == identity {
			return item.Schema, true
		}
	}
	return adapterproto.Schema{}, false
}

func findSchemaField(schema adapterproto.Schema, key string) (adapterproto.Field, bool) {
	for _, field := range schema.Fields {
		if field.Key == key {
			return field, true
		}
	}
	return adapterproto.Field{}, false
}

func cloneResourceRef(ref *adapterproto.ResourceRef) *adapterproto.ResourceRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	copy.Parent = cloneResourceRef(ref.Parent)
	return &copy
}

func (h *Host) stateDocuments(id string, resource *adapterproto.ResourceRef) ([]adapterproto.StateDocument, error) {
	scopes, err := pluginconfig.ResourceScopes(id, resource)
	if err != nil {
		return nil, err
	}
	documents := make([]pluginconfig.StateDoc, len(scopes))
	if h.state != nil {
		documents, err = h.state.Chain(scopes)
		if err != nil {
			return nil, err
		}
	}
	docs := make([]adapterproto.StateDocument, 0, len(scopes))
	for i, scope := range scopes {
		doc := documents[i]
		if doc.Values == nil {
			doc.Values = map[string]json.RawMessage{}
		}
		if doc.Secrets == nil {
			doc.Secrets = map[string]string{}
		}
		docs = append(docs, adapterproto.StateDocument{Resource: cloneResourceRef(scope.Resource), Values: doc.Values, Secrets: doc.Secrets})
	}
	return docs, nil
}

func (h *Host) applyStateMutations(id string, current *adapterproto.ResourceRef, mutations []adapterproto.StateMutation) error {
	commit, err := h.prepareStateMutations(id, current, mutations)
	if err != nil {
		return err
	}
	return commit()
}

func (h *Host) prepareStateMutations(id string, current *adapterproto.ResourceRef, mutations []adapterproto.StateMutation) (func() error, error) {
	if len(mutations) == 0 {
		return func() error { return nil }, nil
	}
	if h.state == nil {
		return nil, fmt.Errorf("adapter state store is unavailable")
	}
	d, err := h.Descriptor(id)
	if err != nil {
		return nil, err
	}
	if err = adapterproto.ValidateResourceRefForDescriptor(d, current); err != nil {
		return nil, fmt.Errorf("invalid resource chain")
	}
	allowed, err := pluginconfig.ResourceScopes(id, current)
	if err != nil {
		return nil, err
	}
	allowedScopes := map[string]pluginconfig.Scope{}
	for _, scope := range allowed {
		allowedScopes[pluginconfig.ScopeIdentity(scope)] = scope
	}
	items := make([]pluginconfig.StateMutation, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation.Resource != nil {
			if err := adapterproto.ValidateResourceRefForDescriptor(d, mutation.Resource); err != nil {
				return nil, fmt.Errorf("adapter state target is invalid")
			}
		}
		scope := pluginconfig.Scope{PluginID: id, Resource: cloneResourceRef(mutation.Resource)}
		validated, ok := allowedScopes[pluginconfig.ScopeIdentity(scope)]
		if !ok {
			return nil, fmt.Errorf("adapter state target is outside the resource chain")
		}
		items = append(items, pluginconfig.StateMutation{Scope: validated, Values: mutation.Values, Secrets: mutation.Secrets, ClearValues: mutation.ClearValues, ClearSecrets: mutation.ClearSecrets})
	}
	return func() error { return h.state.Apply(items) }, nil
}

// ShouldRefreshProactively applies only the adapter-declared expiry window.
func ShouldRefreshProactively(media adapterproto.MediaSource, now time.Time) bool {
	policy := media.RefreshPolicy
	return policy != nil && policy.ExpiresAt != nil && !now.Before(policy.ExpiresAt.Add(-time.Duration(policy.RefreshBeforeSeconds)*time.Second))
}

// ShouldRefreshForStatus checks the adapter-declared status list. Core does not
// attach semantics to common HTTP status codes.
func ShouldRefreshForStatus(media adapterproto.MediaSource, status int) bool {
	if media.RefreshPolicy == nil {
		return false
	}
	for _, candidate := range media.RefreshPolicy.OnHTTPStatus {
		if candidate == status {
			return true
		}
	}
	return false
}

func (h *Host) Refresh(ctx context.Context, id string, resource *adapterproto.ResourceRef, current adapterproto.MediaSource) (adapterproto.MediaSource, error) {
	media, commit, err := h.PrepareRefresh(ctx, id, resource, current)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	if err := commit(); err != nil {
		return adapterproto.MediaSource{}, fmt.Errorf("adapter state could not be saved")
	}
	return media, nil
}

// PrepareRefresh calls and validates the adapter's replacement media source,
// but defers adapter state writes until the caller has completed its own
// network-safety checks. The closure is idempotent for a single caller.
func (h *Host) PrepareRefresh(ctx context.Context, id string, resource *adapterproto.ResourceRef, current adapterproto.MediaSource) (adapterproto.MediaSource, func() error, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.MediaSource{}, nil, err
	}
	if !hasCapability(d, adapterproto.CapabilityRefresh) {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("adapter does not support refresh")
	}
	if err := adapterproto.ValidateResourceRefForDescriptor(d, resource); err != nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("invalid resource reference")
	}
	if err := adapterproto.ValidateMediaSource(current, d.MediaTypes); err != nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("current media source is invalid")
	}
	state, err := h.stateDocuments(id, resource)
	if err != nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("adapter state is unavailable")
	}
	params := adapterproto.RefreshParams{Resource: cloneResourceRef(resource), Current: current, State: state}
	raw, err := h.call(ctx, id, adapterproto.MethodRefresh, params)
	if err != nil {
		return adapterproto.MediaSource{}, nil, err
	}
	var result adapterproto.RefreshResult
	if json.Unmarshal(raw, &result) != nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("adapter returned invalid refresh result")
	}
	if err := validateMediaForDescriptor(d, result.Media); err != nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("adapter returned invalid refresh media source")
	}
	commit, err := h.prepareStateMutations(id, resource, result.State)
	if err != nil {
		return adapterproto.MediaSource{}, nil, fmt.Errorf("adapter state mutation is invalid")
	}
	return result.Media, commit, nil
}

// PrepareMetadata observes adapter-defined source title/description using the
// current media snapshot. State mutations are validated here but committed by
// the caller only after its canonical recording update is durable.
func (h *Host) PrepareMetadata(ctx context.Context, id string, resource *adapterproto.ResourceRef, current adapterproto.MediaSource) (adapterproto.MetadataResult, func() error, bool, error) {
	d, err := h.Descriptor(id)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, false, err
	}
	if !hasCapability(d, adapterproto.CapabilityMetadata) {
		return adapterproto.MetadataResult{}, nil, false, nil
	}
	if err := adapterproto.ValidateResourceRefForDescriptor(d, resource); err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("invalid resource reference")
	}
	if err := adapterproto.ValidateMediaSource(current, d.MediaTypes); err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("current media source is invalid")
	}
	effective, err := h.effective(id, resource)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("adapter configuration unavailable")
	}
	schema, err := h.Schema(id, resource)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("adapter configuration schema unavailable")
	}
	configuration, secrets, err := effectiveConfiguration(schema, effective, true)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("effective adapter configuration is invalid")
	}
	state, err := h.stateDocuments(id, resource)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("adapter state is unavailable")
	}
	params := adapterproto.MetadataParams{
		Resource: cloneResourceRef(resource), Current: current,
		Configuration: configuration, Secrets: secrets, State: state,
	}
	raw, err := h.call(ctx, id, adapterproto.MethodMetadata, params)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, true, err
	}
	var result adapterproto.MetadataResult
	if !utf8.Valid(raw) || json.Unmarshal(raw, &result) != nil || result.Validate() != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("adapter returned invalid metadata")
	}
	commit, err := h.prepareStateMutations(id, resource, result.StateMutations)
	if err != nil {
		return adapterproto.MetadataResult{}, nil, true, fmt.Errorf("adapter state mutation is invalid")
	}
	return result, commit, true, nil
}

func (h *Host) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	for id, session := range h.workflows {
		h.removeWorkflowLocked(id, session, "canceled")
	}
	entries := make([]*entry, 0, len(h.entries))
	for _, e := range h.entries {
		entries = append(entries, e)
	}
	h.mu.Unlock()
	var wg sync.WaitGroup
	var closingMu sync.Mutex
	var closing []*process
	for _, e := range entries {
		wg.Add(1)
		go func(e *entry) {
			defer wg.Done()
			e.opMu.Lock()
			defer e.opMu.Unlock()
			e.stateMu.Lock()
			p := e.process
			e.process = nil
			e.stateMu.Unlock()
			if p != nil {
				closingMu.Lock()
				closing = append(closing, p)
				closingMu.Unlock()
				p.shutdown()
			}
		}(e)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(hostCloseTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		// A shared deadline bounds shutdown independently of adapter count. Kill
		// any process still registered, then join every closer goroutine.
		closingMu.Lock()
		toKill := append([]*process(nil), closing...)
		closingMu.Unlock()
		for _, e := range entries {
			e.stateMu.Lock()
			p := e.process
			e.stateMu.Unlock()
			if p != nil {
				toKill = append(toKill, p)
			}
		}
		for _, p := range toKill {
			p.kill()
		}
		<-done
	}
}
