package adapterproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image/png"
	"math"
	"math/big"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/streammeta"
)

type Descriptor struct {
	ID                  string         `json:"id"`
	Name                string         `json:"name"`
	Version             string         `json:"version"`
	ProtocolVersion     int            `json:"protocol_version"`
	Capabilities        []string       `json:"capabilities,omitempty"`
	InputSchema         Schema         `json:"input_schema"`
	ConfigurationSchema Schema         `json:"configuration_schema"`
	ResourceTypes       []ResourceType `json:"resource_types,omitempty"`
	MediaTypes          []string       `json:"media_types"`
	Branding            *Branding      `json:"branding,omitempty"`
}

// Branding carries optional, adapter-owned presentation assets. Branding is
// descriptive metadata only and does not change protocol-v1 semantics.
type Branding struct {
	Icon *BrandIcon `json:"icon,omitempty"`
}

// BrandIcon is a bounded raster image supplied inline by the adapter. The
// current protocol permits PNG only so consumers never need to sanitize SVG
// or follow adapter-provided URLs.
type BrandIcon struct {
	MediaType string `json:"media_type"`
	Data      []byte `json:"data"`
}

const (
	maxBrandIconBytes     = 64 << 10
	maxBrandIconDimension = 512
)

type Schema struct {
	Fields []Field `json:"fields"`
}

type Field struct {
	Key         string `json:"key"`
	Control     string `json:"control"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	// Inherit declares whether a value stored at this scope may flow into
	// descendant resource scopes. Omitted is deliberately restrictive.
	Inherit     bool              `json:"inherit,omitempty"`
	Default     json.RawMessage   `json:"default,omitempty"`
	Constraints *Constraints      `json:"constraints,omitempty"`
	Options     []Option          `json:"options,omitempty"`
	VisibleWhen json.RawMessage   `json:"visible_when,omitempty"`
	Persistence *FieldPersistence `json:"persistence,omitempty"`
}

// FieldPersistence describes whether an interaction answer may be stored and
// where. Resource references remain opaque to Core and are validated against
// the active, adapter-declared resource chain.
type FieldPersistence struct {
	Mode   string            `json:"mode"`
	Target PersistenceTarget `json:"target"`
}

type PersistenceTarget struct {
	Scope    string       `json:"scope"`
	Resource *ResourceRef `json:"resource,omitempty"`
}

const (
	PersistenceForbidden = "forbidden"
	PersistenceOptional  = "optional"
	PersistenceRequired  = "required"
	PersistencePlugin    = "plugin"
	PersistenceCurrent   = "current_resource"
	PersistenceResource  = "resource"
)

type Constraints struct {
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	MinLength *int     `json:"min_length,omitempty"`
	MaxLength *int     `json:"max_length,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	MinItems  *int     `json:"min_items,omitempty"`
	MaxItems  *int     `json:"max_items,omitempty"`
}

type Option struct {
	Value any    `json:"value"`
	Label string `json:"label"`
}

// UnmarshalJSON preserves option numbers exactly instead of routing them
// through float64, which could lose integer precision before validation.
func (o *Option) UnmarshalJSON(data []byte) error {
	var wire struct {
		Value json.RawMessage `json:"value"`
		Label string          `json:"label"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if len(wire.Value) == 0 {
		return fmt.Errorf("option value is required")
	}
	decoder := json.NewDecoder(strings.NewReader(string(wire.Value)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	o.Value, o.Label = value, wire.Label
	return nil
}

type ResourceType struct {
	Type                string   `json:"type"`
	ParentTypes         []string `json:"parent_types,omitempty"`
	ConfigurationSchema Schema   `json:"configuration_schema,omitempty"`
}

type ResourceRef struct {
	Type   string       `json:"resource_type"`
	ID     string       `json:"resource_id"`
	Parent *ResourceRef `json:"parent,omitempty"`
}

type Resource struct {
	ResourceRef
	DisplayName string                     `json:"display_name,omitempty"`
	Attributes  map[string]json.RawMessage `json:"attributes,omitempty"`
}

// ResourceListParams requests one page of adapter-defined resources. Resource
// types, identifiers, cursor values, and attributes are opaque to Core.
type ResourceListParams struct {
	Parent       *ResourceRef `json:"parent,omitempty"`
	ResourceType string       `json:"resource_type,omitempty"`
	Cursor       string       `json:"cursor,omitempty"`
	Limit        int          `json:"limit"`
}

// ResourceSearchParams requests one page matching an adapter-defined query.
type ResourceSearchParams struct {
	Parent       *ResourceRef `json:"parent,omitempty"`
	ResourceType string       `json:"resource_type,omitempty"`
	Query        string       `json:"query"`
	Cursor       string       `json:"cursor,omitempty"`
	Limit        int          `json:"limit"`
}

// ResourcePage is an opaque, bounded page returned by resource.list or
// resource.search.
type ResourcePage struct {
	Items      []Resource `json:"items"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

type ResolveParams struct {
	Input         json.RawMessage            `json:"input"`
	Resource      *ResourceRef               `json:"resource,omitempty"`
	Configuration map[string]json.RawMessage `json:"configuration,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	State         []StateDocument            `json:"state,omitempty"`
}

// WatchCheckParams asks an adapter to inspect whether its configured source is
// currently live. It reuses resolve's composed configuration, secrets, and
// adapter-owned state without introducing platform semantics into Core.
type WatchCheckParams struct {
	Input         json.RawMessage            `json:"input"`
	Resource      *ResourceRef               `json:"resource,omitempty"`
	Configuration map[string]json.RawMessage `json:"configuration,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	State         []StateDocument            `json:"state,omitempty"`
}

// WatchCheckResult separates a successful offline observation from an
// adapter/network error. SessionRef is opaque and must not be exposed by Core.
type WatchCheckResult struct {
	State          string          `json:"state"`
	SessionRef     string          `json:"session_ref,omitempty"`
	Title          string          `json:"title,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	Media          *MediaSource    `json:"media,omitempty"`
	StateMutations []StateMutation `json:"state_mutations,omitempty"`
}

// Validate checks the generic result envelope. Descriptor-specific media and
// state target validation is performed by adapterhost before it is accepted.
func (result WatchCheckResult) Validate(mediaTypes []string) error {
	if result.State != "offline" && result.State != "live" {
		return fmt.Errorf("watch result state is invalid")
	}
	if len(result.SessionRef) > 4096 || strings.IndexByte(result.SessionRef, 0) >= 0 {
		return fmt.Errorf("watch session reference is invalid")
	}
	if !utf8.ValidString(result.SessionRef) || !utf8.ValidString(result.Title) || len(result.Title) > 4096 {
		return fmt.Errorf("watch result text is invalid")
	}
	if result.StartedAt != nil && (result.StartedAt.IsZero() || result.StartedAt.Year() < 1 || result.StartedAt.Year() > 9999) {
		return fmt.Errorf("watch result timestamp is invalid")
	}
	if result.Media != nil {
		if result.State != "live" || ValidateMediaSource(*result.Media, mediaTypes) != nil {
			return fmt.Errorf("watch result media is invalid")
		}
	}
	if len(result.StateMutations) > 64 {
		return fmt.Errorf("watch state mutation list exceeds limit")
	}
	return nil
}

// ResolveResult is the extensible result form for adapters that need to return
// adapter-owned state mutations alongside a media source. Existing v1 adapters
// may continue returning MediaSource directly.
type ResolveResult struct {
	Media MediaSource     `json:"media"`
	State []StateMutation `json:"state,omitempty"`
}

// ResolveBeginParams starts a stateful generic resolution workflow. WorkflowID
// is generated by Core and opaque to the adapter.
type ResolveBeginParams struct {
	WorkflowID    string                     `json:"workflow_id"`
	Input         json.RawMessage            `json:"input"`
	Resource      *ResourceRef               `json:"resource,omitempty"`
	Configuration map[string]json.RawMessage `json:"configuration,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	State         []StateDocument            `json:"state,omitempty"`
}

// ResolveContinueParams resumes a workflow after resource/configuration
// discovery. Answers are ephemeral inputs for this continuation; configuration
// and secrets contain the latest effective snapshot.
type ResolveContinueParams struct {
	WorkflowID    string                     `json:"workflow_id"`
	Resource      *ResourceRef               `json:"resource,omitempty"`
	Configuration map[string]json.RawMessage `json:"configuration,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	Answers       map[string]json.RawMessage `json:"answers,omitempty"`
	AnswerSecrets map[string]string          `json:"answer_secrets,omitempty"`
	State         []StateDocument            `json:"state,omitempty"`
}

type WorkflowChallenge struct {
	Schema      Schema              `json:"schema"`
	Prompt      *InteractionMessage `json:"prompt,omitempty"`
	Persistable bool                `json:"persistable,omitempty"`
}

// StateDocument is adapter-owned, opaque state for one Core-managed scope.
// The plugin scope has Resource=nil. Secret state is never exposed by Core
// APIs and is stored separately from ordinary adapter state.
type StateDocument struct {
	Resource *ResourceRef               `json:"resource,omitempty"`
	Values   map[string]json.RawMessage `json:"values,omitempty"`
	Secrets  map[string]string          `json:"secrets,omitempty"`
}

// StateMutation merges keys into an adapter-owned state scope. Clear lists
// are explicit so empty values never have deletion semantics.
type StateMutation struct {
	Resource     *ResourceRef               `json:"resource,omitempty"`
	Values       map[string]json.RawMessage `json:"values,omitempty"`
	Secrets      map[string]string          `json:"secrets,omitempty"`
	ClearValues  []string                   `json:"clear_values,omitempty"`
	ClearSecrets []string                   `json:"clear_secrets,omitempty"`
}

type ResolveWorkflowResult struct {
	State          string             `json:"state"`
	WorkflowID     string             `json:"workflow_id"`
	Resource       *ResourceRef       `json:"resource,omitempty"`
	Challenge      *WorkflowChallenge `json:"challenge,omitempty"`
	Media          *MediaSource       `json:"media,omitempty"`
	StateMutations []StateMutation    `json:"state_mutations,omitempty"`
}

type AdapterProvenance struct {
	ID              string `json:"id"`
	Version         string `json:"version"`
	ProtocolVersion int    `json:"protocol_version"`
	Fingerprint     string `json:"descriptor_fingerprint,omitempty"`
}

type MediaSource struct {
	Type          string            `json:"type"`
	ManifestURL   string            `json:"manifest_url"`
	Headers       map[string]string `json:"headers,omitempty"`
	RequestPolicy *RequestPolicy    `json:"request_policy,omitempty"`
	SessionRef    string            `json:"session_ref,omitempty"`
	Refresh       json.RawMessage   `json:"refresh,omitempty"`
	Metadata      json.RawMessage   `json:"metadata,omitempty"`
	ArchivePolicy *ArchivePolicy    `json:"archive_policy,omitempty"`
	RefreshPolicy *RefreshPolicy    `json:"refresh_policy,omitempty"`
}

// RefreshPolicy declares adapter-owned refresh triggers. Core does not infer
// expiry from particular HTTP statuses; only listed statuses may trigger a
// refresh attempt.
type RefreshPolicy struct {
	ExpiresAt            *time.Time `json:"expires_at,omitempty"`
	RefreshBeforeSeconds int        `json:"refresh_before_seconds,omitempty"`
	OnHTTPStatus         []int      `json:"on_http_status,omitempty"`
}

type RefreshParams struct {
	Resource *ResourceRef    `json:"resource,omitempty"`
	Current  MediaSource     `json:"current"`
	State    []StateDocument `json:"state,omitempty"`
}

type RefreshResult struct {
	Media MediaSource     `json:"media"`
	State []StateMutation `json:"state,omitempty"`
}

// MetadataParams asks an adapter to observe generic source metadata using the
// current resolved media and the same effective configuration/state context
// used by other adapter operations. Core does not interpret Current.
type MetadataParams struct {
	Resource      *ResourceRef               `json:"resource,omitempty"`
	Current       MediaSource                `json:"current"`
	Configuration map[string]json.RawMessage `json:"configuration,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	State         []StateDocument            `json:"state,omitempty"`
}

// StreamMetadata contains only source title and description in v1. A nil
// field means the adapter cannot provide a current value; a pointer to an
// empty string is an explicit known-empty value.
type StreamMetadata struct {
	Title       *string `json:"title,omitempty"`
	Description *string `json:"description,omitempty"`
}

type MetadataResult struct {
	Metadata        StreamMetadata  `json:"metadata"`
	SourceUpdatedAt *time.Time      `json:"source_updated_at,omitempty"`
	StateMutations  []StateMutation `json:"state_mutations,omitempty"`
}

func (result MetadataResult) Validate() error {
	if err := streammeta.ValidateText(result.Metadata.Title, streammeta.MaxTitleBytes); err != nil {
		return fmt.Errorf("metadata title is invalid")
	}
	if err := streammeta.ValidateText(result.Metadata.Description, streammeta.MaxDescriptionBytes); err != nil {
		return fmt.Errorf("metadata description is invalid")
	}
	if err := streammeta.ValidateTimestamp(result.SourceUpdatedAt); err != nil {
		return err
	}
	if len(result.StateMutations) > 64 {
		return fmt.Errorf("metadata state mutation list exceeds limit")
	}
	return nil
}

// ArchivePolicy classifies canonical source URI provenance. Fetch URLs and
// manifests are preserved exactly; this value does not redact or rewrite them.
type ArchivePolicy struct {
	SourceURI string `json:"source_uri,omitempty"` // sensitive (default) or public
}

func (m MediaSource) SourceURIClassification() string {
	if m.ArchivePolicy == nil || m.ArchivePolicy.SourceURI == "" {
		return "sensitive"
	}
	return m.ArchivePolicy.SourceURI
}

// RequestPolicy limits where Core may forward adapter-supplied media headers.
// It authorizes header forwarding only; network and SSRF policy remains owned
// by Core. An omitted policy or header_forwarding block is restrictive.
type RequestPolicy struct {
	HeaderForwarding *HeaderForwardingPolicy `json:"header_forwarding,omitempty"`
}

// HeaderForwardingPolicy currently applies one origin set to all adapter-
// supplied headers. Keeping mode as a string leaves room for future protocol
// extensions such as per-header rules without introducing a policy engine now.
type HeaderForwardingPolicy struct {
	Mode    string   `json:"mode,omitempty"`
	Origins []string `json:"origins,omitempty"`
}

const (
	HeaderForwardingSameOrigin = "same_origin"
	HeaderForwardingAllowlist  = "allowlist"
)

type mediaOrigin struct {
	scheme   string
	hostname string
	port     string
}

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	mediaTypePattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$`)
)

const (
	maxSchemaFields            = 512
	maxSchemaOptions           = 2048
	maxVisibilityDepth         = 32
	maxVisibilityNodes         = 2048
	maxDescriptorResourceTypes = 256
)

func IsValidIdentifier(value string) bool { return identifierPattern.MatchString(value) }

func (o mediaOrigin) equal(other mediaOrigin) bool {
	return o.scheme == other.scheme && o.hostname == other.hostname && o.port == other.port
}

// AllowsHeadersFor reports whether the source's adapter-supplied headers may
// be sent to target. Callers must still enforce their independent network
// safety policy before making the request.
func (m MediaSource) AllowsHeadersFor(target *url.URL) bool {
	if target == nil {
		return false
	}
	policy := m.RequestPolicy
	if policy == nil || policy.HeaderForwarding == nil || policy.HeaderForwarding.Mode == "" || policy.HeaderForwarding.Mode == HeaderForwardingSameOrigin {
		manifest, err := parseMediaOrigin(m.ManifestURL)
		if err != nil {
			return false
		}
		targetOrigin, err := originFromURL(target)
		return err == nil && manifest.equal(targetOrigin)
	}
	if policy.HeaderForwarding.Mode != HeaderForwardingAllowlist {
		return false
	}
	targetOrigin, err := originFromURL(target)
	if err != nil {
		return false
	}
	for _, raw := range policy.HeaderForwarding.Origins {
		allowed, err := parseAllowedOrigin(raw)
		if err == nil && allowed.equal(targetOrigin) {
			return true
		}
	}
	return false
}

type InteractionField struct {
	Key         string   `json:"key"`
	Control     string   `json:"control"`
	Label       string   `json:"label"`
	Description string   `json:"description,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Options     []Option `json:"options,omitempty"`
}

type InteractionMessage struct {
	Type          string             `json:"type"`
	InteractionID string             `json:"interaction_id"`
	Title         string             `json:"title,omitempty"`
	Message       string             `json:"message,omitempty"`
	Fields        []InteractionField `json:"fields,omitempty"`
	Data          json.RawMessage    `json:"data,omitempty"`
}

func (m InteractionMessage) Validate() error {
	if strings.TrimSpace(m.InteractionID) == "" || len(m.InteractionID) > 256 {
		return fmt.Errorf("interaction id is required")
	}
	switch m.Type {
	case "action", "prompt", "secret_prompt", "navigate", "display", "status", "complete", "error":
	default:
		return fmt.Errorf("unsupported interaction message type")
	}
	seen := map[string]bool{}
	fields := Schema{Fields: make([]Field, 0, len(m.Fields))}
	for _, field := range m.Fields {
		if !identifierPattern.MatchString(field.Key) || strings.TrimSpace(field.Label) == "" || seen[field.Key] {
			return fmt.Errorf("interaction field keys and labels must be nonempty and unique")
		}
		seen[field.Key] = true
		if field.Control != "text" && field.Control != "secret" && field.Control != "number" && field.Control != "boolean" && field.Control != "select" && field.Control != "multi-select" && field.Control != "textarea" {
			return fmt.Errorf("interaction field control is invalid")
		}
		if (field.Control == "select" || field.Control == "multi-select") && len(field.Options) == 0 {
			return fmt.Errorf("interaction select field requires options")
		}
		fields.Fields = append(fields.Fields, Field{Key: field.Key, Control: field.Control, Label: field.Label, Description: field.Description, Required: field.Required, Options: field.Options})
	}
	if len(m.Data) > 0 && !json.Valid(m.Data) {
		return fmt.Errorf("interaction data is invalid JSON")
	}
	if len(m.Data) > 64<<10 || len(m.Message) > 4096 || len(m.Title) > 512 {
		return fmt.Errorf("interaction message exceeds size limit")
	}
	if err := fields.Validate(); err != nil {
		return fmt.Errorf("interaction fields are invalid")
	}
	return nil
}

func (d Descriptor) Validate() error {
	if !identifierPattern.MatchString(d.ID) {
		return fmt.Errorf("adapter id is invalid")
	}
	if strings.TrimSpace(d.Name) == "" || strings.TrimSpace(d.Version) == "" {
		return fmt.Errorf("adapter name and version are required")
	}
	if d.ProtocolVersion != Version {
		return fmt.Errorf("unsupported protocol version %d", d.ProtocolVersion)
	}
	if err := validateBranding(d.Branding); err != nil {
		return err
	}
	if err := d.InputSchema.Validate(); err != nil {
		return fmt.Errorf("input schema: %w", err)
	}
	if err := d.ConfigurationSchema.Validate(); err != nil {
		return fmt.Errorf("configuration schema: %w", err)
	}
	seenCapabilities := map[string]bool{}
	for _, capability := range d.Capabilities {
		if !identifierPattern.MatchString(capability) {
			return fmt.Errorf("capability is malformed")
		}
		if seenCapabilities[capability] {
			return fmt.Errorf("capability is duplicated")
		}
		seenCapabilities[capability] = true
	}
	if !seenCapabilities[CapabilityResolve] && !seenCapabilities[CapabilityResolveWorkflow] {
		return fmt.Errorf("adapter must declare resolve capability")
	}
	if len(d.MediaTypes) == 0 {
		return fmt.Errorf("adapter must declare at least one media type")
	}
	if len(d.ResourceTypes) > maxDescriptorResourceTypes {
		return fmt.Errorf("descriptor exceeds resource type limit")
	}
	seen := map[string]bool{}
	for _, rt := range d.ResourceTypes {
		if !identifierPattern.MatchString(rt.Type) || seen[rt.Type] {
			return fmt.Errorf("resource type is malformed or duplicated")
		}
		seen[rt.Type] = true
		parents := map[string]bool{}
		for _, parent := range rt.ParentTypes {
			if !identifierPattern.MatchString(parent) || parents[parent] {
				return fmt.Errorf("resource parent type is malformed or duplicated")
			}
			parents[parent] = true
		}
		if err := rt.ConfigurationSchema.Validate(); err != nil {
			return fmt.Errorf("resource schema: %w", err)
		}
	}
	for _, rt := range d.ResourceTypes {
		for _, parent := range rt.ParentTypes {
			if !seen[parent] {
				return fmt.Errorf("resource parent type is not declared")
			}
		}
	}
	resourceParents := make(map[string][]string, len(d.ResourceTypes))
	for _, rt := range d.ResourceTypes {
		resourceParents[rt.Type] = append([]string(nil), rt.ParentTypes...)
	}
	visitingResources := map[string]bool{}
	visitedResources := map[string]bool{}
	var visitResource func(string) bool
	visitResource = func(resourceType string) bool {
		if visitingResources[resourceType] {
			return false
		}
		if visitedResources[resourceType] {
			return true
		}
		visitingResources[resourceType] = true
		for _, parentType := range resourceParents[resourceType] {
			if !visitResource(parentType) {
				return false
			}
		}
		delete(visitingResources, resourceType)
		visitedResources[resourceType] = true
		return true
	}
	for resourceType := range resourceParents {
		if !visitResource(resourceType) {
			return fmt.Errorf("resource parent declarations contain a cycle")
		}
	}
	seenMedia := map[string]bool{}
	for _, mt := range d.MediaTypes {
		if !mediaTypePattern.MatchString(mt) || seenMedia[mt] {
			return fmt.Errorf("media type is malformed or duplicated")
		}
		seenMedia[mt] = true
	}
	return nil
}

func validateBranding(branding *Branding) error {
	if branding == nil || branding.Icon == nil {
		return nil
	}
	icon := branding.Icon
	if icon.MediaType != "image/png" {
		return fmt.Errorf("adapter branding icon media type is unsupported")
	}
	if len(icon.Data) == 0 || len(icon.Data) > maxBrandIconBytes {
		return fmt.Errorf("adapter branding icon size is invalid")
	}
	config, err := png.DecodeConfig(bytes.NewReader(icon.Data))
	if err != nil {
		return fmt.Errorf("adapter branding icon is invalid PNG")
	}
	if config.Width < 1 || config.Height < 1 || config.Width > maxBrandIconDimension || config.Height > maxBrandIconDimension {
		return fmt.Errorf("adapter branding icon dimensions are invalid")
	}
	if _, err = png.Decode(bytes.NewReader(icon.Data)); err != nil {
		return fmt.Errorf("adapter branding icon is invalid PNG")
	}
	return nil
}

func ValidateResourceRef(ref *ResourceRef) error {
	seen := map[*ResourceRef]bool{}
	depth := 0
	for current := ref; current != nil; current = current.Parent {
		depth++
		if depth > 64 {
			return fmt.Errorf("resource hierarchy exceeds limit")
		}
		if seen[current] {
			return fmt.Errorf("resource hierarchy contains a cycle")
		}
		seen[current] = true
		if !identifierPattern.MatchString(current.Type) || strings.TrimSpace(current.ID) == "" || len(current.ID) > 4096 {
			return fmt.Errorf("resource type and id are required")
		}
	}
	return nil
}

// ValidateResourceRefForDescriptor verifies that every resource type and edge
// in a chain was declared by the adapter descriptor. Type names remain opaque.
func ValidateResourceRefForDescriptor(d Descriptor, ref *ResourceRef) error {
	if err := ValidateResourceRef(ref); err != nil {
		return err
	}
	if ref == nil {
		return nil
	}
	declared := make(map[string]ResourceType, len(d.ResourceTypes))
	for _, item := range d.ResourceTypes {
		declared[item.Type] = item
	}
	for current := ref; current != nil; current = current.Parent {
		child, ok := declared[current.Type]
		if !ok {
			return fmt.Errorf("resource type is not declared")
		}
		if current.Parent == nil {
			// ParentTypes declares permitted edges, not a requirement that a
			// resource of this type must have a parent. Parentless roots are
			// valid; every supplied edge is still checked below.
			continue
		}
		allowed := false
		for _, parentType := range child.ParentTypes {
			if current.Parent.Type == parentType {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("resource parent relationship is invalid")
		}
	}
	return nil
}

func (s Schema) Validate() error {
	if len(s.Fields) > maxSchemaFields {
		return fmt.Errorf("schema exceeds field limit")
	}
	seen := map[string]Field{}
	for _, f := range s.Fields {
		if !identifierPattern.MatchString(f.Key) {
			return fmt.Errorf("field key is malformed")
		}
		if _, duplicate := seen[f.Key]; duplicate {
			return fmt.Errorf("field keys must be nonempty and unique")
		}
		seen[f.Key] = f
		if strings.TrimSpace(f.Label) == "" {
			return fmt.Errorf("field %q label is required", f.Key)
		}
		switch f.Control {
		case "text", "secret", "number", "boolean", "select", "multi-select", "textarea", "action", "status":
		default:
			return fmt.Errorf("field %q has unsupported control", f.Key)
		}
		if (f.Control == "select" || f.Control == "multi-select") && len(f.Options) == 0 {
			return fmt.Errorf("field %q requires options", f.Key)
		}
		if len(f.Options) > maxSchemaOptions {
			return fmt.Errorf("field %q exceeds option limit", f.Key)
		}
		if f.Control == "secret" && len(f.Default) > 0 {
			return fmt.Errorf("field %q cannot declare a secret default", f.Key)
		}
		if f.Control != "select" && f.Control != "multi-select" && len(f.Options) != 0 {
			return fmt.Errorf("field %q has options for a control that does not use them", f.Key)
		}
		if f.Control == "action" || f.Control == "status" {
			if f.Required || len(f.Default) != 0 || f.Constraints != nil || f.Persistence != nil {
				return fmt.Errorf("field %q has editable properties on a display-only control", f.Key)
			}
		}
		if f.Persistence != nil {
			if err := validateFieldPersistence(*f.Persistence); err != nil {
				return fmt.Errorf("field %q has invalid persistence policy", f.Key)
			}
		}
		if f.Constraints != nil {
			c := f.Constraints
			if (c.Min != nil || c.Max != nil) && f.Control != "number" {
				return fmt.Errorf("field %q has numeric constraints for a non-number control", f.Key)
			}
			if (c.MinLength != nil || c.MaxLength != nil || c.Pattern != "") && f.Control != "text" && f.Control != "secret" && f.Control != "textarea" {
				return fmt.Errorf("field %q has string constraints for a non-string control", f.Key)
			}
			if (c.MinItems != nil || c.MaxItems != nil) && f.Control != "multi-select" {
				return fmt.Errorf("field %q has item constraints for a non-multi-select control", f.Key)
			}
			if c.Min != nil && (math.IsNaN(*c.Min) || math.IsInf(*c.Min, 0)) || c.Max != nil && (math.IsNaN(*c.Max) || math.IsInf(*c.Max, 0)) {
				return fmt.Errorf("field %q has non-finite numeric constraints", f.Key)
			}
			if c.Min != nil && c.Max != nil && *c.Min > *c.Max {
				return fmt.Errorf("field %q has inverted numeric bounds", f.Key)
			}
			if c.MinLength != nil && *c.MinLength < 0 || c.MaxLength != nil && *c.MaxLength < 0 || c.MinLength != nil && c.MaxLength != nil && *c.MinLength > *c.MaxLength {
				return fmt.Errorf("field %q has invalid length constraints", f.Key)
			}
			if c.Pattern != "" {
				if _, err := regexp.Compile(c.Pattern); err != nil {
					return fmt.Errorf("field %q has invalid pattern", f.Key)
				}
			}
			if c.MinItems != nil && *c.MinItems < 0 || c.MaxItems != nil && *c.MaxItems < 0 || c.MinItems != nil && c.MaxItems != nil && *c.MinItems > *c.MaxItems {
				return fmt.Errorf("field %q has invalid item constraints", f.Key)
			}
		}
		if f.Control == "secret" && len(f.Default) > 0 {
			return fmt.Errorf("field %q cannot define a secret default", f.Key)
		}
		if len(f.Default) > 0 && !json.Valid(f.Default) {
			return fmt.Errorf("field %q has invalid default", f.Key)
		}
		if len(f.Default) > 0 {
			if err := validateValue(f, f.Default); err != nil {
				return fmt.Errorf("field %q has an invalid default", f.Key)
			}
		}
		if (f.Control == "select" || f.Control == "multi-select") && duplicateOptions(f.Options) {
			return fmt.Errorf("field %q has duplicate options", f.Key)
		}
		for _, option := range f.Options {
			if strings.TrimSpace(option.Label) == "" {
				return fmt.Errorf("field %q has an invalid option label", f.Key)
			}
		}
	}
	graph := map[string][]string{}
	for _, f := range s.Fields {
		if len(f.VisibleWhen) == 0 {
			continue
		}
		refs, err := conditionReferences(f.VisibleWhen)
		if err != nil {
			return fmt.Errorf("field %q has invalid visibility condition", f.Key)
		}
		if err := validateConditionLiterals(f.VisibleWhen, seen); err != nil {
			return fmt.Errorf("field %q has invalid visibility comparison", f.Key)
		}
		for _, ref := range refs {
			dep, exists := seen[ref]
			if !exists || dep.Control == "action" || dep.Control == "status" || dep.Control == "secret" || ref == f.Key {
				return fmt.Errorf("field %q visibility condition references an invalid field", f.Key)
			}
			graph[f.Key] = append(graph[f.Key], ref)
		}
	}
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) bool
	visit = func(key string) bool {
		if visiting[key] {
			return false
		}
		if visited[key] {
			return true
		}
		visiting[key] = true
		for _, dep := range graph[key] {
			if !visit(dep) {
				return false
			}
		}
		delete(visiting, key)
		visited[key] = true
		return true
	}
	for key := range graph {
		if !visit(key) {
			return fmt.Errorf("visibility conditions contain a cycle")
		}
	}
	return nil
}

func validateFieldPersistence(p FieldPersistence) error {
	switch p.Mode {
	case PersistenceForbidden, PersistenceOptional, PersistenceRequired:
	default:
		return fmt.Errorf("unknown persistence mode")
	}
	switch p.Target.Scope {
	case PersistencePlugin, PersistenceCurrent:
		if p.Target.Resource != nil {
			return fmt.Errorf("scope does not accept a resource reference")
		}
	case PersistenceResource:
		if p.Target.Resource == nil || ValidateResourceRef(p.Target.Resource) != nil {
			return fmt.Errorf("resource scope requires a valid reference")
		}
	default:
		return fmt.Errorf("unknown persistence scope")
	}
	return nil
}

func duplicateOptions(options []Option) bool {
	seen := map[string]bool{}
	for _, option := range options {
		key, err := canonicalJSON(option.Value)
		if err != nil || seen[key] {
			return true
		}
		seen[key] = true
	}
	return false
}

func conditionReferences(raw json.RawMessage) ([]string, error) {
	nodes := 0
	return conditionReferencesAt(raw, 0, &nodes)
}

func conditionReferencesAt(raw json.RawMessage, depth int, nodes *int) ([]string, error) {
	if depth > maxVisibilityDepth {
		return nil, fmt.Errorf("visibility condition is too deep")
	}
	*nodes = *nodes + 1
	if *nodes > maxVisibilityNodes {
		return nil, fmt.Errorf("visibility condition is too large")
	}
	var node map[string]json.RawMessage
	if json.Unmarshal(raw, &node) != nil || node == nil {
		return nil, fmt.Errorf("condition must be an object")
	}
	if value, ok := node["all"]; ok {
		if len(node) != 1 {
			return nil, fmt.Errorf("all must be the only condition operator")
		}
		var children []json.RawMessage
		if json.Unmarshal(value, &children) != nil || len(children) == 0 {
			return nil, fmt.Errorf("all requires conditions")
		}
		return conditionChildReferences(children, depth, nodes)
	}
	if value, ok := node["any"]; ok {
		if len(node) != 1 {
			return nil, fmt.Errorf("any must be the only condition operator")
		}
		var children []json.RawMessage
		if json.Unmarshal(value, &children) != nil || len(children) == 0 {
			return nil, fmt.Errorf("any requires conditions")
		}
		return conditionChildReferences(children, depth, nodes)
	}
	fieldRaw, hasField := node["field"]
	if !hasField || len(node) != 2 {
		return nil, fmt.Errorf("predicate requires field and one operator")
	}
	var field string
	if json.Unmarshal(fieldRaw, &field) != nil || !identifierPattern.MatchString(field) {
		return nil, fmt.Errorf("predicate field is invalid")
	}
	if value, ok := node["equals"]; ok {
		if !json.Valid(value) {
			return nil, fmt.Errorf("equals value is invalid")
		}
		return []string{field}, nil
	}
	if value, ok := node["not_equals"]; ok {
		if !json.Valid(value) {
			return nil, fmt.Errorf("not_equals value is invalid")
		}
		return []string{field}, nil
	}
	if value, ok := node["truthy"]; ok {
		var truth bool
		if json.Unmarshal(value, &truth) != nil {
			return nil, fmt.Errorf("truthy must be boolean")
		}
		return []string{field}, nil
	}
	return nil, fmt.Errorf("unknown predicate")
}

func conditionChildReferences(children []json.RawMessage, depth int, nodes *int) ([]string, error) {
	var refs []string
	for _, child := range children {
		childRefs, err := conditionReferencesAt(child, depth+1, nodes)
		if err != nil {
			return nil, err
		}
		refs = append(refs, childRefs...)
	}
	return refs, nil
}

func validateConditionLiterals(raw json.RawMessage, fields map[string]Field) error {
	var node map[string]json.RawMessage
	if json.Unmarshal(raw, &node) != nil || node == nil {
		return fmt.Errorf("condition must be an object")
	}
	for _, operator := range []string{"all", "any"} {
		if childRaw, ok := node[operator]; ok {
			var children []json.RawMessage
			if json.Unmarshal(childRaw, &children) != nil {
				return fmt.Errorf("invalid condition list")
			}
			for _, child := range children {
				if err := validateConditionLiterals(child, fields); err != nil {
					return err
				}
			}
			return nil
		}
	}
	var key string
	if json.Unmarshal(node["field"], &key) != nil {
		return fmt.Errorf("invalid condition field")
	}
	field, exists := fields[key]
	if !exists {
		return fmt.Errorf("unknown condition field")
	}
	if _, ok := node["truthy"]; ok && field.Control != "boolean" {
		return fmt.Errorf("truthy requires a boolean field")
	}
	for _, operator := range []string{"equals", "not_equals"} {
		if value, ok := node[operator]; ok {
			if err := validateValue(field, value); err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

func ValidateObject(raw json.RawMessage) error {
	if len(raw) == 0 {
		return fmt.Errorf("input object is required")
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return fmt.Errorf("input must be a JSON object")
	}
	return nil
}

// ValidateObjectAgainstSchema validates a raw object using the same field
// semantics as configuration. Errors identify fields but never echo values.
func ValidateObjectAgainstSchema(schema Schema, raw json.RawMessage) error {
	if err := ValidateObject(raw); err != nil {
		return err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return fmt.Errorf("input must be a JSON object")
	}
	return ValidateValues(schema, values)
}

// ValidateProvidedValues validates supplied keys but does not require all
// required fields, which may be inherited from a less specific scope.
func ValidateProvidedValues(schema Schema, values map[string]json.RawMessage) error {
	return validateValues(schema, values, false)
}

func ValidateValues(schema Schema, values map[string]json.RawMessage) error {
	return validateValues(schema, values, true)
}

func validateValues(schema Schema, values map[string]json.RawMessage, checkRequired bool) error {
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("invalid schema")
	}
	fields := map[string]Field{}
	for _, f := range schema.Fields {
		if f.Control != "action" && f.Control != "status" {
			fields[f.Key] = f
		}
	}
	for key, raw := range values {
		field, ok := fields[key]
		if !ok {
			return fmt.Errorf("unknown configuration key %q", key)
		}
		if err := validateValue(field, raw); err != nil {
			return fmt.Errorf("invalid configuration value for %q", key)
		}
	}
	withDefaults := ApplyDefaults(schema, values)
	for key := range values {
		visible, err := IsVisible(schema, key, withDefaults)
		if err != nil {
			return fmt.Errorf("invalid field visibility")
		}
		if !visible {
			return fmt.Errorf("value supplied for hidden field %q", key)
		}
	}
	for key, field := range fields {
		visible, err := IsVisible(schema, key, withDefaults)
		if err != nil {
			return fmt.Errorf("invalid field visibility")
		}
		if checkRequired && visible && field.Required {
			if _, ok := values[key]; !ok && len(field.Default) == 0 {
				return fmt.Errorf("required configuration key %q is missing", key)
			}
		}
	}
	return nil
}

// ApplyDefaults returns a copy of values with visible field defaults filled in.
// Defaults are evaluated in dependency order so conditionals work regardless
// of field declaration order. The supplied map is never mutated.
func ApplyDefaults(schema Schema, values map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(values)+len(schema.Fields))
	for key, raw := range values {
		out[key] = append(json.RawMessage(nil), raw...)
	}
	byKey := map[string]Field{}
	for _, field := range schema.Fields {
		byKey[field.Key] = field
	}
	visited := map[string]bool{}
	var ordered []Field
	var add func(Field)
	add = func(field Field) {
		if visited[field.Key] {
			return
		}
		visited[field.Key] = true
		refs, _ := conditionReferences(field.VisibleWhen)
		for _, ref := range refs {
			if dep, ok := byKey[ref]; ok {
				add(dep)
			}
		}
		ordered = append(ordered, field)
	}
	for _, field := range schema.Fields {
		add(field)
	}
	for _, field := range ordered {
		if len(field.Default) == 0 {
			continue
		}
		if _, exists := out[field.Key]; exists {
			continue
		}
		visible, err := IsVisible(schema, field.Key, out)
		if err == nil && visible {
			out[field.Key] = append(json.RawMessage(nil), field.Default...)
		}
	}
	return out
}

// IsVisible evaluates a field's validated declarative visibility condition.
// An absent condition makes the field visible.
func IsVisible(schema Schema, fieldKey string, values map[string]json.RawMessage) (bool, error) {
	if err := schema.Validate(); err != nil {
		return false, err
	}
	for _, field := range schema.Fields {
		if field.Key != fieldKey {
			continue
		}
		if len(field.VisibleWhen) == 0 {
			return true, nil
		}
		return evaluateCondition(field.VisibleWhen, values)
	}
	return false, fmt.Errorf("unknown schema field")
}

func evaluateCondition(raw json.RawMessage, values map[string]json.RawMessage) (bool, error) {
	var node map[string]json.RawMessage
	if err := json.Unmarshal(raw, &node); err != nil || node == nil {
		return false, fmt.Errorf("invalid condition")
	}
	if childrenRaw, ok := node["all"]; ok {
		var children []json.RawMessage
		if err := json.Unmarshal(childrenRaw, &children); err != nil {
			return false, fmt.Errorf("invalid all condition")
		}
		for _, child := range children {
			ok, err := evaluateCondition(child, values)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	if childrenRaw, ok := node["any"]; ok {
		var children []json.RawMessage
		if err := json.Unmarshal(childrenRaw, &children); err != nil {
			return false, fmt.Errorf("invalid any condition")
		}
		for _, child := range children {
			ok, err := evaluateCondition(child, values)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}
	var field string
	if err := json.Unmarshal(node["field"], &field); err != nil {
		return false, fmt.Errorf("invalid condition field")
	}
	value, present := values[field]
	if expected, ok := node["equals"]; ok {
		if !present {
			return false, nil
		}
		left, err := canonicalJSONRaw(value)
		if err != nil {
			return false, err
		}
		right, err := canonicalJSONRaw(expected)
		if err != nil {
			return false, err
		}
		return left == right, nil
	}
	if expected, ok := node["not_equals"]; ok {
		if !present {
			return true, nil
		}
		left, err := canonicalJSONRaw(value)
		if err != nil {
			return false, err
		}
		right, err := canonicalJSONRaw(expected)
		if err != nil {
			return false, err
		}
		return left != right, nil
	}
	var wanted bool
	if err := json.Unmarshal(node["truthy"], &wanted); err != nil {
		return false, fmt.Errorf("invalid truthy condition")
	}
	truth := false
	if present {
		truth = jsonTruthy(value)
	}
	return truth == wanted, nil
}

func jsonTruthy(raw json.RawMessage) bool {
	var value any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if d.Decode(&value) != nil {
		return false
	}
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case json.Number:
		n, err := v.Float64()
		return err == nil && n != 0 && !math.IsNaN(n)
	default:
		return true
	}
}

func canonicalJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return canonicalJSONRaw(raw)
}

func canonicalJSONRaw(raw json.RawMessage) (string, error) {
	var value any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return "", err
	}
	return canonicalJSONValue(value)
}

func canonicalJSONValue(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "null", nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case string:
		encoded, err := json.Marshal(v)
		return string(encoded), err
	case json.Number:
		return canonicalJSONNumber(string(v))
	case float64:
		return canonicalJSONValue(json.Number(strconv.FormatFloat(v, 'g', -1, 64)))
	case []any:
		parts := make([]string, len(v))
		for i, item := range v {
			part, err := canonicalJSONValue(item)
			if err != nil {
				return "", err
			}
			parts[i] = part
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			encodedKey, err := json.Marshal(key)
			if err != nil {
				return "", err
			}
			encodedValue, err := canonicalJSONValue(v[key])
			if err != nil {
				return "", err
			}
			parts = append(parts, string(encodedKey)+":"+encodedValue)
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return canonicalJSONRaw(raw)
	}
}

// canonicalJSONNumber normalizes a decimal number without expanding powers of
// ten. Exponents can be very large even in short JSON values, so converting
// 1e1000000000 to a rational would allocate storage proportional to the value
// rather than to the input. The coefficient/exponent pair remains exact and
// comparable while requiring memory proportional only to the JSON token.
func canonicalJSONNumber(raw string) (string, error) {
	if !json.Valid([]byte(raw)) {
		return "", fmt.Errorf("invalid JSON number")
	}
	sign := ""
	if strings.HasPrefix(raw, "-") {
		sign = "-"
		raw = raw[1:]
	}
	mantissa, exponentText, hasExponent := raw, "", false
	if index := strings.IndexAny(raw, "eE"); index >= 0 {
		mantissa, exponentText, hasExponent = raw[:index], raw[index+1:], true
	}
	exponent := new(big.Int)
	if hasExponent {
		if _, ok := exponent.SetString(exponentText, 10); !ok {
			return "", fmt.Errorf("invalid JSON number exponent")
		}
	}
	fractionDigits := int64(0)
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		fractionDigits = int64(len(mantissa) - dot - 1)
		mantissa = mantissa[:dot] + mantissa[dot+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return "number:0", nil
	}
	trimmed := strings.TrimRight(digits, "0")
	removedZeros := len(digits) - len(trimmed)
	exponent.Sub(exponent, big.NewInt(fractionDigits))
	exponent.Add(exponent, big.NewInt(int64(removedZeros)))
	return "number:" + sign + trimmed + "e" + exponent.String(), nil
}

func validateValue(f Field, raw json.RawMessage) error {
	if !json.Valid(raw) {
		return fmt.Errorf("invalid JSON")
	}
	var v any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return err
	}
	switch f.Control {
	case "text", "secret", "textarea":
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("expected string")
		}
		if f.Constraints != nil {
			c := f.Constraints
			if c.MinLength != nil && len([]rune(s)) < *c.MinLength || c.MaxLength != nil && len([]rune(s)) > *c.MaxLength {
				return fmt.Errorf("length constraint")
			}
			if c.Pattern != "" && !regexp.MustCompile(c.Pattern).MatchString(s) {
				return fmt.Errorf("pattern constraint")
			}
		}
	case "number":
		n, ok := v.(json.Number)
		if !ok {
			return fmt.Errorf("expected number")
		}
		value, err := n.Float64()
		if err != nil {
			return err
		}
		if f.Constraints != nil && (f.Constraints.Min != nil && value < *f.Constraints.Min || f.Constraints.Max != nil && value > *f.Constraints.Max) {
			return fmt.Errorf("range constraint")
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("expected boolean")
		}
	case "select":
		if !optionContains(f.Options, v) {
			return fmt.Errorf("invalid option")
		}
	case "multi-select":
		arr, ok := v.([]any)
		if !ok {
			return fmt.Errorf("expected array")
		}
		if f.Constraints != nil && (f.Constraints.MinItems != nil && len(arr) < *f.Constraints.MinItems || f.Constraints.MaxItems != nil && len(arr) > *f.Constraints.MaxItems) {
			return fmt.Errorf("item count constraint")
		}
		seen := make(map[string]bool, len(arr))
		for _, item := range arr {
			if !optionContains(f.Options, item) {
				return fmt.Errorf("invalid option")
			}
			identity, err := canonicalJSON(item)
			if err != nil || seen[identity] {
				return fmt.Errorf("duplicate option")
			}
			seen[identity] = true
		}
	}
	return nil
}

func optionContains(options []Option, candidate any) bool {
	a, _ := canonicalJSON(candidate)
	for _, option := range options {
		b, _ := canonicalJSON(option.Value)
		if a == b {
			return true
		}
	}
	return false
}

func ValidateMediaSource(media MediaSource, supported []string) error {
	if media.Type == "" || media.ManifestURL == "" {
		return fmt.Errorf("media type and manifest URL are required")
	}
	ok := false
	for _, v := range supported {
		if v == media.Type {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("unsupported media type")
	}
	u, err := url.ParseRequestURI(media.ManifestURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return fmt.Errorf("invalid media manifest URL")
	}
	seenHeaders := map[string]bool{}
	for key, value := range media.Headers {
		canonical, ok := canonicalHeaderName(key)
		if !ok || seenHeaders[canonical] || unsafeTransportHeader(canonical) || !validHeaderValue(value) {
			return fmt.Errorf("invalid media header")
		}
		seenHeaders[canonical] = true
	}
	if err := validateRequestPolicy(media.RequestPolicy); err != nil {
		return fmt.Errorf("invalid request policy: %w", err)
	}
	if classification := media.SourceURIClassification(); classification != "sensitive" && classification != "public" {
		return fmt.Errorf("invalid source URI archive classification")
	}
	if media.RefreshPolicy != nil {
		if media.RefreshPolicy.RefreshBeforeSeconds < 0 || media.RefreshPolicy.RefreshBeforeSeconds > 31536000 {
			return fmt.Errorf("invalid media refresh policy")
		}
		seenStatus := map[int]bool{}
		for _, status := range media.RefreshPolicy.OnHTTPStatus {
			if status < 100 || status > 599 || seenStatus[status] {
				return fmt.Errorf("invalid media refresh status policy")
			}
			seenStatus[status] = true
		}
	}
	return nil
}

func canonicalHeaderName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return "", false
		}
	}
	return strings.ToLower(name), true
}

func unsafeTransportHeader(canonicalLower string) bool {
	switch canonicalLower {
	case "host", "content-length", "transfer-encoding", "connection", "proxy-connection", "keep-alive", "te", "trailer", "upgrade":
		return true
	default:
		return strings.HasPrefix(canonicalLower, "proxy-")
	}
}

func ValidateWorkflowResult(result ResolveWorkflowResult, expectedWorkflowID string, supportedMedia []string) error {
	if strings.TrimSpace(result.WorkflowID) == "" || result.WorkflowID != expectedWorkflowID {
		return fmt.Errorf("adapter returned an invalid workflow id")
	}
	if err := ValidateResourceRef(result.Resource); err != nil {
		return fmt.Errorf("adapter returned an invalid resource")
	}
	switch result.State {
	case "resolved":
		if result.Media == nil || result.Challenge != nil {
			return fmt.Errorf("resolved workflow result is inconsistent")
		}
		return ValidateMediaSource(*result.Media, supportedMedia)
	case "resource_discovered":
		if result.Resource == nil || result.Media != nil || result.Challenge != nil {
			return fmt.Errorf("resource discovery result is inconsistent")
		}
	case "configuration_required", "interaction_required":
		if result.Challenge == nil || result.Media != nil {
			return fmt.Errorf("workflow challenge result is inconsistent")
		}
		if err := result.Challenge.Schema.Validate(); err != nil {
			return fmt.Errorf("workflow challenge schema is invalid")
		}
		if result.Challenge.Prompt != nil {
			if err := result.Challenge.Prompt.Validate(); err != nil {
				return fmt.Errorf("workflow prompt is invalid")
			}
		}
	case "error":
		return fmt.Errorf("adapter workflow failed")
	default:
		return fmt.Errorf("adapter returned an unknown workflow state")
	}
	return nil
}

func validateRequestPolicy(policy *RequestPolicy) error {
	if policy == nil || policy.HeaderForwarding == nil {
		return nil
	}
	forwarding := policy.HeaderForwarding
	mode := forwarding.Mode
	if mode == "" {
		mode = HeaderForwardingSameOrigin
	}
	switch mode {
	case HeaderForwardingSameOrigin:
		if len(forwarding.Origins) != 0 {
			return fmt.Errorf("same_origin mode cannot declare extra origins")
		}
	case HeaderForwardingAllowlist:
		if len(forwarding.Origins) == 0 {
			return fmt.Errorf("allowlist mode requires at least one origin")
		}
		seen := map[mediaOrigin]bool{}
		for _, raw := range forwarding.Origins {
			origin, err := parseAllowedOrigin(raw)
			if err != nil {
				return fmt.Errorf("invalid allowed origin %q", raw)
			}
			if seen[origin] {
				return fmt.Errorf("allowed origins must be unique")
			}
			seen[origin] = true
		}
	default:
		return fmt.Errorf("unsupported header forwarding mode %q", forwarding.Mode)
	}
	return nil
}

func parseAllowedOrigin(raw string) (mediaOrigin, error) {
	if raw == "" || strings.ContainsAny(raw, "\\\r\n\t #?") {
		return mediaOrigin{}, fmt.Errorf("origin must be an HTTP(S) origin URL")
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || strings.Contains(raw, "#") {
		return mediaOrigin{}, fmt.Errorf("origin must not include path, query, or fragment")
	}
	return originFromURL(u)
}

func parseMediaOrigin(raw string) (mediaOrigin, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil {
		return mediaOrigin{}, fmt.Errorf("invalid media URL")
	}
	return originFromURL(u)
}

func originFromURL(u *url.URL) (mediaOrigin, error) {
	if u == nil || u.Opaque != "" || u.User != nil || !strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https") || u.Host == "" || u.Hostname() == "" || strings.HasSuffix(u.Host, ":") {
		return mediaOrigin{}, fmt.Errorf("URL does not have a valid HTTP(S) origin")
	}
	scheme := strings.ToLower(u.Scheme)
	hostname := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 0 || portNumber > 65535 {
			return mediaOrigin{}, fmt.Errorf("URL port is invalid")
		}
		port = strconv.Itoa(portNumber)
	}
	return mediaOrigin{scheme: scheme, hostname: hostname, port: port}, nil
}

func validHeaderValue(value string) bool {
	for _, r := range value {
		if r == 0x7f || r < 0x20 && r != '\t' {
			return false
		}
	}
	return true
}
