package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/storageproto"
)

const (
	StorageProviderEndpoint  = "/api/runtime/storage/provider"
	StorageProvidersPrefix   = "/api/runtime/storage/providers/"
	StorageInstancesEndpoint = "/api/runtime/storage/instances"
	StorageInstancesPrefix   = StorageInstancesEndpoint + "/"
	maxStorageConfigBody     = 64 << 10
	maxStorageConfigFields   = 64
)

var storageConfigKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var storageInstanceIDPattern = regexp.MustCompile(`^si_(?:[0-9a-f]{32}|legacy_[0-9a-f]{24})$`)
var storageSetIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// StorageProviderStatus is a bounded projection of the active primary and
// immutable providers available for configuration. It contains no artifact
// paths, process identity, raw errors, endpoint URL, or secret value.
type StorageProviderStatus struct {
	Primary   PrimaryStorageStatus     `json:"primary"`
	Providers []StorageProviderSummary `json:"providers"`
}

type PrimaryStorageStatus struct {
	Kind       string `json:"kind"`
	ProviderID string `json:"provider_id,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	Version    string `json:"version,omitempty"`
	State      string `json:"state"`
}

type StorageProviderSummary struct {
	ID                   string              `json:"id"`
	Name                 string              `json:"name"`
	Version              string              `json:"version"`
	Distribution         string              `json:"distribution,omitempty"`
	ConfigurationManaged bool                `json:"configuration_managed,omitempty"`
	Uninstallable        bool                `json:"uninstallable"`
	Configured           bool                `json:"configured"`
	Active               bool                `json:"active"`
	Health               string              `json:"health"`
	ConfigurationSchema  storageproto.Schema `json:"configuration_schema"`
}

type StorageConfigRequest struct {
	Values  map[string]json.RawMessage `json:"values"`
	Secrets map[string]string          `json:"secrets"`
}

// StorageConfigView intentionally returns only non-secret values and names of
// secret fields that have a stored value. Secret values are write-only.
type StorageConfigView struct {
	Values            map[string]json.RawMessage `json:"values"`
	ConfiguredSecrets []string                   `json:"configured_secrets"`
}

type StorageProbeResult struct {
	State string `json:"state"`
}

// StorageInstanceSummary exposes stable instance identity and selected
// immutable configuration identity. It never includes provider credentials.
type StorageInstanceSummary struct {
	ID           string `json:"id"`
	DisplayName  string `json:"display_name"`
	ProviderID   string `json:"provider_id"`
	ProviderName string `json:"provider_name"`
	DesiredSetID string `json:"desired_set_id"`
	Active       bool   `json:"active"`
	Health       string `json:"health"`
}

type StorageInstanceCreateRequest struct {
	DisplayName string                     `json:"display_name"`
	ProviderID  string                     `json:"provider_id"`
	Values      map[string]json.RawMessage `json:"values"`
	Secrets     map[string]string          `json:"secrets"`
}

func (r StorageInstanceCreateRequest) Validate() error {
	if !validStorageDisplayName(r.DisplayName) || !pluginIDPattern.MatchString(r.ProviderID) {
		return errors.New("storage instance request is invalid")
	}
	return (StorageConfigRequest{Values: r.Values, Secrets: r.Secrets}).Validate()
}

func (s StorageInstanceSummary) Validate() error {
	if !storageInstanceIDPattern.MatchString(s.ID) || !safePluginText(s.DisplayName, 128) ||
		!pluginIDPattern.MatchString(s.ProviderID) || !safePluginText(s.ProviderName, 128) ||
		!storageSetIDPattern.MatchString(s.DesiredSetID) ||
		(s.Health != "unknown" && s.Health != "ready" && s.Health != "unavailable") {
		return errors.New("storage instance summary is invalid")
	}
	return nil
}

func validStorageDisplayName(name string) bool {
	if strings.TrimSpace(name) == "" || len(name) > 128 {
		return false
	}
	for _, character := range name {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func (s StorageProviderStatus) Validate() error {
	if s.Primary.Kind != "local" && s.Primary.Kind != "plugin" || s.Primary.State != "ready" && s.Primary.State != "unavailable" {
		return errors.New("storage provider status is invalid")
	}
	if s.Primary.InstanceID != "" && !storageInstanceIDPattern.MatchString(s.Primary.InstanceID) ||
		s.Primary.Kind == "local" && (s.Primary.ProviderID != "" || s.Primary.Version != "") ||
		s.Primary.Kind == "plugin" && (!pluginIDPattern.MatchString(s.Primary.ProviderID) || !safePluginText(s.Primary.Version, 128)) {
		return errors.New("primary storage identity is invalid")
	}
	if len(s.Providers) > 256 {
		return errors.New("storage provider inventory exceeds limit")
	}
	seen := map[string]bool{}
	for _, item := range s.Providers {
		if !pluginIDPattern.MatchString(item.ID) || seen[item.ID] || !safePluginText(item.Name, 128) || !safePluginText(item.Version, 128) || item.Health != "ready" && item.Health != "unknown" && item.Health != "failed" {
			return errors.New("storage provider item is invalid")
		}
		if item.Distribution != "" && item.Distribution != "bundled" && item.Distribution != "registry" {
			return errors.New("storage provider distribution is invalid")
		}
		if item.Distribution == "bundled" && (!item.ConfigurationManaged || item.Uninstallable) {
			return errors.New("bundled storage provider management policy is invalid")
		}
		seen[item.ID] = true
		if storageproto.ValidateSchema(item.ConfigurationSchema) != nil {
			return errors.New("storage provider schema is invalid")
		}
	}
	return nil
}

func (c StorageConfigRequest) Validate() error {
	if len(c.Values) > maxStorageConfigFields || len(c.Secrets) > maxStorageConfigFields || len(c.Values)+len(c.Secrets) > maxStorageConfigFields {
		return errors.New("storage configuration exceeds field limit")
	}
	for key, value := range c.Values {
		if !storageConfigKeyPattern.MatchString(key) || len(value) == 0 || len(value) > 16<<10 || !json.Valid(value) {
			return errors.New("storage configuration value is invalid")
		}
	}
	for key, value := range c.Secrets {
		if !storageConfigKeyPattern.MatchString(key) || len(value) > 16<<10 || !utf8.ValidString(value) {
			return errors.New("storage configuration secret is invalid")
		}
	}
	return nil
}

func (c StorageConfigView) Validate() error {
	if len(c.Values) > maxStorageConfigFields || len(c.ConfiguredSecrets) > maxStorageConfigFields {
		return errors.New("storage configuration view exceeds limit")
	}
	for key, value := range c.Values {
		if !storageConfigKeyPattern.MatchString(key) || len(value) == 0 || len(value) > 16<<10 || !json.Valid(value) {
			return errors.New("storage configuration view is invalid")
		}
	}
	seen := map[string]bool{}
	for _, key := range c.ConfiguredSecrets {
		if !storageConfigKeyPattern.MatchString(key) || seen[key] {
			return errors.New("storage configuration secret projection is invalid")
		}
		seen[key] = true
	}
	sort.Strings(c.ConfiguredSecrets)
	return nil
}

type StorageController interface {
	StorageStatus(context.Context) (StorageProviderStatus, error)
	StorageConfig(context.Context, string) (StorageConfigView, error)
	ConfigureStorage(context.Context, string, StorageConfigRequest) (StorageConfigView, error)
	ProbeStorage(context.Context, string) (StorageProbeResult, error)
	ActivateStorage(context.Context, string) (StorageProviderStatus, error)
}

type StorageInstanceController interface {
	ListStorageInstances(context.Context) ([]StorageInstanceSummary, error)
	CreateStorageInstance(context.Context, StorageInstanceCreateRequest) (StorageInstanceSummary, error)
	StorageInstanceConfig(context.Context, string) (StorageConfigView, error)
	ConfigureStorageInstance(context.Context, string, StorageConfigRequest) (StorageConfigView, error)
	ProbeStorageInstance(context.Context, string) (StorageProbeResult, error)
	ActivateStorageInstance(context.Context, string) (StorageProviderStatus, error)
}

type storageOperation uint8

const (
	storageStatus storageOperation = iota + 1
	storageConfigRead
	storageConfigWrite
	storageProbe
	storageActivate
)

func routeStorageOperation(method, path string) (storageOperation, string, bool) {
	if method == http.MethodGet && path == StorageProviderEndpoint {
		return storageStatus, "", true
	}
	if !strings.HasPrefix(path, StorageProvidersPrefix) {
		return 0, "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, StorageProvidersPrefix), "/")
	if len(parts) != 2 || !pluginIDPattern.MatchString(parts[0]) {
		return 0, "", false
	}
	switch {
	case parts[1] == "config" && method == http.MethodGet:
		return storageConfigRead, parts[0], true
	case parts[1] == "config" && method == http.MethodPut:
		return storageConfigWrite, parts[0], true
	case parts[1] == "probe" && method == http.MethodPost:
		return storageProbe, parts[0], true
	case parts[1] == "activate" && method == http.MethodPost:
		return storageActivate, parts[0], true
	default:
		return 0, "", false
	}
}

type storageInstanceOperation uint8

const (
	storageInstanceList storageInstanceOperation = iota + 1
	storageInstanceCreate
	storageInstanceConfigRead
	storageInstanceConfigWrite
	storageInstanceProbe
	storageInstanceActivate
)

func routeStorageInstanceOperation(method, path string) (storageInstanceOperation, string, bool) {
	if path == StorageInstancesEndpoint && method == http.MethodGet {
		return storageInstanceList, "", true
	}
	if path == StorageInstancesEndpoint && method == http.MethodPost {
		return storageInstanceCreate, "", true
	}
	if !strings.HasPrefix(path, StorageInstancesPrefix) {
		return 0, "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, StorageInstancesPrefix), "/")
	if len(parts) != 2 || !storageInstanceIDPattern.MatchString(parts[0]) {
		return 0, "", false
	}
	switch {
	case parts[1] == "config" && method == http.MethodGet:
		return storageInstanceConfigRead, parts[0], true
	case parts[1] == "config" && method == http.MethodPut:
		return storageInstanceConfigWrite, parts[0], true
	case parts[1] == "probe" && method == http.MethodPost:
		return storageInstanceProbe, parts[0], true
	case parts[1] == "activate" && method == http.MethodPost:
		return storageInstanceActivate, parts[0], true
	default:
		return 0, "", false
	}
}

func (h *handler) serveStorageInstanceOperation(w http.ResponseWriter, r *http.Request, operation storageInstanceOperation, id string) {
	setResponseHeaders(w)
	mutation := operation != storageInstanceList && operation != storageInstanceConfigRead
	r, authorized := h.authorize(w, r, authn.PermissionStorageManage, mutation)
	if !authorized {
		return
	}
	controller, ok := h.controller.(StorageInstanceController)
	if !ok {
		writeAPIError(w, http.StatusServiceUnavailable, "storage_provider_unavailable", safeControllerErrors["storage_provider_unavailable"].message)
		return
	}
	var value any
	var err error
	switch operation {
	case storageInstanceList:
		value, err = controller.ListStorageInstances(r.Context())
	case storageInstanceCreate:
		var request StorageInstanceCreateRequest
		if err = decodeStorageConfig(w, r, &request); err != nil || request.Validate() != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.CreateStorageInstance(r.Context(), request)
	case storageInstanceConfigRead:
		value, err = controller.StorageInstanceConfig(r.Context(), id)
	case storageInstanceConfigWrite:
		var request StorageConfigRequest
		if err = decodeStorageConfig(w, r, &request); err != nil || request.Validate() != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.ConfigureStorageInstance(r.Context(), id, request)
	case storageInstanceProbe:
		if err := validateEmptyBody(r); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.ProbeStorageInstance(r.Context(), id)
	case storageInstanceActivate:
		if err := validateEmptyBody(r); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.ActivateStorageInstance(r.Context(), id)
	}
	if err != nil {
		writeControllerError(w, err)
		return
	}
	switch item := value.(type) {
	case []StorageInstanceSummary:
		if len(item) > 4096 {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
		for _, summary := range item {
			if summary.Validate() != nil {
				writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
				return
			}
		}
	case StorageInstanceSummary:
		if item.Validate() != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	case StorageConfigView:
		if item.Validate() != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	case StorageProbeResult:
		if item.State != "ready" {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	case StorageProviderStatus:
		if item.Validate() != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	default:
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxResponseBytes {
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	if action, objectID := storageInstanceAudit(operation, id, value); action != "" {
		h.auditMutation(w, r, action, objectID)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(encoded, '\n'))
}

func (h *handler) serveStorageOperation(w http.ResponseWriter, r *http.Request, operation storageOperation, id string) {
	setResponseHeaders(w)
	mutation := operation == storageConfigWrite || operation == storageProbe || operation == storageActivate
	r, authorized := h.authorize(w, r, authn.PermissionStorageManage, mutation)
	if !authorized {
		return
	}
	controller, ok := h.controller.(StorageController)
	if !ok {
		writeAPIError(w, http.StatusServiceUnavailable, "storage_provider_unavailable", safeControllerErrors["storage_provider_unavailable"].message)
		return
	}
	var value any
	var err error
	switch operation {
	case storageStatus:
		value, err = controller.StorageStatus(r.Context())
	case storageConfigRead:
		value, err = controller.StorageConfig(r.Context(), id)
	case storageConfigWrite:
		var config StorageConfigRequest
		if err = decodeStorageConfig(w, r, &config); err != nil || config.Validate() != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.ConfigureStorage(r.Context(), id, config)
	case storageProbe:
		if err := validateEmptyBody(r); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.ProbeStorage(r.Context(), id)
	case storageActivate:
		if err := validateEmptyBody(r); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
		value, err = controller.ActivateStorage(r.Context(), id)
	}
	if err != nil {
		writeControllerError(w, err)
		return
	}
	switch item := value.(type) {
	case StorageProviderStatus:
		if err := item.Validate(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	case StorageConfigView:
		if err := item.Validate(); err != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	case StorageProbeResult:
		if item.State != "ready" {
			writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
			return
		}
	default:
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxResponseBytes {
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	if action, objectID := storageProviderAudit(operation, id); action != "" {
		h.auditMutation(w, r, action, objectID)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(encoded, '\n'))
}

func storageInstanceAudit(operation storageInstanceOperation, id string, value any) (string, string) {
	switch operation {
	case storageInstanceCreate:
		if summary, ok := value.(StorageInstanceSummary); ok {
			return controlplane.AuditStorageInstanceCreated, summary.ID
		}
	case storageInstanceConfigWrite:
		return controlplane.AuditStorageInstanceConfigured, id
	case storageInstanceProbe:
		return controlplane.AuditStorageInstanceProbed, id
	case storageInstanceActivate:
		return controlplane.AuditStorageInstanceActivated, id
	}
	return "", ""
}

func storageProviderAudit(operation storageOperation, id string) (string, string) {
	switch operation {
	case storageConfigWrite:
		return controlplane.AuditStorageProviderConfigured, id
	case storageProbe:
		return controlplane.AuditStorageProviderProbed, id
	case storageActivate:
		return controlplane.AuditStorageProviderActivated, id
	}
	return "", ""
}

func decodeStorageConfig(w http.ResponseWriter, r *http.Request, output any) error {
	if output == nil || r.Body == nil || r.ContentLength > maxStorageConfigBody {
		return errors.New("storage configuration request is invalid")
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("storage configuration request is invalid")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxStorageConfigBody))
	if err != nil {
		return errors.New("storage configuration request is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return errors.New("storage configuration request is invalid")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("storage configuration request is invalid")
	}
	return nil
}
