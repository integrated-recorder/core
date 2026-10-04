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

	"github.com/dltkddnr04/integrated-recorder/internal/storageproto"
)

const (
	StorageProviderEndpoint = "/api/runtime/storage/provider"
	StorageProvidersPrefix  = "/api/runtime/storage/providers/"
	maxStorageConfigBody    = 64 << 10
	maxStorageConfigFields  = 64
)

var storageConfigKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

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

func (s StorageProviderStatus) Validate() error {
	if s.Primary.Kind != "local" && s.Primary.Kind != "plugin" || s.Primary.State != "ready" && s.Primary.State != "unavailable" {
		return errors.New("storage provider status is invalid")
	}
	if s.Primary.Kind == "local" && (s.Primary.ProviderID != "" || s.Primary.Version != "") || s.Primary.Kind == "plugin" && (!pluginIDPattern.MatchString(s.Primary.ProviderID) || !safePluginText(s.Primary.Version, 128)) {
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

func (h *handler) serveStorageOperation(w http.ResponseWriter, r *http.Request, operation storageOperation, id string) {
	setResponseHeaders(w)
	mutation := operation == storageConfigWrite || operation == storageProbe || operation == storageActivate
	if !h.authorize(w, r, mutation) {
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(encoded, '\n'))
}

func decodeStorageConfig(w http.ResponseWriter, r *http.Request, output *StorageConfigRequest) error {
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
