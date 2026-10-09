package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/plugintrust"
)

const PluginsEndpoint = "/api/runtime/plugins"

var errInvalidPluginStatus = errors.New("runtime plugin status is invalid")

var pluginIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// PluginStatus is a bounded public view of the configured approval registry
// and locally desired plugin versions. It never contains URLs, digests, or
// filesystem information.
type PluginStatus struct {
	State       string             `json:"state"`
	FailureCode string             `json:"failure_code,omitempty"`
	Plugins     []PluginStatusItem `json:"plugins"`
}

type PluginStatusItem struct {
	ID               string                  `json:"id"`
	Type             string                  `json:"type"`
	Name             string                  `json:"name"`
	AvailableVersion string                  `json:"available_version,omitempty"`
	InstalledVersion string                  `json:"installed_version,omitempty"`
	Installed        bool                    `json:"installed"`
	UpdateAvailable  bool                    `json:"update_available"`
	Trust            plugintrust.Attestation `json:"trust"`
}

func (s PluginStatus) Validate() error {
	if s.State != "ready" && s.State != "unavailable" && s.State != "not_configured" {
		return errInvalidPluginStatus
	}
	if s.FailureCode != "" && s.FailureCode != "plugin_registry_unavailable" {
		return errInvalidPluginStatus
	}
	if (s.State == "ready" && s.FailureCode != "") || (s.State == "unavailable" && s.FailureCode != "plugin_registry_unavailable") || (s.State == "not_configured" && s.FailureCode != "") || len(s.Plugins) > 256 {
		return errInvalidPluginStatus
	}
	seen := make(map[string]struct{}, len(s.Plugins))
	for _, item := range s.Plugins {
		if !pluginIDPattern.MatchString(item.ID) || (item.Type != "" && item.Type != "source" && item.Type != "storage") || !safePluginText(item.Name, 128) || item.AvailableVersion != "" && !safePluginText(item.AvailableVersion, 128) || item.InstalledVersion != "" && !safePluginText(item.InstalledVersion, 128) || item.Trust.Validate() != nil {
			return errInvalidPluginStatus
		}
		if _, exists := seen[item.ID]; exists {
			return errInvalidPluginStatus
		}
		seen[item.ID] = struct{}{}
		if item.UpdateAvailable && (!item.Installed || item.AvailableVersion == "" || item.InstalledVersion == "" || item.AvailableVersion == item.InstalledVersion) {
			return errInvalidPluginStatus
		}
		if !item.Installed && (item.InstalledVersion != "" || item.UpdateAvailable) {
			return errInvalidPluginStatus
		}
	}
	return nil
}

func safePluginText(value string, max int) bool {
	if len(value) == 0 || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// PluginController is the Host-owned registry lifecycle API. Install, update,
// and uninstall are serialized with application/adapter-set activation by the
// implementation.
type PluginController interface {
	PluginStatus(context.Context) (PluginStatus, error)
	RefreshPlugins(context.Context) (PluginStatus, error)
	InstallPlugin(context.Context, string) (PluginStatus, error)
	UpdatePlugin(context.Context, string) (PluginStatus, error)
	UninstallPlugin(context.Context, string) (PluginStatus, error)
}

type pluginOperation uint8

const (
	pluginStatus pluginOperation = iota + 1
	pluginRefresh
	pluginInstall
	pluginUpdate
	pluginUninstall
)

func routePluginOperation(method, path string) (pluginOperation, string, bool) {
	if method == http.MethodGet && path == PluginsEndpoint {
		return pluginStatus, "", true
	}
	if method == http.MethodPost && path == PluginsEndpoint+"/refresh" {
		return pluginRefresh, "", true
	}
	prefix := PluginsEndpoint + "/"
	if !strings.HasPrefix(path, prefix) {
		return 0, "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) == 1 && method == http.MethodDelete && pluginIDPattern.MatchString(parts[0]) {
		return pluginUninstall, parts[0], true
	}
	if len(parts) == 2 && pluginIDPattern.MatchString(parts[0]) && method == http.MethodPost {
		switch parts[1] {
		case "install":
			return pluginInstall, parts[0], true
		case "update":
			return pluginUpdate, parts[0], true
		}
	}
	return 0, "", false
}

func (h *handler) servePluginOperation(w http.ResponseWriter, r *http.Request, operation pluginOperation, id string) {
	setResponseHeaders(w)
	mutation := operation != pluginStatus
	r, authorized := h.authorize(w, r, authn.PermissionPluginManage, mutation)
	if !authorized {
		return
	}
	if mutation {
		if err := validateEmptyBody(r); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", safeControllerErrors["invalid_request"].message)
			return
		}
	}
	controller, ok := h.controller.(PluginController)
	if !ok {
		writeAPIError(w, http.StatusServiceUnavailable, "plugin_registry_unavailable", safeControllerErrors["plugin_registry_unavailable"].message)
		return
	}
	var status PluginStatus
	var err error
	switch operation {
	case pluginStatus:
		status, err = controller.PluginStatus(r.Context())
	case pluginRefresh:
		status, err = controller.RefreshPlugins(r.Context())
	case pluginInstall:
		status, err = controller.InstallPlugin(r.Context(), id)
	case pluginUpdate:
		status, err = controller.UpdatePlugin(r.Context(), id)
	case pluginUninstall:
		status, err = controller.UninstallPlugin(r.Context(), id)
	}
	if err != nil {
		writeControllerError(w, err)
		return
	}
	if err := status.Validate(); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	encoded, err := json.Marshal(status)
	if err != nil || len(encoded) > maxResponseBytes {
		writeAPIError(w, http.StatusInternalServerError, "internal_error", safeControllerErrors["internal_error"].message)
		return
	}
	switch operation {
	case pluginRefresh:
		h.auditMutation(w, r, controlplane.AuditPluginRegistryRefreshed, "plugin-registry")
	case pluginInstall:
		h.auditMutation(w, r, controlplane.AuditPluginInstalled, id)
	case pluginUpdate:
		h.auditMutation(w, r, controlplane.AuditPluginUpdated, id)
	case pluginUninstall:
		h.auditMutation(w, r, controlplane.AuditPluginUninstalled, id)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(encoded, '\n'))
}
