package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/plugintrust"
)

type adapterAPIView struct {
	Descriptor *adapterDescriptorAPIView `json:"descriptor,omitempty"`
	Status     adapterhost.Status        `json:"status"`
	Trust      *plugintrust.Attestation  `json:"trust,omitempty"`
}

type adapterDescriptorAPIView struct {
	ID                  string                      `json:"id"`
	Name                string                      `json:"name"`
	Version             string                      `json:"version"`
	ProtocolVersion     int                         `json:"protocol_version"`
	Capabilities        []string                    `json:"capabilities,omitempty"`
	InputSchema         adapterproto.Schema         `json:"input_schema"`
	ConfigurationSchema adapterproto.Schema         `json:"configuration_schema"`
	ResourceTypes       []adapterproto.ResourceType `json:"resource_types,omitempty"`
	MediaTypes          []string                    `json:"media_types"`
	Branding            *adapterBrandingAPIView     `json:"branding,omitempty"`
}

type adapterBrandingAPIView struct {
	IconURL string `json:"icon_url,omitempty"`
}

// projectAdapter keeps the existing status/descriptor API shape while
// replacing protocol-owned image bytes with a same-origin authenticated URL.
func (s *Server) projectAdapter(adapter adapterhost.Adapter) adapterAPIView {
	view := adapterAPIView{Status: adapter.Status}
	if s.adapterTrust == nil {
		// Monolithic/development mode has no Runtime Host attestation snapshot.
		// Discovered binaries remain explicitly operator supplied.
		attestation := plugintrust.NewOperator()
		view.Trust = &attestation
	} else if attestation, exists := s.adapterTrust[adapter.Status.ID]; exists && attestation.Provenance != plugintrust.LegacyUnclassified {
		copy := attestation
		view.Trust = &copy
	}
	if descriptor := adapter.Descriptor; descriptor != nil {
		view.Descriptor = &adapterDescriptorAPIView{
			ID: descriptor.ID, Name: descriptor.Name, Version: descriptor.Version,
			ProtocolVersion: descriptor.ProtocolVersion, Capabilities: descriptor.Capabilities,
			InputSchema: descriptor.InputSchema, ConfigurationSchema: descriptor.ConfigurationSchema,
			ResourceTypes: descriptor.ResourceTypes, MediaTypes: descriptor.MediaTypes,
		}
		if descriptor.Branding != nil && descriptor.Branding.Icon != nil && len(descriptor.Branding.Icon.Data) > 0 {
			view.Descriptor.Branding = &adapterBrandingAPIView{IconURL: "/api/adapters/" + url.PathEscape(adapter.Status.ID) + "/icon"}
		}
	}
	return view
}

func (s *Server) adapterIcon(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeError(w, http.StatusNotFound, "adapter icon not found")
		return
	}
	adapter, err := s.adapters.Get(r.PathValue("id"))
	if err != nil || adapter.Descriptor == nil || adapter.Descriptor.Branding == nil || adapter.Descriptor.Branding.Icon == nil {
		writeError(w, http.StatusNotFound, "adapter icon not found")
		return
	}
	icon := adapter.Descriptor.Branding.Icon
	if icon.MediaType != "image/png" || len(icon.Data) == 0 {
		writeError(w, http.StatusNotFound, "adapter icon not found")
		return
	}
	digest := sha256.Sum256(icon.Data)
	etag := `"` + hex.EncodeToString(digest[:]) + `"`
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, max-age=300, must-revalidate")
	w.Header().Set("ETag", etag)
	if matchesETag(r.Header.Values("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(icon.Data)
}

func matchesETag(values []string, current string) bool {
	for _, value := range values {
		for _, candidate := range strings.Split(value, ",") {
			candidate = strings.TrimSpace(candidate)
			if candidate == "*" || candidate == current || strings.TrimPrefix(candidate, "W/") == current {
				return true
			}
		}
	}
	return false
}

// registerAdapterControlRoutes adds the local control actions for already
// discovered adapter binaries. Installation remains an administrator-managed
// filesystem operation; this API never downloads or executes new code.
func (s *Server) registerAdapterControlRoutes() {
	s.mux.HandleFunc("POST /api/adapters/{id}/restart", s.adapterRestart)
	s.mux.HandleFunc("POST /api/adapters/{id}/enable", s.adapterEnable)
	s.mux.HandleFunc("POST /api/adapters/{id}/disable", s.adapterDisable)
}

func (s *Server) adapterRestart(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeError(w, http.StatusNotFound, "adapter not found")
		return
	}
	if _, err := s.controlAdapter(r.PathValue("id")); err != nil {
		writeAdapterControlError(w, err)
		return
	}
	adapter, err := s.adapters.Restart(r.Context(), r.PathValue("id"))
	if err != nil {
		if adapter.Status.State == "rejected" {
			writeError(w, http.StatusConflict, "adapter restart failed descriptor validation")
			return
		}
		writeAdapterControlError(w, err)
		return
	}
	auditErr := s.appendAudit("adapter_restarted", adapter.Status.ID)
	setAdapterAuditHeader(w, s.products != nil, auditErr, true)
	writeJSON(w, http.StatusOK, s.projectAdapter(adapter))
}

func (s *Server) adapterEnable(w http.ResponseWriter, r *http.Request) {
	s.adapterSetEnabled(w, r, true)
}

func (s *Server) adapterDisable(w http.ResponseWriter, r *http.Request) {
	s.adapterSetEnabled(w, r, false)
}

func (s *Server) adapterSetEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	if s.adapters == nil {
		writeError(w, http.StatusNotFound, "adapter not found")
		return
	}
	if s.products == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter management state is unavailable")
		return
	}
	id := r.PathValue("id")
	if _, err := s.controlAdapter(id); err != nil {
		writeAdapterControlError(w, err)
		return
	}
	wasEnabled := s.products.AdapterEnabled(id)
	if err := s.products.SetAdapterEnabled(id, enabled); err != nil {
		writeError(w, http.StatusInternalServerError, "adapter preference could not be saved")
		return
	}
	if err := s.adapters.SetEnabled(id, enabled); err != nil {
		if rollbackErr := s.products.SetAdapterEnabled(id, wasEnabled); rollbackErr != nil {
			writeError(w, http.StatusInternalServerError, "adapter runtime change failed and preference rollback failed")
			return
		}
		writeAdapterControlError(w, err)
		return
	}
	var auditErr error
	if wasEnabled != enabled {
		auditType := "adapter_enabled"
		if !enabled {
			auditType = "adapter_disabled"
		}
		auditErr = s.appendAudit(auditType, id)
	}
	setAdapterAuditHeader(w, s.products != nil, auditErr, wasEnabled != enabled)
	adapter, err := s.adapters.Get(id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "adapter status is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, s.projectAdapter(adapter))
}

func setAdapterAuditHeader(w http.ResponseWriter, available bool, err error, attempted bool) {
	switch {
	case !available:
		w.Header().Set("X-Adapter-Audit", "not_configured")
	case !attempted:
		w.Header().Set("X-Adapter-Audit", "not_needed")
	case err != nil:
		// The adapter operation already succeeded. Report it as success and make
		// the separate audit write failure explicit in response metadata.
		w.Header().Set("X-Adapter-Audit", "failed")
	default:
		w.Header().Set("X-Adapter-Audit", "recorded")
	}
}

func (s *Server) controlAdapter(id string) (adapterhost.Adapter, error) {
	adapter, err := s.adapters.Get(id)
	if err != nil {
		return adapterhost.Adapter{}, adapterhost.ErrAdapterUnavailable
	}
	if adapter.Descriptor == nil {
		return adapterhost.Adapter{}, adapterhost.ErrAdapterUnavailable
	}
	return adapter, nil
}

func writeAdapterControlError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, adapterhost.ErrAdapterUnavailable):
		writeError(w, http.StatusNotFound, "adapter is unavailable")
	case errors.Is(err, adapterhost.ErrAdapterDisabled):
		writeError(w, http.StatusConflict, "adapter is disabled")
	case errors.Is(err, adapterhost.ErrAdapterBusy):
		writeError(w, http.StatusConflict, "adapter operation is in progress")
	case errors.Is(err, adapterhost.ErrAdapterRestartRejected):
		writeError(w, http.StatusConflict, "adapter restart was rejected")
	case errors.Is(err, adapterhost.ErrAdapterHostClosed):
		writeError(w, http.StatusServiceUnavailable, "adapter host is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "adapter operation timed out")
	default:
		writeError(w, http.StatusServiceUnavailable, "adapter operation failed")
	}
}
