// Package server exposes the headless control API and generated HLS VOD views.
package server

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/applog"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storagediagnostic"
	"github.com/integrated-recorder/core/internal/systemsettings"
	"github.com/integrated-recorder/core/internal/watch"
)

type Server struct {
	manager                     recordingManager
	storage                     *storage.Store
	adapters                    *adapterhost.Host
	adapterTrust                map[string]plugintrust.Attestation
	configs                     *pluginconfig.Service
	products                    *management.Store
	integrity                   *integrity.Service
	derivatives                 *derivative.Service
	previews                    *preview.Service
	watches                     *watch.Service
	auth                        *authn.Service
	storageDiagnostics          *storagediagnostic.Store
	settings                    *systemsettings.Store
	effectiveStorage            systemsettings.StorageSettings
	logs                        *applog.Store
	initialIntegrityConcurrency int
	forceSecureCookie           bool
	installationManaged         bool
	version                     string
	commit                      string
	buildInfo                   buildinfo.Info
	mux                         *http.ServeMux
	mu                          sync.Mutex
	workflowTitles              map[string]workflowTitle
	startedAt                   time.Time
	productLocks                [64]sync.RWMutex
	retentionGate               chan struct{}
	backgroundMutationGate      interface {
		WaitAdmission(context.Context) (func(), error)
	}
	handler http.Handler
}

// recordingManager is the Control Plane's lifecycle boundary. Production may
// satisfy it with a Runtime Host IPC client while tests and embedders can
// continue to use acquire.Manager directly.
type recordingManager interface {
	StartResolved(context.Context, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error)
	StartResolvedWithID(context.Context, string, string, adapterproto.MediaSource, *adapterproto.ResourceRef, string, *adapterproto.AdapterProvenance) (*domain.Recording, error)
	Get(string) (*domain.Recording, error)
	List() []*domain.Recording
	ListForManagement(context.Context, int) ([]*domain.Recording, error)
	Stop(string) (*domain.Recording, error)
	Delete(string) error
}

// Options contains optional product-management services. New remains available
// for embedders and tests that only need the original control API.
type Options struct {
	Storage                     *storage.Store
	Management                  *management.Store
	Integrity                   *integrity.Service
	Derivatives                 *derivative.Service
	Previews                    *preview.Service
	Watches                     *watch.Service
	Auth                        *authn.Service
	StorageDiagnostics          *storagediagnostic.Store
	Settings                    *systemsettings.Store
	Logs                        *applog.Store
	InitialIntegrityConcurrency int
	InitialStorageSettings      *systemsettings.StorageSettings
	ForceSecureCookies          bool
	StartedAt                   time.Time
	Version                     string
	Commit                      string
	BuildInfo                   buildinfo.Info
	InstallationManaged         bool
	// AdapterTrust is the admission evidence from this Control generation's
	// immutable adapter-set snapshot. A nil map means a standalone/development
	// Control process and is projected conservatively as local operator input.
	AdapterTrust map[string]plugintrust.Attestation
	MutationGate interface {
		Wrap(http.Handler) http.Handler
	}
	BackgroundMutationGate interface {
		WaitAdmission(context.Context) (func(), error)
	}
}

type workflowTitle struct {
	title       string
	previewMode preview.Mode
	updatedAt   time.Time
}

const (
	workflowTitleTTL  = 30 * time.Minute
	maxWorkflowTitles = 128
)

//go:embed static/*
var staticFiles embed.FS

func New(manager recordingManager, adapters *adapterhost.Host, configs *pluginconfig.Service) http.Handler {
	return NewWithOptions(manager, adapters, configs, Options{})
}

func NewWithOptions(manager recordingManager, adapters *adapterhost.Host, configs *pluginconfig.Service, options Options) *Server {
	startedAt := options.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	build := options.BuildInfo
	if build.Version == "" {
		build = buildinfo.Current()
	}
	version := options.Version
	if version == "" {
		version = build.Version
	}
	commit := options.Commit
	if commit == "" {
		commit = build.Commit
	}
	build.Version = version
	build.Commit = commit
	logs := options.Logs
	if logs == nil {
		logs = applog.NewStore()
	}
	effectiveStorage := systemsettings.StorageSettings{}
	if options.InitialStorageSettings != nil {
		effectiveStorage = *options.InitialStorageSettings
	} else if options.Settings != nil {
		effectiveStorage = options.Settings.Current().Storage
	}
	archiveStore := options.Storage
	if archiveStore == nil {
		if local, ok := manager.(interface{ Store() *storage.Store }); ok {
			archiveStore = local.Store()
		}
	}
	s := &Server{manager: manager, storage: archiveStore, adapters: adapters, adapterTrust: validatedAdapterTrust(options.AdapterTrust), configs: configs, products: options.Management, integrity: options.Integrity, derivatives: options.Derivatives, previews: options.Previews, watches: options.Watches, auth: options.Auth, storageDiagnostics: options.StorageDiagnostics, settings: options.Settings, effectiveStorage: effectiveStorage, logs: logs, initialIntegrityConcurrency: options.InitialIntegrityConcurrency, forceSecureCookie: options.ForceSecureCookies, installationManaged: options.InstallationManaged, version: version, commit: commit, buildInfo: build, mux: http.NewServeMux(), workflowTitles: map[string]workflowTitle{}, startedAt: startedAt, retentionGate: make(chan struct{}, 1), backgroundMutationGate: options.BackgroundMutationGate}
	s.retentionGate <- struct{}{}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /", s.index)
	s.registerAuthRoutes()
	s.mux.HandleFunc("POST /api/setup/storage-test", s.setupStorageTest)
	static, _ := fs.Sub(staticFiles, "static")
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))
	s.mux.HandleFunc("POST /api/recordings", s.create)
	s.mux.HandleFunc("GET /api/resolve-workflows/{id}", s.workflowGet)
	s.mux.HandleFunc("DELETE /api/resolve-workflows/{id}", s.workflowCancel)
	s.mux.HandleFunc("POST /api/resolve-workflows/{id}/continue", s.workflowContinue)
	s.mux.HandleFunc("GET /api/adapters", s.adapterList)
	s.mux.HandleFunc("GET /api/adapters/{id}", s.adapterGet)
	s.mux.HandleFunc("GET /api/adapters/{id}/icon", s.adapterIcon)
	s.mux.HandleFunc("GET /api/adapters/{id}/schema", s.adapterSchema)
	s.mux.HandleFunc("GET /api/adapters/{id}/config", s.configGet)
	s.mux.HandleFunc("PUT /api/adapters/{id}/config", s.configPut)
	s.mux.HandleFunc("GET /api/recordings", s.list)
	s.registerProductRoutes()
	s.registerAdapterControlRoutes()
	s.registerSettingsRoutes()
	s.mux.Handle("GET /api/logs", NewLogHandler(s.logs))
	s.mux.HandleFunc("GET /api/recordings/{id}", s.get)
	s.mux.HandleFunc("GET /api/recordings/{id}/diagnostics/storage", s.storageFailureDiagnostic)
	s.mux.HandleFunc("POST /api/recordings/{id}/stop", s.stop)
	s.registerWatchRoutes()
	s.mux.HandleFunc("GET /api/recordings/{id}/play/master.m3u8", s.masterPlaylist)
	s.mux.HandleFunc("GET /api/recordings/{id}/play/tracks/{track}/playlist.m3u8", s.trackPlaylist)
	s.mux.HandleFunc("GET /api/recordings/{id}/play/live/master.m3u8", s.liveMasterPlaylist)
	s.mux.HandleFunc("GET /api/recordings/{id}/play/live/tracks/{track}/playlist.m3u8", s.liveTrackPlaylist)
	s.mux.HandleFunc("GET /api/recordings/{id}/play/segments/{segmentID}", s.segment)
	var handler http.Handler = s.mux
	if s.auth != nil {
		handler = s.authMiddleware(handler)
	}
	if options.MutationGate != nil {
		handler = options.MutationGate.Wrap(handler)
	}
	if options.InstallationManaged {
		handler = s.installationMiddleware(handler)
	}
	handler = s.flushWorkflowLifecycleMiddleware(handler)
	handler = requestLogMiddleware(s.logs, handler)
	s.handler = securityHeaders(handler)
	return s
}

func validatedAdapterTrust(input map[string]plugintrust.Attestation) map[string]plugintrust.Attestation {
	if input == nil {
		return nil
	}
	output := make(map[string]plugintrust.Attestation, len(input))
	for id, attestation := range input {
		if !adapterproto.IsValidIdentifier(id) {
			continue
		}
		if attestation != plugintrust.Legacy() && attestation.Validate() != nil {
			continue
		}
		output[id] = attestation
	}
	return output
}

// ServeHTTP makes the configured Server usable directly by net/http while
// retaining access to its lifecycle services, including RunRetention.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.handler == nil {
		http.Error(w, "server is unavailable", http.StatusServiceUnavailable)
		return
	}
	s.handler.ServeHTTP(w, r)
}

func (s *Server) flushWorkflowLifecycleMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer s.flushWorkflowLifecycleEvents()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if !isSPARoute(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	if r.URL.Path != "/" && !strings.Contains(r.Header.Get("Accept"), "text/html") {
		http.NotFound(w, r)
		return
	}
	page, err := staticFiles.ReadFile("static/ui/index.html")
	if err != nil {
		http.Error(w, "web application is not built", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(page)
}

type createRequest struct {
	AdapterID   string                    `json:"adapter_id"`
	Input       json.RawMessage           `json:"input"`
	Resource    *adapterproto.ResourceRef `json:"resource,omitempty"`
	Title       string                    `json:"title,omitempty"`
	PreviewMode preview.Mode              `json:"preview_mode,omitempty"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var request createRequest
	if err := decodeJSONBody(w, r, 64<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(request.AdapterID) == "" || len(request.Input) == 0 {
		writeError(w, http.StatusBadRequest, "adapter_id and input are required")
		return
	}
	previewMode, err := normalizePreviewMode(request.PreviewMode)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid preview mode")
		return
	}
	if previewMode == preview.ModeSegment && s.previews == nil {
		writeError(w, http.StatusNotImplemented, "preview generation is unavailable")
		return
	}
	if s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter resolver is unavailable")
		return
	}
	progress, err := s.adapters.BeginResolution(r.Context(), request.AdapterID, request.Input, request.Resource)
	if err != nil {
		if errors.Is(err, adapterhost.ErrWorkflowFailed) {
			writeError(w, http.StatusBadGateway, "adapter resolution failed")
			return
		}
		writeError(w, http.StatusBadRequest, "adapter could not resolve the input")
		return
	}
	if progress.State != "resolved" || progress.Media == nil {
		s.appendWorkflowHistory(progress, "started")
		s.appendWorkflowHistory(progress, "challenge_required")
		if !s.storeWorkflowTitle(progress.WorkflowID, strings.TrimSpace(request.Title), previewMode) {
			_ = s.adapters.CancelWorkflow(progress.WorkflowID)
			writeError(w, http.StatusServiceUnavailable, "too many active workflows")
			return
		}
		writeJSON(w, http.StatusAccepted, progress)
		return
	}
	s.appendWorkflowHistory(progress, "started")
	s.appendWorkflowHistory(progress, "resolved")
	recording, err := s.manager.StartResolved(r.Context(), progress.AdapterID, *progress.Media, progress.Resource, strings.TrimSpace(request.Title), &progress.Provenance)
	if err != nil {
		writeError(w, http.StatusBadRequest, "recording could not be started")
		return
	}
	s.applyPreviewPolicy(recording.ID, previewMode)
	s.appendRecordingEvent(recording.ID, "recording_started", recording.StartedAt, 0, "")
	response, responseErr := s.recordingDetail(recording)
	if responseErr != nil {
		writeJSON(w, http.StatusCreated, detail(recording))
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func (s *Server) workflowGet(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter workflows are unavailable")
		return
	}
	progress, err := s.adapters.Workflow(r.PathValue("id"))
	if err != nil {
		s.deleteWorkflowTitle(r.PathValue("id"))
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	s.touchWorkflowTitle(r.PathValue("id"))
	writeJSON(w, http.StatusOK, progress)
}

func (s *Server) workflowCancel(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter workflows are unavailable")
		return
	}
	err := s.adapters.CancelWorkflow(r.PathValue("id"))
	s.deleteWorkflowTitle(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "workflow not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) storeWorkflowTitle(id, title string, mode preview.Mode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupWorkflowTitlesLocked(time.Now())
	if _, exists := s.workflowTitles[id]; !exists && len(s.workflowTitles) >= maxWorkflowTitles {
		return false
	}
	s.workflowTitles[id] = workflowTitle{title: title, previewMode: mode, updatedAt: time.Now()}
	return true
}

func (s *Server) takeWorkflowTitle(id string) workflowTitle {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupWorkflowTitlesLocked(time.Now())
	item := s.workflowTitles[id]
	delete(s.workflowTitles, id)
	return item
}

func (s *Server) touchWorkflowTitle(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if item, ok := s.workflowTitles[id]; ok {
		item.updatedAt = time.Now()
		s.workflowTitles[id] = item
	}
}

func (s *Server) deleteWorkflowTitle(id string) {
	s.mu.Lock()
	delete(s.workflowTitles, id)
	s.mu.Unlock()
}

func (s *Server) cleanupWorkflowTitlesLocked(now time.Time) {
	for id, item := range s.workflowTitles {
		if now.Sub(item.updatedAt) > workflowTitleTTL {
			delete(s.workflowTitles, id)
		}
	}
}

type workflowContinueRequest struct {
	Values        map[string]json.RawMessage `json:"values,omitempty"`
	Secrets       map[string]string          `json:"secrets,omitempty"`
	Persist       bool                       `json:"persist,omitempty"`
	PersistFields []string                   `json:"persist_fields,omitempty"`
}

func (s *Server) workflowContinue(w http.ResponseWriter, r *http.Request) {
	var request workflowContinueRequest
	if err := decodeJSONBody(w, r, 64<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid workflow continuation")
		return
	}
	if s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter workflows are unavailable")
		return
	}
	var progress adapterhost.WorkflowProgress
	var err error
	if request.Persist && len(request.PersistFields) == 0 {
		progress, err = s.adapters.ContinueResolution(r.Context(), r.PathValue("id"), request.Values, request.Secrets, true)
	} else {
		progress, err = s.adapters.ContinueResolutionFields(r.Context(), r.PathValue("id"), request.Values, request.Secrets, request.PersistFields)
	}
	if err != nil {
		if errors.Is(err, adapterhost.ErrWorkflowFailed) {
			writeError(w, http.StatusBadGateway, "adapter resolution failed")
			return
		}
		writeError(w, http.StatusBadRequest, "workflow continuation was rejected")
		return
	}
	s.appendWorkflowHistory(progress, "continued")
	if progress.State != "resolved" || progress.Media == nil {
		s.appendWorkflowHistory(progress, "challenge_required")
		s.touchWorkflowTitle(progress.WorkflowID)
		writeJSON(w, http.StatusAccepted, progress)
		return
	}
	workflow := s.takeWorkflowTitle(progress.WorkflowID)
	recording, err := s.manager.StartResolved(r.Context(), progress.AdapterID, *progress.Media, progress.Resource, workflow.title, &progress.Provenance)
	if err != nil {
		writeError(w, http.StatusBadRequest, "recording could not be started")
		return
	}
	s.applyPreviewPolicy(recording.ID, workflow.previewMode)
	s.appendRecordingEvent(recording.ID, "recording_started", recording.StartedAt, 0, "")
	response, responseErr := s.recordingDetail(recording)
	if responseErr != nil {
		writeJSON(w, http.StatusCreated, detail(recording))
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func normalizePreviewMode(mode preview.Mode) (preview.Mode, error) {
	if mode == "" {
		return preview.ModeDisabled, nil
	}
	if mode != preview.ModeDisabled && mode != preview.ModeSegment {
		return "", preview.ErrInvalid
	}
	return mode, nil
}

func (s *Server) applyPreviewPolicy(recordingID string, mode preview.Mode) {
	if mode != preview.ModeSegment || s.previews == nil {
		return
	}
	if _, err := s.previews.SetMode(recordingID, mode); err != nil {
		if s.logs != nil {
			s.logs.Add("error", "preview", "preview policy could not be persisted after recording start")
		}
		return
	}
	_ = s.previews.Reconcile()
}

func (s *Server) adapterList(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeJSON(w, http.StatusOK, []adapterAPIView{})
		return
	}
	adapters := s.adapters.List()
	views := make([]adapterAPIView, 0, len(adapters))
	for _, adapter := range adapters {
		views = append(views, s.projectAdapter(adapter))
	}
	writeJSON(w, http.StatusOK, views)
}
func (s *Server) adapterGet(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeError(w, http.StatusNotFound, "adapter not found")
		return
	}
	adapter, err := s.adapters.Get(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "adapter not found")
		return
	}
	writeJSON(w, http.StatusOK, s.projectAdapter(adapter))
}
func (s *Server) adapterSchema(w http.ResponseWriter, r *http.Request) {
	if s.adapters == nil {
		writeError(w, http.StatusNotFound, "adapter not found")
		return
	}
	adapter, err := s.adapters.Get(r.PathValue("id"))
	if err != nil || adapter.Descriptor == nil {
		writeError(w, http.StatusNotFound, "adapter schema is unavailable")
		return
	}
	resource, err := resourceQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid resource scope")
		return
	}
	configurationSchema, err := s.adapters.Schema(r.PathValue("id"), resource)
	if err != nil {
		writeError(w, http.StatusNotFound, "adapter configuration schema is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"input_schema": adapter.Descriptor.InputSchema, "configuration_schema": configurationSchema, "resource_types": adapter.Descriptor.ResourceTypes, "media_types": adapter.Descriptor.MediaTypes})
}

type configPutRequest struct {
	Resource     *adapterproto.ResourceRef  `json:"resource,omitempty"`
	Values       map[string]json.RawMessage `json:"values,omitempty"`
	Secrets      map[string]string          `json:"secrets,omitempty"`
	ClearValues  []string                   `json:"clear_values,omitempty"`
	ClearSecrets []string                   `json:"clear_secrets,omitempty"`
}
type secretState struct {
	Configured bool `json:"configured"`
}
type maskedConfig struct {
	Values  map[string]json.RawMessage `json:"values"`
	Secrets map[string]secretState     `json:"secrets"`
}

type configScopeView struct {
	Values  map[string]json.RawMessage `json:"values"`
	Secrets map[string]secretState     `json:"secrets"`
}
type configAPIView struct {
	Schema        adapterproto.Schema        `json:"schema"`
	Values        map[string]json.RawMessage `json:"values"`
	Secrets       map[string]secretState     `json:"secrets"`
	Stored        configScopeView            `json:"stored"`
	Effective     configScopeView            `json:"effective"`
	ValueSources  map[string]string          `json:"value_sources"`
	SecretSources map[string]string          `json:"secret_sources"`
	CurrentScope  string                     `json:"current_scope"`
}

func configView(values map[string]json.RawMessage, configured map[string]bool, schema adapterproto.Schema) configScopeView {
	fieldControls := map[string]string{}
	for _, field := range schema.Fields {
		fieldControls[field.Key] = field.Control
	}
	ordinary := map[string]json.RawMessage{}
	for key, value := range values {
		// Do not expose values whose current descriptor no longer declares a
		// field. An older schema may have stored such a value as a secret.
		if control, declared := fieldControls[key]; declared && control != "secret" {
			ordinary[key] = append(json.RawMessage(nil), value...)
		}
	}
	secrets := map[string]secretState{}
	for key, value := range configured {
		if fieldControls[key] == "secret" {
			secrets[key] = secretState{Configured: value}
		}
	}
	for key, control := range fieldControls {
		if control != "secret" {
			continue
		}
		if _, ok := secrets[key]; !ok {
			secrets[key] = secretState{}
		}
	}
	return configScopeView{Values: ordinary, Secrets: secrets}
}

func configAPI(schema adapterproto.Schema, snapshot pluginconfig.Snapshot, resource *adapterproto.ResourceRef) configAPIView {
	stored := configView(snapshot.StoredValues, snapshot.StoredSecrets, schema)
	effective := configView(snapshot.EffectiveValues, snapshot.EffectiveSecret, schema)
	return configAPIView{Schema: schema, Values: stored.Values, Secrets: stored.Secrets, Stored: stored, Effective: effective, ValueSources: snapshot.ValueSources, SecretSources: snapshot.SecretSources, CurrentScope: pluginconfig.ScopeIdentity(pluginconfig.Scope{Resource: resource})}
}

func (s *Server) configGet(w http.ResponseWriter, r *http.Request) {
	if s.configs == nil || s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter configuration is unavailable")
		return
	}
	resource, err := resourceQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid resource scope")
		return
	}
	id := r.PathValue("id")
	schema, err := s.adapters.Schema(id, resource)
	if err != nil {
		writeError(w, http.StatusNotFound, "adapter configuration schema is unavailable")
		return
	}
	snapshot, err := s.adapters.ConfigSnapshot(id, resource)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "adapter configuration could not be read")
		return
	}
	writeJSON(w, http.StatusOK, configAPI(schema, snapshot, resource))
}
func (s *Server) configPut(w http.ResponseWriter, r *http.Request) {
	var request configPutRequest
	if err := decodeJSONBody(w, r, 64<<10, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid configuration request")
		return
	}
	if s.configs == nil || s.adapters == nil {
		writeError(w, http.StatusServiceUnavailable, "adapter configuration is unavailable")
		return
	}
	id := r.PathValue("id")
	schema, err := s.adapters.Schema(id, request.Resource)
	if err != nil {
		writeError(w, http.StatusNotFound, "adapter configuration schema is unavailable")
		return
	}
	if err = s.configs.PutPartialWithClears(pluginconfig.Scope{PluginID: id, Resource: request.Resource}, schema, request.Values, request.Secrets, request.ClearValues, request.ClearSecrets); err != nil {
		writeError(w, http.StatusBadRequest, "configuration update was rejected")
		return
	}
	snapshot, err := s.adapters.ConfigSnapshot(id, request.Resource)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "adapter configuration could not be read")
		return
	}
	if auditErr := s.appendAudit("config_changed", id); auditErr != nil {
		w.Header().Set("X-Config-Audit", "failed")
	} else if s.products != nil {
		w.Header().Set("X-Config-Audit", "recorded")
	}
	writeJSON(w, http.StatusOK, configAPI(schema, snapshot, request.Resource))
}

func projectConfig(values map[string]json.RawMessage, configured map[string]bool, schema adapterproto.Schema) maskedConfig {
	fieldControls := make(map[string]string)
	for _, field := range schema.Fields {
		fieldControls[field.Key] = field.Control
	}
	maskedValues := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		if control, declared := fieldControls[key]; declared && control != "secret" {
			maskedValues[key] = value
		}
	}
	maskedSecrets := maskSecrets(configured, schema)
	for key, control := range fieldControls {
		if control != "secret" {
			continue
		}
		if _, exists := values[key]; exists {
			maskedSecrets[key] = secretState{Configured: true}
		}
	}
	return maskedConfig{Values: maskedValues, Secrets: maskedSecrets}
}

func maskSecrets(values map[string]bool, schema adapterproto.Schema) map[string]secretState {
	out := make(map[string]secretState, len(values))
	for _, field := range schema.Fields {
		if field.Control == "secret" {
			out[field.Key] = secretState{Configured: false}
		}
	}
	for key, configured := range values {
		out[key] = secretState{Configured: configured}
	}
	return out
}

func resourceQuery(r *http.Request) (*adapterproto.ResourceRef, error) {
	raw := r.URL.Query().Get("resource")
	if raw == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var ref adapterproto.ResourceRef
	if err = json.Unmarshal(data, &ref); err != nil {
		return nil, err
	}
	if err = adapterproto.ValidateResourceRef(&ref); err != nil {
		return nil, err
	}
	return &ref, nil
}

type recordingSummary struct {
	ID                      string                    `json:"id"`
	Title                   string                    `json:"title,omitempty"`
	AdapterID               string                    `json:"adapter_id,omitempty"`
	Adapter                 *domain.AdapterProvenance `json:"adapter,omitempty"`
	SourceURIClassification string                    `json:"source_uri_classification"`
	Resource                *domain.ResourceReference `json:"resource,omitempty"`
	SourceURL               string                    `json:"source_url,omitempty"`
	State                   domain.RecordingState     `json:"state"`
	CreatedAt               time.Time                 `json:"created_at"`
	StartedAt               time.Time                 `json:"started_at"`
	StoppedAt               any                       `json:"stopped_at,omitempty"`
	TrackCount              int                       `json:"track_count"`
	SegmentCount            int                       `json:"segment_count"`
	Duration                float64                   `json:"duration_seconds"`
	GapCount                int                       `json:"gap_count"`
	LastError               string                    `json:"last_error,omitempty"`
	Preview                 *preview.Summary          `json:"preview,omitempty"`
}

func summary(r *domain.Recording) recordingSummary {
	classification := r.SourceURIClassification
	if classification == "" {
		classification = "sensitive"
	}
	return recordingSummary{ID: r.ID, Title: r.Title, AdapterID: r.AdapterID, Adapter: r.Adapter, Resource: r.Resource, SourceURIClassification: classification, State: r.State, CreatedAt: r.CreatedAt, StartedAt: r.StartedAt, StoppedAt: r.StoppedAt, TrackCount: len(r.Tracks), SegmentCount: r.SegmentCount(), Duration: r.Duration(), GapCount: len(r.Gaps), LastError: publicLastError(r.LastError)}
}

func (s *Server) recordingSummary(r *domain.Recording) recordingSummary {
	item := summary(r)
	if s.previews != nil {
		value := s.previews.Summary(r)
		item.Preview = &value
	}
	return item
}

func publicLastError(value string) string {
	if value == "" {
		return ""
	}
	return "acquisition error"
}

type recordingDetail struct {
	*domain.Recording
	TrackCount   int                  `json:"track_count"`
	SegmentCount int                  `json:"segment_count"`
	Duration     float64              `json:"duration_seconds"`
	Statistics   *recordingStatistics `json:"statistics,omitempty"`
	Preview      *preview.Summary     `json:"preview,omitempty"`
}

type recordingStatistics struct {
	ArchiveSizeBytes      int64                   `json:"archive_size_bytes"`
	MediaPayloadSizeBytes int64                   `json:"media_payload_size_bytes"`
	ManifestSizeBytes     int64                   `json:"manifest_size_bytes"`
	InitPayloadSizeBytes  int64                   `json:"init_payload_size_bytes"`
	SegmentCount          int                     `json:"segment_count"`
	InitSegmentCount      int                     `json:"init_segment_count"`
	ManifestSnapshotCount int                     `json:"manifest_snapshot_count"`
	DurationSeconds       float64                 `json:"duration_seconds"`
	GapCount              int                     `json:"gap_count"`
	GapSegmentCount       int                     `json:"gap_segment_count"`
	GapDurationSeconds    *float64                `json:"gap_duration_seconds"`
	Integrity             storage.IntegrityStatus `json:"integrity"`
}

func detail(r *domain.Recording) recordingDetail {
	projected := *r
	// Source metadata history is served through its dedicated bounded endpoint.
	projected.MetadataTimeline = nil
	projected.MetadataTimelineTruncated = false
	projected.SourceURL = ""
	projected.LastError = publicLastError(projected.LastError)
	if projected.SourceURIClassification == "" {
		projected.SourceURIClassification = "sensitive"
	}
	projected.Snapshots = append([]domain.ManifestSnapshot(nil), r.Snapshots...)
	for index := range projected.Snapshots {
		projected.Snapshots[index].SourceURI = ""
	}
	projected.Tracks = make(map[string]*domain.Track, len(r.Tracks))
	for key, track := range r.Tracks {
		copyTrack := *track
		copyTrack.SourcePlaylistURL = ""
		copyTrack.Segments = append([]domain.Segment(nil), track.Segments...)
		for index := range copyTrack.Segments {
			copyTrack.Segments[index].SourceURI = ""
		}
		copyTrack.InitSegments = append([]domain.Segment(nil), track.InitSegments...)
		for index := range copyTrack.InitSegments {
			copyTrack.InitSegments[index].SourceURI = ""
		}
		projected.Tracks[key] = &copyTrack
	}
	return recordingDetail{Recording: &projected, TrackCount: len(r.Tracks), SegmentCount: r.SegmentCount(), Duration: r.Duration()}
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	recordings := s.manager.List()
	out := make([]recordingSummary, 0, len(recordings))
	for _, recording := range recordings {
		out = append(out, s.recordingSummary(recording))
	}
	writeJSON(w, http.StatusOK, out)
}
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.manager.Get(r.PathValue("id"))
	if err != nil {
		writeStorageError(w, err)
		return
	}
	response, err := s.recordingDetail(recording)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "recording statistics are temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, response)
}
func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	recording, err := s.manager.Stop(r.PathValue("id"))
	if err != nil {
		writeStorageError(w, err)
		return
	}
	kind, message := "recording_stopped", "recording stopped"
	if recording.State == domain.StateCompleted {
		kind, message = "recording_completed", "recording completed"
	} else if recording.State == domain.StateInterrupted {
		kind, message = "recording_interrupted", "recording interrupted"
	} else if recording.State == domain.StateStopped && s.watches != nil {
		s.watches.NotifyRecordingStopped(recording.ID)
	}
	if recording.StoppedAt != nil {
		s.appendRecordingEvent(recording.ID, kind, *recording.StoppedAt, 0, message)
	}
	writeJSON(w, http.StatusOK, detail(recording))
}

func (s *Server) masterPlaylist(w http.ResponseWriter, r *http.Request) {
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.playableRecording(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	track := recording.Tracks["main"]
	if track == nil {
		http.NotFound(w, r)
		return
	}
	if len(track.Segments) == 0 {
		writeError(w, http.StatusConflict, "recording has no captured media segments yet")
		return
	}
	if !s.playableTrackPayloadsAvailable(recording, track) {
		writeError(w, http.StatusServiceUnavailable, "recording media payload is unavailable")
		return
	}
	bandwidth := track.Bandwidth
	if bandwidth <= 0 {
		bandwidth = 1_000_000
	}
	playlist := fmt.Sprintf("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=%d\n/api/recordings/%s/play/tracks/main/playlist.m3u8\n", bandwidth, recording.ID)
	writePlaylist(w, playlist)
}

func (s *Server) trackPlaylist(w http.ResponseWriter, r *http.Request) {
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.playableRecording(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	trackID := r.PathValue("track")
	track := recording.Tracks[trackID]
	if track == nil {
		http.NotFound(w, r)
		return
	}
	if len(track.Segments) == 0 {
		writeError(w, http.StatusConflict, "recording has no captured media segments yet")
		return
	}
	if !s.playableTrackPayloadsAvailable(recording, track) {
		writeError(w, http.StatusServiceUnavailable, "recording media payload is unavailable")
		return
	}
	segments := append([]domain.Segment(nil), track.Segments...)
	sort.Slice(segments, func(i, j int) bool { return segmentBefore(segments[i], segments[j]) })
	maxDuration := 0.0
	for _, segment := range segments {
		if segment.Duration > maxDuration {
			maxDuration = segment.Duration
		}
	}
	target := int(math.Ceil(maxDuration))
	if target < 1 {
		target = 1
	}
	var builder strings.Builder
	mediaSequence := playbackMediaSequence(segments[0])
	fmt.Fprintf(&builder, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:%d\n", target, mediaSequence)
	previous := segments[0]
	previousInit := ""
	lastDiscontinuity := false
	for index, segment := range segments {
		if index > 0 && (previous.SourceEpoch != segment.SourceEpoch || previous.DiscontinuitySequence != segment.DiscontinuitySequence || hasGapBetweenEpoch(recording.Gaps, trackID, previous.SourceEpoch, previous.Sequence, segment.Sequence)) {
			fmt.Fprintln(&builder, "#EXT-X-DISCONTINUITY")
			lastDiscontinuity = true
		}
		if segment.Discontinuity && !lastDiscontinuity {
			fmt.Fprintln(&builder, "#EXT-X-DISCONTINUITY")
			lastDiscontinuity = true
		}
		if segment.InitSegmentID != "" && segment.InitSegmentID != previousInit {
			init, ok := findInit(track, segment.InitSegmentID)
			if !ok {
				writeError(w, http.StatusInternalServerError, "recording references a missing init segment")
				return
			}
			uri := fmt.Sprintf("/api/recordings/%s/play/segments/%s", recording.ID, init.ID)
			fmt.Fprintf(&builder, "#EXT-X-MAP:URI=%s\n", strconv.Quote(uri))
			previousInit = segment.InitSegmentID
		}
		if segment.ProgramDateTime != nil {
			fmt.Fprintf(&builder, "#EXT-X-PROGRAM-DATE-TIME:%s\n", segment.ProgramDateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
		}
		fmt.Fprintf(&builder, "#EXTINF:%s,\n/api/recordings/%s/play/segments/%s\n", strconv.FormatFloat(segment.Duration, 'f', -1, 64), recording.ID, segment.ID)
		previous = segment
		lastDiscontinuity = false
	}
	builder.WriteString("#EXT-X-ENDLIST\n")
	writePlaylist(w, builder.String())
}

// liveMasterPlaylist builds a same-origin HLS entry point for the canonical
// media that has already been committed for an active recording. It never
// exposes the adapter's source playlist or its URLs.
func (s *Server) liveMasterPlaylist(w http.ResponseWriter, r *http.Request) {
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	recording, ok := s.liveRecording(w, r.PathValue("id"))
	if !ok {
		return
	}
	track := recording.Tracks["main"]
	if track == nil {
		http.NotFound(w, r)
		return
	}
	segments := livePlaylistWindow(track.Segments)
	if len(segments) == 0 {
		writeError(w, http.StatusConflict, "recording has no captured media segments yet")
		return
	}
	if !s.livePayloadsAvailable(recording, track, segments) {
		writeError(w, http.StatusServiceUnavailable, "recording media payload is unavailable")
		return
	}
	bandwidth := track.Bandwidth
	if bandwidth <= 0 {
		bandwidth = 1_000_000
	}
	playlist := fmt.Sprintf("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=%d\n/api/recordings/%s/play/live/tracks/main/playlist.m3u8\n", bandwidth, recording.ID)
	writePlaylist(w, playlist)
}

const livePlaylistSegmentLimit = 12

func livePlaylistWindow(source []domain.Segment) []domain.Segment {
	segments := livePlaylistWindowWithoutLimit(source)
	return livePlaylistWindowFromSorted(segments)
}

func livePlaylistWindowFromSorted(segments []domain.Segment) []domain.Segment {
	if len(segments) > livePlaylistSegmentLimit {
		segments = segments[len(segments)-livePlaylistSegmentLimit:]
	}
	return segments
}

func (s *Server) liveTrackPlaylist(w http.ResponseWriter, r *http.Request) {
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	recording, ok := s.liveRecording(w, r.PathValue("id"))
	if !ok {
		return
	}
	trackID := r.PathValue("track")
	track := recording.Tracks[trackID]
	if track == nil {
		http.NotFound(w, r)
		return
	}
	allSegments := livePlaylistWindowWithoutLimit(track.Segments)
	segments := livePlaylistWindowFromSorted(allSegments)
	if len(segments) == 0 {
		writeError(w, http.StatusConflict, "recording has no captured media segments yet")
		return
	}
	if !s.livePayloadsAvailable(recording, track, segments) {
		writeError(w, http.StatusServiceUnavailable, "recording media payload is unavailable")
		return
	}

	maxDuration := 0.0
	for _, segment := range segments {
		if segment.Duration > maxDuration {
			maxDuration = segment.Duration
		}
	}
	target := int(math.Ceil(maxDuration))
	if target < 1 {
		target = 1
	}
	windowStart := len(allSegments) - len(segments)
	discontinuitySequence := uint64(0)
	for index := 0; index < windowStart; index++ {
		if liveDiscontinuityBefore(recording, trackID, allSegments, index) {
			discontinuitySequence++
		}
	}
	mediaSequence := playbackMediaSequence(segments[0])

	var builder strings.Builder
	fmt.Fprintf(&builder, "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:%d\n", target, mediaSequence)
	if discontinuitySequence > 0 {
		fmt.Fprintf(&builder, "#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", discontinuitySequence)
	}
	previousInit := ""
	for index, segment := range segments {
		fullIndex := windowStart + index
		if liveDiscontinuityBefore(recording, trackID, allSegments, fullIndex) {
			fmt.Fprintln(&builder, "#EXT-X-DISCONTINUITY")
		}
		if segment.InitSegmentID != "" && segment.InitSegmentID != previousInit {
			init, ok := findInit(track, segment.InitSegmentID)
			if !ok {
				writeError(w, http.StatusInternalServerError, "recording references a missing init segment")
				return
			}
			uri := fmt.Sprintf("/api/recordings/%s/play/segments/%s", recording.ID, init.ID)
			fmt.Fprintf(&builder, "#EXT-X-MAP:URI=%s\n", strconv.Quote(uri))
			previousInit = segment.InitSegmentID
		}
		if segment.ProgramDateTime != nil {
			fmt.Fprintf(&builder, "#EXT-X-PROGRAM-DATE-TIME:%s\n", segment.ProgramDateTime.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
		}
		fmt.Fprintf(&builder, "#EXTINF:%s,\n/api/recordings/%s/play/segments/%s\n", strconv.FormatFloat(segment.Duration, 'f', -1, 64), recording.ID, segment.ID)
	}
	writePlaylist(w, builder.String())
}

// livePlaylistWindowWithoutLimit returns a sorted snapshot for calculating
// discontinuities before the selected sliding window.
func livePlaylistWindowWithoutLimit(source []domain.Segment) []domain.Segment {
	segments := append([]domain.Segment(nil), source...)
	sort.Slice(segments, func(i, j int) bool { return segmentBefore(segments[i], segments[j]) })
	return segments
}

func liveDiscontinuityBefore(recording *domain.Recording, trackID string, segments []domain.Segment, index int) bool {
	if index < 0 || index >= len(segments) {
		return false
	}
	segment := segments[index]
	if segment.Discontinuity {
		return true
	}
	if index == 0 {
		return false
	}
	previous := segments[index-1]
	return previous.SourceEpoch != segment.SourceEpoch ||
		previous.DiscontinuitySequence != segment.DiscontinuitySequence ||
		hasGapBetweenEpoch(recording.Gaps, trackID, previous.SourceEpoch, previous.Sequence, segment.Sequence)
}

func (s *Server) livePayloadsAvailable(recording *domain.Recording, track *domain.Track, segments []domain.Segment) bool {
	store := s.storage
	for _, segment := range segments {
		info, err := store.StatPayload(recording.ID, segment.StoragePath)
		if err != nil || !info.Regular || info.Size != segment.PayloadSize {
			return false
		}
		if segment.InitSegmentID != "" {
			init, ok := findInit(track, segment.InitSegmentID)
			if !ok {
				return false
			}
			info, err := store.StatPayload(recording.ID, init.StoragePath)
			if err != nil || !info.Regular || info.Size != init.PayloadSize {
				return false
			}
		}
	}
	return true
}

func (s *Server) liveRecording(w http.ResponseWriter, id string) (*domain.Recording, bool) {
	recording, err := s.manager.Get(id)
	if err != nil {
		writeStorageError(w, err)
		return nil, false
	}
	if recording.State != domain.StateRecording {
		writeError(w, http.StatusConflict, "live HLS playback is available only while recording")
		return nil, false
	}
	return recording, true
}

func (s *Server) playableTrackPayloadsAvailable(recording *domain.Recording, track *domain.Track) bool {
	store := s.storage
	if store.HasCanonicalPayloadIssue(recording.ID) {
		return false
	}
	for _, segment := range track.Segments {
		info, err := store.StatPayload(recording.ID, segment.StoragePath)
		if err != nil || !info.Regular || info.Size != segment.PayloadSize {
			return false
		}
		if segment.InitSegmentID != "" {
			init, ok := findInit(track, segment.InitSegmentID)
			if !ok {
				return false
			}
			info, err := store.StatPayload(recording.ID, init.StoragePath)
			if err != nil || !info.Regular || info.Size != init.PayloadSize {
				return false
			}
		}
	}
	return true
}

func (s *Server) segment(w http.ResponseWriter, r *http.Request) {
	lock := s.productLock(r.PathValue("id"))
	lock.RLock()
	defer lock.RUnlock()
	recording, err := s.manager.Get(r.PathValue("id"))
	if err != nil {
		writeStorageError(w, err)
		return
	}
	var found *domain.Segment
	for _, track := range recording.Tracks {
		for i := range track.Segments {
			if track.Segments[i].ID == r.PathValue("segmentID") {
				copy := track.Segments[i]
				found = &copy
				break
			}
		}
		for i := range track.InitSegments {
			if track.InitSegments[i].ID == r.PathValue("segmentID") {
				copy := track.InitSegments[i]
				found = &copy
				break
			}
		}
	}
	if found == nil {
		http.NotFound(w, r)
		return
	}
	var f io.ReadCloser
	status := http.StatusOK
	contentLength := found.PayloadSize
	contentRange := ""
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		info, statErr := s.storage.StatPayload(recording.ID, found.StoragePath)
		if statErr != nil || !info.Regular || info.Size < 0 || info.Size != found.PayloadSize {
			writeError(w, http.StatusNotFound, "stored payload is unavailable")
			return
		}
		offset, length, valid := parseSingleByteRange(rangeHeader, info.Size)
		if !valid {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
			w.Header().Set("Content-Length", "0")
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Type", mediaContentType(found.StoragePath))
			w.Header().Set("Cache-Control", "private, no-store")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		f, err = s.storage.OpenPayloadRangeReaderContext(r.Context(), recording.ID, found.StoragePath, offset, length)
		if err != nil {
			writeError(w, http.StatusNotFound, "stored payload is unavailable")
			return
		}
		status = http.StatusPartialContent
		contentLength = length
		contentRange = fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, info.Size)
	} else {
		f, err = s.storage.OpenPayloadReader(recording.ID, found.StoragePath)
		if err != nil {
			writeError(w, http.StatusNotFound, "stored payload is unavailable")
			return
		}
	}
	defer f.Close()
	// net/http applies the server's WriteTimeout to the entire handler body.
	// Streaming a large source segment to a slow browser may legitimately take
	// longer, so clear that deadline only for this file response.
	if err = http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		writeError(w, http.StatusInternalServerError, "stored payload could not be streamed")
		return
	}
	w.Header().Set("Content-Type", mediaContentType(found.StoragePath))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Accept-Ranges", "bytes")
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
		w.WriteHeader(status)
	}
	if _, err = io.Copy(w, f); err != nil {
		return
	}
}

// parseSingleByteRange parses a single RFC 9110 byte range. Multiple ranges
// are deliberately rejected rather than causing a full-object fallback or a
// multipart response that might buffer the archive object.
func parseSingleByteRange(header string, size int64) (offset, length int64, ok bool) {
	if size <= 0 || size > storage.MaxObjectBytes {
		return 0, 0, false
	}
	unit, spec, found := strings.Cut(strings.TrimSpace(header), "=")
	if !found || !strings.EqualFold(unit, "bytes") || strings.Contains(spec, ",") || strings.Count(spec, "-") != 1 {
		return 0, 0, false
	}
	first, last, _ := strings.Cut(spec, "-")
	if first == "" {
		suffix, valid := parseByteRangeNumber(last)
		if !valid || suffix == 0 {
			return 0, 0, false
		}
		if suffix >= size {
			return 0, size, true
		}
		return size - suffix, suffix, true
	}
	start, valid := parseByteRangeNumber(first)
	if !valid || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if last != "" {
		end, valid = parseByteRangeNumber(last)
		if !valid || end < start {
			return 0, 0, false
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end - start + 1, true
}

func parseByteRangeNumber(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}

func (s *Server) playableRecording(id string) (*domain.Recording, error) {
	r, err := s.manager.Get(id)
	if err != nil {
		return nil, err
	}
	if r.State == domain.StateRecording {
		return nil, fmt.Errorf("VOD playback is available after recording stops")
	}
	return r, nil
}
func hasGapBetween(gaps []domain.Gap, trackID string, previous, next uint64) bool {
	return hasGapBetweenEpoch(gaps, trackID, 0, previous, next)
}

func hasGapBetweenEpoch(gaps []domain.Gap, trackID string, epoch, previous, next uint64) bool {
	if next <= previous || previous == ^uint64(0) {
		return false
	}
	for _, gap := range gaps {
		if gap.TrackID == trackID && gap.SourceEpoch == epoch && gap.ToSequence > previous && gap.FromSequence < next {
			return true
		}
	}
	return next-previous > 1
}

func segmentBefore(a, b domain.Segment) bool {
	// A timeline ordinal is a revisionable playback position. It takes
	// precedence over append-only archive placement whenever both segments have
	// one. Legacy segments (and mixed legacy/new projections) retain a stable
	// fallback through archive placement and source coordinates.
	if a.TimelineOrdinal != 0 || b.TimelineOrdinal != 0 {
		aPosition := playbackPosition(a)
		bPosition := playbackPosition(b)
		if aPosition != bPosition {
			return aPosition < bPosition
		}
	}
	return legacySegmentBefore(a, b)
}

func playbackPosition(segment domain.Segment) uint64 {
	if segment.TimelineOrdinal != 0 {
		return segment.TimelineOrdinal
	}
	if segment.ArchiveOrdinal != 0 {
		return segment.ArchiveOrdinal
	}
	return segment.Sequence
}

func playbackMediaSequence(segment domain.Segment) uint64 {
	if segment.TimelineOrdinal != 0 {
		return segment.TimelineOrdinal - 1
	}
	if segment.ArchiveOrdinal != 0 {
		return segment.ArchiveOrdinal - 1
	}
	return segment.Sequence
}

func legacySegmentBefore(a, b domain.Segment) bool {
	if a.ArchiveOrdinal > 0 || b.ArchiveOrdinal > 0 {
		if a.SourceEpoch != b.SourceEpoch {
			return a.SourceEpoch < b.SourceEpoch
		}
		if a.ArchiveOrdinal != b.ArchiveOrdinal {
			if a.ArchiveOrdinal == 0 {
				return a.Sequence < b.Sequence
			}
			if b.ArchiveOrdinal == 0 {
				return false
			}
			return a.ArchiveOrdinal < b.ArchiveOrdinal
		}
	}
	return a.Sequence < b.Sequence
}
func findInit(track *domain.Track, id string) (domain.Segment, bool) {
	for _, segment := range track.InitSegments {
		if segment.ID == id {
			return segment, true
		}
	}
	return domain.Segment{}, false
}

func mediaContentType(storagePath string) string {
	switch strings.ToLower(filepath.Ext(storagePath)) {
	case ".ts":
		return "video/mp2t"
	case ".m4s":
		return "video/iso.segment"
	case ".mp4":
		return "video/mp4"
	case ".aac":
		return "audio/aac"
	case ".mp3":
		return "audio/mpeg"
	case ".vtt":
		return "text/vtt; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}

func writePlaylist(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, body)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func writeStorageError(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "recording not found")
		return
	}
	writeError(w, http.StatusInternalServerError, "recording storage operation failed")
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, limit int64, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON request")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request must contain exactly one JSON value")
	}
	return nil
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		w.Header().Set("Content-Security-Policy", "default-src 'none'; base-uri 'none'; connect-src 'self'; font-src 'self'; img-src 'self' data:; media-src 'self' blob:; script-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'; worker-src 'self' blob:; frame-ancestors 'none'; form-action 'self'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}
