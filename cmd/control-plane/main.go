// Command control-plane runs one generation of the management/control plane.
// Recorder Engines own all canonical acquisition and lifecycle writes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/resources"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/server"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storageprocess"
	"github.com/integrated-recorder/core/internal/systemsettings"
	"github.com/integrated-recorder/core/internal/watch"
)

func main() {
	if err := run(); err != nil {
		log.Printf("control-plane: %v", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := loadProcessConfig()
	if err != nil {
		return err
	}
	controlToken, err := controlplane.LoadPrivateIPCSecret(config.controlTokenFile)
	if err != nil {
		return fmt.Errorf("load private control IPC credential: %w", err)
	}
	if config.authDisabled && !isLoopbackAddress(config.controlAddr) {
		return errors.New("AUTH_DISABLED is allowed only with loopback binding")
	}

	mutationGate := controlplane.NewMutationGate()
	apiSlot := newHandlerSlot(passiveHealthHandler(config.generationID))
	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()
	appLifecycle := newControlApplicationLifecycle(
		mutationGate,
		apiSlot,
		passiveHealthHandler(config.generationID),
		processCtx,
		func(ctx context.Context) (controlApplicationRuntime, error) {
			return openControlApplication(config, mutationGate, ctx, processCtx)
		},
	)
	lifecycle, err := controlplane.NewLifecycleWithHooks(config.generationID, true, controlplane.LifecycleHooks{
		PrepareActivation:    appLifecycle.prepareActivation,
		Start:                appLifecycle.start,
		Prepare:              appLifecycle.prepareHandoff,
		Resume:               appLifecycle.resume,
		Drain:                appLifecycle.drain,
		DetachEngine:         appLifecycle.detachEngine,
		ValidateInstallation: appLifecycle.validateInstallation,
		InstallationReady:    appLifecycle.installationReady,
	})
	if err != nil {
		return err
	}
	ipcServer, err := runtimeipc.NewServer(config.controlIPCSocket, config.generationID, controlToken, lifecycle)
	if err != nil {
		return fmt.Errorf("initialize control IPC listener: %w", err)
	}
	listener, err := net.Listen("tcp", config.controlAddr)
	if err != nil {
		return errors.New("private control HTTP listener could not be opened")
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ipcCtx, stopIPC := context.WithCancel(context.Background())
	defer stopIPC()
	serveErr := make(chan error, 1)
	httpServer := &http.Server{Handler: apiSlot, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() { serveErr <- httpServer.Serve(listener) }()
	ipcErr := make(chan error, 1)
	go func() { ipcErr <- ipcServer.Serve(ipcCtx) }()
	log.Printf("control generation %s is ready in passive mode", config.generationID)
	var runErr error
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = errors.New("control HTTP listener failed")
		}
	case err := <-ipcErr:
		if err != nil {
			runErr = errors.New("control lifecycle listener failed")
		}
	case <-signalCtx.Done():
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()
	if _, err := lifecycle.Handle(shutdownCtx, controlplane.OperationControlDeactivate, nil); err != nil {
		runErr = errors.Join(runErr, err)
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, errors.New("HTTP shutdown failed"))
		_ = listener.Close()
	}
	stopIPC()
	if runErr != nil {
		return runErr
	}
	return nil
}

type processConfig struct {
	dataDir             string
	controlAddr         string
	generationID        string
	controlIPCSocket    string
	controlTokenFile    string
	engineCatalog       string
	activeEngine        string
	resourceSocket      string
	resourceTokenFile   string
	resourceOwner       string
	storageCatalogRoot  string
	storageProviderSet  string
	adapterDirs         []string
	authDisabled        bool
	forceSecureCookies  bool
	installationManaged bool
}

func loadProcessConfig() (processConfig, error) {
	c := processConfig{
		dataDir: strings.TrimSpace(os.Getenv("DATA_DIR")), controlAddr: strings.TrimSpace(os.Getenv("CONTROL_ADDR")),
		generationID: strings.TrimSpace(os.Getenv("CONTROL_GENERATION_ID")), controlIPCSocket: strings.TrimSpace(os.Getenv("CONTROL_IPC_SOCKET_PATH")),
		controlTokenFile: strings.TrimSpace(os.Getenv("CONTROL_IPC_TOKEN_FILE")), engineCatalog: strings.TrimSpace(os.Getenv("ENGINE_CATALOG_FILE")),
		activeEngine: strings.TrimSpace(os.Getenv("ACTIVE_ENGINE_GENERATION")), authDisabled: os.Getenv("AUTH_DISABLED") == "1", forceSecureCookies: os.Getenv("COOKIE_SECURE") == "1", installationManaged: os.Getenv("RUNTIME_INSTALLATION_MANAGED") == "1",
		resourceSocket: strings.TrimSpace(os.Getenv("RUNTIME_RESOURCE_SOCKET_PATH")), resourceTokenFile: strings.TrimSpace(os.Getenv("RUNTIME_RESOURCE_TOKEN_FILE")), resourceOwner: strings.TrimSpace(os.Getenv("RUNTIME_RESOURCE_OWNER")),
		storageCatalogRoot: strings.TrimSpace(os.Getenv("STORAGE_PROVIDER_CATALOG_ROOT")), storageProviderSet: strings.TrimSpace(os.Getenv("STORAGE_PROVIDER_SET_ID")),
	}
	if c.dataDir == "" || c.controlAddr == "" || c.generationID == "" || c.controlIPCSocket == "" || c.controlTokenFile == "" || c.engineCatalog == "" || c.activeEngine == "" {
		return processConfig{}, errors.New("required Control Plane runtime configuration is missing")
	}
	if !isLoopbackAddress(c.controlAddr) {
		return processConfig{}, errors.New("CONTROL_ADDR must bind to a loopback address")
	}
	resourceFields := []string{c.resourceSocket, c.resourceTokenFile, c.resourceOwner}
	resourcePresent := 0
	for _, value := range resourceFields {
		if value != "" {
			resourcePresent++
		}
	}
	if resourcePresent != 0 && resourcePresent != len(resourceFields) {
		return processConfig{}, errors.New("Runtime Host resource coordinator settings must be provided together")
	}
	if (c.storageCatalogRoot == "") != (c.storageProviderSet == "") {
		return processConfig{}, errors.New("storage provider generation settings must be provided together")
	}
	if c.storageCatalogRoot != "" && (!filepath.IsAbs(c.storageCatalogRoot) || filepath.Clean(c.storageCatalogRoot) != c.storageCatalogRoot || c.storageCatalogRoot != filepath.Join(c.dataDir, "runtime", "storage-providers")) {
		return processConfig{}, errors.New("storage provider catalog identity is invalid")
	}
	adapterDirsValue := strings.TrimSpace(os.Getenv("ADAPTER_DIR"))
	if adapterDirsValue == "" {
		adapterDirsValue = "./adapters"
	}
	c.adapterDirs = filepath.SplitList(adapterDirsValue)
	return c, nil
}

type controlApplicationRuntime interface {
	handler() http.Handler
	startBackground(context.Context) error
	pauseForHandoff(context.Context) error
	resumeAfterHandoff() error
	detachEngine(context.Context, string) error
	close(context.Context) error
}

type controlApplicationFactory func(context.Context) (controlApplicationRuntime, error)

// controlApplicationLifecycle keeps constructed services private to the
// Control process until the Host has fenced the previous generation. Passive
// process readiness does not open shared application state.
type controlApplicationLifecycle struct {
	mu       sync.Mutex
	gate     *controlplane.MutationGate
	slot     *handlerSlot
	passive  http.Handler
	lifetime context.Context
	open     controlApplicationFactory
	app      controlApplicationRuntime
	active   bool
}

type installationLifecycleApplication interface {
	validateInstallation(context.Context) error
	installationReady(context.Context) error
}

func newControlApplicationLifecycle(gate *controlplane.MutationGate, slot *handlerSlot, passive http.Handler, lifetime context.Context, open controlApplicationFactory) *controlApplicationLifecycle {
	return &controlApplicationLifecycle{gate: gate, slot: slot, passive: passive, lifetime: lifetime, open: open}
}

func (l *controlApplicationLifecycle) prepareActivation(ctx context.Context) error {
	if l == nil || l.gate == nil || l.slot == nil || l.open == nil {
		return errors.New("Control application preparation is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app != nil {
		return nil
	}
	created, err := l.open(ctx)
	if err != nil {
		return err
	}
	if created == nil {
		return errors.New("Control application preparation returned no services")
	}
	if err := ctx.Err(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = created.close(cleanupCtx)
		return err
	}
	l.app = created
	l.slot.Set(created.handler())
	return nil
}

func (l *controlApplicationLifecycle) start(context.Context) error {
	if l == nil {
		return errors.New("Control application lifecycle is unavailable")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app == nil {
		return errors.New("Control application was not prepared")
	}
	parent := l.lifetime
	if parent == nil {
		parent = context.Background()
	}
	if err := l.app.startBackground(parent); err != nil {
		l.gate.Fence()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		closeErr := l.app.close(cleanupCtx)
		l.app = nil
		l.slot.Set(l.passive)
		return errors.Join(err, closeErr)
	}
	l.gate.Activate()
	l.active = true
	return nil
}

func (l *controlApplicationLifecycle) prepareHandoff(ctx context.Context) error {
	if l == nil || l.gate == nil {
		return errors.New("Control mutation gate is unavailable")
	}
	l.gate.Fence()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app == nil {
		if err := l.gate.WaitForDrain(ctx); err != nil {
			l.gate.Activate()
			return err
		}
		return nil
	}
	if err := l.app.pauseForHandoff(ctx); err != nil {
		resumeErr := l.app.resumeAfterHandoff()
		if resumeErr == nil {
			l.gate.Activate()
		}
		return errors.Join(err, resumeErr)
	}
	l.active = false
	return nil
}

func (l *controlApplicationLifecycle) detachEngine(ctx context.Context, generationID string) error {
	if l == nil {
		return errors.New("Control application lifecycle is unavailable")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app == nil || !l.active {
		return errors.New("active Control application is unavailable")
	}
	return l.app.detachEngine(ctx, generationID)
}

func (l *controlApplicationLifecycle) validateInstallation(ctx context.Context) error {
	if l == nil {
		return errors.New("Control application lifecycle is unavailable")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app == nil || !l.active {
		return errors.New("active Control application is unavailable")
	}
	app, ok := l.app.(installationLifecycleApplication)
	if !ok {
		return errors.New("Control installation validation is unavailable")
	}
	return app.validateInstallation(ctx)
}

func (l *controlApplicationLifecycle) installationReady(ctx context.Context) error {
	if l == nil {
		return errors.New("Control application lifecycle is unavailable")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app == nil || !l.active {
		return errors.New("active Control application is unavailable")
	}
	app, ok := l.app.(installationLifecycleApplication)
	if !ok {
		return errors.New("Control installation activation is unavailable")
	}
	return app.installationReady(ctx)
}

func (l *controlApplicationLifecycle) resume(context.Context) error {
	if l == nil || l.gate == nil {
		return errors.New("Control mutation gate is unavailable")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app != nil {
		if err := l.app.resumeAfterHandoff(); err != nil {
			return err
		}
		l.gate.Activate()
		l.active = true
		return nil
	}
	l.gate.Activate()
	l.active = true
	return nil
}

func (l *controlApplicationLifecycle) drain(ctx context.Context) error {
	if l == nil || l.gate == nil {
		return nil
	}
	l.gate.Fence()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.app == nil {
		return nil
	}
	if err := l.app.close(ctx); err != nil {
		return err
	}
	l.app = nil
	l.active = false
	if l.slot != nil && l.passive != nil {
		l.slot.Set(l.passive)
	}
	return nil
}

func openControlApplication(config processConfig, gate *controlplane.MutationGate, setupCtx, processCtx context.Context) (*controlApplication, error) {
	if setupCtx == nil {
		setupCtx = context.Background()
	}
	store, storageRuntime, err := storageprocess.OpenStore(setupCtx, config.dataDir, config.storageCatalogRoot, config.storageProviderSet)
	if err != nil {
		return nil, errors.New("initialize generation-pinned archive read facade")
	}
	cleanupStorageRuntime := true
	defer func() {
		if cleanupStorageRuntime && storageRuntime != nil {
			_ = storageRuntime.Close()
		}
	}()
	settings, err := systemsettings.Open(config.dataDir)
	if err != nil {
		return nil, fmt.Errorf("initialize system settings: %w", err)
	}
	startupSettings := settings.Current()
	if err := store.ConfigureIngestOptions(startupSettings.Storage.IngestOptions()); err != nil {
		return nil, fmt.Errorf("configure Control storage observability: %w", err)
	}
	if err := configureRuntimeResources(store, config); err != nil {
		return nil, fmt.Errorf("configure Runtime Host resource coordinator: %w", err)
	}
	catalog, err := controlplane.LoadEngineCatalog(config.engineCatalog)
	if err != nil {
		return nil, fmt.Errorf("load recorder engine catalog: %w", err)
	}
	if catalog.ActiveGenerationID != config.activeEngine {
		return nil, errors.New("engine catalog active generation does not match process configuration")
	}
	manager, err := catalog.NewManagerRouter(store)
	if err != nil {
		return nil, fmt.Errorf("connect recorder engines: %w", err)
	}
	// Load settings only after the Host has prepared the active Control. A
	// candidate only opens shared application state after the previous owner
	// has fenced its background work. This prepared app is retained through the
	// route switch, so no stale settings/projections can overwrite old state.
	configStore, secretStore, stateStore, err := pluginconfig.NewTypedFileStoresAndState(config.dataDir)
	if err != nil {
		return nil, fmt.Errorf("initialize adapter configuration: %w", err)
	}
	configs, err := pluginconfig.NewService(configStore, secretStore)
	if err != nil {
		return nil, fmt.Errorf("initialize adapter configuration: %w", err)
	}
	adapters, err := adapterhost.DiscoverDirs(setupCtx, config.adapterDirs, configs, stateStore)
	if err != nil {
		return nil, fmt.Errorf("discover adapters: %w", err)
	}
	cleanupAdapters := true
	defer func() {
		if cleanupAdapters {
			adapters.Close()
		}
	}()
	products, err := management.Open(config.dataDir)
	if err != nil {
		return nil, fmt.Errorf("initialize management projections: %w", err)
	}
	for _, id := range products.DisabledAdapters() {
		if discovered, getErr := adapters.Get(id); getErr == nil && discovered.Descriptor != nil {
			if setErr := adapters.SetEnabled(id, false); setErr != nil {
				return nil, fmt.Errorf("apply adapter preference: %w", setErr)
			}
		}
	}
	integrityService, err := integrity.Open(config.dataDir, store, settings.IntegrityConcurrency())
	if err != nil {
		return nil, fmt.Errorf("initialize integrity service: %w", err)
	}
	cleanupIntegrity := true
	defer func() {
		if cleanupIntegrity {
			_ = integrityService.Close(context.Background())
		}
	}()
	exportService, err := derivative.Open(config.dataDir, store, os.Getenv("FFMPEG_PATH"), 2)
	if err != nil {
		return nil, fmt.Errorf("initialize export service: %w", err)
	}
	cleanupExport := true
	defer func() {
		if cleanupExport {
			_ = exportService.Close(context.Background())
		}
	}()
	previewService, err := preview.OpenWithGet(config.dataDir, store, manager.List, manager.Get, os.Getenv("FFMPEG_PATH"))
	if err != nil {
		return nil, fmt.Errorf("initialize preview service: %w", err)
	}
	cleanupPreview := true
	defer func() {
		if cleanupPreview {
			_ = previewService.Close(context.Background())
		}
	}()
	watchService, err := watch.New(config.dataDir, adapters, manager, watch.Options{
		Preview: func(recordingID, mode string) error {
			_, setErr := previewService.SetMode(recordingID, preview.Mode(mode))
			return setErr
		},
		CurrentPreview: func(recordingID string) *watch.PreviewSummary {
			recording, getErr := manager.Get(recordingID)
			if getErr != nil {
				return nil
			}
			summary := previewService.Summary(recording)
			return &watch.PreviewSummary{Mode: string(summary.Mode), State: string(summary.State), Available: summary.Available, FrameCount: summary.FrameCount, ImageArchiveOrdinal: summary.ImageArchiveOrdinal, LatestArchiveOrdinal: summary.LatestArchiveOrdinal, UpdatedAt: summary.UpdatedAt}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("initialize Watch service: %w", err)
	}
	cleanupWatch := true
	defer func() {
		if cleanupWatch {
			_ = watchService.Close(context.Background())
		}
	}()
	var authService *authn.Service
	if !config.authDisabled {
		if config.installationManaged && installation.ReadOnly(config.dataDir).State == installation.StateRecoveryRequired {
			authService, err = authn.OpenWithoutBootstrap(config.dataDir)
		} else {
			authService, err = authn.Open(config.dataDir)
		}
		if err != nil {
			return nil, fmt.Errorf("initialize administrator authentication: %w", err)
		}
	}
	api := server.NewWithOptions(manager, adapters, configs, server.Options{
		Storage: store, Management: products, Integrity: integrityService, Derivatives: exportService,
		Previews: previewService, Watches: watchService, Auth: authService, Settings: settings,
		InitialIntegrityConcurrency: settings.IntegrityConcurrency(), InitialStorageSettings: &startupSettings.Storage,
		ForceSecureCookies: config.forceSecureCookies, StartedAt: time.Now().UTC(), BuildInfo: buildinfo.Current(),
		MutationGate: gate, BackgroundMutationGate: gate,
		InstallationManaged: config.installationManaged,
	})
	cleanupAdapters, cleanupIntegrity, cleanupExport, cleanupPreview, cleanupWatch = false, false, false, false, false
	cleanupStorageRuntime = false
	return &controlApplication{adapters: adapters, manager: manager, store: store, storageRuntime: storageRuntime, integrity: integrityService, exports: exportService, previews: previewService, watches: watchService, api: api, gate: gate, lifetime: processCtx, dataDir: config.dataDir, installationManaged: config.installationManaged}, nil
}

func configureRuntimeResources(store *storage.Store, config processConfig) error {
	if store == nil {
		return errors.New("Control storage is unavailable")
	}
	if config.resourceSocket == "" && config.resourceTokenFile == "" && config.resourceOwner == "" {
		// Direct local development can still run without a Runtime Host. Docker
		// and managed Control generations always receive all three values.
		return nil
	}
	if config.resourceSocket == "" || config.resourceTokenFile == "" || config.resourceOwner == "" {
		return errors.New("Runtime Host resource coordinator settings are incomplete")
	}
	token, err := controlplane.LoadPrivateIPCSecret(config.resourceTokenFile)
	if err != nil {
		return errors.New("Runtime Host resource credential is unavailable")
	}
	client, err := resources.NewRuntimeClient(config.resourceSocket, token, config.resourceOwner)
	if err != nil {
		return errors.New("Runtime Host resource coordinator is unavailable")
	}
	if err := store.ConfigureRuntimeIngestCoordinator(client); err != nil {
		return err
	}
	return store.ConfigureRuntimeStorageTelemetry(controlRuntimeStorageTelemetryBridge{client: client})
}

type controlRuntimeStorageTelemetryBridge struct{ client *resources.RuntimeClient }

func (b controlRuntimeStorageTelemetryBridge) ReportStorageIOTotals(readBytes, writeBytes, errorsTotal uint64, ingest storage.IngestSnapshot) error {
	if b.client == nil {
		return errors.New("Runtime Host storage telemetry is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return b.client.ReportProcessTelemetry(ctx, readBytes, writeBytes, errorsTotal, ingest.QueueBytes, ingest.OldestPersistAgeSeconds)
}

func (b controlRuntimeStorageTelemetryBridge) StorageTelemetrySnapshot() (storage.RuntimeStorageTelemetrySnapshot, error) {
	if b.client == nil {
		return storage.RuntimeStorageTelemetrySnapshot{}, errors.New("Runtime Host storage telemetry is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resourcesSnapshot, err := b.client.Snapshot(ctx)
	if err != nil {
		return storage.RuntimeStorageTelemetrySnapshot{}, err
	}
	telemetrySnapshot, err := b.client.TelemetrySnapshot(ctx)
	if err != nil {
		return storage.RuntimeStorageTelemetrySnapshot{}, err
	}
	result := storage.RuntimeStorageTelemetrySnapshot{
		Throughput: storage.PoolThroughput{
			ReadBytesPerSecond: telemetrySnapshot.Throughput.ReadBytesPerSecond, WriteBytesPerSecond: telemetrySnapshot.Throughput.WriteBytesPerSecond,
			ReadBytesTotal: telemetrySnapshot.Throughput.ReadBytesTotal, WriteBytesTotal: telemetrySnapshot.Throughput.WriteBytesTotal,
		},
		EstimatedCeiling: storage.EstimatedCeiling{
			ReadBytesPerSecond: telemetrySnapshot.EstimatedCeiling.ReadBytesPerSecond, WriteBytesPerSecond: telemetrySnapshot.EstimatedCeiling.WriteBytesPerSecond,
			Source: telemetrySnapshot.EstimatedCeiling.Source,
		},
		ErrorsTotal: telemetrySnapshot.ErrorsTotal,
		Samples:     make([]storage.PoolSample, 0, len(telemetrySnapshot.Samples)),
		Ingest: storage.IngestSnapshot{
			BufferCapacityBytes: resourcesSnapshot.Limits.GlobalBufferBytes, PerRecordingCapacityBytes: resourcesSnapshot.Limits.PerRecordingBufferBytes,
			BufferUsedBytes: resourcesSnapshot.UsedBytes, ReservedBytes: resourcesSnapshot.UsedBytes,
			QueueObjects: resourcesSnapshot.QueueObjects, QueueBytes: resourcesSnapshot.QueueBytes,
			OldestPersistAgeSeconds: resourcesSnapshot.OldestAgeSeconds,
			ActiveWriters:           resourcesSnapshot.ActiveWriters, WriterConcurrency: resourcesSnapshot.Limits.WriterConcurrency,
			StorageErrorsTotal: telemetrySnapshot.ErrorsTotal,
		},
	}
	for _, sample := range telemetrySnapshot.Samples {
		result.Samples = append(result.Samples, storage.PoolSample{
			At: sample.At, ReadBytesPerSecond: sample.ReadBytesPerSecond,
			WriteBytesPerSecond: sample.WriteBytesPerSecond, BufferUsedBytes: sample.BufferUsedBytes,
			PersistQueueBytes: sample.PersistQueueBytes,
		})
	}
	return result, nil
}

type controlApplication struct {
	adapters            *adapterhost.Host
	manager             *recorderengine.ManagerRouter
	store               *storage.Store
	storageRuntime      *storageprocess.Runtime
	integrity           *integrity.Service
	exports             *derivative.Service
	previews            *preview.Service
	watches             *watch.Service
	api                 *server.Server
	gate                *controlplane.MutationGate
	lifetime            context.Context
	dataDir             string
	installationManaged bool

	mu                     sync.Mutex
	backgroundCancel       context.CancelFunc
	backgroundDone         chan struct{}
	previewCancel          context.CancelFunc
	previewDone            chan struct{}
	previewRestart         bool
	previewStopping        bool
	retentionCancel        context.CancelFunc
	retentionDone          chan struct{}
	retentionRestart       bool
	retentionStopping      bool
	storageMetricsCancel   context.CancelFunc
	storageMetricsDone     chan struct{}
	storageMetricsRestart  bool
	storageMetricsStopping bool
}

func (a *controlApplication) handler() http.Handler { return a.api }

func (a *controlApplication) installationIsReady() bool {
	return a == nil || !a.installationManaged || installation.ReadOnly(a.dataDir).State == installation.StateReady
}

func (a *controlApplication) validateInstallation(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if a == nil || a.store == nil || a.manager == nil || a.adapters == nil || ctx.Err() != nil {
		return errors.New("Control installation dependencies are unavailable")
	}
	probe := a.store.RunSetupProbe()
	if !probe.WritePassed || !probe.DurabilityPassed {
		return errors.New("primary storage self-test failed")
	}
	return nil
}

func (a *controlApplication) installationReady(ctx context.Context) error {
	if a == nil {
		return errors.New("Control application is unavailable")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	parent := a.lifetime
	if parent == nil {
		parent = context.Background()
	}
	return a.startBackground(parent)
}

func (a *controlApplication) detachEngine(ctx context.Context, generationID string) error {
	if a == nil || a.manager == nil {
		return errors.New("recorder Engine manager is unavailable")
	}
	err := a.manager.DetachGenerationContext(ctx, generationID)
	return normalizeEngineDetachResult(err)
}

func normalizeEngineDetachResult(err error) error {
	if errors.Is(err, recorderengine.ErrGenerationNotAttached) {
		// A candidate Control only attaches Engine processes that were ready
		// when its immutable catalog was created. A dormant rollback Engine can
		// therefore already be absent when Host later asks to retire it. Treat
		// that state as an idempotent detach; all other IPC/inventory failures
		// still prevent Host retirement.
		return nil
	}
	return err
}

func (a *controlApplication) startBackground(parent context.Context) error {
	if !a.installationIsReady() {
		return nil
	}
	a.mu.Lock()
	if a.backgroundCancel != nil {
		a.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	a.backgroundCancel = cancel
	a.backgroundDone = make(chan struct{})
	a.lifetime = ctx
	done := a.backgroundDone
	a.mu.Unlock()
	go func() {
		defer close(done)
		var workers sync.WaitGroup
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := a.watches.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("Control Watch background loop stopped")
			}
		}()
		workers.Wait()
	}()
	if err := a.startPreviewReconciler(ctx); err != nil {
		return err
	}
	if err := a.startRetentionLoop(ctx); err != nil {
		_ = a.stopPreviewReconciler(context.Background())
		return err
	}
	if err := a.startStorageMetricsLoop(ctx); err != nil {
		_ = a.stopRetentionLoop(context.Background())
		_ = a.stopPreviewReconciler(context.Background())
		return err
	}
	return nil
}

func (a *controlApplication) startStorageMetricsLoop(parent context.Context) error {
	if a == nil || a.store == nil {
		return errors.New("Control storage metrics are unavailable")
	}
	interval := a.store.MetricsSamplingInterval()
	if interval <= 0 || interval > time.Hour {
		return errors.New("Control storage metrics interval is invalid")
	}
	a.mu.Lock()
	if a.storageMetricsCancel != nil {
		a.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	a.storageMetricsCancel, a.storageMetricsDone = cancel, done
	a.mu.Unlock()
	go func() {
		defer func() {
			a.mu.Lock()
			restart := a.storageMetricsDone == done && a.storageMetricsRestart
			if a.storageMetricsDone == done {
				a.storageMetricsCancel, a.storageMetricsDone, a.storageMetricsRestart, a.storageMetricsStopping = nil, nil, false, false
			}
			close(done)
			a.mu.Unlock()
			if restart && parent.Err() == nil {
				_ = a.startStorageMetricsLoop(parent)
			}
		}()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		_ = a.store.PoolMetrics()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = a.store.PoolMetrics()
			}
		}
	}()
	return nil
}

func (a *controlApplication) stopStorageMetricsLoop(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	cancel, done := a.storageMetricsCancel, a.storageMetricsDone
	if cancel != nil && done != nil && a.storageMetricsDone == done {
		a.storageMetricsStopping = true
	}
	a.mu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startPreviewReconciler starts only the reconciliation producer. Preview
// workers and durable service state stay alive across a reversible handoff.
func (a *controlApplication) startPreviewReconciler(parent context.Context) error {
	a.mu.Lock()
	if a.previewCancel != nil {
		a.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	a.previewCancel, a.previewDone = cancel, done
	a.mu.Unlock()
	go func() {
		defer func() {
			a.mu.Lock()
			restart := a.previewDone == done && a.previewRestart
			if a.previewDone == done {
				a.previewCancel, a.previewDone, a.previewRestart, a.previewStopping = nil, nil, false, false
			}
			close(done)
			a.mu.Unlock()
			if restart && parent.Err() == nil {
				_ = a.startPreviewReconciler(parent)
			}
		}()
		if err := a.previews.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Control preview background loop stopped")
		}
	}()
	return nil
}

func (a *controlApplication) stopPreviewReconciler(ctx context.Context) error {
	a.mu.Lock()
	cancel, done := a.previewCancel, a.previewDone
	a.mu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	a.mu.Lock()
	if a.previewDone == done {
		a.previewStopping = true
	}
	a.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *controlApplication) startRetentionLoop(parent context.Context) error {
	a.mu.Lock()
	if a.retentionCancel != nil {
		a.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	a.retentionCancel, a.retentionDone = cancel, done
	a.mu.Unlock()
	go func() {
		defer func() {
			a.mu.Lock()
			restart := a.retentionDone == done && a.retentionRestart
			if a.retentionDone == done {
				a.retentionCancel, a.retentionDone, a.retentionRestart, a.retentionStopping = nil, nil, false, false
			}
			close(done)
			a.mu.Unlock()
			if restart && parent.Err() == nil {
				_ = a.startRetentionLoop(parent)
			}
		}()
		a.api.RunRetention(ctx)
	}()
	return nil
}

func (a *controlApplication) stopRetentionLoop(ctx context.Context) error {
	a.mu.Lock()
	cancel, done := a.retentionCancel, a.retentionDone
	a.mu.Unlock()
	if cancel == nil || done == nil {
		return nil
	}
	a.mu.Lock()
	if a.retentionDone == done {
		a.retentionStopping = true
	}
	a.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *controlApplication) pauseForHandoff(ctx context.Context) error {
	if err := a.watches.Pause(ctx); err != nil {
		return err
	}
	if err := a.stopPreviewReconciler(ctx); err != nil {
		return err
	}
	if err := a.gate.WaitForDrain(ctx); err != nil {
		return err
	}
	// No new retention pass can enter after the fence and the shared drain
	// proves the current pass has completed. Stop and join its ticker before a
	// candidate opens the same durable management projections.
	if err := a.stopRetentionLoop(ctx); err != nil {
		return err
	}
	// HTTP mutations and scheduled retention are fenced above. Watch checks
	// have been joined, and the preview producer has stopped, so these waits now
	// cover only accepted work from the old generation.
	type waiter struct {
		name string
		wait func(context.Context) error
	}
	waiters := []waiter{
		{name: "integrity", wait: a.integrity.WaitForIdle},
		{name: "exports", wait: a.exports.WaitForIdle},
		{name: "previews", wait: a.previews.WaitForIdle},
	}
	var workers sync.WaitGroup
	var errMu sync.Mutex
	var joined error
	for _, item := range waiters {
		item := item
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := item.wait(ctx); err != nil {
				errMu.Lock()
				joined = errors.Join(joined, fmt.Errorf("wait for %s jobs: %w", item.name, err))
				errMu.Unlock()
			}
		}()
	}
	workers.Wait()
	return errors.Join(joined, a.stopStorageMetricsLoop(ctx))
}

func (a *controlApplication) resumeAfterHandoff() error {
	if !a.installationIsReady() {
		return nil
	}
	if err := a.watches.Resume(); err != nil {
		return err
	}
	a.mu.Lock()
	previewRunning := a.previewCancel != nil
	if previewRunning && a.previewStopping {
		// Prepare may have timed out while a filesystem reconcile was still
		// unwinding. Restart as soon as that producer exits rather than start a
		// second concurrent Run loop.
		a.previewRestart = true
	}
	a.mu.Unlock()
	if !previewRunning {
		if err := a.startPreviewReconciler(a.lifetime); err != nil {
			return err
		}
	}
	a.mu.Lock()
	retentionRunning := a.retentionCancel != nil
	if retentionRunning && a.retentionStopping {
		a.retentionRestart = true
	}
	a.mu.Unlock()
	if !retentionRunning {
		if err := a.startRetentionLoop(a.lifetime); err != nil {
			return err
		}
	}
	return a.startStorageMetricsLoop(a.lifetime)
}

func (a *controlApplication) close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	cancel, done := a.backgroundCancel, a.backgroundDone
	a.previewRestart = false
	a.retentionRestart = false
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	previewErr := a.stopPreviewReconciler(ctx)
	retentionErr := a.stopRetentionLoop(ctx)
	storageMetricsErr := a.stopStorageMetricsLoop(ctx)
	var joined error
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			joined = ctx.Err()
		}
	}
	joined = errors.Join(previewErr, retentionErr, storageMetricsErr, joined, a.watches.Close(ctx), a.previews.Close(ctx), a.exports.Close(ctx), a.integrity.Close(ctx))
	a.adapters.Close()
	if a.storageRuntime != nil {
		joined = errors.Join(joined, a.storageRuntime.Close())
		a.storageRuntime = nil
	}
	return joined
}

type handlerBox struct{ handler http.Handler }

type handlerSlot struct{ current atomic.Pointer[handlerBox] }

func newHandlerSlot(handler http.Handler) *handlerSlot {
	s := &handlerSlot{}
	s.Set(handler)
	return s
}

func (s *handlerSlot) Set(handler http.Handler) {
	if handler != nil {
		s.current.Store(&handlerBox{handler: handler})
	}
}

func (s *handlerSlot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	box := s.current.Load()
	if box == nil || box.handler == nil {
		http.Error(w, "control generation is unavailable", http.StatusServiceUnavailable)
		return
	}
	box.handler.ServeHTTP(w, r)
}

func passiveHealthHandler(generation string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "generation_id": generation, "state": "passive"})
	})
	return mux
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
